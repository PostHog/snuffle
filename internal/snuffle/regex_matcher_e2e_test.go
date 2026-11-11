package snuffle

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// A regex with a literal prefix must match the whole pattern on every storage
// path, not just the prefix: a-hogs?-kafka matches a-hog-kafka only.
func TestRegexPrefixMatchersEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	for _, layout := range []schemaLayout{schemaLayoutCurrent, schemaLayoutPostHog} {
		test.Run(string(layout), func(test *testing.T) {
			test.Setenv("CH_SCHEMA_LAYOUT", string(layout))
			assertRegexPrefixMatchers(test)
		})
	}
}

func assertRegexPrefixMatchers(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_regex_e2e_%d", time.Now().UnixNano())
	if err := root.Exec(ctx, "CREATE DATABASE "+quoteIdent(database)); err != nil {
		test.Fatal(err)
	}
	defer func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := root.Exec(cleanup, "DROP DATABASE "+quoteIdent(database)+" SYNC"); err != nil {
			test.Errorf("clean test database: %v", err)
		}
	}()
	cfg.CHDatabase = database
	createMetricsSchema(test, ctx, NewClickHouseClient(cfg))

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()

	const metric = "regex_e2e_memory_bytes"
	write := &prompb.WriteRequest{}
	for _, container := range []string{"a-hog-kafka", "a-hogflows-pg", "a-hogoverflow-kafka"} {
		series := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: labels.MetricName, Value: metric},
			{Name: "container", Value: container},
		}}
		for index := range 3 {
			series.Samples = append(series.Samples, prompb.Sample{Timestamp: e2eStartMS + int64(index)*30_000, Value: float64(index)})
		}
		write.Timeseries = append(write.Timeseries, series)
	}
	postRemoteWrite(test, api.URL, write)

	containers := func(result []sampleResultDTO) []string {
		out := []string{}
		for _, series := range result {
			out = append(out, series.Metric["container"])
		}
		slices.Sort(out)
		return out
	}
	instant := func(expression string) []string {
		return containers(apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {expression}, "time": {"1700000070"}}).Result)
	}
	ranged := func(expression string) []string {
		return containers(apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {expression}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"30s"}}).Result)
	}
	series := func(selector string) []string {
		out := []string{}
		for _, labelSet := range apiGet[[]map[string]string](test, api.URL, "/api/v1/series", url.Values{"match[]": {selector}, "start": {"1700000010"}, "end": {"1700000070"}}) {
			out = append(out, labelSet["container"])
		}
		slices.Sort(out)
		return out
	}

	for matcher, want := range map[string][]string{
		`container=~"a-hogs?-kafka"`:     {"a-hog-kafka"},
		`container=~"^a-hogs?-kafka$"`:   {"a-hog-kafka"},
		`container=~"a-ho.-kafka"`:       {"a-hog-kafka"},
		`container=~"a-hog[a-z]*-kafka"`: {"a-hog-kafka", "a-hogoverflow-kafka"},
		`container=~"a-hogs?-zzz"`:       {},
		`container=~"a-hog.*"`:           {"a-hog-kafka", "a-hogflows-pg", "a-hogoverflow-kafka"},
		`container!~"a-hogs?-kafka"`:     {"a-hogflows-pg", "a-hogoverflow-kafka"},
		`container!~"a-hog.*"`:           {},
	} {
		selector := metric + "{" + matcher + "}"
		for name, got := range map[string][]string{
			"instant selector":       instant(selector),
			"instant count by":       instant(`count by (container) (` + selector + `)`),
			"instant sum by":         instant(`sum by (container) (` + selector + `)`),
			"instant topk":           instant(`topk(10, ` + selector + `)`),
			"range selector":         ranged(selector),
			"range count by":         ranged(`count by (container) (` + selector + `)`),
			"range sum by rate":      ranged(`sum by (container) (rate(` + selector + `[1m]))`),
			"range max_over_time by": ranged(`max by (container) (max_over_time(` + selector + `[1m]))`),
			"series":                 series(selector),
		} {
			if !reflect.DeepEqual(got, want) {
				test.Errorf("%s %s = %v, want %v", name, selector, got, want)
			}
		}
	}
}
