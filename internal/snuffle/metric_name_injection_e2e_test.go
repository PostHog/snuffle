package snuffle

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// TestMetricNameRegexInjectionEndToEnd checks that a __name__ regex resolved
// to a stored metric name never turns that name into query syntax. The name
// below closes the selector and the default_rollup around it and appends
// "or vector(123)": printed bare into the rewritten query, it made
// sum({__name__=~"review_injected.*",job="expected"}) return 123. Every path
// the resolver feeds must return the series' own value, 7.
func TestMetricNameRegexInjectionEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	for _, layout := range []schemaLayout{schemaLayoutCurrent, schemaLayoutPostHog} {
		test.Run(string(layout), func(test *testing.T) {
			test.Setenv("CH_SCHEMA_LAYOUT", string(layout))
			assertMetricNameRegexInjection(test)
		})
	}
}

func assertMetricNameRegexInjection(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_name_injection_e2e_%d", time.Now().UnixNano())
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

	const name = `review_injected[5m],0,300000,300000,1)) or vector(123) #`
	series := prompb.TimeSeries{Labels: []prompb.Label{
		{Name: labels.MetricName, Value: name},
		{Name: "job", Value: "expected"},
	}}
	for i := range 240 {
		series.Samples = append(series.Samples, prompb.Sample{Timestamp: e2eStartMS + int64(i)*30_000, Value: 7})
	}
	postRemoteWrite(test, api.URL, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{series}})

	const start, end = e2eStartMS/1000 + 3600, e2eStartMS/1000 + 7200
	const selector = `{__name__=~"review_injected.*",job="expected"}`
	for _, query := range []string{
		`sum(` + selector + `)`,
		`sum(rate(` + selector + `[5m])) * 0 + 7`,
		`max_over_time(` + selector + `[5m])`,
		// A range op (topk_*) and default evaluate their parts separately.
		`topk_max(1, ` + selector + `)`,
		`sum(` + selector + `) default 0`,
	} {
		test.Run(query, func(test *testing.T) {
			check := func(kind string, values []string) {
				test.Helper()
				if len(values) == 0 {
					test.Fatalf("%s: no points", kind)
				}
				for _, value := range values {
					if value != "7" {
						test.Fatalf("%s = %v, want only 7", kind, values)
					}
				}
			}
			ranged := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {query}, "start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {"60"}, "nocache": {"1"}})
			if len(ranged.Result) != 1 {
				test.Fatalf("range: %d series, want 1: %+v", len(ranged.Result), ranged.Result)
			}
			var values []string
			for _, point := range ranged.Result[0].Values {
				values = append(values, sampleString(point))
			}
			check("range", values)
			instant := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {query}, "time": {fmt.Sprint(end)}})
			if len(instant.Result) != 1 {
				test.Fatalf("instant: %d series, want 1: %+v", len(instant.Result), instant.Result)
			}
			check("instant", []string{sampleString(instant.Result[0].Value)})
		})
	}

	window := url.Values{"start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "match[]": {selector}}
	found := apiGet[[]map[string]string](test, api.URL, "/api/v1/series", window)
	if len(found) != 1 || found[0][labels.MetricName] != name || found[0]["job"] != "expected" {
		test.Errorf("series = %v, want the one stored series", found)
	}
	if names := apiGet[[]string](test, api.URL, "/api/v1/label/__name__/values", window); len(names) != 1 || names[0] != name {
		test.Errorf("__name__ values = %v, want %q", names, name)
	}
	if jobs := apiGet[[]string](test, api.URL, "/api/v1/label/job/values", window); len(jobs) != 1 || jobs[0] != "expected" {
		test.Errorf("job values = %v, want [expected]", jobs)
	}
}
