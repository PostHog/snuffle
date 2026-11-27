package snuffle

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// TestMetricNameRegexEndToEnd checks ENG-052: a selector that names its
// metric through a __name__ regex returns what the plain name returns, and in
// the PostHog layout it resolves to the metric names first, so it takes the
// plain name's paths. Expected values come from VictoriaMetrics on the same
// samples: counters eng052_reads_total{inst="a",relname="p0".."p3"} at
// i*(p+1) and eng052_writes_total at 2*i*(p+1), and a histogram count of i,
// every 30s for two hours.
func TestMetricNameRegexEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	for _, layout := range []schemaLayout{schemaLayoutCurrent, schemaLayoutPostHog} {
		test.Run(string(layout), func(test *testing.T) {
			test.Setenv("CH_SCHEMA_LAYOUT", string(layout))
			assertMetricNameRegex(test)
		})
	}
}

func assertMetricNameRegex(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_name_regex_e2e_%d", time.Now().UnixNano())
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
	client := NewClickHouseClient(cfg)
	createMetricsSchema(test, ctx, client)

	server := newServer(cfg)
	mux := http.NewServeMux()
	server.routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()

	write := &prompb.WriteRequest{}
	for metric, factor := range map[string]int{"eng052_reads_total": 1, "eng052_writes_total": 2} {
		for p := range 4 {
			series := prompb.TimeSeries{Labels: []prompb.Label{
				{Name: labels.MetricName, Value: metric},
				{Name: "inst", Value: "a"},
				{Name: "relname", Value: fmt.Sprintf("p%d", p)},
			}}
			for i := range 240 {
				series.Samples = append(series.Samples, prompb.Sample{Timestamp: e2eStartMS + int64(i)*30_000, Value: float64(factor * i * (p + 1))})
			}
			write.Timeseries = append(write.Timeseries, series)
		}
	}
	postRemoteWrite(test, api.URL, write)
	postHog := cfg.postHogSchemaLayout()
	if postHog {
		// A histogram, read through its virtual _bucket, _count and _sum names.
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, aggregation_temporality, has_labels, resource_attributes, attributes, metric_type, value, count, histogram_bounds, histogram_counts)
			SELECT %d, 'eng052hist_latency', cityHash64('eng052hist_latency'), fromUnixTimestamp64Milli(%d + number * 30000, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'cumulative', true, map(), map('tool', 'x'), 'histogram', number * 2.5, number, [1.0], [number, 0]
			FROM numbers(240)`, tableName(database, cfg.MetricsInputTable), e2eTeamID, e2eStartMS)
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}

	const start, end = e2eStartMS/1000 + 3600, e2eStartMS/1000 + 7200
	byName := func(series []sampleResultDTO, value func(sampleResultDTO) []string) map[string][]string {
		out := map[string][]string{}
		for _, s := range series {
			out[s.Metric[labels.MetricName]] = value(s)
		}
		return out
	}
	const relnames = `inst="a",relname=~"p[0-9]+"`
	cases := []struct {
		query string
		want  map[string]float64
	}{
		{`sum(rate(eng052_reads_total{` + relnames + `}[1h]))`, map[string]float64{"": 1.0 / 3}},
		{`sum(rate({__name__=~"eng052_reads_total",` + relnames + `}[1h]))`, map[string]float64{"": 1.0 / 3}},
		{`sum(rate({__name__=~"eng052_r.*",` + relnames + `}[1h]))`, map[string]float64{"": 1.0 / 3}},
		{`sum(rate({__name__!~"eng052_w.*",inst="a"}[5m]))`, map[string]float64{"": 1.0 / 3}},
		{`sum by (__name__) (rate({__name__=~"eng052_.*_total",inst="a"}[5m]) keep_metric_names)`, map[string]float64{"eng052_reads_total": 1.0 / 3, "eng052_writes_total": 2.0 / 3}},
		{`count({__name__=~"eng052_.*",inst="a"})`, map[string]float64{"": 8}},
	}
	if postHog {
		cases = append(cases,
			struct {
				query string
				want  map[string]float64
			}{`sum(rate({__name__=~"eng052hist_.*_count"}[5m]))`, map[string]float64{"": 1.0 / 30}})
	}
	for _, tc := range cases {
		test.Run(tc.query, func(test *testing.T) {
			check := func(kind string, got map[string][]string, points int) {
				test.Helper()
				if len(got) != len(tc.want) {
					test.Fatalf("%s: %v, want %v", kind, got, tc.want)
				}
				for name, want := range tc.want {
					if len(got[name]) != points {
						test.Fatalf("%s %q: %d points, want %d", kind, name, len(got[name]), points)
					}
					for _, value := range got[name] {
						v, err := strconv.ParseFloat(value, 64)
						if err != nil || math.Abs(v-want) > 1e-9 {
							test.Fatalf("%s %q = %s, want %v", kind, name, value, want)
						}
					}
				}
			}
			ranged := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {tc.query}, "start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {"60"}, "nocache": {"1"}})
			check("range", byName(ranged.Result, func(s sampleResultDTO) []string {
				values := []string{}
				for _, point := range s.Values {
					values = append(values, sampleString(point))
				}
				return values
			}), 61)
			instant := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {tc.query}, "time": {fmt.Sprint(end)}})
			check("instant", byName(instant.Result, func(s sampleResultDTO) []string { return []string{sampleString(s.Value)} }), 1)
		})
	}

	series := func(selector string) []map[string]string {
		return apiGet[[]map[string]string](test, api.URL, "/api/v1/series", url.Values{"match[]": {selector}, "start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}})
	}
	if plain, regex := series("eng052_reads_total"), series(`{__name__=~"eng052_r.*"}`); len(plain) != 4 || !reflect.DeepEqual(plain, regex) {
		test.Errorf("series of the regex = %v, want %v", regex, plain)
	}
	names := apiGet[[]string](test, api.URL, "/api/v1/label/__name__/values", url.Values{"match[]": {`{__name__=~"eng052.*"}`}, "start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}})
	wantNames := []string{"eng052_reads_total", "eng052_writes_total"}
	if postHog {
		// The name index lists the stored histogram's own name too.
		wantNames = append(wantNames, "eng052hist_latency", "eng052hist_latency_bucket", "eng052hist_latency_count", "eng052hist_latency_sum")
	}
	slices.Sort(wantNames)
	if !reflect.DeepEqual(names, wantNames) {
		test.Errorf("__name__ values = %v, want %v", names, wantNames)
	}

	// The PostHog layout resolves the regex to names before planning, so the
	// query is the plain one and takes its fast paths and pushdowns.
	team := server.withTeamID(e2eTeamID)
	for query, want := range map[string]string{
		`sum(rate({__name__=~"eng052_r.*",inst="a"}[1h]))`:              `sum(rate({__name__=~"eng052_reads_total",inst="a"}[1h]))`,
		`sum(rate({__name__!~"eng052_w.*|eng052hist.*",inst="a"}[1h]))`: `sum(rate({__name__=~"eng052_reads_total",inst="a"}[1h]))`,
		`sum(rate({__name__=~"eng052_.*_total"}[1h]))`:                  `sum(rate({__name__=~"eng052_reads_total|eng052_writes_total"}[1h]))`,
		`sum(rate({__name__=~"eng052hist_.*_count"}[1h]))`:              `sum(rate({__name__=~"eng052hist_latency_count"}[1h]))`,
		`sum(rate({__name__=~"eng052_reads_total",inst="a"}[1h]))`:      `sum(rate({__name__=~"eng052_reads_total",inst="a"}[1h]))`,
		`sum(rate({__name__=~"eng052_nothing.*",inst="a"}[1h]))`:        `sum(rate({__name__=~"eng052_nothing.*",inst="a"}[1h]))`,
	} {
		if !postHog {
			want = query
		}
		got, err := team.resolveMetricNames(ctx, query, time.Unix(start, 0), time.Unix(end, 0), time.Minute)
		if err != nil || got != want {
			test.Errorf("resolveMetricNames(%s) = %s, %v, want %s", query, got, err, want)
		}
	}
}
