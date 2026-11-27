package snuffle

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

const nanFlagMetric = "snuffle_e2e_flag_eval_seconds_bucket"

// nanFlagSeries returns classic histograms shaped like flag evaluation timing:
// flags a, b and c see observations every 15 s, d stops observing after
// 1699999950 and e never observes. Samples run from 1699999410 to 1700000370.
func nanFlagSeries() []prompb.TimeSeries {
	bounds := []string{"0.005", "0.05", "0.5", "+Inf"}
	observations := map[string]func(k int) [4]int{
		"a": func(k int) [4]int { return [4]int{k % 2, 1 + k%3, 1 + k%2, 1} },
		"b": func(k int) [4]int { return [4]int{0, 2, k % 4, 1} },
		"c": func(k int) [4]int { return [4]int{1, 0, 1, k % 3} },
		"d": func(k int) [4]int {
			if k > 36 {
				return [4]int{}
			}
			return [4]int{1, k % 2, 2, 0}
		},
		"e": func(int) [4]int { return [4]int{} },
	}
	var series []prompb.TimeSeries
	for flag, observe := range observations {
		samples := make([][]prompb.Sample, len(bounds))
		totals := [4]int{3, 2, 3, 1}
		for k := 0; k <= 64; k++ {
			if k > 0 {
				for index, count := range observe(k) {
					totals[index] += count
				}
			}
			cumulative := 0
			for index := range bounds {
				cumulative += totals[index]
				samples[index] = append(samples[index], prompb.Sample{Timestamp: (1699999410 + int64(k)*15) * 1000, Value: float64(cumulative)})
			}
		}
		for index, bound := range bounds {
			series = append(series, prompb.TimeSeries{Labels: []prompb.Label{
				{Name: "__name__", Value: nanFlagMetric},
				{Name: "flag", Value: flag},
				{Name: "le", Value: bound},
			}, Samples: samples[index]})
		}
	}
	return series
}

// assertNaNSkipped checks aggregations over per-series quantiles where some
// series have no observations in the window (ENG-051): VictoriaMetrics gives
// those series no value, so the aggregations skip them. Expected values are
// VictoriaMetrics (Docker, victoriametrics/victoria-metrics:latest) on the same
// samples, at 1700000010 to 1700000370 every 60 s; NaN marks a step without a
// point. The instant query at 1700000370 gives the last step.
func assertNaNSkipped(t *testing.T, baseURL string) {
	t.Helper()
	postRemoteWrite(t, baseURL, &prompb.WriteRequest{Timeseries: nanFlagSeries()})
	nan := math.NaN()
	quantiles := `histogram_quantile(0.5, rate(` + nanFlagMetric + `[5m]))`
	a := []float64{0.05750000000000005, 0.049451219512195124, 0.05, 0.05750000000000005, 0.049451219512195124, 0.05, 0.05750000000000005}
	b := []float64{0.12499999999999999, 0.12499999999999999, 0.12499999999999999, 0.12499999999999999, 0.12499999999999999, 0.12499999999999999, 0.12499999999999999}
	c := []float64{0.26375, 0.28625000000000006, 0.275, 0.26375, 0.28625000000000006, 0.275, 0.26375}
	d := []float64{0.10625000000000001, 0.10625000000000004, 0.10625000000000001, 0.10625000000000001, nan, nan, nan}
	for _, check := range []struct {
		query string
		// want holds the values of each series by its flag label.
		want map[string][]float64
	}{
		{quantiles, map[string][]float64{"a": a, "b": b, "c": c, "d": d}},
		{`avg(` + quantiles + `)`, map[string][]float64{"": {0.138125, 0.1417378048780488, 0.1390625, 0.138125, 0.15356707317073173, 0.15, 0.14875000000000002}}},
		{`sum(` + quantiles + `)`, map[string][]float64{"": {0.5525, 0.5669512195121952, 0.55625, 0.5525, 0.46070121951219517, 0.45, 0.44625000000000004}}},
		{`min(` + quantiles + `)`, map[string][]float64{"": a}},
		{`max(` + quantiles + `)`, map[string][]float64{"": c}},
		{`count(` + quantiles + `)`, map[string][]float64{"": {4, 4, 4, 4, 3, 3, 3}}},
		{`quantile(0.5, ` + quantiles + `)`, map[string][]float64{"": {0.115625, 0.115625, 0.115625, 0.115625, 0.12499999999999999, 0.12499999999999999, 0.12499999999999999}}},
		{`stddev(` + quantiles + `)`, map[string][]float64{"": {0.07660011830930809, 0.08794902896657247, 0.08319487194983836, 0.07660011830930809, 0.09876056521467924, 0.09354143466934854, 0.08585962380537195}}},
		{`topk_avg(2, ` + quantiles + `)`, map[string][]float64{"b": b, "c": c}},
		{quantiles + ` or label_set(vector(7), "flag", "e")`, map[string][]float64{"a": a, "b": b, "c": c, "d": d, "e": {7, 7, 7, 7, 7, 7, 7}}},
		// The fast paths compute x / scalar in ClickHouse; 0/0 is NaN there.
		{`count(` + nanFlagMetric + ` * 0 / 0)`, map[string][]float64{}},
		{`count(` + nanFlagMetric + ` / 0)`, map[string][]float64{"": {20, 20, 20, 20, 20, 20, 20}}},
	} {
		t.Run(check.query, func(t *testing.T) {
			ranged, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {check.query}, "start": {"1700000010"}, "end": {"1700000370"}, "step": {"60"}})
			if errText != "" {
				t.Fatalf("range: %s", errText)
			}
			instant, errText := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {check.query}, "time": {"1700000370"}})
			if errText != "" {
				t.Fatalf("instant: %s", errText)
			}
			got, gotInstant := map[string][]float64{}, map[string][]float64{}
			for _, result := range ranged.Result {
				values := []float64{nan, nan, nan, nan, nan, nan, nan}
				for _, point := range result.Values {
					step := (int(point[0].(float64)) - 1700000010) / 60
					values[step], _ = strconv.ParseFloat(point[1].(string), 64)
				}
				got[result.Metric["flag"]] = values
			}
			for _, result := range instant.Result {
				value, _ := strconv.ParseFloat(result.Value[1].(string), 64)
				gotInstant[result.Metric["flag"]] = []float64{value}
			}
			wantInstant := map[string][]float64{}
			for flag, values := range check.want {
				if last := values[len(values)-1]; !math.IsNaN(last) {
					wantInstant[flag] = []float64{last}
				}
			}
			assertNaNSkippedValues(t, "range", got, check.want)
			assertNaNSkippedValues(t, "instant", gotInstant, wantInstant)
		})
	}
}

// nanDeltaPoints returns PostHog delta histograms with a row every 20 s for 40
// minutes: series 0 always observes, series 1 stops after 15 minutes and
// series 2 never observes, so their rows hold zero counts.
func nanDeltaPoints(base int64) []quantileTestPoint {
	var points []quantileTestPoint
	for series := 0; series < 3; series++ {
		for k := 0; k < 120; k++ {
			counts := make([]uint64, 5)
			if series == 0 || (series == 1 && k < 45) {
				counts[(k*31+series*7)%5] = uint64(1 + k%3)
				counts[(k*17+series)%3] += 2
			}
			points = append(points, quantileTestPoint{series: series, timestamp: base + int64(k)*20000, temporality: "delta", bounds: []float64{0.1, 0.5, 1, 5}, counts: counts})
		}
	}
	return points
}

// TestPostHogHistogramNaNSkipped checks ENG-051 on PostHog histogram rows,
// where the histogram pushdown and the compact path answer instead of the
// engine over raw rows. VictoriaMetrics (Docker,
// victoriametrics/victoria-metrics:latest) has no delta temporality, so the
// expected values read sum_over_time over the same rows imported as classic
// buckets; a quantile does not change with the scale that rate divides by.
func TestPostHogHistogramNaNSkipped(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.AllowUnauthenticated = true
	cfg.RemoteWriteInterval = 0
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_histogram_nan_%d", time.Now().UnixNano())
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
	cfg.TeamID = e2eTeamID
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))
	insertQuantileTestPoints(test, ctx, client, tableName(database, cfg.MetricsInputTable), "nan_delta", nanDeltaPoints(e2eStartMS))

	start, end := e2eStartMS+10*60000, e2eStartMS+40*60000
	repeat := func(value float64, count int) []float64 {
		values := make([]float64, count)
		for index := range values {
			values[index] = value
		}
		return values
	}
	const p50 = 0.45000000000000007
	// The values of series 1 at its first ten steps; it has none after.
	early := []float64{p50, p50, p50, p50, p50, 0.4571428571428572, 0.41666666666666674, 0.375, 0.45999999999999996, 0.16666666666666669}
	last := []float64{0.48750000000000004}
	average := append(append(repeat(p50, 5), 0.4535714285714286, 0.4333333333333334, 0.41250000000000003, 0.455, 0.30833333333333335), append(repeat(p50, 20), last...)...)
	union := append(append(early, repeat(p50, 20)...), last...)
	for _, check := range []struct {
		query string
		want  []float64
		// compact marks a query the compact path answers; the others go
		// through the histogram pushdown.
		compact bool
	}{
		{`avg(histogram_quantile(0.5, rate(nan_delta_bucket[5m])))`, average, false},
		{`histogram_quantile(0.5, sum by (le) (rate(nan_delta_bucket{instance="1"}[5m]))) or histogram_quantile(0.5, sum by (le) (rate(nan_delta_bucket{instance="0"}[5m])))`, union, true},
	} {
		prepared, err := prepareMetricsQLQuery(check.query, time.Minute, cfg.LookbackDelta, time.UnixMilli(start), time.UnixMilli(end))
		if err != nil {
			test.Fatal(err)
		}
		server := newServer(cfg)
		if check.compact {
			if _, handled, err := server.tryCompactHistogramRange(ctx, prepared.query, time.UnixMilli(start), time.UnixMilli(end), time.Minute); !handled || err != nil {
				test.Fatalf("%s: compact path handled=%v error=%v", check.query, handled, err)
			}
		} else if _, _, pushed, err := server.planHistogramPushdown(ctx, prepared.query, rangeEvalTimes(time.UnixMilli(start), time.UnixMilli(end), time.Minute)); !pushed || err != nil {
			test.Fatalf("%s: pushdown=%v error=%v", check.query, pushed, err)
		}
		want := queryDataDTO{Result: []sampleResultDTO{{Metric: map[string]string{}}}}
		for index, value := range check.want {
			want.Result[0].Values = append(want.Result[0].Values, []any{float64(start/1000 + int64(index)*60), strconv.FormatFloat(value, 'g', -1, 64)})
		}
		// Both the fast paths and the engine over raw rows skip NaN.
		for _, fast := range []bool{true, false} {
			serverCfg := cfg
			serverCfg.PostHogCompactHistograms, serverCfg.PostHogHistogramPushdown = fast, fast
			mux := http.NewServeMux()
			newServer(serverCfg).routes(mux)
			api := httptest.NewServer(mux)
			got := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {check.query}, "start": {strconv.FormatInt(start/1000, 10)}, "end": {strconv.FormatInt(end/1000, 10)}, "step": {"60"}, "nocache": {"1"}})
			api.Close()
			assertQuantileResults(test, fmt.Sprintf("%s (fast paths %v)", check.query, fast), got, want)
		}
	}
}

func assertNaNSkippedValues(t *testing.T, name string, got, want map[string][]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: series %v, want %v", name, got, want)
	}
	for flag, values := range want {
		if len(got[flag]) != len(values) {
			t.Fatalf("%s: flag %q = %v, want %v", name, flag, got[flag], values)
		}
		for step, value := range values {
			gotValue := got[flag][step]
			if math.IsNaN(value) != math.IsNaN(gotValue) || math.Abs(gotValue-value) > 1e-12 {
				t.Fatalf("%s: flag %q = %v, want %v", name, flag, fmt.Sprint(got[flag]), fmt.Sprint(values))
			}
		}
	}
}
