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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestPostHogHistogramPushdownMatchesEngine checks that the histogram pushdown
// returns what the engine returns from raw rows, for the query shapes the
// status page sends over a delta histogram.
func TestPostHogHistogramPushdownMatchesEngine(test *testing.T) {
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
	database := fmt.Sprintf("snuffle_histogram_pushdown_%d", time.Now().UnixNano())
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
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	const (
		metric     = "flags_request_duration"
		cumulative = "cumulative_request_duration"
		points     = 240
		interval   = int64(30_000)
	)
	base := e2eStartMS
	// insertSeries writes one source series: a point every 30s with
	// deterministic bucket counts. gap drops points, nanSum stores a NaN sum.
	insertSeries := func(name, temporality, fingerprint, timezone string, seed int, gap string, nanSum int) {
		test.Helper()
		where := "1"
		if gap != "" {
			where = "NOT (" + gap + ")"
		}
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
			SELECT %d, %s, cityHash64(%s), fromUnixTimestamp64Milli(toInt64(%d + number * %d)), now64(6), now64(6) + INTERVAL 1 DAY, 'posthog-app', 'histogram', %s,
				if(number = %d, nan, arraySum(counts) * 0.37) AS sum_value, arraySum(counts) AS observations, [100.0, 500.0, 3000.0], arrayMap(i -> toUInt64((number * 7 + i * 3 + %d) %% 5), range(4)) AS counts,
				true, map('region', 'us'), map('http.response.status_code', '200', 'client.timezone', %s)
			FROM numbers(%d) WHERE %s`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(name), sqlString(fingerprint), base, interval, sqlString(temporality),
			nanSum, seed, sqlString(timezone), points, where)
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	insertSeries(metric, "delta", "ny", "America/New_York", 1, "", 17)
	// A second source with the same labels: the engine sums their points.
	insertSeries(metric, "delta", "ny-copy", "America/New_York", 2, "number % 3 = 0", -1)
	// A gap from minute 40 to 55 leaves some windows without points.
	insertSeries(metric, "delta", "london", "Europe/London", 3, "number >= 80 AND number < 110", -1)
	insertSeries(cumulative, "cumulative", "cumulative", "Asia/Tokyo", 4, "", -1)

	serverFor := func(pushdown bool) *httptest.Server {
		serverCfg := cfg
		serverCfg.PostHogHistogramPushdown = pushdown
		// The reference is the engine over raw rows.
		serverCfg.PostHogCompactHistograms = pushdown
		mux := http.NewServeMux()
		newServer(serverCfg).routes(mux)
		return httptest.NewServer(mux)
	}
	pushdownAPI, engineAPI := serverFor(true), serverFor(false)
	defer pushdownAPI.Close()
	defer engineAPI.Close()

	seconds := func(ms int64) string { return strconv.FormatInt(ms/1000, 10) }
	end := base + (points-1)*interval
	bucket := `{"` + metric + `_bucket", "http.response.status_code"="200"}`
	bucket3000 := `{"` + metric + `_bucket", "http.response.status_code"="200", le="3000"}`
	byTimezone := `by (le, "client.timezone")`
	quantile := func(q, window string) string {
		return `histogram_quantile(` + q + `, sum ` + byTimezone + ` (increase(` + bucket + `[` + window + `]) and ignoring(le) increase(` + bucket3000 + `[` + window + `])))`
	}
	instantQueries := []string{
		quantile("0.5", "1h"),
		quantile("0.95", "90m"),
		quantile("0.99", "2h"),
		`sum by ("client.timezone") (increase(` + metric + `_sum[1h])) / sum by ("client.timezone") (increase(` + metric + `_count[1h]))`,
		`sum by ("client.timezone") (increase(` + metric + `_count[1h]))`,
		`rate(` + metric + `_bucket[10m])`,
		`increase(` + metric + `_count[90s])`,
		`delta(` + metric + `_sum[7m])`,
		`histogram_quantile(0.9, sum by (le) (rate(` + metric + `_bucket[45m])))`,
		`sum(increase(` + metric + `_count))`,
		`label_set(` + quantile("0.5", "1h") + `, "quantile", "p50")`,
		`label_copy(increase(` + metric + `_count[1h]), "__name__", "copied", "client.timezone", "tz")`,
		`label_move(increase(` + metric + `_count[1h]), "client.timezone", "tz")`,
		`label_transform(increase(` + metric + `_count[1h]), "client.timezone", "/", "-")`,
		`drop_common_labels(increase(` + metric + `_count[1h]), label_set(increase(` + metric + `_sum[1h]), "part", "sum"))`,
		`union(increase(` + metric + `_count[1h]), increase(` + metric + `_sum[1h]), increase(` + metric + `_count[2h]) * 0 + 7)`,
		`(label_set(` + quantile("0.5", "1h") + `, "q", "0.5"), label_set(` + quantile("0.99", "1h") + `, "q", "0.99"))`,
	}
	rangeQueries := []struct{ query, step string }{
		{`histogram_quantile(0.5, sum by (le) (rate(` + metric + `_bucket[5m])))`, "60"},
		{`histogram_quantile(0.95, sum by (le) (rate(` + metric + `_bucket[10m])))`, "420"},
		{`sum by ("client.timezone") (increase(` + metric + `_count[15m]))`, "300"},
		{`increase(` + metric + `_bucket{le="500"}[2m])`, "30"},
		{`sum(rate(` + metric + `_count))`, "60"},
		{`union(label_set(histogram_quantile(0.5, sum by (le) (rate(` + metric + `_bucket[5m]))), "q", "p50"), label_set(histogram_quantile(0.9, sum by (le) (rate(` + metric + `_bucket[5m]))), "q", "p90"))`, "60"},
		{`drop_common_labels(increase(` + metric + `_count[15m]))`, "300"},
	}

	test.Run("pushdown plans the status page queries", func(test *testing.T) {
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		server := newServer(teamCfg)
		for _, expression := range append(instantQueries, rangeQueries[0].query, rangeQueries[5].query, rangeQueries[6].query) {
			prepared, err := prepareMetricsQLQuery(expression, 5*time.Minute, teamCfg.LookbackDelta, time.UnixMilli(end), time.UnixMilli(end))
			if err != nil {
				test.Fatal(err)
			}
			_, _, pushed, err := server.planHistogramPushdown(ctx, prepared.query, []int64{end})
			if err != nil || !pushed {
				test.Fatalf("%s: pushed=%v error=%v (prepared %s)", expression, pushed, err, prepared.query)
			}
		}
		for _, expression := range []string{
			`sum(increase(` + cumulative + `_count[1h]))`,
			`sum by (__name__) (increase(` + metric + `_count[1h]))`,
			`label_replace(increase(` + metric + `_count[1h]), "x", "$1", "le", "(.*)")`,
		} {
			prepared, err := prepareMetricsQLQuery(expression, 5*time.Minute, teamCfg.LookbackDelta, time.UnixMilli(end), time.UnixMilli(end))
			if err != nil {
				test.Fatal(err)
			}
			if _, _, pushed, err := server.planHistogramPushdown(ctx, prepared.query, []int64{end}); err != nil || pushed {
				test.Fatalf("%s should keep the engine path: pushed=%v error=%v", expression, pushed, err)
			}
		}
	})

	test.Run("instant queries match the engine", func(test *testing.T) {
		for _, at := range []int64{end, end - 17*interval, base + 50*60_000} {
			for _, expression := range instantQueries {
				values := url.Values{"query": {expression}, "time": {seconds(at)}}
				got := apiGet[queryDataDTO](test, pushdownAPI.URL, "/api/v1/query", values)
				want := apiGet[queryDataDTO](test, engineAPI.URL, "/api/v1/query", values)
				assertPushdownResultsEqual(test, expression, got, want)
			}
		}
	})

	test.Run("range queries match the engine", func(test *testing.T) {
		for _, item := range rangeQueries {
			values := url.Values{"query": {item.query}, "start": {seconds(base + 20*60_000)}, "end": {seconds(end)}, "step": {item.step}}
			got := apiGet[queryDataDTO](test, pushdownAPI.URL, "/api/v1/query_range", values)
			want := apiGet[queryDataDTO](test, engineAPI.URL, "/api/v1/query_range", values)
			assertPushdownResultsEqual(test, item.query, got, want)
		}
	})

	test.Run("cumulative histograms keep the engine result", func(test *testing.T) {
		expression := `histogram_quantile(0.5, sum by (le) (increase(` + cumulative + `_bucket[1h])))`
		values := url.Values{"query": {expression}, "time": {seconds(end)}}
		assertPushdownResultsEqual(test, expression, apiGet[queryDataDTO](test, pushdownAPI.URL, "/api/v1/query", values), apiGet[queryDataDTO](test, engineAPI.URL, "/api/v1/query", values))
	})

	test.Run("the pushdown result does not count raw samples", func(test *testing.T) {
		limited := cfg
		limited.TeamID = e2eTeamID
		// The engine path reads 4 buckets x ~600 rows and fails at this limit.
		limited.MaxSamples = 500
		mux := http.NewServeMux()
		newServer(limited).routes(mux)
		// newServer sets the process-wide MetricsQL output limit.
		defer metricsQLMaxOutputSamples.Store(int64(cfg.MaxSamples))
		api := httptest.NewServer(mux)
		defer api.Close()
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {quantile("0.95", "2h")}, "time": {seconds(end)}})
		if len(result.Result) != 2 {
			test.Fatalf("want one quantile per timezone, got %+v", result)
		}
		limited.PostHogHistogramPushdown = false
		engineMux := http.NewServeMux()
		newServer(limited).routes(engineMux)
		engineOnly := httptest.NewServer(engineMux)
		defer engineOnly.Close()
		response, err := http.Get(engineOnly.URL + e2eAPIPath("/api/v1/query") + "?" + url.Values{"query": {quantile("0.95", "2h")}, "time": {seconds(end)}}.Encode())
		if err != nil {
			test.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnprocessableEntity {
			test.Fatalf("the engine path should hit the sample limit, got status %d", response.StatusCode)
		}
	})
}

// assertPushdownResultsEqual compares two API results series by series, with
// a relative tolerance for the float sums.
func assertPushdownResultsEqual(test *testing.T, expression string, got, want queryDataDTO) {
	test.Helper()
	key := func(metric map[string]string) string {
		parts := make([]string, 0, len(metric))
		for name, value := range metric {
			parts = append(parts, name+"="+value)
		}
		sort.Strings(parts)
		return strings.Join(parts, ",")
	}
	if got.ResultType != want.ResultType || len(got.Result) != len(want.Result) {
		test.Fatalf("%s: pushdown %+v\nengine %+v", expression, got, want)
	}
	if len(want.Result) == 0 {
		test.Fatalf("%s: the engine returned no series, so the comparison checks nothing", expression)
	}
	wantByKey := make(map[string]sampleResultDTO, len(want.Result))
	for _, series := range want.Result {
		wantByKey[key(series.Metric)] = series
	}
	equal := func(a, b []any) bool {
		if len(a) != 2 || len(b) != 2 || a[0] != b[0] {
			return false
		}
		x, errX := strconv.ParseFloat(sampleString(a), 64)
		y, errY := strconv.ParseFloat(sampleString(b), 64)
		if errX != nil || errY != nil {
			return sampleString(a) == sampleString(b)
		}
		return x == y || math.Abs(x-y) <= 1e-9*math.Max(math.Abs(x), math.Abs(y))
	}
	for _, series := range got.Result {
		expected, ok := wantByKey[key(series.Metric)]
		if !ok {
			test.Fatalf("%s: pushdown series %v is not in the engine result %+v", expression, series.Metric, want)
		}
		if series.Value != nil && !equal(series.Value, expected.Value) {
			test.Fatalf("%s %v: pushdown %v, engine %v", expression, series.Metric, series.Value, expected.Value)
		}
		if len(series.Values) != len(expected.Values) {
			test.Fatalf("%s %v: pushdown %d points, engine %d\npushdown %v\nengine %v", expression, series.Metric, len(series.Values), len(expected.Values), series.Values, expected.Values)
		}
		for index := range series.Values {
			if !equal(series.Values[index], expected.Values[index]) {
				test.Fatalf("%s %v point %d: pushdown %v, engine %v", expression, series.Metric, index, series.Values[index], expected.Values[index])
			}
		}
	}
}
