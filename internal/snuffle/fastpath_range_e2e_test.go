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
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	promvalue "github.com/prometheus/prometheus/model/value"
)

// TestRangePushdownMatchesEngine compares ClickHouse pushdown and Prometheus engine results.
// The data includes full and partial resets, gaps longer than the lookback, irregular intervals, stale markers, and late starts.
// Both paths must return the same series and values.
func TestRangePushdownMatchesEngine(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_range_pushdown_e2e_%d", time.Now().UnixNano())
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
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", "create_metrics_posthog_schema.sql"))

	const metric = "test_requests_total"
	const base = int64(1_700_000_000_000)
	stale := math.Float64frombits(promvalue.StaleNaN)
	type point struct {
		offset time.Duration
		value  float64
	}
	insertSeries := func(attrs string, points []point) {
		test.Helper()
		tuples := make([]string, 0, len(points))
		for _, p := range points {
			value := strconv.FormatFloat(p.value, 'f', -1, 64)
			if math.IsNaN(p.value) {
				value = fmt.Sprintf("reinterpretAsFloat64(toUInt64(%d))", uint64(promvalue.StaleNaN))
			}
			tuples = append(tuples, fmt.Sprintf("(%d, %s)", base+p.offset.Milliseconds(), value))
		}
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, has_labels, resource_attributes, attributes)
			SELECT %d, %s, cityHash64(%s), fromUnixTimestamp64Milli(p.1, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'sum', 'cumulative', true, toFloat64(p.2), 1, true, map('region', 'eu'), %s
			FROM (SELECT arrayJoin([%s]) AS p)`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(metric), sqlString(metric+attrs), attrs, strings.Join(tuples, ", "))
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	regular := func(interval time.Duration, from, to time.Duration, start, increment float64) []point {
		points := make([]point, 0, 64)
		value := start
		for offset := from; offset <= to; offset += interval {
			points = append(points, point{offset: offset, value: value})
			value += increment
		}
		return points
	}
	// This steady counter has one sample per minute.
	insertSeries("map('code', '200', 'cluster', 'a')", regular(time.Minute, 0, 40*time.Minute, 100, 7))
	// This counter has a full reset at 20 minutes and a partial reset at 30 minutes.
	// The partial reset drops by less than one eighth of the previous value.
	reset := regular(time.Minute, 0, 40*time.Minute, 1000, 50)
	for i := range reset {
		switch {
		case reset[i].offset == 20*time.Minute:
			reset[i].value = 3
		case reset[i].offset > 20*time.Minute && reset[i].offset < 30*time.Minute:
			reset[i].value = 3 + float64(reset[i].offset-20*time.Minute)/float64(time.Minute)*50
		case reset[i].offset == 30*time.Minute:
			reset[i].value = 480
		case reset[i].offset > 30*time.Minute:
			reset[i].value = 480 + float64(reset[i].offset-30*time.Minute)/float64(time.Minute)*50
		}
	}
	insertSeries("map('code', '200', 'cluster', 'b')", reset)
	// These gaps exceed all tested matrices.
	// A large value follows the first gap, and a small new counter follows later.
	gap := append(regular(time.Minute, 0, 6*time.Minute, 5000, 20), regular(time.Minute, 18*time.Minute, 24*time.Minute, 9000, 20)...)
	gap = append(gap, regular(time.Minute, 36*time.Minute, 40*time.Minute, 2, 1)...)
	insertSeries("map('code', '500', 'cluster', 'a')", gap)
	// This steady series has a stale marker in its middle.
	marked := regular(time.Minute, 0, 40*time.Minute, 10, 3)
	marked = append(marked, point{offset: 21*time.Minute + 15*time.Second, value: stale})
	insertSeries("map('code', '500', 'cluster', 'b')", marked)
	// This dense series starts in the middle of the range.
	insertSeries("map('code', '404', 'cluster', 'a')", regular(15*time.Second, 17*time.Minute+5*time.Second, 40*time.Minute, 0, 2))
	// This separate metric uses irregular sample intervals.
	// The engine estimates the interval for each step.
	// The pushdown estimates one interval for each series.
	// Only increase and delta must match because they do not use this estimate.
	const irregularMetric = "test_irregular_total"
	irregular := make([]point, 0, 32)
	for offset, value := time.Duration(0), 10.0; offset <= 40*time.Minute; value += 3 {
		irregular = append(irregular, point{offset: offset, value: value})
		if len(irregular)%2 == 0 {
			offset += 90 * time.Second
		} else {
			offset += 30 * time.Second
		}
	}
	insertIrregular := func(attrs string, points []point) {
		test.Helper()
		tuples := make([]string, 0, len(points))
		for _, p := range points {
			tuples = append(tuples, fmt.Sprintf("(%d, %s)", base+p.offset.Milliseconds(), strconv.FormatFloat(p.value, 'f', -1, 64)))
		}
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, has_labels, resource_attributes, attributes)
			SELECT %d, %s, cityHash64(%s), fromUnixTimestamp64Milli(p.1, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'sum', 'cumulative', true, toFloat64(p.2), 1, true, map('region', 'eu'), %s
			FROM (SELECT arrayJoin([%s]) AS p)`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(irregularMetric), sqlString(irregularMetric+attrs), attrs, strings.Join(tuples, ", "))
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	insertIrregular("map('code', '200', 'cluster', 'a')", irregular)

	engineCfg := cfg
	engineCfg.RangePushdown = false
	mux := http.NewServeMux()
	newServer(engineCfg).routes(mux)
	engine := httptest.NewServer(mux)
	defer engine.Close()
	pushdown := newServer(cfg)

	type rangeCase struct {
		query string
		step  time.Duration
		// engine is true when the Prometheus engine must evaluate the query.
		engine bool
	}
	// Bare selectors read the last sample within max(step, interval margin).
	// After a long gap, the engine estimates the interval from one sample.
	// It then uses the step.
	// These cases use steps above the one-minute margin.
	cases := []rangeCase{
		{"sum(M) by (code)", 2 * time.Minute, false},
		{"avg(M)", 90 * time.Second, false},
		{"count(M) by (cluster)", 3 * time.Minute, false},
	}
	for _, fn := range []string{"increase", "delta"} {
		cases = append(cases,
			rangeCase{fmt.Sprintf("sum(%s(I[1m])) by (code)", fn), time.Minute, false},
			rangeCase{fmt.Sprintf("max(%s(I[2m]))", fn), 30 * time.Second, false},
		)
	}
	for _, fn := range []string{"increase", "delta", "rate", "irate", "idelta"} {
		cases = append(cases,
			rangeCase{fmt.Sprintf("sum(%s(M[1m])) by (code)", fn), time.Minute, false},
			rangeCase{fmt.Sprintf("sum(%s(M[5m])) by (code)", fn), time.Minute, false},
			rangeCase{fmt.Sprintf("max(%s(M[30s])) by (cluster)", fn), time.Minute, false},
			rangeCase{fmt.Sprintf("min(%s(M[2m])) by (code, cluster)", fn), 30 * time.Second, false},
			// The engine calculates a missing rate window from each step's sample interval.
			rangeCase{fmt.Sprintf("count(%s(M))", fn), time.Minute, fn == "rate"},
			rangeCase{fmt.Sprintf("sum(%s(M{code=\"500\"}[1m]))", fn), 45 * time.Second, false},
		)
	}
	start := time.UnixMilli(base + 5*time.Minute.Milliseconds())
	end := time.UnixMilli(base + 40*time.Minute.Milliseconds())
	for _, tc := range cases {
		query := strings.ReplaceAll(strings.ReplaceAll(tc.query, "M", metric), "I", irregularMetric)
		test.Run(fmt.Sprintf("%s step %s", tc.query, tc.step), func(test *testing.T) {
			want := apiGet[queryDataDTO](test, engine.URL, "/api/v1/query_range", url.Values{
				"query": {query},
				"start": {strconv.FormatFloat(float64(start.UnixMilli())/1000, 'f', -1, 64)},
				"end":   {strconv.FormatFloat(float64(end.UnixMilli())/1000, 'f', -1, 64)},
				"step":  {tc.step.String()},
			})
			prepared, err := prepareMetricsQLQuery(query, tc.step, cfg.LookbackDelta, start, end)
			if err != nil {
				test.Fatal(err)
			}
			got, ok, err := pushdown.tryFastRangeQuery(ctx, prepared.query, start, end, tc.step)
			if err != nil {
				test.Fatal(err)
			}
			if ok == tc.engine {
				test.Fatalf("%s: range pushdown used = %v, want %v", query, ok, !tc.engine)
			}
			if ok {
				assertSameMatrix(test, got, want)
			}
		})
	}
}

func assertSameMatrix(test *testing.T, got queryData, want queryDataDTO) {
	test.Helper()
	series, ok := got.Result.([]sampleResult)
	if !ok || got.ResultType != "matrix" {
		test.Fatalf("pushdown result = %#v", got)
	}
	key := func(metric map[string]string) string {
		parts := make([]string, 0, len(metric))
		for name, value := range metric {
			parts = append(parts, name+"="+value)
		}
		slices.Sort(parts)
		return strings.Join(parts, ",")
	}
	wantByKey := make(map[string][][]any, len(want.Result))
	for _, s := range want.Result {
		wantByKey[key(s.Metric)] = s.Values
	}
	if len(series) != len(want.Result) {
		test.Fatalf("pushdown returned %d series, engine %d: %v vs %v", len(series), len(want.Result), seriesKeys(series, key), want.Result)
	}
	for _, s := range series {
		wantValues, ok := wantByKey[key(s.Metric)]
		if !ok {
			test.Fatalf("pushdown series %v missing from engine result", s.Metric)
		}
		if len(s.Values) != len(wantValues) {
			test.Fatalf("series %v: pushdown has %d points, engine %d\npushdown: %v\nengine:   %v", s.Metric, len(s.Values), len(wantValues), s.Values, wantValues)
		}
		for i := range s.Values {
			gotTS, wantTS := s.Values[i][0].(float64), wantValues[i][0].(float64)
			if gotTS != wantTS {
				test.Fatalf("series %v point %d: timestamp %v, engine %v", s.Metric, i, gotTS, wantTS)
			}
			gotValue, _ := strconv.ParseFloat(sampleString(s.Values[i]), 64)
			wantValue, _ := strconv.ParseFloat(sampleString(wantValues[i]), 64)
			if math.Abs(gotValue-wantValue) > 1e-9*math.Max(1, math.Abs(wantValue)) {
				test.Fatalf("series %v at %v: pushdown %v, engine %v\npushdown: %v\nengine:   %v", s.Metric, gotTS, gotValue, wantValue, s.Values, wantValues)
			}
		}
	}
}

func seriesKeys(series []sampleResult, key func(map[string]string) string) []string {
	keys := make([]string, 0, len(series))
	for _, s := range series {
		keys = append(keys, key(s.Metric))
	}
	return keys
}
