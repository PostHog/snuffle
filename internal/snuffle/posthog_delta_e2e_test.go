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
)

// TestPostHogDeltaSeriesEndToEnd checks ENG-030: a delta counter and a delta
// histogram, exported as one point per call, read back as sent, and increase
// and rate over them match the cumulative equivalent. The expected values come
// from VictoriaMetrics on the same samples: the delta series as sent for raw
// samples and sum_over_time, and the cumulative series (1, 2, ..., 40) for
// increase and rate, including for a range that starts with the series.
func TestPostHogDeltaSeriesEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_delta_e2e_%d", time.Now().UnixNano())
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

	// 40 calls, one every 15s from base+15s to base+10m. Each call exports a
	// counter point of 1 and a histogram point with count 1, sum 250 and one
	// observation in the (100, 1000] bucket.
	const base, calls = int64(1_700_000_000_000), 40
	insert := func(metric, columns, values string) {
		test.Helper()
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, aggregation_temporality, has_labels, resource_attributes, attributes, %s)
			SELECT %d, %s, cityHash64(%s), fromUnixTimestamp64Milli(%d + (number + 1) * 15000, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'delta', true, map('region', 'eu'), map('tool', 'search'), %s
			FROM numbers(%d)`,
			tableName(database, cfg.MetricsInputTable), columns, e2eTeamID, sqlString(metric), sqlString(metric), base, values, calls)
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	insert("eng030_calls_total", "metric_type, is_monotonic, value, count", "'sum', true, 1, 1")
	insert("eng030_duration_ms", "metric_type, value, count, histogram_bounds, histogram_counts", "'histogram', 250, 1, [100.0, 1000.0], [0, 1, 0]")

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	evalTime := strconv.FormatInt(base/1000+600, 10)
	query := func(test *testing.T, expression string) queryDataDTO {
		test.Helper()
		return apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {expression}, "time": {evalTime}})
	}
	assertClose := func(test *testing.T, expression, value string, want float64) {
		test.Helper()
		got, err := strconv.ParseFloat(value, 64)
		if err != nil || math.Abs(got-want) > 1e-9*math.Max(1, math.Abs(want)) {
			test.Fatalf("%s = %s, want %v", expression, value, want)
		}
	}

	for _, tc := range []struct {
		selector string
		perCall  float64
	}{
		{"eng030_calls_total", 1},
		{"eng030_duration_ms_count", 1},
		{"eng030_duration_ms_sum", 250},
		{`eng030_duration_ms_bucket{le="1000"}`, 1},
		{`eng030_duration_ms_bucket{le="+Inf"}`, 1},
	} {
		test.Run(tc.selector, func(test *testing.T) {
			raw := tc.selector + "[10m]"
			result := query(test, raw)
			if len(result.Result) != 1 || len(result.Result[0].Values) != calls {
				test.Fatalf("%s: %+v, want one series with %d points", raw, result.Result, calls)
			}
			for _, point := range result.Result[0].Values {
				assertClose(test, raw, sampleString(point), tc.perCall)
			}
			// 20 calls fall in (base+5m, base+10m]. increase_prometheus is the
			// window sum, as increase is. VictoriaMetrics gives 19 calls for the
			// cumulative equivalent: Prometheus' increase starts at the first
			// sample in the window, whose own increment is a call of a delta
			// series.
			for expression, want := range map[string]float64{
				"sum_over_time(" + tc.selector + "[5m])":       20 * tc.perCall,
				"increase(" + tc.selector + "[5m])":            20 * tc.perCall,
				"increase_prometheus(" + tc.selector + "[5m])": 20 * tc.perCall,
				"rate(" + tc.selector + "[5m])":                20 * tc.perCall / 300,
			} {
				result := query(test, expression)
				if len(result.Result) != 1 {
					test.Fatalf("%s returned %d series, want one", expression, len(result.Result))
				}
				assertClose(test, expression, sampleString(result.Result[0].Value), want)
				// Aggregated range queries, as dashboards send them, go through
				// the range pushdowns. Every 5m window ending in the last 5m
				// holds 20 calls.
				expression = "sum(" + expression + ")"
				ranged := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {expression}, "start": {strconv.FormatInt(base/1000+300, 10)}, "end": {evalTime}, "step": {"60"}})
				if len(ranged.Result) != 1 || len(ranged.Result[0].Values) != 6 {
					test.Fatalf("%s range: %+v, want six points", expression, ranged.Result)
				}
				for _, point := range ranged.Result[0].Values {
					assertClose(test, expression, sampleString(point), want)
				}
			}
			// The series starts inside the range: its first point is its own
			// increment, not a counter value (ENG-048). VictoriaMetrics' increase
			// over the cumulative equivalent counts 4 calls per minute from the
			// start up to the 20 of a full window. The step at base has no point.
			for _, expression := range []string{"increase(" + tc.selector + "[5m])", "sum(increase(" + tc.selector + "[5m]))"} {
				ranged := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {expression}, "start": {strconv.FormatInt(base/1000, 10)}, "end": {evalTime}, "step": {"60"}})
				if len(ranged.Result) != 1 || len(ranged.Result[0].Values) != 10 {
					test.Fatalf("%s from the series start: %+v, want ten points", expression, ranged.Result)
				}
				for index, point := range ranged.Result[0].Values {
					assertClose(test, expression, sampleString(point), float64(min(4*(index+1), 20))*tc.perCall)
				}
			}
		})
	}
}
