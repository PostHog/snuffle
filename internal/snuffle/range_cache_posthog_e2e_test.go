package snuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestRangeCacheServesPostHogDashboardRefreshes refreshes PostHog-layout
// dashboard queries with the window moved forward by one minute, as the
// PostHog proxy sends them: the tenant in X-Team-ID, the ClickHouse user in
// basic auth and the client's start, end and step unchanged. The server uses
// the default configuration, so the cache must be on without any setting.
// SNUFFLE_E2E_RANGE_CACHE_SERIES sets the series per metric (default 20).
func TestRangeCacheServesPostHogDashboardRefreshes(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	test.Setenv("SNUFFLE_RANGE_CACHE_MAX_BYTES", "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_range_cache_e2e_%d", time.Now().UnixNano())
	if err := root.Exec(ctx, "CREATE DATABASE "+quoteIdent(database)); err != nil {
		test.Fatal(err)
	}
	defer func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		if err := root.Exec(cleanup, "DROP DATABASE "+quoteIdent(database)+" SYNC"); err != nil {
			test.Errorf("clean test database: %v", err)
		}
	}()
	cfg.CHDatabase = database
	cfg.QueryTimeout = 5 * time.Minute
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	const base = int64(1_700_000_000_000)
	const minutes = 7*24*60 + 30
	series := envInt("SNUFFLE_E2E_RANGE_CACHE_SERIES", 20, 1)
	columns := "team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes"
	// One sample per minute for each series. The histogram buckets grow by
	// different amounts each minute, so the quantiles move over the window.
	seed := func(metric, metricType, temporality string, fromMinute int, value, counts string) {
		sql := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %d, '%s', cityHash64('%s', s.number), fromUnixTimestamp64Milli(%d + toInt64(m.number) * 60000, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'envoy', '%s', '%s', true,
			%s, if(empty(%s), 1, arraySum(%s)), [0.1, 0.5, 1.0], %s, true, map('region', 'eu'), map('envoy_response_code', toString(200 + s.number))
			FROM numbers(%d) AS s CROSS JOIN numbers(%d, %d) AS m`,
			tableName(database, cfg.MetricsInputTable), columns, e2eTeamID, metric, metric, base, metricType, temporality, value, counts, counts, counts, series, fromMinute, minutes-fromMinute)
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	const histogramFrom = minutes - 24*60 - 40
	seed("envoy_cluster_upstream_rq", "sum", "cumulative", 0, "toFloat64(m.number * (1 + s.number % 7))", "[]")
	seed("http_server_duration_seconds", "histogram", "cumulative", histogramFrom, "toFloat64(m.number)",
		"[m.number * 5, m.number * (2 + s.number % 3), m.number * 3 + m.number % 4, m.number]")
	seed("http_client_duration_seconds", "histogram", "delta", histogramFrom, "1.0",
		"[5, 2 + s.number % 3, 1 + m.number % 4, 1]")

	uncachedCfg := cfg
	uncachedCfg.RangeCacheMaxBytes = 0
	uncachedMux := http.NewServeMux()
	newServer(uncachedCfg).routes(uncachedMux)
	uncached := httptest.NewServer(uncachedMux)
	defer uncached.Close()
	cachedServer := newServer(cfg)
	if cachedServer.rangeCache == nil {
		test.Fatal("the range result cache is off in the default configuration")
	}
	var clock time.Time
	cachedServer.rangeCache.now = func() time.Time { return clock }
	cachedMux := http.NewServeMux()
	cachedServer.routes(cachedMux)
	cached := httptest.NewServer(cachedMux)
	defer cached.Close()

	get := func(test *testing.T, baseURL string, values url.Values) (queryDataDTO, string, time.Duration) {
		test.Helper()
		req, err := http.NewRequest(http.MethodGet, baseURL+"/api/v1/query_range?"+values.Encode(), nil)
		if err != nil {
			test.Fatal(err)
		}
		req.Header.Set("X-Team-ID", strconv.FormatUint(e2eTeamID, 10))
		req.SetBasicAuth(cfg.CHUser, cfg.CHPassword)
		started := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			test.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		elapsed := time.Since(started)
		if err != nil || resp.StatusCode != http.StatusOK {
			test.Fatalf("%s: HTTP %d: %s (%v)", values.Get("query"), resp.StatusCode, body, err)
		}
		var decoded apiResponseDTO[queryDataDTO]
		if err := json.Unmarshal(body, &decoded); err != nil {
			test.Fatal(err)
		}
		return decoded.Data, resp.Header.Get(rangeCacheHeader), elapsed
	}

	end := base + int64(minutes-1)*60_000
	for _, tc := range []struct {
		name, query string
		window      time.Duration
		step        time.Duration
	}{
		{"24h cumulative histogram", "histogram_quantile(0.99, sum by (le) (rate(http_server_duration_seconds_bucket[5m])))", 24 * time.Hour, time.Minute},
		{"24h delta histogram", "histogram_quantile(0.99, sum by (le) (rate(http_client_duration_seconds_bucket[5m])))", 24 * time.Hour, time.Minute},
		{"7d rate", "sum by (envoy_response_code) (rate(envoy_cluster_upstream_rq[5m]))", 7 * 24 * time.Hour, 5 * time.Minute},
	} {
		test.Run(tc.name, func(test *testing.T) {
			step := tc.step.Milliseconds()
			for index, refreshEnd := range []int64{end - 60_000, end} {
				// Grafana aligns start to the step and the proxy forwards it unchanged.
				start := refreshEnd - tc.window.Milliseconds()
				start -= start % step
				values := url.Values{
					"query": {tc.query},
					"start": {strconv.FormatFloat(float64(start)/1000, 'f', -1, 64)},
					"end":   {strconv.FormatFloat(float64(refreshEnd)/1000, 'f', -1, 64)},
					"step":  {tc.step.String()},
				}
				// A live dashboard sends its request just after its last step.
				clock = time.UnixMilli(refreshEnd).Add(30 * time.Second)
				want, _, uncachedTime := get(test, uncached.URL, values)
				got, status, cachedTime := get(test, cached.URL, values)
				wantStatus := rangeCacheMiss
				if index > 0 {
					wantStatus = rangeCachePartial
				}
				test.Logf("window ending %d: %s=%s in %s, uncached %s (%d series)", refreshEnd, rangeCacheHeader, status, cachedTime.Round(time.Millisecond), uncachedTime.Round(time.Millisecond), len(want.Result))
				if status != wantStatus {
					test.Fatalf("window ending %d: cache status %q, want %q", refreshEnd, status, wantStatus)
				}
				if len(want.Result) == 0 {
					test.Fatal("the uncached server returned no series")
				}
				assertSameRangeResult(test, tc.name, got, want)
			}
		})
	}
}
