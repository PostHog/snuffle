package snuffle

import (
	"context"
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

// TestPostHogHistogramPushdownWeekScale reproduces a week of a delta histogram
// with 24 buckets: about 3M rows and 72M bucket values, more than the default
// 50M sample limit. The engine path fails on the limit; the pushdown answers.
// It is opt-in because the insert takes a while.
func TestPostHogHistogramPushdownWeekScale(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" || os.Getenv("SNUFFLE_E2E_SCALE") != "1" {
		test.Skip("set SNUFFLE_E2E=1 and SNUFFLE_E2E_SCALE=1 to run the week-scale histogram test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.AllowUnauthenticated = true
	cfg.RemoteWriteInterval = 0
	cfg.QueryTimeout = 2 * time.Minute
	cfg.CHTimeout = 2 * time.Minute
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_histogram_scale_%d", time.Now().UnixNano())
	if err := root.Exec(ctx, "CREATE DATABASE "+quoteIdent(database)); err != nil {
		test.Fatal(err)
	}
	defer func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := root.Exec(cleanup, "DROP DATABASE "+quoteIdent(database)+" SYNC"); err != nil {
			test.Errorf("clean test database: %v", err)
		}
	}()
	cfg.CHDatabase = database
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	const (
		metric   = "flags_request_duration"
		series   = 300
		points   = 7 * 24 * 60 // one point per minute for a week
		interval = int64(60_000)
	)
	base := e2eStartMS
	started := time.Now()
	for source := 0; source < series; source++ {
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
			SELECT %d, %s, cityHash64(%s), fromUnixTimestamp64Milli(toInt64(%d + number * %d)), now64(6), now64(6) + INTERVAL 1 DAY, 'posthog-app', 'histogram', 'delta',
				arraySum(counts) * 0.37, arraySum(counts), arrayMap(i -> toFloat64(5 * pow(1.6, i)), range(23)), arrayMap(i -> toUInt64((number * 7 + i * 3 + %d) %% 5), range(24)) AS counts,
				true, map('region', 'us'), map('http.response.status_code', '200', 'client.timezone', %s)
			FROM numbers(%d)`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(metric), sqlString(strconv.Itoa(source)), base, interval, source, sqlString(fmt.Sprintf("tz-%d", source%40)), points)
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	test.Logf("inserted %d rows (%d bucket values) in %s", series*points, series*points*24, time.Since(started).Round(time.Second))

	end := base + (points-1)*interval
	query := `histogram_quantile(0.95, sum by (le, "client.timezone") (increase({"` + metric + `_bucket", "http.response.status_code"="200"}[7d]) and ignoring(le) increase({"` + metric + `_bucket", "http.response.status_code"="200", le="3000"}[7d])))`
	run := func(pushdown bool) (int, string, time.Duration) {
		serverCfg := cfg
		serverCfg.TeamID = e2eTeamID
		serverCfg.PostHogHistogramPushdown = pushdown
		mux := http.NewServeMux()
		newServer(serverCfg).routes(mux)
		api := httptest.NewServer(mux)
		defer api.Close()
		started := time.Now()
		response, err := http.Get(api.URL + e2eAPIPath("/api/v1/query") + "?" + url.Values{"query": {query}, "time": {strconv.FormatInt(end/1000, 10)}}.Encode())
		if err != nil {
			test.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body), time.Since(started)
	}
	status, body, took := run(true)
	test.Logf("pushdown: status %d in %s", status, took.Round(time.Millisecond))
	if status != http.StatusOK {
		test.Fatalf("pushdown failed: %s", body)
	}
	status, body, took = run(false)
	test.Logf("engine: status %d in %s: %.200s", status, took.Round(time.Millisecond), body)
	if status == http.StatusOK {
		test.Fatal("the engine path should exceed the sample limit at this scale")
	}
}
