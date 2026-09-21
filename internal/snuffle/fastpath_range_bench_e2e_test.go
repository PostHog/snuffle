package snuffle

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestRangePushdownLatency creates SNUFFLE_E2E_BENCH_SERIES counter series.
// The default is 2,000 series.
// It records one sample per minute for 24 hours.
// It times `sum(increase(m[1m])) by (code)` with a one-minute step.
// The test uses the pushdown and the Prometheus engine, then compares their results.
func TestRangePushdownLatency(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E_BENCH") != "1" {
		test.Skip("set SNUFFLE_E2E_BENCH=1 to run the ClickHouse range pushdown benchmark")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", getenv("SNUFFLE_E2E_BENCH_SCHEMA", "posthog"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_range_bench_%d", time.Now().UnixNano())
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
	cfg.TeamID = e2eTeamID
	client := NewClickHouseClient(cfg)
	schema := "create_metrics_schema.sql"
	if cfg.postHogSchemaLayout() {
		schema = "create_metrics_posthog_schema.sql"
	}
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", schema))

	const metric = "envoy_cluster_upstream_rq"
	const base = int64(1_700_000_000_000)
	const codes = 10
	series := envInt("SNUFFLE_E2E_BENCH_SERIES", 2000, codes)
	clusters := series / codes
	const minutes = 24*60 + 7
	columns := "team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, has_labels, resource_attributes, attributes"
	// Only the first and last samples of each series contain labels.
	// After merges, the series table contains one current row for each series.
	rows := fmt.Sprintf(`SELECT %d, '%s', cityHash64('bench', c.number, k.number), fromUnixTimestamp64Milli(%d + toInt64(m.number) * 60000, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'envoy', 'sum', 'cumulative', true,
		toFloat64(m.number) * toFloat64(1 + (c.number * 10 + k.number) %% 17) + toFloat64(k.number), 1, m.number IN (0, %d), map('region', 'eu'),
		map('envoy_cluster_name', concat('cluster-', toString(c.number)), 'envoy_response_code', toString(200 + k.number * 37 %% 400))
		FROM numbers(%d) AS c CROSS JOIN numbers(%d) AS k CROSS JOIN numbers(%d) AS m`,
		e2eTeamID, metric, base, minutes-1, clusters, codes, minutes)
	seeded := time.Now()
	if cfg.postHogSchemaLayout() {
		if err := client.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) %s", tableName(database, cfg.MetricsInputTable), columns, rows)); err != nil {
			test.Fatal(err)
		}
	} else {
		seriesSQL := fmt.Sprintf("INSERT INTO %s SELECT %d, cityHash64('bench', c.number, k.number), '%s', toJSONString(map('envoy_cluster_name', concat('cluster-', toString(c.number)), 'envoy_response_code', toString(200+k.number*37%%400))), fromUnixTimestamp64Milli(%d), fromUnixTimestamp64Milli(%d) FROM numbers(%d) AS c CROSS JOIN numbers(%d) AS k", tableName(database, cfg.SeriesTable), e2eTeamID, metric, base, base+int64(minutes-1)*60000, clusters, codes)
		if err := client.Exec(ctx, seriesSQL); err != nil {
			test.Fatal(err)
		}
		samplesSQL := fmt.Sprintf("INSERT INTO %s SELECT %d, '%s', fromUnixTimestamp64Milli(%d+toInt64(m.number)*60000), cityHash64('bench', c.number, k.number), toFloat64(m.number)*toFloat64(1+(c.number*10+k.number)%%17)+toFloat64(k.number) FROM numbers(%d) AS c CROSS JOIN numbers(%d) AS k CROSS JOIN numbers(%d) AS m", tableName(database, cfg.SamplesTable), e2eTeamID, metric, base, clusters, codes, minutes)
		if err := client.Exec(ctx, samplesSQL); err != nil {
			test.Fatal(err)
		}
	}
	test.Logf("seeded %d series x %d minutes in %s", clusters*codes, minutes, time.Since(seeded).Round(time.Millisecond))

	engineCfg := cfg
	engineCfg.RangePushdown = false
	engineCfg.QueryTimeout = 5 * time.Minute
	engineMux := http.NewServeMux()
	newServer(engineCfg).routes(engineMux)
	engine := httptest.NewServer(engineMux)
	defer engine.Close()
	pushdownMux := http.NewServeMux()
	newServer(cfg).routes(pushdownMux)
	pushdown := httptest.NewServer(pushdownMux)
	defer pushdown.Close()

	query := fmt.Sprintf("sum(increase(%s[1m])) by (envoy_response_code)", metric)
	start := base + 7*60_000
	end := base + int64(minutes-1)*60_000
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatFloat(float64(start)/1000, 'f', -1, 64)},
		"end":   {strconv.FormatFloat(float64(end)/1000, 'f', -1, 64)},
		"step":  {"1m"},
	}
	timed := func(name, baseURL string) queryDataDTO {
		var data queryDataDTO
		for i := 0; i < 3; i++ {
			started := time.Now()
			data = apiGet[queryDataDTO](test, baseURL, "/api/v1/query_range", params)
			test.Logf("%s run %d: %s (%d series)", name, i+1, time.Since(started).Round(time.Millisecond), len(data.Result))
		}
		return data
	}
	got := timed("pushdown", pushdown.URL)
	want := timed("engine", engine.URL)
	result := make([]sampleResult, 0, len(got.Result))
	for _, s := range got.Result {
		result = append(result, sampleResult{Metric: s.Metric, Values: s.Values})
	}
	assertSameMatrix(test, queryData{ResultType: got.ResultType, Result: result}, want)
}
