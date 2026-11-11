package snuffle

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Capture folds a classic histogram into one row with float bounds, so a
// bucket scraped as le="1.0" reads back as le="1". Matchers on le and
// quantile compare bounds, not spellings.
func TestPostHogBoundLabelMatchersEndToEnd(test *testing.T) {
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
	database := fmt.Sprintf("snuffle_bound_e2e_%d", time.Now().UnixNano())
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
	exec := func(sql string) {
		test.Helper()
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	for index := 1; index <= 3; index++ {
		timestamp := chTimeMillis(e2eStartMS + int64(index-1)*30000)
		// A folded classic histogram: virtual buckets le="0.5", le="1", le="+Inf".
		exec(fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
			SELECT %d, 'request_seconds', cityHash64('request_seconds'), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'histogram', 'cumulative', %d, %d, [0.5, 1.0], [%d, %d, %d], true, map(), map()`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, timestamp, index*12, index*10, index*2, index*3, index*5))
		// Plain series with stored bound labels.
		for _, attributes := range []string{`map('le', '1.0')`, `map('le', '2.5')`, `map('quantile', '1.0')`} {
			exec(fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, value, count, has_labels, attributes)
				SELECT %d, 'plain_bounds', cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'gauge', %d, 1, true, %s`,
				tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(attributes), timestamp, index, attributes))
		}
	}

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	for expression, want := range map[string]int{
		`request_seconds_bucket{le="1.0"}`:                                        1,
		`request_seconds_bucket{le="1"}`:                                          1,
		`request_seconds_bucket{le!="1.0"}`:                                       2,
		`request_seconds_bucket{le="+Inf"}`:                                       1,
		`plain_bounds{le="1"}`:                                                    1,
		`plain_bounds{le="1.0"}`:                                                  1,
		`plain_bounds{le="2.50"}`:                                                 1,
		`plain_bounds{le!="1"}`:                                                   2,
		`plain_bounds{quantile="1"}`:                                              1,
		`rate(request_seconds_bucket{le="1.0"}[1m])`:                              1,
		`histogram_quantile(0.5, sum by (le) (rate(request_seconds_bucket[1m])))`: 1,
	} {
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {expression}, "time": {"1700000070"}})
		if len(result.Result) != want {
			test.Errorf("%s returned %d series, want %d: %v", expression, len(result.Result), want, result.Result)
		}
	}
	rangeResult := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {`sum(rate(request_seconds_bucket{le="1.0"}[1m]))`}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"30"}})
	if len(rangeResult.Result) != 1 {
		test.Errorf("range query returned %d series, want one", len(rangeResult.Result))
	}
	series := apiGet[[]map[string]string](test, api.URL, "/api/v1/series", url.Values{"match[]": {`request_seconds_bucket{le="1.0"}`}, "start": {"1700000010"}, "end": {"1700000070"}})
	if len(series) != 1 || series[0]["le"] != "1" {
		test.Errorf("series = %v, want the le=\"1\" bucket", series)
	}
}
