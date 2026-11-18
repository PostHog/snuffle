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
	"strings"
	"testing"
	"time"
)

// A query that needs the series table must fail with the series limit error
// once it matches CH_MAX_SERIES series, instead of reading an arbitrary
// subset of them, and must keep answering exactly below the limit.
func TestPostHogSeriesLimitEndToEnd(test *testing.T) {
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
	database := fmt.Sprintf("snuffle_series_limit_e2e_%d", time.Now().UnixNano())
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

	// Five series of one job, told apart by region.
	const metric = "snuffle_limit_gauge"
	for region := 1; region <= 5; region++ {
		for index := range 3 {
			sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, value, count, has_labels, resource_attributes, attributes)
				SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'gauge', 1, 1, true, map('service.name', 'api'), map('region', %s)`,
				tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(metric), sqlString(fmt.Sprintf("r%d", region)), chTimeMillis(e2eStartMS+int64(index)*30000), sqlString(fmt.Sprintf("r%d", region)))
			if err := client.Exec(ctx, sql); err != nil {
				test.Fatal(err)
			}
		}
	}

	serve := func(maxSeries int) string {
		limited := cfg
		limited.MaxSeries = maxSeries
		mux := http.NewServeMux()
		newServer(limited).routes(mux)
		api := httptest.NewServer(mux)
		test.Cleanup(api.Close)
		return api.URL
	}
	get := func(test *testing.T, baseURL, path string, values url.Values) (int, apiResponseDTO[json.RawMessage]) {
		test.Helper()
		resp, err := http.Get(fmt.Sprintf("%s/t/%d%s?%s", baseURL, e2eTeamID, path, values.Encode()))
		if err != nil {
			test.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var decoded apiResponseDTO[json.RawMessage]
		if err := json.Unmarshal(body, &decoded); err != nil {
			test.Fatalf("decode %s: %v", body, err)
		}
		return resp.StatusCode, decoded
	}
	instant := func(query string) (string, url.Values) {
		return "/api/v1/query", url.Values{"query": {query}, "time": {"1700000070"}}
	}
	ranged := func(query string) (string, url.Values) {
		return "/api/v1/query_range", url.Values{"query": {query}, "start": {"1700000010"}, "end": {"1700000070"}, "step": {"30"}}
	}
	series := func(match string) (string, url.Values) {
		return "/api/v1/series", url.Values{"match[]": {match}, "start": {"1700000010"}, "end": {"1700000070"}}
	}

	for _, check := range []struct {
		name    string
		request func(string) (string, url.Values)
		query   string
		series  int // series the selector matches
		want    string
	}{
		{"grouped", instant, `count by (job) (snuffle_limit_gauge)`, 5, `[{"metric":{"job":"api"},"value":[1700000070,"5"]}]`},
		{"label filter", instant, `count(snuffle_limit_gauge{region=~"r.*"})`, 5, `[{"metric":{},"value":[1700000070,"5"]}]`},
		{"nested count", instant, `count(count by (region) (snuffle_limit_gauge))`, 5, `[{"metric":{},"value":[1700000070,"5"]}]`},
		{"topk", instant, `topk(2, snuffle_limit_gauge{region=~"r.*"})`, 5, "2 series"},
		{"union", instant, `sum by (job) (snuffle_limit_gauge{region="r1"} / 1 or snuffle_limit_gauge{region="r2"} / 1)`, 2, `[{"metric":{"job":"api"},"value":[1700000070,"2"]}]`},
		{"grouped range", ranged, `count by (job) (snuffle_limit_gauge)`, 5, `[{"metric":{"job":"api"},"values":[[1700000010,"5"],[1700000040,"5"],[1700000070,"5"]]}]`},
		{"filtered range", ranged, `sum(rate(snuffle_limit_gauge{region=~"r.*"}[1m]))`, 5, `[{"metric":{},"values":[[1700000040,"0"],[1700000070,"0"]]}]`},
		{"label matched in Go", series, `snuffle_limit_gauge{region=""}`, 5, `[]`},
	} {
		test.Run(check.name, func(test *testing.T) {
			path, values := check.request(check.query)
			status, body := get(test, serve(check.series+1), path, values)
			if status != http.StatusOK {
				test.Fatalf("below the limit: status %d: %s", status, body.Error)
			}
			if path != "/api/v1/series" {
				var data struct {
					Result json.RawMessage `json:"result"`
				}
				if err := json.Unmarshal(body.Data, &data); err != nil {
					test.Fatal(err)
				}
				body.Data = data.Result
			}
			got := string(body.Data)
			if count, ok := strings.CutSuffix(check.want, " series"); ok {
				var result []json.RawMessage
				if err := json.Unmarshal(body.Data, &result); err != nil {
					test.Fatal(err)
				}
				got = fmt.Sprintf("%d", len(result))
				check.want = count
			}
			if got != check.want {
				test.Fatalf("below the limit = %s, want %s", got, check.want)
			}

			status, body = get(test, serve(check.series), path, values)
			want := fmt.Sprintf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", check.series)
			if status != http.StatusUnprocessableEntity || body.Error != want {
				test.Fatalf("at the limit: status %d error %q, want 422 %q", status, body.Error, want)
			}
		})
	}
}
