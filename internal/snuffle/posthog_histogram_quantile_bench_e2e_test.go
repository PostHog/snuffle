package snuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// TestHistogramQuantileRangeLatency times
// histogram_quantile(0.95, sum by (le) (rate(x_bucket[5m]))) over large
// PostHog histograms (ENG-010, ENG-031):
//
//   - cumulative: SNUFFLE_E2E_BENCH_SERIES classic histograms (default 2,000)
//     of 15 buckets scraped every 15 s for 24 h, queried over 6 h and 24 h at
//     a 60 s and a 5 m step;
//   - delta: one histogram point per event, SNUFFLE_E2E_BENCH_DELTA_PER_DAY
//     points a day (default 1.9M) over 7 d across
//     SNUFFLE_E2E_BENCH_DELTA_SERIES series (default 2,000), queried over 7 d
//     at a 5 m step.
//
// It logs the latency, the status and a checksum of each result, so runs on
// two commits can be compared.
func TestHistogramQuantileRangeLatency(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E_BENCH") != "1" {
		test.Skip("set SNUFFLE_E2E_BENCH=1 to run the histogram quantile benchmark")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.AllowUnauthenticated = true
	cfg.RemoteWriteInterval = 0
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_histogram_quantile_bench_%d", time.Now().UnixNano())
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
	cfg.TeamID = e2eTeamID
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	const (
		base     = int64(1_700_000_000_000)
		day      = int64(24 * 3600 * 1000)
		scrape   = int64(15_000)
		cumName  = "django_http_requests_latency_seconds_by_view_method"
		deltaNam = "llm_call_duration_seconds"
	)
	columns := "team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes"
	bounds := "[0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 25]"
	series := envInt("SNUFFLE_E2E_BENCH_SERIES", 2000, 1)
	points := (day + 10*60_000) / scrape
	seeded := time.Now()
	// The low buckets grow fastest, the high ones speed up over the day, and
	// every 50th series restarts once.
	cumulative := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %d, '%s', cityHash64('cum', s), fromUnixTimestamp64Milli(%d + toInt64(m) * %d, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'django', 'histogram', 'cumulative',
		arraySum(counts) * 0.1, arraySum(counts), %s, arrayMap(i -> toUInt64(c * (16 - i) * (1 + (s + i) %% 3) + intDiv(c * c, 4000) * (i %% 4)), range(15)) AS counts, m %% 240 = 0, map('region', 'us'),
		map('view', concat('view-', toString(s %% 150)), 'method', ['GET', 'POST'][1 + s %% 2], 'instance', toString(s))
		FROM (SELECT intDiv(number, %d) AS s, number %% %d AS m, if(s %% 50 = 0, m %% 3000, m) AS c FROM numbers(%d))`,
		tableName(database, cfg.MetricsInputTable), columns, e2eTeamID, cumName, base, scrape, bounds, points, points, int64(series)*points)
	if err := client.Exec(ctx, cumulative); err != nil {
		test.Fatal(err)
	}
	test.Logf("seeded %d cumulative series x %d points in %s", series, points, time.Since(seeded).Round(time.Millisecond))

	perDay := int64(envInt("SNUFFLE_E2E_BENCH_DELTA_PER_DAY", 1_900_000, 1))
	deltaSeries := int64(envInt("SNUFFLE_E2E_BENCH_DELTA_SERIES", 2000, 1))
	deltaPoints := 7 * perDay
	seeded = time.Now()
	// One point per event: a single observation in one bucket, mostly a low
	// one.
	delta := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %d, '%s', cityHash64('delta', number %% %d), fromUnixTimestamp64Milli(%d + intDiv(toInt64(number) * %d, %d), 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'llm-gateway', 'histogram', 'delta',
		0.3, 1, %s, arrayMap(i -> toUInt64(i = 14 - toUInt8(floor(sqrt(cityHash64(number) %% 225)))), range(15)), intDiv(number, %d) %% 1000 = 0, map('region', 'us'), map('model', concat('model-', toString(number %% %d)))
		FROM numbers(%d)`,
		tableName(database, cfg.MetricsInputTable), columns, e2eTeamID, deltaNam, deltaSeries, base, 7*day, deltaPoints, bounds, deltaSeries, deltaSeries, deltaPoints)
	if err := client.Exec(ctx, delta); err != nil {
		test.Fatal(err)
	}
	test.Logf("seeded %d delta points in %s", deltaPoints, time.Since(seeded).Round(time.Millisecond))

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	run := func(name, metric string, rangeMillis int64, step string) {
		end := base + day
		if metric == deltaNam {
			end = base + 7*day
		}
		test.Logf("%s: %s", name, timeRangeQuery(test, api.URL, `histogram_quantile(0.95, sum by (le) (rate(`+metric+`_bucket[5m])))`, end-rangeMillis, end, step))
	}
	hour := int64(3600 * 1000)
	run("cumulative 6h step 60s", cumName, 6*hour, "60")
	run("cumulative 6h step 5m", cumName, 6*hour, "300")
	run("cumulative 24h step 60s", cumName, 24*hour, "60")
	run("cumulative 24h step 5m", cumName, 24*hour, "300")
	run("delta 7d step 5m", deltaNam, 7*day, "300")
}

// timeRangeQuery runs a range query and describes its latency, status and a
// checksum of the result, so runs on two commits can be compared.
func timeRangeQuery(test *testing.T, apiURL, query string, start, end int64, step string) string {
	test.Helper()
	values := url.Values{"query": {query}, "start": {strconv.FormatInt(start/1000, 10)}, "end": {strconv.FormatInt(end/1000, 10)}, "step": {step}, "nocache": {"1"}}
	started := time.Now()
	response, err := http.Get(apiURL + e2eAPIPath("/api/v1/query_range") + "?" + values.Encode())
	if err != nil {
		test.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	took := time.Since(started).Round(time.Millisecond)
	if response.StatusCode != http.StatusOK {
		return fmt.Sprintf("status %d in %s: %.300s", response.StatusCode, took, body)
	}
	var decoded struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		test.Fatal(err)
	}
	count, sum := 0, 0.0
	for _, result := range decoded.Data.Result {
		for _, point := range result.Values {
			value, _ := strconv.ParseFloat(point[1].(string), 64)
			if !math.IsNaN(value) {
				count++
				sum += value
			}
		}
	}
	return fmt.Sprintf("status 200 in %s, %d series, %d points, sum %.9g", took, len(decoded.Data.Result), count, sum)
}

// TestHistogramBucketSeriesRangeLatency times the ENG-010 sweep shapes over
// about 290k bucket series in one namespace (14,500 sources x 20 buckets,
// SNUFFLE_E2E_BENCH_SOURCES): sum by (le) (rate(x_bucket[4m])), its p99 and
// count(x_bucket), over 1 h at a 60 s step with a 30 s scrape. It seeds both
// forms of a classic histogram: PostHog histogram rows and plain _bucket
// series with an le label, as remote write stores them.
func TestHistogramBucketSeriesRangeLatency(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E_BENCH") != "1" {
		test.Skip("set SNUFFLE_E2E_BENCH=1 to run the bucket series benchmark")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	test.Setenv("SNUFFLE_RANGE_CACHE_MAX_BYTES", "0")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.AllowUnauthenticated = true
	cfg.RemoteWriteInterval = 0
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_bucket_series_bench_%d", time.Now().UnixNano())
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
	cfg.TeamID = e2eTeamID
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	const (
		base   = int64(1_700_000_000_000)
		scrape = int64(30_000)
		points = 140 // 70 minutes
	)
	sources := int64(envInt("SNUFFLE_E2E_BENCH_SOURCES", 14500, 1))
	columns := "team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes"
	bounds := "[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000]"
	// Bucket i of source s grows by (s+i)%7 observations each scrape; every
	// 40th source restarts at minute 35. Sources in another namespace share
	// the metric.
	counts := "arrayMap(i -> toUInt64(c * ((s + i) % 7)), range(20))"
	labels := "map('namespace', if(s % 10 = 9, 'ingestion-other', 'ingestion-analytics-main'), 'pod', concat('pod-', toString(s)))"
	from := fmt.Sprintf("FROM (SELECT intDiv(number, %[1]d) AS s, number %% %[1]d AS m, if(s %% 40 = 0, m %% 70, m) AS c FROM numbers(%[2]d))", points, sources*10/9*points)
	seeded := time.Now()
	histograms := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %d, 'ingestion_lag_ms_histogram', cityHash64('hist', s), fromUnixTimestamp64Milli(%d + toInt64(m) * %d, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'plugin-server', 'histogram', 'cumulative', false,
		arraySum(counts) * 3.0, arraySum(counts), %s, %s AS counts, m %% 20 = 0, map('region', 'us'), %s %s`,
		tableName(database, cfg.MetricsInputTable), columns, e2eTeamID, base, scrape, bounds, counts, labels, from)
	if err := client.Exec(ctx, histograms); err != nil {
		test.Fatal(err)
	}
	// The same buckets as plain cumulative series: one per source and le.
	plain := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %d, 'ingestion_lag_ms_plain_bucket', cityHash64('plain', s, i), fromUnixTimestamp64Milli(%d + toInt64(m) * %d, 'UTC'), now64(6), now64(6) + INTERVAL 1 DAY, 'plugin-server', 'sum', 'cumulative', true,
		toFloat64(arraySum(arraySlice(%s, 1, i + 1))), 1, [], [], m %% 20 = 0, map('region', 'us'), mapUpdate(%s, map('le', if(i = 19, '+Inf', toString(%s[i + 1])))) %s ARRAY JOIN range(20) AS i`,
		tableName(database, cfg.MetricsInputTable), columns, e2eTeamID, base, scrape, counts, labels, bounds, from)
	if err := client.Exec(ctx, plain); err != nil {
		test.Fatal(err)
	}
	test.Logf("seeded %d sources x 20 buckets x %d points in each form in %s", sources*10/9, points, time.Since(seeded).Round(time.Second))

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	start, end := base+10*60000, base+70*60000
	for _, metric := range []string{"ingestion_lag_ms_histogram_bucket", "ingestion_lag_ms_plain_bucket"} {
		selector := metric + `{namespace="ingestion-analytics-main"}`
		for _, query := range []string{
			`sum by (le) (rate(` + selector + `[4m]))`,
			`histogram_quantile(0.99, sum by (le) (rate(` + selector + `[4m])))`,
			`count(` + selector + `)`,
		} {
			test.Logf("%s: %s", query, timeRangeQuery(test, api.URL, query, start, end, "60"))
		}
	}
}
