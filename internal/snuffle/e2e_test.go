package snuffle

import (
	"bytes"
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
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	e2eStartMS       = int64(1_700_000_010_000)
	e2eEndMS         = int64(1_700_000_070_000)
	e2eTeamID        = uint64(42)
	e2eCounterMetric = "snuffle_e2e_requests_total"
	e2eRunningMetric = "snuffle_e2e_running_total"
	e2eHistMetric    = "snuffle_e2e_latency_seconds"
)

func TestEndToEndClickHouse(t *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		t.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	chAddr := getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	dbName := fmt.Sprintf("snuffle_e2e_%d", time.Now().UnixNano())
	rootCfg := ConfigFromEnv()
	rootCfg.CHAddr = chAddr
	rootCfg.CHDatabase = ""
	rootCfg.CHTimeout = 10 * time.Second
	rootClient := NewClickHouseClient(rootCfg)
	waitForClickHouse(t, ctx, rootClient)
	if err := rootClient.Exec(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
		t.Fatalf("create e2e database: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = rootClient.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)+" SYNC")
	}()

	cfg := ConfigFromEnv()
	cfg.CHAddr = chAddr
	cfg.CHDatabase = dbName
	if cfg.postHogSchemaLayout() {
		cfg.SeriesTable = "metric_series3"
		cfg.SamplesTable = "metrics2"
		cfg.MetricsInputTable = "metrics2_input"
		cfg.LabelIndexTable = ""
		cfg.AttributeTable = "metric_attributes3"
		cfg.MetricNamesTable = "metric_names3"
		cfg.LabelPostingsTable = ""
		cfg.ActivityTable = ""
		cfg.MetricsTable = ""
		cfg.HistogramsTable = ""
		cfg.ExemplarsTable = ""
		cfg.SampleAttributes = true
	} else {
		cfg.SeriesTable = "metrics_series"
		cfg.SamplesTable = "metrics_samples"
		cfg.LabelIndexTable = "metrics_label_index"
		cfg.LabelPostingsTable = ""
		cfg.ActivityTable = ""
		cfg.MetricsTable = "metrics_metadata"
		cfg.HistogramsTable = "metrics_histograms"
		cfg.ExemplarsTable = "metrics_exemplars"
	}
	cfg.HTTPHost = "127.0.0.1"
	cfg.CHTimeout = 10 * time.Second
	cfg.QueryTimeout = 15 * time.Second

	client := NewClickHouseClient(cfg)
	createMetricsSchema(t, ctx, client)
	logSchema := "create_logs_snuffle_schema.sql"
	if cfg.postHogLogSchemaLayout() {
		logSchema = "create_logs_posthog_schema.sql"
	}
	loadE2ESchema(t, ctx, client, filepath.Join(repoRoot(t), "scripts", logSchema))

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()

	postRemoteWrite(t, api.URL, e2eWriteRequest())
	t.Run("metrics tenant isolation", func(t *testing.T) {
		assertMetricsTenantIsolation(t, api.URL)
	})
	t.Run("Loki round trip and tenant isolation", func(t *testing.T) {
		assertLokiRoundTrip(t, api.URL, cfg)
	})
	t.Run("invalid ClickHouse credentials", func(t *testing.T) {
		assertInvalidClickHouseCredentials(t, api.URL)
	})

	assertInstantQuery(t, api.URL)
	assertRangeQuery(t, api.URL)
	assertUnionQueries(t, api.URL)
	if !cfg.postHogSchemaLayout() {
		assertMetricsQLRunningSumQuery(t, api.URL)
		assertMetricsQLSumOverTimeSubqueryQuery(t, api.URL)
	}
	assertLabels(t, api.URL)
	assertLabelValues(t, api.URL)
	assertSeries(t, api.URL)
	if !cfg.postHogSchemaLayout() {
		assertMetadata(t, api.URL)
		assertExemplars(t, api.URL)
	}
	assertRemoteReadSamples(t, api.URL, !cfg.postHogSchemaLayout())
	if !cfg.postHogSchemaLayout() {
		assertRemoteReadHistograms(t, api.URL)
	}
	t.Run("counter query intervals", func(t *testing.T) {
		assertCounterQueryIntervals(t, cfg, api.URL)
	})
	if cfg.postHogSchemaLayout() {
		t.Run("filtered label discovery", func(t *testing.T) {
			assertPostHogFilteredLabels(t, ctx, client, cfg)
		})
	}
}

func assertCounterQueryIntervals(t *testing.T, cfg Config, baseURL string) {
	t.Helper()
	// External writers do not use Snuffle's timestamp buckets.
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	writer := httptest.NewServer(mux)
	defer writer.Close()

	const metric = "snuffle_e2e_interval_total"
	end := time.Unix(1700000400, 0)
	for _, interval := range []time.Duration{15 * time.Second, time.Minute} {
		var samples []prompb.Sample
		for ts := end.Add(-5*time.Minute + 5*time.Second); ts.Before(end); ts = ts.Add(interval) {
			samples = append(samples, prompb.Sample{
				Timestamp: ts.UnixMilli(),
				Value:     100 + ts.Sub(end.Add(-5*time.Minute+5*time.Second)).Seconds(),
			})
		}
		postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "__name__", Value: metric},
				{Name: "interval", Value: interval.String()},
			},
			Samples: samples,
		}}})
	}
	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: "__name__", Value: metric},
			{Name: "interval", Value: "reset"},
		},
		Samples: []prompb.Sample{
			{Timestamp: end.Add(-5 * time.Minute).UnixMilli(), Value: 900},
			{Timestamp: end.Add(-4 * time.Minute).UnixMilli(), Value: 100},
			{Timestamp: end.Add(-3 * time.Minute).UnixMilli(), Value: 160},
			{Timestamp: end.Add(-2 * time.Minute).UnixMilli(), Value: 10},
			{Timestamp: end.Add(-time.Minute).UnixMilli(), Value: 70},
			{Timestamp: end.UnixMilli(), Value: 130},
		},
	}}})

	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{{Name: "__name__", Value: metric + "_stale"}},
		Samples: []prompb.Sample{
			{Timestamp: end.Add(-15 * time.Second).UnixMilli(), Value: 100},
			{Timestamp: end.UnixMilli(), Value: math.Float64frombits(0x7ff0000000000002)},
		},
	}}})

	// These counter results match VictoriaMetrics v1.152.0.
	for _, tc := range []struct {
		query string
		value string
		// rangePoints is the expected number of range points when not zero.
		rangePoints int
	}{
		{query: metric + "_stale"},
		// The automatic window ends the series before the second step.
		{query: metric + `{interval="1m0s"}`, value: "340", rangePoints: 1},
		{query: `sum(` + metric + `{interval="1m0s"})`, value: "340"},
		{query: `topk(1, ` + metric + `{interval="1m0s"})`, value: "340"},
		{query: `count(count by (interval) (` + metric + `))`, value: "3"},
		{query: `timestamp(` + metric + `{interval="1m0s"})`, value: "1700000345", rangePoints: 2},
		{query: `absent(` + metric + `{interval="none"})`, value: "1", rangePoints: 2},
		{query: `increase(` + metric + `{interval="15s"}[1m])`, value: "60"},
		{query: `increase(` + metric + `{interval="1m0s"}[1m])`, value: "60", rangePoints: 1},
		{query: `increase(` + metric + `{interval="1m0s"}[5m])`, value: "340"},
		{query: `sum(increase(` + metric + `{interval="1m0s"}[5m]))`, value: "340"},
		{query: `increase(` + metric + `{interval="reset"}[5m])`, value: "290"},
		{query: `sum(increase(` + metric + `{interval="reset"}[5m]))`, value: "290"},
		{query: `increase(` + metric + `{interval="1m0s"})`, value: "60"},
		{query: `rate(` + metric + `{interval="1m0s"})`, value: "1"},
		{query: `rate(` + metric + `{interval="1m0s"}[1m])`, value: "1"},
		{query: `irate(` + metric + `{interval="1m0s"}[1m])`, value: "1"},
		{query: `delta(` + metric + `{interval="1m0s"}[1m])`, value: "60"},
		{query: `idelta(` + metric + `{interval="1m0s"}[1m])`, value: "60"},
		{query: `WITH (m = ` + metric + `{interval="1m0s"}) sum(rate(m))`, value: "1"},
		{query: `increase(` + metric + `{interval="1m0s"}[60])`, value: "60"},
		{query: `increase(` + metric + `{interval="1m0s"}[4i])`, value: "240"},
		{query: `increase(` + metric + `{interval="1m0s"}[1m] offset 1m)`, value: "60"},
		{query: `increase(` + metric + `{interval="1m0s"}[1m] offset 0m)`, value: "60"},
		{query: `increase(` + metric + `{interval="1m0s"}[1m] offset -1m)`},
		// The @ modifier keeps the evaluation time fixed at every step.
		{query: `increase(` + metric + `{interval="1m0s"}[1m] @ 1700000400)`, value: "60", rangePoints: 2},
	} {
		t.Run(tc.query, func(t *testing.T) {
			for _, path := range []string{"/api/v1/query", "/api/v1/query_range"} {
				params := url.Values{"query": {tc.query}, "time": {"1700000400"}, "step": {"1m"}}
				resultType := "vector"
				if path == "/api/v1/query_range" {
					params.Set("start", "1700000400")
					params.Set("end", "1700000460")
					params.Set("step", "1m")
					resultType = "matrix"
				}
				data := apiGet[queryDataDTO](t, baseURL, path, params)
				if data.ResultType != resultType {
					t.Fatalf("%s result type = %s, want %s", path, data.ResultType, resultType)
				}
				if tc.value == "" {
					if len(data.Result) != 0 {
						t.Errorf("%s returned %v with fewer than two samples", path, data.Result)
					}
					continue
				}
				if len(data.Result) != 1 {
					t.Errorf("%s returned %d series, want 1", path, len(data.Result))
					continue
				}
				point := data.Result[0].Value
				if path == "/api/v1/query_range" {
					if len(data.Result[0].Values) == 0 {
						t.Errorf("%s returned no points", path)
						continue
					}
					if tc.rangePoints > 0 && len(data.Result[0].Values) != tc.rangePoints {
						t.Errorf("%s returned %d points, want %d", path, len(data.Result[0].Values), tc.rangePoints)
					}
					point = data.Result[0].Values[0]
					// Check the result when this expression is part of a binary operation.
					params.Set("query", tc.query+" + 0")
					engine := apiGet[queryDataDTO](t, baseURL, path, params)
					if len(engine.Result) != 1 {
						t.Fatalf("the binary expression returned %d series, want 1", len(engine.Result))
					}
					assertSameSampleValues(t, tc.query, data.Result[0].Values, engine.Result[0].Values)
				}
				if got := sampleString(point); got != tc.value {
					t.Errorf("%s value = %s, want %s", path, got, tc.value)
				}
			}
		})
	}
}

func waitForClickHouse(t *testing.T, ctx context.Context, client *ClickHouseClient) {
	t.Helper()
	if client.connErr != nil {
		t.Fatalf("connect clickhouse: %v", client.connErr)
	}

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		pingCtx, pingCancel := context.WithTimeout(readyCtx, 5*time.Second)
		err := client.Ping(pingCtx)
		pingCancel()
		if err == nil {
			return
		}
		lastErr = err

		select {
		case <-readyCtx.Done():
			t.Fatalf("clickhouse did not become ready: %v (last error: %v)", readyCtx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func createMetricsSchema(t *testing.T, ctx context.Context, client *ClickHouseClient) {
	t.Helper()
	schemaPath := os.Getenv("SNUFFLE_E2E_SCHEMA_FILE")
	if schemaPath == "" {
		schemaName := "create_metrics_schema.sql"
		if storageSchemaLayout(os.Getenv("CH_SCHEMA_LAYOUT")) == schemaLayoutPostHog {
			schemaName = "create_metrics_posthog_schema.sql"
		}
		schemaPath = filepath.Join(repoRoot(t), "scripts", schemaName)
	} else if !filepath.IsAbs(schemaPath) {
		schemaPath = filepath.Join(repoRoot(t), schemaPath)
	}
	loadE2ESchema(t, ctx, client, schemaPath)
}

func loadE2ESchema(t *testing.T, ctx context.Context, client *ClickHouseClient, schemaPath string) {
	t.Helper()
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	for _, statement := range strings.Split(string(schema), ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if err := client.Exec(ctx, statement); err != nil {
			t.Fatalf("execute schema statement %q: %v", statement, err)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func e2eWriteRequest() *prompb.WriteRequest {
	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: labels.MetricName, Value: e2eCounterMetric},
					{Name: "job", Value: "api"},
					{Name: "instance", Value: "host-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: e2eStartMS, Value: 10},
					{Timestamp: e2eStartMS + 5_000, Value: 12},
					{Timestamp: e2eEndMS, Value: 25},
				},
				Exemplars: []prompb.Exemplar{{
					Labels:    []prompb.Label{{Name: "trace_id", Value: "abc123"}},
					Value:     25,
					Timestamp: e2eEndMS,
				}},
			},
			{
				Labels: []prompb.Label{
					{Name: labels.MetricName, Value: e2eHistMetric},
					{Name: "job", Value: "api"},
					{Name: "instance", Value: "host-a"},
				},
				Histograms: []prompb.Histogram{{
					Count:     &prompb.Histogram_CountInt{CountInt: 3},
					Sum:       1.5,
					ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
					Timestamp: e2eEndMS,
				}},
			},
			{
				Labels: []prompb.Label{
					{Name: labels.MetricName, Value: e2eRunningMetric},
					{Name: "job", Value: "api"},
					{Name: "instance", Value: "host-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: e2eStartMS, Value: 1},
					{Timestamp: e2eStartMS + 10_000, Value: 2},
					{Timestamp: e2eStartMS + 20_000, Value: 3},
					{Timestamp: e2eStartMS + 30_000, Value: 4},
					{Timestamp: e2eStartMS + 40_000, Value: 5},
					{Timestamp: e2eStartMS + 50_000, Value: 6},
					{Timestamp: e2eEndMS, Value: 7},
				},
			},
		},
		Metadata: []prompb.MetricMetadata{
			{
				MetricFamilyName: e2eCounterMetric,
				Type:             prompb.MetricMetadata_COUNTER,
				Help:             "e2e request count",
				Unit:             "requests",
			},
			{
				MetricFamilyName: e2eHistMetric,
				Type:             prompb.MetricMetadata_HISTOGRAM,
				Help:             "e2e latency",
				Unit:             "seconds",
			},
		},
	}
}

func postRemoteWrite(t *testing.T, baseURL string, req *prompb.WriteRequest) {
	t.Helper()
	postRemoteWriteForTeam(t, baseURL, e2eTeamID, req)
}

func postRemoteWriteForTeam(t *testing.T, baseURL string, teamID uint64, req *prompb.WriteRequest) {
	t.Helper()
	payload, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal remote write: %v", err)
	}
	resp, err := http.Post(fmt.Sprintf("%s/t/%d/api/v1/write", baseURL, teamID), "application/x-protobuf", bytes.NewReader(snappy.Encode(nil, payload)))
	if err != nil {
		t.Fatalf("post remote write: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("remote write status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func assertInstantQuery(t *testing.T, baseURL string) {
	t.Helper()
	data := apiGet[queryDataDTO](t, baseURL, "/api/v1/query", url.Values{
		"query": {`sum by (job) (` + e2eCounterMetric + `)`},
		"time":  {"1700000070"},
	})
	if data.ResultType != "vector" || len(data.Result) != 1 {
		t.Fatalf("instant query result = %#v", data)
	}
	if got := data.Result[0].Metric["job"]; got != "api" {
		t.Fatalf("instant query job = %q", got)
	}
	if got := sampleString(data.Result[0].Value); got != "25" {
		t.Fatalf("instant query value = %q", got)
	}

	lateData := apiGet[queryDataDTO](t, baseURL, "/api/v1/query", url.Values{
		"query": {`sum by (job) (` + e2eCounterMetric + `)`},
		"time":  {"1700000080"},
	})
	if lateData.ResultType != "vector" || len(lateData.Result) != 1 {
		t.Fatalf("late instant query result = %#v", lateData)
	}
	if got := sampleString(lateData.Result[0].Value); got != "25" {
		t.Fatalf("late instant query value = %q", got)
	}
}

func assertRangeQuery(t *testing.T, baseURL string) {
	t.Helper()
	data := apiGet[queryDataDTO](t, baseURL, "/api/v1/query_range", url.Values{
		"query": {e2eCounterMetric + `{job="api"} + 0`},
		"start": {"1700000010"},
		"end":   {"1700000070"},
		"step":  {"60s"},
	})
	if data.ResultType != "matrix" || len(data.Result) != 1 || len(data.Result[0].Values) != 2 {
		t.Fatalf("range query result = %#v", data)
	}
	if got := sampleString(data.Result[0].Values[0]); got != "12" {
		t.Fatalf("range query first bucket value = %q", got)
	}
	if got := sampleString(data.Result[0].Values[1]); got != "25" {
		t.Fatalf("range query final value = %q", got)
	}
}

func assertUnionQueries(t *testing.T, baseURL string) {
	t.Helper()
	// The fast union path handles plain `or`; `or on (job, instance)` is
	// rejected and evaluated by the Prometheus engine, so the two spellings of
	// the same expression must agree.
	fastBody := `increase(` + e2eCounterMetric + `{job="api"}[2m]) / 5 or increase(` + e2eCounterMetric + `{job="none"}[2m]) / 5`
	engineBody := `increase(` + e2eCounterMetric + `{job="api"}[2m]) / 5 or on (job, instance) increase(` + e2eCounterMetric + `{job="none"}[2m]) / 5`

	rangeParams := func(query string) url.Values {
		return url.Values{
			"query": {`sum by (job) (` + query + `)`},
			"start": {"1700000010"},
			"end":   {"1700000130"},
			"step":  {"10s"},
		}
	}
	fast := apiGet[queryDataDTO](t, baseURL, "/api/v1/query_range", rangeParams(fastBody))
	engine := apiGet[queryDataDTO](t, baseURL, "/api/v1/query_range", rangeParams(engineBody))
	if fast.ResultType != "matrix" || len(fast.Result) != 1 || len(fast.Result[0].Values) == 0 {
		t.Fatalf("range union query result = %#v", fast)
	}
	if got := fast.Result[0].Metric["job"]; got != "api" {
		t.Fatalf("range union query job = %q", got)
	}
	assertSameSampleValues(t, "range union", fast.Result[0].Values, engine.Result[0].Values)

	instantParams := func(query string) url.Values {
		return url.Values{
			"query": {`sum by (job) (` + query + `)`},
			"time":  {"1700000070"},
		}
	}
	fastInstant := apiGet[queryDataDTO](t, baseURL, "/api/v1/query", instantParams(fastBody))
	engineInstant := apiGet[queryDataDTO](t, baseURL, "/api/v1/query", instantParams(engineBody))
	if fastInstant.ResultType != "vector" || len(fastInstant.Result) != 1 {
		t.Fatalf("instant union query result = %#v", fastInstant)
	}
	assertSameSampleValues(t, "instant union", [][]any{fastInstant.Result[0].Value}, [][]any{engineInstant.Result[0].Value})
}

func assertSameSampleValues(t *testing.T, name string, got, want [][]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s sample count = %d, want %d (got %#v, want %#v)", name, len(got), len(want), got, want)
	}
	for i := range got {
		if gotTS, wantTS := fmt.Sprint(got[i][0]), fmt.Sprint(want[i][0]); gotTS != wantTS {
			t.Fatalf("%s sample %d timestamp = %s, want %s", name, i, gotTS, wantTS)
		}
		gotValue, err := strconv.ParseFloat(sampleString(got[i]), 64)
		if err != nil {
			t.Fatalf("%s sample %d value parse: %v", name, i, err)
		}
		wantValue, err := strconv.ParseFloat(sampleString(want[i]), 64)
		if err != nil {
			t.Fatalf("%s sample %d want value parse: %v", name, i, err)
		}
		if diff := math.Abs(gotValue - wantValue); diff > 1e-9*math.Max(math.Abs(gotValue), math.Abs(wantValue))+1e-12 {
			t.Fatalf("%s sample %d value = %v, want %v", name, i, gotValue, wantValue)
		}
	}
}

func assertMetricsQLRunningSumQuery(t *testing.T, baseURL string) {
	t.Helper()
	data := apiGet[queryDataDTO](t, baseURL, "/api/v1/query_range", url.Values{
		"query": {`quantile(1, running_sum(delta(` + e2eRunningMetric + `{job="api"}[60s])))`},
		"start": {"1700000060"},
		"end":   {"1700000070"},
		"step":  {"10s"},
	})
	if data.ResultType != "matrix" || len(data.Result) != 1 || len(data.Result[0].Values) == 0 {
		t.Fatalf("running_sum query result = %#v", data)
	}
}

func assertMetricsQLSumOverTimeSubqueryQuery(t *testing.T, baseURL string) {
	t.Helper()
	data := apiGet[queryDataDTO](t, baseURL, "/api/v1/query_range", url.Values{
		"query": {`quantile(1, sum_over_time(delta(` + e2eRunningMetric + `{job="api"}[20s])[30s]))`},
		"start": {"1700000040"},
		"end":   {"1700000070"},
		"step":  {"10s"},
	})
	if data.ResultType != "matrix" || len(data.Result) != 1 || len(data.Result[0].Values) == 0 {
		t.Fatalf("sum_over_time subquery result = %#v", data)
	}
}

func assertPostHogFilteredLabels(t *testing.T, ctx context.Context, client *ClickHouseClient, cfg Config) {
	t.Helper()
	const metric = "snuffle_e2e_discovery"
	const service = "snuffle-discovery"
	insert := fmt.Sprintf(`INSERT INTO %s
		(team_id, metric_name, series_fingerprint, timestamp, observed_timestamp,
		 original_expiry_timestamp, service_name, value, count, has_labels, resource_attributes, attributes)
		SELECT %d, %s, 987654321, %s, now64(6), now64(6) + INTERVAL 1 DAY,
		       %s, 1, 1, true, map('zone', 'resource-zone', 'host', 'host-1'),
		       map('zone', 'metric-zone', 'status', '200')`,
		tableName(cfg.CHDatabase, cfg.MetricsInputTable), e2eTeamID, sqlString(metric), chTimeMillis(e2eStartMS),
		sqlString(service))
	if err := client.Exec(ctx, insert); err != nil {
		t.Fatalf("insert discovery fixture: %v", err)
	}

	for _, version := range []string{"3", "2"} {
		t.Run("metadata"+version, func(t *testing.T) {
			readCfg := cfg
			readCfg.SeriesTable = "metric_series" + version
			readCfg.AttributeTable = "metric_attributes" + version
			if version == "2" {
				readCfg.MetricNamesTable = ""
				readCfg.AttributeTableHasMetricName = false
			}
			mux := http.NewServeMux()
			newServer(readCfg).routes(mux)
			api := httptest.NewServer(mux)
			defer api.Close()

			for _, selector := range []string{metric, metric + `{service_name="` + service + `"}`, `{service_name="` + service + `"}`, metric + `{status="200"}`} {
				t.Run(selector, func(t *testing.T) {
					params := url.Values{
						"match[]": {selector},
						"start":   {"1700000010"},
						"end":     {"1700000070"},
					}
					names := apiGet[[]string](t, api.URL, "/api/v1/labels", params)
					for _, name := range []string{"__name__", "service_name", "zone", "host", "status"} {
						assertStringPresent(t, names, name)
					}
					for name, want := range map[string]string{"host": "host-1", "status": "200"} {
						values := apiGet[[]string](t, api.URL, "/api/v1/label/"+name+"/values", params)
						if len(values) != 1 || values[0] != want {
							t.Errorf("label %q values = %q, want [%q]", name, values, want)
						}
					}
					// A key in both maps lists the resource value; the rollup also lists the metric value.
					zones := apiGet[[]string](t, api.URL, "/api/v1/label/zone/values", params)
					assertStringPresent(t, zones, "resource-zone")

					series := apiGet[[]map[string]string](t, api.URL, "/api/v1/series", params)
					if len(series) != 1 || series[0]["zone"] != "resource-zone" {
						t.Errorf("series for %q = %#v, want zone=resource-zone", selector, series)
					}
				})
			}

			if version == "2" {
				// The old table can contain names that have not reached the new table.
				const oldMetric = "snuffle_e2e_old_catalog_only"
				insertOld := fmt.Sprintf(`INSERT INTO %s
					(team_id, metric_name, series_fingerprint, last_seen, original_expiry_timestamp)
					VALUES (%d, %s, 987654322, %s, now64(6) + INTERVAL 1 DAY)`,
					tableName(cfg.CHDatabase, readCfg.SeriesTable), e2eTeamID, sqlString(oldMetric), chTimeMillis(e2eStartMS))
				if err := client.Exec(ctx, insertOld); err != nil {
					t.Fatalf("insert old catalog fixture: %v", err)
				}
				names := apiGet[[]string](t, api.URL, "/api/v1/label/__name__/values", url.Values{
					"start": {"1700000010"},
					"end":   {"1700000070"},
				})
				assertStringPresent(t, names, oldMetric)
			}
		})
	}
}

func assertLabels(t *testing.T, baseURL string) {
	t.Helper()
	labels := apiGet[[]string](t, baseURL, "/api/v1/labels", url.Values{
		"start": {"1700000010"},
		"end":   {"1700000070"},
	})
	assertStringPresent(t, labels, "__name__")
	assertStringPresent(t, labels, "job")
	assertStringPresent(t, labels, "instance")

	metricLabels := apiGet[[]string](t, baseURL, "/api/v1/labels", url.Values{
		"match[]": {e2eCounterMetric},
		"start":   {"1700000010"},
		"end":     {"1700000070"},
	})
	assertStringPresent(t, metricLabels, "job")
	assertStringPresent(t, metricLabels, "instance")

	names := apiGet[[]string](t, baseURL, "/api/v1/label/__name__/values", url.Values{
		"start": {"1700000010"},
		"end":   {"1700000070"},
	})
	assertStringPresent(t, names, e2eCounterMetric)

	searched := apiGet[[]string](t, baseURL, "/api/v1/label/__name__/values", url.Values{
		"match[]": {`{__name__=~".*e2e_requests.*"}`},
		"start":   {"1700000010"},
		"end":     {"1700000070"},
	})
	if len(searched) != 1 || searched[0] != e2eCounterMetric {
		t.Fatalf("metric name search = %q, want [%q]", searched, e2eCounterMetric)
	}

	jobs := apiGet[[]string](t, baseURL, "/api/v1/label/job/values", url.Values{
		"match[]": {e2eCounterMetric},
		"start":   {"1700000010"},
		"end":     {"1700000070"},
	})
	assertStringPresent(t, jobs, "api")
}

func assertLabelValues(t *testing.T, baseURL string) {
	t.Helper()
	values := apiGet[[]string](t, baseURL, "/api/v1/label/job/values", url.Values{
		"match[]": {e2eCounterMetric + `{instance="host-a"}`},
		"start":   {"1700000010"},
		"end":     {"1700000070"},
	})
	assertStringPresent(t, values, "api")
}

func assertSeries(t *testing.T, baseURL string) {
	t.Helper()
	series := apiGet[[]map[string]string](t, baseURL, "/api/v1/series", url.Values{
		"match[]": {e2eCounterMetric + `{job="api"}`},
		"start":   {"1700000010"},
		"end":     {"1700000070"},
	})
	if len(series) != 1 || series[0][labels.MetricName] != e2eCounterMetric || series[0]["instance"] != "host-a" {
		t.Fatalf("series result = %#v", series)
	}
}

func assertMetadata(t *testing.T, baseURL string) {
	t.Helper()
	metadata := apiGet[map[string][]metadataDTO](t, baseURL, "/api/v1/metadata", url.Values{"metric": {e2eCounterMetric}})
	rows := metadata[e2eCounterMetric]
	if len(rows) != 1 || rows[0].Type != "counter" || rows[0].Unit != "requests" {
		t.Fatalf("metadata result = %#v", metadata)
	}
}

func assertExemplars(t *testing.T, baseURL string) {
	t.Helper()
	result := apiGet[[]exemplarQueryDTO](t, baseURL, "/api/v1/query_exemplars", url.Values{
		"query": {e2eCounterMetric},
		"start": {"1700000010"},
		"end":   {"1700000070"},
	})
	if len(result) != 1 || len(result[0].Exemplars) != 1 || result[0].Exemplars[0].Labels["trace_id"] != "abc123" {
		t.Fatalf("exemplar result = %#v", result)
	}
}

func assertRemoteReadSamples(t *testing.T, baseURL string, expectExemplars bool) {
	t.Helper()
	resp := remoteRead(t, baseURL, e2eCounterMetric)
	if len(resp.Results) != 1 || len(resp.Results[0].Timeseries) != 1 {
		t.Fatalf("remote read samples result = %#v", resp.Results)
	}
	ts := resp.Results[0].Timeseries[0]
	if len(ts.Samples) != 2 || ts.Samples[0].Timestamp != e2eStartMS || ts.Samples[0].Value != 12 || ts.Samples[1].Timestamp != e2eEndMS || ts.Samples[1].Value != 25 {
		t.Fatalf("remote read samples = %#v", ts.Samples)
	}
	if expectExemplars && (len(ts.Exemplars) != 1 || ts.Exemplars[0].Labels[0].Value != "abc123") {
		t.Fatalf("remote read exemplars = %#v", ts.Exemplars)
	}
}

func assertRemoteReadHistograms(t *testing.T, baseURL string) {
	t.Helper()
	resp := remoteRead(t, baseURL, e2eHistMetric)
	if len(resp.Results) != 1 || len(resp.Results[0].Timeseries) != 1 {
		t.Fatalf("remote read histogram result = %#v", resp.Results)
	}
	if got := len(resp.Results[0].Timeseries[0].Histograms); got != 1 {
		t.Fatalf("remote read histogram count = %d", got)
	}
}

func remoteRead(t *testing.T, baseURL, metric string) *prompb.ReadResponse {
	t.Helper()
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: e2eStartMS,
			EndTimestampMs:   e2eEndMS,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  labels.MetricName,
				Value: metric,
			}},
		}},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
	}
	payload, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal remote read: %v", err)
	}
	httpResp, err := http.Post(baseURL+e2eAPIPath("/api/v1/read"), "application/x-protobuf", bytes.NewReader(snappy.Encode(nil, payload)))
	if err != nil {
		t.Fatalf("post remote read: %v", err)
	}
	defer httpResp.Body.Close()
	body, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode != http.StatusOK {
		t.Fatalf("remote read status %d: %s", httpResp.StatusCode, strings.TrimSpace(string(body)))
	}
	decoded, err := snappy.Decode(nil, body)
	if err != nil {
		t.Fatalf("decode remote read response: %v", err)
	}
	var resp prompb.ReadResponse
	if err := resp.Unmarshal(decoded); err != nil {
		t.Fatalf("unmarshal remote read response: %v", err)
	}
	return &resp
}

type apiResponseDTO[T any] struct {
	Status string `json:"status"`
	Data   T      `json:"data"`
	Error  string `json:"error"`
}

type queryDataDTO struct {
	ResultType string            `json:"resultType"`
	Result     []sampleResultDTO `json:"result"`
}

type sampleResultDTO struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
	Values [][]any           `json:"values"`
}

type metadataDTO struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

type exemplarQueryDTO struct {
	SeriesLabels map[string]string `json:"seriesLabels"`
	Exemplars    []exemplarDTO     `json:"exemplars"`
}

type exemplarDTO struct {
	Labels    map[string]string `json:"labels"`
	Value     string            `json:"value"`
	Timestamp float64           `json:"timestamp"`
}

func apiGet[T any](t *testing.T, baseURL, path string, values url.Values) T {
	t.Helper()
	return apiGetForTeam[T](t, baseURL, e2eTeamID, path, values)
}

func apiGetForTeam[T any](t *testing.T, baseURL string, teamID uint64, path string, values url.Values) T {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/t/%d%s?%s", baseURL, teamID, path, values.Encode()))
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded apiResponseDTO[T]
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode %s response %s: %v", path, string(body), err)
	}
	if decoded.Status != "success" {
		t.Fatalf("GET %s returned %q: %s", path, decoded.Status, decoded.Error)
	}
	return decoded.Data
}

func e2eAPIPath(path string) string {
	return fmt.Sprintf("/t/%d%s", e2eTeamID, path)
}

func sampleString(value []any) string {
	if len(value) != 2 {
		return ""
	}
	if s, ok := value[1].(string); ok {
		return s
	}
	return fmt.Sprint(value[1])
}

func assertStringPresent(t *testing.T, values []string, want string) {
	t.Helper()
	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("%q not found in %#v", want, values)
}
