package snuffle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
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
	e2eHistMetric    = "snuffle_e2e_latency_seconds"
)

func TestEndToEndClickHouse(t *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		t.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	chURL := e2eClickHouseURL(getenv("SNUFFLE_E2E_CH_URL", "http://localhost:8123/"))
	dbName := fmt.Sprintf("snuffle_e2e_%d", time.Now().UnixNano())
	rootCfg := ConfigFromEnv()
	rootCfg.CHURL = chURL
	rootCfg.CHDatabase = ""
	rootCfg.CHTimeout = 10 * time.Second
	rootClient := NewClickHouseClient(rootCfg)
	if err := rootClient.ExecWithBody(ctx, "CREATE DATABASE "+quoteIdent(dbName), nil); err != nil {
		t.Fatalf("create e2e database: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = rootClient.ExecWithBody(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)+" SYNC", nil)
	}()

	cfg := ConfigFromEnv()
	cfg.CHURL = chURL
	cfg.CHDatabase = dbName
	cfg.SeriesTable = "metrics_series"
	cfg.SeriesKeysTable = "metrics_series_keys"
	cfg.SamplesTable = "metrics_samples"
	cfg.LabelIndexTable = "metrics_label_index"
	cfg.LabelPostingsTable = "metrics_label_postings"
	cfg.ActivityTable = "metrics_series_activity"
	cfg.MetricsTable = "metrics_metadata"
	cfg.HistogramsTable = "metrics_histograms"
	cfg.ExemplarsTable = "metrics_exemplars"
	cfg.HTTPHost = "127.0.0.1"
	cfg.CHTimeout = 10 * time.Second
	cfg.QueryTimeout = 15 * time.Second

	client := NewClickHouseClient(cfg)
	createMetricsSchema(t, ctx, client)

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()

	postRemoteWrite(t, api.URL, e2eWriteRequest())

	assertInstantQuery(t, api.URL)
	assertRangeQuery(t, api.URL)
	assertLabels(t, api.URL)
	assertLabelValues(t, api.URL)
	assertSeries(t, api.URL)
	assertMetadata(t, api.URL)
	assertExemplars(t, api.URL)
	assertRemoteReadSamples(t, api.URL)
	assertRemoteReadHistograms(t, api.URL)
}

func createMetricsSchema(t *testing.T, ctx context.Context, client *ClickHouseClient) {
	t.Helper()
	schema, err := os.ReadFile(filepath.Join(repoRoot(t), "scripts", "create_metrics_schema.sql"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	for _, statement := range strings.Split(string(schema), ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if err := client.ExecWithBody(ctx, statement, nil); err != nil {
			t.Fatalf("execute schema statement %q: %v", statement, err)
		}
	}
}

func e2eClickHouseURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	query := parsed.Query()
	query.Set("allow_experimental_time_series_aggregate_functions", "1")
	parsed.RawQuery = query.Encode()
	return parsed.String()
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
	payload, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal remote write: %v", err)
	}
	resp, err := http.Post(baseURL+e2eAPIPath("/api/v1/write"), "application/x-protobuf", bytes.NewReader(snappy.Encode(nil, payload)))
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

func assertLabels(t *testing.T, baseURL string) {
	t.Helper()
	labels := apiGet[[]string](t, baseURL, "/api/v1/labels", url.Values{
		"start": {"1700000010"},
		"end":   {"1700000070"},
	})
	assertStringPresent(t, labels, "__name__")
	assertStringPresent(t, labels, "job")
	assertStringPresent(t, labels, "instance")
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

func assertRemoteReadSamples(t *testing.T, baseURL string) {
	t.Helper()
	resp := remoteRead(t, baseURL, e2eCounterMetric)
	if len(resp.Results) != 1 || len(resp.Results[0].Timeseries) != 1 {
		t.Fatalf("remote read samples result = %#v", resp.Results)
	}
	ts := resp.Results[0].Timeseries[0]
	if len(ts.Samples) != 2 || ts.Samples[0].Timestamp != e2eStartMS || ts.Samples[0].Value != 12 || ts.Samples[1].Timestamp != e2eEndMS || ts.Samples[1].Value != 25 {
		t.Fatalf("remote read samples = %#v", ts.Samples)
	}
	if len(ts.Exemplars) != 1 || ts.Exemplars[0].Labels[0].Value != "abc123" {
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
	resp, err := http.Get(baseURL + e2eAPIPath(path) + "?" + values.Encode())
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
