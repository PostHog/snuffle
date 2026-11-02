package snuffle

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

// Two counters with identical labels, selected by a name regex, like PostHog's ingestion pods export.
const (
	sharedAllowed  = `{__name__=~"snuffle_e2e_(logs|metrics)_records_allowed_total"}`
	sharedReceived = `{__name__=~"snuffle_e2e_(logs|metrics)_records_received_total"}`
)

// pod-b has both signals non-zero, so the sum must add two series per pod.
func sharedLabelsetSeries() []prompb.TimeSeries {
	counter := func(name, namespace, pod string, perSecond float64) prompb.TimeSeries {
		ts := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: "__name__", Value: name},
			{Name: "k8s.pod.name", Value: pod},
			{Name: "namespace", Value: namespace},
			{Name: "pod", Value: pod},
		}}
		for i := int64(0); i <= 6; i++ {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: e2eStartMS + i*10_000, Value: perSecond * 10 * float64(i)})
		}
		return ts
	}
	var series []prompb.TimeSeries
	for _, c := range []struct {
		namespace, pod                  string
		logsAllowed, logsReceived       float64
		metricsAllowed, metricsReceived float64
	}{
		{"logs-ingestion", "pod-a", 6, 8, 0, 0},
		{"logs-ingestion", "pod-b", 2, 2, 1, 2},
		{"metrics-ingestion", "pod-c", 0, 0, 3, 6},
	} {
		series = append(series,
			counter("snuffle_e2e_logs_records_allowed_total", c.namespace, c.pod, c.logsAllowed),
			counter("snuffle_e2e_logs_records_received_total", c.namespace, c.pod, c.logsReceived),
			counter("snuffle_e2e_metrics_records_allowed_total", c.namespace, c.pod, c.metricsAllowed),
			counter("snuffle_e2e_metrics_records_received_total", c.namespace, c.pod, c.metricsReceived),
		)
	}
	return series
}

func assertSharedLabelsetAcrossNames(t *testing.T, cfg Config, baseURL string) {
	t.Helper()
	// Real sample timestamps, not write buckets, so the rates below are exact.
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	writer := httptest.NewServer(mux)
	defer writer.Close()
	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: sharedLabelsetSeries()})

	// Prometheus and VictoriaMetrics both give these exact values for this data.
	rate := func(sel string) string { return `sum by (namespace) (rate(` + sel + `[30s]))` }
	ns := []string{"namespace"}
	for _, c := range []struct {
		name, query, label string
		want               map[string]float64
		labels             []string // exact label names per series
	}{
		{"sum of rate", rate(sharedAllowed), "namespace", map[string]float64{"logs-ingestion": 9, "metrics-ingestion": 3}, ns},
		{"sum of increase", `sum by (namespace) (increase(` + sharedAllowed + `[30s]))`, "namespace", map[string]float64{"logs-ingestion": 270, "metrics-ingestion": 90}, ns},
		// the APM ingestion dashboard's queries
		{"label_replace of the sum", `label_replace(` + rate(sharedAllowed) + `, "signal", "$1", "namespace", "(.*)-ingestion")`, "signal", map[string]float64{"logs": 9, "metrics": 3}, []string{"namespace", "signal"}},
		{"ratio of two merged sums", `100 * (1 - ` + rate(sharedAllowed) + ` / ` + rate(sharedReceived) + `)`, "namespace", map[string]float64{"logs-ingestion": 25, "metrics-ingestion": 50}, ns},
	} {
		t.Run(c.name, func(t *testing.T) {
			instant, err := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {c.query}, "time": {"1700000070"}})
			if err != "" {
				t.Fatalf("instant: %s", err)
			}
			assertByLabel(t, "instant", instant, c.label, c.labels, c.want, 1)
			ranged, err := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {c.query}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"10"}})
			if err != "" {
				t.Fatalf("range: %s", err)
			}
			assertByLabel(t, "range", ranged, c.label, c.labels, c.want, 4)
		})
	}

	t.Run("rate output carries no metric name", func(t *testing.T) {
		data, err := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {`rate(snuffle_e2e_logs_records_allowed_total{pod="pod-a"}[30s])`}, "time": {"1700000070"}})
		if err != "" || len(data.Result) != 1 {
			t.Fatalf("result %#v, error %q", data, err)
		}
		if name, ok := data.Result[0].Metric["__name__"]; ok {
			t.Fatalf("rate() kept __name__=%q", name)
		}
	})
	t.Run("label_replace reads the name a function will drop", func(t *testing.T) {
		// The one behaviour change, matching VictoriaMetrics.
		data, err := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {`label_replace(rate(snuffle_e2e_logs_records_allowed_total{pod="pod-a"}[30s]), "metric", "$1", "__name__", "(.+)")`}, "time": {"1700000070"}})
		if err != "" || len(data.Result) != 1 || data.Result[0].Metric["metric"] != "snuffle_e2e_logs_records_allowed_total" {
			t.Fatalf("result %#v, error %q", data, err)
		}
	})
	t.Run("results that keep two series per label set are still rejected", func(t *testing.T) {
		// Prometheus 3 and VictoriaMetrics reject this too.
		for _, path := range []string{"/api/v1/query", "/api/v1/query_range"} {
			_, err := queryOrError(t, baseURL, path, url.Values{"query": {`rate(` + sharedAllowed + `[30s])`}, "time": {"1700000070"}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"10"}})
			if !strings.Contains(err, "same labelset") {
				t.Errorf("%s: want a same labelset error, got %q", path, err)
			}
		}
	})
}

// assertByLabel checks one series per want key, with exactly these labels and values.
func assertByLabel(t *testing.T, kind string, data queryDataDTO, label string, labels []string, want map[string]float64, points int) {
	t.Helper()
	if len(data.Result) != len(want) {
		t.Fatalf("%s: %d series, want %d: %#v", kind, len(data.Result), len(want), data.Result)
	}
	for _, series := range data.Result {
		key := series.Metric[label]
		expected, ok := want[key]
		names := make([]string, 0, len(series.Metric))
		for name := range series.Metric {
			names = append(names, name)
		}
		slices.Sort(names)
		if !ok || !slices.Equal(names, labels) {
			t.Fatalf("%s: unexpected series %v, want labels %v", kind, series.Metric, labels)
		}
		values := series.Values
		if series.Value != nil {
			values = [][]any{series.Value}
		}
		if len(values) != points {
			t.Fatalf("%s %s: %d points, want %d: %v", kind, key, len(values), points, values)
		}
		for _, point := range values {
			got, err := strconv.ParseFloat(sampleString(point), 64)
			if err != nil || math.Abs(got-expected) > 1e-9*math.Max(1, math.Abs(expected)) {
				t.Errorf("%s %s at %v: got %v, want %v", kind, key, point[0], sampleString(point), expected)
			}
		}
	}
}

func queryOrError(t *testing.T, baseURL, path string, values url.Values) (queryDataDTO, string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s%s?%s", baseURL, e2eAPIPath(path), values.Encode()))
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var decoded apiResponseDTO[queryDataDTO]
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if decoded.Status != "success" {
		return queryDataDTO{}, decoded.Error
	}
	return decoded.Data, ""
}
