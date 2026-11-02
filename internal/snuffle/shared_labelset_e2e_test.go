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

// One query selects two counters by a name regex, and pods export both with identical labels. PostHog's
// ingestion pods do this: each exports logs_ingestion_* and metrics_ingestion_*. A function such as rate()
// drops __name__, which leaves two series with one label set before sum by () can merge them. Prometheus 2
// rejected that. VictoriaMetrics and Prometheus 3 with delayed name removal keep the name until the query
// ends and answer.
const (
	sharedAllowed  = `{__name__=~"snuffle_e2e_(logs|metrics)_records_allowed_total"}`
	sharedReceived = `{__name__=~"snuffle_e2e_(logs|metrics)_records_received_total"}`
)

// sharedLabelsetSeries writes 7 samples per counter, 10s apart, from e2eStartMS to e2eEndMS. Rates are per
// second. pod-b exports both signals with non-zero rates, so the sum must add two series per pod, not pick
// one. The other pods export the other signal's counter at 0, as PostHog's pods do. The pods also carry a
// dotted OTel label, as PostHog's do.
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
	// Write with real sample timestamps (as external writers do), not Snuffle's write buckets, so the
	// expected rates below hold exactly.
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	writer := httptest.NewServer(mux)
	defer writer.Close()
	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: sharedLabelsetSeries()})

	// The expected values hold for VictoriaMetrics and for Prometheus. For a [30s] window over samples
	// 10s apart, VictoriaMetrics computes (last - the sample before the window) / 30s and Prometheus
	// extrapolates (last - first) over 20s to 30s. Both give the exact per-second rate.
	rate := func(sel string) string { return `sum by (namespace) (rate(` + sel + `[30s]))` }
	ns := []string{"namespace"}
	for _, c := range []struct {
		name, query, label string
		want               map[string]float64 // by the value of label
		labels             []string           // the label names every series has, exactly
	}{
		{"sum of rate", rate(sharedAllowed), "namespace", map[string]float64{"logs-ingestion": 9, "metrics-ingestion": 3}, ns},
		{"sum of increase", `sum by (namespace) (increase(` + sharedAllowed + `[30s]))`, "namespace", map[string]float64{"logs-ingestion": 270, "metrics-ingestion": 90}, ns},
		// The APM ingestion dashboard's queries, as written
		{"label_replace of the sum", `label_replace(` + rate(sharedAllowed) + `, "signal", "$1", "namespace", "(.*)-ingestion")`, "signal", map[string]float64{"logs": 9, "metrics": 3}, []string{"namespace", "signal"}},
		{"ratio of two merged sums", `100 * (1 - ` + rate(sharedAllowed) + ` / ` + rate(sharedReceived) + `)`, "namespace", map[string]float64{"logs-ingestion": 25, "metrics-ingestion": 50}, ns},
	} {
		t.Run(c.name, func(t *testing.T) {
			// instant at the last sample, and a range whose every step has a full window of samples
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

	// Delayed name removal still drops the name from results, and only when the query ends. These checks
	// pin both sides of that.
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
		// This is the one change for existing queries. VictoriaMetrics behaves the same way. Prometheus 2
		// had already dropped the name, so "metric" came out empty.
		data, err := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {`label_replace(rate(snuffle_e2e_logs_records_allowed_total{pod="pod-a"}[30s]), "metric", "$1", "__name__", "(.+)")`}, "time": {"1700000070"}})
		if err != "" || len(data.Result) != 1 || data.Result[0].Metric["metric"] != "snuffle_e2e_logs_records_allowed_total" {
			t.Fatalf("result %#v, error %q", data, err)
		}
	})
	t.Run("results that keep two series per label set are still rejected", func(t *testing.T) {
		// Without an aggregation the output would hold two series with one label set. Prometheus 3 and
		// VictoriaMetrics reject that too.
		for _, path := range []string{"/api/v1/query", "/api/v1/query_range"} {
			_, err := queryOrError(t, baseURL, path, url.Values{"query": {`rate(` + sharedAllowed + `[30s])`}, "time": {"1700000070"}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"10"}})
			if !strings.Contains(err, "same labelset") {
				t.Errorf("%s: want a same labelset error, got %q", path, err)
			}
		}
	})
}

// assertByLabel checks that the result has exactly one series per want key, with exactly the label names
// in labels (so no __name__), each with points values equal to want.
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

// queryOrError returns the query's data, or the error the API answered with.
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
