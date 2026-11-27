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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

type staleTestPoint struct {
	t                   int64
	counter, gauge, sum float64
	buckets             []uint64 // cumulative over le, +Inf last
	stale               bool
}

var staleTestBounds = []float64{0.1, 0.5, 1}

// staleTestPods returns pod a, scraped every 15 s until it ends with a stale
// marker at minute 20; pod b, which keeps going to minute 30; and pod c,
// which ends with a marker at minute 15 and restarts from zero at minute 20.
func staleTestPods(base int64) map[string][]staleTestPoint {
	pods := map[string][]staleTestPoint{}
	for _, pod := range []string{"a", "b", "c"} {
		last := 120
		if pod == "a" {
			last = 80
		}
		for m := 0; m <= last; m++ {
			p := staleTestPoint{t: base + int64(m)*15000}
			if (pod == "a" && m == last) || (pod == "c" && m == 60) {
				p.stale = true
				pods[pod] = append(pods[pod], p)
				if pod == "a" {
					break
				}
				continue
			}
			if pod == "c" && m > 60 && m < 80 {
				continue
			}
			// Pod c restarts at minute 20: its counters start again from zero.
			n := m
			if pod == "c" && m >= 80 {
				n = m - 80
			}
			p.buckets = make([]uint64, len(staleTestBounds)+1)
			for b := range p.buckets {
				switch pod {
				case "a":
					p.buckets[b] = uint64((n + 1) * (b + 4))
				case "b":
					p.buckets[b] = uint64((n + 1) * (b + 1))
				default:
					p.buckets[b] = uint64((n + 1) * (2*b + 2))
				}
			}
			p.counter, p.gauge, p.sum = float64(5*n+n%3), float64(10+n%7), float64(n)*0.7
			switch pod {
			case "b":
				p.counter, p.gauge = float64(3*n), float64(20+n%5)
			}
			pods[pod] = append(pods[pod], p)
		}
	}
	return pods
}

// TestPostHogStaleMarkers stores series that stop with a Prometheus stale
// marker, written as capture-apm-metrics writes them with
// PROMETHEUS_KEEP_STALE_MARKERS (PostHog/posthog#133926): a counter or gauge
// row whose value is StaleNaN, and for a classic histogram one native row with
// a StaleNaN sum, zero bucket counts and a zero count, on the live fingerprint.
// Capture writes no stale delta histogram: remote write carries cumulative
// series only. Every read path must answer as VictoriaMetrics does for the
// same Prometheus samples, which end pods a and c with StaleNaN on every
// series; pod c's histogram then resumes on the same series from zero. With
// SNUFFLE_E2E_VM_URL set, the test writes the samples to that VictoriaMetrics
// and records its answers in testdata.
func TestPostHogStaleMarkers(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.AllowUnauthenticated = true
	cfg.RemoteWriteInterval = 0
	cfg.RangeCacheMaxBytes = 0
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_stale_markers_%d", time.Now().UnixNano())
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
	cfg.TeamID = e2eTeamID
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	base := e2eStartMS
	pods := staleTestPods(base)
	staleSQL := fmt.Sprintf("reinterpretAsFloat64(toUInt64(%d))", promvalue.StaleNaN)
	value := func(v float64, isStale bool) string {
		if isStale {
			return staleSQL
		}
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	var rows []string
	for pod, points := range pods {
		attributes := fmt.Sprintf("map('instance', %s)", sqlString(pod))
		for _, p := range points {
			ts := chTimeMillis(p.t)
			// Pod c restarts only its histogram: a counter whose window holds only
			// the gap after its marker reads 0 in VictoriaMetrics and no point
			// here, which is the counter rollup's rule, not the histogram's.
			if pod != "c" {
				rows = append(rows,
					fmt.Sprintf("(%d, 'stale_requests_total', cityHash64('counter', %s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'sum', 'cumulative', true, %s, 1, [], [], true, map('region', 'us'), %s)", e2eTeamID, sqlString(pod), ts, value(p.counter, p.stale), attributes),
					fmt.Sprintf("(%d, 'stale_temperature', cityHash64('gauge', %s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'gauge', '', false, %s, 1, [], [], true, map('region', 'us'), %s)", e2eTeamID, sqlString(pod), ts, value(p.gauge, p.stale), attributes))
			}
			// A stale native row keeps the bounds, with zero counts.
			counts, total := make([]string, len(staleTestBounds)+1), uint64(0)
			for b := range counts {
				count := uint64(0)
				if !p.stale {
					count = p.buckets[b]
					if b > 0 {
						count -= p.buckets[b-1]
					}
				}
				counts[b], total = strconv.FormatUint(count, 10), total+count
			}
			rows = append(rows, fmt.Sprintf("(%d, 'stale_latency', cityHash64('histogram', %s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'histogram', 'cumulative', false, %s, %d, [0.1, 0.5, 1], [%s], true, map('region', 'us'), %s)",
				e2eTeamID, sqlString(pod), ts, value(p.sum, p.stale), total, strings.Join(counts, ", "), attributes))
		}
	}
	if err := client.Exec(ctx, "INSERT INTO "+tableName(database, cfg.MetricsInputTable)+" (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes) VALUES "+strings.Join(rows, ", ")); err != nil {
		test.Fatal(err)
	}

	start, end := base+5*60000, base+30*60000
	rangeValues := func() url.Values {
		return url.Values{"start": {strconv.FormatInt(start/1000, 10)}, "end": {strconv.FormatInt(end/1000, 10)}, "step": {"60"}, "nocache": {"1"}}
	}
	queries := []string{
		`rate(stale_requests_total[2m])`,
		`increase(stale_requests_total[2m])`,
		`sum(rate(stale_requests_total[2m]))`,
		`sum(increase(stale_requests_total[2m]))`,
		`stale_requests_total`,
		`stale_temperature`,
		`sum(stale_temperature)`,
		`count(stale_temperature)`,
		`sum by (le) (rate(stale_latency_bucket[5m]))`,
		`histogram_quantile(0.9, sum by (le) (rate(stale_latency_bucket[5m])))`,
		// No window: the compact path reads every row and sums in Go.
		`sum by (le) (rate(stale_latency_bucket))`,
		`count(stale_latency_bucket)`,
		`sum(rate(stale_latency_count[5m]))`,
	}
	golden := filepath.Join(repoRoot(test), "internal", "snuffle", "testdata", "posthog_stale_markers_victoriametrics.json")
	want := map[string]queryDataDTO{}
	if vm := os.Getenv("SNUFFLE_E2E_VM_URL"); vm != "" {
		writeStaleTestPodsToVictoriaMetrics(test, vm, pods)
		for _, query := range queries {
			values := rangeValues()
			values.Set("query", query)
			want[query] = getQueryData(test, vm+"/api/v1/query_range?"+values.Encode())
		}
		encoded, _ := json.MarshalIndent(want, "", "  ")
		if err := os.WriteFile(golden, append(encoded, '\n'), 0o644); err != nil {
			test.Fatal(err)
		}
	} else {
		encoded, err := os.ReadFile(golden)
		if err != nil {
			test.Fatal(err)
		}
		if err := json.Unmarshal(encoded, &want); err != nil {
			test.Fatal(err)
		}
	}

	for _, path := range []struct {
		name                                      string
		compact, histogramPushdown, rangePushdown bool
	}{
		{name: "engine"},
		{name: "range pushdown", rangePushdown: true},
		{name: "compact", compact: true},
		{name: "histogram pushdown", histogramPushdown: true},
	} {
		serverCfg := cfg
		serverCfg.PostHogCompactHistograms = path.compact
		serverCfg.PostHogHistogramPushdown = path.histogramPushdown
		serverCfg.RangePushdown = path.rangePushdown
		mux := http.NewServeMux()
		newServer(serverCfg).routes(mux)
		api := httptest.NewServer(mux)
		for _, query := range queries {
			values := rangeValues()
			values.Set("query", query)
			got := getQueryData(test, api.URL+e2eAPIPath("/api/v1/query_range")+"?"+values.Encode())
			if problem := staleMarkerMismatch(got, want[query]); problem != "" {
				test.Errorf("%s: %s: %s", path.name, query, problem)
			}
		}
		api.Close()
	}
}

func getQueryData(test *testing.T, target string) queryDataDTO {
	test.Helper()
	response, err := http.Get(target)
	if err != nil {
		test.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	var decoded apiResponseDTO[queryDataDTO]
	if err := json.Unmarshal(body, &decoded); err != nil || decoded.Status != "success" {
		test.Fatalf("GET %s: %d %s", target, response.StatusCode, body)
	}
	return decoded.Data
}

// staleMarkerMismatch describes how got differs from want, by the instance
// and le labels, which both stores share, and then by step, or returns "".
func staleMarkerMismatch(got, want queryDataDTO) string {
	index := func(data queryDataDTO) map[string]map[string]float64 {
		out := map[string]map[string]float64{}
		for _, series := range data.Result {
			metric := map[string]string{}
			for _, key := range []string{"instance", "le"} {
				if value, ok := series.Metric[key]; ok {
					metric[key] = value
				}
			}
			points := map[string]float64{}
			for _, value := range series.Values {
				parsed, _ := strconv.ParseFloat(fmt.Sprint(value[1]), 64)
				points[fmt.Sprint(value[0])] = parsed
			}
			out[fmt.Sprint(metric)] = points
		}
		return out
	}
	g, w := index(got), index(want)
	var problems []string
	for key, wantPoints := range w {
		gotPoints, ok := g[key]
		if !ok {
			problems = append(problems, "missing series "+key)
			continue
		}
		for ts, wv := range wantPoints {
			gv, ok := gotPoints[ts]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s missing %s (want %g)", key, ts, wv))
			} else if math.Abs(gv-wv) > 1e-9*math.Max(1, math.Abs(wv)) {
				problems = append(problems, fmt.Sprintf("%s at %s = %g, want %g", key, ts, gv, wv))
			}
		}
		for ts, gv := range gotPoints {
			if _, ok := wantPoints[ts]; !ok {
				problems = append(problems, fmt.Sprintf("%s extra %s = %g", key, ts, gv))
			}
		}
	}
	for key := range g {
		if _, ok := w[key]; !ok {
			problems = append(problems, "extra series "+key)
		}
	}
	sort.Strings(problems)
	if len(problems) > 6 {
		problems = append(problems[:6], fmt.Sprintf("and %d more", len(problems)-6))
	}
	return strings.Join(problems, "; ")
}

// writeStaleTestPodsToVictoriaMetrics writes the pods as Prometheus remote
// write does: one series per counter, gauge and histogram component, with
// StaleNaN on every series of pod a at its stop.
func writeStaleTestPodsToVictoriaMetrics(test *testing.T, vm string, pods map[string][]staleTestPoint) {
	test.Helper()
	stale := math.Float64frombits(promvalue.StaleNaN)
	request := &prompb.WriteRequest{}
	add := func(name, pod string, extra []prompb.Label, value func(staleTestPoint) float64) {
		series := prompb.TimeSeries{Labels: append([]prompb.Label{{Name: "__name__", Value: name}, {Name: "instance", Value: pod}}, extra...)}
		for _, p := range pods[pod] {
			v := stale
			if !p.stale {
				v = value(p)
			}
			series.Samples = append(series.Samples, prompb.Sample{Timestamp: p.t, Value: v})
		}
		request.Timeseries = append(request.Timeseries, series)
	}
	for pod := range pods {
		if pod != "c" {
			add("stale_requests_total", pod, nil, func(p staleTestPoint) float64 { return p.counter })
			add("stale_temperature", pod, nil, func(p staleTestPoint) float64 { return p.gauge })
		}
		add("stale_latency_sum", pod, nil, func(p staleTestPoint) float64 { return p.sum })
		add("stale_latency_count", pod, nil, func(p staleTestPoint) float64 { return float64(p.buckets[len(p.buckets)-1]) })
		for b := range len(staleTestBounds) + 1 {
			le := "+Inf"
			if b < len(staleTestBounds) {
				le = strconv.FormatFloat(staleTestBounds[b], 'g', -1, 64)
			}
			add("stale_latency_bucket", pod, []prompb.Label{{Name: "le", Value: le}}, func(p staleTestPoint) float64 { return float64(p.buckets[b]) })
		}
	}
	payload, err := request.Marshal()
	if err != nil {
		test.Fatal(err)
	}
	response, err := http.Post(vm+"/api/v1/write", "application/x-protobuf", bytes.NewReader(snappy.Encode(nil, payload)))
	if err != nil {
		test.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode/100 != 2 {
		test.Fatalf("VictoriaMetrics remote write: status %d", response.StatusCode)
	}
	flush, err := http.Get(vm + "/internal/force_flush")
	if err != nil {
		test.Fatal(err)
	}
	flush.Body.Close()
}
