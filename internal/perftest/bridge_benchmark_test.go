package snuffle

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type bridgeScenario struct {
	name   string
	path   string
	params map[string]string
}

var bridgeDemoScenarios = []bridgeScenario{
	{name: "instant_selector", path: "/api/v1/query", params: map[string]string{"query": `up{job="api"}`, "time": "1700000060"}},
	{name: "aggregation", path: "/api/v1/query", params: map[string]string{"query": "sum by (job) (up)", "time": "1700000060"}},
	{name: "range_rate", path: "/api/v1/query_range", params: map[string]string{"query": `rate(http_requests_total{status="200"}[30s])`, "start": "1700000030", "end": "1700000060", "step": "15s"}},
	{name: "range_selector", path: "/api/v1/query_range", params: map[string]string{"query": `cpu_usage{core="0"}`, "start": "1700000000", "end": "1700000060", "step": "15s"}},
	{name: "labels", path: "/api/v1/labels", params: map[string]string{"start": "1700000000", "end": "1700000060"}},
	{name: "series", path: "/api/v1/series", params: map[string]string{"match[]": `http_requests_total{status="500"}`, "start": "1700000000", "end": "1700000060"}},
}

var bridgeLargeScenarios = []bridgeScenario{
	{name: "large_one_series", path: "/api/v1/query", params: map[string]string{"query": `load_requests_total{instance="host-4242"}`, "time": "1700100135"}},
	{name: "large_job_rate", path: "/api/v1/query", params: map[string]string{"query": `rate(load_requests_total{job="svc-42",status="200"}[30s])`, "time": "1700100135"}},
	{name: "large_sum_rate", path: "/api/v1/query", params: map[string]string{"query": `sum by (status) (rate(load_requests_total{job="svc-42"}[30s]))`, "time": "1700100135"}},
	{name: "large_broad_sum", path: "/api/v1/query", params: map[string]string{"query": "sum by (job) (load_requests_total)", "time": "1700100135"}},
	{name: "large_topk", path: "/api/v1/query", params: map[string]string{"query": `topk(5, load_requests_total{status="200"})`, "time": "1700100135"}},
	{name: "large_range_rate", path: "/api/v1/query_range", params: map[string]string{"query": `rate(load_requests_total{job="svc-42",status="200"}[30s])`, "start": "1700100090", "end": "1700100135", "step": "15s"}},
	{name: "large_long_range_rate", path: "/api/v1/query_range", params: map[string]string{"query": `rate(load_requests_total{job="svc-42",status="200"}[30s])`, "start": "1700100000", "end": "1700100300", "step": "15s"}},
	{name: "large_series_20k", path: "/api/v1/series", params: map[string]string{"match[]": `load_requests_total{status="500"}`, "start": "1700100000", "end": "1700100135"}},
}

type bridgeFetchResult struct {
	elapsed time.Duration
	bytes   int
	errText string
}

func BenchmarkBridgeHTTP(b *testing.B) {
	baseURL := strings.TrimRight(os.Getenv("BRIDGE_BENCH_URL"), "/")
	if baseURL == "" {
		b.Skip("set BRIDGE_BENCH_URL to run sidecar HTTP benchmarks")
	}
	client := &http.Client{Timeout: envDuration("BRIDGE_BENCH_TIMEOUT", 60*time.Second)}
	if err := checkBridgeHealthy(client, baseURL); err != nil {
		b.Fatalf("bridge is not healthy: %v", err)
	}

	scenarios, ok := bridgeScenarios(envString("BRIDGE_BENCH_PROFILE", "large"))
	if !ok {
		b.Fatalf("unknown BRIDGE_BENCH_PROFILE %q", envString("BRIDGE_BENCH_PROFILE", "large"))
	}
	scenarios = filterBridgeScenarios(scenarios, envCSV("BRIDGE_BENCH_SCENARIO"))
	warmup := envInt("BRIDGE_BENCH_WARMUP", 10, 0)
	concurrency := envInt("BRIDGE_BENCH_CONCURRENCY", 1, 1)

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			benchmarkBridgeScenario(b, client, baseURL, sc, warmup, concurrency)
		})
	}
}

func benchmarkBridgeScenario(b *testing.B, client *http.Client, baseURL string, sc bridgeScenario, warmup, concurrency int) {
	targetURL := bridgeURL(baseURL, sc)
	for i := 0; i < warmup; i++ {
		if result := fetchBridge(client, targetURL); result.errText != "" {
			b.Fatalf("warmup failed: %s", result.errText)
		}
	}

	latencies := make([]float64, 0, b.N)
	var bytesTotal int64
	var firstError string
	b.ReportAllocs()
	b.ResetTimer()
	if concurrency == 1 {
		for i := 0; i < b.N; i++ {
			result := fetchBridge(client, targetURL)
			latencies = append(latencies, float64(result.elapsed.Microseconds())/1000)
			bytesTotal += int64(result.bytes)
			if firstError == "" {
				firstError = result.errText
			}
		}
	} else {
		results := make(chan bridgeFetchResult, b.N)
		jobs := make(chan struct{}, b.N)
		var wg sync.WaitGroup
		for worker := 0; worker < concurrency; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range jobs {
					results <- fetchBridge(client, targetURL)
				}
			}()
		}
		for i := 0; i < b.N; i++ {
			jobs <- struct{}{}
		}
		close(jobs)
		wg.Wait()
		close(results)
		for result := range results {
			latencies = append(latencies, float64(result.elapsed.Microseconds())/1000)
			bytesTotal += int64(result.bytes)
			if firstError == "" {
				firstError = result.errText
			}
		}
	}
	b.StopTimer()

	if firstError != "" {
		b.Fatalf("request failed: %s", firstError)
	}
	reportBenchmarkSummary(b, latencies)
	b.ReportMetric(float64(bytesTotal)/float64(b.N), "response_bytes/op")
}

func bridgeScenarios(profile string) ([]bridgeScenario, bool) {
	switch profile {
	case "demo":
		return bridgeDemoScenarios, true
	case "large":
		return bridgeLargeScenarios, true
	default:
		return nil, false
	}
}

func filterBridgeScenarios(scenarios []bridgeScenario, selected map[string]struct{}) []bridgeScenario {
	if len(selected) == 0 {
		return scenarios
	}
	filtered := make([]bridgeScenario, 0, len(scenarios))
	for _, sc := range scenarios {
		if _, ok := selected[sc.name]; ok {
			filtered = append(filtered, sc)
		}
	}
	return filtered
}

func bridgeURL(baseURL string, sc bridgeScenario) string {
	values := url.Values{}
	for key, value := range sc.params {
		values.Set(key, value)
	}
	return baseURL + sc.path + "?" + values.Encode()
}

func checkBridgeHealthy(client *http.Client, baseURL string) error {
	resp, err := client.Get(baseURL + "/-/healthy")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func fetchBridge(client *http.Client, targetURL string) bridgeFetchResult {
	started := time.Now()
	resp, err := client.Get(targetURL)
	if err != nil {
		return bridgeFetchResult{elapsed: time.Since(started), errText: err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return bridgeFetchResult{elapsed: time.Since(started), errText: err.Error()}
	}
	result := bridgeFetchResult{elapsed: time.Since(started), bytes: len(body)}
	if resp.StatusCode != http.StatusOK {
		result.errText = string(body)
		return result
	}
	var payload struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		result.errText = err.Error()
		return result
	}
	if payload.Status != "success" {
		if payload.Error != "" {
			result.errText = payload.Error
		} else {
			result.errText = string(body)
		}
	}
	return result
}
