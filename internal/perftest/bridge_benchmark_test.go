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

func bridgePostHogMetricScenarios() []bridgeScenario {
	return bridgeTSBSMetricScenarios()
}

func bridgeSnuffleMetricScenarios() []bridgeScenario {
	return bridgeTSBSMetricScenarios()
}

func bridgeTSBSMetricScenarios() []bridgeScenario {
	metric := envString("BRIDGE_BENCH_TSBS_METRIC", "usage_user")
	host := envString("BRIDGE_BENCH_TSBS_HOSTNAME", "host_42")
	eval := envString("BRIDGE_BENCH_TSBS_QUERY_TIME", "1451608185")
	rangeStart := envString("BRIDGE_BENCH_TSBS_RANGE_START", "1451607900")
	rangeEnd := envString("BRIDGE_BENCH_TSBS_RANGE_END", eval)
	step := envString("BRIDGE_BENCH_TSBS_STEP", "15s")
	unionSelectors := envInt("BRIDGE_BENCH_TSBS_UNION_SELECTORS", 32, 2)
	unionRange := envString("BRIDGE_BENCH_TSBS_UNION_RANGE", "5m")
	unionDivisor := envString("BRIDGE_BENCH_TSBS_UNION_DIVISOR", "5")

	hostSelector := fmt.Sprintf(`%s{hostname=%q}`, metric, host)
	metricSelector := fmt.Sprintf(`%s`, metric)
	sumByRegion := fmt.Sprintf(`sum by (region) (%s)`, metric)
	avgByEnvironment := fmt.Sprintf(`avg by (service_environment) (%s)`, metric)
	topk := fmt.Sprintf(`topk(10, %s)`, metric)
	instantUnion := tsbsInstantUnionQuery(metric, unionSelectors, unionRange, unionDivisor)
	nestedCountHostname := fmt.Sprintf(`count(count(%s) by (hostname))`, metric)
	nestedCountRegion := fmt.Sprintf(`count(count(%s) by (region))`, metric)
	nestedCountFilteredHostname := fmt.Sprintf(`count(count(%s{service_environment="production"}) by (hostname))`, metric)

	return []bridgeScenario{
		{name: "tsbs_one_series", path: "/api/v1/query", params: map[string]string{"query": hostSelector, "time": eval}},
		{name: "tsbs_sum_by_region", path: "/api/v1/query", params: map[string]string{"query": sumByRegion, "time": eval}},
		{name: "tsbs_avg_by_environment", path: "/api/v1/query", params: map[string]string{"query": avgByEnvironment, "time": eval}},
		{name: "tsbs_topk", path: "/api/v1/query", params: map[string]string{"query": topk, "time": eval}},
		{name: "tsbs_instant_or_union", path: "/api/v1/query", params: map[string]string{"query": instantUnion, "time": eval}},
		{name: "tsbs_range_or_union", path: "/api/v1/query_range", params: map[string]string{"query": instantUnion, "start": rangeStart, "end": rangeEnd, "step": step}},
		{name: "tsbs_range_selector", path: "/api/v1/query_range", params: map[string]string{"query": hostSelector, "start": rangeStart, "end": rangeEnd, "step": step}},
		{name: "tsbs_range_sum_by_region", path: "/api/v1/query_range", params: map[string]string{"query": sumByRegion, "start": rangeStart, "end": rangeEnd, "step": step}},
		{name: "tsbs_nested_count_hostname", path: "/api/v1/query_range", params: map[string]string{"query": nestedCountHostname, "start": rangeStart, "end": rangeEnd, "step": step}},
		{name: "tsbs_nested_count_region", path: "/api/v1/query_range", params: map[string]string{"query": nestedCountRegion, "start": rangeStart, "end": rangeEnd, "step": step}},
		{name: "tsbs_nested_count_filtered_hostname", path: "/api/v1/query_range", params: map[string]string{"query": nestedCountFilteredHostname, "start": rangeStart, "end": rangeEnd, "step": step}},
		{name: "tsbs_series_metric", path: "/api/v1/series", params: map[string]string{"match[]": metricSelector, "start": rangeStart, "end": rangeEnd}},
		{name: "tsbs_label_values_hostname", path: "/api/v1/label/hostname/values", params: map[string]string{"start": rangeStart, "end": rangeEnd}},
	}
}

func tsbsInstantUnionQuery(metric string, selectors int, window, divisor string) string {
	parts := make([]string, 0, selectors)
	for i := range selectors {
		selector := fmt.Sprintf(`%s{hostname=%q}`, metric, fmt.Sprintf("host_%d", i))
		parts = append(parts, fmt.Sprintf(`increase(%s[%s]) / %s`, selector, window, divisor))
	}
	return fmt.Sprintf(`sum by (hostname) (%s)`, strings.Join(parts, " or "))
}

func TestTSBSInstantUnionQueryUsesLargeInstantAggregateUnion(t *testing.T) {
	query := tsbsInstantUnionQuery("usage_user", 32, "5m", "5")
	for _, want := range []string{
		`sum by (hostname) (`,
		`increase(usage_user{hostname="host_0"}[5m]) / 5`,
		`increase(usage_user{hostname="host_31"}[5m]) / 5`,
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query does not contain %q:\n%s", want, query)
		}
	}
	if got := strings.Count(query, " or "); got != 31 {
		t.Fatalf("or count = %d, want 31 in query:\n%s", got, query)
	}
}

func bridgePostHogLogScenarios() []bridgeScenario {
	start := envString("BRIDGE_BENCH_LOG_START_NS", "1780272000000000000")
	end := envString("BRIDGE_BENCH_LOG_END_NS", "1780358399000000000")
	step := envString("BRIDGE_BENCH_LOG_STEP", "60s")
	limit := envString("BRIDGE_BENCH_LOG_LIMIT", "5000")
	commonRangeParams := func(query string) map[string]string {
		return map[string]string{
			"query":     query,
			"start":     start,
			"end":       end,
			"step":      step,
			"limit":     limit,
			"direction": "backward",
		}
	}
	commonMetadataParams := func() map[string]string {
		return map[string]string{
			"start": start,
			"end":   end,
			"limit": limit,
		}
	}

	return []bridgeScenario{
		{name: "log_basic", path: "/loki/api/v1/query_range", params: commonRangeParams(`{app="snuffle-bench"}`)},
		{name: "log_line_error", path: "/loki/api/v1/query_range", params: commonRangeParams(`{app="snuffle-bench"} |= "error"`)},
		{name: "log_regex_checkout", path: "/loki/api/v1/query_range", params: commonRangeParams(`{app="snuffle-bench"} |~ "checkout|failed"`)},
		{name: "log_host", path: "/loki/api/v1/query_range", params: commonRangeParams(`{app="snuffle-bench",host="host-42"}`)},
		{name: "log_sum_count", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum(count_over_time({app="snuffle-bench"}[5m]))`)},
		{name: "log_count_by_service", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum by (service_name) (count_over_time({app="snuffle-bench"}[5m]))`)},
		{name: "log_rate_by_region", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum by (region) (rate({app="snuffle-bench"} |= "message" [5m]))`)},
		{name: "log_bytes_by_format", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum by (format) (bytes_over_time({app="snuffle-bench"}[5m]))`)},
		{name: "log_topk_rate", path: "/loki/api/v1/query_range", params: commonRangeParams(`topk(5, sum by (service_name) (rate({app="snuffle-bench"}[5m])))`)},
		{name: "log_comparison", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum by (service_name) (count_over_time({app="snuffle-bench"}[5m])) > 10`)},
		{name: "log_logfmt_sum_duration", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum_over_time({app="snuffle-bench",format="logfmt"} | logfmt | unwrap duration(duration) [5m]) by (service_name)`)},
		{name: "log_logfmt_avg_size", path: "/loki/api/v1/query_range", params: commonRangeParams(`avg_over_time({app="snuffle-bench",format="logfmt"} | logfmt | unwrap bytes(size) [5m]) by (region)`)},
		{name: "log_regexp_sum_duration", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum_over_time({app="snuffle-bench",format="logfmt"} | regexp "duration=(?P<duration>[0-9]+ms)" | unwrap duration(duration) [5m]) by (service_name)`)},
		{name: "log_pattern_avg_size", path: "/loki/api/v1/query_range", params: commonRangeParams(`avg_over_time({app="snuffle-bench",format="logfmt"} | pattern "<_> size=<size> <_>" | unwrap bytes(size) [5m]) by (region)`)},
		{name: "log_json_avg_duration", path: "/loki/api/v1/query_range", params: commonRangeParams(`avg_over_time({app="snuffle-bench",format="json"} | json | unwrap duration [5m]) by (service_name)`)},
		{name: "log_json_filter_status", path: "/loki/api/v1/query_range", params: commonRangeParams(`sum_over_time({app="snuffle-bench",format="json"} | json | status >= 500 | unwrap duration [5m]) by (service_name)`)},
		{name: "log_labels", path: "/loki/api/v1/labels", params: commonMetadataParams()},
		{name: "log_label_values_service", path: "/loki/api/v1/label/service_name/values", params: commonMetadataParams()},
		{name: "log_series_host", path: "/loki/api/v1/series", params: map[string]string{"match[]": `{app="snuffle-bench",host="host-42"}`, "start": start, "end": end}},
	}
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

	scenarios, ok := bridgeScenarios(envString("BRIDGE_BENCH_PROFILE", "posthog_metrics"))
	if !ok {
		b.Fatalf("unknown BRIDGE_BENCH_PROFILE %q", envString("BRIDGE_BENCH_PROFILE", "posthog_metrics"))
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
	case "posthog_metrics":
		return bridgePostHogMetricScenarios(), true
	case "snuffle_metrics":
		return bridgeSnuffleMetricScenarios(), true
	case "posthog_logs":
		return bridgePostHogLogScenarios(), true
	case "snuffle_logs":
		return bridgePostHogLogScenarios(), true
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
