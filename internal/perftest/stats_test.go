package snuffle

import (
	"math"
	"sort"
	"testing"
)

type benchmarkSummary struct {
	avg float64
	p50 float64
	p95 float64
	p99 float64
	max float64
}

func summarizeBenchmark(sorted []float64) benchmarkSummary {
	if len(sorted) == 0 {
		return benchmarkSummary{}
	}
	var total float64
	for _, value := range sorted {
		total += value
	}
	return benchmarkSummary{
		avg: total / float64(len(sorted)),
		p50: percentile(sorted, 50),
		p95: percentile(sorted, 95),
		p99: percentile(sorted, 99),
		max: sorted[len(sorted)-1],
	}
}

func percentile(sorted []float64, pct float64) float64 {
	idx := int(math.Round((pct / 100.0) * float64(len(sorted)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func reportBenchmarkSummary(b *testing.B, latencies []float64) {
	sort.Float64s(latencies)
	summary := summarizeBenchmark(latencies)
	b.ReportMetric(summary.avg, "avg_ms")
	b.ReportMetric(summary.p50, "p50_ms")
	b.ReportMetric(summary.p95, "p95_ms")
	b.ReportMetric(summary.p99, "p99_ms")
	b.ReportMetric(summary.max, "max_ms")
}
