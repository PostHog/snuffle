package main

import (
	"math"
	"testing"
)

func TestAutoresearchScoreIncludesIngestAndEachScenario(t *testing.T) {
	result := perfResult{
		Ingest: ingestResult{Rows: 100, RowRate: 250000},
		Memory: &memorySummary{
			ClickHouseQueryCount:        10,
			ClickHouseTotalCPUTimeUS:    80000,
			ClickHouseActiveBytesOnDisk: 3200,
		},
		Benchmarks: []benchmarkResult{
			{Name: "BenchmarkBridgeHTTP/one", AvgMS: 16},
			{Name: "BenchmarkBridgeHTTP/two", AvgMS: 64},
		},
	}

	got, err := autoresearchScore(result)
	if err != nil {
		t.Fatal(err)
	}
	want := math.Pow(4*32*8*16*64, 1.0/5.0)
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("score = %v, want %v", got, want)
	}
}

func TestAutoresearchScoreRequiresPositiveInputs(t *testing.T) {
	for _, result := range []perfResult{
		{Benchmarks: []benchmarkResult{{Name: "BenchmarkBridgeHTTP/one", AvgMS: 16}}},
		{Ingest: ingestResult{RowRate: 250000}},
		{Ingest: ingestResult{RowRate: 250000}, Benchmarks: []benchmarkResult{{Name: "BenchmarkBridgeHTTP/one"}}},
		{Ingest: ingestResult{Rows: 100, RowRate: 250000}, Memory: &memorySummary{ClickHouseQueryCount: 10, ClickHouseTotalCPUTimeUS: 80000}, Benchmarks: []benchmarkResult{{Name: "BenchmarkBridgeHTTP/one", AvgMS: 16}}},
		{Ingest: ingestResult{Rows: 100, RowRate: 250000}, Memory: &memorySummary{ClickHouseActiveBytesOnDisk: 3200}, Benchmarks: []benchmarkResult{{Name: "BenchmarkBridgeHTTP/one", AvgMS: 16}}},
	} {
		if _, err := autoresearchScore(result); err == nil {
			t.Fatalf("autoresearchScore(%+v) succeeded, want error", result)
		}
	}
}
