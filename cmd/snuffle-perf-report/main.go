package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type perfResult struct {
	Version      int               `json:"version"`
	GeneratedAt  string            `json:"generated_at"`
	Source       perfSource        `json:"source"`
	Ingest       ingestResult      `json:"ingest"`
	Query        querySummary      `json:"query"`
	Benchmarks   []benchmarkResult `json:"benchmarks"`
	CompareBasis string            `json:"compare_basis"`
}

type perfSource struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	UseCase     string `json:"use_case"`
	Scale       string `json:"scale"`
	Start       string `json:"start"`
	End         string `json:"end"`
	Interval    string `json:"interval"`
	Seed        string `json:"seed"`
	Workers     string `json:"workers"`
	BatchSize   string `json:"batch_size"`
	Concurrency string `json:"query_concurrency"`
	Benchtime   string `json:"query_benchtime"`
}

type ingestResult struct {
	Rows           uint64  `json:"rows"`
	DurationMillis float64 `json:"duration_ms"`
	MetricRate     float64 `json:"metric_rate"`
	RowRate        float64 `json:"row_rate"`
}

type querySummary struct {
	ScenarioCount int     `json:"scenario_count"`
	GeomeanAvgMS  float64 `json:"geomean_avg_ms"`
	TotalAvgMS    float64 `json:"total_avg_ms"`
}

type benchmarkResult struct {
	Name       string             `json:"name"`
	Iterations int                `json:"iterations"`
	AvgMS      float64            `json:"avg_ms"`
	Metrics    map[string]float64 `json:"metrics"`
}

type tsbsLoadResult struct {
	DurationMillis float64 `json:"DurationMillis"`
	Totals         struct {
		MetricRate float64 `json:"metricRate"`
		RowRate    float64 `json:"rowRate"`
	} `json:"Totals"`
}

type comparisonRatio struct {
	Name  string
	Ratio float64
}

var benchmarkLine = regexp.MustCompile(`^(Benchmark\S+)-\d+\s+(\d+)\s+(.*)$`)

func main() {
	resultsPath := flag.String("results", "perf-results.json", "baseline perf results path")
	currentPath := flag.String("current-output", "", "path for the current run json")
	loadPath := flag.String("load", "", "TSBS load results json")
	benchPath := flag.String("bench", "", "go test benchmark output")
	rows := flag.Uint64("rows", 0, "loaded row count")
	tolerance := flag.Float64("tolerance", 0, "allowed slower ratio before reporting a regression")
	failOnSlower := flag.Bool("fail-on-slower", false, "exit non-zero when the current run is slower")

	source := perfSource{}
	flag.StringVar(&source.Name, "source-name", "tsbs", "benchmark data source name")
	flag.StringVar(&source.Version, "source-version", "latest", "benchmark data source version")
	flag.StringVar(&source.UseCase, "source-use-case", "devops", "TSBS use case")
	flag.StringVar(&source.Scale, "source-scale", "", "TSBS scale")
	flag.StringVar(&source.Start, "source-start", "", "TSBS start timestamp")
	flag.StringVar(&source.End, "source-end", "", "TSBS end timestamp")
	flag.StringVar(&source.Interval, "source-interval", "", "TSBS log interval")
	flag.StringVar(&source.Seed, "source-seed", "", "TSBS seed")
	flag.StringVar(&source.Workers, "source-workers", "", "TSBS loader workers")
	flag.StringVar(&source.BatchSize, "source-batch-size", "", "TSBS loader batch size")
	flag.StringVar(&source.Concurrency, "query-concurrency", "", "query benchmark concurrency")
	flag.StringVar(&source.Benchtime, "query-benchtime", "", "query benchmark benchtime")
	flag.Parse()

	if *loadPath == "" || *benchPath == "" {
		fatalf("usage: snuffle-perf-report --load <tsbs-results.json> --bench <go-bench.out>")
	}

	result, err := buildResult(*loadPath, *benchPath, *rows, source)
	if err != nil {
		fatalf("%v", err)
	}
	if *currentPath != "" {
		if err := writeJSON(*currentPath, result); err != nil {
			fatalf("write current results: %v", err)
		}
	}

	previous, err := readPerfResult(*resultsPath)
	if os.IsNotExist(err) {
		if err := writeJSON(*resultsPath, result); err != nil {
			fatalf("write baseline results: %v", err)
		}
		fmt.Printf("perf baseline created: %s\n", *resultsPath)
		return
	}
	if err != nil {
		fatalf("read baseline results: %v", err)
	}

	ratios := compare(previous, result)
	if len(ratios) == 0 {
		if err := writeJSON(*resultsPath, result); err != nil {
			fatalf("write baseline results: %v", err)
		}
		fmt.Printf("perf baseline replaced: no comparable previous scenarios in %s\n", *resultsPath)
		return
	}
	overall := geomeanRatios(ratios)
	if overall <= 1+*tolerance {
		if err := writeJSON(*resultsPath, result); err != nil {
			fatalf("write baseline results: %v", err)
		}
		if overall <= 1 {
			fmt.Printf("perf faster by %.2f%% overall; wrote %s\n", (1-overall)*100, *resultsPath)
		} else {
			fmt.Printf("perf within %.2f%% tolerance; wrote %s\n", *tolerance*100, *resultsPath)
		}
		return
	}

	sort.Slice(ratios, func(i, j int) bool {
		return ratios[i].Ratio > ratios[j].Ratio
	})
	fmt.Printf("perf slower by %.2f%% overall; keeping %s\n", (overall-1)*100, *resultsPath)
	if *currentPath != "" {
		fmt.Printf("current run saved to %s\n", *currentPath)
	}
	limit := 5
	if len(ratios) < limit {
		limit = len(ratios)
	}
	for i := 0; i < limit; i++ {
		ratio := ratios[i]
		if ratio.Ratio <= 1 {
			continue
		}
		fmt.Printf("- %s slower by %.2f%%\n", ratio.Name, (ratio.Ratio-1)*100)
	}
	if *failOnSlower {
		os.Exit(1)
	}
}

func buildResult(loadPath, benchPath string, rows uint64, source perfSource) (perfResult, error) {
	ingest, err := parseLoadResult(loadPath)
	if err != nil {
		return perfResult{}, err
	}
	ingest.Rows = rows
	benchmarks, err := parseBenchmarks(benchPath)
	if err != nil {
		return perfResult{}, err
	}
	if len(benchmarks) == 0 {
		return perfResult{}, fmt.Errorf("no benchmark rows found in %s", benchPath)
	}
	return perfResult{
		Version:      1,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Source:       source,
		Ingest:       ingest,
		Query:        summarizeQueries(benchmarks),
		Benchmarks:   benchmarks,
		CompareBasis: "geomean(write row-rate ratio, per-scenario query avg_ms ratios); lower is faster",
	}, nil
}

func parseLoadResult(path string) (ingestResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ingestResult{}, fmt.Errorf("read TSBS load result: %w", err)
	}
	var loaded tsbsLoadResult
	if err := json.Unmarshal(data, &loaded); err != nil {
		return ingestResult{}, fmt.Errorf("parse TSBS load result: %w", err)
	}
	return ingestResult{
		DurationMillis: loaded.DurationMillis,
		MetricRate:     loaded.Totals.MetricRate,
		RowRate:        loaded.Totals.RowRate,
	}, nil
}

func parseBenchmarks(path string) ([]benchmarkResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read benchmark output: %w", err)
	}
	var results []benchmarkResult
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		matches := benchmarkLine.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		iterations, err := strconv.Atoi(matches[2])
		if err != nil {
			return nil, fmt.Errorf("parse iterations in %q: %w", line, err)
		}
		metrics := parseBenchmarkMetrics(matches[3])
		avgMS := metrics["avg_ms"]
		if avgMS == 0 && metrics["ns/op"] > 0 {
			avgMS = metrics["ns/op"] / 1_000_000
		}
		results = append(results, benchmarkResult{
			Name:       matches[1],
			Iterations: iterations,
			AvgMS:      avgMS,
			Metrics:    metrics,
		})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	return results, nil
}

func parseBenchmarkMetrics(raw string) map[string]float64 {
	fields := strings.Fields(raw)
	metrics := map[string]float64{}
	for i := 0; i+1 < len(fields); i++ {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			continue
		}
		unit := fields[i+1]
		metrics[unit] = value
		i++
	}
	return metrics
}

func summarizeQueries(benchmarks []benchmarkResult) querySummary {
	values := make([]float64, 0, len(benchmarks))
	var total float64
	for _, benchmark := range benchmarks {
		if benchmark.AvgMS <= 0 {
			continue
		}
		values = append(values, benchmark.AvgMS)
		total += benchmark.AvgMS
	}
	return querySummary{
		ScenarioCount: len(values),
		GeomeanAvgMS:  geomean(values),
		TotalAvgMS:    total,
	}
}

func readPerfResult(path string) (perfResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return perfResult{}, err
	}
	var result perfResult
	if err := json.Unmarshal(data, &result); err != nil {
		return perfResult{}, err
	}
	return result, nil
}

func writeJSON(path string, result perfResult) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func compare(previous, current perfResult) []comparisonRatio {
	var ratios []comparisonRatio
	if previous.Ingest.RowRate > 0 && current.Ingest.RowRate > 0 {
		ratios = append(ratios, comparisonRatio{Name: "write_row_rate", Ratio: previous.Ingest.RowRate / current.Ingest.RowRate})
	} else if previous.Ingest.DurationMillis > 0 && current.Ingest.DurationMillis > 0 {
		ratios = append(ratios, comparisonRatio{Name: "write_duration", Ratio: current.Ingest.DurationMillis / previous.Ingest.DurationMillis})
	}

	previousBenchmarks := map[string]benchmarkResult{}
	for _, benchmark := range previous.Benchmarks {
		previousBenchmarks[benchmark.Name] = benchmark
	}
	for _, benchmark := range current.Benchmarks {
		previousBenchmark, ok := previousBenchmarks[benchmark.Name]
		if !ok || previousBenchmark.AvgMS <= 0 || benchmark.AvgMS <= 0 {
			continue
		}
		ratios = append(ratios, comparisonRatio{Name: benchmark.Name, Ratio: benchmark.AvgMS / previousBenchmark.AvgMS})
	}
	return ratios
}

func geomeanRatios(ratios []comparisonRatio) float64 {
	values := make([]float64, 0, len(ratios))
	for _, ratio := range ratios {
		values = append(values, ratio.Ratio)
	}
	return geomean(values)
}

func geomean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	var count int
	for _, value := range values {
		if value <= 0 {
			continue
		}
		sum += math.Log(value)
		count++
	}
	if count == 0 {
		return 0
	}
	return math.Exp(sum / float64(count))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
