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
	Version         int               `json:"version"`
	Run             string            `json:"run,omitempty"`
	Attempt         int               `json:"attempt,omitempty"`
	RepeatCount     int               `json:"repeat_count,omitempty"`
	SelectedAttempt int               `json:"selected_attempt,omitempty"`
	GeneratedAt     string            `json:"generated_at"`
	Source          perfSource        `json:"source"`
	Ingest          ingestResult      `json:"ingest"`
	Query           querySummary      `json:"query"`
	Benchmarks      []benchmarkResult `json:"benchmarks"`
	CompareBasis    string            `json:"compare_basis"`
}

type perfSuite struct {
	Version      int          `json:"version"`
	GeneratedAt  string       `json:"generated_at"`
	CompareBasis string       `json:"compare_basis"`
	Runs         []perfResult `json:"runs"`
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

type stringList []string

var benchmarkLine = regexp.MustCompile(`^(Benchmark\S+)-\d+\s+(\d+)\s+(.*)$`)

const compareBasis = "geomean(write row-rate ratio, per-scenario query avg_ms ratios); lower is faster"

func main() {
	resultsPath := flag.String("results", "perf-results.json", "baseline perf results path")
	currentPath := flag.String("current-output", "", "path for the current run json")
	loadPath := flag.String("load", "", "load results json")
	benchPath := flag.String("bench", "", "go test benchmark output")
	runName := flag.String("run-name", "", "named suite run to update inside the results file")
	rows := flag.Uint64("rows", 0, "loaded row count")
	attempt := flag.Int("attempt", 0, "1-based suite attempt number for candidate results")
	repeatCount := flag.Int("repeat-count", 0, "total suite attempt count for candidate results")
	tolerance := flag.Float64("tolerance", 0, "allowed slower ratio before reporting a regression")
	failOnSlower := flag.Bool("fail-on-slower", false, "exit non-zero when the current run is slower")
	buildOnly := flag.Bool("build-only", false, "write current-output without reading or updating the baseline")
	var candidatePaths stringList
	flag.Var(&candidatePaths, "candidate", "candidate result json path; repeat for multiple candidates")
	candidatesCSV := flag.String("candidates", "", "comma-separated candidate result json paths")

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

	if strings.TrimSpace(*runName) == "" {
		fatalf("--run-name is required")
	}
	if *candidatesCSV != "" {
		for _, path := range strings.Split(*candidatesCSV, ",") {
			if path = strings.TrimSpace(path); path != "" {
				candidatePaths = append(candidatePaths, path)
			}
		}
	}

	if len(candidatePaths) > 0 {
		selected, err := selectSlowestCandidate(candidatePaths, strings.TrimSpace(*runName))
		if err != nil {
			fatalf("%v", err)
		}
		if *repeatCount > 0 {
			selected.RepeatCount = *repeatCount
		} else {
			selected.RepeatCount = len(candidatePaths)
		}
		selected.SelectedAttempt = selected.Attempt
		if *currentPath != "" {
			if err := writeJSON(*currentPath, selected); err != nil {
				fatalf("write selected current results: %v", err)
			}
		}
		updateSuiteResult(*resultsPath, *currentPath, selected, *tolerance, *failOnSlower)
		return
	}

	if *loadPath == "" || *benchPath == "" {
		fatalf("usage: snuffle-perf-report --load <load-results.json> --bench <go-bench.out>")
	}

	result, err := buildResult(*loadPath, *benchPath, *rows, source)
	if err != nil {
		fatalf("%v", err)
	}
	result.Run = strings.TrimSpace(*runName)
	result.Attempt = *attempt
	result.RepeatCount = *repeatCount
	if *currentPath != "" {
		if err := writeJSON(*currentPath, result); err != nil {
			fatalf("write current results: %v", err)
		}
	}
	if *buildOnly {
		if *currentPath == "" {
			fatalf("--current-output is required with --build-only")
		}
		return
	}
	updateSuiteResult(*resultsPath, *currentPath, result, *tolerance, *failOnSlower)
}

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value != "" {
		*s = append(*s, value)
	}
	return nil
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
		CompareBasis: compareBasis,
	}, nil
}

func parseLoadResult(path string) (ingestResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ingestResult{}, fmt.Errorf("read load result: %w", err)
	}
	var loaded tsbsLoadResult
	if err := json.Unmarshal(data, &loaded); err != nil {
		return ingestResult{}, fmt.Errorf("parse load result: %w", err)
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

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func selectSlowestCandidate(paths []string, runName string) (perfResult, error) {
	if len(paths) == 0 {
		return perfResult{}, fmt.Errorf("no candidate result files provided")
	}
	var selected perfResult
	for i, path := range paths {
		candidate, err := readPerfResult(path)
		if err != nil {
			return perfResult{}, fmt.Errorf("read candidate %s: %w", path, err)
		}
		candidate.Run = strings.TrimSpace(candidate.Run)
		if candidate.Run == "" {
			return perfResult{}, fmt.Errorf("candidate %s has no run name", path)
		}
		if candidate.Run != runName {
			return perfResult{}, fmt.Errorf("candidate %s has run %q, want %q", path, candidate.Run, runName)
		}
		if i == 0 || isSlower(selected, candidate) {
			selected = candidate
		}
	}
	return selected, nil
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
	if result.Version != 1 {
		return perfResult{}, fmt.Errorf("candidate must be result version 1")
	}
	return result, nil
}

func isSlower(currentSlowest, candidate perfResult) bool {
	ratios := compare(currentSlowest, candidate)
	if len(ratios) == 0 {
		return fallbackSlowScore(candidate) > fallbackSlowScore(currentSlowest)
	}
	return geomeanRatios(ratios) > 1
}

func updateSuiteResult(resultsPath, currentPath string, current perfResult, tolerance float64, failOnSlower bool) {
	runs, err := readPerfSuiteRuns(resultsPath)
	if os.IsNotExist(err) {
		if failOnSlower {
			fatalf("missing perf baseline file %s", resultsPath)
		}
		runs = map[string]perfResult{}
	} else if err != nil {
		fatalf("read baseline results: %v", err)
	}

	previous, havePrevious := runs[current.Run]
	if !havePrevious {
		if failOnSlower {
			fatalf("missing perf baseline for run %s in %s", current.Run, resultsPath)
		}
		runs[current.Run] = current
		if err := writePerfSuite(resultsPath, runs); err != nil {
			fatalf("write baseline results: %v", err)
		}
		fmt.Printf("perf baseline created for %s in %s\n", current.Run, resultsPath)
		return
	}

	ratios := compare(previous, current)
	if len(ratios) == 0 {
		runs[current.Run] = current
		if err := writePerfSuite(resultsPath, runs); err != nil {
			fatalf("write baseline results: %v", err)
		}
		fmt.Printf("perf baseline replaced for %s: no comparable previous scenarios in %s\n", current.Run, resultsPath)
		return
	}

	overall := geomeanRatios(ratios)
	if overall <= 1+tolerance {
		runs[current.Run] = current
		if err := writePerfSuite(resultsPath, runs); err != nil {
			fatalf("write baseline results: %v", err)
		}
		if overall <= 1 {
			fmt.Printf("perf %s faster by %.2f%% overall; wrote %s\n", current.Run, (1-overall)*100, resultsPath)
		} else {
			fmt.Printf("perf %s within %.2f%% tolerance; wrote %s\n", current.Run, tolerance*100, resultsPath)
		}
		return
	}

	sort.Slice(ratios, func(i, j int) bool {
		return ratios[i].Ratio > ratios[j].Ratio
	})
	fmt.Printf("perf %s slower by %.2f%% overall; keeping %s\n", current.Run, (overall-1)*100, resultsPath)
	if currentPath != "" {
		fmt.Printf("current run saved to %s\n", currentPath)
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
	if failOnSlower {
		os.Exit(1)
	}
}

func readPerfSuiteRuns(path string) (map[string]perfResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var suite perfSuite
	if err := json.Unmarshal(data, &suite); err != nil {
		return nil, err
	}
	if suite.Version != 2 || suite.Runs == nil {
		return nil, fmt.Errorf("perf baseline must be suite version 2")
	}
	runs := make(map[string]perfResult, len(suite.Runs))
	for _, run := range suite.Runs {
		name := strings.TrimSpace(run.Run)
		if name == "" {
			return nil, fmt.Errorf("perf baseline contains a run without a name")
		}
		run.Run = name
		runs[name] = run
	}
	return runs, nil
}

func writePerfSuite(path string, runs map[string]perfResult) error {
	names := make([]string, 0, len(runs))
	for name := range runs {
		names = append(names, name)
	}
	sort.Strings(names)
	ordered := make([]perfResult, 0, len(names))
	for _, name := range names {
		run := runs[name]
		if run.Run == "" {
			run.Run = name
		}
		ordered = append(ordered, run)
	}
	return writeJSON(path, perfSuite{
		Version:      2,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		CompareBasis: compareBasis,
		Runs:         ordered,
	})
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

func fallbackSlowScore(result perfResult) float64 {
	values := make([]float64, 0, 2)
	if result.Ingest.RowRate > 0 {
		values = append(values, 1/result.Ingest.RowRate)
	} else if result.Ingest.DurationMillis > 0 {
		values = append(values, result.Ingest.DurationMillis)
	}
	if result.Query.GeomeanAvgMS > 0 {
		values = append(values, result.Query.GeomeanAvgMS)
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
