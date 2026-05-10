package snuffle

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

const (
	defaultPromTSDBSeries  = 100_000
	defaultPromTSDBSamples = 100
	promTSDBStartMS        = int64(1_700_100_000_000)
	promTSDBStepMS         = int64(15_000)
)

type promScenario struct {
	name  string
	query string
	start time.Time
	end   time.Time
	step  time.Duration
}

func BenchmarkPrometheusTSDB(b *testing.B) {
	if !envBool("PROM_TSDB_BENCH") {
		b.Skip("set PROM_TSDB_BENCH=1 to run local Prometheus TSDB benchmarks")
	}

	dir := envString("PROM_TSDB_BENCH_DIR", filepath.Join(os.TempDir(), "snuffle-prometheus-tsdb"))
	seriesCount := envInt("PROM_TSDB_BENCH_SERIES", defaultPromTSDBSeries, 1)
	samplesPerSeries := envInt("PROM_TSDB_BENCH_SAMPLES_PER_SERIES", defaultPromTSDBSamples, 2)
	if envBool("PROM_TSDB_BENCH_RESEED") {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}

	db, err := openPromDB(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if !promSeedMarkerExists(dir, seriesCount, samplesPerSeries) {
		started := time.Now()
		if err := seedPromDB(db, seriesCount, samplesPerSeries); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(promSeedMarker(dir, seriesCount, samplesPerSeries), []byte("ok\n"), 0o644); err != nil {
			b.Fatal(err)
		}
		b.Logf("seeded %d series / %d samples in %s", seriesCount, seriesCount*samplesPerSeries, time.Since(started).Round(time.Millisecond))
	}

	engine := promEngine()
	concurrency := envInt("PROM_TSDB_BENCH_CONCURRENCY", 1, 1)
	warmup := envInt("PROM_TSDB_BENCH_WARMUP", 10, 0)
	scenarios := filterPromScenarios(promScenarios(), envCSV("PROM_TSDB_BENCH_SCENARIO"))
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			benchmarkPromScenario(b, engine, db, sc, warmup, concurrency)
		})
	}
}

func benchmarkPromScenario(b *testing.B, engine *promql.Engine, db *tsdb.DB, sc promScenario, warmup, concurrency int) {
	ctx := context.Background()
	for i := 0; i < warmup; i++ {
		if _, err := executePromScenario(ctx, engine, db, sc); err != nil {
			b.Fatalf("warmup failed: %v", err)
		}
	}

	latencies := make([]float64, 0, b.N)
	var resultCount int
	var firstError error
	b.ReportAllocs()
	b.ResetTimer()
	if concurrency == 1 {
		for i := 0; i < b.N; i++ {
			started := time.Now()
			count, err := executePromScenario(ctx, engine, db, sc)
			latencies = append(latencies, time.Since(started).Seconds()*1000)
			resultCount = count
			if firstError == nil {
				firstError = err
			}
		}
	} else {
		results := make(chan promRequestResult, b.N)
		jobs := make(chan struct{}, b.N)
		var wg sync.WaitGroup
		for worker := 0; worker < concurrency; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range jobs {
					started := time.Now()
					count, err := executePromScenario(ctx, engine, db, sc)
					results <- promRequestResult{latencyMS: time.Since(started).Seconds() * 1000, results: count, err: err}
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
			latencies = append(latencies, result.latencyMS)
			resultCount = result.results
			if firstError == nil {
				firstError = result.err
			}
		}
	}
	b.StopTimer()

	if firstError != nil {
		b.Fatalf("query failed: %v", firstError)
	}
	reportBenchmarkSummary(b, latencies)
	b.ReportMetric(float64(resultCount), "results/op")
}

func openPromDB(dir string) (*tsdb.DB, error) {
	opts := tsdb.DefaultOptions()
	opts.NoLockfile = true
	opts.RetentionDuration = int64(30 * 24 * time.Hour / time.Millisecond)
	opts.MinBlockDuration = int64(2 * time.Hour / time.Millisecond)
	opts.MaxBlockDuration = int64(2 * time.Hour / time.Millisecond)
	return tsdb.Open(dir, slog.Default(), nil, opts, nil)
}

func promSeedMarker(dir string, seriesCount, samplesPerSeries int) string {
	return filepath.Join(dir, fmt.Sprintf(".seeded-%d-%d", seriesCount, samplesPerSeries))
}

func promSeedMarkerExists(dir string, seriesCount, samplesPerSeries int) bool {
	_, err := os.Stat(promSeedMarker(dir, seriesCount, samplesPerSeries))
	return err == nil
}

func seedPromDB(db *tsdb.DB, seriesCount, samplesPerSeries int) error {
	ctx := context.Background()
	app := db.Appender(ctx)
	for seriesID := 0; seriesID < seriesCount; seriesID++ {
		status := "200"
		if seriesID%5 == 0 {
			status = "500"
		}
		lbls := labels.FromStrings(
			labels.MetricName, "load_requests_total",
			"job", fmt.Sprintf("svc-%d", seriesID%100),
			"instance", fmt.Sprintf("host-%d", seriesID),
			"status", status,
			"shard", fmt.Sprintf("%d", seriesID%1000),
		)
		var ref storage.SeriesRef
		for sampleIndex := 0; sampleIndex < samplesPerSeries; sampleIndex++ {
			ts := promTSDBStartMS + promTSDBStepMS*int64(sampleIndex)
			value := float64(1000 + seriesID*100 + sampleIndex*(1+seriesID%10))
			nextRef, err := app.Append(ref, lbls, ts, value)
			if err != nil {
				return err
			}
			ref = nextRef
		}
		if seriesID > 0 && seriesID%10_000 == 0 {
			if err := app.Commit(); err != nil {
				return err
			}
			app = db.Appender(ctx)
		}
	}
	return app.Commit()
}

func promEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:               slog.Default(),
		MaxSamples:           50_000_000,
		Timeout:              30 * time.Second,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			return int64(time.Minute / time.Millisecond)
		},
	})
}

func promScenarios() []promScenario {
	eval := time.UnixMilli(1_700_100_135_000)
	return []promScenario{
		{name: "large_one_series", query: `load_requests_total{instance="host-4242"}`, start: eval, end: eval},
		{name: "large_job_rate", query: `rate(load_requests_total{job="svc-42",status="200"}[30s])`, start: eval, end: eval},
		{name: "large_sum_rate", query: `sum by (status) (rate(load_requests_total{job="svc-42"}[30s]))`, start: eval, end: eval},
		{name: "large_broad_sum", query: `sum by (job) (load_requests_total)`, start: eval, end: eval},
		{name: "large_topk", query: `topk(5, load_requests_total{status="200"})`, start: eval, end: eval},
		{name: "large_range_rate", query: `rate(load_requests_total{job="svc-42",status="200"}[30s])`, start: time.UnixMilli(1_700_100_090_000), end: eval, step: 15 * time.Second},
		{name: "large_series_20k", query: `series:load_requests_total{status="500"}`, start: time.UnixMilli(1_700_100_000_000), end: eval},
	}
}

func filterPromScenarios(scenarios []promScenario, selected map[string]struct{}) []promScenario {
	if len(selected) == 0 {
		return scenarios
	}
	filtered := make([]promScenario, 0, len(scenarios))
	for _, sc := range scenarios {
		if _, ok := selected[sc.name]; ok {
			filtered = append(filtered, sc)
		}
	}
	return filtered
}

func executePromScenario(ctx context.Context, engine *promql.Engine, db *tsdb.DB, sc promScenario) (int, error) {
	if selector, ok := strings.CutPrefix(sc.query, "series:"); ok {
		return executePromSeries(ctx, db, selector, sc.start, sc.end)
	}
	var q promql.Query
	var err error
	if sc.step > 0 {
		q, err = engine.NewRangeQuery(ctx, db, promql.NewPrometheusQueryOpts(false, 5*time.Minute), sc.query, sc.start, sc.end, sc.step)
	} else {
		q, err = engine.NewInstantQuery(ctx, db, promql.NewPrometheusQueryOpts(false, 5*time.Minute), sc.query, sc.start)
	}
	if err != nil {
		return 0, err
	}
	defer q.Close()
	res := q.Exec(ctx)
	if res.Err != nil {
		return 0, res.Err
	}
	return promValueLen(res.Value), nil
}

func executePromSeries(ctx context.Context, db *tsdb.DB, selector string, start, end time.Time) (int, error) {
	matchers, err := parser.NewParser(parser.Options{}).ParseMetricSelector(selector)
	if err != nil {
		return 0, err
	}
	querier, err := db.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return 0, err
	}
	defer querier.Close()
	set := querier.Select(ctx, true, nil, matchers...)
	count := 0
	for set.Next() {
		_ = set.At().Labels()
		count++
	}
	return count, set.Err()
}

func promValueLen(value parser.Value) int {
	switch v := value.(type) {
	case promql.Vector:
		return len(v)
	case promql.Matrix:
		return len(v)
	default:
		return 1
	}
}

type promRequestResult struct {
	latencyMS float64
	results   int
	err       error
}
