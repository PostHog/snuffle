package snuffle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

func compactTestPlan(test testing.TB, query string, start, end time.Time, step time.Duration) (*compactHistogramPlan, string) {
	test.Helper()
	prepared, err := prepareMetricsQLQuery(query, step, 5*time.Minute, start, end)
	if err != nil {
		test.Fatal(err)
	}
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
	if err != nil {
		test.Fatal(err)
	}
	return planCompactHistogram(expr, start, end, step, 5*time.Minute), prepared.query
}

func TestCompactHistogramPlans(test *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, query := range []string{
		`histogram_quantiles("p", 0.5, 0.9, sum by(le)(irate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(irate(duration_bucket[2m])))`,
		`(histogram_quantile(0.5, (sum by(le)(irate(duration_bucket{service_name=~"api.*"})))))`,
	} {
		if plan, _ := compactTestPlan(test, query, start, start.Add(time.Hour), time.Minute); plan == nil {
			test.Fatalf("no plan for %s", query)
		}
	}
	for _, query := range []string{
		`histogram_quantiles("p", 0.5, 0.5, sum by(le)(irate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(rate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le,service_name)(irate(duration_bucket)))`,
		`histogram_quantile(0.5, sum without(service_name)(irate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(irate({__name__=~"duration.*"})))`,
		`histogram_quantile(0.5, sum by(le)(irate(duration_bucket{le="1"})))`,
		`histogram_quantile(0.5, sum by(le)(irate(duration_count)))`,
		`histogram_quantile(0.5, sum by(le)(irate(duration_bucket offset 1h)))`,
		`histogram_quantile(0.5, sum by(le)(irate(duration_bucket @ start())))`,
		`histogram_quantile(0.5, sum by(le)(irate((duration_bucket+1)[1m:10s])))`,
		`sum(histogram_quantile(0.5, sum by(le)(irate(duration_bucket))))`,
	} {
		if plan, _ := compactTestPlan(test, query, start, start.Add(time.Hour), time.Minute); plan != nil {
			test.Fatalf("unsafe plan for %s", query)
		}
	}
	if plan, _ := compactTestPlan(test, `histogram_quantile(0.5,sum by(le)(irate(duration_bucket)))`, start, start.Add(2*time.Hour), time.Millisecond); plan != nil {
		test.Fatal("unbounded grid must fall back")
	}
}

func compareCompactHistogram(test *testing.T, samples []postHogHistogramSample, query string, start, end time.Time, step time.Duration) {
	test.Helper()
	plan, prepared := compactTestPlan(test, query, start, end, step)
	if plan == nil {
		test.Fatal("missing compact plan")
	}
	cfg := histogramTestConfig()
	cfg.MaxSamples, cfg.MaxSeries = 1000000, 10000
	evaluator := newCompactHistogramEvaluator(plan, cfg)
	builder := newPostHogHistogramSeriesBuilder(cfg, plan.selector.LabelMatchers, true)
	for _, sample := range samples {
		if err := evaluator.add(context.Background(), sample); err != nil {
			test.Fatal(err)
		}
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			test.Fatal(err)
		}
	}
	actual, err := evaluator.result(context.Background())
	if err != nil {
		test.Fatal(err)
	}
	queryable := &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(_ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
		var series []storage.Series
		for _, meta := range builder.series {
			series = append(series, meta)
		}
		return &seriesSet{series: series, idx: -1}
	}}}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: cfg.MaxSamples, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableAtModifier: true, EnableNegativeOffset: true})
	engineQuery, err := engine.NewRangeQuery(context.Background(), queryable, promql.NewPrometheusQueryOpts(false, 5*time.Minute), prepared, start, end, step)
	if err != nil {
		test.Fatal(err)
	}
	defer engineQuery.Close()
	response := engineQuery.Exec(context.Background())
	if response.Err != nil {
		test.Fatal(response.Err)
	}
	expected := metricsQLValue(response.Value).(promql.Matrix)
	if len(actual) != len(expected) {
		test.Fatalf("series count: %d != %d", len(actual), len(expected))
	}
	byLabels := make(map[string]promql.Series)
	for _, series := range expected {
		byLabels[series.Metric.String()] = series
	}
	for _, series := range actual {
		want, exists := byLabels[series.Metric.String()]
		if !exists || len(series.Floats) != len(want.Floats) {
			test.Fatalf("points differ for %s: %v != %v", series.Metric, series.Floats, want.Floats)
		}
		for index, point := range series.Floats {
			other := want.Floats[index]
			if point.T != other.T || (point.F != other.F && math.Abs(point.F-other.F) > max(1e-10, math.Abs(other.F)*1e-10)) {
				test.Fatalf("point differs: %v != %v", point, other)
			}
		}
	}
}

func TestCompactHistogramMatchesEngine(test *testing.T) {
	start := time.Unix(1700000000, 0)
	random := rand.New(rand.NewSource(12))
	for iteration := 0; iteration < 30; iteration++ {
		var samples []postHogHistogramSample
		for source := 0; source < 3; source++ {
			timestamp := start.Add(-5 * time.Minute).UnixMilli()
			counts := []uint64{0, 0, 0}
			bounds := []float64{0.5, 1}
			if source == 1 {
				bounds = []float64{-1, 2}
			}
			if source == 2 {
				bounds = []float64{math.Copysign(0, -1), 1}
			}
			for point := 0; point < 100; point++ {
				timestamp += int64(5000 + random.Intn(55000))
				if point%13 == 0 {
					timestamp += 180000
				}
				if timestamp > start.Add(10*time.Minute).UnixMilli() {
					break
				}
				for index := range counts {
					if point%11 == 0 {
						counts[index] = uint64(random.Intn(3))
					} else if point%7 == 0 {
						counts[index] = counts[index] * 15 / 16
					} else {
						counts[index] += uint64(random.Intn(20))
					}
				}
				sample := histogramTestSample()
				sample.id, sample.metricName, sample.timestamp = uint64(source+1), "duration", timestamp
				sample.serviceName = fmt.Sprintf("service-%d", source)
				sample.bounds, sample.counts = bounds, append([]uint64(nil), counts...)
				sample.count = counts[0] + counts[1] + counts[2]
				samples = append(samples, sample)
				if point%17 == 0 {
					samples = append(samples, sample)
				}
			}
		}
		window := ""
		if iteration%2 == 0 {
			window = "[2m]"
		}
		query := `histogram_quantiles("p", 0, 0.5, 0.9, 1, sum by(le)(irate(duration_bucket` + window + `)))`
		compareCompactHistogram(test, samples, query, start, start.Add(10*time.Minute), 30*time.Second)
	}
	compareCompactHistogram(test, nil, `histogram_quantile(0.5,sum by(le)(irate(duration_bucket)))`, start, start.Add(time.Minute), time.Minute)
	var frequent []postHogHistogramSample
	for index := 0; index < 400; index++ {
		sample := histogramTestSample()
		sample.metricName = "duration"
		sample.timestamp = start.Add(-5*time.Minute).UnixMilli() + int64(index)*1000
		sample.counts = []uint64{uint64(index), uint64(index * 2), uint64(index * 3)}
		sample.count = uint64(index * 6)
		frequent = append(frequent, sample)
	}
	for _, label := range []string{"le", "__name__", "service_name"} {
		compareCompactHistogram(test, frequent, `histogram_quantiles("`+label+`",0.5,0.9,sum by(le)(irate(duration_bucket)))`, start, start.Add(time.Minute), 10*time.Second)
	}
}

func TestCompactHistogramFallbackAndLimits(test *testing.T) {
	start := time.Unix(1700000000, 0)
	plan, _ := compactTestPlan(test, `histogram_quantile(0.5,sum by(le)(irate(duration_bucket)))`, start, start.Add(time.Minute), time.Minute)
	for _, change := range []string{"bounds", "signed zero", "delta", "invalid bounds", "invalid counts", "overflow", "order", "duplicate"} {
		test.Run(change, func(test *testing.T) {
			evaluator := newCompactHistogramEvaluator(plan, histogramTestConfig())
			sample := histogramTestSample()
			sample.timestamp = start.UnixMilli()
			if change == "signed zero" {
				sample.bounds = []float64{0, 1}
			}
			if err := evaluator.add(context.Background(), sample); err != nil {
				test.Fatal(err)
			}
			sample.timestamp += 1000
			switch change {
			case "bounds":
				sample.bounds = []float64{0.25, 1}
			case "signed zero":
				sample.bounds = []float64{math.Copysign(0, -1), 1}
			case "delta":
				sample.temporality = "delta"
			case "invalid bounds":
				sample.bounds = []float64{1, 0.5}
			case "invalid counts":
				sample.count = 123
			case "overflow":
				sample.counts = []uint64{math.MaxUint64, 1, 0}
			case "order":
				sample.timestamp -= 2000
			case "duplicate":
				sample.timestamp -= 1000
				sample.counts = []uint64{1, 4, 5}
			}
			if err := evaluator.add(context.Background(), sample); !errors.Is(err, errCompactHistogramFallback) {
				test.Fatalf("wanted fallback, got %v", err)
			}
		})
	}
	for _, limit := range []string{"samples", "series"} {
		cfg := histogramTestConfig()
		if limit == "samples" {
			cfg.MaxSamples = 2
		} else {
			cfg.MaxSeries = 2
		}
		evaluator := newCompactHistogramEvaluator(plan, cfg)
		if err := evaluator.add(context.Background(), histogramTestSample()); err == nil || !strings.Contains(err.Error(), "limit exceeded") {
			test.Fatalf("%s limit: %v", limit, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	evaluator := newCompactHistogramEvaluator(plan, histogramTestConfig())
	if err := evaluator.add(ctx, histogramTestSample()); !errors.Is(err, context.Canceled) {
		test.Fatal(err)
	}
}
