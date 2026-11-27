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

// The fingerprint list can be a literal of thousands of ids, so the summed
// SQL must name it once for every rollup, or a query that the streaming read
// answers exceeds the ClickHouse max_query_size.
func TestCompactHistogramSQLNamesFingerprintsOnce(test *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, query := range []string{
		`histogram_quantile(0.5, sum by(le)(rate(duration_bucket[5m])))`,
		`histogram_quantile(0.5, sum by(le)(increase(duration_bucket[5m])))`,
		`histogram_quantile(0.5, sum by(le)(delta(duration_bucket[5m])))`,
	} {
		plan, _ := compactTestPlan(test, query, start, start.Add(time.Hour), time.Minute)
		sql := compactHistogramSQL(ConfigFromEnv(), plan, plan.start-plan.selectorRange+1, "FINGERPRINT_LIST")
		if count := strings.Count(sql, "FINGERPRINT_LIST"); count != 1 {
			test.Errorf("%s: the fingerprint list appears %d times", query, count)
		}
	}
}

func TestCompactHistogramPlans(test *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, query := range []string{
		`histogram_quantiles("p", 0.5, 0.9, sum by(le)(irate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(irate(duration_bucket[2m])))`,
		`(histogram_quantile(0.5, (sum by(le)(irate(duration_bucket{service_name=~"api.*"})))))`,
		`histogram_quantile(0.5, sum by(le)(rate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(rate(duration_bucket[5m])))`,
		`histogram_quantiles("p", 0.5, 0.9, sum by(le)(increase(duration_bucket[1m])))`,
		`histogram_quantile(0.5, sum by(le)(delta(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(idelta(duration_bucket[2m])))`,
	} {
		if plan, _ := compactTestPlan(test, query, start, start.Add(time.Hour), time.Minute); plan == nil {
			test.Fatalf("no plan for %s", query)
		}
	}
	for _, query := range []string{
		`histogram_quantiles("p", 0.5, 0.5, sum by(le)(irate(duration_bucket)))`,
		`histogram_quantile(0.5, sum by(le)(avg_over_time(duration_bucket[1m])))`,
		`histogram_quantile(0.5, sum by(le)(duration_bucket))`,
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

// compactHistogramMatrix evaluates query with the compact evaluator.
func compactHistogramMatrix(test *testing.T, cfg Config, samples []postHogHistogramSample, query string, start, end time.Time, step time.Duration) (promql.Matrix, error) {
	test.Helper()
	prepared, err := prepareMetricsQLQuery(query, step, 5*time.Minute, start, end)
	if err != nil {
		test.Fatal(err)
	}
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
	if err != nil {
		test.Fatal(err)
	}
	branches := planCompactHistogramUnion(expr, start, end, step, 5*time.Minute)
	if branches == nil {
		test.Fatal("missing compact plan")
	}
	evaluators := make(map[string]*compactHistogramEvaluator)
	var union compactHistogramUnion
	for _, branch := range branches {
		key := branch.plan.call.Args[branch.plan.bucketArg].String()
		evaluator, exists := evaluators[key]
		if !exists {
			evaluator = newCompactHistogramEvaluator(branch.plan, cfg)
			for _, sample := range samples {
				if err := evaluator.add(context.Background(), sample); err != nil {
					return nil, err
				}
			}
			if err := evaluator.advance(context.Background(), 0, true); err != nil {
				return nil, err
			}
			evaluators[key] = evaluator
		}
		matrix, err := evaluator.evaluate(context.Background(), branch.plan)
		if err == nil {
			matrix, err = applyCompactSeriesFunctions(matrix, branch.functions)
		}
		if err != nil {
			return nil, err
		}
		union.add(matrix)
	}
	return union.matrix(), nil
}

// engineHistogramMatrix evaluates query with the general path: the histogram
// builder makes the bucket series, and the engine evaluates the query over
// them. The queryable returns a futureSeriesSet, as CHQuerier.Select does, so
// counter rollups read the window sums of delta series.
func engineHistogramMatrix(test *testing.T, cfg Config, samples []postHogHistogramSample, query string, start, end time.Time, step time.Duration) (promql.Matrix, error) {
	test.Helper()
	prepared, err := prepareMetricsQLQuery(query, step, 5*time.Minute, start, end)
	if err != nil {
		test.Fatal(err)
	}
	builder := newPostHogHistogramSeriesBuilder(cfg, nil, true)
	for _, sample := range samples {
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			return nil, err
		}
	}
	series := builder.finish()
	queryable := &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
		var selected []*seriesMeta
		for _, meta := range series {
			if matchesAll(meta.labelMap, matchers) {
				selected = append(selected, meta)
			}
		}
		future := &selectFuture{done: make(chan struct{}), series: selected, delta: newDeltaSeriesIndex(selected)}
		close(future.done)
		return &futureSeriesSet{future: future}
	}}}
	engine := promql.NewEngine(promql.EngineOpts{Parser: metricsQLEngineParser{newBoundLabelParser()}, MaxSamples: cfg.MaxSamples, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableAtModifier: true, EnableNegativeOffset: true})
	engineQuery, err := engine.NewRangeQuery(context.Background(), queryable, promql.NewPrometheusQueryOpts(false, 5*time.Minute), prepared.query, start, end, step)
	if err != nil {
		test.Fatal(err)
	}
	defer engineQuery.Close()
	response := engineQuery.Exec(context.Background())
	if response.Err != nil {
		return nil, response.Err
	}
	return metricsQLValue(response.Value).(promql.Matrix), nil
}

func assertSameHistogramMatrix(test *testing.T, actual, expected promql.Matrix) {
	test.Helper()
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

func compareCompactHistogram(test *testing.T, samples []postHogHistogramSample, query string, start, end time.Time, step time.Duration) {
	test.Helper()
	cfg := histogramTestConfig()
	cfg.MaxSamples, cfg.MaxSeries = 1000000, 10000
	actual, err := compactHistogramMatrix(test, cfg, samples, query, start, end, step)
	if err != nil {
		test.Fatal(err)
	}
	expected, err := engineHistogramMatrix(test, cfg, samples, query, start, end, step)
	if err != nil {
		test.Fatal(err)
	}
	assertSameHistogramMatrix(test, actual, expected)
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
		function := metricsQLCounterFunctions[iteration%len(metricsQLCounterFunctions)]
		query := `histogram_quantiles("p", 0, 0.5, 0.9, 1, sum by(le)(` + function + `(duration_bucket` + window + `)))`
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
	for _, function := range metricsQLCounterFunctions {
		compareCompactHistogram(test, frequent, `histogram_quantile(0.9,sum by(le)(`+function+`(duration_bucket)))`, start, start.Add(time.Minute), 10*time.Second)
		compareCompactHistogram(test, frequent, `histogram_quantile(0.9,sum by(le)(`+function+`(duration_bucket[45s])))`, start, start.Add(time.Minute), 10*time.Second)
	}
	quantile := func(q, function string) string {
		return `histogram_quantile(` + q + `, sum by (le) (` + function + `(duration_bucket)))`
	}
	labelled := func(q, function, clause string) string {
		return `label_replace(` + quantile(q, function) + `, "clause", "` + clause + `", "", "")`
	}
	for _, query := range []string{
		labelled("0.5", "rate", "a") + ` or ` + labelled("0.9", "rate", "b") + ` or ` + labelled("0.95", "rate", "c") + ` or ` + labelled("0.99", "rate", "d"),
		// Different buckets read separately; the same labels keep the first branch.
		labelled("0.5", "rate", "a") + ` or ` + labelled("0.9", "irate", "b") + ` or ` + labelled("0.99", "rate", "a"),
		// Without distinct labels, `or` keeps only the first quantile.
		quantile("0.5", "rate") + ` or ` + quantile("0.9", "rate"),
		`label_replace(label_replace(` + quantile("0.5", "increase") + `, "q", "p$1", "clause", "(.*)"), "q", "", "q", "p")`,
		`label_replace(` + quantile("0.5", "rate") + `, "q", "x", "missing", "y") or ` + labelled("0.9", "rate", "b"),
		`label_join(` + quantile("0.5", "rate") + `, "q", "-", "clause", "missing")`,
		`round(clamp(abs(ln(` + quantile("0.9", "rate") + `)), 0.5, 1.5), 0.1) or sgn(` + labelled("0.5", "rate", "b") + `)`,
		`clamp(` + quantile("0.9", "rate") + `, 2, 1) or ` + labelled("0.5", "rate", "b"),
		`clamp_min(sqrt(` + quantile("0.9", "rate") + `), 0.8) or clamp_max(exp(` + labelled("0.5", "rate", "b") + `), 1.2)`,
		`floor(log2(` + quantile("0.9", "rate") + `)) or ceil(log10(` + labelled("0.5", "rate", "b") + `))`,
		`label_set(` + quantile("0.5", "rate") + `, "q", "p50", "service_name", "")`,
		`label_copy(` + labelled("0.5", "rate", "a") + `, "clause", "q", "missing", "clause")`,
		`label_move(` + labelled("0.5", "rate", "a") + `, "clause", "q", "q", "q")`,
		`label_transform(` + labelled("0.5", "rate", "a-b") + `, "clause", "-", "_")`,
		`label_transform(` + labelled("0.5", "rate", "a") + `, "clause", "a", "")`,
		`union(label_set(` + quantile("0.5", "rate") + `, "q", "p50"), label_set(` + quantile("0.9", "irate") + `, "q", "p90"), "ignored")`,
		`(label_set(` + quantile("0.5", "rate") + `, "q", "a"), label_set(` + quantile("0.9", "rate") + `, "q", "a")) or ` + labelled("0.99", "rate", "c"),
		`union(` + quantile("0.5", "rate") + `, ` + quantile("0.9", "rate") + `)`,
	} {
		compareCompactHistogram(test, frequent, query, start, start.Add(time.Minute), 10*time.Second)
		compareCompactHistogram(test, nil, query, start, start.Add(time.Minute), 10*time.Second)
	}
}

// deltaHistogramTestSamples returns delta histogram points of three sources
// with irregular intervals, gaps, and points that share a timestamp, such as
// points from several clients.
func deltaHistogramTestSamples(random *rand.Rand, from, until int64) []postHogHistogramSample {
	var samples []postHogHistogramSample
	for source := 0; source < 3; source++ {
		timestamp := from
		bounds := []float64{0.5, 1}
		if source == 1 {
			bounds = []float64{-1, 2}
		}
		for point := 0; ; point++ {
			timestamp += int64(5000 + random.Intn(55000))
			if point%13 == 0 {
				timestamp += 180000
			}
			// A gap longer than the lookback leaves a window whose baseline
			// point is outside the selector range.
			if point%17 == 5 {
				timestamp += 420000
			}
			if timestamp > until {
				break
			}
			for copies := 1 + point%3/2; copies > 0; copies-- {
				counts := []uint64{uint64(random.Intn(5)), uint64(random.Intn(20)), uint64(random.Intn(3))}
				sample := histogramTestSample()
				sample.id, sample.metricName, sample.timestamp, sample.temporality = uint64(source+1), "duration", timestamp, "delta"
				sample.serviceName = fmt.Sprintf("service-%d", source)
				sample.bounds, sample.counts = bounds, counts
				sample.count = counts[0] + counts[1] + counts[2]
				samples = append(samples, sample)
			}
		}
	}
	return samples
}

// TestCompactHistogramDeltaMatchesEngine checks that the compact path gives
// the general path's result over delta histograms: rate, increase and delta
// read the sum of the delta points in each window.
func TestCompactHistogramDeltaMatchesEngine(test *testing.T) {
	start := time.Unix(1700000000, 0)
	random := rand.New(rand.NewSource(7))
	for iteration := 0; iteration < 30; iteration++ {
		samples := deltaHistogramTestSamples(random, start.Add(-10*time.Minute).UnixMilli(), start.Add(20*time.Minute).UnixMilli())
		window := ""
		if iteration%2 == 0 {
			window = "[2m]"
		}
		function := []string{"rate", "increase", "delta"}[iteration%3]
		query := `histogram_quantiles("p", 0, 0.5, 0.95, 1, sum by(le)(` + function + `(duration_bucket` + window + `)))`
		compareCompactHistogram(test, samples, query, start, start.Add(20*time.Minute), 30*time.Second)
	}
	samples := deltaHistogramTestSamples(random, start.Add(-10*time.Minute).UnixMilli(), start.Add(20*time.Minute).UnixMilli())
	for _, query := range []string{
		`histogram_quantile(0.95, sum by (le) (rate({__name__="duration_bucket"}[5m])))`,
		`histogram_quantile(0.5, sum by (le) (increase(duration_bucket[45s])))`,
		`label_replace(histogram_quantile(0.5, sum by (le) (rate(duration_bucket))), "q", "a", "", "") or label_replace(histogram_quantile(0.9, sum by (le) (rate(duration_bucket))), "q", "b", "", "")`,
	} {
		compareCompactHistogram(test, samples, query, start, start.Add(20*time.Minute), time.Minute)
	}
}

// TestCompactHistogramDeltaLimitCountsHeldSamples checks that a long range of
// a delta histogram reads more samples than the limit as long as it holds
// fewer, while the general path, which keeps every point it reads, fails. The
// compact result matches the general path with a limit that fits every point.
func TestCompactHistogramDeltaLimitCountsHeldSamples(test *testing.T) {
	start := time.Unix(1700000000, 0)
	end := start.Add(6 * time.Hour)
	query := `histogram_quantile(0.95, sum by (le) (rate({__name__="duration_bucket"}[5m])))`
	var samples []postHogHistogramSample
	for source := 0; source < 2; source++ {
		for timestamp := start.Add(-5 * time.Minute).UnixMilli(); timestamp <= end.UnixMilli(); timestamp += 10000 {
			sample := histogramTestSample()
			sample.id, sample.metricName, sample.timestamp, sample.temporality = uint64(source+1), "duration", timestamp, "delta"
			sample.serviceName = fmt.Sprintf("service-%d", source)
			sample.counts = []uint64{uint64(timestamp/10000%7) + 1, uint64(timestamp/10000%5) * 3, uint64(source)}
			sample.count = sample.counts[0] + sample.counts[1] + sample.counts[2]
			samples = append(samples, sample)
		}
	}
	// Two sources of about 2,200 points and 3 buckets read about 13,000
	// samples. The 73-step grid holds 219 and the 10-minute selector range at
	// most 61 points, or 183 samples.
	limited := histogramTestConfig()
	limited.MaxSamples, limited.MaxSeries = 2000, 10000
	if _, err := engineHistogramMatrix(test, limited, samples, query, start, end, 5*time.Minute); err == nil || !strings.Contains(err.Error(), "histogram sample limit exceeded") {
		test.Fatalf("general path: wanted sample limit error, got %v", err)
	}
	actual, err := compactHistogramMatrix(test, limited, samples, query, start, end, 5*time.Minute)
	if err != nil {
		test.Fatalf("compact path: %v", err)
	}
	if len(actual) != 1 || len(actual[0].Floats) != 73 {
		test.Fatalf("compact path returned %v", actual)
	}
	generous := histogramTestConfig()
	generous.MaxSamples, generous.MaxSeries = 1000000, 10000
	expected, err := engineHistogramMatrix(test, generous, samples, query, start, end, 5*time.Minute)
	if err != nil {
		test.Fatal(err)
	}
	assertSameHistogramMatrix(test, actual, expected)
	tight := limited
	tight.MaxSamples = 300
	if _, err := compactHistogramMatrix(test, tight, samples, query, start, end, 5*time.Minute); err == nil || !strings.Contains(err.Error(), "sample limit exceeded") {
		test.Fatalf("compact path: wanted sample limit error, got %v", err)
	}
}

// NaN is no value in MetricsQL: it hides no later operand.
func TestCompactHistogramUnionSkipsNaN(test *testing.T) {
	var union compactHistogramUnion
	union.add(promql.Matrix{{Metric: labels.FromStrings("job", "a"), Floats: []promql.FPoint{{T: 1, F: math.NaN()}, {T: 2, F: 1}}}})
	union.add(promql.Matrix{
		{Metric: labels.FromStrings("__name__", "x", "job", "a"), Floats: []promql.FPoint{{T: 1, F: 5}, {T: 3, F: 6}}},
		{Metric: labels.FromStrings("job", "a"), Floats: []promql.FPoint{{T: 0, F: 7}}},
	})
	want := promql.Matrix{
		{Metric: labels.FromStrings("__name__", "x", "job", "a"), Floats: []promql.FPoint{{T: 1, F: 5}, {T: 3, F: 6}}},
		{Metric: labels.FromStrings("job", "a"), Floats: []promql.FPoint{{T: 0, F: 7}, {T: 2, F: 1}}},
	}
	if got := union.matrix(); got.String() != want.String() {
		test.Fatalf("union: %v != %v", got, want)
	}
}

func TestCompactHistogramUnionPlans(test *testing.T) {
	start := time.Unix(1700000000, 0)
	plan := func(query string) []compactHistogramBranch {
		prepared, err := prepareMetricsQLQuery(query, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
		if err != nil {
			test.Fatal(err)
		}
		expr, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
		if err != nil {
			test.Fatal(err)
		}
		return planCompactHistogramUnion(expr, start, start.Add(time.Hour), time.Minute, 5*time.Minute)
	}
	quantile := `histogram_quantile(0.5, sum by(le)(rate(duration_bucket)))`
	if branches := plan(`label_replace(` + quantile + `, "clause", "a", "", "") or (label_replace(` + quantile + `, "clause", "b", "", ""))`); len(branches) != 2 || len(branches[1].functions) != 1 {
		test.Fatalf("wanted two labelled branches, got %v", branches)
	}
	for _, query := range []string{
		quantile + ` or on(service_name) ` + quantile,
		quantile + ` or ignoring(service_name) ` + quantile,
		quantile + ` and ` + quantile,
		quantile + ` or sum(rate(duration_count))`,
		`label_replace(` + quantile + `, "__name__", "x", "", "")`,
		`label_replace(` + quantile + `, "clause", "a", "", "(")`,
		`label_join(` + quantile + `, "__name__", "-", "le")`,
		`round(` + quantile + `, scalar(up))`,
		`timestamp(` + quantile + `)`,
		`label_set(` + quantile + `, "__name__", "x")`,
		`label_copy(` + quantile + `, "__name__", "x")`,
		`label_move(` + quantile + `, "x", "__name__")`,
		`label_transform(` + quantile + `, "__name__", "a", "b")`,
		`drop_common_labels(` + quantile + `)`,
		`union(` + quantile + `, sum(rate(duration_count)))`,
		`union(histogram_quantiles("__name__", 0.5, 0.9, sum by(le)(rate(duration_bucket))), ` + quantile + `)`,
		`label_set(histogram_quantiles("__name__", 0.5, 0.9, sum by(le)(rate(duration_bucket))), "q", "x")`,
	} {
		if branches := plan(query); branches != nil {
			test.Fatalf("unsafe plan for %s", query)
		}
	}
}

func TestCompactHistogramFallbackAndLimits(test *testing.T) {
	start := time.Unix(1700000000, 0)
	plan, _ := compactTestPlan(test, `histogram_quantile(0.5,sum by(le)(irate(duration_bucket)))`, start, start.Add(time.Minute), time.Minute)
	for _, change := range []string{"bounds", "signed zero", "delta", "unknown temporality", "invalid bounds", "invalid counts", "overflow", "order", "duplicate"} {
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
				// A source that mixes temporalities has no running total.
				sample.temporality = "delta"
			case "unknown temporality":
				sample.temporality = "unspecified"
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
	// A delta source falls back when it mixes temporalities, which the general
	// path refuses, and for irate and idelta, which read the running total
	// from the first point that the request reads.
	ratePlan, _ := compactTestPlan(test, `histogram_quantile(0.5,sum by(le)(rate(duration_bucket)))`, start, start.Add(time.Minute), time.Minute)
	for _, testcase := range []struct {
		name  string
		plan  *compactHistogramPlan
		first string
		then  string
	}{
		{"cumulative then delta", ratePlan, "cumulative", "delta"},
		{"delta then cumulative", ratePlan, "delta", "cumulative"},
		{"delta irate", plan, "delta", ""},
	} {
		test.Run(testcase.name, func(test *testing.T) {
			evaluator := newCompactHistogramEvaluator(testcase.plan, histogramTestConfig())
			sample := histogramTestSample()
			sample.timestamp, sample.temporality = start.UnixMilli(), testcase.first
			err := evaluator.add(context.Background(), sample)
			if testcase.then != "" {
				if err != nil {
					test.Fatal(err)
				}
				sample.timestamp += 1000
				sample.temporality = testcase.then
				err = evaluator.add(context.Background(), sample)
			}
			if !errors.Is(err, errCompactHistogramFallback) {
				test.Fatalf("wanted fallback, got %v", err)
			}
		})
	}
	for _, limit := range []string{"samples", "series"} {
		cfg := histogramTestConfig()
		if limit == "samples" {
			// The grid holds 2 steps of 3 buckets, which leaves 2 samples for
			// the 3 buckets of the first point.
			cfg.MaxSamples = 8
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

// TestCompactHistogramLimitCountsHeldSamples checks that a long range reads
// more samples than the limit when it holds fewer at once: one source series
// keeps only the points inside its selector range.
func TestCompactHistogramLimitCountsHeldSamples(test *testing.T) {
	start := time.Unix(1700000000, 0)
	plan, _ := compactTestPlan(test, `histogram_quantile(0.5,sum by(le)(rate(duration_bucket[5m])))`, start, start.Add(time.Hour), time.Minute)
	if plan == nil {
		test.Fatal("no compact plan")
	}
	read := func(maxSamples int) error {
		cfg := histogramTestConfig()
		cfg.MaxSamples = maxSamples
		evaluator := newCompactHistogramEvaluator(plan, cfg)
		sample := histogramTestSample()
		for index := 0; index <= 280; index++ {
			sample.timestamp = start.Add(-10*time.Minute + time.Duration(index)*15*time.Second).UnixMilli()
			sample.counts = []uint64{uint64(index), uint64(index), uint64(index)}
			sample.count = uint64(3 * index)
			if err := evaluator.add(context.Background(), sample); err != nil {
				return err
			}
		}
		_, err := evaluator.result(context.Background())
		return err
	}
	// 281 points of 3 buckets read 843 samples. The 61-step grid holds 183
	// and the 10-minute selector range at most 42 points, or 126 samples.
	if err := read(400); err != nil {
		test.Fatalf("read 843 samples holding at most 309: %v", err)
	}
	if err := read(250); err == nil || !strings.Contains(err.Error(), "sample limit exceeded") {
		test.Fatalf("wanted sample limit error, got %v", err)
	}
}

// TestCompactHistogramReleasesNarrowerSourceHistory checks that a source with
// fewer buckets does not keep the previous source's points reachable, since
// the limit counts only the current source's history.
func TestCompactHistogramReleasesNarrowerSourceHistory(test *testing.T) {
	start := time.Unix(1700000000, 0)
	plan, _ := compactTestPlan(test, `histogram_quantile(0.5,sum by(le)(rate(duration_bucket[5m])))`, start, start.Add(time.Hour), time.Minute)
	if plan == nil {
		test.Fatal("no compact plan")
	}
	evaluator := newCompactHistogramEvaluator(plan, histogramTestConfig())
	wide := histogramTestSample()
	wide.bounds, wide.counts, wide.count = []float64{0.25, 0.5, 1}, []uint64{1, 2, 3, 4}, 10
	wide.timestamp = start.UnixMilli()
	if err := evaluator.add(context.Background(), wide); err != nil {
		test.Fatal(err)
	}
	narrow := histogramTestSample()
	narrow.id = 2
	narrow.timestamp = start.UnixMilli()
	if err := evaluator.add(context.Background(), narrow); err != nil {
		test.Fatal(err)
	}
	for index, points := range evaluator.history[len(narrow.counts):cap(evaluator.history)] {
		if points != nil {
			test.Fatalf("history entry %d past the current source still retains capacity for %d points", len(narrow.counts)+index, cap(points))
		}
	}
}
