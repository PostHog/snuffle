package snuffle

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// deltaWindowAt is the evaluation time of the instant queries in these tests.
const deltaWindowAt = int64(1_700_003_600_000)

// deltaWindowPoint is a delta histogram point secondsAgo seconds before
// deltaWindowAt. Its observations fall into the buckets in counts.
func deltaWindowPoint(id uint64, name string, secondsAgo int64, counts ...uint64) postHogHistogramSample {
	sample := deltaHistogramTestSample(id, deltaWindowAt-secondsAgo*1000, counts...)
	sample.metricName = name
	return sample
}

// deltaWindowFixtures returns one histogram per scenario. Each name gets its
// own source ids so that the sources do not share series.
func deltaWindowFixtures() []postHogHistogramSample {
	var samples []postHogHistogramSample
	// No point within the lookback before the window, and two points 4s apart
	// in it. A point 2h earlier makes the running total large.
	samples = append(samples,
		deltaWindowPoint(1, "sparse_seconds", 7200, 50, 0, 0),
		deltaWindowPoint(1, "sparse_seconds", 100, 1, 0, 0),
		deltaWindowPoint(1, "sparse_seconds", 96, 1, 0, 0),
	)
	samples = append(samples, deltaWindowPoint(2, "single_seconds", 60, 1, 0, 0))
	for secondsAgo := int64(3600); secondsAgo >= 0; secondsAgo -= 10 {
		samples = append(samples, deltaWindowPoint(3, "dense_seconds", secondsAgo, 1, 0, 0))
	}
	// Two clients with the same labels report at the same time.
	samples = append(samples,
		deltaWindowPoint(4, "shared_seconds", 30, 1, 0, 0),
		deltaWindowPoint(5, "shared_seconds", 30, 2, 0, 0),
	)
	// The point before the window is within the lookback. The le="0.5" bound
	// disappears 2m before deltaWindowAt and comes back 1m before it. Its
	// bucket series therefore has a stale marker in the window.
	withoutHalf := deltaWindowPoint(6, "stale_seconds", 120, 1, 1)
	withoutHalf.bounds = []float64{1}
	samples = append(samples,
		deltaWindowPoint(6, "stale_seconds", 480, 5, 0, 0),
		withoutHalf,
		deltaWindowPoint(6, "stale_seconds", 60, 2, 0, 0),
	)
	// The only point is in the lookback before the window.
	samples = append(samples, deltaWindowPoint(7, "old_seconds", 480, 3, 0, 0))
	// One point in each of the last two 5m steps.
	samples = append(samples,
		deltaWindowPoint(8, "steps_seconds", 540, 1, 0, 0),
		deltaWindowPoint(8, "steps_seconds", 240, 1, 0, 0),
		deltaWindowPoint(8, "steps_seconds", 239, 1, 0, 0),
	)
	return samples
}

// deltaWindowQueryable serves the series that the histogram builder makes
// from samples, plus extra series. It returns a futureSeriesSet, as
// CHQuerier.Select does. These tests therefore fail if the engine stops
// keeping the set on the selector.
func deltaWindowQueryable(test *testing.T, samples []postHogHistogramSample, extra ...*seriesMeta) storage.Queryable {
	test.Helper()
	cfg := histogramTestConfig()
	cfg.MaxSamples = 100000
	builder := newPostHogHistogramSeriesBuilder(cfg, nil, true)
	for _, sample := range samples {
		if err := builder.add(sample, postHogHistogramSuffixes); err != nil {
			test.Fatal(err)
		}
	}
	series := append(builder.finish(), extra...)
	return &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
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
}

// deltaWindowInstant returns the single value of an instant query at
// deltaWindowAt, or false when the query returns no series.
func deltaWindowInstant(test *testing.T, queryable storage.Queryable, expression string) (float64, bool) {
	test.Helper()
	at := time.UnixMilli(deltaWindowAt)
	vector := evaluateMetricsQLBetween(test, queryable, expression, at, at, 5*time.Minute).(promql.Vector)
	switch len(vector) {
	case 0:
		return 0, false
	case 1:
		return vector[0].F, true
	}
	test.Fatalf("%s returned %d series, want at most one", expression, len(vector))
	return 0, false
}

func assertDeltaWindowValues(test *testing.T, queryable storage.Queryable, cases []deltaWindowCase) {
	test.Helper()
	for _, testcase := range cases {
		test.Run(testcase.query, func(test *testing.T) {
			got, ok := deltaWindowInstant(test, queryable, testcase.query)
			if ok != testcase.present || (ok && math.Abs(got-testcase.want) > 1e-12) {
				test.Fatalf("result = %v (present %v), want %v (present %v)", got, ok, testcase.want, testcase.present)
			}
		})
	}
}

type deltaWindowCase struct {
	query   string
	want    float64
	present bool
}

func TestDeltaHistogramCounterWindowSum(test *testing.T) {
	queryable := deltaWindowQueryable(test, deltaWindowFixtures())
	assertDeltaWindowValues(test, queryable, []deltaWindowCase{
		// Both points in the window count. The point 2h earlier does not.
		{`increase(sparse_seconds_count[5m])`, 2, true},
		{`increase_pure(sparse_seconds_count[5m])`, 2, true},
		{`delta(sparse_seconds_count[5m])`, 2, true},
		{`rate(sparse_seconds_count[5m])`, 2.0 / 300, true},
		{`increase(sparse_seconds_bucket{le="0.5"}[5m])`, 2, true},
		{`increase(sparse_seconds_sum[5m])`, 1, true},
		{`increase(sparse_seconds_count[5m]) keep_metric_names`, 2, true},
		{`sum(rate(sparse_seconds_count[5m]))`, 2.0 / 300, true},
		// Without a window, the window is the 5m step of the instant query.
		{`rate(sparse_seconds_count)`, 2.0 / 300, true},
		{`increase(sparse_seconds_count)`, 2, true},
		{`increase(sparse_seconds_count[3h])`, 52, true},
		// A window with one point keeps the series.
		{`increase(single_seconds_count[5m])`, 1, true},
		{`rate(single_seconds_count[5m])`, 1.0 / 300, true},
		// The window (t-5m, t] holds 30 of the points.
		{`increase(dense_seconds_count[5m])`, 30, true},
		{`rate(dense_seconds_count[5m])`, 0.1, true},
		{`increase(dense_seconds_count[1h])`, 360, true},
		{`increase(shared_seconds_count[5m])`, 3, true},
		{`rate(shared_seconds_count[5m])`, 3.0 / 300, true},
		// The stale marker at 2m does not count. The point at 8m is before
		// the window.
		{`increase(stale_seconds_bucket{le="0.5"}[5m])`, 2, true},
		{`increase(stale_seconds_bucket{le="0.5"}[90s])`, 2, true},
		{`increase(stale_seconds_bucket{le="0.5"}[100s] offset 90s)`, 0, false},
		{`increase(stale_seconds_count[5m])`, 4, true},
		// A window without points returns no series.
		{`increase(old_seconds_count[5m])`, 0, false},
		{`rate(old_seconds_count[5m])`, 0, false},
		{`increase(old_seconds_count[10m])`, 3, true},
		{`increase(steps_seconds_count[5m] offset 5m)`, 1, true},
		{`rate(steps_seconds_count[5m] offset 5m)`, 1.0 / 300, true},
		// irate and idelta read the last two points of the running total.
		{`idelta(sparse_seconds_count[5m])`, 1, true},
		{`irate(sparse_seconds_count[5m])`, 0.25, true},
	})
}

func TestDeltaHistogramCounterWindowSumRange(test *testing.T) {
	queryable := deltaWindowQueryable(test, deltaWindowFixtures())
	end := time.UnixMilli(deltaWindowAt)
	start := end.Add(-10 * time.Minute)
	for _, testcase := range []struct {
		query string
		want  []promql.FPoint
	}{
		// The step 10m before end has no point in its matrix.
		{`increase(steps_seconds_count[5m])`, []promql.FPoint{{T: deltaWindowAt - 300000, F: 1}, {T: deltaWindowAt, F: 2}}},
		{`rate(steps_seconds_count[5m])`, []promql.FPoint{{T: deltaWindowAt - 300000, F: 1.0 / 300}, {T: deltaWindowAt, F: 2.0 / 300}}},
		{`increase(sparse_seconds_count[5m])`, []promql.FPoint{{T: deltaWindowAt, F: 2}}},
		{`rate(dense_seconds_count[5m])`, []promql.FPoint{{T: deltaWindowAt - 600000, F: 0.1}, {T: deltaWindowAt - 300000, F: 0.1}, {T: deltaWindowAt, F: 0.1}}},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			matrix := evaluateMetricsQLBetween(test, queryable, testcase.query, start, end, 5*time.Minute).(promql.Matrix)
			if len(matrix) != 1 || len(matrix[0].Floats) != len(testcase.want) {
				test.Fatalf("result = %v, want one series with %v", matrix, testcase.want)
			}
			for index, point := range matrix[0].Floats {
				want := testcase.want[index]
				if point.T != want.T || math.Abs(point.F-want.F) > 1e-12 {
					test.Fatalf("point %d = %v, want %v", index, point, want)
				}
			}
		})
	}
}

// One client reports often with fast requests. Another reports two slow
// requests 4s apart, with no point shortly before the window. The quantile
// matches the quantile of the pooled bucket counts.
func TestDeltaHistogramQuantilePoolsSparseSeries(test *testing.T) {
	var samples []postHogHistogramSample
	for secondsAgo := int64(3600); secondsAgo >= 0; secondsAgo -= 10 {
		fast := deltaWindowPoint(1, "pooled_seconds", secondsAgo, 1, 0, 0)
		fast.attributes = map[string]string{"client": "fast"}
		samples = append(samples, fast)
	}
	for _, secondsAgo := range []int64{100, 96} {
		slow := deltaWindowPoint(2, "pooled_seconds", secondsAgo, 0, 1, 0)
		slow.attributes = map[string]string{"client": "slow"}
		samples = append(samples, slow)
	}
	queryable := deltaWindowQueryable(test, samples)
	// The pooled window counts are 30 at le="0.5" and 32 at le="1" and
	// le="+Inf". The median is observation 16, in the first bucket.
	want := 0.5 * 16 / 30
	for _, query := range []string{
		`histogram_quantile(0.5, sum by (le) (rate(pooled_seconds_bucket[5m])))`,
		`histogram_quantile(0.5, sum by (le) (increase(pooled_seconds_bucket[5m])))`,
	} {
		test.Run(query, func(test *testing.T) {
			got, ok := deltaWindowInstant(test, queryable, query)
			if !ok || math.Abs(got-want) > 1e-12 {
				test.Fatalf("result = %v (present %v), want %v", got, ok, want)
			}
		})
	}
}

// Cumulative histograms and ordinary counters keep MetricsQL counter
// semantics, including in a selector that also reads a delta histogram.
func TestCumulativeCountersIgnoreDeltaWindowSum(test *testing.T) {
	cumulative := func(secondsAgo int64, count uint64) postHogHistogramSample {
		sample := deltaWindowPoint(50, "cumulative_seconds", secondsAgo, count, 0, 0)
		sample.temporality = "cumulative"
		return sample
	}
	labelMap := map[string]string{labels.MetricName: "plain_requests_total"}
	plain := &seriesMeta{labels: labels.FromMap(labelMap), labelMap: labelMap, samples: []samplePoint{{t: deltaWindowAt - 100000, v: 100}, {t: deltaWindowAt - 96000, v: 101}}}
	samples := append(deltaWindowFixtures(), cumulative(100, 100), cumulative(96, 101))
	queryable := deltaWindowQueryable(test, samples, plain)
	assertDeltaWindowValues(test, queryable, []deltaWindowCase{
		// The first point in the window is the baseline. rate divides by the
		// time between the first and last points.
		{`increase(cumulative_seconds_count[5m])`, 1, true},
		{`rate(cumulative_seconds_count[5m])`, 0.25, true},
		{`increase(cumulative_seconds_bucket{le="0.5"}[5m])`, 1, true},
		{`increase(plain_requests_total[5m])`, 1, true},
		{`rate(plain_requests_total[5m])`, 0.25, true},
		{`sum(increase({__name__=~"plain_requests_total|sparse_seconds_count"}[5m]))`, 3, true},
		{`sum(rate({__name__=~"cumulative_seconds_count|sparse_seconds_count"}[5m]))`, 0.25 + 2.0/300, true},
	})
}

func TestDeltaWindowSum(test *testing.T) {
	cfg := histogramTestConfig()
	builder := newPostHogHistogramSeriesBuilder(cfg, nil, true)
	nan := deltaHistogramTestSample(1, 3000, 1, 1, 1)
	nan.sum = math.NaN()
	for _, sample := range []postHogHistogramSample{
		deltaHistogramTestSample(1, 1000, 1, 1, 2),
		deltaHistogramTestSample(2, 1000, 1, 0, 0),
		nan,
		deltaHistogramTestSample(1, 5000, 2, 0, 0),
	} {
		if err := builder.add(sample, []string{"_count", "_sum"}); err != nil {
			test.Fatal(err)
		}
	}
	series := make(map[string]*seriesMeta)
	for _, meta := range builder.finish() {
		series[meta.metricName] = meta
	}
	for _, testcase := range []struct {
		name       string
		start, end int64
		want       float64
		ok         bool
	}{
		{"test_duration_seconds_count", 0, 1000, 5, true},
		{"test_duration_seconds_count", 1000, 5000, 5, true},
		{"test_duration_seconds_count", 0, 5000, 10, true},
		{"test_duration_seconds_count", 999, 1000, 5, true},
		{"test_duration_seconds_count", 1000, 2999, 0, false},
		{"test_duration_seconds_count", 5000, 9000, 0, false},
		{"test_duration_seconds_sum", 2000, 3000, 0, false},
		{"test_duration_seconds_sum", 0, 9000, 3.5, true},
	} {
		got, ok := series[testcase.name].deltaWindowSum(testcase.start, testcase.end)
		if ok != testcase.ok || got != testcase.want {
			test.Fatalf("%s (%d, %d] = %v (ok %v), want %v (ok %v)", testcase.name, testcase.start, testcase.end, got, ok, testcase.want, testcase.ok)
		}
	}
}
