package snuffle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

func TestPrepareMetricsQLStatistics(test *testing.T) {
	start := time.Unix(1700000010, 0)
	for _, expression := range []string{
		`histogram_quantiles("phi", 0.5, 0.9, bucket)`,
		`histogram_quantiles("phi", 1/2, sum by(le) (rate(bucket[5m])))`,
		`histogram_quantiles("phi", 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, bucket)`,
		`median(value)`,
		`median by (group) (value)`,
		`median(value) without (instance)`,
		`median(value, value + 1) by (group)`,
		`median(1, 3, 7)`,
		`sum(median(value))`,
		`rate(median(value)[5m:1m])`,
		`max_over_time(median(value)[5m:1m])`,
		`median(histogram_quantiles("phi", 0.5, 0.9, bucket))`,
		`WITH (middle(q) = median(q)) middle(value)`,
	} {
		test.Run(expression, func(test *testing.T) {
			prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start.Add(time.Minute))
			if err != nil {
				test.Fatal(err)
			}
			if _, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query); err != nil {
				test.Fatalf("parse %s: %v", prepared.query, err)
			}
		})
	}
	for _, expression := range []string{
		`histogram_quantiles("phi", bucket)`,
		`histogram_quantiles(0.5, 0.9, bucket)`,
		`histogram_quantiles("", 0.5, bucket)`,
		`histogram_quantiles("phi", "0.5", bucket)`,
		`histogram_quantiles("phi", time(), bucket)`,
		`histogram_quantiles("phi", 0.5, bucket) keep_metric_names`,
		`median()`,
		`median("not a vector")`,
		`median(value[5m])`,
		`__snuffle_median("{}", value)`,
		`__snuffle_histogram_quantiles(bucket, "phi", 0.5)`,
	} {
		test.Run("invalid "+expression, func(test *testing.T) {
			prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start)
			if err == nil {
				_, err = parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
			}
			if err == nil {
				test.Fatal("expected an argument error")
			}
		})
	}
	if _, err := prepareMetricsQLQuery(`median(value) by(group) limit 1`, 30*time.Second, 5*time.Minute, start, start.Add(time.Minute)); err == nil {
		test.Fatal("range queries must not apply a different group limit at each step")
	}
}

func statisticsTestQueryable() storage.Queryable {
	var series []*seriesMeta
	add := func(labelMap map[string]string, values []float64) {
		meta := &seriesMeta{labels: labels.FromMap(labelMap), labelMap: labelMap}
		for index, value := range values {
			meta.samples = append(meta.samples, samplePoint{t: 1700000010000 + int64(index)*30000, v: value})
		}
		series = append(series, meta)
	}
	for _, fixture := range []struct {
		group, instance string
		value           float64
	}{
		{"one", "a", 1}, {"one", "b", 2}, {"one", "c", 9},
		{"two", "a", 4}, {"two", "b", 8},
	} {
		add(map[string]string{"__name__": "statistics_value", "group": fixture.group, "instance": fixture.instance}, []float64{fixture.value, fixture.value + 1, fixture.value + 2})
	}
	for _, fixture := range []struct {
		bound string
		count float64
	}{{"1", 2}, {"2", 4}, {"+Inf", 5}} {
		add(map[string]string{"__name__": "statistics_bucket", "le": fixture.bound, "group": "one", "phi": "old"}, []float64{fixture.count, fixture.count * 2, fixture.count * 3})
	}
	nativeLabels := map[string]string{"__name__": "statistics_native", "group": "one"}
	native := &seriesMeta{labels: labels.FromMap(nativeLabels), labelMap: nativeLabels}
	for index := range 3 {
		timestamp := int64(1700000010000 + index*30000)
		point := &histogram.FloatHistogram{Schema: histogram.CustomBucketsSchema, Count: 5, Sum: 7, CustomValues: []float64{1, 2}, PositiveSpans: []histogram.Span{{Offset: 0, Length: 3}}, PositiveBuckets: []float64{2, 2, 1}}
		native.histograms = append(native.histograms, histogramPoint{t: timestamp, h: prompb.FromFloatHistogram(timestamp, point)})
	}
	series = append(series, native)
	return &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
		var selected []storage.Series
		for _, meta := range series {
			if matchesAll(meta.labelMap, matchers) {
				selected = append(selected, meta)
			}
		}
		return &seriesSet{series: selected, idx: -1}
	}}}
}

func evaluateStatistics(test *testing.T, expression string, rangeQuery bool) parser.Value {
	test.Helper()
	start := time.Unix(1700000010, 0)
	end := start.Add(time.Minute)
	if !rangeQuery {
		start = end
	}
	prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, end)
	if err != nil {
		test.Fatal(err)
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: time.Second * 5, LookbackDelta: time.Minute * 5, EnableAtModifier: true, EnableNegativeOffset: true})
	ctx := context.Background()
	options := promql.NewPrometheusQueryOpts(false, 5*time.Minute)
	var query promql.Query
	if rangeQuery {
		query, err = engine.NewRangeQuery(ctx, statisticsTestQueryable(), options, prepared.query, start, end, 30*time.Second)
	} else {
		query, err = engine.NewInstantQuery(ctx, statisticsTestQueryable(), options, prepared.query, end)
	}
	if err != nil {
		test.Fatalf("parse %s: %v", prepared.query, err)
	}
	defer query.Close()
	result := query.Exec(ctx)
	if result.Err != nil {
		test.Fatalf("execute %s: %v", prepared.query, result.Err)
	}
	return metricsQLValue(result.Value)
}

func TestMetricsQLMedianEvaluation(test *testing.T) {
	for _, testcase := range []struct {
		query string
		want  float64
	}{
		{`median(statistics_value)`, 6},
		{`median(1, 3)`, 2},
		{`median(1, 1, 7, 9)`, 4},
		{`median(1, NaN, 3)`, 2},
		{`median(1e308, 1e308)`, 1e308},
		{`median(time(), time() + 2)`, 1700000071},
		{`median(statistics_value{group="one"}, statistics_value{group="one"}, 100)`, 4},
		{`sum(median(statistics_value) by(group))`, 12},
		{`max_over_time(median(statistics_value)[1m:30s])`, 6},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			result := evaluateStatistics(test, testcase.query, false).(promql.Vector)
			if len(result) != 1 || result[0].F != testcase.want {
				test.Fatalf("result = %v, want %v", result, testcase.want)
			}
		})
	}
	for _, grouping := range []string{"by(group)", "without(instance)"} {
		result := evaluateStatistics(test, "median(statistics_value) "+grouping, true).(promql.Matrix)
		if len(result) != 2 {
			test.Fatalf("%s result = %v", grouping, result)
		}
		for _, series := range result {
			first := 2.0
			if series.Metric.Get("group") == "two" {
				first = 6
			}
			if len(series.Floats) != 3 || series.Metric.Len() != 1 {
				test.Fatalf("%s series = %v", grouping, series)
			}
			for index, point := range series.Floats {
				if point.F != first+float64(index) {
					test.Fatalf("%s sample = %v", grouping, point)
				}
			}
		}
	}
	limited := evaluateStatistics(test, "median(statistics_value) by(group) limit 1", false).(promql.Vector)
	if len(limited) != 1 || limited[0].Metric.Get("group") != "one" {
		test.Fatalf("limit result = %v", limited)
	}
	for _, query := range []string{`median(missing_metric)`, `median(NaN)`, `median(statistics_native)`} {
		if result := evaluateStatistics(test, query, false).(promql.Vector); len(result) != 0 {
			test.Fatalf("%s returned %v", query, result)
		}
	}
}

func TestMetricsQLHistogramQuantilesEvaluation(test *testing.T) {
	for _, metric := range []string{"statistics_bucket", "statistics_native"} {
		test.Run(metric, func(test *testing.T) {
			query := fmt.Sprintf(`histogram_quantiles("phi", 0, 0.5, 0.9, 1, %s)`, metric)
			want := map[string]float64{"0": 0, "0.5": 1.25, "0.9": 2, "1": 2}
			if metric == "statistics_native" {
				for quantile := range want {
					single := evaluateStatistics(test, fmt.Sprintf("histogram_quantile(%s, %s)", quantile, metric), false).(promql.Vector)
					if len(single) != 1 {
						test.Fatalf("native reference result = %v", single)
					}
					want[quantile] = single[0].F
				}
			}
			for _, rangeQuery := range []bool{false, true} {
				value := evaluateStatistics(test, query, rangeQuery)
				var series []promql.Series
				if rangeQuery {
					series = []promql.Series(value.(promql.Matrix))
				} else {
					for _, sample := range value.(promql.Vector) {
						series = append(series, promql.Series{Metric: sample.Metric, Floats: []promql.FPoint{{F: sample.F}}})
					}
				}
				if len(series) != len(want) {
					test.Fatalf("result = %v", value)
				}
				for _, sample := range series {
					expected, exists := want[sample.Metric.Get("phi")]
					if !exists || sample.Metric.Get("group") != "one" || sample.Metric.Has("le") || sample.Metric.Has("__name__") {
						test.Fatalf("unexpected labels: %v", sample.Metric)
					}
					for _, point := range sample.Floats {
						if point.F != expected {
							test.Fatalf("%s phi %s = %v, want %v", metric, sample.Metric.Get("phi"), point.F, expected)
						}
					}
				}
			}
		})
	}
	query := `histogram_quantiles("phi", 0.5, missing_bucket)`
	if result := evaluateStatistics(test, query, false).(promql.Vector); len(result) != 0 {
		test.Fatalf("missing histogram result = %v", result)
	}
	prepared, err := prepareMetricsQLQuery(`histogram_quantiles("phi", 0.5, statistics_bucket)`, time.Second, time.Minute, time.Unix(1, 0), time.Unix(1, 0))
	if err != nil || !strings.HasPrefix(prepared.query, "__snuffle_histogram_quantiles(") {
		test.Fatalf("unexpected internal function: %s, %v", prepared.query, err)
	}
}
