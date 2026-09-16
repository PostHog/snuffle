package snuffle

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func histogramTestConfig() Config {
	return Config{SchemaLayout: "posthog", CHDatabase: "test", SamplesTable: "metrics2", SeriesTable: "metric_series3", MaxSeries: 100, MaxSamples: 1000, TeamID: 42}
}

func histogramTestSample() postHogHistogramSample {
	return postHogHistogramSample{id: 1, metricName: "test_duration_seconds", timestamp: 1000, sum: 12, count: 10, bounds: []float64{0.5, 1}, counts: []uint64{2, 3, 5}, temporality: "cumulative", serviceName: "api", attributes: map[string]string{"region": "metric"}, resource: map[string]string{"region": "resource"}}
}

func TestPostHogHistogramBuckets(test *testing.T) {
	sample := histogramTestSample()
	buckets, err := postHogHistogramBuckets(sample)
	if err != nil {
		test.Fatal(err)
	}
	if want := map[string]float64{"0.5": 2, "1": 5, "+Inf": 10}; !reflect.DeepEqual(buckets, want) {
		test.Fatalf("buckets = %v, want %v", buckets, want)
	}
	sample.bounds = nil
	sample.counts = []uint64{10}
	buckets, err = postHogHistogramBuckets(sample)
	if err != nil || !reflect.DeepEqual(buckets, map[string]float64{"+Inf": 10}) {
		test.Fatalf("unbounded histogram = %v, %v", buckets, err)
	}
	for _, testcase := range []struct {
		name   string
		bounds []float64
		counts []uint64
		count  uint64
	}{
		{"length", []float64{1}, []uint64{1}, 1},
		{"unsorted", []float64{2, 1}, []uint64{1, 1, 1}, 3},
		{"duplicate", []float64{1, 1}, []uint64{1, 1, 1}, 3},
		{"nan", []float64{math.NaN()}, []uint64{1, 1}, 2},
		{"infinite", []float64{math.Inf(1)}, []uint64{1, 1}, 2},
		{"count mismatch", []float64{1}, []uint64{1, 1}, 3},
		{"overflow", []float64{1}, []uint64{math.MaxUint64, 1}, 0},
	} {
		test.Run(testcase.name, func(test *testing.T) {
			sample.bounds, sample.counts, sample.count = testcase.bounds, testcase.counts, testcase.count
			if _, err := postHogHistogramBuckets(sample); err == nil {
				test.Fatal("expected invalid histogram error")
			}
		})
	}
}

func TestPostHogHistogramSeries(test *testing.T) {
	sample := histogramTestSample()
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
	if err := builder.add(sample, postHogHistogramSuffixes); err != nil {
		test.Fatal(err)
	}
	if len(builder.series) != 5 {
		test.Fatalf("got %d series, want 5", len(builder.series))
	}
	for _, meta := range builder.series {
		if meta.labelMap["region"] != "resource" || meta.labelMap["service_name"] != "api" {
			test.Fatalf("labels lost: %v", meta.labelMap)
		}
		if meta.samples[0].t != sample.timestamp {
			test.Fatal("timestamp changed")
		}
		if meta.metricName == sample.metricName+"_sum" && meta.samples[0].v != 12 {
			test.Fatal("sum is not the stored sum")
		}
		if meta.metricName == sample.metricName+"_count" && meta.samples[0].v != 10 {
			test.Fatal("count is not the observation count")
		}
	}
	sample.timestamp = 2000
	sample.bounds = []float64{1}
	sample.counts = []uint64{6, 4}
	if err := builder.add(sample, []string{"_bucket"}); err != nil {
		test.Fatal(err)
	}
	for _, meta := range builder.series {
		if meta.labelMap["le"] == "0.5" && (len(meta.samples) != 2 || !isStaleSampleValue(meta.samples[1].v)) {
			test.Fatal("removed bucket did not become stale")
		}
	}
}

func TestPostHogHistogramMatchers(test *testing.T) {
	for _, matcher := range []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "le", "1"),
		labels.MustNewMatcher(labels.MatchNotEqual, "le", "0.5"),
		labels.MustNewMatcher(labels.MatchRegexp, "le", "1|\\+Inf"),
		labels.MustNewMatcher(labels.MatchNotRegexp, "le", "0.*"),
	} {
		builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), []*labels.Matcher{matcher}, true)
		sample := histogramTestSample()
		sample.resource["le"] = "original"
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			test.Fatal(err)
		}
		for _, meta := range builder.series {
			if !matcher.Matches(meta.labelMap["le"]) || meta.labelMap["le"] == "original" {
				test.Fatalf("incorrect bucket matcher: %v", meta.labelMap)
			}
		}
	}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_duration_seconds_sum")}
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), matchers, true)
	if err := builder.add(histogramTestSample(), postHogHistogramSuffixes); err != nil || len(builder.series) != 1 {
		test.Fatalf("metric name matcher: %d series, %v", len(builder.series), err)
	}
}

func TestPostHogHistogramTemporalityAndLimits(test *testing.T) {
	for _, temporality := range []string{"delta", "unspecified", ""} {
		sample := histogramTestSample()
		sample.temporality = temporality
		builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
		if err := builder.add(sample, []string{"_count"}); err == nil || !strings.Contains(err.Error(), "require cumulative") {
			test.Fatalf("unsupported temporality %q: %v", temporality, err)
		}
		builder = newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, false)
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			test.Fatalf("discovery must still work: %v", err)
		}
	}
	for _, limit := range []string{"series", "samples"} {
		cfg := histogramTestConfig()
		if limit == "series" {
			cfg.MaxSeries = 2
		} else {
			cfg.MaxSamples = 2
		}
		builder := newPostHogHistogramSeriesBuilder(cfg, nil, true)
		if err := builder.add(histogramTestSample(), []string{"_bucket"}); err == nil {
			test.Fatalf("%s limit not enforced", limit)
		}
	}
}

func TestPostHogHistogramSQL(test *testing.T) {
	cfg := histogramTestConfig()
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_duration_seconds_bucket"),
		labels.MustNewMatcher(labels.MatchEqual, "service_name", "api"),
		labels.MustNewMatcher(labels.MatchEqual, "le", "1"),
	}
	sql := postHogHistogramAliasesSQL(cfg, 1000, 2000, matchers)
	for _, want := range []string{"metric_name = 'test_duration_seconds'", "suffix = '_bucket'", "metric_type IN ('histogram', 'exponential_histogram')", "service_name = 'api'", "NOT IN (SELECT metric_name FROM `test`.`metric_series3` WHERE team_id = 42 AND metric_name = 'test_duration_seconds_bucket')"} {
		if !strings.Contains(sql, want) {
			test.Fatalf("SQL missing %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, "['le']") {
		test.Fatalf("virtual le must not filter source attributes: %s", sql)
	}
	aliases := []postHogHistogramAlias{{baseName: "test_duration_seconds", suffix: "_bucket"}}
	sql = postHogHistogramSamplesSQL(cfg, 1000, 2000, aliases, matchers)
	for _, want := range []string{"histogram_bounds", "histogram_counts", "aggregation_temporality", "team_id = 42", "fromUnixTimestamp64Milli(1000", "fromUnixTimestamp64Milli(2000", "INNER JOIN selected_series", "ORDER BY series_id, timestamp"} {
		if !strings.Contains(sql, want) {
			test.Fatalf("samples SQL missing %q: %s", want, sql)
		}
	}
	for _, name := range []string{"test_duration_seconds_bucket", "test_duration_seconds_count", "test_duration_seconds_sum"} {
		if !postHogMaySelectHistogram([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}) {
			test.Fatalf("virtual selector %q not detected", name)
		}
	}
	if postHogMaySelectHistogram([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "requests_total")}) {
		test.Fatal("ordinary exact metric must keep its existing query path")
	}
}
