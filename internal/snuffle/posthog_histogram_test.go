package snuffle

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	promvalue "github.com/prometheus/prometheus/model/value"
)

func histogramTestConfig() Config {
	return Config{SchemaLayout: "posthog", CHDatabase: "test", SamplesTable: "metrics4_samples", SeriesTable: "metrics4_series", MaxSeries: 100, MaxSamples: 1000, TeamID: 42}
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
		sample.timestamp += 1000
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			test.Fatal(err)
		}
		for _, meta := range builder.series {
			if !matcher.Matches(meta.labelMap["le"]) || meta.labelMap["le"] == "original" {
				test.Fatalf("incorrect bucket matcher: %v", meta.labelMap)
			}
			if len(meta.samples) != 2 {
				test.Fatalf("matched bucket lost a sample: %v", meta.samples)
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
	exact := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_duration_seconds_count")}
	for _, temporality := range []string{"delta", "unspecified", ""} {
		sample := histogramTestSample()
		sample.temporality = temporality
		builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), exact, true)
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
	for _, want := range []string{"metric_name = 'test_duration_seconds'", "suffix = '_bucket'", "metric_type IN ('histogram', 'exponential_histogram')", "service_name = 'api'"} {
		if !strings.Contains(sql, want) {
			test.Fatalf("SQL missing %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, "['le']") {
		test.Fatalf("virtual le must not filter source attributes: %s", sql)
	}
	if strings.Contains(sql, "NOT IN") {
		test.Fatalf("stored component series must not suppress virtual names: %s", sql)
	}
	aliases := []postHogHistogramAlias{{baseName: "test_duration_seconds", suffix: "_bucket"}}
	sql = postHogHistogramSeriesSQL(cfg, 1000, 2000, aliases, matchers)
	for _, want := range []string{"`test`.`metrics4_series`", "resource_attributes, attributes", "metric_type IN ('histogram', 'exponential_histogram')", "service_name = 'api'", "test_duration_seconds", "time_bucket >= toStartOfHour(fromUnixTimestamp64Milli(1000", "LIMIT 1 BY series_id"} {
		if !strings.Contains(sql, want) {
			test.Fatalf("series SQL missing %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, "'le'") {
		test.Fatalf("virtual le must not filter source attributes: %s", sql)
	}
	sql = postHogHistogramSamplesSQL(cfg, 1000, 2000, []uint64{7}, aliases, matchers)
	for _, want := range []string{"histogram_bounds", "histogram_counts", "aggregation_temporality", "team_id = 42", "fromUnixTimestamp64Milli(1000", "fromUnixTimestamp64Milli(2000", "series_fingerprint IN (7)", "ORDER BY series_id, timestamp"} {
		if !strings.Contains(sql, want) {
			test.Fatalf("samples SQL missing %q: %s", want, sql)
		}
	}
	for _, notWant := range []string{"resource_attributes", "INNER JOIN", "selected_series", "metrics4_series"} {
		if strings.Contains(sql, notWant) {
			test.Fatalf("samples SQL must not carry labels via %q: %s", notWant, sql)
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

func TestPostHogHistogramRepeatedSamples(test *testing.T) {
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
	sample := histogramTestSample()
	if err := builder.add(sample, postHogHistogramSuffixes); err != nil {
		test.Fatal(err)
	}
	initial := append([]*seriesMeta(nil), builder.series...)
	sample.timestamp = 2000
	sample.sum = 3
	sample.count = 4
	sample.counts = []uint64{1, 1, 2}
	if err := builder.add(sample, postHogHistogramSuffixes); err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(initial, builder.series) || builder.samples != 10 {
		test.Fatalf("series changed or samples missing: %d series, %d samples", len(builder.series), builder.samples)
	}
	for _, meta := range builder.series {
		want := float64(3)
		if strings.HasSuffix(meta.metricName, "_count") {
			want = 4
		} else if strings.HasSuffix(meta.metricName, "_bucket") {
			want = map[string]float64{"0.5": 1, "1": 2, "+Inf": 4}[meta.labelMap["le"]]
		}
		if len(meta.samples) != 2 || meta.samples[1] != (samplePoint{t: 2000, v: want}) {
			test.Fatalf("incorrect repeated sample for %s: %v", meta.labels, meta.samples)
		}
		if meta.labelMap["region"] != "resource" || meta.labelMap["service_name"] != "api" {
			test.Fatalf("labels changed: %v", meta.labelMap)
		}
	}
}

func TestPostHogHistogramBoundSetsMergeByLabels(test *testing.T) {
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
	complete := histogramTestSample()
	complete.id = 1
	complete.timestamp = 3000
	complete.sum, complete.count, complete.counts = 36, 30, []uint64{6, 9, 15}
	partial := histogramTestSample()
	partial.id = 2
	partial.timestamp = 2000
	partial.sum, partial.count, partial.bounds, partial.counts = 24, 20, []float64{0.5}, []uint64{4, 16}
	first := histogramTestSample()
	first.id = 1
	for _, sample := range []postHogHistogramSample{complete, first, partial} {
		if err := builder.add(sample, postHogHistogramSuffixes); err != nil {
			test.Fatal(err)
		}
	}
	for _, meta := range builder.series {
		sortSamples(meta.samples)
	}
	got := make(map[string][]samplePoint, len(builder.series))
	for _, meta := range builder.series {
		got[meta.metricName+"|"+meta.labelMap["le"]] = meta.samples
	}
	want := map[string][]samplePoint{
		"test_duration_seconds_bucket|0.5":  {{t: 1000, v: 2}, {t: 2000, v: 4}, {t: 3000, v: 6}},
		"test_duration_seconds_bucket|1":    {{t: 1000, v: 5}, {t: 3000, v: 15}},
		"test_duration_seconds_bucket|+Inf": {{t: 1000, v: 10}, {t: 2000, v: 20}, {t: 3000, v: 30}},
		"test_duration_seconds_count|":      {{t: 1000, v: 10}, {t: 2000, v: 20}, {t: 3000, v: 30}},
		"test_duration_seconds_sum|":        {{t: 1000, v: 12}, {t: 2000, v: 24}, {t: 3000, v: 36}},
	}
	if !reflect.DeepEqual(got, want) {
		test.Fatalf("bound sets did not merge into sorted series: %v", got)
	}
}

func TestPostHogHistogramSourceIdentity(test *testing.T) {
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
	for index := 0; index < 3; index++ {
		sample := histogramTestSample()
		sample.id = uint64(index + 1)
		sample.timestamp += int64(index) * 1000
		if index == 1 {
			sample.resource["region"] = "other"
		}
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			test.Fatal(err)
		}
	}
	if len(builder.series) != 6 {
		test.Fatalf("got %d series, want 6", len(builder.series))
	}
	for _, meta := range builder.series {
		if meta.labelMap["region"] == "other" {
			if len(meta.samples) != 1 || meta.samples[0].t != 2000 {
				test.Fatalf("source labels mixed: %v", meta.samples)
			}
		} else if len(meta.samples) != 2 || meta.samples[0].t != 1000 || meta.samples[1].t != 3000 {
			test.Fatalf("identical labels did not merge: %v", meta.samples)
		}
	}
}

func TestPostHogHistogramBucketReappears(test *testing.T) {
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
	for index := 0; index < 4; index++ {
		sample := histogramTestSample()
		sample.timestamp += int64(index) * 1000
		if index == 1 || index == 2 {
			sample.bounds = []float64{1}
			sample.counts = []uint64{5, 5}
		}
		if err := builder.add(sample, []string{"_bucket"}); err != nil {
			test.Fatal(err)
		}
	}
	if len(builder.series) != 3 {
		test.Fatalf("got %d series, want 3", len(builder.series))
	}
	for _, meta := range builder.series {
		if meta.labelMap["le"] == "0.5" {
			if len(meta.samples) != 3 || meta.samples[0] != (samplePoint{t: 1000, v: 2}) || meta.samples[1].t != 2000 || !isStaleSampleValue(meta.samples[1].v) || meta.samples[2] != (samplePoint{t: 4000, v: 2}) {
				test.Fatalf("incorrect bucket removal or return: %v", meta.samples)
			}
		} else if len(meta.samples) != 4 {
			test.Fatalf("unchanged bucket lost samples: %v", meta.samples)
		}
	}
}

func TestPostHogHistogramRepeatedSampleChecks(test *testing.T) {
	for _, testcase := range []struct {
		name   string
		change func(*postHogHistogramSample)
	}{
		{"temporality", func(sample *postHogHistogramSample) { sample.temporality = "delta" }},
		{"count", func(sample *postHogHistogramSample) { sample.count++ }},
		{"bounds", func(sample *postHogHistogramSample) { sample.bounds = []float64{1, 0.5} }},
	} {
		test.Run(testcase.name, func(test *testing.T) {
			exact := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_duration_seconds_bucket")}
			builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), exact, true)
			sample := histogramTestSample()
			if err := builder.add(sample, []string{"_bucket"}); err != nil {
				test.Fatal(err)
			}
			testcase.change(&sample)
			if err := builder.add(sample, []string{"_bucket"}); err == nil {
				test.Fatal("invalid repeated sample accepted")
			}
		})
	}
	config := histogramTestConfig()
	config.MaxSamples = 4
	builder := newPostHogHistogramSeriesBuilder(config, nil, true)
	if err := builder.add(histogramTestSample(), []string{"_bucket"}); err != nil {
		test.Fatal(err)
	}
	if err := builder.add(histogramTestSample(), []string{"_bucket"}); err == nil || !strings.Contains(err.Error(), "sample limit") {
		test.Fatalf("repeated samples bypassed limit: %v", err)
	}
	config = histogramTestConfig()
	config.MaxSeries = 3
	builder = newPostHogHistogramSeriesBuilder(config, nil, true)
	sample := histogramTestSample()
	if err := builder.add(sample, []string{"_bucket"}); err != nil {
		test.Fatal(err)
	}
	sample.bounds = []float64{2}
	sample.counts = []uint64{1, 9}
	if err := builder.add(sample, []string{"_bucket"}); err == nil || !strings.Contains(err.Error(), "series limit") {
		test.Fatalf("new bucket bypassed series limit: %v", err)
	}
}

func TestPostHogHistogramSignedZeroBounds(test *testing.T) {
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), nil, true)
	sample := histogramTestSample()
	sample.bounds = []float64{math.Copysign(0, -1)}
	sample.counts = []uint64{1, 9}
	if err := builder.add(sample, []string{"_bucket"}); err != nil {
		test.Fatal(err)
	}
	sample.timestamp = 2000
	sample.bounds = []float64{0}
	if err := builder.add(sample, []string{"_bucket"}); err != nil {
		test.Fatal(err)
	}
	if len(builder.series) != 3 {
		test.Fatalf("signed zero boundaries merged: %d series", len(builder.series))
	}
	for _, meta := range builder.series {
		if meta.labelMap["le"] == "-0" && (len(meta.samples) != 2 || !isStaleSampleValue(meta.samples[1].v)) {
			test.Fatalf("old signed zero bucket did not become stale: %v", meta.samples)
		}
	}
}

func BenchmarkPostHogHistogramSeriesBuilder(benchmark *testing.B) {
	for _, suffixes := range [][]string{{"_bucket"}, {"_count", "_sum"}, postHogHistogramSuffixes} {
		benchmark.Run(strings.Join(suffixes, ""), func(benchmark *testing.B) {
			config := histogramTestConfig()
			config.MaxSeries = 1000
			config.MaxSamples = 0
			var samples []postHogHistogramSample
			for source := 0; source < 32; source++ {
				for step := 0; step < 128; step++ {
					sample := histogramTestSample()
					sample.id = uint64(source)
					sample.attributes["instance"] = strconv.Itoa(source)
					sample.timestamp += int64(step) * 1000
					samples = append(samples, sample)
				}
			}
			benchmark.ReportAllocs()
			benchmark.ResetTimer()
			for iteration := 0; iteration < benchmark.N; iteration++ {
				builder := newPostHogHistogramSeriesBuilder(config, nil, true)
				for _, sample := range samples {
					if err := builder.add(sample, suffixes); err != nil {
						benchmark.Fatal(err)
					}
				}
			}
		})
	}
}

func TestPostHogHistogramInvalidSourcesSkippedForBroadSelectors(test *testing.T) {
	invalid := histogramTestSample()
	invalid.id = 2
	invalid.metricName = "test_broken_seconds"
	invalid.count = 11
	delta := histogramTestSample()
	delta.id = 3
	delta.metricName = "test_delta_seconds"
	delta.temporality = "delta"
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "test_.*")}, true)
	for _, sample := range []postHogHistogramSample{invalid, delta, invalid, histogramTestSample()} {
		if err := builder.add(sample, []string{"_bucket", "_count"}); err != nil {
			test.Fatalf("broad selector must skip invalid histogram %q: %v", sample.metricName, err)
		}
	}
	if len(builder.skipped) != 2 {
		test.Fatalf("skipped sources = %d, want the invalid and the delta histogram", len(builder.skipped))
	}
	for _, meta := range builder.series {
		if name := meta.labelMap[labels.MetricName]; !strings.HasPrefix(name, "test_duration_seconds") {
			test.Fatalf("invalid histogram produced series %q", name)
		}
	}
	if len(builder.series) != 4 {
		test.Fatalf("valid histogram produced %d series, want three buckets and a count", len(builder.series))
	}
	strict := newPostHogHistogramSeriesBuilder(histogramTestConfig(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_broken_seconds_bucket")}, true)
	if err := strict.add(invalid, []string{"_bucket"}); err == nil || !strings.Contains(err.Error(), "observation count") {
		test.Fatalf("exact selector must report the invalid histogram: %v", err)
	}
}

func TestPostHogHistogramDiscoveryAndExistenceSQL(test *testing.T) {
	cfg := histogramTestConfig()
	aliases := []postHogHistogramAlias{{baseName: "test_duration_seconds", suffix: "_bucket"}}
	sql := postHogHistogramBoundsSQL(cfg, 1000, 2000, "series_fingerprint IN (SELECT id FROM series_ids)", aliases, nil)
	for _, want := range []string{"SELECT DISTINCT series_fingerprint", "arrayWithConstant(length(histogram_bounds) + 1, toUInt64(0))", "series_fingerprint IN (SELECT id FROM series_ids)", "team_id = 42", "ORDER BY series_id"} {
		if !strings.Contains(sql, want) {
			test.Fatalf("bounds SQL missing %q: %s", want, sql)
		}
	}
	for _, notWant := range []string{"histogram_counts", "toUnixTimestamp64Milli", "ORDER BY series_id, timestamp"} {
		if strings.Contains(sql, notWant) {
			test.Fatalf("bounds SQL must not read samples via %q: %s", notWant, sql)
		}
	}
	sql = postHogHistogramSourceExistsSQL(cfg, 1000, 2000, aliases[0])
	if want := "SELECT 1 FROM `test`.`metrics4_series` WHERE team_id = 42 AND time_bucket >= toStartOfHour(" + chTimeMillis(1000) + ") AND time_bucket <= toStartOfHour(" + chTimeMillis(2000) + ") AND metric_name = 'test_duration_seconds' AND metric_type = 'histogram' LIMIT 1"; sql != want {
		test.Fatalf("bucket existence SQL = %s, want %s", sql, want)
	}
	sql = postHogHistogramSourceExistsSQL(cfg, 1000, 2000, postHogHistogramAlias{baseName: "test_duration_seconds", suffix: "_count"})
	if !strings.Contains(sql, "metric_type IN ('histogram', 'exponential_histogram')") {
		test.Fatalf("count existence SQL must accept exponential histograms: %s", sql)
	}
}

func TestMergePostHogHistogramSeries(test *testing.T) {
	series := func(name, le string, samples ...samplePoint) *seriesMeta {
		labelMap := map[string]string{labels.MetricName: name, "service_name": "api"}
		if le != "" {
			labelMap["le"] = le
		}
		set := labels.FromMap(labelMap)
		return &seriesMeta{id: set.Hash(), metricName: name, labelMap: labelMap, labels: set, samples: samples}
	}
	stale := math.Float64frombits(promvalue.StaleNaN)
	real := []*seriesMeta{
		series("duration_bucket", "1", samplePoint{t: 2000, v: 8}),
		series("duration_bucket", "+Inf", samplePoint{t: 2000, v: 10}),
		series("duration_count", "", samplePoint{t: 2000, v: 10}),
	}
	virtual := []*seriesMeta{
		series("duration_bucket", "1", samplePoint{t: 1000, v: 5}, samplePoint{t: 2000, v: stale}, samplePoint{t: 3000, v: 12}),
		series("duration_bucket", "+Inf", samplePoint{t: 1000, v: 6}, samplePoint{t: 3000, v: 15}),
		series("duration_sum", "", samplePoint{t: 1000, v: 3}, samplePoint{t: 3000, v: 9}),
	}
	merged := mergePostHogHistogramSeries(real, virtual)
	got := make(map[string][]samplePoint, len(merged))
	for _, meta := range merged {
		got[meta.labels.String()] = meta.samples
	}
	want := map[string][]samplePoint{
		real[0].labels.String():    {{t: 1000, v: 5}, {t: 2000, v: 8}, {t: 3000, v: 12}},
		real[1].labels.String():    {{t: 1000, v: 6}, {t: 2000, v: 10}, {t: 3000, v: 15}},
		real[2].labels.String():    {{t: 2000, v: 10}},
		virtual[2].labels.String(): {{t: 1000, v: 3}, {t: 3000, v: 9}},
	}
	if len(merged) != len(want) || !reflect.DeepEqual(got, want) {
		test.Fatalf("merged series = %v, want %v", got, want)
	}
	if only := mergePostHogHistogramSeries(nil, virtual); len(only) != len(virtual) {
		test.Fatalf("virtual only = %d series", len(only))
	}
	if only := mergePostHogHistogramSeries(real, nil); len(only) != len(real) {
		test.Fatalf("real only = %d series", len(only))
	}
	unsorted := mergeSamplesPreferFirst([]samplePoint{{t: 3000, v: 3}, {t: 1000, v: 1}}, []samplePoint{{t: 2000, v: 2}, {t: 1000, v: 9}})
	if !reflect.DeepEqual(unsorted, []samplePoint{{t: 1000, v: 1}, {t: 2000, v: 2}, {t: 3000, v: 3}}) {
		test.Fatalf("unsorted merge = %v", unsorted)
	}
}
