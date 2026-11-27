package snuffle

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func TestPostHogExactHistogramAlias(test *testing.T) {
	for _, name := range []string{"duration_bucket", "duration_count", "duration_sum", "duration_sum_bucket"} {
		alias, ok := postHogExactHistogramAlias([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)})
		if !ok || alias.baseName+alias.suffix != name || alias.baseName == "" {
			test.Fatalf("alias for %q = %+v, %v", name, alias, ok)
		}
	}
	for _, matcher := range []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "duration"),
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "_bucket"),
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, ""),
		labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "duration_(bucket|sum)"),
		labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, "duration_bucket"),
		labels.MustNewMatcher(labels.MatchEqual, "service_name", "duration_bucket"),
	} {
		if alias, ok := postHogExactHistogramAlias([]*labels.Matcher{matcher}); ok {
			test.Fatalf("unexpected exact alias for %s: %+v", matcher, alias)
		}
	}
}

func TestPostHogExactHistogramMetadataSQL(test *testing.T) {
	for _, suffix := range postHogHistogramSuffixes {
		test.Run(suffix, func(test *testing.T) {
			cfg := histogramTestConfig()
			alias := postHogHistogramAlias{baseName: "duration", suffix: suffix}
			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "duration"+suffix),
				labels.MustNewMatcher(labels.MatchEqual, "service_name", "api"),
				labels.MustNewMatcher(labels.MatchEqual, "le", "1"),
			}
			sql := postHogExactHistogramMetadataSQL(cfg, 1000, 2000, alias, matchers)
			for _, want := range []string{
				"SELECT series_fingerprint AS series_id, " + postHogSeriesLabelColumns,
				"metric_name IN ('duration" + suffix + "', 'duration')",
				"time_bucket >= toStartOfHour(fromUnixTimestamp64Milli(1000",
				"LIMIT 1 BY series_id LIMIT 100",
			} {
				if !strings.Contains(sql, want) {
					test.Fatalf("SQL missing %q: %s", want, sql)
				}
			}
			if strings.Contains(sql, "NOT IN") {
				test.Fatalf("stored component series must not suppress the virtual branch: %s", sql)
			}
			branches := strings.Split(sql, ") OR (")
			if len(branches) != 2 || !strings.Contains(branches[0], "['le']") || strings.Contains(branches[1], "['le']") {
				test.Fatalf("le must filter real labels only: %s", sql)
			}
			if strings.Count(sql, postHogServiceNameExprSQL()+" = 'api'") != 2 {
				test.Fatalf("both branches must filter service names: %s", sql)
			}
			typeFilter := postHogHistogramTypeFilter
			if suffix == "_bucket" {
				typeFilter = "metric_type = 'histogram'"
			}
			if !strings.Contains(branches[1], typeFilter) || strings.Contains(branches[0], "metric_type") {
				test.Fatalf("histogram types must filter the virtual branch only: %s", sql)
			}
			if strings.Contains(sql, "ARRAY JOIN") || strings.Contains(sql, "metric_names3") {
				test.Fatalf("exact metadata must use the authoritative series table: %s", sql)
			}
		})
	}
}

// A stored delta counter whose name ends in _count, _sum or _bucket takes the
// exact histogram alias path. Its series must carry the temporality flag, as
// the float path does, so counter functions read its running total.
func TestPostHogExactHistogramMetadataMarksDeltaSeries(test *testing.T) {
	cfg := Config{CHDatabase: "default", SchemaLayout: "posthog", SeriesTable: "metrics4_series", SamplesTable: "metrics4_samples", MaxSeries: 10, TeamID: 42}
	alias := postHogHistogramAlias{baseName: "calls", suffix: "_count"}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "calls_count")}
	if sql := postHogExactHistogramMetadataSQL(cfg, 1000, 2000, alias, matchers); !strings.Contains(sql, "aggregation_temporality = 'delta' AS delta") {
		test.Fatalf("exact metadata must select the temporality: %s", sql)
	}
	for _, testCase := range []struct {
		name  string
		delta uint8
	}{{"delta", 1}, {"cumulative", 0}} {
		test.Run(testCase.name, func(test *testing.T) {
			conn := &failingConn{rows: [][]any{{uint64(1), "calls_count", "api", map[string]string{}, map[string]string{}, testCase.delta}}}
			withClickHouseConn(test, conn)
			querier := &CHQuerier{queryable: NewCHQueryable(NewClickHouseClient(cfg), cfg)}
			series, _, _, err := querier.selectPostHogExactHistogramMetadata(context.Background(), 1000, 2000, alias, matchers)
			if err != nil || len(series) != 1 {
				test.Fatalf("got %d series, %v; want 1", len(series), err)
			}
			if series[0].delta != (testCase.delta == 1) {
				test.Fatalf("delta = %v, want %v", series[0].delta, testCase.delta == 1)
			}
		})
	}
}

func TestPostHogExactHistogramConflictingNames(test *testing.T) {
	querier := &CHQuerier{}
	for _, matcher := range []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "other_bucket"),
		labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, "duration_bucket"),
		labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "other_.*"),
	} {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "duration_bucket"), matcher}
		series, err := querier.selectPostHogExactHistogramSeries(context.Background(), 1000, 2000, postHogHistogramAlias{baseName: "duration", suffix: "_bucket"}, matchers, true, false)
		if err != nil || len(series) != 0 {
			test.Fatalf("conflicting matchers returned %v, %v", series, err)
		}
	}
}
