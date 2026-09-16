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
				"last_seen >= fromUnixTimestamp64Milli(1000",
				"LIMIT 1 BY series_id LIMIT 100",
				"'duration" + suffix + "' NOT IN (SELECT metric_name FROM `test`.`metric_series3` WHERE team_id = 42 AND metric_name = 'duration" + suffix + "')",
			} {
				if !strings.Contains(sql, want) {
					test.Fatalf("SQL missing %q: %s", want, sql)
				}
			}
			branches := strings.Split(sql, ") OR (")
			if len(branches) != 2 || !strings.Contains(branches[0], "['le']") || strings.Contains(branches[1], "['le']") {
				test.Fatalf("le must filter real labels only: %s", sql)
			}
			if strings.Count(sql, "service_name = 'api'") != 2 {
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
