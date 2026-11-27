package snuffle

import (
	"reflect"
	"strings"
	"testing"
)

// ENG-052: a __name__ regex that lists literal names is a set of names, as a
// plain name is, so the planners that need names accept it; any other name
// matcher is unbounded and resolves through the metric name index.
func TestMetricNameMatchers(test *testing.T) {
	for _, tc := range []struct {
		selector  string
		names     []string // nil: unbounded
		exact     string
		histogram bool
		sql       string
	}{
		{`m{a="x"}`, []string{"m"}, "m", false, "metric_name = 'm'"},
		{`{__name__=~"m",a="x"}`, []string{"m"}, "m", false, "metric_name IN ('m')"},
		{`{__name__=~"a|b"}`, []string{"a", "b"}, "", false, "metric_name IN ('a','b')"},
		{`{__name__=~"a|b",__name__!="a"}`, []string{"b"}, "b", false, "metric_name IN ('a','b') AND metric_name != 'a'"},
		{`{__name__=~"a|h_bucket"}`, []string{"a", "h_bucket"}, "", true, "metric_name IN ('a','h_bucket')"},
		{`{__name__=~"a.*"}`, nil, "", true, "startsWith(metric_name, 'a')"},
		{`{__name__!~"a.*",b="x"}`, nil, "", true, "NOT startsWith(metric_name, 'a')"},
		{`{__name__=~".*",b="x"}`, nil, "", true, ""},
	} {
		test.Run(tc.selector, func(test *testing.T) {
			matchers, err := newBoundLabelParser().ParseMetricSelector(tc.selector)
			if err != nil {
				test.Fatal(err)
			}
			names, bounded := metricNameSet(matchers)
			if bounded != (tc.names != nil) || (bounded && !reflect.DeepEqual(names, tc.names)) {
				test.Errorf("metricNameSet = %v, %v, want %v", names, bounded, tc.names)
			}
			if got := metricNamesUnbounded(matchers); got != (tc.names == nil) {
				test.Errorf("metricNamesUnbounded = %v", got)
			}
			if got := exactMetricName(matchers); got != tc.exact {
				test.Errorf("exactMetricName = %q, want %q", got, tc.exact)
			}
			if got := postHogMaySelectHistogram(matchers); got != tc.histogram {
				test.Errorf("postHogMaySelectHistogram = %v, want %v", got, tc.histogram)
			}
			if got := strings.Join(metricNameConstraints(matchers), " AND "); got != tc.sql {
				test.Errorf("metricNameConstraints = %s, want %s", got, tc.sql)
			}
		})
	}
}

// The histogram alias scan reads the series table by metric name: the base
// names of a name regex come from the metric name index.
func TestPostHogHistogramAliasesUseNameIndex(test *testing.T) {
	cfg := Config{SchemaLayout: string(schemaLayoutPostHog), SeriesTable: "metrics4_series", MetricNamesTable: "metrics4_names", MaxSeries: 10}
	matchers, err := newBoundLabelParser().ParseMetricSelector(`{__name__=~"h.*_count",a="x"}`)
	if err != nil {
		test.Fatal(err)
	}
	want := "metric_name IN (SELECT metric_name FROM `metrics4_names` ARRAY JOIN ['_bucket', '_count', '_sum'] AS suffix WHERE team_id = 0 AND "
	if sql := postHogHistogramAliasesSQL(cfg, 0, 1, matchers); !strings.Contains(sql, want) {
		test.Errorf("aliases SQL does not read the name index:\n%s", sql)
	}
}
