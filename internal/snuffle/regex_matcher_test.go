package snuffle

import (
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// A literal prefix is necessary for a regex match, not sufficient: every
// builder must still check the whole pattern unless it is exactly prefix.*.
func TestRegexMatcherConditions(test *testing.T) {
	for _, tc := range []struct {
		regex string
		want  string // positive form on column COL
	}{
		{`foo-bars?-kafka`, `(startsWith(COL, 'foo-bar') AND match(COL, '^(?:foo-bars?-kafka)$'))`},
		{`foo.-x`, `(startsWith(COL, 'foo') AND match(COL, '^(?:foo.-x)$'))`},
		{`foo[a-z]*-x`, `(startsWith(COL, 'foo') AND match(COL, '^(?:foo[a-z]*-x)$'))`},
		{`^foo-bars?-kafka$`, `(startsWith(COL, 'foo-bar') AND match(COL, '^(?:^foo-bars?-kafka$)$'))`},
		{`^foo-bar$`, `COL IN ('foo-bar')`},
		{`foo.*`, `startsWith(COL, 'foo')`},
		{`foo-bar|foo-baz`, `COL IN ('foo-bar','foo-baz')`},
		{`foo-ba(r|z)-x`, `COL IN ('foo-bar-x','foo-baz-x')`},
		{`.*-kafka`, `match(COL, '^(?:.*-kafka)$')`},
	} {
		test.Run(tc.regex, func(test *testing.T) {
			positive := labels.MustNewMatcher(labels.MatchRegexp, "container", tc.regex)
			negative := labels.MustNewMatcher(labels.MatchNotRegexp, "container", tc.regex)
			on := func(column string) string { return strings.ReplaceAll(tc.want, "COL", column) }
			not := func(column string) string {
				if strings.HasPrefix(tc.want, "COL IN") {
					return strings.Replace(on(column), " IN ", " NOT IN ", 1)
				}
				return "NOT " + on(column)
			}
			check := func(name, got, want string) {
				test.Helper()
				if got != want {
					test.Errorf("%s = %s, want %s", name, got, want)
				}
			}
			got, _ := stringColumnMatcherCondition("col", positive)
			check("column =~", got, on("col"))
			got, _ = stringColumnMatcherCondition("col", negative)
			check("column !~", got, not("col"))
			got, _ = metricMatcherCondition(labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, tc.regex))
			check("metric =~", got, on("metric_name"))
			got, _ = metricMatcherCondition(labels.MustNewMatcher(labels.MatchNotRegexp, labels.MetricName, tc.regex))
			check("metric !~", got, not("metric_name"))
			membership, got, _ := positiveLabelIndexMembershipCondition(positive)
			check("label index =~", membership+" "+got, "IN "+on("label_value"))
			// The caller excludes the series this condition selects.
			got, _ = negativeLabelIndexCondition(negative)
			check("label index !~", got, on("label_value"))
		})
	}
}
