package snuffle

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

func sampleBaseFilters(cfg Config, matchers []*labels.Matcher, mint, maxt int64) []string {
	filters := []string{teamFilter(cfg)}
	filters = append(filters, sampleTimeFilters(cfg, mint, maxt)...)
	filters = append(filters, metricNameConstraints(matchers)...)
	filters = append(filters, postHogServiceNameFilters(cfg, matchers)...)
	return filters
}

func sampleTimeFilters(cfg Config, mint, maxt int64) []string {
	filters := []string{
		"timestamp >= " + chTimeMillis(mint),
		"timestamp <= " + chTimeMillis(maxt),
	}
	if cfg.postHogSchemaLayout() {
		filters = append(filters,
			"time_bucket >= toStartOfDay("+chTimeMillis(mint)+")",
			"time_bucket <= toStartOfDay("+chTimeMillis(maxt)+")",
		)
	}
	return filters
}

func postHogServiceNameFilters(cfg Config, matchers []*labels.Matcher) []string {
	if !cfg.postHogSchemaLayout() {
		return nil
	}
	for _, matcher := range matchers {
		if matcher.Name != "service.name" && matcher.Name != "service_name" {
			continue
		}
		switch matcher.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if matcher.Matches("") {
				continue
			}
			if condition, ok := stringColumnMatcherCondition("service_name", matcher); ok {
				return []string{condition}
			}
		}
	}
	return nil
}

func stringColumnMatcherCondition(column string, matcher *labels.Matcher) (string, bool) {
	switch matcher.Type {
	case labels.MatchEqual:
		return column + " = " + sqlString(matcher.Value), true
	case labels.MatchNotEqual:
		return column + " != " + sqlString(matcher.Value), true
	case labels.MatchRegexp:
		if values := matcher.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return column + " IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := matcher.Prefix(); prefix != "" {
			return "startsWith(" + column + ", " + sqlString(prefix) + ")", true
		}
		return "match(" + column + ", " + sqlString(promRegexToCH(matcher.Value)) + ")", true
	case labels.MatchNotRegexp:
		if values := matcher.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return column + " NOT IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := matcher.Prefix(); prefix != "" {
			return "NOT startsWith(" + column + ", " + sqlString(prefix) + ")", true
		}
		return "NOT match(" + column + ", " + sqlString(promRegexToCH(matcher.Value)) + ")", true
	default:
		return "", false
	}
}

func sampleSelectedSeriesFilters(cfg Config) []string {
	return sampleIDMembershipFilters(cfg, "IN", "SELECT id FROM selected_series")
}

func sampleExplicitIDFilters(cfg Config, ids []uint64) []string {
	return sampleIDMembershipFilters(cfg, "IN", joinUint64(ids))
}

func sampleIDMembershipFilters(cfg Config, membership, source string) []string {
	filters := []string{"id " + membership + " (" + source + ")"}
	if cfg.postHogSchemaLayout() {
		filters = append(filters, "resource_fingerprint "+membership+" ("+source+")")
	}
	return filters
}
