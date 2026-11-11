package snuffle

import (
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// PostHog capture moves the Prometheus job label into service_name, so the
// series table has no job attribute for remote-written series.
const postHogJobExpr = "if(mapContains(resource_attributes, 'job'), resource_attributes['job'], if(mapContains(attributes, 'job'), attributes['job'], service_name))"

func TestPostHogLabelMapExposesServiceNameAsJob(t *testing.T) {
	for name, test := range map[string]struct {
		serviceName string
		resource    map[string]string
		attrs       map[string]string
		want        map[string]string
	}{
		"moved job": {
			serviceName: "kube-state-metrics",
			resource:    map[string]string{"service.name": "kube-state-metrics"},
			attrs:       map[string]string{"namespace": "wiz"},
			want: map[string]string{
				labels.MetricName: "kube_pod_info",
				"service_name":    "kube-state-metrics",
				"service.name":    "kube-state-metrics",
				"job":             "kube-state-metrics",
				"namespace":       "wiz",
			},
		},
		"metric job attribute": {
			serviceName: "checkout",
			attrs:       map[string]string{"job": "batch"},
			want:        map[string]string{labels.MetricName: "kube_pod_info", "service_name": "checkout", "job": "batch"},
		},
		"resource job attribute": {
			serviceName: "checkout",
			resource:    map[string]string{"job": "resource"},
			attrs:       map[string]string{"job": "metric"},
			want:        map[string]string{labels.MetricName: "kube_pod_info", "service_name": "checkout", "job": "resource"},
		},
		"no service": {
			attrs: map[string]string{"namespace": "wiz"},
			want:  map[string]string{labels.MetricName: "kube_pod_info", "namespace": "wiz"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := postHogLabelMap("kube_pod_info", test.serviceName, test.resource, test.attrs)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("labels = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPostHogJobMatcherFallsBackToServiceName(t *testing.T) {
	cfg := Config{
		CHDatabase:   "posthog",
		SchemaLayout: "posthog",
		SeriesTable:  "metrics4_series",
		SamplesTable: "metrics4_samples",
		MaxSeries:    500,
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "kube_pod_info"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "kube-state-metrics"),
	}
	sql := postHogSelectedSeriesSQL(cfg, matchers, 1000, 2000, cfg.MaxSeries, nil)
	if want := postHogJobExpr + " = 'kube-state-metrics'"; !strings.Contains(sql, want) {
		t.Fatalf("series SQL %q does not contain %q", sql, want)
	}

	// job is not a samples column: a series without a job attribute has one
	// only through service_name, so the series table must be read.
	if !postHogNeedsSeriesLookup(matchers, nil) {
		t.Fatal("a job matcher must read the series table")
	}
	if !postHogNeedsSeriesLookup(nil, []string{"job"}) {
		t.Fatal("grouping by job must read the series table")
	}
	plan := newPostHogQueryPlan(cfg, nil, []string{"job"}, 1000, 2000, false)
	if want := postHogJobExpr + " AS `__group_0`"; !strings.Contains(plan.seriesSQL, want) {
		t.Fatalf("grouping SQL %q does not contain %q", plan.seriesSQL, want)
	}
}

func TestPostHogJobLabelValuesIncludeServiceNames(t *testing.T) {
	cfg := Config{
		CHDatabase:                  "posthog",
		SchemaLayout:                "posthog",
		SeriesTable:                 "metrics4_series",
		AttributeTable:              "metrics4_attributes",
		MetricNamesTable:            "metrics4_names",
		AttributeTableHasMetricName: true,
		TeamID:                      7,
	}
	metric := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "kube_pod_info")
	sql, ok := postHogLabelValuesSQL(cfg, "job", 1000, 2000, 50, []*labels.Matcher{metric})
	if !ok {
		t.Fatal("job values for an exact metric should read the attribute rollup")
	}
	for _, want := range []string{
		"FROM `posthog`.`metrics4_attributes` WHERE",
		"metric_name = 'kube_pod_info'",
		"SELECT attribute_value AS label_value FROM `posthog`.`metrics4_attributes` WHERE",
		"attribute_key = 'job'",
		// A service with a job attribute does not also list its service name.
		"SELECT DISTINCT service_name AS label_value FROM `posthog`.`metrics4_attributes` WHERE",
		"(metric_name, service_name) NOT IN (SELECT metric_name, service_name FROM `posthog`.`metrics4_attributes` WHERE",
		"WHERE label_value != ''",
		"ORDER BY label_value" + sqlLimit(50),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}

	// The legacy attribute table has no metric_name column.
	cfg.AttributeTableHasMetricName = false
	sql, ok = postHogLabelValuesSQL(cfg, "job", 1000, 2000, 0, nil)
	if !ok || !strings.Contains(sql, " service_name NOT IN (SELECT service_name FROM `posthog`.`metrics4_attributes` WHERE") {
		t.Fatalf("legacy rollup should exclude services by name only: %s", sql)
	}

	cfg.AttributeTable = ""
	sql, ok = postHogLabelValuesSQL(cfg, "job", 1000, 2000, 0, nil)
	if !ok || !strings.Contains(sql, "SELECT DISTINCT "+postHogJobExpr+" AS label_value FROM `posthog`.`metrics4_series`") || !strings.Contains(sql, "label_value != ''") {
		t.Fatalf("without a rollup, job values should read the series table: %s", sql)
	}
}
