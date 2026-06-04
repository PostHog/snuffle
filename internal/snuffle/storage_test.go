package snuffle

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestSampleIteratorSeek(t *testing.T) {
	it := &sampleIterator{
		idx: -1,
		points: []seriesPoint{
			{t: 10, f: 1, typ: chunkenc.ValFloat},
			{t: 20, f: 2, typ: chunkenc.ValFloat},
			{t: 30, f: 3, typ: chunkenc.ValFloat},
		},
	}

	if got := it.Seek(20); got != chunkenc.ValFloat {
		t.Fatalf("Seek returned %s", got)
	}
	ts, value := it.At()
	if ts != 20 || value != 2 {
		t.Fatalf("At after seek = (%d, %v), want (20, 2)", ts, value)
	}
	if got := it.Seek(15); got != chunkenc.ValFloat {
		t.Fatalf("Seek before current returned %s", got)
	}
	ts, value = it.At()
	if ts != 20 || value != 2 {
		t.Fatalf("Seek moved backwards to (%d, %v)", ts, value)
	}
	if got := it.Next(); got != chunkenc.ValFloat {
		t.Fatalf("Next returned %s", got)
	}
	ts, value = it.At()
	if ts != 30 || value != 3 {
		t.Fatalf("At after next = (%d, %v), want (30, 3)", ts, value)
	}
}

func TestParseLabelsJSONAcceptsObjectAndEncodedString(t *testing.T) {
	for _, raw := range []string{
		`{"job":"api","instance":"host-1"}`,
		`"{\"job\":\"api\",\"instance\":\"host-1\"}"`,
	} {
		labelsMap, err := parseLabelsJSON(json.RawMessage(raw))
		if err != nil {
			t.Fatalf("parseLabelsJSON(%s) returned error: %v", raw, err)
		}
		if labelsMap["job"] != "api" || labelsMap["instance"] != "host-1" {
			t.Fatalf("parseLabelsJSON(%s) = %#v", raw, labelsMap)
		}
	}
}

func TestLatestSamplesSQLFromMatchers(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
	}

	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, true)
	if !ok {
		t.Fatal("samplesSQLFromMatchers returned ok=false")
	}
	for _, want := range []string{
		"argMax(value, timestamp)",
		"SELECT id, timestamp, value",
		"`default`.`samples`",
		"`default`.`label_index`",
		"team_id = 0",
		"metric_name = 'http_requests_total'",
		"label_name = 'job'",
		"label_value = 'api'",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestLatestSamplesSQLPushesSafeNegativeMatcher(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchNotEqual, "status", "500"),
	}
	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, true)
	if !ok {
		t.Fatal("negative matcher that matches missing labels should push down through NOT IN")
	}
	for _, want := range []string{
		"`default`.`label_index`",
		"id NOT IN",
		"label_name = 'status'",
		"label_value = '500'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestLatestSamplesSQLRejectsNegativeMatcherThatDoesNotMatchMissingLabels(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchNotEqual, "status", ""),
	}
	if _, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, true); ok {
		t.Fatal("negative matcher that excludes missing labels should fall back to exact selected IDs")
	}
}

func TestRangeSamplesSQLFromMatchers(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
	}

	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, false)
	if !ok {
		t.Fatal("samplesSQLFromMatchers returned ok=false")
	}
	for _, want := range []string{
		"toUnixTimestamp64Milli(timestamp)",
		"SELECT id, timestamp, value",
		"ORDER BY id, timestamp",
		"`default`.`samples`",
		"`default`.`label_index`",
		"team_id = 0",
		"metric_name = 'http_requests_total'",
		"label_name = 'job'",
		"label_value = 'api'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestRangeSamplesSQLIgnoresNoopLabelMatcher(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		SamplesTable:    "samples",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "node_cpu_seconds_total"),
		labels.MustNewMatcher(labels.MatchRegexp, "type", ".*"),
		labels.MustNewMatcher(labels.MatchEqual, "ready", "true"),
	}

	sql, ok := samplesSQLFromMatchers(cfg, matchers, 1000, 2000, false)
	if !ok {
		t.Fatal("samplesSQLFromMatchers returned ok=false")
	}
	if strings.Contains(sql, "label_name = 'type'") {
		t.Fatalf("SQL %q should not push noop matcher", sql)
	}
	for _, want := range []string{
		"metric_name = 'node_cpu_seconds_total'",
		"label_name = 'ready'",
		"label_value = 'true'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestSamplesSQLKeepsMetricConstraintForIDBatches(t *testing.T) {
	cfg := Config{
		CHDatabase:   "default",
		SamplesTable: "samples",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql := samplesSQL(cfg, []uint64{1, 2}, 1000, 2000, false, matchers)
	for _, want := range []string{
		"`default`.`samples`",
		"team_id = 0",
		"id IN (1,2)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name = 'http_requests_total'",
		"ORDER BY id, timestamp",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
	if strings.Contains(sql, "label_name = 'status'") {
		t.Fatalf("small id-batch sample SQL should not redo label-index filtering: %s", sql)
	}
}

func TestPostHogSeriesSamplesSQLUsesPostHogTablesAndAttributePredicates(t *testing.T) {
	cfg := Config{
		CHDatabase:   "posthog",
		SchemaLayout: "posthog",
		SamplesTable: "metrics1",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "service_name", "checkout"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql := postHogSeriesSamplesSQL(cfg, matchers, 1000, 2000, false)
	for _, want := range []string{
		"`posthog`.`metrics1`",
		"xxHash64(metric_name, service_name, resource_fingerprint, mapSort(attributes_map_str)) AS series_id",
		"time_bucket >= toStartOfDay(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"time_bucket <= toStartOfDay(fromUnixTimestamp64Milli(2000, 'UTC'))",
		"service_name = 'checkout'",
		"metric_name = 'http_requests_total'",
		"if(mapContains(attributes_map_str, 'status__str'), attributes_map_str['status__str'], resource_attributes['status']) = '200'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
	for _, notWant := range []string{"metrics_series", "metrics_label_index", "id IN"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("posthog SQL should not use %q:\n%s", notWant, sql)
		}
	}
}

func TestPostHogLoadSamplesSQLFiltersByComputedSeriesID(t *testing.T) {
	cfg := Config{
		CHDatabase:   "posthog",
		SchemaLayout: "posthog",
		SamplesTable: "metrics1",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
	}

	sql := postHogLoadSamplesSQL(cfg, []uint64{1, 2}, matchers, 1000, 2000, true)
	for _, want := range []string{
		"`posthog`.`metrics1`",
		"time_bucket >= toStartOfDay(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"xxHash64(metric_name, service_name, resource_fingerprint, mapSort(attributes_map_str)) IN (1,2)",
		"argMax(value, timestamp)",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestTopKSelectedSeriesCarriesLabelsAndMetricConstraint(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}

	sql, ok := selectedSeriesSQL(cfg, matchers, 1000, 2000, []string{"id", "metric_name", "labels_json"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	for _, want := range []string{
		"`default`.`series`",
		"`default`.`label_index`",
		"any(metric_name) AS metric_name",
		"any(labels_json) AS labels_json",
		"team_id = 0",
		"metric_name = 'http_requests_total'",
		"label_name = 'status'",
		"label_value = '200'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}
