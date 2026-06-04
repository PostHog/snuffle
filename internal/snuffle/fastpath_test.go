package snuffle

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

func TestOrVectorSelectorsFlattensSelectorUnion(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (job) ((up{job="api"} or up{job="worker"} or process_start_time_seconds{job="api"}))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}

	selectors, ok := orVectorSelectors(aggregate.Expr)
	if !ok {
		t.Fatal("orVectorSelectors returned ok=false")
	}
	if len(selectors) != 3 {
		t.Fatalf("selector count = %d, want 3", len(selectors))
	}
}

func TestFastAggregateRangeUnionRejectsLargeUnions(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job="0"} or up{job="1"} or up{job="2"} or up{job="3"} or up{job="4"} or up{job="5"} or up{job="6"} or up{job="7"} or up{job="8"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}
	selectors, ok := orVectorSelectors(aggregate.Expr)
	if !ok {
		t.Fatal("orVectorSelectors returned ok=false")
	}
	if len(selectors) <= 8 {
		t.Fatalf("selector count = %d, want more than fast-path cap", len(selectors))
	}
}

func TestNestedCountRangeSQLUsesSeriesBounds(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}
	inner, ok := outer.Expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("inner expr is %T, want *parser.AggregateExpr", outer.Expr)
	}
	selector, ok := inner.Expr.(*parser.VectorSelector)
	if !ok {
		t.Fatalf("selector expr is %T, want *parser.VectorSelector", inner.Expr)
	}

	start := time.Unix(1778398980, 0).UTC()
	end := time.Unix(1778402580, 0).UTC()
	step := time.Minute
	stepMillis := step.Milliseconds()
	source, mint, maxt, ok := selectorRangeGridSource(cfg, selector, start, end, step)
	if !ok {
		t.Fatal("selectorRangeGridSource returned ok=false")
	}
	selectedSeries, ok := selectedSeriesSQL(cfg, selector.LabelMatchers, mint, maxt, []string{"id", "min_time", "max_time"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	sql, ok := nestedCountRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, selectedSeries, source.start, start, stepMillis, steps, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"min(min_time) AS min_time",
		"max(max_time) AS max_time",
		"`default`.`series`",
		"`default`.`label_index`",
		"range(toUInt64(61)) AS step_idx",
		"max_time >= fromUnixTimestamp64Milli(",
		"min_time <= fromUnixTimestamp64Milli(",
		"group_intervals AS",
		"groupArray((toUnixTimestamp64Milli(min_time), toUnixTimestamp64Milli(max_time))) AS active_ranges",
		"GROUP BY `__group_0`",
		"arrayExists(x ->",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"metrics_samples", "timeSeriesLastToGrid", "sample_value", "id IN (SELECT id FROM selected_series)"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountTimestampSamplesRangeSQLUsesEvalTimestamps(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	stepMillis := time.Minute.Milliseconds()
	sql, ok := nestedCountTimestampSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 61, 5*time.Minute)
	if !ok {
		t.Fatal("nestedCountTimestampSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name = 'node_cpu_seconds_total'",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"`default`.`samples` ANY LEFT JOIN group_labels USING id",
		"toFloat64(uniqExact(ifNull(`__group_0`, ''))) AS value",
		"modulo(toUnixTimestamp64Milli(timestamp) - 1778398980000, 60000) = 0",
		nonStaleSampleSQL("value"),
		"GROUP BY ts ORDER BY ts",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"active_ids AS", "range(toUInt64", "timeSeriesLastToGrid", "metrics_series"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountTimestampSamplesRangeSQLBucketsUnalignedEvalTimes(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	evalStart := time.Unix(1778398987, 0).UTC()
	outputStart := time.Unix(1778398999, 0).UTC()
	sql, ok := nestedCountTimestampSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, evalStart, outputStart, time.Minute.Milliseconds(), 61, 5*time.Minute)
	if !ok {
		t.Fatal("nestedCountTimestampSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"step_map AS",
		"FROM numbers(toUInt64(61))",
		"intDiv(toInt64(1778398987000) + toInt64(number) * 60000, 15000) * 15000 AS bucket_ms",
		"active_samples ON active_samples.timestamp = fromUnixTimestamp64Milli(step_map.bucket_ms, 'UTC')",
		"SELECT toInt64(1778398999000) + toInt64(number) * 60000 AS ts",
		"label_name = 'ready'",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"modulo(toUnixTimestamp64Milli(timestamp)", "range(toUInt64"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestExactBucketRangeSelectorRequiresAlignedRemoteWriteRange(t *testing.T) {
	cfg := Config{RemoteWriteInterval: 15 * time.Second}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`usage_user{hostname="host_42"}`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	selector := expr.(*parser.VectorSelector)
	start := time.Unix(1451607900, 0).UTC()
	end := time.Unix(1451608185, 0).UTC()

	if !exactBucketRangeSelector(cfg, selector, start, end, 15*time.Second) {
		t.Fatal("aligned range should use exact bucket path")
	}
	if exactBucketRangeSelector(cfg, selector, start.Add(time.Millisecond), end, 15*time.Second) {
		t.Fatal("unaligned start should not use exact bucket path")
	}
	if exactBucketRangeSelector(cfg, selector, start, end, 30*time.Second) {
		t.Fatal("non-remote-write step should not use exact bucket path")
	}

	selector.OriginalOffset = time.Minute
	if exactBucketRangeSelector(cfg, selector, start, end, 15*time.Second) {
		t.Fatal("offset selector should not use exact bucket path")
	}
}

func TestLastGridExprTurnsStaleSamplesIntoNulls(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := time.Unix(160, 0).UTC()

	sql := lastGridExpr(start, end, 15*time.Second, 5*time.Minute)
	for _, want := range []string{
		"timeSeriesLastToGrid",
		"arrayMap(x -> if(isNull(x) OR",
		"reinterpretAsUInt64(assumeNotNull(x))",
		"NULL, x)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestMetricsQLRunningSumParses(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	if _, err := p.ParseExpr(`quantile(1, running_sum(delta(node_cpu_seconds_total{ready=~"true"}[1m])))`); err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
}

func TestAggregateRangeSourceSQLWrapsRunningSum(t *testing.T) {
	cfg := Config{
		LookbackDelta: 5 * time.Minute,
	}
	s := newServer(cfg)
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`quantile(1, running_sum(delta(node_cpu_seconds_total{ready=~"true"}[1m])))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)

	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, time.Unix(1700000000, 0).UTC(), time.Unix(1700000060, 0).UTC(), 15*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if !source.runningSumWrap {
		t.Fatal("running_sum should mark the range source for cumulative wrapping")
	}
	if selector == nil || selector.Name != "node_cpu_seconds_total" {
		t.Fatalf("selector = %#v", selector)
	}
	if mint != 1699999940000 || maxt != 1700000060000 {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}
	if !strings.Contains(source.gridExpr, "timeSeriesDeltaToGrid") {
		t.Fatalf("grid expression does not use delta grid:\n%s", source.gridExpr)
	}

	sql := rangeSourcePerSeriesSQL(source, "SELECT id, timestamp, value FROM samples")
	for _, want := range []string{
		"timeSeriesDeltaToGrid",
		"arrayCumSum",
		"assumeNotNull",
		"if(seen = 0, NULL, running_sum)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if got := strings.Count(sql, "timeSeriesDeltaToGrid"); got != 1 {
		t.Fatalf("delta grid expression count = %d, want 1:\n%s", got, sql)
	}
}

func TestRewriteImplicitMetricsQLSubquerySteps(t *testing.T) {
	query := `quantile(1, sum_over_time(delta(foo{label="[5m]"}[1m])[5m]))`
	got := rewriteImplicitMetricsQLSubquerySteps(query, 30*time.Second)
	want := `quantile(1, sum_over_time(delta(foo{label="[5m]"}[1m])[5m:30s]))`
	if got != want {
		t.Fatalf("rewrite = %q, want %q", got, want)
	}
}

func TestAggregateRangeSourceSQLWrapsSumOverTimeSubquery(t *testing.T) {
	cfg := Config{
		LookbackDelta: 5 * time.Minute,
	}
	s := newServer(cfg)
	expr, rewritten, err := s.parseFastRangeExpr(`quantile(1, sum_over_time(delta(node_cpu_seconds_total{ready=~"true"}[1m])[5m]))`, 30*time.Second)
	if err != nil {
		t.Fatalf("parseFastRangeExpr returned error: %v", err)
	}
	if !rewritten {
		t.Fatal("parseFastRangeExpr should rewrite the MetricsQL implicit subquery step")
	}
	aggregate := expr.(*parser.AggregateExpr)

	start := time.Unix(1700000000, 0).UTC()
	end := time.Unix(1700000060, 0).UTC()
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, start, end, 30*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if source.sumOverTime == nil {
		t.Fatal("sum_over_time over a subquery should mark the range source for rollup wrapping")
	}
	if !source.gridStart.Equal(start.Add(-5*time.Minute)) || source.gridStep != 30*time.Second {
		t.Fatalf("input grid = start %s step %s", source.gridStart, source.gridStep)
	}
	if selector == nil || selector.Name != "node_cpu_seconds_total" {
		t.Fatalf("selector = %#v", selector)
	}
	if mint != 1699999640000 || maxt != 1700000060000 {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}

	sql := rangeSourcePerSeriesSQL(source, "SELECT id, timestamp, value FROM samples")
	for _, want := range []string{
		"timeSeriesDeltaToGrid",
		"arraySlice",
		"arraySum",
		"range(toUInt64(3))",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestNestedCountSamplesInstantSQLUsesLookbackWindow(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	sql, ok := nestedCountSamplesInstantSQL(cfg, selector.LabelMatchers, inner.Grouping, 1778398680000, 1778398980000, 1778398980000)
	if !ok {
		t.Fatal("nestedCountSamplesInstantSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name = 'node_cpu_seconds_total'",
		"timestamp >= fromUnixTimestamp64Milli(1778398680000, 'UTC') AND timestamp <= fromUnixTimestamp64Milli(1778398980000, 'UTC')",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"active_ids AS",
		"argMax(value, timestamp)",
		nonStaleSampleSQL("value"),
		"active_ids ANY LEFT JOIN group_labels USING id",
		"toFloat64(uniqExact(ifNull(`__group_0`, ''))) AS value",
		"SELECT toInt64(1778398980000) AS ts",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"range(toUInt64", "timeSeriesLastToGrid", "metrics_series"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestLatestSamplesForSelectedSeriesSQLUsesLookbackWindow(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		RemoteWriteInterval: 15 * time.Second,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`http_requests_total{job="api"}`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	selector := expr.(*parser.VectorSelector)

	sql := latestSamplesForSelectedSeriesSQL(cfg, selector.LabelMatchers, 1000, 2000)
	for _, want := range []string{
		"argMax(value, timestamp)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name = 'http_requests_total'",
		"id IN (SELECT id FROM selected_series)",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "timestamp = fromUnixTimestamp64Milli") {
		t.Fatalf("instant latest SQL should not require one exact sample bucket:\n%s", sql)
	}
}

func TestLatestSamplesForSelectedSeriesSQLSkipsSelectedIDsForMetricOnlyMatcher(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		RemoteWriteInterval: 15 * time.Second,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`http_requests_total`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	selector := expr.(*parser.VectorSelector)

	sql := latestSamplesForSelectedSeriesSQL(cfg, selector.LabelMatchers, 1000, 2000)
	for _, want := range []string{
		"argMax(value, timestamp)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name = 'http_requests_total'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "id IN (SELECT id FROM selected_series)") {
		t.Fatalf("metric-only latest SQL should use the sample metric_name key directly:\n%s", sql)
	}
}

func TestPostHogSampleGroupSQLSupportsMapLabels(t *testing.T) {
	_, _, _, ok := postHogSampleGroupSQL([]string{"service_name", "__name__"})
	if !ok {
		t.Fatal("expected physical posthog grouping labels to be supported")
	}
	_, _, perIDSelects, ok := postHogSampleGroupSQL([]string{"region"})
	if !ok {
		t.Fatal("expected ordinary posthog labels to be grouped from maps")
	}
	if got := strings.Join(perIDSelects, ", "); !strings.Contains(got, "attributes_map_str['region__str']") || !strings.Contains(got, "resource_attributes['region']") {
		t.Fatalf("expected map-backed group expression, got %s", got)
	}
}

func TestNestedCountBitmapRangeSQLUsesPostingBitmaps(t *testing.T) {
	cfg := Config{
		CHDatabase:         "default",
		LabelPostingsTable: "label_postings",
		ActivityTable:      "series_activity",
		LookbackDelta:      5 * time.Minute,
		TeamID:             42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	stepMillis := time.Minute.Milliseconds()
	sql, ok := nestedCountBitmapRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 61, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountBitmapRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`label_postings`",
		"`default`.`series_activity`",
		"metric_ids AS",
		"selected_ids AS",
		"active_by_step AS",
		"active_selected AS",
		"group_label_values AS",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"groupBitmapOrState(ids)",
		"bitmapAndCardinality",
		"range(toUInt64(61))",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"metrics_samples", "timeSeriesLastToGrid", "metrics_series", "metrics_label_index", "type"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountSamplesRangeSQLUsesLastGridAndLabelIndex(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	stepMillis := time.Minute.Milliseconds()
	sql, ok := nestedCountSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 61, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name = 'node_cpu_seconds_total'",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"group_labels AS",
		"per_series AS",
		"timeSeriesLastToGrid",
		"arrayMap(x -> if(isNull(x) OR",
		"active_groups AS",
		"arrayEnumerate(vals) AS idx",
		nonStaleNullableSampleSQL("sample_value"),
		"ANY LEFT JOIN",
		"GROUP BY idx, `__group_0`",
		"toFloat64(count()) AS value",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"series_activity", "label_postings", "groupBitmap", "active_ids AS", "argMax(value, ts)", "range(toUInt64"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountSelectedSamplesRangeSQLUsesLastGridFallback(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count({__name__=~"node_.*", ready=~"true"}) by (cpu, mode))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	end := time.Unix(1778402580, 0).UTC()
	step := time.Minute
	stepMillis := step.Milliseconds()
	source, mint, maxt, ok := selectorRangeGridSource(cfg, selector, start, end, step)
	if !ok {
		t.Fatal("selectorRangeGridSource returned ok=false")
	}
	selectedSeries, ok := selectedSeriesSQL(cfg, selector.LabelMatchers, mint, maxt, []string{"id", "min_time", "max_time"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	sql, ok := nestedCountSelectedSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, selectedSeries, source.start, start, stepMillis, steps, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountSelectedSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"selected_series AS",
		"`default`.`series`",
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name",
		"min(min_time) AS min_time",
		"max(max_time) AS max_time",
		"max_time >= fromUnixTimestamp64Milli(",
		"min_time <= fromUnixTimestamp64Milli(",
		"id IN (SELECT id FROM selected_series)",
		"label_name IN ('cpu', 'mode')",
		"group_labels AS",
		"per_series AS",
		"timeSeriesLastToGrid",
		"active_groups AS",
		"arrayEnumerate(vals) AS idx",
		nonStaleNullableSampleSQL("sample_value"),
		"GROUP BY idx, `__group_0`, `__group_1`",
		"toFloat64(count()) AS value",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"series_activity", "label_postings", "groupBitmap", "active_ids AS", "argMax(value, ts)", "range(toUInt64"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}
