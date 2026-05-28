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
