package snuffle

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestSeriesExprBranchesFlattenSelectorUnion(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (job) ((up{job="api"} or up{job="worker"} or process_start_time_seconds{job="api"}))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}

	branches, ok := seriesExprBranches(aggregate.Expr)
	if !ok {
		t.Fatal("seriesExprBranches returned ok=false")
	}
	if len(branches) != 3 {
		t.Fatalf("branch count = %d, want 3", len(branches))
	}
	for _, branch := range branches {
		if branch.kind != seriesExprSelector || !branch.transform.identity() {
			t.Fatalf("plain selector branch parsed as %#v", branch)
		}
	}
}

func TestSeriesExprBranchesRejectMatchingModifiers(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	for _, query := range []string{
		`up{job="api"} or on (job) up{job="worker"}`,
		`up{job="api"} or ignoring (instance) up{job="worker"}`,
	} {
		expr, err := p.ParseExpr(query)
		if err != nil {
			t.Fatalf("ParseExpr(%q) returned error: %v", query, err)
		}
		if branches, ok := seriesExprBranches(expr); ok {
			t.Fatalf("seriesExprBranches(%q) = %#v, want rejection", query, branches)
		}
	}
}

func TestPostHogAggregateUnionPlanFactorsSingleVaryingLabel(t *testing.T) {
	cfg := Config{
		CHDatabase:    "default",
		SchemaLayout:  "posthog",
		SeriesTable:   "metric_series2",
		SamplesTable:  "metrics2",
		MaxSeries:     1000,
		LookbackDelta: 5 * time.Minute,
	}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (hostname) (usage_user{hostname="host_0"} / 5 or usage_user{hostname="host_1"} / 5)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	start := time.Unix(1700000000, 0).UTC()
	plan, ok := s.postHogAggregateUnionPlan(aggregate, start, start.Add(time.Hour), time.Minute)
	if !ok {
		t.Fatal("postHogAggregateUnionPlan returned ok=false")
	}

	sql := postHogAggregateUnionSQL(cfg, plan, aggregate.Grouping, start, time.Minute.Milliseconds(), "sum(sample_value)")
	for _, want := range []string{
		"WITH selected_series AS (SELECT series_fingerprint AS series_id, metric_name, service_name, resource_attributes, attributes, if(mapContains(resource_attributes, 'hostname'), resource_attributes['hostname'], attributes['hostname']) AS `__group_0` FROM `default`.`metric_series2` WHERE",
		"timeSeriesLastToGrid",
		"IN ('host_0','host_1')",
		"metric_name = 'usage_user'",
		"series_fingerprint IN (SELECT series_id FROM selected_series)",
		"GROUP BY series_id",
		"INNER JOIN selected_series USING series_id",
		"x / 5",
		"`default`.`metrics2`",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("union SQL does not contain %q:\n%s", want, sql)
		}
	}
	// the varying hostname expression must be factored into one IN lookup on
	// the series table, not one map lookup per branch
	if got := strings.Count(sql, "mapContains(resource_attributes, 'hostname')"); got != 2 {
		t.Fatalf("hostname lookup count = %d, want 2 (group expression and IN filter):\n%s", got, sql)
	}
	if strings.Contains(sql, "attributes_map_str") {
		t.Fatalf("union SQL must not read the metrics1 attributes_map_str column:\n%s", sql)
	}
	if got := strings.Count(sql, "FROM `default`.`metrics2`"); got != 1 {
		t.Fatalf("samples table scan count = %d, want 1:\n%s", got, sql)
	}
	if got := strings.Count(sql, "FROM `default`.`metric_series2`"); got != 1 {
		t.Fatalf("series table scan count = %d, want 1:\n%s", got, sql)
	}
}

func TestPostHogAggregateUnionPlanRejectsMixedTransforms(t *testing.T) {
	s := &Server{cfg: Config{SchemaLayout: "posthog", SeriesTable: "metric_series2", SamplesTable: "metrics2", LookbackDelta: 5 * time.Minute}}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (hostname) (usage_user{hostname="host_0"} / 5 or usage_user{hostname="host_1"} / 6)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	start := time.Unix(1700000000, 0).UTC()
	if plan, ok := s.postHogAggregateUnionPlan(expr.(*parser.AggregateExpr), start, start.Add(time.Hour), time.Minute); ok {
		t.Fatalf("mixed scalar transforms should be rejected, got plan %#v", plan)
	}
}

func TestPostHogAggregateUnionPlanRejectsPlainSingleSelector(t *testing.T) {
	s := &Server{cfg: Config{SchemaLayout: "posthog", SeriesTable: "metric_series2", SamplesTable: "metrics2", LookbackDelta: 5 * time.Minute}}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (region) (usage_user{hostname="host_0"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	start := time.Unix(1700000000, 0).UTC()
	if plan, ok := s.postHogAggregateUnionPlan(expr.(*parser.AggregateExpr), start, start.Add(time.Hour), time.Minute); ok {
		t.Fatalf("plain single selector should use the dedicated path, got plan %#v", plan)
	}
}

func TestPostHogUnionMatcherConditionFallsBackToOr(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`up{job="api",env="prod"} or up{job="worker",env="dev"}`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	branches, ok := seriesExprBranches(expr)
	if !ok || len(branches) != 2 {
		t.Fatalf("seriesExprBranches ok=%v len=%d, want 2 branches", ok, len(branches))
	}
	selectors := []*parser.VectorSelector{branches[0].selector, branches[1].selector}
	condition, ok := postHogUnionMatcherCondition(selectors, postHogSeriesLabelExpr)
	if !ok {
		t.Fatal("postHogUnionMatcherCondition returned ok=false")
	}
	// two labels vary, so the IN factoring must not apply
	if !strings.Contains(condition, " OR ") {
		t.Fatalf("condition = %q, want OR of branch conjunctions", condition)
	}
	for _, want := range []string{"'api'", "'worker'", "'prod'", "'dev'"} {
		if !strings.Contains(condition, want) {
			t.Fatalf("condition %q does not contain %s", condition, want)
		}
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
		"toFloat64(uniq(ifNull(`__group_0`, ''))) AS value",
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

func TestLatestSamplesForSelectedSeriesUnionSQLFiltersSelectedIDsAndMetricNames(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		RemoteWriteInterval: 15 * time.Second,
	}

	sql := latestSamplesForSelectedSeriesUnionSQL(cfg, []string{"up", "process_start_time_seconds"}, 1000, 2000)
	for _, want := range []string{
		"argMax(value, timestamp)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name IN ('up','process_start_time_seconds')",
		"id IN (SELECT id FROM selected_series)",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestSelectedSeriesExactMatcherUnionSQLUsesSingleLabelIndexScan(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (topic, consumer_group) (
		warpstream_consumer_group_max_offset{topic="a",consumer_group="a_ws"} / 5
		or
		warpstream_consumer_group_max_offset{topic="b",consumer_group="b_ws"} / 5
	)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	s := &Server{cfg: Config{LookbackDelta: 5 * time.Minute}}
	branches, ok := s.instantSeriesExprBranches(aggregate.Expr, time.Unix(1700000000, 0).UTC())
	if !ok {
		t.Fatal("instantSeriesExprBranches returned ok=false")
	}
	selectors := []*parser.VectorSelector{branches[0].selector, branches[1].selector}

	sql, ok := selectedSeriesExactMatcherUnionSQL(cfg, selectors, []string{"id"}, 0, 0)
	if !ok {
		t.Fatal("selectedSeriesExactMatcherUnionSQL returned ok=false")
	}
	for _, want := range []string{
		"`default`.`label_index` AS li",
		"INNER JOIN values('branch UInt32, full_mask UInt64, match_bit UInt64, metric_name String, label_name String, label_value String'",
		"(0, 3, 1, 'warpstream_consumer_group_max_offset', 'topic', 'a')",
		"(0, 3, 2, 'warpstream_consumer_group_max_offset', 'consumer_group', 'a_ws')",
		"li.metric_name = mr.metric_name",
		"li.label_name = mr.label_name",
		"li.label_value = mr.label_value",
		"metric_name = 'warpstream_consumer_group_max_offset'",
		"label_name IN ('topic','consumer_group')",
		"label_value IN ('a','a_ws','b','b_ws')",
		"groupBitOr(mr.match_bit) = full_mask",
		"topic",
		"consumer_group",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "`default`.`series`") {
		t.Fatalf("id-only exact matcher union should not scan series table:\n%s", sql)
	}
}

func TestSelectedSeriesExactMatcherUnionSQLFactorsSingleLabelValues(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (hostname) (
		usage_user{hostname="host_0"} / 5
		or
		usage_user{hostname="host_1"} / 5
	)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	selectors := branchSelectors(t, aggregate.Expr)

	sql, ok := selectedSeriesExactMatcherUnionSQL(cfg, selectors, []string{"id"}, 0, 0)
	if !ok {
		t.Fatal("selectedSeriesExactMatcherUnionSQL returned ok=false")
	}
	for _, want := range []string{
		"`default`.`label_index`",
		"metric_name = 'usage_user'",
		"label_name = 'hostname'",
		"label_value IN ('host_0','host_1')",
		"GROUP BY id",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"values(", "`default`.`series`", "uniqExact"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestSelectedSeriesSQLOnlyReadsRequestedColumns(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "usage_user"),
		labels.MustNewMatcher(labels.MatchEqual, "hostname", "host_42"),
	}
	sql, ok := selectedSeriesSQL(cfg, matchers, 1000, 2000, []string{"id"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	for _, want := range []string{
		"SELECT id FROM `default`.`label_index`",
		"metric_name = 'usage_user'",
		"label_name = 'hostname'",
		"label_value = 'host_42'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"`default`.`series`", "labels_json", "min_time", "max_time"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestSelectedSeriesIDSQLIntersectsLabelIndexMatchers(t *testing.T) {
	cfg := Config{CHDatabase: "default", SeriesTable: "series", LabelIndexTable: "label_index", TeamID: 42}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "usage_user"),
		labels.MustNewMatcher(labels.MatchRegexp, "type", "offline|online"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "clickhouse"),
		labels.MustNewMatcher(labels.MatchNotEqual, "shard", "test"),
	}

	sql, ok := selectedSeriesSQL(cfg, matchers, 1000, 2000, []string{"id"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	for _, want := range []string{
		"label_name = 'job' AND label_value = 'clickhouse'",
		"id IN (SELECT id FROM `default`.`label_index`",
		"label_name = 'type' AND label_value IN ('offline','online')",
		"id NOT IN (SELECT id FROM `default`.`label_index`",
		"label_name = 'shard' AND label_value = 'test'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestSelectedSeriesExactMatcherUnionSQLRejectsNonExactMatchers(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job=~"api|worker"} or up{job="db"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	selectors := branchSelectors(t, aggregate.Expr)
	if sql, ok := selectedSeriesExactMatcherUnionSQL(Config{}, selectors, []string{"id"}, 0, 0); ok {
		t.Fatalf("selectedSeriesExactMatcherUnionSQL returned ok=true with SQL:\n%s", sql)
	}
}

func TestExactMetricNamesForSelectorsRequiresExactMetrics(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job="api"} or process_start_time_seconds{job="api"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	names := exactMetricNamesForSelectors(branchSelectors(t, aggregate.Expr))
	if got, want := strings.Join(names, ","), "up,process_start_time_seconds"; got != want {
		t.Fatalf("exactMetricNamesForSelectors = %q, want %q", got, want)
	}

	expr, err = p.ParseExpr(`sum({__name__=~"up|process_start_time_seconds", job="api"} or up{job="worker"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate = expr.(*parser.AggregateExpr)
	if names := exactMetricNamesForSelectors(branchSelectors(t, aggregate.Expr)); names != nil {
		t.Fatalf("exactMetricNamesForSelectors = %#v, want nil", names)
	}
}

func TestCommonExactMetricMatchersRequiresOneMetric(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job="api"} or up{job="worker"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	if matchers := commonExactMetricMatchers(branchSelectors(t, aggregate.Expr)); matchers == nil {
		t.Fatal("commonExactMetricMatchers returned nil for same-metric union")
	}

	expr, err = p.ParseExpr(`sum(up{job="api"} or process_start_time_seconds{job="worker"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate = expr.(*parser.AggregateExpr)
	if matchers := commonExactMetricMatchers(branchSelectors(t, aggregate.Expr)); matchers != nil {
		t.Fatalf("commonExactMetricMatchers = %#v, want nil for mixed metrics", matchers)
	}
}

func branchSelectors(t *testing.T, expr parser.Expr) []*parser.VectorSelector {
	t.Helper()
	branches, ok := seriesExprBranches(expr)
	if !ok {
		t.Fatal("seriesExprBranches returned ok=false")
	}
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	for _, branch := range branches {
		selectors = append(selectors, branch.selector)
	}
	return selectors
}

func TestPostHogQueryPlanReadsSeriesTableOnlyForMapLabels(t *testing.T) {
	cfg := Config{CHDatabase: "default", SchemaLayout: "posthog", SeriesTable: "metric_series2", SamplesTable: "metrics2", MaxSeries: 10}
	nameMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")}

	plan := newPostHogQueryPlan(cfg, nameMatchers, []string{"service_name", "__name__"}, 1000, 2000, false)
	if plan.useSeries {
		t.Fatal("physical grouping labels must not read the series table")
	}
	if got := strings.Join(plan.perSeriesGroupSelects(), ", "); got != "any(service_name) AS `__group_0`, any(metric_name) AS `__group_1`" {
		t.Fatalf("per-series group selects = %s", got)
	}
	if got := strings.Join(plan.sampleWhere(), " AND "); strings.Contains(got, "selected_series") {
		t.Fatalf("samples filter must not reference selected_series: %s", got)
	}

	plan = newPostHogQueryPlan(cfg, nameMatchers, []string{"region"}, 1000, 2000, false)
	if !plan.useSeries {
		t.Fatal("map-backed grouping labels must read the series table")
	}
	if !strings.Contains(plan.seriesSQL, "if(mapContains(resource_attributes, 'region'), resource_attributes['region'], attributes['region']) AS `__group_0`") {
		t.Fatalf("expected map-backed group expression in series SQL, got %s", plan.seriesSQL)
	}
	if !strings.Contains(plan.seriesSQL, "last_seen >= "+chTimeMillis(1000-postHogLabelWindowMillis)) || !strings.Contains(plan.seriesSQL, "LIMIT 1 BY series_id LIMIT 10") {
		t.Fatalf("series SQL must bound by last_seen and collapse duplicates: %s", plan.seriesSQL)
	}
	where := strings.Join(plan.sampleWhere(), " AND ")
	if !strings.Contains(where, "series_fingerprint IN (SELECT series_id FROM selected_series)") || strings.Contains(where, "metric_name IN (SELECT") {
		t.Fatalf("samples filter with an exact metric name = %s", where)
	}

	plan = newPostHogQueryPlan(cfg, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "region", "eu")}, nil, 1000, 2000, false)
	if where := strings.Join(plan.sampleWhere(), " AND "); !strings.Contains(where, "metric_name IN (SELECT metric_name FROM selected_series)") {
		t.Fatalf("samples filter without a metric name must follow the selected metric names: %s", where)
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
