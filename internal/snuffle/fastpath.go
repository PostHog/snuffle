package snuffle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func (s *Server) tryFastInstantQuery(ctx context.Context, query string, evalTime time.Time) (queryData, bool, error) {
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		return queryData{}, false, nil
	}
	if s.cfg.postHogSchemaLayout() {
		type selectorRange struct {
			selector *parser.VectorSelector
			matrix   time.Duration
		}
		var selectors []selectorRange
		parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
			if selector, ok := node.(*parser.VectorSelector); ok {
				var matrix time.Duration
				if len(path) > 0 {
					if parent, ok := path[len(path)-1].(*parser.MatrixSelector); ok {
						matrix = parent.Range
					}
				}
				selectors = append(selectors, selectorRange{selector: selector, matrix: matrix})
			}
			return nil
		})
		for _, item := range selectors {
			window, ok := selectorWindowFor(item.selector, evalTime, s.cfg.LookbackDelta)
			if !ok {
				return queryData{}, false, nil
			}
			virtual, err := s.postHogSelectsHistogram(ctx, window.mint-item.matrix.Milliseconds(), window.maxt, item.selector.LabelMatchers)
			if err != nil {
				return queryData{}, false, err
			}
			if virtual {
				return queryData{}, false, nil
			}
		}
	}
	expr, ok := unwrapInstantDefaultRollups(expr)
	if !ok {
		return queryData{}, false, nil
	}

	if aggregate, ok := expr.(*parser.AggregateExpr); ok {
		if s.cfg.postHogSchemaLayout() {
			if data, ok, err := s.tryPostHogNestedCountInstantQuery(ctx, aggregate, evalTime); ok || err != nil {
				return data, ok, err
			}
			if data, ok, err := s.tryFastAggregate(ctx, aggregate, evalTime); ok || err != nil {
				return data, ok, err
			}
			if data, ok, err := s.tryFastTopK(ctx, aggregate, evalTime); ok || err != nil {
				return data, ok, err
			}
			return queryData{}, false, nil
		}
		if data, ok, err := s.tryFastNestedCountInstantQuery(ctx, aggregate, evalTime); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastAggregate(ctx, aggregate, evalTime); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastTopK(ctx, aggregate, evalTime); ok || err != nil {
			return data, ok, err
		}
	}
	return queryData{}, false, nil
}

const maxAggregateUnionSelectors = 1024

func (s *Server) tryFastAggregate(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "value")
	if !ok {
		return queryData{}, false, nil
	}
	if s.cfg.postHogSchemaLayout() {
		if data, ok, err := s.tryPostHogInstantAggregateUnionQuery(ctx, expr, evalTime); ok || err != nil {
			return data, ok, err
		}
		selector, ok := expr.Expr.(*parser.VectorSelector)
		if !ok {
			return queryData{}, false, nil
		}
		window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
		if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		return s.tryPostHogInstantAggregate(ctx, expr, selector, window, evalTime, aggSQL)
	}
	if data, ok, err := s.tryFastInstantSeriesExprAggregate(ctx, expr, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}
	selector, ok := expr.Expr.(*parser.VectorSelector)
	if !ok {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}

	if data, ok, err := s.tryPostHogInstantAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}
	if data, ok, err := s.trySampleLabelIndexInstantAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}
	if data, ok, err := s.tryLabelIndexAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}

	return queryData{}, false, nil
}

func (s *Server) tryPostHogInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	if !s.cfg.postHogSchemaLayout() {
		return queryData{}, false, nil
	}
	if !postHogGroupingSupported(expr.Grouping) {
		return queryData{}, false, nil
	}
	plan := newPostHogQueryPlan(s.cfg, selector.LabelMatchers, expr.Grouping, window.mint, window.maxt, false)

	perIDSelect := []string{
		"series_fingerprint AS series_id",
		"argMax(value, timestamp) AS value",
	}
	perIDSelect = append(perIDSelect, plan.perSeriesGroupSelects()...)
	perID := fmt.Sprintf(
		"SELECT %s FROM %s GROUP BY series_id",
		strings.Join(perIDSelect, ", "),
		postHogSampleRowsFrom(s.cfg, plan.sampleWhere(), false),
	)

	perID = fmt.Sprintf("SELECT * FROM (%s) WHERE %s", perID, nonStaleSampleSQL("value"))

	groupBy := plan.groupAliases()
	selectParts := make([]string, 0, len(groupBy)+2)
	selectParts = append(selectParts, groupBy...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), plan.joinSeries(perID))
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(plan.withSelectedSeries(sql), s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

// postHogAggregateUnionPlan describes one ClickHouse query for an
// `agg by (...) (branch or branch or ...)` expression over the PostHog layout,
// where every branch is an equivalent selector or range-function expression.
type postHogAggregateUnionPlan struct {
	gridVals string
	where    []string
	plan     *postHogQueryPlan
}

func (s *Server) tryPostHogInstantAggregateUnionQuery(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "sample_value")
	if !ok {
		return queryData{}, false, nil
	}
	plan, ok := s.postHogAggregateUnionPlan(expr, evalTime, evalTime, time.Second)
	if !ok {
		return queryData{}, false, nil
	}
	sql := postHogAggregateUnionSQL(s.cfg, plan, expr.Grouping, evalTime, time.Second.Milliseconds(), aggSQL)
	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) postHogAggregateUnionPlan(expr *parser.AggregateExpr, start, end time.Time, step time.Duration) (*postHogAggregateUnionPlan, bool) {
	branches, ok := seriesExprBranches(expr.Expr)
	if !ok || len(branches) == 0 || len(branches) > maxAggregateUnionSelectors {
		return nil, false
	}
	if len(branches) == 1 && branches[0].kind == seriesExprSelector && branches[0].transform.identity() {
		// the dedicated plain selector aggregate paths handle it
		return nil, false
	}

	first := branches[0]
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(first.node, start, end, step)
	if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
		return nil, false
	}
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	selectors = append(selectors, first.selector)
	for _, branch := range branches[1:] {
		if !first.equivalentTo(branch) || !postHogMatchersPushdownSafe(branch.selector.LabelMatchers) {
			return nil, false
		}
		branchSource, _, branchMint, branchMaxt, ok := s.aggregateRangeSourceSQL(branch.node, start, end, step)
		if !ok || branchMint != mint || branchMaxt != maxt || branchSource.gridExpr != source.gridExpr {
			return nil, false
		}
		selectors = append(selectors, branch.selector)
	}

	if !postHogGroupingSupported(expr.Grouping) {
		return nil, false
	}
	useSeries := postHogNeedsSeriesLookup(nil, expr.Grouping)
	for _, unionSelector := range selectors {
		if postHogNeedsSeriesLookup(unionSelector.LabelMatchers, nil) {
			useSeries = true
		}
	}
	plan := &postHogQueryPlan{cfg: s.cfg, grouping: expr.Grouping, mint: mint, maxt: maxt, useSeries: useSeries}

	where := []string{teamFilter(s.cfg)}
	where = append(where, sampleTimeFilters(s.cfg, mint, maxt)...)
	if useSeries {
		// Every label is on the series table, so the union condition selects
		// series there and the samples scan follows the selected fingerprints.
		seriesCondition, ok := postHogUnionMatcherCondition(selectors, postHogSeriesLabelExpr)
		if !ok {
			return nil, false
		}
		seriesWhere := postHogSeriesFilters(s.cfg, nil, mint, maxt)
		if seriesCondition != "" {
			seriesWhere = append(seriesWhere, seriesCondition)
		}
		plan.seriesSQL = postHogSelectedSeriesWhereSQL(s.cfg, seriesWhere, s.cfg.MaxSeries, plan.seriesGroupSelects())
		where = append(where,
			"series_fingerprint IN (SELECT series_id FROM selected_series)",
			"metric_name IN (SELECT metric_name FROM selected_series)",
		)
	} else {
		sampleCondition, ok := postHogUnionMatcherCondition(selectors, postHogSampleLabelExpr)
		if !ok {
			return nil, false
		}
		if sampleCondition != "" {
			where = append(where, sampleCondition)
		}
	}
	return &postHogAggregateUnionPlan{
		gridVals: transformGridValsSQL(source.gridExpr, first.transform),
		where:    where,
		plan:     plan,
	}, true
}

func postHogAggregateUnionSQL(cfg Config, plan *postHogAggregateUnionPlan, grouping []string, start time.Time, stepMillis int64, aggSQL string) string {
	groupAliases := plan.plan.groupAliases()
	perSeriesSelects := make([]string, 0, len(groupAliases)+2)
	perSeriesSelects = append(perSeriesSelects, "series_fingerprint AS series_id")
	perSeriesSelects = append(perSeriesSelects, plan.plan.perSeriesGroupSelects()...)
	perSeriesSelects = append(perSeriesSelects, plan.gridVals+" AS vals")
	perSeries := fmt.Sprintf(
		"SELECT %s FROM %s GROUP BY series_id",
		strings.Join(perSeriesSelects, ", "),
		postHogSampleRowsFrom(cfg, plan.where, false),
	)
	perSeries = fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(append(append([]string{}, groupAliases...), "vals"), ", "),
		plan.plan.joinSeries(perSeries),
	)

	selectParts := append([]string{}, groupAliases...)
	selectParts = append(selectParts, fmt.Sprintf("toInt64(%d) + (toInt64(idx) - 1) * %d AS ts", start.UnixMilli(), stepMillis))
	selectParts = append(selectParts, aggSQL+" AS value")
	groupByParts := append(append([]string{}, groupAliases...), "idx")

	sql := fmt.Sprintf(
		"SELECT %s FROM (%s) AS per_series ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE %s GROUP BY %s ORDER BY %s",
		strings.Join(selectParts, ", "),
		perSeries,
		nonStaleNullableSampleSQL("sample_value"),
		strings.Join(groupByParts, ", "),
		strings.Join(groupByParts, ", "),
	)
	return withMaxThreads(plan.plan.withSelectedSeries(sql), cfg.AggregateThreads)
}

// postHogUnionMatcherCondition builds one WHERE condition matching rows
// selected by any of the union's selectors, with labels resolved by labelExpr
// for the target table. Returns "" when a branch matches all rows.
func postHogUnionMatcherCondition(selectors []*parser.VectorSelector, labelExpr func(string) (string, bool)) (string, bool) {
	if condition, ok := postHogUnionInCondition(selectors, labelExpr); ok {
		return condition, true
	}
	branchConditions := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		conditions, ok := postHogSelectorMatcherConditions(selector.LabelMatchers, labelExpr)
		if !ok {
			return "", false
		}
		if len(conditions) == 0 {
			return "", true
		}
		branchConditions = append(branchConditions, "("+strings.Join(conditions, " AND ")+")")
	}
	if len(branchConditions) == 1 {
		return branchConditions[0], true
	}
	return "(" + strings.Join(branchConditions, " OR ") + ")", true
}

func postHogSelectorMatcherConditions(matchers []*labels.Matcher, labelExpr func(string) (string, bool)) ([]string, bool) {
	conditions := make([]string, 0, len(matchers))
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
			continue
		}
		condition, ok := postHogMatcherConditionWith(matcher, labelExpr)
		if !ok {
			return nil, false
		}
		conditions = append(conditions, condition)
	}
	return conditions, true
}

// postHogUnionInCondition recognizes selectors that differ on exactly one
// equality matcher and emits `common AND label_expr IN (...)` so the per-row
// label lookup runs once instead of once per branch.
func postHogUnionInCondition(selectors []*parser.VectorSelector, labelExpr func(string) (string, bool)) (string, bool) {
	if len(selectors) < 2 {
		return "", false
	}
	matcherMaps := make([]map[string]*labels.Matcher, len(selectors))
	for i, selector := range selectors {
		matcherMap := make(map[string]*labels.Matcher, len(selector.LabelMatchers))
		for _, matcher := range selector.LabelMatchers {
			if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
				continue
			}
			if _, dup := matcherMap[matcher.Name]; dup {
				return "", false
			}
			matcherMap[matcher.Name] = matcher
		}
		if i > 0 && len(matcherMap) != len(matcherMaps[0]) {
			return "", false
		}
		matcherMaps[i] = matcherMap
	}

	names := make([]string, 0, len(matcherMaps[0]))
	for name := range matcherMaps[0] {
		names = append(names, name)
	}
	sort.Strings(names)

	varying := ""
	common := make([]string, 0, len(names))
	for _, name := range names {
		firstMatcher := matcherMaps[0][name]
		same := true
		for _, matcherMap := range matcherMaps[1:] {
			other, ok := matcherMap[name]
			if !ok {
				return "", false
			}
			if other.Type != firstMatcher.Type || other.Value != firstMatcher.Value {
				same = false
			}
		}
		if same {
			condition, ok := postHogMatcherConditionWith(firstMatcher, labelExpr)
			if !ok {
				return "", false
			}
			common = append(common, condition)
			continue
		}
		if varying != "" {
			return "", false
		}
		for _, matcherMap := range matcherMaps {
			if matcherMap[name].Type != labels.MatchEqual {
				return "", false
			}
		}
		varying = name
	}
	if varying == "" {
		return "", false
	}

	values := make([]string, 0, len(selectors))
	seen := make(map[string]struct{}, len(selectors))
	for _, matcherMap := range matcherMaps {
		value := matcherMap[varying].Value
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, sqlString(value))
	}
	column, ok := labelExpr(varying)
	if !ok {
		return "", false
	}
	conditions := append(common, column+" IN ("+strings.Join(values, ",")+")")
	return "(" + strings.Join(conditions, " AND ") + ")", true
}

func (s *Server) tryLabelIndexAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	latest := latestSamplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt)

	return s.queryLabelIndexInstantAggregate(ctx, expr, selectedSeries, latest, selector.LabelMatchers, evalTime, aggSQL)
}

func (s *Server) tryFastInstantSeriesExprAggregate(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	branches, ok := s.instantSeriesExprBranches(expr.Expr, evalTime)
	if !ok || len(branches) == 0 || len(branches) > maxAggregateUnionSelectors {
		return queryData{}, false, nil
	}
	if len(branches) == 1 && branches[0].kind == instantSeriesSelector && branches[0].transform.identity() {
		return queryData{}, false, nil
	}

	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	first := branches[0]
	for _, branch := range branches {
		if !first.equivalentTo(branch) || !matchersPushdownSafe(branch.selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		selectors = append(selectors, branch.selector)
	}

	groupMatchers := first.selector.LabelMatchers
	selectedSeries, ok := selectedSeriesForInstantBranchesSQL(s.cfg, branches, selectors, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	if len(branches) > 1 {
		groupMatchers = commonExactMetricMatchers(selectors)
	}
	sourceSQL, sourceName := s.instantSeriesExprSourceSQL(first, selectors)
	if sourceSQL == "" {
		return queryData{}, false, nil
	}
	return s.queryInstantSeriesAggregate(ctx, expr, selectedSeries, sourceName, sourceSQL, groupMatchers, evalTime, aggSQL)
}

func (s *Server) queryLabelIndexInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selectedSeries, latest string, groupMatchers []*labels.Matcher, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	return s.queryInstantSeriesAggregate(ctx, expr, selectedSeries, "latest", latest, groupMatchers, evalTime, aggSQL)
}

func (s *Server) queryInstantSeriesAggregate(ctx context.Context, expr *parser.AggregateExpr, selectedSeries, sourceName, sourceSQL string, groupMatchers []*labels.Matcher, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, groupMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		sourceName + " AS (" + sourceSQL + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	source := aggregateSourceSQL(sourceName, expr.Grouping, groupJoin)
	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM %s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		source,
	)
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) trySampleLabelIndexInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	if s.cfg.postHogSchemaLayout() || groupingHasMetricName(expr.Grouping) {
		return queryData{}, false, nil
	}
	metric := exactMetricName(selector.LabelMatchers)
	if metric == "" || s.cfg.SamplesTable == "" || s.cfg.LabelIndexTable == "" {
		return queryData{}, false, nil
	}
	idFilters, ok := nonMetricSampleIDFiltersFromMatchers(s.cfg, metric, selector.LabelMatchers)
	if !ok {
		return queryData{}, false, nil
	}

	where := sampleBaseFilters(s.cfg, selector.LabelMatchers, window.mint, window.maxt)
	where = append(where, idFilters...)
	latest := fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value FROM %s WHERE %s GROUP BY id",
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	latest = fmt.Sprintf("SELECT * FROM (%s) WHERE %s", latest, nonStaleSampleSQL("value"))

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQLWithSelectedFilter(s.cfg, selector.LabelMatchers, expr.Grouping, false)
	withParts := []string{"latest AS (" + latest + ")"}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM latest%s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		groupJoin,
	)
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryFastNestedCountInstantQuery(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	inner, selector, ok := nestedCountSelector(expr)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	sql, ok := nestedCountSamplesInstantSQL(s.cfg, selector.LabelMatchers, inner.Grouping, window.mint, window.maxt, evalTime.UnixMilli())
	if !ok {
		return queryData{}, false, nil
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)
	data, err := s.queryNestedCountInstantValue(ctx, sql)
	return data, true, err
}

func (s *Server) tryPostHogNestedCountInstantQuery(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	inner, selector, ok := nestedCountSelector(expr)
	if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	if !postHogGroupingSupported(inner.Grouping) || len(inner.Grouping) == 0 {
		return queryData{}, false, nil
	}
	plan := newPostHogQueryPlan(s.cfg, selector.LabelMatchers, inner.Grouping, window.mint, window.maxt, false)
	perSeriesSelects := []string{
		"series_fingerprint AS series_id",
		"argMax(value, timestamp) AS value",
	}
	perSeriesSelects = append(perSeriesSelects, plan.perSeriesGroupSelects()...)
	perSeries := fmt.Sprintf(
		"SELECT %s FROM %s GROUP BY series_id",
		strings.Join(perSeriesSelects, ", "),
		postHogSampleRowsFrom(s.cfg, plan.sampleWhere(), false),
	)
	sql := fmt.Sprintf(
		"SELECT toInt64(%d) AS ts, toFloat64(uniq(%s)) AS count_value FROM %s WHERE %s",
		evalTime.UnixMilli(),
		postHogUniqGroupExpr(plan.groupAliases()),
		plan.joinSeries(perSeries),
		nonStaleSampleSQL("value"),
	)
	sql = withMaxThreads(plan.withSelectedSeries(sql), s.cfg.AggregateThreads)
	data, err := s.queryNestedCountInstantValue(ctx, sql)
	return data, true, err
}

func nestedCountSelector(expr *parser.AggregateExpr) (*parser.AggregateExpr, *parser.VectorSelector, bool) {
	if expr.Op != parser.COUNT || expr.Without || len(expr.Grouping) != 0 {
		return nil, nil, false
	}
	inner, ok := expr.Expr.(*parser.AggregateExpr)
	if !ok || inner.Op != parser.COUNT || inner.Without || len(inner.Grouping) == 0 {
		return nil, nil, false
	}
	if groupingHasMetricName(inner.Grouping) {
		return nil, nil, false
	}
	selector, ok := inner.Expr.(*parser.VectorSelector)
	if !ok {
		return nil, nil, false
	}
	return inner, selector, true
}

func nestedCountSamplesInstantSQL(cfg Config, matchers []*labels.Matcher, grouping []string, mint, maxt, evalMillis int64) (string, bool) {
	if cfg.SamplesTable == "" || cfg.LabelIndexTable == "" || len(grouping) != 1 || grouping[0] == labels.MetricName {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	idFilters, ok := nonMetricSampleIDFiltersFromMatchers(cfg, metric, matchers)
	if !ok {
		return "", false
	}

	sampleWhere := sampleBaseFilters(cfg, matchers, mint, maxt)
	sampleWhere = append(sampleWhere, idFilters...)

	groupColumn, groupLabels := nestedCountGroupLabelsSQL(cfg, metric, grouping[0])
	return fmt.Sprintf(
		"WITH group_labels AS (%s), active_ids AS (SELECT id FROM (SELECT id, argMax(value, timestamp) AS value FROM %s WHERE %s GROUP BY id) WHERE %s) SELECT toInt64(%d) AS ts, %s AS value FROM active_ids ANY LEFT JOIN group_labels USING id",
		groupLabels,
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
		nonStaleSampleSQL("value"),
		evalMillis,
		nestedCountDistinctGroupSQL(groupColumn),
	), true
}

func nestedCountDistinctGroupSQL(groupColumn string) string {
	return "toFloat64(uniq(ifNull(" + groupColumn + ", '')))"
}

func nestedCountGroupLabelsSQL(cfg Config, metric, groupName string) (string, string) {
	groupColumn := quoteIdent(groupAlias(0))
	return groupColumn, fmt.Sprintf(
		"SELECT id, label_value AS %s FROM %s WHERE %s AND metric_name = %s AND label_name = %s",
		groupColumn,
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		teamFilter(cfg),
		sqlString(metric),
		sqlString(groupName),
	)
}

func nonMetricSampleIDFiltersFromMatchers(cfg Config, metric string, matchers []*labels.Matcher) ([]string, bool) {
	var filters []string
	for _, m := range matchers {
		if m.Name == labels.MetricName || matcherIsNoop(m) {
			continue
		}
		membership, condition, ok := labelIndexMembershipCondition(m)
		if !ok {
			return nil, false
		}
		source := fmt.Sprintf(
			"SELECT id FROM %s WHERE %s AND metric_name = %s AND label_name = %s AND %s",
			tableName(cfg.CHDatabase, cfg.LabelIndexTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(m.Name),
			condition,
		)
		filters = append(filters, sampleIDMembershipFilters(cfg, membership, source)...)
	}
	return filters, true
}

func (s *Server) queryNestedCountInstantValue(ctx context.Context, sql string) (queryData, error) {
	var result []sampleResult
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var ts int64
		var value float64
		if err := row.Scan(&ts, &value); err != nil {
			return err
		}
		result = append(result, sampleResult{
			Metric: map[string]string{},
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: result}, nil
}

func nestedCountRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, selectedSeries string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	groupSelect, groupBy := labelIndexGroupSQL(grouping)
	filterGroupLabelsToSelected := exactMetricName(matchers) == ""
	groupLabels, groupJoin := labelIndexGroupLabelsSQLWithSelectedFilter(cfg, matchers, grouping, filterGroupLabelsToSelected)
	if groupLabels == "" || len(groupBy) == 0 {
		return "", false
	}

	evalMillisExpr := fmt.Sprintf("(%d + toInt64(step_idx) * %d)", evalStart.UnixMilli(), stepMillis)
	lookbackStartExpr := fmt.Sprintf("(%s - %d)", evalMillisExpr, lookback.Milliseconds())

	return fmt.Sprintf(
		"WITH selected_series AS (%s), group_labels AS (%s), group_intervals AS (SELECT %s, groupArray((toUnixTimestamp64Milli(min_time), toUnixTimestamp64Milli(max_time))) AS active_ranges FROM selected_series%s GROUP BY %s), active_groups AS (SELECT step_idx FROM group_intervals ARRAY JOIN range(toUInt64(%d)) AS step_idx WHERE arrayExists(x -> (tupleElement(x, 2) >= %s AND tupleElement(x, 1) <= %s), active_ranges)) SELECT toInt64(%d) + toInt64(step_idx) * %d AS ts, toFloat64(count()) AS value FROM active_groups GROUP BY step_idx ORDER BY step_idx",
		selectedSeries,
		groupLabels,
		strings.Join(groupSelect, ", "),
		groupJoin,
		strings.Join(groupBy, ", "),
		steps,
		lookbackStartExpr,
		evalMillisExpr,
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func nestedCountBitmapRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	if cfg.LabelPostingsTable == "" || cfg.ActivityTable == "" || len(grouping) != 1 || grouping[0] == labels.MetricName {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	selectedCTEs, selectedFrom, selectedExpr, ok := bitmapSelectedIDsSQL(cfg, metric, matchers)
	if !ok {
		return "", false
	}

	groupColumn := quoteIdent(groupAlias(0))
	groupName := grouping[0]
	evalMillisExpr := fmt.Sprintf("(%d + toInt64(step_idx) * %d)", evalStart.UnixMilli(), stepMillis)
	lookbackStartExpr := fmt.Sprintf("(%s - %d)", evalMillisExpr, lookback.Milliseconds())
	activityStart := evalStart.Add(-lookback)
	activityEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)

	withParts := append([]string{}, selectedCTEs...)
	withParts = append(withParts,
		fmt.Sprintf("selected_ids AS (SELECT %s AS bm FROM %s)", selectedExpr, selectedFrom),
		fmt.Sprintf(
			"active_by_step AS (SELECT step_idx, groupBitmapState(id) AS bm FROM (SELECT bucket, arrayJoin(ids) AS id FROM %s WHERE %s AND metric_name = %s AND bucket >= %s AND bucket <= %s) ARRAY JOIN range(toUInt64(%d)) AS step_idx WHERE toUnixTimestamp64Milli(bucket) >= %s AND toUnixTimestamp64Milli(bucket) <= %s GROUP BY step_idx)",
			tableName(cfg.CHDatabase, cfg.ActivityTable),
			teamFilter(cfg),
			sqlString(metric),
			chTimeMillis(activityStart.UnixMilli()),
			chTimeMillis(activityEnd.UnixMilli()),
			steps,
			lookbackStartExpr,
			evalMillisExpr,
		),
		"active_selected AS (SELECT step_idx, bitmapAnd(active_by_step.bm, selected_ids.bm) AS bm FROM active_by_step CROSS JOIN selected_ids WHERE bitmapAndCardinality(active_by_step.bm, selected_ids.bm) > 0)",
		fmt.Sprintf(
			"group_label_values AS (SELECT label_value AS %s, groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s GROUP BY label_value)",
			groupColumn,
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(groupName),
		),
		fmt.Sprintf(
			"group_label_present AS (SELECT groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s)",
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(groupName),
		),
		fmt.Sprintf(
			"label_groups AS (SELECT %s, bm FROM group_label_values UNION ALL SELECT '' AS %s, bitmapAndnot(metric_ids.bm, group_label_present.bm) AS bm FROM metric_ids CROSS JOIN group_label_present)",
			groupColumn,
			groupColumn,
		),
	)

	return fmt.Sprintf(
		"WITH %s SELECT toInt64(%d) + toInt64(active_selected.step_idx) * %d AS ts, toFloat64(count()) AS value FROM active_selected CROSS JOIN label_groups WHERE bitmapAndCardinality(active_selected.bm, label_groups.bm) > 0 GROUP BY active_selected.step_idx ORDER BY active_selected.step_idx",
		strings.Join(withParts, ", "),
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func bitmapSelectedIDsSQL(cfg Config, metric string, matchers []*labels.Matcher) ([]string, string, string, bool) {
	ctes := []string{
		fmt.Sprintf(
			"metric_ids AS (SELECT groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s AND label_value = %s)",
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(labels.MetricName),
			sqlString(metric),
		),
	}
	fromParts := []string{"metric_ids"}
	expr := "metric_ids.bm"
	matcherIndex := 0
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName || matcherIsNoop(matcher) {
			continue
		}
		positive, condition, ok := bitmapMatcherCondition(matcher)
		if !ok {
			return nil, "", "", false
		}
		alias := fmt.Sprintf("matcher_%d", matcherIndex)
		matcherIndex++
		ctes = append(ctes, fmt.Sprintf(
			"%s AS (SELECT groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s AND %s)",
			alias,
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(matcher.Name),
			condition,
		))
		fromParts = append(fromParts, alias)
		if positive {
			expr = "bitmapAnd(" + expr + ", " + alias + ".bm)"
		} else {
			expr = "bitmapAndnot(" + expr + ", " + alias + ".bm)"
		}
	}
	return ctes, strings.Join(fromParts, " CROSS JOIN "), expr, true
}

func bitmapMatcherCondition(matcher *labels.Matcher) (bool, string, bool) {
	switch matcher.Type {
	case labels.MatchEqual, labels.MatchRegexp:
		_, condition, ok := positiveLabelIndexMembershipCondition(matcher)
		return true, condition, ok
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		condition, ok := negativeLabelIndexCondition(matcher)
		return false, condition, ok
	default:
		return false, "", false
	}
}

type selectorGridSource struct {
	start time.Time
	end   time.Time
}

type aggregateRangeSource struct {
	gridExpr string
}

func (s *Server) aggregateRangeSourceSQL(expr parser.Expr, start, end time.Time, step time.Duration) (aggregateRangeSource, *parser.VectorSelector, int64, int64, bool) {
	selector, ok := expr.(*parser.VectorSelector)
	if !ok {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	source, mint, maxt, ok := selectorRangeGridSource(s.cfg, selector, start, end, step)
	if !ok {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	return aggregateRangeSource{gridExpr: lastGridExpr(source.start, source.end, step, s.cfg.LookbackDelta)}, selector, mint, maxt, true
}

func selectorRangeGridSource(cfg Config, selector *parser.VectorSelector, start, end time.Time, step time.Duration) (selectorGridSource, int64, int64, bool) {
	if selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil {
		return selectorGridSource{}, 0, 0, false
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	mint := shiftedStart.Add(-cfg.LookbackDelta).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	return selectorGridSource{start: shiftedStart, end: shiftedEnd}, mint, maxt, true
}

func lastGridExpr(start, end time.Time, step, lookback time.Duration) string {
	grid := fmt.Sprintf(
		"timeSeriesLastToGrid(%s, %s, %s, %s)(timestamp, value)",
		chTimeMillis(start.UnixMilli()),
		chTimeMillis(end.UnixMilli()),
		formatDurationSeconds(step),
		formatDurationSeconds(lookback),
	)
	return staleAwareNullableGridSQL(grid)
}

func (s *Server) queryInstantAggregateResults(ctx context.Context, sql string, grouping []string) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 128)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		groupValues := make([]string, len(grouping))
		var ts int64
		var value float64
		dest := make([]any, 0, len(groupValues)+2)
		for i := range groupValues {
			dest = append(dest, &groupValues[i])
		}
		dest = append(dest, &ts, &value)
		if err := row.Scan(dest...); err != nil {
			return err
		}
		results = append(results, sampleResult{
			Metric: groupingMetricValues(groupValues, grouping),
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	return results, err
}

func groupingMetricValues(values []string, grouping []string) map[string]string {
	metric, _ := groupingMetricAndKeyValues(values, grouping)
	return metric
}

func groupingMetricAndKeyValues(values []string, grouping []string) (map[string]string, string) {
	metric := make(map[string]string, len(grouping))
	keyParts := make([]string, 0, len(grouping)*2)
	for i, name := range grouping {
		value := values[i]
		if value != "" {
			metric[name] = value
		}
		keyParts = append(keyParts, name, value)
	}
	return metric, strings.Join(keyParts, "\xff")
}

func (s *Server) tryFastTopK(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Op != parser.TOPK && expr.Op != parser.BOTTOMK {
		return queryData{}, false, nil
	}
	if len(expr.Grouping) != 0 || expr.Without {
		return queryData{}, false, nil
	}
	limit, ok := aggregateLimit(expr.Param)
	if !ok || limit <= 0 {
		return queryData{}, false, nil
	}
	selector, ok := expr.Expr.(*parser.VectorSelector)
	if !ok {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	if s.cfg.postHogSchemaLayout() {
		if !postHogMatchersPushdownSafe(selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		return s.tryPostHogTopK(ctx, selector, window, evalTime, limit, expr.Op)
	}
	if !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	if data, ok, err := s.tryLabelIndexTopK(ctx, selector, window, evalTime, limit, expr.Op); ok || err != nil {
		return data, ok, err
	}

	return queryData{}, false, nil
}

func (s *Server) tryPostHogTopK(ctx context.Context, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, limit int, op parser.ItemType) (queryData, bool, error) {
	direction := "DESC"
	if op == parser.BOTTOMK {
		direction = "ASC"
	}
	plan := newPostHogQueryPlan(s.cfg, selector.LabelMatchers, nil, window.mint, window.maxt, true)
	latest := fmt.Sprintf(
		"SELECT series_fingerprint AS series_id, argMax(value, timestamp) AS value FROM %s GROUP BY series_id",
		postHogSampleRowsFrom(s.cfg, plan.sampleWhere(), false),
	)
	latest = fmt.Sprintf(
		"SELECT series_id, value FROM (%s) WHERE %s ORDER BY value %s LIMIT %d",
		latest,
		nonStaleSampleSQL("value"),
		direction,
		limit,
	)
	sql := fmt.Sprintf(
		"SELECT %s, toInt64(%d) AS ts, value FROM %s ORDER BY value %s",
		postHogSeriesLabelColumns,
		evalTime.UnixMilli(),
		plan.joinSeries(latest),
		direction,
	)
	sql = withMaxThreads(plan.withSelectedSeries(sql), s.cfg.AggregateThreads)

	results := make([]sampleResult, 0, limit)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var metricName string
		var serviceName string
		var resourceAttrs map[string]string
		var attrs map[string]string
		var ts int64
		var value float64
		if err := row.Scan(&metricName, &serviceName, &resourceAttrs, &attrs, &ts, &value); err != nil {
			return err
		}
		metric := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
		if !matchesAll(metric, selector.LabelMatchers) {
			return nil
		}
		results = append(results, sampleResult{
			Metric: metric,
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryLabelIndexTopK(ctx context.Context, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, limit int, op parser.ItemType) (queryData, bool, error) {
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return queryData{}, false, nil
	}

	direction := "DESC"
	if op == parser.BOTTOMK {
		direction = "ASC"
	}
	latestSource := latestSamplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), top_series AS (SELECT id, value FROM (%s) ORDER BY value %s LIMIT %d) SELECT selected_series.metric_name AS metric_name, selected_series.labels_json AS labels_json, toInt64(%d) AS ts, top_series.value AS value FROM top_series ANY INNER JOIN selected_series USING id ORDER BY value %s",
		selectedSeries,
		latestSource,
		direction,
		limit,
		evalTime.UnixMilli(),
		direction,
	)

	results := make([]sampleResult, 0, limit)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var metricName string
		var labelsJSON string
		var ts int64
		var value float64
		if err := row.Scan(&metricName, &labelsJSON, &ts, &value); err != nil {
			return err
		}
		metric, _, err := metricLabelMap(metricName, []byte(labelsJSON), nil)
		if err != nil {
			return err
		}
		results = append(results, sampleResult{
			Metric: metric,
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

type selectorWindow struct {
	mint int64
	maxt int64
}

func selectorWindowFor(selector *parser.VectorSelector, evalTime time.Time, lookback time.Duration) (selectorWindow, bool) {
	if selector.StartOrEnd != 0 {
		return selectorWindow{}, false
	}
	ref := evalTime.UnixMilli()
	if selector.Timestamp != nil {
		ref = *selector.Timestamp
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	ref -= offset.Milliseconds()
	return selectorWindow{mint: ref - lookback.Milliseconds(), maxt: ref}, true
}

type instantSeriesExprKind int

const (
	instantSeriesSelector instantSeriesExprKind = iota
)

type instantSeriesExprBranch struct {
	kind      instantSeriesExprKind
	selector  *parser.VectorSelector
	window    selectorWindow
	transform scalarTransform
}

func (b instantSeriesExprBranch) equivalentTo(other instantSeriesExprBranch) bool {
	return b.kind == other.kind &&
		b.window == other.window &&
		b.transform.signature() == other.transform.signature()
}

type scalarTransformOp struct {
	op         parser.ItemType
	scalar     float64
	scalarLeft bool
}

type scalarTransform struct {
	ops []scalarTransformOp
}

func (t scalarTransform) identity() bool {
	return len(t.ops) == 0
}

func (t scalarTransform) signature() string {
	if len(t.ops) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t.ops))
	for _, op := range t.ops {
		side := "right"
		if op.scalarLeft {
			side = "left"
		}
		parts = append(parts, fmt.Sprintf("%s:%s:%s", op.op.String(), side, strconv.FormatFloat(op.scalar, 'g', -1, 64)))
	}
	return strings.Join(parts, "|")
}

func (t scalarTransform) append(op parser.ItemType, scalar float64, scalarLeft bool) scalarTransform {
	t.ops = append(t.ops, scalarTransformOp{op: op, scalar: scalar, scalarLeft: scalarLeft})
	return t
}

func (t scalarTransform) apply(value string) string {
	for _, op := range t.ops {
		scalar := strconv.FormatFloat(op.scalar, 'g', -1, 64)
		if op.scalarLeft {
			value = scalarBinarySQL(op.op, scalar, value)
		} else {
			value = scalarBinarySQL(op.op, value, scalar)
		}
	}
	return value
}

func scalarBinarySQL(op parser.ItemType, left, right string) string {
	switch op {
	case parser.ADD:
		return "(" + left + " + " + right + ")"
	case parser.SUB:
		return "(" + left + " - " + right + ")"
	case parser.MUL:
		return "(" + left + " * " + right + ")"
	case parser.DIV:
		return "(" + left + " / " + right + ")"
	default:
		return left
	}
}

func scalarArithmeticOperator(op parser.ItemType) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.MUL, parser.DIV:
		return true
	default:
		return false
	}
}

type seriesExprKind int

const (
	seriesExprSelector seriesExprKind = iota
)

// seriesExprBranch is one operand of a chain of plain `or` operators: a vector
// selector with any scalar arithmetic folded into transform. node keeps the
// selector expression with the scalar arithmetic stripped.
type seriesExprBranch struct {
	node      parser.Expr
	kind      seriesExprKind
	selector  *parser.VectorSelector
	transform scalarTransform
}

func (b seriesExprBranch) equivalentTo(other seriesExprBranch) bool {
	return b.kind == other.kind &&
		b.transform.signature() == other.transform.signature()
}

func seriesExprBranches(expr parser.Expr) ([]seriesExprBranch, bool) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return seriesExprBranches(e.Expr)
	case *parser.BinaryExpr:
		if e.Op == parser.LOR {
			// `or on(...)` / `or ignoring(...)` deduplicate on a label subset,
			// which a series-id union cannot express
			if e.VectorMatching == nil || e.VectorMatching.Card != parser.CardManyToMany || e.VectorMatching.On || len(e.VectorMatching.MatchingLabels) != 0 {
				return nil, false
			}
			left, ok := seriesExprBranches(e.LHS)
			if !ok {
				return nil, false
			}
			right, ok := seriesExprBranches(e.RHS)
			if !ok {
				return nil, false
			}
			return append(left, right...), true
		}
	}
	branch, ok := seriesExprBranchFor(expr)
	if !ok {
		return nil, false
	}
	return []seriesExprBranch{branch}, true
}

func seriesExprBranchFor(expr parser.Expr) (seriesExprBranch, bool) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return seriesExprBranchFor(e.Expr)
	case *parser.VectorSelector:
		if e.Anchored || e.Smoothed {
			return seriesExprBranch{}, false
		}
		return seriesExprBranch{node: e, kind: seriesExprSelector, selector: e}, true
	case *parser.BinaryExpr:
		if !scalarArithmeticOperator(e.Op) {
			return seriesExprBranch{}, false
		}
		if scalar, ok := finiteNumber(e.RHS); ok {
			branch, ok := seriesExprBranchFor(e.LHS)
			if !ok {
				return seriesExprBranch{}, false
			}
			branch.transform = branch.transform.append(e.Op, scalar, false)
			return branch, true
		}
		if scalar, ok := finiteNumber(e.LHS); ok {
			branch, ok := seriesExprBranchFor(e.RHS)
			if !ok {
				return seriesExprBranch{}, false
			}
			branch.transform = branch.transform.append(e.Op, scalar, true)
			return branch, true
		}
	}
	return seriesExprBranch{}, false
}

func (s *Server) instantSeriesExprBranches(expr parser.Expr, evalTime time.Time) ([]instantSeriesExprBranch, bool) {
	structural, ok := seriesExprBranches(expr)
	if !ok {
		return nil, false
	}
	branches := make([]instantSeriesExprBranch, 0, len(structural))
	for _, branch := range structural {
		converted, ok := s.instantSeriesExprBranchFor(branch, evalTime)
		if !ok {
			return nil, false
		}
		branches = append(branches, converted)
	}
	return branches, true
}

func (s *Server) instantSeriesExprBranchFor(branch seriesExprBranch, evalTime time.Time) (instantSeriesExprBranch, bool) {
	switch branch.kind {
	case seriesExprSelector:
		window, ok := selectorWindowFor(branch.selector, evalTime, s.cfg.LookbackDelta)
		if !ok {
			return instantSeriesExprBranch{}, false
		}
		return instantSeriesExprBranch{kind: instantSeriesSelector, selector: branch.selector, window: window, transform: branch.transform}, true
	default:
		return instantSeriesExprBranch{}, false
	}
}

func (s *Server) instantSeriesExprSourceSQL(branch instantSeriesExprBranch, selectors []*parser.VectorSelector) (string, string) {
	union := len(selectors) > 1
	switch branch.kind {
	case instantSeriesSelector:
		var latest string
		if union {
			latest = latestSamplesForSelectedSeriesUnionSQL(s.cfg, exactMetricNamesForSelectors(selectors), branch.window.mint, branch.window.maxt)
		} else {
			latest = latestSamplesForSelectedSeriesSQL(s.cfg, branch.selector.LabelMatchers, branch.window.mint, branch.window.maxt)
		}
		return transformValueSQL(latest, branch.transform), "latest"
	default:
		return "", ""
	}
}

func transformValueSQL(source string, transform scalarTransform) string {
	if transform.identity() {
		return source
	}
	return fmt.Sprintf("SELECT id, %s AS value FROM (%s)", transform.apply("value"), source)
}

func transformGridValsSQL(gridExpr string, transform scalarTransform) string {
	if transform.identity() {
		return gridExpr
	}
	return fmt.Sprintf("arrayMap(x -> if(isNull(x), NULL, %s), %s)", transform.apply("x"), gridExpr)
}

func aggregateSQL(expr *parser.AggregateExpr, valueColumn string) (string, bool) {
	switch expr.Op {
	case parser.SUM:
		return "sum(" + valueColumn + ")", true
	case parser.AVG:
		return "avg(" + valueColumn + ")", true
	case parser.COUNT:
		return "toFloat64(count())", true
	case parser.MIN:
		return "min(" + valueColumn + ")", true
	case parser.MAX:
		return "max(" + valueColumn + ")", true
	case parser.GROUP:
		return "max(toFloat64(1))", true
	case parser.QUANTILE:
		quantile, ok := finiteNumber(expr.Param)
		if !ok || quantile < 0 || quantile > 1 {
			return "", false
		}
		return fmt.Sprintf("quantileExact(%s)(%s)", strconv.FormatFloat(quantile, 'f', -1, 64), valueColumn), true
	default:
		return "", false
	}
}

func finiteNumber(expr parser.Expr) (float64, bool) {
	switch e := expr.(type) {
	case *parser.NumberLiteral:
		if math.IsNaN(e.Val) || math.IsInf(e.Val, 0) {
			return 0, false
		}
		return e.Val, true
	case *parser.UnaryExpr:
		value, ok := finiteNumber(e.Expr)
		if !ok {
			return 0, false
		}
		switch e.Op {
		case parser.ADD:
			return value, true
		case parser.SUB:
			return -value, true
		default:
			return 0, false
		}
	default:
		return 0, false
	}
}

func withMaxThreads(sql string, maxThreads int) string {
	if maxThreads <= 0 {
		return sql
	}
	return sql + " SETTINGS max_threads = " + strconv.Itoa(maxThreads)
}

func aggregateLimit(expr parser.Expr) (int, bool) {
	value, ok := finiteNumber(expr)
	if !ok || value < 0 {
		return 0, false
	}
	return int(value), true
}

func matchersPushdownSafe(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if matcherIsNoop(m) {
			continue
		}
		switch m.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if m.Name != labels.MetricName && m.Matches("") {
				return false
			}
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			if m.Name != labels.MetricName && !m.Matches("") {
				return false
			}
		default:
			return false
		}
	}
	return len(matchers) > 0
}

func matcherIsNoop(m *labels.Matcher) bool {
	if m.Type != labels.MatchRegexp {
		return false
	}
	switch strings.TrimSpace(m.Value) {
	case ".*", "(.*)", "(?:.*)", ".*|", "|.*", "(.*)|", "|(.*)", "(?:.*)|", "|(?:.*)":
		return true
	default:
		return false
	}
}

func selectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, selectParts []string) (string, bool) {
	if !matchersPushdownSafe(matchers) {
		return "", false
	}
	if len(selectParts) == 0 {
		selectParts = []string{"id"}
	}
	if selectPartsIDOnly(selectParts) {
		if sql, ok := selectedSeriesIDsFromLabelIndexSQL(cfg, matchers); ok {
			return sql, true
		}
	}
	where := []string{
		teamFilter(cfg),
	}
	if selectedSeriesNeedsBounds(selectParts) {
		where = append(where, seriesTimeFilters(cfg, matchers, mint, maxt)...)
	}
	where = append(where, seriesPreFilters(cfg, matchers)...)
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT %s FROM %s WHERE %s) GROUP BY id",
		strings.Join(selectedSeriesProjection(selectParts), ", "),
		selectedSeriesSourceColumns(selectParts),
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	), true
}

func selectedSeriesIDsFromLabelIndexSQL(cfg Config, matchers []*labels.Matcher) (string, bool) {
	metric := exactMetricName(matchers)
	if metric == "" || cfg.LabelIndexTable == "" {
		return "", false
	}
	base := -1
	for i, matcher := range matchers {
		membership, _, ok := labelIndexMembershipCondition(matcher)
		if ok && membership == "IN" && (base < 0 || matcher.Type == labels.MatchEqual) {
			base = i
			if matcher.Type == labels.MatchEqual {
				break
			}
		}
	}
	if base < 0 {
		return "", false
	}

	table := tableName(cfg.CHDatabase, cfg.LabelIndexTable)
	filters := []string{teamFilter(cfg), "metric_name = " + sqlString(metric)}
	for i, matcher := range matchers {
		if matcherIsNoop(matcher) || matcher.Name == labels.MetricName {
			continue
		}
		membership, condition, ok := labelIndexMembershipCondition(matcher)
		if !ok {
			return "", false
		}
		predicate := "label_name = " + sqlString(matcher.Name) + " AND " + condition
		if i == base {
			filters = append(filters, predicate)
			continue
		}
		filters = append(filters, fmt.Sprintf(
			"id %s (SELECT id FROM %s WHERE %s AND metric_name = %s AND %s)",
			membership, table, teamFilter(cfg), sqlString(metric), predicate,
		))
	}
	return fmt.Sprintf("SELECT id FROM %s WHERE %s GROUP BY id", table, strings.Join(filters, " AND ")), true
}

func selectedSeriesForInstantBranchesSQL(cfg Config, branches []instantSeriesExprBranch, selectors []*parser.VectorSelector, selectParts []string) (string, bool) {
	windows := make([]selectorWindow, 0, len(branches))
	for _, branch := range branches {
		windows = append(windows, branch.window)
	}
	return selectedSeriesUnionSQL(cfg, selectors, windows, selectParts)
}

func selectedSeriesUnionSQL(cfg Config, selectors []*parser.VectorSelector, windows []selectorWindow, selectParts []string) (string, bool) {
	if len(selectors) == 0 || len(selectors) != len(windows) {
		return "", false
	}
	if len(selectors) > 1 {
		mint, maxt := windows[0].mint, windows[0].maxt
		for _, window := range windows[1:] {
			mint = min(mint, window.mint)
			maxt = max(maxt, window.maxt)
		}
		if sql, ok := selectedSeriesExactMatcherUnionSQL(cfg, selectors, selectParts, mint, maxt); ok {
			return sql, true
		}
	}

	selectedParts := make([]string, 0, len(selectors))
	for i, selector := range selectors {
		selectedSeries, ok := selectedSeriesSQL(cfg, selector.LabelMatchers, windows[i].mint, windows[i].maxt, selectParts)
		if !ok {
			return "", false
		}
		selectedParts = append(selectedParts, selectedSeries)
	}
	if len(selectedParts) == 1 {
		return selectedParts[0], true
	}
	return strings.Join(selectedParts, " UNION DISTINCT "), true
}

type exactLabelMatcher struct {
	name  string
	value string
}

type exactSelectorMatcherSet struct {
	metric string
	labels []exactLabelMatcher
}

func selectedSeriesExactMatcherUnionSQL(cfg Config, selectors []*parser.VectorSelector, selectParts []string, mint, maxt int64) (string, bool) {
	if len(selectors) < 2 || selectedSeriesNeedsBounds(selectParts) {
		return "", false
	}
	sets := make([]exactSelectorMatcherSet, 0, len(selectors))
	metricNames := make([]string, 0, len(selectors))
	seenMetrics := make(map[string]struct{}, len(selectors))
	for _, selector := range selectors {
		set, ok := exactSelectorMatcherSetFor(selector.LabelMatchers)
		if !ok {
			return "", false
		}
		sets = append(sets, set)
		if _, ok := seenMetrics[set.metric]; !ok {
			seenMetrics[set.metric] = struct{}{}
			metricNames = append(metricNames, set.metric)
		}
	}

	matchedIDs := exactMatcherUnionIDsSQL(cfg, sets, mint, maxt)
	if selectPartsIDOnly(selectParts) {
		return "SELECT id FROM (" + matchedIDs + ") GROUP BY id", true
	}

	where := []string{teamFilter(cfg), "id IN (" + matchedIDs + ")"}
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT %s FROM %s WHERE %s) GROUP BY id",
		strings.Join(selectedSeriesProjection(selectParts), ", "),
		selectedSeriesSourceColumns(selectParts),
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	), true
}

func selectPartsIDOnly(selectParts []string) bool {
	return len(selectParts) == 1 && selectParts[0] == "id"
}

func exactSelectorMatcherSetFor(matchers []*labels.Matcher) (exactSelectorMatcherSet, bool) {
	if len(matchers) == 0 {
		return exactSelectorMatcherSet{}, false
	}
	var metric string
	seenLabels := make(map[string]struct{}, len(matchers))
	labelMatchers := make([]exactLabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) {
			continue
		}
		if matcher.Name == labels.MetricName {
			if matcher.Type != labels.MatchEqual || matcher.Value == "" {
				return exactSelectorMatcherSet{}, false
			}
			if metric != "" && metric != matcher.Value {
				return exactSelectorMatcherSet{}, false
			}
			metric = matcher.Value
			continue
		}
		if matcher.Name == "" || matcher.Type != labels.MatchEqual || matcher.Matches("") {
			return exactSelectorMatcherSet{}, false
		}
		if _, ok := seenLabels[matcher.Name]; ok {
			return exactSelectorMatcherSet{}, false
		}
		seenLabels[matcher.Name] = struct{}{}
		labelMatchers = append(labelMatchers, exactLabelMatcher{name: matcher.Name, value: matcher.Value})
	}
	// One bit per matcher tracks branch completeness in SQL, so a branch can
	// carry at most 64 of them.
	if metric == "" || len(labelMatchers) == 0 || len(labelMatchers) > 64 {
		return exactSelectorMatcherSet{}, false
	}
	return exactSelectorMatcherSet{metric: metric, labels: labelMatchers}, true
}

func exactMatcherUnionIDsSQL(cfg Config, sets []exactSelectorMatcherSet, mint, maxt int64) string {
	if sql, ok := singleExactLabelUnionIDsSQL(cfg, sets); ok {
		return sql
	}
	return "SELECT id FROM (" + exactMatcherUnionBranchIDsSQL(cfg, sets, mint, maxt) + ") GROUP BY id"
}

func exactMatcherUnionBranchIDsSQL(cfg Config, sets []exactSelectorMatcherSet, mint, maxt int64) string {
	matcherRows := make([]string, 0, len(sets))
	metricNames := make([]string, 0, len(sets))
	labelNames := make([]string, 0, len(sets))
	labelValues := make([]string, 0, len(sets))
	seenMetricNames := make(map[string]struct{}, len(sets))
	seenLabelNames := make(map[string]struct{}, len(sets))
	seenLabelValues := make(map[string]struct{}, len(sets))
	for branch, set := range sets {
		if _, ok := seenMetricNames[set.metric]; !ok {
			seenMetricNames[set.metric] = struct{}{}
			metricNames = append(metricNames, set.metric)
		}
		for position, matcher := range set.labels {
			if _, ok := seenLabelNames[matcher.name]; !ok {
				seenLabelNames[matcher.name] = struct{}{}
				labelNames = append(labelNames, matcher.name)
			}
			if _, ok := seenLabelValues[matcher.value]; !ok {
				seenLabelValues[matcher.value] = struct{}{}
				labelValues = append(labelValues, matcher.value)
			}
			matcherRows = append(matcherRows, fmt.Sprintf(
				"(%d, %d, %d, %s, %s, %s)",
				branch,
				fullBranchMask(len(set.labels)),
				uint64(1)<<uint(position),
				sqlString(set.metric),
				sqlString(matcher.name),
				sqlString(matcher.value),
			))
		}
	}
	where := []string{teamFilter(cfg)}
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	where = append(where, inCondition("label_name", labelNames))
	where = append(where, inCondition("label_value", labelValues))
	if active, ok := activeSeriesIDsForMetricsSQL(cfg, metricNames, mint, maxt); ok {
		where = append(where, "li.id IN ("+active+")")
	}
	// A series matches a branch once it has hit every one of that branch's
	// label matchers. Counting distinct label names for that check builds a
	// string hash set per (branch, id) group, which dominated dashboard CPU;
	// OR-ing one bit per matcher is exact, duplicate-safe, and a plain UInt64.
	return fmt.Sprintf(
		"SELECT mr.branch AS branch, li.id AS id FROM %s AS li INNER JOIN %s AS mr ON li.metric_name = mr.metric_name AND li.label_name = mr.label_name AND li.label_value = mr.label_value WHERE %s GROUP BY branch, id, full_mask HAVING groupBitOr(mr.match_bit) = full_mask",
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		"values('branch UInt32, full_mask UInt64, match_bit UInt64, metric_name String, label_name String, label_value String', "+strings.Join(matcherRows, ", ")+")",
		strings.Join(where, " AND "),
	)
}

// fullBranchMask is the set of matcher bits a series must hit to match a branch.
func fullBranchMask(matchers int) uint64 {
	return uint64(1)<<uint(matchers) - 1
}

func singleExactLabelUnionIDsSQL(cfg Config, sets []exactSelectorMatcherSet) (string, bool) {
	if len(sets) == 0 {
		return "", false
	}
	metric := sets[0].metric
	labelName := ""
	values := make([]string, 0, len(sets))
	seenValues := make(map[string]struct{}, len(sets))
	for _, set := range sets {
		if set.metric != metric || len(set.labels) != 1 {
			return "", false
		}
		label := set.labels[0]
		if labelName == "" {
			labelName = label.name
		}
		if label.name != labelName {
			return "", false
		}
		if _, ok := seenValues[label.value]; ok {
			continue
		}
		seenValues[label.value] = struct{}{}
		values = append(values, label.value)
	}
	where := []string{
		teamFilter(cfg),
		"metric_name = " + sqlString(metric),
		"label_name = " + sqlString(labelName),
		inCondition("label_value", values),
	}
	return fmt.Sprintf(
		"SELECT id FROM %s WHERE %s GROUP BY id",
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		strings.Join(where, " AND "),
	), true
}

func inCondition(column string, values []string) string {
	if len(values) == 1 {
		return column + " = " + sqlString(values[0])
	}
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, sqlString(value))
	}
	return column + " IN (" + strings.Join(quoted, ",") + ")"
}

func selectedSeriesNeedsBounds(selectParts []string) bool {
	for _, part := range selectParts {
		if part == "min_time" || part == "max_time" {
			return true
		}
	}
	return false
}

func selectedSeriesProjection(selectParts []string) []string {
	projected := make([]string, 0, len(selectParts))
	for _, part := range selectParts {
		switch part {
		case "id":
			projected = append(projected, "id")
		case "metric_name":
			projected = append(projected, "any(metric_name) AS metric_name")
		case "labels_json":
			projected = append(projected, "any(labels_json) AS labels_json")
		case "min_time":
			projected = append(projected, "min(min_time) AS min_time")
		case "max_time":
			projected = append(projected, "max(max_time) AS max_time")
		default:
			projected = append(projected, part)
		}
	}
	return projected
}

func selectedSeriesSourceColumns(selectParts []string) string {
	all := "id, metric_name, labels_json, min_time, max_time"
	columns := []string{"id"}
	add := func(column string) {
		for _, existing := range columns {
			if existing == column {
				return
			}
		}
		columns = append(columns, column)
	}
	for _, part := range selectParts {
		switch part {
		case "id":
		case "metric_name", "labels_json", "min_time", "max_time":
			add(part)
		default:
			return all
		}
	}
	return strings.Join(columns, ", ")
}

func latestSamplesForSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64) string {
	where := sampleBaseFilters(cfg, matchers, mint, maxt)
	if !metricOnlyMatchers(matchers) {
		where = append(where, sampleSelectedSeriesFilters(cfg)...)
	}
	source := rawSamplesSourceSQL(cfg, strings.Join(where, " AND "))
	latest := fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value, max(timestamp) AS ts_col FROM (%s) GROUP BY id",
		source,
	)
	return fmt.Sprintf("SELECT id, value, ts_col FROM (%s) WHERE %s", latest, nonStaleSampleSQL("value"))
}

func samplesForSelectedSeriesUnionSQL(cfg Config, metricNames []string, mint, maxt int64) string {
	where := []string{teamFilter(cfg)}
	where = append(where, sampleTimeFilters(cfg, mint, maxt)...)
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	where = append(where, sampleSelectedSeriesFilters(cfg)...)
	return rawSamplesSourceSQL(cfg, strings.Join(where, " AND "))
}

func latestSamplesForSelectedSeriesUnionSQL(cfg Config, metricNames []string, mint, maxt int64) string {
	source := samplesForSelectedSeriesUnionSQL(cfg, metricNames, mint, maxt)
	latest := fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value, max(timestamp) AS ts_col FROM (%s) GROUP BY id",
		source,
	)
	return fmt.Sprintf("SELECT id, value, ts_col FROM (%s) WHERE %s", latest, nonStaleSampleSQL("value"))
}

func exactMetricNamesForSelectors(selectors []*parser.VectorSelector) []string {
	names := make([]string, 0, len(selectors))
	seen := make(map[string]struct{}, len(selectors))
	for _, selector := range selectors {
		name := exactMetricName(selector.LabelMatchers)
		if name == "" {
			return nil
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func commonExactMetricMatchers(selectors []*parser.VectorSelector) []*labels.Matcher {
	if len(selectors) == 0 {
		return nil
	}
	names := exactMetricNamesForSelectors(selectors)
	if len(names) != 1 {
		return nil
	}
	return selectors[0].LabelMatchers
}

func metricNamesCondition(metricNames []string) string {
	switch len(metricNames) {
	case 0:
		return ""
	case 1:
		return "metric_name = " + sqlString(metricNames[0])
	default:
		quoted := make([]string, 0, len(metricNames))
		for _, name := range metricNames {
			quoted = append(quoted, sqlString(name))
		}
		return "metric_name IN (" + strings.Join(quoted, ",") + ")"
	}
}

func metricGroupProjection(grouping []string) []string {
	for _, name := range grouping {
		if name == labels.MetricName {
			return []string{"metric_name"}
		}
	}
	return nil
}

func labelIndexGroupSQL(grouping []string) ([]string, []string) {
	selects := make([]string, 0, len(grouping))
	groupBy := make([]string, 0, len(grouping))
	for i, name := range grouping {
		alias := groupAlias(i)
		if name == labels.MetricName {
			selects = append(selects, "selected_series.metric_name AS "+quoteIdent(alias))
		} else {
			selects = append(selects, "ifNull(group_labels."+quoteIdent(alias)+", '') AS "+quoteIdent(alias))
		}
		groupBy = append(groupBy, quoteIdent(alias))
	}
	return selects, groupBy
}

// postHogGroupingSupported reports whether every grouping label can be
// resolved on the PostHog series table.
func postHogGroupingSupported(grouping []string) bool {
	for _, name := range grouping {
		if _, ok := postHogSeriesLabelExpr(name); !ok {
			return false
		}
	}
	return true
}

func postHogUniqGroupExpr(exprs []string) string {
	if len(exprs) == 1 {
		return exprs[0]
	}
	return "tuple(" + strings.Join(exprs, ", ") + ")"
}

func aggregateSourceSQL(base string, grouping []string, groupJoin string) string {
	source := base
	if groupingHasMetricName(grouping) {
		source += " ANY INNER JOIN selected_series USING id"
	}
	return source + groupJoin
}

func groupingHasMetricName(grouping []string) bool {
	for _, name := range grouping {
		if name == labels.MetricName {
			return true
		}
	}
	return false
}

func labelIndexGroupLabelsSQL(cfg Config, matchers []*labels.Matcher, grouping []string) (string, string) {
	return labelIndexGroupLabelsSQLWithSelectedFilter(cfg, matchers, grouping, true)
}

func labelIndexGroupLabelsSQLWithSelectedFilter(cfg Config, matchers []*labels.Matcher, grouping []string, filterToSelected bool) (string, string) {
	labelGroups := make([]struct {
		index int
		name  string
	}, 0, len(grouping))
	seen := make(map[string]struct{})
	for i, name := range grouping {
		if name == labels.MetricName {
			continue
		}
		key := name
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		labelGroups = append(labelGroups, struct {
			index int
			name  string
		}{index: i, name: name})
	}
	if len(labelGroups) == 0 {
		return "", ""
	}

	selects := []string{"id"}
	names := make([]string, 0, len(labelGroups))
	for _, group := range labelGroups {
		names = append(names, sqlString(group.name))
		selects = append(selects, fmt.Sprintf(
			"maxIf(label_value, label_name = %s) AS %s",
			sqlString(group.name),
			quoteIdent(groupAlias(group.index)),
		))
	}
	where := []string{
		teamFilter(cfg),
		"label_name IN (" + strings.Join(names, ", ") + ")",
	}
	metric := exactMetricName(matchers)
	if metric != "" {
		where = append(where, "metric_name = "+sqlString(metric))
	}
	if filterToSelected && metric == "" {
		where = append(where, "id IN (SELECT id FROM selected_series)")
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY id",
		strings.Join(selects, ", "),
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		strings.Join(where, " AND "),
	), " ANY LEFT JOIN group_labels USING id"
}

func groupAlias(index int) string {
	return fmt.Sprintf("__group_%d", index)
}
