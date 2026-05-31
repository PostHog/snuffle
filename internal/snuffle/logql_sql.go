package snuffle

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type logQLMetricSQLPlan struct {
	rangeAgg    *logQLRangeAggregation
	grouping    *logQLGrouping
	outerAgg    string
	valueExpr   string
	groupLabels []string
	groupExprs  []string
}

func (s *Server) tryLogQLRangeMetricSQL(ctx context.Context, expr *logQLExpr, start, end time.Time, step time.Duration) (lokiQueryData, bool, error) {
	plan, ok := buildLogQLMetricSQLPlan(expr)
	if !ok {
		return lokiQueryData{}, false, nil
	}
	rows, err := s.queryLogQLMetricSQL(ctx, plan, start.UnixNano(), end.UnixNano(), step)
	if err != nil {
		return lokiQueryData{}, true, err
	}
	return lokiQueryData{ResultType: "matrix", Result: rows, Stats: lokiEmptyStats()}, true, nil
}

func (s *Server) tryLogQLInstantMetricSQL(ctx context.Context, expr *logQLExpr, ts time.Time) (lokiQueryData, bool, error) {
	plan, ok := buildLogQLMetricSQLPlan(expr)
	if !ok {
		return lokiQueryData{}, false, nil
	}
	rows, err := s.queryLogQLMetricSQL(ctx, plan, ts.UnixNano(), ts.UnixNano(), time.Second)
	if err != nil {
		return lokiQueryData{}, true, err
	}
	vector := make([]logMetricVectorResult, 0, len(rows))
	for _, row := range rows {
		if len(row.Values) == 0 {
			continue
		}
		vector = append(vector, logMetricVectorResult{
			Metric: row.Metric,
			Value:  row.Values[len(row.Values)-1],
		})
	}
	return lokiQueryData{ResultType: "vector", Result: vector, Stats: lokiEmptyStats()}, true, nil
}

func buildLogQLMetricSQLPlan(expr *logQLExpr) (*logQLMetricSQLPlan, bool) {
	if expr == nil || expr.comparison != nil {
		return nil, false
	}
	if expr.rangeAgg != nil {
		plan := buildLogQLRangeAggSQLPlan(expr.rangeAgg, nil, "")
		return plan, plan != nil
	}
	if expr.aggregation == nil || expr.aggregation.expr == nil || expr.aggregation.expr.rangeAgg == nil || expr.aggregation.expr.comparison != nil {
		return nil, false
	}
	switch expr.aggregation.fn {
	case "sum":
	default:
		return nil, false
	}
	plan := buildLogQLRangeAggSQLPlan(expr.aggregation.expr.rangeAgg, expr.aggregation.grouping, expr.aggregation.fn)
	return plan, plan != nil
}

func buildLogQLRangeAggSQLPlan(rangeAgg *logQLRangeAggregation, grouping *logQLGrouping, outerAgg string) *logQLMetricSQLPlan {
	if rangeAgg == nil || !logQLMetricSelectorSQLSafe(rangeAgg.selector) {
		return nil
	}
	if grouping == nil {
		grouping = rangeAgg.grouping
	}
	if grouping != nil && grouping.without {
		return nil
	}
	if outerAgg == "" && grouping == nil {
		return nil
	}
	valueExpr := ""
	switch rangeAgg.fn {
	case "count_over_time":
		valueExpr = "toFloat64(sum(log_count))"
	case "rate":
		valueExpr = fmt.Sprintf("toFloat64(sum(log_count)) / %s", formatDurationSeconds(rangeAgg.window))
	case "bytes_over_time":
		valueExpr = "toFloat64(sum(byte_count))"
	case "bytes_rate":
		valueExpr = fmt.Sprintf("toFloat64(sum(byte_count)) / %s", formatDurationSeconds(rangeAgg.window))
	default:
		return nil
	}
	groupLabels, groupExprs := logQLSQLGroupingExprs(grouping)
	return &logQLMetricSQLPlan{
		rangeAgg:    rangeAgg,
		grouping:    grouping,
		outerAgg:    outerAgg,
		valueExpr:   valueExpr,
		groupLabels: groupLabels,
		groupExprs:  groupExprs,
	}
}

func logQLMetricSelectorSQLSafe(selector logQLSelector) bool {
	for _, stage := range selector.stages {
		switch stage.kind {
		case "line_filter":
		default:
			return false
		}
	}
	return true
}

func logQLSQLGroupingExprs(grouping *logQLGrouping) ([]string, []string) {
	if grouping == nil || grouping.without {
		return nil, nil
	}
	labels := make([]string, 0, len(grouping.labels))
	exprs := make([]string, 0, len(grouping.labels))
	for _, label := range grouping.labels {
		if label == "" || strings.HasPrefix(label, "__") {
			continue
		}
		labels = append(labels, label)
		exprs = append(exprs, logQLMetricGroupLabelValueExpr(label))
	}
	return labels, exprs
}

func logQLMetricGroupLabelValueExpr(label string) string {
	switch label {
	case "service_name":
		return "service_name"
	case "level", "severity", "severity_text", "detected_level":
		return "severity_text"
	case "trace_id":
		return "trace_id"
	case "span_id":
		return "span_id"
	default:
		return logQLLabelValueExpr(label)
	}
}

func logQLMetricBaseFilters(cfg Config, selector logQLSelector, startNS, endNS int64) []string {
	filters := []string{
		teamFilter(cfg),
		"timestamp >= " + chTimeNanos(startNS),
		"timestamp <= " + chTimeNanos(endNS),
		"time_bucket >= toStartOfDay(" + chTimeNanos(startNS) + ")",
		"time_bucket <= toStartOfDay(" + chTimeNanos(endNS) + ")",
	}
	for _, matcher := range selector.matchers {
		filters = append(filters, logQLMetricMatcherCondition(matcher))
	}
	canPushLine := true
	for _, stage := range selector.stages {
		if stage.kind == "line_format" {
			canPushLine = false
		}
		if canPushLine && stage.kind == "line_filter" {
			filters = append(filters, logQLLineFilterCondition(*stage.lineFilter))
		}
	}
	return filters
}

func logQLMetricMatcherCondition(m logQLLabelMatcher) string {
	return logQLStringCondition(logQLMetricLabelFilterValueExpr(m.name), m.op, m.value, true)
}

func logQLMetricLabelFilterValueExpr(label string) string {
	switch label {
	case "service_name", "service.name":
		return "service_name"
	case "level", "severity", "severity_text", "detected_level":
		return "severity_text"
	case "trace_id":
		return "trace_id"
	case "span_id":
		return "span_id"
	default:
		return logQLStreamLabelValueExpr(label)
	}
}

func (s *Server) queryLogQLMetricSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	if step <= 0 {
		step = time.Minute
	}
	if logQLMetricBucketSQLSafe(plan, step) {
		return s.queryLogQLMetricBucketSQL(ctx, plan, startNS, endNS, step)
	}
	points := int64(1)
	if endNS > startNS {
		points = ((endNS - startNS) / step.Nanoseconds()) + 1
	}
	if points < 1 {
		points = 1
	}
	fetchStartNS := startNS - plan.rangeAgg.window.Nanoseconds() - plan.rangeAgg.offset.Nanoseconds()
	fetchEndNS := endNS - plan.rangeAgg.offset.Nanoseconds()
	where := logQLMetricBaseFilters(s.cfg, plan.rangeAgg.selector, fetchStartNS, fetchEndNS)

	selects := []string{"eval_ns"}
	groupBy := []string{"eval_ns"}
	orderBy := make([]string, 0, len(plan.groupLabels)+1)
	for i := range plan.groupExprs {
		alias := fmt.Sprintf("label_%d", i)
		selects = append(selects, alias)
		groupBy = append(groupBy, alias)
		orderBy = append(orderBy, alias)
	}
	orderBy = append(orderBy, "eval_ns")
	selects = append(selects, plan.valueExpr+" AS value")

	preAggSelects := []string{"ts_ns", "count() AS log_count", "sum(length(body)) AS byte_count"}
	preAggGroupBy := []string{"ts_ns"}
	for i, expr := range plan.groupExprs {
		alias := fmt.Sprintf("label_%d", i)
		preAggSelects = append(preAggSelects, expr+" AS "+alias)
		preAggGroupBy = append(preAggGroupBy, alias)
	}
	preAggSQL := fmt.Sprintf(
		"(SELECT %s FROM %s GROUP BY %s)",
		strings.Join(preAggSelects, ", "),
		logQLDedupLogsSubquery(s.cfg, where),
		strings.Join(preAggGroupBy, ", "),
	)

	sql := fmt.Sprintf(
		"WITH toInt64(%d) AS start_ns, toInt64(%d) AS step_ns, toInt64(%d) AS window_ns SELECT %s FROM %s AS logs CROSS JOIN (SELECT start_ns + (toInt64(number) * step_ns) AS eval_ns FROM numbers(%d)) AS grid WHERE logs.ts_ns > eval_ns - window_ns AND logs.ts_ns <= eval_ns GROUP BY %s ORDER BY %s",
		startNS,
		step.Nanoseconds(),
		plan.rangeAgg.window.Nanoseconds(),
		strings.Join(selects, ", "),
		preAggSQL,
		points,
		strings.Join(groupBy, ", "),
		strings.Join(orderBy, ", "),
	)

	seriesByKey := map[string]*logMetricMatrixResult{}
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var evalNS int64
		groupValues := make([]string, len(plan.groupLabels))
		var value float64
		dest := make([]any, 0, 2+len(groupValues))
		dest = append(dest, &evalNS)
		for i := range groupValues {
			dest = append(dest, &groupValues[i])
		}
		dest = append(dest, &value)
		if err := row.Scan(dest...); err != nil {
			return err
		}
		labels := make(map[string]string, len(plan.groupLabels))
		for i, label := range plan.groupLabels {
			if groupValues[i] != "" {
				labels[label] = groupValues[i]
			}
		}
		key := labelsKey(labels)
		result := seriesByKey[key]
		if result == nil {
			result = &logMetricMatrixResult{Metric: stableLabelMap(labels)}
			seriesByKey[key] = result
		}
		result.Values = append(result.Values, []any{float64(evalNS) / 1e9, formatSample(value)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	results := make([]logMetricMatrixResult, 0, len(seriesByKey))
	for _, result := range seriesByKey {
		results = append(results, *result)
	}
	sort.Slice(results, func(i, j int) bool { return labelsKey(results[i].Metric) < labelsKey(results[j].Metric) })
	return results, nil
}

func logQLMetricBucketSQLSafe(plan *logQLMetricSQLPlan, step time.Duration) bool {
	if plan == nil || plan.rangeAgg == nil || step <= 0 {
		return false
	}
	switch plan.rangeAgg.fn {
	case "count_over_time", "rate":
	default:
		return false
	}
	return plan.rangeAgg.window%step == 0 && plan.rangeAgg.offset == 0
}

func (s *Server) queryLogQLMetricBucketSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	points := int64(1)
	if endNS > startNS {
		points = ((endNS - startNS) / step.Nanoseconds()) + 1
	}
	if points < 1 {
		points = 1
	}
	fetchStartNS := startNS - plan.rangeAgg.window.Nanoseconds()
	fetchEndNS := endNS
	where := logQLMetricBaseFilters(s.cfg, plan.rangeAgg.selector, fetchStartNS, fetchEndNS)

	bucketSelects := []string{"intDiv(toInt64(toUnixTimestamp64Nano(timestamp)) + step_ns - 1, step_ns) * step_ns AS bucket_ns", "uniqExact(uuid) AS log_count"}
	bucketGroupBy := []string{"bucket_ns"}
	outerSelects := []string{"eval_ns"}
	outerGroupBy := []string{"eval_ns"}
	orderBy := make([]string, 0, len(plan.groupLabels)+1)
	for i, expr := range plan.groupExprs {
		alias := fmt.Sprintf("label_%d", i)
		bucketSelects = append(bucketSelects, expr+" AS "+alias)
		bucketGroupBy = append(bucketGroupBy, alias)
		outerSelects = append(outerSelects, alias)
		outerGroupBy = append(outerGroupBy, alias)
		orderBy = append(orderBy, alias)
	}
	orderBy = append(orderBy, "eval_ns")
	valueExpr := "toFloat64(sum(log_count))"
	if plan.rangeAgg.fn == "rate" {
		valueExpr = fmt.Sprintf("toFloat64(sum(log_count)) / %s", formatDurationSeconds(plan.rangeAgg.window))
	}
	outerSelects = append(outerSelects, valueExpr+" AS value")

	bucketSQL := fmt.Sprintf(
		"(SELECT %s FROM %s WHERE %s GROUP BY %s)",
		strings.Join(bucketSelects, ", "),
		tableName(s.cfg.CHDatabase, s.cfg.LogsTable),
		strings.Join(where, " AND "),
		strings.Join(bucketGroupBy, ", "),
	)
	sql := fmt.Sprintf(
		"WITH toInt64(%d) AS start_ns, toInt64(%d) AS step_ns, toInt64(%d) AS window_ns SELECT %s FROM %s AS buckets CROSS JOIN (SELECT start_ns + (toInt64(number) * step_ns) AS eval_ns FROM numbers(%d)) AS grid WHERE buckets.bucket_ns > eval_ns - window_ns AND buckets.bucket_ns <= eval_ns GROUP BY %s ORDER BY %s",
		startNS,
		step.Nanoseconds(),
		plan.rangeAgg.window.Nanoseconds(),
		strings.Join(outerSelects, ", "),
		bucketSQL,
		points,
		strings.Join(outerGroupBy, ", "),
		strings.Join(orderBy, ", "),
	)
	return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
}

func (s *Server) scanLogQLMetricSQLResults(ctx context.Context, sql string, groupLabels []string) ([]logMetricMatrixResult, error) {
	seriesByKey := map[string]*logMetricMatrixResult{}
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var evalNS int64
		groupValues := make([]string, len(groupLabels))
		var value float64
		dest := make([]any, 0, 2+len(groupValues))
		dest = append(dest, &evalNS)
		for i := range groupValues {
			dest = append(dest, &groupValues[i])
		}
		dest = append(dest, &value)
		if err := row.Scan(dest...); err != nil {
			return err
		}
		labels := make(map[string]string, len(groupLabels))
		for i, label := range groupLabels {
			if groupValues[i] != "" {
				labels[label] = groupValues[i]
			}
		}
		key := labelsKey(labels)
		result := seriesByKey[key]
		if result == nil {
			result = &logMetricMatrixResult{Metric: stableLabelMap(labels)}
			seriesByKey[key] = result
		}
		result.Values = append(result.Values, []any{float64(evalNS) / 1e9, formatSample(value)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	results := make([]logMetricMatrixResult, 0, len(seriesByKey))
	for _, result := range seriesByKey {
		results = append(results, *result)
	}
	sort.Slice(results, func(i, j int) bool { return labelsKey(results[i].Metric) < labelsKey(results[j].Metric) })
	return results, nil
}
