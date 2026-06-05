package snuffle

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type logQLMetricSQLPlan struct {
	rangeAgg        *logQLRangeAggregation
	grouping        *logQLGrouping
	outerAgg        string
	valueExpr       string
	groupLabels     []string
	groupExprs      []string
	comparison      *logQLComparison
	topK            int
	hasTopK         bool
	topKIsTop       bool
	unwrapValueExpr string
	selectorFilters []string
}

func (s *Server) tryLogQLRangeMetricSQL(ctx context.Context, expr *logQLExpr, start, end time.Time, step time.Duration) (lokiQueryData, bool, error) {
	plan, ok := buildLogQLMetricSQLPlan(s.cfg, expr)
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
	plan, ok := buildLogQLMetricSQLPlan(s.cfg, expr)
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

func buildLogQLMetricSQLPlan(cfg Config, expr *logQLExpr) (*logQLMetricSQLPlan, bool) {
	if expr == nil {
		return nil, false
	}
	var plan *logQLMetricSQLPlan
	if expr.rangeAgg != nil {
		plan = buildLogQLRangeAggSQLPlan(cfg, expr.rangeAgg, nil, "")
	} else if expr.aggregation != nil && expr.aggregation.expr != nil && expr.aggregation.expr.rangeAgg != nil && expr.aggregation.expr.comparison == nil {
		switch expr.aggregation.fn {
		case "sum":
		default:
			return nil, false
		}
		plan = buildLogQLRangeAggSQLPlan(cfg, expr.aggregation.expr.rangeAgg, expr.aggregation.grouping, expr.aggregation.fn)
	} else if expr.topK != nil && expr.topK.expr != nil && expr.topK.expr.comparison == nil {
		child, ok := buildLogQLMetricSQLPlan(cfg, expr.topK.expr)
		if !ok || child == nil || child.hasTopK {
			return nil, false
		}
		plan = child
		plan.topK = expr.topK.k
		plan.hasTopK = true
		plan.topKIsTop = expr.topK.isTop
	} else {
		return nil, false
	}
	if plan == nil {
		return nil, false
	}
	plan.comparison = expr.comparison
	return plan, plan != nil
}

func buildLogQLRangeAggSQLPlan(cfg Config, rangeAgg *logQLRangeAggregation, grouping *logQLGrouping, outerAgg string) *logQLMetricSQLPlan {
	if rangeAgg == nil {
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
		if !logQLMetricSelectorSQLSafe(rangeAgg.selector) {
			return nil
		}
		valueExpr = "toFloat64(sum(log_count))"
	case "rate":
		if !logQLMetricSelectorSQLSafe(rangeAgg.selector) {
			return nil
		}
		valueExpr = fmt.Sprintf("toFloat64(sum(log_count)) / %s", formatDurationSeconds(rangeAgg.window))
	case "bytes_over_time":
		if !logQLMetricSelectorSQLSafe(rangeAgg.selector) {
			return nil
		}
		valueExpr = "toFloat64(sum(byte_count))"
	case "bytes_rate":
		if !logQLMetricSelectorSQLSafe(rangeAgg.selector) {
			return nil
		}
		valueExpr = fmt.Sprintf("toFloat64(sum(byte_count)) / %s", formatDurationSeconds(rangeAgg.window))
	default:
		unwrapValueExpr, selectorFilters, ok := buildLogQLUnwrapMetricSQL(cfg, rangeAgg)
		if !ok {
			return nil
		}
		groupLabels, groupExprs := logQLSQLGroupingExprs(cfg, grouping)
		return &logQLMetricSQLPlan{
			rangeAgg:        rangeAgg,
			grouping:        grouping,
			outerAgg:        outerAgg,
			groupLabels:     groupLabels,
			groupExprs:      groupExprs,
			unwrapValueExpr: unwrapValueExpr,
			selectorFilters: selectorFilters,
		}
	}
	groupLabels, groupExprs := logQLSQLGroupingExprs(cfg, grouping)
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

func logQLSQLGroupingExprs(cfg Config, grouping *logQLGrouping) ([]string, []string) {
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
		exprs = append(exprs, logQLMetricGroupLabelValueExpr(cfg, label))
	}
	return labels, exprs
}

func logQLMetricGroupLabelValueExpr(cfg Config, label string) string {
	switch label {
	case "service_name":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "service_name"
	case "level", "severity", "severity_text", "detected_level":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "severity_text"
	case "trace_id":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "trace_id"
	case "span_id":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "span_id"
	default:
		return logQLLabelValueExpr(cfg, label)
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
		filters = append(filters, logQLMetricMatcherCondition(cfg, matcher))
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

func logQLMetricMatcherCondition(cfg Config, m logQLLabelMatcher) string {
	return logQLStringCondition(logQLMetricLabelFilterValueExpr(cfg, m.name), m.op, m.value, true)
}

func logQLMetricLabelFilterValueExpr(cfg Config, label string) string {
	switch label {
	case "service_name", "service.name":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "service_name"
	case "level", "severity", "severity_text", "detected_level":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "severity_text"
	case "trace_id":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "trace_id"
	case "span_id":
		if !cfg.postHogLogSchemaLayout() {
			return logQLSnuffleLabelValueExpr(label)
		}
		return "span_id"
	default:
		return logQLStreamLabelValueExpr(cfg, label)
	}
}

func buildLogQLUnwrapMetricSQL(cfg Config, rangeAgg *logQLRangeAggregation) (string, []string, bool) {
	if rangeAgg == nil {
		return "", nil, false
	}
	switch rangeAgg.fn {
	case "sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "stdvar_over_time", "stddev_over_time", "first_over_time", "last_over_time":
	default:
		return "", nil, false
	}
	parserKind := ""
	parserParams := map[string]string{}
	filters := make([]string, 0, len(rangeAgg.selector.stages))
	unwrapValueExpr := ""
	for _, stage := range rangeAgg.selector.stages {
		switch stage.kind {
		case "line_filter":
			filters = append(filters, logQLLineFilterCondition(*stage.lineFilter))
		case "parser":
			if parserKind != "" {
				return "", nil, false
			}
			switch stage.parser {
			case "json":
				parserKind = "json"
				parserParams = parseParserParams(stage.parserParam)
			case "logfmt":
				if strings.TrimSpace(stage.parserParam) != "" {
					return "", nil, false
				}
				parserKind = "logfmt"
			case "regexp":
				params, ok := logQLSQLNamedCaptureParams(stage.parserParam)
				if !ok {
					return "", nil, false
				}
				parserKind = "regexp"
				parserParams = params
			case "pattern":
				re, err := compileLogQLPattern(stage.parserParam)
				if err != nil {
					return "", nil, false
				}
				params, ok := logQLSQLNamedCaptureParams(re.String())
				if !ok {
					return "", nil, false
				}
				parserKind = "pattern"
				parserParams = params
			default:
				return "", nil, false
			}
		case "label_filter":
			condition, ok := logQLSQLLabelFiltersCondition(cfg, stage.labelFilters, parserKind, parserParams)
			if !ok {
				return "", nil, false
			}
			if condition != "" {
				filters = append(filters, condition)
			}
		case "unwrap":
			valueExpr, ok := logQLSQLPipelineLabelValueExpr(cfg, stage.unwrapLabel, parserKind, parserParams, false)
			if !ok {
				return "", nil, false
			}
			unwrapValueExpr = logQLSQLUnwrapValueExpr(valueExpr, stage.unwrapFunc)
			if unwrapValueExpr == "" {
				return "", nil, false
			}
		default:
			return "", nil, false
		}
	}
	if unwrapValueExpr == "" {
		return "", nil, false
	}
	return unwrapValueExpr, filters, true
}

func logQLSQLLabelFiltersCondition(cfg Config, filters []logQLLabelFilter, parserKind string, parserParams map[string]string) (string, bool) {
	if len(filters) == 0 {
		return "", true
	}
	first, ok := logQLSQLLabelFilterCondition(cfg, filters[0], parserKind, parserParams)
	if !ok {
		return "", false
	}
	condition := first
	for i := 1; i < len(filters); i++ {
		next, ok := logQLSQLLabelFilterCondition(cfg, filters[i], parserKind, parserParams)
		if !ok {
			return "", false
		}
		connector := "AND"
		if filters[i-1].connector == "or" {
			connector = "OR"
		}
		condition = "(" + condition + " " + connector + " " + next + ")"
	}
	return condition, true
}

func logQLSQLLabelFilterCondition(cfg Config, filter logQLLabelFilter, parserKind string, parserParams map[string]string) (string, bool) {
	if filter.name == "__error__" {
		switch filter.op {
		case "=", "==":
			return sqlString(filter.value) + " = ''", true
		case "!=":
			return sqlString(filter.value) + " != ''", true
		default:
			return "", false
		}
	}
	valueExpr, ok := logQLSQLPipelineLabelValueExpr(cfg, filter.name, parserKind, parserParams, true)
	if !ok {
		return "", false
	}
	switch filter.op {
	case "=", "==":
		return logQLStringCondition(valueExpr, "=", filter.value, true), true
	case "!=", "=~", "!~":
		return logQLStringCondition(valueExpr, filter.op, filter.value, true), true
	case ">", ">=", "<", "<=":
		if !filter.numeric {
			return "", false
		}
		left := logQLSQLComparableValueExpr(valueExpr, filter.valueType)
		if left == "" {
			return "", false
		}
		return left + " " + filter.op + " " + strconv.FormatFloat(filter.numValue, 'g', -1, 64), true
	default:
		return "", false
	}
}

func logQLSQLPipelineLabelValueExpr(cfg Config, label, parserKind string, parserParams map[string]string, withFallback bool) (string, bool) {
	switch parserKind {
	case "logfmt":
		parsed := "extractKeyValuePairs(body, '=', ' ')[" + sqlString(label) + "]"
		if !withFallback {
			return parsed, true
		}
		fallback := logQLMetricLabelFilterValueExpr(cfg, label)
		return "ifNull(nullIf(" + parsed + ", ''), " + fallback + ")", true
	case "json":
		path := parserParams[label]
		if path == "" {
			path = label
		}
		parsed := "JSON_VALUE(body, " + sqlString(logQLJSONSQLPath(path)) + ")"
		if !withFallback {
			return parsed, true
		}
		fallback := logQLMetricLabelFilterValueExpr(cfg, label)
		return "ifNull(nullIf(" + parsed + ", ''), " + fallback + ")", true
	case "regexp", "pattern":
		index := parserParams[label]
		if index == "" {
			if withFallback {
				return logQLMetricLabelFilterValueExpr(cfg, label), true
			}
			return "", false
		}
		parsed := "arrayElement(extractGroups(body, " + sqlString(parserParams["__regex"]) + "), " + index + ")"
		if !withFallback {
			return parsed, true
		}
		fallback := logQLMetricLabelFilterValueExpr(cfg, label)
		return "ifNull(nullIf(" + parsed + ", ''), " + fallback + ")", true
	case "":
		return logQLMetricLabelFilterValueExpr(cfg, label), true
	default:
		return "", false
	}
}

func logQLSQLNamedCaptureParams(pattern string) (map[string]string, bool) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, false
	}
	params := map[string]string{"__regex": pattern}
	haveNamedCapture := false
	for i, name := range re.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		params[sanitizeLogLabelName(name)] = strconv.Itoa(i)
		haveNamedCapture = true
	}
	return params, haveNamedCapture
}

func logQLJSONSQLPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "$"
	}
	if strings.HasPrefix(path, "$") {
		return path
	}
	if strings.HasPrefix(path, ".") || strings.HasPrefix(path, "[") {
		return "$" + path
	}
	return "$." + path
}

func logQLSQLUnwrapValueExpr(valueExpr, conversion string) string {
	switch conversion {
	case "":
		return "toFloat64OrNull(" + valueExpr + ")"
	case "duration", "duration_seconds":
		return logQLSQLDurationSecondsExpr(valueExpr)
	case "bytes":
		return logQLSQLBytesValueExpr(valueExpr)
	default:
		return ""
	}
}

func logQLSQLComparableValueExpr(valueExpr, valueType string) string {
	switch valueType {
	case "duration":
		return "(" + logQLSQLDurationSecondsExpr(valueExpr) + " * 1000000000)"
	case "bytes":
		return logQLSQLBytesValueExpr(valueExpr)
	default:
		return "toFloat64OrNull(" + valueExpr + ")"
	}
}

func logQLSQLDurationSecondsExpr(valueExpr string) string {
	normalized := "replaceAll(" + valueExpr + ", 'μs', 'us')"
	pattern := `^\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:ns|us|µs|μs|ms|s|m|h|d|w)?\s*$`
	return "if(match(" + valueExpr + ", " + sqlString(pattern) + "), parseTimeDelta(" + normalized + "), CAST(NULL, 'Nullable(Float64)'))"
}

func logQLSQLBytesValueExpr(valueExpr string) string {
	trimmed := "trim(" + valueExpr + ")"
	number := "toFloat64OrNull(extract(" + trimmed + ", " + sqlString(`^\s*([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))`) + "))"
	unit := "lowerUTF8(extract(" + trimmed + ", " + sqlString(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\s*([A-Za-z]+)\s*$`) + "))"
	multiplier := "multiIf(" +
		unit + " IN ('', 'b'), 1.0, " +
		unit + " IN ('kb', 'k'), 1000.0, " +
		unit + " IN ('mb', 'm'), 1000000.0, " +
		unit + " IN ('gb', 'g'), 1000000000.0, " +
		unit + " IN ('tb', 't'), 1000000000000.0, " +
		unit + " IN ('kib', 'ki'), 1024.0, " +
		unit + " IN ('mib', 'mi'), 1048576.0, " +
		unit + " IN ('gib', 'gi'), 1073741824.0, " +
		unit + " IN ('tib', 'ti'), 1099511627776.0, " +
		"CAST(NULL, 'Nullable(Float64)'))"
	return number + " * " + multiplier
}

func (s *Server) queryLogQLMetricSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	if step <= 0 {
		step = time.Minute
	}
	if plan.unwrapValueExpr != "" {
		return s.queryLogQLUnwrapMetricSQL(ctx, plan, startNS, endNS, step)
	}
	if logQLMetricSnuffleStatsSQLSafe(s.cfg, plan, step) {
		return s.queryLogQLMetricSnuffleStatsSQL(ctx, plan, startNS, endNS, step)
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
		logQLLogsSourceSQL(s.cfg, where),
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
	sql = logQLMetricResultSQL(sql, plan)

	seriesByKey := map[string]*logMetricMatrixResult{}
	groupValues := make([]string, len(plan.groupLabels))
	dest := make([]any, 0, 2+len(groupValues))
	var evalNS int64
	var value float64
	dest = append(dest, &evalNS)
	for i := range groupValues {
		dest = append(dest, &groupValues[i])
	}
	dest = append(dest, &value)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		if err := row.Scan(dest...); err != nil {
			return err
		}
		key := logQLGroupValuesKey(groupValues)
		result := seriesByKey[key]
		if result == nil {
			result = &logMetricMatrixResult{Metric: logQLLabelsFromGroupValues(plan.groupLabels, groupValues)}
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

func (s *Server) queryLogQLUnwrapMetricSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	if logQLUnwrapMetricBucketSQLSafe(plan, step) {
		return s.queryLogQLUnwrapMetricBucketSQL(ctx, plan, startNS, endNS, step)
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
	where := logQLUnwrapMetricBaseFilters(s.cfg, plan.rangeAgg.selector, fetchStartNS, fetchEndNS, plan.selectorFilters)
	sourceSQL := logQLUnwrapMetricSourceSQL(s.cfg, plan, where)

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
	selects = append(selects, logQLUnwrapWindowAggregateExpr(plan.rangeAgg.fn)+" AS value")

	sql := fmt.Sprintf(
		"WITH toInt64(%d) AS start_ns, toInt64(%d) AS step_ns, toInt64(%d) AS window_ns SELECT %s FROM %s AS logs CROSS JOIN (SELECT start_ns + (toInt64(number) * step_ns) AS eval_ns FROM numbers(%d)) AS grid WHERE logs.ts_ns > eval_ns - window_ns AND logs.ts_ns <= eval_ns GROUP BY %s ORDER BY %s",
		startNS,
		step.Nanoseconds(),
		plan.rangeAgg.window.Nanoseconds(),
		strings.Join(selects, ", "),
		sourceSQL,
		points,
		strings.Join(groupBy, ", "),
		strings.Join(orderBy, ", "),
	)
	sql = logQLMetricResultSQL(sql, plan)
	return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
}

func logQLUnwrapMetricBucketSQLSafe(plan *logQLMetricSQLPlan, step time.Duration) bool {
	if plan == nil || plan.rangeAgg == nil || plan.unwrapValueExpr == "" || step <= 0 {
		return false
	}
	switch plan.rangeAgg.fn {
	case "sum_over_time":
	default:
		return false
	}
	return plan.rangeAgg.window%step == 0 && plan.rangeAgg.offset == 0
}

func (s *Server) queryLogQLUnwrapMetricBucketSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	fetchStartNS := startNS - plan.rangeAgg.window.Nanoseconds()
	fetchEndNS := endNS
	where := logQLUnwrapMetricBaseFilters(s.cfg, plan.rangeAgg.selector, fetchStartNS, fetchEndNS, plan.selectorFilters)
	sourceSQL := logQLUnwrapMetricSourceSQL(s.cfg, plan, where)

	bucketSelects := []string{
		"intDiv(ts_ns + step_ns - 1, step_ns) * step_ns AS bucket_ns",
		"count() AS value_count",
		"sum(unwrap_value) AS value_sum",
		"sum(unwrap_value * unwrap_value) AS value_sum_squares",
		"min(unwrap_value) AS value_min",
		"max(unwrap_value) AS value_max",
	}
	bucketGroupBy := []string{"bucket_ns"}
	outerSelects := []string{"eval_ns"}
	outerGroupBy := []string{"eval_ns"}
	orderBy := make([]string, 0, len(plan.groupLabels)+1)
	for i := range plan.groupExprs {
		alias := fmt.Sprintf("label_%d", i)
		bucketSelects = append(bucketSelects, alias)
		bucketGroupBy = append(bucketGroupBy, alias)
		outerSelects = append(outerSelects, alias)
		outerGroupBy = append(outerGroupBy, alias)
		orderBy = append(orderBy, alias)
	}
	orderBy = append(orderBy, "eval_ns")
	outerSelects = append(outerSelects, logQLUnwrapBucketAggregateExpr(plan.rangeAgg.fn)+" AS value")

	bucketSQL := fmt.Sprintf(
		"(SELECT %s FROM %s GROUP BY %s)",
		strings.Join(bucketSelects, ", "),
		sourceSQL,
		strings.Join(bucketGroupBy, ", "),
	)
	windowSteps := plan.rangeAgg.window / step
	if windowSteps < 1 {
		windowSteps = 1
	}
	fanoutSelects := append([]string{}, outerGroupBy...)
	fanoutSelects = append(fanoutSelects, "value_count", "value_sum", "value_sum_squares", "value_min", "value_max")
	fanoutSQL := fmt.Sprintf(
		"(SELECT %s FROM (SELECT *, start_ns + (intDiv(greatest(bucket_ns - start_ns, toInt64(0)) + step_ns - 1, step_ns) * step_ns) + (toInt64(offset) * step_ns) AS eval_ns FROM %s ARRAY JOIN range(toUInt64(%d)) AS offset) WHERE eval_ns >= start_ns AND eval_ns <= end_ns AND bucket_ns > eval_ns - window_ns AND bucket_ns <= eval_ns)",
		strings.Join(fanoutSelects, ", "),
		bucketSQL,
		int64(windowSteps),
	)
	sql := fmt.Sprintf(
		"WITH toInt64(%d) AS start_ns, toInt64(%d) AS end_ns, toInt64(%d) AS step_ns, toInt64(%d) AS window_ns SELECT %s FROM %s GROUP BY %s ORDER BY %s",
		startNS,
		endNS,
		step.Nanoseconds(),
		plan.rangeAgg.window.Nanoseconds(),
		strings.Join(outerSelects, ", "),
		fanoutSQL,
		strings.Join(outerGroupBy, ", "),
		strings.Join(orderBy, ", "),
	)
	sql = logQLMetricResultSQL(sql, plan)
	return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
}

func logQLUnwrapMetricBaseFilters(cfg Config, selector logQLSelector, startNS, endNS int64, selectorFilters []string) []string {
	filters := []string{
		teamFilter(cfg),
		"timestamp >= " + chTimeNanos(startNS),
		"timestamp <= " + chTimeNanos(endNS),
		"time_bucket >= toStartOfDay(" + chTimeNanos(startNS) + ")",
		"time_bucket <= toStartOfDay(" + chTimeNanos(endNS) + ")",
	}
	for _, matcher := range selector.matchers {
		filters = append(filters, logQLMetricMatcherCondition(cfg, matcher))
	}
	filters = append(filters, selectorFilters...)
	return filters
}

func logQLUnwrapMetricSourceSQL(cfg Config, plan *logQLMetricSQLPlan, where []string) string {
	selects := []string{"ts_ns", plan.unwrapValueExpr + " AS unwrap_value"}
	for i, expr := range plan.groupExprs {
		selects = append(selects, expr+" AS "+fmt.Sprintf("label_%d", i))
	}
	return fmt.Sprintf(
		"(SELECT %s FROM %s WHERE isNotNull(unwrap_value))",
		strings.Join(selects, ", "),
		logQLLogsSourceSQL(cfg, where),
	)
}

func logQLUnwrapWindowAggregateExpr(fn string) string {
	switch fn {
	case "sum_over_time":
		return "toFloat64(assumeNotNull(sum(unwrap_value)))"
	case "avg_over_time":
		return "toFloat64(assumeNotNull(avg(unwrap_value)))"
	case "min_over_time":
		return "toFloat64(assumeNotNull(min(unwrap_value)))"
	case "max_over_time":
		return "toFloat64(assumeNotNull(max(unwrap_value)))"
	case "first_over_time":
		return "toFloat64(assumeNotNull(argMin(unwrap_value, ts_ns)))"
	case "last_over_time":
		return "toFloat64(assumeNotNull(argMax(unwrap_value, ts_ns)))"
	case "stdvar_over_time":
		return "toFloat64(greatest(0.0, assumeNotNull(varPop(unwrap_value))))"
	case "stddev_over_time":
		return "toFloat64(sqrt(greatest(0.0, assumeNotNull(varPop(unwrap_value)))))"
	default:
		return "toFloat64(0)"
	}
}

func logQLUnwrapBucketAggregateExpr(fn string) string {
	mean := "assumeNotNull(sum(value_sum)) / sum(value_count)"
	variance := "greatest(0.0, assumeNotNull(sum(value_sum_squares)) / sum(value_count) - pow(" + mean + ", 2))"
	switch fn {
	case "sum_over_time":
		return "toFloat64(assumeNotNull(sum(value_sum)))"
	case "avg_over_time":
		return "toFloat64(" + mean + ")"
	case "min_over_time":
		return "toFloat64(assumeNotNull(min(value_min)))"
	case "max_over_time":
		return "toFloat64(assumeNotNull(max(value_max)))"
	case "stdvar_over_time":
		return "toFloat64(" + variance + ")"
	case "stddev_over_time":
		return "toFloat64(sqrt(" + variance + "))"
	default:
		return "toFloat64(0)"
	}
}

func logQLMetricResultSQL(sql string, plan *logQLMetricSQLPlan) string {
	if plan == nil || (plan.comparison == nil && !plan.hasTopK) {
		return sql
	}
	current := sql
	if plan.comparison != nil {
		current = "SELECT * FROM (" + current + ") WHERE " + logQLSQLComparisonCondition(plan.comparison)
	}
	if plan.hasTopK {
		direction := "DESC"
		if !plan.topKIsTop {
			direction = "ASC"
		}
		current = fmt.Sprintf("SELECT * FROM (SELECT * FROM (%s) ORDER BY eval_ns ASC, value %s LIMIT %d BY eval_ns)", current, direction, plan.topK)
	}
	return "SELECT * FROM (" + current + ") ORDER BY " + logQLMetricResultOrderBy(plan)
}

func logQLSQLComparisonCondition(cmp *logQLComparison) string {
	if cmp == nil {
		return "1"
	}
	return "value " + cmp.op + " " + strconv.FormatFloat(cmp.value, 'g', -1, 64)
}

func logQLMetricResultOrderBy(plan *logQLMetricSQLPlan) string {
	orderBy := make([]string, 0, len(plan.groupLabels)+1)
	for i := range plan.groupLabels {
		orderBy = append(orderBy, fmt.Sprintf("label_%d", i))
	}
	orderBy = append(orderBy, "eval_ns")
	return strings.Join(orderBy, ", ")
}

func logQLMetricBucketSQLSafe(plan *logQLMetricSQLPlan, step time.Duration) bool {
	if plan == nil || plan.rangeAgg == nil || step <= 0 {
		return false
	}
	switch plan.rangeAgg.fn {
	case "count_over_time", "rate", "bytes_over_time", "bytes_rate":
	default:
		return false
	}
	return plan.rangeAgg.window%step == 0 && plan.rangeAgg.offset == 0
}

func logQLMetricSnuffleStatsSQLSafe(cfg Config, plan *logQLMetricSQLPlan, step time.Duration) bool {
	if cfg.postHogLogSchemaLayout() || cfg.LogStreamStatsTable == "" || cfg.LogStreamsTable == "" {
		return false
	}
	if !logQLMetricBucketSQLSafe(plan, step) || step < time.Minute || step%time.Minute != 0 {
		return false
	}
	if len(plan.rangeAgg.selector.stages) != 0 {
		return false
	}
	for _, matcher := range plan.rangeAgg.selector.matchers {
		if !logQLSnuffleStatsLabelSafe(matcher.name) {
			return false
		}
	}
	for _, label := range plan.groupLabels {
		if !logQLSnuffleStatsLabelSafe(label) {
			return false
		}
	}
	return true
}

func logQLSnuffleStatsLabelSafe(label string) bool {
	switch label {
	case "trace_id", "span_id":
		return false
	default:
		return true
	}
}

func logQLSnuffleStatsLabelIndexJoinCount(cfg Config, plan *logQLMetricSQLPlan) int {
	if cfg.postHogLogSchemaLayout() || cfg.LogStreamLabelsTable == "" || plan == nil || plan.rangeAgg == nil {
		return 0
	}
	if len(plan.groupLabels) == 0 && len(plan.rangeAgg.selector.matchers) == 0 {
		return 0
	}
	indexJoinCount := 0
	for _, label := range plan.groupLabels {
		if !logQLSnuffleStatsLabelSafe(label) {
			return 0
		}
		if logQLSnuffleStatsCoreLabel(label) {
			return 0
		}
		indexJoinCount++
	}
	for _, matcher := range plan.rangeAgg.selector.matchers {
		if !logQLSnuffleStatsLabelIndexMatcherSafe(matcher) {
			return 0
		}
		indexJoinCount++
	}
	return indexJoinCount
}

func logQLSnuffleStatsLabelIndexMatcherSafe(matcher logQLLabelMatcher) bool {
	if !logQLSnuffleStatsLabelSafe(matcher.name) {
		return false
	}
	switch matcher.op {
	case "=":
		return matcher.value != ""
	case "=~":
		re, err := regexp.Compile(promRegexToCH(matcher.value))
		return err == nil && !re.MatchString("")
	default:
		return false
	}
}

func logQLSnuffleStatsIndexedTableSQL(cfg Config, plan *logQLMetricSQLPlan) string {
	joins := make([]string, 0, len(plan.groupLabels)+len(plan.rangeAgg.selector.matchers))
	for i, label := range plan.groupLabels {
		alias := fmt.Sprintf("label_%d", i)
		joins = append(joins, fmt.Sprintf(
			"ANY LEFT JOIN (SELECT team_id, stream_id, label_value AS %s FROM %s WHERE %s AND label_name = %s) AS group_label_%d USING (team_id, stream_id)",
			alias,
			tableName(cfg.CHDatabase, cfg.LogStreamLabelsTable),
			teamFilter(cfg),
			sqlString(label),
			i,
		))
	}
	for i, matcher := range plan.rangeAgg.selector.matchers {
		joins = append(joins, fmt.Sprintf(
			"ANY INNER JOIN (SELECT team_id, stream_id FROM %s WHERE %s AND %s AND %s) AS filter_label_%d USING (team_id, stream_id)",
			tableName(cfg.CHDatabase, cfg.LogStreamLabelsTable),
			teamFilter(cfg),
			logQLSnuffleStreamLabelKeyCondition("label_name", matcher.name),
			logQLSnuffleStatsLabelIndexMatcherCondition(matcher),
			i,
		))
	}
	return fmt.Sprintf(
		"%s AS stats %s",
		tableName(cfg.CHDatabase, cfg.LogStreamStatsTable),
		strings.Join(joins, " "),
	)
}

func logQLSnuffleStatsLabelIndexMatcherCondition(matcher logQLLabelMatcher) string {
	switch matcher.op {
	case "=":
		return "label_value = " + sqlString(matcher.value)
	case "=~":
		return "match(label_value, " + sqlString(promRegexToCH(matcher.value)) + ")"
	default:
		return "1"
	}
}

func (s *Server) queryLogQLMetricSnuffleStatsSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	points := int64(1)
	if endNS > startNS {
		points = ((endNS - startNS) / step.Nanoseconds()) + 1
	}
	if points < 1 {
		points = 1
	}
	fetchStartNS := startNS - plan.rangeAgg.window.Nanoseconds()
	fetchEndNS := endNS
	labelIndexJoinCount := logQLSnuffleStatsLabelIndexJoinCount(s.cfg, plan)
	useLabelIndex := labelIndexJoinCount > 0
	where := logQLSnuffleStatsBaseFilters(s.cfg, plan.rangeAgg.selector, fetchStartNS, fetchEndNS)
	sourceSQL := logQLSnuffleStatsTableSQL(s.cfg)
	bucketSettings := ""
	if useLabelIndex {
		where = logQLSnuffleStatsTimeFilters(s.cfg, fetchStartNS, fetchEndNS)
		sourceSQL = logQLSnuffleStatsIndexedTableSQL(s.cfg, plan)
		if labelIndexJoinCount == 1 {
			bucketSettings = logQLSnuffleFullSortingMergeJoinSettings(s.cfg)
		} else {
			bucketSettings = " SETTINGS join_algorithm = 'hash'"
		}
	}

	minuteEndNS := "(toInt64(toUnixTimestamp(bucket)) * 1000000000 + 60000000000)"
	bucketSelects := []string{
		"intDiv(" + minuteEndNS + " + step_ns - 1, step_ns) * step_ns AS bucket_ns",
		"sum(log_count) AS log_count",
		"sum(byte_count) AS byte_count",
	}
	bucketGroupBy := []string{"bucket_ns"}
	outerSelects := []string{"eval_ns"}
	orderBy := make([]string, 0, len(plan.groupLabels)+1)
	for i, label := range plan.groupLabels {
		alias := fmt.Sprintf("label_%d", i)
		labelExpr := logQLSnuffleStatsLabelValueExpr(label)
		if useLabelIndex {
			labelExpr = alias
		}
		bucketSelects = append(bucketSelects, labelExpr+" AS "+alias)
		bucketGroupBy = append(bucketGroupBy, alias)
		outerSelects = append(outerSelects, alias)
		orderBy = append(orderBy, alias)
	}
	orderBy = append(orderBy, "eval_ns")

	bucketSQL := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY %s%s",
		strings.Join(bucketSelects, ", "),
		sourceSQL,
		strings.Join(where, " AND "),
		strings.Join(bucketGroupBy, ", "),
		bucketSettings,
	)
	windowSteps := plan.rangeAgg.window / step
	if windowSteps < 1 {
		windowSteps = 1
	}
	if plan.rangeAgg.fn == "count_over_time" && len(plan.groupLabels) > 0 && !plan.hasTopK {
		fanoutSelects := []string{"eval_ns"}
		outerGroupBy := []string{"eval_ns"}
		for i := range plan.groupLabels {
			alias := fmt.Sprintf("label_%d", i)
			fanoutSelects = append(fanoutSelects, alias)
			outerGroupBy = append(outerGroupBy, alias)
		}
		fanoutSelects = append(fanoutSelects, "log_count", "byte_count")
		fanoutSQL := fmt.Sprintf(
			"(SELECT %s FROM (SELECT *, start_ns + (intDiv(greatest(bucket_ns - start_ns, toInt64(0)) + step_ns - 1, step_ns) * step_ns) + (toInt64(offset) * step_ns) AS eval_ns FROM (%s) ARRAY JOIN range(toUInt64(%d)) AS offset) WHERE eval_ns >= start_ns AND eval_ns <= end_ns AND bucket_ns > eval_ns - window_ns AND bucket_ns <= eval_ns)",
			strings.Join(fanoutSelects, ", "),
			bucketSQL,
			int64(windowSteps),
		)
		sql := fmt.Sprintf(
			"WITH toInt64(%d) AS start_ns, toInt64(%d) AS end_ns, toInt64(%d) AS step_ns, toInt64(%d) AS window_ns SELECT %s, toFloat64(sum(log_count)) AS value FROM %s GROUP BY %s ORDER BY %s",
			startNS,
			endNS,
			step.Nanoseconds(),
			plan.rangeAgg.window.Nanoseconds(),
			strings.Join(outerSelects, ", "),
			fanoutSQL,
			strings.Join(outerGroupBy, ", "),
			strings.Join(orderBy, ", "),
		)
		sql = logQLMetricResultSQL(sql, plan)
		return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
	}

	windowFrame := int64(windowSteps) - 1
	groupSelects := []string{"1 AS group_key"}
	gridSelects := []string{"start_ns + (toInt64(n.number) * step_ns) AS eval_ns"}
	joinConditions := []string{"buckets.bucket_ns = grid.eval_ns"}
	partitionBy := ""
	if len(plan.groupLabels) > 0 {
		groupSelects = groupSelects[:0]
		for i := range plan.groupLabels {
			alias := fmt.Sprintf("label_%d", i)
			groupSelects = append(groupSelects, alias)
			gridSelects = append(gridSelects, alias)
			joinConditions = append(joinConditions, "buckets."+alias+" = grid."+alias)
		}
		partitionBy = "PARTITION BY " + strings.Join(groupSelects, ", ") + " "
	}
	groupsSQL := "SELECT 1 AS group_key"
	if len(plan.groupLabels) > 0 {
		groupsSQL = "SELECT DISTINCT " + strings.Join(groupSelects, ", ") + " FROM buckets"
	}
	valueExpr := "toFloat64(window_log_count)"
	switch plan.rangeAgg.fn {
	case "rate":
		valueExpr = fmt.Sprintf("toFloat64(window_log_count) / %s", formatDurationSeconds(plan.rangeAgg.window))
	case "bytes_over_time":
		valueExpr = "toFloat64(window_byte_count)"
	case "bytes_rate":
		valueExpr = fmt.Sprintf("toFloat64(window_byte_count) / %s", formatDurationSeconds(plan.rangeAgg.window))
	}
	sql := fmt.Sprintf(
		"WITH toInt64(%d) AS start_ns, toInt64(%d) AS step_ns, buckets AS (%s), groups AS (%s) SELECT %s, %s AS value FROM (SELECT %s, sum(ifNull(log_count, 0)) OVER (%sORDER BY eval_ns ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS window_log_count, sum(ifNull(byte_count, 0)) OVER (%sORDER BY eval_ns ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS window_byte_count FROM (SELECT %s FROM groups CROSS JOIN numbers(%d) AS n) AS grid LEFT JOIN buckets ON %s) WHERE window_log_count > 0 ORDER BY %s",
		startNS,
		step.Nanoseconds(),
		bucketSQL,
		groupsSQL,
		strings.Join(outerSelects, ", "),
		valueExpr,
		strings.Join(outerSelects, ", "),
		partitionBy,
		windowFrame,
		partitionBy,
		windowFrame,
		strings.Join(gridSelects, ", "),
		points,
		strings.Join(joinConditions, " AND "),
		strings.Join(orderBy, ", "),
	)
	sql = logQLMetricResultSQL(sql, plan)
	return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
}

func logQLSnuffleStatsTableSQL(cfg Config) string {
	return fmt.Sprintf(
		"(SELECT stats.team_id AS team_id, stats.bucket AS bucket, stats.stream_id AS stream_id, stats.log_count AS log_count, stats.byte_count AS byte_count, streams.labels AS labels, streams.resource_attributes AS resource_attributes, streams.service_name AS stream_service_name, streams.severity_text AS stream_severity_text FROM %s AS stats ANY INNER JOIN %s AS streams USING (team_id, stream_id)%s)",
		tableName(cfg.CHDatabase, cfg.LogStreamStatsTable),
		tableName(cfg.CHDatabase, cfg.LogStreamsTable),
		logQLSnuffleFullSortingMergeJoinSettings(cfg),
	)
}

func logQLSnuffleStatsLabelValueExpr(label string) string {
	if logQLSnuffleStatsCoreLabel(label) {
		switch label {
		case "service_name", "service.name":
			return "stream_service_name"
		default:
			return "stream_severity_text"
		}
	}
	return logQLSnuffleStreamOnlyLabelValueExpr(label)
}

func logQLSnuffleStatsCoreLabel(label string) bool {
	switch label {
	case "service_name", "service.name":
		return true
	case "level", "severity", "severity_text", "detected_level":
		return true
	default:
		return false
	}
}

func logQLSnuffleStatsTimeFilters(cfg Config, startNS, endNS int64) []string {
	return []string{
		teamFilter(cfg),
		"bucket >= toStartOfMinute(" + chTimeNanos(startNS) + ")",
		"bucket <= toStartOfMinute(" + chTimeNanos(endNS) + ")",
	}
}

func logQLSnuffleStatsBaseFilters(cfg Config, selector logQLSelector, startNS, endNS int64) []string {
	filters := logQLSnuffleStatsTimeFilters(cfg, startNS, endNS)
	for _, matcher := range selector.matchers {
		filters = append(filters, logQLStringCondition(logQLSnuffleStatsLabelValueExpr(matcher.name), matcher.op, matcher.value, true))
	}
	return filters
}

func (s *Server) queryLogQLMetricBucketSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
	if !logQLMetricPreferWindowBucketSQL(plan) {
		return s.queryLogQLMetricBucketJoinSQL(ctx, plan, startNS, endNS, step)
	}
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

	bucketSelects := []string{"intDiv(toInt64(toUnixTimestamp64Nano(timestamp)) + step_ns - 1, step_ns) * step_ns AS bucket_ns", "count() AS log_count", "sum(length(body)) AS byte_count"}
	bucketGroupBy := []string{"bucket_ns"}
	outerSelects := []string{"eval_ns"}
	orderBy := make([]string, 0, len(plan.groupLabels)+1)
	for i, expr := range plan.groupExprs {
		alias := fmt.Sprintf("label_%d", i)
		bucketSelects = append(bucketSelects, expr+" AS "+alias)
		bucketGroupBy = append(bucketGroupBy, alias)
		outerSelects = append(outerSelects, alias)
		orderBy = append(orderBy, alias)
	}
	orderBy = append(orderBy, "eval_ns")

	bucketSQL := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY %s",
		strings.Join(bucketSelects, ", "),
		logQLLogsTableSQL(s.cfg),
		strings.Join(where, " AND "),
		strings.Join(bucketGroupBy, ", "),
	)
	windowSteps := plan.rangeAgg.window / step
	if windowSteps < 1 {
		windowSteps = 1
	}
	windowFrame := int64(windowSteps) - 1
	groupSelects := []string{"1 AS group_key"}
	gridSelects := []string{"start_ns + (toInt64(n.number) * step_ns) AS eval_ns"}
	joinConditions := []string{"buckets.bucket_ns = grid.eval_ns"}
	partitionBy := ""
	if len(plan.groupLabels) > 0 {
		groupSelects = groupSelects[:0]
		for i := range plan.groupLabels {
			alias := fmt.Sprintf("label_%d", i)
			groupSelects = append(groupSelects, alias)
			gridSelects = append(gridSelects, alias)
			joinConditions = append(joinConditions, "buckets."+alias+" = grid."+alias)
		}
		partitionBy = "PARTITION BY " + strings.Join(groupSelects, ", ") + " "
	}
	groupsSQL := "SELECT " + strings.Join(groupSelects, ", ") + " FROM buckets"
	if len(plan.groupLabels) > 0 {
		groupsSQL = "SELECT DISTINCT " + strings.Join(groupSelects, ", ") + " FROM buckets"
	}
	valueExpr := "toFloat64(window_log_count)"
	switch plan.rangeAgg.fn {
	case "rate":
		valueExpr = fmt.Sprintf("toFloat64(window_log_count) / %s", formatDurationSeconds(plan.rangeAgg.window))
	case "bytes_over_time":
		valueExpr = "toFloat64(window_byte_count)"
	case "bytes_rate":
		valueExpr = fmt.Sprintf("toFloat64(window_byte_count) / %s", formatDurationSeconds(plan.rangeAgg.window))
	}
	sql := fmt.Sprintf(
		"WITH toInt64(%d) AS start_ns, toInt64(%d) AS step_ns, buckets AS (%s), groups AS (%s) SELECT %s, %s AS value FROM (SELECT %s, sum(ifNull(log_count, 0)) OVER (%sORDER BY eval_ns ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS window_log_count, sum(ifNull(byte_count, 0)) OVER (%sORDER BY eval_ns ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS window_byte_count FROM (SELECT %s FROM groups CROSS JOIN numbers(%d) AS n) AS grid LEFT JOIN buckets ON %s) WHERE window_log_count > 0 ORDER BY %s",
		startNS,
		step.Nanoseconds(),
		bucketSQL,
		groupsSQL,
		strings.Join(outerSelects, ", "),
		valueExpr,
		strings.Join(outerSelects, ", "),
		partitionBy,
		windowFrame,
		partitionBy,
		windowFrame,
		strings.Join(gridSelects, ", "),
		points,
		strings.Join(joinConditions, " AND "),
		strings.Join(orderBy, ", "),
	)
	sql = logQLMetricResultSQL(sql, plan)
	return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
}

func logQLMetricPreferWindowBucketSQL(plan *logQLMetricSQLPlan) bool {
	if plan == nil {
		return false
	}
	if plan.hasTopK || plan.comparison != nil {
		return true
	}
	for _, label := range plan.groupLabels {
		switch label {
		case "region", "env", "environment", "format", "level", "severity", "severity_text", "detected_level", "status":
			continue
		default:
			return true
		}
	}
	return false
}

func (s *Server) queryLogQLMetricBucketJoinSQL(ctx context.Context, plan *logQLMetricSQLPlan, startNS, endNS int64, step time.Duration) ([]logMetricMatrixResult, error) {
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

	bucketSelects := []string{"intDiv(toInt64(toUnixTimestamp64Nano(timestamp)) + step_ns - 1, step_ns) * step_ns AS bucket_ns", "count() AS log_count", "sum(length(body)) AS byte_count"}
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
	switch plan.rangeAgg.fn {
	case "rate":
		valueExpr = fmt.Sprintf("toFloat64(sum(log_count)) / %s", formatDurationSeconds(plan.rangeAgg.window))
	case "bytes_over_time":
		valueExpr = "toFloat64(sum(byte_count))"
	case "bytes_rate":
		valueExpr = fmt.Sprintf("toFloat64(sum(byte_count)) / %s", formatDurationSeconds(plan.rangeAgg.window))
	}
	outerSelects = append(outerSelects, valueExpr+" AS value")

	bucketSQL := fmt.Sprintf(
		"(SELECT %s FROM %s WHERE %s GROUP BY %s)",
		strings.Join(bucketSelects, ", "),
		logQLLogsTableSQL(s.cfg),
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
	sql = logQLMetricResultSQL(sql, plan)
	return s.scanLogQLMetricSQLResults(ctx, sql, plan.groupLabels)
}

func (s *Server) scanLogQLMetricSQLResults(ctx context.Context, sql string, groupLabels []string) ([]logMetricMatrixResult, error) {
	seriesByKey := map[string]*logMetricMatrixResult{}
	groupValues := make([]string, len(groupLabels))
	dest := make([]any, 0, 2+len(groupValues))
	var evalNS int64
	var value float64
	dest = append(dest, &evalNS)
	for i := range groupValues {
		dest = append(dest, &groupValues[i])
	}
	dest = append(dest, &value)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		if err := row.Scan(dest...); err != nil {
			return err
		}
		key := logQLGroupValuesKey(groupValues)
		result := seriesByKey[key]
		if result == nil {
			result = &logMetricMatrixResult{Metric: logQLLabelsFromGroupValues(groupLabels, groupValues)}
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

func logQLGroupValuesKey(values []string) string {
	if len(values) == 0 {
		return ""
	}
	var b strings.Builder
	for _, value := range values {
		b.WriteString(value)
		b.WriteByte('\xff')
	}
	return b.String()
}

func logQLLabelsFromGroupValues(groupLabels, groupValues []string) map[string]string {
	labels := make(map[string]string, len(groupLabels))
	for i, label := range groupLabels {
		if groupValues[i] != "" {
			labels[label] = groupValues[i]
		}
	}
	return stableLabelMap(labels)
}
