package snuffle

import (
	"context"
	"fmt"
	"math"
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

	switch e := expr.(type) {
	case *parser.AggregateExpr:
		if data, ok, err := s.tryFastAggregate(ctx, e, evalTime); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastTopK(ctx, e, evalTime); ok || err != nil {
			return data, ok, err
		}
	}
	return queryData{}, false, nil
}

func (s *Server) tryFastRangeQuery(ctx context.Context, query string, start, end time.Time, step time.Duration) (queryData, bool, error) {
	if step <= 0 {
		return queryData{}, false, nil
	}
	stepMillis := step.Milliseconds()
	if stepMillis <= 0 {
		return queryData{}, false, nil
	}
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		return queryData{}, false, nil
	}
	if selector, ok := expr.(*parser.VectorSelector); ok {
		return s.tryFastSelectorRangeQuery(ctx, selector, start, end, step, stepMillis)
	}
	if aggregate, ok := expr.(*parser.AggregateExpr); ok {
		if data, ok, err := s.tryFastNestedCountRangeQuery(ctx, aggregate, start, end, step, stepMillis); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastAggregateRangeQuery(ctx, aggregate, start, end, step, stepMillis); ok || err != nil {
			return data, ok, err
		}
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	if steps < 64 {
		return queryData{}, false, nil
	}
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(expr, start, end, step)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}

	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return queryData{}, false, nil
	}
	selectedSeries += fmt.Sprintf(" LIMIT %d", s.cfg.MaxSeries)
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt)

	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (%s) GROUP BY id",
		source.gridExpr,
		sampleSource,
	)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT id, metric_name, labels_json, per_series.vals AS vals FROM per_series ANY INNER JOIN selected_series USING id ORDER BY id",
		selectedSeries,
		perSeries,
	)

	results, err := s.queryRangeGridResults(ctx, sql, selector.LabelMatchers, start, end, stepMillis)
	if err != nil {
		return queryData{}, true, err
	}
	if len(results) >= s.cfg.MaxSeries {
		return queryData{}, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) tryFastSelectorRangeQuery(ctx context.Context, selector *parser.VectorSelector, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	if selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	mint := shiftedStart.Add(-s.cfg.LookbackDelta).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return queryData{}, false, nil
	}
	selectedSeries += fmt.Sprintf(" LIMIT %d", s.cfg.MaxSeries)
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt)
	gridExpr := fmt.Sprintf(
		"timeSeriesLastToGrid(%s, %s, %s, %s)(timestamp, value)",
		chTimeMillis(shiftedStart.UnixMilli()),
		chTimeMillis(shiftedEnd.UnixMilli()),
		formatDurationSeconds(step),
		formatDurationSeconds(s.cfg.LookbackDelta),
	)
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (%s) GROUP BY id",
		gridExpr,
		sampleSource,
	)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT id, metric_name, labels_json, per_series.vals AS vals FROM per_series ANY INNER JOIN selected_series USING id ORDER BY id",
		selectedSeries,
		perSeries,
	)

	results, err := s.queryRangeGridResults(ctx, sql, selector.LabelMatchers, start, end, stepMillis)
	if err != nil {
		return queryData{}, true, err
	}
	if len(results) >= s.cfg.MaxSeries {
		return queryData{}, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) queryRangeGridResults(ctx context.Context, sql string, matchers []*labels.Matcher, start, end time.Time, stepMillis int64) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 1024)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var labelsJSON string
		var vals []*float64
		if err := row.Scan(&id, &metricName, &labelsJSON, &vals); err != nil {
			return err
		}
		_ = id
		metric, ok, err := metricLabelMap(metricName, []byte(labelsJSON), matchers)
		if err != nil || !ok {
			return err
		}
		values := make([][]any, 0, len(vals))
		for i, value := range vals {
			if value == nil {
				continue
			}
			ts := start.UnixMilli() + int64(i)*stepMillis
			if ts > end.UnixMilli() {
				break
			}
			values = append(values, []any{float64(ts) / 1000, formatSample(*value)})
		}
		if len(values) > 0 {
			results = append(results, sampleResult{Metric: metric, Values: values})
		}
		return nil
	})
	return results, err
}

type rangeGridFunctionSpec struct {
	name     string
	increase bool
}

func rangeGridFunction(name string) (rangeGridFunctionSpec, bool) {
	switch name {
	case "rate":
		return rangeGridFunctionSpec{name: "timeSeriesRateToGrid"}, true
	case "irate":
		return rangeGridFunctionSpec{name: "timeSeriesInstantRateToGrid"}, true
	case "increase":
		return rangeGridFunctionSpec{name: "timeSeriesRateToGrid", increase: true}, true
	case "delta":
		return rangeGridFunctionSpec{name: "timeSeriesDeltaToGrid"}, true
	case "idelta":
		return rangeGridFunctionSpec{name: "timeSeriesInstantDeltaToGrid"}, true
	default:
		return rangeGridFunctionSpec{}, false
	}
}

func (s *Server) tryFastAggregate(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "value")
	if !ok {
		return queryData{}, false, nil
	}
	if data, ok, err := s.tryFastRangeFunctionAggregate(ctx, expr, evalTime, aggSQL); ok || err != nil {
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

	if data, ok, err := s.tryLabelIndexAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}

	return queryData{}, false, nil
}

func (s *Server) tryLabelIndexAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, selector.LabelMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		"latest AS (" + latestSamplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt) + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	source := aggregateSourceSQL("latest", expr.Grouping, groupJoin)
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

func (s *Server) tryFastAggregateRangeQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "sample_value")
	if !ok {
		return queryData{}, false, nil
	}
	if data, ok, err := s.tryFastAggregateRangeUnionQuery(ctx, expr, start, end, step, stepMillis, aggSQL); ok || err != nil {
		return data, ok, err
	}

	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(expr.Expr, start, end, step)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}

	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	if vectorSelector, ok := expr.Expr.(*parser.VectorSelector); ok && exactBucketRangeSelector(s.cfg, vectorSelector, start, end, step) {
		return s.queryFastExactAggregateRange(ctx, expr, selectedSeries, selector.LabelMatchers, start, end, aggSQL)
	}
	return s.queryFastAggregateRange(ctx, expr, selectedSeries, selector.LabelMatchers, source.gridExpr, mint, maxt, start, stepMillis, aggSQL)
}

func (s *Server) tryFastAggregateRangeUnionQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, stepMillis int64, aggSQL string) (queryData, bool, error) {
	selectors, ok := orVectorSelectors(expr.Expr)
	if !ok || len(selectors) < 2 || len(selectors) > 8 {
		return queryData{}, false, nil
	}

	var mint int64
	var maxt int64
	var shiftedStart time.Time
	var shiftedEnd time.Time
	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedParts := make([]string, 0, len(selectors))
	for i, selector := range selectors {
		source, selectorMint, selectorMaxt, ok := selectorRangeGridSource(s.cfg, selector, start, end, step)
		if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		if i == 0 {
			mint = selectorMint
			maxt = selectorMaxt
			shiftedStart = source.start
			shiftedEnd = source.end
		} else if selectorMint != mint || selectorMaxt != maxt || !source.start.Equal(shiftedStart) || !source.end.Equal(shiftedEnd) {
			return queryData{}, false, nil
		}
		selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, selectorMint, selectorMaxt, selectedProjection)
		if !ok {
			return queryData{}, false, nil
		}
		selectedParts = append(selectedParts, selectedSeries)
	}
	selectedSeries := strings.Join(selectedParts, " UNION DISTINCT ")
	gridExpr := lastGridExpr(shiftedStart, shiftedEnd, step, s.cfg.LookbackDelta)
	return s.queryFastAggregateRange(ctx, expr, selectedSeries, nil, gridExpr, mint, maxt, start, stepMillis, aggSQL)
}

func (s *Server) queryFastAggregateRange(ctx context.Context, expr *parser.AggregateExpr, selectedSeries string, groupMatchers []*labels.Matcher, gridExpr string, mint, maxt int64, start time.Time, stepMillis int64, aggSQL string) (queryData, bool, error) {
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, groupMatchers, mint, maxt)
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (%s) GROUP BY id",
		gridExpr,
		sampleSource,
	)

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, groupMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		"per_series AS (" + perSeries + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, fmt.Sprintf("toInt64(%d) + (toInt64(idx) - 1) * %d AS ts", start.UnixMilli(), stepMillis))
	selectParts = append(selectParts, aggSQL+" AS value")

	groupByParts := append([]string{}, groupBy...)
	groupByParts = append(groupByParts, "idx")
	orderByParts := append([]string{}, groupBy...)
	orderByParts = append(orderByParts, "idx")

	rangeSource := aggregateSourceSQL("per_series", expr.Grouping, groupJoin)
	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM %s ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE isNotNull(sample_value) GROUP BY %s ORDER BY %s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		rangeSource,
		strings.Join(groupByParts, ", "),
		strings.Join(orderByParts, ", "),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) queryFastExactAggregateRange(ctx context.Context, expr *parser.AggregateExpr, selectedSeries string, groupMatchers []*labels.Matcher, start, end time.Time, aggSQL string) (queryData, bool, error) {
	where := []string{
		teamFilter(s.cfg),
		fmt.Sprintf("timestamp >= %s", chTimeMillis(start.UnixMilli())),
		fmt.Sprintf("timestamp <= %s", chTimeMillis(end.UnixMilli())),
		"id IN (SELECT id FROM selected_series)",
	}
	where = append(where, metricNameConstraints(groupMatchers)...)
	samples := fmt.Sprintf(
		"SELECT id, timestamp, value AS sample_value FROM %s WHERE %s",
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, groupMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		"samples AS (" + samples + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toUnixTimestamp64Milli(timestamp) AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	groupByParts := append([]string{}, groupBy...)
	groupByParts = append(groupByParts, "ts")
	orderByParts := append([]string{}, groupBy...)
	orderByParts = append(orderByParts, "ts")

	source := aggregateSourceSQL("samples", expr.Grouping, groupJoin)
	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM %s GROUP BY %s ORDER BY %s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		source,
		strings.Join(groupByParts, ", "),
		strings.Join(orderByParts, ", "),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) queryAggregateRangeRows(ctx context.Context, sql string, grouping []string) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 128)
	seen := make(map[string]int, 128)
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
		metric, key := groupingMetricAndKeyValues(groupValues, grouping)
		idx, ok := seen[key]
		if !ok {
			idx = len(results)
			seen[key] = idx
			results = append(results, sampleResult{Metric: metric})
		}
		results[idx].Values = append(results[idx].Values, []any{float64(ts) / 1000, formatSample(value)})
		return nil
	})
	return results, err
}

func (s *Server) tryFastNestedCountRangeQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	if expr.Op != parser.COUNT || expr.Without || len(expr.Grouping) != 0 {
		return queryData{}, false, nil
	}
	inner, ok := expr.Expr.(*parser.AggregateExpr)
	if !ok || inner.Op != parser.COUNT || inner.Without || len(inner.Grouping) == 0 {
		return queryData{}, false, nil
	}
	if groupingHasMetricName(inner.Grouping) {
		return queryData{}, false, nil
	}
	selector, ok := inner.Expr.(*parser.VectorSelector)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	source, mint, maxt, ok := selectorRangeGridSource(s.cfg, selector, start, end, step)
	if !ok {
		return queryData{}, false, nil
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	if steps <= 0 {
		return queryData{}, false, nil
	}
	if sql, ok := nestedCountSamplesRangeSQL(s.cfg, selector.LabelMatchers, inner.Grouping, source.start, start, stepMillis, steps, s.cfg.LookbackDelta); ok {
		sql = withMaxThreads(sql, s.cfg.AggregateThreads)
		data, err := s.queryNestedCountRangeValues(ctx, sql)
		if err != nil {
			return queryData{}, true, err
		}
		return data, true, nil
	}
	if sql, ok := nestedCountBitmapRangeSQL(s.cfg, selector.LabelMatchers, inner.Grouping, source.start, start, stepMillis, steps, s.cfg.LookbackDelta); ok {
		sql = withMaxThreads(sql, s.cfg.AggregateThreads)
		data, err := s.queryNestedCountRangeValues(ctx, sql)
		if err != nil {
			return queryData{}, true, err
		}
		return data, true, nil
	}
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, []string{"id", "min_time", "max_time"})
	if !ok {
		return queryData{}, false, nil
	}
	sql, ok := nestedCountRangeSQL(s.cfg, selector.LabelMatchers, inner.Grouping, selectedSeries, source.start, start, stepMillis, steps, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	data, err := s.queryNestedCountRangeValues(ctx, sql)
	return data, true, err
}

func nestedCountSamplesRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	if cfg.SamplesTable == "" || cfg.LabelIndexTable == "" || len(grouping) != 1 || grouping[0] == labels.MetricName {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	idFilters, ok := sampleIDFiltersFromMatchers(cfg, matchers, evalStart.UnixMilli(), outputStart.UnixMilli()+((steps-1)*stepMillis))
	if !ok {
		return "", false
	}

	evalMillisExpr := fmt.Sprintf("(%d + toInt64(step_idx) * %d)", evalStart.UnixMilli(), stepMillis)
	lookbackStartExpr := fmt.Sprintf("(%s - %d)", evalMillisExpr, lookback.Milliseconds())
	sampleStart := evalStart.Add(-lookback)
	sampleEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)
	sampleWhere := []string{
		teamFilter(cfg),
		"metric_name = " + sqlString(metric),
		"timestamp >= " + chTimeMillis(sampleStart.UnixMilli()),
		"timestamp <= " + chTimeMillis(sampleEnd.UnixMilli()),
	}
	sampleWhere = append(sampleWhere, idFilters...)

	groupName := grouping[0]
	groupColumn := quoteIdent(groupAlias(0))
	groupLabels := fmt.Sprintf(
		"SELECT id, label_value AS %s FROM %s WHERE %s AND metric_name = %s AND label_name = %s",
		groupColumn,
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		teamFilter(cfg),
		sqlString(metric),
		sqlString(groupName),
	)

	return fmt.Sprintf(
		"WITH active_ids AS (SELECT step_idx, id FROM (SELECT id, toUnixTimestamp64Milli(timestamp) AS ts FROM %s WHERE %s) ARRAY JOIN range(toUInt64(%d)) AS step_idx WHERE ts >= %s AND ts <= %s GROUP BY step_idx, id), active_groups AS (SELECT step_idx, ifNull(%s, '') AS %s FROM active_ids ANY LEFT JOIN (%s) AS group_labels USING id GROUP BY step_idx, %s) SELECT toInt64(%d) + toInt64(step_idx) * %d AS ts, toFloat64(count()) AS value FROM active_groups GROUP BY step_idx ORDER BY step_idx",
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
		steps,
		lookbackStartExpr,
		evalMillisExpr,
		groupColumn,
		groupColumn,
		groupLabels,
		groupColumn,
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func (s *Server) queryNestedCountRangeValues(ctx context.Context, sql string) (queryData, error) {
	values := make([][]any, 0, 128)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var ts int64
		var value float64
		if err := row.Scan(&ts, &value); err != nil {
			return err
		}
		values = append(values, []any{float64(ts) / 1000, formatSample(value)})
		return nil
	})
	if err != nil {
		return queryData{}, err
	}
	result := []sampleResult{{Metric: map[string]string{}, Values: values}}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: result}, nil
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
	if selector, ok := expr.(*parser.VectorSelector); ok {
		source, mint, maxt, ok := selectorRangeGridSource(s.cfg, selector, start, end, step)
		if !ok {
			return aggregateRangeSource{}, nil, 0, 0, false
		}
		return aggregateRangeSource{gridExpr: lastGridExpr(source.start, source.end, step, s.cfg.LookbackDelta)}, selector, mint, maxt, true
	}

	call, ok := expr.(*parser.Call)
	if !ok || len(call.Args) != 1 {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	fn, ok := rangeGridFunction(call.Func.Name)
	if !ok {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	matrix, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok || matrix.Range <= 0 {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok || selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	mint := shiftedStart.Add(-matrix.Range).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	rangeSeconds := formatDurationSeconds(matrix.Range)
	gridExpr := fmt.Sprintf(
		"%s(%s, %s, %s, %s)(timestamp, value)",
		fn.name,
		chTimeMillis(shiftedStart.UnixMilli()),
		chTimeMillis(shiftedEnd.UnixMilli()),
		formatDurationSeconds(step),
		rangeSeconds,
	)
	if fn.increase {
		gridExpr = fmt.Sprintf("arrayMap(x -> if(isNull(x), NULL, x * %s), %s)", rangeSeconds, gridExpr)
	}
	return aggregateRangeSource{gridExpr: gridExpr}, selector, mint, maxt, true
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

func exactBucketRangeSelector(cfg Config, selector *parser.VectorSelector, start, end time.Time, step time.Duration) bool {
	if cfg.RemoteWriteInterval <= 0 || step != cfg.RemoteWriteInterval {
		return false
	}
	if selector.OriginalOffset != 0 || selector.Offset != 0 {
		return false
	}
	intervalMillis := cfg.RemoteWriteInterval.Milliseconds()
	if intervalMillis <= 0 {
		return false
	}
	startMillis := start.UnixMilli()
	endMillis := end.UnixMilli()
	return bucketTimestampMS(startMillis, cfg.RemoteWriteInterval) == startMillis &&
		bucketTimestampMS(endMillis, cfg.RemoteWriteInterval) == endMillis
}

func lastGridExpr(start, end time.Time, step, lookback time.Duration) string {
	return fmt.Sprintf(
		"timeSeriesLastToGrid(%s, %s, %s, %s)(timestamp, value)",
		chTimeMillis(start.UnixMilli()),
		chTimeMillis(end.UnixMilli()),
		formatDurationSeconds(step),
		formatDurationSeconds(lookback),
	)
}

func (s *Server) tryFastRangeFunctionAggregate(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	call, ok := expr.Expr.(*parser.Call)
	if !ok || len(call.Args) != 1 {
		return queryData{}, false, nil
	}
	fn, ok := rangeFunctionMode(call.Func.Name)
	if !ok {
		return queryData{}, false, nil
	}
	matrix, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return queryData{}, false, nil
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok || selector.Anchored || selector.Smoothed {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, matrix.Range)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}

	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	perSeries := rangeFunctionPerSeriesSQL(s.cfg, window, selector.LabelMatchers, fn)
	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, selector.LabelMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		"per_series AS (" + perSeries + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}
	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	source := aggregateSourceSQL("per_series", expr.Grouping, groupJoin)
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
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	if data, ok, err := s.tryLabelIndexTopK(ctx, selector, window, evalTime, limit, expr.Op); ok || err != nil {
		return data, ok, err
	}

	return queryData{}, false, nil
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

type rangeFunction struct {
	counter bool
	rate    bool
	instant bool
}

func rangeFunctionMode(name string) (rangeFunction, bool) {
	switch name {
	case "rate":
		return rangeFunction{counter: true, rate: true}, true
	case "irate":
		return rangeFunction{counter: true, rate: true, instant: true}, true
	case "increase":
		return rangeFunction{counter: true, rate: false}, true
	case "delta":
		return rangeFunction{counter: false, rate: false}, true
	case "idelta":
		return rangeFunction{counter: false, rate: false, instant: true}, true
	default:
		return rangeFunction{}, false
	}
}

func rangeFunctionSampleSourceSQL(cfg Config, window selectorWindow, matchers []*labels.Matcher) string {
	source := samplesForSelectedSeriesSQL(cfg, matchers, window.mint, window.maxt)
	return fmt.Sprintf(
		"SELECT id, arrayMap(x -> x.1, pts) AS ts, arrayMap(x -> x.2, pts) AS vals FROM (SELECT id, arraySort(x -> x.1, groupArray((toUnixTimestamp64Milli(timestamp), value))) AS pts FROM (%s) GROUP BY id HAVING length(pts) > 1)",
		source,
	)
}

func rangeFunctionPerSeriesSQL(cfg Config, window selectorWindow, matchers []*labels.Matcher, fn rangeFunction) string {
	source := rangeFunctionSampleSourceSQL(cfg, window, matchers)
	if fn.instant {
		result := "if(last_v < prev_v, last_v, last_v - prev_v)"
		if !fn.counter {
			result = "last_v - prev_v"
		}
		if fn.rate {
			result = "(" + result + ") / ((last_t - prev_t) / 1000.0)"
		}
		return fmt.Sprintf(
			"SELECT id, %s AS value FROM (SELECT id, vals[-2] AS prev_v, vals[-1] AS last_v, ts[-2] AS prev_t, ts[-1] AS last_t FROM (%s) WHERE length(vals) >= 2 AND last_t > prev_t)",
			result,
			source,
		)
	}
	rangeSeconds := float64(window.maxt-window.mint) / 1000.0
	resetCorrection := "0.0"
	durationToStart := "duration_to_start1"
	if fn.counter {
		resetCorrection = "arraySum(arrayMap((prev, curr) -> if(curr < prev, prev, 0.0), arrayPopBack(vals), arrayPopFront(vals)))"
		durationToStart = "duration_to_start2"
	}
	result := "raw_delta + reset_correction"
	factor := "(sampled_interval + " + durationToStart + " + duration_to_end2) / sampled_interval"
	if fn.rate {
		factor += " / " + strconv.FormatFloat(rangeSeconds, 'f', -1, 64)
	}
	return fmt.Sprintf(
		"SELECT id, ((%s) * (%s)) AS value FROM (SELECT id, vals[1] AS first_v, vals[-1] AS last_v, ts[1] AS first_t, ts[-1] AS last_t, length(vals)-1 AS num_minus_one, (last_v - first_v) AS raw_delta, %s AS reset_correction, (last_t - first_t) / 1000.0 AS sampled_interval, (first_t - %d) / 1000.0 AS duration_to_start0, (%d - last_t) / 1000.0 AS duration_to_end0, sampled_interval / num_minus_one AS avg_between, avg_between * 1.1 AS threshold, if(duration_to_start0 >= threshold, avg_between / 2, duration_to_start0) AS duration_to_start1, if(raw_delta + reset_correction > 0 AND first_v >= 0, sampled_interval * (first_v / (raw_delta + reset_correction)), duration_to_start1) AS duration_to_zero, if(duration_to_zero < duration_to_start1, duration_to_zero, duration_to_start1) AS duration_to_start2, if(duration_to_end0 >= threshold, avg_between / 2, duration_to_end0) AS duration_to_end2 FROM (%s))",
		result,
		factor,
		resetCorrection,
		window.mint,
		window.maxt,
		source,
	)
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
	lit, ok := expr.(*parser.NumberLiteral)
	if !ok || math.IsNaN(lit.Val) || math.IsInf(lit.Val, 0) {
		return 0, false
	}
	return lit.Val, true
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

func orVectorSelectors(expr parser.Expr) ([]*parser.VectorSelector, bool) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return orVectorSelectors(e.Expr)
	case *parser.VectorSelector:
		return []*parser.VectorSelector{e}, true
	case *parser.BinaryExpr:
		if e.Op != parser.LOR || e.VectorMatching == nil || e.VectorMatching.Card != parser.CardManyToMany {
			return nil, false
		}
		left, ok := orVectorSelectors(e.LHS)
		if !ok {
			return nil, false
		}
		right, ok := orVectorSelectors(e.RHS)
		if !ok {
			return nil, false
		}
		return append(left, right...), true
	default:
		return nil, false
	}
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
	where := []string{
		teamFilter(cfg),
	}
	if selectedSeriesNeedsBounds(selectParts) {
		where = append(where, seriesTimeFilters(cfg, matchers, mint, maxt)...)
	}
	where = append(where, seriesPreFilters(cfg, matchers)...)
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT id, metric_name, labels_json, min_time, max_time FROM %s WHERE %s) GROUP BY id",
		strings.Join(selectedSeriesProjection(selectParts), ", "),
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	), true
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

func latestSamplesForSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64) string {
	if cfg.RemoteWriteInterval > 0 {
		bucket := bucketTimestampMS(maxt, cfg.RemoteWriteInterval)
		if bucket >= mint {
			where := []string{
				teamFilter(cfg),
				"timestamp = " + chTimeMillis(bucket),
				"id IN (SELECT id FROM selected_series)",
			}
			where = append(where, metricNameConstraints(matchers)...)
			return fmt.Sprintf(
				"SELECT id, any(value) AS value, max(timestamp) AS ts_col FROM %s WHERE %s GROUP BY id",
				tableName(cfg.CHDatabase, cfg.SamplesTable),
				strings.Join(where, " AND "),
			)
		}
	}
	source := samplesForSelectedSeriesSQL(cfg, matchers, mint, maxt)
	return fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value, max(timestamp) AS ts_col FROM (%s) GROUP BY id",
		source,
	)
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
	if filterToSelected && (metric == "" || !metricOnlyMatchers(matchers)) {
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
