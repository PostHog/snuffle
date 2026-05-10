package snuffle

import (
	"context"
	"encoding/json"
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

	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM %s WHERE timestamp >= %s AND timestamp <= %s AND id IN (SELECT id FROM selected_series) GROUP BY id",
		source.gridExpr,
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		chTimeMillis(mint),
		chTimeMillis(maxt),
	)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT selected_series.id AS id, selected_series.metric_name AS metric_name, selected_series.labels_json AS labels_json, per_series.vals AS vals FROM per_series ANY INNER JOIN selected_series USING id ORDER BY selected_series.id",
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
	gridExpr := fmt.Sprintf(
		"timeSeriesLastToGrid(%s, %s, %s, %s)(timestamp, value)",
		chTimeMillis(shiftedStart.UnixMilli()),
		chTimeMillis(shiftedEnd.UnixMilli()),
		formatDurationSeconds(step),
		formatDurationSeconds(s.cfg.LookbackDelta),
	)
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM %s WHERE timestamp >= %s AND timestamp <= %s AND id IN (SELECT id FROM selected_series) GROUP BY id",
		gridExpr,
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		chTimeMillis(mint),
		chTimeMillis(maxt),
	)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT selected_series.id AS id, selected_series.metric_name AS metric_name, selected_series.labels_json AS labels_json, per_series.vals AS vals FROM per_series ANY INNER JOIN selected_series USING id ORDER BY selected_series.id",
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
	err := s.client.QueryJSONEachRow(ctx, sql, func(raw json.RawMessage) error {
		var row struct {
			MetricName string            `json:"metric_name"`
			LabelsJSON json.RawMessage   `json:"labels_json"`
			Vals       []json.RawMessage `json:"vals"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		metric, ok, err := metricLabelMap(row.MetricName, row.LabelsJSON, matchers)
		if err != nil || !ok {
			return err
		}
		values := make([][]any, 0, len(row.Vals))
		for i, rawValue := range row.Vals {
			if string(rawValue) == "null" {
				continue
			}
			ts := start.UnixMilli() + int64(i)*stepMillis
			if ts > end.UnixMilli() {
				break
			}
			value, err := rawFloat(rawValue)
			if err != nil {
				return err
			}
			values = append(values, []any{float64(ts) / 1000, formatSample(value)})
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
		"latest AS (" + latestSamplesForSelectedSeriesSQL(s.cfg, window.mint, window.maxt) + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, strconv.FormatInt(evalTime.UnixMilli(), 10)+" AS ts")
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
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM %s WHERE timestamp >= %s AND timestamp <= %s AND id IN (SELECT id FROM selected_series) GROUP BY id",
		source.gridExpr,
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		chTimeMillis(mint),
		chTimeMillis(maxt),
	)

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
	selectParts = append(selectParts, fmt.Sprintf("%d + (toInt64(idx) - 1) * %d AS ts", start.UnixMilli(), stepMillis))
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

	results := make([]sampleResult, 0, 128)
	seen := make(map[string]int, 128)
	err := s.client.QueryJSONEachRow(ctx, sql, func(raw json.RawMessage) error {
		var row map[string]json.RawMessage
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		value, err := rawFloat(row["value"])
		if err != nil {
			return err
		}
		ts, err := rawInt64(row["ts"])
		if err != nil {
			return err
		}
		metric, key := groupingMetricAndKey(row, expr.Grouping)
		idx, ok := seen[key]
		if !ok {
			idx = len(results)
			seen[key] = idx
			results = append(results, sampleResult{Metric: metric})
		}
		results[idx].Values = append(results[idx].Values, []any{float64(ts) / 1000, formatSample(value)})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

type aggregateRangeSource struct {
	gridExpr string
}

func (s *Server) aggregateRangeSourceSQL(expr parser.Expr, start, end time.Time, step time.Duration) (aggregateRangeSource, *parser.VectorSelector, int64, int64, bool) {
	if selector, ok := expr.(*parser.VectorSelector); ok {
		if selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil {
			return aggregateRangeSource{}, nil, 0, 0, false
		}
		offset := selector.OriginalOffset
		if offset == 0 {
			offset = selector.Offset
		}
		shiftedStart := start.Add(-offset)
		shiftedEnd := end.Add(-offset)
		mint := shiftedStart.Add(-s.cfg.LookbackDelta).UnixMilli()
		maxt := shiftedEnd.UnixMilli()
		gridExpr := fmt.Sprintf(
			"timeSeriesLastToGrid(%s, %s, %s, %s)(timestamp, value)",
			chTimeMillis(shiftedStart.UnixMilli()),
			chTimeMillis(shiftedEnd.UnixMilli()),
			formatDurationSeconds(step),
			formatDurationSeconds(s.cfg.LookbackDelta),
		)
		return aggregateRangeSource{gridExpr: gridExpr}, selector, mint, maxt, true
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
	perSeries := rangeFunctionPerSeriesSQL(s.cfg, window, fn)
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
	selectParts = append(selectParts, strconv.FormatInt(evalTime.UnixMilli(), 10)+" AS ts")
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
	err := s.client.QueryJSONEachRow(ctx, sql, func(raw json.RawMessage) error {
		var row map[string]json.RawMessage
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		value, err := rawFloat(row["value"])
		if err != nil {
			return err
		}
		ts, err := rawInt64(row["ts"])
		if err != nil {
			return err
		}
		results = append(results, sampleResult{
			Metric: groupingMetric(row, grouping),
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	return results, err
}

func groupingMetric(row map[string]json.RawMessage, grouping []string) map[string]string {
	metric, _ := groupingMetricAndKey(row, grouping)
	return metric
}

func groupingMetricAndKey(row map[string]json.RawMessage, grouping []string) (map[string]string, string) {
	metric := make(map[string]string, len(grouping))
	keyParts := make([]string, 0, len(grouping)*2)
	for i, name := range grouping {
		value, _ := rawString(row[groupAlias(i)])
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
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, []string{"id"})
	if !ok {
		return queryData{}, false, nil
	}

	direction := "DESC"
	if op == parser.BOTTOMK {
		direction = "ASC"
	}
	seriesLookup := topKSeriesLookupSQL(s.cfg, selector.LabelMatchers)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), top_series AS (SELECT id, argMax(value, timestamp) AS value FROM %s WHERE timestamp >= %s AND timestamp <= %s AND id IN (SELECT id FROM selected_series) GROUP BY id ORDER BY value %s LIMIT %d) SELECT s.metric_name AS metric_name, s.labels_json AS labels_json, %d AS ts, top_series.value AS value FROM top_series ANY INNER JOIN %s AS s USING id ORDER BY value %s",
		selectedSeries,
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		chTimeMillis(window.mint),
		chTimeMillis(window.maxt),
		direction,
		limit,
		evalTime.UnixMilli(),
		seriesLookup,
		direction,
	)

	results := make([]sampleResult, 0, limit)
	err := s.client.QueryJSONEachRow(ctx, sql, func(raw json.RawMessage) error {
		var row struct {
			MetricName string          `json:"metric_name"`
			LabelsJSON json.RawMessage `json:"labels_json"`
			TS         int64           `json:"ts"`
			Value      float64         `json:"value"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		metric, _, err := metricLabelMap(row.MetricName, row.LabelsJSON, nil)
		if err != nil {
			return err
		}
		results = append(results, sampleResult{
			Metric: metric,
			Value:  []any{float64(row.TS) / 1000, formatSample(row.Value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func topKSeriesLookupSQL(cfg Config, matchers []*labels.Matcher) string {
	where := []string{"id IN (SELECT id FROM top_series)"}
	where = append(where, metricNameConstraints(matchers)...)
	return fmt.Sprintf(
		"(SELECT id, any(metric_name) AS metric_name, any(labels_json) AS labels_json FROM (SELECT id, metric_name, labels_json FROM %s WHERE %s) GROUP BY id)",
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	)
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

func rangeFunctionSampleSourceSQL(cfg Config, window selectorWindow) string {
	table := tableName(cfg.CHDatabase, cfg.SamplesTable)
	where := []string{
		fmt.Sprintf("timestamp >= %s", chTimeMillis(window.mint)),
		fmt.Sprintf("timestamp <= %s", chTimeMillis(window.maxt)),
		"id IN (SELECT id FROM selected_series)",
	}
	return fmt.Sprintf(
		"SELECT id, arrayMap(x -> x.1, pts) AS ts, arrayMap(x -> x.2, pts) AS vals FROM (SELECT id, arraySort(x -> x.1, groupArray((toUnixTimestamp64Milli(timestamp), value))) AS pts FROM %s WHERE %s GROUP BY id HAVING length(pts) > 1)",
		table,
		strings.Join(where, " AND "),
	)
}

func rangeFunctionPerSeriesSQL(cfg Config, window selectorWindow, fn rangeFunction) string {
	source := rangeFunctionSampleSourceSQL(cfg, window)
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
		return "count()", true
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
		fmt.Sprintf("max_time >= %s", chTimeMillis(mint)),
		fmt.Sprintf("min_time <= %s", chTimeMillis(maxt)),
	}
	where = append(where, seriesPreFilters(cfg, matchers)...)
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT id, metric_name, labels_json FROM %s WHERE %s) GROUP BY id",
		strings.Join(selectedSeriesProjection(selectParts), ", "),
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	), true
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
		default:
			projected = append(projected, part)
		}
	}
	return projected
}

func latestSamplesForSelectedSeriesSQL(cfg Config, mint, maxt int64) string {
	return fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value, max(timestamp) AS ts_col FROM %s WHERE timestamp >= %s AND timestamp <= %s AND id IN (SELECT id FROM selected_series) GROUP BY id",
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		chTimeMillis(mint),
		chTimeMillis(maxt),
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
		"label_name IN (" + strings.Join(names, ", ") + ")",
	}
	metric := exactMetricName(matchers)
	if metric != "" {
		where = append(where, "metric_name = "+sqlString(metric))
	}
	if metric == "" || !metricOnlyMatchers(matchers) {
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

func rawFloat(raw json.RawMessage) (float64, error) {
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(s, 64)
}

func rawInt64(raw json.RawMessage) (int64, error) {
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return int64(f), nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func rawString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	return string(raw), nil
}
