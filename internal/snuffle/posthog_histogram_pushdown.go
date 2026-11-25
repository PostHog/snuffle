package snuffle

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

// Histogram pushdown evaluates counter rollups over delta histogram series in
// ClickHouse, so a week of a histogram no longer sends every raw row to Go.
//
// For a delta series, rate, increase, increase_pure and delta read the sum of
// the finite points in the window (t-w, t] (see metricsQLRollup and
// deltaWindowSum). ClickHouse sums the points of each series and bucket into
// cells whose edges are the evaluation times and the window starts, so every
// window is a whole number of cells. Go adds up the cells of each window.
//
// The planner replaces each rollup it can push down with a selector of a
// synthetic metric. An in-memory querier serves that metric: one point per
// evaluation time, or a stale marker where the window holds no finite point.
// The engine then evaluates the rest of the expression as before: binary
// operators, aggregations, histogram_quantile. The synthetic metric name is
// removed from the result.
//
// The planner leaves a rollup to the engine when it cannot match the engine:
// cumulative or exponential histograms, invalid bucket layouts, real series
// with the same name, offsets, subqueries, most label functions and anything
// that reads __name__.

const histogramPushdownPrefix = "__snuffle_pushdown_"

var histogramPushdownFunctions = map[string]bool{"rate": true, "increase": true, "increase_pure": true, "delta": true}

// histogramPushdownBlockedFunctions read or write labels, including the metric
// name, or read sample timestamps. The synthetic selector differs from the
// rollup in both, so queries with these functions keep the engine path.
var histogramPushdownBlockedFunctions = []string{"label", "absent", "timestamp", "union", "keep_metric_names", "info"}

// histogramPushdownLabelFunctions are not blocked: metricsQLLabelBuilder
// drops the synthetic name as the rollup drops its name.
var histogramPushdownLabelFunctions = map[string]bool{
	metricsQLInternalPrefix + "label_set":          true,
	metricsQLInternalPrefix + "label_copy":         true,
	metricsQLInternalPrefix + "label_move":         true,
	metricsQLInternalPrefix + "label_transform":    true,
	metricsQLInternalPrefix + "drop_common_labels": true,
	metricsQLUnionName:                             true,
}

// errHistogramPushdownUnsupported stops a pushdown query when a row has data
// the pushdown does not reproduce. The engine path then reads the selector.
var errHistogramPushdownUnsupported = errors.New("histogram pushdown unsupported")

type histogramPushdownCall struct {
	call     *parser.Call
	name     string
	selector *parser.VectorSelector
	alias    postHogHistogramAlias
	window   int64
}

// parseHistogramPushdownCall accepts the rewritten counter rollup
// `__snuffle_<fn>(selector[matrix], window, step, lookback, instant)` with an
// explicit window, an exact histogram component name and no offset.
func parseHistogramPushdownCall(call *parser.Call) (histogramPushdownCall, bool) {
	if call.Func == nil || len(call.Args) != 5 || !strings.HasPrefix(call.Func.Name, metricsQLInternalPrefix) {
		return histogramPushdownCall{}, false
	}
	name := strings.TrimPrefix(call.Func.Name, metricsQLInternalPrefix)
	if !histogramPushdownFunctions[name] {
		return histogramPushdownCall{}, false
	}
	window, ok := call.Args[1].(*parser.NumberLiteral)
	if !ok || window.Val <= 0 || window.Val != math.Trunc(window.Val) || window.Val >= float64(math.MaxInt64) {
		return histogramPushdownCall{}, false
	}
	matrix, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return histogramPushdownCall{}, false
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok || selector.Offset != 0 || selector.OriginalOffset != 0 || selector.Timestamp != nil || selector.StartOrEnd != 0 || selector.Anchored || selector.Smoothed {
		return histogramPushdownCall{}, false
	}
	alias, ok := postHogExactHistogramAlias(selector.LabelMatchers)
	if !ok {
		return histogramPushdownCall{}, false
	}
	return histogramPushdownCall{call: call, name: name, selector: selector, alias: alias, window: int64(window.Val)}, true
}

// histogramPushdownCandidates returns the rollups the planner may push down.
// It returns none when the query reads labels in a way the synthetic selector
// would change.
func histogramPushdownCandidates(expr parser.Expr) []histogramPushdownCall {
	blocked := false
	var candidates []histogramPushdownCall
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.AggregateExpr:
			if containsMetricName(n.Grouping) {
				blocked = true
			}
		case *parser.BinaryExpr:
			if n.VectorMatching != nil && (containsMetricName(n.VectorMatching.MatchingLabels) || containsMetricName(n.VectorMatching.Include)) {
				blocked = true
			}
		case *parser.Call:
			if n.Func == nil || histogramPushdownLabelFunctions[n.Func.Name] {
				return nil
			}
			for _, fragment := range histogramPushdownBlockedFunctions {
				if strings.Contains(n.Func.Name, fragment) {
					blocked = true
				}
			}
			candidate, ok := parseHistogramPushdownCall(n)
			if !ok {
				return nil
			}
			for _, ancestor := range path {
				if _, ok := ancestor.(*parser.SubqueryExpr); ok {
					return nil
				}
			}
			candidates = append(candidates, candidate)
		}
		return nil
	})
	if blocked {
		return nil
	}
	return candidates
}

func containsMetricName(names []string) bool {
	for _, name := range names {
		if name == labels.MetricName {
			return true
		}
	}
	return false
}

// planHistogramPushdown rewrites the prepared query for the evaluation times.
// It reports false when no rollup could be pushed down.
func (s *Server) planHistogramPushdown(ctx context.Context, query string, times []int64) (string, storage.Queryable, bool, error) {
	if !s.cfg.PostHogHistogramPushdown || !s.cfg.postHogSchemaLayout() || len(times) == 0 || len(times) > maxRangePushdownPoints {
		return "", nil, false, nil
	}
	if !strings.Contains(query, metricsQLInternalPrefix) {
		return "", nil, false, nil
	}
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		return "", nil, false, nil
	}
	candidates := histogramPushdownCandidates(expr)
	if len(candidates) == 0 {
		return "", nil, false, nil
	}
	replacements := make(map[*parser.Call]parser.Expr, len(candidates))
	series := make(map[string][]*seriesMeta, len(candidates))
	// budget is the number of points the candidates may materialize between
	// them; every series holds one point per evaluation time.
	budget := s.cfg.MaxSamples
	for index, candidate := range candidates {
		name := histogramPushdownPrefix + strconv.Itoa(index) + candidate.alias.suffix
		metas, ok, err := s.histogramPushdownSeries(ctx, candidate, times, name, &budget)
		if err != nil {
			return "", nil, false, err
		}
		if !ok {
			continue
		}
		series[name] = metas
		replacements[candidate.call] = &parser.VectorSelector{
			Name:          name,
			LabelMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)},
		}
	}
	if len(replacements) == 0 {
		return "", nil, false, nil
	}
	expr = replaceHistogramPushdownCalls(expr, replacements)
	return expr.String(), &histogramPushdownQueryable{inner: s.queryable, series: series}, true, nil
}

// replaceHistogramPushdownCalls swaps each replaced call for its selector.
func replaceHistogramPushdownCalls(expr parser.Expr, replacements map[*parser.Call]parser.Expr) parser.Expr {
	switch n := expr.(type) {
	case *parser.Call:
		if replacement, ok := replacements[n]; ok {
			return replacement
		}
		for index, arg := range n.Args {
			n.Args[index] = replaceHistogramPushdownCalls(arg, replacements)
		}
	case *parser.AggregateExpr:
		n.Expr = replaceHistogramPushdownCalls(n.Expr, replacements)
		if n.Param != nil {
			n.Param = replaceHistogramPushdownCalls(n.Param, replacements)
		}
	case *parser.BinaryExpr:
		n.LHS = replaceHistogramPushdownCalls(n.LHS, replacements)
		n.RHS = replaceHistogramPushdownCalls(n.RHS, replacements)
	case *parser.ParenExpr:
		n.Expr = replaceHistogramPushdownCalls(n.Expr, replacements)
	case *parser.UnaryExpr:
		n.Expr = replaceHistogramPushdownCalls(n.Expr, replacements)
	case *parser.SubqueryExpr:
		n.Expr = replaceHistogramPushdownCalls(n.Expr, replacements)
	case *parser.StepInvariantExpr:
		n.Expr = replaceHistogramPushdownCalls(n.Expr, replacements)
	}
	return expr
}

type histogramPushdownCell struct {
	end    int64
	total  float64
	points uint64
}

type histogramPushdownSeries struct {
	labelMap map[string]string
	cells    map[int64]*histogramPushdownCell
}

// histogramPushdownSeries evaluates one rollup at every evaluation time. It
// reports false when the selected data needs the engine.
func (s *Server) histogramPushdownSeries(ctx context.Context, candidate histogramPushdownCall, times []int64, name string, budget *int) ([]*seriesMeta, bool, error) {
	first, last := times[0], times[len(times)-1]
	// One evaluation time needs only the cell (t-w, t]. A grid step of w makes
	// both grids the same, so every point in the window lands in that cell.
	step := candidate.window
	if len(times) > 1 {
		step = times[1] - times[0]
	}
	mint := first - candidate.window
	matchers := candidate.selector.LabelMatchers
	// Real classic histograms, such as remote-written _bucket series, are the
	// common case. A primary key lookup rules them out before the metadata
	// query.
	if found, err := s.postHogSelectsHistogram(ctx, mint, last, matchers); err != nil || !found {
		return nil, false, err
	}
	querier := &CHQuerier{queryable: s.queryable, mint: mint, maxt: last}
	real, sources, ids, err := querier.selectPostHogExactHistogramMetadata(ctx, mint, last, candidate.alias, matchers)
	if err != nil {
		return nil, false, err
	}
	if len(real) > 0 {
		return nil, false, nil
	}
	if len(ids) == 0 {
		return nil, true, nil
	}

	bySeries := make(map[string]*histogramPushdownSeries)
	rows := 0
	sqlFor := func(_ []uint64, idCondition string) string {
		return histogramPushdownSQL(s.cfg, candidate.alias, matchers, idCondition, first, last, step, candidate.window)
	}
	err = queryPostHogSeriesIDs(ctx, s.client, s.cfg.IDChunkSize, ids, sqlFor, func(row clickHouseRow) error {
		var id uint64
		var bound float64
		var cell int64
		var total float64
		var points uint64
		var bad uint8
		if err := row.Scan(&id, &bound, &cell, &total, &points, &bad); err != nil {
			return err
		}
		if bad != 0 {
			return errHistogramPushdownUnsupported
		}
		rows++
		if s.cfg.MaxSamples > 0 && rows > s.cfg.MaxSamples {
			return fmt.Errorf("histogram sample limit exceeded (%d); shorten the time range or tighten matchers", s.cfg.MaxSamples)
		}
		source, ok := sources[id]
		if !ok || points == 0 {
			return nil
		}
		labelMap := maps.Clone(postHogLabelMap(source.metricName, source.serviceName, source.resource, source.attributes))
		labelMap[labels.MetricName] = source.metricName + candidate.alias.suffix
		if candidate.alias.suffix == "_bucket" {
			labelMap["le"] = formatHistogramPushdownBound(bound)
		}
		if !matchesAll(labelMap, matchers) {
			return nil
		}
		key := labels.FromMap(labelMap).String()
		series := bySeries[key]
		if series == nil {
			if len(bySeries) >= s.cfg.MaxSeries {
				return fmt.Errorf("histogram series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
			}
			series = &histogramPushdownSeries{labelMap: labelMap, cells: make(map[int64]*histogramPushdownCell)}
			bySeries[key] = series
		}
		// Several sources can share one label set. The engine sums their
		// points, so their cells add up.
		existing := series.cells[cell]
		if existing == nil {
			existing = &histogramPushdownCell{end: cell}
			series.cells[cell] = existing
		}
		existing.total += total
		existing.points += points
		return nil
	})
	if errors.Is(err, errHistogramPushdownUnsupported) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	keys := make([]string, 0, len(bySeries))
	for key := range bySeries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// Every series expands to len(times) points, stale markers included, so
	// charge the expansion before allocating it.
	if s.cfg.MaxSamples > 0 {
		if len(keys) > *budget/len(times) {
			return nil, false, fmt.Errorf("histogram sample limit exceeded (%d); shorten the time range or tighten matchers", s.cfg.MaxSamples)
		}
		*budget -= len(keys) * len(times)
	}
	out := make([]*seriesMeta, 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		series := bySeries[key]
		samples, present := histogramPushdownWindows(series.cells, times, candidate.window, candidate.name == "rate")
		if !present {
			continue
		}
		labelMap := series.labelMap
		labelMap[labels.MetricName] = name
		lset := labels.FromMap(labelMap)
		out = append(out, &seriesMeta{id: lset.Hash(), metricName: name, labelMap: labelMap, labels: lset, samples: samples})
	}
	return out, true, nil
}

// histogramPushdownWindows returns one point per evaluation time: the sum of
// the cells in (t-window, t], or a stale marker when they hold no finite
// point. rate divides the sum by the window in seconds.
func histogramPushdownWindows(cells map[int64]*histogramPushdownCell, times []int64, window int64, rate bool) ([]samplePoint, bool) {
	sorted := make([]*histogramPushdownCell, 0, len(cells))
	for _, cell := range cells {
		sorted = append(sorted, cell)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].end < sorted[j].end })
	totals := make([]float64, len(sorted)+1)
	points := make([]uint64, len(sorted)+1)
	for index, cell := range sorted {
		totals[index+1] = totals[index] + cell.total
		points[index+1] = points[index] + cell.points
	}
	// upTo returns how many cells end at or before t.
	upTo := func(t int64) int {
		return sort.Search(len(sorted), func(i int) bool { return sorted[i].end > t })
	}
	samples := make([]samplePoint, 0, len(times))
	present := false
	for _, t := range times {
		hi, lo := upTo(t), upTo(t-window)
		if points[hi] == points[lo] {
			samples = append(samples, samplePoint{t: t, v: math.Float64frombits(promvalue.StaleNaN)})
			continue
		}
		value := totals[hi] - totals[lo]
		if rate {
			value /= float64(window) / 1000
		}
		samples = append(samples, samplePoint{t: t, v: value})
		present = true
	}
	return samples, present
}

// formatHistogramPushdownBound spells a bucket bound as
// postHogHistogramBuckets does.
func formatHistogramPushdownBound(bound float64) string {
	if math.IsInf(bound, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(bound, 'g', -1, 64)
}

// histogramPushdownSQL sums the points of each series, bucket bound and cell.
// A cell ends at the next evaluation time or window start at or after the
// point, whichever is first, so each window (t-w, t] is a run of whole cells.
// bad marks a row the engine would read differently: another temporality or
// metric type, or a bucket layout that postHogHistogramBuckets rejects or
// normalizes.
func histogramPushdownSQL(cfg Config, alias postHogHistogramAlias, matchers []*labels.Matcher, idCondition string, first, last, step, window int64) string {
	mint := first - window
	where := postHogSampleFilters(cfg, postHogHistogramSampleMatchers([]postHogHistogramAlias{alias}, matchers), mint, last)
	where = append(where, postHogHistogramTypeFilter, idCondition)
	rowsSQL := fmt.Sprintf(
		"SELECT series_fingerprint AS series_id, toUnixTimestamp64Milli(timestamp) AS ts, value AS sum_value, count AS observations, histogram_counts, histogram_bounds, aggregation_temporality, metric_type FROM %s",
		postHogSampleRowsFrom(cfg, where, true),
	)
	bad := "aggregation_temporality != 'delta' OR metric_type != 'histogram'"
	f, l, st, w := strconv.FormatInt(first, 10), strconv.FormatInt(last, 10), strconv.FormatInt(step, 10), strconv.FormatInt(window, 10)
	cell := fmt.Sprintf(
		"least(%[1]s + toInt64(ceil((ts - %[1]s) / %[2]s)) * %[2]s, %[1]s - %[3]s + toInt64(ceil((ts - %[1]s + %[3]s) / %[2]s)) * %[2]s)",
		f, st, w,
	)
	inWindow := fmt.Sprintf("ts > %s - %s AND ts <= %s", f, w, l)
	const settings = " SETTINGS short_circuit_function_evaluation = 'disable'"
	if alias.suffix != "_bucket" {
		value := "toFloat64(observations)"
		if alias.suffix == "_sum" {
			value = "toFloat64(sum_value)"
		}
		return fmt.Sprintf(
			"SELECT series_id, toFloat64(0) AS bound, %s AS cell, sumIf(v, isFinite(v)) AS total, countIf(isFinite(v)) AS points, max(toUInt8(%s)) AS bad FROM (SELECT *, %s AS v FROM (%s)) WHERE %s GROUP BY series_id, cell ORDER BY series_id, cell%s",
			cell, bad, value, rowsSQL, inWindow, settings,
		)
	}
	bad += " OR length(histogram_counts) != length(histogram_bounds) + 1" +
		" OR arraySum(histogram_counts) != observations" +
		" OR NOT arrayAll(b -> isFinite(b), histogram_bounds)" +
		" OR (length(histogram_bounds) > 1 AND arrayMin(arrayPopFront(arrayDifference(histogram_bounds))) <= 0)"
	// Bucket counts are integers, so ClickHouse sums the count arrays of each
	// series, bound layout and cell first and expands buckets only for the
	// sums. The layout and array length are in the key, so sumForEach always
	// adds arrays of one length. A bad row still reaches the result through
	// its group.
	cells := fmt.Sprintf(
		"SELECT series_id, histogram_bounds AS bounds, %s AS cell, sumForEach(histogram_counts) AS counts, count() AS points, max(toUInt8(%s)) AS bad FROM (%s) WHERE %s GROUP BY series_id, bounds, length(histogram_counts), cell",
		cell, bad, rowsSQL, inWindow,
	)
	// LEFT keeps a group with no buckets, so its bad flag still reaches Go.
	return fmt.Sprintf(
		"SELECT series_id, if(bucket <= length(bounds), toFloat64(bounds[bucket]), inf) AS bound, cell, toFloat64(cumulative) AS total, points, bad FROM (%s) LEFT ARRAY JOIN arrayCumSum(counts) AS cumulative, arrayEnumerate(counts) AS bucket ORDER BY series_id, bound, cell%s",
		cells, settings,
	)
}

// histogramPushdownQueryable serves the synthetic metrics from memory and
// every other selector from the inner queryable.
type histogramPushdownQueryable struct {
	inner  storage.Queryable
	series map[string][]*seriesMeta
}

func (q *histogramPushdownQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	inner, err := q.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &histogramPushdownQuerier{Querier: inner, series: q.series}, nil
}

type histogramPushdownQuerier struct {
	storage.Querier
	series map[string][]*seriesMeta
}

func (q *histogramPushdownQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if series, ok := q.series[exactMetricName(matchers)]; ok {
		return seriesSetFromMeta(series, sortSeries)
	}
	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

// stripHistogramPushdownNames removes the synthetic metric names that reach
// the result. The rollup they replaced has no metric name.
func stripHistogramPushdownNames(value parser.Value) parser.Value {
	strip := func(lset labels.Labels) (labels.Labels, bool) {
		if !strings.HasPrefix(lset.Get(labels.MetricName), histogramPushdownPrefix) {
			return lset, false
		}
		return labels.NewBuilder(lset).Del(labels.MetricName).Labels(), true
	}
	switch v := value.(type) {
	case promql.Vector:
		for index := range v {
			v[index].Metric, _ = strip(v[index].Metric)
		}
	case promql.Matrix:
		changed := false
		for index := range v {
			var stripped bool
			v[index].Metric, stripped = strip(v[index].Metric)
			changed = changed || stripped
		}
		if changed {
			sort.Sort(v)
		}
	}
	return value
}
