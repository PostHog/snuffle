package snuffle

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// Range pushdown evaluates `agg by (...) (F(selector[w]))` for PostHog in one ClickHouse query.
// At each step, ClickHouse calculates rollups and aggregates across series.
// It returns one row per group and step instead of each raw sample.
// The Prometheus engine evaluates all other query shapes.

// maxRangePushdownPoints is the Prometheus engine limit for points in one series.
// The engine returns its own error for larger ranges.
const maxRangePushdownPoints = 11000

// rangePushdownSentinel represents a missing previous sample.
// Its value is before all valid timestamps.
// The value makes recency checks false and prevents overflow.
const rangePushdownSentinel = "toInt64(-4000000000000000000)"

// rangeRollupCall contains one MetricsQL counter rollup or default rollup.
// prepareMetricsQLQuery rewrites the rollup before this type stores it.
// The type stores the function name, the selector in the matrix, and durations in milliseconds.
// The function name does not include the internal prefix.
type rangeRollupCall struct {
	name     string
	selector *parser.VectorSelector
	window   int64 // Zero selects the MetricsQL automatic window.
	step     int64
	lookback int64
	matrix   int64 // History before each step: window or step, plus lookback.
}

// counter reports whether this rollup removes counter resets.
func (c rangeRollupCall) counter() bool {
	return c.name == "increase" || c.name == "rate" || c.name == "irate"
}

// windowTotal reports whether this rollup sums the complete window.
// increase and delta start from zero if no valid previous sample exists.
// These functions accept any previous sample in the matrix.
func (c rangeRollupCall) windowTotal() bool {
	return c.name == "increase" || c.name == "delta"
}

var rangePushdownFunctions = map[string]bool{
	"increase":       true,
	"delta":          true,
	"rate":           true,
	"irate":          true,
	"idelta":         true,
	"default_rollup": true,
}

func (s *Server) tryFastRangeQuery(ctx context.Context, query string, start, end time.Time, step time.Duration) (queryData, bool, error) {
	if !s.cfg.postHogSchemaLayout() || !s.cfg.RangePushdown {
		return queryData{}, false, nil
	}
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		return queryData{}, false, nil
	}
	aggregate, ok := unparenExpr(expr).(*parser.AggregateExpr)
	if !ok || aggregate.Without || !postHogGroupingSupported(aggregate.Grouping) {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(aggregate, "rollup_value")
	if !ok {
		return queryData{}, false, nil
	}
	call, ok := parseRangeRollupCall(aggregate.Expr, step, s.cfg.LookbackDelta)
	if !ok || !postHogMatchersPushdownSafe(call.selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	points := (end.UnixMilli()-start.UnixMilli())/call.step + 1
	if points > maxRangePushdownPoints || call.expansion() > int64(s.cfg.RangePushdownMaxExpansion) {
		return queryData{}, false, nil
	}
	virtual, err := s.postHogSelectsHistogram(ctx, call.selector.LabelMatchers)
	if err != nil {
		return queryData{}, false, err
	}
	if virtual {
		return queryData{}, false, nil
	}
	mint := start.UnixMilli() - call.matrix
	plan := newPostHogQueryPlan(s.cfg, call.selector.LabelMatchers, aggregate.Grouping, mint, end.UnixMilli(), len(aggregate.Grouping) > 0)
	sumContributions := aggregate.Op == parser.SUM && call.windowTotal()
	sql := rangeRollupSQL(s.cfg, plan, call, start.UnixMilli(), points, aggSQL, sumContributions)
	results, err := s.queryRangeAggregateResults(ctx, sql, aggregate.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

// expansion returns the maximum number of steps that can use one sample.
func (c rangeRollupCall) expansion() int64 {
	span := c.window
	if c.name == "default_rollup" {
		span = c.matrix
	}
	return (span+c.step-1)/c.step + 1
}

// parseRangeRollupCall parses this rewritten form:
// `__snuffle_F(selector[matrix], window, step, lookback, 0)`.
// It requires the request step and lookback.
// It rejects selectors that have an offset or an @ modifier.
// Only default_rollup accepts a missing window.
func parseRangeRollupCall(expr parser.Expr, step, lookback time.Duration) (rangeRollupCall, bool) {
	call, ok := unparenExpr(expr).(*parser.Call)
	if !ok || call.Func == nil || len(call.Args) != 5 {
		return rangeRollupCall{}, false
	}
	// The engine registers the internal default_rollup as last_over_time.
	name := strings.TrimPrefix(call.Func.Name, metricsQLInternalPrefix)
	if call.Func.Name == metricsQLDefaultRollupName {
		name = "default_rollup"
	}
	if !rangePushdownFunctions[name] {
		return rangeRollupCall{}, false
	}
	var numbers [4]int64
	for index, arg := range call.Args[1:] {
		number, ok := arg.(*parser.NumberLiteral)
		if !ok || math.IsNaN(number.Val) || number.Val < 0 || number.Val >= float64(math.MaxInt64) || number.Val != math.Trunc(number.Val) {
			return rangeRollupCall{}, false
		}
		numbers[index] = int64(number.Val)
	}
	out := rangeRollupCall{name: name, window: numbers[0], step: numbers[1], lookback: numbers[2]}
	if numbers[3] != 0 || out.step != step.Milliseconds() || out.lookback != lookback.Milliseconds() || out.step <= 0 || out.lookback <= 0 {
		return rangeRollupCall{}, false
	}
	// The engine handles counters that have no explicit window.
	// It calculates each window from the sample interval at that step.
	if out.window == 0 && name != "default_rollup" {
		return rangeRollupCall{}, false
	}
	matrix, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok {
		return rangeRollupCall{}, false
	}
	out.matrix = matrix.Range.Milliseconds()
	explicit := out.window
	if explicit == 0 {
		explicit = out.step
	}
	if out.matrix != explicit+out.lookback {
		return rangeRollupCall{}, false
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok || selector.OriginalOffset != 0 || selector.Offset != 0 || selector.Timestamp != nil || selector.StartOrEnd != 0 || selector.Anchored || selector.Smoothed {
		return rangeRollupCall{}, false
	}
	out.selector = selector
	return out, true
}

// rangeRollupSQL builds the ClickHouse pushdown query.
// Read the query from the inner stage to the outer stage:
//
//  1. samples reads samples from [start - matrix, end].
//     It removes stale markers for counters but keeps them for default_rollup.
//  2. series sorts each series into arrays.
//     It adds the previous and next sample for each sample.
//     metricsQLSampleInterval estimates one interval across the complete range.
//     VictoriaMetrics also estimates one interval for each series.
//     The engine estimates an interval for each step.
//     Thus, irregular series can produce different results.
//  3. steps joins each sample to its applicable steps.
//     Counters use the window.
//     default_rollup stops using a sample when the next sample arrives.
//  4. contributions calculates the MetricsQL counter contribution for each sample.
//     The first sample uses a valid previous sample or starts the counter.
//     Later samples use their predecessor.
//  5. rollups calculates one value for each series and step.
//  6. The final stage aggregates series by group and step.
//     A sum of increase or delta adds contributions directly and skips stage 5.
func rangeRollupSQL(cfg Config, plan *postHogQueryPlan, call rangeRollupCall, startMillis, points int64, aggSQL string, sumContributions bool) string {
	start := strconv.FormatInt(startMillis, 10)
	step := strconv.FormatInt(call.step, 10)
	lookback := strconv.FormatInt(call.lookback, 10)
	matrix := strconv.FormatInt(call.matrix, 10)
	defaultRollup := call.name == "default_rollup"

	where := plan.sampleWhere()
	if !defaultRollup {
		where = append(where, nonStaleSampleSQL("value"))
	}
	samples := fmt.Sprintf(
		"SELECT series_fingerprint AS series_id, toUnixTimestamp64Milli(timestamp) AS ts, value AS v FROM %s WHERE %s",
		postHogSamplesTable(cfg),
		strings.Join(where, " AND "),
	)

	windowExpr := strconv.FormatInt(call.window, 10)
	if call.window == 0 {
		windowExpr = "least(greatest(" + step + ", max_prev), " + lookback + ")"
	}
	series := fmt.Sprintf(
		"SELECT series_id, arraySort(x -> x.1, groupArray((ts, v))) AS pts, arrayMap(p -> p.1, pts) AS tss, arrayMap(p -> p.2, pts) AS vs, "+
			"arrayPushFront(arrayPopBack(arrayMap(x -> toNullable(x), tss)), NULL) AS prev_tss, arrayPushFront(arrayPopBack(arrayMap(x -> toNullable(x), vs)), NULL) AS prev_vs, "+
			"arrayPushBack(arrayPopFront(arrayMap(x -> toNullable(x), tss)), NULL) AS next_tss, arrayPushBack(arrayPopFront(arrayMap(x -> toNullable(x), vs)), NULL) AS next_vs, "+
			"toInt64(floor(arrayReduce('quantileExactInclusiveOrDefault(0.6)', arrayPopFront(arrayDifference(tss))))) AS series_gap, "+
			"least(if(series_gap <= 0, toInt64(%s), %s), %s) AS max_prev, toInt64(%s) AS window_ms "+
			"FROM (%s) GROUP BY series_id",
		step, metricsQLSampleIntervalMarginSQL("series_gap"), lookback, windowExpr, samples,
	)
	neighbours := fmt.Sprintf(
		"SELECT series_id, max_prev, window_ms, ts, v, prev_ts, prev_v, next_ts, next_v FROM (%s) "+
			"ARRAY JOIN tss AS ts, vs AS v, prev_tss AS prev_ts, prev_vs AS prev_v, next_tss AS next_ts, next_vs AS next_v",
		series,
	)

	// A sample applies when ts <= t_i and t_i < ts + span.
	span := "window_ms"
	if defaultRollup {
		span = "least(" + matrix + ", ifNull(next_ts - ts, " + matrix + "))"
	}
	steps := fmt.Sprintf(
		"SELECT series_id, max_prev, window_ms, ts, v, prev_ts, prev_v, next_ts, next_v, idx, %[1]s + (toInt64(idx) - 1) * %[2]s AS t_i "+
			"FROM (SELECT *, toInt64(ceil((ts - %[1]s) / %[2]s)) + 1 AS first_step, toUInt64(greatest(first_step, 1)) AS step_lo, "+
			"toUInt64(greatest(least(toInt64(ceil((ts - %[1]s + %[3]s) / %[2]s)) + 1, %[4]d), greatest(first_step, 1))) AS step_hi FROM (%[5]s)) "+
			"ARRAY JOIN range(step_lo, step_hi) AS idx",
		start, step, span, points+1, neighbours,
	)

	diff := "v - p_v"
	if call.counter() {
		// MetricsQL treats a drop below one eighth of the previous value as a partial reset.
		diff = "if(v >= p_v, v - p_v, if((p_v - v) * 8 < p_v, 0, v))"
	}
	// Without a valid previous sample, increase and delta start at zero when the value appears to start a new counter.
	// rate starts at the first sample.
	first := "0"
	if call.windowTotal() {
		first = "if(abs(v) < 10 * (abs(if(ifNull(next_ts, t_i + 1) <= t_i, ifNull(next_v, v) - v, 0)) + 1), v, 0)"
	}
	// increase and delta use a previous sample that is inside the matrix.
	// The sample must also be inside the lookback from the window start.
	// VictoriaMetrics uses the same rule.
	// rate, irate, and idelta also require the sample to be in the estimated interval.
	hasPrev := "p_ts > t_i - " + matrix
	if !call.windowTotal() {
		hasPrev += " AND p_ts > t_i - window_ms - max_prev"
	}
	contributions := fmt.Sprintf(
		"SELECT series_id, idx, ts, v, t_i, window_ms, ifNull(prev_ts, %s) AS p_ts, ifNull(prev_v, 0) AS p_v, "+
			"p_ts <= t_i - window_ms AS is_first, toUInt8(%s) AS has_prev, toUInt8(is_first AND has_prev) AS first_has_prev, "+
			"%s AS c_diff, if(isNull(prev_ts), 0, ts - p_ts) AS gap, "+
			"if(is_first, if(has_prev, c_diff, %s), c_diff) AS contribution, if(is_first, if(has_prev, gap, 0), gap) AS gap_contribution FROM (%s)",
		rangePushdownSentinel, hasPrev, diff, first, steps,
	)

	groupBy := plan.groupAliases()
	selectParts := append([]string{}, groupBy...)
	selectParts = append(selectParts, "toInt64("+start+") + (toInt64(idx) - 1) * "+step+" AS ts")
	groupByParts := append(append([]string{}, groupBy...), "idx")
	var sql string
	if sumContributions {
		selectParts = append(selectParts, "sum(contribution) AS value")
		sql = fmt.Sprintf(
			"SELECT %s FROM %s GROUP BY %s ORDER BY %s",
			strings.Join(selectParts, ", "),
			plan.joinSeries(contributions),
			strings.Join(groupByParts, ", "),
			strings.Join(groupByParts, ", "),
		)
	} else {
		rollups := fmt.Sprintf(
			"SELECT series_id, idx, %s AS rollup_value FROM (%s) GROUP BY series_id, idx HAVING isNotNull(rollup_value) AND NOT isNaN(rollup_value)",
			rangeRollupValueSQL(call.name), contributions,
		)
		selectParts = append(selectParts, aggSQL+" AS value")
		sql = fmt.Sprintf(
			"SELECT %s FROM %s GROUP BY %s ORDER BY %s",
			strings.Join(selectParts, ", "),
			plan.joinSeries(rollups),
			strings.Join(groupByParts, ", "),
			strings.Join(groupByParts, ", "),
		)
	}
	return withRangePushdownSettings(plan.withSelectedSeries(sql), cfg.RangeQueryThreads)
}

// withRangePushdownSettings adds the ClickHouse query settings.
// ClickHouse can evaluate the expression tree of each lazy `if` branch more than once.
// This behavior made these queries several times slower.
func withRangePushdownSettings(sql string, maxThreads int) string {
	settings := "short_circuit_function_evaluation = 'disable'"
	if maxThreads > 0 {
		settings += ", max_threads = " + strconv.Itoa(maxThreads)
	}
	return sql + " SETTINGS " + settings
}

// rangeRollupValueSQL returns one function value for each series and step.
// It calculates the value from that step's contribution rows.
// NULL represents no point, as it does in metricsQLValue.
func rangeRollupValueSQL(name string) string {
	switch name {
	case "increase", "delta":
		return "sum(contribution)"
	case "rate":
		return "if(count() + max(first_has_prev) < 2 OR sum(gap_contribution) <= 0, NULL, sum(contribution) / (sum(gap_contribution) / 1000))"
	case "irate":
		return "if((count() >= 2 OR max(first_has_prev) = 1) AND argMax(gap, ts) > 0, argMax(c_diff, ts) / (argMax(gap, ts) / 1000), NULL)"
	case "idelta":
		return "if(count() >= 2 OR max(first_has_prev) = 1, argMax(c_diff, ts), argMax(v, ts))"
	default:
		// This branch returns the last sample unless it is stale or too old.
		return "any(if(t_i - ts < window_ms AND " + nonStaleSampleSQL("v") + ", v, NULL))"
	}
}

// metricsQLSampleIntervalMarginSQL implements the metricsQLSampleInterval margin table.
// It accepts an Int64 interval in milliseconds.
func metricsQLSampleIntervalMarginSQL(interval string) string {
	i := interval
	return fmt.Sprintf(
		"multiIf(%[1]s <= 2000, %[1]s * 5, %[1]s <= 4000, %[1]s * 3, %[1]s <= 8000, %[1]s * 2, %[1]s <= 16000, %[1]s + intDiv(%[1]s, 2), %[1]s <= 32000, %[1]s + intDiv(%[1]s, 4), %[1]s + intDiv(%[1]s, 8))",
		i,
	)
}

// queryRangeAggregateResults converts ordered rows into one matrix series for each group.
func (s *Server) queryRangeAggregateResults(ctx context.Context, sql string, grouping []string) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 64)
	lastKey := ""
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
		if len(results) == 0 || key != lastKey {
			results = append(results, sampleResult{Metric: metric, Values: make([][]any, 0, 128)})
			lastKey = key
		}
		last := &results[len(results)-1]
		last.Values = append(last.Values, []any{float64(ts) / 1000, formatSample(value)})
		return nil
	})
	return results, err
}
