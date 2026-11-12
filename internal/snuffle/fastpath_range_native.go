package snuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// Native range queries keep the engine's per-step windows. Expanding samples
// into their windows lets ClickHouse return evaluated points, not raw samples.
// ponytail: window expansion is bounded by CH_RANGE_PUSHDOWN_MAX_EXPANSION;
// use a sliding-window implementation if large window/step ratios matter.
type nativeRangePlan struct {
	cfg              Config
	start, end, step int64
	ctes, checks     []string
}

func (p *nativeRangePlan) add(sql string) string {
	name := fmt.Sprintf("native_range_%d", len(p.ctes))
	p.ctes = append(p.ctes, name+" AS ("+sql+")")
	return name
}

func (s *Server) tryNativeRangeQuery(ctx context.Context, prepared metricsQLQuery, start, end time.Time, step time.Duration) (queryData, bool, error) {
	if step.Milliseconds() <= 0 || end.Before(start) || (end.UnixMilli()-start.UnixMilli())/step.Milliseconds()+1 > maxRangePushdownPoints {
		return queryData{}, false, nil
	}
	expr, err := s.parser.ParseExpr(prepared.query)
	if err != nil {
		return queryData{}, false, nil
	}
	p := nativeRangePlan{cfg: s.cfg, start: start.UnixMilli(), end: end.UnixMilli(), step: step.Milliseconds()}
	subquery, offset := parseRangeOffsetSubquery(expr, step, s.cfg.LookbackDelta)
	if offset {
		p.end -= subquery.offset
		first := p.start - subquery.offset - subquery.matrix
		p.start = p.step * (first / p.step)
		if p.start <= first {
			p.start += p.step
		}
		expr = subquery.aggregate
	}
	source, ok := p.expression(expr)
	if !ok {
		return queryData{}, false, nil
	}
	sql := "SELECT toJSONString(mapFromArrays(arrayMap(x -> x.1, metric), arrayMap(x -> x.2, metric))) AS labels, ts, value, toUInt8(0) AS status FROM " + source
	for _, check := range p.checks {
		sql += " UNION ALL " + check
	}
	sql = "WITH " + strings.Join(p.ctes, ", ") + " SELECT * FROM (" + sql + ") ORDER BY labels, ts"
	sql = withRangePushdownSettings(sql, s.cfg.RangeQueryThreads)
	var groups []rangeAggregateGroup
	lastKey := ""
	histograms := false
	err = s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var key string
		var ts int64
		var value float64
		var status uint8
		if err := row.Scan(&key, &ts, &value, &status); err != nil {
			return err
		}
		switch status {
		case 1:
			histograms = true
			return nil
		case 2:
			return fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
		}
		if math.IsNaN(value) {
			return nil
		}
		if len(groups) == 0 || key != lastKey {
			var metric map[string]string
			if err := json.Unmarshal([]byte(key), &metric); err != nil {
				return err
			}
			groups = append(groups, rangeAggregateGroup{metric: metric})
			lastKey = key
		}
		points := groups[len(groups)-1].points
		if len(points) > 0 && points[len(points)-1].t == ts {
			return fmt.Errorf("vector cannot contain metrics with the same labelset")
		}
		groups[len(groups)-1].points = append(groups[len(groups)-1].points, samplePoint{t: ts, v: value})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	// Native histogram functions have separate Prometheus semantics. The same
	// SQL statement checks for matching histogram data before accepting floats.
	if histograms {
		return queryData{}, false, nil
	}
	if offset {
		groups = rangeOffsetSubqueryRollup(groups, subquery, start.UnixMilli(), end.UnixMilli(), p.step, s.cfg.LookbackDelta.Milliseconds())
	}
	if prepared.runningSum {
		for i := range groups {
			var sum float64
			for j := range groups[i].points {
				sum += groups[i].points[j].v
				groups[i].points[j].v = sum
			}
		}
	}
	return queryData{ResultType: "matrix", Result: rangeAggregateSampleResults(groups)}, true, nil
}

// Each expression returns sorted label tuples, an output timestamp and a value.
// Keeping this shape through nested aggregates also handles count(count(...)).
func (p *nativeRangePlan) expression(expr parser.Expr) (string, bool) {
	switch e := unparenExpr(expr).(type) {
	case *parser.AggregateExpr:
		aggregate, ok := aggregateSQL(e, "value")
		if !ok {
			return "", false
		}
		var source string
		if call, isCall := unparenExpr(e.Expr).(*parser.Call); isCall && e.Op == parser.SUM && (call.Func.Name == metricsQLInternalPrefix+"increase" || call.Func.Name == metricsQLInternalPrefix+"delta") {
			source, ok = p.rollup(call, true)
		} else {
			source, ok = p.expression(e.Expr)
		}
		if !ok {
			return "", false
		}
		names := make([]string, len(e.Grouping))
		for i, name := range e.Grouping {
			names[i] = sqlString(name)
		}
		predicate := "has([" + strings.Join(names, ",") + "], x.1)"
		if e.Without {
			predicate = "NOT " + predicate + " AND x.1 != '__name__'"
		}
		grouped := "SELECT arrayFilter(x -> " + predicate + ", metric) AS grouped_metric, ts, value FROM " + source
		if e.Op == parser.QUANTILE {
			q, _ := finiteNumber(e.Param)
			// Prometheus sorts NaNs first and interpolates even at an exact
			// rank. ClickHouse's quantile aggregates discard NaNs instead.
			return p.add(fmt.Sprintf("SELECT metric, ts, vals[lo]*(1-weight)+vals[hi]*weight AS value FROM (SELECT grouped_metric AS metric, ts, arraySort(x -> (NOT isNaN(x),x),groupArray(value)) AS vals, %s*(length(vals)-1) AS rank, toUInt64(floor(rank))+1 AS lo, least(lo+1,length(vals)) AS hi, rank-floor(rank) AS weight FROM (%s) GROUP BY grouped_metric, ts)", strconv.FormatFloat(q, 'g', -1, 64), grouped)), true
		}
		return p.add("SELECT grouped_metric AS metric, ts, " + aggregate + " AS value FROM (" + grouped + ") GROUP BY grouped_metric, ts"), true
	case *parser.BinaryExpr:
		if e.Op == parser.LOR {
			return p.union(e)
		}
		if !scalarArithmeticOperator(e.Op) {
			return "", false
		}
		scalar, ok := finiteNumber(e.RHS)
		child := e.LHS
		scalarLeft := false
		if !ok {
			scalar, ok = finiteNumber(e.LHS)
			child = e.RHS
			scalarLeft = true
		}
		if !ok {
			return "", false
		}
		source, ok := p.expression(child)
		if !ok {
			return "", false
		}
		value := scalarTransform{}.append(e.Op, scalar, scalarLeft).apply("value")
		return p.add("SELECT arrayFilter(x -> x.1 != '__name__', metric) AS metric, ts, " + value + " AS value FROM " + source), true
	case *parser.Call:
		return p.rollup(e, false)
	}
	return "", false
}

// Flatten unions so a long dashboard selector list does not duplicate the
// complete left-hand SQL subtree at every branch. Keep every left-hand series
// and suppress right-hand matches using PromQL's signature (without __name__).
func (p *nativeRangePlan) union(expr parser.Expr) (string, bool) {
	var branches []string
	seen := make(map[string]bool)
	var collect func(parser.Expr) bool
	collect = func(expr parser.Expr) bool {
		if e, ok := unparenExpr(expr).(*parser.BinaryExpr); ok && e.Op == parser.LOR {
			if e.VectorMatching == nil || e.VectorMatching.On || len(e.VectorMatching.MatchingLabels) != 0 {
				return false
			}
			return collect(e.LHS) && collect(e.RHS)
		}
		key := expr.String()
		if seen[key] {
			return true
		}
		if len(branches) >= maxAggregateUnionSelectors {
			return false
		}
		source, ok := p.expression(expr)
		if !ok {
			return false
		}
		seen[key] = true
		branches = append(branches, fmt.Sprintf("SELECT metric, ts, value, %d AS priority FROM %s", len(branches), source))
		return true
	}
	if !collect(expr) {
		return "", false
	}
	return p.add("SELECT metric, ts, value FROM (SELECT *, min(priority) OVER (PARTITION BY arrayFilter(x -> x.1 != '__name__', metric), ts) AS chosen FROM (" + strings.Join(branches, " UNION ALL ") + ")) WHERE priority = chosen"), true
}

func (p *nativeRangePlan) rollup(expr *parser.Call, sumContributions bool) (string, bool) {
	call, ok := parseRangeRollupCall(expr, time.Duration(p.step)*time.Millisecond, p.cfg.LookbackDelta)
	if !ok && expr.Func.Name == metricsQLInternalPrefix+"rate" && len(expr.Args) == 5 {
		if window, zero := expr.Args[1].(*parser.NumberLiteral); zero && window.Val == 0 {
			explicit := *expr
			explicit.Args = append(parser.Expressions(nil), expr.Args...)
			explicit.Args[1] = &parser.NumberLiteral{Val: float64(p.step)}
			call, ok = parseRangeRollupCall(&explicit, time.Duration(p.step)*time.Millisecond, p.cfg.LookbackDelta)
			call.window = 0
		}
	}
	if !ok {
		// Ordinary PromQL range functions retain their explicit matrix window.
		if len(expr.Args) != 1 {
			return "", false
		}
		switch expr.Func.Name {
		case "sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "count_over_time", "last_over_time", "present_over_time":
		default:
			return "", false
		}
		matrix, ok := expr.Args[0].(*parser.MatrixSelector)
		if !ok {
			return "", false
		}
		selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
		if !ok || selector.Timestamp != nil || selector.StartOrEnd != 0 || selector.Anchored || selector.Smoothed {
			return "", false
		}
		call = rangeRollupCall{name: expr.Func.Name, selector: selector, window: matrix.Range.Milliseconds(), matrix: matrix.Range.Milliseconds(), step: p.step, lookback: p.cfg.LookbackDelta.Milliseconds(), offset: selector.OriginalOffset.Milliseconds()}
	}
	if (call.matrix+p.step-1)/p.step+1 > int64(p.cfg.RangePushdownMaxExpansion) {
		return "", false
	}
	mint, maxt := p.start-call.offset-call.matrix, p.end-call.offset
	selected, ok := selectedSeriesSQL(p.cfg, call.selector.LabelMatchers, mint, maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return "", false
	}
	metric := "JSONExtractKeysAndValues(labels_json, 'String')"
	if call.name == "default_rollup" || call.name == "last_over_time" {
		metric = "arrayConcat(" + metric + ", [('__name__', toString(metric_name))])"
	}
	selected = p.add("SELECT id, arraySort(" + metric + ") AS metric FROM (" + selected + fmt.Sprintf(" LIMIT %d", p.cfg.MaxSeries) + ")")
	p.checks = append(p.checks, fmt.Sprintf("SELECT '' AS labels, toInt64(0) AS ts, toFloat64(0) AS value, toUInt8(2) AS status FROM %s HAVING count() >= %d", selected, p.cfg.MaxSeries))
	where := sampleBaseFilters(p.cfg, call.selector.LabelMatchers, mint, maxt)
	where = append(where, "id IN (SELECT id FROM "+selected+")")
	if p.cfg.HistogramsTable != "" {
		p.checks = append(p.checks, "SELECT '' AS labels, toInt64(0) AS ts, toFloat64(0) AS value, toUInt8(1) AS status FROM (SELECT 1 FROM "+tableName(p.cfg.CHDatabase, p.cfg.HistogramsTable)+" WHERE "+strings.Join(where, " AND ")+" LIMIT 1)")
	}
	if call.name != "default_rollup" {
		where = append(where, nonStaleSampleSQL("value"))
	}
	samples := p.add("SELECT id, toUnixTimestamp64Milli(timestamp) AS sample_ts, value AS sample_value FROM " + tableName(p.cfg.CHDatabase, p.cfg.SamplesTable) + " WHERE " + strings.Join(where, " AND "))
	var values string
	if call.windowTotal() {
		values = p.totalRollup(samples, call, sumContributions)
	} else {
		values = p.windowRollup(samples, call)
	}
	return p.add("SELECT metric, ts, value FROM " + values + " INNER JOIN " + selected + " USING id WHERE NOT isNaN(value)"), true
}

func (p *nativeRangePlan) windowRollup(samples string, call rangeRollupCall) string {
	points := (p.end-p.start)/p.step + 1
	expanded := p.add(fmt.Sprintf("SELECT id, sample_ts, sample_value, idx, toInt64(%d) + toInt64(idx)*%d AS ts FROM %s ARRAY JOIN range(toUInt64(greatest(0, ceil((sample_ts + %d - %d) / %d))), toUInt64(greatest(0, least(%d, ceil((sample_ts + %d + %d - %d) / %d))))) AS idx", p.start, p.step, samples, call.offset, p.start, p.step, points, call.offset, call.matrix, p.start, p.step))
	arrays := p.add(fmt.Sprintf("SELECT id, ts, ts - %d AS eval_ts, arraySort(x -> x.1, groupArray((sample_ts, sample_value))) AS points FROM %s GROUP BY id, ts", call.offset, expanded))
	return p.rollupValues(arrays, call)
}

func (p *nativeRangePlan) rollupValues(source string, call rangeRollupCall) string {
	switch call.name {
	case "sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "count_over_time", "last_over_time", "present_over_time":
		values := map[string]string{"sum_over_time": "arraySum(vs)", "avg_over_time": "arrayAvg(vs)", "min_over_time": "arrayMin(vs)", "max_over_time": "arrayMax(vs)", "count_over_time": "toFloat64(length(vs))", "last_over_time": "vs[-1]", "present_over_time": "toFloat64(1)"}
		return p.add("SELECT id, ts, " + values[call.name] + " AS value FROM (SELECT *, arrayMap(x -> x.2, points) AS vs FROM " + source + ")")
	}
	// Match metricsQLSampleInterval: the last 20 intervals in this step's
	// left-open matrix, with interpolation before applying the integer margin.
	interval := p.add(fmt.Sprintf("SELECT *, arraySort(arraySlice(arrayPopFront(arrayDifference(arrayMap(x -> x.1, points))), -20)) AS gaps, 0.6*(length(gaps)-1) AS pos, toUInt64(pos)+1 AS gap_index, if(empty(gaps), toInt64(%d), gaps[gap_index] + toInt64((gaps[least(gap_index+1,length(gaps))]-gaps[gap_index])*(pos-floor(pos)))) AS sample_gap FROM %s", p.step, source))
	window := strconv.FormatInt(call.window, 10)
	if call.window == 0 {
		window = fmt.Sprintf("greatest(%d, max_prev)", p.step)
		if call.name == "default_rollup" {
			window = fmt.Sprintf("least(%s, %d)", window, call.lookback)
		}
	}
	prepared := p.add(fmt.Sprintf("SELECT *, least(if(sample_gap <= 0 OR empty(gaps), toInt64(%d), %s), toInt64(%d)) AS max_prev, toInt64(%s) AS window_ms, arrayFilter(x -> x.1 > eval_ts-window_ms, points) AS current, length(points)-length(current) AS before FROM %s", p.step, metricsQLSampleIntervalMarginSQL("sample_gap"), call.lookback, window, interval))
	if call.name == "default_rollup" {
		return p.add("SELECT id, ts, current[-1].2 AS value FROM " + prepared + " WHERE NOT empty(current) AND " + nonStaleSampleSQL("current[-1].2"))
	}
	prev := "before > 0 AND points[before].1 > eval_ts-window_ms-max_prev"
	if call.windowTotal() {
		prev = "before > 0 AND (points[before].1 > eval_ts-window_ms-max_prev OR current[1].1-points[before].1 < " + strconv.FormatInt(call.lookback, 10) + ")"
	}
	base := p.add("SELECT *, (" + prev + ") AS has_prev, if(has_prev, before, before+1) AS initial_begin FROM " + prepared + " WHERE NOT empty(current)")
	begin := "initial_begin"
	if call.name == "irate" || call.name == "idelta" {
		begin = "greatest(initial_begin, length(points)-1)"
	}
	baseline := "points[begin].2"
	if call.windowTotal() {
		baseline = "if(has_prev, points[begin].2, if(abs(current[1].2) < 10*(abs(if(length(current)>1, current[2].2-current[1].2, 0))+1), 0, current[1].2))"
	}
	if call.name == "idelta" {
		baseline = "if(length(points)-begin=0, 0, points[begin].2)"
	}
	ready := p.add("SELECT *, " + begin + " AS begin, " + baseline + " AS baseline, arraySlice(points, begin) AS tail FROM " + base)
	value := "points[-1].2-baseline"
	if call.counter() {
		value += " + arraySum(arrayMap((v, prev) -> if(v < prev, if((prev-v)*8 < prev, prev-v, prev), 0), arrayMap(x -> x.2, tail), arrayPushFront(arrayPopBack(arrayMap(x -> x.2, tail)), baseline)))"
	}
	if call.name == "rate" || call.name == "irate" {
		value = "if(length(tail)<2 OR points[-1].1-points[begin].1<=0, nan, (" + value + ")/((points[-1].1-points[begin].1)/1000))"
	}
	return p.add("SELECT id, ts, " + value + " AS value FROM " + ready)
}

// For series without a lookback-sized gap, increase and delta need only
// neighbours, not the full matrix at every step. Series with larger gaps use
// the exact per-step interval calculation above; both stay in one statement.
func (p *nativeRangePlan) totalRollup(samples string, call rangeRollupCall, sumContributions bool) string {
	arrays := p.add(fmt.Sprintf("SELECT id, arraySort(x -> x.1, groupArray((sample_ts,sample_value))) AS points, arrayMax(arrayDifference(arrayMap(x -> x.1,points))) >= %d AS long_gap FROM %s GROUP BY id", call.lookback, samples))
	neighbours := p.add("SELECT id, sample.1 AS sample_ts, sample.2 AS v, sample.3 AS prev_ts, sample.4 AS prev_v, sample.5 AS next_ts, sample.6 AS next_v FROM (SELECT id, arrayMap(x -> x.1,points) AS tss, arrayMap(x -> x.2,points) AS vs, arrayZip(tss,vs,arrayPushFront(arrayPopBack(tss)," + rangePushdownSentinel + "),arrayPushFront(arrayPopBack(vs),toFloat64(0)),arrayPushBack(arrayPopFront(tss),toInt64(9223372036854775807)),arrayPushBack(arrayPopFront(vs),toFloat64(0))) AS neighbours FROM " + arrays + " WHERE NOT long_gap) ARRAY JOIN neighbours AS sample")
	count := (p.end-p.start)/p.step + 1
	expanded := p.add(fmt.Sprintf("SELECT *, toInt64(%d)+toInt64(idx)*%d AS ts, ts - (%d) AS eval_ts FROM %s ARRAY JOIN range(toUInt64(greatest(0,ceil((sample_ts + (%d) - (%d))/%d))),toUInt64(greatest(0,least(%d,ceil((sample_ts + (%d) + (%d) - (%d))/%d))))) AS idx", p.start, p.step, call.offset, neighbours, call.offset, p.start, p.step, count, call.offset, call.window, p.start, p.step))
	diff := "v-prev_v"
	if call.counter() {
		diff = "if(v>=prev_v,v-prev_v,if((prev_v-v)*8<prev_v,0,v))"
	}
	baseline := "if(abs(v)<10*(abs(if(next_ts<=eval_ts,next_v-v,0))+1),v,0)"
	contribution := fmt.Sprintf("if(prev_ts > eval_ts-%d, %s, %s)", call.matrix, diff, baseline)
	fastSQL := "SELECT id, ts, " + contribution + " AS value FROM " + expanded
	if !sumContributions {
		fastSQL = "SELECT id, ts, sum(" + contribution + ") AS value FROM " + expanded + " GROUP BY id, ts"
	}
	fast := p.add(fastSQL)
	sparse := p.add("SELECT id, sample.1 AS sample_ts, sample.2 AS sample_value FROM " + arrays + " ARRAY JOIN points AS sample WHERE long_gap")
	exact := p.windowRollup(sparse, call)
	return p.add("SELECT * FROM " + fast + " UNION ALL SELECT * FROM " + exact)
}
