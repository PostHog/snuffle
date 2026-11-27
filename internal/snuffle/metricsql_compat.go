package snuffle

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

// metricsQLInternalPrefix marks functions that only prepareMetricsQLQuery emits.
const metricsQLInternalPrefix = "__snuffle_"

// metricsQLCarrierPrefix marks a Prometheus function name that the MetricsQL
// parser rejects. The name travels through the parser as a string argument.
const metricsQLCarrierPrefix = metricsQLInternalPrefix + "fn:"

// metricsQLDefaultRollupName is the engine name of the internal default_rollup.
// The engine keeps metric names only for last_over_time and first_over_time.
const metricsQLDefaultRollupName = "last_over_time"

const metricsQLTlastOverTimeName = metricsQLInternalPrefix + "tlast_over_time"

var metricsQLCounterFunctions = []string{"increase", "rate", "delta", "irate", "idelta"}

// metricsQLPureCounterFunctions are counter functions that only the engine
// evaluates. increase_pure is increase where a new counter starts from 0.
// increase_prometheus is increase from the first sample in the window, as
// Prometheus computes it without extrapolation.
var metricsQLPureCounterFunctions = map[string]string{"increase_pure": "increase", "increase_prometheus": "increase"}

// metricsQLDeltaWindowFunctions return the sum of the points in the window for
// a delta series. rate divides that sum by the window. The running total of a
// delta series starts at the query range. The MetricsQL baseline rules
// therefore miscount a series with no point shortly before the window.
var metricsQLDeltaWindowFunctions = map[string]bool{"increase": true, "increase_pure": true, "increase_prometheus": true, "delta": true, "rate": true}

// deltaSeriesOf returns the delta series that the selector read with
// these labels. It returns nil for a subquery, which has no storage series set.
// It reads the series set that the engine leaves on the selector after it
// expands the series. If the engine stops keeping it, delta series fall back
// to MetricsQL counter rules, and the delta window tests fail.
func deltaSeriesOf(vs *parser.VectorSelector, lset labels.Labels) *seriesMeta {
	set, ok := vs.UnexpandedSeriesSet.(*futureSeriesSet)
	if !ok {
		return nil
	}
	return set.deltaSeries().find(lset)
}

// metricsQLSelectorFunctions inspect their selector argument in the engine.
// Their selector arguments keep the Prometheus lookback.
var metricsQLSelectorFunctions = map[string]bool{"absent": true, "timestamp": true}

func init() {
	rollupArgTypes := []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeScalar, parser.ValueTypeScalar, parser.ValueTypeScalar, parser.ValueTypeScalar}
	for _, name := range metricsQLCounterFunctions {
		internalName := metricsQLInternalPrefix + name
		// metricsQLWithNextSample adds an optional sixth argument.
		parser.Functions[internalName] = &parser.Function{Name: internalName, ArgTypes: append(slices.Clone(rollupArgTypes), parser.ValueTypeScalar), Variadic: 1, ReturnType: parser.ValueTypeVector}
		promql.FunctionCalls[internalName] = metricsQLRollup(name, promql.FunctionCalls[name])
	}
	for name, histogramName := range metricsQLPureCounterFunctions {
		internalName := metricsQLInternalPrefix + name
		parser.Functions[internalName] = &parser.Function{Name: internalName, ArgTypes: rollupArgTypes, ReturnType: parser.ValueTypeVector}
		promql.FunctionCalls[internalName] = metricsQLRollup(name, promql.FunctionCalls[histogramName])
	}
	// tlast_over_time is ts_of_last_over_time, which Prometheus marks experimental.
	tlast := *parser.Functions["ts_of_last_over_time"]
	tlast.Name, tlast.Experimental = metricsQLTlastOverTimeName, false
	parser.Functions[tlast.Name] = &tlast
	promql.FunctionCalls[tlast.Name] = promql.FunctionCalls["ts_of_last_over_time"]
	runningSum := metricsQLInternalPrefix + "running_sum"
	parser.Functions[runningSum] = &parser.Function{Name: runningSum, ArgTypes: []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeScalar}, ReturnType: parser.ValueTypeVector}
	promql.FunctionCalls[runningSum] = func(v []promql.Vector, m promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		// The subquery holds every step since the range start. Sum the points
		// from the start to the evaluation time.
		var sum float64
		seen := false
		for _, p := range m[0].Floats {
			if p.T >= int64(v[0][0].F) && !math.IsNaN(p.F) {
				sum += p.F
				seen = true
			}
		}
		if !seen {
			return enh.Out, nil
		}
		return append(enh.Out, promql.Sample{F: sum}), nil
	}
	parser.Functions[metricsQLGroupJoinName] = &parser.Function{Name: metricsQLGroupJoinName, ArgTypes: []parser.ValueType{parser.ValueTypeVector, parser.ValueTypeVector, parser.ValueTypeString}, Variadic: -1, ReturnType: parser.ValueTypeVector}
	promql.FunctionCalls[metricsQLGroupJoinName] = metricsQLGroupJoin
	parser.Functions[metricsQLGroupJoinEndName] = &parser.Function{Name: metricsQLGroupJoinEndName, ArgTypes: []parser.ValueType{parser.ValueTypeVector}, ReturnType: parser.ValueTypeVector}
	promql.FunctionCalls[metricsQLGroupJoinEndName] = metricsQLGroupJoinEnd
	parser.Functions[metricsQLInternalPrefix+"default_rollup"] = &parser.Function{Name: metricsQLDefaultRollupName, ArgTypes: rollupArgTypes, ReturnType: parser.ValueTypeVector}
	original := promql.FunctionCalls[metricsQLDefaultRollupName]
	rollup := metricsQLRollup("default_rollup", original)
	promql.FunctionCalls[metricsQLDefaultRollupName] = func(v []promql.Vector, m promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		if len(args) > 1 {
			return rollup(v, m, args, enh)
		}
		// The storage adapter keeps stale markers as NaN for this engine name.
		// Public last_over_time ignores those markers.
		series := seriesWithoutNaN(m[0])
		if len(series.Floats)+len(series.Histograms) == 0 {
			return enh.Out, nil
		}
		return original(v, promql.Matrix{series}, args, enh)
	}
}

// metricsQLQuery is a request expression prepared for the Prometheus engine.
type metricsQLQuery struct {
	query string
	// runningSum applies a running sum to the range result.
	runningSum bool
	// rangeOps transform the result of query over the whole range, innermost
	// first, after runningSum and the range cache.
	rangeOps []metricsQLRangeOp
	// defaultQuery is the right side of an outermost `query default right`,
	// in MetricsQL, and defaultRight its preparation. It fills the gaps of
	// the result before the range ops.
	defaultQuery string
	defaultRight *metricsQLQuery
}

// prepareMetricsQLQuery expands MetricsQL syntax before either execution path.
// Counter functions use separate names so Prometheus SQL optimizations cannot
// replace their calculations with extrapolated PromQL results.
func prepareMetricsQLQuery(query string, step, lookback time.Duration, start, end time.Time) (metricsQLQuery, error) {
	return prepareMetricsQLQueryDepth(query, step, lookback, start, end, 0)
}

// prepareMetricsQLQueryDepth prepares query, the right side of depth nested
// default operators.
func prepareMetricsQLQueryDepth(query string, step, lookback time.Duration, start, end time.Time, depth int) (metricsQLQuery, error) {
	expr, err := metricsql.Parse(carryUnsupportedFunctions(query))
	if err != nil {
		return metricsQLQuery{}, err
	}
	if step <= 0 {
		step = 5 * time.Minute
	}
	if step < time.Millisecond {
		return metricsQLQuery{}, fmt.Errorf("step must be at least 1ms")
	}
	if lookback <= 0 {
		lookback = 5 * time.Minute
	}
	restoreCarriedFunctions(expr)

	var prepared metricsQLQuery
	for {
		op, inner, err := prepareRangeTopK(expr)
		if err != nil {
			return metricsQLQuery{}, err
		}
		if op == nil {
			break
		}
		if len(prepared.rangeOps) >= maxRangeOps {
			return metricsQLQuery{}, fmt.Errorf("topk_* and bottomk_* calls nest at most %d deep", maxRangeOps)
		}
		prepared.rangeOps = append([]metricsQLRangeOp{op}, prepared.rangeOps...)
		// Like the argument of any aggregate, a range selector or subquery
		// reads its last sample.
		expr = metricsQLImplicitRollup(inner)
	}
	if be, ok := expr.(*metricsql.BinaryOpExpr); ok && strings.EqualFold(be.Op, "default") {
		if be.GroupModifier.Op != "" || be.JoinModifier.Op != "" {
			return metricsQLQuery{}, fmt.Errorf("default does not support on, ignoring, group_left or group_right")
		}
		// Each level parses the rest of the query again, so bound the nesting.
		if depth >= maxRangeOps {
			return metricsQLQuery{}, fmt.Errorf("default operators nest at most %d deep", maxRangeOps)
		}
		prepared.defaultQuery = string(be.Right.AppendString(nil))
		right, err := prepareMetricsQLQueryDepth(prepared.defaultQuery, step, lookback, start, end, depth+1)
		if err != nil {
			return metricsQLQuery{}, err
		}
		prepared.defaultRight = &right
		expr = be.Left
	}
	if fe, ok := expr.(*metricsql.FuncExpr); ok && fe.Name == "running_sum" {
		if len(fe.Args) != 1 || fe.KeepMetricNames {
			return metricsQLQuery{}, fmt.Errorf("running_sum requires one series argument; keep_metric_names is not supported")
		}
		if start.Equal(end) {
			return metricsQLQuery{}, fmt.Errorf("running_sum requires a range query")
		}
		prepared.runningSum = true
		expr = fe.Args[0]
	}
	expr = metricsQLDefaultSelectors(expr)
	rewriter := metricsQLRewriter{lookback: lookback, start: start, end: end}
	if err := rewriter.rewrite(&expr, step, start.Equal(end)); err != nil {
		return metricsQLQuery{}, err
	}
	prepared.query = quoteDottedIdents(string(expr.AppendString(nil)))
	return prepared, nil
}

// carryUnsupportedFunctions renames Prometheus functions that the MetricsQL
// parser rejects into the variadic union function. The original name becomes
// the first argument. restoreCarriedFunctions reverses the change.
func carryUnsupportedFunctions(query string) string {
	var out strings.Builder
	for i := 0; i < len(query); {
		c := query[i]
		if c == '"' || c == '\'' || c == '`' {
			end := skipQuotedString(query, i)
			out.WriteString(query[i:end])
			i = end
			continue
		}
		if !isIdentStart(c) {
			out.WriteByte(c)
			i++
			continue
		}
		j := i + 1
		for j < len(query) && isIdentPart(query[j]) {
			j++
		}
		ident := query[i:j]
		k := skipSpaces(query, j)
		if k >= len(query) || query[k] != '(' || parser.Functions[ident] == nil || metricsql.IsSupportedFunction(ident) {
			out.WriteString(ident)
			i = j
			continue
		}
		out.WriteString(`union("` + metricsQLCarrierPrefix + ident + `"`)
		k = skipSpaces(query, k+1)
		if k < len(query) && query[k] == ')' {
			out.WriteByte(')')
			i = k + 1
			continue
		}
		out.WriteString(", ")
		i = k
	}
	return out.String()
}

func restoreCarriedFunctions(expr metricsql.Expr) {
	metricsql.VisitAll(expr, func(e metricsql.Expr) {
		fe, ok := e.(*metricsql.FuncExpr)
		if !ok || !strings.EqualFold(fe.Name, "union") || len(fe.Args) == 0 {
			return
		}
		name, ok := fe.Args[0].(*metricsql.StringExpr)
		if !ok || !strings.HasPrefix(name.S, metricsQLCarrierPrefix) {
			return
		}
		fe.Name = strings.TrimPrefix(name.S, metricsQLCarrierPrefix)
		fe.Args = fe.Args[1:]
	})
}

func skipQuotedString(query string, start int) int {
	quote := query[start]
	for i := start + 1; i < len(query); i++ {
		switch query[i] {
		case '\\':
			if quote != '`' {
				i++
			}
		case quote:
			return i + 1
		}
	}
	return len(query)
}

func skipSpaces(query string, i int) int {
	for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
		i++
	}
	return i
}

// quoteDottedIdents rewrites the dotted names that the MetricsQL printer emits
// bare (http.server.duration) into the quoted form that Prometheus requires.
// A dotted metric name becomes a quoted selector entry, and a dotted label name
// in a matcher or a grouping list becomes a quoted string. The input is always
// printer output, so braces and grouping lists never nest.
func quoteDottedIdents(query string) string {
	var out strings.Builder
	inBraces, inList := false, false
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '"' || c == '\'' || c == '`':
			end := skipQuotedString(query, i)
			out.WriteString(query[i:end])
			i = end
		case isIdentStart(c):
			end := i
			for end < len(query) && (isIdentPart(query[end]) || query[end] == '.') {
				end++
			}
			ident := query[i:end]
			i = end
			next := byte(0)
			if i < len(query) {
				next = query[i]
			}
			switch {
			case !strings.Contains(ident, "."):
				out.WriteString(ident)
				if next == '(' && groupingModifiers[ident] {
					inList = true
				}
			case inBraces || inList:
				out.WriteString(strconv.Quote(ident))
			case next == '{':
				out.WriteString("{" + strconv.Quote(ident) + ",")
				i++
				inBraces = true
			default:
				out.WriteString("{" + strconv.Quote(ident) + "}")
			}
		default:
			switch c {
			case '{':
				inBraces = true
			case '}':
				inBraces = false
			case ')':
				inList = false
			}
			out.WriteByte(c)
			i++
		}
	}
	return out.String()
}

var groupingModifiers = map[string]bool{"by": true, "without": true, "on": true, "ignoring": true, "group_left": true, "group_right": true}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || c == ':' || (c >= '0' && c <= '9')
}

// metricsQLRewriter resolves step-relative durations and rewrites rollup
// functions. The step changes inside a subquery, so the rewriter carries it.
type metricsQLRewriter struct {
	lookback time.Duration
	start    time.Time
	end      time.Time
	// subquerySteps is the most steps the engine evaluates inside the
	// enclosing subquery, or 0 outside a subquery. span is the time range
	// that evaluation covers.
	subquerySteps int64
	span          time.Duration
}

// inSubquery returns a copy of the rewriter for the expression inside a
// subquery over window at step. The engine evaluates a subquery once across
// the whole enclosing range extended back by the window, at multiples of the
// step, so it takes at most (span + window)/step + 1 steps. Nested subqueries
// extend the span again.
func (r metricsQLRewriter) inSubquery(window, step time.Duration) metricsQLRewriter {
	span := r.end.Sub(r.start)
	if r.subquerySteps > 0 {
		span = r.span
	}
	r.subquerySteps = metricsQLMaxPointsPerSeries
	r.span = span
	if window > 0 && step > 0 && span <= time.Duration(math.MaxInt64)-window {
		r.span = span + window
		r.subquerySteps = min(int64(r.span/step)+1, metricsQLMaxPointsPerSeries)
	}
	return r
}

func (r metricsQLRewriter) rewrite(expr *metricsql.Expr, step time.Duration, instant bool) error {
	switch e := (*expr).(type) {
	case *metricsql.DurationExpr:
		_, err := resolveMetricsQLDuration(e, step)
		return err
	case *metricsql.RollupExpr:
		// Prometheus requires an explicit subquery step after expressions.
		if _, raw := e.Expr.(*metricsql.MetricExpr); !raw && e.Window != nil && e.Step == nil {
			e.Step = metricsQLDuration(step)
		}
		childStep, err := r.resolveRollupDurations(e, step)
		if err != nil {
			return err
		}
		if e.Step != nil {
			var window time.Duration
			if e.Window != nil {
				window = time.Duration(e.Window.Duration(step.Milliseconds())) * time.Millisecond
			}
			return r.inSubquery(window, childStep).rewrite(&e.Expr, childStep, false)
		}
		return r.rewrite(&e.Expr, childStep, instant)
	case *metricsql.FuncExpr:
		if e.Name == "" || e.Name == "union" {
			replacement, err := rewriteUnion(e)
			if err != nil {
				return err
			}
			*expr = replacement
			if union, ok := replacement.(*metricsql.FuncExpr); ok && union.Name == metricsQLUnionName {
				return r.rewriteArgs(union.Args, step, instant)
			}
			return r.rewrite(expr, step, instant)
		}
		if !instant && strings.HasPrefix(e.Name, "sort_by_label") {
			// Range results keep label order, so sorting at every step is
			// wasted work. Validate the call, then drop it.
			if _, err := rewriteLabelFunction(e); err != nil {
				return err
			}
			*expr = e.Args[0]
			return r.rewrite(expr, step, instant)
		}
		// Label functions reject keep_metric_names in rewriteLabelFunction.
		if _, label := metricsQLLabelFunctions[e.Name]; !label && e.KeepMetricNames && e.Name != "histogram_quantiles" && e.Name != "running_sum" {
			return r.rewriteKeepMetricNames(expr, e, step, instant)
		}
		return r.rewriteFunc(e, step, instant)
	case *metricsql.AggrFuncExpr:
		if isRangeTopK(e.Name) {
			return fmt.Errorf("%s is supported only as the outermost function of a query", e.Name)
		}
		if e.Name == "quantiles" {
			replacement, err := r.rewriteQuantiles(e, step, instant)
			if err != nil {
				return err
			}
			*expr = replacement
			return nil
		}
		if e.Name == "median" {
			replacement, err := r.rewriteMedian(e, step, instant)
			if err != nil {
				return err
			}
			*expr = replacement
			return nil
		}
		return r.rewriteArgs(e.Args, step, instant)
	case *metricsql.BinaryOpExpr:
		if strings.EqualFold(e.Op, "default") {
			return fmt.Errorf("default is supported only as the outermost operator of a query or of the argument of topk_* and bottomk_*")
		}
		if strings.EqualFold(e.Op, "or") {
			// MetricsQL reads a number in `x or 0` as a series without
			// labels. Prometheus only accepts set operators between vectors.
			e.Left, e.Right = numberAsVector(e.Left), numberAsVector(e.Right)
		}
		if err := r.rewrite(&e.Left, step, instant); err != nil {
			return err
		}
		if err := r.rewrite(&e.Right, step, instant); err != nil {
			return err
		}
		rewriteGroupJoin(expr, e, groupJoinPointsBucket(r.pointsPerSeries(step, instant)))
	}
	return nil
}

// metricsQLGroupJoinName pairs each series on the many side of a group_left
// or group_right join with each one-side series of its match group, and sets
// the copied labels of that series.
const metricsQLGroupJoinName = metricsQLInternalPrefix + "group_join"

// metricsQLGroupJoinEndName wraps the rewritten join and removes the label
// that keeps its many-side series apart, see metricsQLGroupJoin.
const metricsQLGroupJoinEndName = metricsQLInternalPrefix + "group_join_end"

// metricsQLGroupJoinMarker holds the copied labels a many-side series had
// before the join overwrote them. Series that differ only in those labels
// would otherwise share a label set before the operator filters them.
const metricsQLGroupJoinMarker = metricsQLInternalPrefix + "group_join_origin"

// rewriteGroupJoin applies the VictoriaMetrics rule for group_left(l) and
// group_right(l): one-side series of a match group that differ in l each give
// a result, where Prometheus fails with many-to-many matching. The many side
// becomes __snuffle_group_join(many, one, points, "on"|"ignoring",
// matching..., "", copied...), where points is how many points the engine may
// hold for each output series. The join matches on the copied labels too.
// One-side series that also share l still fail, as in VictoriaMetrics.
// ponytail: the engine evaluates the one side twice. A join function that also
// applies the operator would evaluate it once, if that cost shows.
func rewriteGroupJoin(slot *metricsql.Expr, e *metricsql.BinaryOpExpr, points int) {
	join, group := strings.ToLower(e.JoinModifier.Op), strings.ToLower(e.GroupModifier.Op)
	if (join != "group_left" && join != "group_right") || len(e.JoinModifier.Args) == 0 || group == "" || e.JoinModifierPrefix != nil {
		return
	}
	many, one := &e.Left, e.Right
	if join == "group_right" {
		many, one = &e.Right, e.Left
	}
	// The one side is both an argument and the operand, so it is serialized
	// twice. A join nested in it would double its whole subtree at every
	// level and grow the query exponentially. Such a join keeps Prometheus'
	// rule and still fails when a match group has several one-side series.
	if containsGroupJoin(one) {
		return
	}
	// A scalar operand is not an instant vector argument. Prometheus joins it
	// with no matching labels, so the join keeps that rule.
	if isScalarOperand(*many) || isScalarOperand(one) {
		return
	}
	args := []metricsql.Expr{*many, one, &metricsql.StringExpr{S: strconv.Itoa(points)}, &metricsql.StringExpr{S: group}}
	for _, name := range e.GroupModifier.Args {
		args = append(args, &metricsql.StringExpr{S: name})
	}
	args = append(args, &metricsql.StringExpr{})
	for _, name := range e.JoinModifier.Args {
		args = append(args, &metricsql.StringExpr{S: name})
	}
	*many = &metricsql.FuncExpr{Name: metricsQLGroupJoinName, Args: args}
	// The many side now carries the copied labels. Prometheus rejects a label
	// in both on() and group_left(), so the join copies none.
	copied := e.JoinModifier.Args
	if group == "on" {
		for _, name := range copied {
			if !slices.Contains(e.GroupModifier.Args, name) {
				e.GroupModifier.Args = append(e.GroupModifier.Args, name)
			}
		}
	} else {
		e.GroupModifier.Args = slices.DeleteFunc(slices.Clone(e.GroupModifier.Args), func(name string) bool { return slices.Contains(copied, name) })
		// The marker is on the many side only, so the join must not match on it.
		e.GroupModifier.Args = append(e.GroupModifier.Args, metricsQLGroupJoinMarker)
	}
	e.JoinModifier.Args = nil
	*slot = &metricsql.FuncExpr{Name: metricsQLGroupJoinEndName, Args: []metricsql.Expr{e}}
}

// groupJoinPointsBucket rounds a point count up to a power of two, at most the
// engine's cap. The count is written into the query text, and the range cache
// compares the text for a range shifted by one step, so a count that follows
// the range exactly would make every such query uncacheable. The budget a
// bucket gives is at most twice as strict as the exact count.
func groupJoinPointsBucket(points int) int {
	bucket := 1
	for bucket < points && bucket < metricsQLMaxPointsPerSeries {
		bucket <<= 1
	}
	return min(bucket, metricsQLMaxPointsPerSeries)
}

// isScalarOperand reports whether expr is a number, a calculation on numbers,
// or time(), pi() or scalar(): an operand Prometheus types as a scalar.
func isScalarOperand(expr metricsql.Expr) bool {
	if call, ok := expr.(*metricsql.FuncExpr); ok {
		switch strings.ToLower(call.Name) {
		case "scalar", "time", "pi":
			return true
		}
	}
	scalar := true
	metricsql.VisitAll(expr, func(node metricsql.Expr) {
		switch node.(type) {
		case *metricsql.MetricExpr, *metricsql.RollupExpr, *metricsql.FuncExpr, *metricsql.AggrFuncExpr:
			scalar = false
		}
	})
	return scalar
}

func containsGroupJoin(expr metricsql.Expr) bool {
	found := false
	metricsql.VisitAll(expr, func(node metricsql.Expr) {
		if call, ok := node.(*metricsql.FuncExpr); ok && call.Name == metricsQLGroupJoinName {
			found = true
		}
	})
	return found
}

func metricsQLGroupJoin(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	points, err := strconv.Atoi(args[2].(*parser.StringLiteral).Val)
	if err != nil {
		panic(fmt.Errorf("invalid internal group join points: %w", err))
	}
	names := stringArgs(args[3:])
	separator := slices.Index(names, "")
	matching, copied := slices.Clone(names[1:separator]), names[separator+1:]
	on := names[0] == "on"
	if !on {
		matching = append(matching, labels.MetricName)
	}
	slices.Sort(matching)
	var buf []byte
	signature := func(lset labels.Labels) string {
		if on {
			buf = lset.BytesWithLabels(buf, matching...)
		} else {
			buf = lset.BytesWithoutLabels(buf, matching...)
		}
		return string(buf)
	}
	// The copied labels of each one-side series, once per distinct value.
	// Series that share them stay a duplicate for the join to reject.
	groups := map[string][]labels.Labels{}
	seen := map[string]map[string]struct{}{}
	for _, sample := range vectors[1] {
		key := signature(sample.Metric)
		values := sample.Metric.MatchLabels(true, copied...)
		id := values.String()
		if _, dup := seen[key][id]; dup {
			continue
		}
		if seen[key] == nil {
			seen[key] = map[string]struct{}{}
		}
		seen[key][id] = struct{}{}
		groups[key] = append(groups[key], values)
	}
	// The engine checks MaxSamples only after this function returns, and in a
	// range query it first preallocates a point slice for each new output
	// series. Refuse an expansion over the budget before allocating it, with
	// each added series counted for all its points. A many-side series that
	// pairs with one one-side series adds nothing: it is the series the engine
	// already loaded and budgets, so a one-to-one join is never refused here.
	limit := metricsQLMaxOutputSamples.Load()
	if limit > 0 {
		var added int64
		for _, sample := range vectors[0] {
			added += int64(max(len(groups[signature(sample.Metric)])-1, 0))
		}
		if added > limit/int64(max(points, 1)) {
			panic(promql.ErrTooManySamples("group_left"))
		}
	}
	var extra []labels.Labels
	builder := labels.NewBuilder(labels.EmptyLabels())
	for _, sample := range vectors[0] {
		origin := sample.Metric.MatchLabels(true, copied...).String()
		for index, values := range groups[signature(sample.Metric)] {
			builder.Reset(sample.Metric)
			builder.Set(metricsQLGroupJoinMarker, origin)
			for _, name := range copied {
				if value := values.Get(name); value != "" {
					builder.Set(name, value)
				} else {
					builder.Del(name)
				}
			}
			out := sample
			out.Metric = builder.Labels()
			enh.Out = append(enh.Out, out)
			if index > 0 {
				extra = append(extra, out.Metric)
			}
		}
	}
	// The same series at a later step reuses its point slice, so count the
	// distinct series the join added across all steps of the query.
	if limit > 0 && len(extra) > 0 {
		if !quantilesSeriesFor(args[2].(*parser.StringLiteral)).admit(extra, []float64{0}, points, limit) {
			panic(promql.ErrTooManySamples("group_left"))
		}
	}
	return enh.Out, nil
}

// metricsQLGroupJoinEnd removes the marker label set by metricsQLGroupJoin.
func metricsQLGroupJoinEnd(vectors []promql.Vector, _ promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	builder := labels.NewBuilder(labels.EmptyLabels())
	for _, sample := range vectors[0] {
		builder.Reset(sample.Metric)
		builder.Del(metricsQLGroupJoinMarker)
		out := sample
		out.Metric = builder.Labels()
		enh.Out = append(enh.Out, out)
	}
	return enh.Out, nil
}

func numberAsVector(expr metricsql.Expr) metricsql.Expr {
	number, ok := expr.(*metricsql.NumberExpr)
	if !ok {
		return expr
	}
	vector := &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{number}}
	if math.IsNaN(number.N) {
		// MetricsQL treats NaN as no data, so `NaN or x` is x. vector(NaN)
		// would be a present series in Prometheus and hide x; use an empty
		// vector instead.
		vector.Args = []metricsql.Expr{&metricsql.NumberExpr{N: 0}}
		return &metricsql.BinaryOpExpr{Op: "<", Left: vector, Right: &metricsql.NumberExpr{N: 0}}
	}
	return vector
}

func (r metricsQLRewriter) rewriteArgs(args []metricsql.Expr, step time.Duration, instant bool) error {
	for index := range args {
		if err := r.rewrite(&args[index], step, instant); err != nil {
			return err
		}
	}
	return nil
}

func (r metricsQLRewriter) rewriteFunc(e *metricsql.FuncExpr, step time.Duration, instant bool) error {
	if strings.HasPrefix(e.Name, metricsQLInternalPrefix) {
		return fmt.Errorf("function %q is reserved for query execution", e.Name)
	}
	if e.Name == "histogram_quantiles" {
		return r.rewriteHistogramQuantiles(e, step, instant)
	}
	if e.Name == "histogram_share" {
		return r.rewriteHistogramShare(e, step, instant)
	}
	if renamed, err := rewriteLabelFunction(e); renamed || err != nil {
		if err != nil {
			return err
		}
		return r.rewriteArgs(e.Args, step, instant)
	}
	if e.Name == "running_sum" {
		return r.rewriteNestedRunningSum(e, step, instant)
	}
	if metricsQLSelectorFunctions[e.Name] || !metricsql.IsRollupFunc(e.Name) {
		if fn := parser.Functions[e.Name]; fn != nil {
			for i, arg := range e.Args {
				if i < len(fn.ArgTypes) && fn.ArgTypes[i] == parser.ValueTypeVector {
					if _, numeric := arg.(*metricsql.NumberExpr); numeric {
						e.Args[i] = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{arg}}
					}
				}
			}
		}
		return r.rewriteArgs(e.Args, step, instant)
	}
	argIndex := metricsql.GetRollupArgIdx(e)
	if argIndex < 0 || argIndex >= len(e.Args) {
		return r.rewriteArgs(e.Args, step, instant)
	}
	for i := range e.Args {
		if i == argIndex {
			continue
		}
		if err := r.rewrite(&e.Args[i], step, instant); err != nil {
			return err
		}
	}
	roll, ok := e.Args[argIndex].(*metricsql.RollupExpr)
	if !ok {
		roll = &metricsql.RollupExpr{Expr: e.Args[argIndex]}
		e.Args[argIndex] = roll
	}
	implicit := roll.Window == nil
	if implicit {
		roll.Window = metricsQLDuration(step)
	}
	if _, raw := roll.Expr.(*metricsql.MetricExpr); !raw && roll.Step == nil {
		roll.Step = metricsQLDuration(step)
	}
	childStep, err := r.resolveRollupDurations(roll, step)
	if err != nil {
		return err
	}
	window := time.Duration(roll.Window.Duration(step.Milliseconds())) * time.Millisecond
	if window <= 0 {
		return fmt.Errorf("the query window must be at least 1ms")
	}
	_, pure := metricsQLPureCounterFunctions[e.Name]
	counter := pure || e.Name == "default_rollup" || slices.Contains(metricsQLCounterFunctions, e.Name)
	// Counter rollups fetch a lookback of history before the window below, and
	// the engine evaluates the subquery over that larger window.
	budgetWindow := window
	if counter && window <= time.Duration(math.MaxInt64)-r.lookback {
		budgetWindow += r.lookback
	}
	if err := r.inSubquery(budgetWindow, childStep).rewrite(&roll.Expr, childStep, false); err != nil {
		return err
	}
	if !counter {
		if e.Name == "tlast_over_time" {
			e.Name = metricsQLTlastOverTimeName
		}
		return nil
	}
	if len(e.Args) != 1 || e.KeepMetricNames {
		return fmt.Errorf("%s requires one series argument; keep_metric_names is not supported", e.Name)
	}
	if window > time.Duration(math.MaxInt64)-2*r.lookback {
		return fmt.Errorf("the query window is too large")
	}
	_, raw := roll.Expr.(*metricsql.MetricExpr)
	switch {
	case e.Name == "default_rollup" && instant && implicit:
		// A bare selector in an instant query reads only the lookback window.
		roll.Window = metricsQLDuration(r.lookback)
	case e.Name == "default_rollup" && implicit && raw:
		// A bare selector in a range query keeps its last sample for the
		// sample interval, up to the lookback. A series written about once
		// per lookback has a single sample in step + lookback at most steps,
		// so the interval fell back to the step (ENG-029). Over two lookbacks
		// it has two samples whenever its last sample is within the lookback.
		// A lone sample still falls back to the step, as in VictoriaMetrics,
		// so a stopped series ends as it does there.
		roll.Window = metricsQLDuration(defaultRollupHistory(window, r.lookback))
	default:
		// Fetch history before the window for the previous sample and
		// the sample interval. Explicit windows keep their original size.
		roll.Window = metricsQLDuration(window + r.lookback)
	}
	windowArg := window.Milliseconds()
	if implicit && (e.Name == "rate" || e.Name == "default_rollup") {
		windowArg = 0
	}
	instantArg := 0.0
	if instant {
		instantArg = 1
	}
	e.Name = metricsQLInternalPrefix + e.Name
	e.Args = append(e.Args,
		&metricsql.NumberExpr{N: float64(windowArg)},
		&metricsql.NumberExpr{N: float64(step.Milliseconds())},
		&metricsql.NumberExpr{N: float64(r.lookback.Milliseconds())},
		&metricsql.NumberExpr{N: instantArg},
	)
	return nil
}

// rewriteNestedRunningSum evaluates a running_sum inside another expression as
// a subquery that spans the range from start to each step. Prometheus aligns
// subquery steps to multiples of the step, so start must be aligned. The
// outermost running_sum of a request avoids this cost: prepareMetricsQLQuery
// removes it and metricsQLRunningSum sums the range result.
func (r metricsQLRewriter) rewriteNestedRunningSum(e *metricsql.FuncExpr, step time.Duration, instant bool) error {
	if len(e.Args) != 1 || e.KeepMetricNames {
		return fmt.Errorf("running_sum requires one series argument; keep_metric_names is not supported")
	}
	if instant {
		return fmt.Errorf("running_sum requires a range query")
	}
	if r.start.UnixMilli()%step.Milliseconds() != 0 {
		return fmt.Errorf("a nested running_sum requires start aligned to step")
	}
	span := r.end.Sub(r.start)
	if span > time.Duration(math.MaxInt64)-step {
		return fmt.Errorf("the running_sum range is too large")
	}
	inner := e.Args[0]
	if err := r.inSubquery(span+step, step).rewrite(&inner, step, false); err != nil {
		return err
	}
	e.Name = metricsQLInternalPrefix + "running_sum"
	e.Args = []metricsql.Expr{
		&metricsql.RollupExpr{Expr: inner, Window: metricsQLDuration(span + step), Step: metricsQLDuration(step)},
		&metricsql.NumberExpr{N: float64(r.start.UnixMilli())},
	}
	return nil
}

// resolveRollupDurations replaces step-relative durations with milliseconds
// and returns the step for the expression inside the rollup.
func (r metricsQLRewriter) resolveRollupDurations(e *metricsql.RollupExpr, step time.Duration) (time.Duration, error) {
	var err error
	if e.Window, err = resolveMetricsQLDuration(e.Window, step); err != nil {
		return 0, err
	}
	if e.Step, err = resolveMetricsQLDuration(e.Step, step); err != nil {
		return 0, err
	}
	if e.Offset, err = resolveMetricsQLDuration(e.Offset, step); err != nil {
		return 0, err
	}
	if e.Step == nil {
		return step, nil
	}
	childStep := time.Duration(e.Step.Duration(step.Milliseconds())) * time.Millisecond
	if childStep < time.Millisecond {
		return 0, fmt.Errorf("the subquery step must be at least 1ms")
	}
	return childStep, nil
}

// defaultRollupHistory is the matrix range of a bare selector in a range
// query: the step and two lookbacks.
func defaultRollupHistory(step, lookback time.Duration) time.Duration {
	return step + 2*lookback
}

func metricsQLDuration(d time.Duration) *metricsql.DurationExpr {
	// DurationExpr has no public constructor. Parse only our generated duration.
	e, err := metricsql.Parse("m offset " + fmt.Sprintf("%dms", d.Milliseconds()))
	if err != nil {
		panic(err)
	}
	return e.(*metricsql.RollupExpr).Offset
}

func resolveMetricsQLDuration(d *metricsql.DurationExpr, step time.Duration) (*metricsql.DurationExpr, error) {
	if d == nil {
		return nil, nil
	}
	millis := d.Duration(step.Milliseconds())
	if millis > math.MaxInt64/int64(time.Millisecond) || millis < math.MinInt64/int64(time.Millisecond) {
		return nil, fmt.Errorf("the query duration is too large")
	}
	return metricsQLDuration(time.Duration(millis) * time.Millisecond), nil
}

func metricsQLRollup(name string, histogramFallback promql.FunctionCall) promql.FunctionCall {
	return func(vectors []promql.Vector, matrix promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		ms := args[0].(*parser.MatrixSelector)
		vs := ms.VectorSelector.(*parser.VectorSelector)
		window := int64(vectors[0][0].F)
		step := int64(vectors[1][0].F)
		lookback := int64(vectors[2][0].F)
		instant := vectors[3][0].F != 0
		// metricsQLWithNextSample moved the selector a lookback ahead.
		ahead := int64(0)
		if len(vectors) > 4 {
			ahead = lookback
		}
		end := enh.Ts - vs.Offset.Milliseconds() - ahead
		if vs.Timestamp != nil {
			end = *vs.Timestamp - vs.OriginalOffset.Milliseconds()
		}
		input, after := seriesSplit(matrix[0], end)
		next := math.NaN()
		if ahead > 0 && len(after.Floats) > 0 && after.Floats[0].T <= int64(vectors[4][0].F) {
			next = after.Floats[0].F
		}
		points := input.Floats
		maxPrev := step
		if !instant {
			maxPrev = metricsQLSampleInterval(points, step)
		}
		maxPrev = min(maxPrev, lookback)
		if window == 0 {
			switch {
			case name != "default_rollup":
				window = max(step, maxPrev)
			case instant:
				// A bare selector in an instant query keeps the Prometheus lookback.
				window = lookback
			default:
				window = min(max(step, maxPrev), lookback)
			}
		}
		start := end - window
		series := seriesAfter(input, start)
		first := len(points) - len(series.Floats)
		if name != "default_rollup" {
			if meta := deltaSeriesOf(vs, input.Metric); meta != nil {
				if metricsQLDeltaWindowFunctions[name] {
					// The window sum is the delta series' own increment: the
					// first-sample rule for new counters (next) does not apply.
					value, ok := meta.deltaWindowSum(start, end)
					if !ok {
						return enh.Out, nil
					}
					if name == "rate" {
						value /= float64(window) / 1000
					}
					return append(enh.Out, promql.Sample{F: value}), nil
				}
				if name == "idelta" {
					// The difference of the last two running totals is the
					// last point's own value. Reading it from the totals, not
					// from the matrix, keeps a lone point from returning the
					// total since the start of the request.
					value, ok := meta.deltaLastValue(start, end)
					if !ok {
						return enh.Out, nil
					}
					return append(enh.Out, promql.Sample{F: value}), nil
				}
				// irate reads the running total, as it reads the cumulative
				// equivalent of the series.
				if len(points) > 0 {
					points = meta.deltaTotalPoints(points[0].T, end)
					first = sort.Search(len(points), func(i int) bool { return points[i].T > start })
				}
			}
		}
		if name == "default_rollup" {
			if len(series.Floats)+len(series.Histograms) == 0 || endsWithStaleMarker(series) {
				return enh.Out, nil
			}
			return histogramFallback(nil, promql.Matrix{series}, args[:1], enh)
		}
		if len(series.Histograms) > 0 {
			// Native histograms retain the existing Prometheus calculations.
			copySelector := *ms
			copySelector.Range = time.Duration(window) * time.Millisecond
			if ahead > 0 {
				// Prometheus reads the window end from the selector offset.
				original := *vs
				original.Offset += time.Duration(ahead) * time.Millisecond
				copySelector.VectorSelector = &original
			}
			return histogramFallback(nil, promql.Matrix{series}, parser.Expressions{&copySelector}, enh)
		}
		value := metricsQLCounterValue(name, points, first, start, maxPrev, lookback, next)
		if math.IsNaN(value) {
			return enh.Out, nil
		}
		return append(enh.Out, promql.Sample{F: value}), nil
	}
}

// metricsQLWithNextSample lets increase and delta in a range query see the
// sample after each window. VictoriaMetrics uses it to decide whether a
// series' first sample starts the counter (realNextValue in rollupDelta), and
// reads no sample after the query end. Each selector reads a lookback past
// the step, and a sixth argument holds the last time the next sample may have.
// ponytail: looks one lookback ahead; a series scraped less often keeps the
// old rule for its first sample.
func metricsQLWithNextSample(expr parser.Expr, end time.Time) bool {
	changed := false
	defaultRollup := metricsQLInternalPrefix + "default_rollup"
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		call, ok := node.(*parser.Call)
		if ok && call.Func == parser.Functions[defaultRollup] {
			// The caller prints expr, and this function's name is the engine name last_over_time.
			named := *call.Func
			named.Name = defaultRollup
			call.Func = &named
		}
		if !ok || len(call.Args) != 5 || (call.Func.Name != metricsQLInternalPrefix+"increase" && call.Func.Name != metricsQLInternalPrefix+"delta") {
			return nil
		}
		matrix, ok := call.Args[0].(*parser.MatrixSelector)
		if !ok {
			return nil
		}
		vs, ok := matrix.VectorSelector.(*parser.VectorSelector)
		lookback, isLookback := call.Args[3].(*parser.NumberLiteral)
		instant, isInstant := call.Args[4].(*parser.NumberLiteral)
		if !ok || !isLookback || !isInstant || instant.Val != 0 || vs.Timestamp != nil || vs.StartOrEnd != 0 {
			return nil
		}
		last := end.UnixMilli() - vs.OriginalOffset.Milliseconds()
		for _, parent := range path {
			if subquery, ok := parent.(*parser.SubqueryExpr); ok {
				if subquery.Timestamp != nil || subquery.StartOrEnd != 0 {
					return nil
				}
				last -= subquery.OriginalOffset.Milliseconds()
			}
		}
		ahead := time.Duration(lookback.Val) * time.Millisecond
		vs.OriginalOffset -= ahead
		matrix.Range += ahead
		call.Args = append(call.Args, &parser.NumberLiteral{Val: float64(last)})
		changed = true
		return nil
	})
	return changed
}

// seriesSplit splits series into the points at or before end and those after.
func seriesSplit(series promql.Series, end int64) (promql.Series, promql.Series) {
	after := series
	floats := sort.Search(len(series.Floats), func(i int) bool { return series.Floats[i].T > end })
	histograms := sort.Search(len(series.Histograms), func(i int) bool { return series.Histograms[i].T > end })
	series.Floats, after.Floats = series.Floats[:floats], series.Floats[floats:]
	series.Histograms, after.Histograms = series.Histograms[:histograms], series.Histograms[histograms:]
	return series, after
}

// seriesAfter keeps the points after start. Range windows are left-open.
func seriesAfter(series promql.Series, start int64) promql.Series {
	floats := series.Floats
	series.Floats = floats[sort.Search(len(floats), func(i int) bool { return floats[i].T > start }):]
	histograms := series.Histograms
	series.Histograms = histograms[sort.Search(len(histograms), func(i int) bool { return histograms[i].T > start }):]
	return series
}

// endsWithStaleMarker reports whether the latest point is a stale marker,
// which the storage adapter delivers as NaN.
func endsWithStaleMarker(series promql.Series) bool {
	if len(series.Floats) == 0 {
		return false
	}
	last := series.Floats[len(series.Floats)-1]
	if !math.IsNaN(last.F) {
		return false
	}
	return len(series.Histograms) == 0 || series.Histograms[len(series.Histograms)-1].T < last.T
}

// seriesWithoutNaN removes NaN points. It copies the points only when needed
// because the input can alias an engine buffer.
func seriesWithoutNaN(series promql.Series) promql.Series {
	first := slices.IndexFunc(series.Floats, func(p promql.FPoint) bool { return math.IsNaN(p.F) })
	if first < 0 {
		return series
	}
	points := make([]promql.FPoint, 0, len(series.Floats)-1)
	points = append(points, series.Floats[:first]...)
	for _, p := range series.Floats[first+1:] {
		if !math.IsNaN(p.F) {
			points = append(points, p)
		}
	}
	series.Floats = points
	return series
}

// VictoriaMetrics estimates the sample interval with the 0.6 quantile of the
// last 20 intervals, then adds a margin for timestamp variation.
// Reference: VictoriaMetrics v1.152.0, app/vmselect/promql/rollup.go.
func metricsQLSampleInterval(points []promql.FPoint, fallback int64) int64 {
	if len(points) < 2 {
		return fallback
	}
	var buf [20]int64
	intervals := buf[:0]
	for i := max(1, len(points)-20); i < len(points); i++ {
		intervals = append(intervals, points[i].T-points[i-1].T)
	}
	slices.Sort(intervals)
	position := 0.6 * float64(len(intervals)-1)
	index := int(position)
	interval := intervals[index]
	if index+1 < len(intervals) {
		interval += int64(float64(intervals[index+1]-interval) * (position - float64(index)))
	}
	if interval <= 0 {
		return fallback
	}
	switch {
	case interval <= 2000:
		return interval * 5
	case interval <= 4000:
		return interval * 3
	case interval <= 8000:
		return interval * 2
	case interval <= 16000:
		return interval + interval/2
	case interval <= 32000:
		return interval + interval/4
	default:
		return interval + interval/8
	}
}

// next is the series' first sample after the window, or NaN.
func metricsQLCounterValue(name string, points []promql.FPoint, first int, start, maxPrev, lookback int64, next float64) float64 {
	prev := first - 1
	if name == "increase_prometheus" {
		// Prometheus ignores the sample before the window.
		prev = -1
	}
	havePrev := prev >= 0 && points[prev].T > start-maxPrev
	isRate := name == "rate" || name == "irate"
	if first == len(points) {
		return math.NaN()
	}
	isIncrease := name == "increase" || name == "increase_pure" || name == "increase_prometheus"
	if !havePrev && (isIncrease || name == "delta") && prev >= 0 && points[first].T-points[prev].T < lookback {
		havePrev = true
	}
	begin := first
	baseline := points[first].F
	if havePrev {
		begin = prev
		baseline = points[prev].F
	} else if name == "increase_pure" {
		baseline = 0
	} else if name == "increase_prometheus" {
		if first+1 == len(points) {
			return math.NaN()
		}
	} else if !isRate {
		var delta float64
		if first+1 < len(points) {
			delta = points[first+1].F - points[first].F
		} else if !math.IsNaN(next) {
			delta = next - points[first].F
		}
		if math.Abs(baseline) < 10*(math.Abs(delta)+1) {
			baseline = 0
		}
	}
	if name == "irate" || name == "idelta" {
		begin = max(begin, len(points)-2)
		baseline = points[begin].F
		if name == "idelta" && len(points)-begin == 1 {
			baseline = 0
		}
	}
	if isRate && len(points)-begin < 2 {
		return math.NaN()
	}
	value := points[len(points)-1].F - baseline
	if isIncrease || isRate {
		previous := baseline
		for _, point := range points[begin:] {
			if point.F < previous {
				// MetricsQL treats small drops as partial counter resets.
				if (previous-point.F)*8 < previous {
					value += previous - point.F
				} else {
					value += previous
				}
			}
			previous = point.F
		}
	}
	if isRate {
		seconds := float64(points[len(points)-1].T-points[begin].T) / 1000
		if seconds <= 0 {
			return math.NaN()
		}
		value /= seconds
	}
	return value
}

const metricsQLDropNaNName = metricsQLInternalPrefix + "drop_nan"

func init() {
	parser.Functions[metricsQLDropNaNName] = &parser.Function{Name: metricsQLDropNaNName, ArgTypes: []parser.ValueType{parser.ValueTypeVector}, ReturnType: parser.ValueTypeVector}
	promql.FunctionCalls[metricsQLDropNaNName] = func(vectors []promql.Vector, _ promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		for _, sample := range vectors[0] {
			if sample.H != nil || !math.IsNaN(sample.F) {
				enh.Out = append(enh.Out, sample)
			}
		}
		return enh.Out, nil
	}
	// MetricsQL rollups return no value where Prometheus returns NaN. Dropping it
	// inside the rollup spares the aggregation operand another pass over its
	// whole matrix, which would double the sample count the engine holds.
	for name, function := range parser.Functions {
		if strings.HasPrefix(name, metricsQLInternalPrefix) || name == metricsQLDefaultRollupName || function.ReturnType != parser.ValueTypeVector || !slices.Contains(function.ArgTypes, parser.ValueTypeMatrix) {
			continue
		}
		if call := promql.FunctionCalls[name]; call != nil {
			promql.FunctionCalls[name] = metricsQLRollupWithoutNaN(call)
			metricsQLNaNFreeRollups[name] = true
		}
	}
}

// metricsQLNaNFreeRollups are the matrix-argument functions that return no
// sample where the result would be NaN. The engine parser leaves them unwrapped.
var metricsQLNaNFreeRollups = map[string]bool{}

func metricsQLRollupWithoutNaN(call promql.FunctionCall) promql.FunctionCall {
	return func(vectors []promql.Vector, matrix promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		out, warnings := call(vectors, matrix, args, enh)
		kept := out[:0]
		for _, sample := range out {
			if sample.H != nil || !math.IsNaN(sample.F) {
				kept = append(kept, sample)
			}
		}
		return kept, warnings
	}
}

// metricsQLEngineParser parses queries for the engine. VictoriaMetrics treats
// NaN as no value: aggregations skip it, `or` and union fill it from a later
// operand, `and` does not match it and absent counts it as absent. Prometheus
// keeps NaN, so one NaN series makes avg or sum NaN, which the response drops.
// The parser wraps those operands in drop_nan. The fast paths parse with the
// inner parser, so they still recognise their shapes.
type metricsQLEngineParser struct {
	parser.Parser
}

func (p metricsQLEngineParser) ParseExpr(input string) (parser.Expr, error) {
	expr, err := p.Parser.ParseExpr(input)
	if err != nil {
		return nil, err
	}
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch n := node.(type) {
		case *parser.AggregateExpr:
			n.Expr = metricsQLDropNaN(n.Expr)
		case *parser.BinaryExpr:
			if n.Op.IsSetOperator() {
				n.LHS, n.RHS = metricsQLDropNaN(n.LHS), metricsQLDropNaN(n.RHS)
			}
		case *parser.Call:
			if n.Func.Name == "absent" || n.Func.Name == metricsQLUnionName {
				for index, arg := range n.Args {
					n.Args[index] = metricsQLDropNaN(arg)
				}
			}
		}
		return nil
	})
	return expr, nil
}

// metricsQLDropNaN wraps expr in drop_nan unless it cannot return NaN. The
// MetricsQL rollups drop NaN values, and leaving them unwrapped keeps
// sum(rate(...)) and avg(deriv(...)) free of another pass. Selectors stay bare because absent
// reads its labels from them.
func metricsQLDropNaN(expr parser.Expr) parser.Expr {
	switch e := unparenExpr(expr).(type) {
	case *parser.VectorSelector, *parser.NumberLiteral:
		return expr
	case *parser.Call:
		name, internal := strings.CutPrefix(e.Func.Name, metricsQLInternalPrefix)
		if e.Func.Name == metricsQLDefaultRollupName || metricsQLNaNFreeRollups[e.Func.Name] || (internal && (slices.Contains(metricsQLCounterFunctions, name) || metricsQLPureCounterFunctions[name] != "")) {
			return expr
		}
	}
	return &parser.Call{Func: parser.Functions[metricsQLDropNaNName], Args: parser.Expressions{expr}, PosRange: expr.PositionRange()}
}

// MetricsQL returns numeric constants as vectors and removes NaN output points.
// The result never aliases the input slices: the engine returns the input
// matrix to its buffer pools when the query closes.
func metricsQLValue(value parser.Value) parser.Value {
	switch v := value.(type) {
	case promql.Scalar:
		if math.IsNaN(v.V) {
			return promql.Vector{}
		}
		return promql.Vector{{T: v.T, F: v.V}}
	case promql.Vector:
		out := make(promql.Vector, 0, len(v))
		for _, p := range v {
			if p.H != nil || !math.IsNaN(p.F) {
				out = append(out, p)
			}
		}
		return out
	case promql.Matrix:
		out := make(promql.Matrix, 0, len(v))
		for _, series := range v {
			series = seriesWithoutNaN(series)
			if len(series.Floats)+len(series.Histograms) > 0 {
				out = append(out, series)
			}
		}
		return out
	default:
		return value
	}
}

// metricsQLRunningSum replaces each float point with the sum of the points
// at or before it. Histogram points are not summed and are dropped.
func metricsQLRunningSum(value parser.Value) parser.Value {
	matrix, ok := value.(promql.Matrix)
	if !ok {
		return value
	}
	out := make(promql.Matrix, 0, len(matrix))
	for _, series := range matrix {
		if len(series.Floats) == 0 {
			continue
		}
		points := make([]promql.FPoint, len(series.Floats))
		var sum float64
		for i, p := range series.Floats {
			sum += p.F
			points[i] = promql.FPoint{T: p.T, F: sum}
		}
		out = append(out, promql.Series{Metric: series.Metric, Floats: points, DropName: series.DropName})
	}
	return out
}

// Raw selectors are rollup inputs. Selectors in vector expressions use
// default_rollup, including selectors inside aggregates and binary operations.
func metricsQLDefaultSelectors(expr metricsql.Expr) metricsql.Expr {
	switch e := expr.(type) {
	case *metricsql.MetricExpr:
		return &metricsql.FuncExpr{Name: "default_rollup", Args: []metricsql.Expr{e}}
	case *metricsql.RollupExpr:
		if number, ok := e.Expr.(*metricsql.NumberExpr); ok && e.Window == nil && !e.ForSubquery() {
			// VictoriaMetrics binds `60 offset 7d` to the number and leaves
			// it unchanged. Prometheus rejects offset on a number.
			return number
		}
		if _, raw := e.Expr.(*metricsql.MetricExpr); !raw {
			e.Expr = metricsQLDefaultSelectors(e.Expr)
		}
		if e.Window == nil && !e.ForSubquery() {
			return &metricsql.FuncExpr{Name: "default_rollup", Args: []metricsql.Expr{e}}
		}
	case *metricsql.FuncExpr:
		if !metricsql.IsRollupFunc(e.Name) && !metricsQLSelectorFunctions[e.Name] {
			for i, arg := range e.Args {
				if !prometheusMatrixArg(e.Name, i) {
					arg = metricsQLImplicitRollup(arg)
				}
				e.Args[i] = metricsQLDefaultSelectors(arg)
			}
			return expr
		}
		for i, arg := range e.Args {
			switch a := arg.(type) {
			case *metricsql.MetricExpr:
			case *metricsql.RollupExpr:
				if _, raw := a.Expr.(*metricsql.MetricExpr); !raw {
					if metricsQLSelectorFunctions[e.Name] {
						e.Args[i] = metricsQLDefaultSelectors(a)
					} else {
						a.Expr = metricsQLDefaultSelectors(a.Expr)
					}
				}
			default:
				e.Args[i] = metricsQLDefaultSelectors(arg)
			}
		}
	case *metricsql.AggrFuncExpr:
		for i, arg := range e.Args {
			e.Args[i] = metricsQLDefaultSelectors(metricsQLImplicitRollup(arg))
		}
	case *metricsql.BinaryOpExpr:
		e.Left = metricsQLDefaultSelectors(metricsQLImplicitRollup(e.Left))
		e.Right = metricsQLDefaultSelectors(metricsQLImplicitRollup(e.Right))
	}
	return expr
}

// metricsQLImplicitRollup wraps a range selector or subquery that is not the
// argument of a rollup function in default_rollup, as VictoriaMetrics does:
// sum(m[5m]) is sum(default_rollup(m[5m])), the last sample in the window.
func metricsQLImplicitRollup(expr metricsql.Expr) metricsql.Expr {
	if e, ok := expr.(*metricsql.RollupExpr); ok && e.Window != nil {
		return &metricsql.FuncExpr{Name: "default_rollup", Args: []metricsql.Expr{e}}
	}
	return expr
}

// prometheusMatrixArg reports whether a Prometheus function that MetricsQL
// does not know as a rollup takes a range vector at the argument index.
func prometheusMatrixArg(name string, index int) bool {
	function := parser.Functions[name]
	if function == nil || len(function.ArgTypes) == 0 {
		return false
	}
	index = min(index, len(function.ArgTypes)-1)
	return function.ArgTypes[index] == parser.ValueTypeMatrix
}

// unwrapInstantDefaultRollups replaces the internal default_rollup calls of an
// instant query with their selectors. Both read the latest non-stale sample in
// the lookback window, so the SQL fast paths stay exact. Other internal
// functions need the evaluation engine, so the expression is rejected.
func unwrapInstantDefaultRollups(expr parser.Expr) (parser.Expr, bool) {
	switch e := expr.(type) {
	case *parser.Call:
		if e.Func != nil && e.Func.Name == metricsQLDefaultRollupName && len(e.Args) == 5 {
			matrix, ok := e.Args[0].(*parser.MatrixSelector)
			if !ok {
				return nil, false
			}
			selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
			instant, isNumber := e.Args[4].(*parser.NumberLiteral)
			if !ok || !isNumber || instant.Val != 1 {
				return nil, false
			}
			// An explicit window such as sum(m[1h]) is not the lookback.
			if window, isNumber := e.Args[1].(*parser.NumberLiteral); !isNumber || window.Val != 0 {
				return nil, false
			}
			return selector, true
		}
		if e.Func != nil && strings.HasPrefix(e.Func.Name, metricsQLInternalPrefix) {
			return nil, false
		}
		for i, arg := range e.Args {
			unwrapped, ok := unwrapInstantDefaultRollups(arg)
			if !ok {
				return nil, false
			}
			e.Args[i] = unwrapped
		}
	case *parser.AggregateExpr:
		inner, ok := unwrapInstantDefaultRollups(e.Expr)
		if !ok {
			return nil, false
		}
		e.Expr = inner
		if e.Param != nil {
			param, ok := unwrapInstantDefaultRollups(e.Param)
			if !ok {
				return nil, false
			}
			e.Param = param
		}
	case *parser.BinaryExpr:
		lhs, ok := unwrapInstantDefaultRollups(e.LHS)
		if !ok {
			return nil, false
		}
		rhs, ok := unwrapInstantDefaultRollups(e.RHS)
		if !ok {
			return nil, false
		}
		e.LHS, e.RHS = lhs, rhs
	case *parser.ParenExpr:
		inner, ok := unwrapInstantDefaultRollups(e.Expr)
		if !ok {
			return nil, false
		}
		e.Expr = inner
	case *parser.UnaryExpr:
		inner, ok := unwrapInstantDefaultRollups(e.Expr)
		if !ok {
			return nil, false
		}
		e.Expr = inner
	case *parser.SubqueryExpr:
		inner, ok := unwrapInstantDefaultRollups(e.Expr)
		if !ok {
			return nil, false
		}
		e.Expr = inner
	case *parser.StepInvariantExpr:
		inner, ok := unwrapInstantDefaultRollups(e.Expr)
		if !ok {
			return nil, false
		}
		e.Expr = inner
	}
	return expr, true
}
