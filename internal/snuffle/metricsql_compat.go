package snuffle

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metricsql"
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

var metricsQLCounterFunctions = []string{"increase", "rate", "delta", "irate", "idelta"}

// metricsQLSelectorFunctions inspect their selector argument in the engine.
// Their selector arguments keep the Prometheus lookback.
var metricsQLSelectorFunctions = map[string]bool{"absent": true, "timestamp": true}

func init() {
	rollupArgTypes := []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeScalar, parser.ValueTypeScalar, parser.ValueTypeScalar, parser.ValueTypeScalar}
	for _, name := range metricsQLCounterFunctions {
		internalName := metricsQLInternalPrefix + name
		parser.Functions[internalName] = &parser.Function{Name: internalName, ArgTypes: rollupArgTypes, ReturnType: parser.ValueTypeVector}
		promql.FunctionCalls[internalName] = metricsQLRollup(name, promql.FunctionCalls[name])
	}
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
}

// prepareMetricsQLQuery expands MetricsQL syntax before either execution path.
// Counter functions use separate names so Prometheus SQL optimizations cannot
// replace their calculations with extrapolated PromQL results.
func prepareMetricsQLQuery(query string, step, lookback time.Duration, start, end time.Time) (metricsQLQuery, error) {
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
	prepared.query = string(expr.AppendString(nil))
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
		return r.rewrite(&e.Expr, childStep, instant && e.Step == nil)
	case *metricsql.FuncExpr:
		return r.rewriteFunc(e, step, instant)
	case *metricsql.AggrFuncExpr:
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
		if err := r.rewrite(&e.Left, step, instant); err != nil {
			return err
		}
		return r.rewrite(&e.Right, step, instant)
	}
	return nil
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
	if err := r.rewrite(&roll.Expr, childStep, false); err != nil {
		return err
	}
	if e.Name != "default_rollup" && !slices.Contains(metricsQLCounterFunctions, e.Name) {
		return nil
	}
	if len(e.Args) != 1 || e.KeepMetricNames {
		return fmt.Errorf("%s requires one series argument; keep_metric_names is not supported", e.Name)
	}
	if window > time.Duration(math.MaxInt64)-r.lookback {
		return fmt.Errorf("the query window is too large")
	}
	switch {
	case e.Name == "default_rollup" && instant && implicit:
		// A bare selector in an instant query reads only the lookback window.
		roll.Window = metricsQLDuration(r.lookback)
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
	if err := r.rewrite(&inner, step, false); err != nil {
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
		points := matrix[0].Floats
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
		end := enh.Ts - vs.Offset.Milliseconds()
		if vs.Timestamp != nil {
			end = *vs.Timestamp - vs.OriginalOffset.Milliseconds()
		}
		start := end - window
		series := seriesAfter(matrix[0], start)
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
			return histogramFallback(nil, promql.Matrix{series}, parser.Expressions{&copySelector}, enh)
		}
		first := len(points) - len(series.Floats)
		value := metricsQLCounterValue(name, points, first, start, maxPrev, lookback)
		if math.IsNaN(value) {
			return enh.Out, nil
		}
		return append(enh.Out, promql.Sample{F: value}), nil
	}
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

func metricsQLCounterValue(name string, points []promql.FPoint, first int, start, maxPrev, lookback int64) float64 {
	prev := first - 1
	havePrev := prev >= 0 && points[prev].T > start-maxPrev
	isRate := name == "rate" || name == "irate"
	if first == len(points) {
		return math.NaN()
	}
	if !havePrev && (name == "increase" || name == "delta") && prev >= 0 && points[first].T-points[prev].T < lookback {
		havePrev = true
	}
	begin := first
	baseline := points[first].F
	if havePrev {
		begin = prev
		baseline = points[prev].F
	} else if !isRate {
		var delta float64
		if first+1 < len(points) {
			delta = points[first+1].F - points[first].F
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
	if name == "increase" || isRate {
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
		if _, ok := e.Expr.(*metricsql.MetricExpr); ok {
			if e.Window == nil && !e.ForSubquery() {
				return &metricsql.FuncExpr{Name: "default_rollup", Args: []metricsql.Expr{e}}
			}
		} else {
			e.Expr = metricsQLDefaultSelectors(e.Expr)
		}
	case *metricsql.FuncExpr:
		if !metricsql.IsRollupFunc(e.Name) && !metricsQLSelectorFunctions[e.Name] {
			for i, arg := range e.Args {
				e.Args[i] = metricsQLDefaultSelectors(arg)
			}
			return expr
		}
		for i, arg := range e.Args {
			switch a := arg.(type) {
			case *metricsql.MetricExpr:
			case *metricsql.RollupExpr:
				if _, raw := a.Expr.(*metricsql.MetricExpr); !raw {
					a.Expr = metricsQLDefaultSelectors(a.Expr)
				}
			default:
				e.Args[i] = metricsQLDefaultSelectors(arg)
			}
		}
	case *metricsql.AggrFuncExpr:
		for i, arg := range e.Args {
			e.Args[i] = metricsQLDefaultSelectors(arg)
		}
	case *metricsql.BinaryOpExpr:
		e.Left = metricsQLDefaultSelectors(e.Left)
		e.Right = metricsQLDefaultSelectors(e.Right)
	}
	return expr
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
