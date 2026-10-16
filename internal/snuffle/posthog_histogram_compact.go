package snuffle

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

var errCompactHistogramFallback = errors.New("histogram requires the general evaluation path")

// compactHistogramInstantPoints is the number of points that irate and idelta
// need for each bucket: they read the last two points, and
// metricsQLSampleInterval reads the last 21 timestamps.
const compactHistogramInstantPoints = 21

type compactHistogramPlan struct {
	call                                              *parser.Call
	selector                                          *parser.VectorSelector
	alias                                             postHogHistogramAlias
	function                                          string
	bucketArg                                         int
	start, end, step, window, lookback, selectorRange int64
	steps, maxPoints                                  int
}

func unparenExpr(expr parser.Expr) parser.Expr {
	for {
		paren, ok := expr.(*parser.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

func planCompactHistogram(expr parser.Expr, start, end time.Time, step, lookback time.Duration) *compactHistogramPlan {
	call, ok := unparenExpr(expr).(*parser.Call)
	if !ok {
		return nil
	}
	plan := &compactHistogramPlan{call: call, start: start.UnixMilli(), end: end.UnixMilli(), step: step.Milliseconds(), lookback: lookback.Milliseconds()}
	switch call.Func.Name {
	case metricsQLInternalPrefix + "histogram_quantiles":
		if len(call.Args) < 3 {
			return nil
		}
		if _, ok := call.Args[1].(*parser.StringLiteral); !ok {
			return nil
		}
	case "histogram_quantile":
		if len(call.Args) != 2 {
			return nil
		}
		plan.bucketArg = 1
	default:
		return nil
	}
	quantiles := make(map[float64]bool)
	for index, arg := range call.Args {
		if index == plan.bucketArg || (plan.bucketArg == 0 && index == 1) {
			continue
		}
		number, ok := unparenExpr(arg).(*parser.NumberLiteral)
		if !ok || math.IsNaN(number.Val) || math.IsInf(number.Val, 0) || quantiles[number.Val] {
			return nil
		}
		quantiles[number.Val] = true
	}
	aggregate, ok := unparenExpr(call.Args[plan.bucketArg]).(*parser.AggregateExpr)
	if !ok || aggregate.Op != parser.SUM || aggregate.Without || len(aggregate.Grouping) != 1 || aggregate.Grouping[0] != "le" {
		return nil
	}
	rate, ok := unparenExpr(aggregate.Expr).(*parser.Call)
	if !ok || len(rate.Args) != 5 {
		return nil
	}
	plan.function = strings.TrimPrefix(rate.Func.Name, metricsQLInternalPrefix)
	if plan.function == rate.Func.Name || !slices.Contains(metricsQLCounterFunctions, plan.function) {
		return nil
	}
	if plan.function == "irate" || plan.function == "idelta" {
		plan.maxPoints = compactHistogramInstantPoints
	}
	var numbers [4]int64
	for index, arg := range rate.Args[1:] {
		number, ok := arg.(*parser.NumberLiteral)
		if !ok || math.IsNaN(number.Val) || number.Val < 0 || number.Val >= float64(math.MaxInt64) || number.Val != math.Trunc(number.Val) {
			return nil
		}
		numbers[index] = int64(number.Val)
	}
	// A zero window is an implicit rate window: the rewrite fetches one step
	// of history before the lookback, and each step resolves the window from
	// the sample interval.
	plan.window = numbers[0]
	if numbers[1] != plan.step || numbers[2] != plan.lookback || numbers[3] != 0 || plan.step <= 0 || plan.lookback <= 0 {
		return nil
	}
	fetched := plan.window
	if fetched == 0 {
		fetched = plan.step
	}
	if fetched > math.MaxInt64-plan.lookback || plan.start < math.MinInt64+fetched+plan.lookback {
		return nil
	}
	plan.selectorRange = fetched + plan.lookback
	matrix, ok := rate.Args[0].(*parser.MatrixSelector)
	if !ok || matrix.Range.Milliseconds() != plan.selectorRange {
		return nil
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok || selector.OriginalOffset != 0 || selector.Timestamp != nil || selector.StartOrEnd != 0 {
		return nil
	}
	alias, ok := postHogExactHistogramAlias(selector.LabelMatchers)
	if !ok || alias.suffix != "_bucket" {
		return nil
	}
	for _, matcher := range selector.LabelMatchers {
		if matcher.Name == "le" {
			return nil
		}
	}
	span := plan.end - plan.start
	if span < 0 || span/plan.step >= 100000 {
		return nil
	}
	plan.steps = int(span/plan.step) + 1
	plan.selector, plan.alias = selector, alias
	return plan
}

type compactHistogramSum struct {
	value, correction float64
	present           bool
}

func (sum *compactHistogramSum) add(value float64) {
	next := sum.value + value
	if math.Abs(sum.value) >= math.Abs(value) {
		sum.correction += (sum.value - next) + value
	} else {
		sum.correction += (value - next) + sum.value
	}
	sum.value, sum.present = next, true
}

// compactHistogramEvaluator reads the samples of one source series at a time.
// For each bucket it keeps the points inside the selector range, which are
// the points that the engine passes to the counter function.
type compactHistogramEvaluator struct {
	plan                  *compactHistogramPlan
	cfg                   Config
	buckets               map[string][]compactHistogramSum
	columns               [][]compactHistogramSum
	bounds                []float64
	current               []float64
	history               [][]promql.FPoint
	sourceID              uint64
	haveSource            bool
	grid, samples, series int
	output                int
}

func newCompactHistogramEvaluator(plan *compactHistogramPlan, cfg Config) *compactHistogramEvaluator {
	return &compactHistogramEvaluator{plan: plan, cfg: cfg, buckets: make(map[string][]compactHistogramSum)}
}

func (evaluator *compactHistogramEvaluator) advance(ctx context.Context, until int64, finish bool) error {
	plan := evaluator.plan
	for evaluator.grid < plan.steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		timestamp := plan.start + int64(evaluator.grid)*plan.step
		if !finish && timestamp >= until {
			break
		}
		if len(evaluator.history) > 0 {
			// Later steps start later, so points before the range are not
			// needed again. Shift in place to keep the capacity for appends.
			times := evaluator.history[0]
			first := sort.Search(len(times), func(index int) bool { return times[index].T > timestamp-plan.selectorRange })
			// irate and idelta read only the newest points. Trim them when the
			// history reaches twice the limit, so that the copy runs rarely.
			if plan.maxPoints > 0 && len(times)-first > 2*plan.maxPoints {
				first = len(times) - plan.maxPoints
			}
			if first > 0 {
				for index, points := range evaluator.history {
					evaluator.history[index] = points[:copy(points, points[first:])]
				}
			}
		}
		if len(evaluator.history) > 0 && len(evaluator.history[0]) > 0 {
			times := evaluator.history[0]
			maxPrevious := min(metricsQLSampleInterval(times, plan.step), plan.lookback)
			window := plan.window
			if window == 0 {
				window = max(plan.step, maxPrevious)
			}
			start := timestamp - window
			firstPoint := sort.Search(len(times), func(index int) bool { return times[index].T > start })
			for index, column := range evaluator.columns {
				value := metricsQLCounterValue(plan.function, evaluator.history[index], firstPoint, start, maxPrevious, plan.lookback)
				if !math.IsNaN(value) {
					column[evaluator.grid].add(value)
				}
			}
		}
		evaluator.grid++
	}
	return nil
}

func (evaluator *compactHistogramEvaluator) add(ctx context.Context, sample postHogHistogramSample) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if evaluator.haveSource && evaluator.sourceID != sample.id {
		if err := evaluator.advance(ctx, 0, true); err != nil {
			return err
		}
		evaluator.haveSource = false
	}
	if sample.temporality != "cumulative" {
		return errCompactHistogramFallback
	}
	if len(sample.counts) != len(sample.bounds)+1 {
		return errCompactHistogramFallback
	}
	if evaluator.cfg.MaxSamples > 0 && len(sample.counts) > evaluator.cfg.MaxSamples-evaluator.samples {
		return fmt.Errorf("histogram sample limit exceeded (%d); shorten the time range or tighten matchers", evaluator.cfg.MaxSamples)
	}
	evaluator.samples += len(sample.counts)
	if !evaluator.haveSource {
		if len(sample.counts) > evaluator.cfg.MaxSeries-evaluator.series {
			return fmt.Errorf("histogram series limit exceeded (%d); tighten matchers", evaluator.cfg.MaxSeries)
		}
		evaluator.series += len(sample.counts)
		evaluator.bounds = append(evaluator.bounds[:0], sample.bounds...)
		evaluator.current = make([]float64, len(sample.counts))
		evaluator.history = slices.Grow(evaluator.history[:0], len(sample.counts))[:len(sample.counts)]
		for index := range evaluator.history {
			evaluator.history[index] = evaluator.history[index][:0]
		}
		evaluator.columns = evaluator.columns[:0]
		for index := range sample.counts {
			bound := "+Inf"
			if index < len(sample.bounds) {
				bound = strconv.FormatFloat(sample.bounds[index], 'g', -1, 64)
			}
			column, exists := evaluator.buckets[bound]
			if !exists {
				if evaluator.cfg.MaxSamples > 0 && (len(evaluator.buckets)+1) > evaluator.cfg.MaxSamples/evaluator.plan.steps {
					return errCompactHistogramFallback
				}
				column = make([]compactHistogramSum, evaluator.plan.steps)
				evaluator.buckets[bound] = column
			}
			evaluator.columns = append(evaluator.columns, column)
		}
		evaluator.grid, evaluator.sourceID, evaluator.haveSource = 0, sample.id, true
	} else if !slices.EqualFunc(evaluator.bounds, sample.bounds, func(left, right float64) bool { return math.Float64bits(left) == math.Float64bits(right) }) {
		return errCompactHistogramFallback
	}
	if err := evaluator.advance(ctx, sample.timestamp, false); err != nil {
		return err
	}
	times := evaluator.history[0]
	duplicate := len(times) > 0 && times[len(times)-1].T == sample.timestamp
	if len(times) > 0 && times[len(times)-1].T > sample.timestamp {
		return errCompactHistogramFallback
	}
	var cumulative uint64
	for index, count := range sample.counts {
		if count > math.MaxUint64-cumulative {
			return errCompactHistogramFallback
		}
		cumulative += count
		if index < len(sample.bounds) {
			bound := sample.bounds[index]
			if math.IsNaN(bound) || math.IsInf(bound, 0) || (index > 0 && bound <= sample.bounds[index-1]) {
				return errCompactHistogramFallback
			}
		}
		if duplicate && evaluator.current[index] != float64(cumulative) {
			return errCompactHistogramFallback
		}
		evaluator.current[index] = float64(cumulative)
	}
	if cumulative != sample.count {
		return errCompactHistogramFallback
	}
	for index, value := range evaluator.current {
		evaluator.history[index] = append(evaluator.history[index], promql.FPoint{T: sample.timestamp, F: value})
	}
	return nil
}

func (evaluator *compactHistogramEvaluator) result(ctx context.Context) (promql.Matrix, error) {
	if err := evaluator.advance(ctx, 0, true); err != nil {
		return nil, err
	}
	matrix, err := evaluator.evaluate(ctx, evaluator.plan)
	if err != nil {
		return nil, err
	}
	return dropCompactHistogramNaNs(matrix), nil
}

// evaluate applies the quantile call of plan to the summed buckets. Plans that
// share the bucket expression share the evaluator, so one read of the buckets
// serves several quantiles. The result keeps NaN points: in a union they still
// hide a later operand's series with the same labels, as in the engine.
func (evaluator *compactHistogramEvaluator) evaluate(ctx context.Context, plan *compactHistogramPlan) (promql.Matrix, error) {
	vectors := make([]promql.Vector, len(plan.call.Args))
	for index, arg := range plan.call.Args {
		if number, ok := unparenExpr(arg).(*parser.NumberLiteral); ok {
			vectors[index] = promql.Vector{{F: number.Val}}
		}
	}
	bounds := make([]string, 0, len(evaluator.buckets))
	for bound := range evaluator.buckets {
		bounds = append(bounds, bound)
	}
	sort.Strings(bounds)
	bucketLabels := make([]labels.Labels, len(bounds))
	for index, bound := range bounds {
		bucketLabels[index] = labels.FromStrings("le", bound)
	}
	byLabels := make(map[string]int)
	result := promql.Matrix{}
	helper := &promql.EvalNodeHelper{}
	for grid := 0; grid < plan.steps; grid++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vector := vectors[plan.bucketArg][:0]
		for index, bound := range bounds {
			sum := evaluator.buckets[bound][grid]
			if sum.present {
				vector = append(vector, promql.Sample{Metric: bucketLabels[index], F: sum.value + sum.correction})
			}
		}
		vectors[plan.bucketArg] = vector
		helper.Ts, helper.Out = plan.start+int64(grid)*plan.step, helper.Out[:0]
		values, _ := promql.FunctionCalls[plan.call.Func.Name](vectors, nil, plan.call.Args, helper)
		for _, sample := range values {
			key := sample.Metric.String()
			index, exists := byLabels[key]
			if !exists {
				index = len(result)
				byLabels[key] = index
				result = append(result, promql.Series{Metric: sample.Metric})
			}
			if !math.IsNaN(sample.F) {
				if evaluator.cfg.MaxSamples > 0 && evaluator.output >= evaluator.cfg.MaxSamples {
					return nil, errCompactHistogramFallback
				}
				evaluator.output++
			}
			result[index].Floats = append(result[index].Floats, promql.FPoint{T: helper.Ts, F: sample.F})
		}
	}
	return result, nil
}

func dropCompactHistogramNaNs(matrix promql.Matrix) promql.Matrix {
	result := matrix[:0]
	for _, series := range matrix {
		series.Floats = slices.DeleteFunc(series.Floats, func(point promql.FPoint) bool { return math.IsNaN(point.F) })
		if len(series.Floats) > 0 {
			result = append(result, series)
		}
	}
	sort.Sort(result)
	return result
}

// compactSeriesFunction is a function call around a compact quantile, applied
// to the quantile result.
type compactSeriesFunction func(promql.Matrix) promql.Matrix

// compactSeriesFunctions are the functions that can wrap a compact quantile.
// Each one reads every series on its own, so running it on the compact result
// gives the engine's result. A parser returns false for arguments that the
// engine rejects or that need the general path.
var compactSeriesFunctions = map[string]func(*parser.Call) (compactSeriesFunction, bool){
	"label_replace": parseCompactLabelReplace,
	"label_join":    parseCompactLabelJoin,
	"abs":           parseCompactValueFunction,
	"ceil":          parseCompactValueFunction,
	"floor":         parseCompactValueFunction,
	"round":         parseCompactValueFunction,
	"clamp":         parseCompactValueFunction,
	"clamp_min":     parseCompactValueFunction,
	"clamp_max":     parseCompactValueFunction,
	"sgn":           parseCompactValueFunction,
	"sqrt":          parseCompactValueFunction,
	"exp":           parseCompactValueFunction,
	"ln":            parseCompactValueFunction,
	"log2":          parseCompactValueFunction,
	"log10":         parseCompactValueFunction,
}

func compactStringArgs(args parser.Expressions) ([]string, bool) {
	values := make([]string, len(args))
	for index, arg := range args {
		literal, ok := unparenExpr(arg).(*parser.StringLiteral)
		if !ok {
			return nil, false
		}
		values[index] = literal.Val
	}
	return values, true
}

// compactLabelDestination reports whether a label function may write the
// label. A new metric name changes how the engine removes names.
func compactLabelDestination(name string) bool {
	return model.UTF8Validation.IsValidLabelName(name) && name != labels.MetricName
}

// parseCompactLabelReplace matches the engine's label_replace: the regex is
// anchored, and an empty result removes the label.
func parseCompactLabelReplace(call *parser.Call) (compactSeriesFunction, bool) {
	if len(call.Args) != 5 {
		return nil, false
	}
	values, ok := compactStringArgs(call.Args[1:])
	if !ok || !compactLabelDestination(values[0]) {
		return nil, false
	}
	destination, replacement, source := values[0], values[1], values[2]
	regex, err := regexp.Compile("^(?s:" + values[3] + ")$")
	if err != nil {
		return nil, false
	}
	return func(matrix promql.Matrix) promql.Matrix {
		for index := range matrix {
			value := matrix[index].Metric.Get(source)
			indexes := regex.FindStringSubmatchIndex(value)
			if indexes == nil {
				continue
			}
			builder := labels.NewBuilder(matrix[index].Metric)
			builder.Set(destination, string(regex.ExpandString(nil, replacement, value, indexes)))
			matrix[index].Metric = builder.Labels()
		}
		return matrix
	}, true
}

// parseCompactLabelJoin matches the engine's label_join.
func parseCompactLabelJoin(call *parser.Call) (compactSeriesFunction, bool) {
	if len(call.Args) < 3 {
		return nil, false
	}
	values, ok := compactStringArgs(call.Args[1:])
	if !ok || !compactLabelDestination(values[0]) {
		return nil, false
	}
	destination, separator, sources := values[0], values[1], values[2:]
	for _, source := range sources {
		if !model.UTF8Validation.IsValidLabelName(source) {
			return nil, false
		}
	}
	return func(matrix promql.Matrix) promql.Matrix {
		joined := make([]string, len(sources))
		for index := range matrix {
			for position, source := range sources {
				joined[position] = matrix[index].Metric.Get(source)
			}
			builder := labels.NewBuilder(matrix[index].Metric)
			builder.Set(destination, strings.Join(joined, separator))
			matrix[index].Metric = builder.Labels()
		}
		return matrix
	}, true
}

// parseCompactValueFunction accepts an element-wise function whose other
// arguments are numbers. It calls the engine's implementation for each point.
func parseCompactValueFunction(call *parser.Call) (compactSeriesFunction, bool) {
	function := promql.FunctionCalls[call.Func.Name]
	if function == nil || len(call.Args) == 0 {
		return nil, false
	}
	vectors := make([]promql.Vector, len(call.Args))
	for index, arg := range call.Args[1:] {
		number, ok := unparenExpr(arg).(*parser.NumberLiteral)
		if !ok {
			return nil, false
		}
		vectors[index+1] = promql.Vector{{F: number.Val}}
	}
	return func(matrix promql.Matrix) promql.Matrix {
		result := make(promql.Matrix, 0, len(matrix))
		helper := &promql.EvalNodeHelper{}
		for _, series := range matrix {
			output := promql.Series{Metric: series.Metric}
			for _, point := range series.Floats {
				vectors[0] = promql.Vector{{Metric: series.Metric, T: point.T, F: point.F}}
				helper.Ts, helper.Out = point.T, helper.Out[:0]
				values, _ := function(vectors, nil, call.Args, helper)
				for _, sample := range values {
					output.Metric = sample.Metric
					output.Floats = append(output.Floats, promql.FPoint{T: point.T, F: sample.F})
				}
			}
			if len(output.Floats) > 0 {
				result = append(result, output)
			}
		}
		return result
	}, true
}

// compactHistogramBranch is one operand of a union of quantiles: the quantile
// plan and the functions around it, innermost first.
type compactHistogramBranch struct {
	plan      *compactHistogramPlan
	functions []compactSeriesFunction
}

// planCompactHistogramUnion accepts a quantile call, functions from
// compactSeriesFunctions around it, and unions of those joined with a plain
// `or`. A dashboard that charts several quantiles, each with its own series
// label, sends this form.
func planCompactHistogramUnion(expr parser.Expr, start, end time.Time, step, lookback time.Duration) []compactHistogramBranch {
	var branches []compactHistogramBranch
	var collect func(parser.Expr) bool
	collect = func(expr parser.Expr) bool {
		expr = unparenExpr(expr)
		if binary, ok := expr.(*parser.BinaryExpr); ok {
			if binary.Op != parser.LOR || binary.VectorMatching == nil || binary.VectorMatching.On || len(binary.VectorMatching.MatchingLabels) != 0 {
				return false
			}
			return collect(binary.LHS) && collect(binary.RHS)
		}
		var functions []compactSeriesFunction
		for {
			call, ok := expr.(*parser.Call)
			if !ok {
				break
			}
			parse, ok := compactSeriesFunctions[call.Func.Name]
			if !ok {
				break
			}
			function, ok := parse(call)
			if !ok {
				return false
			}
			functions = append(functions, function)
			expr = unparenExpr(call.Args[0])
		}
		slices.Reverse(functions)
		plan := planCompactHistogram(expr, start, end, step, lookback)
		if plan == nil || len(branches) >= maxAggregateUnionSelectors {
			return false
		}
		branches = append(branches, compactHistogramBranch{plan: plan, functions: functions})
		return true
	}
	if !collect(expr) {
		return nil
	}
	return branches
}

// applyCompactSeriesFunctions applies the functions of a branch. When two
// series end with the same labels, the engine merges them or fails, so that
// case stays on the general path.
func applyCompactSeriesFunctions(matrix promql.Matrix, functions []compactSeriesFunction) (promql.Matrix, error) {
	for _, function := range functions {
		matrix = function(matrix)
		seen := make(map[string]bool, len(matrix))
		for _, series := range matrix {
			key := series.Metric.String()
			if seen[key] {
				return nil, errCompactHistogramFallback
			}
			seen[key] = true
		}
	}
	return matrix, nil
}

// compactHistogramUnion combines branch results with the semantics of `or`: at
// each step, a series is kept unless an earlier branch had a series with the
// same labels, ignoring the metric name, at that step.
type compactHistogramUnion struct {
	result   promql.Matrix
	byLabels map[string]int
	present  map[int64]map[string]bool
}

func (union *compactHistogramUnion) add(matrix promql.Matrix) {
	if union.byLabels == nil {
		union.byLabels, union.present = make(map[string]int), make(map[int64]map[string]bool)
	}
	type mark struct {
		timestamp int64
		signature string
	}
	var marks []mark
	for _, series := range matrix {
		signature := series.Metric.DropMetricName().String()
		key := series.Metric.String()
		for _, point := range series.Floats {
			if union.present[point.T][signature] {
				continue
			}
			marks = append(marks, mark{point.T, signature})
			if math.IsNaN(point.F) {
				continue
			}
			index, exists := union.byLabels[key]
			if !exists {
				index = len(union.result)
				union.byLabels[key] = index
				union.result = append(union.result, promql.Series{Metric: series.Metric})
			}
			union.result[index].Floats = append(union.result[index].Floats, point)
		}
	}
	for _, mark := range marks {
		if union.present[mark.timestamp] == nil {
			union.present[mark.timestamp] = make(map[string]bool)
		}
		union.present[mark.timestamp][mark.signature] = true
	}
}

func (union *compactHistogramUnion) matrix() promql.Matrix {
	for _, series := range union.result {
		slices.SortFunc(series.Floats, func(left, right promql.FPoint) int { return cmp.Compare(left.T, right.T) })
	}
	result := union.result
	if result == nil {
		result = promql.Matrix{}
	}
	sort.Sort(result)
	return result
}

func (server *Server) tryCompactHistogramRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (promql.Matrix, bool, error) {
	if !server.cfg.postHogSchemaLayout() || !server.cfg.PostHogCompactHistograms {
		return nil, false, nil
	}
	expr, err := server.parser.ParseExpr(query)
	if err != nil {
		return nil, false, nil
	}
	branches := planCompactHistogramUnion(expr, start, end, step, server.cfg.LookbackDelta)
	if branches == nil {
		return nil, false, nil
	}
	// Branches over the same buckets differ only in their quantiles, so each
	// distinct bucket expression is read once.
	evaluators := make(map[string]*compactHistogramEvaluator)
	var union compactHistogramUnion
	for _, branch := range branches {
		key := branch.plan.call.Args[branch.plan.bucketArg].String()
		evaluator, exists := evaluators[key]
		if !exists {
			var handled bool
			evaluator, handled, err = server.readCompactHistogram(ctx, branch.plan)
			if !handled || err != nil {
				return nil, handled, err
			}
			evaluators[key] = evaluator
		}
		matrix, err := evaluator.evaluate(ctx, branch.plan)
		if err == nil {
			matrix, err = applyCompactSeriesFunctions(matrix, branch.functions)
		}
		if errors.Is(err, errCompactHistogramFallback) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		union.add(matrix)
	}
	return union.matrix(), true, nil
}

// readCompactHistogram reads the buckets of plan and sums them on the step
// grid. It reports false when the data needs the general evaluation path.
func (server *Server) readCompactHistogram(ctx context.Context, plan *compactHistogramPlan) (*compactHistogramEvaluator, bool, error) {
	querier := &CHQuerier{queryable: server.queryable}
	mint := plan.start - plan.window - plan.lookback + 1
	real, sources, ids, err := querier.selectPostHogExactHistogramMetadata(ctx, mint, plan.end, plan.alias, plan.selector.LabelMatchers)
	if err != nil {
		return nil, true, err
	}
	if len(real) > 0 {
		return nil, false, nil
	}
	seen := make(map[string]bool)
	selected := make([]uint64, 0, len(ids))
	for _, id := range ids {
		source := sources[id]
		labelMap := postHogLabelMap(source.metricName, source.serviceName, source.resource, source.attributes)
		if !matchesAll(labelMap, postHogHistogramBaseMatchers(plan.selector.LabelMatchers)) {
			continue
		}
		delete(labelMap, "le")
		key := labels.FromMap(labelMap).String()
		if seen[key] {
			return nil, false, nil
		}
		seen[key] = true
		selected = append(selected, id)
	}
	evaluator := newCompactHistogramEvaluator(plan, server.cfg)
	var sample postHogHistogramSample
	err = queryPostHogSeriesIDs(ctx, server.client, server.cfg.IDChunkSize, selected, func(_ []uint64, idCondition string) string {
		return postHogHistogramSamplesWhereSQL(server.cfg, mint, plan.end, idCondition, []postHogHistogramAlias{plan.alias}, plan.selector.LabelMatchers)
	}, func(row clickHouseRow) error {
		if err := sample.scanPacked(row); err != nil {
			return err
		}
		return evaluator.add(ctx, sample)
	})
	if err == nil {
		err = evaluator.advance(ctx, 0, true)
	}
	if errors.Is(err, errCompactHistogramFallback) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	return evaluator, true, nil
}
