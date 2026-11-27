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

// errCompactHistogramSplit reports that a source changes its bucket layout or
// temporality inside the range. The streaming read always rejects such a
// source, so reading every row first only delays the next backend.
var errCompactHistogramSplit = errors.New("histogram source changes layout or temporality in the range")

// compactHistogramInstantPoints is the number of points that irate and idelta
// need for each bucket: they read the last two points, and
// metricsQLSampleInterval reads the last 21 timestamps.
const compactHistogramInstantPoints = 21

type compactHistogramPlan struct {
	// call is the quantile call, or nil when the plan is the bare
	// sum by (le) and returns one series per bucket.
	call                                              *parser.Call
	buckets                                           *parser.AggregateExpr
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
	plan := &compactHistogramPlan{start: start.UnixMilli(), end: end.UnixMilli(), step: step.Milliseconds(), lookback: lookback.Milliseconds()}
	bucketExpr := unparenExpr(expr)
	if call, ok := bucketExpr.(*parser.Call); ok {
		plan.call = call
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
		bucketExpr = unparenExpr(call.Args[plan.bucketArg])
	}
	aggregate, ok := bucketExpr.(*parser.AggregateExpr)
	if !ok || aggregate.Op != parser.SUM || aggregate.Without || len(aggregate.Grouping) != 1 || aggregate.Grouping[0] != "le" {
		return nil
	}
	plan.buckets = aggregate
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
//
// A delta source keeps the running total that the general path keeps in
// deltaTotals (see mergePostHogDeltaSamples and postHogDeltaTotals): points
// that share a timestamp become one point. rate, increase and delta read the window sum of that total, as
// metricsQLRollup does with deltaWindowSum. The total of the last point that
// left the selector range is the baseline of a window with no earlier point
// in range.
type compactHistogramEvaluator struct {
	plan         *compactHistogramPlan
	cfg          Config
	buckets      map[string][]compactHistogramSum
	columns      [][]compactHistogramSum
	bounds       []float64
	current      []float64
	history      [][]promql.FPoint
	sourceID     uint64
	haveSource   bool
	grid, series int
	output       int
	// delta is the temporality of the current source. deltaTotal is the
	// running total before the newest timestamp, and deltaSum the sum of the
	// points at the newest timestamp.
	delta                bool
	deltaTotal, deltaSum []float64
	trimmedTotal         []float64
	trimmed              bool
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
					if evaluator.delta {
						evaluator.trimmedTotal[index] = points[first-1].F
					}
					evaluator.history[index] = points[:copy(points, points[first:])]
				}
				evaluator.trimmed = evaluator.trimmed || evaluator.delta
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
				var value float64
				if evaluator.delta {
					value = evaluator.deltaWindowSum(index, firstPoint, window)
				} else {
					value = metricsQLCounterValue(plan.function, evaluator.history[index], firstPoint, start, maxPrevious, plan.lookback, math.NaN())
				}
				if !math.IsNaN(value) {
					column[evaluator.grid].add(value)
				}
			}
		}
		evaluator.grid++
	}
	return nil
}

// deltaWindowSum returns the value of the plan's rollup over the window of a
// delta bucket, as metricsQLRollup computes it from seriesMeta.deltaWindowSum:
// the running total at the last point in the window minus the total at the
// last point before the window. It returns NaN when the window holds no
// point. first is the index of the first point in the window.
func (evaluator *compactHistogramEvaluator) deltaWindowSum(index, first int, window int64) float64 {
	points := evaluator.history[index]
	if first == len(points) {
		return math.NaN()
	}
	sum := points[len(points)-1].F
	if first > 0 {
		sum -= points[first-1].F
	} else if evaluator.trimmed {
		sum -= evaluator.trimmedTotal[index]
	}
	if evaluator.plan.function == "rate" {
		sum /= float64(window) / 1000
	}
	return sum
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
	// Over a delta series, rate, increase and delta read window sums. irate
	// and idelta read the running total as a cumulative counter, and that
	// total starts at the first point the request reads.
	delta := sample.temporality == "delta"
	if (!delta && sample.temporality != "cumulative") || (delta && !metricsQLDeltaWindowFunctions[evaluator.plan.function]) {
		return errCompactHistogramFallback
	}
	if len(sample.counts) != len(sample.bounds)+1 {
		return errCompactHistogramFallback
	}
	if !evaluator.haveSource {
		if len(sample.counts) > evaluator.cfg.MaxSeries-evaluator.series {
			return fmt.Errorf("histogram series limit exceeded (%d); tighten matchers", evaluator.cfg.MaxSeries)
		}
		evaluator.series += len(sample.counts)
		evaluator.bounds = append(evaluator.bounds[:0], sample.bounds...)
		evaluator.current = make([]float64, len(sample.counts))
		// A narrower source must not keep a wider one's points reachable
		// through the backing array: the limit counts only the current
		// source's history.
		if retained := evaluator.history[:cap(evaluator.history)]; len(sample.counts) < len(retained) {
			clear(retained[len(sample.counts):])
		}
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
		evaluator.delta, evaluator.trimmed = delta, false
		if delta {
			evaluator.deltaTotal = make([]float64, len(sample.counts))
			evaluator.deltaSum = make([]float64, len(sample.counts))
			evaluator.trimmedTotal = make([]float64, len(sample.counts))
		}
	} else if !slices.EqualFunc(evaluator.bounds, sample.bounds, func(left, right float64) bool { return math.Float64bits(left) == math.Float64bits(right) }) {
		return errCompactHistogramFallback
	} else if evaluator.delta != delta {
		// The general path refuses a series that mixes temporalities.
		return errCompactHistogramFallback
	}
	if err := evaluator.advance(ctx, sample.timestamp, false); err != nil {
		return err
	}
	times := evaluator.history[0]
	// The limit counts the samples held at once, as the Prometheus engine
	// does: the in-range points of the current source series and the summed
	// step grid. Rows stream in one source at a time, so a long range reads
	// many more samples than it holds.
	if held := (len(times) + 1) * len(sample.counts); evaluator.cfg.MaxSamples > 0 && held > evaluator.cfg.MaxSamples-len(evaluator.buckets)*evaluator.plan.steps {
		return fmt.Errorf("histogram sample limit exceeded (%d); shorten the time range or tighten matchers", evaluator.cfg.MaxSamples)
	}
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
		switch {
		case evaluator.delta:
			// Points that share a timestamp add up, as in
			// mergePostHogDeltaSamples: the sum of the timestamp is added to
			// the total of the earlier timestamps.
			if duplicate {
				evaluator.deltaSum[index] += float64(cumulative)
			} else {
				evaluator.deltaTotal[index] += evaluator.deltaSum[index]
				evaluator.deltaSum[index] = float64(cumulative)
			}
			evaluator.current[index] = evaluator.deltaTotal[index] + evaluator.deltaSum[index]
		case duplicate && evaluator.current[index] != float64(cumulative):
			return errCompactHistogramFallback
		default:
			evaluator.current[index] = float64(cumulative)
		}
	}
	if cumulative != sample.count {
		return errCompactHistogramFallback
	}
	for index, value := range evaluator.current {
		if evaluator.delta && duplicate {
			evaluator.history[index][len(evaluator.history[index])-1].F = value
			continue
		}
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

// evaluate applies the quantile call of plan to the summed buckets, or
// returns the buckets themselves for a bare sum by (le). Plans that share the
// bucket expression share the evaluator, so one read of the buckets serves
// several quantiles. The result keeps NaN points: in a union they still hide
// a later operand's series with the same labels, as in the engine.
func (evaluator *compactHistogramEvaluator) evaluate(ctx context.Context, plan *compactHistogramPlan) (promql.Matrix, error) {
	args := parser.Expressions{plan.buckets}
	if plan.call != nil {
		args = plan.call.Args
	}
	vectors := make([]promql.Vector, len(args))
	for index, arg := range args {
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
		values := vector
		if plan.call != nil {
			values, _ = promql.FunctionCalls[plan.call.Func.Name](vectors, nil, plan.call.Args, helper)
		}
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
type compactSeriesFunction func(promql.Matrix) (promql.Matrix, error)

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

	metricsQLInternalPrefix + "label_set":       parseCompactLabelEdit,
	metricsQLInternalPrefix + "label_copy":      parseCompactLabelEdit,
	metricsQLInternalPrefix + "label_move":      parseCompactLabelEdit,
	metricsQLInternalPrefix + "label_transform": parseCompactLabelEdit,
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
	return func(matrix promql.Matrix) (promql.Matrix, error) {
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
		return matrix, nil
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
	return func(matrix promql.Matrix) (promql.Matrix, error) {
		joined := make([]string, len(sources))
		for index := range matrix {
			for position, source := range sources {
				joined[position] = matrix[index].Metric.Get(source)
			}
			builder := labels.NewBuilder(matrix[index].Metric)
			builder.Set(destination, strings.Join(joined, separator))
			matrix[index].Metric = builder.Labels()
		}
		return matrix, nil
	}, true
}

// parseCompactLabelEdit accepts a MetricsQL label edit whose label names are
// valid and not __name__. It calls the engine's implementation once per series.
func parseCompactLabelEdit(call *parser.Call) (compactSeriesFunction, bool) {
	function := promql.FunctionCalls[call.Func.Name]
	values, ok := compactStringArgs(call.Args[1:])
	if function == nil || !ok || len(values) == 0 {
		return nil, false
	}
	names := values
	switch strings.TrimPrefix(call.Func.Name, metricsQLInternalPrefix) {
	case "label_set":
		names = nil
		for index := 0; index < len(values); index += 2 {
			names = append(names, values[index])
		}
	case "label_transform":
		names = values[:1]
	}
	for _, name := range names {
		if !compactLabelDestination(name) {
			return nil, false
		}
	}
	return func(matrix promql.Matrix) (result promql.Matrix, err error) {
		// label_transform panics when a value exceeds its limit. The engine
		// path reports that error.
		defer func() {
			if recover() != nil {
				result, err = nil, errCompactHistogramFallback
			}
		}()
		helper := &promql.EvalNodeHelper{}
		vectors := []promql.Vector{{{}}}
		for index := range matrix {
			vectors[0][0] = promql.Sample{Metric: matrix[index].Metric}
			helper.Out = helper.Out[:0]
			output, _ := function(vectors, nil, call.Args, helper)
			matrix[index].Metric = output[0].Metric
		}
		return matrix, nil
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
	return func(matrix promql.Matrix) (promql.Matrix, error) {
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
		return result, nil
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
		// MetricsQL union keeps the first series with each label set. `or`
		// ignores the metric name, so it matches only quantiles without one.
		if union, ok := expr.(*parser.Call); ok && union.Func.Name == metricsQLUnionName {
			first := len(branches)
			for _, arg := range union.Args {
				if !collect(arg) {
					return false
				}
			}
			for _, branch := range branches[first:] {
				if compactHistogramNamed(branch.plan) {
					return false
				}
			}
			return true
		}
		if binary, ok := expr.(*parser.BinaryExpr); ok {
			if binary.Op != parser.LOR || binary.VectorMatching == nil || binary.VectorMatching.On || len(binary.VectorMatching.MatchingLabels) != 0 {
				return false
			}
			return collect(binary.LHS) && collect(binary.RHS)
		}
		var functions []compactSeriesFunction
		labelEdit := false
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
			labelEdit = labelEdit || strings.HasPrefix(call.Func.Name, metricsQLInternalPrefix)
			expr = unparenExpr(call.Args[0])
		}
		slices.Reverse(functions)
		plan := planCompactHistogram(expr, start, end, step, lookback)
		if plan == nil || len(branches) >= maxAggregateUnionSelectors || (labelEdit && compactHistogramNamed(plan)) {
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

// compactHistogramNamed reports whether the quantile label is __name__. The
// engine drops such a name at MetricsQL label edits and keeps it at `or`.
func compactHistogramNamed(plan *compactHistogramPlan) bool {
	if plan.call == nil {
		return false
	}
	label, ok := plan.call.Args[1].(*parser.StringLiteral)
	return ok && label.Val == labels.MetricName
}

// applyCompactSeriesFunctions applies the functions of a branch. When two
// series end with the same labels, the engine merges them or fails, so that
// case stays on the general path.
func applyCompactSeriesFunctions(matrix promql.Matrix, functions []compactSeriesFunction) (promql.Matrix, error) {
	for _, function := range functions {
		var err error
		if matrix, err = function(matrix); err != nil {
			return nil, err
		}
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
		key := branch.plan.buckets.String()
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

// compactReadMode selects which reads readCompactHistogram may run.
type compactReadMode int

const (
	// compactReadAll runs the summed read, then the streaming read.
	compactReadAll compactReadMode = iota
	// compactReadSummed runs only the summed read, and declines when it
	// cannot answer, so the histogram pushdown can try before the streaming read.
	compactReadSummed
	// compactReadStreaming runs only the streaming read.
	compactReadStreaming
)

type compactReadModeKey struct{}

func withCompactReadMode(ctx context.Context, mode compactReadMode) context.Context {
	return context.WithValue(ctx, compactReadModeKey{}, mode)
}

func compactReadModeOf(ctx context.Context) compactReadMode {
	mode, ok := ctx.Value(compactReadModeKey{}).(compactReadMode)
	if !ok {
		return compactReadAll
	}
	return mode
}

// compactHistogramSources returns the histogram sources a bucket selector
// reads, one per label set without le. It reports false when the team stores
// no histogram with the base name, which a primary key lookup settles before
// reading the series, or when the selector also finds real series with the
// name, or two sources share a label set: the engine merges those.
func (server *Server) compactHistogramSources(ctx context.Context, alias postHogHistogramAlias, matchers []*labels.Matcher, mint, maxt int64) ([]uint64, bool, error) {
	if found, err := server.postHogSelectsHistogram(ctx, mint, maxt, matchers); err != nil || !found {
		return nil, false, err
	}
	querier := &CHQuerier{queryable: server.queryable}
	real, sources, ids, err := querier.selectPostHogExactHistogramMetadata(ctx, mint, maxt, alias, matchers)
	if err != nil || len(real) > 0 {
		return nil, false, err
	}
	seen := make(map[string]bool)
	selected := make([]uint64, 0, len(ids))
	for _, id := range ids {
		source := sources[id]
		labelMap := postHogLabelMap(source.metricName, source.serviceName, source.resource, source.attributes)
		if !matchesAll(labelMap, postHogHistogramBaseMatchers(matchers)) {
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
	return selected, true, nil
}

// rangePushdownBuckets fills an evaluator with the sum by (le) that the range
// pushdown computes, for a classic histogram stored as plain _bucket series.
// It reports false when the range pushdown declines, as it does whenever the
// selector also reaches PostHog histograms.
func (server *Server) rangePushdownBuckets(ctx context.Context, plan *compactHistogramPlan) (*compactHistogramEvaluator, bool, error) {
	groups, ok, err := server.tryFastRangeGroups(ctx, metricsQLQuery{query: plan.buckets.String()}, time.UnixMilli(plan.start), time.UnixMilli(plan.end), time.Duration(plan.step)*time.Millisecond)
	if !ok || err != nil {
		return nil, ok, err
	}
	evaluator := newCompactHistogramEvaluator(plan, server.cfg)
	for _, group := range groups {
		// Each le holds a dense column of steps, so the sample budget bounds
		// the columns before they are allocated, as in the other reads.
		if server.cfg.MaxSamples > 0 && len(evaluator.buckets)+1 > server.cfg.MaxSamples/plan.steps {
			return nil, false, nil
		}
		column := make([]compactHistogramSum, plan.steps)
		for _, point := range group.points {
			if index := (point.t - plan.start) / plan.step; index >= 0 && index < int64(plan.steps) {
				column[index].add(point.v)
			}
		}
		evaluator.buckets[group.metric["le"]] = column
	}
	return evaluator, true, nil
}

// readCompactHistogram reads the buckets of plan and sums them on the step
// grid. It reports false when the data needs the general evaluation path.
func (server *Server) readCompactHistogram(ctx context.Context, plan *compactHistogramPlan) (*compactHistogramEvaluator, bool, error) {
	// The first step reads the range of the selector, as the engine does. A
	// rate without a window has a range of one step plus the lookback.
	mint := plan.start - plan.selectorRange + 1
	selected, ok, err := server.compactHistogramSources(ctx, plan.alias, plan.selector.LabelMatchers, mint, plan.end)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return server.rangePushdownBuckets(ctx, plan)
	}
	evaluator := newCompactHistogramEvaluator(plan, server.cfg)
	mode := compactReadModeOf(ctx)
	if mode != compactReadStreaming {
		summed, err := server.sumCompactHistogram(ctx, plan, evaluator, selected, mint)
		if errors.Is(err, errCompactHistogramSplit) {
			return nil, false, nil
		}
		if summed || err != nil {
			return evaluator, true, err
		}
		if mode == compactReadSummed {
			return nil, false, nil
		}
	}
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

// sumCompactHistogram fills the evaluator from ClickHouse, which evaluates the
// rollup of every bucket of every source at each step and sums the buckets by
// bound, so Go receives buckets x steps values instead of every raw row. It
// reports false, with the evaluator untouched, when the streaming read must
// evaluate the plan: an implicit window, a rollup other than rate over a
// cumulative source, a row the streaming read rejects, or no rows at all.
func (server *Server) sumCompactHistogram(ctx context.Context, plan *compactHistogramPlan, evaluator *compactHistogramEvaluator, ids []uint64, mint int64) (bool, error) {
	if plan.window == 0 || !metricsQLDeltaWindowFunctions[plan.function] || len(ids) == 0 {
		return false, nil
	}
	rows := 0
	err := queryPostHogSeriesIDs(ctx, server.client, server.cfg.IDChunkSize, ids, func(_ []uint64, idCondition string) string {
		return compactHistogramSQL(server.cfg, plan, mint, idCondition)
	}, func(row clickHouseRow) error {
		rows++
		var bounds, sums []float64
		var present []uint8
		var bad, split uint8
		if err := row.Scan(&bounds, &sums, &present, &bad, &split); err != nil {
			return err
		}
		if split != 0 {
			return errCompactHistogramSplit
		}
		if bad != 0 || len(sums) != (len(bounds)+1)*plan.steps || len(present) != plan.steps {
			return errCompactHistogramFallback
		}
		for bucket := 0; bucket <= len(bounds); bucket++ {
			bound := math.Inf(1)
			if bucket < len(bounds) {
				bound = bounds[bucket]
			}
			key := formatHistogramPushdownBound(bound)
			column, exists := evaluator.buckets[key]
			if !exists {
				if evaluator.cfg.MaxSamples > 0 && (len(evaluator.buckets)+1) > evaluator.cfg.MaxSamples/plan.steps {
					return errCompactHistogramFallback
				}
				column = make([]compactHistogramSum, plan.steps)
				evaluator.buckets[key] = column
			}
			for index, sum := range sums[bucket*plan.steps : (bucket+1)*plan.steps] {
				if present[index] != 0 {
					column[index].add(sum)
				}
			}
		}
		return nil
	})
	if errors.Is(err, errCompactHistogramSplit) {
		clear(evaluator.buckets)
		return false, err
	}
	// The summed read holds a step grid for every bucket of every source, so a
	// wide query can exhaust ClickHouse memory or time where the streaming read
	// succeeds. The read declines when the summed one hits such a limit.
	if errors.Is(err, errCompactHistogramFallback) || isClickHouseResourceError(err) || (err == nil && rows == 0) {
		clear(evaluator.buckets)
		return false, nil
	}
	return err == nil, err
}

// compactHistogramRowsSQL reads the histogram rows of where, one per point. A
// trailing +Inf bound with an empty overflow bucket is one +Inf bucket, as in
// normalizePostHogHistogramBounds. row_bad marks a row that the streaming read
// evaluates differently, and is_stale a stale marker: capture-apm-metrics
// writes one as a row with a StaleNaN sum and zero bucket counts.
func compactHistogramRowsSQL(cfg Config, where []string) string {
	trailingInf := "notEmpty(histogram_bounds) AND histogram_bounds[-1] = inf AND length(histogram_counts) = length(histogram_bounds) + 1 AND histogram_counts[-1] = 0"
	bad := "aggregation_temporality NOT IN ('delta', 'cumulative') OR metric_type != 'histogram'" +
		" OR length(counts) != length(bounds) + 1" +
		" OR arraySum(counts) != observations" +
		" OR NOT arrayAll(b -> isFinite(b), bounds)" +
		" OR (length(bounds) > 1 AND arrayMin(arrayPopFront(arrayDifference(bounds))) <= 0)"
	return fmt.Sprintf(
		"SELECT series_fingerprint AS series_id, toUnixTimestamp64Milli(timestamp) AS ts, count AS observations, "+
			"if(%[1]s, arrayPopBack(histogram_counts), histogram_counts) AS counts, if(%[1]s, arrayPopBack(histogram_bounds), histogram_bounds) AS bounds, "+
			"aggregation_temporality = 'delta' AS is_delta, cityHash64(bounds) AS layout, toUInt8(%[2]s) AS row_bad, toUInt8(%[4]s) AS is_stale FROM %[3]s",
		trailingInf, bad, postHogSampleRowsFrom(cfg, where, true), staleSampleSQL("value"),
	)
}

// compactStepRunsSQL counts, for no point and then for each point of the
// sorted times array, the steps whose time minus offset it is the last point
// at or before.
func compactStepRunsSQL(start, step int64, steps int, offset string) string {
	return fmt.Sprintf(
		"arrayPopFront(arrayDifference(arrayConcat([toInt64(0)], arrayMap(t -> least(greatest(toInt64(ceil((t + %s - %d) / %d)), 0), %d), times), [toInt64(%d)])))",
		offset, start, step, steps, steps,
	)
}

// compactExpandSQL repeats element k of values, which belongs to point k (0
// to no point), for its run of steps, lining the values up with the grid.
func compactExpandSQL(values, runs string) string {
	return "arrayFlatten(arrayMap((x, n) -> arrayWithConstant(n, x), " + values + ", " + runs + "))"
}

// compactMaxPrevSQL returns, for each step, metricsQLSampleInterval over the
// points of the sorted times array in (t - history, t], with the step as the
// fallback, capped at the lookback: the max_prev the engine uses at t. The
// last 21 points at or before each step come from the end runs, each as a copy
// of times shifted by its distance, because a lambda that read times itself
// would copy the array once per step.
func compactMaxPrevSQL(history, start, step int64, steps int, lookback int64, endRuns string) string {
	params, arrays, gaps := []string{"i", "p0"}, []string{fmt.Sprintf("range(%d)", steps), compactExpandSQL("arrayPushFront(times, 0)", endRuns)}, []string{}
	for k := 1; k <= 20; k++ {
		params = append(params, fmt.Sprintf("p%d", k))
		arrays = append(arrays, compactExpandSQL(fmt.Sprintf("arrayPushFront(arrayShiftRight(times, %d), 0)", k), endRuns))
		gaps = append(gaps, fmt.Sprintf("if(p%[1]d > %[2]d + i * %[3]d - %[4]d, p%[5]d - p%[1]d, -1)", k, start, step, history, k-1))
	}
	interval := "toInt64(floor(arrayReduce('quantileExactInclusiveOrDefault(0.6)', arrayFilter(g -> g >= 0, [" + strings.Join(gaps, ", ") + "]))))"
	return fmt.Sprintf(
		"arrayMap(iv -> least(if(iv <= 0, %d, %s), %d), arrayMap((%s) -> %s, %s))",
		step, metricsQLSampleIntervalMarginSQL("iv"), lookback, strings.Join(params, ", "), interval, strings.Join(arrays, ", "),
	)
}

// compactHistogramSQL returns one row per bucket layout: for each bucket and
// step, the sum over sources of the rollup, and for each step whether any
// source had a value there.
//
// Each source bucket becomes one series of running totals. A delta bucket
// adds up its points, so a window sum is the total at the last point in the
// window minus the total at the last point before it. A cumulative bucket
// adds up the increase from each point to the next, with the reset rule of
// metricsQLCounterValue, so a rate is the difference of two totals over the
// time between their points. How recent a previous point must be follows
// metricsQLSampleInterval over the selector range at each step, as in the
// streaming read.
//
// The points that end each step and each window start are found without a
// search: a point is the last one at or before every step from the first
// step at or after it up to the first step at or after the next point.
// Repeating each value for its run of steps lines it up with the step grid.
func compactHistogramSQL(cfg Config, plan *compactHistogramPlan, mint int64, idCondition string) string {
	matchers := postHogHistogramSampleMatchers([]postHogHistogramAlias{plan.alias}, plan.selector.LabelMatchers)
	where := postHogSampleFilters(cfg, matchers, mint, plan.end)
	// A stale marker is not a sample of any window, as in a range selector,
	// so its zero counts never read as a counter reset.
	where = append(where, postHogHistogramTypeFilter, idCondition, nonStaleSampleSQL("value"))
	rows := compactHistogramRowsSQL(cfg, where)
	// Only rate reads cumulative sources here: a cumulative source under
	// another rollup marks its layout bad, so the streaming read takes over.
	// The fingerprint list, which can be a long literal, appears once. Such a
	// source gathers no points, since none of them can be used.
	cumulativeBad, gather := "", "1"
	if plan.function != "rate" {
		cumulativeBad, gather = " OR min(is_delta) = 0", "is_delta"
	}
	// The aggregation gathers each bucket of a source into its own lane, in
	// the order of the times; the next stage sorts both by time.
	sources := fmt.Sprintf(
		"SELECT any(bounds) AS layout_bounds, any(is_delta) AS delta, max(row_bad) OR min(layout) != max(layout) OR min(is_delta) != max(is_delta)%[1]s AS group_bad, "+
			"min(layout) != max(layout) OR min(is_delta) != max(is_delta) AS layout_split, "+
			"groupArrayIf(ts, %[2]s) AS read_times, groupArrayForEachIf(arrayMap(c -> toInt64(c), arrayCumSum(counts)), %[2]s) AS read_lanes FROM (%[3]s) GROUP BY series_id",
		cumulativeBad, gather, rows,
	)
	start, step, steps := strconv.FormatInt(plan.start, 10), strconv.FormatInt(plan.step, 10), strconv.Itoa(plan.steps)
	window := strconv.FormatInt(plan.window, 10)
	runs := func(offset string) string { return compactStepRunsSQL(plan.start, plan.step, plan.steps, offset) }
	expand := compactExpandSQL
	scale := "1"
	if plan.function == "rate" {
		scale = "1000 / " + window
	}
	// Per step: e is the last point at or before the step, s the last point at
	// or before its window start. A cumulative rate begins at s when it is
	// recent enough, as metricsQLCounterValue does, and otherwise at the point
	// after s, the first in the window. A step without a value adds 0 and is
	// not present.
	value := fmt.Sprintf(
		"(e, s, h, et, bt, ev, sv, fv) -> if(delta, if(e > s, toFloat64(ev - sv) * %s, 0), "+
			"if(e > s AND (h OR e > s + 1) AND et > bt, toFloat64(ev - if(h, sv, fv)) * 1000 / (et - bt), 0))",
		scale,
	)
	// The buckets of a source are lanes of running totals inside one row, so
	// the step grid of the source is computed once for all its buckets. A lane
	// of a bad source can be shorter than its times; arrayResize keeps the
	// arrays the same size until Go discards the result. A source already
	// known to be bad gets no times and no lanes, so it builds no step grid
	// for a result Go discards.
	series := fmt.Sprintf(
		"SELECT layout_bounds, layout_split, toUInt8(group_bad OR length(read_lanes) != length(layout_bounds) + 1 OR (NOT delta AND has(arrayPopFront(arrayDifference(times)), 0))) AS bad, "+
			"arraySort(if(group_bad, emptyArrayInt64(), read_times)) AS times, "+
			"arrayMap(lane -> arrayCumSum(arrayPushFront(if(delta, lane, arrayMap((c, p) -> if(c >= p, c - p, if((p - c) * 8 < p, 0, c)), lane, arrayPushFront(arrayPopBack(lane), lane[1]))), 0)), "+
			"arrayMap(lane -> arraySort((v, t) -> t, arrayResize(lane, length(read_times)), read_times), if(group_bad, [], read_lanes))) AS totals, "+
			"%[4]s AS end_runs, %[5]s AS start_runs, "+
			"%[6]s AS end_index, %[7]s AS start_index, %[8]s AS end_time, %[9]s AS start_time, "+
			// max_prev of a step is metricsQLSampleInterval at its last point.
			"%[19]s AS max_prev, "+
			"arrayMap((i, s, st, mp) -> s > 0 AND st > %[10]s + i * %[1]s - %[11]s - mp, range(%[12]s), start_index, start_time, max_prev) AS have_prev, "+
			"arrayMap((h, st, ft) -> if(h, st, ft), have_prev, start_time, %[13]s) AS begin_time, "+
			"arrayMap((e, s, h, et, bt) -> toUInt8(if(delta, e > s, e > s AND (h OR e > s + 1) AND et > bt)), end_index, start_index, have_prev, end_time, begin_time) AS present, "+
			"arrayFlatten(arrayMap(lane -> arrayMap(%[14]s, end_index, start_index, have_prev, end_time, begin_time, %[15]s, %[16]s, %[17]s), totals)) AS step_values "+
			"FROM (%[18]s)",
		step, "", 0, runs("0"), runs(window),
		expand("range(length(times) + 1)", "end_runs"), expand("range(length(times) + 1)", "start_runs"),
		expand("arrayPushFront(times, 0)", "end_runs"), expand("arrayPushFront(times, 0)", "start_runs"),
		start, window, steps,
		expand("arrayPushBack(times, 0)", "start_runs"),
		value, expand("lane", "end_runs"), expand("lane", "start_runs"), expand("arrayPushBack(arrayPopFront(lane), 0)", "start_runs"),
		sources,
		compactMaxPrevSQL(plan.selectorRange, plan.start, plan.step, plan.steps, plan.lookback, "end_runs"),
	)
	// A layout whose buckets x steps exceed the sample budget is marked bad and
	// sent without its sums, so Go never decodes an oversized grid; the
	// streaming read then enforces the limit as before.
	oversized := "0"
	if cfg.MaxSamples > 0 {
		oversized = fmt.Sprintf("(length(layout_bounds) + 1) * %d > %d", plan.steps, cfg.MaxSamples)
	}
	return withRangePushdownSettings(fmt.Sprintf(
		"SELECT layout_bounds, if(%[1]s, emptyArrayFloat64(), sumForEach(step_values)) AS sums, maxForEach(present) AS present, toUInt8(%[1]s OR max(bad)) AS bad, max(layout_split) AS layout_split FROM (%[2]s) GROUP BY layout_bounds",
		oversized, series,
	), cfg.RangeQueryThreads)
}

// countHistogramBuckets answers count(x_bucket{...}) over PostHog histograms:
// at each step it adds the buckets of every source that the bare selector
// returns a point for, so ClickHouse reads each source once and Go receives
// one count per step instead of every bucket series. It reports false when
// the engine must evaluate the query: an le matcher, real series with the
// name, sources that share a label set, or a source that changes layout or
// temporality.
func (s *Server) countHistogramBuckets(ctx context.Context, call rangeRollupCall, mint, maxt, start, points int64) ([]rangeAggregateGroup, bool, error) {
	alias, ok := postHogExactHistogramAlias(call.selector.LabelMatchers)
	if !ok || alias.suffix != "_bucket" || call.window != 0 || call.offset != 0 {
		return nil, false, nil
	}
	for _, matcher := range call.selector.LabelMatchers {
		if matcher.Name == "le" {
			return nil, false, nil
		}
	}
	ids, ok, err := s.compactHistogramSources(ctx, alias, call.selector.LabelMatchers, mint, maxt)
	if !ok || err != nil {
		return nil, err != nil, err
	}
	if len(ids) == 0 {
		return nil, true, nil
	}
	counts := make([]float64, points)
	bad := false
	err = queryPostHogSeriesIDs(ctx, s.client, s.cfg.IDChunkSize, ids, func(_ []uint64, idCondition string) string {
		return histogramBucketCountSQL(s.cfg, alias, call, mint, maxt, start, points, idCondition)
	}, func(row clickHouseRow) error {
		var rowCounts []uint64
		var rowBad uint8
		if err := row.Scan(&rowCounts, &rowBad); err != nil {
			return err
		}
		bad = bad || rowBad != 0
		for index := range min(len(rowCounts), len(counts)) {
			counts[index] += float64(rowCounts[index])
		}
		return nil
	})
	if err != nil || bad {
		return nil, err != nil, err
	}
	group := rangeAggregateGroup{metric: map[string]string{}}
	for index, count := range counts {
		if count > 0 {
			group.points = append(group.points, samplePoint{t: start + int64(index)*call.step, v: count})
		}
	}
	if len(group.points) == 0 {
		return nil, true, nil
	}
	return []rangeAggregateGroup{group}, true, nil
}

// histogramBucketCountSQL returns, for each step, the buckets of the sources
// that have a point in the bare selector's window (t - w, t], where w is
// min(max(step, interval), lookback) and the interval is metricsQLSampleInterval
// over the selector matrix at t, as in metricsQLRollup. Delta points that
// share a time count once, as the engine merges them, and are stale only when
// every one is; a cumulative source with such points is left to the engine. A
// source whose last point is a stale marker is absent.
func histogramBucketCountSQL(cfg Config, alias postHogHistogramAlias, call rangeRollupCall, mint, maxt, start, points int64, idCondition string) string {
	where := postHogSampleFilters(cfg, postHogHistogramSampleMatchers([]postHogHistogramAlias{alias}, call.selector.LabelMatchers), mint, maxt)
	where = append(where, postHogHistogramTypeFilter, idCondition)
	sources := fmt.Sprintf(
		"SELECT any(length(counts)) AS buckets, arraySort(groupArray((ts, 1 - is_stale))) AS read_points, "+
			"arrayFilter((p, n) -> p.1 != n.1, read_points, arrayPushBack(arrayPopFront(read_points), (toInt64(0), toUInt8(0)))) AS points, "+
			"arrayMap(p -> p.1, points) AS times, arrayMap(p -> p.2, points) AS live, "+
			"max(row_bad) OR min(layout) != max(layout) OR min(is_delta) != max(is_delta) OR (min(is_delta) = 0 AND length(times) != count()) AS bad "+
			"FROM (%s) GROUP BY series_id",
		compactHistogramRowsSQL(cfg, where),
	)
	runs := compactStepRunsSQL(start, call.step, int(points), "0")
	return withRangePushdownSettings(fmt.Sprintf(
		"SELECT sumForEach(present) AS counts, toUInt8(max(bad)) AS bad FROM (SELECT bad, %[1]s AS end_runs, "+
			"arrayMap((i, t, l, mp) -> if(t > 0 AND l AND %[2]d + i * %[3]d - t < least(greatest(%[3]d, mp), %[4]d), toUInt64(buckets), 0), range(%[5]d), %[6]s, %[9]s, %[7]s) AS present FROM (%[8]s))",
		runs, start, call.step, call.lookback, points,
		compactExpandSQL("arrayPushFront(times, 0)", "end_runs"), compactMaxPrevSQL(call.matrix, start, call.step, int(points), call.lookback, "end_runs"), sources,
		compactExpandSQL("arrayPushFront(live, 0)", "end_runs"),
	), cfg.RangeQueryThreads)
}
