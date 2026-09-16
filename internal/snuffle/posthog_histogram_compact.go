package snuffle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

var errCompactHistogramFallback = errors.New("histogram requires the general evaluation path")

type compactHistogramPlan struct {
	call                               *parser.Call
	selector                           *parser.VectorSelector
	alias                              postHogHistogramAlias
	bucketArg                          int
	start, end, step, window, lookback int64
	steps                              int
}

func histogramUnparen(expr parser.Expr) parser.Expr {
	for {
		paren, ok := expr.(*parser.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

func planCompactHistogram(expr parser.Expr, start, end time.Time, step, lookback time.Duration) *compactHistogramPlan {
	call, ok := histogramUnparen(expr).(*parser.Call)
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
		number, ok := histogramUnparen(arg).(*parser.NumberLiteral)
		if !ok || math.IsNaN(number.Val) || math.IsInf(number.Val, 0) || quantiles[number.Val] {
			return nil
		}
		quantiles[number.Val] = true
	}
	aggregate, ok := histogramUnparen(call.Args[plan.bucketArg]).(*parser.AggregateExpr)
	if !ok || aggregate.Op != parser.SUM || aggregate.Without || len(aggregate.Grouping) != 1 || aggregate.Grouping[0] != "le" {
		return nil
	}
	rate, ok := histogramUnparen(aggregate.Expr).(*parser.Call)
	if !ok || rate.Func.Name != metricsQLInternalPrefix+"irate" || len(rate.Args) != 5 {
		return nil
	}
	var numbers [4]int64
	for index, arg := range rate.Args[1:] {
		number, ok := arg.(*parser.NumberLiteral)
		if !ok || math.IsNaN(number.Val) || number.Val < 0 || number.Val >= float64(math.MaxInt64) || number.Val != math.Trunc(number.Val) {
			return nil
		}
		numbers[index] = int64(number.Val)
	}
	plan.window = numbers[0]
	if plan.window <= 0 || numbers[1] != plan.step || numbers[2] != plan.lookback || numbers[3] != 0 || plan.step <= 0 || plan.lookback <= 0 {
		return nil
	}
	if plan.window > math.MaxInt64-plan.lookback || plan.start < math.MinInt64+plan.window+plan.lookback {
		return nil
	}
	matrix, ok := rate.Args[0].(*parser.MatrixSelector)
	if !ok || matrix.Range.Milliseconds() != plan.window+plan.lookback {
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

type compactHistogramEvaluator struct {
	plan                  *compactHistogramPlan
	cfg                   Config
	buckets               map[string][]compactHistogramSum
	columns               [][]compactHistogramSum
	bounds                []float64
	previous, current     []float64
	times                 []promql.FPoint
	sourceID              uint64
	haveSource            bool
	grid, samples, series int
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
		first := sort.Search(len(evaluator.times), func(index int) bool { return evaluator.times[index].T > timestamp-plan.window-plan.lookback })
		history := evaluator.times[first:]
		if len(history) >= 2 && history[len(history)-1].T > timestamp-plan.window {
			maxPrevious := min(metricsQLSampleInterval(history, plan.step), plan.lookback)
			points := [2]promql.FPoint{history[len(history)-2], history[len(history)-1]}
			firstPoint := 0
			if points[0].T <= timestamp-plan.window {
				firstPoint = 1
			}
			for index, column := range evaluator.columns {
				points[0].F, points[1].F = evaluator.previous[index], evaluator.current[index]
				value := metricsQLCounterValue("irate", points[:], firstPoint, timestamp-plan.window, maxPrevious, plan.lookback)
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
		evaluator.previous = make([]float64, len(sample.counts))
		evaluator.current = make([]float64, len(sample.counts))
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
		evaluator.times = evaluator.times[:0]
		evaluator.grid, evaluator.sourceID, evaluator.haveSource = 0, sample.id, true
	} else if !slices.EqualFunc(evaluator.bounds, sample.bounds, func(left, right float64) bool { return math.Float64bits(left) == math.Float64bits(right) }) {
		return errCompactHistogramFallback
	}
	if err := evaluator.advance(ctx, sample.timestamp, false); err != nil {
		return err
	}
	duplicate := len(evaluator.times) > 0 && evaluator.times[len(evaluator.times)-1].T == sample.timestamp
	if len(evaluator.times) > 0 && evaluator.times[len(evaluator.times)-1].T > sample.timestamp {
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
		evaluator.previous[index], evaluator.current[index] = evaluator.current[index], float64(cumulative)
	}
	if cumulative != sample.count {
		return errCompactHistogramFallback
	}
	if len(evaluator.times) == 21 {
		copy(evaluator.times, evaluator.times[1:])
		evaluator.times = evaluator.times[:20]
	}
	evaluator.times = append(evaluator.times, promql.FPoint{T: sample.timestamp})
	return nil
}

func (evaluator *compactHistogramEvaluator) result(ctx context.Context) (promql.Matrix, error) {
	if err := evaluator.advance(ctx, 0, true); err != nil {
		return nil, err
	}
	plan := evaluator.plan
	vectors := make([]promql.Vector, len(plan.call.Args))
	for index, arg := range plan.call.Args {
		if number, ok := histogramUnparen(arg).(*parser.NumberLiteral); ok {
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
	samples := 0
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
			if math.IsNaN(sample.F) {
				continue
			}
			key := sample.Metric.String()
			index, exists := byLabels[key]
			if !exists {
				index = len(result)
				byLabels[key] = index
				result = append(result, promql.Series{Metric: sample.Metric})
			}
			if evaluator.cfg.MaxSamples > 0 && samples >= evaluator.cfg.MaxSamples {
				return nil, errCompactHistogramFallback
			}
			result[index].Floats = append(result[index].Floats, promql.FPoint{T: helper.Ts, F: sample.F})
			samples++
		}
	}
	sort.Sort(result)
	return result, nil
}

func (server *Server) tryCompactHistogramRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (promql.Matrix, bool, error) {
	if !server.cfg.postHogSchemaLayout() || !server.cfg.PostHogCompactHistograms {
		return nil, false, nil
	}
	expr, err := server.parser.ParseExpr(query)
	if err != nil {
		return nil, false, nil
	}
	plan := planCompactHistogram(expr, start, end, step, server.cfg.LookbackDelta)
	if plan == nil {
		return nil, false, nil
	}
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
	for _, batch := range idBatches(selected, server.cfg.IDChunkSize) {
		err = server.client.QueryRows(ctx, postHogHistogramSamplesSQL(server.cfg, mint, plan.end, batch, []postHogHistogramAlias{plan.alias}, plan.selector.LabelMatchers), func(row clickHouseRow) error {
			if err := sample.scanPacked(row); err != nil {
				return err
			}
			return evaluator.add(ctx, sample)
		})
		if err != nil {
			break
		}
	}
	var result promql.Matrix
	if err == nil {
		result, err = evaluator.result(ctx)
	}
	if errors.Is(err, errCompactHistogramFallback) {
		return nil, false, nil
	}
	return result, true, err
}
