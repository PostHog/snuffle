package snuffle

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/model/labels"
)

// metricsQLRangeOp transforms the float result of a query over the whole
// range. It serves MetricsQL functions whose value at a step depends on the
// other steps, such as topk_max. Ops run after the range cache, so they must
// not modify the input groups or their points.
// An op returns the context's error when the query is canceled or times out.
type metricsQLRangeOp func(ctx context.Context, groups []rangeAggregateGroup) ([]rangeAggregateGroup, error)

// maxRangeOps bounds the nesting of topk_* and bottomk_* calls in one query.
const maxRangeOps = 8

// hasRangeOps reports whether the query needs applyRangeOps.
func (prepared metricsQLQuery) hasRangeOps() bool {
	return len(prepared.rangeOps) > 0 || prepared.defaultRight != nil
}

// applyRangeOps fills the gaps of result with the right side of default,
// which evalQuery evaluates over the same range, then runs the range ops of
// the query, innermost first.
func (prepared metricsQLQuery) applyRangeOps(ctx context.Context, result rangeQueryResult, evalQuery func(query string, prepared metricsQLQuery) (rangeQueryResult, *rangeQueryError)) (rangeQueryResult, *rangeQueryError) {
	if !prepared.hasRangeOps() {
		return result, nil
	}
	errFloats := &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: errors.New("default, topk_* and bottomk_* support only float series")}
	if !result.floats {
		return result, errFloats
	}
	if prepared.defaultRight != nil {
		right, err := evalQuery(prepared.defaultQuery, *prepared.defaultRight)
		if err != nil {
			return result, err
		}
		if !right.floats {
			return result, errFloats
		}
		// The right side can have a default or topk_* of its own.
		if right, err = prepared.defaultRight.applyRangeOps(ctx, right, evalQuery); err != nil {
			return result, err
		}
		groups, fillErr := rangeDefault(ctx, result.groups, right.groups, int(metricsQLMaxOutputSamples.Load()))
		if fillErr != nil {
			return result, &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: fillErr}
		}
		result.groups = groups
	}
	for _, op := range prepared.rangeOps {
		groups, err := op(ctx, result.groups)
		if err != nil {
			return result, &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: err}
		}
		result.groups = groups
	}
	return result, nil
}

// rangeDefault is MetricsQL's `left default right`: each step where a series
// of left has no point (or NaN) takes the value of the series of right with
// the same labels, apart from the metric name. A right side of one series
// without labels, such as a number, fills every series. Without left series
// the result is right. It does not modify its input, which can belong to the
// range cache. It returns an error when the filled result would hold more than
// maxSamples points (zero means no limit) or ctx ends.
// ponytail: VictoriaMetrics also fills series whose samples lie up to 5m and
// the rollup window before the range start but give no point in the range;
// this fills only series with a point in the range.
func rangeDefault(ctx context.Context, left, right []rangeAggregateGroup, maxSamples int) ([]rangeAggregateGroup, error) {
	if len(left) == 0 {
		return right, nil
	}
	scalar := len(right) == 1 && len(right[0].metric) == 0
	key := func(metric map[string]string) string {
		if scalar {
			return ""
		}
		return labels.FromMap(metric).DropReserved(func(name string) bool { return name == labels.MetricName }).String()
	}
	// VictoriaMetrics takes the first right series with a value at the step.
	fills := map[string]map[int64]float64{}
	for _, group := range right {
		k := key(group.metric)
		if fills[k] == nil {
			fills[k] = map[int64]float64{}
		}
		for _, point := range group.points {
			if _, ok := fills[k][point.t]; !ok && !math.IsNaN(point.v) {
				fills[k][point.t] = point.v
			}
		}
	}
	out := make([]rangeAggregateGroup, 0, len(left))
	total := 0
	for _, group := range left {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fill := fills[key(group.metric)]
		if len(fill) == 0 {
			total += len(group.points)
			if maxSamples > 0 && total > maxSamples {
				return nil, errDefaultSamples(maxSamples)
			}
			out = append(out, group)
			continue
		}
		seen := make(map[int64]bool, len(group.points))
		for _, point := range group.points {
			seen[point.t] = true
		}
		added := 0
		for t := range fill {
			if !seen[t] {
				added++
			}
		}
		// Check the budget before allocating the filled series.
		total += len(group.points) + added
		if maxSamples > 0 && total > maxSamples {
			return nil, errDefaultSamples(maxSamples)
		}
		points := make([]samplePoint, 0, len(group.points)+added)
		for _, point := range group.points {
			if v, ok := fill[point.t]; ok && math.IsNaN(point.v) {
				point.v = v
			}
			points = append(points, point)
		}
		for t, v := range fill {
			if !seen[t] {
				points = append(points, samplePoint{t: t, v: v})
			}
		}
		slices.SortFunc(points, func(a, b samplePoint) int { return cmp.Compare(a.t, b.t) })
		out = append(out, rangeAggregateGroup{metric: group.metric, points: points})
	}
	return out, nil
}

func errDefaultSamples(maxSamples int) error {
	return fmt.Errorf("default sample limit exceeded (%d); shorten the time range or tighten matchers", maxSamples)
}

// metricsQLRangeStats rank the series of topk_<stat> and bottomk_<stat>.
// Their input holds no NaN.
var metricsQLRangeStats = map[string]func(values []float64) float64{
	"min": func(values []float64) float64 { return slices.Min(values) },
	"max": func(values []float64) float64 { return slices.Max(values) },
	"avg": func(values []float64) float64 {
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	},
	// VictoriaMetrics takes the upper middle value of an even count.
	// ponytail: VictoriaMetrics samples series over 1000 points before
	// taking the median; this uses every point.
	"median": func(values []float64) float64 {
		sorted := slices.Sorted(slices.Values(values))
		return sorted[int(0.5*float64(len(sorted)-1)+0.5)]
	},
	"last": func(values []float64) float64 { return values[len(values)-1] },
}

// isRangeTopK reports whether name is topk_<stat> or bottomk_<stat>.
func isRangeTopK(name string) bool {
	direction, stat, _ := strings.Cut(strings.ToLower(name), "_")
	return (direction == "topk" || direction == "bottomk") && metricsQLRangeStats[stat] != nil
}

// prepareRangeTopK turns an outermost topk_<stat>(k, q, "label=value") into
// the range op and its inner expression q. It returns a nil op for any other
// expression.
func prepareRangeTopK(expr metricsql.Expr) (metricsQLRangeOp, metricsql.Expr, error) {
	call, ok := expr.(*metricsql.AggrFuncExpr)
	if !ok || !isRangeTopK(call.Name) {
		return nil, nil, nil
	}
	name := strings.ToLower(call.Name)
	if len(call.Args) < 2 || len(call.Args) > 3 {
		return nil, nil, fmt.Errorf("%s requires 2 or 3 arguments", name)
	}
	if call.Modifier.Op != "" || call.Limit > 0 {
		return nil, nil, fmt.Errorf("%s does not support by, without or limit", name)
	}
	k, ok := call.Args[0].(*metricsql.NumberExpr)
	if !ok {
		return nil, nil, fmt.Errorf("%s requires a number as its first argument", name)
	}
	remainder := ""
	if len(call.Args) == 3 {
		label, ok := call.Args[2].(*metricsql.StringExpr)
		if !ok {
			return nil, nil, fmt.Errorf("%s requires a string as its third argument", name)
		}
		remainder = label.S
	}
	direction, stat, _ := strings.Cut(name, "_")
	bottom := direction == "bottomk"
	rank := metricsQLRangeStats[stat]
	op := func(ctx context.Context, groups []rangeAggregateGroup) ([]rangeAggregateGroup, error) {
		return rangeTopK(ctx, groups, k.N, rank, bottom, remainder)
	}
	return op, call.Args[1], nil
}

// rangeTopK keeps the k series with the highest (or, for bottom, the lowest)
// rank over the range, as VictoriaMetrics' topk_<stat> and bottomk_<stat> do.
// A non-empty remainder "label=value" (or "label", meaning label=label) adds a
// series with only that label, holding the sum of the other series per step.
// It checks ctx once per series and returns its error when it is done.
func rangeTopK(ctx context.Context, groups []rangeAggregateGroup, k float64, rank func([]float64) float64, bottom bool, remainder string) ([]rangeAggregateGroup, error) {
	type ranked struct {
		group  rangeAggregateGroup
		labels labels.Labels
		rank   float64
	}
	series := make([]ranked, 0, len(groups))
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		values := nonNaNValues(group.points)
		r := math.NaN()
		if len(values) > 0 {
			r = rank(values)
		}
		series = append(series, ranked{group: group, labels: labels.FromMap(group.metric), rank: r})
	}
	// Series without points rank last. VictoriaMetrics breaks ties by its
	// internal series order, which is arbitrary; this one uses the labels.
	sort.Slice(series, func(i, j int) bool {
		a, b := series[i].rank, series[j].rank
		switch {
		case math.IsNaN(a) != math.IsNaN(b):
			return math.IsNaN(b)
		case a != b && !math.IsNaN(a):
			return a > b != bottom
		}
		return labels.Compare(series[i].labels, series[j].labels) < 0
	})
	kept := 0
	if k > 0 {
		kept = int(min(k, float64(len(series))))
	}
	out := make([]rangeAggregateGroup, 0, kept+1)
	for _, s := range series[:kept] {
		if !math.IsNaN(s.rank) {
			out = append(out, s.group)
		}
	}
	if remainder == "" {
		return out, nil
	}
	sums := map[int64]float64{}
	for _, s := range series[kept:] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, point := range s.group.points {
			if !math.IsNaN(point.v) {
				sums[point.t] += point.v
			}
		}
	}
	if len(sums) == 0 {
		return out, nil
	}
	points := make([]samplePoint, 0, len(sums))
	for t, v := range sums {
		points = append(points, samplePoint{t: t, v: v})
	}
	slices.SortFunc(points, func(a, b samplePoint) int { return cmp.Compare(a.t, b.t) })
	label, value, found := strings.Cut(remainder, "=")
	if !found {
		value = label
	}
	return append(out, rangeAggregateGroup{metric: map[string]string{label: value}, points: points}), nil
}

func nonNaNValues(points []samplePoint) []float64 {
	values := make([]float64, 0, len(points))
	for _, point := range points {
		if !math.IsNaN(point.v) {
			values = append(values, point.v)
		}
	}
	return values
}
