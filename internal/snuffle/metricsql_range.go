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

// applyRangeOps runs the range ops of the query, innermost first.
func (prepared metricsQLQuery) applyRangeOps(ctx context.Context, result rangeQueryResult) (rangeQueryResult, *rangeQueryError) {
	if len(prepared.rangeOps) == 0 {
		return result, nil
	}
	if !result.floats {
		return result, &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: errors.New("topk_* and bottomk_* functions support only float series")}
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
