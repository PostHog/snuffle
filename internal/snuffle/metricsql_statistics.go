package snuffle

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

func init() {
	upstream, ok := parser.Functions["histogram_quantiles"]
	if !ok {
		panic("metricsql: the Prometheus dependency has no histogram_quantiles function")
	}
	histogramQuantiles := *upstream
	histogramQuantiles.Name = metricsQLInternalPrefix + "histogram_quantiles"
	histogramQuantiles.Experimental = false
	histogramQuantiles.Variadic = -1
	parser.Functions[histogramQuantiles.Name] = &histogramQuantiles
	promql.FunctionCalls[histogramQuantiles.Name] = metricsQLHistogramQuantiles

	medianName := metricsQLInternalPrefix + "median"
	parser.Functions[medianName] = &parser.Function{
		Name: medianName, ArgTypes: []parser.ValueType{parser.ValueTypeString, parser.ValueTypeVector, parser.ValueTypeVector}, Variadic: -1, ReturnType: parser.ValueTypeVector,
	}
	promql.FunctionCalls[medianName] = metricsQLMedian

	quantilesName := metricsQLInternalPrefix + "quantiles"
	parser.Functions[quantilesName] = &parser.Function{
		Name: quantilesName, ArgTypes: []parser.ValueType{parser.ValueTypeString, parser.ValueTypeString, parser.ValueTypeVector, parser.ValueTypeScalar}, Variadic: -1, ReturnType: parser.ValueTypeVector,
	}
	promql.FunctionCalls[quantilesName] = metricsQLQuantiles
}

func (rewriter metricsQLRewriter) rewriteHistogramQuantiles(expr *metricsql.FuncExpr, step time.Duration, instant bool) error {
	if len(expr.Args) < 3 {
		return fmt.Errorf("histogram_quantiles requires a label name, at least one quantile, and a histogram vector")
	}
	if expr.KeepMetricNames {
		return fmt.Errorf("histogram_quantiles does not support keep_metric_names")
	}
	label, ok := expr.Args[0].(*metricsql.StringExpr)
	if !ok || !model.LabelName(label.S).IsValid() {
		return fmt.Errorf("histogram_quantiles requires a valid, non-empty label name as its first argument")
	}
	for _, quantile := range expr.Args[1 : len(expr.Args)-1] {
		if _, ok := quantile.(*metricsql.NumberExpr); !ok {
			return fmt.Errorf("histogram_quantiles requires constant numeric quantiles")
		}
	}
	args := make([]metricsql.Expr, 0, len(expr.Args))
	buckets := expr.Args[len(expr.Args)-1]
	if _, numeric := buckets.(*metricsql.NumberExpr); numeric {
		buckets = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{buckets}}
	}
	args = append(args, buckets, label)
	args = append(args, expr.Args[1:len(expr.Args)-1]...)
	expr.Name = metricsQLInternalPrefix + "histogram_quantiles"
	expr.Args = args
	return rewriter.rewriteArgs(expr.Args, step, instant)
}

func metricsQLHistogramQuantiles(vectors []promql.Vector, matrix promql.Matrix, args parser.Expressions, helper *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	result, warnings := promql.FunctionCalls["histogram_quantiles"](vectors, matrix, args, helper)
	labelName := args[1].(*parser.StringLiteral).Val
	builder := labels.NewBuilder(labels.EmptyLabels())
	for index := range result {
		quantile, err := strconv.ParseFloat(result[index].Metric.Get(labelName), 64)
		if err != nil {
			continue
		}
		builder.Reset(result[index].Metric)
		builder.Set(labelName, strconv.FormatFloat(quantile, 'g', -1, 64))
		result[index].Metric = builder.Labels()
	}
	return result, warnings
}

type metricsQLMedianGrouping struct {
	Labels  []string `json:"labels,omitempty"`
	Without bool     `json:"without,omitempty"`
	Limit   int      `json:"limit,omitempty"`
}

// metric returns the labels of the group that a series belongs to.
func (grouping metricsQLMedianGrouping) metric(builder *labels.Builder, metric labels.Labels) labels.Labels {
	builder.Reset(metric)
	if grouping.Without {
		builder.Del(grouping.Labels...)
		builder.Del(labels.MetricName)
	} else {
		builder.Keep(grouping.Labels...)
	}
	return builder.Labels()
}

func (rewriter metricsQLRewriter) rewriteMedian(expr *metricsql.AggrFuncExpr, step time.Duration, instant bool) (metricsql.Expr, error) {
	if len(expr.Args) == 0 {
		return nil, fmt.Errorf("median requires at least one vector argument")
	}
	if expr.Limit > 0 && !instant {
		return nil, fmt.Errorf("median limit is supported only for instant queries outside subqueries")
	}
	if err := rewriter.rewriteArgs(expr.Args, step, instant); err != nil {
		return nil, err
	}
	grouping := metricsQLMedianGrouping{Labels: expr.Modifier.Args, Without: expr.Modifier.Op == "without", Limit: expr.Limit}
	encoded, err := json.Marshal(grouping)
	if err != nil {
		return nil, err
	}
	args := []metricsql.Expr{&metricsql.StringExpr{S: string(encoded)}}
	for index, arg := range expr.Args {
		parsed, err := parser.NewParser(parser.Options{}).ParseExpr(string(arg.AppendString(nil)))
		if err != nil {
			return nil, fmt.Errorf("median argument %d: %w", index+1, err)
		}
		if parsed.Type() == parser.ValueTypeScalar {
			arg = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{arg}}
		}
		args = append(args, arg)
	}
	return &metricsql.FuncExpr{Name: metricsQLInternalPrefix + "median", Args: args}, nil
}

func metricsQLMedian(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, helper *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	var grouping metricsQLMedianGrouping
	if err := json.Unmarshal([]byte(args[0].(*parser.StringLiteral).Val), &grouping); err != nil {
		panic(fmt.Errorf("invalid internal median grouping: %w", err))
	}
	type medianGroup struct {
		metric labels.Labels
		values []float64
	}
	groups := make(map[string]*medianGroup)
	builder := labels.NewBuilder(labels.EmptyLabels())
	var warnings annotations.Annotations
	for index, vector := range vectors[1:] {
		for _, sample := range vector {
			if sample.H != nil {
				warnings.Add(annotations.NewHistogramIgnoredInAggregationInfo("median", args[index+1].PositionRange()))
				continue
			}
			if math.IsNaN(sample.F) {
				continue
			}
			metric := grouping.metric(builder, sample.Metric)
			key := metric.String()
			group := groups[key]
			if group == nil {
				group = &medianGroup{metric: metric}
				groups[key] = group
			}
			group.values = append(group.values, sample.F)
		}
	}
	for _, group := range groups {
		sort.Float64s(group.values)
		middle := len(group.values) / 2
		median := group.values[middle]
		if len(group.values)%2 == 0 {
			lower, upper := group.values[middle-1], median
			median = (lower + upper) / 2
			if math.IsInf(median, 0) && !math.IsInf(lower, 0) && !math.IsInf(upper, 0) {
				median = lower/2 + upper/2
			}
		}
		helper.Out = append(helper.Out, promql.Sample{Metric: group.metric, F: median})
	}
	sort.Slice(helper.Out, func(first, second int) bool {
		return labels.Compare(helper.Out[first].Metric, helper.Out[second].Metric) < 0
	})
	if grouping.Limit > 0 && len(helper.Out) > grouping.Limit {
		helper.Out = helper.Out[:grouping.Limit]
	}
	return helper.Out, warnings
}

// rewriteQuantiles rewrites quantiles("label", phi1, ..., phiN, q) by (...)
// into an internal function that evaluates q once and returns one series for
// each phi and group, labelled with the phi.
// Reference: aggrFuncQuantiles in VictoriaMetrics app/vmselect/promql/aggr.go.
func (rewriter metricsQLRewriter) rewriteQuantiles(expr *metricsql.AggrFuncExpr, step time.Duration, instant bool) (metricsql.Expr, error) {
	if len(expr.Args) < 3 {
		return nil, fmt.Errorf("quantiles requires a label name, at least one phi, and a series argument")
	}
	label, ok := expr.Args[0].(*metricsql.StringExpr)
	if !ok || !model.LabelName(label.S).IsValid() {
		return nil, fmt.Errorf("quantiles requires a valid, non-empty label name as its first argument")
	}
	phis := expr.Args[1 : len(expr.Args)-1]
	for _, phi := range phis {
		if _, ok := phi.(*metricsql.NumberExpr); !ok {
			return nil, fmt.Errorf("quantiles requires constant numeric phis")
		}
	}
	if expr.Limit > 0 && !instant {
		return nil, fmt.Errorf("quantiles limit is supported only for instant queries outside subqueries")
	}
	series := expr.Args[len(expr.Args)-1]
	if err := rewriter.rewrite(&series, step, instant); err != nil {
		return nil, err
	}
	if _, numeric := series.(*metricsql.NumberExpr); numeric {
		series = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{series}}
	}
	grouping := metricsQLMedianGrouping{Labels: expr.Modifier.Args, Without: expr.Modifier.Op == "without", Limit: expr.Limit}
	encoded, err := json.Marshal(grouping)
	if err != nil {
		return nil, err
	}
	args := []metricsql.Expr{&metricsql.StringExpr{S: string(encoded)}, label, series}
	args = append(args, phis...)
	return &metricsql.FuncExpr{Name: metricsQLInternalPrefix + "quantiles", Args: args}, nil
}

func metricsQLQuantiles(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, helper *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	var grouping metricsQLMedianGrouping
	if err := json.Unmarshal([]byte(args[0].(*parser.StringLiteral).Val), &grouping); err != nil {
		panic(fmt.Errorf("invalid internal quantiles grouping: %w", err))
	}
	label := args[1].(*parser.StringLiteral).Val
	phis := make([]float64, 0, len(vectors)-3)
	for _, vector := range vectors[3:] {
		phis = append(phis, vector[0].F)
	}
	type quantilesGroup struct {
		metric labels.Labels
		values []float64
	}
	groups := make(map[string]*quantilesGroup)
	var order []*quantilesGroup
	builder := labels.NewBuilder(labels.EmptyLabels())
	var warnings annotations.Annotations
	for _, sample := range vectors[2] {
		if sample.H != nil {
			warnings.Add(annotations.NewHistogramIgnoredInAggregationInfo("quantiles", args[2].PositionRange()))
			continue
		}
		if math.IsNaN(sample.F) {
			continue
		}
		metric := grouping.metric(builder, sample.Metric)
		key := metric.String()
		group := groups[key]
		if group == nil {
			group = &quantilesGroup{metric: metric}
			groups[key] = group
			order = append(order, group)
		}
		group.values = append(group.values, sample.F)
	}
	sort.Slice(order, func(first, second int) bool {
		return labels.Compare(order[first].metric, order[second].metric) < 0
	})
	if grouping.Limit > 0 && len(order) > grouping.Limit {
		order = order[:grouping.Limit]
	}
	for _, group := range order {
		sort.Float64s(group.values)
		for _, phi := range phis {
			builder.Reset(group.metric)
			builder.Set(label, fmt.Sprintf("%g", phi))
			helper.Out = append(helper.Out, promql.Sample{Metric: builder.Labels(), F: metricsQLQuantileSorted(phi, group.values)})
		}
	}
	return helper.Out, warnings
}

// metricsQLQuantileSorted interpolates a quantile over sorted values, as
// Prometheus and VictoriaMetrics do.
func metricsQLQuantileSorted(phi float64, values []float64) float64 {
	switch {
	case len(values) == 0 || math.IsNaN(phi):
		return math.NaN()
	case phi < 0:
		return math.Inf(-1)
	case phi > 1:
		return math.Inf(1)
	}
	rank := phi * float64(len(values)-1)
	lower := math.Max(0, math.Floor(rank))
	upper := math.Min(float64(len(values)-1), lower+1)
	weight := rank - math.Floor(rank)
	return values[int(lower)]*(1-weight) + values[int(upper)]*weight
}
