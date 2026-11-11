package snuffle

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

const metricsQLKeepMetricNamesName = metricsQLInternalPrefix + "keep_metric_names"

func init() {
	parser.Functions[metricsQLKeepMetricNamesName] = &parser.Function{Name: metricsQLKeepMetricNamesName, ArgTypes: []parser.ValueType{parser.ValueTypeVector}, ReturnType: parser.ValueTypeVector}
	// The engine removes metric names after evaluation, so the samples still
	// carry the name of their input series. Clearing DropName keeps it.
	promql.FunctionCalls[metricsQLKeepMetricNamesName] = func(vectors []promql.Vector, _ promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		for _, sample := range vectors[0] {
			sample.DropName = false
			enh.Out = append(enh.Out, sample)
		}
		return enh.Out, nil
	}
}

// rewriteKeepMetricNames evaluates f(...) keep_metric_names as f(...) inside
// an internal function that keeps the metric names of the result.
// The name of each result series is that of its input series, so every
// series argument must be a selector: after another function or an operator
// VictoriaMetrics would keep an empty name, which the engine cannot tell.
func (r metricsQLRewriter) rewriteKeepMetricNames(expr *metricsql.Expr, e *metricsql.FuncExpr, step time.Duration, instant bool) error {
	for _, arg := range e.Args {
		if !metricsQLSelectorArg(arg) {
			return fmt.Errorf("%s: keep_metric_names is supported only on series selectors", e.Name)
		}
	}
	e.KeepMetricNames = false
	if err := r.rewriteFunc(e, step, instant); err != nil {
		return err
	}
	*expr = &metricsql.FuncExpr{Name: metricsQLKeepMetricNamesName, Args: []metricsql.Expr{e}}
	return nil
}

// metricsQLSelectorArg reports whether a function argument is a constant or
// a series selector, possibly with a window or the implicit default_rollup.
func metricsQLSelectorArg(arg metricsql.Expr) bool {
	switch a := arg.(type) {
	case *metricsql.NumberExpr, *metricsql.StringExpr, *metricsql.MetricExpr:
		return true
	case *metricsql.RollupExpr:
		_, raw := a.Expr.(*metricsql.MetricExpr)
		return raw && a.Step == nil
	case *metricsql.FuncExpr:
		return a.Name == "default_rollup" && len(a.Args) == 1 && !a.KeepMetricNames && metricsQLSelectorArg(a.Args[0])
	}
	return false
}
