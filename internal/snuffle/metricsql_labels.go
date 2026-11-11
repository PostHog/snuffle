package snuffle

import (
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

// metricsQLLabelFunctions are MetricsQL label functions that the Prometheus
// engine lacks, or implements with different semantics. The rewriter renames
// each to an internal function.
// Reference: VictoriaMetrics app/vmselect/promql/transform.go.
var metricsQLLabelFunctions = map[string]struct {
	args     []parser.ValueType
	variadic int
	call     promql.FunctionCall
}{
	"label_match":    {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString, parser.ValueTypeString}, 0, metricsQLLabelMatch(true)},
	"label_mismatch": {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString, parser.ValueTypeString}, 0, metricsQLLabelMatch(false)},
	"label_value":    {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, 0, metricsQLLabelValue},
	// Prometheus marks sort_by_label experimental and sorts naturally.
	// VictoriaMetrics compares the values as bytes.
	"sort_by_label":              {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLSortByLabel(stringLess, false)},
	"sort_by_label_desc":         {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLSortByLabel(stringLess, true)},
	"sort_by_label_numeric":      {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLSortByLabel(metricsQLNumericLess, false)},
	"sort_by_label_numeric_desc": {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLSortByLabel(metricsQLNumericLess, true)},
}

func init() {
	for name, function := range metricsQLLabelFunctions {
		internalName := metricsQLInternalPrefix + name
		parser.Functions[internalName] = &parser.Function{Name: internalName, ArgTypes: function.args, Variadic: function.variadic, ReturnType: parser.ValueTypeVector}
		promql.FunctionCalls[internalName] = function.call
	}
}

// rewriteLabelFunction renames a MetricsQL label function to its internal
// function. It reports false for other functions.
func rewriteLabelFunction(e *metricsql.FuncExpr) (bool, error) {
	if _, ok := metricsQLLabelFunctions[e.Name]; !ok {
		return false, nil
	}
	if e.KeepMetricNames {
		return true, fmt.Errorf("%s does not support keep_metric_names", e.Name)
	}
	if function := metricsQLLabelFunctions[e.Name]; function.variadic != 0 && len(e.Args) < len(function.args) {
		return true, fmt.Errorf("%s requires a series argument and at least one label name", e.Name)
	}
	if e.Name == "label_match" || e.Name == "label_mismatch" {
		if len(e.Args) != 3 {
			return true, fmt.Errorf("%s requires a series argument, a label name and a regexp", e.Name)
		}
		pattern, ok := e.Args[2].(*metricsql.StringExpr)
		if !ok {
			return true, fmt.Errorf("%s requires a constant regexp", e.Name)
		}
		if _, err := metricsql.CompileRegexpAnchored(pattern.S); err != nil {
			return true, fmt.Errorf("%s: cannot compile regexp %q: %w", e.Name, pattern.S, err)
		}
	}
	if len(e.Args) == 0 {
		return true, fmt.Errorf("%s requires a series argument", e.Name)
	}
	for _, arg := range e.Args[1:] {
		if _, ok := arg.(*metricsql.StringExpr); !ok {
			return true, fmt.Errorf("%s requires constant string arguments after the series", e.Name)
		}
	}
	// MetricsQL accepts a number as the series, as other functions do.
	if _, numeric := e.Args[0].(*metricsql.NumberExpr); numeric {
		e.Args[0] = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{e.Args[0]}}
	}
	e.Name = metricsQLInternalPrefix + e.Name
	return true, nil
}

// metricsQLLabelString reads a label like VictoriaMetrics: a function that
// drops the metric name has no __name__ label.
func metricsQLLabelString(sample promql.Sample, name string) string {
	if name == labels.MetricName && sample.DropName {
		return ""
	}
	return sample.Metric.Get(name)
}

// metricsQLLabelMatch keeps the series whose label matches (or, for
// label_mismatch, does not match) the anchored regexp.
func metricsQLLabelMatch(keepMatches bool) promql.FunctionCall {
	return func(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		label := args[1].(*parser.StringLiteral).Val
		pattern, err := metricsql.CompileRegexpAnchored(args[2].(*parser.StringLiteral).Val)
		if err != nil {
			// The rewriter compiles the regexp before evaluation.
			panic(fmt.Errorf("invalid internal label_match regexp: %w", err))
		}
		for _, sample := range vectors[0] {
			if pattern.MatchString(metricsQLLabelString(sample, label)) == keepMatches {
				enh.Out = append(enh.Out, sample)
			}
		}
		return enh.Out, nil
	}
}

// metricsQLLabelValue replaces each sample with the numeric value of a label.
// A sample whose label does not parse, or whose input is NaN, gives no output,
// so parent aggregates ignore it as they do in VictoriaMetrics.
func metricsQLLabelValue(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	label := args[1].(*parser.StringLiteral).Val
	for _, sample := range vectors[0] {
		if math.IsNaN(sample.F) {
			continue
		}
		// VictoriaMetrics resets the metric name before the lookup.
		text := ""
		if label != labels.MetricName {
			text = sample.Metric.Get(label)
		}
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			continue
		}
		enh.Out = append(enh.Out, promql.Sample{Metric: sample.Metric, F: value, DropName: true})
	}
	return enh.Out, nil
}

// metricsQLSortByLabel orders an instant vector by label values. Series that
// tie on every label keep their input order. Range results keep the
// engine's label order, as they do for the Prometheus sort functions.
func metricsQLSortByLabel(less func(a, b string) bool, descending bool) promql.FunctionCall {
	return func(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, _ *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		names := make([]string, 0, len(args)-1)
		for _, arg := range args[1:] {
			names = append(names, arg.(*parser.StringLiteral).Val)
		}
		slices.SortStableFunc(vectors[0], func(a, b promql.Sample) int {
			for _, name := range names {
				first, second := metricsQLLabelString(a, name), metricsQLLabelString(b, name)
				if first == second {
					continue
				}
				if descending {
					first, second = second, first
				}
				if less(first, second) {
					return -1
				}
				return 1
			}
			return 0
		})
		return vectors[0], nil
	}
}

func stringLess(a, b string) bool { return a < b }

// metricsQLNumericLess compares strings with embedded numbers by their
// numeric value. It is a port of numericLess in VictoriaMetrics.
func metricsQLNumericLess(a, b string) bool {
	for {
		if len(b) == 0 {
			return false
		}
		if len(a) == 0 {
			return true
		}
		aPrefix, bPrefix := numericPrefix(a), numericPrefix(b)
		a, b = a[len(aPrefix):], b[len(bPrefix):]
		if len(aPrefix) > 0 || len(bPrefix) > 0 {
			if len(aPrefix) == 0 {
				return false
			}
			if len(bPrefix) == 0 {
				return true
			}
			aNumber, _ := strconv.ParseFloat(aPrefix, 64)
			bNumber, _ := strconv.ParseFloat(bPrefix, 64)
			if aNumber != bNumber {
				return aNumber < bNumber
			}
		}
		aPrefix, bPrefix = nonNumericPrefix(a), nonNumericPrefix(b)
		a, b = a[len(aPrefix):], b[len(bPrefix):]
		if aPrefix != bPrefix {
			return aPrefix < bPrefix
		}
	}
}

func numericPrefix(s string) string {
	i := 0
	if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
		i++
	}
	hasNumber, hasDot := false, false
	for ; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			hasNumber = true
			continue
		}
		if !hasDot && s[i] == '.' {
			hasDot = true
			continue
		}
		if !hasNumber {
			return ""
		}
		return s[:i]
	}
	if !hasNumber {
		return ""
	}
	return s
}

func nonNumericPrefix(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return s[:i]
		}
	}
	return s
}
