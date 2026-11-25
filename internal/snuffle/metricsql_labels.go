package snuffle

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

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
	"label_copy":                 {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLLabelCopy(false)},
	"label_move":                 {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLLabelCopy(true)},
	"label_set":                  {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString}, -1, metricsQLLabelSet},
	"label_transform":            {[]parser.ValueType{parser.ValueTypeVector, parser.ValueTypeString, parser.ValueTypeString, parser.ValueTypeString}, 0, metricsQLLabelTransform},
	// ponytail: per step, VictoriaMetrics looks at every series in the range.
	// The results differ only when the series set changes within the range.
	"drop_common_labels": {[]parser.ValueType{parser.ValueTypeVector}, -1, metricsQLDropCommonLabels},
}

// metricsQLUnionName is the internal function for union(a, b) and the bare
// tuple (a, b), which MetricsQL parses as a function with no name.
const metricsQLUnionName = metricsQLInternalPrefix + "union"

func init() {
	for name, function := range metricsQLLabelFunctions {
		internalName := metricsQLInternalPrefix + name
		parser.Functions[internalName] = &parser.Function{Name: internalName, ArgTypes: function.args, Variadic: function.variadic, ReturnType: parser.ValueTypeVector}
		promql.FunctionCalls[internalName] = function.call
	}
	parser.Functions[metricsQLUnionName] = &parser.Function{Name: metricsQLUnionName, ArgTypes: []parser.ValueType{parser.ValueTypeVector}, Variadic: -1, ReturnType: parser.ValueTypeVector}
	promql.FunctionCalls[metricsQLUnionName] = metricsQLUnion
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
	if len(e.Args) == 0 {
		return true, fmt.Errorf("%s requires a series argument", e.Name)
	}
	if e.Name == "drop_common_labels" {
		for i, arg := range e.Args {
			e.Args[i] = numberAsVector(arg)
		}
		e.Name = metricsQLInternalPrefix + e.Name
		return true, nil
	}
	for _, arg := range e.Args[1:] {
		if _, ok := arg.(*metricsql.StringExpr); !ok {
			return true, fmt.Errorf("%s requires constant string arguments after the series", e.Name)
		}
	}
	switch e.Name {
	case "label_match", "label_mismatch", "label_transform":
		compile, want := metricsql.CompileRegexpAnchored, 3
		if e.Name == "label_transform" {
			// label_transform replaces every match, so its regexp is unanchored.
			compile, want = metricsql.CompileRegexp, 4
		}
		if len(e.Args) != want {
			return true, fmt.Errorf("%s requires %d arguments", e.Name, want)
		}
		pattern := e.Args[2].(*metricsql.StringExpr).S
		if _, err := compile(pattern); err != nil {
			return true, fmt.Errorf("%s: cannot compile regexp %q: %w", e.Name, pattern, err)
		}
	case "label_copy", "label_move", "label_set":
		if len(e.Args)%2 == 0 {
			return true, fmt.Errorf("%s requires label name pairs after the series", e.Name)
		}
	default:
		if function := metricsQLLabelFunctions[e.Name]; function.variadic != 0 && len(e.Args) < len(function.args) {
			return true, fmt.Errorf("%s requires a series argument and at least one label name", e.Name)
		}
	}
	// MetricsQL accepts a number as the series, as other functions do.
	if _, numeric := e.Args[0].(*metricsql.NumberExpr); numeric {
		e.Args[0] = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{e.Args[0]}}
	}
	e.Name = metricsQLInternalPrefix + e.Name
	return true, nil
}

// rewriteUnion prepares union(a, b, ...) and the bare tuple (a, b, ...). A
// string argument is a NaN series named after the string in VictoriaMetrics,
// which no result keeps, so it is dropped. A number is a series without labels.
func rewriteUnion(e *metricsql.FuncExpr) (metricsql.Expr, error) {
	if e.KeepMetricNames {
		return nil, fmt.Errorf("union does not support keep_metric_names")
	}
	var args []metricsql.Expr
	numbers := 0
	for _, arg := range e.Args {
		switch arg.(type) {
		case *metricsql.StringExpr:
			continue
		case *metricsql.NumberExpr:
			numbers++
		}
		args = append(args, numberAsVector(arg))
	}
	switch {
	case numbers > 1 && numbers == len(e.Args):
		// Each number is a series without labels, so VictoriaMetrics fails
		// with duplicate output series too.
		return nil, fmt.Errorf("a list of numbers gives duplicate output series")
	case len(args) == 0:
		return numberAsVector(&metricsql.NumberExpr{N: math.NaN()}), nil
	case len(args) == 1:
		return args[0], nil
	}
	return &metricsql.FuncExpr{Name: metricsQLUnionName, Args: args}, nil
}

// metricsQLLabelString reads a label like VictoriaMetrics: a function that
// drops the metric name has no __name__ label.
func metricsQLLabelString(sample promql.Sample, name string) string {
	if name == labels.MetricName && sample.DropName {
		return ""
	}
	return sample.Metric.Get(name)
}

// metricsQLLabelBuilder starts a label edit of a sample. A series whose
// metric name is dropped has no __name__, as in VictoriaMetrics. A histogram
// pushdown name replaces a rollup, which drops its name, so it is dropped too.
func metricsQLLabelBuilder(builder *labels.Builder, sample promql.Sample) *labels.Builder {
	builder.Reset(sample.Metric)
	if sample.DropName || strings.HasPrefix(sample.Metric.Get(labels.MetricName), histogramPushdownPrefix) {
		builder.Del(labels.MetricName)
	}
	return builder
}

func stringArgs(args parser.Expressions) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		out = append(out, arg.(*parser.StringLiteral).Val)
	}
	return out
}

// metricsQLLabelCopy copies (or, for label_move, moves) the label pairs
// src1, dst1, ... A missing source label leaves the destination unchanged.
func metricsQLLabelCopy(move bool) promql.FunctionCall {
	return func(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		pairs := stringArgs(args[1:])
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, sample := range vectors[0] {
			metricsQLLabelBuilder(builder, sample)
			for i := 0; i+1 < len(pairs); i += 2 {
				src, dst := pairs[i], pairs[i+1]
				value := builder.Get(src)
				if value == "" {
					continue
				}
				builder.Set(dst, value)
				if move && src != dst {
					builder.Del(src)
				}
			}
			sample.Metric, sample.DropName = builder.Labels(), false
			enh.Out = append(enh.Out, sample)
		}
		return enh.Out, nil
	}
}

// metricsQLLabelSet sets the label pairs name1, value1, ... An empty value
// removes the label.
func metricsQLLabelSet(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	pairs := stringArgs(args[1:])
	builder := labels.NewBuilder(labels.EmptyLabels())
	for _, sample := range vectors[0] {
		metricsQLLabelBuilder(builder, sample)
		for i := 0; i+1 < len(pairs); i += 2 {
			builder.Set(pairs[i], pairs[i+1])
		}
		sample.Metric, sample.DropName = builder.Labels(), false
		enh.Out = append(enh.Out, sample)
	}
	return enh.Out, nil
}

// metricsQLLabelTransform replaces every match of an unanchored regexp in a
// label value. A label whose value does not match is unchanged, and an empty
// result removes the label.
func metricsQLLabelTransform(vectors []promql.Vector, _ promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	values := stringArgs(args[1:])
	label, replacement := values[0], values[2]
	pattern, err := metricsql.CompileRegexp(values[1])
	if err != nil {
		// The rewriter compiles the regexp before evaluation.
		panic(fmt.Errorf("invalid internal label_transform regexp: %w", err))
	}
	builder := labels.NewBuilder(labels.EmptyLabels())
	for _, sample := range vectors[0] {
		metricsQLLabelBuilder(builder, sample)
		if value := builder.Get(label); pattern.MatchString(value) {
			replaced, ok := replaceAllLimited(pattern, value, replacement, metricsQLMaxLabelValueBytes)
			if !ok {
				panic(fmt.Errorf("label_transform: the value of label %q exceeds %d bytes", label, metricsQLMaxLabelValueBytes))
			}
			builder.Set(label, replaced)
		}
		sample.Metric, sample.DropName = builder.Labels(), false
		enh.Out = append(enh.Out, sample)
	}
	return enh.Out, nil
}

// metricsQLMaxLabelValueBytes caps a label_transform result. Nested
// unanchored replacements multiply the value length at each level.
// ponytail: one fixed cap; make it configurable if real labels need more.
const metricsQLMaxLabelValueBytes = 64 << 10

// replaceAllLimited is regexp.ReplaceAllString that stops before the result
// exceeds limit bytes. Submatches lie within their match, so each expansion
// adds at most len(replacement) plus the match length per "$" reference.
func replaceAllLimited(pattern *regexp.Regexp, value, replacement string, limit int) (string, bool) {
	references := strings.Count(replacement, "$")
	var out []byte
	last := 0
	for _, match := range pattern.FindAllStringSubmatchIndex(value, -1) {
		if len(out)+match[0]-last+len(replacement)+references*(match[1]-match[0]) > limit {
			return "", false
		}
		out = append(out, value[last:match[0]]...)
		out = pattern.ExpandString(out, replacement, value, match)
		last = match[1]
	}
	out = append(out, value[last:]...)
	return string(out), len(out) <= limit
}

// metricsQLDropCommonLabels joins its arguments and removes each label that
// has the same value on every series.
func metricsQLDropCommonLabels(vectors []promql.Vector, _ promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	builder := labels.NewBuilder(labels.EmptyLabels())
	for _, vector := range vectors {
		for _, sample := range vector {
			sample.Metric, sample.DropName = metricsQLLabelBuilder(builder, sample).Labels(), false
			enh.Out = append(enh.Out, sample)
		}
	}
	counts := map[labels.Label]int{}
	for _, sample := range enh.Out {
		sample.Metric.Range(func(label labels.Label) { counts[label]++ })
	}
	var common []string
	for label, count := range counts {
		if count == len(enh.Out) {
			common = append(common, label.Name)
		}
	}
	if len(common) == 0 {
		return enh.Out, nil
	}
	for i := range enh.Out {
		builder.Reset(enh.Out[i].Metric)
		enh.Out[i].Metric = builder.Del(common...).Labels()
	}
	return enh.Out, nil
}

// metricsQLUnion keeps the first series with each label set, in argument
// order. ponytail: per step; VictoriaMetrics picks the first series for the
// whole range, so a later argument cannot fill gaps in an earlier one there.
func metricsQLUnion(vectors []promql.Vector, _ promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	builder := labels.NewBuilder(labels.EmptyLabels())
	seen := map[string]bool{}
	for _, vector := range vectors {
		for _, sample := range vector {
			key := metricsQLLabelBuilder(builder, sample).Labels().String()
			if seen[key] {
				continue
			}
			seen[key] = true
			enh.Out = append(enh.Out, sample)
		}
	}
	return enh.Out, nil
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
