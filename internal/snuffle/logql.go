package snuffle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"
)

type logQLExpr struct {
	logSelector *logQLSelector
	rangeAgg    *logQLRangeAggregation
	aggregation *logQLAggregation
	topK        *logQLTopK
	labelFunc   *logQLLabelFunction
	binaryOp    *logQLBinaryOp
	scalar      *float64
	vector      *float64
	comparison  *logQLComparison
}

type logQLSelector struct {
	matchers []logQLLabelMatcher
	stages   []logQLStage
}

type logQLRangeAggregation struct {
	fn       string
	selector logQLSelector
	window   time.Duration
	offset   time.Duration
	grouping *logQLGrouping
	param    float64
}

type logQLAggregation struct {
	fn       string
	grouping *logQLGrouping
	expr     *logQLExpr
}

type logQLTopK struct {
	fn    string
	k     int
	expr  *logQLExpr
	isTop bool
}

type logQLLabelFunction struct {
	fn          string
	expr        *logQLExpr
	dstLabel    string
	replacement string
	srcLabel    string
	regex       *regexp.Regexp
	separator   string
	srcLabels   []string
}

type logQLBinaryOp struct {
	op           string
	lhs          *logQLExpr
	rhs          *logQLExpr
	boolModifier bool
	matching     *logQLVectorMatching
}

type logQLVectorMatching struct {
	on      bool
	labels  []string
	group   string
	include []string
}

type logQLGrouping struct {
	without bool
	labels  []string
}

type logQLComparison struct {
	op    string
	value float64
}

type logQLLabelMatcher struct {
	name  string
	op    string
	value string
	re    *regexp.Regexp
}

type logQLLineFilter struct {
	op    string
	value string
	re    *regexp.Regexp
}

type logQLLabelFilter struct {
	name      string
	op        string
	value     string
	numeric   bool
	numValue  float64
	valueType string
	re        *regexp.Regexp
	connector string
}

type logQLStage struct {
	kind         string
	lineFilter   *logQLLineFilter
	labelFilters []logQLLabelFilter
	parser       string
	parserParam  string
	lineFormat   string
	labelFormats []logQLLabelFormat
	dropLabels   []string
	keepLabels   []string
	unwrapLabel  string
	unwrapFunc   string
}

type logQLJSONPathToken struct {
	key     string
	index   int
	isIndex bool
}

type logQLLabelFormat struct {
	name       string
	sourceName string
	constValue string
	isConst    bool
	isTemplate bool
}

type logRow struct {
	tsNS         int64
	observedNS   int64
	line         string
	labels       map[string]string
	streamLabels map[string]string
	fields       map[string]string
	unwrap       float64
	haveUnwrap   bool
}

type logStreamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type logMetricVectorResult struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
}

type logMetricMatrixResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

func parseLogQL(input string) (*logQLExpr, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, errors.New("missing query")
	}
	return parseLogQLBinary(input, 1)
}

func parseLogQLBinary(input string, minPrec int) (*logQLExpr, error) {
	input, minPrec = trimLogQLOuterParensForBinary(strings.TrimSpace(input), minPrec)
	opStart, opEnd, op, prec, ok := findTopLevelLogQLBinaryOp(input, minPrec)
	if ok {
		matching, boolModifier, rhsText, err := parseLogQLBinaryModifiers(input[opEnd:], isLogQLComparisonOp(op))
		if err != nil {
			return nil, err
		}
		lhs, err := parseLogQLBinary(input[:opStart], prec+1)
		if err != nil {
			return nil, err
		}
		rhs, err := parseLogQLBinary(rhsText, prec+1)
		if err != nil {
			return nil, err
		}
		return &logQLExpr{binaryOp: &logQLBinaryOp{op: op, lhs: lhs, rhs: rhs, boolModifier: boolModifier, matching: matching}}, nil
	}
	return parseLogQLPrimary(input)
}

func parseLogQLPrimary(input string) (*logQLExpr, error) {
	input = trimLogQLOuterParens(strings.TrimSpace(input))
	if strings.HasPrefix(input, "{") {
		selector, err := parseLogQLSelector(input)
		if err != nil {
			return nil, err
		}
		return &logQLExpr{logSelector: selector}, nil
	}
	if scalar, ok := parseLogQLScalar(input); ok {
		return &logQLExpr{scalar: &scalar}, nil
	}
	name, rest := readIdentifier(input)
	switch name {
	case "count_over_time", "rate", "bytes_over_time", "bytes_rate", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "first_over_time", "last_over_time", "stdvar_over_time", "stddev_over_time", "absent_over_time":
		expr, tail, err := parseLogQLRangeAggregation(name, rest)
		if err != nil {
			return nil, err
		}
		if tail != "" {
			return nil, fmt.Errorf("unexpected trailing LogQL input %q", tail)
		}
		return expr, nil
	case "sum", "min", "max", "avg", "count", "stddev", "stdvar":
		return parseLogQLAggregation(name, rest)
	case "topk", "bottomk":
		return parseLogQLTopK(name, rest)
	case "vector":
		return parseLogQLVector(rest)
	case "label_replace":
		return parseLogQLLabelReplace(rest)
	case "label_join":
		return parseLogQLLabelJoin(rest)
	case "quantile_over_time":
		expr, tail, err := parseLogQLQuantileOverTime(rest)
		if err != nil {
			return nil, err
		}
		if tail != "" {
			return nil, fmt.Errorf("unexpected trailing LogQL input %q", tail)
		}
		return expr, nil
	default:
		return nil, fmt.Errorf("unsupported LogQL expression %q", input)
	}
}

func parseLogQLVector(rest string) (*logQLExpr, error) {
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, err
	}
	if tail != "" {
		return nil, fmt.Errorf("unexpected vector tail %q", tail)
	}
	value, ok := parseLogQLScalar(inner)
	if !ok {
		return nil, fmt.Errorf("vector expects a scalar argument, got %q", strings.TrimSpace(inner))
	}
	return &logQLExpr{vector: &value}, nil
}

func parseLogQLLabelReplace(rest string) (*logQLExpr, error) {
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, err
	}
	if tail != "" {
		return nil, fmt.Errorf("unexpected label_replace tail %q", tail)
	}
	parts := splitTopLevel(inner, ',')
	if len(parts) != 5 {
		return nil, errors.New("label_replace expects an expression, destination label, replacement, source label, and regex")
	}
	child, err := parseLogQL(parts[0])
	if err != nil {
		return nil, err
	}
	dstLabel, err := unquoteLogQLString(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}
	replacement, err := unquoteLogQLString(strings.TrimSpace(parts[2]))
	if err != nil {
		return nil, err
	}
	srcLabel, err := unquoteLogQLString(strings.TrimSpace(parts[3]))
	if err != nil {
		return nil, err
	}
	pattern, err := unquoteLogQLString(strings.TrimSpace(parts[4]))
	if err != nil {
		return nil, err
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid label_replace regex %q: %w", pattern, err)
	}
	return &logQLExpr{labelFunc: &logQLLabelFunction{
		fn:          "label_replace",
		expr:        child,
		dstLabel:    dstLabel,
		replacement: replacement,
		srcLabel:    srcLabel,
		regex:       re,
	}}, nil
}

func parseLogQLLabelJoin(rest string) (*logQLExpr, error) {
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, err
	}
	if tail != "" {
		return nil, fmt.Errorf("unexpected label_join tail %q", tail)
	}
	parts := splitTopLevel(inner, ',')
	if len(parts) < 4 {
		return nil, errors.New("label_join expects an expression, destination label, separator, and source labels")
	}
	child, err := parseLogQL(parts[0])
	if err != nil {
		return nil, err
	}
	dstLabel, err := unquoteLogQLString(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}
	separator, err := unquoteLogQLString(strings.TrimSpace(parts[2]))
	if err != nil {
		return nil, err
	}
	srcLabels := make([]string, 0, len(parts)-3)
	for _, part := range parts[3:] {
		label, err := unquoteLogQLString(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		srcLabels = append(srcLabels, label)
	}
	return &logQLExpr{labelFunc: &logQLLabelFunction{
		fn:        "label_join",
		expr:      child,
		dstLabel:  dstLabel,
		separator: separator,
		srcLabels: srcLabels,
	}}, nil
}

func parseLogQLQuantileOverTime(rest string) (*logQLExpr, string, error) {
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, "", err
	}
	parts := splitTopLevel(inner, ',')
	if len(parts) != 2 {
		return nil, "", errors.New("quantile_over_time expects a quantile and a range selector")
	}
	param, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil || param < 0 || param > 1 {
		return nil, "", fmt.Errorf("invalid quantile_over_time quantile %q", strings.TrimSpace(parts[0]))
	}
	selector, window, offset, err := parseLogQLRangeSelector(parts[1])
	if err != nil {
		return nil, "", err
	}
	grouping, tail, err := parseOptionalGrouping(strings.TrimSpace(tail))
	if err != nil {
		return nil, "", err
	}
	comparison, tail, err := parseOptionalComparison(strings.TrimSpace(tail))
	if err != nil {
		return nil, "", err
	}
	return &logQLExpr{
		rangeAgg:   &logQLRangeAggregation{fn: "quantile_over_time", selector: *selector, window: window, offset: offset, grouping: grouping, param: param},
		comparison: comparison,
	}, strings.TrimSpace(tail), nil
}

func parseLogQLAggregation(name, rest string) (*logQLExpr, error) {
	grouping, rest, err := parseOptionalGrouping(strings.TrimSpace(rest))
	if err != nil {
		return nil, err
	}
	inner, tail, err := parseParenthesized(rest)
	if err != nil {
		return nil, err
	}
	child, err := parseLogQL(inner)
	if err != nil {
		return nil, err
	}
	if grouping == nil {
		grouping, tail, err = parseOptionalGrouping(strings.TrimSpace(tail))
		if err != nil {
			return nil, err
		}
	}
	comparison, tail, err := parseOptionalComparison(strings.TrimSpace(tail))
	if err != nil {
		return nil, err
	}
	if tail != "" {
		return nil, fmt.Errorf("unexpected trailing LogQL input %q", tail)
	}
	return &logQLExpr{
		aggregation: &logQLAggregation{fn: name, grouping: grouping, expr: child},
		comparison:  comparison,
	}, nil
}

func parseLogQLTopK(name, rest string) (*logQLExpr, error) {
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, err
	}
	parts := splitTopLevel(inner, ',')
	if len(parts) != 2 {
		return nil, fmt.Errorf("%s expects a limit and an expression", name)
	}
	k, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || k < 0 {
		return nil, fmt.Errorf("invalid %s limit %q", name, strings.TrimSpace(parts[0]))
	}
	child, err := parseLogQL(parts[1])
	if err != nil {
		return nil, err
	}
	comparison, tail, err := parseOptionalComparison(strings.TrimSpace(tail))
	if err != nil {
		return nil, err
	}
	if tail != "" {
		return nil, fmt.Errorf("unexpected trailing LogQL input %q", tail)
	}
	return &logQLExpr{
		topK:       &logQLTopK{fn: name, k: k, expr: child, isTop: name == "topk"},
		comparison: comparison,
	}, nil
}

func parseLogQLRangeAggregation(name, rest string) (*logQLExpr, string, error) {
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, "", err
	}
	selector, window, offset, err := parseLogQLRangeSelector(inner)
	if err != nil {
		return nil, "", err
	}
	grouping, tail, err := parseOptionalGrouping(strings.TrimSpace(tail))
	if err != nil {
		return nil, "", err
	}
	comparison, tail, err := parseOptionalComparison(strings.TrimSpace(tail))
	if err != nil {
		return nil, "", err
	}
	return &logQLExpr{
		rangeAgg:   &logQLRangeAggregation{fn: name, selector: *selector, window: window, offset: offset, grouping: grouping},
		comparison: comparison,
	}, strings.TrimSpace(tail), nil
}

func parseLogQLRangeSelector(input string) (*logQLSelector, time.Duration, time.Duration, error) {
	open, close := findLastRange(input)
	if open < 0 || close < 0 {
		return nil, 0, 0, errors.New("range aggregation requires a selector range like [5m]")
	}
	window, err := parseLogQLDuration(strings.TrimSpace(input[open+1 : close]))
	if err != nil {
		return nil, 0, 0, err
	}
	tail := strings.TrimSpace(input[close+1:])
	var offset time.Duration
	if tail != "" {
		name, rest := readIdentifier(tail)
		if name != "offset" {
			return nil, 0, 0, fmt.Errorf("unexpected range selector tail %q", tail)
		}
		offset, err = parseLogQLDuration(strings.TrimSpace(rest))
		if err != nil {
			return nil, 0, 0, err
		}
	}
	selector, err := parseLogQLSelector(strings.TrimSpace(input[:open]))
	if err != nil {
		return nil, 0, 0, err
	}
	return selector, window, offset, nil
}

func parseLogQLSelector(input string) (*logQLSelector, error) {
	input = strings.TrimSpace(input)
	if !strings.HasPrefix(input, "{") {
		return nil, fmt.Errorf("log stream selector must start with {")
	}
	close := findMatching(input, 0, '{', '}')
	if close < 0 {
		return nil, errors.New("unterminated log stream selector")
	}
	matchers, err := parseLogQLMatchers(input[1:close])
	if err != nil {
		return nil, err
	}
	stages, err := parseLogQLPipeline(input[close+1:])
	if err != nil {
		return nil, err
	}
	return &logQLSelector{matchers: matchers, stages: stages}, nil
}

func parseLogQLMatchers(input string) ([]logQLLabelMatcher, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, nil
	}
	parts := splitTopLevel(input, ',')
	matchers := make([]logQLLabelMatcher, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, op, raw, err := parseMatcherPart(part)
		if err != nil {
			return nil, err
		}
		value, err := unquoteLogQLString(raw)
		if err != nil {
			return nil, err
		}
		m := logQLLabelMatcher{name: name, op: op, value: value}
		if op == "=~" || op == "!~" {
			m.re, err = regexp.Compile("^(?:" + value + ")$")
			if err != nil {
				return nil, fmt.Errorf("invalid label regex for %s: %w", name, err)
			}
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}

func parseMatcherPart(part string) (string, string, string, error) {
	for _, op := range []string{"=~", "!~", "!=", "="} {
		if idx := strings.Index(part, op); idx >= 0 {
			name := strings.TrimSpace(part[:idx])
			value := strings.TrimSpace(part[idx+len(op):])
			if name == "" || value == "" {
				return "", "", "", fmt.Errorf("invalid label matcher %q", part)
			}
			return name, op, value, nil
		}
	}
	return "", "", "", fmt.Errorf("invalid label matcher %q", part)
}

func parseLogQLPipeline(input string) ([]logQLStage, error) {
	var stages []logQLStage
	input = strings.TrimSpace(input)
	for input != "" {
		matchedLineFilter := false
		for _, op := range []string{"|=", "|~", "!~", "!=", "|>", "!>"} {
			if strings.HasPrefix(input, op) {
				value, rest, err := consumeQuoted(strings.TrimSpace(input[len(op):]))
				if err != nil {
					return nil, err
				}
				filter, err := newLogQLLineFilter(op, value)
				if err != nil {
					return nil, err
				}
				stages = append(stages, logQLStage{kind: "line_filter", lineFilter: filter})
				input = strings.TrimSpace(rest)
				matchedLineFilter = true
				break
			}
		}
		if matchedLineFilter {
			continue
		}
		if !strings.HasPrefix(input, "|") {
			return nil, fmt.Errorf("unexpected LogQL pipeline input %q", input)
		}
		input = strings.TrimSpace(input[1:])
		segment, rest := consumeUntilPipe(input)
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, errors.New("empty LogQL pipeline stage")
		}
		stage, err := parseLogQLStage(segment)
		if err != nil {
			return nil, err
		}
		stages = append(stages, stage)
		input = strings.TrimSpace(rest)
	}
	return stages, nil
}

func newLogQLLineFilter(op, value string) (*logQLLineFilter, error) {
	filter := &logQLLineFilter{op: op, value: value}
	if op == "|~" || op == "!~" {
		re, err := regexp.Compile(value)
		if err != nil {
			return nil, fmt.Errorf("invalid line regex %q: %w", value, err)
		}
		filter.re = re
	}
	if op == "|>" || op == "!>" {
		re, err := compileLogQLPattern(value)
		if err != nil {
			return nil, err
		}
		filter.re = re
	}
	return filter, nil
}

func parseLogQLStage(segment string) (logQLStage, error) {
	name, rest := readIdentifier(segment)
	rest = strings.TrimSpace(rest)
	switch name {
	case "json", "logfmt":
		return logQLStage{kind: "parser", parser: name, parserParam: rest}, nil
	case "decolorize":
		if rest != "" {
			return logQLStage{}, fmt.Errorf("unexpected decolorize tail %q", rest)
		}
		return logQLStage{kind: "decolorize"}, nil
	case "regexp":
		value, tail, err := consumeQuoted(rest)
		if err != nil {
			return logQLStage{}, err
		}
		if strings.TrimSpace(tail) != "" {
			return logQLStage{}, fmt.Errorf("unexpected regexp parser tail %q", tail)
		}
		if _, err := regexp.Compile(value); err != nil {
			return logQLStage{}, fmt.Errorf("invalid regexp parser %q: %w", value, err)
		}
		return logQLStage{kind: "parser", parser: "regexp", parserParam: value}, nil
	case "pattern":
		value, tail, err := consumeQuoted(rest)
		if err != nil {
			return logQLStage{}, err
		}
		if strings.TrimSpace(tail) != "" {
			return logQLStage{}, fmt.Errorf("unexpected pattern parser tail %q", tail)
		}
		if _, err := compileLogQLPattern(value); err != nil {
			return logQLStage{}, err
		}
		return logQLStage{kind: "parser", parser: "pattern", parserParam: value}, nil
	case "line_format":
		value, tail, err := consumeQuoted(rest)
		if err != nil {
			return logQLStage{}, err
		}
		if strings.TrimSpace(tail) != "" {
			return logQLStage{}, fmt.Errorf("unexpected line_format tail %q", tail)
		}
		return logQLStage{kind: "line_format", lineFormat: value}, nil
	case "label_format":
		formats, err := parseLogQLLabelFormats(rest)
		if err != nil {
			return logQLStage{}, err
		}
		return logQLStage{kind: "label_format", labelFormats: formats}, nil
	case "drop":
		parts := splitTopLevel(rest, ',')
		labels := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if idx := strings.Index(part, "="); idx >= 0 {
				part = strings.TrimSpace(part[:idx])
			}
			labels = append(labels, part)
		}
		return logQLStage{kind: "drop", dropLabels: labels}, nil
	case "keep":
		parts := splitTopLevel(rest, ',')
		labels := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if idx := strings.Index(part, "="); idx >= 0 {
				part = strings.TrimSpace(part[:idx])
			}
			labels = append(labels, part)
		}
		return logQLStage{kind: "keep", keepLabels: labels}, nil
	case "unwrap", "unwrap_value":
		if rest == "" {
			return logQLStage{}, errors.New("unwrap requires a label name")
		}
		unwrapFunc, funcRest := readIdentifier(rest)
		if (unwrapFunc == "bytes" || unwrapFunc == "duration" || unwrapFunc == "duration_seconds") && strings.HasPrefix(strings.TrimSpace(funcRest), "(") {
			inner, tail, err := parseParenthesized(funcRest)
			if err != nil {
				return logQLStage{}, err
			}
			if strings.TrimSpace(tail) != "" {
				return logQLStage{}, fmt.Errorf("unexpected unwrap conversion tail %q", tail)
			}
			label := strings.TrimSpace(inner)
			if label == "" {
				return logQLStage{}, errors.New("unwrap conversion requires a label name")
			}
			return logQLStage{kind: "unwrap", unwrapLabel: label, unwrapFunc: unwrapFunc}, nil
		}
		label, _ := readIdentifier(rest)
		if label == "" {
			label = strings.TrimSpace(rest)
		}
		return logQLStage{kind: "unwrap", unwrapLabel: label}, nil
	default:
		filters, err := parseLogQLLabelFilterStage(segment)
		if err != nil {
			return logQLStage{}, err
		}
		return logQLStage{kind: "label_filter", labelFilters: filters}, nil
	}
}

func parseLogQLLabelFormats(input string) ([]logQLLabelFormat, error) {
	parts := splitTopLevel(input, ',')
	formats := make([]logQLLabelFormat, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx < 0 {
			return nil, fmt.Errorf("invalid label_format operation %q", part)
		}
		name := strings.TrimSpace(part[:idx])
		value := strings.TrimSpace(part[idx+1:])
		if name == "" || value == "" {
			return nil, fmt.Errorf("invalid label_format operation %q", part)
		}
		if strings.HasPrefix(value, `"`) || strings.HasPrefix(value, "`") {
			unquoted, err := unquoteLogQLString(value)
			if err != nil {
				return nil, err
			}
			if strings.Contains(unquoted, "{{") {
				formats = append(formats, logQLLabelFormat{name: name, constValue: unquoted, isTemplate: true})
			} else {
				formats = append(formats, logQLLabelFormat{name: name, constValue: unquoted, isConst: true})
			}
			continue
		}
		formats = append(formats, logQLLabelFormat{name: name, sourceName: value})
	}
	return formats, nil
}

func parseLogQLLabelFilterStage(input string) ([]logQLLabelFilter, error) {
	tokens, err := lexLogQLFilter(input)
	if err != nil {
		return nil, err
	}
	var filters []logQLLabelFilter
	for len(tokens) > 0 {
		if len(tokens) < 3 {
			return nil, fmt.Errorf("invalid label filter %q", input)
		}
		f := logQLLabelFilter{name: tokens[0], op: tokens[1]}
		raw := tokens[2]
		if strings.HasPrefix(raw, `"`) || strings.HasPrefix(raw, "`") {
			value, err := unquoteLogQLString(raw)
			if err != nil {
				return nil, err
			}
			f.value = value
		} else if v, valueType, ok := parseLogQLNumericLiteral(raw); ok {
			f.numeric = true
			f.numValue = v
			f.valueType = valueType
			f.value = raw
		} else {
			f.value = raw
		}
		if f.op == "=~" || f.op == "!~" {
			f.re, err = regexp.Compile("^(?:" + f.value + ")$")
			if err != nil {
				return nil, fmt.Errorf("invalid label filter regex %q: %w", f.value, err)
			}
		}
		tokens = tokens[3:]
		if len(tokens) > 0 {
			connector := strings.ToLower(tokens[0])
			if connector == "," {
				connector = "and"
			}
			if connector != "and" && connector != "or" {
				return nil, fmt.Errorf("invalid label filter connector %q", tokens[0])
			}
			f.connector = connector
			tokens = tokens[1:]
		}
		filters = append(filters, f)
	}
	return filters, nil
}

func (m logQLLabelMatcher) matches(labels map[string]string) bool {
	value := labels[m.name]
	switch m.op {
	case "=":
		return value == m.value
	case "!=":
		return value != m.value
	case "=~":
		return m.re != nil && m.re.MatchString(value)
	case "!~":
		return m.re == nil || !m.re.MatchString(value)
	default:
		return false
	}
}

func (f logQLLineFilter) matches(line string) bool {
	switch f.op {
	case "|=":
		return strings.Contains(line, f.value)
	case "!=":
		return !strings.Contains(line, f.value)
	case "|~", "|>":
		return f.re != nil && f.re.MatchString(line)
	case "!~":
		return f.re == nil || !f.re.MatchString(line)
	case "!>":
		return f.re == nil || !f.re.MatchString(line)
	default:
		return true
	}
}

func (f logQLLabelFilter) matches(row *logRow) bool {
	value := row.labels[f.name]
	if value == "" {
		value = row.fields[f.name]
	}
	switch f.op {
	case "=", "==":
		return value == f.value
	case "!=":
		return value != f.value
	case "=~":
		return f.re != nil && f.re.MatchString(value)
	case "!~":
		return f.re == nil || !f.re.MatchString(value)
	case ">", ">=", "<", "<=":
		left, ok := parseLogQLComparableValue(value, f.valueType)
		if !ok {
			return false
		}
		right := f.numValue
		switch f.op {
		case ">":
			return left > right
		case ">=":
			return left >= right
		case "<":
			return left < right
		case "<=":
			return left <= right
		}
	}
	return false
}

func (s logQLSelector) matchesLabels(labels map[string]string) bool {
	for _, matcher := range s.matchers {
		if !matcher.matches(labels) {
			return false
		}
	}
	return true
}

func (s logQLSelector) apply(row *logRow) bool {
	streamLabels := row.streamLabels
	if len(streamLabels) == 0 {
		streamLabels = row.labels
	}
	if !s.matchesLabels(streamLabels) {
		return false
	}
	for _, stage := range s.stages {
		switch stage.kind {
		case "line_filter":
			if !stage.lineFilter.matches(row.line) {
				return false
			}
		case "parser":
			stage.applyParser(row)
		case "label_filter":
			if !matchesLogQLLabelFilters(stage.labelFilters, row) {
				return false
			}
		case "line_format":
			row.line = renderLogQLTemplate(stage.lineFormat, row)
		case "label_format":
			for _, format := range stage.labelFormats {
				if format.isConst {
					row.labels[format.name] = format.constValue
					continue
				}
				if format.isTemplate {
					row.labels[format.name] = renderLogQLTemplate(format.constValue, row)
					continue
				}
				row.labels[format.name] = row.value(format.sourceName)
			}
		case "drop":
			for _, label := range stage.dropLabels {
				delete(row.labels, label)
				delete(row.fields, label)
			}
		case "keep":
			keep := map[string]struct{}{}
			for _, label := range stage.keepLabels {
				keep[label] = struct{}{}
			}
			for label := range row.labels {
				if _, ok := keep[label]; !ok {
					delete(row.labels, label)
				}
			}
			for label := range row.fields {
				if _, ok := keep[label]; !ok {
					delete(row.fields, label)
				}
			}
		case "decolorize":
			row.line = decolorizeLogLine(row.line)
		case "unwrap":
			value := row.value(stage.unwrapLabel)
			parsed, ok := parseLogQLUnwrapValue(value, stage.unwrapFunc)
			if !ok || math.IsNaN(parsed) {
				setLogQLParserError(row, "SampleExtractionErr")
				continue
			}
			row.unwrap = parsed
			row.haveUnwrap = true
		}
	}
	return true
}

func (s logQLStage) applyParser(row *logRow) {
	switch s.parser {
	case "json":
		fields, err := parseFlatJSONFieldsErr(row.line, s.parserParam)
		if err != nil {
			setLogQLParserError(row, "JSONParserErr")
			return
		}
		for key, value := range fields {
			row.fields[key] = value
			row.labels[key] = value
		}
	case "logfmt":
		for key, value := range parseLogfmtFields(row.line) {
			row.fields[key] = value
			row.labels[key] = value
		}
	case "regexp":
		re, err := regexp.Compile(s.parserParam)
		if err != nil {
			return
		}
		applyNamedCaptureParser(re, row)
	case "pattern":
		re, err := compileLogQLPattern(s.parserParam)
		if err != nil {
			return
		}
		applyNamedCaptureParser(re, row)
	}
}

func parseLogQLUnwrapValue(value, conversion string) (float64, bool) {
	switch conversion {
	case "":
		parsed, err := strconv.ParseFloat(value, 64)
		return parsed, err == nil
	case "bytes":
		return parseLogQLBytesValue(value)
	case "duration":
		duration, err := parseLogQLDuration(value)
		if err != nil {
			return 0, false
		}
		return duration.Seconds(), true
	case "duration_seconds":
		duration, err := parseLogQLDuration(value)
		if err != nil {
			return 0, false
		}
		return duration.Seconds(), true
	default:
		return 0, false
	}
}

func setLogQLParserError(row *logRow, value string) {
	if row.labels == nil {
		row.labels = map[string]string{}
	}
	if row.fields == nil {
		row.fields = map[string]string{}
	}
	if row.labels["__error__"] == "" {
		row.labels["__error__"] = value
	}
	if row.fields["__error__"] == "" {
		row.fields["__error__"] = value
	}
}

func matchesLogQLLabelFilters(filters []logQLLabelFilter, row *logRow) bool {
	if len(filters) == 0 {
		return true
	}
	result := filters[0].matches(row)
	for i := 1; i < len(filters); i++ {
		switch filters[i-1].connector {
		case "or":
			result = result || filters[i].matches(row)
		default:
			result = result && filters[i].matches(row)
		}
	}
	return result
}

func (r *logRow) value(name string) string {
	if name == "_entry" || name == "__line__" {
		return r.line
	}
	if v, ok := r.labels[name]; ok {
		return v
	}
	return r.fields[name]
}

func parseFlatJSONFields(line, params string) map[string]string {
	fields, err := parseFlatJSONFieldsErr(line, params)
	if err != nil {
		return nil
	}
	return fields
}

func parseFlatJSONFieldsErr(line, params string) (map[string]string, error) {
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(decoded))
	paramMap := parseParserParams(params)
	if len(paramMap) > 0 {
		for label, path := range paramMap {
			if value, ok := valueAtJSONPath(decoded, path); ok {
				out[label] = stringifyLogValue(value)
			}
		}
		return out, nil
	}
	for key, value := range decoded {
		flattenJSONLogField(out, sanitizeLogLabelName(key), value)
	}
	return out, nil
}

func flattenJSONLogField(out map[string]string, prefix string, value any) {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 {
			out[prefix] = "{}"
			return
		}
		for key, child := range v {
			childKey := prefix + "_" + sanitizeLogLabelName(key)
			flattenJSONLogField(out, childKey, child)
		}
	case []any:
		out[prefix] = stringifyLogValue(v)
	default:
		out[prefix] = stringifyLogValue(v)
	}
}

func parseParserParams(params string) map[string]string {
	params = strings.TrimSpace(params)
	if params == "" {
		return nil
	}
	out := map[string]string{}
	for _, part := range splitTopLevel(params, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(part[:idx])
		raw := strings.TrimSpace(part[idx+1:])
		value, err := unquoteLogQLString(raw)
		if err != nil {
			continue
		}
		out[key] = value
	}
	return out
}

func valueAtJSONPath(input map[string]any, path string) (any, bool) {
	tokens, ok := parseLogQLJSONPath(path)
	if !ok {
		return nil, false
	}
	var current any = input
	for _, token := range tokens {
		if token.isIndex {
			values, ok := current.([]any)
			if !ok || token.index < 0 || token.index >= len(values) {
				return nil, false
			}
			current = values[token.index]
			continue
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[token.key]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func parseLogQLJSONPath(path string) ([]logQLJSONPathToken, bool) {
	path = strings.TrimSpace(strings.TrimPrefix(path, "."))
	if path == "" {
		return nil, false
	}
	tokens := []logQLJSONPathToken{}
	for i := 0; i < len(path); {
		switch path[i] {
		case '.':
			i++
			continue
		case '[':
			close := findMatching(path, i, '[', ']')
			if close < 0 {
				return nil, false
			}
			inner := strings.TrimSpace(path[i+1 : close])
			if inner == "" {
				return nil, false
			}
			if inner[0] == '"' || inner[0] == '`' {
				key, err := unquoteLogQLString(inner)
				if err != nil {
					return nil, false
				}
				tokens = append(tokens, logQLJSONPathToken{key: key})
			} else {
				index, err := strconv.Atoi(inner)
				if err != nil {
					return nil, false
				}
				tokens = append(tokens, logQLJSONPathToken{index: index, isIndex: true})
			}
			i = close + 1
		default:
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			key := strings.TrimSpace(path[start:i])
			if key != "" {
				tokens = append(tokens, logQLJSONPathToken{key: key})
			}
		}
	}
	return tokens, len(tokens) > 0
}

func parseLogfmtFields(line string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(line); {
		for i < len(line) && unicode.IsSpace(rune(line[i])) {
			i++
		}
		start := i
		for i < len(line) && line[i] != '=' && !unicode.IsSpace(rune(line[i])) {
			i++
		}
		if start == i || i >= len(line) || line[i] != '=' {
			for i < len(line) && !unicode.IsSpace(rune(line[i])) {
				i++
			}
			continue
		}
		key := sanitizeLogLabelName(line[start:i])
		i++
		if i < len(line) && line[i] == '"' {
			j := i + 1
			escaped := false
			for ; j < len(line); j++ {
				if escaped {
					escaped = false
					continue
				}
				if line[j] == '\\' {
					escaped = true
					continue
				}
				if line[j] == '"' {
					break
				}
			}
			raw := line[i:minInt(j+1, len(line))]
			value, err := strconv.Unquote(raw)
			if err != nil {
				value = strings.Trim(raw, `"`)
			}
			out[key] = value
			i = minInt(j+1, len(line))
			continue
		}
		start = i
		for i < len(line) && !unicode.IsSpace(rune(line[i])) {
			i++
		}
		out[key] = line[start:i]
	}
	return out
}

func applyNamedCaptureParser(re *regexp.Regexp, row *logRow) {
	match := re.FindStringSubmatch(row.line)
	if match == nil {
		return
	}
	names := re.SubexpNames()
	for i := 1; i < len(match) && i < len(names); i++ {
		if names[i] == "" {
			continue
		}
		name := sanitizeLogLabelName(names[i])
		row.fields[name] = match[i]
		row.labels[name] = match[i]
	}
}

func compileLogQLPattern(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	anchored := !strings.HasPrefix(pattern, "<_>")
	if anchored {
		b.WriteString("^")
	}
	for i := 0; i < len(pattern); {
		if pattern[i] != '<' {
			start := i
			for i < len(pattern) && pattern[i] != '<' {
				i++
			}
			b.WriteString(regexp.QuoteMeta(pattern[start:i]))
			continue
		}
		end := strings.IndexByte(pattern[i:], '>')
		if end < 0 {
			return nil, fmt.Errorf("invalid pattern parser expression %q", pattern)
		}
		name := pattern[i+1 : i+end]
		if name == "_" || name == "" {
			b.WriteString(".*?")
		} else {
			b.WriteString("(?P<")
			b.WriteString(sanitizeLogLabelName(name))
			b.WriteString(">.*?)")
		}
		i += end + 1
	}
	return regexp.Compile(b.String())
}

func renderLogQLTemplate(tpl string, row *logRow) string {
	data := map[string]string{}
	for key, value := range row.fields {
		data[key] = value
	}
	for key, value := range row.labels {
		data[key] = value
	}
	data["_entry"] = row.line
	data["__line__"] = row.line
	data["line"] = row.line
	data["timestamp"] = strconv.FormatFloat(float64(row.tsNS)/1e9, 'f', -1, 64)
	parsed, err := template.New("logql").Option("missingkey=zero").Funcs(logQLTemplateFuncs(row)).Parse(tpl)
	if err == nil {
		var buf bytes.Buffer
		if err := parsed.Execute(&buf, data); err == nil {
			return buf.String()
		}
	}
	re := regexp.MustCompile(`\{\{\s*\.?([A-Za-z_][A-Za-z0-9_\.]*)\s*\}\}`)
	return re.ReplaceAllStringFunc(tpl, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		return row.value(parts[1])
	})
}

func logQLTemplateFuncs(row *logRow) template.FuncMap {
	return template.FuncMap{
		"lower":     strings.ToLower,
		"upper":     strings.ToUpper,
		"title":     strings.Title,
		"trim":      strings.TrimSpace,
		"contains":  strings.Contains,
		"hasPrefix": strings.HasPrefix,
		"hasSuffix": strings.HasSuffix,
		"replace": func(old, newValue, input string) string {
			return strings.ReplaceAll(input, old, newValue)
		},
		"default": func(defaultValue, input string) string {
			if input == "" {
				return defaultValue
			}
			return input
		},
		"trunc": func(length int, input string) string {
			if length < 0 {
				length = 0
			}
			runes := []rune(input)
			if len(runes) <= length {
				return input
			}
			return string(runes[:length])
		},
		"substr": func(start, end int, input string) string {
			runes := []rune(input)
			if start < 0 {
				start = 0
			}
			if end < 0 || end > len(runes) {
				end = len(runes)
			}
			if start > end || start >= len(runes) {
				return ""
			}
			return string(runes[start:end])
		},
		"__line__": func() string { return row.line },
	}
}

var ansiEscapeRE = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)

func decolorizeLogLine(line string) string {
	return ansiEscapeRE.ReplaceAllString(line, "")
}

func logQLSelectSQL(cfg Config, selector logQLSelector, startNS, endNS int64, limit int, direction string) string {
	return logQLRawSelectSQL(cfg, selector, startNS, endNS, limit, direction)
}

func logQLRawSelectSQL(cfg Config, selector logQLSelector, startNS, endNS int64, limit int, direction string) string {
	where := logQLBaseFilters(cfg, selector, startNS, endNS)
	order := "ASC"
	if strings.ToLower(direction) == "backward" {
		order = "DESC"
	}
	maxRows := cfg.LogQueryMaxRows
	if maxRows <= 0 {
		maxRows = 100000
	}
	if limit <= 0 || limit > maxRows {
		limit = maxRows
	}
	return fmt.Sprintf(
		"SELECT toInt64(toUnixTimestamp64Nano(timestamp)) AS ts_ns, toInt64(toUnixTimestamp64Nano(observed_timestamp)) AS observed_ns, body, service_name, severity_text, trace_id, span_id, resource_attributes, attributes_map_str FROM %s WHERE %s ORDER BY ts_ns %s, observed_ns DESC LIMIT %d",
		tableName(cfg.CHDatabase, cfg.LogsTable),
		strings.Join(where, " AND "),
		order,
		limit,
	)
}

func logQLLogsSourceSQL(cfg Config, where []string) string {
	return fmt.Sprintf(
		"(SELECT toInt64(toUnixTimestamp64Nano(timestamp)) AS ts_ns, body, service_name, severity_text, trace_id, span_id, resource_attributes, attributes_map_str FROM %s WHERE %s)",
		tableName(cfg.CHDatabase, cfg.LogsTable),
		strings.Join(where, " AND "),
	)
}

func logQLBaseFilters(cfg Config, selector logQLSelector, startNS, endNS int64) []string {
	filters := []string{
		teamFilter(cfg),
		"timestamp >= " + chTimeNanos(startNS),
		"timestamp <= " + chTimeNanos(endNS),
		"time_bucket >= toStartOfDay(" + chTimeNanos(startNS) + ")",
		"time_bucket <= toStartOfDay(" + chTimeNanos(endNS) + ")",
	}
	for _, matcher := range selector.matchers {
		filters = append(filters, logQLMatcherCondition(matcher))
	}
	canPushLine := true
	for _, stage := range selector.stages {
		if stage.kind == "line_format" {
			canPushLine = false
		}
		if canPushLine && stage.kind == "line_filter" {
			filters = append(filters, logQLLineFilterCondition(*stage.lineFilter))
		}
	}
	return filters
}

func logQLMatcherCondition(m logQLLabelMatcher) string {
	return logQLStringCondition(logQLStreamLabelValueExpr(m.name), m.op, m.value, true)
}

func logQLLineFilterCondition(f logQLLineFilter) string {
	switch f.op {
	case "|=":
		return "position(body, " + sqlString(f.value) + ") > 0"
	case "!=":
		return "position(body, " + sqlString(f.value) + ") = 0"
	case "|~":
		return "match(body, " + sqlString(f.value) + ")"
	case "|>":
		if f.re == nil {
			return "1"
		}
		return "match(body, " + sqlString(f.re.String()) + ")"
	case "!~":
		return "NOT match(body, " + sqlString(f.value) + ")"
	case "!>":
		if f.re == nil {
			return "1"
		}
		return "NOT match(body, " + sqlString(f.re.String()) + ")"
	default:
		return "1"
	}
}

func logQLStringCondition(expr, op, value string, anchorRegex bool) string {
	switch op {
	case "=":
		return expr + " = " + sqlString(value)
	case "!=":
		return expr + " != " + sqlString(value)
	case "=~":
		if anchorRegex {
			value = promRegexToCH(value)
		}
		return "match(" + expr + ", " + sqlString(value) + ")"
	case "!~":
		if anchorRegex {
			value = promRegexToCH(value)
		}
		return "NOT match(" + expr + ", " + sqlString(value) + ")"
	default:
		return "1"
	}
}

func logQLStreamLabelValueExpr(name string) string {
	switch name {
	case "service_name":
		return "service_name"
	case "service.name":
		return "if(service_name != '', service_name, resource_attributes['service.name'])"
	case "level", "severity", "severity_text", "detected_level":
		if name == "detected_level" {
			return "if(mapContains(attributes_map_str, 'detected_level__str'), attributes_map_str['detected_level__str'], severity_text)"
		}
		return "severity_text"
	case "trace_id":
		return "trace_id"
	case "span_id":
		return "span_id"
	default:
		attrKey := sqlString(name + "__str")
		resourceKey := sqlString(name)
		return "if(mapContains(attributes_map_str, " + attrKey + "), attributes_map_str[" + attrKey + "], resource_attributes[" + resourceKey + "])"
	}
}

func logQLLabelValueExpr(name string) string {
	return logQLStreamLabelValueExpr(name)
}

func chTimeNanos(ns int64) string {
	return "fromUnixTimestamp64Nano(" + strconv.FormatInt(ns, 10) + ", 'UTC')"
}

func scanLogRows(ctx context.Context, client *ClickHouseClient, sql string) ([]logRow, error) {
	rows := make([]logRow, 0, 1024)
	err := client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var out logRow
		var serviceName string
		var severityText string
		var traceID string
		var spanID string
		var resourceAttrs map[string]string
		var attrs map[string]string
		if err := row.Scan(&out.tsNS, &out.observedNS, &out.line, &serviceName, &severityText, &traceID, &spanID, &resourceAttrs, &attrs); err != nil {
			return err
		}
		out.labels, out.streamLabels, out.fields = labelsAndFieldsFromLogColumns(serviceName, severityText, traceID, spanID, resourceAttrs, attrs)
		rows = append(rows, out)
		return nil
	})
	return rows, err
}

func labelsFromLogColumns(serviceName, severityText, traceID, spanID string, resourceAttrs, attrs map[string]string) map[string]string {
	labels, _, _ := labelsAndFieldsFromLogColumns(serviceName, severityText, traceID, spanID, resourceAttrs, attrs)
	return labels
}

func labelsAndFieldsFromLogColumns(serviceName, severityText, traceID, spanID string, resourceAttrs, attrs map[string]string) (map[string]string, map[string]string, map[string]string) {
	fields := make(map[string]string, len(resourceAttrs)+len(attrs)+4)
	for key, value := range resourceAttrs {
		if key == "" {
			continue
		}
		fields[key] = value
	}
	if serviceName != "" {
		fields["service_name"] = serviceName
		fields["service.name"] = serviceName
	}
	if severityText != "" {
		fields["level"] = severityText
		fields["severity_text"] = severityText
		fields["detected_level"] = severityText
	}
	if traceID != "" {
		fields["trace_id"] = traceID
	}
	if spanID != "" {
		fields["span_id"] = spanID
	}
	labels := logLabelsFromCoreColumns(serviceName, severityText, traceID, spanID)
	for key, value := range attrs {
		if key == legacyLokiStreamLabelsAttributeKey || key == legacyLokiEntryMetadataAttributeKey {
			continue
		}
		key = strings.TrimSuffix(key, "__str")
		if key == "" {
			continue
		}
		fields[key] = value
		labels[key] = value
	}
	return labels, cloneStringMap(labels), fields
}

func logLabelsFromCoreColumns(serviceName, severityText, traceID, spanID string) map[string]string {
	labels := map[string]string{}
	if serviceName != "" {
		labels["service_name"] = serviceName
		labels["service.name"] = serviceName
	}
	if severityText != "" {
		labels["level"] = severityText
		labels["severity_text"] = severityText
		labels["detected_level"] = severityText
	}
	if traceID != "" {
		labels["trace_id"] = traceID
	}
	if spanID != "" {
		labels["span_id"] = spanID
	}
	return labels
}

func applyLogQLSelector(rows []logRow, selector logQLSelector) []logRow {
	out := rows[:0]
	for i := range rows {
		row := rows[i]
		row.labels = cloneStringMap(row.labels)
		row.fields = cloneStringMap(row.fields)
		if selector.apply(&row) {
			out = append(out, row)
		}
	}
	return out
}

func finalizeLogQLRowsAfterSQLFilter(rows []logRow, selector logQLSelector, fullyPushed bool) []logRow {
	if !fullyPushed {
		return applyLogQLSelector(rows, selector)
	}
	return annotateLogQLPushedEqualityMatchers(rows, selector)
}

func annotateLogQLPushedEqualityMatchers(rows []logRow, selector logQLSelector) []logRow {
	for i := range rows {
		for _, matcher := range selector.matchers {
			if matcher.op != "=" {
				continue
			}
			if rows[i].labels == nil {
				rows[i].labels = map[string]string{}
			}
			if rows[i].fields == nil {
				rows[i].fields = map[string]string{}
			}
			if _, ok := rows[i].labels[matcher.name]; !ok {
				rows[i].labels[matcher.name] = matcher.value
			}
			if _, ok := rows[i].fields[matcher.name]; !ok {
				rows[i].fields[matcher.name] = matcher.value
			}
		}
	}
	return rows
}

func logStreamResults(rows []logRow, limit int, direction string) []logStreamResult {
	if strings.ToLower(direction) != "backward" {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].tsNS == rows[j].tsNS {
				return rows[i].observedNS < rows[j].observedNS
			}
			return rows[i].tsNS < rows[j].tsNS
		})
	} else {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].tsNS == rows[j].tsNS {
				return rows[i].observedNS > rows[j].observedNS
			}
			return rows[i].tsNS > rows[j].tsNS
		})
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	byKey := make(map[string]int)
	results := make([]logStreamResult, 0)
	for _, row := range rows {
		key := labelsKey(row.labels)
		idx, ok := byKey[key]
		if !ok {
			idx = len(results)
			byKey[key] = idx
			results = append(results, logStreamResult{Stream: stableLabelMap(row.labels)})
		}
		results[idx].Values = append(results[idx].Values, []string{strconv.FormatInt(row.tsNS, 10), row.line})
	}
	return results
}

func evaluateLogQLInstantMetric(expr *logQLExpr, rows []logRow, tsNS int64) []logMetricVectorResult {
	samples := evaluateLogQLMetricAt(expr, rows, tsNS)
	return metricSamplesToVector(samples, tsNS)
}

func evaluateLogQLRangeMetric(expr *logQLExpr, rows []logRow, startNS, endNS int64, step time.Duration) []logMetricMatrixResult {
	series := map[string]*logMetricMatrixResult{}
	if step <= 0 {
		step = time.Minute
	}
	for ts := startNS; ts <= endNS; ts += step.Nanoseconds() {
		samples := evaluateLogQLMetricAt(expr, rows, ts)
		for _, sample := range samples {
			key := labelsKey(sample.labels)
			result := series[key]
			if result == nil {
				result = &logMetricMatrixResult{Metric: stableLabelMap(sample.labels)}
				series[key] = result
			}
			result.Values = append(result.Values, []any{float64(ts) / 1e9, formatSample(sample.value)})
		}
	}
	out := make([]logMetricMatrixResult, 0, len(series))
	for _, result := range series {
		out = append(out, *result)
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i].Metric) < labelsKey(out[j].Metric) })
	return out
}

type logMetricSample struct {
	labels map[string]string
	value  float64
}

func evaluateLogQLMetricAt(expr *logQLExpr, rows []logRow, tsNS int64) []logMetricSample {
	if expr == nil {
		return nil
	}
	var samples []logMetricSample
	switch {
	case expr.rangeAgg != nil:
		samples = evaluateLogQLRangeAggregation(expr.rangeAgg, rows, tsNS)
	case expr.aggregation != nil:
		samples = evaluateLogQLAggregation(expr.aggregation, rows, tsNS)
	case expr.topK != nil:
		samples = evaluateLogQLTopK(expr.topK, rows, tsNS)
	case expr.labelFunc != nil:
		samples = evaluateLogQLLabelFunction(expr.labelFunc, rows, tsNS)
	case expr.binaryOp != nil:
		samples = evaluateLogQLBinaryOp(expr.binaryOp, rows, tsNS)
	case expr.scalar != nil:
		samples = []logMetricSample{{labels: map[string]string{}, value: *expr.scalar}}
	case expr.vector != nil:
		samples = []logMetricSample{{labels: map[string]string{}, value: *expr.vector}}
	}
	if expr.comparison != nil {
		samples = filterLogQLComparison(samples, expr.comparison)
	}
	return samples
}

func evaluateLogQLRangeAggregation(agg *logQLRangeAggregation, rows []logRow, tsNS int64) []logMetricSample {
	startNS := tsNS - agg.window.Nanoseconds()
	if agg.offset != 0 {
		startNS -= agg.offset.Nanoseconds()
		tsNS -= agg.offset.Nanoseconds()
	}
	grouped := map[string][]logRow{}
	groupLabels := map[string]map[string]string{}
	for _, row := range rows {
		if row.tsNS < startNS || row.tsNS > tsNS {
			continue
		}
		rowCopy := row
		rowCopy.labels = cloneStringMap(row.labels)
		rowCopy.fields = make(map[string]string)
		if !agg.selector.apply(&rowCopy) {
			continue
		}
		labels := rangeAggregationLabels(rowCopy.labels, agg.grouping)
		key := labelsKey(labels)
		grouped[key] = append(grouped[key], rowCopy)
		groupLabels[key] = labels
	}
	if agg.fn == "absent_over_time" && len(grouped) == 0 {
		return []logMetricSample{{labels: map[string]string{}, value: 1}}
	}
	samples := make([]logMetricSample, 0, len(grouped))
	for key, group := range grouped {
		value := rangeAggregationValue(agg.fn, group, agg.window, agg.param)
		if math.IsNaN(value) {
			continue
		}
		samples = append(samples, logMetricSample{labels: groupLabels[key], value: value})
	}
	return samples
}

func rangeAggregationValue(fn string, rows []logRow, window time.Duration, param float64) float64 {
	switch fn {
	case "count_over_time":
		return float64(len(rows))
	case "rate":
		return float64(len(rows)) / window.Seconds()
	case "bytes_over_time":
		var total int
		for _, row := range rows {
			total += len(row.line)
		}
		return float64(total)
	case "bytes_rate":
		var total int
		for _, row := range rows {
			total += len(row.line)
		}
		return float64(total) / window.Seconds()
	case "sum_over_time":
		var sum float64
		for _, row := range rows {
			if row.haveUnwrap {
				sum += row.unwrap
			}
		}
		return sum
	case "avg_over_time":
		var sum float64
		var count int
		for _, row := range rows {
			if row.haveUnwrap {
				sum += row.unwrap
				count++
			}
		}
		if count == 0 {
			return math.NaN()
		}
		return sum / float64(count)
	case "min_over_time":
		value := math.Inf(1)
		for _, row := range rows {
			if row.haveUnwrap && row.unwrap < value {
				value = row.unwrap
			}
		}
		if math.IsInf(value, 1) {
			return math.NaN()
		}
		return value
	case "max_over_time":
		value := math.Inf(-1)
		for _, row := range rows {
			if row.haveUnwrap && row.unwrap > value {
				value = row.unwrap
			}
		}
		if math.IsInf(value, -1) {
			return math.NaN()
		}
		return value
	case "first_over_time":
		sort.Slice(rows, func(i, j int) bool { return rows[i].tsNS < rows[j].tsNS })
		for _, row := range rows {
			if row.haveUnwrap {
				return row.unwrap
			}
		}
	case "last_over_time":
		sort.Slice(rows, func(i, j int) bool { return rows[i].tsNS > rows[j].tsNS })
		for _, row := range rows {
			if row.haveUnwrap {
				return row.unwrap
			}
		}
	case "stdvar_over_time", "stddev_over_time":
		values := unwrappedValues(rows)
		if len(values) == 0 {
			return math.NaN()
		}
		var sum float64
		for _, value := range values {
			sum += value
		}
		mean := sum / float64(len(values))
		var variance float64
		for _, value := range values {
			delta := value - mean
			variance += delta * delta
		}
		variance /= float64(len(values))
		if fn == "stddev_over_time" {
			return math.Sqrt(variance)
		}
		return variance
	case "quantile_over_time":
		values := unwrappedValues(rows)
		if len(values) == 0 {
			return math.NaN()
		}
		sort.Float64s(values)
		index := int(math.Ceil(param*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(values) {
			index = len(values) - 1
		}
		return values[index]
	}
	return math.NaN()
}

func unwrappedValues(rows []logRow) []float64 {
	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		if row.haveUnwrap {
			values = append(values, row.unwrap)
		}
	}
	return values
}

func evaluateLogQLAggregation(agg *logQLAggregation, rows []logRow, tsNS int64) []logMetricSample {
	child := evaluateLogQLMetricAt(agg.expr, rows, tsNS)
	grouped := map[string][]float64{}
	groupLabels := map[string]map[string]string{}
	for _, sample := range child {
		labels := groupingLabels(sample.labels, agg.grouping)
		key := labelsKey(labels)
		grouped[key] = append(grouped[key], sample.value)
		groupLabels[key] = labels
	}
	out := make([]logMetricSample, 0, len(grouped))
	for key, values := range grouped {
		out = append(out, logMetricSample{labels: groupLabels[key], value: aggregateValues(agg.fn, values)})
	}
	return out
}

func evaluateLogQLTopK(topK *logQLTopK, rows []logRow, tsNS int64) []logMetricSample {
	samples := evaluateLogQLMetricAt(topK.expr, rows, tsNS)
	sort.Slice(samples, func(i, j int) bool {
		if topK.isTop {
			return samples[i].value > samples[j].value
		}
		return samples[i].value < samples[j].value
	})
	if topK.k < len(samples) {
		samples = samples[:topK.k]
	}
	return samples
}

func evaluateLogQLLabelFunction(fn *logQLLabelFunction, rows []logRow, tsNS int64) []logMetricSample {
	samples := evaluateLogQLMetricAt(fn.expr, rows, tsNS)
	out := make([]logMetricSample, 0, len(samples))
	for _, sample := range samples {
		labels := cloneStringMap(sample.labels)
		switch fn.fn {
		case "label_replace":
			source := labels[fn.srcLabel]
			if fn.regex != nil {
				match := fn.regex.FindStringSubmatchIndex(source)
				if match != nil {
					var expanded []byte
					expanded = fn.regex.ExpandString(expanded, fn.replacement, source, match)
					labels[fn.dstLabel] = string(expanded)
				}
			}
		case "label_join":
			values := make([]string, 0, len(fn.srcLabels))
			for _, label := range fn.srcLabels {
				values = append(values, labels[label])
			}
			labels[fn.dstLabel] = strings.Join(values, fn.separator)
		}
		out = append(out, logMetricSample{labels: labels, value: sample.value})
	}
	return out
}

func evaluateLogQLBinaryOp(op *logQLBinaryOp, rows []logRow, tsNS int64) []logMetricSample {
	lhs := evaluateLogQLMetricAt(op.lhs, rows, tsNS)
	rhs := evaluateLogQLMetricAt(op.rhs, rows, tsNS)
	lhsScalar, lhsValue := logQLScalarSample(op.lhs, lhs)
	rhsScalar, rhsValue := logQLScalarSample(op.rhs, rhs)
	switch {
	case lhsScalar && rhsScalar:
		value, keep := applyLogQLBinaryValue(op.op, lhsValue, rhsValue, op.boolModifier)
		if !keep {
			return nil
		}
		return []logMetricSample{{labels: map[string]string{}, value: value}}
	case lhsScalar:
		return evaluateLogQLScalarVectorBinary(op, lhsValue, rhs, true)
	case rhsScalar:
		return evaluateLogQLScalarVectorBinary(op, rhsValue, lhs, false)
	case isLogQLSetOp(op.op):
		return evaluateLogQLSetBinary(op, lhs, rhs)
	default:
		return evaluateLogQLVectorBinary(op, lhs, rhs)
	}
}

func logQLScalarSample(expr *logQLExpr, samples []logMetricSample) (bool, float64) {
	if expr == nil || expr.scalar == nil || len(samples) != 1 || len(samples[0].labels) != 0 {
		return false, 0
	}
	return true, samples[0].value
}

func evaluateLogQLScalarVectorBinary(op *logQLBinaryOp, scalar float64, samples []logMetricSample, scalarOnLeft bool) []logMetricSample {
	out := make([]logMetricSample, 0, len(samples))
	for _, sample := range samples {
		left, right := sample.value, scalar
		if scalarOnLeft {
			left, right = scalar, sample.value
		}
		value, keep := applyLogQLBinaryValue(op.op, left, right, op.boolModifier)
		if !keep {
			continue
		}
		out = append(out, logMetricSample{labels: cloneStringMap(sample.labels), value: value})
	}
	return out
}

func evaluateLogQLVectorBinary(op *logQLBinaryOp, lhs, rhs []logMetricSample) []logMetricSample {
	rhsByKey := map[string][]logMetricSample{}
	for _, sample := range rhs {
		key := logQLBinaryMatchKey(sample.labels, op.matching)
		rhsByKey[key] = append(rhsByKey[key], sample)
	}
	out := make([]logMetricSample, 0, len(lhs))
	for _, left := range lhs {
		key := logQLBinaryMatchKey(left.labels, op.matching)
		for _, right := range rhsByKey[key] {
			value, keep := applyLogQLBinaryValue(op.op, left.value, right.value, op.boolModifier)
			if !keep {
				continue
			}
			out = append(out, logMetricSample{labels: logQLBinaryOutputLabels(left.labels, right.labels, op.matching), value: value})
			if op.matching == nil || op.matching.group == "" {
				break
			}
		}
	}
	return out
}

func evaluateLogQLSetBinary(op *logQLBinaryOp, lhs, rhs []logMetricSample) []logMetricSample {
	rhsByKey := map[string]logMetricSample{}
	for _, sample := range rhs {
		rhsByKey[logQLBinaryMatchKey(sample.labels, op.matching)] = sample
	}
	out := make([]logMetricSample, 0, len(lhs)+len(rhs))
	seen := map[string]struct{}{}
	for _, left := range lhs {
		key := logQLBinaryMatchKey(left.labels, op.matching)
		_, matched := rhsByKey[key]
		switch op.op {
		case "and":
			if matched {
				out = append(out, logMetricSample{labels: cloneStringMap(left.labels), value: left.value})
			}
		case "unless":
			if !matched {
				out = append(out, logMetricSample{labels: cloneStringMap(left.labels), value: left.value})
			}
		case "or":
			out = append(out, logMetricSample{labels: cloneStringMap(left.labels), value: left.value})
			seen[key] = struct{}{}
		}
	}
	if op.op == "or" {
		for _, right := range rhs {
			key := logQLBinaryMatchKey(right.labels, op.matching)
			if _, ok := seen[key]; ok {
				continue
			}
			out = append(out, logMetricSample{labels: cloneStringMap(right.labels), value: right.value})
		}
	}
	return out
}

func applyLogQLBinaryValue(op string, left, right float64, boolModifier bool) (float64, bool) {
	switch op {
	case "+":
		return left + right, true
	case "-":
		return left - right, true
	case "*":
		return left * right, true
	case "/":
		return left / right, true
	case "%":
		return math.Mod(left, right), true
	case "^":
		return math.Pow(left, right), true
	case "==", "!=", ">", ">=", "<", "<=":
		ok := false
		switch op {
		case "==":
			ok = left == right
		case "!=":
			ok = left != right
		case ">":
			ok = left > right
		case ">=":
			ok = left >= right
		case "<":
			ok = left < right
		case "<=":
			ok = left <= right
		}
		if boolModifier {
			if ok {
				return 1, true
			}
			return 0, true
		}
		if ok {
			return left, true
		}
		return 0, false
	default:
		return math.NaN(), false
	}
}

func logQLBinaryMatchKey(labels map[string]string, matching *logQLVectorMatching) string {
	if matching == nil {
		return labelsKey(labels)
	}
	set := make(map[string]struct{}, len(matching.labels))
	for _, label := range matching.labels {
		set[label] = struct{}{}
	}
	selected := map[string]string{}
	if matching.on {
		for _, label := range matching.labels {
			if value, ok := labels[label]; ok {
				selected[label] = value
			}
		}
		return labelsKey(selected)
	}
	for key, value := range labels {
		if _, drop := set[key]; drop {
			continue
		}
		selected[key] = value
	}
	return labelsKey(selected)
}

func logQLBinaryOutputLabels(lhs, rhs map[string]string, matching *logQLVectorMatching) map[string]string {
	if matching == nil {
		return cloneStringMap(lhs)
	}
	if matching.group == "right" {
		out := cloneStringMap(rhs)
		for _, label := range matching.include {
			if value, ok := lhs[label]; ok {
				out[label] = value
			}
		}
		return out
	}
	out := cloneStringMap(lhs)
	if matching.group == "left" {
		for _, label := range matching.include {
			if value, ok := rhs[label]; ok {
				out[label] = value
			}
		}
	}
	return out
}

func aggregateValues(fn string, values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	switch fn {
	case "sum":
		var sum float64
		for _, value := range values {
			sum += value
		}
		return sum
	case "count":
		return float64(len(values))
	case "avg":
		var sum float64
		for _, value := range values {
			sum += value
		}
		return sum / float64(len(values))
	case "min":
		min := values[0]
		for _, value := range values[1:] {
			if value < min {
				min = value
			}
		}
		return min
	case "max":
		max := values[0]
		for _, value := range values[1:] {
			if value > max {
				max = value
			}
		}
		return max
	case "stdvar", "stddev":
		var sum float64
		for _, value := range values {
			sum += value
		}
		mean := sum / float64(len(values))
		var variance float64
		for _, value := range values {
			delta := value - mean
			variance += delta * delta
		}
		variance /= float64(len(values))
		if fn == "stddev" {
			return math.Sqrt(variance)
		}
		return variance
	}
	return math.NaN()
}

func filterLogQLComparison(samples []logMetricSample, cmp *logQLComparison) []logMetricSample {
	out := samples[:0]
	for _, sample := range samples {
		ok := false
		switch cmp.op {
		case "==":
			ok = sample.value == cmp.value
		case "!=":
			ok = sample.value != cmp.value
		case ">":
			ok = sample.value > cmp.value
		case ">=":
			ok = sample.value >= cmp.value
		case "<":
			ok = sample.value < cmp.value
		case "<=":
			ok = sample.value <= cmp.value
		}
		if ok {
			out = append(out, sample)
		}
	}
	return out
}

func metricSamplesToVector(samples []logMetricSample, tsNS int64) []logMetricVectorResult {
	out := make([]logMetricVectorResult, 0, len(samples))
	for _, sample := range samples {
		out = append(out, logMetricVectorResult{
			Metric: stableLabelMap(sample.labels),
			Value:  []any{float64(tsNS) / 1e9, formatSample(sample.value)},
		})
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i].Metric) < labelsKey(out[j].Metric) })
	return out
}

func groupingLabels(input map[string]string, grouping *logQLGrouping) map[string]string {
	if grouping == nil {
		return map[string]string{}
	}
	set := make(map[string]struct{}, len(grouping.labels))
	for _, label := range grouping.labels {
		set[label] = struct{}{}
	}
	out := map[string]string{}
	if grouping.without {
		for key, value := range input {
			if key == "__name__" {
				continue
			}
			if _, drop := set[key]; !drop {
				out[key] = value
			}
		}
		return out
	}
	for _, label := range grouping.labels {
		if value, ok := input[label]; ok {
			out[label] = value
		}
	}
	return out
}

func rangeAggregationLabels(input map[string]string, grouping *logQLGrouping) map[string]string {
	if grouping == nil {
		return stableLabelMap(input)
	}
	return groupingLabels(input, grouping)
}

func labelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(labels[key])
		b.WriteByte('\xff')
	}
	return b.String()
}

func stableLabelMap(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		if key == "" || strings.HasPrefix(key, "__") {
			continue
		}
		out[key] = value
	}
	return out
}

func cloneStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func parseOptionalGrouping(input string) (*logQLGrouping, string, error) {
	input = strings.TrimSpace(input)
	name, rest := readIdentifier(input)
	if name != "by" && name != "without" {
		return nil, input, nil
	}
	inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
	if err != nil {
		return nil, "", err
	}
	labels := splitCSVTrim(inner)
	return &logQLGrouping{without: name == "without", labels: labels}, strings.TrimSpace(tail), nil
}

func parseOptionalComparison(input string) (*logQLComparison, string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, "", nil
	}
	for _, op := range []string{">=", "<=", "==", "!=", ">", "<"} {
		if strings.HasPrefix(input, op) {
			valueText := strings.TrimSpace(input[len(op):])
			valueEnd := len(valueText)
			for i, r := range valueText {
				if unicode.IsSpace(r) {
					valueEnd = i
					break
				}
			}
			value, err := strconv.ParseFloat(valueText[:valueEnd], 64)
			if err != nil {
				return nil, "", fmt.Errorf("invalid comparison value %q", valueText[:valueEnd])
			}
			return &logQLComparison{op: op, value: value}, strings.TrimSpace(valueText[valueEnd:]), nil
		}
	}
	return nil, input, nil
}

func parseLogQLScalar(input string) (float64, bool) {
	value, err := strconv.ParseFloat(strings.TrimSpace(input), 64)
	return value, err == nil
}

func trimLogQLOuterParens(input string) string {
	for {
		input = strings.TrimSpace(input)
		if !strings.HasPrefix(input, "(") {
			return input
		}
		close := findMatching(input, 0, '(', ')')
		if close != len(input)-1 {
			return input
		}
		input = input[1:close]
	}
}

func trimLogQLOuterParensForBinary(input string, minPrec int) (string, int) {
	for {
		input = strings.TrimSpace(input)
		if !strings.HasPrefix(input, "(") {
			return input, minPrec
		}
		close := findMatching(input, 0, '(', ')')
		if close != len(input)-1 {
			return input, minPrec
		}
		input = input[1:close]
		minPrec = 1
	}
}

func findTopLevelLogQLBinaryOp(input string, minPrec int) (int, int, string, int, bool) {
	if strings.HasPrefix(strings.TrimSpace(input), "{") {
		return 0, 0, "", 0, false
	}
	bestStart, bestEnd, bestPrec := -1, -1, 100
	bestOp := ""
	depth := 0
	quote := byte(0)
	escaped := false
	for i := 0; i < len(input); i++ {
		c := input[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if quote == '"' && c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '`':
			quote = c
			continue
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 {
			continue
		}
		op, end, prec, ok := readLogQLBinaryOpAt(input, i)
		if !ok || prec < minPrec {
			continue
		}
		if isLogQLComparisonOp(op) && rhsIsPlainLogQLScalar(input[end:]) {
			continue
		}
		if prec <= bestPrec {
			bestStart, bestEnd, bestOp, bestPrec = i, end, op, prec
		}
	}
	return bestStart, bestEnd, bestOp, bestPrec, bestStart >= 0
}

func readLogQLBinaryOpAt(input string, i int) (string, int, int, bool) {
	for _, op := range []string{">=", "<=", "==", "!=", "+", "-", "*", "/", "%", "^", ">", "<"} {
		if strings.HasPrefix(input[i:], op) {
			if (op == "+" || op == "-") && isUnaryLogQLOperator(input, i) {
				return "", i, 0, false
			}
			return op, i + len(op), logQLBinaryPrecedence(op), true
		}
	}
	if !isLogQLWordBoundary(input, i-1) {
		return "", i, 0, false
	}
	for _, op := range []string{"unless", "and", "or"} {
		if len(input[i:]) >= len(op) && strings.EqualFold(input[i:i+len(op)], op) && isLogQLWordBoundary(input, i+len(op)) {
			return strings.ToLower(op), i + len(op), logQLBinaryPrecedence(op), true
		}
	}
	return "", i, 0, false
}

func logQLBinaryPrecedence(op string) int {
	switch op {
	case "or":
		return 1
	case "and", "unless":
		return 2
	case "==", "!=", ">", ">=", "<", "<=":
		return 3
	case "+", "-":
		return 4
	case "*", "/", "%":
		return 5
	case "^":
		return 6
	default:
		return 0
	}
}

func isLogQLComparisonOp(op string) bool {
	switch op {
	case "==", "!=", ">", ">=", "<", "<=":
		return true
	default:
		return false
	}
}

func isLogQLSetOp(op string) bool {
	return op == "and" || op == "or" || op == "unless"
}

func isUnaryLogQLOperator(input string, idx int) bool {
	for i := idx - 1; i >= 0; i-- {
		if unicode.IsSpace(rune(input[i])) {
			continue
		}
		return strings.ContainsRune("(,+-*/%^<>=!", rune(input[i]))
	}
	return true
}

func isLogQLWordBoundary(input string, idx int) bool {
	if idx < 0 || idx >= len(input) {
		return true
	}
	r := rune(input[idx])
	return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.')
}

func rhsIsPlainLogQLScalar(input string) bool {
	input = strings.TrimSpace(input)
	if input == "" {
		return false
	}
	_, boolModifier, rhs, err := parseLogQLBinaryModifiers(input, true)
	if err != nil {
		return false
	}
	if boolModifier {
		return false
	}
	_, ok := parseLogQLScalar(rhs)
	return ok
}

func parseLogQLBinaryModifiers(input string, allowBool bool) (*logQLVectorMatching, bool, string, error) {
	input = strings.TrimSpace(input)
	var matching *logQLVectorMatching
	boolModifier := false
	for {
		name, rest := readIdentifier(input)
		switch name {
		case "bool":
			if !allowBool {
				return nil, false, "", errors.New("bool modifier is only valid for comparison operators")
			}
			boolModifier = true
			input = strings.TrimSpace(rest)
		case "on", "ignoring":
			inner, tail, err := parseParenthesized(strings.TrimSpace(rest))
			if err != nil {
				return nil, false, "", err
			}
			if matching == nil {
				matching = &logQLVectorMatching{}
			}
			matching.on = name == "on"
			matching.labels = splitCSVTrim(inner)
			input = strings.TrimSpace(tail)
		case "group_left", "group_right":
			include := []string{}
			tail := strings.TrimSpace(rest)
			if strings.HasPrefix(tail, "(") {
				inner, parsedTail, err := parseParenthesized(tail)
				if err != nil {
					return nil, false, "", err
				}
				include = splitCSVTrim(inner)
				tail = parsedTail
			}
			if matching == nil {
				matching = &logQLVectorMatching{}
			}
			matching.group = strings.TrimPrefix(name, "group_")
			matching.include = include
			input = strings.TrimSpace(tail)
		default:
			return matching, boolModifier, input, nil
		}
	}
}

func parseLogQLNumericLiteral(raw string) (float64, string, bool) {
	if value, err := strconv.ParseFloat(raw, 64); err == nil {
		return value, "", true
	}
	if value, ok := parseLogQLDurationValue(raw); ok {
		return value, "duration", true
	}
	if value, ok := parseLogQLBytesValue(raw); ok {
		return value, "bytes", true
	}
	return 0, "", false
}

func parseLogQLComparableValue(raw, valueType string) (float64, bool) {
	switch valueType {
	case "duration":
		return parseLogQLDurationValue(raw)
	case "bytes":
		return parseLogQLBytesValue(raw)
	default:
		value, err := strconv.ParseFloat(raw, 64)
		return value, err == nil
	}
}

func parseLogQLDurationValue(raw string) (float64, bool) {
	duration, err := parseLogQLDuration(raw)
	if err != nil {
		return 0, false
	}
	return float64(duration), true
}

func parseLogQLBytesValue(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	i := 0
	for i < len(raw) && (raw[i] == '+' || raw[i] == '-' || raw[i] == '.' || (raw[i] >= '0' && raw[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, false
	}
	value, err := strconv.ParseFloat(raw[:i], 64)
	if err != nil {
		return 0, false
	}
	unit := strings.ToLower(strings.TrimSpace(raw[i:]))
	multiplier := float64(1)
	switch unit {
	case "", "b":
	case "kb", "k":
		multiplier = 1000
	case "mb", "m":
		multiplier = 1000 * 1000
	case "gb", "g":
		multiplier = 1000 * 1000 * 1000
	case "tb", "t":
		multiplier = 1000 * 1000 * 1000 * 1000
	case "kib", "ki":
		multiplier = 1024
	case "mib", "mi":
		multiplier = 1024 * 1024
	case "gib", "gi":
		multiplier = 1024 * 1024 * 1024
	case "tib", "ti":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, false
	}
	return value * multiplier, true
}

func parseParenthesized(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	if !strings.HasPrefix(input, "(") {
		return "", "", fmt.Errorf("expected parenthesized expression, got %q", input)
	}
	close := findMatching(input, 0, '(', ')')
	if close < 0 {
		return "", "", errors.New("unterminated parenthesized expression")
	}
	return input[1:close], strings.TrimSpace(input[close+1:]), nil
}

func findMatching(input string, start int, open, close byte) int {
	depth := 0
	quote := byte(0)
	escaped := false
	for i := start; i < len(input); i++ {
		c := input[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if quote == '"' && c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '"' || c == '`' {
			quote = c
			continue
		}
		if c == open {
			depth++
			continue
		}
		if c == close {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func findLastRange(input string) (int, int) {
	quote := byte(0)
	escaped := false
	close := -1
	for i := len(input) - 1; i >= 0; i-- {
		c := input[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if quote == '"' && c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '"' || c == '`' {
			quote = c
			continue
		}
		if c == ']' && close < 0 {
			close = i
			continue
		}
		if c == '[' && close >= 0 {
			return i, close
		}
	}
	return -1, -1
}

func consumeQuoted(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" || (input[0] != '"' && input[0] != '`') {
		return "", "", fmt.Errorf("expected quoted string, got %q", input)
	}
	quote := input[0]
	escaped := false
	for i := 1; i < len(input); i++ {
		c := input[i]
		if escaped {
			escaped = false
			continue
		}
		if quote == '"' && c == '\\' {
			escaped = true
			continue
		}
		if c == quote {
			value, err := unquoteLogQLString(input[:i+1])
			return value, input[i+1:], err
		}
	}
	return "", "", errors.New("unterminated quoted string")
}

func consumeUntilPipe(input string) (string, string) {
	quote := byte(0)
	escaped := false
	for i := 0; i < len(input); i++ {
		c := input[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if quote == '"' && c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '"' || c == '`' {
			quote = c
			continue
		}
		if c == '|' {
			return input[:i], input[i:]
		}
	}
	return input, ""
}

func unquoteLogQLString(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if raw[0] == '`' {
		if len(raw) < 2 || raw[len(raw)-1] != '`' {
			return "", errors.New("unterminated raw string")
		}
		return raw[1 : len(raw)-1], nil
	}
	return strconv.Unquote(raw)
}

func splitTopLevel(input string, sep byte) []string {
	var parts []string
	start := 0
	depth := 0
	quote := byte(0)
	escaped := false
	for i := 0; i < len(input); i++ {
		c := input[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if quote == '"' && c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '`':
			quote = c
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		default:
			if c == sep && depth == 0 {
				parts = append(parts, input[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, input[start:])
	return parts
}

func splitCSVTrim(input string) []string {
	parts := splitTopLevel(input, ',')
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func lexLogQLFilter(input string) ([]string, error) {
	var tokens []string
	for i := 0; i < len(input); {
		for i < len(input) && unicode.IsSpace(rune(input[i])) {
			i++
		}
		if i >= len(input) {
			break
		}
		if input[i] == '"' || input[i] == '`' {
			_, rest, err := consumeQuoted(input[i:])
			if err != nil {
				return nil, err
			}
			consumed := len(input[i:]) - len(rest)
			tokens = append(tokens, input[i:i+consumed])
			i += consumed
			continue
		}
		if input[i] == ',' {
			tokens = append(tokens, ",")
			i++
			continue
		}
		for _, op := range []string{">=", "<=", "==", "!=", "=~", "!~", "=", ">", "<"} {
			if strings.HasPrefix(input[i:], op) {
				tokens = append(tokens, op)
				i += len(op)
				goto next
			}
		}
		{
			start := i
			for i < len(input) && !unicode.IsSpace(rune(input[i])) && !strings.ContainsRune("=!<>,", rune(input[i])) {
				i++
			}
			tokens = append(tokens, input[start:i])
		}
	next:
	}
	return tokens, nil
}

func readIdentifier(input string) (string, string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", ""
	}
	i := 0
	for i < len(input) {
		r := rune(input[i])
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.' || r == ':') {
			break
		}
		i++
	}
	return input[:i], strings.TrimSpace(input[i:])
}

func parseLogQLDuration(input string) (time.Duration, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return 0, errors.New("missing duration")
	}
	if strings.HasSuffix(input, "d") || strings.HasSuffix(input, "w") {
		multiplier := 24 * time.Hour
		number := strings.TrimSuffix(input, "d")
		if strings.HasSuffix(input, "w") {
			multiplier = 7 * 24 * time.Hour
			number = strings.TrimSuffix(input, "w")
		}
		value, err := strconv.ParseFloat(number, 64)
		if err != nil || value <= 0 {
			return 0, fmt.Errorf("invalid duration %q", input)
		}
		return time.Duration(value * float64(multiplier)), nil
	}
	d, err := time.ParseDuration(input)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid duration %q", input)
	}
	return d, nil
}

func stringifyLogValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		payload, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(payload)
	}
}

func sanitizeLogLabelName(input string) string {
	if input == "" {
		return "_"
	}
	var b strings.Builder
	for i, r := range input {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
