package snuffle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type logQLExpr struct {
	logSelector *logQLSelector
	rangeAgg    *logQLRangeAggregation
	aggregation *logQLAggregation
	topK        *logQLTopK
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
	unwrapLabel  string
}

type logQLLabelFormat struct {
	name       string
	sourceName string
	constValue string
	isConst    bool
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
	if strings.HasPrefix(input, "{") {
		selector, err := parseLogQLSelector(input)
		if err != nil {
			return nil, err
		}
		return &logQLExpr{logSelector: selector}, nil
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
		for _, op := range []string{"|=", "|~", "!~", "!=", "|>"} {
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
	if op == "|>" {
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
	case "unwrap", "unwrap_value":
		if rest == "" {
			return logQLStage{}, errors.New("unwrap requires a label name")
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
			formats = append(formats, logQLLabelFormat{name: name, constValue: unquoted, isConst: true})
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
		} else if v, err := strconv.ParseFloat(raw, 64); err == nil {
			f.numeric = true
			f.numValue = v
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
		left, err := strconv.ParseFloat(value, 64)
		if err != nil {
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
				row.labels[format.name] = row.value(format.sourceName)
			}
		case "drop":
			for _, label := range stage.dropLabels {
				delete(row.labels, label)
				delete(row.fields, label)
			}
		case "unwrap":
			value := row.value(stage.unwrapLabel)
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(parsed) {
				return false
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
		for key, value := range parseFlatJSONFields(row.line, s.parserParam) {
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
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		return nil
	}
	out := make(map[string]string, len(decoded))
	paramMap := parseParserParams(params)
	if len(paramMap) > 0 {
		for label, path := range paramMap {
			if value, ok := valueAtJSONPath(decoded, path); ok {
				out[label] = stringifyLogValue(value)
			}
		}
		return out
	}
	for key, value := range decoded {
		out[sanitizeLogLabelName(key)] = stringifyLogValue(value)
	}
	return out
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
	path = strings.TrimPrefix(path, ".")
	parts := strings.Split(path, ".")
	var current any = input
	for _, part := range parts {
		part = strings.Trim(part, "[]\"'")
		if part == "" {
			continue
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
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
	re := regexp.MustCompile(`\{\{\s*\.?([A-Za-z_][A-Za-z0-9_\.]*)\s*\}\}`)
	return re.ReplaceAllStringFunc(tpl, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		return row.value(parts[1])
	})
}

func logQLSelectSQL(cfg Config, selector logQLSelector, startNS, endNS int64, limit int, direction string) string {
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
		"SELECT ts_ns, observed_ns, body, service_name, severity_text, trace_id, span_id, resource_attributes, attributes_map_str FROM %s ORDER BY ts_ns %s, stream_key ASC, observed_ns DESC LIMIT %d",
		logQLDedupLogsSubquery(cfg, where),
		order,
		limit,
	)
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
		"SELECT toInt64(toUnixTimestamp64Nano(timestamp)) AS ts_ns, toInt64(toUnixTimestamp64Nano(observed_timestamp)) AS observed_ns, body, service_name, severity_text, trace_id, span_id, resource_attributes, attributes_map_str FROM %s WHERE %s ORDER BY ts_ns %s, %s ASC, observed_ns DESC LIMIT %d",
		tableName(cfg.CHDatabase, cfg.LogsTable),
		strings.Join(where, " AND "),
		order,
		logQLRawStreamKeyExpr(),
		limit,
	)
}

func logQLCompactSelectSQL(cfg Config, selector logQLSelector, startNS, endNS int64, limit int, direction string) string {
	where := logQLCompactBaseFilters(cfg, selector, startNS, endNS)
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
	streamKey := sqlString(lokiStreamLabelsAttributeKey)
	metadataKey := sqlString(lokiEntryMetadataAttributeKey)
	return fmt.Sprintf(
		"SELECT toInt64(toUnixTimestamp64Nano(timestamp)) AS ts_ns, toInt64(toUnixTimestamp64Nano(observed_timestamp)) AS observed_ns, body, service_name, severity_text, trace_id, span_id, attributes_map_str[%s] AS stream_labels, attributes_map_str[%s] AS entry_metadata FROM %s WHERE %s ORDER BY ts_ns %s, stream_labels ASC, observed_ns DESC LIMIT %d",
		streamKey,
		metadataKey,
		tableName(cfg.CHDatabase, cfg.LogsTable),
		strings.Join(where, " AND "),
		order,
		limit,
	)
}

func logQLDedupLogsSubquery(cfg Config, where []string) string {
	metadataKey := sqlString(lokiEntryMetadataAttributeKey)
	dedupKey := logQLRawStreamKeyExpr()
	weight := "length(attributes_map_str[" + metadataKey + "]) + length(trace_id) + length(span_id)"
	return fmt.Sprintf(
		"(SELECT ts_ns, body, dedup_key AS stream_key, argMax(toInt64(toUnixTimestamp64Nano(observed_timestamp)), dedup_weight) AS observed_ns, argMax(service_name, dedup_weight) AS service_name, argMax(severity_text, dedup_weight) AS severity_text, argMax(trace_id, dedup_weight) AS trace_id, argMax(span_id, dedup_weight) AS span_id, argMax(resource_attributes, dedup_weight) AS resource_attributes, argMax(attributes_map_str, dedup_weight) AS attributes_map_str FROM (SELECT toInt64(toUnixTimestamp64Nano(timestamp)) AS ts_ns, body, observed_timestamp, service_name, severity_text, trace_id, span_id, resource_attributes, attributes_map_str, %s AS dedup_key, %s AS dedup_weight FROM %s WHERE %s) GROUP BY ts_ns, body, dedup_key)",
		dedupKey,
		weight,
		tableName(cfg.CHDatabase, cfg.LogsTable),
		strings.Join(where, " AND "),
	)
}

func logQLRawStreamKeyExpr() string {
	streamKey := sqlString(lokiStreamLabelsAttributeKey)
	return "if(mapContains(attributes_map_str, " + streamKey + "), attributes_map_str[" + streamKey + "], concat(toString(resource_attributes), toString(attributes_map_str)))"
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

func logQLCompactBaseFilters(cfg Config, selector logQLSelector, startNS, endNS int64) []string {
	filters := []string{
		teamFilter(cfg),
		"timestamp >= " + chTimeNanos(startNS),
		"timestamp <= " + chTimeNanos(endNS),
		"time_bucket >= toStartOfDay(" + chTimeNanos(startNS) + ")",
		"time_bucket <= toStartOfDay(" + chTimeNanos(endNS) + ")",
	}
	for _, matcher := range selector.matchers {
		filters = append(filters, logQLCompactMatcherCondition(matcher))
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

func logQLCompactMatcherCondition(m logQLLabelMatcher) string {
	return logQLStringCondition(logQLCompactLabelValueExpr(m.name), m.op, m.value, true)
}

func logQLCompactLabelValueExpr(label string) string {
	switch label {
	case "service_name", "service.name":
		return "service_name"
	case "level", "severity", "severity_text", "detected_level":
		return "severity_text"
	case "trace_id":
		return "trace_id"
	case "span_id":
		return "span_id"
	default:
		return logQLStreamLabelValueExpr(label)
	}
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
		return "1"
	case "!~":
		return "NOT match(body, " + sqlString(f.value) + ")"
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
	streamLabelExpr := func(fallback string) string {
		key := sqlString(lokiStreamLabelsAttributeKey)
		return "if(mapContains(attributes_map_str, " + key + "), JSONExtractString(attributes_map_str[" + key + "], " + sqlString(name) + "), " + fallback + ")"
	}
	switch name {
	case "service_name":
		return streamLabelExpr("service_name")
	case "service.name":
		return streamLabelExpr("if(service_name != '', service_name, resource_attributes['service.name'])")
	case "level", "severity", "severity_text", "detected_level":
		fallback := "severity_text"
		if name == "detected_level" {
			fallback = "if(mapContains(attributes_map_str, 'detected_level__str'), attributes_map_str['detected_level__str'], severity_text)"
		}
		return streamLabelExpr(fallback)
	case "trace_id":
		return streamLabelExpr("trace_id")
	case "span_id":
		return streamLabelExpr("span_id")
	default:
		attrKey := sqlString(name + "__str")
		resourceKey := sqlString(name)
		return streamLabelExpr("if(mapContains(attributes_map_str, " + attrKey + "), attributes_map_str[" + attrKey + "], resource_attributes[" + resourceKey + "])")
	}
}

func logQLLabelValueExpr(name string) string {
	stream := logQLStreamLabelValueExpr(name)
	metadataKey := sqlString(lokiEntryMetadataAttributeKey)
	metadata := "if(mapContains(attributes_map_str, " + metadataKey + "), JSONExtractString(attributes_map_str[" + metadataKey + "], " + sqlString(name) + "), '')"
	fallback := "''"
	switch name {
	case "service_name":
		fallback = "service_name"
	case "service.name":
		fallback = "if(service_name != '', service_name, resource_attributes['service.name'])"
	case "level", "severity", "severity_text", "detected_level":
		fallback = "severity_text"
		if name == "detected_level" {
			fallback = "if(mapContains(attributes_map_str, 'detected_level__str'), attributes_map_str['detected_level__str'], severity_text)"
		}
	case "trace_id":
		fallback = "trace_id"
	case "span_id":
		fallback = "span_id"
	default:
		attrKey := sqlString(name + "__str")
		resourceKey := sqlString(name)
		fallback = "if(mapContains(attributes_map_str, " + attrKey + "), attributes_map_str[" + attrKey + "], resource_attributes[" + resourceKey + "])"
	}
	return "multiIf(" + stream + " != '', " + stream + ", " + metadata + " != '', " + metadata + ", " + fallback + ")"
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

func scanCompactLogRows(ctx context.Context, client *ClickHouseClient, sql string) ([]logRow, error) {
	rows := make([]logRow, 0, 1024)
	err := client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var out logRow
		var serviceName string
		var severityText string
		var traceID string
		var spanID string
		var streamLabelsJSON string
		var metadataJSON string
		if err := row.Scan(&out.tsNS, &out.observedNS, &out.line, &serviceName, &severityText, &traceID, &spanID, &streamLabelsJSON, &metadataJSON); err != nil {
			return err
		}
		out.labels, out.streamLabels, out.fields = labelsAndFieldsFromLokiMarkers(serviceName, severityText, traceID, spanID, streamLabelsJSON, metadataJSON)
		rows = append(rows, out)
		return nil
	})
	return rows, err
}

func labelsFromLogColumns(serviceName, severityText, traceID, spanID string, resourceAttrs, attrs map[string]string) map[string]string {
	labels, _, _ := labelsAndFieldsFromLogColumns(serviceName, severityText, traceID, spanID, resourceAttrs, attrs)
	return labels
}

func labelsAndFieldsFromLokiMarkers(serviceName, severityText, traceID, spanID, streamLabelsJSON, metadataJSON string) (map[string]string, map[string]string, map[string]string) {
	streamLabels := parseStringMapJSON(streamLabelsJSON)
	if streamLabels == nil {
		streamLabels = map[string]string{}
	}
	metadata := parseStringMapJSON(metadataJSON)
	fields := make(map[string]string, len(streamLabels)+len(metadata)+6)
	for key, value := range streamLabels {
		if key != "" {
			fields[key] = value
		}
	}
	for key, value := range metadata {
		if key != "" {
			fields[key] = value
		}
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
	if len(streamLabels) == 0 {
		labels := make(map[string]string, len(fields))
		for key, value := range fields {
			labels[key] = value
			streamLabels[key] = value
		}
		return labels, streamLabels, fields
	}
	outputLabels := cloneStringMap(streamLabels)
	for key, value := range metadata {
		if key != "" {
			outputLabels[key] = value
		}
	}
	if severityText != "" {
		outputLabels["detected_level"] = severityText
	}
	return outputLabels, streamLabels, fields
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
	for key, value := range attrs {
		if key == lokiStreamLabelsAttributeKey || key == lokiEntryMetadataAttributeKey {
			continue
		}
		key = strings.TrimSuffix(key, "__str")
		if key == "" {
			continue
		}
		fields[key] = value
	}
	metadata := parseStringMapJSON(attrs[lokiEntryMetadataAttributeKey])
	for key, value := range metadata {
		if key != "" {
			fields[key] = value
		}
	}
	streamLabels := map[string]string{}
	if raw := attrs[lokiStreamLabelsAttributeKey]; raw != "" {
		if streamLabels := parseStringMapJSON(raw); len(streamLabels) > 0 {
			outputLabels := cloneStringMap(streamLabels)
			for key, value := range metadata {
				if key != "" {
					outputLabels[key] = value
				}
			}
			if severityText != "" {
				outputLabels["detected_level"] = severityText
			}
			return outputLabels, streamLabels, fields
		}
	}
	labels := make(map[string]string, len(fields))
	for key, value := range fields {
		labels[key] = value
		streamLabels[key] = value
	}
	return labels, streamLabels, fields
}

func stableJSONMap(input map[string]string) string {
	if len(input) == 0 {
		return "{}"
	}
	data, err := json.Marshal(stableLabelMap(input))
	if err != nil {
		return "{}"
	}
	return string(data)
}

func parseStringMapJSON(input string) map[string]string {
	decoded := map[string]string{}
	if err := json.Unmarshal([]byte(input), &decoded); err == nil {
		return decoded
	}
	raw := map[string]any{}
	if err := json.Unmarshal([]byte(input), &raw); err != nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		out[key] = stringifyLogValue(value)
	}
	return out
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
		for _, op := range []string{">=", "<=", "==", "!=", "=~", "!~", "=", ">", "<"} {
			if strings.HasPrefix(input[i:], op) {
				tokens = append(tokens, op)
				i += len(op)
				goto next
			}
		}
		{
			start := i
			for i < len(input) && !unicode.IsSpace(rune(input[i])) && !strings.ContainsRune("=!<>", rune(input[i])) {
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
