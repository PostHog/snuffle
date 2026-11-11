package snuffle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

func labelsTestQueryable() storage.Queryable {
	var series []*seriesMeta
	for _, fixture := range []struct {
		workload, cpu, node string
		value               float64
	}{
		{"capture-abc", "0", "b", 1},
		{"capture-def", "10", "a", 2},
		{"ingestion-xyz", "2", "a", 3},
		{"", "x", "c", 4},
	} {
		labelMap := map[string]string{"__name__": "labels_value", "cpu": fixture.cpu, "node": fixture.node}
		if fixture.workload != "" {
			labelMap["workload"] = fixture.workload
		}
		meta := &seriesMeta{labels: labels.FromMap(labelMap), labelMap: labelMap}
		for index := range 3 {
			meta.samples = append(meta.samples, samplePoint{t: 1700000010000 + int64(index)*30000, v: fixture.value})
		}
		series = append(series, meta)
	}
	return &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
		var selected []storage.Series
		for _, meta := range series {
			if matchesAll(meta.labelMap, matchers) {
				selected = append(selected, meta)
			}
		}
		return &seriesSet{series: selected, idx: -1}
	}}}
}

func evaluateMetricsQL(test *testing.T, queryable storage.Queryable, expression string, rangeQuery bool) parser.Value {
	test.Helper()
	start := time.Unix(1700000010, 0)
	end := start.Add(time.Minute)
	if !rangeQuery {
		start = end
	}
	prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, end)
	if err != nil {
		test.Fatal(err)
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableAtModifier: true, EnableNegativeOffset: true, EnableDelayedNameRemoval: true})
	ctx := context.Background()
	options := promql.NewPrometheusQueryOpts(false, 5*time.Minute)
	var query promql.Query
	if rangeQuery {
		query, err = engine.NewRangeQuery(ctx, queryable, options, prepared.query, start, end, 30*time.Second)
	} else {
		query, err = engine.NewInstantQuery(ctx, queryable, options, prepared.query, end)
	}
	if err != nil {
		test.Fatalf("parse %s: %v", prepared.query, err)
	}
	defer query.Close()
	result := query.Exec(ctx)
	if result.Err != nil {
		test.Fatalf("execute %s: %v", prepared.query, result.Err)
	}
	return metricsQLValue(result.Value)
}

func vectorLabel(vector promql.Vector, label string) []string {
	values := make([]string, 0, len(vector))
	for _, sample := range vector {
		values = append(values, sample.Metric.Get(label))
	}
	return values
}

func TestMetricsQLLabelMatch(test *testing.T) {
	for _, testcase := range []struct {
		query string
		want  string
	}{
		{`label_match(labels_value, "workload", "capture.*")`, "capture-abc,capture-def"},
		{`label_match(labels_value, "workload", "capture")`, ""},
		{`label_match(labels_value, "workload", "")`, ""},
		{`label_match(labels_value, "missing", "")`, "capture-abc,capture-def,ingestion-xyz,"},
		{`label_mismatch(labels_value, "workload", "capture.*")`, "ingestion-xyz,"},
		{`label_match(label_replace(labels_value, "owner", "$1", "workload", "(.+)-[a-z]+"), "owner", "capture")`, "capture-abc,capture-def"},
		{`label_match(rate(labels_value[1m]), "__name__", "labels_value")`, ""},
		{`label_match(labels_value, "__name__", "labels_value")`, "capture-abc,capture-def,ingestion-xyz,"},
		{`sum(label_match(labels_value, "workload", "capture.*"))`, ""},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			result := evaluateMetricsQL(test, labelsTestQueryable(), testcase.query, false).(promql.Vector)
			if strings.HasPrefix(testcase.query, "sum(") {
				if len(result) != 1 || result[0].F != 3 {
					test.Fatalf("result = %v", result)
				}
				return
			}
			got := vectorLabel(result, "workload")
			if strings.Join(sortedStrings(got), ",") != testcase.want {
				test.Fatalf("workloads = %v, want %s", got, testcase.want)
			}
		})
	}
	matrix := evaluateMetricsQL(test, labelsTestQueryable(), `label_match(labels_value, "workload", "capture.*")`, true).(promql.Matrix)
	if len(matrix) != 2 || matrix[0].Metric.Get("__name__") != "labels_value" || len(matrix[0].Floats) != 3 {
		test.Fatalf("range result = %v", matrix)
	}
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	// Keep the empty value last so expectations read naturally.
	if len(out) > 0 && out[0] == "" {
		out = append(out[1:], "")
	}
	return out
}

func TestMetricsQLLabelValue(test *testing.T) {
	result := evaluateMetricsQL(test, labelsTestQueryable(), `label_value(labels_value, "cpu")`, false).(promql.Vector)
	got := map[string]float64{}
	for _, sample := range result {
		if sample.Metric.Has("__name__") {
			test.Fatalf("label_value keeps the metric name: %v", sample.Metric)
		}
		got[sample.Metric.Get("cpu")] = sample.F
	}
	// The value "x" does not parse, so its series has no output.
	if len(got) != 3 || got["0"] != 0 || got["10"] != 10 || got["2"] != 2 {
		test.Fatalf("result = %v", result)
	}
	top := evaluateMetricsQL(test, labelsTestQueryable(), `topk(10, max by (node) (label_value(labels_value, "cpu")) + 1)`, false).(promql.Vector)
	values := map[string]float64{}
	for _, sample := range top {
		values[sample.Metric.Get("node")] = sample.F
	}
	if len(values) != 2 || values["a"] != 11 || values["b"] != 1 {
		test.Fatalf("topk result = %v", top)
	}
	matrix := evaluateMetricsQL(test, labelsTestQueryable(), `label_value(labels_value{cpu="10"}, "cpu")`, true).(promql.Matrix)
	if len(matrix) != 1 || len(matrix[0].Floats) != 3 || matrix[0].Floats[2].F != 10 {
		test.Fatalf("range result = %v", matrix)
	}
}

func TestMetricsQLSortByLabel(test *testing.T) {
	for _, testcase := range []struct {
		query string
		want  string
	}{
		// VictoriaMetrics compares label values as bytes, so "10" sorts before "2".
		{`sort_by_label(labels_value, "cpu")`, "0,10,2,x"},
		{`sort_by_label_desc(labels_value, "cpu")`, "x,2,10,0"},
		{`sort_by_label_numeric(labels_value, "cpu")`, "0,2,10,x"},
		{`sort_by_label_numeric_desc(labels_value, "cpu")`, "x,10,2,0"},
		{`sort_by_label(labels_value, "node", "cpu")`, "10,2,0,x"},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			result := evaluateMetricsQL(test, labelsTestQueryable(), testcase.query, false).(promql.Vector)
			if got := strings.Join(vectorLabel(result, "cpu"), ","); got != testcase.want {
				test.Fatalf("order = %s, want %s", got, testcase.want)
			}
		})
	}
}

func TestPrepareMetricsQLLabelFunctionErrors(test *testing.T) {
	start := time.Unix(1700000010, 0)
	for _, expression := range []string{
		`label_match(m, "a")`,
		`label_match(m, "a", "(")`,
		`label_mismatch(m, "a", "b", "c")`,
		`label_value(m)`,
		`sort_by_label(m)`,
		`label_value()`,
		`sort_by_label(m, 123)`,
		`__snuffle_label_match(m, "a", "b")`,
	} {
		test.Run(expression, func(test *testing.T) {
			prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start)
			if err == nil {
				_, err = parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
			}
			if err == nil {
				test.Fatalf("expected an error, prepared %s", prepared.query)
			}
		})
	}
}

func TestMetricsQLNumericLess(test *testing.T) {
	for _, testcase := range []struct {
		a, b string
		want bool
	}{
		{"2", "10", true},
		{"10", "2", false},
		{"a2", "a10", true},
		{"a", "1", false},
		{"1", "a", true},
		{"", "a", true},
		{"1.5", "1.25", false},
		{"x9", "x9y", true},
	} {
		if got := metricsQLNumericLess(testcase.a, testcase.b); got != testcase.want {
			test.Errorf("numericLess(%q, %q) = %v, want %v", testcase.a, testcase.b, got, testcase.want)
		}
	}
}

func TestMetricsQLLabelValueEdgeCases(test *testing.T) {
	scalar := func(query string) (float64, int) {
		result := evaluateMetricsQL(test, labelsTestQueryable(), query, false).(promql.Vector)
		total := 0.0
		for _, sample := range result {
			total += sample.F
		}
		return total, len(result)
	}
	// Unparseable label values are dropped before the parent aggregate.
	if got, _ := scalar(`sum(label_value(labels_value, "cpu"))`); got != 12 {
		test.Fatalf("sum = %v, want 12", got)
	}
	if got, _ := scalar(`count(label_value(labels_value, "cpu"))`); got != 3 {
		test.Fatalf("count = %v, want 3", got)
	}
	// NaN input points stay missing.
	if _, count := scalar(`label_value(labels_value / 0 * 0, "cpu")`); count != 0 {
		test.Fatalf("NaN input gave %d samples", count)
	}
	// __name__ is reset before the lookup.
	if _, count := scalar(`label_value(label_replace(labels_value, "__name__", "42", "", ""), "__name__")`); count != 0 {
		test.Fatalf("__name__ gave %d samples", count)
	}
}

func TestMetricsQLSortByLabelTiesKeepInputOrder(test *testing.T) {
	result := evaluateMetricsQL(test, labelsTestQueryable(), `sort_by_label(sort_by_label_desc(labels_value, "cpu"), "node")`, false).(promql.Vector)
	if got := strings.Join(vectorLabel(result, "cpu"), ","); got != "2,10,0,x" {
		test.Fatalf("order = %s", got)
	}
}

func TestMetricsQLSortByLabelRangeSkipsSort(test *testing.T) {
	matrix := evaluateMetricsQL(test, labelsTestQueryable(), `sort_by_label(labels_value, "cpu")`, true).(promql.Matrix)
	if len(matrix) != 4 {
		test.Fatalf("series = %d, want 4", len(matrix))
	}
}

func TestPrepareMetricsQLLabelFunctionRangeErrors(test *testing.T) {
	start := time.Unix(1700000010, 0)
	// A range query drops sort_by_label, so it must still reject bad arguments.
	for _, expression := range []string{`sort_by_label(m, 123)`, `sort_by_label_desc(m)`} {
		if _, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start.Add(time.Hour)); err == nil {
			test.Fatalf("%s: expected an error", expression)
		}
	}
}

func TestMetricsQLLabelFunctionsAcceptNumbers(test *testing.T) {
	result := evaluateMetricsQL(test, labelsTestQueryable(), `label_match(1, "missing", "")`, false).(promql.Vector)
	if len(result) != 1 || result[0].F != 1 {
		test.Fatalf("label_match result = %v", result)
	}
	result = evaluateMetricsQL(test, labelsTestQueryable(), `sort_by_label(1, "job")`, false).(promql.Vector)
	if len(result) != 1 || result[0].F != 1 {
		test.Fatalf("sort_by_label result = %v", result)
	}
}
