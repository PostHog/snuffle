package snuffle

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestPrepareMetricsQLQueryRewrites(t *testing.T) {
	start := time.Unix(1700000000, 0)
	end := start.Add(time.Hour)
	promParser := parser.NewParser(parser.Options{})
	for _, tc := range []struct {
		name, query, want string
		instant           bool
		// metricsQLOnly marks syntax the Prometheus parser does not accept.
		metricsQLOnly bool
	}{
		{name: "bare selector range", query: "sum(m)", want: "sum(__snuffle_default_rollup(m[315000ms], 0, 15000, 300000, 0))"},
		{name: "bare selector instant reads the lookback only", query: "m", instant: true, want: "__snuffle_default_rollup(m[300000ms], 0, 15000, 300000, 1)"},
		{name: "counter keeps explicit window", query: "rate(m[1m])", want: "__snuffle_rate(m[360000ms], 60000, 15000, 300000, 0)"},
		{name: "timestamp keeps its selector", query: "timestamp(m)", want: "timestamp(m)"},
		{name: "timestamp with offset keeps its selector", query: "timestamp(m offset 1m)", want: "timestamp(m offset 60000ms)"},
		{name: "absent keeps its selector", query: `absent(m{job="x"})`, want: `absent(m{job="x"})`},
		{name: "absent wraps inner expressions", query: "absent(sum(m))", want: "absent(sum(__snuffle_default_rollup(m[315000ms], 0, 15000, 300000, 0)))"},
		{name: "prometheus function unknown to metricsql", query: "histogram_count(m)", want: "histogram_count(__snuffle_default_rollup(m[315000ms], 0, 15000, 300000, 0))"},
		{name: "unknown function with spaces and nested call", query: "histogram_sum (rate(m[5m]))", want: "histogram_sum(__snuffle_rate(m[600000ms], 300000, 15000, 300000, 0))"},
		{name: "zero argument prometheus function", query: "pi()", want: "pi()"},
		{name: "function name inside a string", query: `label_replace(m, "a", "info(", "b", ".*")`, want: `label_replace(__snuffle_default_rollup(m[315000ms], 0, 15000, 300000, 0), "a", "info(", "b", ".*")`},
		{name: "rollup argument index", query: "hoeffding_bound_lower(0.9, m[5m])", want: "hoeffding_bound_lower(0.9, m[300000ms])", metricsQLOnly: true},
		{name: "quantile rollup argument index", query: "quantile_over_time(0.9, m)", want: "quantile_over_time(0.9, m[15000ms])"},
		{name: "subquery uses its own step", query: "rate(sum(m)[5m:1m])", want: "__snuffle_rate(sum(__snuffle_default_rollup(m[360000ms], 0, 60000, 300000, 0))[600000ms:60000ms], 300000, 15000, 300000, 0)"},
		{name: "subquery step units resolve against the subquery step", query: "max_over_time(rate(m)[1h:1m])", instant: true, want: "max_over_time(__snuffle_rate(m[360000ms], 0, 60000, 300000, 0)[3600000ms:60000ms])"},
		{name: "numeric argument becomes a vector", query: "abs(-1)", want: "abs(vector(-1))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queryEnd := end
			if tc.instant {
				queryEnd = start
			}
			prepared, err := prepareMetricsQLQuery(tc.query, 15*time.Second, 5*time.Minute, start, queryEnd)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if prepared.query != tc.want {
				t.Fatalf("query = %s, want %s", prepared.query, tc.want)
			}
			if tc.metricsQLOnly {
				return
			}
			if _, err := promParser.ParseExpr(prepared.query); err != nil {
				t.Fatalf("prometheus parse: %v", err)
			}
		})
	}
}

func TestPrepareMetricsQLQueryRunningSum(t *testing.T) {
	start := time.Unix(1700000001, 0)
	end := start.Add(time.Hour)
	prepared, err := prepareMetricsQLQuery("running_sum(sum(m))", 15*time.Second, 5*time.Minute, start, end)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !prepared.runningSum || prepared.query != "sum(__snuffle_default_rollup(m[315000ms], 0, 15000, 300000, 0))" {
		t.Fatalf("unexpected result: %#v", prepared)
	}
	for _, tc := range []struct{ query, wantErr string }{
		{query: "sum(running_sum(m))", wantErr: "aligned"},
		{query: "__snuffle_rate(m[1m], 1, 1, 1, 1)", wantErr: "reserved"},
		{query: "__snuffle_running_sum(m[1m:1m], 1)", wantErr: "reserved"},
	} {
		if _, err := prepareMetricsQLQuery(tc.query, 15*time.Second, 5*time.Minute, start, end); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: err = %v, want %q", tc.query, err, tc.wantErr)
		}
	}
	if _, err := prepareMetricsQLQuery("running_sum(m)", 15*time.Second, 5*time.Minute, start, start); err == nil || !strings.Contains(err.Error(), "range query") {
		t.Errorf("instant running_sum err = %v", err)
	}

	// A nested running_sum spans the range from the aligned start as a subquery.
	aligned := time.Unix(1700000010, 0)
	prepared, err = prepareMetricsQLQuery("quantile(1, running_sum(delta(m[60s])))", 10*time.Second, 5*time.Minute, aligned, aligned.Add(10*time.Second))
	if err != nil {
		t.Fatalf("nested prepare: %v", err)
	}
	want := "quantile(1, __snuffle_running_sum(__snuffle_delta(m[360000ms], 60000, 10000, 300000, 0)[20000ms:10000ms], 1.70000001e+12))"
	if prepared.runningSum || prepared.query != want {
		t.Fatalf("nested query = %#v, want %s", prepared, want)
	}
	if _, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query); err != nil {
		t.Fatalf("prometheus parse: %v", err)
	}
}

func TestMetricsQLRunningSum(t *testing.T) {
	value := metricsQLRunningSum(promql.Matrix{{
		Metric: labels.FromStrings("a", "b"),
		Floats: []promql.FPoint{{T: 1000, F: 1}, {T: 2000, F: 2}, {T: 3000, F: 3}},
	}})
	matrix := value.(promql.Matrix)
	if len(matrix) != 1 || len(matrix[0].Floats) != 3 {
		t.Fatalf("unexpected matrix: %#v", matrix)
	}
	for i, want := range []float64{1, 3, 6} {
		if matrix[0].Floats[i].F != want {
			t.Errorf("point %d = %v, want %v", i, matrix[0].Floats[i].F, want)
		}
	}
}

func TestMetricsQLValueDoesNotAliasEngineMatrix(t *testing.T) {
	nan := math.NaN()
	kept := []promql.FPoint{{T: 1000, F: 1}, {T: 2000, F: 2}}
	input := promql.Matrix{
		{Metric: labels.FromStrings("a", "1"), Floats: []promql.FPoint{{T: 1000, F: nan}}},
		{Metric: labels.FromStrings("a", "2"), Floats: kept},
		{Metric: labels.FromStrings("a", "3"), Floats: []promql.FPoint{{T: 1000, F: nan}, {T: 2000, F: 3}}},
	}
	out := metricsQLValue(input).(promql.Matrix)
	if len(out) != 2 {
		t.Fatalf("got %d series, want 2", len(out))
	}
	if &out[0] == &input[0] || &out[0] == &input[1] {
		t.Fatal("the output slice aliases the engine matrix")
	}
	if len(input) != 3 || input[0].Metric.Get("a") != "1" || input[1].Metric.Get("a") != "2" {
		t.Fatal("the engine matrix was modified")
	}
	if &out[0].Floats[0] != &kept[0] {
		t.Error("a series without NaN should keep its points")
	}
	if len(out[1].Floats) != 1 || out[1].Floats[0].F != 3 || len(input[2].Floats) != 2 {
		t.Errorf("NaN filtering changed the input or kept NaN: %#v", out[1].Floats)
	}
	vector := metricsQLValue(promql.Scalar{T: 1000, V: 2}).(promql.Vector)
	if len(vector) != 1 || vector[0].F != 2 {
		t.Errorf("scalar result = %#v", vector)
	}
}

func TestMetricsQLCounterValueGuardsZeroDuration(t *testing.T) {
	points := []promql.FPoint{{T: 1000, F: 1}, {T: 1000, F: 5}}
	if got := metricsQLCounterValue("rate", points, 0, 0, 1000, 300000); !math.IsNaN(got) {
		t.Fatalf("rate over a zero duration = %v, want NaN", got)
	}
	if got := metricsQLCounterValue("increase", points, 0, 0, 1000, 300000); got != 5 {
		t.Fatalf("increase = %v, want 5", got)
	}
}

func TestUnwrapInstantDefaultRollups(t *testing.T) {
	promParser := parser.NewParser(parser.Options{})
	for _, tc := range []struct {
		query string
		want  string
		ok    bool
	}{
		{query: "sum(__snuffle_default_rollup(m[300000ms], 0, 300000, 300000, 1)) by (a)", want: "sum by (a) (m)", ok: true},
		{query: "topk(3, __snuffle_default_rollup(m[300000ms] offset 60000ms, 0, 300000, 300000, 1))", want: "topk(3, m offset 1m)", ok: true},
		{query: "sum(__snuffle_default_rollup(m[315000ms], 0, 15000, 300000, 0))"},
		{query: "sum(__snuffle_rate(m[600000ms], 300000, 300000, 300000, 1))"},
		{query: "last_over_time(m[5m])", want: "last_over_time(m[5m])", ok: true},
	} {
		expr, err := promParser.ParseExpr(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		got, ok := unwrapInstantDefaultRollups(expr)
		if ok != tc.ok {
			t.Errorf("%s: ok = %t, want %t", tc.query, ok, tc.ok)
			continue
		}
		if ok && got.String() != tc.want {
			t.Errorf("%s: got %s, want %s", tc.query, got.String(), tc.want)
		}
	}
}

func TestCarryUnsupportedFunctions(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{query: "histogram_count(m)", want: `union("__snuffle_fn:histogram_count", m)`},
		{query: "rate(m[5m])", want: "rate(m[5m])"},
		{query: `m{info="x"}`, want: `m{info="x"}`},
		{query: "info()", want: `union("__snuffle_fn:info")`},
		{query: `"histogram_count(" + m`, want: `"histogram_count(" + m`},
	} {
		if got := carryUnsupportedFunctions(tc.query); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.query, got, tc.want)
		}
	}
}
