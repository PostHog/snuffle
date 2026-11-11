package snuffle

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// A range selector outside a rollup function uses default_rollup, the last
// sample in the window, as in VictoriaMetrics.
func TestMetricsQLImplicitRollup(test *testing.T) {
	sums := func(value parser.Value) map[string]float64 {
		out := map[string]float64{}
		for _, sample := range value.(promql.Vector) {
			out[sample.Metric.Get("group")] = sample.F
		}
		return out
	}
	for _, testcase := range []struct {
		query string
		want  map[string]float64
	}{
		{`sum by (group) (statistics_value[1m])`, map[string]float64{"one": 18, "two": 16}},
		{`sum by (group) (statistics_value{instance="a"}[1m] + statistics_value{instance="a"}[1m])`, map[string]float64{"one": 6, "two": 12}},
		{`sum by (group) (abs(-statistics_value{instance="a"}[1m]))`, map[string]float64{"one": 3, "two": 6}},
		{`sum by (group) (statistics_value[1m:30s])`, map[string]float64{"one": 18, "two": 16}},
		// The window, unlike the lookback, has no samples here.
		{`sum by (group) (statistics_value[10s] offset 15s)`, map[string]float64{}},
		{`sum by (group) (statistics_value offset 15s)`, map[string]float64{"one": 15, "two": 14}},
		// increase of an expression evaluates it as a subquery.
		// The step is 30s, so increase covers one step of the expression.
		{`sum by (group) (increase(statistics_value[1m] * 2))`, map[string]float64{"one": 6, "two": 4}},
		{`sum by (group) (increase(statistics_value[1m] - statistics_value{instance="a"}[1m] * 0))`, map[string]float64{"one": 1, "two": 1}},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			got := sums(evaluateStatistics(test, testcase.query, false))
			if len(got) != len(testcase.want) {
				test.Fatalf("result = %v, want %v", got, testcase.want)
			}
			for group, value := range testcase.want {
				if got[group] != value {
					test.Fatalf("result = %v, want %v", got, testcase.want)
				}
			}
		})
	}
	matrix := evaluateStatistics(test, `sum by (group) (statistics_value[1m])`, true).(promql.Matrix)
	if len(matrix) != 2 || len(matrix[0].Floats) != 3 {
		test.Fatalf("range result = %v", matrix)
	}
}

func TestPrepareMetricsQLImplicitRollup(test *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, testcase := range []struct{ query, want string }{
		{`sum by (q) (m[5m])`, `sum(__snuffle_default_rollup(m[600000ms], 300000, 15000, 300000, 1)) by(q)`},
		// A bare range selector stays a range vector, as in Prometheus.
		{`m[5m]`, `m[300000ms]`},
		{`rate(m[5m])`, `__snuffle_rate(m[600000ms], 300000, 15000, 300000, 1)`},
		// Prometheus functions that take a range vector keep it.
		{`double_exponential_smoothing(m[5m], 0.5, 0.5)`, `double_exponential_smoothing(m[300000ms], 0.5, 0.5)`},
	} {
		prepared, err := prepareMetricsQLQuery(testcase.query, 15*time.Second, 5*time.Minute, start, start)
		if err != nil {
			test.Fatalf("%s: %v", testcase.query, err)
		}
		if prepared.query != testcase.want {
			test.Errorf("%s = %s, want %s", testcase.query, prepared.query, testcase.want)
		}
	}
}
