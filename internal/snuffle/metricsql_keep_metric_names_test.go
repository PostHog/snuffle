package snuffle

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

func evaluateKeepMetricNames(test *testing.T, expression string, rangeQuery bool) parser.Value {
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
	// Like the server, the engine removes metric names only at the end.
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableDelayedNameRemoval: true})
	options := promql.NewPrometheusQueryOpts(false, 5*time.Minute)
	var query promql.Query
	if rangeQuery {
		query, err = engine.NewRangeQuery(context.Background(), statisticsTestQueryable(), options, prepared.query, start, end, 30*time.Second)
	} else {
		query, err = engine.NewInstantQuery(context.Background(), statisticsTestQueryable(), options, prepared.query, end)
	}
	if err != nil {
		test.Fatalf("parse %s: %v", prepared.query, err)
	}
	defer query.Close()
	result := query.Exec(context.Background())
	if result.Err != nil {
		test.Fatalf("execute %s: %v", prepared.query, result.Err)
	}
	return metricsQLValue(result.Value)
}

func TestMetricsQLKeepMetricNames(test *testing.T) {
	names := func(value parser.Value) string {
		var out []string
		switch v := value.(type) {
		case promql.Vector:
			for _, sample := range v {
				out = append(out, sample.Metric.Get("__name__")+"/"+sample.Metric.Get("error"))
			}
		case promql.Matrix:
			for _, series := range v {
				out = append(out, series.Metric.Get("__name__")+"/"+series.Metric.Get("error"))
			}
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	selector := `{__name__=~"statistics_(value|bucket)", group="one", instance=~"a|", le=~"1|"}`
	for _, testcase := range []struct {
		query string
		want  string
	}{
		{`increase(` + selector + `[1m]) keep_metric_names`, "statistics_bucket/,statistics_value/"},
		{`increase(` + selector + `[1m])`, "/,/"},
		{`rate(statistics_value{instance="a", group="one"}) keep_metric_names`, "statistics_value/"},
		{`max_over_time(statistics_value{instance="a", group="one"}[1m]) keep_metric_names`, "statistics_value/"},
		{`abs(statistics_value{instance="a", group="one"}) keep_metric_names`, "statistics_value/"},
		{`abs(statistics_value{instance="a", group="one"})`, "/"},
		// The dashboard repro: the name survives until label_replace reads it.
		{`label_replace(increase(` + selector + `[1h]) keep_metric_names, "error", "$1", "__name__", "statistics_(.*)")`, "statistics_bucket/bucket,statistics_value/value"},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			for _, rangeQuery := range []bool{false, true} {
				if got := names(evaluateKeepMetricNames(test, testcase.query, rangeQuery)); got != testcase.want {
					test.Fatalf("range=%t names = %s, want %s", rangeQuery, got, testcase.want)
				}
			}
		})
	}
	start := time.Unix(1700000010, 0)
	for _, expression := range []string{
		// The argument no longer has a metric name, so VictoriaMetrics would
		// keep an empty one. Snuffle cannot tell, so it rejects the query.
		`abs(sum(m)) keep_metric_names`,
		`rate(sum(m)[5m:1m]) keep_metric_names`,
		`abs(m + 1) keep_metric_names`,
	} {
		if _, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start); err == nil || !strings.Contains(err.Error(), "keep_metric_names") {
			test.Errorf("%s: err = %v", expression, err)
		}
	}
}
