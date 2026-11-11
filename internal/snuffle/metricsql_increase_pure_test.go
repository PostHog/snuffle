package snuffle

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

func increasePureTestQueryable() storage.Queryable {
	var series []*seriesMeta
	for name, values := range map[string][]float64{
		"new_counter":   {100, 101, 102},
		"reset_counter": {100, 5, 7},
	} {
		labelMap := map[string]string{"__name__": name}
		meta := &seriesMeta{labels: labels.FromMap(labelMap), labelMap: labelMap}
		for index, value := range values {
			meta.samples = append(meta.samples, samplePoint{t: 1700000010000 + int64(index)*30000, v: value})
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

func evaluateIncreasePure(test *testing.T, expression string) float64 {
	test.Helper()
	at := time.Unix(1700000070, 0)
	prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, at, at)
	if err != nil {
		test.Fatal(err)
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableDelayedNameRemoval: true})
	query, err := engine.NewInstantQuery(context.Background(), increasePureTestQueryable(), promql.NewPrometheusQueryOpts(false, 5*time.Minute), prepared.query, at)
	if err != nil {
		test.Fatalf("parse %s: %v", prepared.query, err)
	}
	defer query.Close()
	result := query.Exec(context.Background())
	if result.Err != nil {
		test.Fatalf("execute %s: %v", prepared.query, result.Err)
	}
	vector := metricsQLValue(result.Value).(promql.Vector)
	if len(vector) != 1 {
		return math.NaN()
	}
	return vector[0].F
}

func TestMetricsQLIncreasePure(test *testing.T) {
	for _, testcase := range []struct {
		query string
		want  float64
	}{
		// increase does not count a large first value of a new series.
		{`increase(new_counter[5m])`, 2},
		// increase_pure assumes every counter starts from 0.
		{`increase_pure(new_counter[5m])`, 102},
		{`sum(increase_pure(new_counter[5m]))`, 102},
		{`increase_pure(new_counter)`, 1},
		// A sample before the window is the baseline.
		{`increase_pure(new_counter[30s])`, 1},
		{`increase_pure(reset_counter[5m])`, 107},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			if got := evaluateIncreasePure(test, testcase.query); got != testcase.want {
				test.Fatalf("result = %v, want %v", got, testcase.want)
			}
		})
	}
	if _, err := prepareMetricsQLQuery(`increase_pure(m[5m]) keep_metric_names`, time.Minute, 5*time.Minute, time.Unix(0, 0), time.Unix(0, 0)); err == nil {
		test.Fatal("keep_metric_names must be rejected until it is supported")
	}
}
