package snuffle

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

func TestPrepareMetricsQLExpressionOffsets(test *testing.T) {
	start := time.Unix(1700000010, 0)
	for _, expression := range []string{
		`sum(increase(requests_total{service=~"api|worker"}[30s])) offset 24h`,
		`sum(requests_total) by (service) offset -24h`,
		`abs(sum(requests_total)) offset 0s`,
		`(sum(requests_total) + sum(requests_total offset 1h)) offset 2i`,
		`(sum(requests_total) offset 1h) offset 2h`,
		`timestamp(sum(requests_total) offset 24h)`,
		`absent(sum(requests_total) offset 24h)`,
		`max_over_time((sum(requests_total) offset 24h)[1m:15s])`,
		`sum(max_over_time(requests_total[1m])) offset 24h`,
		`sum(requests_total) @ 1700000010 offset 24h`,
		`vector(time()) offset 24h`,
	} {
		test.Run(expression, func(test *testing.T) {
			for _, end := range []time.Time{start, start.Add(time.Minute)} {
				prepared, err := prepareMetricsQLQuery(expression, 15*time.Second, 5*time.Minute, start, end)
				if err != nil {
					test.Fatal(err)
				}
				if _, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query); err != nil {
					test.Fatalf("parse %s: %v", prepared.query, err)
				}
			}
		})
	}
}

func offsetTestQueryable(start time.Time) storage.Queryable {
	var series []*seriesMeta
	for _, factor := range []float64{1, 2} {
		service := "api"
		if factor == 2 {
			service = "worker"
		}
		labelMap := map[string]string{"__name__": "requests_total", "service": service}
		meta := &seriesMeta{labels: labels.FromMap(labelMap), labelMap: labelMap}
		for _, period := range []struct {
			duration time.Duration
			factor   float64
		}{{-24 * time.Hour, 1}, {0, 10}, {24 * time.Hour, 100}} {
			for index := -4; index <= 6; index++ {
				meta.samples = append(meta.samples, samplePoint{
					t: start.Add(period.duration + time.Duration(index)*15*time.Second).UnixMilli(),
					v: 1000 + float64((index+4)*(index+4))*factor*period.factor,
				})
			}
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

func TestMetricsQLExpressionOffsetEvaluation(test *testing.T) {
	base := time.Unix(1700000010, 0)
	for _, testcase := range []struct {
		query   string
		want    []float64
		service string
	}{
		{query: `sum(increase(requests_total{service=~"api|worker"}[30s])) offset 24h`, want: []float64{36, 48, 60}},
		{query: `sum(increase(requests_total[30s])) offset -24h`, want: []float64{3600, 4800, 6000}},
		{query: `sum(increase(requests_total[30s])) offset 0s`, want: []float64{360, 480, 600}},
		{query: `sum(increase(requests_total[2i])) offset 5760i`, want: []float64{36, 48, 60}},
		{query: `(sum(increase(requests_total[30s])) offset 12h) offset 12h`, want: []float64{36, 48, 60}},
		{query: `sum(increase(requests_total[30s] offset 12h)) offset 12h`, want: []float64{36, 48, 60}},
		{query: `abs(sum(increase(requests_total[30s]))) offset 24h`, want: []float64{36, 48, 60}},
		{query: `(sum(increase(requests_total[30s])) * 2) offset 24h`, want: []float64{72, 96, 120}},
		{query: `sum(increase(requests_total[30s])) @ 1700000010 offset 24h`, want: []float64{36, 36, 36}},
		{query: `vector(time()) offset 24h`, want: []float64{1700000010 - 86400, 1700000025 - 86400, 1700000040 - 86400}},
		{query: `sum(increase(requests_total[30s])) offset 48h`},
		{query: `sum by(service) (increase(requests_total{service="api"}[30s])) offset 24h`, want: []float64{12, 16, 20}, service: "api"},
		{query: `max_over_time((sum(increase(requests_total[30s])) offset 24h)[30s:15s])`, want: []float64{36, 48, 60}},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			for _, delay := range []time.Duration{0, 7 * time.Second} {
				for _, rangeQuery := range []bool{false, true} {
					start := base.Add(delay)
					end := start
					if rangeQuery {
						end = start.Add(30 * time.Second)
					}
					prepared, err := prepareMetricsQLQuery(testcase.query, 15*time.Second, 5*time.Minute, start, end)
					if err != nil {
						test.Fatal(err)
					}
					engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableAtModifier: true, EnableNegativeOffset: true})
					options := promql.NewPrometheusQueryOpts(false, 5*time.Minute)
					ctx := context.Background()
					var query promql.Query
					if rangeQuery {
						query, err = engine.NewRangeQuery(ctx, offsetTestQueryable(base), options, prepared.query, start, end, 15*time.Second)
					} else {
						query, err = engine.NewInstantQuery(ctx, offsetTestQueryable(base), options, prepared.query, start)
					}
					if err != nil {
						test.Fatalf("parse %s: %v", prepared.query, err)
					}
					result := query.Exec(ctx)
					if result.Err != nil {
						query.Close()
						test.Fatalf("execute %s: %v", prepared.query, result.Err)
					}
					var points []promql.FPoint
					switch value := result.Value.(type) {
					case promql.Vector:
						if len(value) > 1 {
							test.Fatalf("unexpected vector: %v", value)
						}
						if len(value) == 1 {
							points = []promql.FPoint{{T: value[0].T, F: value[0].F}}
							if value[0].Metric.Get("service") != testcase.service {
								test.Errorf("unexpected labels: %v", value[0].Metric)
							}
						}
					case promql.Matrix:
						if len(value) > 1 {
							test.Fatalf("unexpected matrix: %v", value)
						}
						if len(value) == 1 {
							points = value[0].Floats
							if value[0].Metric.Get("service") != testcase.service {
								test.Errorf("unexpected labels: %v", value[0].Metric)
							}
						}
					default:
						test.Fatalf("unexpected result: %T", value)
					}
					want := testcase.want
					if !rangeQuery && len(want) > 0 {
						want = want[:1]
					}
					if len(points) != len(want) {
						test.Errorf("range=%t delay=%s: got %v, want %v", rangeQuery, delay, points, want)
					} else {
						for index, point := range points {
							timestamp := start.Add(time.Duration(index) * 15 * time.Second).UnixMilli()
							if point.F != want[index] || point.T != timestamp {
								test.Errorf("range=%t delay=%s point=%d: got %v at %d, want %v at %d", rangeQuery, delay, index, point.F, point.T, want[index], timestamp)
							}
						}
					}
					query.Close()
				}
			}
		})
	}
}
