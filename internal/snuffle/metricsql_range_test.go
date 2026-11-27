package snuffle

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPrepareMetricsQLRangeTopK(test *testing.T) {
	start := time.Unix(1700000000, 0)
	prepare := func(query string) (metricsQLQuery, error) {
		return prepareMetricsQLQuery(query, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
	}
	for query, wantOps := range map[string]int{
		`topk_max(10, max by (partition) (depth))`:                  1,
		`topk_avg(15, sum by (nodepool) (alloc), "nodepool=other")`: 1,
		`bottomk_last(1, topk_min(2, alloc))`:                       2,
		`topk_median(1, running_sum(alloc))`:                        1,
		`topk_max(5, alloc[5m])`:                                    1,
		`topk_avg(3, alloc[1h:1m])`:                                 1,
	} {
		prepared, err := prepare(query)
		if err != nil {
			test.Fatalf("%s: %v", query, err)
		}
		if len(prepared.rangeOps) != wantOps || strings.Contains(prepared.query, "topk") {
			test.Errorf("%s: %d range ops, query %q", query, len(prepared.rangeOps), prepared.query)
		}
	}
	for query, wantErr := range map[string]string{
		`sum(topk_max(1, alloc))`:             "outermost",
		`running_sum(topk_max(1, alloc))`:     "outermost",
		`topk_max(1, alloc) by (nodepool)`:    "by, without or limit",
		`topk_max(scalar(alloc), alloc)`:      "number",
		`topk_max(1, alloc, 2)`:               "string",
		`topk_max(1)`:                         "2 or 3 arguments",
		`topk_max(1, alloc, "other", "more")`: "2 or 3 arguments",
	} {
		if _, err := prepare(query); err == nil || !strings.Contains(err.Error(), wantErr) {
			test.Errorf("%s: error %v, want %q", query, err, wantErr)
		}
	}
}

func TestRangeTopK(test *testing.T) {
	group := func(name string, values ...float64) rangeAggregateGroup {
		g := rangeAggregateGroup{metric: map[string]string{"name": name}}
		for i, v := range values {
			g.points = append(g.points, samplePoint{t: int64(i), v: v})
		}
		return g
	}
	nan := math.NaN()
	groups := []rangeAggregateGroup{group("a", 1, 9), group("b", nan), group("c", 5, nan), group("d", 5, 2)}
	names := func(out []rangeAggregateGroup) []string {
		var names []string
		for _, g := range out {
			names = append(names, g.metric["name"]+g.metric["rest"])
		}
		return names
	}
	byMax := metricsQLRangeStats["max"]
	topK := func(groups []rangeAggregateGroup, k float64, rank func([]float64) float64, bottom bool, remainder string) []rangeAggregateGroup {
		out, err := rangeTopK(context.Background(), groups, k, rank, bottom, remainder)
		if err != nil {
			test.Fatal(err)
		}
		return out
	}
	// The series with only NaN ranks last; c and d tie and keep label order.
	if got := names(topK(groups, 3, byMax, false, "")); !reflect.DeepEqual(got, []string{"a", "c", "d"}) {
		test.Errorf("topk_max = %v", got)
	}
	if got := names(topK(groups, 1, byMax, true, "")); !reflect.DeepEqual(got, []string{"c"}) {
		test.Errorf("bottomk_max = %v", got)
	}
	if got := names(topK(groups, 10, byMax, false, "rest")); !reflect.DeepEqual(got, []string{"a", "c", "d"}) {
		test.Errorf("topk_max(10) = %v, want no empty series and no remainder", got)
	}
	if got := names(topK(groups, nan, byMax, false, "")); got != nil {
		test.Errorf("topk_max(NaN) = %v", got)
	}
	out := topK(groups, 1, byMax, false, "rest")
	if remainder := out[len(out)-1]; !reflect.DeepEqual(remainder, rangeAggregateGroup{metric: map[string]string{"rest": "rest"}, points: []samplePoint{{0, 10}, {1, 2}}}) {
		test.Errorf("remainder = %+v", remainder)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, remainder := range []string{"", "rest"} {
		if _, err := rangeTopK(canceled, groups, 1, byMax, false, remainder); !errors.Is(err, context.Canceled) {
			test.Errorf("canceled rangeTopK(%q) error = %v", remainder, err)
		}
	}
	if got := metricsQLRangeStats["median"]([]float64{4, 1, 3, 2}); got != 3 {
		test.Errorf("median = %v, want the upper middle value 3", got)
	}
}

func TestRangeOpsBounds(test *testing.T) {
	start := time.Unix(1700000000, 0)
	prepare := func(depth int) error {
		query := "alloc"
		for range depth {
			query = "topk_median(1, " + query + ")"
		}
		_, err := prepareMetricsQLQuery(query, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
		return err
	}
	if err := prepare(maxRangeOps); err != nil {
		test.Errorf("%d nested calls: %v", maxRangeOps, err)
	}
	if err := prepare(maxRangeOps + 1); err == nil || !strings.Contains(err.Error(), "nest at most") {
		test.Errorf("%d nested calls: error %v", maxRangeOps+1, err)
	}

	prepared, err := prepareMetricsQLQuery(`topk_max(1, alloc)`, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
	if err != nil {
		test.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	groups := []rangeAggregateGroup{{metric: map[string]string{"a": "1"}, points: []samplePoint{{t: 0, v: 1}}}}
	if _, queryErr := prepared.applyRangeOps(canceled, rangeQueryResult{floats: true, groups: groups}); queryErr == nil || !errors.Is(queryErr.err, context.Canceled) {
		test.Errorf("canceled applyRangeOps error = %v", queryErr)
	}
}
