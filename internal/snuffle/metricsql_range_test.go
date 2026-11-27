package snuffle

import (
	"context"
	"errors"
	"fmt"
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

func TestPrepareMetricsQLDefault(test *testing.T) {
	start := time.Unix(1700000000, 0)
	prepare := func(query string) (metricsQLQuery, error) {
		return prepareMetricsQLQuery(query, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
	}
	for query, want := range map[string]struct {
		left, right string
		topK        bool
	}{
		`sum by (name) (rate(cpu[5m])) default 0`:              {left: `sum by (name) (rate(cpu[5m]))`, right: `0`},
		`topk_avg(5, sum by (name) (rate(cpu[5m])) default 0)`: {left: `sum by (name) (rate(cpu[5m]))`, right: `0`, topK: true},
		// default binds looser than + and or. The parser folds 0 + 1.
		`cpu default 0 + 1`:                 {left: `cpu`, right: `1`},
		`cpu default 0 or other`:            {left: `cpu`, right: `0 or other`},
		`cpu default max by (name) (other)`: {left: `cpu`, right: `max(other) by(name)`},
	} {
		prepared, err := prepare(query)
		if err != nil {
			test.Fatalf("%s: %v", query, err)
		}
		left, err := prepare(want.left)
		if err != nil {
			test.Fatal(err)
		}
		if prepared.query != left.query || prepared.defaultQuery != want.right || prepared.defaultRight == nil || want.topK != (len(prepared.rangeOps) > 0) {
			test.Errorf("%s: query %q, default %q, %d range ops", query, prepared.query, prepared.defaultQuery, len(prepared.rangeOps))
		}
	}
	for query, wantErr := range map[string]string{
		`(cpu default 0) * 2`:         "outermost",
		`sum(cpu default 0)`:          "outermost",
		`cpu default on(name) other`:  "does not support on",
		`cpu default sum(x[5m:] @ 1)`: "",
	} {
		_, err := prepare(query)
		if (err == nil) != (wantErr == "") || (err != nil && !strings.Contains(err.Error(), wantErr)) {
			test.Errorf("%s: error %v, want %q", query, err, wantErr)
		}
	}
}

func TestRangeDefault(test *testing.T) {
	const gap = -1
	group := func(metric map[string]string, values ...float64) rangeAggregateGroup {
		g := rangeAggregateGroup{metric: metric}
		for i, v := range values {
			if v != gap {
				g.points = append(g.points, samplePoint{t: int64(i), v: v})
			}
		}
		return g
	}
	nan := math.NaN()
	a := group(map[string]string{"__name__": "m", "p": "a"}, 1, gap, nan, 1)
	b := group(map[string]string{"p": "b"}, gap, 2, gap, gap)
	left := []rangeAggregateGroup{a, b}
	format := func(groups []rangeAggregateGroup) []string {
		var out []string
		for _, g := range groups {
			out = append(out, fmt.Sprint(g.metric, g.points))
		}
		return out
	}
	for name, c := range map[string]struct {
		left, right, want []rangeAggregateGroup
	}{
		"scalar": {left, []rangeAggregateGroup{group(nil, 0, 0, 0, 0)}, []rangeAggregateGroup{
			group(a.metric, 1, 0, 0, 1), group(b.metric, 0, 2, 0, 0),
		}},
		// The metric name takes no part in the match. The first right series
		// with a value fills; z matches nothing.
		"vector": {left, []rangeAggregateGroup{
			group(map[string]string{"p": "a"}, 7, gap, 7, 7),
			group(map[string]string{"__name__": "other", "p": "a"}, 8, 8, 8, 8),
			group(map[string]string{"p": "z"}, 9, 9, 9, 9),
		}, []rangeAggregateGroup{group(a.metric, 1, 8, 7, 1), b}},
		"empty left": {nil, []rangeAggregateGroup{b}, []rangeAggregateGroup{b}},
	} {
		filled, err := rangeDefault(context.Background(), c.left, c.right, 0)
		if err != nil {
			test.Fatalf("%s: %v", name, err)
		}
		if got, want := format(filled), format(c.want); !reflect.DeepEqual(got, want) {
			test.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
	if !math.IsNaN(a.points[1].v) {
		test.Errorf("rangeDefault modified its input: %v", a.points)
	}

	// The scalar fill of left makes 4 + 4 points. A budget below that fails,
	// as does a canceled context.
	scalar := []rangeAggregateGroup{group(nil, 0, 0, 0, 0)}
	if _, err := rangeDefault(context.Background(), left, scalar, 7); err == nil || !strings.Contains(err.Error(), "sample limit") {
		test.Errorf("budget of 7: error %v", err)
	}
	if _, err := rangeDefault(context.Background(), left, scalar, 8); err != nil {
		test.Errorf("budget of 8: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rangeDefault(canceled, left, scalar, 0); !errors.Is(err, context.Canceled) {
		test.Errorf("canceled context: error %v", err)
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

	prepareDefaults := func(depth int) error {
		query := "alloc"
		for range depth {
			query = "alloc default (" + query + ")"
		}
		_, err := prepareMetricsQLQuery(query, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
		return err
	}
	if err := prepareDefaults(maxRangeOps); err != nil {
		test.Errorf("%d nested defaults: %v", maxRangeOps, err)
	}
	if err := prepareDefaults(maxRangeOps + 1); err == nil || !strings.Contains(err.Error(), "nest at most") {
		test.Errorf("%d nested defaults: error %v", maxRangeOps+1, err)
	}

	prepared, err := prepareMetricsQLQuery(`topk_max(1, alloc)`, time.Minute, 5*time.Minute, start, start.Add(time.Hour))
	if err != nil {
		test.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	groups := []rangeAggregateGroup{{metric: map[string]string{"a": "1"}, points: []samplePoint{{t: 0, v: 1}}}}
	if _, queryErr := prepared.applyRangeOps(canceled, rangeQueryResult{floats: true, groups: groups}, nil); queryErr == nil || !errors.Is(queryErr.err, context.Canceled) {
		test.Errorf("canceled applyRangeOps error = %v", queryErr)
	}
}
