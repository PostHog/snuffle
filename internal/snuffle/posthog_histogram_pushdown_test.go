package snuffle

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestHistogramPushdownWindows(test *testing.T) {
	cells := map[int64]*histogramPushdownCell{
		// A cell that ends exactly at a window start is outside that window.
		100: {end: 100, total: 1, points: 1},
		150: {end: 150, total: 2, points: 2},
		200: {end: 200, total: 4, points: 1},
		// A cell of only non-finite points has no total and no points.
	}
	samples, present := histogramPushdownWindows(cells, []int64{100, 200, 300, 400}, 100, false)
	if !present {
		test.Fatal("want points")
	}
	want := []float64{1, 6, math.NaN(), math.NaN()}
	for index, sample := range samples {
		if math.IsNaN(want[index]) {
			if !promvalue.IsStaleNaN(sample.v) {
				test.Fatalf("point %d = %v, want a stale marker", index, sample.v)
			}
			continue
		}
		if sample.v != want[index] {
			test.Fatalf("point %d = %v, want %v", index, sample.v, want[index])
		}
	}
	rates, _ := histogramPushdownWindows(cells, []int64{200}, 100, true)
	if rates[0].v != 6/0.1 {
		test.Fatalf("rate = %v, want %v", rates[0].v, 6/0.1)
	}
	if _, present := histogramPushdownWindows(map[int64]*histogramPushdownCell{}, []int64{100}, 100, false); present {
		test.Fatal("no cells must report no points")
	}
}

func TestHistogramPushdownCandidates(test *testing.T) {
	server := newServer(ConfigFromEnv())
	cases := []struct {
		query string
		want  int
	}{
		{`histogram_quantile(0.5, sum by (le) (increase(a_bucket[1h]) and ignoring(le) increase(a_bucket{le="3000"}[1h])))`, 2},
		{`sum(rate(a_count[5m])) / sum(rate(a_sum[5m]))`, 2},
		{`increase(a_count[1h])`, 1},
		// A plain counter has no histogram suffix.
		{`increase(requests_total[1h])`, 0},
		// prepareMetricsQLQuery writes the automatic window as the step, and
		// the engine uses that literal window too.
		{`increase(a_count)`, 1},
		{`increase(a_count[1h] offset 1h)`, 0},
		{`max_over_time(increase(a_count[1h])[1d:1h])`, 0},
		{`irate(a_count[5m])`, 0},
		{`sum by (__name__) (increase(a_count[1h]))`, 0},
		{`increase(a_count[1h]) / on(__name__) increase(a_sum[1h])`, 0},
		{`label_replace(increase(a_count[1h]), "x", "y", "", "")`, 0},
		{`timestamp(increase(a_count[1h]))`, 0},
	}
	for _, item := range cases {
		prepared, err := prepareMetricsQLQuery(item.query, time.Minute, 5*time.Minute, time.Unix(1_700_000_000, 0), time.Unix(1_700_000_000, 0))
		if err != nil {
			test.Fatalf("%s: %v", item.query, err)
		}
		expr, err := server.parser.ParseExpr(prepared.query)
		if err != nil {
			test.Fatalf("%s: %v", prepared.query, err)
		}
		if got := len(histogramPushdownCandidates(expr)); got != item.want {
			test.Fatalf("%s (prepared %s): %d candidates, want %d", item.query, prepared.query, got, item.want)
		}
	}
}

func TestHistogramPushdownRewriteParses(test *testing.T) {
	server := newServer(ConfigFromEnv())
	prepared, err := prepareMetricsQLQuery(`histogram_quantile(0.5, sum by (le, "client.timezone") (increase({"a_bucket", "x.y"="1"}[1h]) and ignoring(le) increase({"a_bucket", le="3000"}[1h])))`, time.Minute, 5*time.Minute, time.Unix(1_700_000_000, 0), time.Unix(1_700_000_000, 0))
	if err != nil {
		test.Fatal(err)
	}
	expr, err := server.parser.ParseExpr(prepared.query)
	if err != nil {
		test.Fatal(err)
	}
	candidates := histogramPushdownCandidates(expr)
	replacements := make(map[*parser.Call]parser.Expr, len(candidates))
	for index, candidate := range candidates {
		name := histogramPushdownPrefix + strconv.Itoa(index) + candidate.alias.suffix
		replacements[candidate.call] = &parser.VectorSelector{Name: name, LabelMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}}
	}
	rewritten := replaceHistogramPushdownCalls(expr, replacements).String()
	want := `histogram_quantile(0.5, sum by (le, "client.timezone") (__snuffle_pushdown_0_bucket and ignoring (le) __snuffle_pushdown_1_bucket))`
	if rewritten != want {
		test.Fatalf("rewritten = %s, want %s", rewritten, want)
	}
	if _, err := server.parser.ParseExpr(rewritten); err != nil {
		test.Fatalf("rewritten query does not parse: %v", err)
	}
}

func TestRangeEvalTimesNearInt64Limit(test *testing.T) {
	// 9223372036854774s is ~1.6s below the int64 millisecond limit, so
	// stepping past end overflows. The loop must still stop after one point.
	end := time.UnixMilli(9223372036854774000)
	times := rangeEvalTimes(end, end, 2*time.Second)
	if len(times) != 1 || times[0] != end.UnixMilli() {
		test.Fatalf("want one evaluation time, got %v", times)
	}
	start := time.UnixMilli(9223372036854770000)
	times = rangeEvalTimes(start, end, 2*time.Second)
	if len(times) != 3 || times[2] != end.UnixMilli() {
		test.Fatalf("want three evaluation times, got %v", times)
	}
}
