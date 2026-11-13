package snuffle

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestMetricsQLQuantiles(test *testing.T) {
	// statistics_value at the instant: group one has 3, 4, 11 and group two 6, 10.
	byPhi := func(value parser.Value) map[string]float64 {
		out := map[string]float64{}
		for _, sample := range value.(promql.Vector) {
			if sample.Metric.Has("__name__") || sample.Metric.Has("instance") {
				test.Fatalf("unexpected labels %v", sample.Metric)
			}
			out[sample.Metric.Get("group")+"/"+sample.Metric.Get("p")] = sample.F
		}
		return out
	}
	for _, testcase := range []struct {
		query string
		want  map[string]float64
	}{
		{`quantiles("p", 0.5, 1, statistics_value)`, map[string]float64{"/0.5": 6, "/1": 11}},
		{`quantiles("p", 0.9, 0.1, statistics_value) by (group)`, map[string]float64{"one/0.9": 9.6, "one/0.1": 3.2, "two/0.9": 9.6, "two/0.1": 6.4}},
		{`quantiles("p", 0.25, statistics_value) without (instance)`, map[string]float64{"one/0.25": 3.5, "two/0.25": 7}},
		{`quantiles("p", -1, 2, statistics_value)`, map[string]float64{"/-1": math.Inf(-1), "/2": math.Inf(1)}},
		{`quantiles("p", 0.5, avg_over_time(statistics_value[1m]))`, map[string]float64{"/0.5": 5.5}},
		{`quantiles("p", 0.5, missing_metric)`, map[string]float64{}},
		{`quantiles("p", 0.5, 1, time())`, map[string]float64{"/0.5": 1700000070, "/1": 1700000070}},
		{`quantiles("p", 0.5, time() - 70)`, map[string]float64{"/0.5": 1700000000}},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			got := byPhi(evaluateStatistics(test, testcase.query, false))
			if len(got) != len(testcase.want) {
				test.Fatalf("result = %v, want %v", got, testcase.want)
			}
			for key, value := range testcase.want {
				if math.Abs(got[key]-value) > 1e-9 && got[key] != value {
					test.Fatalf("result = %v, want %v", got, testcase.want)
				}
			}
		})
	}
	matrix := evaluateStatistics(test, `quantiles("p", 0.5, 1, statistics_value) by (group)`, true).(promql.Matrix)
	if len(matrix) != 4 {
		test.Fatalf("range result = %v", matrix)
	}
	for _, series := range matrix {
		if len(series.Floats) != 3 {
			test.Fatalf("range series = %v", series)
		}
	}
	// The dashboard repro.
	if _, err := prepareMetricsQLQuery(`quantiles("p", 0.9, 0.1, avg_over_time(consumer_batch_utilization[5m]))`, 30*time.Second, 5*time.Minute, time.Unix(1700000010, 0), time.Unix(1700000070, 0)); err != nil {
		test.Fatal(err)
	}
	start := time.Unix(1700000010, 0)
	for _, expression := range []string{
		`quantiles("p", statistics_value)`,
		`quantiles(0.5, 0.9, statistics_value)`,
		`quantiles("", 0.5, statistics_value)`,
		`quantiles("p", "0.5", statistics_value)`,
		`quantiles("p", time(), statistics_value)`,
		`__snuffle_quantiles("{}", "p", statistics_value, 0.5)`,
	} {
		prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start)
		if err == nil {
			_, err = parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
		}
		if err == nil {
			test.Errorf("%s: expected an error, prepared %s", expression, prepared.query)
		}
	}
	if _, err := prepareMetricsQLQuery(`quantiles("p", 0.5, statistics_value) by (group) limit 1`, 30*time.Second, 5*time.Minute, start, start.Add(time.Minute)); err == nil {
		test.Fatal("range queries must not apply a different group limit at each step")
	}
}

func TestMetricsQLQuantilesSampleBudget(test *testing.T) {
	// Two groups times two phis is four samples, over a budget of three.
	metricsQLMaxOutputSamples.Store(3)
	defer metricsQLMaxOutputSamples.Store(0)
	at := time.Unix(1700000070, 0)
	prepared, err := prepareMetricsQLQuery(`quantiles("p", 0.9, 0.1, statistics_value) by (group)`, 30*time.Second, 5*time.Minute, at, at)
	if err != nil {
		test.Fatal(err)
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute})
	query, err := engine.NewInstantQuery(context.Background(), statisticsTestQueryable(), promql.NewPrometheusQueryOpts(false, 5*time.Minute), prepared.query, at)
	if err != nil {
		test.Fatal(err)
	}
	defer query.Close()
	result := query.Exec(context.Background())
	var tooMany promql.ErrTooManySamples
	if !errors.As(result.Err, &tooMany) {
		test.Fatalf("err = %v, want ErrTooManySamples", result.Err)
	}
	metricsQLMaxOutputSamples.Store(4)
	if got := evaluateStatistics(test, `quantiles("p", 0.9, 0.1, statistics_value) by (group)`, false).(promql.Vector); len(got) != 4 {
		test.Fatalf("within budget: %v", got)
	}

	// A range query over three steps preallocates three points for each of
	// the four series, so it needs a budget of twelve.
	start := time.Unix(1700000010, 0)
	end := start.Add(time.Minute)
	prepared, err = prepareMetricsQLQuery(`quantiles("p", 0.9, 0.1, statistics_value) by (group)`, 30*time.Second, 5*time.Minute, start, end)
	if err != nil {
		test.Fatal(err)
	}
	metricsQLMaxOutputSamples.Store(11)
	rangeQuery, err := engine.NewRangeQuery(context.Background(), statisticsTestQueryable(), promql.NewPrometheusQueryOpts(false, 5*time.Minute), prepared.query, start, end, 30*time.Second)
	if err != nil {
		test.Fatal(err)
	}
	defer rangeQuery.Close()
	if result := rangeQuery.Exec(context.Background()); !errors.As(result.Err, &tooMany) {
		test.Fatalf("range err = %v, want ErrTooManySamples", result.Err)
	}
	metricsQLMaxOutputSamples.Store(12)
	if got := evaluateStatistics(test, `quantiles("p", 0.9, 0.1, statistics_value) by (group)`, true).(promql.Matrix); len(got) != 4 {
		test.Fatalf("range within budget: %v", got)
	}
}

func TestMetricsQLQuantilesPointsPerSeries(test *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, testcase := range []struct {
		query string
		end   time.Time
		want  string
	}{
		{`quantiles("p", 0.5, x)`, start, `"points":1`},
		{`quantiles("p", 0.5, x)`, start.Add(time.Hour), `"points":121`},
		{`quantiles("p", 0.5, x)`, start.Add(30 * 24 * time.Hour), `"points":5000`},
		{`max_over_time(quantiles("p", 0.5, x)[10m:1m])`, start, `"points":11`},
		// An hour-long range query evaluates the subquery across the whole range.
		{`max_over_time(quantiles("p", 0.5, x)[10m:1m])`, start.Add(time.Hour), `"points":71`},
		{`max_over_time(quantiles("p", 0.5, x)[1s:1s])`, start.Add(time.Hour), `"points":3602`},
		{`max_over_time(max_over_time(quantiles("p", 0.5, x)[1m:1m])[10m:1m])`, start.Add(time.Hour), `"points":72`},
		// Counter rollups add the lookback to the window: (10m + 5m)/1m + 1.
		{`rate(quantiles("p", 0.5, x)[10m:1m])`, start, `"points":16`},
		{`rate(quantiles("p", 0.1, 0.9, time())[1s:1s])`, start, `"points":302`},
		{`max_over_time(quantiles("p", 0.5, x)[100d:1s])`, start, `"points":5000`},
	} {
		prepared, err := prepareMetricsQLQuery(testcase.query, 30*time.Second, 5*time.Minute, start, testcase.end)
		if err != nil {
			test.Fatal(err)
		}
		if !strings.Contains(prepared.query, strings.ReplaceAll(testcase.want, `"`, `\"`)) {
			test.Fatalf("%s (end %v) = %s, want %s", testcase.query, testcase.end, prepared.query, testcase.want)
		}
	}
}

func TestMetricsQLQuantilesSubqueryBudget(test *testing.T) {
	// The subquery evaluates 11 steps over 2 groups, so 22 points fit a
	// budget of 22 even though the engine could allocate 5000 per series.
	query := `max_over_time(quantiles("p", 0.5, statistics_value) by (group)[10m:1m])`
	metricsQLMaxOutputSamples.Store(22)
	defer metricsQLMaxOutputSamples.Store(0)
	if got := evaluateStatistics(test, query, false).(promql.Vector); len(got) != 2 {
		test.Fatalf("within budget: %v", got)
	}
	metricsQLMaxOutputSamples.Store(21)
	at := time.Unix(1700000070, 0)
	prepared, err := prepareMetricsQLQuery(query, 30*time.Second, 5*time.Minute, at, at)
	if err != nil {
		test.Fatal(err)
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 100000, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute})
	instant, err := engine.NewInstantQuery(context.Background(), statisticsTestQueryable(), promql.NewPrometheusQueryOpts(false, 5*time.Minute), prepared.query, at)
	if err != nil {
		test.Fatal(err)
	}
	defer instant.Close()
	var tooMany promql.ErrTooManySamples
	if result := instant.Exec(context.Background()); !errors.As(result.Err, &tooMany) {
		test.Fatalf("err = %v, want ErrTooManySamples", result.Err)
	}
}

func TestMetricsQLQuantilesDottedNames(test *testing.T) {
	at := time.Unix(1700000070, 0)
	for _, query := range []string{
		`quantiles("p", 0.5, {"http.server.duration"})`,
		`quantiles("p", 0.5, statistics_value{"http.method"="GET"}) by ("http.method")`,
		`median({"http.server.duration"})`,
	} {
		if _, err := prepareMetricsQLQuery(query, 30*time.Second, 5*time.Minute, at, at); err != nil {
			test.Fatalf("%s: %v", query, err)
		}
	}
}

func TestMetricsQLQuantilesSeriesBudgetIsCumulative(test *testing.T) {
	call := &parser.StringLiteral{Val: "call"}
	other := &parser.StringLiteral{Val: "other"}
	group := func(value string) labels.Labels { return labels.FromStrings("group", value) }
	phis := []float64{0.1, 0.9}
	// A budget of 6 points at 1 point per series allows 6 distinct series.
	tracker := quantilesSeriesFor(call)
	if !tracker.admit([]labels.Labels{group("a"), group("b")}, phis, 1, 6) {
		test.Fatal("four series fit a budget of six")
	}
	// The same groups again add nothing.
	if !tracker.admit([]labels.Labels{group("a"), group("b")}, phis, 1, 6) {
		test.Fatal("repeated groups must not count twice")
	}
	// A different group at the next step brings the total to six.
	if !tracker.admit([]labels.Labels{group("c")}, phis, 1, 6) {
		test.Fatal("six series fit a budget of six")
	}
	// Another new group would make eight, over the budget, though each step is small.
	if tracker.admit([]labels.Labels{group("d")}, phis, 1, 6) {
		test.Fatal("groups that change between steps must count cumulatively")
	}
	// Points per series scale the budget, and trackers are per call.
	if quantilesSeriesFor(other).admit([]labels.Labels{group("a")}, phis, 4, 6) {
		test.Fatal("two series at four points exceed a budget of six")
	}
	if quantilesSeriesFor(call) != tracker {
		test.Fatal("the tracker must be stable for a call")
	}
}

func TestMetricsQLQuantilesRepeatedPhis(test *testing.T) {
	at := time.Unix(1700000070, 0)
	for _, query := range []string{`quantiles("p", 0.5, 0.50, x)`, `quantiles("p", 1, 1, 1, x)`} {
		if _, err := prepareMetricsQLQuery(query, 30*time.Second, 5*time.Minute, at, at); err == nil {
			test.Fatalf("%s must be rejected", query)
		}
	}
	// A guard against repeated phis that bypass rewriting: one step's output
	// is counted in full, whatever the distinct series are.
	groups := []labels.Labels{labels.FromStrings("group", "a")}
	repeated := make([]float64, 100)
	if quantilesSeriesFor(&parser.StringLiteral{Val: "repeated"}).admit(groups, repeated, 1, 10) {
		test.Fatal("a hundred outputs exceed a budget of ten")
	}
}
