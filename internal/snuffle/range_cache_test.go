package snuffle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

const (
	testRangeStep   = int64(time.Minute / time.Millisecond)
	testRangeOffset = 5 * time.Minute
)

// testRangeNow is on the step grid, so the tests can name steps by their distance from it.
var testRangeNow = time.UnixMilli(1_700_000_040_000).UTC()

// rangeSource is a query backend for the cache tests. Each series has a value
// at every step in its life, and the source records the ranges it evaluates.
type rangeSource struct {
	series []rangeSourceSeries
	// unconfirmed makes the source evaluate without a ClickHouse query, as a
	// query without a selector does. Such an evaluation does not prove the credentials.
	unconfirmed bool
	mu          sync.Mutex
	calls       [][2]int64
	err         *rangeQueryError
}

type rangeSourceSeries struct {
	name  string
	first int64
	last  int64
	value func(t int64) float64
}

func (source *rangeSource) eval(_ context.Context, start, end time.Time) (rangeQueryResult, *rangeQueryError) {
	source.mu.Lock()
	source.calls = append(source.calls, [2]int64{start.UnixMilli(), end.UnixMilli()})
	source.mu.Unlock()
	if source.err != nil {
		return rangeQueryResult{backend: "test"}, source.err
	}
	return rangeQueryResult{backend: "test", floats: true, clickHouseAccepted: !source.unconfirmed, groups: source.groups(start.UnixMilli(), end.UnixMilli())}, nil
}

// groups returns what an evaluation without a cache returns.
func (source *rangeSource) groups(start, end int64) []rangeAggregateGroup {
	groups := make([]rangeAggregateGroup, 0, len(source.series))
	for _, series := range source.series {
		var points []samplePoint
		for t := start; t <= end; t += testRangeStep {
			if t >= series.first && t <= series.last {
				points = append(points, samplePoint{t: t, v: series.value(t)})
			}
		}
		if len(points) > 0 {
			groups = append(groups, rangeAggregateGroup{metric: map[string]string{"name": series.name}, points: points})
		}
	}
	return groups
}

func stepValue(t int64) float64 { return float64(t / testRangeStep) }

func newTestRangeCache(now *time.Time) *rangeResultCache {
	cache := newRangeResultCache(Config{RangeCacheMaxBytes: 1 << 20, RangeCacheOffset: testRangeOffset, RangeCacheTTL: time.Hour}, nil)
	cache.now = func() time.Time { return *now }
	return cache
}

func verified(context.Context) error { return nil }

func minutes(count int64) int64 { return count * testRangeStep }

// A dashboard repeats a query over a window that moves forward. The second
// request must evaluate only the steps that were not settled at the first
// request, and its result must be the result of a complete evaluation.
func TestRangeCacheEvaluatesOnlyUnsettledStepsOfAMovingWindow(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	// The series are in label order, which is the order of the cache results.
	source := &rangeSource{series: []rangeSourceSeries{
		// This series starts after the first request, so only the evaluation holds it.
		{name: "started", first: base + minutes(2), last: base + minutes(60), value: stepValue},
		{name: "steady", first: 0, last: base + minutes(60), value: stepValue},
		// This series stops in the settled steps, so only the cache holds it at the second request.
		{name: "stopped", first: 0, last: base - minutes(20), value: stepValue},
	}}
	cache := newTestRangeCache(&now)
	key := rangeCacheKeyParts{query: "q"}.key(0)

	first, status, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, source.eval)
	if err != nil || status != rangeCacheMiss {
		t.Fatalf("first request: status %q, error %v", status, err)
	}
	if want := source.groups(base-minutes(60), base); !reflect.DeepEqual(first.groups, want) {
		t.Fatalf("first request result = %v, want %v", first.groups, want)
	}

	now = now.Add(5 * time.Minute)
	source.calls = nil
	second, status, err := cache.query(context.Background(), key, base-minutes(55), base+minutes(5), testRangeStep, false, verified, source.eval)
	if err != nil || status != rangeCachePartial {
		t.Fatalf("second request: status %q, error %v", status, err)
	}
	// At the first request, the steps up to 6 minutes before it were settled: the offset plus one step.
	if want := [][2]int64{{base - minutes(5), base + minutes(5)}}; !reflect.DeepEqual(source.calls, want) {
		t.Fatalf("second request evaluated %v, want %v", source.calls, want)
	}
	if want := source.groups(base-minutes(55), base+minutes(5)); !reflect.DeepEqual(second.groups, want) {
		t.Fatalf("second request result = %v, want %v", second.groups, want)
	}
}

// A sample that arrives late changes the steps near the current time. The
// cache must not keep those steps, or the late sample never shows.
func TestRangeCacheEvaluatesStepsInTheOffsetAgain(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	late := 0.0
	source := &rangeSource{series: []rangeSourceSeries{{name: "a", first: 0, last: base + minutes(60), value: func(t int64) float64 {
		if t == base-minutes(4) {
			return late
		}
		return stepValue(t)
	}}}}
	cache := newTestRangeCache(&now)
	key := rangeCacheKeyParts{query: "q"}.key(0)
	if _, _, err := cache.query(context.Background(), key, base-minutes(30), base, testRangeStep, false, verified, source.eval); err != nil {
		t.Fatal(err)
	}

	late = 42
	now = now.Add(time.Minute)
	result, _, err := cache.query(context.Background(), key, base-minutes(29), base+minutes(1), testRangeStep, false, verified, source.eval)
	if err != nil {
		t.Fatal(err)
	}
	if want := source.groups(base-minutes(29), base+minutes(1)); !reflect.DeepEqual(result.groups, want) {
		t.Fatalf("result = %v, want the late sample at 4 minutes before the first request: %v", result.groups, want)
	}
}

func refused(context.Context) error { return errors.New("authentication failed") }

// The cache key holds the user but not the password. A request with the user of
// a cached entry and a wrong password must get no cached step and must not
// change the entry. The evaluation of the request is its only answer.
func TestRangeCacheGivesNoCachedStepToARequestThatClickHouseDidNotConfirm(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	// The values change after the entry is stored, so a cached step differs from an evaluated step.
	generation := 0.0
	value := func(t int64) float64 { return stepValue(t) + generation }
	source := &rangeSource{series: []rangeSourceSeries{{name: "a", first: 0, last: base + minutes(60), value: value}}}
	cache := newTestRangeCache(&now)
	key := rangeCacheKeyParts{query: "q"}.key(0)
	if _, _, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, source.eval); err != nil {
		t.Fatal(err)
	}
	cachedSteps := source.groups(base-minutes(50), base-minutes(10))
	generation = 1000
	now = now.Add(10 * time.Minute)

	for _, tc := range []struct {
		name        string
		start, end  int64
		unconfirmed bool
		noCache     bool
	}{
		{name: "every step is cached", start: -50, end: -10},
		// This evaluation reads nothing from ClickHouse, so it does not confirm the request.
		{name: "the first steps are cached", start: -50, end: 10, unconfirmed: true},
		{name: "the request replaces the entry", start: -50, end: 10, unconfirmed: true, noCache: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source.unconfirmed = tc.unconfirmed
			source.calls = nil
			start, end := base+minutes(tc.start), base+minutes(tc.end)
			result, status, err := cache.query(context.Background(), key, start, end, testRangeStep, tc.noCache, refused, source.eval)
			if err != nil || status != rangeCacheBypass {
				t.Fatalf("status %q, error %v", status, err)
			}
			if want := source.groups(start, end); !reflect.DeepEqual(result.groups, want) {
				t.Fatalf("result = %v, want only evaluated steps %v", result.groups, want)
			}
			if last := source.calls[len(source.calls)-1]; last != [2]int64{start, end} {
				t.Fatalf("evaluations = %v, want a last evaluation of every step", source.calls)
			}
		})
	}

	source.unconfirmed = false
	source.calls = nil
	result, status, err := cache.query(context.Background(), key, base-minutes(50), base-minutes(10), testRangeStep, false, verified, source.eval)
	if err != nil || status != rangeCacheHit || len(source.calls) != 0 || !reflect.DeepEqual(result.groups, cachedSteps) {
		t.Fatalf("confirmed request: status %q, error %v, evaluations %v; want the steps stored before the refused requests", status, err, source.calls)
	}
}

// An evaluation that reads ClickHouse proves the credentials of its request.
// The cache must ask ClickHouse itself only for a request without such an evaluation.
func TestRangeCacheAsksClickHouseOnlyWhenTheEvaluationDidNot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		start, end  int64
		unconfirmed bool
		want        int
	}{
		{name: "every step is cached", start: -50, end: -10, want: 1},
		{name: "the evaluation read ClickHouse", start: -50, end: 10, want: 0},
		{name: "the evaluation read nothing from ClickHouse", start: -50, end: 10, unconfirmed: true, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := testRangeNow
			base := now.UnixMilli()
			source := &rangeSource{series: []rangeSourceSeries{{name: "a", first: 0, last: base + minutes(60), value: stepValue}}}
			cache := newTestRangeCache(&now)
			key := rangeCacheKeyParts{query: "q"}.key(0)
			if _, _, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, source.eval); err != nil {
				t.Fatal(err)
			}
			now = now.Add(10 * time.Minute)
			source.unconfirmed = tc.unconfirmed
			checks := 0
			start, end := base+minutes(tc.start), base+minutes(tc.end)
			result, status, err := cache.query(context.Background(), key, start, end, testRangeStep, false, func(context.Context) error {
				checks++
				return nil
			}, source.eval)
			if err != nil || status == rangeCacheBypass || checks != tc.want {
				t.Fatalf("status %q, error %v, credential checks %d, want %d", status, err, checks, tc.want)
			}
			if want := source.groups(start, end); !reflect.DeepEqual(result.groups, want) {
				t.Fatalf("result = %v, want %v", result.groups, want)
			}
		})
	}
}

// Each of these requests cannot use the cached steps. A request that used
// them would return steps that are missing, expired or explicitly refused.
func TestRangeCacheEvaluatesEveryStepWhenTheEntryCannotAnswer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		start   int64
		wait    time.Duration
		noCache bool
	}{
		{name: "the request starts before the cached steps", start: -90},
		{name: "the request starts after the cached steps", start: 30, wait: 2 * time.Hour},
		{name: "the entry is older than the TTL", start: -30, wait: time.Hour},
		{name: "the request sets nocache", start: -30, noCache: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := testRangeNow
			base := now.UnixMilli()
			source := &rangeSource{series: []rangeSourceSeries{{name: "a", first: 0, last: base + minutes(600), value: stepValue}}}
			cache := newTestRangeCache(&now)
			key := rangeCacheKeyParts{query: "q"}.key(0)
			if _, _, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, source.eval); err != nil {
				t.Fatal(err)
			}
			source.calls = nil

			now = now.Add(tc.wait)
			start, end := base+minutes(tc.start), now.UnixMilli()
			result, status, err := cache.query(context.Background(), key, start, end, testRangeStep, tc.noCache, verified, source.eval)
			if err != nil || status != rangeCacheMiss {
				t.Fatalf("status %q, error %v", status, err)
			}
			if want := [][2]int64{{start, end}}; !reflect.DeepEqual(source.calls, want) {
				t.Fatalf("evaluated %v, want %v", source.calls, want)
			}
			if want := source.groups(start, end); !reflect.DeepEqual(result.groups, want) {
				t.Fatalf("result = %v, want %v", result.groups, want)
			}
		})
	}
}

// The cache joins the steps of a series by its labels, so it cannot keep these results.
func TestRangeCacheDoesNotStoreResultsItCannotJoin(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	points := []samplePoint{{t: base - minutes(30), v: 1}}
	for _, tc := range []struct {
		name   string
		result rangeQueryResult
		err    *rangeQueryError
	}{
		{name: "a result with histogram points", result: rangeQueryResult{backend: "test"}},
		{name: "two series with the same labels", result: rangeQueryResult{backend: "test", floats: true, groups: []rangeAggregateGroup{
			{metric: map[string]string{"name": "a"}, points: points},
			{metric: map[string]string{"name": "a"}, points: points},
		}}},
		{name: "a failed evaluation", result: rangeQueryResult{backend: "test"}, err: &rangeQueryError{status: 422, errorType: "execution", err: errors.New("failed")}},
		{name: "a result without series", result: rangeQueryResult{backend: "test", floats: true}},
		// A delta histogram is read as a total that starts at the first sample of the request.
		{name: "a result that depends on the request start", result: rangeQueryResult{backend: "test", floats: true, rangeDependent: true, groups: []rangeAggregateGroup{
			{metric: map[string]string{"name": "a"}, points: points},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newTestRangeCache(&now)
			key := rangeCacheKeyParts{query: "q"}.key(0)
			var starts []int64
			eval := func(_ context.Context, start, _ time.Time) (rangeQueryResult, *rangeQueryError) {
				starts = append(starts, start.UnixMilli())
				return tc.result, tc.err
			}
			for range 2 {
				result, _, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, eval)
				if err != tc.err || len(result.groups) != len(tc.result.groups) {
					t.Fatalf("result = %v, error %v; want the evaluation result", result.groups, err)
				}
			}
			// A stored result would let the second request evaluate only its last steps.
			if want := []int64{base - minutes(60), base - minutes(60)}; !reflect.DeepEqual(starts, want) {
				t.Fatalf("evaluations started at %v, want every step evaluated for each request: %v", starts, want)
			}
		})
	}
}

// Responses share their points with the cache, and dashboards send the panels
// of one query at the same time. The race detector must find no write to shared
// points, and no request can get the steps of another request.
func TestRangeCacheAnswersConcurrentRequestsForOneQuery(t *testing.T) {
	base := testRangeNow.UnixMilli()
	source := &rangeSource{series: []rangeSourceSeries{
		{name: "a", first: 0, last: base + minutes(600), value: stepValue},
		{name: "b", first: base + minutes(7), last: base + minutes(600), value: stepValue},
	}}
	var elapsed atomic.Int64
	cache := newRangeResultCache(Config{RangeCacheMaxBytes: 1 << 20, RangeCacheOffset: testRangeOffset, RangeCacheTTL: 20 * time.Minute}, nil)
	cache.now = func() time.Time { return testRangeNow.Add(time.Duration(elapsed.Load()) * time.Minute) }
	key := rangeCacheKeyParts{query: "q"}.key(0)

	var group sync.WaitGroup
	failures := make(chan string, 8)
	for worker := range 8 {
		group.Go(func() {
			for request := range 100 {
				// The workers move the time forward, so entries are read, extended and expired together.
				moved := elapsed.Add(int64(worker % 2))
				start, end := base+minutes(moved-30-int64(request%3)), base+minutes(moved)
				result, _, err := cache.query(context.Background(), key, start, end, testRangeStep, false, verified, source.eval)
				if err != nil || !reflect.DeepEqual(result.groups, source.groups(start, end)) {
					failures <- "a concurrent request got a result that differs from a complete evaluation"
					return
				}
			}
		})
	}
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
}

// A request with nocache replaces steps that a late backfill made wrong. A
// slower request that started before it still holds the old steps, and must
// not write them back.
func TestRangeCacheKeepsARefreshWhenAnOlderRequestFinishesLater(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	backfilled := false
	source := &rangeSource{series: []rangeSourceSeries{{name: "a", first: 0, last: base + minutes(60), value: func(t int64) float64 {
		if backfilled && t == base-minutes(30) {
			return 42
		}
		return stepValue(t)
	}}}}
	cache := newTestRangeCache(&now)
	key := rangeCacheKeyParts{query: "q"}.key(0)
	if _, _, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, source.eval); err != nil {
		t.Fatal(err)
	}

	now = now.Add(10 * time.Minute)
	backfilled = true
	refreshed := false
	slow := func(ctx context.Context, start, end time.Time) (rangeQueryResult, *rangeQueryError) {
		// The refresh runs while this request evaluates its new steps.
		if !refreshed {
			refreshed = true
			if _, _, err := cache.query(ctx, key, base-minutes(50), base+minutes(10), testRangeStep, true, verified, source.eval); err != nil {
				t.Fatal(err)
			}
		}
		return source.eval(ctx, start, end)
	}
	if _, status, err := cache.query(context.Background(), key, base-minutes(50), base+minutes(10), testRangeStep, false, verified, slow); err != nil || status != rangeCachePartial {
		t.Fatalf("slow request: status %q, error %v", status, err)
	}

	result, status, err := cache.query(context.Background(), key, base-minutes(40), base-minutes(20), testRangeStep, false, verified, source.eval)
	if err != nil || status != rangeCacheHit {
		t.Fatalf("request after the refresh: status %q, error %v", status, err)
	}
	if want := source.groups(base-minutes(40), base-minutes(20)); !reflect.DeepEqual(result.groups, want) {
		t.Fatalf("result = %v, want the refreshed steps %v", result.groups, want)
	}
}

// The engine reuses the memory of its result after the query closes. A result
// that still pointed into it would show the points of a later query.
func TestRangeQueryResultCopiesHistogramPointsOfTheEngineValue(t *testing.T) {
	matrix := promql.Matrix{{
		Metric:     labels.FromStrings("name", "a"),
		Histograms: []promql.HPoint{{T: 60_000, H: &histogram.FloatHistogram{Count: 3, Sum: 1.5}}},
	}}
	result := rangeQueryResultFromValue("prometheus", matrix)

	matrix[0].Histograms[0].H.Count = 777
	matrix[0].Histograms[0].T = 120_000
	encoded, err := json.Marshal(result.data())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(encoded); !strings.Contains(got, `"count":"3"`) || strings.Contains(got, "777") || !strings.Contains(got, "[60,") {
		t.Fatalf("result changed with the engine value: %s", got)
	}
}

// The size limit counts the series that an entry keeps. A series with points
// only in the offset is left out, and must leave no unused memory in the entry.
func TestRangeCacheEntryHoldsNoMemoryForSeriesItLeftOut(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	source := &rangeSource{series: []rangeSourceSeries{{name: "settled", first: 0, last: base, value: stepValue}}}
	for index := range 50 {
		source.series = append(source.series, rangeSourceSeries{name: fmt.Sprintf("new-%02d", index), first: base - minutes(2), last: base, value: stepValue})
	}
	cache := newTestRangeCache(&now)
	key := rangeCacheKeyParts{query: "q"}.key(0)
	if _, _, err := cache.query(context.Background(), key, base-minutes(60), base, testRangeStep, false, verified, source.eval); err != nil {
		t.Fatal(err)
	}
	entry := cache.get(key)
	if entry == nil || len(entry.series) != 1 || cap(entry.series) != 1 {
		t.Fatalf("entry = %+v, want one series in a slice with space for one series", entry)
	}
}

// The cache shares a host with ClickHouse, so it must stay in its size limit.
func TestRangeCacheRemovesTheLeastRecentlyUsedEntryAtTheSizeLimit(t *testing.T) {
	now := testRangeNow
	base := now.UnixMilli()
	source := &rangeSource{series: []rangeSourceSeries{{name: "a", first: 0, last: base, value: stepValue}}}
	cache := newTestRangeCache(&now)
	query := func(name string) string {
		_, status, err := cache.query(context.Background(), rangeCacheKeyParts{query: name}.key(0), base-minutes(60), base-minutes(10), testRangeStep, false, verified, source.eval)
		if err != nil {
			t.Fatal(err)
		}
		return status
	}
	query("first")
	// One entry can use a quarter of the limit, so the limit holds four entries of this size.
	cache.maxBytes = cache.bytes * rangeCacheMaxEntryFraction
	query("second")
	query("third")
	query("fourth")
	query("first")
	query("fifth")

	if got := []string{query("first"), query("fifth"), query("second")}; !reflect.DeepEqual(got, []string{rangeCacheHit, rangeCacheHit, rangeCacheMiss}) {
		t.Fatalf("statuses for first, fifth, second = %v; want the second entry removed", got)
	}
	if cache.bytes > cache.maxBytes {
		t.Fatalf("cache holds %d bytes, limit %d", cache.bytes, cache.maxBytes)
	}
}

// Each key part separates results that must not answer each other.
func TestRangeCacheKeySeparatesRequests(t *testing.T) {
	base := rangeCacheKeyParts{username: "user", query: "sum(m)", step: 60_000, lookback: 300_000, phase: 0}
	if base.key(1) == base.key(2) {
		t.Error("two tenants have the same key")
	}
	for name, change := range map[string]func(*rangeCacheKeyParts){
		"user":     func(p *rangeCacheKeyParts) { p.username = "other" },
		"query":    func(p *rangeCacheKeyParts) { p.query = "sum(n)" },
		"step":     func(p *rangeCacheKeyParts) { p.step = 30_000 },
		"lookback": func(p *rangeCacheKeyParts) { p.lookback = 60_000 },
		"phase":    func(p *rangeCacheKeyParts) { p.phase = 15_000 },
		// A text boundary that moves between two parts must change the key.
		"user and query boundary": func(p *rangeCacheKeyParts) { p.username, p.query = "users", "um(m)" },
	} {
		changed := base
		change(&changed)
		if changed.key(1) == base.key(1) {
			t.Errorf("a different %s has the same key", name)
		}
	}
}

// A step of these queries has a value that depends on the request range or on
// samples after the step, so a cached step would be wrong in another request.
func TestRangeCacheableRejectsQueriesThatDependOnTheRange(t *testing.T) {
	server := newServer(Config{LookbackDelta: 5 * time.Minute, MaxSamples: 1000, QueryTimeout: time.Second})
	step := time.Minute
	start := time.UnixMilli(1_700_000_040_000)
	end := start.Add(time.Hour)
	for _, tc := range []struct {
		query string
		end   time.Time
		want  bool
	}{
		{query: `sum(rate(m[5m])) by (job)`, end: end, want: true},
		{query: `sum(increase(m[5m] offset 1h)) / count(m)`, end: end, want: true},
		{query: `max_over_time(rate(m[1m])[10m:1m])`, end: end, want: true},
		{query: `m @ end()`, end: end},
		{query: `m @ start()`, end: end},
		{query: `m @ 1700000000`, end: end},
		{query: `sum(rate(m[5m] offset -10m))`, end: end},
		{query: `running_sum(sum(m))`, end: end},
		{query: `running_sum(sum(m)) + 1`, end: end},
		{query: `sum(m)`, end: start.Add(step)},
	} {
		prepared, err := prepareMetricsQLQuery(tc.query, step, server.cfg.LookbackDelta, start, tc.end)
		if err != nil {
			t.Fatalf("prepare %q: %v", tc.query, err)
		}
		if got := server.rangeCacheable(tc.query, prepared, start, tc.end, step); got != tc.want {
			t.Errorf("rangeCacheable(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}
