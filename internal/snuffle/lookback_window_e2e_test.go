package snuffle

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	lookbackE2EGauge   = "snuffle_e2e_lookback_gauge"
	lookbackE2ECounter = "snuffle_e2e_lookback_total"
	lookbackE2EStop    = "snuffle_e2e_lookback_stop"
	// lookbackE2EBase is on the minute, so 60 s steps from it are step aligned.
	lookbackE2EBase = int64(1_700_299_980_000)
)

// assertRangeStepsIndependentOfStart checks that the value of a step depends
// only on the samples and the step time, never on where the request range
// starts: samples before the start count for the first steps (ENG-025), and a
// series that stops has the same last point in every window (ENG-026). It runs
// with the range pushdown and with the Prometheus engine. The expected values
// come from VictoriaMetrics over the same samples, in its 1-hour window.
func assertRangeStepsIndependentOfStart(t *testing.T, cfg Config) {
	t.Helper()
	// Samples keep their irregular times, as from an external writer.
	cfg.RemoteWriteInterval = 0
	engineCfg := cfg
	engineCfg.RangePushdown = false
	for index, pathCfg := range []Config{cfg, engineCfg} {
		name := map[bool]string{true: "pushdown", false: "engine"}[pathCfg.RangePushdown]
		// The PostHog range pushdown estimates the sample interval at each
		// sample, as VictoriaMetrics does for each series. The engine and the
		// native pushdown estimate it at each step, so for the irregular
		// series that stop they keep fewer points than VictoriaMetrics, in
		// every window alike.
		postHogPushdown := pathCfg.RangePushdown && pathCfg.postHogSchemaLayout()
		mux := http.NewServeMux()
		newServer(pathCfg).routes(mux)
		api := httptest.NewServer(mux)
		defer api.Close()
		if index == 0 {
			postRemoteWrite(t, api.URL, lookbackE2EWriteRequest())
		}
		minute := func(m int) int64 { return lookbackE2EBase + int64(m)*time.Minute.Milliseconds() }
		get := func(t *testing.T, query string, start, end int64, step time.Duration) queryDataDTO {
			t.Helper()
			return apiGet[queryDataDTO](t, api.URL, "/api/v1/query_range", url.Values{
				"query": {query},
				"start": {strconv.FormatFloat(float64(start)/1000, 'f', -1, 64)},
				"end":   {strconv.FormatFloat(float64(end)/1000, 'f', -1, 64)},
				"step":  {step.String()},
			})
		}

		// ENG-025: a 2-minute window that starts at t returns the steps of a
		// 1-hour window that holds them.
		for _, tc := range []struct {
			query string
			// first holds the values of the first three steps.
			first []float64
		}{
			{query: `G`},
			{query: `sum(G)`, first: []float64{99, 99, 99}},
			{query: `rate(C[5m])`},
			{query: `sum(rate(C[5m]))`, first: []float64{0.22932291666666665, 0.2299702380952381, 0.22138364779874214}},
			{query: `avg_over_time(G[5m])`},
			{query: `sum(avg_over_time(G[5m]))`, first: []float64{99, 99, 99}},
		} {
			query := strings.NewReplacer("G", lookbackE2EGauge, "C", lookbackE2ECounter).Replace(tc.query)
			first := tc.first
			t.Run(name+" short window "+query, func(t *testing.T) {
				const step = 15 * time.Second
				// Steps start 3 s after the minute, off the phase of every sample.
				at := minute(60) + 3000
				long := get(t, query, at-30*time.Minute.Milliseconds(), at+30*time.Minute.Milliseconds(), step)
				short := get(t, query, at, at+2*time.Minute.Milliseconds(), step)
				if len(short.Result) == 0 {
					t.Fatalf("the 2-minute window returned no series; the 1-hour window returned %d", len(long.Result))
				}
				assertSameOverlappingSteps(t, "1-hour window", long, "2-minute window", short)
				if first != nil {
					if len(short.Result) != 1 || len(short.Result[0].Values) < len(first) {
						t.Fatalf("want one series with at least %d points, got %v", len(first), short.Result)
					}
					for i, want := range first {
						got, _ := strconv.ParseFloat(sampleString(short.Result[0].Values[i]), 64)
						if math.Abs(got-want) > 1e-9*math.Abs(want) {
							t.Errorf("step %d = %v, VictoriaMetrics returns %v", i, got, want)
						}
					}
				}
			})
		}

		// ENG-026: windows that start 0 to 3 steps apart and share their end
		// return the same points for the steps they share.
		query := `sum by (pod) (` + lookbackE2EStop + `)`
		t.Run(name+" window starts "+query, func(t *testing.T) {
			const step = time.Minute
			results := make([]queryDataDTO, 4)
			for offset := range results {
				results[offset] = get(t, query, minute(20+offset), minute(80), step)
				if len(results[offset].Result) == 0 {
					t.Fatalf("start offset %d minutes returned no series", offset)
				}
			}
			if postHogPushdown {
				// VictoriaMetrics returns as many points per series, and the same
				// last point, so a stopped series ends where it does.
				want := map[string][3]float64{"s1": {28, 1700302800, 101}, "s2": {34, 1700303160, 106}, "s3": {39, 1700303460, 102}, "s4": {43, 1700303760, 107}, "s5": {49, 1700304780, 102}, "s6": {32, 1700303040, 104}}
				for _, series := range results[0].Result {
					last := series.Values[len(series.Values)-1]
					value, _ := strconv.ParseFloat(sampleString(last), 64)
					if got := [3]float64{float64(len(series.Values)), last[0].(float64), value}; got != want[series.Metric["pod"]] {
						t.Errorf("pod %s: points, last time and last value %v, VictoriaMetrics returns %v", series.Metric["pod"], got, want[series.Metric["pod"]])
					}
				}
			}
			for a := range results {
				for b := a + 1; b < len(results); b++ {
					assertSameOverlappingSteps(t, fmt.Sprintf("start +%dm", a), results[a], fmt.Sprintf("start +%dm", b), results[b])
				}
			}
		})
	}
}

// assertSameOverlappingSteps checks that every step present in either result
// and inside both request ranges has the same series and value in both. It
// treats the range of each result as the steps from its first to its last
// point over all series, which covers every step in these tests.
func assertSameOverlappingSteps(t *testing.T, nameA string, a queryDataDTO, nameB string, b queryDataDTO) {
	t.Helper()
	type step struct {
		series string
		time   string
	}
	collect := func(result queryDataDTO) (map[step]string, float64, float64) {
		points := map[step]string{}
		first, last := 0.0, 0.0
		for _, series := range result.Result {
			key := labels.FromMap(series.Metric).String()
			for _, point := range series.Values {
				ts, _ := point[0].(float64)
				if first == 0 || ts < first {
					first = ts
				}
				last = max(last, ts)
				points[step{key, strconv.FormatFloat(ts, 'f', -1, 64)}] = sampleString(point)
			}
		}
		return points, first, last
	}
	pointsA, firstA, lastA := collect(a)
	pointsB, firstB, lastB := collect(b)
	from, to := max(firstA, firstB), min(lastA, lastB)
	inBoth := func(key step) bool {
		ts, _ := strconv.ParseFloat(key.time, 64)
		return ts >= from && ts <= to
	}
	for key, valueA := range pointsA {
		if !inBoth(key) {
			continue
		}
		valueB, ok := pointsB[key]
		if !ok {
			t.Errorf("%s has %s at %s = %s, %s has no point", nameA, key.series, key.time, valueA, nameB)
			continue
		}
		parsedA, _ := strconv.ParseFloat(valueA, 64)
		parsedB, _ := strconv.ParseFloat(valueB, 64)
		if parsedA != parsedB {
			t.Errorf("%s at %s: %s = %s, %s = %s", key.series, key.time, nameA, valueA, nameB, valueB)
		}
	}
	for key, valueB := range pointsB {
		if _, ok := pointsA[key]; !ok && inBoth(key) {
			t.Errorf("%s has %s at %s = %s, %s has no point", nameB, key.series, key.time, valueB, nameA)
		}
	}
}

// lookbackE2EWriteRequest holds two hours of samples: a gauge sampled every 5
// minutes, a regular counter sampled every 15 s, an irregular counter, and
// irregular series that stop at irregular times. The intervals of the
// irregular series repeat 55, 70, 55, 55 and 70 seconds, so an interval
// estimate over the whole request range changes with where the range starts.
func lookbackE2EWriteRequest() *prompb.WriteRequest {
	series := func(metric, pod string, phase, to time.Duration, intervals []time.Duration, value func(i int) float64) prompb.TimeSeries {
		out := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: labels.MetricName, Value: metric},
			{Name: "job", Value: "api"},
			{Name: "pod", Value: pod},
		}}
		for i, offset := 0, phase; offset <= to; i, offset = i+1, offset+intervals[i%len(intervals)] {
			out.Samples = append(out.Samples, prompb.Sample{Timestamp: lookbackE2EBase + offset.Milliseconds(), Value: value(i)})
		}
		return out
	}
	const end = 120 * time.Minute
	seconds := func(values ...int) []time.Duration {
		out := make([]time.Duration, len(values))
		for i, value := range values {
			out[i] = time.Duration(value) * time.Second
		}
		return out
	}
	fiveMinutes := []time.Duration{5 * time.Minute}
	irregular := seconds(55, 70, 55, 55, 70)
	counter := func(i int) float64 {
		// The increase per sample changes, so rate changes with its window.
		total := 0.0
		for j := range i {
			total += 1 + float64(j%7)*0.375
		}
		return 1000 + total
	}
	stop := func(i int) float64 { return float64(100 + i%9) }
	return &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		series(lookbackE2EGauge, "g1", 37*time.Second, end, fiveMinutes, func(i int) float64 { return float64(10 + i*7%11) }),
		series(lookbackE2EGauge, "g2", 2*time.Minute+11*time.Second, end, fiveMinutes, func(i int) float64 { return float64(100 - i) }),
		series(lookbackE2ECounter, "c1", 4*time.Second, end, seconds(15), counter),
		series(lookbackE2ECounter, "c2", 9*time.Second, end, seconds(15, 40, 15, 15, 40), counter),
		series(lookbackE2EStop, "s1", 7*time.Second, 47*time.Minute+13*time.Second, irregular, stop),
		series(lookbackE2EStop, "s2", 13*time.Second, 52*time.Minute+41*time.Second, irregular, stop),
		series(lookbackE2EStop, "s3", 23*time.Second, 58*time.Minute+7*time.Second, irregular, stop),
		series(lookbackE2EStop, "s4", 41*time.Second, 63*time.Minute+29*time.Second, irregular, stop),
		series(lookbackE2EStop, "s5", 31*time.Minute+19*time.Second, end, irregular, stop),
		// This series has one sample per step, 5 s before each step. Its
		// interval keeps its last point 65 s after its last sample.
		series(lookbackE2EStop, "s6", 55*time.Second, 49*time.Minute+55*time.Second, seconds(60), stop),
	}}
}
