package snuffle

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

// A gauge written every 5 minutes, like kube-state-metrics. Pod i writes
// 30*i seconds after pod 0; p9 stops after its fifth sample. Pod dense writes
// every 30 seconds until it stops, and pod once writes a single sample.
const (
	eng029Metric  = "snuffle_e2e_eng029_gauge"
	eng029StartMS = int64(1_700_020_000_000)
	eng029Pods    = 10
)

func eng029Series() []prompb.TimeSeries {
	var series []prompb.TimeSeries
	for i := range eng029Pods {
		ts := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: "__name__", Value: eng029Metric},
			{Name: "pod", Value: "p" + strconv.Itoa(i)},
		}}
		samples := 12
		if i == eng029Pods-1 {
			samples = 5
		}
		for n := range samples {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: eng029StartMS + int64(i)*30_000 + int64(n)*300_000, Value: float64(i + 1)})
		}
		series = append(series, ts)
	}
	dense := prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: eng029Metric}, {Name: "pod", Value: "dense"}}}
	for n := range int64(40) {
		dense.Samples = append(dense.Samples, prompb.Sample{Timestamp: eng029StartMS + n*30_000, Value: 1})
	}
	once := prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: eng029Metric}, {Name: "pod", Value: "once"}}}
	once.Samples = []prompb.Sample{{Timestamp: eng029StartMS + 905_000, Value: 1}}
	return append(series, dense, once)
}

// assertBinaryOpLookback checks ENG-029: both sides of a binary operator see
// the same instant vector as the plain selector, and a stopped series ends
// after the lookback. Expected counts come from VictoriaMetrics on the same
// samples with max_lookback=5m, as changes from the range start:
//   - 11 series at the start: 10 pods and dense.
//   - once is counted at one step: its interval falls back to the step.
//   - dense ends 37.5 seconds (its interval and margin) or one step after its
//     last sample at 1700021170.
//   - p9 ends 5 minutes, the lookback, after its last sample at 1700021470.
//
// once is off the step grid: VictoriaMetrics adds a margin to the step
// fallback, Snuffle does not, and at this timestamp both count it at one step.
func assertBinaryOpLookback(t *testing.T, cfg Config, baseURL string) {
	t.Helper()
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	writer := httptest.NewServer(mux)
	defer writer.Close()
	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: eng029Series()})

	const start, end = int64(1_700_020_600), int64(1_700_022_400)
	for _, c := range []struct {
		step int64
		// Seconds after 1700020000 at which the count changes to the value.
		changes [][2]int64
	}{
		{15, [][2]int64{{600, 11}, {915, 12}, {930, 11}, {1215, 10}, {1770, 9}}},
		{60, [][2]int64{{600, 11}, {960, 12}, {1020, 11}, {1260, 10}, {1800, 9}}},
	} {
		step := c.step
		for _, query := range []string{
			`count(` + eng029Metric + `)`,
			`count(` + eng029Metric + ` > 0)`,
			`count(` + eng029Metric + ` / ` + eng029Metric + `)`,
		} {
			t.Run(strconv.FormatInt(step, 10)+"s "+query, func(t *testing.T) {
				data, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{
					"query": {query}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)}, "step": {strconv.FormatInt(step, 10)},
				})
				if errText != "" {
					t.Fatal(errText)
				}
				if len(data.Result) != 1 {
					t.Fatalf("%d series, want 1: %#v", len(data.Result), data.Result)
				}
				got := map[int64]string{}
				for _, point := range data.Result[0].Values {
					got[int64(point[0].(float64))] = point[1].(string)
				}
				for ts := start; ts <= end; ts += step {
					var want int64
					for _, change := range c.changes {
						if ts >= 1_700_020_000+change[0] {
							want = change[1]
						}
					}
					if got[ts] != strconv.FormatInt(want, 10) {
						t.Errorf("at %d: %q, want %d", ts, got[ts], want)
					}
				}
			})
		}
	}
}
