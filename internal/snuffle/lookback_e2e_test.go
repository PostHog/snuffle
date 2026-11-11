package snuffle

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

// A gauge written every 5 minutes, like kube-state-metrics, with each pod's
// samples at a different offset.
const (
	sparseGaugeMetric   = "snuffle_e2e_sparse_gauge"
	sparseGaugeStartMS  = int64(1_700_010_000_000)
	sparseGaugeInterval = int64(300_000)
	sparseGaugeSamples  = 30
)

// A pod sampled every 90 seconds, then every 60 seconds until it stops,
// and a counter sampled every 30 seconds.
const (
	stoppingGaugeMetric = "snuffle_e2e_stopping_gauge"
	steadyCounterMetric = "snuffle_e2e_steady_counter_total"
)

func sparseGaugeSeries() []prompb.TimeSeries {
	stopping := prompb.TimeSeries{Labels: []prompb.Label{
		{Name: "__name__", Value: stoppingGaugeMetric},
		{Name: "pod", Value: "pod-old"},
	}}
	ts := sparseGaugeStartMS
	for n := 0; n < 40; n++ {
		stopping.Samples = append(stopping.Samples, prompb.Sample{Timestamp: ts, Value: float64(n)})
		if n < 20 {
			ts += 90_000
		} else {
			ts += 60_000
		}
	}
	counter := prompb.TimeSeries{Labels: []prompb.Label{
		{Name: "__name__", Value: steadyCounterMetric},
		{Name: "pod", Value: "pod-a"},
	}}
	for n := int64(0); n < 300; n++ {
		counter.Samples = append(counter.Samples, prompb.Sample{Timestamp: sparseGaugeStartMS + n*30_000, Value: float64(n * n)})
	}
	series := []prompb.TimeSeries{stopping, counter}
	for i, pod := range []string{"pod-a", "pod-b", "pod-c"} {
		ts := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: "__name__", Value: sparseGaugeMetric},
			{Name: "pod", Value: pod},
		}}
		for n := int64(0); n < sparseGaugeSamples; n++ {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: sparseGaugeStartMS + int64(i)*100_000 + n*sparseGaugeInterval, Value: 1})
		}
		series = append(series, ts)
	}
	return series
}

// assertSparseSeriesLookback checks that every selector sees a sample for the
// lookback after it, wherever the selector is and wherever the range starts,
// as in Prometheus. The last sample of each pod is 5 minutes old at most
// steps, so a window shorter than the lookback drops it.
func assertSparseSeriesLookback(t *testing.T, cfg Config, baseURL string) {
	t.Helper()
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	writer := httptest.NewServer(mux)
	defer writer.Close()
	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: sparseGaugeSeries()})

	seconds := func(ms int64) string { return strconv.FormatInt(ms/1000, 10) }
	// Every pod has written its first sample by then.
	from := sparseGaugeStartMS + 2*sparseGaugeInterval
	windows := []struct{ name, start, end string }{
		{"one hour", seconds(from), seconds(from + 3_600_000)},
		{"two minutes", seconds(from + 1_200_000), seconds(from + 1_320_000)},
		{"two minutes, offset start", seconds(from + 1_207_000), seconds(from + 1_327_000)},
	}
	queries := []struct {
		query string
		want  float64
	}{
		{`count(` + sparseGaugeMetric + `)`, 3},
		{`sum(` + sparseGaugeMetric + `)`, 3},
		{`count(` + sparseGaugeMetric + ` > 0)`, 3},
		{`sum(` + sparseGaugeMetric + ` * 2)`, 6},
		{`count(` + sparseGaugeMetric + ` and ` + sparseGaugeMetric + `)`, 3},
		{`sum(` + sparseGaugeMetric + `) / count(` + sparseGaugeMetric + `)`, 1},
		{`sum(` + sparseGaugeMetric + ` / ` + sparseGaugeMetric + `)`, 3},
	}
	for _, w := range windows {
		for _, q := range queries {
			t.Run(w.name+" "+q.query, func(t *testing.T) {
				data, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {q.query}, "start": {w.start}, "end": {w.end}, "step": {"15"}})
				if errText != "" {
					t.Fatal(errText)
				}
				start, _ := strconv.ParseInt(w.start, 10, 64)
				end, _ := strconv.ParseInt(w.end, 10, 64)
				want := int((end-start)/15 + 1)
				if len(data.Result) != 1 {
					t.Fatalf("%d series, want 1: %#v", len(data.Result), data.Result)
				}
				values := data.Result[0].Values
				missing := want - len(values)
				for _, point := range values {
					if value, _ := strconv.ParseFloat(point[1].(string), 64); value != q.want {
						t.Errorf("value %v at %v, want %v", value, point[0], q.want)
					}
				}
				if missing != 0 {
					t.Errorf("%d of %d steps, want all of them", len(values), want)
				}
			})
		}
		t.Run(w.name+" plain selector", func(t *testing.T) {
			data, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {sparseGaugeMetric}, "start": {w.start}, "end": {w.end}, "step": {"15"}})
			if errText != "" {
				t.Fatal(errText)
			}
			start, _ := strconv.ParseInt(w.start, 10, 64)
			end, _ := strconv.ParseInt(w.end, 10, 64)
			want := int((end-start)/15 + 1)
			if len(data.Result) != 3 {
				t.Fatalf("%d series, want 3", len(data.Result))
			}
			for _, series := range data.Result {
				if len(series.Values) != want {
					t.Errorf("%s: %d of %d steps", series.Metric["pod"], len(series.Values), want)
				}
			}
		})
	}
	// A point depends on the data around it, not on where the range starts.
	stop := sparseGaugeStartMS + 20*90_000 + 19*60_000
	for _, c := range []struct {
		query          string
		step           int64
		starts         []int64
		end            int64
		wantStopPoints bool
	}{
		// The stopping pod's last sample at the steps after it: from the
		// first start the range holds its 90-second intervals, from the
		// second only its 60-second ones.
		{`sum by (pod) (` + stoppingGaugeMetric + `)`, 10_000, []int64{sparseGaugeStartMS + 600_000, stop - 600_000}, stop + 300_000, true},
		{stoppingGaugeMetric + ` > 0`, 10_000, []int64{sparseGaugeStartMS + 600_000, stop - 600_000}, stop + 300_000, true},
		// Windows over samples before the range start.
		{`sum(rate(` + steadyCounterMetric + `[5m]))`, 15_000, []int64{sparseGaugeStartMS + 3_600_000, sparseGaugeStartMS + 5_400_000}, sparseGaugeStartMS + 5_520_000, false},
		{`sum(increase(` + steadyCounterMetric + `[5m]))`, 15_000, []int64{sparseGaugeStartMS + 3_600_000, sparseGaugeStartMS + 5_400_000}, sparseGaugeStartMS + 5_520_000, false},
		{`avg_over_time(` + steadyCounterMetric + `[5m])`, 15_000, []int64{sparseGaugeStartMS + 3_600_000, sparseGaugeStartMS + 5_400_000}, sparseGaugeStartMS + 5_520_000, false},
	} {
		t.Run("range start "+c.query, func(t *testing.T) {
			points := make([]map[string]string, len(c.starts))
			for i, start := range c.starts {
				data, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {c.query}, "start": {seconds(start)}, "end": {seconds(c.end)}, "step": {strconv.FormatInt(c.step/1000, 10)}})
				if errText != "" {
					t.Fatal(errText)
				}
				points[i] = map[string]string{}
				for _, series := range data.Result {
					for _, point := range series.Values {
						points[i][strconv.FormatFloat(point[0].(float64), 'f', -1, 64)] = point[1].(string)
					}
				}
			}
			last := c.starts[len(c.starts)-1]
			for ts := last; ts <= c.end; ts += c.step {
				key := seconds(ts)
				for i := 1; i < len(points); i++ {
					if points[0][key] != points[i][key] {
						t.Errorf("at %s: %q from %s, %q from %s", key, points[0][key], seconds(c.starts[0]), points[i][key], seconds(c.starts[i]))
					}
				}
			}
			if c.wantStopPoints {
				// The interval is 60 seconds, so the last sample stays for 67.5.
				for _, after := range []int64{60_000, 70_000} {
					_, ok := points[0][seconds(stop+after)]
					if ok != (after < 67_500) {
						t.Errorf("point %ds after the last sample: %v", after/1000, ok)
					}
				}
			}
		})
	}

	// An instant query 4 minutes after pod-a's sample.
	at := seconds(sparseGaugeStartMS + 5*sparseGaugeInterval + 240_000)
	for _, q := range queries {
		data, errText := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {q.query}, "time": {at}})
		if errText != "" {
			t.Fatalf("instant %s: %s", q.query, errText)
		}
		if len(data.Result) != 1 {
			t.Errorf("instant %s: %d series, want 1", q.query, len(data.Result))
			continue
		}
		if value, _ := strconv.ParseFloat(data.Result[0].Value[1].(string), 64); value != q.want {
			t.Errorf("instant %s = %v, want %v", q.query, value, q.want)
		}
	}
}
