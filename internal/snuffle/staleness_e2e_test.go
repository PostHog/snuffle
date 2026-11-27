package snuffle

import (
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

// ENG-050: A{pod="p"} is written every 30 s until eng050ReplacedMS, where a
// stale marker ends it, as Prometheus writes one when a series' labels change.
// From then on A{pod="p",node="n"} replaces it. B{pod="p"} is written every
// 30 s throughout.
const (
	eng050A          = "snuffle_e2e_eng050_a"
	eng050B          = "snuffle_e2e_eng050_b"
	eng050StartMS    = int64(1_700_400_000_000)
	eng050ReplacedMS = eng050StartMS + 20*60_000
	eng050EndMS      = eng050StartMS + 40*60_000
)

func eng050WriteRequest() *prompb.WriteRequest {
	series := func(name string, from, to int64, value float64, extra ...prompb.Label) prompb.TimeSeries {
		ts := prompb.TimeSeries{Labels: append([]prompb.Label{{Name: "__name__", Value: name}, {Name: "pod", Value: "p"}}, extra...)}
		for at := from; at < to; at += 30_000 {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: at, Value: value})
		}
		return ts
	}
	old := series(eng050A, eng050StartMS, eng050ReplacedMS, 14)
	old.Samples = append(old.Samples, prompb.Sample{Timestamp: eng050ReplacedMS, Value: math.Float64frombits(promvalue.StaleNaN)})
	return &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		old,
		series(eng050A, eng050ReplacedMS, eng050EndMS, 14, prompb.Label{Name: "node", Value: "n"}),
		series(eng050B, eng050StartMS, eng050EndMS, 2),
	}}
}

// assertReplacedSeriesStaleness checks ENG-050 and #42: a series ended by a
// stale marker leaves instant vectors at the marker, so its replacement is
// the only series and joins on pod find one match. VictoriaMetrics returns
// one series at every step of these queries, at a 60 s step and in instant
// queries, with the values below.
//
// The PostHog layout has the marker here because Snuffle's remote write
// stores it. PostHog capture drops it unless PROMETHEUS_KEEP_STALE_MARKERS is
// set; without it, Snuffle and VictoriaMetrics both keep the old series in
// instant vectors for the lookback after its last sample.
//
// Only the 60 s step is checked: at a 15 s step, the replacement's first
// sample is alone in its history, so Snuffle keeps it for one step and
// misses t+15 s, where VictoriaMetrics, which estimates the interval from the
// later samples too, has it. That is a series start, not staleness.
func assertReplacedSeriesStaleness(t *testing.T, cfg Config) {
	t.Helper()
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	postRemoteWrite(t, api.URL, eng050WriteRequest())

	queries := []struct {
		query string
		value string
		// node reports whether the result keeps A's node label after the
		// replacement.
		node bool
	}{
		{query: `count(` + eng050A + `)`, value: "1"},
		{query: `sum(` + eng050A + `)`, value: "14"},
		{query: eng050A + ` / on(pod) ` + eng050B, value: "7"},
		{query: eng050B + ` * on(pod) group_left(node) ` + eng050A, value: "28", node: true},
	}
	check := func(t *testing.T, at int64, result []sampleResultDTO, points func(sampleResultDTO) map[int64]string, value string, node bool) {
		t.Helper()
		var got []string
		for _, series := range result {
			if v, ok := points(series)[at]; ok {
				got = append(got, v+" node="+series.Metric["node"])
			}
		}
		wantNode := ""
		if node && at*1000 >= eng050ReplacedMS {
			wantNode = "n"
		}
		if want := value + " node=" + wantNode; len(got) != 1 || got[0] != want {
			t.Errorf("at %d: %q, want [%q]", at, got, want)
		}
	}
	const step = 60
	start, end := (eng050StartMS+10*60_000)/1000, (eng050StartMS+30*60_000)/1000
	for _, q := range queries {
		t.Run("60s "+q.query, func(t *testing.T) {
			data, errText := queryOrError(t, api.URL, "/api/v1/query_range", url.Values{
				"query": {q.query}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)}, "step": {"60"},
			})
			if errText != "" {
				t.Fatal(errText)
			}
			points := func(series sampleResultDTO) map[int64]string {
				out := map[int64]string{}
				for _, point := range series.Values {
					out[int64(point[0].(float64))] = point[1].(string)
				}
				return out
			}
			for at := start; at <= end; at += step {
				check(t, at, data.Result, points, q.value, q.node)
			}
		})
	}
	for _, offset := range []int64{-30, 0, 15, 60, 120, 290} {
		at := eng050ReplacedMS/1000 + offset
		for _, q := range queries {
			t.Run("instant "+strconv.FormatInt(offset, 10)+"s "+q.query, func(t *testing.T) {
				data, errText := queryOrError(t, api.URL, "/api/v1/query", url.Values{"query": {q.query}, "time": {strconv.FormatInt(at, 10)}})
				if errText != "" {
					t.Fatal(errText)
				}
				points := func(series sampleResultDTO) map[int64]string {
					return map[int64]string{at: series.Value[1].(string)}
				}
				check(t, at, data.Result, points, q.value, q.node)
			})
		}
	}
}
