package snuffle

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// ENG-049: a many side of two series and a one side of three series that
// share the matching label m. eng049_one differs in the copied label l;
// eng049_collide has two series with the same l. Every series has a sample
// every 15 seconds for 10 minutes, none stale or replaced.
const eng049StartMS = int64(1_700_049_000_000)

func eng049Series() []prompb.TimeSeries {
	var series []prompb.TimeSeries
	for _, s := range []struct {
		labels []string
		value  float64
	}{
		{[]string{"eng049_many", "m", "a", "inst", "1"}, 2},
		{[]string{"eng049_many", "m", "a", "inst", "2"}, 3},
		{[]string{"eng049_one", "m", "a", "l", "x"}, 10},
		{[]string{"eng049_one", "m", "a", "l", "y"}, 20},
		{[]string{"eng049_one", "m", "a", "l", "z"}, 30},
		{[]string{"eng049_collide", "m", "a", "l", "x", "k", "1"}, 10},
		{[]string{"eng049_collide", "m", "a", "l", "x", "k", "2"}, 20},
		{[]string{"eng049_collide", "m", "a", "l", "y", "k", "3"}, 30},
	} {
		ts := prompb.TimeSeries{Labels: []prompb.Label{{Name: labels.MetricName, Value: s.labels[0]}}}
		for i := 1; i+1 < len(s.labels); i += 2 {
			ts.Labels = append(ts.Labels, prompb.Label{Name: s.labels[i], Value: s.labels[i+1]})
		}
		for n := range int64(41) {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: eng049StartMS + n*15_000, Value: s.value})
		}
		series = append(series, ts)
	}
	return series
}

// assertGroupJoinCopiedLabels checks ENG-049: in a group_left(l) or
// group_right(l) join, one-side series that differ in l each give a result,
// as in VictoriaMetrics. Expected results come from VictoriaMetrics on the
// same samples, instant at 1700049300 and range 1700049060..1700049600 step 60.
// VictoriaMetrics fails where the one-side series also collide in l, and for
// group_left without labels.
func assertGroupJoinCopiedLabels(t *testing.T, cfg Config, baseURL string) {
	t.Helper()
	cfg.RemoteWriteInterval = 0
	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	writer := httptest.NewServer(mux)
	defer writer.Close()
	postRemoteWrite(t, writer.URL, &prompb.WriteRequest{Timeseries: eng049Series()})

	product := map[string]string{
		`{inst="1", l="x", m="a"}`: "20",
		`{inst="1", l="y", m="a"}`: "40",
		`{inst="1", l="z", m="a"}`: "60",
		`{inst="2", l="x", m="a"}`: "30",
		`{inst="2", l="y", m="a"}`: "60",
		`{inst="2", l="z", m="a"}`: "90",
	}
	for _, c := range []struct {
		query string
		want  map[string]string // nil: VictoriaMetrics fails
	}{
		{`eng049_many * on(m) group_left(l) eng049_one`, product},
		{`eng049_one * on(m) group_right(l) eng049_many`, product},
		{`eng049_many * ignoring(inst, l) group_left(l) eng049_one`, product},
		{`vector(1) * on() group_left(l) max by (l) (eng049_one)`, map[string]string{`{l="x"}`: "10", `{l="y"}`: "20", `{l="z"}`: "30"}},
		{`eng049_many < on(m) group_left(l) eng049_one`, map[string]string{
			`{__name__="eng049_many", inst="1", l="x", m="a"}`: "2",
			`{__name__="eng049_many", inst="1", l="y", m="a"}`: "2",
			`{__name__="eng049_many", inst="1", l="z", m="a"}`: "2",
			`{__name__="eng049_many", inst="2", l="x", m="a"}`: "3",
			`{__name__="eng049_many", inst="2", l="y", m="a"}`: "3",
			`{__name__="eng049_many", inst="2", l="z", m="a"}`: "3",
		}},
		{`eng049_many * on(m) group_left(l, k) eng049_collide`, map[string]string{
			`{inst="1", k="1", l="x", m="a"}`: "20",
			`{inst="1", k="2", l="x", m="a"}`: "40",
			`{inst="1", k="3", l="y", m="a"}`: "60",
			`{inst="2", k="1", l="x", m="a"}`: "30",
			`{inst="2", k="2", l="x", m="a"}`: "60",
			`{inst="2", k="3", l="y", m="a"}`: "90",
		}},
		{`eng049_many * on(m) group_left(l) eng049_collide`, nil},
		{`eng049_collide * on(m) group_right(l) eng049_many`, nil},
		{`eng049_many * on(m) group_left eng049_one`, nil},
	} {
		t.Run(c.query, func(t *testing.T) {
			instant, instantErr := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {c.query}, "time": {"1700049300"}})
			ranged, rangeErr := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {c.query}, "start": {"1700049060"}, "end": {"1700049600"}, "step": {"60"}})
			if c.want == nil {
				for _, errText := range []string{instantErr, rangeErr} {
					if !strings.Contains(errText, "found duplicate series for the match group") {
						t.Errorf("error = %q, want the many-to-many error", errText)
					}
				}
				return
			}
			if instantErr != "" || rangeErr != "" {
				t.Fatalf("instant: %q, range: %q", instantErr, rangeErr)
			}
			got := map[string]string{}
			for _, r := range instant.Result {
				got[labels.FromMap(r.Metric).String()] = r.Value[1].(string)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("instant = %v, want %v", got, c.want)
			}
			if len(ranged.Result) != len(c.want) {
				t.Errorf("range: %d series, want %d: %v", len(ranged.Result), len(c.want), ranged.Result)
			}
			for _, r := range ranged.Result {
				metric := labels.FromMap(r.Metric).String()
				if len(r.Values) != 10 {
					t.Errorf("range %s: %d points, want 10", metric, len(r.Values))
				}
				for _, point := range r.Values {
					if point[1] != c.want[metric] {
						t.Errorf("range %s at %v = %v, want %q", metric, point[0], point[1], c.want[metric])
					}
				}
			}
		})
	}
}
