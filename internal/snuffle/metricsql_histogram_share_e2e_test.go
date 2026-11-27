package snuffle

import (
	"math"
	"net/url"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

// assertHistogramShare checks MetricsQL's histogram_share(le, buckets)
// against VictoriaMetrics v1 (Docker, victoriametrics/victoria-metrics:latest)
// run on the same samples.
func assertHistogramShare(t *testing.T, baseURL string) {
	t.Helper()
	const metric = "snuffle_e2e:share_bucket:avg1d"
	bases := map[string][]float64{"prod": {10, 30, 35, 40}, "dev": {2, 3, 9, 10}}
	bounds := []string{"1", "2", "4", "+Inf"}
	var series []prompb.TimeSeries
	for environment, base := range bases {
		for _, instance := range []string{"a", "b"} {
			for index, bound := range bounds {
				var samples []prompb.Sample
				for step := range 3 {
					value := base[index]
					if instance == "a" {
						value += float64(step * index)
					}
					samples = append(samples, prompb.Sample{Timestamp: e2eStartMS + int64(step)*30_000, Value: value})
				}
				series = append(series, prompb.TimeSeries{Labels: []prompb.Label{
					{Name: "__name__", Value: metric},
					{Name: "environment", Value: environment},
					{Name: "instance", Value: instance},
					{Name: "le", Value: bound},
				}, Samples: samples})
			}
		}
	}
	postRemoteWrite(t, baseURL, &prompb.WriteRequest{Timeseries: series})

	buckets := `sum by (environment, le) (` + metric + `)`
	// Values per environment at 1700000010, 1700000040 and 1700000070.
	for _, c := range []struct {
		query     string
		dev, prod []float64
	}{
		{"histogram_share(2, " + buckets + ")", // on a bound
			[]float64{0.3, 0.30434782608695654, 0.3076923076923077}, []float64{0.75, 0.7349397590361446, 0.7209302325581395}},
		{"histogram_share(3, " + buckets + ")", // between bounds
			[]float64{0.6, 0.5869565217391304, 0.5769230769230769}, []float64{0.8125, 0.8012048192771085, 0.7906976744186046}},
		{"histogram_share(0.5, " + buckets + ")", // below the first bound
			[]float64{0.1, 0.08695652173913043, 0.07692307692307693}, []float64{0.125, 0.12048192771084337, 0.11627906976744186}},
		{"histogram_share(8, " + buckets + ")", // above the last finite bound
			[]float64{0.9, 0.8695652173913043, 0.8461538461538461}, []float64{0.875, 0.8674698795180723, 0.8604651162790697}},
		{"1 - histogram_share(8, " + buckets + ")",
			[]float64{0.09999999999999998, 0.13043478260869568, 0.15384615384615385}, []float64{0.125, 0.1325301204819277, 0.13953488372093026}},
	} {
		t.Run(c.query, func(t *testing.T) {
			want := map[string][]float64{"dev": c.dev, "prod": c.prod}
			check := func(environment string, step int, raw any) {
				t.Helper()
				got, _ := strconv.ParseFloat(raw.(string), 64)
				if expected := want[environment][step]; math.Abs(got-expected) > 1e-12 {
					t.Errorf("%s step %d = %v, want %v", environment, step, got, expected)
				}
			}
			instant, errText := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {c.query}, "time": {"1700000070"}})
			if errText != "" {
				t.Fatalf("instant: %s", errText)
			}
			if len(instant.Result) != 2 {
				t.Fatalf("instant: %d series, want 2: %#v", len(instant.Result), instant.Result)
			}
			for _, result := range instant.Result {
				if len(result.Metric) != 1 {
					t.Errorf("instant labels = %v, want only environment", result.Metric)
				}
				check(result.Metric["environment"], 2, result.Value[1])
			}
			ranged, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {c.query}, "start": {"1700000010"}, "end": {"1700000070"}, "step": {"30"}})
			if errText != "" {
				t.Fatalf("range: %s", errText)
			}
			if len(ranged.Result) != 2 {
				t.Fatalf("range: %d series, want 2: %#v", len(ranged.Result), ranged.Result)
			}
			for _, result := range ranged.Result {
				if len(result.Values) != 3 {
					t.Fatalf("range %v: %d points, want 3", result.Metric, len(result.Values))
				}
				for step, point := range result.Values {
					check(result.Metric["environment"], step, point[1])
				}
			}
		})
	}
}
