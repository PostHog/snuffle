package snuffle

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// ENG-048: increase() of a counter that starts inside the range counts its
// first sample from zero when the first sample is small next to the step to
// the series' next sample, as VictoriaMetrics does. The next sample can be
// after the window. Every path must use the same rule, so a ratio of two new
// counters is never +Inf because one side's first step was 0.
func TestIncreaseFirstSampleEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	for _, layout := range []schemaLayout{schemaLayoutCurrent, schemaLayoutPostHog} {
		test.Run(string(layout), func(test *testing.T) {
			test.Setenv("CH_SCHEMA_LAYOUT", string(layout))
			assertIncreaseFirstSample(test)
		})
	}
}

func assertIncreaseFirstSample(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_increase_first_e2e_%d", time.Now().UnixNano())
	if err := root.Exec(ctx, "CREATE DATABASE "+quoteIdent(database)); err != nil {
		test.Fatal(err)
	}
	defer func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := root.Exec(cleanup, "DROP DATABASE "+quoteIdent(database)+" SYNC"); err != nil {
			test.Errorf("clean test database: %v", err)
		}
	}()
	cfg.CHDatabase = database
	createMetricsSchema(test, ctx, NewClickHouseClient(cfg))

	// Both counters start at S on a 15 s scrape. periods (A) starts at 24 and
	// grows 20 a minute; throttled (B) starts at 8 and grows 0-1 a minute.
	// Pod c stops after 90 s and starts again at S+9m with no sample in
	// the matrix before it, a gap longer than the lookback.
	const start = int64(1_700_000_640_000)
	write := &prompb.WriteRequest{}
	add := func(name, pod string, from, count int, value func(int) float64) {
		series := prompb.TimeSeries{Labels: []prompb.Label{{Name: labels.MetricName, Value: name}, {Name: "pod", Value: pod}}}
		for i := range count {
			series.Samples = append(series.Samples, prompb.Sample{Timestamp: start + int64(from+15*i)*1000, Value: value(i)})
		}
		write.Timeseries = append(write.Timeseries, series)
	}
	periods := func(base float64) func(int) float64 { return func(i int) float64 { return base + 5*float64(i) } }
	throttled := func(base float64) func(int) float64 { return func(i int) float64 { return base + float64(i/6) } }
	add("eng048_periods_total", "a", 0, 61, periods(24))
	add("eng048_throttled_total", "a", 0, 61, throttled(8))
	add("eng048_periods_total", "c", 0, 7, periods(24))
	add("eng048_periods_total", "c", 540, 25, periods(40))
	add("eng048_throttled_total", "c", 0, 7, throttled(8))
	add("eng048_throttled_total", "c", 540, 25, throttled(3))
	servers := map[string]string{}
	for name, pushdown := range map[string]bool{"pushdown": true, "engine": false} {
		serverCfg := cfg
		serverCfg.RangePushdown = pushdown
		mux := http.NewServeMux()
		newServer(serverCfg).routes(mux)
		api := httptest.NewServer(mux)
		defer api.Close()
		servers[name] = api.URL
	}
	postRemoteWrite(test, servers["engine"], write)

	// VictoriaMetrics v2.24.0 on the same samples, /api/v1/query_range from S-2m
	// to S+14m at a 60 s step. Keys are seconds after S.
	const (
		periodsA   = "0:24 60:20 120:20 180:20 240:20 300:20 360:20 420:20 480:20 540:20 600:20 660:20 720:20 780:20 840:20"
		periodsC   = "0:24 60:20 120:10 540:40 600:20 660:20 720:20 780:20 840:20"
		throttledA = "0:8 60:0 120:1 180:1 240:0 300:1 360:1 420:0 480:1 540:1 600:0 660:1 720:1 780:0 840:1"
		throttledC = "0:8 60:0 120:1 540:3 600:0 660:1 720:1 780:0 840:1"
		ratioA     = "0:0.3333333333333333 60:0 120:0.05 180:0.05 240:0 300:0.05 360:0.05 420:0 480:0.05 540:0.05 600:0 660:0.05 720:0.05 780:0 840:0.05"
		ratioC     = "0:0.3333333333333333 60:0 120:0.1 540:0.075 600:0 660:0.05 720:0.05 780:0 840:0.05"
	)
	const a, b = "eng048_periods_total[1m]", "eng048_throttled_total[1m]"
	ratio := "increase(" + b + ") / increase(" + a + ")"
	cases := []struct {
		query string
		want  map[string]string // by pod; "" is a series without a pod label
	}{
		{"increase(" + a + ")", map[string]string{"a": periodsA, "c": periodsC}},
		{"increase(" + b + ")", map[string]string{"a": throttledA, "c": throttledC}},
		{"sum(increase(" + a + "))", map[string]string{"": "0:48 60:40 120:30 180:20 240:20 300:20 360:20 420:20 480:20 540:60 600:40 660:40 720:40 780:40 840:40"}},
		{"sum by (pod) (increase(" + a + "))", map[string]string{"a": periodsA, "c": periodsC}},
		{"sum by (pod) (increase(" + b + "))", map[string]string{"a": throttledA, "c": throttledC}},
		{ratio, map[string]string{"a": ratioA, "c": ratioC}},
		{"max(" + ratio + ")", map[string]string{"": "0:0.3333333333333333 60:0 120:0.1 180:0.05 240:0 300:0.05 360:0.05 420:0 480:0.05 540:0.075 600:0 660:0.05 720:0.05 780:0 840:0.05"}},
	}
	for _, server := range []string{"pushdown", "engine"} {
		for _, c := range cases {
			test.Run(server+" "+c.query, func(test *testing.T) {
				data := apiGet[queryDataDTO](test, servers[server], "/api/v1/query_range", url.Values{
					"query": {c.query},
					"start": {strconv.FormatInt(start/1000-120, 10)},
					"end":   {strconv.FormatInt(start/1000+840, 10)},
					"step":  {"60"},
				})
				got := map[string]string{}
				for _, series := range data.Result {
					points := make([]string, 0, len(series.Values))
					for _, point := range series.Values {
						value, _ := strconv.ParseFloat(point[1].(string), 64)
						points = append(points, fmt.Sprintf("%d:%s", int64(point[0].(float64))-start/1000, strconv.FormatFloat(value, 'g', -1, 64)))
					}
					got[series.Metric["pod"]] = strings.Join(points, " ")
				}
				if len(got) != len(c.want) {
					test.Fatalf("series = %v, want %v", got, c.want)
				}
				for pod, want := range c.want {
					if !samePoints(got[pod], want) {
						test.Errorf("pod %q:\n got %s\nwant %s", pod, got[pod], want)
					}
				}
			})
		}
	}
}

// samePoints compares "offset:value" lists with a relative tolerance.
func samePoints(got, want string) bool {
	gotPoints, wantPoints := strings.Fields(got), strings.Fields(want)
	if len(gotPoints) != len(wantPoints) {
		return false
	}
	for i := range gotPoints {
		gotTS, gotValue, _ := strings.Cut(gotPoints[i], ":")
		wantTS, wantValue, _ := strings.Cut(wantPoints[i], ":")
		g, _ := strconv.ParseFloat(gotValue, 64)
		w, _ := strconv.ParseFloat(wantValue, 64)
		if gotTS != wantTS || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
			return false
		}
	}
	return true
}
