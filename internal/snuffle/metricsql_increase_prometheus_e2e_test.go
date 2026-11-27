package snuffle

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// increase_prometheus is last minus first sample in the window, with counter
// resets removed, and tlast_over_time is the time of the last sample. The
// expected values come from VictoriaMetrics on the same samples.
func TestMetricsQLIncreasePrometheusEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	for _, layout := range []schemaLayout{schemaLayoutCurrent, schemaLayoutPostHog} {
		test.Run(string(layout), func(test *testing.T) {
			test.Setenv("CH_SCHEMA_LAYOUT", string(layout))
			assertIncreasePrometheus(test)
		})
	}
}

func assertIncreasePrometheus(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_increase_prometheus_e2e_%d", time.Now().UnixNano())
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

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()

	// Samples every 30s from 1700000010; index -> value.
	write := &prompb.WriteRequest{}
	for _, s := range []struct {
		name, env string
		values    map[int]float64
	}{
		{"steady", "a", map[int]float64{0: 100, 1: 110, 2: 120, 3: 130, 4: 140, 5: 150, 6: 160, 7: 170, 8: 180, 9: 190}},
		{"reset", "a", map[int]float64{0: 100, 1: 110, 2: 120, 3: 5, 4: 15, 5: 25, 6: 35, 7: 45, 8: 55, 9: 65}},
		// Starts mid-range.
		{"late", "b", map[int]float64{5: 500, 6: 510, 7: 520, 8: 530, 9: 540}},
		// A drop of less than 1/8 is jitter, not a reset.
		{"jitter", "b", map[int]float64{0: 100, 1: 110, 2: 120, 3: 118, 4: 128, 5: 138, 6: 148, 7: 158, 8: 168, 9: 178}},
		{"gap", "b", map[int]float64{0: 1, 1: 2, 2: 3, 8: 4, 9: 5}},
	} {
		series := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: labels.MetricName, Value: "eng041_counter"},
			{Name: "case", Value: s.name},
			{Name: "env", Value: s.env},
		}}
		for index := range 10 {
			if value, ok := s.values[index]; ok {
				series.Samples = append(series.Samples, prompb.Sample{Timestamp: e2eStartMS + int64(index)*30_000, Value: value})
			}
		}
		write.Timeseries = append(write.Timeseries, series)
	}
	postRemoteWrite(test, api.URL, write)

	// Each series as "seconds after 1700000000:value" pairs, keyed by label.
	render := func(result []sampleResultDTO, label string) map[string]string {
		out := map[string]string{}
		for _, series := range result {
			points := series.Values
			if series.Value != nil {
				points = [][]any{series.Value}
			}
			var parts []string
			for _, point := range points {
				parts = append(parts, fmt.Sprintf("%d:%s", int64(point[0].(float64))-1_700_000_000, point[1]))
			}
			out[series.Metric[label]] = strings.Join(parts, " ")
		}
		return out
	}
	ranged := func(query string) map[string]string {
		return render(apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {query}, "start": {"1700000010"}, "end": {"1700000310"}, "step": {"30"}}).Result, "case")
	}
	instant := func(query, label string) map[string]string {
		return render(apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {query}, "time": {"1700000300"}}).Result, label)
	}

	for _, c := range []struct {
		name string
		got  map[string]string
		want map[string]string
	}{
		{"increase_prometheus range", ranged(`increase_prometheus(eng041_counter[2m])`), map[string]string{
			"steady": "40:10 70:20 100:30 130:30 160:30 190:30 220:30 250:30 280:30 310:20",
			"reset":  "40:10 70:20 100:25 130:25 160:25 190:30 220:30 250:30 280:30 310:20",
			"late":   "190:10 220:20 250:30 280:30 310:20",
			"jitter": "40:10 70:20 100:20 130:20 160:20 190:30 220:30 250:30 280:30 310:20",
			"gap":    "40:1 70:2 100:2 130:1 280:1 310:1",
		}},
		// The implicit window is the step, which holds one sample.
		{"increase_prometheus implicit window", ranged(`increase_prometheus(eng041_counter)`), map[string]string{}},
		{"tlast_over_time range", ranged(`tlast_over_time(eng041_counter[1m])`), map[string]string{
			"steady": "10:1700000010 40:1700000040 70:1700000070 100:1700000100 130:1700000130 160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280 310:1700000280",
			"reset":  "10:1700000010 40:1700000040 70:1700000070 100:1700000100 130:1700000130 160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280 310:1700000280",
			"late":   "160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280 310:1700000280",
			"jitter": "10:1700000010 40:1700000040 70:1700000070 100:1700000100 130:1700000130 160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280 310:1700000280",
			"gap":    "10:1700000010 40:1700000040 70:1700000070 100:1700000070 250:1700000250 280:1700000280 310:1700000280",
		}},
		{"tlast_over_time implicit window", ranged(`tlast_over_time(eng041_counter)`), map[string]string{
			"steady": "10:1700000010 40:1700000040 70:1700000070 100:1700000100 130:1700000130 160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280",
			"reset":  "10:1700000010 40:1700000040 70:1700000070 100:1700000100 130:1700000130 160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280",
			"late":   "160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280",
			"jitter": "10:1700000010 40:1700000040 70:1700000070 100:1700000100 130:1700000130 160:1700000160 190:1700000190 220:1700000220 250:1700000250 280:1700000280",
			"gap":    "10:1700000010 40:1700000040 70:1700000070 250:1700000250 280:1700000280",
		}},
		// The shapes of the dashboards in the issue.
		{"sum increase_prometheus", instant(`sum by (env) (increase_prometheus(eng041_counter[30h]))`, "env"), map[string]string{"a": "300:175", "b": "300:124"}},
		{"max tlast_over_time", instant(`max by (env) (tlast_over_time(eng041_counter[1h]))`, "env"), map[string]string{"a": "300:1700000280", "b": "300:1700000280"}},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			test.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}
