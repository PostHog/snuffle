package snuffle

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

func TestNativeRangePushdownMatchesEngine(t *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		t.Skip("set SNUFFLE_E2E=1")
	}
	t.Setenv("CH_SCHEMA_LAYOUT", "current")
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.TeamID = e2eTeamID
	cfg.DefaultTeamID = e2eTeamID
	cfg.AllowUnauthenticated = true
	cfg.RangePushdown = true
	cfg.RemoteWriteInterval = 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root := NewClickHouseClient(cfg)
	database := fmt.Sprintf("snuffle_native_range_%d", time.Now().UnixNano())
	if err := root.Exec(ctx, "CREATE DATABASE "+quoteIdent(database)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Exec(context.Background(), "DROP DATABASE "+quoteIdent(database)+" SYNC"); err != nil {
			t.Error(err)
		}
		root.conn.Close()
	}()
	cfg.CHDatabase = database
	fast := newServer(cfg)
	defer fast.client.conn.Close()
	loadE2ESchema(t, ctx, fast.client, filepath.Join(repoRoot(t), "scripts", "create_metrics_schema.sql"))
	cfg.RangePushdown = false
	engine := newServer(cfg)
	defer engine.client.conn.Close()
	mux := http.NewServeMux()
	engine.routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	base := int64(1_700_000_000_000)
	req := &prompb.WriteRequest{}
	for series := 0; series < 7; series++ {
		row := prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: "native_counter"}, {Name: "job", Value: "api"}, {Name: "instance", Value: strconv.Itoa(series)}, {Name: "group", Value: strconv.Itoa(series % 2)}}}
		for i := 0; i < 60; i++ {
			if series == 1 && i > 12 && i < 30 {
				continue
			}
			if series == 2 && i < 25 {
				continue
			}
			if series == 6 && i != 5 {
				continue
			}
			ts := base + int64(i)*30_000
			if series == 5 {
				ts = base + 4*60_000 + int64(i)*2_000
			}
			if series == 3 {
				ts += int64(i%4) * 3_000
			}
			value := float64(100 + i*7)
			if series == 1 {
				value = float64((i % 15) * 3)
			}
			if series == 2 && i == 40 {
				value -= 10
			}
			if series == 4 && i == 35 {
				value = math.Float64frombits(promvalue.StaleNaN)
			}
			row.Samples = append(row.Samples, prompb.Sample{Timestamp: ts, Value: value})
		}
		req.Timeseries = append(req.Timeseries, row)
	}
	other := req.Timeseries[0]
	other.Labels = append([]prompb.Label(nil), other.Labels...)
	other.Labels[0].Value = "native_other"
	req.Timeseries = append(req.Timeseries, other)
	postRemoteWrite(t, api.URL, req)
	// Identical labels in another tenant must never contribute.
	postRemoteWriteForTeam(t, api.URL, e2eTeamID+1, req)
	queryCount := func() float64 {
		var metric dto.Metric
		if err := fast.metrics.clickHouseQueries.WithLabelValues("ok").Write(&metric); err != nil {
			t.Fatal(err)
		}
		return metric.GetCounter().GetValue()
	}
	queries := []string{
		`native_counter`, `native_counter or native_other`, `sum({__name__=~"native_counter|native_other"})`, `native_counter{instance="missing"}`, `native_counter{instance!="1"}`,
		`sum(native_counter)`, `running_sum(sum(increase(native_counter[1m])))`, `avg by (group)(native_counter)`, `sum without(instance)(native_counter)`,
		`count(count by(group)(native_counter))`, `quantile(0,0/native_counter)`, `min(0/native_counter)`, `count(0/native_counter)`, `quantile(0.6,native_counter)`,
		`sum(native_counter{group="0"} or native_counter{instance=~"0|1|2"})`,
		`(native_counter{instance="0"}*2) or native_counter{instance="0"}`,
		`sum(native_counter*2)`, `sum(10/native_counter)`,
		`sum(rate(native_counter))`, `sum(increase(native_counter[2m] offset 3m))`, `sum(increase(native_counter[2m] offset -1m))`, `sum(rate(native_counter[1m] offset -2m))`, `sum(increase(native_counter[2m])) offset 3m`, `sum(native_counter offset 3m)`, `sum(native_counter) offset 2m`,
	}
	for _, fn := range []string{"increase", "rate", "irate", "delta", "idelta", "sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "count_over_time", "last_over_time", "present_over_time"} {
		queries = append(queries, fmt.Sprintf(`%s(native_counter[2m])`, fn), fmt.Sprintf(`sum by(group)(%s(native_counter[2m]))`, fn))
	}
	var branches []string
	for i := 0; i < 32; i++ {
		branches = append(branches, fmt.Sprintf(`native_counter{instance="%d"}`, i))
	}
	queries = append(queries, "sum("+strings.Join(branches, " or ")+")")
	start, end := time.UnixMilli(base+4*60_000), time.UnixMilli(base+28*60_000)
	for _, step := range []time.Duration{time.Minute, 45 * time.Second} {
		for _, query := range queries {
			t.Run(query+"/"+step.String(), func(t *testing.T) {
				params := url.Values{"query": {query}, "start": {strconv.FormatFloat(float64(start.UnixMilli())/1000, 'f', -1, 64)}, "end": {strconv.FormatFloat(float64(end.UnixMilli())/1000, 'f', -1, 64)}, "step": {step.String()}}
				want := apiGet[queryDataDTO](t, api.URL, "/api/v1/query_range", params)
				prepared, err := prepareMetricsQLQuery(query, step, cfg.LookbackDelta, start, end)
				if err != nil {
					t.Fatal(err)
				}
				before := queryCount()
				got, ok, err := fast.tryFastRangeQuery(ctx, prepared, start, end, step)
				if err != nil || !ok {
					t.Fatalf("handled=%v err=%v", ok, err)
				}
				if n := queryCount() - before; n != 1 {
					t.Fatalf("issued %v SQL queries, want 1", n)
				}
				assertSameMatrix(t, got, want)
			})
		}
	}
	t.Run("unsupported queries and disabled pushdown", func(t *testing.T) {
		for _, query := range []string{`native_counter @ 1700000240`, `native_counter + native_counter`, `sum(rate(native_counter[1h]))`} {
			prepared, err := prepareMetricsQLQuery(query, time.Second, cfg.LookbackDelta, start, end)
			if err != nil {
				t.Fatal(err)
			}
			before := queryCount()
			_, ok, err := fast.tryFastRangeQuery(ctx, prepared, start, end, time.Second)
			if ok || err != nil || queryCount() != before {
				t.Fatalf("%s: handled=%v error=%v", query, ok, err)
			}
		}
		prepared, _ := prepareMetricsQLQuery(`sum(native_counter)`, time.Minute, cfg.LookbackDelta, start, end)
		_, ok, err := engine.tryFastRangeQuery(ctx, prepared, start, end, time.Minute)
		if ok || err != nil {
			t.Fatalf("disabled: handled=%v error=%v", ok, err)
		}
	})
	t.Run("query errors are returned", func(t *testing.T) {
		failed := *fast
		failed.cfg.SamplesTable = "missing_samples_table"
		prepared, _ := prepareMetricsQLQuery(`sum(native_counter)`, time.Minute, cfg.LookbackDelta, start, end)
		_, ok, err := failed.tryFastRangeQuery(ctx, prepared, start, end, time.Minute)
		if !ok || err == nil {
			t.Fatalf("handled=%v error=%v", ok, err)
		}
	})
	t.Run("series limit", func(t *testing.T) {
		limited := *fast
		limited.cfg.MaxSeries = 2
		prepared, _ := prepareMetricsQLQuery(`sum(native_counter)`, time.Minute, cfg.LookbackDelta, start, end)
		_, ok, err := limited.tryFastRangeQuery(ctx, prepared, start, end, time.Minute)
		if !ok || err == nil {
			t.Fatalf("handled=%v error=%v", ok, err)
		}
	})
	t.Run("native histograms retain engine semantics", func(t *testing.T) {
		hist := req.Timeseries[0]
		hist.Samples = nil
		hist.Histograms = []prompb.Histogram{{Count: &prompb.Histogram_CountInt{CountInt: 3}, Sum: 1.5, ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1}, Timestamp: start.UnixMilli()}}
		postRemoteWrite(t, api.URL, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{hist}})
		prepared, _ := prepareMetricsQLQuery(`sum(native_counter)`, time.Minute, cfg.LookbackDelta, start, end)
		_, ok, err := fast.tryFastRangeQuery(ctx, prepared, start, end, time.Minute)
		if ok || err != nil {
			t.Fatalf("handled=%v error=%v", ok, err)
		}
	})
}
