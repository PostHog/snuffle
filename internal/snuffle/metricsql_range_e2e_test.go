package snuffle

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// topk_<stat> and bottomk_<stat> rank series over the whole range. The
// expected results come from VictoriaMetrics on the same samples.
func TestMetricsQLRangeTopKEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	for _, layout := range []schemaLayout{schemaLayoutCurrent, schemaLayoutPostHog} {
		test.Run(string(layout), func(test *testing.T) {
			test.Setenv("CH_SCHEMA_LAYOUT", string(layout))
			assertMetricsQLRangeTopK(test)
		})
	}
}

func assertMetricsQLRangeTopK(test *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_topk_e2e_%d", time.Now().UnixNano())
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

	// Samples every minute from 1700000000 at minute 0 to minute 10. NaN
	// marks no sample: d, e, f and p4 have gaps.
	const start = int64(1_700_000_000)
	nan := math.NaN()
	write := &prompb.WriteRequest{}
	add := func(metric string, lbls []string, values ...float64) {
		series := prompb.TimeSeries{Labels: []prompb.Label{{Name: labels.MetricName, Value: metric}}}
		for i := 0; i < len(lbls); i += 2 {
			series.Labels = append(series.Labels, prompb.Label{Name: lbls[i], Value: lbls[i+1]})
		}
		for minute, value := range values {
			if !math.IsNaN(value) {
				series.Samples = append(series.Samples, prompb.Sample{Timestamp: (start + int64(minute)*60) * 1000, Value: value})
			}
		}
		write.Timeseries = append(write.Timeseries, series)
	}
	add("topk_e2e_depth", []string{"partition", "a", "pod", "x"}, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	add("topk_e2e_depth", []string{"partition", "b", "pod", "x"}, 10, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	add("topk_e2e_depth", []string{"partition", "c", "pod", "x"}, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3)
	add("topk_e2e_depth", []string{"partition", "d", "pod", "x"}, 5, 5, 5, 5, 5)
	add("topk_e2e_depth", []string{"partition", "e", "pod", "x"}, nan, nan, nan, nan, nan, nan, 5, 5, 5, 5, 5)
	add("topk_e2e_depth", []string{"partition", "f", "pod", "x"}, nan, nan, nan, nan, nan, 2, 2)
	for _, node := range []string{"n1", "n2"} {
		add("topk_e2e_alloc", []string{"nodepool", "p1", "node", node}, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)
		add("topk_e2e_alloc", []string{"nodepool", "p2", "node", node}, 0.5, 0.5, 0.5, 0.5, 0.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5)
		add("topk_e2e_alloc", []string{"nodepool", "p3", "node", node}, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		add("topk_e2e_alloc", []string{"nodepool", "p4", "node", node}, nan, nan, nan, 3, 3, 3)
	}
	// For default: b has a gap and c starts late. The fallback has no c, a
	// gap in b and an extra z.
	add("default_e2e_depth", []string{"p", "a"}, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	add("default_e2e_depth", []string{"p", "b"}, 2, 2, 2, nan, nan, nan, nan, 2, 2, 2, 2)
	add("default_e2e_depth", []string{"p", "c"}, nan, nan, nan, nan, nan, nan, 3, 3, 3, 3, 3)
	add("default_e2e_fallback", []string{"p", "a"}, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7)
	add("default_e2e_fallback", []string{"p", "b"}, 8, 8, 8, 8, 8, nan, nan, nan, 8, 8, 8)
	add("default_e2e_fallback", []string{"p", "z"}, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9)
	add("default_e2e_cpu_seconds_total", []string{"name", "a", "tid", "1"}, 0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60)
	add("default_e2e_cpu_seconds_total", []string{"name", "b", "tid", "2"}, nan, nan, nan, nan, nan, nan, 0, 12, 24, 36, 48)
	add("default_e2e_cpu_seconds_total", []string{"name", "c", "tid", "3"}, 0, 3, 6, 9)
	add("default_e2e_cpu_seconds_total", []string{"name", "d", "tid", "4"}, 0, 30, 60, nan, nan, nan, nan, 0, 30, 60, 90)
	postRemoteWrite(test, api.URL, write)

	// Results as sorted lines `{labels} minute:value ...`.
	format := func(result []sampleResultDTO) []string {
		lines := []string{}
		for _, series := range result {
			points := series.Values
			if series.Value != nil {
				points = [][]any{series.Value}
			}
			line := labels.FromMap(series.Metric).String()
			for _, point := range points {
				line += fmt.Sprintf(" %d:%s", (int64(point[0].(float64))-start)/60, point[1])
			}
			lines = append(lines, line)
		}
		slices.Sort(lines)
		return lines
	}
	flat := func(metric string, from, to int, value string) string {
		line := metric
		for minute := from; minute <= to; minute++ {
			line += " " + strconv.Itoa(minute) + ":" + value
		}
		return line
	}
	a := flat(`{partition="a"}`, 1, 10, "1")
	b := `{partition="b"} 1:10` + flat("", 2, 10, "0")
	c := flat(`{partition="c"}`, 1, 10, "3")
	d := flat(`{partition="d"}`, 1, 5, "5")
	e := flat(`{partition="e"}`, 6, 10, "5")
	f := flat(`{partition="f"}`, 5, 7, "2")
	p1 := flat(`{nodepool="p1"}`, 1, 10, "4")
	p2 := `{nodepool="p2"}` + flat("", 1, 4, "1") + flat("", 5, 10, "9")
	p3 := flat(`{nodepool="p3"}`, 1, 10, "2")
	p4 := flat(`{nodepool="p4"}`, 3, 6, "6")
	rest := " 1:14 2:4 3:4 4:4 5:6 6:6 7:6 8:4 9:4 10:4"
	const depth = `max by (partition) (topk_e2e_depth)`
	const alloc = `sum by (nodepool) (topk_e2e_alloc)`
	const ratio = `(` + depth + ` - 1) / (` + depth + ` - 1)`
	const gapped = `max by (p) (default_e2e_depth)`
	const fallback = `max by (p) (default_e2e_fallback)`
	const missing = `max by (p) (default_e2e_missing)`
	const cpu = `sum by (name, tid) (rate(default_e2e_cpu_seconds_total[2m]))`
	da := flat(`{p="a"}`, 1, 10, "1")
	dc := flat(`{p="c"}`, 6, 10, "3")
	cpuD := `{name="d", tid="4"} 1:0.5 2:0.5 3:0.5 4:0 5:0 6:0 7:0 8:0.5 9:0.5 10:0.5`

	for query, want := range map[string][]string{
		// The repros of ENG-006, with k above the series count.
		`topk_max(10, ` + depth + `)`:                        {a, b, c, d, e, f},
		`topk_avg(15, ` + alloc + `, "nodepool=other")`:      {p1, p2, p3, p4},
		`topk_max(3, ` + depth + `)`:                         {b, d, e},
		`topk_avg(2, ` + depth + `, "partition=other")`:      {d, e, `{partition="other"}` + rest},
		`bottomk_min(2, ` + depth + `)`:                      {a, b},
		`topk_last(2, ` + depth + `, "rest")`:                {d, e, `{rest="rest"}` + rest},
		`topk_median(2, ` + depth + `)`:                      {d, e},
		`bottomk_avg(2, ` + depth + `, "partition=other")`:   {a, b, `{partition="other"} 1:8 2:8 3:8 4:8 5:10 6:10 7:10 8:8 9:8 10:8`},
		`topk_avg(2, ` + alloc + `, "nodepool=other")`:       {p2, p4, flat(`{nodepool="other"}`, 1, 10, "6")},
		`bottomk_median(1, ` + alloc + `, "nodepool=other")`: {p3, `{nodepool="other"} 1:5 2:5 3:11 4:11 5:19 6:19 7:13 8:13 9:13 10:13`},
		`topk_min(1, ` + alloc + `)`:                         {p4},
		`bottomk_last(1, ` + alloc + `)`:                     {p3},
		`TOPK_MAX(1, ` + depth + `)`:                         {b},
		`topk_max(10, ` + ratio + `)`:                        {flat(`{partition="b"}`, 1, 10, "1"), flat(`{partition="c"}`, 1, 10, "1"), flat(`{partition="d"}`, 1, 5, "1"), flat(`{partition="e"}`, 6, 10, "1"), flat(`{partition="f"}`, 5, 7, "1")},
		// d and e tie at 5. VictoriaMetrics keeps either, by its internal
		// series order; Snuffle keeps the first by labels.
		`topk_max(2, ` + depth + `)`: {b, d},

		// default (ENG-009).
		gapped + ` default 0`:                     {da, `{p="b"} 1:2 2:2 3:2 4:0 5:0 6:0 7:2 8:2 9:2 10:2`, `{p="c"}` + flat("", 1, 5, "0") + flat("", 6, 10, "3")},
		gapped + ` default 0 + 1`:                 {da, `{p="b"} 1:2 2:2 3:2 4:1 5:1 6:1 7:2 8:2 9:2 10:2`, `{p="c"}` + flat("", 1, 5, "1") + flat("", 6, 10, "3")},
		gapped + ` default ` + fallback:           {da, `{p="b"} 1:2 2:2 3:2 4:8 5:8 7:2 8:2 9:2 10:2`, dc},
		gapped + ` default 0 or ` + fallback:      {da, `{p="b"} 1:2 2:2 3:2 4:8 5:8 7:2 8:2 9:2 10:2`, dc},
		gapped + ` default sum(` + fallback + `)`: {da, `{p="b"} 1:2 2:2 3:2 4:24 5:24 6:16 7:2 8:2 9:2 10:2`, `{p="c"}` + flat("", 1, 5, "24") + flat("", 6, 10, "3")},
		`default_e2e_depth{p="b"} default 0`:      {`{__name__="default_e2e_depth", p="b"} 1:2 2:2 3:2 4:0 5:0 6:0 7:2 8:2 9:2 10:2`},
		missing + ` default 0`:                    {flat(`{}`, 1, 10, "0")},
		missing + ` default ` + fallback:          {flat(`{p="a"}`, 1, 10, "7"), `{p="b"}` + flat("", 1, 5, "8") + flat("", 8, 10, "8"), flat(`{p="z"}`, 1, 10, "9")},
		cpu + ` default 0`: {
			flat(`{name="a", tid="1"}`, 1, 10, "0.1"), `{name="b", tid="2"}` + flat("", 1, 6, "0") + flat("", 7, 10, "0.2"),
			`{name="c", tid="3"}` + flat("", 1, 4, "0.05") + flat("", 5, 10, "0"), cpuD,
		},
		// The repro of ENG-009.
		`topk_avg(2, ` + cpu + ` default 0)`: {flat(`{name="a", tid="1"}`, 1, 10, "0.1"), cpuD},
	} {
		slices.Sort(want)
		// The second pass reads the inner queries and the right side of
		// default from the range cache; the ops must not change the cached
		// series.
		for pass := 1; pass <= 2; pass++ {
			got := format(apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {query}, "start": {"1700000060"}, "end": {"1700000600"}, "step": {"60"}}).Result)
			if !reflect.DeepEqual(got, want) {
				test.Errorf("range %s (pass %d)\n got %q\nwant %q", query, pass, got, want)
			}
		}
	}

	// Instant queries rank on the single point, at minute 5.
	for query, want := range map[string][]string{
		`topk_avg(2, ` + alloc + `, "nodepool=other")`:       {`{nodepool="other"} 5:6`, `{nodepool="p2"} 5:9`, `{nodepool="p4"} 5:6`},
		`topk_max(10, ` + depth + `)`:                        {`{partition="a"} 5:1`, `{partition="b"} 5:0`, `{partition="c"} 5:3`, `{partition="d"} 5:5`, `{partition="f"} 5:2`},
		`bottomk_median(1, ` + alloc + `, "nodepool=other")`: {`{nodepool="other"} 5:19`, `{nodepool="p3"} 5:2`},
		`topk_min(1, ` + alloc + `)`:                         {`{nodepool="p2"} 5:9`},
		`TOPK_MAX(1, ` + depth + `)`:                         {`{partition="d"} 5:5`},
		`topk_max(10, ` + ratio + `)`:                        {`{partition="b"} 5:1`, `{partition="c"} 5:1`, `{partition="d"} 5:1`, `{partition="f"} 5:1`},
		gapped + ` default 0`:                                {`{p="a"} 5:1`, `{p="b"} 5:2`},
		missing + ` default 0`:                               {`{} 5:0`},
		missing + ` default ` + fallback:                     {`{p="a"} 5:7`, `{p="b"} 5:8`, `{p="z"} 5:9`},
	} {
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {query}, "time": {"1700000300"}})
		if result.ResultType != "vector" {
			test.Errorf("instant %s: result type %q, want vector", query, result.ResultType)
		}
		if got := format(result.Result); !reflect.DeepEqual(got, want) {
			test.Errorf("instant %s\n got %q\nwant %q", query, got, want)
		}
	}
}
