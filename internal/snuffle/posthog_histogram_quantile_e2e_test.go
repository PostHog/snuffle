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
)

type quantileTestPoint struct {
	series      int
	timestamp   int64
	temporality string
	bounds      []float64
	counts      []uint64
}

// quantileTestCumulative returns three classic histograms scraped every 15 s
// for 40 minutes: series 1 restarts at minute 22.5, series 2 scrapes with
// jitter and misses minutes 15 to 20.
func quantileTestCumulative(base int64) []quantileTestPoint {
	var points []quantileTestPoint
	for series := 0; series < 3; series++ {
		totals := make([]uint64, 5)
		for m := 0; m < 160; m++ {
			if series == 1 && m == 90 {
				clear(totals)
			}
			for bucket := range totals {
				totals[bucket] += uint64((series+1)*(5-bucket) + (m*(bucket+1)+series)%3)
			}
			if series == 2 && m >= 60 && m < 80 {
				continue
			}
			timestamp := base + int64(m)*15000
			if series == 2 {
				timestamp += int64(m*7919) % 3000
			}
			points = append(points, quantileTestPoint{series: series, timestamp: timestamp, temporality: "cumulative", bounds: []float64{0.1, 0.5, 1, 5}, counts: append([]uint64(nil), totals...)})
		}
	}
	return points
}

// quantileTestDelta returns two delta histograms with one observation group
// every 20 s at irregular times; series 1 misses minutes 10 to 16.6.
func quantileTestDelta(base int64) []quantileTestPoint {
	var points []quantileTestPoint
	for series := 0; series < 2; series++ {
		for k := 0; k < 120; k++ {
			if series == 1 && k >= 30 && k < 50 {
				continue
			}
			counts := make([]uint64, 5)
			counts[(k*31+series*7)%5] = uint64(1 + k%3)
			counts[(k*17+series)%3] += 2
			points = append(points, quantileTestPoint{series: series, timestamp: base + int64(k)*20000 + int64(k*104729)%15000, temporality: "delta", bounds: []float64{0.1, 0.5, 1, 5}, counts: counts})
		}
	}
	return points
}

func insertQuantileTestPoints(test *testing.T, ctx context.Context, client *ClickHouseClient, table, metric string, points []quantileTestPoint) {
	test.Helper()
	rows := make([]string, 0, len(points))
	for _, point := range points {
		var total uint64
		counts := make([]string, len(point.counts))
		for index, count := range point.counts {
			total += count
			counts[index] = strconv.FormatUint(count, 10)
		}
		bounds := make([]string, len(point.bounds))
		for index, bound := range point.bounds {
			bounds[index] = strconv.FormatFloat(bound, 'g', -1, 64)
		}
		if math.IsInf(point.bounds[len(point.bounds)-1], 1) {
			bounds[len(bounds)-1] = "inf"
		}
		rows = append(rows, fmt.Sprintf("(%d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'histogram', %s, %d, %d, [%s], [%s], true, map('region', 'us'), map('instance', %s))",
			e2eTeamID, sqlString(metric), sqlString(metric+strconv.Itoa(point.series)), chTimeMillis(point.timestamp), sqlString(point.temporality), total, total,
			strings.Join(bounds, ", "), strings.Join(counts, ", "), sqlString(strconv.Itoa(point.series))))
	}
	sql := "INSERT INTO " + table + " (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes) VALUES " + strings.Join(rows, ", ")
	if err := client.Exec(ctx, sql); err != nil {
		test.Fatal(err)
	}
}

// TestPostHogHistogramQuantileSum checks the quantile of sum by (le) that
// ClickHouse sums (ENG-010, ENG-031) against VictoriaMetrics and against the
// engine over raw rows.
func TestPostHogHistogramQuantileSum(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	cfg.AllowUnauthenticated = true
	cfg.RemoteWriteInterval = 0
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_histogram_quantile_%d", time.Now().UnixNano())
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
	cfg.TeamID = e2eTeamID
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))

	base := e2eStartMS
	table := tableName(database, cfg.MetricsInputTable)
	cumulative, delta := quantileTestCumulative(base), quantileTestDelta(base)
	insertQuantileTestPoints(test, ctx, client, table, "latency_cumulative", cumulative)
	insertQuantileTestPoints(test, ctx, client, table, "latency_delta", delta)
	// One metric with both temporalities, a prom-client +Inf bound and two
	// delta points at one time.
	mixed := append(append([]quantileTestPoint(nil), cumulative[:160]...), delta[:120]...)
	for index := range mixed {
		if index < 160 {
			mixed[index].bounds = []float64{0.1, 0.5, 1, 5, math.Inf(1)}
			mixed[index].counts = append(append([]uint64(nil), mixed[index].counts...), 0)
		} else {
			mixed[index].series = 1
		}
	}
	mixed = append(mixed, quantileTestPoint{series: 1, timestamp: delta[7].timestamp, temporality: "delta", bounds: delta[7].bounds, counts: []uint64{0, 4, 0, 0, 1}})
	insertQuantileTestPoints(test, ctx, client, table, "latency_mixed", mixed)
	// A source scraped every 15 s that slows to every 60 s at minute 25, and
	// delta sources that write three rows at each minute.
	var shift, dups []quantileTestPoint
	totals := make([]uint64, 5)
	for second := int64(0); second < 40*60; second += 15 {
		if second >= 25*60 && second%60 != 30 {
			continue
		}
		for bucket := range totals {
			totals[bucket] += uint64(5 - bucket + int(second/15)%3)
		}
		shift = append(shift, quantileTestPoint{timestamp: base + second*1000, temporality: "cumulative", bounds: []float64{0.1, 0.5, 1, 5}, counts: append([]uint64(nil), totals...)})
	}
	for minute := int64(0); minute < 40; minute++ {
		for series := 0; series < 2; series++ {
			for row := 0; row < 3; row++ {
				counts := make([]uint64, 5)
				counts[(int(minute)+row+series)%5] = uint64(1 + row)
				dups = append(dups, quantileTestPoint{series: series, timestamp: base + minute*60000 + 7000, temporality: "delta", bounds: []float64{0.1, 0.5, 1, 5}, counts: counts})
			}
		}
	}
	insertQuantileTestPoints(test, ctx, client, table, "latency_shift", shift)
	insertQuantileTestPoints(test, ctx, client, table, "latency_dups", dups)
	// The cumulative samples again as plain _bucket series with an le label,
	// as remote write stores a classic histogram.
	var plain []string
	for _, point := range cumulative {
		var running uint64
		for index, count := range point.counts {
			running += count
			le := "+Inf"
			if index < len(point.bounds) {
				le = strconv.FormatFloat(point.bounds[index], 'g', -1, 64)
			}
			plain = append(plain, fmt.Sprintf("(%d, 'latency_plain_bucket', cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'sum', 'cumulative', true, %d, 1, true, map('region', 'us'), map('instance', %s, 'le', %s))",
				e2eTeamID, sqlString(strconv.Itoa(point.series)+"/"+le), chTimeMillis(point.timestamp), running, sqlString(strconv.Itoa(point.series)), sqlString(le)))
		}
	}
	if err := client.Exec(ctx, "INSERT INTO "+table+" (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, is_monotonic, value, count, has_labels, resource_attributes, attributes) VALUES "+strings.Join(plain, ", ")); err != nil {
		test.Fatal(err)
	}

	serverFor := func(summed bool, maxSamples int) *httptest.Server {
		serverCfg := cfg
		// The reference is the engine over raw rows.
		serverCfg.PostHogCompactHistograms = summed
		serverCfg.PostHogHistogramPushdown = summed
		serverCfg.RangePushdown = summed
		serverCfg.MaxSamples = maxSamples
		mux := http.NewServeMux()
		newServer(serverCfg).routes(mux)
		return httptest.NewServer(mux)
	}
	// The sums need only buckets x steps samples: 5 x 31 here. Reading the
	// raw points, or one series per source bucket, takes more.
	summedAPI, engineAPI, limitedAPI := serverFor(true, cfg.MaxSamples), serverFor(false, cfg.MaxSamples), serverFor(true, 160)
	defer summedAPI.Close()
	defer engineAPI.Close()
	defer limitedAPI.Close()
	start, end := base+10*60000, base+40*60000
	rangeQuery := func(api *httptest.Server, query, step string) queryDataDTO {
		test.Helper()
		return apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {query}, "start": {strconv.FormatInt(start/1000, 10)}, "end": {strconv.FormatInt(end/1000, 10)}, "step": {step}, "nocache": {"1"}})
	}
	summedQueries := func(query, step string) int64 {
		test.Helper()
		server := newServer(cfg)
		stepDuration, _ := time.ParseDuration(step + "s")
		prepared, err := prepareMetricsQLQuery(query, stepDuration, cfg.LookbackDelta, time.UnixMilli(start), time.UnixMilli(end))
		if err != nil {
			test.Fatal(err)
		}
		stats := &promRequestStats{}
		if _, handled, err := server.tryCompactHistogramRange(withPromRequestStats(ctx, stats), prepared.query, time.UnixMilli(start), time.UnixMilli(end), stepDuration); !handled || err != nil {
			test.Fatalf("%s: compact path handled=%v error=%v", query, handled, err)
		}
		return stats.clickHouseQueries.Load()
	}
	for _, check := range []struct {
		query, step string
		// queries is 3 when ClickHouse sums the buckets: the histogram
		// lookup, metadata and the sums.
		queries int64
	}{
		{`histogram_quantile(0.75, sum by (le) (rate(latency_cumulative_bucket[5m])))`, "60", 3},
		{`histogram_quantile(0.5, sum by (le) (rate(latency_cumulative_bucket[2m])))`, "45", 3},
		{`histogram_quantile(0.5, sum by (le) (rate(latency_delta_bucket[5m])))`, "60", 3},
		{`histogram_quantiles("q", 0.5, 0.9, sum by (le) (increase(latency_delta_bucket[10m])))`, "120", 3},
		{`histogram_quantile(0.9, sum by (le) (delta(latency_delta_bucket[3m])))`, "60", 3},
		{`histogram_quantile(0.9, sum by (le) (rate(latency_mixed_bucket[5m])))`, "60", 3},
		// A previous point counts when it is within the interval estimated
		// at each step, which grows when the scrapes slow down.
		{`histogram_quantile(0.5, sum by (le) (rate(latency_shift_bucket[1m])))`, "60", 3},
		// The bare buckets of the ENG-010 sweep, one series per le.
		{`sum by (le) (rate(latency_cumulative_bucket[4m]))`, "60", 3},
		{`sum by (le) (rate(latency_mixed_bucket[4m]))`, "60", 3},
		// increase over a cumulative source is left to the streaming read.
		{`histogram_quantile(0.9, sum by (le) (increase(latency_cumulative_bucket[5m])))`, "60", 4},
	} {
		if queries := summedQueries(check.query, check.step); queries != check.queries {
			test.Errorf("%s: %d ClickHouse queries, want %d", check.query, queries, check.queries)
		}
		got, want := rangeQuery(summedAPI, check.query, check.step), rangeQuery(engineAPI, check.query, check.step)
		assertQuantileResults(test, check.query, got, want)
	}
	// count of the bucket series is answered from one presence query per
	// request (ENG-010), including across the gap and restart.
	// increase over cumulative sources is left to the streaming read. The
	// summed read must say so without building the step grid of every bucket
	// for a result Go discards: a bad source contributes no sums.
	{
		server := newServer(cfg)
		prepared, err := prepareMetricsQLQuery(`histogram_quantile(0.9, sum by (le) (increase(latency_cumulative_bucket[5m])))`, time.Minute, cfg.LookbackDelta, time.UnixMilli(start), time.UnixMilli(end))
		if err != nil {
			test.Fatal(err)
		}
		expr, err := server.parser.ParseExpr(prepared.query)
		if err != nil {
			test.Fatal(err)
		}
		plan := planCompactHistogram(expr, time.UnixMilli(start), time.UnixMilli(end), time.Minute, cfg.LookbackDelta)
		mint := plan.start - plan.selectorRange + 1
		ids, ok, err := server.compactHistogramSources(ctx, plan.alias, plan.selector.LabelMatchers, mint, plan.end)
		if !ok || err != nil {
			test.Fatalf("sources: ok=%v error=%v", ok, err)
		}
		err = queryPostHogSeriesIDs(ctx, server.client, cfg.IDChunkSize, ids, func(_ []uint64, idCondition string) string {
			return compactHistogramSQL(cfg, plan, mint, idCondition)
		}, func(row clickHouseRow) error {
			var bounds, sums []float64
			var present []uint8
			var bad, split uint8
			if err := row.Scan(&bounds, &sums, &present, &bad, &split); err != nil {
				return err
			}
			if bad == 0 || len(sums) != 0 {
				return fmt.Errorf("increase over cumulative sources: bad=%d with %d sums, want bad with none", bad, len(sums))
			}
			return nil
		})
		if err != nil {
			test.Fatal(err)
		}
	}
	// At a 15 s step the window of a bare selector follows each step's
	// interval estimate: it grows where the scrapes slow down, and the delta
	// rows that share a minute count as one point, as the engine merges them.
	for _, check := range []struct{ query, step string }{
		{`count(latency_cumulative_bucket)`, "60"},
		{`count(latency_delta_bucket{instance="1"})`, "60"},
		{`count(latency_mixed_bucket)`, "60"},
		{`count(latency_shift_bucket)`, "15"},
		{`count(latency_dups_bucket)`, "15"},
	} {
		step, _ := time.ParseDuration(check.step + "s")
		prepared, err := prepareMetricsQLQuery(check.query, step, cfg.LookbackDelta, time.UnixMilli(start), time.UnixMilli(end))
		if err != nil {
			test.Fatal(err)
		}
		if _, ok, err := newServer(cfg).tryFastRangeGroups(ctx, prepared, time.UnixMilli(start), time.UnixMilli(end), step); !ok || err != nil {
			test.Fatalf("%s: range pushdown handled=%v error=%v", check.query, ok, err)
		}
		assertQuantileResults(test, check.query, rangeQuery(summedAPI, check.query, check.step), rangeQuery(engineAPI, check.query, check.step))
	}
	// The plain buckets need 5 le series x 31 steps. One sample less, and the
	// compact path declines before it allocates them.
	{
		limited := cfg
		limited.MaxSamples = 5*31 - 1
		query := `histogram_quantile(0.75, sum by (le) (rate(latency_plain_bucket[5m])))`
		prepared, err := prepareMetricsQLQuery(query, time.Minute, cfg.LookbackDelta, time.UnixMilli(start), time.UnixMilli(end))
		if err != nil {
			test.Fatal(err)
		}
		if _, handled, err := newServer(limited).tryCompactHistogramRange(ctx, prepared.query, time.UnixMilli(start), time.UnixMilli(end), time.Minute); handled || err != nil {
			test.Fatalf("%s: compact path handled=%v error=%v over the sample budget", query, handled, err)
		}
	}
	// VictoriaMetrics v1.157.0 gives these values for the same samples,
	// imported as classic histograms (cumulative le buckets). It has no delta
	// temporality, so the delta quantile there reads
	// sum_over_time(latency_delta_bucket[5m]), the window sum that rate
	// divides by 300 s; a quantile does not change with that scale.
	victoriaMetrics := map[string][]float64{
		`histogram_quantile(0.75, sum by (le) (rate(latency_cumulative_bucket[5m])))`: {
			0.9465313972611403, 0.9481854364612792, 0.9446141837999301, 0.9464016571864373, 0.9481854364612792, 0.9446141837999301, 0.9451292638853664, 0.94777850579431,
			0.9429002345929496, 0.9432626727027791, 0.9774999999999998, 0.9457613625513226, 0.9493929868473816, 0.9487103551840836, 0.9459383993801194, 0.9471069881845395,
			0.9483245042537718, 0.9446141837999301, 0.9464016571864373, 0.9481854364612792, 0.9446141837999301, 0.9464016571864373, 0.9481854364612792, 0.9447524036180776,
			0.9464016571864373, 0.9483245042537718, 0.9446141837999301, 0.9464016571864373, 0.9481854364612792, 0.9446141837999301, 0.945744475348338,
		},
		`histogram_quantile(0.5, sum by (le) (rate(latency_delta_bucket[5m])))`: {
			0.45, 0.42, 0.42307692307692313, 0.43333333333333335, 0.40476190476190477, 0.45, 0.45, 0.5119047619047619, 0.48888888888888893, 0.5092592592592593,
			0.46153846153846156, 0.4928571428571429, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45, 0.45,
		},
	}
	for query, values := range victoriaMetrics {
		want := queryDataDTO{Result: []sampleResultDTO{{Metric: map[string]string{}}}}
		for index, value := range values {
			want.Result[0].Values = append(want.Result[0].Values, []any{float64(start/1000 + int64(index)*60), strconv.FormatFloat(value, 'g', -1, 64)})
		}
		assertQuantileResults(test, query+" against VictoriaMetrics", rangeQuery(limitedAPI, query, "60"), want)
		// The plain _bucket form of the cumulative samples: the range
		// pushdown sums the buckets by le and Go applies the quantile.
		if !strings.Contains(query, "latency_cumulative_bucket") {
			continue
		}
		plainQuery := strings.ReplaceAll(query, "latency_cumulative_bucket", "latency_plain_bucket")
		summedQueries(plainQuery, "60")
		assertQuantileResults(test, plainQuery+" against VictoriaMetrics", rangeQuery(summedAPI, plainQuery, "60"), want)
	}
}

func assertQuantileResults(test *testing.T, name string, got, want queryDataDTO) {
	test.Helper()
	if len(got.Result) != len(want.Result) || len(got.Result) == 0 {
		test.Fatalf("%s: %d series, want %d", name, len(got.Result), len(want.Result))
	}
	for index := range want.Result {
		if fmt.Sprint(got.Result[index].Metric) != fmt.Sprint(want.Result[index].Metric) || len(got.Result[index].Values) != len(want.Result[index].Values) {
			test.Fatalf("%s: series %v with %d points, want %v with %d", name, got.Result[index].Metric, len(got.Result[index].Values), want.Result[index].Metric, len(want.Result[index].Values))
		}
		for point, wantValue := range want.Result[index].Values {
			gotValue := got.Result[index].Values[point]
			g, _ := strconv.ParseFloat(fmt.Sprint(gotValue[1]), 64)
			w, _ := strconv.ParseFloat(fmt.Sprint(wantValue[1]), 64)
			if fmt.Sprint(gotValue[0]) != fmt.Sprint(wantValue[0]) || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				test.Fatalf("%s: point %v = %v, want %v = %v", name, gotValue[0], gotValue[1], wantValue[0], wantValue[1])
			}
		}
	}
}
