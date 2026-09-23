package snuffle

import (
	"context"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
)

func TestPostHogHistogramEndToEnd(test *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		test.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	test.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := ConfigFromEnv()
	cfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	cfg.CHDatabase = ""
	root := NewClickHouseClient(cfg)
	waitForClickHouse(test, ctx, root)
	database := fmt.Sprintf("snuffle_histogram_e2e_%d", time.Now().UnixNano())
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
	client := NewClickHouseClient(cfg)
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", metricsSchemaFile(cfg.storageSchemaLayout())))
	const metric = "test_duration_seconds"
	insertHistogram := func(name, temporality, metricType string, team uint64) {
		test.Helper()
		bounds := "[0.5, 1.0]"
		if metricType == "exponential_histogram" {
			bounds = "[0.5, 1.0, 2.0]"
		}
		for index := 1; index <= 3; index++ {
			sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
				SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', %s, %s, %d, %d, %s, [%d, %d, %d], true, map('region', 'resource'), map('region', 'metric', 'status', '200')`,
				tableName(database, cfg.MetricsInputTable), team, sqlString(name), sqlString(name), chTimeMillis(e2eStartMS+int64(index-1)*30000), sqlString(metricType), sqlString(temporality), index*12, index*10, bounds, index*2, index*3, index*5)
			if err := client.Exec(ctx, sql); err != nil {
				test.Fatal(err)
			}
		}
	}
	insertReal := func(name string, team uint64, timestamp int64) {
		test.Helper()
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, value, count, has_labels, attributes)
			SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'other', 'sum', 777, 1, true, map('le', '0.5')`,
			tableName(database, cfg.MetricsInputTable), team, sqlString(name), sqlString(name), chTimeMillis(timestamp))
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	insertPlain := func(name string, timestamp int64, value float64, attributes map[string]string) {
		test.Helper()
		pairs := make([]string, 0, len(attributes)*2)
		for _, key := range slices.Sorted(maps.Keys(attributes)) {
			pairs = append(pairs, sqlString(key), sqlString(attributes[key]))
		}
		fingerprint := name + "|" + strings.Join(pairs, "|")
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, value, count, has_labels, resource_attributes, attributes)
			SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'sum', %v, 1, true, map('region', 'resource'), map(%s)`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(name), sqlString(fingerprint), chTimeMillis(timestamp), value, strings.Join(pairs, ", "))
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	insertHistogram(metric, "cumulative", "histogram", e2eTeamID)
	insertReal(metric+"_count", e2eTeamID+1, e2eEndMS)
	mux := http.NewServeMux()
	server := newServer(cfg)
	server.routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	query := func(test *testing.T, expression string) queryDataDTO {
		test.Helper()
		return apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {expression}, "time": {"1700000070"}})
	}
	assertValue := func(test *testing.T, expression string, want float64) {
		test.Helper()
		result := query(test, expression)
		if len(result.Result) != 1 {
			test.Fatalf("%s returned %d series, want one", expression, len(result.Result))
		}
		got, err := strconv.ParseFloat(sampleString(result.Result[0].Value), 64)
		if err != nil || math.Abs(got-want) > 1e-9 {
			test.Fatalf("%s = %v (%v), want %v", expression, got, err, want)
		}
	}
	test.Run("combined metadata matches general path", func(test *testing.T) {
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		querier := &CHQuerier{queryable: NewCHQueryable(client, teamCfg)}
		for _, suffix := range postHogHistogramSuffixes {
			for _, withSamples := range []bool{false, true} {
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric+suffix)}
				stats := &promRequestStats{}
				var got []*seriesMeta
				var err error
				if withSamples {
					got, err = querier.selectPostHogSeriesSamples(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, false, matchers...)
				} else {
					got, err = querier.selectPostHogSeries(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, matchers...)
				}
				if err != nil {
					test.Fatal(err)
				}
				if count := stats.clickHouseQueries.Load(); count != 2 {
					test.Fatalf("%s samples=%v used %d queries, want metadata + samples", suffix, withSamples, count)
				}
				want, err := querier.appendPostHogHistogramSeries(ctx, nil, e2eStartMS, e2eEndMS, matchers, withSamples)
				if err != nil {
					test.Fatal(err)
				}
				snapshot := func(series []*seriesMeta) map[string][]samplePoint {
					result := make(map[string][]samplePoint, len(series))
					for _, meta := range series {
						result[meta.labels.String()] = meta.samples
					}
					return result
				}
				if !reflect.DeepEqual(snapshot(got), snapshot(want)) {
					test.Fatalf("%s samples=%v: combined metadata differs from general path", suffix, withSamples)
				}
			}
		}
	})
	test.Run("compact range matches engine", func(test *testing.T) {
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		teamCfg.PostHogCompactHistograms = true
		compactServer := newServer(teamCfg)
		start, end, step := time.UnixMilli(e2eStartMS), time.UnixMilli(e2eEndMS), 30*time.Second
		for _, expression := range []string{
			`histogram_quantiles("p", 0.5, 0.9, sum by(le)(irate(` + metric + `_bucket)))`,
			`histogram_quantile(0.5, sum by(le)(irate(` + metric + `_bucket[1m])))`,
		} {
			prepared, err := prepareMetricsQLQuery(expression, step, teamCfg.LookbackDelta, start, end)
			if err != nil {
				test.Fatal(err)
			}
			stats := &promRequestStats{}
			actual, handled, err := compactServer.tryCompactHistogramRange(withPromRequestStats(ctx, stats), prepared.query, start, end, step)
			if err != nil || !handled || stats.clickHouseQueries.Load() != 2 {
				test.Fatalf("compact query: handled=%v queries=%d error=%v", handled, stats.clickHouseQueries.Load(), err)
			}
			engineQuery, err := compactServer.engine.NewRangeQuery(ctx, compactServer.queryable, promql.NewPrometheusQueryOpts(false, teamCfg.LookbackDelta), prepared.query, start, end, step)
			if err != nil {
				test.Fatal(err)
			}
			response := engineQuery.Exec(ctx)
			if response.Err != nil {
				test.Fatal(response.Err)
			}
			expected := responseDataFromValue(metricsQLValue(response.Value))
			if !reflect.DeepEqual(responseDataFromValue(actual), expected) {
				test.Fatalf("compact output differs from engine: %v != %v", responseDataFromValue(actual), expected)
			}
			engineQuery.Close()
		}
		teamCfg.PostHogCompactHistograms = false
		disabled := newServer(teamCfg)
		if _, handled, err := disabled.tryCompactHistogramRange(ctx, "", start, end, step); handled || err != nil {
			test.Fatal("disabled path did not fall back")
		}
	})
	test.Run("packed binary trailing byte", func(test *testing.T) {
		const count uint64 = 0x0a00000000000000
		err := client.QueryRows(ctx, fmt.Sprintf("SELECT toUInt64(1), toInt64(1000), toFloat64(0), toUInt64(%d), formatRow('RowBinary', [toFloat64(1)], [toUInt64(0), toUInt64(%d)]), 'cumulative'", count, count), func(row clickHouseRow) error {
			var sample postHogHistogramSample
			if err := sample.scanPacked(row); err != nil {
				return err
			}
			if len(sample.counts) != 2 || sample.counts[1] != count {
				return fmt.Errorf("packed data lost its final byte: %v", sample.counts)
			}
			return nil
		})
		if err != nil {
			test.Fatal(err)
		}
	})
	test.Run("compact data guards", func(test *testing.T) {
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		teamCfg.PostHogCompactHistograms = true
		compactServer := newServer(teamCfg)
		start, end, step := time.UnixMilli(e2eStartMS), time.UnixMilli(e2eEndMS+30000), 30*time.Second
		for _, guard := range []string{"changing", "overlap"} {
			name := "guard_" + guard + "_duration"
			insertHistogram(name, "cumulative", "histogram", e2eTeamID)
			fingerprint := sqlString(name)
			bounds := "[0.25,1.0]"
			if guard == "overlap" {
				fingerprint = sqlString(name + "_copy")
				bounds = "[0.5,1.0]"
			}
			sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes) SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6)+INTERVAL 1 DAY, 'api', 'histogram', 'cumulative', 48, 40, %s, [8,12,20], true, map('region','resource'), map('region','metric','status','200')", tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(name), fingerprint, chTimeMillis(e2eEndMS+30000), bounds)
			if err := client.Exec(ctx, sql); err != nil {
				test.Fatal(err)
			}
			expression := `histogram_quantile(0.5,sum by(le)(irate(` + name + `_bucket)))`
			prepared, err := prepareMetricsQLQuery(expression, step, teamCfg.LookbackDelta, start, end)
			if err != nil {
				test.Fatal(err)
			}
			if _, handled, err := compactServer.tryCompactHistogramRange(ctx, prepared.query, start, end, step); handled || err != nil {
				test.Fatalf("%s guard: handled=%v error=%v", guard, handled, err)
			}
			result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {expression}, "start": {"1700000010"}, "end": {"1700000100"}, "step": {"30"}})
			if len(result.Result) != 1 || len(result.Result[0].Values) == 0 {
				test.Fatalf("%s fallback returned no data: %v", guard, result)
			}
		}
	})
	test.Run("combined metadata limits", func(test *testing.T) {
		for _, limit := range []string{"source", "series", "samples"} {
			limited := cfg
			limited.TeamID = e2eTeamID
			switch limit {
			case "source":
				limited.MaxSeries = 1
			case "series":
				limited.MaxSeries = 2
			case "samples":
				limited.MaxSamples = 1
			}
			querier := &CHQuerier{queryable: NewCHQueryable(client, limited)}
			_, err := querier.selectPostHogSeriesSamples(ctx, e2eStartMS, e2eEndMS, false, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric+"_bucket"))
			if err == nil || !strings.Contains(err.Error(), "limit exceeded") {
				test.Fatalf("%s limit: %v", limit, err)
			}
		}
	})
	test.Run("missing histogram needs only metadata", func(test *testing.T) {
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		querier := &CHQuerier{queryable: NewCHQueryable(client, teamCfg)}
		stats := &promRequestStats{}
		series, err := querier.selectPostHogSeriesSamples(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, false, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "missing_duration_bucket"))
		if err != nil || len(series) != 0 || stats.clickHouseQueries.Load() != 1 {
			test.Fatalf("missing histogram: series=%d queries=%d error=%v", len(series), stats.clickHouseQueries.Load(), err)
		}
	})
	test.Run("queries", func(test *testing.T) {
		assertValue(test, metric+`_bucket{le="0.5"}`, 6)
		assertValue(test, metric+`_bucket{le="1",region="resource"}`, 15)
		assertValue(test, metric+`_bucket{le="+Inf"}`, 30)
		assertValue(test, metric+"_count", 30)
		assertValue(test, metric+"_sum", 36)
		assertValue(test, "sum("+metric+"_count)", 30)
		assertValue(test, `sum({__name__=~"test_duration_seconds_(count|sum)"})`, 66)
		assertValue(test, "histogram_quantile(0.5, sum by (le) (rate("+metric+"_bucket[1m])))", 1)
		assertValue(test, "median("+metric+"_count, "+metric+"_sum)", 33)
		quantiles := query(test, `histogram_quantiles("phi", 0.2, 0.5, 0.9, sum by (le) (rate(`+metric+`_bucket[1m])))`)
		if len(quantiles.Result) != 3 {
			test.Fatalf("multiple quantiles = %v", quantiles)
		}
		for _, result := range quantiles.Result {
			want := map[string]string{"0.2": "0.5", "0.5": "1", "0.9": "1"}
			if expected, exists := want[result.Metric["phi"]]; !exists || sampleString(result.Value) != expected {
				test.Fatalf("unexpected quantile = %v", result)
			}
		}
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {metric + `_bucket{le="1"}`}, "start": {"1700000010"}, "end": {"1700000070"}, "step": {"30"}})
		if len(result.Result) != 1 || len(result.Result[0].Values) != 3 {
			test.Fatalf("range samples: %+v", result)
		}
		for index, point := range result.Result[0].Values {
			if got := sampleString(point); got != strconv.Itoa((index+1)*5) {
				test.Fatalf("range sample %d = %s", index, got)
			}
		}
	})
	test.Run("discovery", func(test *testing.T) {
		params := url.Values{"start": {"1700000010"}, "end": {"1700000070"}, "match[]": {`{__name__=~"test_duration_seconds.*"}`}}
		names := apiGet[[]string](test, api.URL, "/api/v1/label/__name__/values", params)
		for _, suffix := range postHogHistogramSuffixes {
			assertStringPresent(test, names, metric+suffix)
		}
		params.Set("match[]", metric+"_bucket")
		bounds := apiGet[[]string](test, api.URL, "/api/v1/label/le/values", params)
		if !reflect.DeepEqual(bounds, []string{"+Inf", "0.5", "1"}) {
			test.Fatalf("bounds = %v", bounds)
		}
		labelNames := apiGet[[]string](test, api.URL, "/api/v1/labels", params)
		assertStringPresent(test, labelNames, "le")
		params.Set("match[]", metric+`_bucket{le="1",region="resource"}`)
		series := apiGet[[]map[string]string](test, api.URL, "/api/v1/series", params)
		if len(series) != 1 || series[0]["le"] != "1" || series[0]["region"] != "resource" {
			test.Fatalf("series = %v", series)
		}
		values := apiGet[[]string](test, api.URL, "/api/v1/label/status/values", params)
		if !reflect.DeepEqual(values, []string{"200"}) {
			test.Fatalf("filtered attributes = %v", values)
		}
	})
	test.Run("remote read", func(test *testing.T) {
		response, err := server.withTeamID(e2eTeamID).remoteReadSamples(ctx, &prompb.ReadRequest{Queries: []*prompb.Query{{StartTimestampMs: e2eStartMS, EndTimestampMs: e2eEndMS, Matchers: []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: metric + "_bucket"}, {Type: prompb.LabelMatcher_EQ, Name: "le", Value: "1"}}}}})
		if err != nil {
			test.Fatal(err)
		}
		if len(response.Results[0].Timeseries) != 1 || len(response.Results[0].Timeseries[0].Samples) != 3 || response.Results[0].Timeseries[0].Samples[2].Value != 15 {
			test.Fatalf("remote read = %v", response)
		}
	})
	test.Run("real and virtual series coexist", func(test *testing.T) {
		insertReal(metric+"_bucket", e2eTeamID, e2eEndMS)
		compactCfg := cfg
		compactCfg.TeamID = e2eTeamID
		compactCfg.PostHogCompactHistograms = true
		compactServer := newServer(compactCfg)
		start, end, step := time.UnixMilli(e2eStartMS), time.UnixMilli(e2eEndMS), 30*time.Second
		prepared, err := prepareMetricsQLQuery(`histogram_quantile(0.5,sum by(le)(irate(`+metric+`_bucket)))`, step, compactCfg.LookbackDelta, start, end)
		if err != nil {
			test.Fatal(err)
		}
		if _, handled, err := compactServer.tryCompactHistogramRange(ctx, prepared.query, start, end, step); handled || err != nil {
			test.Fatalf("real metric did not fall back: %v %v", handled, err)
		}
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		querier := &CHQuerier{queryable: NewCHQueryable(client, teamCfg)}
		for _, withSamples := range []bool{false, true} {
			stats := &promRequestStats{}
			matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric+"_bucket"), labels.MustNewMatcher(labels.MatchEqual, "le", "0.5")}
			var series []*seriesMeta
			var err error
			if withSamples {
				series, err = querier.selectPostHogSeriesSamples(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, true, matchers...)
			} else {
				series, err = querier.selectPostHogSeries(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, matchers...)
			}
			if err != nil || len(series) != 2 {
				test.Fatalf("real and virtual samples=%v: series=%d queries=%d error=%v", withSamples, len(series), stats.clickHouseQueries.Load(), err)
			}
			byService := map[string]*seriesMeta{}
			for _, meta := range series {
				byService[meta.labelMap["service_name"]] = meta
			}
			real, virtual := byService["other"], byService["api"]
			if real == nil || virtual == nil {
				test.Fatalf("real and virtual samples=%v: %+v", withSamples, series)
			}
			if withSamples && (len(real.samples) != 1 || real.samples[0].v != 777 || len(virtual.samples) == 0 || virtual.samples[len(virtual.samples)-1].v != 6) {
				test.Fatalf("real and virtual latest samples: real=%+v virtual=%+v", real.samples, virtual.samples)
			}
		}
		assertValue(test, metric+`_bucket{service_name="other"}`, 777)
		assertValue(test, metric+`_bucket{service_name="api",le="0.5"}`, 6)
		assertValue(test, "sum("+metric+"_bucket)", 777+6+15+30)
		assertValue(test, metric+"_count", 30)
		assertValue(test, metric+"_sum", 36)
		assertValue(test, `sum({__name__=~"test_duration_seconds_(bucket|count)"})`, 777+6+15+30+30)
		insertReal(metric+"_sum", e2eTeamID, e2eStartMS-3600000)
		assertValue(test, metric+"_sum", 36)
	})
	test.Run("mixed native and plain rows merge", func(test *testing.T) {
		const name = "test_mixed_seconds"
		base := map[string]string{"region": "metric", "status": "200"}
		bucket := func(le string) map[string]string {
			attributes := maps.Clone(base)
			attributes["le"] = le
			return attributes
		}
		native := func(timestamp int64, sum float64, count uint64, counts string) {
			test.Helper()
			sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
				SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'api', 'histogram', 'cumulative', %v, %d, [0.5, 1.0], %s, true, map('region', 'resource'), map('region', 'metric', 'status', '200')`,
				tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(name), sqlString(name), chTimeMillis(timestamp), sum, count, counts)
			if err := client.Exec(ctx, sql); err != nil {
				test.Fatal(err)
			}
		}
		t1, t2, t3 := e2eStartMS, e2eStartMS+30000, e2eStartMS+60000
		native(t1, 12, 10, "[2, 3, 5]")
		insertPlain(name+"_bucket", t2, 4, bucket("0.5"))
		insertPlain(name+"_bucket", t2, 10, bucket("1"))
		insertPlain(name+"_bucket", t2, 20, bucket("+Inf"))
		insertPlain(name+"_count", t2, 20, base)
		insertPlain(name+"_sum", t2, 24, base)
		native(t3, 36, 30, "[6, 9, 15]")
		rangeQuery := func(expression string) queryDataDTO {
			test.Helper()
			return apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {expression}, "start": {"1700000010"}, "end": {"1700000070"}, "step": {"30"}})
		}
		for _, testcase := range []struct {
			expression string
			values     []string
		}{
			{name + `_bucket{le="0.5"}`, []string{"2", "4", "6"}},
			{name + `_bucket{le="1"}`, []string{"5", "10", "15"}},
			{name + `_bucket{le="+Inf"}`, []string{"10", "20", "30"}},
			{name + "_count", []string{"10", "20", "30"}},
			{name + "_sum", []string{"12", "24", "36"}},
		} {
			result := rangeQuery(testcase.expression)
			if len(result.Result) != 1 || len(result.Result[0].Values) != len(testcase.values) {
				test.Fatalf("%s: %+v", testcase.expression, result)
			}
			for index, point := range result.Result[0].Values {
				if got := sampleString(point); got != testcase.values[index] {
					test.Fatalf("%s sample %d = %s, want %s", testcase.expression, index, got, testcase.values[index])
				}
			}
		}
		increase := rangeQuery("sum by (le) (increase(" + name + "_bucket[1m]))")
		if len(increase.Result) != 3 {
			test.Fatalf("increase by le = %+v", increase)
		}
		for _, series := range increase.Result {
			last := sampleString(series.Values[len(series.Values)-1])
			want := map[string]string{"0.5": "4", "1": "10", "+Inf": "20"}[series.Metric["le"]]
			if last != want {
				test.Fatalf("increase le=%s = %s, want %s", series.Metric["le"], last, want)
			}
		}
		assertValue(test, "histogram_quantile(0.5, sum by (le) (rate("+name+"_bucket[1m])))", 1)
		params := url.Values{"start": {"1700000010"}, "end": {"1700000070"}, "match[]": {name + "_bucket"}}
		bounds := apiGet[[]string](test, api.URL, "/api/v1/label/le/values", params)
		if !reflect.DeepEqual(bounds, []string{"+Inf", "0.5", "1"}) {
			test.Fatalf("bounds = %v", bounds)
		}
		series := apiGet[[]map[string]string](test, api.URL, "/api/v1/series", params)
		if len(series) != 3 {
			test.Fatalf("series = %v", series)
		}
		names := apiGet[[]string](test, api.URL, "/api/v1/label/__name__/values", url.Values{"start": {"1700000010"}, "end": {"1700000070"}, "match[]": {`{__name__=~"test_mixed_seconds.*"}`}})
		seen := make(map[string]int, len(names))
		for _, value := range names {
			seen[value]++
		}
		for _, suffix := range postHogHistogramSuffixes {
			if seen[name+suffix] != 1 {
				test.Fatalf("names must list %s once: %v", name+suffix, names)
			}
		}
	})
	test.Run("exponential count and sum", func(test *testing.T) {
		insertHistogram("test_exponential_seconds", "cumulative", "exponential_histogram", e2eTeamID)
		assertValue(test, "test_exponential_seconds_count", 30)
		assertValue(test, "test_exponential_seconds_sum", 36)
		if result := query(test, "test_exponential_seconds_bucket"); len(result.Result) != 0 {
			test.Fatalf("incomplete exponential bounds must not produce buckets: %v", result)
		}
	})
	test.Run("delta rejection", func(test *testing.T) {
		insertHistogram("test_delta_seconds", "delta", "histogram", e2eTeamID)
		response, err := http.Get(api.URL + "/t/42/api/v1/query?" + url.Values{"query": {"test_delta_seconds_count"}, "time": {"1700000070"}}.Encode())
		if err != nil {
			test.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(string(body), "require cumulative") {
			test.Fatalf("delta query: status %d: %s", response.StatusCode, body)
		}
	})
	test.Run("invalid histograms fail only exact selectors", func(test *testing.T) {
		result := query(test, `{__name__=~"test_.*_seconds_count"}`)
		names := make([]string, 0, len(result.Result))
		for _, series := range result.Result {
			names = append(names, series.Metric[labels.MetricName])
		}
		sort.Strings(names)
		if want := []string{"test_duration_seconds_count", "test_exponential_seconds_count", "test_mixed_seconds_count"}; !reflect.DeepEqual(names, want) {
			test.Fatalf("broad selector over a delta histogram = %v, want %v", names, want)
		}
	})
	test.Run("large fingerprint sets use an external table", func(test *testing.T) {
		teamCfg := cfg
		teamCfg.TeamID = e2eTeamID
		chunkedCfg := teamCfg
		chunkedCfg.IDChunkSize = 1
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "test_.*_seconds_(count|sum)")}
		want, err := (&CHQuerier{queryable: NewCHQueryable(client, teamCfg)}).selectPostHogSeriesSamples(ctx, e2eStartMS, e2eEndMS, false, matchers...)
		if err != nil || len(want) < 2 {
			test.Fatalf("series = %d, %v", len(want), err)
		}
		stats := &promRequestStats{}
		got, err := (&CHQuerier{queryable: NewCHQueryable(client, chunkedCfg)}).selectPostHogSeriesSamples(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, false, matchers...)
		if err != nil {
			test.Fatal(err)
		}
		snapshot := func(series []*seriesMeta) map[string][]samplePoint {
			result := make(map[string][]samplePoint, len(series))
			for _, meta := range series {
				result[meta.labels.String()] = meta.samples
			}
			return result
		}
		if !reflect.DeepEqual(snapshot(got), snapshot(want)) {
			test.Fatalf("external table read differs from chunked read")
		}
		if count := stats.clickHouseQueries.Load(); count != 5 {
			test.Fatalf("used %d queries, want series, one stored sample read, aliases, histogram series and one external sample read", count)
		}
	})
	test.Run("fast path checks for stored histograms", func(test *testing.T) {
		teamServer := server.withTeamID(e2eTeamID)
		for _, testcase := range []struct {
			matcher *labels.Matcher
			virtual bool
		}{
			{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "requests_total_count"), false},
			{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric+"_count"), true},
			{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_exponential_seconds_bucket"), false},
			{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_exponential_seconds_sum"), true},
			{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "requests_.*"), true},
		} {
			virtual, err := teamServer.postHogSelectsHistogram(ctx, e2eStartMS, e2eEndMS, []*labels.Matcher{testcase.matcher})
			if err != nil || virtual != testcase.virtual {
				test.Fatalf("%s selects histogram = %v (%v), want %v", testcase.matcher, virtual, err, testcase.virtual)
			}
		}
	})
}
