package snuffle

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
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
	loadE2ESchema(test, ctx, client, filepath.Join(repoRoot(test), "scripts", "create_metrics_posthog_schema.sql"))
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
	test.Run("legacy tables", func(test *testing.T) {
		legacy := cfg
		legacy.SeriesTable = "metric_series2"
		legacy.AttributeTable = "metric_attributes2"
		legacy.AttributeTableHasMetricName = false
		legacy.MetricNamesTable = ""
		legacyMux := http.NewServeMux()
		newServer(legacy).routes(legacyMux)
		legacyAPI := httptest.NewServer(legacyMux)
		defer legacyAPI.Close()
		result := apiGet[queryDataDTO](test, legacyAPI.URL, "/api/v1/query", url.Values{"query": {metric + `_bucket{le="1"}`}, "time": {"1700000070"}})
		if len(result.Result) != 1 || sampleString(result.Result[0].Value) != "15" {
			test.Fatalf("legacy histogram query = %v", result)
		}
		params := url.Values{"start": {"1700000010"}, "end": {"1700000070"}, "match[]": {metric + "_bucket"}}
		names := apiGet[[]string](test, legacyAPI.URL, "/api/v1/label/__name__/values", params)
		assertStringPresent(test, names, metric+"_bucket")
	})
	test.Run("real names take priority", func(test *testing.T) {
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
			wantQueries := int64(1)
			if withSamples {
				series, err = querier.selectPostHogSeriesSamples(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, true, matchers...)
				wantQueries = 2
			} else {
				series, err = querier.selectPostHogSeries(withPromRequestStats(ctx, stats), e2eStartMS, e2eEndMS, matchers...)
			}
			if err != nil || len(series) != 1 || stats.clickHouseQueries.Load() != wantQueries {
				test.Fatalf("real metric samples=%v: series=%d queries=%d error=%v", withSamples, len(series), stats.clickHouseQueries.Load(), err)
			}
			if series[0].labelMap["service_name"] != "other" || (withSamples && (len(series[0].samples) != 1 || series[0].samples[0].v != 777)) {
				test.Fatalf("real metric samples=%v: %+v", withSamples, series[0])
			}
		}
		assertValue(test, metric+"_bucket", 777)
		assertValue(test, "sum("+metric+"_bucket)", 777)
		if result := query(test, metric+`_bucket{service_name="api"}`); len(result.Result) != 0 {
			test.Fatalf("real name must suppress the virtual metric across label filters: %v", result)
		}
		assertValue(test, metric+"_count", 30)
		assertValue(test, metric+"_sum", 36)
		assertValue(test, `sum({__name__=~"test_duration_seconds_(bucket|count)"})`, 807)
		insertReal(metric+"_sum", e2eTeamID, e2eStartMS-3600000)
		if result := query(test, metric+"_sum"); len(result.Result) != 0 {
			test.Fatalf("real name outside the query window must still take priority: %v", result)
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
}
