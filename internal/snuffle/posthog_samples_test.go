package snuffle

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func assertContainsAll(t *testing.T, sql string, wants []string, notWants []string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range notWants {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestPostHogSeriesFiltersUseHourBuckets(t *testing.T) {
	cfg := rangeTestConfig()
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}
	sql := postHogSelectedSeriesSQL(cfg, matchers, 1000, 7_200_000, cfg.MaxSeries, nil)
	assertContainsAll(t, sql, []string{
		"FROM `test`.`metrics4_series` WHERE team_id = 42",
		"time_bucket >= toStartOfHour(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"time_bucket <= toStartOfHour(fromUnixTimestamp64Milli(7200000, 'UTC'))",
		"attributes['status']) = '200'",
		"LIMIT 1 BY series_id LIMIT 100",
	}, []string{"last_seen"})
}

func TestPostHogLoadSamplesSQLCombinesPointArrays(t *testing.T) {
	cfg := rangeTestConfig()
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "service_name", "checkout"),
		labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
	}
	sql := postHogLoadSamplesSQL(cfg, []uint64{7, 9}, []string{"http_requests_total"}, matchers, 1000, 2000, false)
	assertContainsAll(t, sql, []string{
		"SELECT series_id, tupleElement(points[1], 1) AS first_ts, arrayDifference(arrayMap(p -> p.1, points)) AS ts_deltas, arrayMap(p -> p.2, points) AS vals",
		"arraySort(groupArrayArray(arrayFilter(p -> p.1 >= 1000 AND p.1 <= 2000, arrayZip(arrayMap(t -> toUnixTimestamp64Milli(t), timestamp_arr), value_arr)))) AS points FROM `test`.`metrics4_samples` WHERE",
		"time_bucket >= toStartOfHour(fromUnixTimestamp64Milli(1000, 'UTC'))",
		"time_bucket <= toStartOfHour(fromUnixTimestamp64Milli(2000, 'UTC'))",
		"metric_name = 'http_requests_total'",
		"service_name = 'checkout'",
		"series_fingerprint IN (7,9)",
		"GROUP BY series_id HAVING notEmpty(points)",
		"SETTINGS max_threads = 4",
	}, []string{"ARRAY JOIN", "timestamp >=", "'status'", "metrics4_series"})

	sql = postHogLoadSamplesSQL(cfg, []uint64{7}, []string{"http_requests_total"}, matchers, 1000, 2000, true)
	assertContainsAll(t, sql, []string{
		"FROM `test`.`metrics4_samples` ARRAY JOIN timestamp_arr AS timestamp, value_arr AS value WHERE",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"argMax(value, timestamp)",
		nonStaleSampleSQL("value"),
	}, []string{"groupArrayArray", "count_arr"})
}

func TestPostHogHistogramSQLReadsPointArrays(t *testing.T) {
	cfg := rangeTestConfig()
	aliases := []postHogHistogramAlias{{baseName: "test_duration_seconds", suffix: "_bucket"}}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_duration_seconds_bucket")}

	sql := postHogHistogramSamplesSQL(cfg, 1000, 2000, []uint64{7}, aliases, matchers)
	assertContainsAll(t, sql, []string{
		"formatRow('RowBinary', histogram_bounds, histogram_counts) AS histogram_arrays, aggregation_temporality FROM `test`.`metrics4_samples` ARRAY JOIN timestamp_arr AS timestamp, value_arr AS value, count_arr AS count, histogram_counts_arr AS histogram_counts WHERE",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"metric_type IN ('histogram', 'exponential_histogram')",
		"series_fingerprint IN (7)",
		"ORDER BY series_id, timestamp",
	}, nil)

	sql = postHogHistogramBoundsSQL(cfg, 1000, 2000, "series_fingerprint IN (7)", aliases, matchers)
	assertContainsAll(t, sql, []string{
		"SELECT DISTINCT series_fingerprint AS series_id",
		"FROM `test`.`metrics4_samples` WHERE",
		"arrayExists(t -> t >= fromUnixTimestamp64Milli(1000, 'UTC') AND t <= fromUnixTimestamp64Milli(2000, 'UTC'), timestamp_arr)",
	}, []string{"ARRAY JOIN", "timestamp >="})
}

func TestPostHogRangeRollupSQLCombinesPointArrays(t *testing.T) {
	cfg := rangeTestConfig()
	step := time.Minute
	for _, testcase := range []struct {
		query    string
		nonStale bool
	}{
		{`sum(increase(http_requests_total[1m])) by (code)`, true},
		{`avg(http_requests_total)`, false},
	} {
		aggregate := preparedRangeAggregate(t, testcase.query, step)
		call, ok := parseRangeRollupCall(aggregate.Expr, step, cfg.LookbackDelta)
		if !ok {
			t.Fatalf("%s: not accepted", testcase.query)
		}
		aggSQL, _ := aggregateSQL(aggregate, "rollup_value")
		start := int64(1_700_000_000_000)
		plan := newPostHogQueryPlan(cfg, call.selector.LabelMatchers, aggregate.Grouping, start-call.matrix, start+3_600_000, len(aggregate.Grouping) > 0)
		sql := rangeRollupSQL(cfg, plan, call, start, start, 61, aggSQL, aggregate.Op == parser.SUM && call.windowTotal())
		points := postHogPointsSQL(start-call.matrix, start+3_600_000, testcase.nonStale)
		wants := []string{
			"SELECT series_fingerprint AS series_id, " + points + " AS pts_part FROM `test`.`metrics4_samples` WHERE team_id = 42",
			"time_bucket >= toStartOfHour(fromUnixTimestamp64Milli(" + strconv.FormatInt(start-call.matrix, 10) + ", 'UTC'))",
			"arraySort(x -> x.1, groupArrayArray(pts_part)) AS pts",
			"GROUP BY series_id HAVING notEmpty(pts)",
			"ARRAY JOIN range(step_lo, step_hi) AS idx",
			postHogTimeRangeHint(start-call.matrix, start+3_600_000),
		}
		notWants := []string{"groupArray((ts, v))", "timestamp >=", " ARRAY JOIN timestamp_arr"}
		if testcase.nonStale {
			wants = append(wants, nonStaleSampleSQL("p.2"), "`test`.`metrics4_series`", "time_bucket <= toStartOfHour(fromUnixTimestamp64Milli(1700003600000, 'UTC'))")
			notWants = append(notWants, "last_seen", nonStaleSampleSQL("value"))
		} else {
			notWants = append(notWants, nonStaleSampleSQL("p.2"))
		}
		assertContainsAll(t, sql, wants, notWants)
	}
}

func TestPostHogSampleReadsCarryTimeRangeIndexHint(t *testing.T) {
	cfg := rangeTestConfig()
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total")}
	hint := "indexHint(arrayMin(timestamp_arr) <= fromUnixTimestamp64Milli(2000, 'UTC') AND arrayMax(timestamp_arr) >= fromUnixTimestamp64Milli(1000, 'UTC'))"
	aliases := []postHogHistogramAlias{{baseName: "http_requests", suffix: "_bucket"}}
	for name, sql := range map[string]string{
		"range read":       postHogLoadSamplesSQL(cfg, []uint64{7}, nil, matchers, 1000, 2000, false),
		"latest read":      postHogLoadSamplesSQL(cfg, []uint64{7}, nil, matchers, 1000, 2000, true),
		"histogram read":   postHogHistogramSamplesSQL(cfg, 1000, 2000, []uint64{7}, aliases, nil),
		"histogram bounds": postHogHistogramBoundsSQL(cfg, 1000, 2000, "series_fingerprint IN (7)", aliases, nil),
	} {
		if strings.Count(sql, hint) != 1 {
			t.Fatalf("%s must carry the time range hint once:\n%s", name, sql)
		}
	}
}
