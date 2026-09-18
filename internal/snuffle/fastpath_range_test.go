package snuffle

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func rangeTestConfig() Config {
	return Config{
		SchemaLayout:      "posthog",
		CHDatabase:        "test",
		SamplesTable:      "metrics2",
		SeriesTable:       "metric_series3",
		MaxSeries:         100,
		TeamID:            42,
		LookbackDelta:     5 * time.Minute,
		RangePushdown:     true,
		RangeQueryThreads: 4,
	}
}

// preparedRangeAggregate rewrites a query as handleQueryRange does and returns
// the aggregate the pushdown inspects.
func preparedRangeAggregate(t *testing.T, query string, step time.Duration) *parser.AggregateExpr {
	t.Helper()
	start := time.UnixMilli(1_700_000_000_000)
	prepared, err := prepareMetricsQLQuery(query, step, 5*time.Minute, start, start.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
	if err != nil {
		t.Fatalf("parse %q: %v", prepared.query, err)
	}
	aggregate, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("%q is not an aggregate: %T", prepared.query, expr)
	}
	return aggregate
}

func TestParseRangeRollupCallAcceptsCountersAndSelectors(t *testing.T) {
	for _, tc := range []struct {
		query  string
		name   string
		window int64
		matrix int64
	}{
		{`sum(increase(http_requests_total[1m])) by (code)`, "increase", 60_000, 360_000},
		{`max(rate(http_requests_total{job="api"}[5m]))`, "rate", 300_000, 600_000},
		{`avg(irate(http_requests_total[30s])) by (code)`, "irate", 30_000, 330_000},
		{`min(delta(http_requests_total[2m]))`, "delta", 120_000, 420_000},
		{`count(idelta(http_requests_total[1m]))`, "idelta", 60_000, 360_000},
		{`sum(increase(http_requests_total))`, "increase", 60_000, 360_000},
		{`sum(http_requests_total) by (code)`, "default_rollup", 0, 360_000},
	} {
		aggregate := preparedRangeAggregate(t, tc.query, time.Minute)
		call, ok := parseRangeRollupCall(aggregate.Expr, time.Minute, 5*time.Minute)
		if !ok {
			t.Fatalf("%s: not accepted", tc.query)
		}
		if call.name != tc.name || call.window != tc.window || call.matrix != tc.matrix || call.step != 60_000 || call.lookback != 300_000 {
			t.Fatalf("%s: call = %+v", tc.query, call)
		}
		if call.selector == nil || call.selector.Name != "http_requests_total" {
			t.Fatalf("%s: selector = %v", tc.query, call.selector)
		}
	}
}

func TestParseRangeRollupCallRejectsUnsupportedShapes(t *testing.T) {
	for _, query := range []string{
		// rate without a window sizes it per step from the sample interval.
		`sum(rate(http_requests_total))`,
		`sum(increase(http_requests_total[1m] offset 5m))`,
		`sum(increase(http_requests_total[1m] @ 1700000000))`,
		`sum(increase(http_requests_total[1m]) * 2)`,
		`sum(abs(http_requests_total))`,
		`sum(sum_over_time(http_requests_total[1m]))`,
	} {
		aggregate := preparedRangeAggregate(t, query, time.Minute)
		if _, ok := parseRangeRollupCall(aggregate.Expr, time.Minute, 5*time.Minute); ok {
			t.Fatalf("%s: accepted", query)
		}
	}
	// The step and lookback must be the ones the request was rewritten with.
	aggregate := preparedRangeAggregate(t, `sum(increase(http_requests_total[1m]))`, time.Minute)
	if _, ok := parseRangeRollupCall(aggregate.Expr, 30*time.Second, 5*time.Minute); ok {
		t.Fatal("accepted a call rewritten for another step")
	}
	if _, ok := parseRangeRollupCall(aggregate.Expr, time.Minute, time.Minute); ok {
		t.Fatal("accepted a call rewritten for another lookback")
	}
}

func TestRangeRollupCallExpansion(t *testing.T) {
	counter := rangeRollupCall{name: "rate", window: 300_000, step: 60_000, lookback: 300_000, matrix: 600_000}
	if got := counter.expansion(); got != 6 {
		t.Fatalf("rate[5m] at 1m expands to %d steps, want 6", got)
	}
	selector := rangeRollupCall{name: "default_rollup", step: 60_000, lookback: 300_000, matrix: 360_000}
	if got := selector.expansion(); got != 7 {
		t.Fatalf("selector at 1m expands to %d steps, want 7", got)
	}
}

func rangeTestSQL(t *testing.T, query string, step time.Duration) string {
	t.Helper()
	cfg := rangeTestConfig()
	aggregate := preparedRangeAggregate(t, query, step)
	call, ok := parseRangeRollupCall(aggregate.Expr, step, cfg.LookbackDelta)
	if !ok {
		t.Fatalf("%s: not accepted", query)
	}
	aggSQL, ok := aggregateSQL(aggregate, "rollup_value")
	if !ok {
		t.Fatalf("%s: aggregate not supported", query)
	}
	start := int64(1_700_000_000_000)
	plan := newPostHogQueryPlan(cfg, call.selector.LabelMatchers, aggregate.Grouping, start-call.matrix, start+3_600_000, len(aggregate.Grouping) > 0)
	sum := aggregate.Op == parser.SUM && call.windowTotal()
	return rangeRollupSQL(cfg, plan, call, start, 61, aggSQL, sum)
}

func TestRangeRollupSQLSumsIncreaseContributionsDirectly(t *testing.T) {
	sql := rangeTestSQL(t, `sum(increase(http_requests_total[1m])) by (code)`, time.Minute)
	for _, want := range []string{
		"WITH selected_series AS (",
		"`test`.`metric_series3`",
		"last_seen >= fromUnixTimestamp64Milli(1699999640000, 'UTC')",
		"metric_name = 'http_requests_total'",
		"reinterpretAsUInt64(value) != 9218868437227405314",
		"arraySort(x -> x.1, groupArray((ts, v))) AS pts",
		"quantileExactInclusiveOrDefault(0.6)",
		"toInt64(60000) AS window_ms",
		"ARRAY JOIN range(step_lo, step_hi) AS idx",
		"least(toInt64(ceil((ts - 1700000000000 + window_ms) / 60000)) + 1, 62)",
		"if(v >= p_v, v - p_v, if((p_v - v) * 8 < p_v, 0, v)) AS c_diff",
		"toUInt8(p_ts > t_i - 360000) AS has_prev",
		"sum(contribution) AS value",
		"INNER JOIN selected_series USING series_id",
		"GROUP BY `__group_0`, idx ORDER BY `__group_0`, idx",
		"SETTINGS short_circuit_function_evaluation = 'disable', max_threads = 4",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"rollup_value", "max_prev)", "HAVING"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestRangeRollupSQLRatePerSeries(t *testing.T) {
	sql := rangeTestSQL(t, `max(rate(http_requests_total{job="api"}[5m])) by (code)`, time.Minute)
	for _, want := range []string{
		"toUInt8(p_ts > t_i - 600000 AND p_ts > t_i - window_ms - max_prev) AS has_prev",
		"if(count() + max(first_has_prev) < 2 OR sum(gap_contribution) <= 0, NULL, sum(contribution) / (sum(gap_contribution) / 1000)) AS rollup_value",
		"GROUP BY series_id, idx HAVING isNotNull(rollup_value) AND NOT isNaN(rollup_value)",
		"max(rollup_value) AS value",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "abs(v) < 10") {
		t.Fatalf("rate must start from the first sample, not the zero baseline heuristic:\n%s", sql)
	}
}

func TestRangeRollupSQLSelectorKeepsStaleMarkersAndAutoWindow(t *testing.T) {
	sql := rangeTestSQL(t, `avg(http_requests_total)`, 30*time.Second)
	for _, want := range []string{
		"toInt64(least(greatest(30000, max_prev), 300000)) AS window_ms",
		"least(330000, ifNull(next_ts - ts, 330000))",
		"any(if(t_i - ts < window_ms AND reinterpretAsUInt64(v) != 9218868437227405314, v, NULL)) AS rollup_value",
		"avg(rollup_value) AS value",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "reinterpretAsUInt64(value)") {
		t.Fatalf("the selector scan must keep stale markers so they can end a series:\n%s", sql)
	}
	if strings.Contains(sql, "selected_series") {
		t.Fatalf("a bare metric name without grouping must not read the series table:\n%s", sql)
	}
}

func TestRangeRollupValueSQLPerFunction(t *testing.T) {
	if got := rangeRollupValueSQL("increase"); got != "sum(contribution)" {
		t.Fatalf("increase = %q", got)
	}
	if got := rangeRollupValueSQL("irate"); !strings.Contains(got, "argMax(c_diff, ts) / (argMax(gap, ts) / 1000)") {
		t.Fatalf("irate = %q", got)
	}
	if got := rangeRollupValueSQL("idelta"); got != "if(count() >= 2 OR max(first_has_prev) = 1, argMax(c_diff, ts), argMax(v, ts))" {
		t.Fatalf("idelta = %q", got)
	}
}

func TestMetricsQLSampleIntervalMarginSQLThresholds(t *testing.T) {
	sql := metricsQLSampleIntervalMarginSQL("i")
	for _, want := range []string{"i <= 2000, i * 5", "i <= 4000, i * 3", "i <= 8000, i * 2", "i <= 16000, i + intDiv(i, 2)", "i <= 32000, i + intDiv(i, 4)", "i + intDiv(i, 8))"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("margin SQL %q lacks %q", sql, want)
		}
	}
}

func TestDecodePackedSamples(t *testing.T) {
	payload := binary.AppendUvarint(nil, 3)
	for _, ts := range []int64{1000, 2000, 3000} {
		payload = binary.LittleEndian.AppendUint64(payload, uint64(ts))
	}
	payload = binary.AppendUvarint(payload, 3)
	for _, v := range []float64{1.5, math.NaN(), 3} {
		payload = binary.LittleEndian.AppendUint64(payload, math.Float64bits(v))
	}
	samples, err := decodePackedSamples(payload, []samplePoint{{t: 500, v: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 4 || samples[0].t != 500 || samples[1].t != 1000 || samples[1].v != 1.5 || !math.IsNaN(samples[2].v) || samples[3].t != 3000 || samples[3].v != 3 {
		t.Fatalf("samples = %+v", samples)
	}
	if _, err := decodePackedSamples(payload[:len(payload)-8], nil); err == nil {
		t.Fatal("truncated payload decoded")
	}
	if _, err := decodePackedSamples(append(payload, 0), nil); err == nil {
		t.Fatal("trailing bytes decoded")
	}
}

func TestFloatSeriesIteratorSortsAndSeeks(t *testing.T) {
	meta := &seriesMeta{samples: []samplePoint{{t: 3000, v: 3}, {t: 1000, v: 1}, {t: 2000, v: 2}}}
	it := meta.Iterator(nil)
	if _, ok := it.(*floatSampleIterator); !ok {
		t.Fatalf("iterator = %T, want floatSampleIterator", it)
	}
	if meta.samples[0].t != 1000 || meta.samples[2].t != 3000 {
		t.Fatalf("samples not sorted in place: %+v", meta.samples)
	}
	if typ := it.Seek(1500); typ != chunkenc.ValFloat {
		t.Fatalf("Seek(1500) = %v", typ)
	}
	if ts, v := it.At(); ts != 2000 || v != 2 {
		t.Fatalf("At() = %d, %v", ts, v)
	}
	if typ := it.Seek(1500); typ != chunkenc.ValFloat || it.AtT() != 2000 {
		t.Fatalf("Seek backwards moved the iterator: %v %d", typ, it.AtT())
	}
	if typ := it.Next(); typ != chunkenc.ValFloat || it.AtT() != 3000 {
		t.Fatalf("Next() = %v at %d", typ, it.AtT())
	}
	if typ := it.Next(); typ != chunkenc.ValNone {
		t.Fatalf("Next() past the end = %v", typ)
	}
	if typ := it.Seek(5000); typ != chunkenc.ValNone {
		t.Fatalf("Seek past the end = %v", typ)
	}
	if _, h := it.AtHistogram(nil); h != nil {
		t.Fatal("float iterator returned a histogram")
	}
}
