package snuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
)

// TestPostHogHistogramLayoutChange stores a histogram whose bucket layout
// changes mid-range (10 bounds, then 12) and one row with 10 bounds and 12
// bucket counts (ENG-045). The samples table keeps one bound list per series
// and hour, so a row of a new layout written in an hour of the old one reads
// like that. Every histogram path must read each layout as its own le series,
// and skip, count and report the row that does not fit its bounds.
// histogramLayoutChangeVictoriaMetrics holds the VictoriaMetrics results for
// the other rows.
func TestPostHogHistogramLayoutChange(test *testing.T) {
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
	database := fmt.Sprintf("snuffle_histogram_layout_%d", time.Now().UnixNano())
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

	const (
		rows     = 19
		interval = int64(600_000)
	)
	base := e2eStartMS
	insertRow := func(name, temporality string, timestamp int64, bounds []float64, counts []uint64) {
		test.Helper()
		var total uint64
		for _, count := range counts {
			total += count
		}
		sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
			SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'cdp', 'histogram', %s, %v, %d, %s, %s, true, map(), map()`,
			tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(name), sqlString(name), chTimeMillis(timestamp), sqlString(temporality),
			float64(total)/2, total, clickHouseFloatArray(bounds), clickHouseUintArray(counts))
		if err := client.Exec(ctx, sql); err != nil {
			test.Fatal(err)
		}
	}
	// 10 bounds up to 23:53 UTC, then 12 bounds from the next hour on, after a
	// deploy that resets the counters.
	for _, temporality := range []string{"cumulative", "delta"} {
		name := "layout_" + temporality
		for i := 0; i < rows; i++ {
			buckets := 10
			if i >= 11 {
				buckets = 12
			}
			bounds := make([]float64, buckets)
			counts := make([]uint64, buckets+1)
			for k := range counts {
				if k < buckets {
					bounds[k] = float64(k + 1)
				}
				switch {
				case temporality == "delta" && i < 11:
					counts[k] = uint64((i + k) % 4)
				case temporality == "delta":
					counts[k] = uint64((i*2 + k) % 5)
				case i < 11:
					counts[k] = uint64((i + 1) * (k%3 + 1))
				default:
					counts[k] = uint64((i - 10) * (k%4 + 1))
				}
			}
			insertRow(name, temporality, base+int64(i)*interval, bounds, counts)
		}
		// The row of the report: 10 bounds but 12 bucket counts, at 22:58 UTC.
		malformed := make([]uint64, 12)
		for k := range malformed {
			malformed[k] = 1000
		}
		insertRow(name, temporality, base+4*interval+interval/2, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, malformed)
	}

	type path struct {
		name              string
		compact, pushdown bool
	}
	seconds := func(ms int64) string { return strconv.FormatInt(ms/1000, 10) }
	end := base + (rows-1)*interval
	// The late range reads no window that holds the malformed row, so the
	// pushdown evaluates it in ClickHouse.
	ranges := []struct {
		name       string
		start      int64
		malformed  bool
		wantPushed bool
	}{
		{"from the first full window", base + 6*interval, true, false},
		{"after the malformed row", base + 12*interval, false, true},
	}
	for _, p := range []path{{"general", false, false}, {"compact", true, false}, {"pushdown", true, true}} {
		test.Run(p.name, func(test *testing.T) {
			serverCfg := cfg
			serverCfg.PostHogCompactHistograms = p.compact
			serverCfg.PostHogHistogramPushdown = p.pushdown
			server := newServer(serverCfg)
			mux := http.NewServeMux()
			server.routes(mux)
			api := httptest.NewServer(mux)
			defer api.Close()
			for _, r := range ranges {
				for query, expected := range histogramLayoutChangeVictoriaMetrics {
					values := url.Values{"query": {query}, "start": {seconds(r.start)}, "end": {seconds(end)}, "step": {"600"}}
					skippedBefore := skippedRowsValue(test, server.metrics)
					got, warnings := queryRangeWithWarnings(test, api.URL, values)
					assertVictoriaMetricsRange(test, r.name+": "+query, got, expected, r.start)
					skipped := skippedRowsValue(test, server.metrics) - skippedBefore
					warned := strings.Contains(strings.Join(warnings, "\n"), "skipped 1 malformed histogram row")
					if r.malformed && (skipped != 1 || !warned) {
						test.Fatalf("%s %s: skipped metric +%v, warnings %q; want one skipped row reported", r.name, query, skipped, warnings)
					}
					if !r.malformed && (skipped != 0 || len(warnings) != 0) {
						test.Fatalf("%s %s: skipped metric +%v, warnings %q; want none", r.name, query, skipped, warnings)
					}
				}
				if p.pushdown {
					query := "sum by (le) (increase(layout_delta_bucket[1h]))"
					step := 10 * time.Minute
					prepared, err := prepareMetricsQLQuery(query, step, serverCfg.LookbackDelta, time.UnixMilli(r.start), time.UnixMilli(end))
					if err != nil {
						test.Fatal(err)
					}
					teamCfg := serverCfg
					teamCfg.TeamID = e2eTeamID
					_, _, pushed, err := newServer(teamCfg).planHistogramPushdown(ctx, prepared.query, rangeEvalTimes(time.UnixMilli(r.start), time.UnixMilli(end), step))
					if err != nil || pushed != r.wantPushed {
						test.Fatalf("%s: pushed=%v error=%v, want pushed=%v", r.name, pushed, err, r.wantPushed)
					}
				}
			}
		})
	}
}

func skippedRowsValue(test *testing.T, metrics *bridgeMetrics) float64 {
	test.Helper()
	var metric dto.Metric
	if err := metrics.histogramRowsSkipped.Write(&metric); err != nil {
		test.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func clickHouseFloatArray(values []float64) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatFloat(value, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func clickHouseUintArray(values []uint64) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatUint(value, 10)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func queryRangeWithWarnings(test *testing.T, baseURL string, values url.Values) (queryDataDTO, []string) {
	test.Helper()
	response, err := http.Get(baseURL + e2eAPIPath("/api/v1/query_range") + "?" + values.Encode())
	if err != nil {
		test.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		test.Fatalf("%s: status %d: %s", values.Get("query"), response.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded struct {
		Data     queryDataDTO `json:"data"`
		Warnings []string     `json:"warnings"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		test.Fatalf("decode %s: %v", body, err)
	}
	return decoded.Data, decoded.Warnings
}

// histogramLayoutChangeVictoriaMetrics maps each query to its
// VictoriaMetrics result: le -> one value per 10m step from
// histogramLayoutChangeFirstStep to the last row, "_" where the bucket has no
// point. The values are instant queries at each step, because VictoriaMetrics
// range queries carry state across steps: they fill a bucket missing at a step
// from the other steps, and read the first point of a new bucket as an
// increase from zero. VictoriaMetrics reads a delta row as a gauge, so the
// delta queries ran as sum_over_time.
var histogramLayoutChangeVictoriaMetrics = map[string]map[string]string{
	"histogram_quantile(0.9, sum by (le) (increase(layout_cumulative_bucket[1h])))": {
		"": "9.9 9.9 9.9 9.9 9.9 12 12 12 12 12 11.475 11.475 11.475",
	},
	"histogram_quantile(0.9, sum by (le) (increase(layout_delta_bucket[1h])))": {
		"": "9.9 9.677777777777777 9.9 10 9.9 12 12 12 12 12 11.853846153846154 11.55 11.783333333333333",
	},
	"sum by (le) (increase(layout_cumulative_bucket[1h]))": {
		"1":    "6 6 6 6 6 6 6 6 6 6 6 6 6",
		"2":    "18 18 18 18 18 18 18 18 18 18 18 18 18",
		"3":    "36 36 36 36 36 36 36 36 36 36 36 36 36",
		"4":    "42 42 42 42 42 45 48 51 54 57 60 60 60",
		"5":    "54 54 54 54 54 56 58 60 62 64 66 66 66",
		"6":    "72 72 72 72 72 73 74 75 76 77 78 78 78",
		"7":    "78 78 78 78 78 81 84 87 90 93 96 96 96",
		"8":    "90 90 90 90 90 95 100 105 110 115 120 120 120",
		"9":    "108 108 108 108 108 111 114 117 120 123 126 126 126",
		"10":   "114 114 114 114 114 118 122 126 130 134 138 138 138",
		"11":   "_ _ _ _ _ 0 52 78 104 130 156 156 156",
		"12":   "_ _ _ _ _ 0 60 90 120 150 180 180 180",
		"+Inf": "126 126 126 126 126 136 146 156 166 176 186 186 186",
	},
	"sum by (le) (increase(layout_delta_bucket[1h]))": {
		"1":    "9 11 9 7 9 10 12 10 13 12 12 14 11",
		"2":    "20 20 16 16 20 22 21 21 27 25 25 24 23",
		"3":    "29 27 25 27 29 32 32 34 38 35 39 35 36",
		"4":    "36 36 36 36 36 39 40 44 46 46 49 47 50",
		"5":    "45 47 45 43 45 48 50 51 55 58 60 60 60",
		"6":    "56 56 52 52 56 59 62 64 70 71 72 74 71",
		"7":    "65 63 61 63 65 68 71 74 82 81 85 84 83",
		"8":    "72 72 72 72 72 79 82 86 91 92 99 95 96",
		"9":    "81 83 81 79 81 87 90 95 101 104 109 107 110",
		"10":   "92 92 88 88 92 97 100 105 112 117 120 120 120",
		"11":   "_ _ _ _ _ 22 46 67 90 110 132 134 131",
		"12":   "_ _ _ _ _ 25 49 72 99 120 145 144 143",
		"+Inf": "101 99 97 99 101 112 120 130 142 147 159 155 156",
	},
}

// histogramLayoutChangeFirstStep is the first step of
// histogramLayoutChangeVictoriaMetrics, one hour after the first row.
const histogramLayoutChangeFirstStep = e2eStartMS + 3_600_000

// assertVictoriaMetricsRange compares a range result that starts at start, in
// 10m steps, with the VictoriaMetrics points at the same steps.
func assertVictoriaMetricsRange(test *testing.T, name string, got queryDataDTO, want map[string]string, start int64) {
	test.Helper()
	gotPoints := make(map[string]map[int64]string, len(got.Result))
	for _, series := range got.Result {
		points := make(map[int64]string, len(series.Values))
		for _, point := range series.Values {
			points[int64(point[0].(float64)*1000)] = sampleString(point)
		}
		gotPoints[series.Metric["le"]] = points
	}
	for le, values := range want {
		for index, value := range strings.Fields(values) {
			timestamp := histogramLayoutChangeFirstStep + int64(index)*600_000
			if timestamp < start || value == "_" {
				continue
			}
			gotValue, ok := gotPoints[le][timestamp]
			x, errX := strconv.ParseFloat(gotValue, 64)
			y, errY := strconv.ParseFloat(value, 64)
			if !ok || errX != nil || errY != nil || math.Abs(x-y) > 1e-9*math.Max(1, math.Abs(y)) {
				test.Errorf("%s le=%q at %d: got %q (present %v), VictoriaMetrics %q", name, le, timestamp, gotValue, ok, value)
			}
			delete(gotPoints[le], timestamp)
		}
	}
	for le, points := range gotPoints {
		if len(points) > 0 {
			test.Fatalf("%s le=%q: points %v are not in the VictoriaMetrics result", name, le, points)
		}
	}
}
