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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestLogQLPostHogLayoutMatchesReferenceSQL runs LogQL metric queries against
// rows shaped like PostHog's JSON logs (body fields already parsed into
// attributes, namespace in resource attributes) and compares every series
// and point with a hand-written ClickHouse reference query.
func TestLogQLPostHogLayoutMatchesReferenceSQL(t *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		t.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const teamID = 42
	dbName := fmt.Sprintf("snuffle_logql_posthog_%d", time.Now().UnixNano())
	rootCfg := ConfigFromEnv()
	rootCfg.CHAddr = getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	rootCfg.CHDatabase = ""
	rootCfg.CHTimeout = 10 * time.Second
	rootClient := NewClickHouseClient(rootCfg)
	waitForClickHouse(t, ctx, rootClient)
	if err := rootClient.Exec(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
		t.Fatalf("create database: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = rootClient.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)+" SYNC")
	}()

	cfg := rootCfg
	cfg.CHDatabase = dbName
	cfg.LogSchemaLayout = string(logSchemaLayoutPostHog)
	cfg.LogsTable = "logs34"
	cfg.HTTPHost = "127.0.0.1"
	cfg.QueryTimeout = 15 * time.Second
	cfg.LogQueryMaxRows = 100000
	client := NewClickHouseClient(cfg)
	loadE2ESchema(t, ctx, client, filepath.Join(repoRoot(t), "scripts", "create_logs_posthog_schema.sql"))

	// 1200 rows, one every 3s over an hour. Most are JSON "committed" lines
	// in the target namespace; some are plain-text committed lines without
	// attributes, unrelated lines, or committed lines in another namespace.
	start := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	end := start.Add(time.Hour)
	if err := client.Exec(ctx, fmt.Sprintf(`INSERT INTO logs34 (team_id, original_expiry_timestamp, uuid, timestamp, observed_timestamp, body, severity_text, service_name, resource_attributes, attributes_map_str)
WITH
    number AS n,
    toDateTime64(%d, 6, 'UTC') + toIntervalSecond(n * 3) AS ts,
    if(n %% 4 = 3, 'other-namespace', 'warehouse-sources-load') AS ns,
    concat('ws-', toString(n %% 3)) AS service,
    multiIf(n %% 10 = 0, 'text', n %% 10 = 1, 'unrelated', 'json') AS kind,
    if(intDiv(n, 2) %% 2 = 0, 'enforce', 'observe') AS mode,
    toString(intDiv(n, 4) %% 3) AS mpp,
    toString(10 + (n * 37) %% 500) AS rewrite_ms,
    toString((n * 7) %% 5) AS partitions
SELECT %d, now() + toIntervalDay(30), toString(n), ts, ts,
    multiIf(
        kind = 'json', concat('{"msg":"deltalite write: committed","governor_mode":"', mode, '","governor_mpp":', mpp, ',"rewrite_ms":', rewrite_ms, ',"partitions_touched":', partitions, '}'),
        kind = 'text', concat('deltalite write: committed rewrite_ms=', rewrite_ms),
        'request handled'),
    if(n %% 13 = 0, 'warn', 'info'), service,
    map('k8s.namespace.name', ns, 'service.name', service),
    if(kind = 'json', map('msg__str', 'deltalite write: committed', 'governor_mode__str', mode, 'governor_mpp__str', mpp, 'rewrite_ms__str', rewrite_ms, 'partitions_touched__str', partitions), map())
FROM numbers(1200)`, start.Unix(), teamID)); err != nil {
		t.Fatalf("seed logs: %v", err)
	}

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()

	selector := `{k8s.namespace.name="warehouse-sources-load"} |= "deltalite write: committed"`
	where := fmt.Sprintf(`team_id = %d AND resource_attributes['k8s.namespace.name'] = 'warehouse-sources-load' AND body LIKE '%%deltalite write: committed%%'`, teamID)
	const attr = `attributes_map_str['%s__str']`
	// group label, evaluation timestamp (seconds) and value for a 5m window
	// evaluated every 5m: a line at ts belongs to the point ceil(ts/5m)*5m.
	reference := func(label, value, extraWhere string) string {
		return fmt.Sprintf(
			`SELECT toString(%s) AS label, intDiv(toInt64(toUnixTimestamp64Nano(timestamp)) + 299999999999, 300000000000) * 300 AS t, %s AS value FROM logs34 WHERE %s%s GROUP BY label, t HAVING t >= %d AND t <= %d`,
			label, value, where, extraWhere, start.Unix(), end.Unix(),
		)
	}
	for _, test := range []struct {
		name      string
		query     string
		groupBy   string
		reference string
	}{
		{
			name:      "count",
			query:     `sum(count_over_time(` + selector + ` [5m]))`,
			reference: reference("''", "toFloat64(count())", ""),
		},
		{
			name:      "count by attribute",
			query:     `sum by (governor_mpp) (count_over_time(` + selector + ` [5m]))`,
			groupBy:   "governor_mpp",
			reference: reference(fmt.Sprintf(attr, "governor_mpp"), "toFloat64(count())", ""),
		},
		{
			name:      "json parser keeps unparseable lines",
			query:     `sum(count_over_time(` + selector + ` | json [5m]))`,
			reference: reference("''", "toFloat64(count())", ""),
		},
		{
			name:      "json parser error filter",
			query:     `sum(count_over_time(` + selector + ` | json | __error__="" [5m]))`,
			reference: reference("''", "toFloat64(count())", " AND isValidJSON(body)"),
		},
		{
			name:      "string attribute filter",
			query:     `sum(count_over_time(` + selector + ` | governor_mode="enforce" [5m]))`,
			reference: reference("''", "toFloat64(count())", " AND "+fmt.Sprintf(attr, "governor_mode")+" = 'enforce'"),
		},
		{
			name:      "numeric attribute filter",
			query:     `sum(count_over_time(` + selector + ` | partitions_touched >= 2 [5m]))`,
			reference: reference("''", "toFloat64(count())", " AND toFloat64OrNull("+fmt.Sprintf(attr, "partitions_touched")+") >= 2"),
		},
		{
			name:      "numeric equality filter",
			query:     `sum(count_over_time(` + selector + ` | json | partitions_touched == 2 [5m]))`,
			reference: reference("''", "toFloat64(count())", " AND toFloat64OrNull("+fmt.Sprintf(attr, "partitions_touched")+") = 2"),
		},
		{
			name:      "unwrap quantile by service",
			query:     `quantile_over_time(0.5, ` + selector + ` | unwrap rewrite_ms [5m]) by (service_name)`,
			groupBy:   "service_name",
			reference: reference("service_name", "groupArray(toFloat64OrNull("+fmt.Sprintf(attr, "rewrite_ms")+"))", " AND "+fmt.Sprintf(attr, "rewrite_ms")+" != ''"),
		},
		{
			name:      "json unwrap avg by service",
			query:     `avg_over_time(` + selector + ` | json | unwrap rewrite_ms [5m]) by (service_name)`,
			groupBy:   "service_name",
			reference: reference("service_name", "toFloat64(assumeNotNull(avg(toFloat64OrNull("+fmt.Sprintf(attr, "rewrite_ms")+"))))", " AND "+fmt.Sprintf(attr, "rewrite_ms")+" != ''"),
		},
		{
			name:      "json filter unwrap quantile by service",
			query:     `quantile_over_time(0.5, ` + selector + ` | json | partitions_touched >= 2 | unwrap rewrite_ms [5m]) by (service_name)`,
			groupBy:   "service_name",
			reference: reference("service_name", "groupArray(toFloat64OrNull("+fmt.Sprintf(attr, "rewrite_ms")+"))", " AND toFloat64OrNull("+fmt.Sprintf(attr, "partitions_touched")+") >= 2"),
		},
		{
			// line_format has no SQL form, so this runs through the Go
			// evaluator, which must still see the resource-attribute matcher
			name:      "go fallback",
			query:     `sum(count_over_time(` + selector + ` | line_format "{{.msg}}" [5m]))`,
			reference: reference("''", "toFloat64(count())", ""),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := logQLReferenceSeries(t, ctx, client, test.reference, test.groupBy)
			if len(want) == 0 {
				t.Fatal("reference query returned no rows")
			}
			data := apiGetForTeam[queryDataDTO](t, api.URL, teamID, "/loki/api/v1/query_range", url.Values{
				"query": {test.query},
				"start": {strconv.FormatInt(start.Unix(), 10)},
				"end":   {strconv.FormatInt(end.Unix(), 10)},
				"step":  {"300"},
			})
			got := map[string]float64{}
			for _, series := range data.Result {
				for _, point := range series.Values {
					value, err := strconv.ParseFloat(sampleString(point), 64)
					if err != nil {
						t.Fatalf("parse sample %v: %v", point, err)
					}
					got[logQLReferenceKey(series.Metric, int64(point[0].(float64)))] = value
				}
			}
			assertLogQLSeriesEqual(t, got, want)
		})
	}

	t.Run("row cap is an error, not an empty result", func(t *testing.T) {
		capped := cfg
		capped.LogQueryMaxRows = 10
		mux := http.NewServeMux()
		newServer(capped).routes(mux)
		cappedAPI := httptest.NewServer(mux)
		defer cappedAPI.Close()
		resp, err := http.Get(fmt.Sprintf("%s/t/%d/loki/api/v1/query_range?%s", cappedAPI.URL, teamID, url.Values{
			"query": {`sum(count_over_time(` + selector + ` | line_format "{{.msg}}" [5m]))`},
			"start": {strconv.FormatInt(start.Unix(), 10)},
			"end":   {strconv.FormatInt(end.Unix(), 10)},
			"step":  {"300"},
		}.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var decoded struct {
			Status    string `json:"status"`
			ErrorType string `json:"errorType"`
		}
		if err := json.Unmarshal(body, &decoded); err != nil || resp.StatusCode == http.StatusOK || decoded.Status != "error" || decoded.ErrorType != "execution" {
			t.Fatalf("capped query status %d: %s", resp.StatusCode, body)
		}
	})
}

// logQLReferenceSeries runs a reference query returning (label, t, value)
// rows, where value is a Float64 or, for quantiles, an Array(Float64) of the
// raw values to take the median of.
func logQLReferenceSeries(t *testing.T, ctx context.Context, client *ClickHouseClient, sql, groupBy string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	quantile := strings.Contains(sql, "groupArray(")
	err := client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var label string
		var ts int64
		var value float64
		var values []float64
		dest := any(&value)
		if quantile {
			dest = &values
		}
		if err := row.Scan(&label, &ts, dest); err != nil {
			return err
		}
		labels := map[string]string{}
		if groupBy != "" && label != "" {
			labels[groupBy] = label
		}
		if quantile {
			value = logQLReferenceMedian(values)
		}
		out[logQLReferenceKey(labels, ts)] = value
		return nil
	})
	if err != nil {
		t.Fatalf("reference query %s: %v", sql, err)
	}
	return out
}

// logQLReferenceMedian is the Prometheus/Loki 0.5 quantile: linear
// interpolation between the two middle values.
func logQLReferenceMedian(values []float64) float64 {
	sort.Float64s(values)
	rank := 0.5 * float64(len(values)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	return values[lower] + (values[upper]-values[lower])*(rank-float64(lower))
}

func logQLReferenceKey(labels map[string]string, ts int64) string {
	return labelsKey(labels) + "@" + strconv.FormatInt(ts, 10)
}

func assertLogQLSeriesEqual(t *testing.T, got, want map[string]float64) {
	t.Helper()
	for key, wantValue := range want {
		gotValue, ok := got[key]
		if !ok {
			t.Errorf("missing point %s (want %v)", key, wantValue)
			continue
		}
		if math.Abs(gotValue-wantValue) > 1e-9*math.Max(1, math.Abs(wantValue)) {
			t.Errorf("point %s = %v, want %v", key, gotValue, wantValue)
		}
	}
	for key, gotValue := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected point %s = %v", key, gotValue)
		}
	}
}
