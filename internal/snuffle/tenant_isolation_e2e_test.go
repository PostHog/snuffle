package snuffle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// Team A and team B write series and log streams with identical names and
// labels but different values. Team C never writes. Every check is read the
// way PostHog's proxy reads it (X-Team-ID header, no tenant path), and must
// return the same answer for A before and after B writes, and the same answer
// for C before and after anyone writes.
const (
	isoTeamA  = uint64(7001)
	isoTeamB  = uint64(7002)
	isoTeamC  = uint64(7003)
	isoSecret = "team-b-secret"
	// isoEval is the instant query time; samples cover the ten minutes before it.
	isoEval = int64(1_700_100_000)
)

type isoCheck struct {
	name   string
	path   string
	params url.Values
	// data is set when team A must get a non-empty answer, so the check is known
	// to exercise a code path rather than compare two empty results.
	data bool
}

func TestTenantIsolationEndToEnd(t *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		t.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cfg, client, baseURL := startIsolationE2E(t, ctx, nil)

	logStart := time.Now().UTC().Truncate(time.Minute).Add(-8 * time.Minute)
	checks := append(isoMetricChecks(cfg), isoLogChecks(logStart)...)

	before := isoSnapshot(t, baseURL, isoTeamC, checks)
	isoWriteTeam(t, ctx, cfg, client, baseURL, isoTeamA, logStart)
	teamA := isoSnapshot(t, baseURL, isoTeamA, checks)
	for _, check := range checks {
		if check.data && !isoHasData(teamA[check.name]) {
			t.Errorf("team A %s returned no data, so the check proves nothing: %s", check.name, teamA[check.name])
		}
	}
	isoWriteTeam(t, ctx, cfg, client, baseURL, isoTeamB, logStart)
	teamB := isoSnapshot(t, baseURL, isoTeamB, checks)

	for team, want := range map[uint64]map[string]string{isoTeamA: teamA, isoTeamC: before} {
		got := isoSnapshot(t, baseURL, team, checks)
		for _, check := range checks {
			if strings.Contains(got[check.name], isoSecret) {
				t.Errorf("team %d %s leaks team B: %s", team, check.name, got[check.name])
			} else if got[check.name] != want[check.name] {
				t.Errorf("team %d %s changed after team B wrote:\n before: %s\n after:  %s", team, check.name, want[check.name], got[check.name])
			}
		}
	}
	// Team B's own answers must differ, or B's data never landed and the
	// comparison above was vacuous.
	differs := 0
	for _, check := range checks {
		if check.data && teamB[check.name] != teamA[check.name] {
			differs++
		}
	}
	if differs == 0 {
		t.Fatal("team B's answers equal team A's for every check; B's writes did not land")
	}

	t.Run("team selection", func(t *testing.T) { assertIsoTeamSelection(t, baseURL, teamA, teamB) })
	t.Run("requests without a team are refused", func(t *testing.T) { assertIsoNoTeamRefused(t, baseURL) })
	t.Run("requests without credentials are refused", func(t *testing.T) {
		strict := cfg
		strict.AllowUnauthenticated = false
		mux := http.NewServeMux()
		newServer(strict).routes(mux)
		server := httptest.NewServer(mux)
		defer server.Close()
		assertIsoNoCredentialsRefused(t, server.URL)
	})
	t.Run("writes cannot alias another team", func(t *testing.T) { assertIsoWriteTeamAliasing(t, baseURL, logStart) })
}

func startIsolationE2E(t *testing.T, ctx context.Context, tune func(*Config)) (Config, *ClickHouseClient, string) {
	t.Helper()
	chAddr := getenv("SNUFFLE_E2E_CH_ADDR", "127.0.0.1:9000")
	rootCfg := ConfigFromEnv()
	rootCfg.CHAddr = chAddr
	rootCfg.CHDatabase = ""
	rootClient := NewClickHouseClient(rootCfg)
	waitForClickHouse(t, ctx, rootClient)
	dbName := fmt.Sprintf("snuffle_isolation_e2e_%d", time.Now().UnixNano())
	if err := rootClient.Exec(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
		t.Fatalf("create e2e database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = rootClient.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)+" SYNC")
	})

	cfg := e2eConfig(chAddr, dbName)
	// Timestamps here are not Snuffle's own scrape buckets.
	cfg.RemoteWriteInterval = 0
	if tune != nil {
		tune(&cfg)
	}
	client := NewClickHouseClient(cfg)
	createMetricsSchema(t, ctx, client)
	logSchema := "create_logs_snuffle_schema.sql"
	if cfg.postHogLogSchemaLayout() {
		logSchema = "create_logs_posthog_schema.sql"
	}
	loadE2ESchema(t, ctx, client, filepath.Join(repoRoot(t), "scripts", logSchema))

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	t.Cleanup(api.Close)
	return cfg, client, api.URL
}

func isoMetricChecks(cfg Config) []isoCheck {
	const m = "iso_requests_total"
	const h = "iso_latency_seconds"
	exprs := []struct {
		query string
		data  bool
	}{
		{m, true},
		{m + `{job="api"}`, true},
		{m + `{job=~"api|worker"}`, true},
		{m + `{job=~"a.*"}`, true},
		{m + `{instance=~"h.+"}`, true},
		{m + `{job=~".+"}`, true},
		{m + `{job!="worker"}`, true},
		{m + `{job!~"work.*"}`, true},
		{`{__name__="` + m + `"}`, true},
		{`{__name__=~"iso_.*"}`, true},
		{`{__name__=~"iso_requests_total|iso_temp"}`, true},
		{`{job="api"}`, true},
		{m + ` + 0`, true},
		{`sum(` + m + `)`, true},
		{`sum by (job) (` + m + `)`, true},
		{`count by (__name__) ({__name__=~".+"})`, true},
		{`max(` + m + `)`, true},
		{`topk(1, ` + m + `)`, true},
		{`quantile(0.5, ` + m + `)`, true},
		{`count_values("v", iso_temp)`, true},
		{`rate(` + m + `[1m])`, true},
		{`rate(` + m + `)`, true},
		{`irate(` + m + `[1m])`, true},
		{`increase(` + m + `[5m])`, true},
		{`increase(` + m + `)`, true},
		{`sum(rate(` + m + `[1m]))`, true},
		{`sum by (job) (rate(` + m + `[5m]))`, true},
		{`sum(increase(` + m + `[5m]))`, true},
		{`sum by (job) (increase(` + m + `[5m]))`, true},
		{`delta(iso_temp[5m])`, true},
		{`sum_over_time(iso_temp[5m])`, true},
		{`avg_over_time(iso_temp[5m])`, true},
		{`max_over_time(iso_temp[5m])`, true},
		{`count_over_time(iso_temp[5m])`, true},
		{`last_over_time(iso_temp[5m])`, true},
		{`quantile_over_time(0.5, iso_temp[5m])`, true},
		{m + ` offset 1m`, true},
		{m + ` @ ` + strconv.FormatInt(isoEval, 10), true},
		{`rate(` + m + `[1m] offset 1m)`, true},
		{`max_over_time(rate(` + m + `[1m])[5m:1m])`, true},
		{m + ` or iso_temp`, true},
		{m + ` unless on (job) iso_temp`, true},
		{m + ` and on (job) iso_temp`, true},
		{m + ` / on (job, instance) iso_temp`, true},
		{`label_replace(` + m + `, "x", "$1", "job", "(.*)")`, true},
		{`absent(` + m + `)`, false},
		{`count(iso_secret_b_total)`, false},
		{`histogram_quantile(0.9, sum by (le) (rate(` + h + `_bucket[5m])))`, true},
		{`histogram_quantile(0.9, rate(` + h + `_bucket[5m]))`, true},
		{`histogram_quantile(0.9, sum by (le, job) (increase(` + h + `_bucket[5m])))`, true},
		{`sum(rate(` + h + `_sum[5m])) / sum(rate(` + h + `_count[5m]))`, true},
		{`{"service.name"="svc"}`, true},
		{`iso_otel_requests_total{"http.route"="/x"}`, true},
		{`sum by ("service.name") (rate(iso_otel_requests_total[5m]))`, true},
	}
	if cfg.postHogSchemaLayout() {
		const ph = "iso_ph_duration_seconds"
		exprs = append(exprs, []struct {
			query string
			data  bool
		}{
			{ph + `_count`, true},
			{ph + `_bucket`, true},
			{`histogram_quantile(0.9, sum by (le) (rate(` + ph + `_bucket[5m])))`, true},
			{`histogram_quantile(0.9, sum by (le, status) (increase(` + ph + `_bucket[5m])))`, true},
			{`histogram_quantile(0.9, rate(` + ph + `_bucket[5m])) or histogram_quantile(0.5, rate(` + ph + `_bucket[5m]))`, true},
			{`sum(rate(` + ph + `_sum[5m])) / sum(rate(` + ph + `_count[5m]))`, true},
		}...)
	} else {
		exprs = append(exprs, []struct {
			query string
			data  bool
		}{
			{`native_iso_seconds`, true},
			{`histogram_quantile(0.9, rate(native_iso_seconds[5m]))`, true},
			{`histogram_count(sum(rate(native_iso_seconds[5m])))`, true},
		}...)
	}

	evalTime := strconv.FormatInt(isoEval, 10)
	rangeStart := strconv.FormatInt(isoEval-300, 10)
	var checks []isoCheck
	for _, expr := range exprs {
		checks = append(checks,
			isoCheck{name: "instant " + expr.query, path: "/api/v1/query", params: url.Values{"query": {expr.query}, "time": {evalTime}}, data: expr.data},
			isoCheck{name: "range " + expr.query, path: "/api/v1/query_range", params: url.Values{"query": {expr.query}, "start": {rangeStart}, "end": {evalTime}, "step": {"60s"}}, data: expr.data},
		)
	}
	window := func(extra url.Values) url.Values {
		values := url.Values{"start": {strconv.FormatInt(isoEval-600, 10)}, "end": {evalTime}}
		for key, value := range extra {
			values[key] = value
		}
		return values
	}
	checks = append(checks,
		isoCheck{name: "labels", path: "/api/v1/labels", params: window(nil), data: true},
		isoCheck{name: "labels match", path: "/api/v1/labels", params: window(url.Values{"match[]": {m}}), data: true},
		isoCheck{name: "labels two matches", path: "/api/v1/labels", params: window(url.Values{"match[]": {m, `{__name__=~"iso_secret.*"}`}}), data: true},
		isoCheck{name: "metric names", path: "/api/v1/label/__name__/values", params: window(nil), data: true},
		isoCheck{name: "metric names regex", path: "/api/v1/label/__name__/values", params: window(url.Values{"match[]": {`{__name__=~".*requests.*"}`}}), data: true},
		isoCheck{name: "job values", path: "/api/v1/label/job/values", params: window(nil), data: true},
		isoCheck{name: "instance values match", path: "/api/v1/label/instance/values", params: window(url.Values{"match[]": {m + `{job="api"}`}}), data: true},
		isoCheck{name: "tenant label values", path: "/api/v1/label/tenant/values", params: window(nil)},
		isoCheck{name: "dotted label values", path: "/api/v1/label/service.name/values", params: window(nil), data: true},
		isoCheck{name: "series", path: "/api/v1/series", params: window(url.Values{"match[]": {m}}), data: true},
		isoCheck{name: "series several matches", path: "/api/v1/series", params: window(url.Values{"match[]": {m + `{job="api"}`, `iso_temp`, `{__name__=~"iso_secret.*"}`}}), data: true},
		isoCheck{name: "series regex name", path: "/api/v1/series", params: window(url.Values{"match[]": {`{__name__=~"iso_.+"}`}}), data: true},
		isoCheck{name: "series label only", path: "/api/v1/series", params: window(url.Values{"match[]": {`{job="api"}`}}), data: true},
		isoCheck{name: "metadata", path: "/api/v1/metadata", params: url.Values{}, data: !cfg.postHogSchemaLayout()},
		isoCheck{name: "metadata metric", path: "/api/v1/metadata", params: url.Values{"metric": {m}}, data: !cfg.postHogSchemaLayout()},
		isoCheck{name: "exemplars", path: "/api/v1/query_exemplars", params: window(url.Values{"query": {m}}), data: !cfg.postHogSchemaLayout()},
		isoCheck{name: "remote read", path: "/api/v1/read", params: url.Values{"match": {m}}, data: true},
		isoCheck{name: "remote read regex", path: "/api/v1/read", params: url.Values{"match": {`iso_.*`}}, data: true},
		isoCheck{name: "remote read native histograms", path: "/api/v1/read", params: url.Values{"match": {`native_iso_seconds`}}, data: !cfg.postHogSchemaLayout()},
	)
	return checks
}

func isoLogChecks(start time.Time) []isoCheck {
	from := strconv.FormatInt(start.Add(-2*time.Minute).UnixNano(), 10)
	to := strconv.FormatInt(start.Add(8*time.Minute).UnixNano(), 10)
	// Every line falls in the five minutes before this.
	instant := strconv.FormatInt(start.Add(4*time.Minute).UnixNano(), 10)
	const s = `{service_name="iso-svc"}`
	streams := []string{
		s,
		`{service_name=~"iso.*"}`,
		`{service_name=~".+"}`,
		`{level="info"}`,
		s + ` |= "hello"`,
		s + ` |~ "hel+o"`,
		s + ` != "zzz"`,
		s + ` !~ "zz+"`,
		s + ` | json`,
		s + ` | json | user="alice"`,
		s + ` | json | latency > 1`,
		s + ` | json | line_format "{{.user}} {{.msg}}"`,
		s + ` | logfmt`,
		s + ` | regexp "(?P<word>hello)"`,
		s + ` | pattern "<_> <word> <_>"`,
		s + ` | label_format who=user`,
	}
	metrics := []string{
		`count_over_time(` + s + `[5m])`,
		`sum(count_over_time(` + s + `[5m]))`,
		`sum(count_over_time({service_name=~".+"}[5m]))`,
		// The instant form of this and the topk below answers empty even with one team.
		`sum by (service_name) (count_over_time({service_name=~".+"}[5m]))`,
		`sum by (level) (rate(` + s + `[5m]))`,
		`bytes_over_time(` + s + `[5m])`,
		`sum(bytes_rate(` + s + `[5m]))`,
		`sum by (user) (count_over_time(` + s + ` | json [5m]))`,
		`avg_over_time(` + s + ` | json | unwrap latency [5m])`,
		`max_over_time(` + s + ` | json | unwrap latency [5m]) by (service_name)`,
		`quantile_over_time(0.9, ` + s + ` | json | unwrap latency [5m]) by (service_name)`,
		`topk(1, sum by (service_name) (count_over_time({service_name=~".+"}[5m])))`,
		`count_over_time(` + s + ` |= "hello" [5m])`,
		`sum(count_over_time(` + s + `[5m])) / sum(count_over_time({level="info"}[5m]))`,
		`sum(count_over_time(` + s + `[5m] offset 1m))`,
	}
	var checks []isoCheck
	for _, query := range streams {
		for _, direction := range []string{"forward", "backward"} {
			checks = append(checks, isoCheck{
				name:   "loki " + direction + " " + query,
				path:   "/loki/api/v1/query_range",
				params: url.Values{"query": {query}, "start": {from}, "end": {to}, "limit": {"100"}, "direction": {direction}},
				data:   true,
			})
		}
	}
	for _, query := range metrics {
		checks = append(checks,
			isoCheck{name: "loki instant " + query, path: "/loki/api/v1/query", params: url.Values{"query": {query}, "time": {instant}}, data: !strings.Contains(query, "sum by (service_name)")},
			isoCheck{name: "loki range " + query, path: "/loki/api/v1/query_range", params: url.Values{"query": {query}, "start": {from}, "end": {to}, "step": {"60s"}}, data: true},
		)
	}
	window := func(extra url.Values) url.Values {
		values := url.Values{"start": {from}, "end": {to}}
		for key, value := range extra {
			values[key] = value
		}
		return values
	}
	checks = append(checks,
		isoCheck{name: "loki labels", path: "/loki/api/v1/labels", params: window(nil), data: true},
		isoCheck{name: "loki labels query", path: "/loki/api/v1/labels", params: window(url.Values{"query": {s}}), data: true},
		isoCheck{name: "loki service values", path: "/loki/api/v1/label/service_name/values", params: window(nil), data: true},
		isoCheck{name: "loki level values", path: "/loki/api/v1/label/level/values", params: window(nil), data: true},
		isoCheck{name: "loki metadata values", path: "/loki/api/v1/label/plain_attr/values", params: window(nil)},
		isoCheck{name: "loki secret metadata values", path: "/loki/api/v1/label/secret_attr/values", params: window(nil)},
		isoCheck{name: "loki trace values", path: "/loki/api/v1/label/trace_id/values", params: window(nil)},
		isoCheck{name: "loki values query", path: "/loki/api/v1/label/level/values", params: window(url.Values{"query": {s}}), data: true},
		isoCheck{name: "loki series", path: "/loki/api/v1/series", params: window(url.Values{"match[]": {s}}), data: true},
		isoCheck{name: "loki series regex", path: "/loki/api/v1/series", params: window(url.Values{"match[]": {`{service_name=~".+"}`}}), data: true},
		isoCheck{name: "loki index stats", path: "/loki/api/v1/index/stats", params: window(url.Values{"query": {s}}), data: true},
		isoCheck{name: "loki index stats regex", path: "/loki/api/v1/index/stats", params: window(url.Values{"query": {`{service_name=~".+"}`}}), data: true},
	)
	return checks
}

// isoWriteTeam writes the same series and streams for every team. Team B's
// values are far from team A's, and B alone also writes series, label values
// and log lines that carry isoSecret.
func isoWriteTeam(t *testing.T, ctx context.Context, cfg Config, client *ClickHouseClient, baseURL string, team uint64, logStart time.Time) {
	t.Helper()
	b := team == isoTeamB
	scale := func(a, bValue float64) float64 {
		if b {
			return bValue
		}
		return a
	}
	series := func(lbls map[string]string, value func(i int) float64) prompb.TimeSeries {
		ts := prompb.TimeSeries{}
		for _, name := range sortedKeys(lbls) {
			ts.Labels = append(ts.Labels, prompb.Label{Name: name, Value: lbls[name]})
		}
		for i := 0; i <= 40; i++ {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: (isoEval-600)*1000 + int64(i)*15_000, Value: value(i)})
		}
		return ts
	}
	counter := func(base, perStep float64) func(int) float64 {
		return func(i int) float64 { return base + float64(i)*perStep }
	}
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		series(map[string]string{"__name__": "iso_requests_total", "job": "api", "instance": "h1"}, counter(scale(10, 1_000_000), scale(1, 1000))),
		series(map[string]string{"__name__": "iso_requests_total", "job": "worker", "instance": "h2"}, counter(scale(5, 2_000_000), scale(2, 7000))),
		series(map[string]string{"__name__": "iso_temp", "job": "api", "instance": "h1"}, func(i int) float64 { return scale(20+float64(i%3), 500_000+float64(i)) }),
		series(map[string]string{"__name__": "iso_otel_requests_total", "service.name": "svc", "http.route": "/x"}, counter(scale(1, 3_000_000), scale(1, 900))),
		series(map[string]string{"__name__": "iso_latency_seconds_sum", "job": "api"}, counter(0, scale(0.5, 9000))),
		series(map[string]string{"__name__": "iso_latency_seconds_count", "job": "api"}, counter(0, scale(3, 90_000))),
	}}
	for _, le := range []struct {
		bound  string
		aShare float64
		bShare float64
	}{{"0.1", 1, 10}, {"1", 2, 50_000}, {"+Inf", 3, 90_000}} {
		req.Timeseries = append(req.Timeseries, series(map[string]string{"__name__": "iso_latency_seconds_bucket", "job": "api", "le": le.bound}, counter(0, scale(le.aShare, le.bShare))))
	}
	if b {
		req.Timeseries = append(req.Timeseries,
			series(map[string]string{"__name__": "iso_requests_total", "job": "api", "instance": "h9", "tenant": isoSecret}, counter(1, 1)),
			series(map[string]string{"__name__": "iso_secret_b_total", "job": "api", "secret": isoSecret}, counter(1, 1)),
		)
	}
	if !cfg.postHogSchemaLayout() {
		native := prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: "native_iso_seconds"}, {Name: "job", Value: "native"}}}
		for i := 0; i <= 40; i++ {
			count := uint64(1+i) * uint64(scale(3, 300_000))
			native.Histograms = append(native.Histograms, prompb.Histogram{
				Count:          &prompb.Histogram_CountInt{CountInt: count},
				Sum:            float64(count) * scale(0.25, 40),
				Schema:         0,
				ZeroThreshold:  0.001,
				ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: count / 3},
				PositiveSpans:  []prompb.BucketSpan{{Offset: 0, Length: 1}},
				PositiveDeltas: []int64{int64(count - count/3)},
				Timestamp:      (isoEval-600)*1000 + int64(i)*15_000,
			})
		}
		req.Timeseries = append(req.Timeseries, native)
		req.Timeseries[0].Exemplars = []prompb.Exemplar{{
			Labels:    []prompb.Label{{Name: "trace_id", Value: map[bool]string{true: isoSecret, false: "trace-a"}[b]}},
			Value:     scale(50, 1_000_040),
			Timestamp: (isoEval - 30) * 1000,
		}}
		help := "requests"
		if b {
			help = isoSecret
		}
		req.Metadata = []prompb.MetricMetadata{{MetricFamilyName: "iso_requests_total", Type: prompb.MetricMetadata_COUNTER, Help: help, Unit: "requests"}}
		if b {
			req.Metadata = append(req.Metadata, prompb.MetricMetadata{MetricFamilyName: "iso_secret_b_total", Type: prompb.MetricMetadata_COUNTER, Help: isoSecret})
		}
	}
	postRemoteWriteForTeam(t, baseURL, team, req)

	if cfg.postHogSchemaLayout() {
		// Compact histograms only arrive from the OTel pipeline, so write the input
		// table directly, with the same series fingerprint for both teams.
		for i := 0; i <= 20; i++ {
			counts := fmt.Sprintf("[%d, %d, %d]", (i+1)*int(scale(2, 20_000)), (i+1)*int(scale(3, 30_000)), (i+1)*int(scale(5, 50_000)))
			sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
				SELECT %d, 'iso_ph_duration_seconds', cityHash64('iso_ph_duration_seconds'), %s, now64(6), now64(6) + INTERVAL 1 DAY, 'svc', 'histogram', 'cumulative', %v, %d, [0.5, 1.0], %s, true, map('service.name', 'svc'), map('status', '200')`,
				tableName(cfg.CHDatabase, cfg.MetricsInputTable), team, chTimeMillis((isoEval-600)*1000+int64(i)*30_000), scale(float64(i), 1e6+float64(i)), (i+1)*int(scale(10, 100_000)), counts)
			if err := client.Exec(ctx, sql); err != nil {
				t.Fatalf("insert compact histogram: %v", err)
			}
		}
	}

	lines := []struct {
		offset time.Duration
		line   string
		meta   map[string]any
	}{
		{0, `{"msg":"hello a","user":"alice","latency":12}`, map[string]any{"plain_attr": "a", "trace_id": "trace-a"}},
		{time.Minute, `{"msg":"hello again","user":"alice","latency":7}`, map[string]any{"plain_attr": "a"}},
		{3 * time.Minute, `msg=hello user=alice latency=3`, nil},
	}
	if b {
		lines = []struct {
			offset time.Duration
			line   string
			meta   map[string]any
		}{
			{0, `{"msg":"hello ` + isoSecret + `","user":"` + isoSecret + `","latency":999999}`, map[string]any{"plain_attr": isoSecret, "secret_attr": isoSecret, "trace_id": isoSecret}},
			{30 * time.Second, `{"msg":"hello b2","user":"bob","latency":555555}`, nil},
			{time.Minute, `{"msg":"hello b3","user":"bob","latency":444444}`, nil},
			{2 * time.Minute, `msg=hello user=` + isoSecret + ` latency=777777`, nil},
		}
	}
	var values [][]any
	for _, entry := range lines {
		value := []any{strconv.FormatInt(logStart.Add(entry.offset).UnixNano(), 10), entry.line}
		if entry.meta != nil {
			value = append(value, entry.meta)
		}
		values = append(values, value)
	}
	streams := []lokiPushStream{{Stream: map[string]string{"service_name": "iso-svc", "level": "info"}, Values: values}}
	if b {
		streams = append(streams, lokiPushStream{
			Stream: map[string]string{"service_name": "iso-secret-b", "level": "info"},
			Values: [][]any{{strconv.FormatInt(logStart.Add(time.Minute).UnixNano(), 10), "secret line " + isoSecret}},
		})
	}
	isoLokiPush(t, baseURL, fmt.Sprintf("/t/%d", team), lokiPushRequest{Streams: streams})
}

func isoLokiPush(t *testing.T, baseURL, prefix string, req lokiPushRequest) {
	t.Helper()
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+prefix+"/loki/api/v1/push", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Loki push %s status %d: %s", prefix, resp.StatusCode, body)
	}
}

func isoSnapshot(t *testing.T, baseURL string, team uint64, checks []isoCheck) map[string]string {
	t.Helper()
	snapshot := make(map[string]string, len(checks))
	for _, check := range checks {
		snapshot[check.name] = isoFetch(t, baseURL, team, check)
	}
	return snapshot
}

// isoFetch sends the request the way PostHog's proxy does and returns a
// canonical form of the answer: status plus JSON with arrays sorted.
func isoFetch(t *testing.T, baseURL string, team uint64, check isoCheck) string {
	t.Helper()
	if check.path == "/api/v1/read" {
		return isoRemoteRead(t, baseURL, team, check.params.Get("match"))
	}
	req, err := http.NewRequest(http.MethodGet, baseURL+check.path+"?"+check.params.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Team-ID", strconv.FormatUint(team, 10))
	return isoDo(t, req)
}

func isoDo(t *testing.T, req *http.Request) string {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	canonical, _ := json.Marshal(isoCanonical(decoded))
	return fmt.Sprintf("%d %s", resp.StatusCode, canonical)
}

func isoRemoteRead(t *testing.T, baseURL string, team uint64, metricRegex string) string {
	t.Helper()
	read := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: (isoEval - 600) * 1000,
			EndTimestampMs:   isoEval * 1000,
			Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_RE, Name: labels.MetricName, Value: metricRegex}},
		}},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
	}
	payload, err := read.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/read", bytes.NewReader(snappy.Encode(nil, payload)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Team-ID", strconv.FormatUint(team, 10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	decoded, err := snappy.Decode(nil, body)
	if err != nil {
		t.Fatalf("decode remote read: %v", err)
	}
	var result prompb.ReadResponse
	if err := result.Unmarshal(decoded); err != nil {
		t.Fatalf("unmarshal remote read: %v", err)
	}
	var generic any
	encoded, _ := json.Marshal(result)
	_ = json.Unmarshal(encoded, &generic)
	canonical, _ := json.Marshal(isoCanonical(generic))
	return fmt.Sprintf("%d %s", resp.StatusCode, canonical)
}

// isoCanonical sorts every array so that row order, which no API promises,
// does not register as a difference.
func isoCanonical(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			v[key] = isoCanonical(item)
		}
		return v
	case []any:
		keys := make([]string, len(v))
		for i := range v {
			v[i] = isoCanonical(v[i])
			encoded, _ := json.Marshal(v[i])
			keys[i] = string(encoded)
		}
		sort.Sort(byKey{keys, v})
		return v
	default:
		return v
	}
}

type byKey struct {
	keys   []string
	values []any
}

func (b byKey) Len() int           { return len(b.keys) }
func (b byKey) Less(i, j int) bool { return b.keys[i] < b.keys[j] }
func (b byKey) Swap(i, j int) {
	b.keys[i], b.keys[j] = b.keys[j], b.keys[i]
	b.values[i], b.values[j] = b.values[j], b.values[i]
}

func isoHasData(snapshot string) bool {
	status, body, _ := strings.Cut(snapshot, " ")
	if status != "200" {
		return false
	}
	var decoded struct {
		Data       json.RawMessage `json:"data"`
		Results    json.RawMessage `json:"results"`
		Streams    *int            `json:"streams"`
		Entries    *int            `json:"entries"`
		ResultType string          `json:"resultType"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		return false
	}
	if decoded.Streams != nil {
		return *decoded.Streams > 0 || (decoded.Entries != nil && *decoded.Entries > 0)
	}
	if decoded.Results != nil {
		return strings.Contains(string(decoded.Results), `"labels"`)
	}
	var data any
	if err := json.Unmarshal(decoded.Data, &data); err != nil {
		return false
	}
	switch d := data.(type) {
	case []any:
		return len(d) > 0
	case map[string]any:
		if result, ok := d["result"].([]any); ok {
			return len(result) > 0
		}
		return len(d) > 0
	}
	return false
}

func assertIsoTeamSelection(t *testing.T, baseURL string, teamA, teamB map[string]string) {
	check := isoCheck{name: "instant iso_requests_total", path: "/api/v1/query", params: url.Values{"query": {"iso_requests_total"}, "time": {strconv.FormatInt(isoEval, 10)}}}
	want, leaked := teamA[check.name], teamB[check.name]
	header := strconv.FormatUint(isoTeamA, 10)
	other := strconv.FormatUint(isoTeamB, 10)
	for name, build := range map[string]func() *http.Request{
		"team_id query parameter": func() *http.Request {
			params := url.Values{"query": check.params["query"], "time": check.params["time"], "team_id": {other}}
			req, _ := http.NewRequest(http.MethodGet, baseURL+check.path+"?"+params.Encode(), nil)
			return req
		},
		"team_id form field": func() *http.Request {
			form := url.Values{"query": check.params["query"], "time": check.params["time"], "team_id": {other}}
			req, _ := http.NewRequest(http.MethodPost, baseURL+check.path, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return req
		},
		"team_id multipart field": func() *http.Request {
			body := "--x\r\nContent-Disposition: form-data; name=\"query\"\r\n\r\niso_requests_total\r\n--x\r\nContent-Disposition: form-data; name=\"time\"\r\n\r\n" + strconv.FormatInt(isoEval, 10) + "\r\n--x\r\nContent-Disposition: form-data; name=\"team_id\"\r\n\r\n" + other + "\r\n--x--\r\n"
			req, _ := http.NewRequest(http.MethodPost, baseURL+check.path, strings.NewReader(body))
			req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
			return req
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := build()
			req.Header.Set("X-Team-ID", header)
			// Snuffle does not parse multipart bodies, so that request may fail,
			// but no request may answer with team B's data.
			if got := isoDo(t, req); got == leaked || (got != want && strings.HasPrefix(got, "200 ")) {
				t.Errorf("X-Team-ID %s with %s returned %s, want team A's %s", header, name, got, want)
			}
		})
	}
}

func assertIsoNoTeamRefused(t *testing.T, baseURL string) {
	for _, path := range []string{"/api/v1/query?query=iso_requests_total&time=" + strconv.FormatInt(isoEval, 10), "/api/v1/labels", "/loki/api/v1/labels"} {
		resp, err := http.Get(baseURL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s without a team returned %d, want 400: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
		}
	}
}

func assertIsoNoCredentialsRefused(t *testing.T, baseURL string) {
	// The e2e ClickHouse default user has no password, like any ClickHouse that
	// still has its stock default user. /metrics and the health checks carry no
	// tenant data and stay open for scrapers and load balancers.
	for _, path := range []string{"/api/v1/labels", "/api/v1/query?query=iso_requests_total&time=" + strconv.FormatInt(isoEval, 10), "/loki/api/v1/labels", "/t/7002/api/v1/series?match[]=iso_requests_total"} {
		req, _ := http.NewRequest(http.MethodGet, baseURL+path, nil)
		req.Header.Set("X-Team-ID", strconv.FormatUint(isoTeamB, 10))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s without credentials returned %d, want 401: %.200s", path, resp.StatusCode, body)
		}
	}
}

func assertIsoWriteTeamAliasing(t *testing.T, baseURL string, logStart time.Time) {
	// team_id is Int32 in the log tables and in the PostHog metrics tables. A
	// write for team A + 2^32 must not land in team A.
	alias := isoTeamA + 1<<32
	marker := "aliased-write-marker"
	logs, _ := json.Marshal(lokiPushRequest{Streams: []lokiPushStream{{
		Stream: map[string]string{"service_name": "iso-alias"},
		Values: [][]any{{strconv.FormatInt(logStart.Add(time.Minute).UnixNano(), 10), marker}},
	}}})
	metrics, _ := (&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "iso_alias_total"}, {Name: "marker", Value: marker}},
		Samples: []prompb.Sample{{Timestamp: isoEval * 1000, Value: 1}},
	}}}).Marshal()
	for path, body := range map[string][]byte{"/loki/api/v1/push": logs, "/api/v1/write": snappy.Encode(nil, metrics)} {
		resp, err := http.Post(fmt.Sprintf("%s/t/%d%s", baseURL, alias, path), "application/json", bytes.NewReader(body))
		if path == "/api/v1/write" {
			resp, err = http.Post(fmt.Sprintf("%s/t/%d%s", baseURL, alias, path), "application/x-protobuf", bytes.NewReader(body))
		}
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s for team %d answered %d, want 400", path, alias, resp.StatusCode)
		}
	}

	from := strconv.FormatInt(logStart.Add(-2*time.Minute).UnixNano(), 10)
	to := strconv.FormatInt(logStart.Add(8*time.Minute).UnixNano(), 10)
	for _, check := range []isoCheck{
		{name: "logs", path: "/loki/api/v1/query_range", params: url.Values{"query": {`{service_name="iso-alias"}`}, "start": {from}, "end": {to}, "limit": {"10"}}},
		{name: "metrics", path: "/api/v1/series", params: url.Values{"match[]": {"iso_alias_total"}, "start": {strconv.FormatInt(isoEval-60, 10)}, "end": {strconv.FormatInt(isoEval, 10)}}},
	} {
		if got := isoFetch(t, baseURL, isoTeamA, check); strings.Contains(got, marker) {
			t.Errorf("a %s write for team %d landed in team %d: %s", check.name, alias, isoTeamA, got)
		}
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestQueryResourceLimitsEndToEnd checks that one request cannot outlive the
// query timeout or allocate without bound.
func TestQueryResourceLimitsEndToEnd(t *testing.T) {
	if os.Getenv("SNUFFLE_E2E") != "1" {
		t.Skip("set SNUFFLE_E2E=1 to run the ClickHouse e2e test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const timeout = time.Second
	cfg, client, baseURL := startIsolationE2E(t, ctx, func(cfg *Config) {
		cfg.QueryTimeout = timeout
		cfg.LogQueryMaxRows = 1000
	})
	logStart := time.Now().UTC().Truncate(time.Minute).Add(-8 * time.Minute)
	isoWriteTeam(t, ctx, cfg, client, baseURL, isoTeamA, logStart)
	from := strconv.FormatInt(logStart.Add(-2*time.Minute).UnixNano(), 10)
	to := strconv.FormatInt(logStart.Add(8*time.Minute).UnixNano(), 10)

	send := func(t *testing.T, method, path string, values url.Values) (int, int, time.Duration) {
		t.Helper()
		var req *http.Request
		if method == http.MethodPost {
			req, _ = http.NewRequest(method, baseURL+path, strings.NewReader(values.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			req, _ = http.NewRequest(method, baseURL+path+"?"+values.Encode(), nil)
		}
		req.Header.Set("X-Team-ID", strconv.FormatUint(isoTeamA, 10))
		started := time.Now()
		resp, err := (&http.Client{Timeout: timeout + 10*time.Second}).Do(req)
		if err != nil {
			return 0, 0, time.Since(started)
		}
		defer resp.Body.Close()
		n, _ := io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, int(n), time.Since(started)
	}

	t.Run("line_format templates stop at the query timeout", func(t *testing.T) {
		// 2e8 template steps per log line, several seconds for three lines. The
		// handler keeps running after the client gives up, so the server's
		// Close at cleanup waits for it.
		query := `{service_name="iso-svc"} | line_format "{{range 14000}}{{range 14000}}{{end}}{{end}}"`
		status, _, took := send(t, http.MethodGet, "/loki/api/v1/query_range", url.Values{"query": {query}, "start": {from}, "end": {to}, "limit": {"1"}})
		if took > timeout+2*time.Second {
			t.Errorf("line_format loop answered %d after %s, want at most the %s query timeout", status, took.Round(time.Millisecond), timeout)
		}
	})
	t.Run("line_format output is bounded", func(t *testing.T) {
		// Each replace multiplies the line by 11, so six of them turn the three
		// ~50 byte lines into ~270MB of response.
		expr := ".msg"
		for i := 0; i < 6; i++ {
			expr = `(replace "" "0123456789" ` + expr + `)`
		}
		query := `{service_name="iso-svc"} | json | line_format "{{ ` + strings.ReplaceAll(expr, `"`, `\"`) + ` }}"`
		status, size, took := send(t, http.MethodGet, "/loki/api/v1/query_range", url.Values{"query": {query}, "start": {from}, "end": {to}, "limit": {"10"}})
		if status == http.StatusOK && size > 16<<20 {
			t.Errorf("a %d byte query produced a %d byte response in %s", len(query), size, took.Round(time.Millisecond))
		}
	})
	t.Run("LogQL parsing stops at the query timeout", func(t *testing.T) {
		// ~200KB of nesting.
		query := strings.Repeat("sum(", 40000) + `count_over_time({service_name="iso-svc"}[1m])` + strings.Repeat(")", 40000)
		status, _, took := send(t, http.MethodPost, "/loki/api/v1/query", url.Values{"query": {query}, "time": {to}})
		if took > timeout+2*time.Second {
			t.Errorf("a %d byte LogQL query answered %d after %s, want at most the %s query timeout", len(query), status, took.Round(time.Millisecond), timeout)
		}
	})
	t.Run("PromQL range queries cap their resolution", func(t *testing.T) {
		// Prometheus refuses more than 11,000 points per series; this asks for 1e6.
		status, size, took := send(t, http.MethodGet, "/api/v1/query_range", url.Values{"query": {"rate(iso_requests_total[1m])"}, "start": {strconv.FormatInt(isoEval-1_000_000, 10)}, "end": {strconv.FormatInt(isoEval, 10)}, "step": {"1"}})
		if status != http.StatusBadRequest {
			t.Errorf("a 1e6 point range query answered %d with %d bytes in %s, want 400", status, size, took.Round(time.Millisecond))
		}
	})
	for name, query := range map[string]string{
		// The Prometheus parser recomputes each node's type from its children, so
		// deep trees cost quadratic time before the timeout starts. The cost
		// follows the operator count; these are 200KB and 80KB.
		"nested": strings.Repeat("sum(", 40000) + "m" + strings.Repeat(")", 40000),
		"binary": "m" + strings.Repeat(" + m", 20000),
	} {
		t.Run("PromQL parsing stops at the query timeout/"+name, func(t *testing.T) {
			status, _, took := send(t, http.MethodPost, "/api/v1/query", url.Values{"query": {query}, "time": {strconv.FormatInt(isoEval, 10)}})
			if took > timeout+2*time.Second {
				t.Errorf("a %d byte PromQL query answered %d after %s, want at most the %s query timeout", len(query), status, took.Round(time.Millisecond), timeout)
			}
		})
	}
	t.Run("LogQL metrics evaluated in Go stop at the query timeout", func(t *testing.T) {
		// A parser stage keeps the query off the SQL path. Every row is in every
		// one-day window, so 10,800 points re-run the pipeline over every row.
		now := time.Now().UTC()
		var values [][]any
		for i := 0; i < 3000; i++ {
			values = append(values, []any{strconv.FormatInt(now.Add(-time.Hour+time.Duration(i)*time.Second).UnixNano(), 10), fmt.Sprintf(`{"n":%d}`, i)})
		}
		isoLokiPush(t, baseURL, fmt.Sprintf("/t/%d", isoTeamA), lokiPushRequest{Streams: []lokiPushStream{{Stream: map[string]string{"service_name": "iso-bulk"}, Values: values}}})
		status, _, took := send(t, http.MethodGet, "/loki/api/v1/query_range", url.Values{
			"query": {`count_over_time({service_name="iso-bulk"} | json [1d])`},
			"start": {strconv.FormatInt(now.Add(-3*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"1"},
		})
		if took > timeout+2*time.Second {
			t.Errorf("Go-side LogQL evaluation answered %d after %s, want at most the %s query timeout", status, took.Round(time.Millisecond), timeout)
		}
	})
	t.Run("LogQL rows are bounded across selectors", func(t *testing.T) {
		// Each distinct selector reads up to LogQueryMaxRows (1000 here), so
		// twenty of them over 800 10KB lines would hold 160MB.
		now := time.Now().UTC()
		var values [][]any
		for i := 0; i < 800; i++ {
			values = append(values, []any{strconv.FormatInt(now.Add(-time.Hour+time.Duration(i)*time.Second).UnixNano(), 10), strings.Repeat("x", 10<<10)})
		}
		isoLokiPush(t, baseURL, fmt.Sprintf("/t/%d", isoTeamA), lokiPushRequest{Streams: []lokiPushStream{{Stream: map[string]string{"service_name": "iso-sel"}, Values: values}}})
		// Right-nested: the parser rejects a flat chain of three or more terms.
		query := `sum(count_over_time({service_name="iso-sel"} != "y0" [1d]))`
		for i := 1; i < 20; i++ {
			query = fmt.Sprintf(`sum(count_over_time({service_name="iso-sel"} != "y%d" [1d])) + (%s)`, i, query)
		}
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		status, _, _ := send(t, http.MethodGet, "/loki/api/v1/query", url.Values{"query": {query}, "time": {strconv.FormatInt(now.UnixNano(), 10)}})
		runtime.ReadMemStats(&after)
		// Under the shared limit the 1000 rows cost ~65MB with decoding; before it
		// this query allocated ~1.4GB.
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 160<<20 {
			t.Errorf("twenty selectors answered %d after allocating %dMB, want the rows bounded by LogQueryMaxRows overall", status, grew>>20)
		}
	})
	for _, path := range []string{"/api/v1/write", "/api/v1/read"} {
		t.Run(path+" bounds the decoded body", func(t *testing.T) {
			// A snappy block that claims to decode to 1GiB, in ten bytes.
			body := append(binary.AppendUvarint(nil, 1<<30), 0, 0, 0, 0, 0)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			req, _ := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("X-Team-ID", strconv.FormatUint(isoTeamA, 10))
			isoDo(t, req)
			runtime.ReadMemStats(&after)
			if grew := after.TotalAlloc - before.TotalAlloc; grew > 256<<20 {
				t.Errorf("a %d byte %s body made Snuffle allocate %dMB", len(body), path, grew>>20)
			}
		})
	}
	t.Run("Loki push bounds the decompressed body", func(t *testing.T) {
		// 120MB of log line that gzips to about 120KB.
		payload, _ := json.Marshal(lokiPushRequest{Streams: []lokiPushStream{{
			Stream: map[string]string{"service_name": "iso-bomb"},
			Values: [][]any{{strconv.FormatInt(time.Now().UnixNano(), 10), strings.Repeat("a", 120<<20)}},
		}}})
		var compressed bytes.Buffer
		gz := gzip.NewWriter(&compressed)
		_, _ = gz.Write(payload)
		_ = gz.Close()
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/t/%d/loki/api/v1/push", baseURL, isoTeamA), &compressed)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("a gzip push that inflates to %dMB answered %d, want 413", len(payload)>>20, resp.StatusCode)
		}
	})
	t.Run("LogQL rejects a zero step", func(t *testing.T) {
		status, _, _ := send(t, http.MethodGet, "/loki/api/v1/query_range", url.Values{"query": {`count_over_time({service_name="iso-svc"}[5m])`}, "start": {from}, "end": {to}, "step": {"0.0000000001"}})
		if status != http.StatusBadRequest {
			t.Errorf("step=1e-10 answered %d, want 400 (0 means the handler panicked)", status)
		}
	})
}
