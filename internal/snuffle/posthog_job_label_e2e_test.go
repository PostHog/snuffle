package snuffle

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// PostHog capture writes a remote-written job label to service_name only.
// PromQL must still see job.
func TestPostHogJobLabelEndToEnd(test *testing.T) {
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
	database := fmt.Sprintf("snuffle_job_e2e_%d", time.Now().UnixNano())
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

	const metric = "kube_pod_info"
	insert := func(service string, attributes map[string]string) {
		test.Helper()
		pairs := make([]string, 0, len(attributes)*2)
		for _, key := range slices.Sorted(maps.Keys(attributes)) {
			pairs = append(pairs, sqlString(key), sqlString(attributes[key]))
		}
		fingerprint := service + "|" + strings.Join(pairs, "|")
		for index := range 3 {
			sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, value, count, has_labels, resource_attributes, attributes)
				SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, %s, 'gauge', 1, 1, true, map('service.name', %s), map(%s)`,
				tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(metric), sqlString(fingerprint), chTimeMillis(e2eStartMS+int64(index)*30000), sqlString(service), sqlString(service), strings.Join(pairs, ", "))
			if err := client.Exec(ctx, sql); err != nil {
				test.Fatal(err)
			}
		}
	}
	// Remote-written series: capture moved job into service_name.
	insert("kube-state-metrics", map[string]string{"namespace": "wiz"})
	insert("kube-state-metrics", map[string]string{"namespace": "posthog"})
	// A series with its own job attribute keeps it.
	insert("checkout", map[string]string{"namespace": "shop", "job": "batch"})

	mux := http.NewServeMux()
	newServer(cfg).routes(mux)
	api := httptest.NewServer(mux)
	defer api.Close()
	window := url.Values{"start": {"1700000010"}, "end": {"1700000070"}}
	with := func(values url.Values, key, value string) url.Values {
		out := maps.Clone(values)
		out.Set(key, value)
		return out
	}
	counts := func(test *testing.T, expression string) map[string]string {
		test.Helper()
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {expression}, "time": {"1700000070"}})
		out := make(map[string]string, len(result.Result))
		for _, series := range result.Result {
			out[series.Metric["job"]] = sampleString(series.Value)
		}
		return out
	}

	for expression, want := range map[string]map[string]string{
		`count(kube_pod_info{job="kube-state-metrics"})`:  {"": "2"},
		`count(kube_pod_info{job="batch"})`:               {"": "1"},
		`kube_pod_info{job="checkout"}`:                   {},
		`count(kube_pod_info{job=~"kube-.*"})`:            {"": "2"},
		`count(kube_pod_info{job!="kube-state-metrics"})`: {"": "1"},
		`count by (job) (kube_pod_info)`:                  {"kube-state-metrics": "2", "batch": "1"},
		`sum by (job) (rate(kube_pod_info[1m]))`:          {"kube-state-metrics": "0", "batch": "0"},
	} {
		if got := counts(test, expression); !reflect.DeepEqual(got, want) {
			test.Errorf("%s = %v, want %v", expression, got, want)
		}
	}

	series := apiGet[[]map[string]string](test, api.URL, "/api/v1/series", with(window, "match[]", `kube_pod_info{job="kube-state-metrics"}`))
	if len(series) != 2 || series[0]["job"] != "kube-state-metrics" || series[0]["service_name"] != "kube-state-metrics" {
		test.Errorf("series = %v, want two kube-state-metrics series with job", series)
	}
	if names := apiGet[[]string](test, api.URL, "/api/v1/labels", with(window, "match[]", metric)); !slices.Contains(names, "job") {
		test.Errorf("label names %v do not contain job", names)
	}
	if names := apiGet[[]string](test, api.URL, "/api/v1/labels", window); !slices.Contains(names, "job") {
		test.Errorf("label names %v do not contain job", names)
	}
	for name, values := range map[string]url.Values{
		"all":         window,
		"metric":      with(window, "match[]", metric),
		"job matcher": with(window, "match[]", `kube_pod_info{job=~".+"}`),
	} {
		got := apiGet[[]string](test, api.URL, "/api/v1/label/job/values", values)
		if want := []string{"batch", "kube-state-metrics"}; !reflect.DeepEqual(got, want) {
			test.Errorf("%s job values = %v, want %v", name, got, want)
		}
	}
}
