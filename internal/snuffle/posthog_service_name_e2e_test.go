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

// PostHog capture writes a remote-written job to the service_name column and
// keeps a scraped service_name label as an attribute. PromQL must see the
// scraped service_name, and OTLP series without the attribute must still read
// the column.
func TestPostHogScrapedServiceNameEndToEnd(test *testing.T) {
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
	database := fmt.Sprintf("snuffle_service_name_e2e_%d", time.Now().UnixNano())
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

	const metric = "batch_exports_request_latency_sum"
	insert := func(service string, attributes map[string]string) {
		test.Helper()
		pairs := make([]string, 0, len(attributes)*2)
		for _, key := range slices.Sorted(maps.Keys(attributes)) {
			pairs = append(pairs, sqlString(key), sqlString(attributes[key]))
		}
		fingerprint := service + "|" + strings.Join(pairs, "|")
		for index := range 3 {
			sql := fmt.Sprintf(`INSERT INTO %s (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, value, count, has_labels, resource_attributes, attributes)
				SELECT %d, %s, cityHash64(%s), %s, now64(6), now64(6) + INTERVAL 1 DAY, %s, 'gauge', %d, 1, true, map('service.name', %s), map(%s)`,
				tableName(database, cfg.MetricsInputTable), e2eTeamID, sqlString(metric), sqlString(fingerprint), chTimeMillis(e2eStartMS+int64(index)*30000), sqlString(service), index, sqlString(service), strings.Join(pairs, ", "))
			if err := client.Exec(ctx, sql); err != nil {
				test.Fatal(err)
			}
		}
	}
	// Scraped series: job went to the column, service_name stayed an attribute.
	insert("kubernetes-pods", map[string]string{"service_name": "temporal-core-sdk", "pod": "worker-a"})
	insert("kubernetes-pods", map[string]string{"service_name": "temporal-core-sdk", "pod": "worker-b"})
	// A scraped series without its own service_name.
	insert("kubernetes-pods", map[string]string{"pod": "web-a"})
	// An OTLP series: service_name is the column only.
	insert("checkout", map[string]string{"status": "200"})

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
	instant := func(test *testing.T, expression string) map[string]string {
		test.Helper()
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query", url.Values{"query": {expression}, "time": {"1700000070"}})
		out := make(map[string]string, len(result.Result))
		for _, series := range result.Result {
			out[series.Metric["service_name"]] = sampleString(series.Value)
		}
		return out
	}
	ranged := func(test *testing.T, expression string) map[string]string {
		test.Helper()
		result := apiGet[queryDataDTO](test, api.URL, "/api/v1/query_range", url.Values{"query": {expression}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"30s"}})
		out := make(map[string]string, len(result.Result))
		for _, series := range result.Result {
			if len(series.Values) > 0 {
				out[series.Metric["service_name"]] = sampleString(series.Values[len(series.Values)-1])
			}
		}
		return out
	}

	all := map[string]string{"temporal-core-sdk": "2", "kubernetes-pods": "1", "checkout": "1"}
	for expression, want := range map[string]map[string]string{
		`count(` + metric + `{service_name="temporal-core-sdk"})`:                                                             {"": "2"},
		`count(` + metric + `{service_name="kubernetes-pods"})`:                                                               {"": "1"},
		`count(` + metric + `{service_name="checkout"})`:                                                                      {"": "1"},
		`count(` + metric + `{service_name=~"temporal-.*"})`:                                                                  {"": "2"},
		`count(` + metric + `{service_name!="kubernetes-pods"})`:                                                              {"": "3"},
		`count by (service_name) (` + metric + `)`:                                                                            all,
		`sum by (service_name) (` + metric + `)`:                                                                              {"temporal-core-sdk": "4", "kubernetes-pods": "2", "checkout": "2"},
		`sum by (service_name) (rate(` + metric + `[1m]))`:                                                                    {"temporal-core-sdk": "0.06666666666666667", "kubernetes-pods": "0.03333333333333333", "checkout": "0.03333333333333333"},
		`sum by (service_name) (` + metric + `{service_name="temporal-core-sdk"} or ` + metric + `{service_name="checkout"})`: {"temporal-core-sdk": "4", "checkout": "2"},
		`topk(10, ` + metric + `{service_name="temporal-core-sdk"})`:                                                          {"temporal-core-sdk": "2"},
	} {
		if got := instant(test, expression); !reflect.DeepEqual(got, want) {
			test.Errorf("%s = %v, want %v", expression, got, want)
		}
	}
	for expression, want := range map[string]map[string]string{
		`count by (service_name) (` + metric + `)`:                           all,
		`sum by (service_name) (increase(` + metric + `[1m]))`:               {"temporal-core-sdk": "4", "kubernetes-pods": "2", "checkout": "2"},
		`count(` + metric + `{service_name="temporal-core-sdk"})`:            {"": "2"},
		`count by (service_name) (` + metric + `{service_name!="checkout"})`: {"temporal-core-sdk": "2", "kubernetes-pods": "1"},
	} {
		if got := ranged(test, expression); !reflect.DeepEqual(got, want) {
			test.Errorf("range %s = %v, want %v", expression, got, want)
		}
	}

	series := apiGet[[]map[string]string](test, api.URL, "/api/v1/series", with(window, "match[]", metric+`{service_name="temporal-core-sdk"}`))
	if len(series) != 2 || series[0]["service_name"] != "temporal-core-sdk" || series[1]["service_name"] != "temporal-core-sdk" {
		test.Errorf("series = %v, want two temporal-core-sdk series", series)
	}
	for name, values := range map[string]url.Values{
		"all":    window,
		"metric": with(window, "match[]", metric),
	} {
		got := apiGet[[]string](test, api.URL, "/api/v1/label/service_name/values", values)
		if want := []string{"checkout", "kubernetes-pods", "temporal-core-sdk"}; !reflect.DeepEqual(got, want) {
			test.Errorf("%s service_name values = %v, want %v", name, got, want)
		}
	}
	pods := apiGet[[]string](test, api.URL, "/api/v1/label/pod/values", with(window, "match[]", metric+`{service_name="temporal-core-sdk"}`))
	if !slices.Contains(pods, "worker-a") || !slices.Contains(pods, "worker-b") {
		test.Errorf("pod values for temporal-core-sdk = %v, want worker-a and worker-b", pods)
	}
	names := apiGet[[]string](test, api.URL, "/api/v1/labels", with(window, "match[]", metric+`{service_name="temporal-core-sdk"}`))
	if !slices.Contains(names, "pod") || !slices.Contains(names, "service_name") {
		test.Errorf("label names for temporal-core-sdk = %v, want pod and service_name", names)
	}
}
