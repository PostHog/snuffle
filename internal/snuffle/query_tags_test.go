package snuffle

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dto "github.com/prometheus/client_model/go"
)

func TestRouteQueryTagsLogComment(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		want     string
	}{
		{"/api/v1/query_range", `{"product":"metrics","query_type":"query_range","service_name":"snuffle","team_id":42}`},
		{"/api/v1/label/:name/values", `{"product":"metrics","query_type":"label_values","service_name":"snuffle","team_id":42}`},
		{"/api/v1/write", `{"product":"metrics","query_type":"remote_write","service_name":"snuffle","team_id":42}`},
		{"/loki/api/v1/query_range", `{"product":"logs","query_type":"loki_query_range","service_name":"snuffle","team_id":42}`},
		{"/loki/api/v1/push", `{"product":"logs","query_type":"loki_push","service_name":"snuffle","team_id":42}`},
	} {
		if got := routeQueryTags(tc.endpoint, 42).logComment(); got != tc.want {
			t.Errorf("%s log_comment = %s, want %s", tc.endpoint, got, tc.want)
		}
	}
}

func TestLogCommentDefaultsWithoutRequestTags(t *testing.T) {
	if got := logCommentFromContext(context.Background()); got != `{"service_name":"snuffle"}` {
		t.Fatalf("default log_comment = %s", got)
	}
}

func TestParseCallerQueryTagsKeepsAllowlistedTypedKeys(t *testing.T) {
	tags, rejected := parseCallerQueryTags(`{
		"user_id": 7,
		"org_id": "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b",
		"access_method": "personal_api_key",
		"route_id": null,
		"kind": "request",
		"is_impersonated": true,
		"dashboard_id": "12",
		"insight_id": 1.5,
		"workload": 3,
		"scene": "Metrics",
		"query": {"kind": "HogQLQuery"}
	}`)
	want := map[string]any{
		"user_id":         int64(7),
		"org_id":          "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b",
		"access_method":   "personal_api_key",
		"kind":            "request",
		"is_impersonated": true,
	}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags = %#v, want %#v", tags, want)
	}
	sort.Strings(rejected)
	wantRejected := []string{"invalid_type", "invalid_type", "invalid_type", "unknown_key", "unknown_key"}
	if !reflect.DeepEqual(rejected, wantRejected) {
		t.Fatalf("rejected = %v, want %v", rejected, wantRejected)
	}
}

func TestParseCallerQueryTagsRejectsMalformedHeaders(t *testing.T) {
	for _, header := range []string{`{`, `[1]`, `"user_id"`, `{"user_id": 1} {}`} {
		tags, rejected := parseCallerQueryTags(header)
		if tags != nil || !reflect.DeepEqual(rejected, []string{"invalid_json"}) {
			t.Errorf("parseCallerQueryTags(%q) = (%v, %v), want invalid_json", header, tags, rejected)
		}
	}
}

func TestParseCallerQueryTagsSizeCap(t *testing.T) {
	header := func(size int) string {
		prefix, suffix := `{"name":"`, `"}`
		return prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
	}
	if tags, rejected := parseCallerQueryTags(header(maxQueryTagsHeaderBytes)); len(tags) != 1 || len(rejected) != 0 {
		t.Fatalf("header at the cap = (%d tags, %v), want 1 tag", len(tags), rejected)
	}
	tags, rejected := parseCallerQueryTags(header(maxQueryTagsHeaderBytes + 1))
	if tags != nil || !reflect.DeepEqual(rejected, []string{"too_large"}) {
		t.Fatalf("header over the cap = (%v, %v), want too_large", tags, rejected)
	}
}

func TestCallerQueryTagsCannotOverrideSnuffleKeys(t *testing.T) {
	caller, rejected := parseCallerQueryTags(`{"team_id": 999, "service_name": "app", "query_type": "insert", "product": "dashboards", "user_id": 7}`)
	if len(rejected) != 3 {
		t.Fatalf("rejected = %v, want three reserved keys", rejected)
	}
	tags := routeQueryTags("/api/v1/query", 42)
	tags.caller = caller
	tags.caller["team_id"] = 999 // even a tag that slipped past parsing loses
	var got map[string]any
	if err := json.Unmarshal([]byte(tags.logComment()), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"team_id": float64(42), "service_name": "snuffle", "query_type": "query", "product": "dashboards", "user_id": float64(7)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("log_comment = %v, want %v", got, want)
	}
}

func TestQueryTagsReachEveryClickHouseStatement(t *testing.T) {
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	recorder := &logCommentRecorder{}
	previousOpen := openClickHouse
	openClickHouse = func(Config, string, string) (clickhouse.Conn, error) { return recorder, nil }
	t.Cleanup(func() { openClickHouse = previousOpen })

	for _, layout := range []string{"current", "posthog"} {
		t.Setenv("CH_SCHEMA_LAYOUT", layout)
		server := newServer(ConfigFromEnv())
		mux := http.NewServeMux()
		server.routes(mux)
		for _, tc := range []struct {
			path, query, want string
		}{
			{"/t/42/api/v1/query_range", `sum(rate(m{job="a"}[5m])) / sum(rate(m[5m]))`, `{"product":"metrics","query_type":"query_range","service_name":"snuffle","team_id":42,"user_id":7}`},
			{"/t/42/api/v1/query", `count(count by (job) (m))`, `{"product":"metrics","query_type":"query","service_name":"snuffle","team_id":42,"user_id":7}`},
			{"/t/42/loki/api/v1/query_range", `sum(count_over_time({app="api"} |= "x" [1m]))`, `{"product":"logs","query_type":"loki_query_range","service_name":"snuffle","team_id":42,"user_id":7}`},
		} {
			recorder.reset()
			params := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}
			req := httptest.NewRequest(http.MethodGet, tc.path+"?"+params.Encode(), nil)
			req.Header.Set("X-Team-ID", "999")
			req.Header.Set("X-PostHog-Query-Tags", `{"user_id": 7, "team_id": 999}`)
			mux.ServeHTTP(httptest.NewRecorder(), req)
			comments := recorder.recorded()
			if len(comments) == 0 {
				t.Fatalf("%s %s: no ClickHouse statements recorded", layout, tc.path)
			}
			for _, comment := range comments {
				if comment != tc.want {
					t.Fatalf("%s %s: log_comment = %s, want %s", layout, tc.path, comment, tc.want)
				}
			}
		}
		if got := counterValue(t, server.metrics, "reserved_key"); got != 3 {
			t.Fatalf("%s reserved_key rejections = %v, want 3", layout, got)
		}
	}
}

func counterValue(t *testing.T, metrics *bridgeMetrics, reason string) float64 {
	t.Helper()
	var metric dto.Metric
	if err := metrics.queryTagsRejected.WithLabelValues(reason).Write(&metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

// logCommentRecorder records the log_comment each statement would carry.
type logCommentRecorder struct {
	driver.Conn
	mu       sync.Mutex
	comments []string
}

func (r *logCommentRecorder) record(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.comments = append(r.comments, logCommentFromContext(ctx))
}

func (r *logCommentRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.comments = nil
}

func (r *logCommentRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.comments...)
}

func (r *logCommentRecorder) Query(ctx context.Context, _ string, _ ...any) (driver.Rows, error) {
	r.record(ctx)
	return emptyRows{}, nil
}

func (r *logCommentRecorder) Exec(ctx context.Context, _ string, _ ...any) error {
	r.record(ctx)
	return nil
}

func (r *logCommentRecorder) Close() error { return nil }

func TestCallerQueryTagsHeaderCanBeDisabled(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	req.Header.Set("X-PostHog-Query-Tags", `{"user_id": 7}`)
	if tags := (&Server{cfg: Config{}}).callerQueryTags(req); tags != nil {
		t.Fatalf("disabled header tags = %v, want none", tags)
	}
	if tags := (&Server{cfg: Config{QueryTagsHeader: "X-PostHog-Query-Tags"}}).callerQueryTags(req); tags["user_id"] != int64(7) {
		t.Fatalf("enabled header tags = %v, want user_id 7", tags)
	}
}
