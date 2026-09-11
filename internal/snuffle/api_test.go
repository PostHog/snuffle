package snuffle

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func TestParseTeamPath(t *testing.T) {
	teamID, path, err := parseTeamPath("/t/42/api/v1/query")
	if err != nil {
		t.Fatalf("parseTeamPath returned error: %v", err)
	}
	if teamID != 42 || path != "/api/v1/query" {
		t.Fatalf("parseTeamPath = (%d, %q), want (42, /api/v1/query)", teamID, path)
	}

	teamID, path, err = parseTeamPath("/team/7/api/v1/read")
	if err != nil {
		t.Fatalf("parseTeamPath returned error: %v", err)
	}
	if teamID != 7 || path != "/api/v1/read" {
		t.Fatalf("parseTeamPath = (%d, %q), want (7, /api/v1/read)", teamID, path)
	}
}

func TestTeamIDFromRequest(t *testing.T) {
	server := &Server{cfg: Config{
		DefaultTeamID:  3,
		TeamHeader:     "X-Scope-OrgID",
		TeamQueryParam: "tenant",
	}}

	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	if got, err := server.teamIDFromRequest(req); err != nil || got != 3 {
		t.Fatalf("default team = (%d, %v), want 3", got, err)
	}

	req = httptest.NewRequest("GET", "/api/v1/query?tenant=5", nil)
	if got, err := server.teamIDFromRequest(req); err != nil || got != 5 {
		t.Fatalf("query team = (%d, %v), want 5", got, err)
	}

	req = httptest.NewRequest("GET", "/api/v1/query?tenant=5", nil)
	req.Header.Set("X-Scope-OrgID", "9")
	if got, err := server.teamIDFromRequest(req); err != nil || got != 9 {
		t.Fatalf("header team = (%d, %v), want 9", got, err)
	}
}

func TestClickHouseCredentialsPassThrough(t *testing.T) {
	for _, tc := range []struct {
		name          string
		fallback      bool
		basicUser     string
		basicPassword string
		haveBasic     bool
		wantUser      string
		wantPassword  string
	}{
		{name: "request credentials", fallback: true, basicUser: "alice", basicPassword: "secret", haveBasic: true, wantUser: "alice", wantPassword: "secret"},
		{name: "passwordless request credentials", fallback: true, basicUser: "alice", haveBasic: true, wantUser: "alice"},
		{name: "missing credentials", wantUser: "", wantPassword: ""},
		{name: "configured fallback", fallback: true, wantUser: "configured-user", wantPassword: "configured-password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newServer(Config{
				CHUser:               "configured-user",
				CHPassword:           "configured-password",
				AllowUnauthenticated: tc.fallback,
			})
			handler := server.clickHouseAuthHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestConn, ok := r.Context().Value(requestClickHouseConnectionKey{}).(*requestClickHouseConnection)
				if !ok {
					t.Fatal("request did not contain ClickHouse credentials")
				}
				if requestConn.username != tc.wantUser || requestConn.password != tc.wantPassword {
					t.Fatalf("ClickHouse credentials = (%q, %q), want (%q, %q)", requestConn.username, requestConn.password, tc.wantUser, tc.wantPassword)
				}
			}))

			req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
			if tc.haveBasic {
				req.SetBasicAuth(tc.basicUser, tc.basicPassword)
			}
			handler.ServeHTTP(httptest.NewRecorder(), req)
		})
	}
}

func TestBuildInfoEndpoint(t *testing.T) {
	// Grafana calls /api/v1/status/buildinfo on datasource "Save & Test" and
	// semver-parses data.version to toggle features; it must be served on the
	// Prometheus surface (Loki has its own) and carry a parseable version.
	server := newServer(Config{})
	mux := http.NewServeMux()
	server.routes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/status/buildinfo", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("buildinfo status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"success"`) {
		t.Fatalf("buildinfo body missing success envelope: %s", body)
	}
	if !strings.Contains(body, `"version":"3.11.0"`) {
		t.Fatalf("buildinfo must report the vendored Prometheus engine version 3.11.0: %s", body)
	}
	// No team header or path needed — Grafana calls this before any
	// team-scoped request, so it must not 400 for a missing tenant.
}

func TestPostHogMetricTypeToPrometheus(t *testing.T) {
	// OTel metric_type + is_monotonic on metric_series maps onto the Prometheus
	// family type Grafana expects from /api/v1/metadata.
	cases := []struct {
		metricType string
		monotonic  bool
		want       string
	}{
		{"sum", true, "counter"},
		{"sum", false, "gauge"},
		{"gauge", false, "gauge"},
		{"histogram", false, "histogram"},
		{"exponential_histogram", false, "histogram"},
		{"summary", false, "summary"},
		{"mystery", false, "unknown"},
	}
	for _, tc := range cases {
		if got := postHogMetricTypeToPrometheus(tc.metricType, tc.monotonic); got != tc.want {
			t.Fatalf("postHogMetricTypeToPrometheus(%q, %v) = %q, want %q", tc.metricType, tc.monotonic, got, tc.want)
		}
	}
}

func TestPostHogMetadataSQL(t *testing.T) {
	cfg := Config{
		CHDatabase:        "posthog",
		SchemaLayout:      "posthog",
		MetricSeriesTable: "metric_series",
		TeamID:            7,
	}
	sql := postHogMetadataSQL(cfg, "", 100)
	for _, want := range []string{
		"`posthog`.`metric_series`",
		"team_id = 7",
		"argMax(metric_type, last_seen)",
		"argMax(is_monotonic, last_seen)",
		"argMax(unit, last_seen)",
		"GROUP BY metric_name",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("posthog metadata SQL missing %q:\n%s", want, sql)
		}
	}

	sql = postHogMetadataSQL(cfg, "http_requests_total", 100)
	if !strings.Contains(sql, "metric_name = 'http_requests_total'") {
		t.Fatalf("metadata SQL should filter by metric when given:\n%s", sql)
	}
}

func TestTeamSourceHeaderMode(t *testing.T) {
	server := &Server{cfg: Config{
		DefaultTeamID:  3,
		TeamHeader:     "X-Team-ID",
		TeamQueryParam: "team_id",
		TeamSource:     "header",
	}}

	// Header wins.
	req := httptest.NewRequest("GET", "/api/v1/query?team_id=5", nil)
	req.Header.Set("X-Team-ID", "9")
	if got, err := server.teamIDFromRequest(req); err != nil || got != 9 {
		t.Fatalf("header team = (%d, %v), want 9", got, err)
	}

	// Query param and default are ignored in header mode; missing header errors.
	req = httptest.NewRequest("GET", "/api/v1/query?team_id=5", nil)
	if _, err := server.teamIDFromRequest(req); err == nil {
		t.Fatal("header mode must reject a request without the team header")
	}

	// The /t/ and /team/ path prefixes are not registered in header mode.
	mux := http.NewServeMux()
	server.routes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/t/42/api/v1/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/t/ in header mode = %d, want 404", rec.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	server := newServer(Config{})
	mux := http.NewServeMux()
	server.routes(mux)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "snuffle_self_scrape_last_timestamp_seconds") {
		t.Fatalf("/metrics body did not include self-scrape metric")
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("/metrics body did not include Go runtime metrics")
	}
}

func TestGzipJSONHandlerCompressesJSON(t *testing.T) {
	handler := gzipJSONHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()
	body, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Fatalf("decoded body = %q", string(body))
	}
}

func TestGzipJSONHandlerSkipsNonJSON(t *testing.T) {
	handler := gzipJSONHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/read", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if rec.Body.String() != "payload" {
		t.Fatalf("body = %q, want payload", rec.Body.String())
	}
}

func TestSortedSeriesLabelMapsOrdersByPrometheusLabels(t *testing.T) {
	apiLabels := map[string]string{labels.MetricName: "up", "job": "api"}
	workerLabels := map[string]string{labels.MetricName: "up", "job": "worker"}
	seen := map[uint64]*seriesMeta{
		100: {id: 100, labelMap: workerLabels, labels: labels.FromMap(workerLabels)},
		200: {id: 200, labelMap: apiLabels, labels: labels.FromMap(apiLabels)},
	}

	got := sortedSeriesLabelMaps(seen)
	if len(got) != 2 || got[0]["job"] != "api" || got[1]["job"] != "worker" {
		t.Fatalf("sortedSeriesLabelMaps = %#v", got)
	}
}

func TestSortedSeriesJSONRowsOrdersByPrometheusLabels(t *testing.T) {
	apiLabels := map[string]string{labels.MetricName: "up", "job": "api"}
	workerLabels := map[string]string{labels.MetricName: "up", "job": "worker"}
	seen := map[uint64]seriesJSONRow{
		100: {id: 100, labels: `{"__name__":"up","job":"worker"}`, labelSet: labels.FromMap(workerLabels)},
		200: {id: 200, labels: `{"__name__":"up","job":"api"}`, labelSet: labels.FromMap(apiLabels)},
	}

	got := sortedSeriesJSONRows(seen)
	if len(got) != 2 || got[0].labels != `{"__name__":"up","job":"api"}` || got[1].labels != `{"__name__":"up","job":"worker"}` {
		t.Fatalf("sortedSeriesJSONRows = %#v", got)
	}
}
