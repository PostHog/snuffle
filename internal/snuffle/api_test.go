package snuffle

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
