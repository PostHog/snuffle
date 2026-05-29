package snuffle

import (
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
