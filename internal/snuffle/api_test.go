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

func TestEndpointPathsDefaultRemoteWrite(t *testing.T) {
	mux := http.NewServeMux()
	(&Server{}).routes(mux)

	if status := routeStatus(mux, http.MethodPost, "/write"); status != http.StatusBadRequest {
		t.Fatalf("POST /write status = %d, want %d", status, http.StatusBadRequest)
	}
	if status := routeStatus(mux, http.MethodPost, "/api/v1/write"); status != http.StatusNotFound {
		t.Fatalf("POST /api/v1/write status = %d, want %d", status, http.StatusNotFound)
	}
	if status := routeStatus(mux, http.MethodGet, "/api/v1/query"); status != http.StatusBadRequest {
		t.Fatalf("GET /api/v1/query status = %d, want %d", status, http.StatusBadRequest)
	}
	if status := routeStatus(mux, http.MethodPost, "/t/42/write"); status != http.StatusBadRequest {
		t.Fatalf("POST /t/42/write status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestEndpointPathsCustom(t *testing.T) {
	server := &Server{cfg: Config{
		APIPathPrefix:   "prometheus",
		RemoteWritePath: "ingest",
	}}
	mux := http.NewServeMux()
	server.routes(mux)

	if status := routeStatus(mux, http.MethodGet, "/prometheus/query"); status != http.StatusBadRequest {
		t.Fatalf("GET /prometheus/query status = %d, want %d", status, http.StatusBadRequest)
	}
	if status := routeStatus(mux, http.MethodPost, "/ingest"); status != http.StatusBadRequest {
		t.Fatalf("POST /ingest status = %d, want %d", status, http.StatusBadRequest)
	}
	if status := routeStatus(mux, http.MethodPost, "/t/42/ingest"); status != http.StatusBadRequest {
		t.Fatalf("POST /t/42/ingest status = %d, want %d", status, http.StatusBadRequest)
	}
	if status := routeStatus(mux, http.MethodGet, "/api/v1/query"); status != http.StatusNotFound {
		t.Fatalf("GET /api/v1/query status = %d, want %d", status, http.StatusNotFound)
	}
}

func routeStatus(handler http.Handler, method, path string) int {
	req := httptest.NewRequest(method, path, strings.NewReader("not-snappy"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Code
}
