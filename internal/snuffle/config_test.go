package snuffle

import (
	"testing"
	"time"
)

func TestConfigFromEnvRemoteWriteInterval(t *testing.T) {
	t.Setenv("REMOTE_WRITE_SAMPLE_INTERVAL", "")
	if got := ConfigFromEnv().RemoteWriteInterval; got != 15*time.Second {
		t.Fatalf("default RemoteWriteInterval = %s, want 15s", got)
	}

	t.Setenv("REMOTE_WRITE_SAMPLE_INTERVAL", "0")
	if got := ConfigFromEnv().RemoteWriteInterval; got != 0 {
		t.Fatalf("disabled RemoteWriteInterval = %s, want 0", got)
	}

	t.Setenv("REMOTE_WRITE_SAMPLE_INTERVAL", "30s")
	if got := ConfigFromEnv().RemoteWriteInterval; got != 30*time.Second {
		t.Fatalf("configured RemoteWriteInterval = %s, want 30s", got)
	}
}

func TestConfigFromEnvTeamSettings(t *testing.T) {
	t.Setenv("SNUFFLE_DEFAULT_TEAM_ID", "42")
	t.Setenv("SNUFFLE_TEAM_HEADER", "X-Scope-OrgID")
	t.Setenv("SNUFFLE_TEAM_QUERY_PARAM", "tenant")

	cfg := ConfigFromEnv()
	if cfg.TeamID != 42 || cfg.DefaultTeamID != 42 {
		t.Fatalf("team defaults = team_id %d default %d, want 42", cfg.TeamID, cfg.DefaultTeamID)
	}
	if cfg.TeamHeader != "X-Scope-OrgID" {
		t.Fatalf("TeamHeader = %q", cfg.TeamHeader)
	}
	if cfg.TeamQueryParam != "tenant" {
		t.Fatalf("TeamQueryParam = %q", cfg.TeamQueryParam)
	}
}

func TestConfigFromEnvEndpointPaths(t *testing.T) {
	t.Setenv("SNUFFLE_API_PATH_PREFIX", "")
	t.Setenv("SNUFFLE_REMOTE_WRITE_PATH", "")
	t.Setenv("SNUFFLE_REMOTE_READ_PATH", "")

	cfg := ConfigFromEnv()
	if cfg.APIPathPrefix != "/api/v1" {
		t.Fatalf("default APIPathPrefix = %q, want /api/v1", cfg.APIPathPrefix)
	}
	if cfg.RemoteWritePath != "/write" {
		t.Fatalf("default RemoteWritePath = %q, want /write", cfg.RemoteWritePath)
	}
	if cfg.RemoteReadPath != "/api/v1/read" {
		t.Fatalf("default RemoteReadPath = %q, want /api/v1/read", cfg.RemoteReadPath)
	}

	t.Setenv("SNUFFLE_API_PATH_PREFIX", "prom")
	t.Setenv("SNUFFLE_REMOTE_WRITE_PATH", "ingest/")
	cfg = ConfigFromEnv()
	if cfg.APIPathPrefix != "/prom" {
		t.Fatalf("configured APIPathPrefix = %q, want /prom", cfg.APIPathPrefix)
	}
	if cfg.RemoteWritePath != "/ingest" {
		t.Fatalf("configured RemoteWritePath = %q, want /ingest", cfg.RemoteWritePath)
	}
	if cfg.RemoteReadPath != "/prom/read" {
		t.Fatalf("derived RemoteReadPath = %q, want /prom/read", cfg.RemoteReadPath)
	}

	t.Setenv("SNUFFLE_REMOTE_READ_PATH", "remote-read")
	cfg = ConfigFromEnv()
	if cfg.RemoteReadPath != "/remote-read" {
		t.Fatalf("configured RemoteReadPath = %q, want /remote-read", cfg.RemoteReadPath)
	}
}
