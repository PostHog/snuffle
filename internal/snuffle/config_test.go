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
