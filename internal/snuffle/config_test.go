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

func TestConfigFromEnvSchemaLayout(t *testing.T) {
	t.Setenv("CH_AGGREGATE_MAX_THREADS", "")
	t.Setenv("CH_SCHEMA_LAYOUT", "")
	if cfg := ConfigFromEnv(); cfg.AggregateThreads != 1 {
		t.Fatalf("current AggregateThreads default = %d, want 1", cfg.AggregateThreads)
	}

	t.Setenv("CH_SCHEMA_LAYOUT", "posthog")

	cfg := ConfigFromEnv()
	if cfg.SchemaLayout != "posthog" {
		t.Fatalf("SchemaLayout = %q, want posthog", cfg.SchemaLayout)
	}
	if !cfg.SampleAttributes {
		t.Fatalf("SampleAttributes = false, want true for posthog layout")
	}
	if cfg.SamplesTable != "metrics" || cfg.SeriesTable != "" || cfg.LabelIndexTable != "" || cfg.AttributeTable != "metric_attributes" {
		t.Fatalf("posthog tables = samples %q series %q label_index %q attributes %q", cfg.SamplesTable, cfg.SeriesTable, cfg.LabelIndexTable, cfg.AttributeTable)
	}
	if cfg.AggregateThreads != 1 {
		t.Fatalf("posthog AggregateThreads default = %d, want 1", cfg.AggregateThreads)
	}

	t.Setenv("SNUFFLE_SAMPLE_ATTRIBUTES", "0")
	cfg = ConfigFromEnv()
	if cfg.SampleAttributes {
		t.Fatalf("SampleAttributes override = true, want false")
	}
}

func TestConfigFromEnvSelfScrapeSettings(t *testing.T) {
	t.Setenv("SNUFFLE_DEFAULT_TEAM_ID", "42")
	t.Setenv("SNUFFLE_SELF_SCRAPE_INTERVAL", "30s")
	t.Setenv("SNUFFLE_SELF_SCRAPE_TEAM_ID", "99")
	t.Setenv("SNUFFLE_SELF_SCRAPE_JOB", "bridge")
	t.Setenv("SNUFFLE_SELF_SCRAPE_INSTANCE", "bridge-1:9091")

	cfg := ConfigFromEnv()
	if !cfg.SelfScrapeEnabled {
		t.Fatalf("SelfScrapeEnabled = false, want true")
	}
	if cfg.SelfScrapeInterval != 30*time.Second {
		t.Fatalf("SelfScrapeInterval = %s, want 30s", cfg.SelfScrapeInterval)
	}
	if cfg.SelfScrapeTeamID != 99 {
		t.Fatalf("SelfScrapeTeamID = %d, want 99", cfg.SelfScrapeTeamID)
	}
	if cfg.SelfScrapeJob != "bridge" || cfg.SelfScrapeInstance != "bridge-1:9091" {
		t.Fatalf("self scrape labels = job %q instance %q", cfg.SelfScrapeJob, cfg.SelfScrapeInstance)
	}
}
