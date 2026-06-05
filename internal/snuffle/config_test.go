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
	if cfg.LogSchemaLayout != "posthog" || cfg.LogsTable != "logs34" || cfg.LogAttributesTable != "log_attributes2" {
		t.Fatalf("posthog log defaults = layout %q logs %q attributes %q", cfg.LogSchemaLayout, cfg.LogsTable, cfg.LogAttributesTable)
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

func TestConfigFromEnvLogSettings(t *testing.T) {
	t.Setenv("SNUFFLE_LOG_RETENTION", "48h")
	t.Setenv("SNUFFLE_LOG_QUERY_MAX_ROWS", "1234")

	cfg := ConfigFromEnv()
	if cfg.LogSchemaLayout != "snuffle" {
		t.Fatalf("LogSchemaLayout = %q, want snuffle", cfg.LogSchemaLayout)
	}
	if cfg.LogsTable != "logs" || cfg.LogStreamsTable != "log_streams" || cfg.LogAttributesTable != "" || cfg.LogStreamStatsTable != "log_stream_stats" {
		t.Fatalf("snuffle log tables = logs %q streams %q attrs %q stats %q", cfg.LogsTable, cfg.LogStreamsTable, cfg.LogAttributesTable, cfg.LogStreamStatsTable)
	}
	if cfg.LogRetention != 48*time.Hour {
		t.Fatalf("LogRetention = %s, want 48h", cfg.LogRetention)
	}
	if cfg.LogQueryMaxRows != 1234 {
		t.Fatalf("LogQueryMaxRows = %d, want 1234", cfg.LogQueryMaxRows)
	}

	t.Setenv("CH_LOG_SCHEMA_LAYOUT", "posthog")
	cfg = ConfigFromEnv()
	if cfg.LogSchemaLayout != "posthog" || cfg.LogsTable != "logs34" || cfg.LogAttributesTable != "log_attributes2" || cfg.LogStreamsTable != "" || cfg.LogStreamStatsTable != "" {
		t.Fatalf("posthog log layout = layout %q logs %q streams %q attrs %q stats %q", cfg.LogSchemaLayout, cfg.LogsTable, cfg.LogStreamsTable, cfg.LogAttributesTable, cfg.LogStreamStatsTable)
	}

	t.Setenv("CH_LOGS_TABLE", "custom_logs")
	t.Setenv("CH_LOG_STREAMS_TABLE", "custom_streams")
	t.Setenv("CH_LOG_ATTRIBUTES_TABLE", "custom_attrs")
	t.Setenv("CH_LOG_STREAM_STATS_TABLE", "custom_stats")
	cfg = ConfigFromEnv()
	if cfg.LogsTable != "custom_logs" || cfg.LogStreamsTable != "custom_streams" || cfg.LogAttributesTable != "custom_attrs" || cfg.LogStreamStatsTable != "custom_stats" {
		t.Fatalf("custom log tables = logs %q streams %q attrs %q stats %q", cfg.LogsTable, cfg.LogStreamsTable, cfg.LogAttributesTable, cfg.LogStreamStatsTable)
	}
}
