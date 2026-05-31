package snuffle

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	CHAddr              string
	CHUser              string
	CHPassword          string
	CHDatabase          string
	SchemaLayout        string
	SeriesTable         string
	SamplesTable        string
	LabelIndexTable     string
	AttributeTable      string
	LabelPostingsTable  string
	ActivityTable       string
	MetricsTable        string
	HistogramsTable     string
	ExemplarsTable      string
	HTTPHost            string
	HTTPPort            string
	CHTimeout           time.Duration
	QueryTimeout        time.Duration
	LookbackDelta       time.Duration
	MaxSamples          int
	MaxSeries           int
	IDChunkSize         int
	AggregateThreads    int
	RemoteWriteInterval time.Duration
	SampleAttributes    bool
	TeamID              uint64
	DefaultTeamID       uint64
	TeamHeader          string
	TeamQueryParam      string
	Pprof               bool
	SelfScrapeEnabled   bool
	SelfScrapeInterval  time.Duration
	SelfScrapeTeamID    uint64
	SelfScrapeJob       string
	SelfScrapeInstance  string
}

func ConfigFromEnv() Config {
	httpPort := getenv("SIDECAR_PORT", "9091")
	defaultTeamID := envUint64("SNUFFLE_DEFAULT_TEAM_ID", 0)
	schemaLayout := storageSchemaLayout(getenv("CH_SCHEMA_LAYOUT", getenv("SNUFFLE_SCHEMA_LAYOUT", string(schemaLayoutCurrent))))
	sampleAttributesDefault := schemaLayout == schemaLayoutPostHog
	seriesTableDefault := "metrics_series"
	samplesTableDefault := "metrics_samples"
	labelIndexTableDefault := "metrics_label_index"
	histogramsTableDefault := "metrics_histograms"
	exemplarsTableDefault := "metrics_exemplars"
	metadataTableDefault := "metrics_metadata"
	aggregateThreadsDefault := 1
	if schemaLayout == schemaLayoutPostHog {
		seriesTableDefault = ""
		samplesTableDefault = "metrics"
		labelIndexTableDefault = ""
		histogramsTableDefault = ""
		exemplarsTableDefault = ""
		metadataTableDefault = ""
	}
	return Config{
		CHAddr:              getenv("CH_ADDR", "localhost:9000"),
		CHUser:              getenv("CH_USER", "default"),
		CHPassword:          os.Getenv("CH_PASSWORD"),
		CHDatabase:          getenv("CH_DATABASE", "default"),
		SchemaLayout:        string(schemaLayout),
		SeriesTable:         getenv("CH_SERIES_TABLE", getenv("CH_TAGS_TABLE", seriesTableDefault)),
		SamplesTable:        getenv("CH_SAMPLES_TABLE", getenv("CH_DATA_TABLE", samplesTableDefault)),
		LabelIndexTable:     getenv("CH_LABEL_INDEX_TABLE", labelIndexTableDefault),
		AttributeTable:      getenv("CH_ATTRIBUTE_TABLE", "metric_attributes"),
		LabelPostingsTable:  getenv("CH_LABEL_POSTINGS_TABLE", ""),
		ActivityTable:       getenv("CH_ACTIVITY_TABLE", ""),
		MetricsTable:        getenv("CH_METRICS_TABLE", metadataTableDefault),
		HistogramsTable:     getenv("CH_HISTOGRAMS_TABLE", histogramsTableDefault),
		ExemplarsTable:      getenv("CH_EXEMPLARS_TABLE", exemplarsTableDefault),
		HTTPHost:            getenv("SIDECAR_HOST", "0.0.0.0"),
		HTTPPort:            httpPort,
		CHTimeout:           envDurationSeconds("CH_TIMEOUT_SECONDS", 30*time.Second),
		QueryTimeout:        envDurationSeconds("PROMQL_QUERY_TIMEOUT_SECONDS", 30*time.Second),
		LookbackDelta:       envDuration("PROMQL_LOOKBACK_DELTA", 5*time.Minute),
		MaxSamples:          envInt("PROMQL_MAX_SAMPLES", 50_000_000, 1),
		MaxSeries:           envInt("CH_MAX_SERIES", 1_000_000, 1),
		IDChunkSize:         envInt("CH_ID_CHUNK_SIZE", 20000, 1),
		AggregateThreads:    envInt("CH_AGGREGATE_MAX_THREADS", aggregateThreadsDefault, 0),
		RemoteWriteInterval: envDurationAllowZero("REMOTE_WRITE_SAMPLE_INTERVAL", 15*time.Second),
		SampleAttributes:    envBool("SNUFFLE_SAMPLE_ATTRIBUTES", sampleAttributesDefault),
		TeamID:              defaultTeamID,
		DefaultTeamID:       defaultTeamID,
		TeamHeader:          getenv("SNUFFLE_TEAM_HEADER", "X-Team-ID"),
		TeamQueryParam:      getenv("SNUFFLE_TEAM_QUERY_PARAM", "team_id"),
		Pprof:               envBool("SNUFFLE_PPROF", false),
		SelfScrapeEnabled:   envBool("SNUFFLE_SELF_SCRAPE_ENABLED", true),
		SelfScrapeInterval:  envDurationAllowZero("SNUFFLE_SELF_SCRAPE_INTERVAL", 15*time.Second),
		SelfScrapeTeamID:    envUint64("SNUFFLE_SELF_SCRAPE_TEAM_ID", defaultTeamID),
		SelfScrapeJob:       getenv("SNUFFLE_SELF_SCRAPE_JOB", "snuffle"),
		SelfScrapeInstance:  getenv("SNUFFLE_SELF_SCRAPE_INSTANCE", defaultSelfScrapeInstance(httpPort)),
	}
}

func defaultSelfScrapeInstance(port string) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}
	return hostname + ":" + port
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback, min int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < min {
		return fallback
	}
	return parsed
}

func envUint64(key string, fallback uint64) uint64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch value {
	case "":
		return fallback
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed * float64(time.Second))
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err == nil && parsed > 0 {
		return parsed
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	return fallback
}

func envDurationAllowZero(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	if value == "0" {
		return 0
	}
	parsed, err := time.ParseDuration(value)
	if err == nil && parsed >= 0 {
		return parsed
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	return fallback
}
