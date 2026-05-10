package snuffle

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	CHURL            string
	CHUser           string
	CHPassword       string
	CHDatabase       string
	SeriesTable      string
	SamplesTable     string
	LabelIndexTable  string
	MetricsTable     string
	HistogramsTable  string
	ExemplarsTable   string
	HTTPHost         string
	HTTPPort         string
	CHTimeout        time.Duration
	QueryTimeout     time.Duration
	LookbackDelta    time.Duration
	MaxSamples       int
	MaxSeries        int
	IDChunkSize      int
	AggregateThreads int
}

func ConfigFromEnv() Config {
	return Config{
		CHURL:            getenv("CH_URL", "http://localhost:8123/"),
		CHUser:           getenv("CH_USER", "default"),
		CHPassword:       os.Getenv("CH_PASSWORD"),
		CHDatabase:       getenv("CH_DATABASE", "default"),
		SeriesTable:      getenv("CH_SERIES_TABLE", getenv("CH_TAGS_TABLE", "metrics_series")),
		SamplesTable:     getenv("CH_SAMPLES_TABLE", getenv("CH_DATA_TABLE", "metrics_samples")),
		LabelIndexTable:  getenv("CH_LABEL_INDEX_TABLE", "metrics_label_index"),
		MetricsTable:     getenv("CH_METRICS_TABLE", "metrics_metadata"),
		HistogramsTable:  getenv("CH_HISTOGRAMS_TABLE", "metrics_histograms"),
		ExemplarsTable:   getenv("CH_EXEMPLARS_TABLE", "metrics_exemplars"),
		HTTPHost:         getenv("SIDECAR_HOST", "0.0.0.0"),
		HTTPPort:         getenv("SIDECAR_PORT", "9091"),
		CHTimeout:        envDurationSeconds("CH_TIMEOUT_SECONDS", 30*time.Second),
		QueryTimeout:     envDurationSeconds("PROMQL_QUERY_TIMEOUT_SECONDS", 30*time.Second),
		LookbackDelta:    envDuration("PROMQL_LOOKBACK_DELTA", 5*time.Minute),
		MaxSamples:       envInt("PROMQL_MAX_SAMPLES", 50_000_000, 1),
		MaxSeries:        envInt("CH_MAX_SERIES", 1_000_000, 1),
		IDChunkSize:      envInt("CH_ID_CHUNK_SIZE", 20000, 1),
		AggregateThreads: envInt("CH_AGGREGATE_MAX_THREADS", 4, 0),
	}
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
