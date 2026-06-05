DROP TABLE IF EXISTS logs_to_log_stream_days SYNC;
DROP TABLE IF EXISTS logs_to_log_stream_stats SYNC;
DROP TABLE IF EXISTS logs SYNC;
DROP TABLE IF EXISTS log_stream_days SYNC;
DROP TABLE IF EXISTS log_stream_stats SYNC;
DROP TABLE IF EXISTS log_label_index SYNC;
DROP TABLE IF EXISTS log_stream_labels SYNC;
DROP TABLE IF EXISTS log_streams SYNC;

CREATE TABLE log_streams
(
    team_id Int32 CODEC(T64, Default),
    stream_id UInt64,
    labels Map(LowCardinality(String), String),
    resource_attributes Map(LowCardinality(String), String),
    service_name LowCardinality(String) MATERIALIZED if(mapContains(labels, 'service_name'), labels['service_name'], if(mapContains(labels, 'service.name'), labels['service.name'], resource_attributes['service.name'])),
    severity_text LowCardinality(String) MATERIALIZED multiIf(mapContains(labels, 'level'), labels['level'], mapContains(labels, 'severity_text'), labels['severity_text'], mapContains(labels, 'detected_level'), labels['detected_level'], ''),
    resource_fingerprint UInt64 MATERIALIZED cityHash64(resource_attributes) CODEC(Delta, Default),
    updated_at DateTime64(6, 'UTC') CODEC(DoubleDelta, Default),
    INDEX idx_labels_keys mapKeys(labels) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_labels_values mapValues(labels) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_resource_keys mapKeys(resource_attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_resource_values mapValues(resource_attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_service_name service_name TYPE set(1024) GRANULARITY 1,
    INDEX idx_severity_text severity_text TYPE set(16) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (team_id, stream_id)
SETTINGS
    index_granularity_bytes = 104857600,
    index_granularity = 8192,
    map_serialization_version = 'with_buckets',
    map_buckets_strategy = 'constant',
    max_buckets_in_map = 16;

CREATE TABLE log_stream_stats
(
    team_id Int32 CODEC(T64, Default),
    bucket DateTime('UTC') CODEC(DoubleDelta, Default),
    stream_id UInt64,
    log_count UInt64 CODEC(T64, Default),
    byte_count UInt64 CODEC(T64, Default)
)
ENGINE = SummingMergeTree((log_count, byte_count))
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (team_id, stream_id, bucket)
TTL bucket + toIntervalDay(31)
SETTINGS
    ttl_only_drop_parts = 1,
    index_granularity_bytes = 104857600,
    index_granularity = 8192;

CREATE TABLE log_stream_labels
(
    team_id Int32 CODEC(T64, Default),
    label_name LowCardinality(String),
    label_value String,
    stream_id UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (team_id, stream_id, label_name, label_value)
SETTINGS
    index_granularity_bytes = 104857600,
    index_granularity = 8192;

CREATE TABLE logs
(
    team_id Int32 CODEC(T64, Default),
    timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta, Default),
    time_bucket DateTime ALIAS toStartOfDay(timestamp),
    expires_at DateTime64(6, 'UTC') CODEC(DoubleDelta, Default),
    stream_id UInt64,
    observed_ns Int64 CODEC(Delta, Default),
    body String,
    fields Map(LowCardinality(String), String),
    INDEX idx_timestamp_minmax timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_stream_bloom stream_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_fields_keys mapKeys(fields) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body_ngram lower(body) TYPE ngrambf_v1(3, 16384, 2, 0) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, stream_id, timestamp, observed_ns)
TTL expires_at
SETTINGS
    ttl_only_drop_parts = 1,
    index_granularity_bytes = 104857600,
    index_granularity = 8192,
    map_serialization_version = 'with_buckets',
    map_buckets_strategy = 'constant',
    max_buckets_in_map = 16;
