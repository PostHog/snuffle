-- Slim PostHog-style logs schema using ClickHouse JSON for log attributes.
--
-- This schema is intentionally not a drop-in replacement for the current
-- Map-based logs34 table. The bridge must write attributes_json instead of
-- attributes_map_str and query attribute paths through JSON subcolumns, e.g.
--   attributes_json.status__str
--   attributes_json.some_dynamic_key.:String
--
-- The hot logs table drops columns that this bridge does not query today:
-- uuid, _partition, _topic, _offset, _bytes_uncompressed, _bytes_compressed,
-- and _record_count. Keep the existing schema if those fields are required by
-- another consumer.

DROP TABLE IF EXISTS logs34_to_resource_attributes SYNC;
DROP TABLE IF EXISTS logs34_to_log_attributes SYNC;
DROP VIEW IF EXISTS logs SYNC;
DROP TABLE IF EXISTS logs34 SYNC;
DROP TABLE IF EXISTS log_attributes2 SYNC;

CREATE TABLE log_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String) DEFAULT 'log',
    INDEX idx_attribute_key attribute_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_value attribute_value TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(time_bucket)
ORDER BY (team_id, attribute_type, attribute_key, time_bucket, attribute_value)
TTL time_bucket + toIntervalDay(15)
SETTINGS index_granularity = 8192;

CREATE TABLE logs34
(
    time_bucket DateTime MATERIALIZED toStartOfDay(timestamp),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    original_expiry_time_bucket DateTime ALIAS toStartOfInterval(original_expiry_timestamp, toIntervalMinute(10)),
    team_id Int32,
    trace_id String,
    span_id String,
    trace_flags Int32,
    timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta),
    observed_timestamp DateTime64(6, 'UTC'),
    created_at DateTime64(6, 'UTC') MATERIALIZED now64(6),
    body String,
    severity_text LowCardinality(String),
    severity_number Int32,
    service_name LowCardinality(String),
    resource_attributes Map(LowCardinality(String), String),
    resource_fingerprint UInt64 MATERIALIZED cityHash64(resource_attributes),
    instrumentation_scope String,
    event_name String,
    attributes_json JSON(
        max_dynamic_paths = 64,
        max_dynamic_types = 4,
        app__str String,
        env__str String,
        region__str String,
        format__str String,
        pod__str String,
        host__str String,
        route__str String,
        status__str String,
        duration__str String,
        size__str String,
        detected_level__str String
    ),
    level String ALIAS severity_text,
    mat_body_ipv4_matches Array(String) ALIAS extractAll(body, '(?:\\d{1,3}\\.){3}\\d{1,3}'),
    time_minute DateTime ALIAS toStartOfMinute(timestamp),
    INDEX idx_severity_text_set severity_text TYPE set(10) GRANULARITY 1,
    INDEX idx_body_ngram3 lower(body) TYPE ngrambf_v1(3, 25000, 2, 0) GRANULARITY 1,
    INDEX idx_observed_minmax observed_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_timestamp_minmax timestamp TYPE minmax GRANULARITY 1,
    PROJECTION projection_aggregate_counts
    (
        SELECT
            team_id,
            time_bucket,
            toStartOfMinute(timestamp),
            service_name,
            severity_text,
            resource_fingerprint,
            count() AS event_count
        GROUP BY
            team_id,
            time_bucket,
            toStartOfMinute(timestamp),
            service_name,
            severity_text,
            resource_fingerprint
    )
)
ENGINE = MergeTree
PARTITION BY toDate(original_expiry_timestamp)
PRIMARY KEY (team_id, time_bucket, service_name, resource_fingerprint, severity_text, timestamp)
ORDER BY (team_id, time_bucket, service_name, resource_fingerprint, severity_text, timestamp)
TTL original_expiry_timestamp
SETTINGS
    index_granularity_bytes = 104857600,
    index_granularity = 8192,
    ttl_only_drop_parts = 1,
    add_minmax_index_for_numeric_columns = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map_with_buckets',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    object_shared_data_buckets_for_wide_part = 32,
    object_shared_data_buckets_for_compact_part = 8;

CREATE VIEW logs AS SELECT * FROM logs34;

-- Discovery rows for typed, common log attributes. Arbitrary dynamic
-- attributes should be inserted into log_attributes2 by the ingest path if
-- they need to appear in /loki/api/v1/labels or label values.
CREATE MATERIALIZED VIEW logs34_to_log_attributes TO log_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String)
)
AS SELECT
    team_id,
    toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
    attribute.1 AS attribute_key,
    attribute.2 AS attribute_value,
    'log' AS attribute_type
FROM
(
    SELECT
        team_id,
        timestamp,
        arrayJoin([
            ('app', attributes_json.app__str),
            ('env', attributes_json.env__str),
            ('region', attributes_json.region__str),
            ('format', attributes_json.format__str),
            ('pod', attributes_json.pod__str),
            ('host', attributes_json.host__str),
            ('route', attributes_json.route__str),
            ('status', attributes_json.status__str),
            ('duration', attributes_json.duration__str),
            ('size', attributes_json.size__str),
            ('detected_level', attributes_json.detected_level__str)
        ]) AS attribute
    FROM logs34
)
WHERE attribute.2 != ''
GROUP BY
    team_id,
    time_bucket,
    attribute_key,
    attribute_value,
    attribute_type;

CREATE MATERIALIZED VIEW logs34_to_resource_attributes TO log_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String)
)
AS SELECT
    team_id,
    time_bucket,
    attribute_key,
    attribute_value,
    attribute_type
FROM
(
    SELECT
        team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        arrayJoin(resource_attributes) AS attribute,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        'resource' AS attribute_type
    FROM logs34
)
WHERE attribute_value != ''
GROUP BY
    team_id,
    time_bucket,
    attribute_key,
    attribute_value,
    attribute_type;
