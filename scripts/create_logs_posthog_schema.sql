DROP TABLE IF EXISTS logs34_to_resource_attributes SYNC;
DROP TABLE IF EXISTS logs34_to_log_attributes SYNC;
DROP VIEW IF EXISTS logs SYNC;
DROP TABLE IF EXISTS logs34 SYNC;
DROP TABLE IF EXISTS log_attributes2 SYNC;

CREATE TABLE log_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    resource_fingerprint UInt64 DEFAULT 0,
    attribute_key LowCardinality(String),
    attribute_value String CODEC(ZSTD(5)),
    attribute_count SimpleAggregateFunction(sum, UInt64),
    attribute_type LowCardinality(String) DEFAULT 'log',
    original_expiry_time_bucket DateTime DEFAULT now(),
    INDEX idx_attribute_key attribute_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_value attribute_value TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_key_n3 attribute_key TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_attribute_value_n3 attribute_value TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(time_bucket)
ORDER BY (team_id, attribute_type, time_bucket, resource_fingerprint, attribute_key, attribute_value)
TTL time_bucket + toIntervalDay(15)
SETTINGS
    deduplicate_merge_projection_mode = 'drop',
    index_granularity = 8192;

CREATE TABLE logs34
(
    time_bucket DateTime MATERIALIZED toStartOfDay(timestamp),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    original_expiry_time_bucket DateTime ALIAS toStartOfInterval(original_expiry_timestamp, toIntervalMinute(10)),
    uuid String,
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
    attributes_map_str Map(LowCardinality(String), String),
    level String ALIAS severity_text,
    mat_body_ipv4_matches Array(String) ALIAS extractAll(body, '(?:\\d{1,3}\\.){3}\\d{1,3}'),
    time_minute DateTime ALIAS toStartOfMinute(timestamp),
    attributes Map(String, String) ALIAS mapApply((k, v) -> (left(k, -5), v), attributes_map_str),
    attributes_map_float Map(LowCardinality(String), Float64) MATERIALIZED mapFilter((k, v) -> (v IS NOT NULL), mapApply((k, v) -> (concat(left(k, -5), '__float'), toFloat64OrNull(v)), attributes_map_str)),
    attributes_map_datetime Map(LowCardinality(String), DateTime64(6, 'UTC')) MATERIALIZED mapFilter((k, v) -> (v IS NOT NULL), mapApply((k, v) -> (concat(left(k, -5), '__datetime'), parseDateTimeBestEffortOrNull(v, 6)), attributes_map_str)),
    _partition UInt32,
    _topic String,
    _offset UInt64,
    _bytes_uncompressed UInt64,
    _bytes_compressed UInt64,
    _record_count UInt64,
    INDEX idx_severity_text_set severity_text TYPE set(10) GRANULARITY 1,
    INDEX idx_attributes_str_keys mapKeys(attributes_map_str) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attributes_str_values mapValues(attributes_map_str) TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_mat_body_ipv4_matches mat_body_ipv4_matches TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body_ngram3 lower(body) TYPE ngrambf_v1(3, 25000, 2, 0) GRANULARITY 1,
    INDEX idx_uuid_bloom uuid TYPE bloom_filter(0.01) GRANULARITY 1,
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
    map_serialization_version = 'with_buckets',
    map_buckets_strategy = 'constant',
    max_buckets_in_map = 32;

CREATE VIEW logs AS SELECT * FROM logs34;

CREATE MATERIALIZED VIEW logs34_to_log_attributes TO log_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    resource_fingerprint UInt64,
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64)
)
AS SELECT
    team_id,
    time_bucket,
    original_expiry_time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        toStartOfInterval(original_expiry_timestamp, toIntervalMinute(10)) AS original_expiry_time_bucket,
        service_name AS service_name,
        resource_fingerprint,
        mapFilter((k, v) -> ((length(k) < 256) AND (length(v) < 256)), attributes) AS attributes,
        arrayJoin(attributes) AS attribute,
        'log' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM logs34
    GROUP BY
        team_id,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        resource_fingerprint,
        attributes
);

CREATE MATERIALIZED VIEW logs34_to_resource_attributes TO log_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    resource_fingerprint UInt64,
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64)
)
AS SELECT
    team_id,
    time_bucket,
    original_expiry_time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        toStartOfInterval(original_expiry_timestamp, toIntervalMinute(10)) AS original_expiry_time_bucket,
        service_name AS service_name,
        resource_fingerprint,
        arrayJoin(resource_attributes) AS attribute,
        'resource' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM logs34
    GROUP BY
        team_id,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        resource_fingerprint,
        resource_attributes
);
