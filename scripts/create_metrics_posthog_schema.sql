DROP VIEW IF EXISTS metrics SYNC;
DROP TABLE IF EXISTS metrics1_to_resource_attributes SYNC;
DROP TABLE IF EXISTS metrics1_to_metric_attributes SYNC;
DROP TABLE IF EXISTS metrics1 SYNC;
DROP TABLE IF EXISTS metric_attributes SYNC;
DROP TABLE IF EXISTS metrics2_input_to_resource_attributes SYNC;
DROP TABLE IF EXISTS metrics2_input_to_metric_attributes SYNC;
DROP TABLE IF EXISTS metrics2_input_to_metric_series SYNC;
DROP TABLE IF EXISTS metrics2_input_to_metrics SYNC;
DROP TABLE IF EXISTS metrics2_input SYNC;
DROP TABLE IF EXISTS metrics2 SYNC;
DROP TABLE IF EXISTS metric_series2 SYNC;
DROP TABLE IF EXISTS metric_attributes2 SYNC;
DROP TABLE IF EXISTS metrics2_input_to_resource_attributes3 SYNC;
DROP TABLE IF EXISTS metrics2_input_to_metric_attributes3 SYNC;
DROP TABLE IF EXISTS metrics2_input_to_metric_names3 SYNC;
DROP TABLE IF EXISTS metrics2_input_to_metric_series3 SYNC;
DROP TABLE IF EXISTS metric_series3 SYNC;
DROP TABLE IF EXISTS metric_attributes3 SYNC;
DROP TABLE IF EXISTS metric_names3 SYNC;

CREATE TABLE metrics2_input
(
    uuid String,
    team_id Int32,
    metric_name LowCardinality(String),
    series_fingerprint UInt64,
    resource_fingerprint UInt64,
    timestamp DateTime64(6, 'UTC'),
    observed_timestamp DateTime64(6, 'UTC'),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    service_name LowCardinality(String),
    metric_type LowCardinality(String),
    value Float64,
    count UInt64,
    histogram_bounds Array(Float64),
    histogram_counts Array(UInt64),
    trace_id String,
    span_id String,
    trace_flags Int32,
    has_labels Bool,
    unit LowCardinality(String),
    aggregation_temporality LowCardinality(String),
    is_monotonic Bool,
    instrumentation_scope String,
    resource_attributes Map(LowCardinality(String), String),
    attributes Map(LowCardinality(String), String),
    _partition UInt32,
    _topic String,
    _offset UInt64
)
ENGINE = Null;

CREATE TABLE metrics2
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime MATERIALIZED toStartOfHour(timestamp),
    series_fingerprint UInt64 CODEC(Delta, Default),
    resource_fingerprint UInt64 DEFAULT 0,
    timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta),
    observed_timestamp DateTime64(6, 'UTC'),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    created_at DateTime64(6, 'UTC') MATERIALIZED now64(6),
    service_name LowCardinality(String),
    metric_type LowCardinality(String),
    value Float64 CODEC(Gorilla),
    count UInt64 DEFAULT 1 CODEC(T64),
    histogram_bounds Array(Float64),
    histogram_counts Array(UInt64),
    trace_id String,
    span_id String,
    trace_flags Int32,
    has_labels Bool DEFAULT false,
    unit LowCardinality(String),
    aggregation_temporality LowCardinality(String),
    is_monotonic Bool DEFAULT false,
    instrumentation_scope String,
    _partition UInt32,
    _topic String,
    _offset UInt64,
    INDEX idx_metric_type_set metric_type TYPE set(10) GRANULARITY 1,
    INDEX idx_service_set service_name TYPE set(1000) GRANULARITY 1,
    INDEX idx_trace_id_bf trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_resource_fingerprint resource_fingerprint TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_observed_minmax observed_timestamp TYPE minmax GRANULARITY 1,
    PROJECTION projection_series_activity
    (
        SELECT
            team_id,
            service_name,
            metric_name,
            metric_type,
            resource_fingerprint,
            series_fingerprint,
            toStartOfHour(timestamp) AS hour,
            count() AS sample_count,
            max(timestamp) AS last_seen
        GROUP BY
            team_id,
            service_name,
            metric_name,
            metric_type,
            resource_fingerprint,
            series_fingerprint,
            hour
    )
)
ENGINE = MergeTree
PARTITION BY toDate(original_expiry_timestamp)
ORDER BY (team_id, metric_name, time_bucket, series_fingerprint, timestamp)
TTL original_expiry_timestamp
SETTINGS
    index_granularity_bytes = 104857600,
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE TABLE metric_series2
(
    team_id Int32,
    metric_name LowCardinality(String),
    series_fingerprint UInt64 CODEC(Delta, Default),
    metric_type LowCardinality(String),
    unit LowCardinality(String),
    aggregation_temporality LowCardinality(String),
    is_monotonic Bool DEFAULT false,
    service_name LowCardinality(String),
    instrumentation_scope String,
    resource_attributes Map(LowCardinality(String), String),
    resource_fingerprint UInt64 MATERIALIZED cityHash64(resource_attributes),
    attributes Map(LowCardinality(String), String),
    last_seen DateTime64(6, 'UTC'),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    INDEX idx_service_set service_name TYPE set(1000) GRANULARITY 1,
    INDEX idx_resource_fingerprint resource_fingerprint TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_keys mapKeys(attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_values mapValues(attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (team_id, metric_name, series_fingerprint)
TTL original_expiry_timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE metric_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64),
    INDEX idx_attribute_key attribute_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_value attribute_value TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_key_n3 attribute_key TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_attribute_value_n3 attribute_value TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(original_expiry_time_bucket)
ORDER BY (team_id, attribute_type, time_bucket, attribute_key, attribute_value)
TTL original_expiry_time_bucket
SETTINGS
    allow_dimensions_outside_sorting_key = 1,
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW metrics2_input_to_metrics TO metrics2
AS SELECT
    team_id,
    metric_name,
    series_fingerprint,
    resource_fingerprint,
    timestamp,
    observed_timestamp,
    original_expiry_timestamp,
    service_name,
    metric_type,
    value,
    count,
    histogram_bounds,
    histogram_counts,
    trace_id,
    span_id,
    trace_flags,
    has_labels,
    unit,
    aggregation_temporality,
    is_monotonic,
    instrumentation_scope,
    _partition,
    _topic,
    _offset
FROM metrics2_input;

CREATE MATERIALIZED VIEW metrics2_input_to_metric_series TO metric_series2
AS SELECT
    team_id,
    metric_name,
    series_fingerprint,
    metric_type,
    unit,
    aggregation_temporality,
    is_monotonic,
    service_name,
    instrumentation_scope,
    resource_attributes,
    attributes,
    timestamp AS last_seen,
    original_expiry_timestamp
FROM metrics2_input
WHERE has_labels;

CREATE MATERIALIZED VIEW metrics2_input_to_metric_attributes TO metric_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
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
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        toStartOfInterval(timestamp, toIntervalHour(1)) AS time_bucket,
        toStartOfInterval(original_expiry_timestamp, toIntervalHour(1)) AS original_expiry_time_bucket,
        service_name AS service_name,
        mapFilter((k, v) -> ((length(k) < 256) AND (length(v) < 256)), attributes) AS filtered_attributes,
        arrayJoin(filtered_attributes) AS attribute,
        'metric' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM metrics2_input
    WHERE has_labels
    GROUP BY
        team_id,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        filtered_attributes
);

CREATE MATERIALIZED VIEW metrics2_input_to_resource_attributes TO metric_attributes2
(
    team_id Int32,
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
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
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        toStartOfInterval(timestamp, toIntervalHour(1)) AS time_bucket,
        toStartOfInterval(original_expiry_timestamp, toIntervalHour(1)) AS original_expiry_time_bucket,
        service_name AS service_name,
        resource_attributes AS filtered_attributes,
        arrayJoin(filtered_attributes) AS attribute,
        'resource' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM metrics2_input
    WHERE has_labels
    GROUP BY
        team_id,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        filtered_attributes
);

CREATE TABLE metric_series3
(
    team_id Int32,
    metric_name LowCardinality(String),
    series_fingerprint UInt64 CODEC(Delta, Default),
    metric_type LowCardinality(String),
    unit LowCardinality(String),
    aggregation_temporality LowCardinality(String),
    is_monotonic Bool DEFAULT false,
    service_name LowCardinality(String),
    instrumentation_scope String,
    resource_attributes Map(LowCardinality(String), String),
    resource_fingerprint UInt64 MATERIALIZED cityHash64(resource_attributes),
    attributes Map(LowCardinality(String), String),
    last_seen DateTime64(6, 'UTC'),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    INDEX idx_service_set service_name TYPE set(1000) GRANULARITY 1,
    INDEX idx_resource_fingerprint resource_fingerprint TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_keys mapKeys(attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_values mapValues(attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_last_seen_minmax last_seen TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(last_seen)
PARTITION BY toDate(original_expiry_timestamp)
ORDER BY (team_id, metric_name, series_fingerprint)
TTL original_expiry_timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE metric_attributes3
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64),
    INDEX idx_attribute_key attribute_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_value attribute_value TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_key_n3 attribute_key TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_attribute_value_n3 attribute_value TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(original_expiry_time_bucket)
ORDER BY (team_id, metric_name, attribute_type, time_bucket, attribute_key, attribute_value, service_name, original_expiry_time_bucket)
TTL original_expiry_time_bucket
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE TABLE metric_names3
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    original_expiry_timestamp SimpleAggregateFunction(max, DateTime64(6, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(original_expiry_time_bucket)
ORDER BY (team_id, time_bucket, metric_name, original_expiry_time_bucket)
TTL original_expiry_timestamp
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW metrics2_input_to_metric_series3 TO metric_series3
AS SELECT
    team_id,
    metric_name,
    series_fingerprint,
    metric_type,
    unit,
    aggregation_temporality,
    is_monotonic,
    service_name,
    instrumentation_scope,
    resource_attributes,
    attributes,
    timestamp AS last_seen,
    original_expiry_timestamp
FROM metrics2_input
WHERE has_labels;

CREATE MATERIALIZED VIEW metrics2_input_to_metric_names3 TO metric_names3
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    original_expiry_timestamp SimpleAggregateFunction(max, DateTime64(6, 'UTC'))
)
AS SELECT
    team_id,
    metric_name,
    toStartOfHour(timestamp) AS time_bucket,
    toStartOfHour(input.original_expiry_timestamp) AS original_expiry_time_bucket,
    maxSimpleState(input.original_expiry_timestamp) AS original_expiry_timestamp
FROM metrics2_input AS input
WHERE has_labels
GROUP BY team_id, time_bucket, metric_name, original_expiry_time_bucket;

CREATE MATERIALIZED VIEW metrics2_input_to_metric_attributes3 TO metric_attributes3
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64)
)
AS SELECT
    team_id,
    metric_name,
    time_bucket,
    original_expiry_time_bucket,
    service_name,
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        metric_name AS metric_name,
        toStartOfInterval(timestamp, toIntervalHour(1)) AS time_bucket,
        toStartOfInterval(original_expiry_timestamp, toIntervalHour(1)) AS original_expiry_time_bucket,
        service_name AS service_name,
        mapFilter((k, v) -> ((length(k) < 256) AND (length(v) < 256)), attributes) AS filtered_attributes,
        arrayJoin(filtered_attributes) AS attribute,
        'metric' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM metrics2_input
    WHERE has_labels
    GROUP BY
        team_id,
        metric_name,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        filtered_attributes
);

CREATE MATERIALIZED VIEW metrics2_input_to_resource_attributes3 TO metric_attributes3
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime64(0, 'UTC'),
    original_expiry_time_bucket DateTime64(0, 'UTC'),
    service_name LowCardinality(String),
    attribute_key LowCardinality(String),
    attribute_value String,
    attribute_type LowCardinality(String),
    attribute_count SimpleAggregateFunction(sum, UInt64)
)
AS SELECT
    team_id,
    metric_name,
    time_bucket,
    original_expiry_time_bucket,
    service_name,
    attribute_key,
    attribute_value,
    attribute_type,
    attribute_count
FROM
(
    SELECT
        team_id AS team_id,
        metric_name AS metric_name,
        toStartOfInterval(timestamp, toIntervalHour(1)) AS time_bucket,
        toStartOfInterval(original_expiry_timestamp, toIntervalHour(1)) AS original_expiry_time_bucket,
        service_name AS service_name,
        resource_attributes AS filtered_attributes,
        arrayJoin(filtered_attributes) AS attribute,
        'resource' AS attribute_type,
        attribute.1 AS attribute_key,
        attribute.2 AS attribute_value,
        sumSimpleState(1) AS attribute_count
    FROM metrics2_input
    WHERE has_labels
    GROUP BY
        team_id,
        metric_name,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        filtered_attributes
);
