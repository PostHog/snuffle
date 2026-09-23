DROP TABLE IF EXISTS metrics4_input_to_metrics4_samples SYNC;
DROP TABLE IF EXISTS metrics4_input_to_metrics4_series SYNC;
DROP TABLE IF EXISTS metrics4_input_to_metrics4_names SYNC;
DROP TABLE IF EXISTS metrics4_input_to_metrics4_attributes SYNC;
DROP TABLE IF EXISTS metrics4_input_to_metrics4_resource_attributes SYNC;
DROP TABLE IF EXISTS metrics4_input SYNC;
DROP TABLE IF EXISTS metrics4_samples SYNC;
DROP TABLE IF EXISTS metrics4_series SYNC;
DROP TABLE IF EXISTS metrics4_names SYNC;
DROP TABLE IF EXISTS metrics4_attributes SYNC;

CREATE TABLE metrics4_input
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

CREATE TABLE metrics4_samples
(
    team_id Int32,
    metric_name LowCardinality(String),
    time_bucket DateTime('UTC'),
    series_fingerprint UInt64 CODEC(Delta(8), Default),
    original_expiry_date Date32,
    resource_fingerprint SimpleAggregateFunction(any, UInt64),
    service_name SimpleAggregateFunction(any, LowCardinality(String)),
    metric_type SimpleAggregateFunction(any, LowCardinality(String)),
    unit SimpleAggregateFunction(any, LowCardinality(String)),
    aggregation_temporality SimpleAggregateFunction(any, LowCardinality(String)),
    is_monotonic SimpleAggregateFunction(max, UInt8),
    has_labels SimpleAggregateFunction(max, UInt8),
    instrumentation_scope SimpleAggregateFunction(any, String),
    histogram_bounds SimpleAggregateFunction(anyLast, Array(Float64)),
    _topic SimpleAggregateFunction(any, LowCardinality(String)),
    timestamp_min DateTime64(6, 'UTC') ALIAS arrayMin(timestamp_arr),
    timestamp_max DateTime64(6, 'UTC') ALIAS arrayMax(timestamp_arr),
    timestamp_arr SimpleAggregateFunction(groupArrayArray(10000), Array(DateTime64(6, 'UTC'))) CODEC(DoubleDelta, Default),
    observed_timestamp_arr SimpleAggregateFunction(groupArrayArray(10000), Array(DateTime64(6, 'UTC'))) CODEC(DoubleDelta, Default),
    value_arr SimpleAggregateFunction(groupArrayArray(10000), Array(Float64)) CODEC(Gorilla, Default),
    count_arr SimpleAggregateFunction(groupArrayArray(10000), Array(UInt64)) CODEC(T64, Default),
    histogram_counts_arr SimpleAggregateFunction(groupArrayArray(10000), Array(Array(UInt64))) CODEC(T64, Default),
    trace_id_arr SimpleAggregateFunction(groupArrayArray(10000), Array(String)),
    span_id_arr SimpleAggregateFunction(groupArrayArray(10000), Array(String)),
    trace_flags_arr SimpleAggregateFunction(groupArrayArray(10000), Array(Int32)),
    INDEX idx_metric_type_set metric_type TYPE set(10) GRANULARITY 1,
    INDEX idx_time_bucket_minmax time_bucket TYPE minmax GRANULARITY 1,
    INDEX idx_timestamp_min_minmax arrayMin(timestamp_arr) TYPE minmax GRANULARITY 1,
    INDEX idx_timestamp_max_minmax arrayMax(timestamp_arr) TYPE minmax GRANULARITY 1,
    INDEX idx_trace_id_bf trace_id_arr TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY original_expiry_date
ORDER BY (team_id, metric_name, time_bucket, series_fingerprint)
TTL original_expiry_date
SETTINGS
    index_granularity = 128,
    ttl_only_drop_parts = 1;

CREATE TABLE metrics4_series
(
    team_id Int32,
    metric_name LowCardinality(String),
    series_fingerprint UInt64 CODEC(Delta(8), Default),
    metric_type LowCardinality(String),
    unit LowCardinality(String),
    aggregation_temporality LowCardinality(String),
    is_monotonic Bool DEFAULT false,
    service_name LowCardinality(String),
    instrumentation_scope String,
    resource_attributes Map(LowCardinality(String), String),
    resource_fingerprint UInt64 MATERIALIZED cityHash64(resource_attributes),
    attributes Map(LowCardinality(String), String),
    timestamp DateTime64(6, 'UTC'),
    time_bucket DateTime('UTC') MATERIALIZED toStartOfHour(timestamp),
    original_expiry_timestamp DateTime64(6, 'UTC'),
    INDEX idx_service_set service_name TYPE set(1000) GRANULARITY 1,
    INDEX idx_resource_fingerprint resource_fingerprint TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_keys mapKeys(attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_values mapValues(attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_timestamp_minmax timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_time_bucket_minmax time_bucket TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(timestamp)
PARTITION BY toStartOfWeek(original_expiry_timestamp)
ORDER BY (team_id, metric_name, series_fingerprint, time_bucket)
TTL original_expiry_timestamp
SETTINGS
    index_granularity = 1024,
    ttl_only_drop_parts = 1;

CREATE TABLE metrics4_names
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

CREATE TABLE metrics4_attributes
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

CREATE MATERIALIZED VIEW metrics4_input_to_metrics4_samples TO metrics4_samples
AS SELECT
    team_id,
    metric_name,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS time_bucket,
    series_fingerprint,
    toDate32(original_expiry_timestamp) AS original_expiry_date,
    any(resource_fingerprint) AS resource_fingerprint,
    any(service_name) AS service_name,
    any(metric_type) AS metric_type,
    any(unit) AS unit,
    any(aggregation_temporality) AS aggregation_temporality,
    max(toUInt8(is_monotonic)) AS is_monotonic,
    max(toUInt8(has_labels)) AS has_labels,
    any(instrumentation_scope) AS instrumentation_scope,
    anyLast(histogram_bounds) AS histogram_bounds,
    any(_topic) AS _topic,
    groupArray(10000)(timestamp) AS timestamp_arr,
    groupArray(10000)(observed_timestamp) AS observed_timestamp_arr,
    groupArray(10000)(value) AS value_arr,
    groupArray(10000)(count) AS count_arr,
    groupArray(10000)(histogram_counts) AS histogram_counts_arr,
    groupArray(10000)(trace_id) AS trace_id_arr,
    groupArray(10000)(span_id) AS span_id_arr,
    groupArray(10000)(trace_flags) AS trace_flags_arr
FROM metrics4_input
GROUP BY
    team_id,
    metric_name,
    time_bucket,
    series_fingerprint,
    original_expiry_date;

CREATE MATERIALIZED VIEW metrics4_input_to_metrics4_series TO metrics4_series
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
    timestamp,
    original_expiry_timestamp
FROM metrics4_input
WHERE has_labels;

CREATE MATERIALIZED VIEW metrics4_input_to_metrics4_names TO metrics4_names
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
FROM metrics4_input AS input
WHERE has_labels
GROUP BY team_id, time_bucket, metric_name, original_expiry_time_bucket;

CREATE MATERIALIZED VIEW metrics4_input_to_metrics4_attributes TO metrics4_attributes
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
    FROM metrics4_input
    WHERE has_labels
    GROUP BY
        team_id,
        metric_name,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        filtered_attributes
);

CREATE MATERIALIZED VIEW metrics4_input_to_metrics4_resource_attributes TO metrics4_attributes
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
    FROM metrics4_input
    WHERE has_labels
    GROUP BY
        team_id,
        metric_name,
        time_bucket,
        original_expiry_time_bucket,
        service_name,
        filtered_attributes
);
