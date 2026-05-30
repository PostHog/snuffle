DROP TABLE IF EXISTS metrics_samples_to_resource_attributes_mv SYNC;
DROP TABLE IF EXISTS metrics_samples_to_metric_attributes_mv SYNC;
DROP TABLE IF EXISTS metrics_series_activity_from_histograms_mv SYNC;
DROP TABLE IF EXISTS metrics_series_activity_from_samples_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_label_index_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_series_mv SYNC;
DROP TABLE IF EXISTS metrics_label_index_from_series_mv SYNC;
DROP TABLE IF EXISTS metrics_samples SYNC;
DROP TABLE IF EXISTS metrics_histograms SYNC;
DROP TABLE IF EXISTS metrics_exemplars SYNC;
DROP TABLE IF EXISTS metrics_metadata SYNC;
DROP TABLE IF EXISTS metric_attributes SYNC;
DROP TABLE IF EXISTS metrics_series_activity SYNC;
DROP TABLE IF EXISTS metrics_label_postings SYNC;
DROP TABLE IF EXISTS metrics_label_index SYNC;
DROP TABLE IF EXISTS metrics_series SYNC;

CREATE TABLE metric_attributes
(
    team_id UInt64 CODEC(DoubleDelta, ZSTD(1)),
    time_bucket DateTime64(0, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    resource_fingerprint UInt64 DEFAULT 0 CODEC(DoubleDelta, ZSTD(1)),
    attribute_key LowCardinality(String) CODEC(ZSTD(1)),
    attribute_value String CODEC(ZSTD(1)),
    attribute_count SimpleAggregateFunction(sum, UInt64),
    attribute_type LowCardinality(String),
    INDEX idx_attribute_key attribute_key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_value attribute_value TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attribute_key_n3 attribute_key TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_attribute_value_n3 attribute_value TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(time_bucket)
ORDER BY (team_id, attribute_type, time_bucket, resource_fingerprint, service_name, attribute_key, attribute_value)
SETTINGS
    deduplicate_merge_projection_mode = 'drop',
    index_granularity = 8192;

CREATE TABLE metrics_series
(
    team_id UInt64,
    id UInt64,
    metric_name LowCardinality(String),
    labels_json String,
    min_time DateTime64(3, 'UTC'),
    max_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (team_id, metric_name, id)
SETTINGS index_granularity = 1024;

ALTER TABLE metrics_series
    ADD PROJECTION by_id
    (
        SELECT team_id, id, metric_name, labels_json, min_time, max_time
        ORDER BY (team_id, id)
    );

CREATE TABLE metrics_label_index
(
    team_id UInt64,
    metric_name LowCardinality(String),
    label_name LowCardinality(String),
    label_value String,
    id UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (team_id, metric_name, label_name, label_value, id)
SETTINGS index_granularity = 1024, deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE metrics_label_index
    ADD PROJECTION by_label_value
    (
        SELECT team_id, metric_name, label_name, label_value, id
        ORDER BY (team_id, label_name, label_value, id, metric_name)
    );

ALTER TABLE metrics_label_index
    ADD PROJECTION by_id_label
    (
        SELECT team_id, metric_name, label_name, label_value, id
        ORDER BY (team_id, id, label_name, metric_name, label_value)
    );

CREATE TABLE metrics_samples
(
    time_bucket DateTime MATERIALIZED toStartOfDay(timestamp) CODEC(DoubleDelta, ZSTD(1)),
    uuid String DEFAULT '' CODEC(ZSTD(1)),
    team_id UInt64 CODEC(ZSTD(1)),
    trace_id String DEFAULT '' CODEC(ZSTD(1)),
    span_id String DEFAULT '' CODEC(ZSTD(1)),
    trace_flags Int32 DEFAULT 0 CODEC(ZSTD(1)),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    observed_timestamp DateTime64(3, 'UTC') DEFAULT timestamp CODEC(DoubleDelta, ZSTD(1)),
    created_at DateTime64(6, 'UTC') MATERIALIZED now64(6) CODEC(DoubleDelta, ZSTD(1)),
    service_name LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    metric_name LowCardinality(String) CODEC(ZSTD(1)),
    metric_type LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    value Float64 CODEC(Gorilla, ZSTD(1)),
    count UInt64 DEFAULT 1 CODEC(T64, ZSTD(1)),
    histogram_bounds Array(Float64) DEFAULT [] CODEC(ZSTD(1)),
    histogram_counts Array(UInt64) DEFAULT [] CODEC(ZSTD(1)),
    unit LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    aggregation_temporality LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    is_monotonic Bool DEFAULT false CODEC(ZSTD(1)),
    resource_attributes Map(String, String) DEFAULT map() CODEC(ZSTD(1)),
    id UInt64 CODEC(DoubleDelta, ZSTD(1)),
    resource_fingerprint UInt64 MATERIALIZED id CODEC(DoubleDelta, ZSTD(1)),
    instrumentation_scope String DEFAULT '' CODEC(ZSTD(1)),
    attributes_map_str Map(String, String) DEFAULT map() CODEC(ZSTD(1)),
    attributes_map_float Map(String, Float64) DEFAULT map() CODEC(ZSTD(1)),
    time_minute DateTime ALIAS toStartOfMinute(timestamp),
    attributes Map(String, String) ALIAS mapApply((k, v) -> (left(k, length(k) - 5), v), attributes_map_str),
    INDEX idx_metric_name_set metric_name TYPE set(100) GRANULARITY 1,
    INDEX idx_metric_type_set metric_type TYPE set(10) GRANULARITY 1,
    INDEX idx_attributes_str_keys mapKeys(attributes_map_str) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attributes_str_values mapValues(attributes_map_str) TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_observed_minmax observed_timestamp TYPE minmax GRANULARITY 1,
    PROJECTION projection_aggregate_counts
    (
        SELECT
            team_id,
            time_bucket,
            toStartOfMinute(timestamp),
            service_name,
            metric_name,
            metric_type,
            resource_fingerprint,
            count() AS event_count,
            sum(value) AS total_value,
            min(value) AS min_value,
            max(value) AS max_value
        GROUP BY
            team_id,
            time_bucket,
            toStartOfMinute(timestamp),
            service_name,
            metric_name,
            metric_type,
            resource_fingerprint
    )
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
PRIMARY KEY (team_id, time_bucket, service_name, metric_name, resource_fingerprint, timestamp)
ORDER BY (team_id, time_bucket, service_name, metric_name, resource_fingerprint, timestamp)
SETTINGS
    index_granularity_bytes = 104857600,
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE TABLE metrics_histograms
(
    team_id UInt64,
    metric_name LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    histogram String,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_exemplars
(
    team_id UInt64,
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    value Float64,
    labels_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_metadata
(
    team_id UInt64,
    metric_family_name LowCardinality(String),
    type LowCardinality(String),
    unit String,
    help String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (team_id, metric_family_name)
SETTINGS index_granularity = 1024;

CREATE MATERIALIZED VIEW metrics_label_index_from_series_mv TO metrics_label_index AS
SELECT
    team_id,
    metric_name,
    tupleElement(label_pair, 1) AS label_name,
    tupleElement(label_pair, 2) AS label_value,
    id
FROM metrics_series
ARRAY JOIN JSONExtractKeysAndValues(labels_json, 'String') AS label_pair;

CREATE MATERIALIZED VIEW metrics_samples_to_metric_attributes_mv TO metric_attributes AS
SELECT
    team_id,
    time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value,
    'metric' AS attribute_type,
    toUInt64(sum(row_count)) AS attribute_count
FROM
(
    SELECT
        team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        service_name,
        resource_fingerprint,
        mapFilter((k, v) -> ((length(k) < 256) AND (length(v) < 256)), attributes) AS filtered_attributes,
        count() AS row_count
    FROM metrics_samples
    GROUP BY
        team_id,
        time_bucket,
        service_name,
        resource_fingerprint,
        filtered_attributes
)
ARRAY JOIN
    mapKeys(filtered_attributes) AS attribute_key,
    mapValues(filtered_attributes) AS attribute_value
GROUP BY
    team_id,
    time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value;

CREATE MATERIALIZED VIEW metrics_samples_to_resource_attributes_mv TO metric_attributes AS
SELECT
    team_id,
    time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value,
    'resource' AS attribute_type,
    toUInt64(sum(row_count)) AS attribute_count
FROM
(
    SELECT
        team_id,
        toStartOfInterval(timestamp, toIntervalMinute(10)) AS time_bucket,
        service_name,
        resource_fingerprint,
        resource_attributes AS filtered_attributes,
        count() AS row_count
    FROM metrics_samples
    GROUP BY
        team_id,
        time_bucket,
        service_name,
        resource_fingerprint,
        filtered_attributes
)
ARRAY JOIN
    mapKeys(filtered_attributes) AS attribute_key,
    mapValues(filtered_attributes) AS attribute_value
GROUP BY
    team_id,
    time_bucket,
    service_name,
    resource_fingerprint,
    attribute_key,
    attribute_value;
