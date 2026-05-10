DROP TABLE IF EXISTS metrics_samples SYNC;
DROP TABLE IF EXISTS metrics_histograms SYNC;
DROP TABLE IF EXISTS metrics_exemplars SYNC;
DROP TABLE IF EXISTS metrics_metadata SYNC;
DROP TABLE IF EXISTS metrics_label_index SYNC;
DROP TABLE IF EXISTS metrics_series SYNC;

CREATE TABLE metrics_series
(
    id UInt64,
    metric_name LowCardinality(String),
    labels_json String,
    min_time DateTime64(3, 'UTC'),
    max_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (metric_name, id)
SETTINGS index_granularity = 1024;

ALTER TABLE metrics_series
    ADD PROJECTION by_id
    (
        SELECT id, metric_name, labels_json, min_time, max_time
        ORDER BY id
    );

CREATE TABLE metrics_label_index
(
    metric_name LowCardinality(String),
    label_name LowCardinality(String),
    label_value String,
    id UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (metric_name, label_name, label_value, id)
SETTINGS index_granularity = 1024, deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE metrics_label_index
    ADD PROJECTION by_label_value
    (
        SELECT metric_name, label_name, label_value, id
        ORDER BY (label_name, label_value, id, metric_name)
    );

ALTER TABLE metrics_label_index
    ADD PROJECTION by_id_label
    (
        SELECT metric_name, label_name, label_value, id
        ORDER BY (id, label_name, metric_name, label_value)
    );

CREATE TABLE metrics_samples
(
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    value Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_histograms
(
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    histogram String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_exemplars
(
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    value Float64,
    labels_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_metadata
(
    metric_family_name LowCardinality(String),
    type LowCardinality(String),
    unit String,
    help String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY metric_family_name
SETTINGS index_granularity = 1024;
