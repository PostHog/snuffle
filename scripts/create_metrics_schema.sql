DROP TABLE IF EXISTS metrics_series_activity_from_histograms_mv SYNC;
DROP TABLE IF EXISTS metrics_series_activity_from_samples_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_label_index_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_series_mv SYNC;
DROP TABLE IF EXISTS metrics_samples SYNC;
DROP TABLE IF EXISTS metrics_histograms SYNC;
DROP TABLE IF EXISTS metrics_exemplars SYNC;
DROP TABLE IF EXISTS metrics_metadata SYNC;
DROP TABLE IF EXISTS metrics_series_activity SYNC;
DROP TABLE IF EXISTS metrics_label_postings SYNC;
DROP TABLE IF EXISTS metrics_label_index SYNC;
DROP TABLE IF EXISTS metrics_series SYNC;

CREATE TABLE metrics_series
(
    team_id UInt64,
    id UInt64,
    bitmap_id UInt64,
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
        SELECT team_id, id, bitmap_id, metric_name, labels_json, min_time, max_time
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

CREATE TABLE metrics_label_postings
(
    team_id UInt64,
    metric_name LowCardinality(String),
    label_name LowCardinality(String),
    label_value String,
    ids AggregateFunction(groupBitmap, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (team_id, metric_name, label_name, label_value)
SETTINGS index_granularity = 128;

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
    team_id UInt64,
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    value Float64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, id, timestamp)
SETTINGS index_granularity = 1024;

CREATE TABLE metrics_series_activity
(
    team_id UInt64,
    metric_name LowCardinality(String),
    bucket DateTime64(3, 'UTC'),
    ids AggregateFunction(groupBitmap, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (team_id, metric_name, bucket)
SETTINGS index_granularity = 128;

CREATE TABLE metrics_histograms
(
    team_id UInt64,
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

CREATE MATERIALIZED VIEW metrics_label_postings_from_series_mv TO metrics_label_postings AS
SELECT
    team_id,
    metric_name,
    '__name__' AS label_name,
    metric_name AS label_value,
    groupBitmapState(bitmap_id) AS ids
FROM metrics_series
GROUP BY team_id, metric_name;

CREATE MATERIALIZED VIEW metrics_label_postings_from_label_index_mv TO metrics_label_postings AS
SELECT
    idx.team_id,
    idx.metric_name,
    idx.label_name,
    idx.label_value,
    groupBitmapState(series.bitmap_id) AS ids
FROM metrics_label_index AS idx
INNER JOIN metrics_series AS series USING (team_id, id)
GROUP BY idx.team_id, idx.metric_name, idx.label_name, idx.label_value;

CREATE MATERIALIZED VIEW metrics_series_activity_from_samples_mv TO metrics_series_activity AS
SELECT
    samples.team_id,
    series.metric_name,
    samples.timestamp AS bucket,
    groupBitmapState(series.bitmap_id) AS ids
FROM metrics_samples AS samples
INNER JOIN metrics_series AS series USING (team_id, id)
GROUP BY samples.team_id, series.metric_name, samples.timestamp;

CREATE MATERIALIZED VIEW metrics_series_activity_from_histograms_mv TO metrics_series_activity AS
SELECT
    histograms.team_id,
    series.metric_name,
    histograms.timestamp AS bucket,
    groupBitmapState(series.bitmap_id) AS ids
FROM metrics_histograms AS histograms
INNER JOIN metrics_series AS series USING (team_id, id)
GROUP BY histograms.team_id, series.metric_name, histograms.timestamp;
