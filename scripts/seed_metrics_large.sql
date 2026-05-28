DROP TABLE IF EXISTS metrics_series_activity_from_histograms_mv SYNC;
DROP TABLE IF EXISTS metrics_series_activity_from_samples_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_label_index_mv SYNC;
DROP TABLE IF EXISTS metrics_label_postings_from_series_mv SYNC;
DROP TABLE IF EXISTS metrics_label_index_from_series_mv SYNC;
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
    team_id UInt64,
    metric_name LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    id UInt64,
    value Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, id, timestamp)
SETTINGS index_granularity = 1024;

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

INSERT INTO metrics_series
    (team_id, id, metric_name, labels_json, min_time, max_time)
WITH
    0 AS team_id,
    100000 AS series_count,
    100 AS samples_per_series,
    1700100000000 AS start_ms,
    15000 AS step_ms
SELECT
    team_id,
    id,
    metric_name,
    toJSONString(all_tags) AS labels_json,
    fromUnixTimestamp64Milli(start_ms) AS min_time,
    fromUnixTimestamp64Milli(start_ms + step_ms * (samples_per_series - 1)) AS max_time
FROM
(
    SELECT
        number + 1 AS id,
        'load_requests_total' AS metric_name,
        map(
            'job', concat('svc-', toString(number % 100)),
            'instance', concat('host-', toString(number)),
            'status', if(number % 5 = 0, '500', '200'),
            'shard', toString(number % 1000)
        ) AS all_tags
    FROM numbers(series_count)
);

ALTER TABLE metrics_series MATERIALIZE PROJECTION by_id;

ALTER TABLE metrics_label_index MATERIALIZE PROJECTION by_label_value;
ALTER TABLE metrics_label_index MATERIALIZE PROJECTION by_id_label;

INSERT INTO metrics_samples
    (team_id, metric_name, timestamp, id, value)
WITH
    0 AS team_id,
    100000 AS series_count,
    100 AS samples_per_series,
    1700100000000 AS start_ms,
    15000 AS step_ms
SELECT
    team_id,
    'load_requests_total' AS metric_name,
    fromUnixTimestamp64Milli(start_ms + step_ms * sample_index) AS timestamp,
    series_id + 1 AS id,
    toFloat64(1000 + series_id * 100 + sample_index * (1 + series_id % 10)) AS value
FROM
(
    SELECT
        intDiv(number, samples_per_series) AS series_id,
        number % samples_per_series AS sample_index
    FROM numbers(series_count * samples_per_series)
);
