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
    label_value LowCardinality(String),
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

INSERT INTO metrics_series
    (id, metric_name, labels_json, min_time, max_time)
WITH
    100000 AS series_count,
    100 AS samples_per_series,
    1700100000000 AS start_ms,
    15000 AS step_ms
SELECT
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

INSERT INTO metrics_label_index
    (metric_name, label_name, label_value, id)
WITH 100000 AS series_count
SELECT
    metric_name,
    label_name,
    all_tags[label_name] AS label_value,
    id
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
)
ARRAY JOIN mapKeys(all_tags) AS label_name;

ALTER TABLE metrics_label_index MATERIALIZE PROJECTION by_label_value;
ALTER TABLE metrics_label_index MATERIALIZE PROJECTION by_id_label;

INSERT INTO metrics_samples
    (timestamp, id, value)
WITH
    100000 AS series_count,
    100 AS samples_per_series,
    1700100000000 AS start_ms,
    15000 AS step_ms
SELECT
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
