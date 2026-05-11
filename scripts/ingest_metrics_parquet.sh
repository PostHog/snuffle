#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAGS_FILE="${1:-$ROOT_DIR/prom_metrics_tags_20260510.parquet}"
DATA_FILE="${2:-$ROOT_DIR/prom_metrics_data_20260510.parquet}"
CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-9000}"
CH_DATABASE="${CH_DATABASE:-default}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
TEAM_ID="${TEAM_ID:-0}"
LOCAL_MAX_THREADS="${LOCAL_MAX_THREADS:-8}"
BUILD_BITMAP_INDEXES="${BUILD_BITMAP_INDEXES:-1}"
ACTIVITY_BUCKET_SECONDS="${ACTIVITY_BUCKET_SECONDS:-15}"

if [[ ! "$TEAM_ID" =~ ^[0-9]+$ ]]; then
  echo "TEAM_ID must be an unsigned integer" >&2
  exit 1
fi

client=(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --database "$CH_DATABASE" --user "$CH_USER")
if [[ -n "$CH_PASSWORD" ]]; then
  client+=(--password "$CH_PASSWORD")
fi

echo "Creating metrics_* tables in ${CH_DATABASE}"
"${client[@]}" --multiquery < "$ROOT_DIR/scripts/create_metrics_schema.sql"

echo "Loading metrics_series from $TAGS_FILE"
clickhouse-local \
  --max_threads="$LOCAL_MAX_THREADS" \
  --query "
    SELECT
      toUInt64($TEAM_ID) AS team_id,
      series_id AS id,
      any(metric_name) AS metric_name,
      any(toJSONString(tags)) AS labels_json,
      min(min_time) AS min_time,
      max(max_time) AS max_time
    FROM
    (
      SELECT
        cityHash64(toString(id)) AS series_id,
        metric_name,
        tags,
        min_time,
        max_time
      FROM file('$TAGS_FILE', Parquet)
    )
    GROUP BY series_id
    FORMAT Native
  " | "${client[@]}" --query "INSERT INTO metrics_series FORMAT Native"

if [[ "$BUILD_BITMAP_INDEXES" != "0" ]]; then
  echo "Building metrics_series_keys"
  "${client[@]}" --query "
    INSERT INTO metrics_series_keys
    SELECT
      team_id,
      id,
      row_number() OVER (PARTITION BY team_id ORDER BY id) AS bitmap_id
    FROM
    (
      SELECT team_id, id
      FROM metrics_series
      GROUP BY team_id, id
    )
  "
fi

echo "Loading metrics_label_index from $TAGS_FILE"
clickhouse-local \
  --max_threads="$LOCAL_MAX_THREADS" \
  --query "
    SELECT
      toUInt64($TEAM_ID) AS team_id,
      metric_name,
      label_name,
      label_value,
      series_id AS id
    FROM
    (
      SELECT
        metric_name,
        label_name,
        tags[label_name] AS label_value,
        cityHash64(toString(id)) AS series_id
      FROM file('$TAGS_FILE', Parquet)
      ARRAY JOIN mapKeys(tags) AS label_name
    )
    FORMAT Native
  " | "${client[@]}" --query "INSERT INTO metrics_label_index FORMAT Native"

if [[ "$BUILD_BITMAP_INDEXES" != "0" ]]; then
  echo "Building metrics_label_postings"
  "${client[@]}" --query "
    INSERT INTO metrics_label_postings
    SELECT
      idx.team_id,
      idx.metric_name,
      idx.label_name,
      idx.label_value,
      groupBitmapState(keys.bitmap_id) AS ids
    FROM metrics_label_index AS idx
    INNER JOIN metrics_series_keys AS keys USING (team_id, id)
    GROUP BY idx.team_id, idx.metric_name, idx.label_name, idx.label_value
  "
  "${client[@]}" --query "
    INSERT INTO metrics_label_postings
    SELECT
      series.team_id,
      series.metric_name,
      '__name__' AS label_name,
      series.metric_name AS label_value,
      groupBitmapState(keys.bitmap_id) AS ids
    FROM
    (
      SELECT team_id, id, any(metric_name) AS metric_name
      FROM metrics_series
      GROUP BY team_id, id
    ) AS series
    INNER JOIN metrics_series_keys AS keys USING (team_id, id)
    GROUP BY series.team_id, series.metric_name
  "
fi

echo "Loading metrics_samples from $DATA_FILE"
clickhouse-local \
  --max_threads="$LOCAL_MAX_THREADS" \
  --query "
    SELECT
      toUInt64($TEAM_ID) AS team_id,
      timestamp,
      cityHash64(toString(id)) AS id,
      value,
      toUInt64(toUnixTimestamp64Milli(timestamp)) AS version
    FROM file('$DATA_FILE', Parquet)
    FORMAT Native
  " | "${client[@]}" --query "INSERT INTO metrics_samples FORMAT Native"

if [[ "$BUILD_BITMAP_INDEXES" != "0" ]]; then
  echo "Building metrics_series_activity"
  "${client[@]}" --query "
    INSERT INTO metrics_series_activity
    SELECT
      samples.team_id,
      series.metric_name,
      toStartOfInterval(samples.timestamp, INTERVAL $ACTIVITY_BUCKET_SECONDS SECOND) AS bucket,
      groupBitmapState(series.bitmap_id) AS ids
    FROM metrics_samples AS samples
    INNER JOIN
    (
      SELECT series.team_id, series.id, series.metric_name, keys.bitmap_id
      FROM
      (
        SELECT team_id, id, any(metric_name) AS metric_name
        FROM metrics_series
        GROUP BY team_id, id
      ) AS series
      INNER JOIN metrics_series_keys AS keys USING (team_id, id)
    ) AS series USING (team_id, id)
    GROUP BY samples.team_id, series.metric_name, samples.timestamp
  "
fi

echo "Finished ingest. Table sizes:"
"${client[@]}" --query "
  SELECT
    table,
    formatReadableQuantity(sum(rows)) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS bytes
  FROM system.parts
  WHERE active AND database = currentDatabase() AND table IN ('metrics_series', 'metrics_series_keys', 'metrics_label_index', 'metrics_label_postings', 'metrics_samples', 'metrics_series_activity', 'metrics_histograms', 'metrics_exemplars', 'metrics_metadata')
  GROUP BY table
  ORDER BY table
  FORMAT PrettyCompact
"
