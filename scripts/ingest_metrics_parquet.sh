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
      team_id,
      id,
      metric_name,
      labels_json,
      min_time,
      max_time
    FROM
    (
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
    )
    FORMAT Native
  " | "${client[@]}" --query "INSERT INTO metrics_series FORMAT Native"

echo "Loading metrics_samples from $DATA_FILE"
clickhouse-local \
  --max_threads="$LOCAL_MAX_THREADS" \
  --query "
    SELECT
      data.team_id,
      series.metric_name,
      data.timestamp,
      data.id,
      data.value,
      data.version
    FROM
    (
      SELECT
        toUInt64($TEAM_ID) AS team_id,
        timestamp,
        cityHash64(toString(id)) AS id,
        value,
        toUInt64(toUnixTimestamp64Milli(timestamp)) AS version
      FROM file('$DATA_FILE', Parquet)
    ) AS data
    ANY INNER JOIN
    (
      SELECT
        toUInt64($TEAM_ID) AS team_id,
        cityHash64(toString(id)) AS id,
        any(metric_name) AS metric_name
      FROM file('$TAGS_FILE', Parquet)
      GROUP BY team_id, id
    ) AS series USING (team_id, id)
    FORMAT Native
  " | "${client[@]}" --query "INSERT INTO metrics_samples FORMAT Native"

echo "Finished ingest. Table sizes:"
"${client[@]}" --query "
  SELECT
    table,
    formatReadableQuantity(sum(rows)) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS bytes
  FROM system.parts
  WHERE active AND database = currentDatabase() AND table IN ('metrics_series', 'metrics_label_index', 'metrics_samples', 'metrics_histograms', 'metrics_exemplars', 'metrics_metadata')
  GROUP BY table
  ORDER BY table
  FORMAT PrettyCompact
"
