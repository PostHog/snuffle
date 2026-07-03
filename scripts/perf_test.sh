#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORKDIR="${PERF_WORKDIR:-$ROOT/.perf}"
RESULTS_FILE="${PERF_RESULTS_FILE:-$ROOT/perf-results.json}"
mkdir -p "$WORKDIR"

PERF_RUNS="${PERF_RUNS:-snuffle_metrics,snuffle_logs}"
PERF_START_CLICKHOUSE="${PERF_START_CLICKHOUSE:-1}"
PERF_COMPARE_TOLERANCE="${PERF_COMPARE_TOLERANCE:-0}"
PERF_FAIL_ON_SLOWER="${PERF_FAIL_ON_SLOWER:-false}"
PERF_REPEAT="${PERF_REPEAT:-1}"
PERF_MIN_ROWS="${PERF_MIN_ROWS:-1000000}"
PERF_MIN_LOG_ROWS="${PERF_MIN_LOG_ROWS:-10000}"

TSBS_VERSION="${TSBS_VERSION:-v0.0.0-20260527045238-8323e59c7402}"
TSBS_USE_CASE="${TSBS_USE_CASE:-devops}"
TSBS_SCALE="${TSBS_SCALE:-50}"
TSBS_START="${TSBS_START:-2016-01-01T00:00:00Z}"
TSBS_END="${TSBS_END:-2016-01-01T01:00:00Z}"
TSBS_INTERVAL="${TSBS_INTERVAL:-15s}"
TSBS_SEED="${TSBS_SEED:-1}"
TSBS_WORKERS="${TSBS_WORKERS:-2}"
TSBS_BATCH_SIZE="${TSBS_BATCH_SIZE:-10000}"
TSBS_REPORTING_PERIOD="${TSBS_REPORTING_PERIOD:-5s}"

POSTHOG_LOG_ROWS="${POSTHOG_LOG_ROWS:-1000000}"
POSTHOG_LOG_START="${POSTHOG_LOG_START:-$(date -u -d 'yesterday' '+%Y-%m-%d 00:00:00')}"
POSTHOG_LOG_RANGE_SECONDS="${POSTHOG_LOG_RANGE_SECONDS:-86400}"
POSTHOG_LOG_STEP="${POSTHOG_LOG_STEP:-60s}"
POSTHOG_LOG_LIMIT="${POSTHOG_LOG_LIMIT:-5000}"

CH_ADDR="${CH_ADDR:-localhost:9000}"
CH_HOST="${CH_HOST:-${CH_ADDR%%:*}}"
CH_NATIVE_PORT="${CH_NATIVE_PORT:-${CH_ADDR##*:}}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DATABASE="${CH_DATABASE:-snuffle_perf}"
CH_LOG_SCHEMA_LAYOUT="${CH_LOG_SCHEMA_LAYOUT:-}"
CH_LOGS_TABLE="${CH_LOGS_TABLE:-}"
CH_LOG_STREAMS_TABLE="${CH_LOG_STREAMS_TABLE:-}"
CH_LOG_STREAM_LABELS_TABLE="${CH_LOG_STREAM_LABELS_TABLE:-}"
CH_LOG_ATTRIBUTES_TABLE="${CH_LOG_ATTRIBUTES_TABLE:-}"
CH_LOG_STREAM_STATS_TABLE="${CH_LOG_STREAM_STATS_TABLE:-}"

SIDECAR_HOST="${SIDECAR_HOST:-127.0.0.1}"
SIDECAR_PORT="${SIDECAR_PORT:-9091}"
SNUFFLE_URL="${PERF_SNUFFLE_URL:-http://$SIDECAR_HOST:$SIDECAR_PORT}"
SNUFFLE_DEFAULT_TEAM_ID="${SNUFFLE_DEFAULT_TEAM_ID:-${TENANT:-0}}"
SNUFFLE_SELF_SCRAPE_ENABLED="${SNUFFLE_SELF_SCRAPE_ENABLED:-false}"
REMOTE_WRITE_SAMPLE_INTERVAL="${REMOTE_WRITE_SAMPLE_INTERVAL:-$TSBS_INTERVAL}"
CH_TIMEOUT_SECONDS="${CH_TIMEOUT_SECONDS:-120}"
PROMQL_QUERY_TIMEOUT_SECONDS="${PROMQL_QUERY_TIMEOUT_SECONDS:-120}"
SNUFFLE_LOG_QUERY_MAX_ROWS="${SNUFFLE_LOG_QUERY_MAX_ROWS:-$POSTHOG_LOG_ROWS}"

BRIDGE_BENCH_CONCURRENCY="${BRIDGE_BENCH_CONCURRENCY:-1}"
BRIDGE_BENCH_WARMUP="${BRIDGE_BENCH_WARMUP:-0}"
BRIDGE_BENCHTIME="${BRIDGE_BENCHTIME:-1x}"
BRIDGE_BENCH_TIMEOUT="${BRIDGE_BENCH_TIMEOUT:-120s}"
BRIDGE_BENCH_GO_TEST_TIMEOUT="${BRIDGE_BENCH_GO_TEST_TIMEOUT:-60m}"
PERF_MEMORY_SAMPLE_INTERVAL_MS="${PERF_MEMORY_SAMPLE_INTERVAL_MS:-100}"

DATA_KEY="$(printf '%s' "$TSBS_USE_CASE-scale-$TSBS_SCALE-$TSBS_START-$TSBS_END-$TSBS_INTERVAL-$TSBS_SEED" | tr -c 'A-Za-z0-9_.-' '_')"
DATA_FILE="${PERF_DATA_FILE:-$WORKDIR/tsbs-$DATA_KEY.prom}"
SNUFFLE_BIN="$WORKDIR/snuffle"
SNUFFLE_PID=""
SNUFFLE_LOG=""
MEMORY_SAMPLER_PID=""

cleanup() {
  stop_memory_sampler
  stop_snuffle
}
trap cleanup EXIT

is_true() {
  case "${1,,}" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

validate_identifier() {
  if [[ ! "$1" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "invalid ClickHouse identifier: $1" >&2
    return 1
  fi
}

validate_uint() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be an unsigned integer, got $value" >&2
    return 1
  fi
}

validate_positive_uint() {
  local name="$1"
  local value="$2"
  validate_uint "$name" "$value"
  if [[ "$value" -lt 1 ]]; then
    echo "$name must be greater than zero, got $value" >&2
    return 1
  fi
}

trim_run_name() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

now_millis() {
  date +%s%3N
}

process_status_kb() {
  local pid="$1"
  local field="$2"
  awk -v field="$field:" '$1 == field { print $2; found = 1; exit } END { if (!found) print 0 }' "/proc/$pid/status" 2>/dev/null || printf '0\n'
}

start_memory_sampler() {
  local pid="$1"
  local state_file="$2"
  if [[ -z "$pid" || ! -r "/proc/$pid/status" ]]; then
    printf '0 0 0\n' > "$state_file"
    MEMORY_SAMPLER_PID=""
    return 0
  fi

  local interval_seconds
  interval_seconds="$(awk -v ms="$PERF_MEMORY_SAMPLE_INTERVAL_MS" 'BEGIN { if (ms <= 0) ms = 100; printf "%.3f", ms / 1000.0 }')"
  (
    local peak_rss_kb=0
    local peak_hwm_kb=0
    local samples=0
    while [[ -r "/proc/$pid/status" ]]; do
      local rss_kb
      local hwm_kb
      rss_kb="$(process_status_kb "$pid" "VmRSS")"
      hwm_kb="$(process_status_kb "$pid" "VmHWM")"
      if [[ "$rss_kb" =~ ^[0-9]+$ && "$rss_kb" -gt "$peak_rss_kb" ]]; then
        peak_rss_kb="$rss_kb"
      fi
      if [[ "$hwm_kb" =~ ^[0-9]+$ && "$hwm_kb" -gt "$peak_hwm_kb" ]]; then
        peak_hwm_kb="$hwm_kb"
      fi
      samples="$((samples + 1))"
      printf '%s %s %s\n' "$((peak_rss_kb * 1024))" "$((peak_hwm_kb * 1024))" "$samples" > "$state_file"
      sleep "$interval_seconds"
    done
  ) &
  MEMORY_SAMPLER_PID="$!"
}

stop_memory_sampler() {
  if [[ -n "${MEMORY_SAMPLER_PID:-}" ]]; then
    kill "$MEMORY_SAMPLER_PID" >/dev/null 2>&1 || true
    wait "$MEMORY_SAMPLER_PID" >/dev/null 2>&1 || true
    MEMORY_SAMPLER_PID=""
  fi
}

clickhouse_now64() {
  ch_client --query "SELECT now64(6) FORMAT TSVRaw"
}

collect_clickhouse_memory() {
  local window_start="$1"
  local window_end="$2"
  local output_file="$3"
  if ! ch_client \
    --param_database="$CH_DATABASE" \
    --param_window_start="$window_start" \
    --param_window_end="$window_end" \
    --query "
      SELECT
        count() AS query_count,
        toUInt64(max(memory_usage)) AS peak_memory_bytes,
        toFloat64(ifNull(avgOrNull(memory_usage), 0)) AS avg_peak_memory_bytes,
        toUInt64(sum(memory_usage)) AS total_peak_memory_bytes,
        toUInt64(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) AS total_cpu_time_us,
        toUInt64(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'])) AS total_user_system_time_us,
        toUInt64(sum(query_duration_ms)) AS total_query_duration_ms,
        toUInt64(sum(read_rows)) AS read_rows,
        toUInt64(sum(read_bytes)) AS read_bytes
      FROM system.query_log
      WHERE current_database = {database:String}
        AND type = 'QueryFinish'
        AND is_initial_query = 1
        AND query_kind = 'Select'
        AND notEmpty(tables)
        AND event_time_microseconds >= parseDateTime64BestEffort({window_start:String}, 6)
        AND event_time_microseconds < parseDateTime64BestEffort({window_end:String}, 6)
      FORMAT TSVRaw" > "$output_file"; then
    printf '0\t0\t0\t0\t0\t0\t0\t0\t0\n' > "$output_file"
  fi
}

collect_clickhouse_storage() {
  local output_file="$1"
  if ! ch_client \
    --param_database="$CH_DATABASE" \
    --query "
      SELECT
        toUInt64(sum(bytes_on_disk)) AS active_bytes_on_disk,
        toUInt64(sum(data_compressed_bytes)) AS active_compressed_bytes,
        toUInt64(sum(rows)) AS active_rows,
        toUInt64(count()) AS active_parts
      FROM system.parts
      WHERE database = {database:String}
        AND active
        AND startsWith(table, 'metrics_')
      FORMAT TSVRaw" > "$output_file"; then
    printf '0\t0\t0\t0\n' > "$output_file"
  fi
}

write_memory_result() {
  local output_file="$1"
  local process_state="$2"
  local clickhouse_state="$3"
  local storage_state="$4"
  local snuffle_peak_rss_bytes=0
  local snuffle_peak_hwm_bytes=0
  local snuffle_memory_samples=0
  local clickhouse_query_count=0
  local clickhouse_peak_memory_bytes=0
  local clickhouse_avg_peak_memory_bytes=0
  local clickhouse_total_peak_memory_bytes=0
  local clickhouse_total_cpu_time_us=0
  local clickhouse_total_user_system_time_us=0
  local clickhouse_total_query_duration_ms=0
  local clickhouse_read_rows=0
  local clickhouse_read_bytes=0
  local clickhouse_active_bytes_on_disk=0
  local clickhouse_active_compressed_bytes=0
  local clickhouse_active_rows=0
  local clickhouse_active_parts=0

  if [[ -s "$process_state" ]]; then
    read -r snuffle_peak_rss_bytes snuffle_peak_hwm_bytes snuffle_memory_samples < "$process_state" || true
  fi
  if [[ -s "$clickhouse_state" ]]; then
    IFS=$'\t' read -r clickhouse_query_count clickhouse_peak_memory_bytes clickhouse_avg_peak_memory_bytes clickhouse_total_peak_memory_bytes clickhouse_total_cpu_time_us clickhouse_total_user_system_time_us clickhouse_total_query_duration_ms clickhouse_read_rows clickhouse_read_bytes < "$clickhouse_state" || true
  fi
  if [[ -s "$storage_state" ]]; then
    IFS=$'\t' read -r clickhouse_active_bytes_on_disk clickhouse_active_compressed_bytes clickhouse_active_rows clickhouse_active_parts < "$storage_state" || true
  fi

  printf '{\n' > "$output_file"
  printf '  "snuffle_peak_rss_bytes": %s,\n' "${snuffle_peak_rss_bytes:-0}" >> "$output_file"
  printf '  "snuffle_peak_hwm_bytes": %s,\n' "${snuffle_peak_hwm_bytes:-0}" >> "$output_file"
  printf '  "snuffle_memory_samples": %s,\n' "${snuffle_memory_samples:-0}" >> "$output_file"
  printf '  "clickhouse_query_count": %s,\n' "${clickhouse_query_count:-0}" >> "$output_file"
  printf '  "clickhouse_peak_memory_bytes": %s,\n' "${clickhouse_peak_memory_bytes:-0}" >> "$output_file"
  printf '  "clickhouse_avg_peak_memory_bytes": %s,\n' "${clickhouse_avg_peak_memory_bytes:-0}" >> "$output_file"
  printf '  "clickhouse_total_peak_memory_bytes": %s,\n' "${clickhouse_total_peak_memory_bytes:-0}" >> "$output_file"
  printf '  "clickhouse_total_cpu_time_us": %s,\n' "${clickhouse_total_cpu_time_us:-0}" >> "$output_file"
  printf '  "clickhouse_total_user_system_time_us": %s,\n' "${clickhouse_total_user_system_time_us:-0}" >> "$output_file"
  printf '  "clickhouse_total_query_duration_ms": %s,\n' "${clickhouse_total_query_duration_ms:-0}" >> "$output_file"
  printf '  "clickhouse_read_rows": %s,\n' "${clickhouse_read_rows:-0}" >> "$output_file"
  printf '  "clickhouse_read_bytes": %s,\n' "${clickhouse_read_bytes:-0}" >> "$output_file"
  printf '  "clickhouse_active_bytes_on_disk": %s,\n' "${clickhouse_active_bytes_on_disk:-0}" >> "$output_file"
  printf '  "clickhouse_active_compressed_bytes": %s,\n' "${clickhouse_active_compressed_bytes:-0}" >> "$output_file"
  printf '  "clickhouse_active_rows": %s,\n' "${clickhouse_active_rows:-0}" >> "$output_file"
  printf '  "clickhouse_active_parts": %s\n' "${clickhouse_active_parts:-0}" >> "$output_file"
  printf '}\n' >> "$output_file"
}

datetime_to_ns() {
  local value="$1"
  local seconds
  if ! seconds="$(date -u -d "$value UTC" +%s 2>/dev/null)"; then
    echo "could not parse UTC datetime: $value" >&2
    return 1
  fi
  printf '%s000000000' "$seconds"
}

ch_base_client() {
  if command -v clickhouse-client >/dev/null 2>&1; then
    local args=(clickhouse-client --host "$CH_HOST" --port "$CH_NATIVE_PORT" --user "$CH_USER")
    if [[ -n "$CH_PASSWORD" ]]; then
      args+=(--password "$CH_PASSWORD")
    fi
    "${args[@]}" "$@"
    return
  fi

  local args=(docker compose exec -T clickhouse clickhouse-client --user "$CH_USER")
  if [[ -n "$CH_PASSWORD" ]]; then
    args+=(--password "$CH_PASSWORD")
  fi
  "${args[@]}" "$@"
}

ch_client() {
  ch_base_client --database "$CH_DATABASE" "$@"
}

wait_for_clickhouse() {
  for _ in $(seq 1 120); do
    if ch_base_client --query "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ClickHouse did not become ready" >&2
  return 1
}

wait_for_http() {
  local url="$1"
  for _ in $(seq 1 120); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$url did not become ready" >&2
  if [[ -n "$SNUFFLE_LOG" && -f "$SNUFFLE_LOG" ]]; then
    tail -n 80 "$SNUFFLE_LOG" >&2 || true
  fi
  return 1
}

table_count() {
  local table="$1"
  ch_client --query "SELECT count() FROM $table" | tr -d '[:space:]'
}

wait_for_table_rows() {
  local table="$1"
  local expected="$2"
  if [[ -z "$expected" || "$expected" == "0" ]]; then
    return 0
  fi
  ch_client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" >/dev/null 2>&1 || true
  for _ in $(seq 1 120); do
    local got
    got="$(table_count "$table" || true)"
    if [[ "$got" =~ ^[0-9]+$ && "$got" -ge "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "$table did not reach expected row count: got $(table_count "$table" || echo unknown), expected $expected" >&2
  return 1
}

stop_snuffle() {
  if [[ -n "${SNUFFLE_PID:-}" ]]; then
    kill "$SNUFFLE_PID" >/dev/null 2>&1 || true
    wait "$SNUFFLE_PID" >/dev/null 2>&1 || true
    SNUFFLE_PID=""
  fi
}

start_snuffle() {
  local run_dir="$1"
  local schema_layout="$2"
  local samples_table="$3"
  local sample_attributes="$4"
  local log_schema_layout="${5:-$CH_LOG_SCHEMA_LAYOUT}"
  local logs_table="${6:-$CH_LOGS_TABLE}"
  local log_streams_table="${7:-$CH_LOG_STREAMS_TABLE}"
  local log_stream_labels_table="${8:-$CH_LOG_STREAM_LABELS_TABLE}"
  local log_attributes_table="${9:-$CH_LOG_ATTRIBUTES_TABLE}"
  local log_stream_stats_table="${10:-$CH_LOG_STREAM_STATS_TABLE}"
  if [[ -n "${PERF_SNUFFLE_URL:-}" ]]; then
    return 0
  fi
  stop_snuffle
  SNUFFLE_LOG="$run_dir/snuffle.log"
  env \
    CH_ADDR="$CH_ADDR" \
    CH_USER="$CH_USER" \
    CH_PASSWORD="$CH_PASSWORD" \
    CH_DATABASE="$CH_DATABASE" \
    CH_SCHEMA_LAYOUT="$schema_layout" \
    CH_SAMPLES_TABLE="$samples_table" \
    CH_LOG_SCHEMA_LAYOUT="$log_schema_layout" \
    CH_LOGS_TABLE="$logs_table" \
    CH_LOG_STREAMS_TABLE="$log_streams_table" \
    CH_LOG_STREAM_LABELS_TABLE="$log_stream_labels_table" \
    CH_LOG_ATTRIBUTES_TABLE="$log_attributes_table" \
    CH_LOG_STREAM_STATS_TABLE="$log_stream_stats_table" \
    SIDECAR_HOST="$SIDECAR_HOST" \
    SIDECAR_PORT="$SIDECAR_PORT" \
    SNUFFLE_DEFAULT_TEAM_ID="$SNUFFLE_DEFAULT_TEAM_ID" \
    REMOTE_WRITE_SAMPLE_INTERVAL="$REMOTE_WRITE_SAMPLE_INTERVAL" \
    SNUFFLE_SAMPLE_ATTRIBUTES="$sample_attributes" \
    SNUFFLE_SELF_SCRAPE_ENABLED="$SNUFFLE_SELF_SCRAPE_ENABLED" \
    SNUFFLE_LOG_QUERY_MAX_ROWS="$SNUFFLE_LOG_QUERY_MAX_ROWS" \
    CH_TIMEOUT_SECONDS="$CH_TIMEOUT_SECONDS" \
    PROMQL_QUERY_TIMEOUT_SECONDS="$PROMQL_QUERY_TIMEOUT_SECONDS" \
    "$SNUFFLE_BIN" >"$SNUFFLE_LOG" 2>&1 &
  SNUFFLE_PID="$!"
}

ensure_tsbs_data() {
  if [[ ! -s "$DATA_FILE" || "${PERF_REGENERATE_DATA:-0}" != "0" ]]; then
    go run "github.com/timescale/tsbs/cmd/tsbs_generate_data@$TSBS_VERSION" \
      --format prometheus \
      --use-case "$TSBS_USE_CASE" \
      --scale "$TSBS_SCALE" \
      --timestamp-start "$TSBS_START" \
      --timestamp-end "$TSBS_END" \
      --log-interval "$TSBS_INTERVAL" \
      --seed "$TSBS_SEED" \
      --file "$DATA_FILE"
  fi
}

write_load_result() {
  local path="$1"
  local rows="$2"
  local duration_ms="$3"
  local rate
  rate="$(awk -v rows="$rows" -v ms="$duration_ms" 'BEGIN { if (ms > 0) printf "%.6f", rows / (ms / 1000.0); else printf "0.000000" }')"
  printf '{\n "DurationMillis": %s,\n "Totals": {\n  "metricRate": %.6f,\n  "rowRate": %.6f\n }\n}\n' "$duration_ms" "$rate" "$rate" >"$path"
}

run_bridge_bench() {
  local run_dir="$1"
  local profile="$2"
  local bench_output="$run_dir/go-bench.out"
  local process_memory_state="$run_dir/process-memory.state"
  local clickhouse_memory_state="$run_dir/clickhouse-memory.tsv"
  local clickhouse_storage_state="$run_dir/clickhouse-storage.tsv"
  local memory_output="$run_dir/memory-results.json"
  local query_window_start
  local query_window_end
  local bench_status
  ch_client --query "SYSTEM FLUSH LOGS" >/dev/null 2>&1 || true
  query_window_start="$(clickhouse_now64)"
  start_memory_sampler "$SNUFFLE_PID" "$process_memory_state"
  set +e
  env \
    BRIDGE_BENCH_URL="$SNUFFLE_URL" \
    BRIDGE_BENCH_PROFILE="$profile" \
    BRIDGE_BENCH_CONCURRENCY="$BRIDGE_BENCH_CONCURRENCY" \
    BRIDGE_BENCH_WARMUP="$BRIDGE_BENCH_WARMUP" \
    BRIDGE_BENCH_TIMEOUT="$BRIDGE_BENCH_TIMEOUT" \
    BRIDGE_BENCH_LOG_START_NS="$POSTHOG_LOG_START_NS" \
    BRIDGE_BENCH_LOG_END_NS="$POSTHOG_LOG_END_NS" \
    BRIDGE_BENCH_LOG_STEP="$POSTHOG_LOG_STEP" \
    BRIDGE_BENCH_LOG_LIMIT="$POSTHOG_LOG_LIMIT" \
    go test -run '^$' -bench '^BenchmarkBridgeHTTP$' ./internal/perftest -benchtime="$BRIDGE_BENCHTIME" -timeout="$BRIDGE_BENCH_GO_TEST_TIMEOUT" | tee "$bench_output"
  bench_status="${PIPESTATUS[0]}"
  set -e
  stop_memory_sampler
  query_window_end="$(clickhouse_now64)"
  ch_client --query "SYSTEM FLUSH LOGS" >/dev/null 2>&1 || true
  collect_clickhouse_memory "$query_window_start" "$query_window_end" "$clickhouse_memory_state"
  collect_clickhouse_storage "$clickhouse_storage_state"
  write_memory_result "$memory_output" "$process_memory_state" "$clickhouse_memory_state" "$clickhouse_storage_state"
  if [[ "$bench_status" -ne 0 ]]; then
    return "$bench_status"
  fi
}

report_run_attempt() {
  local run_name="$1"
  local run_dir="$2"
  local load_results="$3"
  local bench_output="$4"
  local rows="$5"
  local source_name="$6"
  local source_version="$7"
  local source_use_case="$8"
  local source_scale="$9"
  local source_start="${10}"
  local source_end="${11}"
  local source_interval="${12}"
  local source_seed="${13}"
  local source_workers="${14}"
  local source_batch_size="${15}"
  local attempt="${16}"
  local memory_results="$run_dir/memory-results.json"
  go run ./cmd/snuffle-perf-report \
    --build-only \
    --current-output "$run_dir/perf-results.current.json" \
    --run-name "$run_name" \
    --attempt "$attempt" \
    --repeat-count "$PERF_REPEAT" \
    --load "$load_results" \
    --bench "$bench_output" \
    --memory "$memory_results" \
    --rows "$rows" \
    --source-name "$source_name" \
    --source-version "$source_version" \
    --source-use-case "$source_use_case" \
    --source-scale "$source_scale" \
    --source-start "$source_start" \
    --source-end "$source_end" \
    --source-interval "$source_interval" \
    --source-seed "$source_seed" \
    --source-workers "$source_workers" \
    --source-batch-size "$source_batch_size" \
    --query-concurrency "$BRIDGE_BENCH_CONCURRENCY" \
    --query-benchtime "$BRIDGE_BENCHTIME"
}

select_run_baseline() {
  local run_name="$1"
  local candidates="${RUN_CANDIDATES[$run_name]:-}"
  if [[ -z "$candidates" ]]; then
    echo "no candidates recorded for $run_name" >&2
    exit 1
  fi
  mkdir -p "$WORKDIR/$run_name"
  go run ./cmd/snuffle-perf-report \
    --results "$RESULTS_FILE" \
    --current-output "$WORKDIR/$run_name/perf-results.current.json" \
    --run-name "$run_name" \
    --repeat-count "$PERF_REPEAT" \
    --candidates "$candidates" \
    --tolerance "$PERF_COMPARE_TOLERANCE" \
    --fail-on-slower="$PERF_FAIL_ON_SLOWER"
}

record_candidate() {
  local run_name="$1"
  local candidate_path="$2"
  if [[ -n "${RUN_CANDIDATES[$run_name]:-}" ]]; then
    RUN_CANDIDATES[$run_name]+=",$candidate_path"
  else
    RUN_CANDIDATES[$run_name]="$candidate_path"
  fi
}

run_tsbs_metrics() {
  local attempt="$1"
  local run_name="$2"
  local schema_file="$3"
  local schema_layout="$4"
  local samples_table="$5"
  local sample_attributes="$6"
  local source_name="$7"
  local run_dir="$WORKDIR/$run_name/attempt-$attempt"
  local load_results="$run_dir/tsbs-load-results.json"
  local load_output="$run_dir/tsbs-load.out"
  local bench_output="$run_dir/go-bench.out"
  mkdir -p "$run_dir"

  echo "==> $run_name attempt $attempt/$PERF_REPEAT: recreate metrics schema from $schema_file"
  validate_identifier "$samples_table"
  ch_client --multiquery < "$schema_file"
  start_snuffle "$run_dir" "$schema_layout" "$samples_table" "$sample_attributes"
  wait_for_http "$SNUFFLE_URL/-/healthy"

  ensure_tsbs_data
  go run ./cmd/snuffle-tsbs-replay \
    --file "$DATA_FILE" \
    --url "$SNUFFLE_URL/api/v1/write" \
    --workers "$TSBS_WORKERS" \
    --batch-size "$TSBS_BATCH_SIZE" \
    --reporting-period "$TSBS_REPORTING_PERIOD" \
    --results-file "$load_results" | tee "$load_output"

  local expected_rows
  expected_rows="$(awk '/^loaded [0-9]+ rows/{print $2}' "$load_output" | tail -n 1)"
  if [[ ! "$expected_rows" =~ ^[0-9]+$ ]]; then
    echo "could not parse TSBS loaded row count from $load_output" >&2
    exit 1
  fi
  if [[ "$expected_rows" -lt "$PERF_MIN_ROWS" ]]; then
    echo "TSBS dataset is too small: loaded $expected_rows rows, minimum is $PERF_MIN_ROWS" >&2
    echo "Increase TSBS_SCALE or TSBS_END, or lower PERF_MIN_ROWS for a local smoke run." >&2
    exit 1
  fi
  wait_for_table_rows "$samples_table" "$expected_rows"

  run_bridge_bench "$run_dir" "$run_name"
  report_run_attempt "$run_name" "$run_dir" "$load_results" "$bench_output" "$expected_rows" "$source_name" "$TSBS_VERSION" "$TSBS_USE_CASE" "$TSBS_SCALE" "$TSBS_START" "$TSBS_END" "$TSBS_INTERVAL" "$TSBS_SEED" "$TSBS_WORKERS" "$TSBS_BATCH_SIZE" "$attempt"
  record_candidate "$run_name" "$run_dir/perf-results.current.json"
  stop_snuffle
}

run_posthog_metrics() {
  run_tsbs_metrics "$1" "posthog_metrics" "$ROOT/scripts/create_metrics_posthog_schema.sql" "posthog" "metrics1" "1" "tsbs-posthog-metrics"
}

run_snuffle_metrics() {
  run_tsbs_metrics "$1" "snuffle_metrics" "$ROOT/scripts/create_metrics_schema.sql" "current" "metrics_samples" "0" "tsbs-snuffle-metrics"
}

run_posthog_logs() {
  local attempt="$1"
  local run_name="posthog_logs"
  local run_dir="$WORKDIR/$run_name/attempt-$attempt"
  local load_results="$run_dir/log-load-results.json"
  local bench_output="$run_dir/go-bench.out"
  local logs_table="logs34"
  local attributes_table="log_attributes2"
  mkdir -p "$run_dir"

  echo "==> $run_name attempt $attempt/$PERF_REPEAT: recreate PostHog logs schema"
  validate_identifier "$logs_table"
  ch_client --multiquery < "$ROOT/scripts/create_logs_posthog_schema.sql"

  echo "==> $run_name: seed $POSTHOG_LOG_ROWS log rows"
  local started_ms
  local duration_ms
  started_ms="$(now_millis)"
  ch_client \
    --param_rows="$POSTHOG_LOG_ROWS" \
    --param_tenant="$SNUFFLE_DEFAULT_TEAM_ID" \
    --param_bench_start="$POSTHOG_LOG_START" \
    --multiquery < "$ROOT/scripts/seed_logs_posthog.sql"
  duration_ms="$(( $(now_millis) - started_ms ))"
  wait_for_table_rows "$logs_table" "$POSTHOG_LOG_ROWS"
  if [[ "$POSTHOG_LOG_ROWS" -lt "$PERF_MIN_LOG_ROWS" ]]; then
    echo "log dataset is too small: loaded $POSTHOG_LOG_ROWS rows, minimum is $PERF_MIN_LOG_ROWS" >&2
    echo "Increase POSTHOG_LOG_ROWS, or lower PERF_MIN_LOG_ROWS for a local smoke run." >&2
    exit 1
  fi
  write_load_result "$load_results" "$POSTHOG_LOG_ROWS" "$duration_ms"

  start_snuffle "$run_dir" "posthog" "metrics1" "1" "posthog" "$logs_table" "" "" "$attributes_table" ""
  wait_for_http "$SNUFFLE_URL/-/healthy"
  run_bridge_bench "$run_dir" "posthog_logs"
  report_run_attempt "$run_name" "$run_dir" "$load_results" "$bench_output" "$POSTHOG_LOG_ROWS" "posthog-logs-synthetic" "synthetic-v1" "logs" "$POSTHOG_LOG_ROWS" "$POSTHOG_LOG_START" "${POSTHOG_LOG_RANGE_SECONDS}s" "$POSTHOG_LOG_STEP" "" "" "" "$attempt"
  record_candidate "$run_name" "$run_dir/perf-results.current.json"
  stop_snuffle
}

run_snuffle_logs() {
  local attempt="$1"
  local run_name="snuffle_logs"
  local run_dir="$WORKDIR/$run_name/attempt-$attempt"
  local load_results="$run_dir/log-load-results.json"
  local bench_output="$run_dir/go-bench.out"
  local logs_table="logs"
  local streams_table="log_streams"
  local labels_table="log_stream_labels"
  local attributes_table=""
  local stats_table="log_stream_stats"
  mkdir -p "$run_dir"

  echo "==> $run_name attempt $attempt/$PERF_REPEAT: recreate Snuffle logs schema"
  validate_identifier "$logs_table"
  validate_identifier "$streams_table"
  validate_identifier "$labels_table"
  validate_identifier "$stats_table"
  ch_client --multiquery < "$ROOT/scripts/create_logs_snuffle_schema.sql"

  echo "==> $run_name: seed $POSTHOG_LOG_ROWS log rows"
  local started_ms
  local duration_ms
  started_ms="$(now_millis)"
  ch_client \
    --param_rows="$POSTHOG_LOG_ROWS" \
    --param_tenant="$SNUFFLE_DEFAULT_TEAM_ID" \
    --param_bench_start="$POSTHOG_LOG_START" \
    --multiquery < "$ROOT/scripts/seed_logs_snuffle.sql"
  duration_ms="$(( $(now_millis) - started_ms ))"
  wait_for_table_rows "$logs_table" "$POSTHOG_LOG_ROWS"
  if [[ "$POSTHOG_LOG_ROWS" -lt "$PERF_MIN_LOG_ROWS" ]]; then
    echo "log dataset is too small: loaded $POSTHOG_LOG_ROWS rows, minimum is $PERF_MIN_LOG_ROWS" >&2
    echo "Increase POSTHOG_LOG_ROWS, or lower PERF_MIN_LOG_ROWS for a local smoke run." >&2
    exit 1
  fi
  write_load_result "$load_results" "$POSTHOG_LOG_ROWS" "$duration_ms"

  start_snuffle "$run_dir" "posthog" "metrics1" "1" "snuffle" "$logs_table" "$streams_table" "$labels_table" "$attributes_table" "$stats_table"
  wait_for_http "$SNUFFLE_URL/-/healthy"
  run_bridge_bench "$run_dir" "snuffle_logs"
  report_run_attempt "$run_name" "$run_dir" "$load_results" "$bench_output" "$POSTHOG_LOG_ROWS" "snuffle-logs-synthetic" "synthetic-v1" "logs" "$POSTHOG_LOG_ROWS" "$POSTHOG_LOG_START" "${POSTHOG_LOG_RANGE_SECONDS}s" "$POSTHOG_LOG_STEP" "" "" "" "$attempt"
  record_candidate "$run_name" "$run_dir/perf-results.current.json"
  stop_snuffle
}

run_named() {
  local run_name="$1"
  local attempt="$2"
  case "$run_name" in
    posthog_metrics)
      run_posthog_metrics "$attempt"
      ;;
    snuffle_metrics)
      run_snuffle_metrics "$attempt"
      ;;
    posthog_logs)
      run_posthog_logs "$attempt"
      ;;
    snuffle_logs)
      run_snuffle_logs "$attempt"
      ;;
    *)
      echo "unknown PERF_RUNS entry: $run_name" >&2
      echo "known runs: posthog_metrics, snuffle_metrics, posthog_logs, snuffle_logs" >&2
      exit 1
      ;;
  esac
}

validate_run_name() {
  local run_name="$1"
  case "$run_name" in
    posthog_metrics|snuffle_metrics|posthog_logs|snuffle_logs)
      ;;
    *)
      echo "unknown PERF_RUNS entry: $run_name" >&2
      echo "known runs: posthog_metrics, snuffle_metrics, posthog_logs, snuffle_logs" >&2
      exit 1
      ;;
  esac
}

validate_identifier "$CH_DATABASE"
validate_positive_uint "PERF_REPEAT" "$PERF_REPEAT"
validate_uint "POSTHOG_LOG_ROWS" "$POSTHOG_LOG_ROWS"
validate_uint "POSTHOG_LOG_RANGE_SECONDS" "$POSTHOG_LOG_RANGE_SECONDS"
validate_uint "SNUFFLE_DEFAULT_TEAM_ID" "$SNUFFLE_DEFAULT_TEAM_ID"
POSTHOG_LOG_START_NS="${POSTHOG_LOG_START_NS:-$(datetime_to_ns "$POSTHOG_LOG_START")}"
POSTHOG_LOG_END_NS="${POSTHOG_LOG_END_NS:-$((POSTHOG_LOG_START_NS + (POSTHOG_LOG_RANGE_SECONDS - 1) * 1000000000))}"

if is_true "$PERF_START_CLICKHOUSE"; then
  if ch_base_client --query "SELECT 1" >/dev/null 2>&1; then
    echo "ClickHouse already reachable at $CH_HOST:$CH_NATIVE_PORT; skipping docker compose up"
  else
    docker compose up -d clickhouse
  fi
fi

wait_for_clickhouse
ch_base_client --query "CREATE DATABASE IF NOT EXISTS $CH_DATABASE"

if [[ -z "${PERF_SNUFFLE_URL:-}" ]]; then
  go build -o "$SNUFFLE_BIN" ./cmd/snuffle
fi

declare -A RUN_CANDIDATES=()
IFS=',' read -r -a raw_runs <<< "$PERF_RUNS"
runs=()
for raw_run in "${raw_runs[@]}"; do
  run="$(trim_run_name "$raw_run")"
  if [[ -z "$run" ]]; then
    continue
  fi
  validate_run_name "$run"
  runs+=("$run")
done

if [[ "${#runs[@]}" -eq 0 ]]; then
  echo "PERF_RUNS did not contain any run names" >&2
  exit 1
fi

for attempt in $(seq 1 "$PERF_REPEAT"); do
  echo "==> suite attempt $attempt/$PERF_REPEAT"
  for run in "${runs[@]}"; do
    run_named "$run" "$attempt"
  done
done

for run in "${runs[@]}"; do
  echo "==> $run: selecting slowest candidate from $PERF_REPEAT attempt(s)"
  select_run_baseline "$run"
done
