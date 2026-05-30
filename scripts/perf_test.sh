#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORKDIR="${PERF_WORKDIR:-$ROOT/.perf}"
RESULTS_FILE="${PERF_RESULTS_FILE:-$ROOT/perf-results.json}"
mkdir -p "$WORKDIR"

TSBS_VERSION="${TSBS_VERSION:-v0.0.0-20260527045238-8323e59c7402}"
TSBS_USE_CASE="${TSBS_USE_CASE:-devops}"
TSBS_SCALE="${TSBS_SCALE:-1000}"
TSBS_START="${TSBS_START:-2016-01-01T00:00:00Z}"
TSBS_END="${TSBS_END:-2016-01-01T01:00:00Z}"
TSBS_INTERVAL="${TSBS_INTERVAL:-15s}"
TSBS_SEED="${TSBS_SEED:-1}"
TSBS_WORKERS="${TSBS_WORKERS:-16}"
TSBS_BATCH_SIZE="${TSBS_BATCH_SIZE:-250000}"
TSBS_REPORTING_PERIOD="${TSBS_REPORTING_PERIOD:-5s}"

CH_ADDR="${CH_ADDR:-localhost:9000}"
CH_HOST="${CH_HOST:-${CH_ADDR%%:*}}"
CH_NATIVE_PORT="${CH_NATIVE_PORT:-${CH_ADDR##*:}}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DATABASE="${CH_DATABASE:-snuffle_perf}"

SIDECAR_HOST="${SIDECAR_HOST:-127.0.0.1}"
SIDECAR_PORT="${SIDECAR_PORT:-9091}"
SNUFFLE_URL="${PERF_SNUFFLE_URL:-http://$SIDECAR_HOST:$SIDECAR_PORT}"
SNUFFLE_DEFAULT_TEAM_ID="${SNUFFLE_DEFAULT_TEAM_ID:-0}"
REMOTE_WRITE_SAMPLE_INTERVAL="${REMOTE_WRITE_SAMPLE_INTERVAL:-$TSBS_INTERVAL}"
CH_TIMEOUT_SECONDS="${CH_TIMEOUT_SECONDS:-120}"
PROMQL_QUERY_TIMEOUT_SECONDS="${PROMQL_QUERY_TIMEOUT_SECONDS:-120}"

BRIDGE_BENCH_CONCURRENCY="${BRIDGE_BENCH_CONCURRENCY:-10}"
BRIDGE_BENCH_WARMUP="${BRIDGE_BENCH_WARMUP:-10}"
BRIDGE_BENCHTIME="${BRIDGE_BENCHTIME:-50x}"
BRIDGE_BENCH_TIMEOUT="${BRIDGE_BENCH_TIMEOUT:-120s}"
PERF_COMPARE_TOLERANCE="${PERF_COMPARE_TOLERANCE:-0}"
PERF_FAIL_ON_SLOWER="${PERF_FAIL_ON_SLOWER:-false}"
PERF_START_CLICKHOUSE="${PERF_START_CLICKHOUSE:-1}"
PERF_MIN_ROWS="${PERF_MIN_ROWS:-1000000}"
PERF_SCHEMA="${PERF_SCHEMA:-current}"
SNUFFLE_SAMPLE_ATTRIBUTES="${SNUFFLE_SAMPLE_ATTRIBUTES:-}"

case "$PERF_SCHEMA" in
  current)
    PERF_SCHEMA_FILE="${PERF_SCHEMA_FILE:-$ROOT/scripts/create_metrics_schema.sql}"
    CH_SCHEMA_LAYOUT="${CH_SCHEMA_LAYOUT:-current}"
    SNUFFLE_SAMPLE_ATTRIBUTES="${SNUFFLE_SAMPLE_ATTRIBUTES:-0}"
    ;;
  posthog|posthog_compat)
    PERF_SCHEMA_FILE="${PERF_SCHEMA_FILE:-$ROOT/scripts/create_metrics_posthog_schema.sql}"
    CH_SCHEMA_LAYOUT="${CH_SCHEMA_LAYOUT:-posthog}"
    SNUFFLE_SAMPLE_ATTRIBUTES="${SNUFFLE_SAMPLE_ATTRIBUTES:-1}"
    ;;
  *)
    PERF_SCHEMA_FILE="${PERF_SCHEMA_FILE:-$PERF_SCHEMA}"
    CH_SCHEMA_LAYOUT="${CH_SCHEMA_LAYOUT:-current}"
    SNUFFLE_SAMPLE_ATTRIBUTES="${SNUFFLE_SAMPLE_ATTRIBUTES:-0}"
    ;;
esac

DATA_KEY="$(printf '%s' "$TSBS_USE_CASE-scale-$TSBS_SCALE-$TSBS_START-$TSBS_END-$TSBS_INTERVAL-$TSBS_SEED" | tr -c 'A-Za-z0-9_.-' '_')"
DATA_FILE="${PERF_DATA_FILE:-$WORKDIR/tsbs-$DATA_KEY.prom}"
LOAD_RESULTS="$WORKDIR/tsbs-load-results.json"
LOAD_OUTPUT="$WORKDIR/tsbs-load.out"
BENCH_OUTPUT="$WORKDIR/go-bench.out"
CURRENT_RESULTS="$WORKDIR/perf-results.current.json"
SNUFFLE_BIN="$WORKDIR/snuffle"
SNUFFLE_LOG="$WORKDIR/snuffle.log"
SNUFFLE_PID=""

cleanup() {
  if [[ -n "$SNUFFLE_PID" ]]; then
    kill "$SNUFFLE_PID" >/dev/null 2>&1 || true
    wait "$SNUFFLE_PID" >/dev/null 2>&1 || true
  fi
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
  if [[ -f "$SNUFFLE_LOG" ]]; then
    tail -n 80 "$SNUFFLE_LOG" >&2 || true
  fi
  return 1
}

sample_count() {
  ch_client --query "SELECT count() FROM metrics_samples" | tr -d '[:space:]'
}

wait_for_samples() {
  local expected="$1"
  if [[ -z "$expected" || "$expected" == "0" ]]; then
    return 0
  fi
  ch_client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" >/dev/null 2>&1 || true
  for _ in $(seq 1 120); do
    local got
    got="$(sample_count || true)"
    if [[ "$got" =~ ^[0-9]+$ && "$got" -ge "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "metrics_samples did not reach expected row count: got $(sample_count || echo unknown), expected $expected" >&2
  return 1
}

if is_true "$PERF_START_CLICKHOUSE"; then
  docker compose up -d clickhouse
fi

wait_for_clickhouse
validate_identifier "$CH_DATABASE"
ch_base_client --query "CREATE DATABASE IF NOT EXISTS $CH_DATABASE"
ch_client --multiquery < "$PERF_SCHEMA_FILE"

if [[ -z "${PERF_SNUFFLE_URL:-}" ]]; then
  go build -o "$SNUFFLE_BIN" ./cmd/snuffle
  env \
    CH_ADDR="$CH_ADDR" \
    CH_USER="$CH_USER" \
    CH_PASSWORD="$CH_PASSWORD" \
    CH_DATABASE="$CH_DATABASE" \
    CH_SCHEMA_LAYOUT="$CH_SCHEMA_LAYOUT" \
    SIDECAR_HOST="$SIDECAR_HOST" \
    SIDECAR_PORT="$SIDECAR_PORT" \
    SNUFFLE_DEFAULT_TEAM_ID="$SNUFFLE_DEFAULT_TEAM_ID" \
    REMOTE_WRITE_SAMPLE_INTERVAL="$REMOTE_WRITE_SAMPLE_INTERVAL" \
    SNUFFLE_SAMPLE_ATTRIBUTES="$SNUFFLE_SAMPLE_ATTRIBUTES" \
    CH_TIMEOUT_SECONDS="$CH_TIMEOUT_SECONDS" \
    PROMQL_QUERY_TIMEOUT_SECONDS="$PROMQL_QUERY_TIMEOUT_SECONDS" \
    "$SNUFFLE_BIN" >"$SNUFFLE_LOG" 2>&1 &
  SNUFFLE_PID="$!"
fi

wait_for_http "$SNUFFLE_URL/-/healthy"

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

go run ./cmd/snuffle-tsbs-replay \
  --file "$DATA_FILE" \
  --url "$SNUFFLE_URL/api/v1/write" \
  --workers "$TSBS_WORKERS" \
  --batch-size "$TSBS_BATCH_SIZE" \
  --reporting-period "$TSBS_REPORTING_PERIOD" \
  --results-file "$LOAD_RESULTS" | tee "$LOAD_OUTPUT"

EXPECTED_ROWS="$(awk '/^loaded [0-9]+ rows/{print $2}' "$LOAD_OUTPUT" | tail -n 1)"
if [[ ! "$EXPECTED_ROWS" =~ ^[0-9]+$ ]]; then
  echo "could not parse TSBS loaded row count from $LOAD_OUTPUT" >&2
  exit 1
fi
if [[ "$EXPECTED_ROWS" -lt "$PERF_MIN_ROWS" ]]; then
  echo "TSBS dataset is too small: loaded $EXPECTED_ROWS rows, minimum is $PERF_MIN_ROWS" >&2
  echo "Increase TSBS_SCALE or TSBS_END, or lower PERF_MIN_ROWS for a local smoke run." >&2
  exit 1
fi
wait_for_samples "$EXPECTED_ROWS"

env \
  BRIDGE_BENCH_URL="$SNUFFLE_URL" \
  BRIDGE_BENCH_PROFILE=tsbs \
  BRIDGE_BENCH_CONCURRENCY="$BRIDGE_BENCH_CONCURRENCY" \
  BRIDGE_BENCH_WARMUP="$BRIDGE_BENCH_WARMUP" \
  BRIDGE_BENCH_TIMEOUT="$BRIDGE_BENCH_TIMEOUT" \
  go test -run '^$' -bench '^BenchmarkBridgeHTTP$' ./internal/perftest -benchtime="$BRIDGE_BENCHTIME" -timeout=30m | tee "$BENCH_OUTPUT"

go run ./cmd/snuffle-perf-report \
  --results "$RESULTS_FILE" \
  --current-output "$CURRENT_RESULTS" \
  --load "$LOAD_RESULTS" \
  --bench "$BENCH_OUTPUT" \
  --rows "$EXPECTED_ROWS" \
  --tolerance "$PERF_COMPARE_TOLERANCE" \
  --fail-on-slower="$PERF_FAIL_ON_SLOWER" \
  --source-name tsbs \
  --source-version "$TSBS_VERSION" \
  --source-use-case "$TSBS_USE_CASE" \
  --source-scale "$TSBS_SCALE" \
  --source-start "$TSBS_START" \
  --source-end "$TSBS_END" \
  --source-interval "$TSBS_INTERVAL" \
  --source-seed "$TSBS_SEED" \
  --source-workers "$TSBS_WORKERS" \
  --source-batch-size "$TSBS_BATCH_SIZE" \
  --query-concurrency "$BRIDGE_BENCH_CONCURRENCY" \
  --query-benchtime "$BRIDGE_BENCHTIME"
