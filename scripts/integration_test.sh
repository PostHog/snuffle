#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CH_ADDR="${SNUFFLE_E2E_CH_ADDR:-localhost:9000}"
CH_HOST="${CH_ADDR%:*}"
CH_PORT="${CH_ADDR##*:}"

cd "$ROOT_DIR"

started_docker=0
cleanup() {
  if [[ "$started_docker" == "1" && "${SNUFFLE_E2E_KEEP_DOCKER:-}" != "1" ]]; then
    docker compose down
  fi
}
trap cleanup EXIT

clickhouse_ready() {
  if command -v clickhouse-client >/dev/null 2>&1; then
    clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --query "SELECT 1" >/dev/null 2>&1
    return $?
  fi
  docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1
}

if [[ "${SNUFFLE_E2E_SKIP_DOCKER:-}" != "1" ]]; then
  if docker compose up -d clickhouse; then
    started_docker=1
  elif clickhouse_ready; then
    docker compose rm -f clickhouse >/dev/null 2>&1 || true
    docker compose down --remove-orphans >/dev/null 2>&1 || true
    echo "Docker Compose ClickHouse startup failed, but $CH_ADDR is already healthy; using the existing ClickHouse."
  else
    docker compose logs clickhouse || true
    docker compose rm -f clickhouse >/dev/null 2>&1 || true
    docker compose down --remove-orphans >/dev/null 2>&1 || true
    echo "Failed to start ClickHouse and no healthy ClickHouse is available at $CH_ADDR" >&2
    exit 1
  fi
fi

for attempt in {1..60}; do
  if clickhouse_ready; then
    break
  fi
  if [[ "$attempt" == "60" ]]; then
    docker compose logs clickhouse || true
    echo "ClickHouse did not become ready at $CH_ADDR" >&2
    exit 1
  fi
  sleep 1
done

go test ./...

SNUFFLE_E2E=1 \
SNUFFLE_E2E_CH_ADDR="$CH_ADDR" \
go test -count=1 -run '^TestEndToEndClickHouse$' ./internal/snuffle -timeout=3m
