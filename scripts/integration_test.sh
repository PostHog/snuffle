#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CH_URL="${SNUFFLE_E2E_CH_URL:-http://localhost:8123}"

cd "$ROOT_DIR"

started_docker=0
cleanup() {
  if [[ "$started_docker" == "1" && "${SNUFFLE_E2E_KEEP_DOCKER:-}" != "1" ]]; then
    docker compose down
  fi
}
trap cleanup EXIT

if [[ "${SNUFFLE_E2E_SKIP_DOCKER:-}" != "1" ]]; then
  if docker compose up -d clickhouse; then
    started_docker=1
  elif curl -fsS "${CH_URL%/}/ping" >/dev/null; then
    docker compose rm -f clickhouse >/dev/null 2>&1 || true
    docker compose down --remove-orphans >/dev/null 2>&1 || true
    echo "Docker Compose ClickHouse startup failed, but $CH_URL is already healthy; using the existing ClickHouse."
  else
    docker compose logs clickhouse || true
    docker compose rm -f clickhouse >/dev/null 2>&1 || true
    docker compose down --remove-orphans >/dev/null 2>&1 || true
    echo "Failed to start ClickHouse and no healthy ClickHouse is available at $CH_URL" >&2
    exit 1
  fi
fi

for attempt in {1..60}; do
  if curl -fsS "${CH_URL%/}/ping" >/dev/null; then
    break
  fi
  if [[ "$attempt" == "60" ]]; then
    docker compose logs clickhouse || true
    echo "ClickHouse did not become ready at $CH_URL" >&2
    exit 1
  fi
  sleep 1
done

go test ./...

SNUFFLE_E2E=1 \
SNUFFLE_E2E_CH_URL="$CH_URL" \
go test -count=1 -run '^TestEndToEndClickHouse$' ./internal/snuffle -timeout=3m
