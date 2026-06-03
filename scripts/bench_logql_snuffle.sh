#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_DATABASE="${CH_DATABASE:-snuffle_logql_perf}"
ROWS="${ROWS:-500000}"
RUNS="${RUNS:-5}"
RESET="${RESET:-1}"
TENANT="${TENANT:-42}"
SIDECAR_PORT="${SIDECAR_PORT:-19091}"
SNUFFLE_URL="${SNUFFLE_URL:-}"
BENCH_START="${BENCH_START:-2026-06-01 00:00:00}"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/.cache/logql-bench}"

mkdir -p "$WORK_DIR"

client=(clickhouse-client --host "$CH_HOST" --port "$CH_PORT")

if [[ "$RESET" == "1" ]]; then
  "${client[@]}" --query "DROP DATABASE IF EXISTS ${CH_DATABASE} SYNC; CREATE DATABASE ${CH_DATABASE}" --multiquery
  "${client[@]}" --database "$CH_DATABASE" --multiquery < "$ROOT_DIR/scripts/create_logs_posthog_schema.sql"
fi

existing_rows="$("${client[@]}" --database "$CH_DATABASE" --query "SELECT count() FROM logs34" 2>/dev/null || echo 0)"
if [[ "$existing_rows" == "0" ]]; then
  echo "Seeding $ROWS log rows into $CH_DATABASE.logs34 ..."
  "${client[@]}" --database "$CH_DATABASE" --query "
INSERT INTO logs34 (team_id, original_expiry_timestamp, uuid, trace_id, span_id, trace_flags, timestamp, observed_timestamp, body, severity_text, severity_number, service_name, resource_attributes, instrumentation_scope, event_name, attributes_map_str)
WITH
    number AS n,
    toDateTime64('$BENCH_START', 6, 'UTC') + toIntervalSecond(n % 86400) + toIntervalMicrosecond(intDiv(n, 86400) % 1000000) AS ts,
    concat('svc-', toString(n % 64)) AS service,
    concat('pod-', toString(n % 256)) AS pod,
    concat('host-', toString(n % 1024)) AS host,
    arrayElement(['us-east', 'us-west', 'eu-central', 'ap-south'], (n % 4) + 1) AS region,
    arrayElement(['prod', 'staging', 'dev'], (n % 3) + 1) AS env,
    if(n % 2 = 0, 'logfmt', 'json') AS fmt,
    if(n % 97 = 0, 'error', if(n % 19 = 0, 'warn', if(n % 7 = 0, 'debug', 'info'))) AS sev,
    if(sev = 'error', 17, if(sev = 'warn', 13, if(sev = 'debug', 5, 9))) AS sevnum,
    arrayElement(['/checkout', '/login', '/search', '/api/items', '/health'], (n % 5) + 1) AS route,
    if(n % 97 = 0, '500', if(n % 23 = 0, '404', '200')) AS status,
    toString(5 + (n % 2000)) AS duration_ms,
    toString(128 + (n % 8192)) AS size_b,
    if(fmt = 'logfmt',
        concat('level=', sev, ' service=', service, ' env=', env, ' region=', region, ' pod=', pod, ' route=', route, ' status=', status, ' duration=', duration_ms, 'ms size=', size_b, 'B request_id=req-', toString(n), if(n % 97 = 0, ' error=true message=checkout_failed', ' message=ok')),
        concat('{\"level\":\"', sev, '\",\"service\":\"', service, '\",\"env\":\"', env, '\",\"region\":\"', region, '\",\"pod\":\"', pod, '\",\"route\":\"', route, '\",\"status\":', status, ',\"duration\":', duration_ms, ',\"size\":', size_b, ',\"message\":\"', if(n % 97 = 0, 'checkout error', 'ok'), '\"}')
    ) AS line
SELECT
    $TENANT,
    ts + toIntervalDay(30),
    concat('bench:', toString(n)),
    concat('trace-', toString(n % 100000)),
    concat('span-', toString(n % 100000)),
    0,
    ts,
    ts,
    line,
    sev,
    sevnum,
    service,
    map('service.name', service, 'host.hostname', host, 'region', region),
    '',
    '',
    map('app__str', 'snuffle-bench', 'env__str', env, 'region__str', region, 'format__str', fmt, 'pod__str', pod, 'host__str', host, 'route__str', route, 'status__str', status, 'duration__str', duration_ms, 'size__str', size_b, 'detected_level__str', sev)
FROM numbers($ROWS)
SETTINGS max_insert_threads = 4"
fi

server_parent=""
server_pid=""
if [[ -z "$SNUFFLE_URL" ]]; then
  SNUFFLE_URL="http://127.0.0.1:${SIDECAR_PORT}"
  CH_ADDR="${CH_HOST}:${CH_PORT}" \
  CH_DATABASE="$CH_DATABASE" \
  CH_LOGS_TABLE=logs34 \
  CH_LOG_ATTRIBUTES_TABLE=log_attributes2 \
  SNUFFLE_DEFAULT_TEAM_ID="$TENANT" \
  SNUFFLE_TEAM_QUERY_PARAM= \
  SIDECAR_HOST=127.0.0.1 \
  SIDECAR_PORT="$SIDECAR_PORT" \
  SNUFFLE_SELF_SCRAPE_ENABLED=false \
  PROMQL_QUERY_TIMEOUT_SECONDS=120 \
  SNUFFLE_LOG_QUERY_MAX_ROWS="$ROWS" \
  go run "$ROOT_DIR/cmd/snuffle" > "$WORK_DIR/snuffle.log" 2>&1 &
  server_parent="$!"
  cleanup() {
    [[ -n "${server_pid:-}" ]] && kill "$server_pid" 2>/dev/null || true
    [[ -n "${server_parent:-}" ]] && kill "$server_parent" 2>/dev/null || true
    [[ -n "${server_parent:-}" ]] && wait "$server_parent" 2>/dev/null || true
  }
  trap cleanup EXIT
  sleep 5
  server_pid="$(pgrep -P "$server_parent" | tail -n 1 || true)"
fi

python3 - "$SNUFFLE_URL" "$RUNS" "$server_pid" <<'PY'
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request

base, runs, server_pid = sys.argv[1], int(sys.argv[2]), sys.argv[3]
start_ns = 1780272000000000000
end_ns = 1780358399000000000

queries = [
    ("log-basic", "log", '{app="snuffle-bench"}'),
    ("log-line-error", "log", '{app="snuffle-bench"} |= "error"'),
    ("log-regex-checkout", "log", '{app="snuffle-bench"} |~ "checkout|failed"'),
    ("log-host", "log", '{app="snuffle-bench",host="host-42"}'),
    ("sum-count", "metric", 'sum(count_over_time({app="snuffle-bench"}[5m]))'),
    ("count-by-service", "metric", 'sum by (service_name) (count_over_time({app="snuffle-bench"}[5m]))'),
    ("rate-by-region", "metric", 'sum by (region) (rate({app="snuffle-bench"} |= "message" [5m]))'),
    ("bytes-by-format", "metric", 'sum by (format) (bytes_over_time({app="snuffle-bench"}[5m]))'),
    ("topk-rate", "metric", 'topk(5, sum by (service_name) (rate({app="snuffle-bench"}[5m])))'),
    ("comparison", "metric", 'sum by (service_name) (count_over_time({app="snuffle-bench"}[5m])) > 10'),
    ("logfmt-sum-duration", "metric", 'sum_over_time({app="snuffle-bench",format="logfmt"} | logfmt | unwrap duration(duration) [5m]) by (service_name)'),
    ("logfmt-avg-size", "metric", 'avg_over_time({app="snuffle-bench",format="logfmt"} | logfmt | unwrap bytes(size) [5m]) by (region)'),
    ("regexp-sum-duration", "metric", 'sum_over_time({app="snuffle-bench",format="logfmt"} | regexp "duration=(?P<duration>[0-9]+ms)" | unwrap duration(duration) [5m]) by (service_name)'),
    ("pattern-avg-size", "metric", 'avg_over_time({app="snuffle-bench",format="logfmt"} | pattern "<_> size=<size> <_>" | unwrap bytes(size) [5m]) by (region)'),
    ("json-avg-duration", "metric", 'avg_over_time({app="snuffle-bench",format="json"} | json | unwrap duration [5m]) by (service_name)'),
    ("json-filter-status", "metric", 'sum_over_time({app="snuffle-bench",format="json"} | json | status >= 500 | unwrap duration [5m]) by (service_name)'),
]

def proc_cpu_seconds(pid):
    if not pid:
        return None
    try:
        fields = open(f"/proc/{pid}/stat", "r", encoding="utf-8").read().split()
        ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return (int(fields[13]) + int(fields[14])) / ticks
    except Exception:
        return None

def proc_rss_mb(pid):
    if not pid:
        return None
    try:
        for line in open(f"/proc/{pid}/status", "r", encoding="utf-8"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except Exception:
        return None
    return None

def query(q, kind):
    params = {"query": q, "start": str(start_ns), "end": str(end_ns), "limit": "5000", "direction": "backward"}
    if kind == "metric":
        params["step"] = "60s"
    t0_cpu = proc_cpu_seconds(server_pid)
    t0 = time.perf_counter()
    with urllib.request.urlopen(base.rstrip("/") + "/loki/api/v1/query_range?" + urllib.parse.urlencode(params), timeout=180) as resp:
        payload = json.loads(resp.read().decode())
    wall = time.perf_counter() - t0
    t1_cpu = proc_cpu_seconds(server_pid)
    if payload.get("status") != "success":
        raise RuntimeError(payload)
    data = payload.get("data", {})
    result = data.get("result") or []
    result_type = data.get("resultType")
    samples = 0
    if result_type == "streams":
        samples = sum(len(item.get("values") or []) for item in result)
    elif result_type == "matrix":
        samples = sum(len(item.get("values") or []) for item in result)
    else:
        samples = len(result)
    cpu = None if t0_cpu is None or t1_cpu is None else max(0.0, t1_cpu - t0_cpu)
    return wall, cpu, proc_rss_mb(server_pid), result_type, len(result), samples

print("name,kind,median_s,min_s,max_s,median_cpu_s,max_rss_mb,result_type,series_or_streams,samples")
for name, kind, q in queries:
    query(q, kind)
    rows = [query(q, kind) for _ in range(runs)]
    wall = [row[0] for row in rows]
    cpu = [row[1] for row in rows if row[1] is not None]
    rss = [row[2] for row in rows if row[2] is not None]
    meta = rows[-1][3:]
    print(
        f"{name},{kind},{statistics.median(wall):.6f},{min(wall):.6f},{max(wall):.6f},"
        f"{statistics.median(cpu) if cpu else 0:.6f},{max(rss) if rss else 0:.1f},{meta[0]},{meta[1]},{meta[2]}",
        flush=True,
    )
PY
