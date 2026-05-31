#!/usr/bin/env bash
set -euo pipefail

# Benchmarks this bridge's Loki-compatible API against a real Loki endpoint.
#
# The dataset comes from Grafana Loki's pkg/logql/bench generator. The script
# streams synthetic bench logs, pushes the same batches to both endpoints, then
# runs a small workload derived from pkg/logql/bench/queries/fast.
#
# Required:
#   REAL_LOKI_URL=http://localhost:3100
#   SNUFFLE_URL=http://localhost:9091
#
# Optional:
#   LOKI_REPO=/path/to/grafana/loki
#   TENANT=42
#   LINES=50000
#   BATCH_LINES=1000
#   RUNS=5
#   BENCH_START=2024-01-01T00:00:00Z
#   BENCH_SPREAD=24h

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOKI_REPO="${LOKI_REPO:-$ROOT_DIR/.cache/loki}"
REAL_LOKI_URL="${REAL_LOKI_URL:-http://localhost:3100}"
SNUFFLE_URL="${SNUFFLE_URL:-http://localhost:9091}"
TENANT="${TENANT:-42}"
LINES="${LINES:-50000}"
BATCH_LINES="${BATCH_LINES:-1000}"
RUNS="${RUNS:-5}"
BENCH_START="${BENCH_START:-2024-01-01T00:00:00Z}"
BENCH_SPREAD="${BENCH_SPREAD:-24h}"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/.cache/logql-bench}"

mkdir -p "$WORK_DIR" "$(dirname "$LOKI_REPO")"

if [[ ! -d "$LOKI_REPO/pkg/logql/bench" ]]; then
  git clone --depth 1 https://github.com/grafana/loki.git "$LOKI_REPO"
fi

DATA_FILE="$WORK_DIR/loki-bench.ndjson"

echo "Generating $LINES log lines with Loki pkg/logql/bench ..."
set +o pipefail
(cd "$LOKI_REPO" && go run ./pkg/logql/bench/cmd/stream -format=json -start="$BENCH_START" -spread="$BENCH_SPREAD") \
  | head -n "$LINES" > "$DATA_FILE"
set -o pipefail

python3 - "$DATA_FILE" "$REAL_LOKI_URL" "$SNUFFLE_URL" "$TENANT" "$BATCH_LINES" "$RUNS" "$BENCH_START" "$BENCH_SPREAD" <<'PY'
import datetime as dt
import collections
import hashlib
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

data_file, real_url, snuffle_url, tenant, batch_lines, runs, bench_start, bench_spread = sys.argv[1:]
batch_lines = int(batch_lines)
runs = int(runs)

def parse_time(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value).astimezone(dt.timezone.utc)

def duration_to_seconds(value):
    units = {"s": 1, "m": 60, "h": 3600}
    return int(float(value[:-1]) * units[value[-1]])

def canonical_ts(value):
    if isinstance(value, str):
        if value.isdigit():
            return value
        return str(int(round(float(value) * 1_000_000_000)))
    return str(int(round(float(value) * 1_000_000_000)))

def canonical_labels(labels):
    return {str(k): str(labels[k]) for k in sorted(labels or {})}

def canonical_query_data(data, rows_only=False):
    result_type = data.get("resultType")
    result = data.get("result") or []
    if result_type == "streams":
        rows = []
        for stream in result:
            labels = canonical_labels(stream.get("stream") or {})
            for value in stream.get("values") or []:
                item = {"ts": canonical_ts(value[0]), "line": str(value[1])}
                if not rows_only:
                    item["stream"] = labels
                if not rows_only and len(value) > 2:
                    item["metadata"] = canonical_labels(value[2] or {})
                rows.append(item)
        rows.sort(key=lambda item: json.dumps(item, sort_keys=True))
        return {"resultType": result_type, "result": rows}
    if result_type in ("matrix", "vector"):
        series = []
        for item in result:
            metric = canonical_labels(item.get("metric") or {})
            if result_type == "matrix":
                values = [[canonical_ts(value[0]), str(value[1])] for value in item.get("values") or []]
                series.append({"metric": metric, "values": values})
            else:
                value = item.get("value") or []
                series.append({"metric": metric, "value": [canonical_ts(value[0]), str(value[1])] if len(value) >= 2 else value})
        series.sort(key=lambda item: json.dumps(item.get("metric") or {}, sort_keys=True))
        return {"resultType": result_type, "result": series}
    return {"resultType": result_type, "result": result}

def flattened_stream_rows(data):
    rows = []
    if data.get("resultType") != "streams":
        return rows
    for stream in data.get("result") or []:
        for value in stream.get("values") or []:
            rows.append((canonical_ts(value[0]), str(value[1])))
    return rows

def row_equivalent(left, right):
    if left.get("resultType") != right.get("resultType"):
        return False
    if left.get("resultType") != "streams":
        return result_hash(left) == result_hash(right)
    left_rows = flattened_stream_rows(left)
    right_rows = flattened_stream_rows(right)
    if collections.Counter(left_rows) == collections.Counter(right_rows):
        return True
    if len(left_rows) != len(right_rows) or not left_rows or not right_rows:
        return False
    cutoff = max(min(ts for ts, _ in left_rows), min(ts for ts, _ in right_rows))
    left_newer = collections.Counter(row for row in left_rows if row[0] > cutoff)
    right_newer = collections.Counter(row for row in right_rows if row[0] > cutoff)
    return left_newer == right_newer

def result_stats(data):
    result = data.get("result") or []
    result_type = data.get("resultType")
    if result_type == "streams":
        return len(result), sum(len(stream.get("values") or []) for stream in result)
    if result_type == "matrix":
        return len(result), sum(len(item.get("values") or []) for item in result)
    return len(result), len(result)

def result_hash(data, rows_only=False):
    encoded = json.dumps(canonical_query_data(data, rows_only), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()

start = parse_time(bench_start)
end = start + dt.timedelta(seconds=duration_to_seconds(bench_spread))
start_ns = int(start.timestamp() * 1_000_000_000)
end_ns = int(end.timestamp() * 1_000_000_000)
step = "60"

def ns_from_rfc3339(value):
    parsed = parse_time(value)
    return str(int(parsed.timestamp() * 1_000_000_000))

def post_json(url, path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url.rstrip("/") + path,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Scope-OrgID": tenant,
            "X-Team-ID": tenant,
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        resp.read()

def push_endpoint(url, rows):
    streams = {}
    for item in rows:
        labels = item.get("labels") or {}
        line = item.get("line", "")
        ts = ns_from_rfc3339(item["ts"])
        metadata = {}
        for key, value in (item.get("resource") or {}).items():
            metadata[f"resource_{key}"] = str(value)
        trace = item.get("trace") or {}
        for key in ("trace_id", "span_id"):
            if trace.get(key):
                metadata[key] = str(trace[key])
        key = tuple(sorted((str(k), str(v)) for k, v in labels.items()))
        stream = streams.setdefault(key, {"stream": dict(key), "values": []})
        value = [ts, line]
        if metadata:
            value.append(metadata)
        stream["values"].append(value)
    post_json(url, "/loki/api/v1/push", {"streams": list(streams.values())})

def load_batches():
    batch = []
    with open(data_file, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_lines:
                yield batch
                batch = []
    if batch:
        yield batch

for name, url in (("real_loki", real_url), ("snuffle", snuffle_url)):
    print(f"pushing to {name} {url}")
    for batch in load_batches():
        push_endpoint(url, batch)

queries = [
    ("basic-selector", "log", '{service_name=~".+"}'),
    ("line-filter", "log", '{service_name=~".+"} |= "level"'),
    ("regex-error", "log", '{service_name=~".+"} |~ "(?i)error"'),
    ("negative-debug", "log", '{service_name=~".+"} !~ "(?i)debug"'),
    ("sum-count", "metric", 'sum(count_over_time({service_name=~".+"}[5m]))'),
    ("sum-rate-filter", "metric", 'sum(rate({service_name=~".+"} |= "level" [5m]))'),
    ("count-by-service", "metric", 'sum by (service_name) (count_over_time({service_name=~".+"}[5m]))'),
]

def query(url, q, kind):
    params = {
        "query": q,
        "start": str(start_ns),
        "end": str(end_ns),
        "limit": "5000",
        "direction": "backward",
    }
    if kind == "metric":
        params["step"] = step
    path = "/loki/api/v1/query_range?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url.rstrip("/") + path,
        headers={"X-Scope-OrgID": tenant, "X-Team-ID": tenant},
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = json.loads(resp.read().decode())
    elapsed = time.perf_counter() - t0
    data = payload.get("data", {})
    series_count, sample_count = result_stats(data)
    return elapsed, data.get("resultType"), series_count, sample_count, result_hash(data), result_hash(data, True), data

print("\nquery,kind,endpoint,min_s,median_s,max_s,result_type,result_count,sample_count,result_hash,row_hash,stable_hash")
summaries = {}
for desc, kind, q in queries:
    for endpoint_name, url in (("real_loki", real_url), ("snuffle", snuffle_url)):
        timings = []
        result_type = ""
        result_count = 0
        sample_count = 0
        hashes = []
        row_hashes = []
        last_data = {}
        for _ in range(runs):
            elapsed, result_type, result_count, sample_count, digest, row_digest, last_data = query(url, q, kind)
            timings.append(elapsed)
            hashes.append(digest)
            row_hashes.append(row_digest)
        stable_hash = len(set(hashes)) == 1
        summaries[(desc, endpoint_name)] = {
            "hash": hashes[-1],
            "row_hash": row_hashes[-1],
            "stable": stable_hash,
            "result_type": result_type,
            "result_count": result_count,
            "sample_count": sample_count,
            "data": last_data,
        }
        print(
            f"{desc},{kind},{endpoint_name},"
            f"{min(timings):.6f},{statistics.median(timings):.6f},{max(timings):.6f},"
            f"{result_type},{result_count},{sample_count},{hashes[-1]},{row_hashes[-1]},{stable_hash}"
        )

print("\nquery,parity,row_parity")
for desc, _, _ in queries:
    real = summaries[(desc, "real_loki")]
    snuffle = summaries[(desc, "snuffle")]
    ok = real["hash"] == snuffle["hash"] and real["result_type"] == snuffle["result_type"]
    row_ok = row_equivalent(real["data"], snuffle["data"])
    print(f"{desc},{ok},{row_ok}")
PY
