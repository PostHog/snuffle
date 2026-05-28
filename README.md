# snuffle

A Go Prometheus API and remote-storage sidecar backed by ClickHouse.

The sidecar in `cmd/snuffle` uses Prometheus's own PromQL parser and
engine for language semantics, while the storage layer uses ClickHouse SQL fast
paths for common high-cardinality query shapes. It does not call ClickHouse
`prometheusQuery` or `prometheusQueryRange`.

## Supported Endpoints

- `GET|POST /api/v1/query`
- `GET|POST /api/v1/query_range`
- `GET|POST /api/v1/labels`
- `GET|POST /api/v1/label/<name>/values`
- `GET|POST /api/v1/series`
- `GET|POST /api/v1/metadata`
- `GET|POST /api/v1/query_exemplars`
- `POST /api/v1/write`
- `POST /api/v1/read`
- `GET /-/healthy`
- `GET /-/ready`
- compatibility stubs for `/api/v1/rules` and `/api/v1/alerts`

PromQL compatibility comes from the upstream Prometheus engine, so functions,
aggregations, binary matching, subqueries, `@`, `offset`, `label_replace`,
`topk`, and related language features are handled by Prometheus rather than by
a local parser clone. The storage layer stores float samples, native
histograms, exemplars, and metric metadata. Streamed remote-read chunk
responses are not implemented.

## Run

```bash
go run ./cmd/snuffle
```

Example against the metrics schema:

```bash
CH_ADDR=localhost:9000 \
CH_DATABASE=default \
CH_SERIES_TABLE=metrics_series \
CH_SAMPLES_TABLE=metrics_samples \
CH_LABEL_INDEX_TABLE=metrics_label_index \
CH_HISTOGRAMS_TABLE=metrics_histograms \
CH_EXEMPLARS_TABLE=metrics_exemplars \
CH_METRICS_TABLE=metrics_metadata \
SIDECAR_PORT=9091 \
go run ./cmd/snuffle
```

Query it like Prometheus:

```bash
curl --get 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=topk(5, load_requests_total{status="200"})' \
  --data-urlencode 'time=1700100135'

curl --get 'http://localhost:9091/api/v1/query_range' \
  --data-urlencode 'query=rate(load_requests_total{instance="host-4242"}[30s])' \
  --data-urlencode 'start=1700100060' \
  --data-urlencode 'end=1700100135' \
  --data-urlencode 'step=15s'
```

For multi-tenant requests, either send `X-Team-ID` or put the tenant in the
path:

```bash
curl -H 'X-Team-ID: 42' --get 'http://localhost:9091/api/v1/query' \
  --data-urlencode 'query=up'

curl --get 'http://localhost:9091/t/42/api/v1/query' \
  --data-urlencode 'query=up'
```

## Storage Layout

The recommended schema is in `scripts/create_metrics_schema.sql`:

- `metrics_series`: append-friendly series metadata with `team_id`, `id`,
  `metric_name`, `labels_json String`, `min_time`, and `max_time`, ordered by
  `(team_id, metric_name, id)`
- `metrics_samples`: float samples with `metric_name`, `timestamp`, `id`,
  `value`, and `version`, using `ReplacingMergeTree(version)` and ordered by
  `(team_id, id, timestamp)`
- `metrics_label_index`: inverted label index
  `(team_id, metric_name, label_name, label_value, id)` for arbitrary label
  pruning
- `metrics_histograms`: native histogram samples stored as remote-write
  protobuf payloads with `metric_name` and `version`, keyed by
  `(team_id, id, timestamp)`
- `metrics_exemplars`: exemplar samples keyed by `(team_id, id, timestamp)`
  with exemplar labels in an opaque JSON string
- `metrics_metadata`: latest metric family `type`, `unit`, and `help`

All hot-path columns are non-null. Missing labels are represented by absence
from the label index and label JSON object. Full labelsets are read from an
opaque JSON string; ClickHouse does not run `JSONExtract` on labels in the
query path.

## Query Shape

- arbitrary positive label filters are pruned through `label_index`
- every ClickHouse query includes `team_id = <tenant>` before series, label,
  sample, histogram, exemplar, or metadata pruning
- exact Prometheus matcher semantics are preserved by a final Go-side matcher
  filter
- instant selectors fetch only the latest sample per series in the lookback
  window
- selective reads use exact selected IDs; broad reads use ClickHouse subqueries
  to avoid oversized `IN (...)` lists
- sample queries read raw replacement rows and rely on
  `ReplacingMergeTree(version)` for duplicate cleanup, avoiding per-query
  `argMax(value, version)` work
- plain `/query_range` selectors use ClickHouse `timeSeriesLastToGrid` so the
  sidecar receives one row per series instead of one row per sample
- safe `/query_range` aggregations use ClickHouse grid functions and aggregate
  the grid server-side, including `rate`, `irate`, `increase`, `delta`, and
  `idelta`
- safe instant aggregations such as `sum by (...)`, `avg`, `count`, `min`,
  `max`, `group`, plus ungrouped `topk` / `bottomk`, are pushed down into
  ClickHouse SQL
- label metadata endpoints push `limit` down into ClickHouse where possible
- `/series` avoids loading samples
- remote write inserts series metadata, label-index rows, float samples,
  native histograms, exemplars, and metric metadata through ClickHouse
  native protocol batches
- remote write snaps float sample and native histogram timestamps to
  `REMOTE_WRITE_SAMPLE_INTERVAL` bucket starts, default `15s`; `version` keeps
  the latest original sample in each bucket, and query SQL dedupes with
  `argMax` so reads do not wait for background merges
- remote read reuses the same label-pruned sample path as PromQL selectors and
  returns float samples, native histograms, and exemplars

## Configuration

Environment variables:

- `CH_ADDR`: ClickHouse native address, default `localhost:9000`; accepts a
  comma-separated list for multiple replicas
- `CH_USER`: ClickHouse user, default `default`
- `CH_PASSWORD`: ClickHouse password
- `CH_DATABASE`: database, default `default`
- `CH_SERIES_TABLE`: series table, default `metrics_series`
- `CH_SAMPLES_TABLE`: samples table, default `metrics_samples`
- `CH_LABEL_INDEX_TABLE`: label index table, default `metrics_label_index`
- `CH_HISTOGRAMS_TABLE`: native histogram table, default `metrics_histograms`
- `CH_EXEMPLARS_TABLE`: exemplar table, default `metrics_exemplars`
- `CH_METRICS_TABLE`: metric metadata table, default
  `metrics_metadata`
- `CH_TIMEOUT_SECONDS`: ClickHouse timeout, default `30`
- `SIDECAR_HOST`: listen host, default `0.0.0.0`
- `SIDECAR_PORT`: listen port, default `9091`
- `PROMQL_QUERY_TIMEOUT_SECONDS`: query timeout, default `30`
- `PROMQL_LOOKBACK_DELTA`: lookback delta, default `5m`
- `PROMQL_MAX_SAMPLES`: Prometheus engine sample limit, default `50000000`
- `CH_MAX_SERIES`: maximum matching series per select, default `1000000`
- `CH_ID_CHUNK_SIZE`: selected-ID batch size, default `20000`
- `CH_AGGREGATE_MAX_THREADS`: per-query `max_threads` for aggregate pushdowns,
  default `4`; set `0` to leave ClickHouse defaults unchanged
- `REMOTE_WRITE_SAMPLE_INTERVAL`: remote-write sample/histogram bucket size,
  default `15s`; set `0` to store incoming timestamps unchanged
- `SNUFFLE_DEFAULT_TEAM_ID`: fallback tenant when no tenant is provided,
  default `0`
- `SNUFFLE_TEAM_HEADER`: tenant header, default `X-Team-ID`
- `SNUFFLE_TEAM_QUERY_PARAM`: tenant query parameter, default `team_id`

Tenant precedence is `/t/{team_id}/api/v1/...` or
`/team/{team_id}/api/v1/...`, then the configured header, then the configured
query parameter, then `SNUFFLE_DEFAULT_TEAM_ID`.

## Docker Demo

Start ClickHouse:

```bash
docker compose up -d clickhouse
```

Create the metrics schema:

```bash
docker exec -i snuffle-clickhouse clickhouse-client --multiquery < scripts/create_metrics_schema.sql
```

Run the sidecar:

```bash
CH_ADDR=localhost:9000 \
CH_DATABASE=default \
CH_SERIES_TABLE=metrics_series \
CH_SAMPLES_TABLE=metrics_samples \
CH_LABEL_INDEX_TABLE=metrics_label_index \
CH_HISTOGRAMS_TABLE=metrics_histograms \
CH_EXEMPLARS_TABLE=metrics_exemplars \
CH_METRICS_TABLE=metrics_metadata \
go run ./cmd/snuffle
```

Large benchmark seeds:

```bash
docker exec -i snuffle-clickhouse clickhouse-client --multiquery < scripts/seed_metrics_large.sql
```

The metrics seed creates 100k series, 400k label-index rows, and 10M samples.
Use Go benchmarks as regression smoke tests, not as product capacity estimates:

```bash
BRIDGE_BENCH_URL=http://localhost:9091 \
BRIDGE_BENCH_PROFILE=large \
BRIDGE_BENCH_CONCURRENCY=10 \
BRIDGE_BENCH_WARMUP=10 \
go test -run '^$' -bench '^BenchmarkBridgeHTTP$' ./internal/perftest -benchtime=100x -timeout=10m
```

Run the local Prometheus TSDB baseline with the same scenario names:

```bash
PROM_TSDB_BENCH=1 \
PROM_TSDB_BENCH_CONCURRENCY=10 \
PROM_TSDB_BENCH_WARMUP=10 \
go test -run '^$' -bench '^BenchmarkPrometheusTSDB$' ./internal/perftest -benchtime=100x -timeout=30m
```

Run the TSBS devops regression benchmark:

```bash
make perf-test
```

This generates TSBS Prometheus remote-write data, replays the generated
protobuf messages through `/api/v1/write`, runs the TSBS HTTP query profile,
and compares the run against `perf-results.json`. Faster runs replace
`perf-results.json`; slower runs keep the existing file and print the
regression size. By default the target starts the local ClickHouse service from
`docker-compose.yml` and uses a `snuffle_perf` database. The default TSBS shape
is about 24.2M rows (`TSBS_SCALE=1000`, 1 hour, 15s interval), and the
target refuses to record a run below `PERF_MIN_ROWS=1000000`. The TSBS module is
pinned by default for repeatability; override `TSBS_VERSION` when intentionally
changing datasets. Set `PERF_START_CLICKHOUSE=0`, `CH_ADDR`, and `CH_DATABASE`
to use an existing ClickHouse/database.

Run the Docker-backed integration suite:

```bash
./scripts/integration_test.sh
```

Production latency depends on hardware, ClickHouse settings, part sizes,
cardinality, query mix, and ingestion/merge load.
