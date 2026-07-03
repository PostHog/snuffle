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
- `POST /loki/api/v1/push`
- `GET|POST /loki/api/v1/query`
- `GET|POST /loki/api/v1/query_range`
- `GET|POST /loki/api/v1/labels`
- `GET|POST /loki/api/v1/label/<name>/values`
- `GET|POST /loki/api/v1/series`
- `GET /metrics`
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

Snuffle supports two first-class ClickHouse layouts. Select one with
`CH_SCHEMA_LAYOUT=current` or `CH_SCHEMA_LAYOUT=posthog`.

The default Prometheus-native schema is in `scripts/create_metrics_schema.sql`:

- `metrics_series`: append-friendly series metadata with `team_id`, `id`,
  `metric_name`, `labels_json String`, `min_time`, and `max_time`, ordered by
  `(team_id, metric_name, id)`
- `metrics_samples`: float samples with `metric_name`, `timestamp`, `id`,
  and `value`, using `MergeTree` and ordered by
  `(team_id, metric_name, id, timestamp)`
- `metrics_label_index`: inverted label index
  `(team_id, metric_name, label_name, label_value, id)` for arbitrary label
  pruning, with `label_value` stored as `LowCardinality(String)`
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

The posthog-style schema is in `scripts/create_metrics_posthog_schema.sql`.
It uses the real PostHog-style table surface: `metrics`/`metrics1` for samples
and `metric_attributes` for attribute discovery. With
`CH_SCHEMA_LAYOUT=posthog`, Snuffle builds Prometheus labels from
`metric_name`, `service_name`, `resource_attributes`, and `attributes_map_str`,
and computes series identity in ClickHouse with
`cityHash64(metric_name, service_name, mapSort(resource_attributes),
mapSort(attributes_map_str))`.

Snuffle-native logs for Loki/LogQL are in `scripts/create_logs_snuffle_schema.sql`.
This is the optimized layout for new deployments:

- `logs`: one row per log line with tenant, timestamp, expiry, stream id, body,
  and per-entry fields
- `log_streams`: one row per stream id with stream labels and resource
  attributes stored once, plus materialized service/severity columns
- `log_stream_stats`: minute rollups by stream for active-stream pruning and
  fast count/rate/bytes LogQL aggregations

This layout intentionally does not preserve PostHog physical columns. It stores
repeated stream/resource labels once, keeps the hot table narrow, and uses the
stream dictionary for selector pruning while Snuffle reconstructs the logical
Loki label surface at query time.

PostHog-style logs remain supported with `CH_LOG_SCHEMA_LAYOUT=posthog` and
`scripts/create_logs_posthog_schema.sql`. In that mode the Loki push API writes
Loki stream labels and structured metadata into the OpenTelemetry-shaped
`logs34` table:

- `service_name` / `service.name` become the `service_name` column
- `level`, `severity`, and `severity_text` become `severity_text`
- `trace_id` and `span_id` are promoted to their native columns
- structured metadata named `resource_*` or `resource.*` becomes
  `resource_attributes`
- other labels and metadata become `attributes_map_str` entries with the
  PostHog `__str` suffix

LogQL support covers stream selectors, line filters, label filters, `json`,
`logfmt`, `regexp`, `pattern`, `line_format`, `label_format`, `drop`,
`unwrap`, range aggregations such as `count_over_time` and `rate`, simple
aggregations, and `topk`/`bottomk`.

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
- sample queries read raw rows from `MergeTree`; duplicate remote-write retries
  are not deduped in the float sample table
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
  `REMOTE_WRITE_SAMPLE_INTERVAL` bucket starts, default `15s`
- remote read reuses the same label-pruned sample path as PromQL selectors and
  returns float samples, native histograms, and exemplars
- `/metrics` exposes bridge, Go runtime, and process metrics in Prometheus
  text format
- the bridge self-scrapes that same registry and writes the samples into the
  configured ClickHouse metrics tables with `job` and `instance` labels

## Configuration

Environment variables:

- `CH_ADDR`: ClickHouse native address, default `localhost:9000`; accepts a
  comma-separated list for multiple replicas
- `CH_USER`: ClickHouse user, default `default`
- `CH_PASSWORD`: ClickHouse password
- `CH_DATABASE`: database, default `default`
- `CH_SCHEMA_LAYOUT`: storage layout, `current` or `posthog`, default
  `current`
- `CH_SERIES_TABLE`: series table, default `metrics_series`; unused by the
  posthog layout unless explicitly configured
- `CH_SAMPLES_TABLE`: samples table, default `metrics_samples`, or `metrics`
  for the posthog layout
- `CH_LABEL_INDEX_TABLE`: label index table, default `metrics_label_index`;
  unused by the posthog layout unless explicitly configured
- `CH_ATTRIBUTE_TABLE`: posthog attribute table, default `metric_attributes`
- `CH_HISTOGRAMS_TABLE`: native histogram table, default `metrics_histograms`
- `CH_EXEMPLARS_TABLE`: exemplar table, default `metrics_exemplars`
- `CH_METRICS_TABLE`: metric metadata table, default
  `metrics_metadata`
- `CH_LOG_SCHEMA_LAYOUT`: logs storage layout, `snuffle` or `posthog`; defaults
  to `posthog` when `CH_SCHEMA_LAYOUT=posthog`, otherwise `snuffle`
- `CH_LOGS_TABLE`: logs table, default `logs` for the Snuffle log layout and
  `logs34` for the PostHog log layout
- `CH_LOG_STREAMS_TABLE`: Snuffle log stream dictionary table, default
  `log_streams`
- `CH_LOG_ATTRIBUTES_TABLE`: PostHog log attribute table, default
  `log_attributes2` for PostHog logs and empty for Snuffle logs;
  `CH_LOG_ATTRIBUTE_TABLE` is also accepted
- `CH_LOG_STREAM_STATS_TABLE`: Snuffle log minute rollup table, default
  `log_stream_stats`
- `CH_TIMEOUT_SECONDS`: ClickHouse timeout, default `30`
- `SIDECAR_HOST`: listen host, default `0.0.0.0`
- `SIDECAR_PORT`: listen port, default `9091`
- `PROMQL_QUERY_TIMEOUT_SECONDS`: query timeout, default `30`
- `PROMQL_LOOKBACK_DELTA`: lookback delta, default `5m`
- `PROMQL_MAX_SAMPLES`: Prometheus engine sample limit, default `50000000`
- `CH_MAX_SERIES`: maximum matching series per select, default `1000000`
- `CH_ID_CHUNK_SIZE`: selected-ID batch size, default `20000`
- `CH_AGGREGATE_MAX_THREADS`: per-query `max_threads` for aggregate pushdowns,
  default `1`; set `0` to leave ClickHouse defaults unchanged
- `REMOTE_WRITE_SAMPLE_INTERVAL`: remote-write sample/histogram bucket size,
  default `15s`; set `0` to store incoming timestamps unchanged
- `SNUFFLE_SAMPLE_ATTRIBUTES`: write sample label maps into
  `attributes_map_str`; defaults to `true` for `CH_SCHEMA_LAYOUT=posthog` and
  `false` otherwise
- `SNUFFLE_DEFAULT_TEAM_ID`: fallback tenant when no tenant is provided,
  default `0`
- `SNUFFLE_TEAM_HEADER`: tenant header, default `X-Team-ID`
- `SNUFFLE_TEAM_QUERY_PARAM`: tenant query parameter, default `team_id`
- `SNUFFLE_SELF_SCRAPE_ENABLED`: write bridge `/metrics` samples downstream,
  default `true`
- `SNUFFLE_SELF_SCRAPE_INTERVAL`: self-scrape interval, default `15s`; set `0`
  to disable downstream self-scrape writes while keeping `/metrics` exposed
- `SNUFFLE_SELF_SCRAPE_TEAM_ID`: tenant used for self-scraped bridge metrics,
  default `SNUFFLE_DEFAULT_TEAM_ID`
- `SNUFFLE_SELF_SCRAPE_JOB`: `job` label for self-scraped bridge metrics,
  default `snuffle`
- `SNUFFLE_SELF_SCRAPE_INSTANCE`: `instance` label for self-scraped bridge
  metrics, default `<hostname>:<SIDECAR_PORT>`
- `SNUFFLE_LOG_RETENTION`: expiry applied when Loki push writes into `logs34`,
  default `720h`
- `SNUFFLE_LOG_QUERY_MAX_ROWS`: maximum raw log rows read per LogQL query,
  default `100000`

Tenant precedence is `/t/{team_id}/api/v1/...` or
`/team/{team_id}/api/v1/...`, then the configured header, then the configured
query parameter, then `SNUFFLE_DEFAULT_TEAM_ID`.

## Docker Demo

Start ClickHouse:

```bash
docker compose up -d clickhouse
```

Create the PostHog metrics schema and Snuffle-native logs schema:

```bash
docker exec -i snuffle-clickhouse clickhouse-client --multiquery < scripts/create_metrics_posthog_schema.sql
docker exec -i snuffle-clickhouse clickhouse-client --multiquery < scripts/create_logs_snuffle_schema.sql
```

Run the sidecar:

```bash
CH_ADDR=localhost:9000 \
CH_DATABASE=default \
CH_SCHEMA_LAYOUT=posthog \
CH_LOG_SCHEMA_LAYOUT=snuffle \
CH_SAMPLES_TABLE=metrics1 \
CH_LOGS_TABLE=logs \
CH_LOG_STREAMS_TABLE=log_streams \
CH_LOG_STREAM_STATS_TABLE=log_stream_stats \
go run ./cmd/snuffle
```

For PostHog-compatible logs instead, create
`scripts/create_logs_posthog_schema.sql` and run with
`CH_LOG_SCHEMA_LAYOUT=posthog`, `CH_LOGS_TABLE=logs34`, and
`CH_LOG_ATTRIBUTES_TABLE=log_attributes2`.

Run the Snuffle metrics and logs regression benchmark suite:

```bash
make perf-test
```

Run the PostHog-compatible suite explicitly:

```bash
make perf-test-posthog
```

Run the Codex Autoresearch-ready Snuffle metrics TSBS benchmark:

```bash
make autoresearch-snuffle-metrics
```

This targets only the Snuffle-native metrics schema, writes comparison state
under ignored `.perf/`, and prints `METRIC snuffle_metrics_score=<number>` for
Autoresearch. Lower is better; the score includes ingest, query latency,
ClickHouse CPU, and metrics-table storage.

By default this runs `PERF_RUNS=snuffle_metrics,snuffle_logs` once
(`PERF_REPEAT=1`) with CI-sized data. The metrics run generates TSBS
Prometheus remote-write data, replays it through `/api/v1/write`, and queries
the Snuffle-native metrics schema. The logs run seeds the Snuffle-native
`logs` schema and queries it through LogQL. `make perf-test-posthog` runs
`PERF_RUNS=posthog_metrics,posthog_logs` against `.perf/perf-results-posthog.json`.
Attempt artifacts are stored under
`.perf/<run>/attempt-<n>/`, selected run results are copied to
`.perf/<run>/perf-results.current.json`, and accepted suite baselines are kept
in `perf-results.json`. Each run result also records managed Snuffle RSS and
ClickHouse query-memory totals from the benchmark window. Use
`PERF_RUNS=snuffle_metrics`, `PERF_RUNS=snuffle_logs`,
`PERF_RUNS=posthog_metrics`, or `PERF_RUNS=posthog_logs` for a targeted run,
`BRIDGE_BENCH_SCENARIO=<scenario>` for a targeted query scenario, and env vars
like `TSBS_SCALE`, `POSTHOG_LOG_ROWS`, `POSTHOG_LOG_START`, or
`BRIDGE_BENCHTIME` for larger local runs. The default log window starts at
yesterday's UTC midnight so schema TTLs do not expire freshly seeded data.
Set `PERF_START_CLICKHOUSE=0`, `CH_ADDR`, and `CH_DATABASE` to use an existing
ClickHouse/database.

Run the Docker-backed integration suite:

```bash
./scripts/integration_test.sh
```

Production latency depends on hardware, ClickHouse settings, part sizes,
cardinality, query mix, and ingestion/merge load.
