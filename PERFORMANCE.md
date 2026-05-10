# Query Shape Notes

Local benchmarks are useful for regression checks, but they are not product
capacity claims. The important parts of this bridge are the schema shape, the
ClickHouse query shape, and the number of Go-to-ClickHouse interactions. The
sidecar does not call ClickHouse `prometheusQuery` or `prometheusQueryRange`.

## Dataset

The final large benchmark uses `scripts/seed_metrics_large.sql`:

- 100,000 series
- 10,000,000 samples, 100 samples per series at 15s spacing
- 400,000 inverted label-index rows
- metric: `load_requests_total`
- labels: `job`, `instance`, `status`, `shard`

## Recommended Metrics Layout

For unknown incoming labels, the default should not assume hot label names.

`metrics_samples`

- columns: `timestamp DateTime64(3)`, `id UInt64`, `value Float64`
- primary order: `(id, timestamp)`
- all columns are non-null

`metrics_series`

- one row per time series
- columns: `id`, `metric_name`, `labels_json String`, `min_time`, `max_time`
- primary order: `(metric_name, id)`
- projection: `(metric_name, id)` for metric/time pruning
- projection: `(id)` for fetching metadata after ClickHouse has already found
  a small set of series, such as `topk`
- all columns are non-null

`metrics_label_index`

- columns: `metric_name`, `label_name`, `label_value`, `id`
- primary order: `(metric_name, label_name, label_value, id)`
- projection: `(label_name, label_value, id, metric_name)` for arbitrary label lookup
- projection: `(id, label_name, metric_name, label_value)` for fetching group labels
- used for arbitrary label filtering and label metadata
- all columns are non-null

`labels_json String` is useful for compact label storage and returning complete
labelsets. Arbitrary label filtering still goes through the inverted index.

Cold-path companion tables:

- `metrics_histograms`: native histogram samples as remote-write protobuf
  blobs keyed by `(id, timestamp)`
- `metrics_exemplars`: exemplar samples keyed by `(id, timestamp)`
- `metrics_metadata`: latest metadata per metric family

## JSON vs Map Findings

The product layout keeps labels in an opaque JSON string for compact storage
and response materialization, but does not use ClickHouse JSON functions for
filtering or grouping. All arbitrary label predicates and group labels go
through `metrics_label_index`.

`Map` was larger than `JSON` on the local 10M-row dataset and did not add a
query-planning advantage once the inverted index became the hot path. The
bridge therefore avoids `Map` for labels.

## Benchmark Harness

Use Go benchmarks as a repeatable regression harness for query-shape changes:

```bash
BRIDGE_BENCH_URL=http://localhost:9091 \
BRIDGE_BENCH_PROFILE=large \
BRIDGE_BENCH_CONCURRENCY=10 \
BRIDGE_BENCH_WARMUP=10 \
go test -run '^$' -bench '^BenchmarkBridgeHTTP$' ./internal/perftest -benchtime=100x -timeout=10m
```

For the local Prometheus TSDB comparison:

```bash
PROM_TSDB_BENCH=1 \
PROM_TSDB_BENCH_CONCURRENCY=10 \
PROM_TSDB_BENCH_WARMUP=10 \
go test -run '^$' -bench '^BenchmarkPrometheusTSDB$' ./internal/perftest -benchtime=100x -timeout=30m
```

The Go sidecar uses Prometheus's native PromQL engine and the metrics schema:

- `metrics_series`
- `metrics_samples`
- `metrics_label_index`
- `metrics_histograms`
- `metrics_exemplars`
- `metrics_metadata`

Compare the generated ClickHouse SQL and `system.query_log` rows when changing
schema or query planning. Focus on read rows, read bytes, marks used, projection
selection, number of HTTP requests, and response materialization cost.

Implemented storage optimizations:

- exact metric/time pruning on `series`
- arbitrary positive label matcher pruning through `label_index`
- label-index projections for matcher lookup and group-label lookup
- exact Prometheus matcher semantics are preserved by a final Go-side matcher
  filter
- instant vector selectors fetch only the latest sample in the lookback window
- sample reads use exact selected IDs against a sample table ordered by
  `(id,timestamp)` with tighter index granularity
- broad latest-sample reads use ClickHouse subqueries instead of oversized
  `IN (...)` lists
- plain `/query_range` selectors use `timeSeriesLastToGrid` to return one row
  per series with an array of step values
- safe `/query_range` aggregates use ClickHouse grid functions and aggregate
  arrays server-side, avoiding raw-sample materialization in Go
- no-op Grafana regexes such as `=~".*"` do not disable ClickHouse pushdown
- `/series` resolves labelsets without reading samples
- label metadata endpoints push `limit` into ClickHouse when clients provide it
- metric metadata is stored separately and never joined into the sample hot path
- native histograms and exemplars are stored separately from float samples and
  loaded only when the request path needs them
- safe instant `sum`, `avg`, `count`, `min`, `max`, `group`, `topk`, and
  `bottomk` push down to ClickHouse
- safe aggregate-over-range-function shapes such as
  `sum by (...) (rate(selector[5m]))` and
  `sum by (...) (irate(selector[5m]))` push down to ClickHouse for float samples
- post-aggregation joins use `ANY JOIN` only after both sides are one row per
  series; raw sample joins stay normal joins so ClickHouse does not collapse
  samples before aggregation
- aggregate pushdowns can use `CH_AGGREGATE_MAX_THREADS` to avoid per-query
  thread over-subscription under concurrent dashboard traffic
- the Go ClickHouse client reuses a precomputed endpoint and avoids copying
  scanner rows before JSON decoding
- series labels are stored as an opaque JSON string and parsed in Go only when
  labels must be returned; filtering and grouping use `label_index`

## Comparison Summary

Default generated TimeSeries targets are functional but weak for PromQL-style
filters because the tags target is ordered mainly by `(metric_name, id)`, and
the data target is ordered by `(id, timestamp)`.

The generic inverted-index layout is the right default for arbitrary labels
because it avoids making product assumptions about incoming label names.

## Gigapipe Comparison

The local `/root/git/gigapipe` schema uses the same core shape:

- `time_series`: `date`, `fingerprint UInt64`, `labels String`, `name String`,
  later `type UInt8`; `ReplacingMergeTree`, partitioned by `date`, ordered by
  `fingerprint` and later `(fingerprint, type)`.
- `time_series_gin`: inverted label index with `date`, `key`, `val`,
  `fingerprint`, later `type`; ordered by `(key, val, fingerprint, type)`.
  It is populated by a materialized view that expands the serialized label
  object into key/value rows.
- `samples_v3`: raw samples with `fingerprint UInt64`, `timestamp_ns Int64`,
  `value Float64`, `string String`, later `type`; partitioned by day and
  ordered by configurable `SAMPLES_ORDER_RUL`, defaulting to `timestamp_ns`.
- `metrics_15s`: downsampled aggregates ordered by `(fingerprint,
  timestamp_ns, type)`, populated from `samples_v3`.

Their PromQL path uses the official Prometheus parser/query interfaces in Go,
resolves matchers through the inverted label index, fetches labels separately
from `time_series`, and switches to the `metrics_15s` downsample table when the
query step/range allows it.

For our workload, the main schema difference versus Gigapipe is keeping the
stored label object as an opaque JSON string while maintaining the separate
inverted label index for query planning.
