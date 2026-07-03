.PHONY: perf-test autoresearch-snuffle-metrics

AUTORESEARCH_METRIC_NAME ?= snuffle_metrics_score
AUTORESEARCH_PERF_RESULTS_FILE ?= .perf/autoresearch-snuffle-metrics-baseline.json
AUTORESEARCH_PERF_REPEAT ?= 2
AUTORESEARCH_BRIDGE_BENCH_WARMUP ?= 2
AUTORESEARCH_BRIDGE_BENCHTIME ?= 3x

perf-test:
	bash scripts/perf_test.sh

autoresearch-snuffle-metrics:
	PERF_RUNS=snuffle_metrics \
	PERF_RESULTS_FILE="$(AUTORESEARCH_PERF_RESULTS_FILE)" \
	PERF_FAIL_ON_SLOWER=false \
	PERF_REPEAT="$(AUTORESEARCH_PERF_REPEAT)" \
	BRIDGE_BENCH_WARMUP="$(AUTORESEARCH_BRIDGE_BENCH_WARMUP)" \
	BRIDGE_BENCHTIME="$(AUTORESEARCH_BRIDGE_BENCHTIME)" \
	bash scripts/perf_test.sh
	go run ./cmd/snuffle-perf-report \
		--emit-autoresearch-metrics .perf/snuffle_metrics/perf-results.current.json \
		--metric-name "$(AUTORESEARCH_METRIC_NAME)"
