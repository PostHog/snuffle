.PHONY: lint fmt-check test integration-test perf-test perf-test-posthog autoresearch-snuffle-metrics

lint: fmt-check
	go vet -stdmethods=false ./...

fmt-check:
	@files="$$(gofmt -l cmd internal)" || exit $$?; if [ -n "$$files" ]; then \
		printf 'Run gofmt on these files:\n%s\n' "$$files"; exit 1; \
	fi

test:
	go test -race -count=1 -timeout=5m ./...

integration-test:
	bash scripts/integration_test.sh

AUTORESEARCH_METRIC_NAME ?= snuffle_metrics_score
AUTORESEARCH_PERF_RESULTS_FILE ?= .perf/autoresearch-snuffle-metrics-baseline.json
AUTORESEARCH_PERF_REPEAT ?= 2
AUTORESEARCH_BRIDGE_BENCH_WARMUP ?= 2
AUTORESEARCH_BRIDGE_BENCHTIME ?= 3x
POSTHOG_PERF_RESULTS_FILE ?= .perf/perf-results-posthog.json

perf-test:
	bash scripts/perf_test.sh

perf-test-posthog:
	PERF_RUNS=posthog_metrics,posthog_logs \
	PERF_RESULTS_FILE="$(POSTHOG_PERF_RESULTS_FILE)" \
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
