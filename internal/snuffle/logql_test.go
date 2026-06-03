package snuffle

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseLogQLLogSelectorWithPipeline(t *testing.T) {
	expr, err := parseLogQL(`{service_name="api", namespace=~"prod|staging"} |= "error" | json | status >= 500 | line_format "{{.method}} {{.path}}"`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	if expr.logSelector == nil {
		t.Fatal("expected log selector expression")
	}
	if len(expr.logSelector.matchers) != 2 {
		t.Fatalf("matchers = %d, want 2", len(expr.logSelector.matchers))
	}
	if len(expr.logSelector.stages) != 4 {
		t.Fatalf("stages = %d, want 4", len(expr.logSelector.stages))
	}
}

func TestParseLogQLMetricAggregation(t *testing.T) {
	expr, err := parseLogQL(`sum by (service_name) (rate({service_name=~"api|worker"} |= "level" [5m])) > 1`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	if expr.aggregation == nil || expr.aggregation.fn != "sum" {
		t.Fatalf("aggregation = %#v", expr.aggregation)
	}
	if expr.aggregation.grouping == nil || expr.aggregation.grouping.labels[0] != "service_name" {
		t.Fatalf("grouping = %#v", expr.aggregation.grouping)
	}
	if expr.comparison == nil || expr.comparison.op != ">" || expr.comparison.value != 1 {
		t.Fatalf("comparison = %#v", expr.comparison)
	}
	if expr.aggregation.expr.rangeAgg == nil || expr.aggregation.expr.rangeAgg.window != 5*time.Minute {
		t.Fatalf("range aggregation = %#v", expr.aggregation.expr.rangeAgg)
	}
}

func TestEvaluateLogQLVectorFunction(t *testing.T) {
	expr, err := parseLogQL(`vector(1)`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	if expr.vector == nil || *expr.vector != 1 {
		t.Fatalf("vector expression = %#v, want 1", expr.vector)
	}

	vector := evaluateLogQLInstantMetric(expr, nil, int64(time.Second))
	if len(vector) != 1 {
		t.Fatalf("vector length = %d, want 1: %#v", len(vector), vector)
	}
	if len(vector[0].Metric) != 0 || vector[0].Value[1].(string) != "1" {
		t.Fatalf("vector result = %#v, want unlabeled value 1", vector[0])
	}

	matrix := evaluateLogQLRangeMetric(expr, nil, 0, int64(2*time.Second), time.Second)
	if len(matrix) != 1 {
		t.Fatalf("matrix length = %d, want 1: %#v", len(matrix), matrix)
	}
	if len(matrix[0].Metric) != 0 || len(matrix[0].Values) != 3 {
		t.Fatalf("matrix result = %#v, want unlabeled 3-step series", matrix[0])
	}
	for _, value := range matrix[0].Values {
		if value[1].(string) != "1" {
			t.Fatalf("matrix value = %#v, want 1", value)
		}
	}
}

func TestLokiQueryVectorFunction(t *testing.T) {
	server := &Server{cfg: Config{QueryTimeout: time.Second}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query=vector(1)", nil)
	rec := httptest.NewRecorder()

	server.handleLokiQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"success"`) || !strings.Contains(body, `"resultType":"vector"`) {
		t.Fatalf("unexpected body: %s", body)
	}
	if strings.Contains(body, "unsupported LogQL expression") {
		t.Fatalf("body still reports unsupported vector expression: %s", body)
	}
}

func TestLogQLSelectSQLUsesLogsSchema(t *testing.T) {
	cfg := Config{
		CHDatabase:      "posthog",
		LogsTable:       "logs34",
		LogQueryMaxRows: 1000,
	}
	selector, err := parseLogQLSelector(`{service_name="checkout", level=~"error|warn", trace_id!="abc"} |= "failed"`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	sql := logQLSelectSQL(cfg, *selector, 1000, 2000, 50, "backward")
	for _, want := range []string{
		"`posthog`.`logs34`",
		"team_id = 0",
		"timestamp >= fromUnixTimestamp64Nano(1000, 'UTC')",
		"time_bucket >= toStartOfDay(fromUnixTimestamp64Nano(1000, 'UTC'))",
		"JSONExtractString(attributes_map_str['_loki_stream_labels__str'], 'service_name')",
		"= 'checkout'",
		"match(if(mapContains(attributes_map_str, '_loki_stream_labels__str'), JSONExtractString(attributes_map_str['_loki_stream_labels__str'], 'level'), severity_text), '^(?:error|warn)$')",
		"JSONExtractString(attributes_map_str['_loki_stream_labels__str'], 'trace_id')",
		"!= 'abc'",
		"position(body, 'failed') > 0",
		"ORDER BY ts_ns DESC, stream_key ASC, observed_ns DESC LIMIT 50",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL %q does not contain %q", sql, want)
		}
	}
}

func TestApplyLogQLPipelineParsersAndFilters(t *testing.T) {
	selector, err := parseLogQLSelector(`{service_name="api"} | json | status >= 500 | line_format "{{.method}} {{.path}}" | label_format route=path`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	rows := []logRow{{
		tsNS:   1000,
		line:   `{"method":"GET","path":"/checkout","status":503}`,
		labels: map[string]string{"service_name": "api"},
		fields: map[string]string{},
	}}
	got := applyLogQLSelector(rows, *selector)
	if len(got) != 1 {
		t.Fatalf("rows after selector = %d, want 1", len(got))
	}
	if got[0].line != "GET /checkout" {
		t.Fatalf("formatted line = %q", got[0].line)
	}
	if got[0].labels["route"] != "/checkout" {
		t.Fatalf("route label = %q", got[0].labels["route"])
	}
}

func TestApplyLogQLPipelineErrorsKeepDecolorizeAndPatternNot(t *testing.T) {
	selector, err := parseLogQLSelector(`{service_name="api"} !> "<_> debug <_>" | decolorize | json | __error__="" | keep service_name, method`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: 1000, line: "\x1b[31m{\"method\":\"GET\",\"status\":200}\x1b[0m", labels: map[string]string{"service_name": "api", "pod": "a"}, fields: map[string]string{}},
		{tsNS: 2000, line: "x debug y", labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: 3000, line: "{bad json", labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
	}
	got := applyLogQLSelector(rows, *selector)
	if len(got) != 1 {
		t.Fatalf("rows after selector = %d, want 1: %#v", len(got), got)
	}
	if got[0].line != `{"method":"GET","status":200}` {
		t.Fatalf("line = %q", got[0].line)
	}
	if got[0].labels["method"] != "GET" || got[0].labels["pod"] != "" {
		t.Fatalf("labels = %#v", got[0].labels)
	}
}

func TestLogQLLabelFiltersSupportDurationAndBytes(t *testing.T) {
	selector, err := parseLogQLSelector(`{service_name="api"} | logfmt | duration >= 500ms and bytes < 2KiB`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: 1000, line: `duration=750ms bytes=1024`, labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: 2000, line: `duration=100ms bytes=1024`, labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: 3000, line: `duration=750ms bytes=4096`, labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
	}
	got := applyLogQLSelector(rows, *selector)
	if len(got) != 1 || got[0].tsNS != 1000 {
		t.Fatalf("rows after selector = %#v, want only first row", got)
	}
}

func TestParseFlatJSONFieldsFlattensNestedObjects(t *testing.T) {
	fields := parseFlatJSONFields(`{"http":{"method":"GET","status":200},"path":"/x"}`, "")
	if fields["http_method"] != "GET" || fields["http_status"] != "200" || fields["path"] != "/x" {
		t.Fatalf("fields = %#v", fields)
	}
}

func TestEvaluateLogQLRangeMetric(t *testing.T) {
	expr, err := parseLogQL(`sum by (service_name) (count_over_time({service_name=~"api|worker"}[1m]))`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: "a", labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: "b", labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: "c", labels: map[string]string{"service_name": "worker"}, fields: map[string]string{}},
	}
	vector := evaluateLogQLInstantMetric(expr, rows, int64(time.Minute))
	if len(vector) != 2 {
		t.Fatalf("vector length = %d, want 2: %#v", len(vector), vector)
	}
	values := map[string]string{}
	for _, sample := range vector {
		values[sample.Metric["service_name"]] = sample.Value[1].(string)
	}
	if values["api"] != "2" || values["worker"] != "1" {
		t.Fatalf("values = %#v", values)
	}
}

func TestEvaluateLogQLLabelReplaceAndJoin(t *testing.T) {
	expr, err := parseLogQL(`label_join(label_replace(sum by (service_name) (count_over_time({service_name=~"api-.+"}[1m])), "cluster", "$1", "service_name", "api-(.+)"), "joined", "/", "cluster", "service_name")`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: "a", labels: map[string]string{"service_name": "api-east"}, fields: map[string]string{}},
	}
	vector := evaluateLogQLInstantMetric(expr, rows, int64(time.Minute))
	if len(vector) != 1 {
		t.Fatalf("vector length = %d, want 1: %#v", len(vector), vector)
	}
	if vector[0].Metric["cluster"] != "east" || vector[0].Metric["joined"] != "east/api-east" {
		t.Fatalf("metric labels = %#v", vector[0].Metric)
	}
}

func TestEvaluateLogQLBinaryArithmeticAndMatching(t *testing.T) {
	expr, err := parseLogQL(`sum by (service_name) (count_over_time({service_name=~"api|worker"}[1m])) / on (service_name) sum by (service_name) (count_over_time({service_name=~"api|worker"} |= "ok" [1m]))`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: "ok", labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: "bad", labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: int64(30 * time.Second), line: "ok", labels: map[string]string{"service_name": "worker"}, fields: map[string]string{}},
	}
	vector := evaluateLogQLInstantMetric(expr, rows, int64(time.Minute))
	values := map[string]string{}
	for _, sample := range vector {
		values[sample.Metric["service_name"]] = sample.Value[1].(string)
	}
	if values["api"] != "2" || values["worker"] != "1" {
		t.Fatalf("values = %#v", values)
	}
}

func TestEvaluateLogQLQuantileOverTime(t *testing.T) {
	expr, err := parseLogQL(`quantile_over_time(0.5, {service_name="api"} | json | unwrap duration [1m]) by (service_name)`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: `{"duration":1}`, labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: `{"duration":5}`, labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
		{tsNS: int64(30 * time.Second), line: `{"duration":9}`, labels: map[string]string{"service_name": "api"}, fields: map[string]string{}},
	}
	vector := evaluateLogQLInstantMetric(expr, rows, int64(time.Minute))
	if len(vector) != 1 {
		t.Fatalf("vector length = %d, want 1: %#v", len(vector), vector)
	}
	if vector[0].Value[1].(string) != "5" {
		t.Fatalf("quantile value = %v, want 5", vector[0].Value[1])
	}
}

func TestLokiPushJSONRows(t *testing.T) {
	server := &Server{cfg: Config{TeamID: 42, LogRetention: time.Hour}}
	req := lokiPushRequest{Streams: []lokiPushStream{{
		Stream: map[string]string{"service_name": "api", "level": "error"},
		Values: [][]any{{"1700000000000000000", "failed request", map[string]any{"trace_id": "abc", "resource_host.name": "host-1"}}},
	}}}
	rows, err := server.rowsFromLokiPushRequest(req)
	if err != nil {
		t.Fatalf("rowsFromLokiPushRequest returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.teamID != 42 || row.serviceName != "api" || row.severityText != "error" || row.traceID != "abc" {
		t.Fatalf("unexpected row: %#v", row)
	}
	if row.resourceAttributes["host.name"] != "host-1" {
		t.Fatalf("resource attrs = %#v", row.resourceAttributes)
	}
	if row.attributes[lokiStreamLabelsAttributeKey] == "" {
		t.Fatalf("missing stream label marker: %#v", row.attributes)
	}
	labels, streamLabels, fields := labelsAndFieldsFromLogColumns(row.serviceName, row.severityText, row.traceID, row.spanID, row.resourceAttributes, row.attributes)
	if labels["level"] != "error" || labels["service_name"] != "api" {
		t.Fatalf("stream labels = %#v", labels)
	}
	if streamLabels["level"] != "error" || streamLabels["service_name"] != "api" {
		t.Fatalf("stream labels = %#v", streamLabels)
	}
	if _, ok := streamLabels["host.name"]; ok {
		t.Fatalf("structured metadata leaked into selector labels: %#v", streamLabels)
	}
	if fields["host.name"] != "host-1" || fields["trace_id"] != "abc" {
		t.Fatalf("fields = %#v", fields)
	}
}
