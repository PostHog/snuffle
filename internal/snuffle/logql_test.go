package snuffle

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestLokiQueryGrafanaCompatibilityProbes(t *testing.T) {
	tests := []struct {
		name       string
		rangeMode  bool
		query      string
		resultType string
		want       string
	}{
		{"instant vector", false, `vector(1)`, "vector", `"1"`},
		{"instant vector arithmetic", false, `vector(1)+vector(1)`, "vector", `"2"`},
		{"instant vector aggregation", false, `sum(vector(0))`, "vector", `"0"`},
		{"instant label replace vector", false, `label_replace(vector(0), "probe", "grafana", "", ".*")`, "vector", `"probe":"grafana"`},
		{"instant vector bool comparison", false, `vector(1) == bool vector(0)`, "vector", `"0"`},
		{"range vector", true, `vector(1)`, "matrix", `"1"`},
		{"range vector arithmetic", true, `vector(1)+vector(1)`, "matrix", `"2"`},
		{"range label join vector", true, `label_join(vector(1), "probe", "/", "missing")`, "matrix", `"probe":""`},
	}
	server := &Server{cfg: Config{QueryTimeout: time.Second}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{"query": {tt.query}}
			handler := server.handleLokiQuery
			path := "/loki/api/v1/query"
			if tt.rangeMode {
				handler = server.handleLokiQueryRange
				path = "/loki/api/v1/query_range"
				params.Set("start", "1970-01-01T00:00:00Z")
				params.Set("end", "1970-01-01T00:00:02Z")
				params.Set("step", "1s")
			}
			req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
			rec := httptest.NewRecorder()

			handler(rec, req)

			resp := readLokiTestResponse(t, rec)
			if resp.Status != "success" || resp.Data.ResultType != tt.resultType {
				t.Fatalf("response = %#v, body %s", resp, rec.Body.String())
			}
			if !strings.Contains(string(resp.Data.Result), tt.want) {
				t.Fatalf("result = %s, want fragment %s", string(resp.Data.Result), tt.want)
			}
		})
	}
}

func TestParseLogQLCompatibilityCatalog(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"selector matcher operators", `{service_name="api", namespace!="", pod=~"api-[0-9]+", trace_id!~"deadbeef"}`},
		{"raw string matcher and line filter", "{app=`api`} |= `error \"quoted\"`"},
		{"chained line filters", `{app="api"} != "health" |~ "status=(4|5)[0-9]{2}" !~ "debug"`},
		{"pattern line filters", `{app="api"} |> "<_> status=<status> <_>" !> "<_> debug <_>"`},
		{"json aliases and filters", `{app="api"} | json method="request.method", status="response.status" | status >= 500`},
		{"json default flattening and bool labels", `{app="api"} | json | response_status >= 500 or method="POST"`},
		{"logfmt duration and bytes filters", `{app="api"} | logfmt | status >= 500 and duration < 1s and bytes <= 2MiB`},
		{"regexp parser", `{app="api"} | regexp "method=(?P<method>[^ ]+) status=(?P<status>[0-9]+)" | method="GET"`},
		{"pattern parser", `{app="api"} | pattern "<method> <path> <status>" | status!="200"`},
		{"line and label formatting", `{app="api"} | json | line_format "{{.method}} {{.path}}" | label_format route=path, family="{{.status}}" | keep app, route, family`},
		{"constant and copied label formats", `{app="api"} | label_format fixed="prod", copied=app, templated="{{.app}}-{{.service_name}}" | drop templated`},
		{"millisecond range", `count_over_time({app="api"}[1ms])`},
		{"day and week range offset", `count_over_time({app="api"}[1d] offset 1w)`},
		{"rate with line filter offset", `rate({app="api"} |= "error" [30s] offset 1h)`},
		{"bytes over time", `bytes_over_time({app="api"} != "health" [5m])`},
		{"bytes rate", `bytes_rate({app="api"} |~ "GET|POST" [5m])`},
		{"sum over time unwrap", `sum_over_time({app="api"} | logfmt | unwrap value [5m]) by (app)`},
		{"avg over time unwrap value alias", `avg_over_time({app="api"} | logfmt | unwrap_value latency [5m]) without (instance)`},
		{"quantile over time", `quantile_over_time(0.95, {app="api"} | logfmt | unwrap duration [5m]) by (app)`},
		{"absent over time", `absent_over_time({app="api"}[5m])`},
		{"aggregation suffix grouping", `sum(count_over_time({app="api"}[5m])) by (app)`},
		{"aggregation prefix grouping", `sum by (app) (rate({app="api"}[5m]))`},
		{"aggregation without", `avg without (instance) (count_over_time({app="api"}[5m]))`},
		{"count aggregation", `count(count_over_time({app="api"}[5m]))`},
		{"stddev aggregation", `stddev by (app) (count_over_time({app="api"}[5m]))`},
		{"stdvar aggregation", `stdvar by (app) (count_over_time({app="api"}[5m]))`},
		{"topk", `topk(3, sum by (app) (rate({app=~"api|web"}[5m])))`},
		{"bottomk", `bottomk(3, sum by (app) (rate({app=~"api|web"}[5m])))`},
		{"label replace", `label_replace(sum by (app) (count_over_time({app="api"}[5m])), "service", "$1", "app", "(.*)")`},
		{"label join", `label_join(sum by (cluster, app) (count_over_time({app="api"}[5m])), "joined", "/", "cluster", "app")`},
		{"vector precedence", `vector(1) + vector(2) * vector(3)`},
		{"vector parentheses", `(vector(1) + vector(2)) * vector(3)`},
		{"binary on matching", `sum by (app) (count_over_time({app="api"}[5m])) / on (app) sum by (app) (count_over_time({app="api"} |= "ok" [5m]))`},
		{"binary ignoring group left", `sum by (app, instance) (count_over_time({app="api"}[5m])) / ignoring (instance) group_left (region) sum by (app, region) (count_over_time({app="api"}[5m]))`},
		{"set and", `sum by (app) (count_over_time({app="api"}[5m])) and on (app) sum by (app) (count_over_time({app="api"} |= "ok" [5m]))`},
		{"set or vector fallback", `sum by (app) (count_over_time({app="api"}[5m])) or vector(0)`},
		{"set unless", `sum by (app) (count_over_time({app="api"}[5m])) unless on (app) sum by (app) (count_over_time({app="api"} |= "ok" [5m]))`},
		{"bool comparison", `vector(1) > bool vector(0)`},
		{"aggregation comparison", `sum(count_over_time({app="api"}[5m])) >= 1`},
		{"modulo", `vector(5) % vector(2)`},
		{"power", `vector(2) ^ vector(3)`},
		{"raw regex line filter", "{app=\"api\"} |~ `(?i)error|warn`"},
		{"post unwrap error filter", `rate({app="api"} | json | unwrap value | __error__!~".*" | value >5 [5m])`},
		{"loki json array path", `{app="api"} | json first_server="servers[0]"`},
		{"loki json bracket key path", `{app="api"} | json api_key="request.headers[\"X-API-KEY\"]"`},
		{"loki comma label filters", `sum_over_time({app="api"} | json | foo>=5,bar<25ms | unwrap bytes(size) [5m])`},
		{"unwrap duration conversion", `avg_over_time({app="api"} | logfmt | unwrap duration(duration) [5m]) by (app)`},
		{"unwrap duration seconds conversion", `avg_over_time({app="api"} | logfmt | unwrap duration_seconds(duration) [5m]) by (app)`},
		{"unwrap bytes conversion", `sum_over_time({app="api"} | logfmt | unwrap bytes(size) [5m]) by (app)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseLogQL(tt.query); err != nil {
				t.Fatalf("parseLogQL returned error: %v", err)
			}
		})
	}
}

func TestParseLogQLKnownUnsupportedLokiCompatibilityGaps(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"sort", `sort(sum by (app) (rate({app="api"}[5m])))`},
		{"sort desc", `sort_desc(sum by (app) (rate({app="api"}[5m])))`},
		{"approx topk", `approx_topk(3, sum by (app) (rate({app="api"}[5m])))`},
		{"rate counter", `rate_counter({app="api"} | unwrap value [5m])`},
		{"unpack parser", `sum(count_over_time({app="api"} | unpack | json [5m]))`},
		{"line filter ip matcher", `{app="api"} |= ip("127.0.0.1")`},
		{"line filter or chain", `{app="api"} |= "500" or "200"`},
		{"parenthesized label filters", `{app="api"} | logfmt | (status="500" or status="503")`},
		{"parenthesized log range selector", `bytes_over_time(({app="api"} |= "error")[5m])`},
		{"negative offset", `count_over_time({app="api"}[5m] offset -1m)`},
		{"variants expression", `variants(count_over_time({app="api"}[5m])) of ({app="api"}[5m])`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseLogQL(tt.query); err == nil {
				t.Fatalf("parseLogQL unexpectedly accepted unsupported Loki query %q", tt.query)
			}
		})
	}
}

func TestEvaluateLogQLGrafanaProbeExpressions(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantLabels map[string]string
		wantValue  float64
		wantEmpty  bool
	}{
		{"vector", `vector(1)`, map[string]string{}, 1, false},
		{"vector arithmetic", `vector(1)+vector(1)`, map[string]string{}, 2, false},
		{"vector aggregation", `sum(vector(0))`, map[string]string{}, 0, false},
		{"label replace vector", `label_replace(vector(0), "probe", "grafana", "", ".*")`, map[string]string{"probe": "grafana"}, 0, false},
		{"label join vector", `label_join(label_replace(vector(2), "service", "api", "", ".*"), "joined", "/", "service", "missing")`, map[string]string{"service": "api", "joined": "api/"}, 2, false},
		{"bool comparison true", `vector(1) > bool vector(0)`, map[string]string{}, 1, false},
		{"bool comparison false", `vector(1) < bool vector(0)`, map[string]string{}, 0, false},
		{"filter comparison true", `vector(1) > vector(0)`, map[string]string{}, 1, false},
		{"filter comparison false", `vector(0) > vector(1)`, nil, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parseLogQL(tt.query)
			if err != nil {
				t.Fatalf("parseLogQL returned error: %v", err)
			}
			samples := evaluateLogQLMetricAt(expr, nil, int64(time.Second))
			if tt.wantEmpty {
				if len(samples) != 0 {
					t.Fatalf("samples = %#v, want none", samples)
				}
				return
			}
			if len(samples) != 1 {
				t.Fatalf("samples = %d, want 1: %#v", len(samples), samples)
			}
			if labelsKey(samples[0].labels) != labelsKey(tt.wantLabels) {
				t.Fatalf("labels = %#v, want %#v", samples[0].labels, tt.wantLabels)
			}
			if math.Abs(samples[0].value-tt.wantValue) > 1e-9 {
				t.Fatalf("value = %v, want %v", samples[0].value, tt.wantValue)
			}
		})
	}
}

func TestParseLogQLReferenceQueriesFromGigapipeAndLoki(t *testing.T) {
	queries := []string{
		`{ foo = "bar" } | decolorize`,
		`{test_id="json"} | json sid="str_id" | sid >= 598`,
		`{test_id="json"} | json | str_id < 2 or str_id >= 598 and str_id > 0`,
		`{test_id="plain"} | regexp "^[^0-9]+(?P<e>[0-9]+)$"`,
		`{app="api"} |> "<_> status=<status> <_>" | status="500"`,
		`{app="api"} !> "<_> debug <_>" | keep app, status`,
		`count_over_time({test_id="json"} [1m] offset 1m)`,
		`sum by (test_id) (rate({test_id="plain"} |~ "2[0-9]$" [1s]))`,
		`sum_over_time({test_id="json"} | json | unwrap int_lbl [3s]) by (test_id, lbl_repl)`,
		`sum(sum_over_time({test_id="json"} | json | unwrap str_id [10s]) by (test_id, str_id)) by (test_id) > 1000`,
		`avg(count_over_time({ foo = "bar" }[5h])) by (bar,foo)`,
		`max without (bar) (count_over_time({ foo = "bar" }[5h]))`,
		`bottomk(30, sum by (foo) (rate({ foo = "bar" }[5h])))`,
		`label_replace(count_over_time({ foo = "bar" }[5h]), "bar", "$1", "foo", "(.*)")`,
		`label_join(sum by (service_name)(count_over_time({service_name=~"api-.+"}[1m])), "joined", "/", "service_name")`,
		`topk(10, sum by(name)(rate({region="us-east1"}[5m])))`,
		`vector(1)+vector(1)`,
		`rate({app="nginx"} | logfmt | org_id=3677 | unwrap Ingester_TotalReached[1m])`,
		`absent_over_time({app="nginx"} | json | remote_user="foo" [1m])`,
		`quantile_over_time(0.99, {app="nginx"} | logfmt | unwrap duration [5m]) by (app)`,
		`rate({region="us-east1"} | json | line_format "something else" | logfmt[5m])`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			if _, err := parseLogQL(query); err != nil {
				t.Fatalf("parseLogQL returned error: %v", err)
			}
		})
	}
}

func TestApplyLogQLPipelineReferenceStages(t *testing.T) {
	selector, err := parseLogQLSelector(`{app="api"} | json status_code="http.status", route="path" | status_code >= 500 and route="/checkout" | label_format family="{{.status_code}}" | line_format "{{.route}} {{.status_code}}" | keep app, family, route`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: 1000, line: `{"http":{"status":503},"path":"/checkout"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: 2000, line: `{"http":{"status":200},"path":"/checkout"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: 3000, line: `{"http":{"status":503},"path":"/health"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
	}

	got := applyLogQLSelector(rows, *selector)
	if len(got) != 1 {
		t.Fatalf("rows after selector = %d, want 1: %#v", len(got), got)
	}
	if got[0].line != "/checkout 503" {
		t.Fatalf("line = %q, want formatted checkout status", got[0].line)
	}
	if got[0].labels["app"] != "api" || got[0].labels["family"] != "503" || got[0].labels["route"] != "/checkout" {
		t.Fatalf("labels = %#v, want kept app/family/route labels", got[0].labels)
	}
	if got[0].labels["status_code"] != "" {
		t.Fatalf("status_code label should have been dropped: %#v", got[0].labels)
	}
}

func TestApplyLogQLJSONPathExtractionCompatibility(t *testing.T) {
	selector, err := parseLogQLSelector(`{app="api"} | json first_server="servers[0]", api_key="request.headers[\"X-API-KEY\"]", param="top.params[0].param", bracket_param="top.params[0][\"param\"]" | api_key!=""`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	rows := []logRow{{
		tsNS: 1000,
		line: `{
			"servers":["edge-a","edge-b"],
			"request":{"headers":{"X-API-KEY":"secret"}},
			"top":{"params":[{"param":"first"},{"param":"second"}]}
		}`,
		labels: map[string]string{"app": "api"},
		fields: map[string]string{},
	}}

	got := applyLogQLSelector(rows, *selector)
	if len(got) != 1 {
		t.Fatalf("rows after selector = %d, want 1: %#v", len(got), got)
	}
	labels := got[0].labels
	if labels["first_server"] != "edge-a" || labels["api_key"] != "secret" || labels["param"] != "first" || labels["bracket_param"] != "first" {
		t.Fatalf("labels = %#v, want Loki JSON path extractions", labels)
	}
}

func TestApplyLogQLCommaSeparatedLabelFiltersCompatibility(t *testing.T) {
	selector, err := parseLogQLSelector(`{app="api"} | json | foo>=5,bar<25ms,size>=1KiB`)
	if err != nil {
		t.Fatalf("parseLogQLSelector returned error: %v", err)
	}
	rows := []logRow{
		{tsNS: 1000, line: `{"foo":6,"bar":"10ms","size":"2KiB"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: 2000, line: `{"foo":4,"bar":"10ms","size":"2KiB"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: 3000, line: `{"foo":6,"bar":"30ms","size":"2KiB"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: 4000, line: `{"foo":6,"bar":"10ms","size":"512B"}`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
	}

	got := applyLogQLSelector(rows, *selector)
	if len(got) != 1 || got[0].tsNS != 1000 {
		t.Fatalf("rows after selector = %#v, want only first row", got)
	}
}

func TestEvaluateLogQLUnwrapConversionFunctions(t *testing.T) {
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: `duration=250ms size=1.5KiB`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: `duration=750ms size=512B`, labels: map[string]string{"app": "api"}, fields: map[string]string{}},
	}
	tests := []struct {
		name  string
		query string
		want  float64
	}{
		{"duration", `sum_over_time({app="api"} | logfmt | unwrap duration(duration) [1m]) by (app)`, 1},
		{"duration seconds", `sum_over_time({app="api"} | logfmt | unwrap duration_seconds(duration) [1m]) by (app)`, 1},
		{"bytes", `sum_over_time({app="api"} | logfmt | unwrap bytes(size) [1m]) by (app)`, 2048},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parseLogQL(tt.query)
			if err != nil {
				t.Fatalf("parseLogQL returned error: %v", err)
			}
			samples := evaluateLogQLMetricAt(expr, rows, int64(time.Minute))
			if len(samples) != 1 {
				t.Fatalf("samples = %d, want 1: %#v", len(samples), samples)
			}
			if math.Abs(samples[0].value-tt.want) > 1e-9 {
				t.Fatalf("value = %v, want %v", samples[0].value, tt.want)
			}
		})
	}
}

func TestApplyLogQLRegexpAndPatternReferenceStages(t *testing.T) {
	regexpSelector, err := parseLogQLSelector(`{app="api"} | regexp "^[^0-9]+(?P<code>[0-9]+)$" | code >= 100`)
	if err != nil {
		t.Fatalf("parseLogQLSelector regexp returned error: %v", err)
	}
	regexpRows := []logRow{
		{tsNS: 1000, line: "abc123", labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: 2000, line: "abc099", labels: map[string]string{"app": "api"}, fields: map[string]string{}},
	}
	regexpGot := applyLogQLSelector(regexpRows, *regexpSelector)
	if len(regexpGot) != 1 || regexpGot[0].labels["code"] != "123" {
		t.Fatalf("regexp rows = %#v, want only code 123", regexpGot)
	}

	patternSelector, err := parseLogQLSelector(`{app="api"} |> "<_> (204) <_>" | pattern "<_> msg=\"<method> <path> (<status>) <duration>\""`)
	if err != nil {
		t.Fatalf("parseLogQLSelector pattern returned error: %v", err)
	}
	patternRows := []logRow{{
		tsNS:   1000,
		line:   `level=debug ts=2021-05-19T07:54:26Z msg="POST /loki/api/v1/push (204) 1.238734ms"`,
		labels: map[string]string{"app": "api"},
		fields: map[string]string{},
	}}
	patternGot := applyLogQLSelector(patternRows, *patternSelector)
	if len(patternGot) != 1 {
		t.Fatalf("pattern rows = %d, want 1: %#v", len(patternGot), patternGot)
	}
	if patternGot[0].labels["method"] != "POST" || patternGot[0].labels["path"] != "/loki/api/v1/push" || patternGot[0].labels["status"] != "204" {
		t.Fatalf("pattern labels = %#v", patternGot[0].labels)
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

func TestEvaluateLogQLBinaryMatchingAndVectorFallbacks(t *testing.T) {
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: "ok", labels: map[string]string{"env": "prod", "app": "api", "instance": "a", "region": "us"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: "fail", labels: map[string]string{"env": "prod", "app": "api", "instance": "b", "region": "us"}, fields: map[string]string{}},
	}

	groupLeftExpr, err := parseLogQL(`sum by (app, instance) (count_over_time({env="prod"}[1m])) / on (app) group_left (region) sum by (app, region) (count_over_time({env="prod"} |= "ok" [1m]))`)
	if err != nil {
		t.Fatalf("parse group_left returned error: %v", err)
	}
	groupLeft := evaluateLogQLInstantMetric(groupLeftExpr, rows, int64(time.Minute))
	if len(groupLeft) != 2 {
		t.Fatalf("group_left samples = %d, want 2: %#v", len(groupLeft), groupLeft)
	}
	for _, sample := range groupLeft {
		if sample.Metric["app"] != "api" || sample.Metric["region"] != "us" || sample.Value[1].(string) != "1" {
			t.Fatalf("group_left sample = %#v", sample)
		}
	}

	fallbackExpr, err := parseLogQL(`sum by (app) (count_over_time({app="missing"}[1m])) or vector(0)`)
	if err != nil {
		t.Fatalf("parse vector fallback returned error: %v", err)
	}
	fallback := evaluateLogQLInstantMetric(fallbackExpr, rows, int64(time.Minute))
	if len(fallback) != 1 || len(fallback[0].Metric) != 0 || fallback[0].Value[1].(string) != "0" {
		t.Fatalf("fallback = %#v, want unlabeled vector(0)", fallback)
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

func TestEvaluateLogQLRangeAggregationFunctions(t *testing.T) {
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: "value=2", labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: "value=4", labels: map[string]string{"app": "api"}, fields: map[string]string{}},
		{tsNS: int64(30 * time.Second), line: "value=8", labels: map[string]string{"app": "api"}, fields: map[string]string{}},
	}
	tests := []struct {
		name  string
		query string
		want  float64
	}{
		{"sum", `sum_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 14},
		{"avg", `avg_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 14.0 / 3.0},
		{"min", `min_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 2},
		{"max", `max_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 8},
		{"first", `first_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 2},
		{"last", `last_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 8},
		{"stdvar", `stdvar_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, 56.0 / 9.0},
		{"stddev", `stddev_over_time({app="api"} | logfmt | unwrap value [1m]) by (app)`, math.Sqrt(56.0 / 9.0)},
		{"quantile", `quantile_over_time(0.5, {app="api"} | logfmt | unwrap value [1m]) by (app)`, 4},
		{"count", `count_over_time({app="api"}[1m])`, 3},
		{"rate", `rate({app="api"}[1m])`, 3.0 / 60.0},
		{"bytes", `bytes_over_time({app="api"}[1m])`, float64(len("value=2") + len("value=4") + len("value=8"))},
		{"bytes_rate", `bytes_rate({app="api"}[1m])`, float64(len("value=2")+len("value=4")+len("value=8")) / 60.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parseLogQL(tt.query)
			if err != nil {
				t.Fatalf("parseLogQL returned error: %v", err)
			}
			samples := evaluateLogQLMetricAt(expr, rows, int64(time.Minute))
			if len(samples) != 1 {
				t.Fatalf("samples = %d, want 1: %#v", len(samples), samples)
			}
			if math.Abs(samples[0].value-tt.want) > 1e-9 {
				t.Fatalf("value = %v, want %v", samples[0].value, tt.want)
			}
		})
	}
}

func TestEvaluateLogQLAbsentOverTime(t *testing.T) {
	expr, err := parseLogQL(`absent_over_time({app="missing"}[1m])`)
	if err != nil {
		t.Fatalf("parseLogQL returned error: %v", err)
	}
	samples := evaluateLogQLMetricAt(expr, []logRow{
		{tsNS: int64(10 * time.Second), line: "a", labels: map[string]string{"app": "api"}, fields: map[string]string{}},
	}, int64(time.Minute))
	if len(samples) != 1 || len(samples[0].labels) != 0 || samples[0].value != 1 {
		t.Fatalf("absent samples = %#v, want unlabeled value 1", samples)
	}
}

func TestEvaluateLogQLGroupingTopBottomAndSetOperators(t *testing.T) {
	rows := []logRow{
		{tsNS: int64(10 * time.Second), line: "ok", labels: map[string]string{"app": "api", "service_name": "checkout", "instance": "a"}, fields: map[string]string{}},
		{tsNS: int64(20 * time.Second), line: "ok", labels: map[string]string{"app": "api", "service_name": "checkout", "instance": "b"}, fields: map[string]string{}},
		{tsNS: int64(30 * time.Second), line: "fail", labels: map[string]string{"app": "api", "service_name": "billing", "instance": "c"}, fields: map[string]string{}},
	}

	topExpr, err := parseLogQL(`topk(1, sum by (service_name) (count_over_time({app="api"}[1m])))`)
	if err != nil {
		t.Fatalf("parse topk returned error: %v", err)
	}
	top := evaluateLogQLInstantMetric(topExpr, rows, int64(time.Minute))
	if len(top) != 1 || top[0].Metric["service_name"] != "checkout" || top[0].Value[1].(string) != "2" {
		t.Fatalf("topk = %#v, want checkout value 2", top)
	}

	bottomExpr, err := parseLogQL(`bottomk(1, sum by (service_name) (count_over_time({app="api"}[1m])))`)
	if err != nil {
		t.Fatalf("parse bottomk returned error: %v", err)
	}
	bottom := evaluateLogQLInstantMetric(bottomExpr, rows, int64(time.Minute))
	if len(bottom) != 1 || bottom[0].Metric["service_name"] != "billing" || bottom[0].Value[1].(string) != "1" {
		t.Fatalf("bottomk = %#v, want billing value 1", bottom)
	}

	withoutExpr, err := parseLogQL(`sum without (instance) (count_over_time({app="api"}[1m]))`)
	if err != nil {
		t.Fatalf("parse without returned error: %v", err)
	}
	without := evaluateLogQLInstantMetric(withoutExpr, rows, int64(time.Minute))
	withoutValues := logMetricValuesByLabel(without, "service_name")
	if withoutValues["checkout"] != "2" || withoutValues["billing"] != "1" {
		t.Fatalf("without values = %#v", withoutValues)
	}
	for _, sample := range without {
		if _, ok := sample.Metric["instance"]; ok {
			t.Fatalf("instance label leaked through without grouping: %#v", sample.Metric)
		}
	}

	unlessExpr, err := parseLogQL(`sum by (service_name) (count_over_time({app="api"}[1m])) unless on (service_name) sum by (service_name) (count_over_time({app="api"} |= "ok" [1m]))`)
	if err != nil {
		t.Fatalf("parse unless returned error: %v", err)
	}
	unless := evaluateLogQLInstantMetric(unlessExpr, rows, int64(time.Minute))
	if len(unless) != 1 || unless[0].Metric["service_name"] != "billing" || unless[0].Value[1].(string) != "1" {
		t.Fatalf("unless = %#v, want only billing", unless)
	}
}

func logMetricValuesByLabel(samples []logMetricVectorResult, label string) map[string]string {
	out := map[string]string{}
	for _, sample := range samples {
		out[sample.Metric[label]] = sample.Value[1].(string)
	}
	return out
}

type lokiTestResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

func readLokiTestResponse(t *testing.T, rec *httptest.ResponseRecorder) lokiTestResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp lokiTestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid Loki JSON response %q: %v", rec.Body.String(), err)
	}
	if resp.ErrorType != "" || resp.Error != "" {
		t.Fatalf("unexpected Loki error response: %#v", resp)
	}
	return resp
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
