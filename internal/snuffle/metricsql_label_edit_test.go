package snuffle

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

// labelSets returns the label sets of a result, sorted, joined by ";".
func labelSets(value parser.Value) string {
	var sets []string
	switch v := value.(type) {
	case promql.Vector:
		for _, sample := range v {
			sets = append(sets, sample.Metric.String())
		}
	case promql.Matrix:
		for _, series := range v {
			sets = append(sets, series.Metric.String())
		}
	}
	slices.Sort(sets)
	return strings.Join(sets, ";")
}

// Expectations follow VictoriaMetrics app/vmselect/promql/transform.go.
func TestMetricsQLLabelEditFunctions(test *testing.T) {
	const m = `labels_value{cpu="0"}`
	for _, testcase := range []struct{ query, want string }{
		{`label_move(` + m + `, "workload", "app")`, `{__name__="labels_value", app="capture-abc", cpu="0", node="b"}`},
		// A missing source label leaves the destination alone.
		{`label_move(` + m + `, "missing", "node")`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
		{`label_move(` + m + `, "workload", "workload")`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
		{`label_move(` + m + `, "workload", "node")`, `{__name__="labels_value", cpu="0", node="capture-abc"}`},
		{`label_move(` + m + `, "__name__", "metric")`, `{cpu="0", metric="labels_value", node="b", workload="capture-abc"}`},
		{`label_move(` + m + `, "cpu", "a", "a", "b")`, `{__name__="labels_value", b="0", node="b", workload="capture-abc"}`},
		{`label_copy(` + m + `, "workload", "app")`, `{__name__="labels_value", app="capture-abc", cpu="0", node="b", workload="capture-abc"}`},
		{`label_copy(` + m + `, "node", "__name__")`, `{__name__="b", cpu="0", node="b", workload="capture-abc"}`},
		// A function that drops the metric name leaves no __name__ to copy.
		{`label_copy(rate(` + m + `[1m]), "__name__", "metric")`, `{cpu="0", node="b", workload="capture-abc"}`},
		{`label_set(` + m + `, "node", "", "env", "prod")`, `{__name__="labels_value", cpu="0", env="prod", workload="capture-abc"}`},
		{`label_set(` + m + `)`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
		{`label_set(1, "foo", "bar")`, `{foo="bar"}`},
		{`label_set(rate(` + m + `[1m]), "__name__", "renamed")`, `{__name__="renamed", cpu="0", node="b", workload="capture-abc"}`},
		// label_transform replaces every match of an unanchored regexp.
		{`label_transform(` + m + `, "workload", "[a-c]", "X")`, `{__name__="labels_value", cpu="0", node="b", workload="XXpture-XXX"}`},
		{`label_transform(` + m + `, "workload", "^(.*)-(.*)$", "$2.$1")`, `{__name__="labels_value", cpu="0", node="b", workload="abc.capture"}`},
		{`label_transform(` + m + `, "workload", "capture-abc", "")`, `{__name__="labels_value", cpu="0", node="b"}`},
		{`label_transform(` + m + `, "workload", "zzz", "y")`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
		{`label_transform(` + m + `, "missing", ".*", "x")`, `{__name__="labels_value", cpu="0", missing="x", node="b", workload="capture-abc"}`},
		{`drop_common_labels(labels_value{workload=~"capture.*"})`, `{cpu="0", node="b", workload="capture-abc"};{cpu="10", node="a", workload="capture-def"}`},
		{`drop_common_labels(labels_value{node="a"})`, `{cpu="10", workload="capture-def"};{cpu="2", workload="ingestion-xyz"}`},
		{`drop_common_labels(labels_value{cpu="0"}, labels_value{cpu="10"})`, `{cpu="0", node="b", workload="capture-abc"};{cpu="10", node="a", workload="capture-def"}`},
		{`drop_common_labels(` + m + `)`, `{}`},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			for _, rangeQuery := range []bool{false, true} {
				if got := labelSets(evaluateMetricsQL(test, labelsTestQueryable(), testcase.query, rangeQuery)); got != testcase.want {
					test.Fatalf("range=%v: got %s, want %s", rangeQuery, got, testcase.want)
				}
			}
		})
	}
}

func TestMetricsQLUnion(test *testing.T) {
	for _, testcase := range []struct{ query, want string }{
		{`(labels_value{cpu="0"}, labels_value{cpu="10"}, "x")`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"};{__name__="labels_value", cpu="10", node="a", workload="capture-def"}`},
		// The first series with a label set wins.
		{`union(labels_value{cpu="0"}, labels_value{node="b"})`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
		// Unlike or, the metric name is part of the label set.
		{`(labels_value{cpu="0"}, label_set(labels_value{cpu="0"}, "__name__", "other"))`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"};{__name__="other", cpu="0", node="b", workload="capture-abc"}`},
		{`(labels_value{cpu="0"}, 5)`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"};{}`},
		{`(labels_value{cpu="0"}, "x", "y")`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
		{`("x", "y")`, ``},
		{`sum by (node) ((labels_value{cpu="0"}, labels_value{cpu="10"}))`, `{node="a"};{node="b"}`},
		{`max_over_time((labels_value{cpu="0"}, "a", "b")[5m])`, `{cpu="0", node="b", workload="capture-abc"}`},
		// Ordinary parentheses are not a union.
		{`(labels_value{cpu="0"})`, `{__name__="labels_value", cpu="0", node="b", workload="capture-abc"}`},
	} {
		test.Run(testcase.query, func(test *testing.T) {
			for _, rangeQuery := range []bool{false, true} {
				if got := labelSets(evaluateMetricsQL(test, labelsTestQueryable(), testcase.query, rangeQuery)); got != testcase.want {
					test.Fatalf("range=%v: got %s, want %s", rangeQuery, got, testcase.want)
				}
			}
		})
	}
}

func TestPrepareMetricsQLLabelEditErrors(test *testing.T) {
	start := time.Unix(1700000010, 0)
	for _, expression := range []string{
		`label_move(m, "a")`,
		`label_copy(m, "a", "b", "c")`,
		`label_set(m, "a")`,
		`label_set(m, "a", 1)`,
		`label_transform(m, "a", "b")`,
		`label_transform(m, "a", "(", "b")`,
		`label_move(m, "a", "b") keep_metric_names`,
		`drop_common_labels()`,
		`(1, 2)`,
		`union(m, n) keep_metric_names`,
		`__snuffle_union(m, n)`,
	} {
		test.Run(expression, func(test *testing.T) {
			prepared, err := prepareMetricsQLQuery(expression, 30*time.Second, 5*time.Minute, start, start)
			if err == nil {
				_, err = parser.NewParser(parser.Options{}).ParseExpr(prepared.query)
			}
			if err == nil {
				test.Fatalf("expected an error, prepared %s", prepared.query)
			}
		})
	}
}

// The deployment variable of the Grafana "Kubernetes Deployment" dashboard
// (ENG-038, ENG-039). The third branch is a bare tuple, a union of the series
// and two strings.
const deploymentVariableQuery = "min by (deployment) (\tlabel_move(\t\tkube_daemonset_created{namespace=\"ns\"}, \"daemonset\", \"deployment\"\t)[15d])OR min by (deployment) (\tlabel_move(\t\tkube_statefulset_created{namespace=\"ns\"}, \"statefulset\", \"deployment\"\t)[15d])OR min by (deployment) (    (\t    kube_deployment_created{namespace=\"ns\"}, \"deployment\", \"deployment\"    )[15d])OR min by (deployment) (\tlabel_move(\t\trollout_info{namespace=\"ns\"}, \"name\", \"deployment\"\t)[15d])"

func TestMetricsQLDeploymentVariableQuery(test *testing.T) {
	var series []*seriesMeta
	for _, labelMap := range []map[string]string{
		{"__name__": "kube_daemonset_created", "namespace": "ns", "daemonset": "node-agent", "instance": "ksm-1"},
		{"__name__": "kube_daemonset_created", "namespace": "ns", "daemonset": "node-agent", "instance": "ksm-2"},
		{"__name__": "kube_statefulset_created", "namespace": "ns", "statefulset": "kafka"},
		{"__name__": "kube_deployment_created", "namespace": "ns", "deployment": "web"},
		{"__name__": "kube_deployment_created", "namespace": "other", "deployment": "elsewhere"},
		{"__name__": "rollout_info", "namespace": "ns", "name": "consumer", "phase": "Healthy"},
	} {
		meta := &seriesMeta{labels: labels.FromMap(labelMap), labelMap: labelMap}
		for index := range 3 {
			meta.samples = append(meta.samples, samplePoint{t: 1700002740000 + int64(index)*30000, v: 1700000000})
		}
		series = append(series, meta)
	}
	queryable := &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
		var selected []storage.Series
		for _, meta := range series {
			if matchesAll(meta.labelMap, matchers) {
				selected = append(selected, meta)
			}
		}
		return &seriesSet{series: selected, idx: -1}
	}}}
	want := `{deployment="consumer"};{deployment="kafka"};{deployment="node-agent"};{deployment="web"}`
	// An hourly step keeps the 15d subquery within the test engine's sample limit.
	end := time.Unix(1700002800, 0)
	for _, start := range []time.Time{end, end.Add(-2 * time.Hour)} {
		if got := labelSets(evaluateMetricsQLBetween(test, queryable, deploymentVariableQuery, start, end, time.Hour)); got != want {
			test.Fatalf("start=%v: got %s, want %s", start, got, want)
		}
	}
}

func TestReplaceAllLimited(test *testing.T) {
	for _, testcase := range []struct{ pattern, value, replacement string }{
		{`[a-c]`, "capture-abc", "X"},
		{``, "abc", "-"},
		{`x*`, "axxbx", "<$0>"},
		{`^(.*)-(.*)$`, "capture-abc", "$2.$1"},
		{`(b)`, "abcb", "${1}$1$$"},
	} {
		pattern := regexp.MustCompile(testcase.pattern)
		want := pattern.ReplaceAllString(testcase.value, testcase.replacement)
		got, ok := replaceAllLimited(pattern, testcase.value, testcase.replacement, 1<<10)
		if !ok || got != want {
			test.Fatalf("%s on %q: got %q, %v, want %q", testcase.pattern, testcase.value, got, ok, want)
		}
	}
	if _, ok := replaceAllLimited(regexp.MustCompile(`.`), "abcd", "xx", 7); ok {
		test.Fatal("expected the limit to stop the replacement")
	}
	// Nested replacements multiply the value length. The query fails instead
	// of building a large label.
	replacement := strings.Repeat("x", 63)
	query := `label_transform(1, "a", "", "` + replacement + `")`
	for range 3 {
		query = `label_transform(` + query + `, "a", ".", "` + replacement + `")`
	}
	start := time.Unix(1700000010, 0)
	prepared, err := prepareMetricsQLQuery(query, 30*time.Second, 5*time.Minute, start, start)
	if err != nil {
		test.Fatal(err)
	}
	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 16, Timeout: 5 * time.Second, LookbackDelta: 5 * time.Minute, EnableDelayedNameRemoval: true})
	instant, err := engine.NewInstantQuery(context.Background(), labelsTestQueryable(), nil, prepared.query, start)
	if err != nil {
		test.Fatal(err)
	}
	defer instant.Close()
	if result := instant.Exec(context.Background()); result.Err == nil || !strings.Contains(result.Err.Error(), "exceeds") {
		test.Fatalf("expected a label size error, got %v", result.Err)
	}
}
