package snuffle

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestCanonicalBoundMatchers(t *testing.T) {
	for _, test := range []struct {
		matcher *labels.Matcher
		want    string
	}{
		// Go clients write le="1", Python, Java and Prometheus 3 write le="1.0".
		{labels.MustNewMatcher(labels.MatchEqual, "le", "1.0"), `le=~"1|1\\.0"`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", "1"), `le=~"1|1\\.0"`},
		{labels.MustNewMatcher(labels.MatchNotEqual, "le", "10.0"), `le!~"10|10\\.0"`},
		{labels.MustNewMatcher(labels.MatchEqual, "quantile", "1.0"), `quantile=~"1|1\\.0"`},
		{labels.MustNewMatcher(labels.MatchEqual, "quantile", "0"), `quantile=~"0|0\\.0"`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", "1e+06"), `le=~"1000000|1000000\\.0|1e\\+06"`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", "1e-05"), `le=~"0\\.00001|1e-05"`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", "+Inf"), `le=~"\\+Inf|Inf"`},
		// One spelling only: the matcher is unchanged.
		{labels.MustNewMatcher(labels.MatchEqual, "le", "0.005"), `le="0.005"`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", ""), `le=""`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", "NaN"), `le="NaN"`},
		{labels.MustNewMatcher(labels.MatchEqual, "le", "high"), `le="high"`},
		// Other labels and regexps keep their exact spelling.
		{labels.MustNewMatcher(labels.MatchEqual, "status", "1.0"), `status="1.0"`},
		{labels.MustNewMatcher(labels.MatchRegexp, "le", "1.0"), `le=~"1.0"`},
	} {
		got := canonicalBoundMatchers([]*labels.Matcher{test.matcher})
		if len(got) != 1 || got[0].String() != test.want {
			t.Errorf("%s = %v, want %s", test.matcher, got, test.want)
		}
	}

	equal := labels.MustNewMatcher(labels.MatchEqual, "le", "1.0")
	if !canonicalBoundMatchers([]*labels.Matcher{equal})[0].Matches("1") {
		t.Fatal(`le="1.0" must match a stored le="1"`)
	}
	if values := canonicalBoundMatchers([]*labels.Matcher{equal})[0].SetMatches(); len(values) != 2 {
		t.Fatalf("set matches = %v, want two values for an IN filter", values)
	}
	notEqual := labels.MustNewMatcher(labels.MatchNotEqual, "le", "1.0")
	if converted := canonicalBoundMatchers([]*labels.Matcher{notEqual})[0]; converted.Matches("1") || !converted.Matches("") {
		t.Fatal(`le!="1.0" must drop le="1" and keep series without le`)
	}
	input := []*labels.Matcher{equal}
	canonicalBoundMatchers(input)
	if input[0] != equal {
		t.Fatal("canonicalBoundMatchers must not modify its input")
	}
}

func TestServerParserCanonicalizesBoundMatchers(t *testing.T) {
	server := newServer(Config{})
	expr, err := server.parser.ParseExpr(`histogram_quantile(0.9, sum by (le) (rate(request_seconds_bucket{le!="1.0"}[5m]))) + on() group(rpc_seconds{quantile="1"})`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if selector, ok := node.(*parser.VectorSelector); ok {
			for _, matcher := range selector.LabelMatchers {
				if matcher.Name != labels.MetricName {
					got = append(got, matcher.String())
				}
			}
		}
		return nil
	})
	if want := []string{`le!~"1|1\\.0"`, `quantile=~"1|1\\.0"`}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("matchers = %q, want %q", got, want)
	}

	sets, err := server.parser.ParseMetricSelectors([]string{`request_seconds_bucket{le="1.0"}`})
	if err != nil {
		t.Fatal(err)
	}
	for _, matcher := range sets[0] {
		if matcher.Name == "le" && matcher.String() != `le=~"1|1\\.0"` {
			t.Fatalf("match[] matcher = %s", matcher)
		}
	}
}
