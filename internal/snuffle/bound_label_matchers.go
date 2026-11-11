package snuffle

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// boundLabels carry a float bound: le on classic histogram buckets and
// quantile on summaries. Clients spell one bound in several ways: Go writes
// le="1", Python, Java and Prometheus 3 write le="1.0". PostHog capture folds
// a classic histogram into one row with float bounds, so a bucket scraped as
// le="1.0" reads back as le="1".
var boundLabels = map[string]bool{"le": true, "quantile": true}

// boundLabelParser makes = and != matchers on bound labels match every
// spelling of the bound, so le="1.0" also selects le="1". It wraps the parser
// of the engine and of the fast paths, so every query path sees the same
// matchers.
type boundLabelParser struct {
	parser.Parser
}

func newBoundLabelParser() parser.Parser {
	return boundLabelParser{Parser: parser.NewParser(parser.Options{})}
}

func (p boundLabelParser) ParseExpr(input string) (parser.Expr, error) {
	expr, err := p.Parser.ParseExpr(input)
	if err != nil {
		return nil, err
	}
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if selector, ok := node.(*parser.VectorSelector); ok {
			selector.LabelMatchers = canonicalBoundMatchers(selector.LabelMatchers)
		}
		return nil
	})
	return expr, nil
}

func (p boundLabelParser) ParseMetricSelector(input string) ([]*labels.Matcher, error) {
	matchers, err := p.Parser.ParseMetricSelector(input)
	if err != nil {
		return nil, err
	}
	return canonicalBoundMatchers(matchers), nil
}

func (p boundLabelParser) ParseMetricSelectors(input []string) ([][]*labels.Matcher, error) {
	sets, err := p.Parser.ParseMetricSelectors(input)
	if err != nil {
		return nil, err
	}
	for i, matchers := range sets {
		sets[i] = canonicalBoundMatchers(matchers)
	}
	return sets, nil
}

// canonicalBoundMatchers turns an = or != matcher on a bound label into a
// regexp over the spellings of its bound. The regexp is a set of literals, so
// SQL still filters with IN. Regexp matchers keep their exact spelling. The
// input is not modified.
func canonicalBoundMatchers(matchers []*labels.Matcher) []*labels.Matcher {
	var out []*labels.Matcher
	for i, matcher := range matchers {
		converted := canonicalBoundMatcher(matcher)
		if converted == matcher {
			continue
		}
		if out == nil {
			out = slices.Clone(matchers)
		}
		out[i] = converted
	}
	if out == nil {
		return matchers
	}
	return out
}

func canonicalBoundMatcher(matcher *labels.Matcher) *labels.Matcher {
	if !boundLabels[matcher.Name] {
		return matcher
	}
	var matchType labels.MatchType
	switch matcher.Type {
	case labels.MatchEqual:
		matchType = labels.MatchRegexp
	case labels.MatchNotEqual:
		matchType = labels.MatchNotRegexp
	default:
		return matcher
	}
	spellings := boundSpellings(matcher.Value)
	if len(spellings) < 2 {
		return matcher
	}
	quoted := make([]string, len(spellings))
	for i, spelling := range spellings {
		quoted[i] = regexp.QuoteMeta(spelling)
	}
	converted, err := labels.NewMatcher(matchType, matcher.Name, strings.Join(quoted, "|"))
	if err != nil {
		return matcher
	}
	return converted
}

// boundSpellings lists the common spellings of the bound in value, sorted:
// the shortest form ("1", "1e+06"), the decimal form ("1000000"), and either
// with ".0" when it has no fraction or exponent. A value that is not a
// number, or is NaN, has no other spellings.
func boundSpellings(value string) []string {
	bound, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(bound) {
		return nil
	}
	spellings := []string{value}
	switch {
	case math.IsInf(bound, 1):
		spellings = append(spellings, "+Inf", "Inf")
	case math.IsInf(bound, -1):
		spellings = append(spellings, "-Inf")
	default:
		for _, format := range []byte{'g', 'f'} {
			spelling := strconv.FormatFloat(bound, format, -1, 64)
			spellings = append(spellings, spelling)
			if !strings.ContainsAny(spelling, ".e") {
				spellings = append(spellings, spelling+".0")
			}
		}
	}
	slices.Sort(spellings)
	return slices.Compact(spellings)
}
