package snuffle

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// maxResolvedMetricNames bounds the names a __name__ matcher resolves to: a
// regex over more literals is no longer a set (labels.Matcher.SetMatches), so
// SQL could not filter it with IN. ponytail: a matcher over more names keeps
// its regex scan.
const maxResolvedMetricNames = 256

// maxResolvedNameBytes bounds the bytes of the quoted names one query or API
// request rewrites its matchers to, over all its selectors. The name count
// does not bound bytes: metric names can be long, and the rewritten matchers
// are copied into the query text (escaped, so up to twice their size) before
// any sample limit applies. A request over the budget keeps the matchers it
// wrote.
const maxResolvedNameBytes = 64 << 10

// resolveMetricNames rewrites each selector of a PostHog-layout query whose
// __name__ matchers do not bound it to a set of names (a regex that is not a
// list of literals, !~ or !=) to the names they match in the metric name index
// over the window the query reads. The selector then filters metric_name with
// = or IN, as a plain name does, and every fast path, pushdown and planner
// sees the names.
func (s *Server) resolveMetricNames(ctx context.Context, query string, start, end time.Time, step time.Duration) (string, error) {
	if !s.metricNameIndex() {
		return query, nil
	}
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		return query, nil
	}
	var selectors []*parser.VectorSelector
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if selector, ok := node.(*parser.VectorSelector); ok && metricNamesUnbounded(selector.LabelMatchers) {
			selectors = append(selectors, selector)
		}
		return nil
	})
	if len(selectors) == 0 {
		return query, nil
	}
	// The names in the window of the whole query cover those of each
	// selector. metricsQLWithNextSample reads up to a lookback past the end.
	mint, maxt := promql.FindMinMaxTime(&parser.EvalStmt{Expr: expr, Start: start, End: end, Interval: step, LookbackDelta: s.cfg.LookbackDelta})
	maxt += s.cfg.LookbackDelta.Milliseconds()
	changed := false
	budget := maxResolvedNameBytes
	for _, selector := range selectors {
		matchers, err := s.resolveMetricNameMatchers(ctx, mint, maxt, selector.LabelMatchers, &budget)
		if err != nil {
			return "", err
		}
		if matchers != nil {
			// Leave Name empty: VectorSelector.String prints Name verbatim and
			// drops the __name__ matcher, so a stored name that is not an
			// identifier would be re-parsed as PromQL syntax. The quoted
			// __name__ matcher is escaped.
			selector.Name = ""
			selector.LabelMatchers = matchers
			changed = true
		}
	}
	if !changed {
		return query, nil
	}
	// default_rollup prints as the engine function it wraps; print its own name.
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if call, ok := node.(*parser.Call); ok && call.Func == parser.Functions[metricsQLInternalPrefix+"default_rollup"] {
			named := *call.Func
			named.Name = metricsQLInternalPrefix + "default_rollup"
			call.Func = &named
		}
		return nil
	})
	return expr.String(), nil
}

// resolveMetricNameMatcherSets resolves the __name__ matchers of each set of
// a series or labels API request over [mint, maxt].
func (s *Server) resolveMetricNameMatcherSets(ctx context.Context, mint, maxt int64, sets [][]*labels.Matcher) error {
	if !s.metricNameIndex() {
		return nil
	}
	budget := maxResolvedNameBytes
	for i, matchers := range sets {
		resolved, err := s.resolveMetricNameMatchers(ctx, mint, maxt, matchers, &budget)
		if err != nil {
			return err
		}
		if resolved != nil {
			sets[i] = resolved
		}
	}
	return nil
}

func (s *Server) metricNameIndex() bool {
	return s.cfg.postHogSchemaLayout() && s.cfg.MetricNamesTable != ""
}

// resolveMetricNameMatchers replaces unbounded __name__ matchers with the
// names they match in [mint, maxt], virtual histogram names included. It
// returns nil to keep the matchers: they are bounded already, or match no
// name or too many, or their names do not fit the request's remaining byte
// budget, which the rewrite then spends.
func (s *Server) resolveMetricNameMatchers(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher, budget *int) ([]*labels.Matcher, error) {
	if !metricNamesUnbounded(matchers) {
		return nil, nil
	}
	var nameMatchers, out []*labels.Matcher
	for _, matcher := range matchers {
		if matcher.Name != labels.MetricName {
			out = append(out, matcher)
			continue
		}
		// The name lookup skips an = or =~ matcher that matches "" (see
		// postHogMatcherCanSkip), so its names would not be bounded by it.
		if postHogMatcherCanSkip(matcher) {
			return nil, nil
		}
		nameMatchers = append(nameMatchers, matcher)
	}
	q := &CHQuerier{queryable: s.queryable, mint: mint, maxt: maxt}
	// Real names beyond the cap already make the matcher too broad to rewrite,
	// so the lookup skips the virtual histogram name scan then: the query
	// would run that scan again when it selects.
	names, err := q.postHogLabelValuesCapped(ctx, labels.MetricName, maxResolvedMetricNames+1, maxResolvedMetricNames+1, nameMatchers...)
	if err != nil || len(names) > maxResolvedMetricNames {
		// Over the cap the lookup skipped the virtual names: check the cap
		// before filtering, so a short list never drops them.
		return nil, err
	}
	names = slices.DeleteFunc(names, func(name string) bool { return !matchesAllMetricName(name, nameMatchers) })
	if len(names) == 0 {
		return nil, nil
	}
	// A single name is a one-name regex, not =: an = matcher makes a histogram
	// selector strict (an invalid source fails the query), which the broad
	// selector the user wrote is not.
	size := len(names) // the | separators
	for i, name := range names {
		// Remote write stores a name's bytes unchecked. A regex over invalid
		// UTF-8 does not compile, and a name with U+FFFD is printed literally
		// by the matcher's quoting, which the PromQL lexer reads as an invalid
		// rune, so the rewritten query would not re-parse. ContainsRune with
		// RuneError matches both. Keep the matcher the user wrote, which the
		// base layout answered.
		if strings.ContainsRune(name, utf8.RuneError) {
			return nil, nil
		}
		names[i] = regexp.QuoteMeta(name)
		size += len(names[i])
	}
	if size > *budget {
		return nil, nil
	}
	matcher, err := labels.NewMatcher(labels.MatchRegexp, labels.MetricName, strings.Join(names, "|"))
	if err != nil {
		// The names are quoted literals, so this is not the user's mistake:
		// keep their matcher rather than fail a query that ran before.
		return nil, nil
	}
	*budget -= size
	return append(out, matcher), nil
}

// metricNamesUnbounded reports whether the selector has __name__ matchers that
// do not bound it to a set of names.
func metricNamesUnbounded(matchers []*labels.Matcher) bool {
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName {
			_, bounded := metricNameSet(matchers)
			return !bounded
		}
	}
	return false
}
