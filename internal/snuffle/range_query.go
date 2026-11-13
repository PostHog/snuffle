package snuffle

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

type rangeQueryError struct {
	status    int
	errorType string
	err       error
}

type rangeQueryResult struct {
	backend string
	// floats is true when groups holds the complete result. The range cache
	// stores only such results.
	floats bool
	groups []rangeAggregateGroup
	// encoded holds every other result, such as a matrix with histogram points.
	// It is a copy, because the engine reuses the memory of its result after the query closes.
	encoded      queryData
	encodedStats [3]int
	// rangeDependent is true when a step can have another value in another
	// request range. The range cache does not store such a result.
	rangeDependent bool
}

func (result rangeQueryResult) data() queryData {
	if result.floats {
		return queryData{ResultType: string(parser.ValueTypeMatrix), Result: rangeAggregateSampleResults(result.groups)}
	}
	return result.encoded
}

func (result rangeQueryResult) stats() (series, samples, histograms int) {
	if !result.floats {
		return result.encodedStats[0], result.encodedStats[1], result.encodedStats[2]
	}
	for _, group := range result.groups {
		samples += len(group.points)
	}
	return len(result.groups), samples, 0
}

// rangeQueryResultFromValue copies an engine value. The caller must call it
// before it closes the engine query.
func rangeQueryResultFromValue(backend string, value parser.Value) rangeQueryResult {
	encode := func() rangeQueryResult {
		series, samples, histograms := valueStats(value)
		return rangeQueryResult{backend: backend, encoded: responseDataFromValue(value), encodedStats: [3]int{series, samples, histograms}}
	}
	matrix, ok := value.(promql.Matrix)
	if !ok {
		return encode()
	}
	groups := make([]rangeAggregateGroup, 0, len(matrix))
	for _, series := range matrix {
		if len(series.Histograms) > 0 {
			return encode()
		}
		points := make([]samplePoint, 0, len(series.Floats))
		for _, point := range series.Floats {
			points = append(points, samplePoint{t: point.T, v: point.F})
		}
		groups = append(groups, rangeAggregateGroup{metric: series.Metric.Map(), points: points})
	}
	return rangeQueryResult{backend: backend, floats: true, groups: groups}
}

type rangeEvalNotesKey struct{}

// rangeEvalNotes collects facts that the storage layer finds during one range evaluation.
type rangeEvalNotes struct {
	rangeDependent atomic.Bool
}

// markRangeDependent records that the evaluation in ctx read data whose value
// at a step depends on the first sample the request reads.
func markRangeDependent(ctx context.Context) {
	if notes, ok := ctx.Value(rangeEvalNotesKey{}).(*rangeEvalNotes); ok {
		notes.rangeDependent.Store(true)
	}
}

func (s *Server) evalRangeQuery(ctx context.Context, prepared metricsQLQuery, start, end time.Time, step time.Duration) (rangeQueryResult, *rangeQueryError) {
	notes := &rangeEvalNotes{}
	result, err := s.evalRangeQueryBackends(context.WithValue(ctx, rangeEvalNotesKey{}, notes), prepared, start, end, step)
	result.rangeDependent = notes.rangeDependent.Load()
	return result, err
}

func (s *Server) evalRangeQueryBackends(ctx context.Context, prepared metricsQLQuery, start, end time.Time, step time.Duration) (rangeQueryResult, *rangeQueryError) {
	if !prepared.runningSum {
		if matrix, handled, err := s.tryCompactHistogramRange(ctx, prepared.query, start, end, step); handled {
			if err != nil {
				return rangeQueryResult{backend: "compact_histogram"}, &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: err}
			}
			return rangeQueryResultFromValue("compact_histogram", matrix), nil
		}
	}
	if groups, ok, err := s.tryFastRangeGroups(ctx, prepared, start, end, step); ok {
		if err != nil {
			return rangeQueryResult{backend: "fastpath_range"}, &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: err}
		}
		return rangeQueryResult{backend: "fastpath_range", floats: true, groups: groups}, nil
	}
	q, err := s.engine.NewRangeQuery(ctx, s.queryable, promql.NewPrometheusQueryOpts(false, s.cfg.LookbackDelta), prepared.query, start, end, step)
	if err != nil {
		return rangeQueryResult{backend: "prometheus"}, &rangeQueryError{status: http.StatusBadRequest, errorType: "bad_data", err: err}
	}
	defer q.Close()

	result := q.Exec(ctx)
	if result.Err != nil {
		return rangeQueryResult{backend: "prometheus"}, &rangeQueryError{status: http.StatusUnprocessableEntity, errorType: "execution", err: result.Err}
	}
	value := metricsQLValue(result.Value)
	if prepared.runningSum {
		value = metricsQLRunningSum(value)
	}
	return rangeQueryResultFromValue("prometheus", value), nil
}

// cachedRangeQuery returns an empty cache status when the cache is disabled.
// It needs the request query next to its prepared form, because rangeCacheable
// prepares the request query again for another range.
func (s *Server) cachedRangeQuery(ctx context.Context, query string, prepared metricsQLQuery, start, end time.Time, step time.Duration, noCache bool) (rangeQueryResult, string, *rangeQueryError) {
	if s.rangeCache == nil {
		result, err := s.evalRangeQuery(ctx, prepared, start, end, step)
		return result, "", err
	}
	if !s.rangeCacheable(query, prepared, start, end, step) {
		result, err := s.evalRangeQuery(ctx, prepared, start, end, step)
		return result, rangeCacheBypass, err
	}
	username, password := s.client.requestCredentials(ctx)
	stepMillis := step.Milliseconds()
	key := rangeCacheKeyParts{
		teamID:   s.cfg.TeamID,
		username: username,
		password: password,
		query:    prepared.query,
		step:     stepMillis,
		lookback: s.cfg.LookbackDelta.Milliseconds(),
		phase:    start.UnixMilli() % stepMillis,
	}.key()
	eval := func(ctx context.Context, start, end time.Time) (rangeQueryResult, *rangeQueryError) {
		return s.evalRangeQuery(ctx, prepared, start, end, step)
	}
	return s.rangeCache.query(ctx, key, start.UnixMilli(), end.UnixMilli(), stepMillis, noCache, s.verifyRangeCacheAccess, eval)
}

// verifyRangeCacheAccess asks ClickHouse whether the request may read samples.
// The query reads no rows. It fails for a user that ClickHouse does not accept
// and for a user without a grant to select from the samples table.
func (s *Server) verifyRangeCacheAccess(ctx context.Context) error {
	sql := "SELECT 1 FROM " + tableName(s.cfg.CHDatabase, s.cfg.SamplesTable) + " WHERE 0 LIMIT 1"
	return s.client.QueryRows(ctx, sql, func(clickHouseRow) error { return nil })
}

// rangeCacheable reports whether the value of each step is the same in every
// request range that holds the step. Only then can steps from one request
// answer another request.
func (s *Server) rangeCacheable(query string, prepared metricsQLQuery, start, end time.Time, step time.Duration) bool {
	stepMillis := step.Milliseconds()
	// The outer running_sum adds the points from the range start.
	if prepared.runningSum || stepMillis <= 0 || step != time.Duration(stepMillis)*time.Millisecond || start.UnixMilli() < 0 {
		return false
	}
	// The preparation of a range of one step is the preparation of an instant query.
	if end.UnixMilli()-start.UnixMilli() < 2*stepMillis {
		return false
	}
	// The preparation can write the range into the query, as it does for a
	// nested running_sum. Such a query changes when the range changes.
	shifted, err := prepareMetricsQLQuery(query, step, s.cfg.LookbackDelta, start.Add(step), end)
	if err != nil || shifted.query != prepared.query {
		return false
	}
	expr, err := s.parser.ParseExpr(prepared.query)
	if err != nil {
		return false
	}
	cacheable := true
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch selector := node.(type) {
		case *parser.VectorSelector:
			// An @ modifier reads a time that does not follow the step. A negative
			// offset reads samples after the step, which can still arrive.
			if selector.Timestamp != nil || selector.StartOrEnd != 0 || selector.OriginalOffset < 0 || selector.OriginalOffsetExpr != nil {
				cacheable = false
			}
		case *parser.MatrixSelector:
			if selector.RangeExpr != nil {
				cacheable = false
			}
		case *parser.SubqueryExpr:
			if selector.Timestamp != nil || selector.StartOrEnd != 0 || selector.OriginalOffset < 0 || selector.OriginalOffsetExpr != nil || selector.RangeExpr != nil || selector.StepExpr != nil {
				cacheable = false
			}
		}
		return nil
	})
	return cacheable
}
