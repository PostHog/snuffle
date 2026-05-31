package snuffle

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	lokiStreamLabelsAttributeKey  = "_loki_stream_labels__str"
	lokiEntryMetadataAttributeKey = "_loki_entry_metadata__str"
)

type lokiResponse struct {
	Status    string `json:"status"`
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

type lokiQueryData struct {
	ResultType string `json:"resultType"`
	Result     any    `json:"result"`
	Stats      any    `json:"stats,omitempty"`
}

type lokiPushRequest struct {
	Streams []lokiPushStream `json:"streams"`
}

type lokiPushStream struct {
	Stream  map[string]string `json:"stream"`
	Labels  string            `json:"labels"`
	Values  [][]any           `json:"values"`
	Entries []lokiPushEntry   `json:"entries"`
}

type lokiPushEntry struct {
	Timestamp string  `json:"ts"`
	Time      string  `json:"timestamp"`
	Line      string  `json:"line"`
	Value     float64 `json:"value"`
}

type lokiLogInsertRow struct {
	teamID                  int32
	originalExpiryTimestamp time.Time
	uuid                    string
	traceID                 string
	spanID                  string
	traceFlags              int32
	timestamp               time.Time
	observedTimestamp       time.Time
	body                    string
	severityText            string
	severityNumber          int32
	serviceName             string
	resourceAttributes      map[string]string
	instrumentationScope    string
	eventName               string
	attributes              map[string]string
}

func (s *Server) lokiRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/loki/api/v1/push", s.teamHandler((*Server).handleLokiPush))
	mux.HandleFunc("/loki/api/v1/query", s.teamHandler((*Server).handleLokiQuery))
	mux.HandleFunc("/loki/api/v1/query_range", s.teamHandler((*Server).handleLokiQueryRange))
	mux.HandleFunc("/loki/api/v1/labels", s.teamHandler((*Server).handleLokiLabels))
	mux.HandleFunc("/loki/api/v1/label/", s.teamHandler((*Server).handleLokiLabelValues))
	mux.HandleFunc("/loki/api/v1/series", s.teamHandler((*Server).handleLokiSeries))
	mux.HandleFunc("/loki/api/v1/index/stats", s.teamHandler((*Server).handleLokiIndexStats))
	mux.HandleFunc("/loki/api/v1/status/buildinfo", s.teamHandler((*Server).handleLokiBuildInfo))
	return mux
}

func (s *Server) handleLokiPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeLokiError(w, http.StatusMethodNotAllowed, "bad_method", errors.New("method not allowed"))
		return
	}
	if s.cfg.LogsTable == "" {
		writeLokiError(w, http.StatusServiceUnavailable, "unavailable", errors.New("CH_LOGS_TABLE is empty"))
		return
	}
	rows, err := s.decodeLokiPushRows(r)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.CHTimeout)
	defer cancel()
	if err := s.insertLokiLogRows(ctx, rows); err != nil {
		writeLokiError(w, http.StatusServiceUnavailable, "execution", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleLokiQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	query := r.Form.Get("query")
	if query == "" {
		writeLokiError(w, http.StatusBadRequest, "bad_data", errors.New("missing query parameter"))
		return
	}
	ts, err := parseLokiQueryTime(r.Form.Get("time"), time.Now())
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	limit, err := parseLimitDefault(r.Form.Get("limit"), 100)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	direction := lokiDirection(r.Form.Get("direction"))
	expr, err := parseLogQL(query)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	if expr.logSelector != nil {
		startNS := ts.Add(-6 * time.Hour).UnixNano()
		endNS := ts.UnixNano()
		rows, err := s.queryLogQLRows(ctx, *expr.logSelector, startNS, endNS, limit, direction)
		if err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		writeLokiSuccess(w, lokiQueryData{ResultType: "streams", Result: logStreamResults(rows, limit, direction), Stats: lokiEmptyStats()})
		return
	}
	startNS, endNS := logQLMetricFetchBounds(expr, ts.UnixNano(), ts.UnixNano())
	if data, ok, err := s.tryLogQLInstantMetricSQL(ctx, expr, ts); ok {
		if err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		writeLokiSuccess(w, data)
		return
	}
	rows, err := s.queryLogQLMetricRows(ctx, expr, startNS, endNS)
	if err != nil {
		writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	writeLokiSuccess(w, lokiQueryData{ResultType: "vector", Result: evaluateLogQLInstantMetric(expr, rows, ts.UnixNano()), Stats: lokiEmptyStats()})
}

func (s *Server) handleLokiQueryRange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	query := r.Form.Get("query")
	if query == "" {
		writeLokiError(w, http.StatusBadRequest, "bad_data", errors.New("missing query parameter"))
		return
	}
	start, err := parseLokiQueryTime(r.Form.Get("start"), time.Time{})
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("invalid start: %w", err))
		return
	}
	end, err := parseLokiQueryTime(r.Form.Get("end"), time.Time{})
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("invalid end: %w", err))
		return
	}
	if end.Before(start) {
		writeLokiError(w, http.StatusBadRequest, "bad_data", errors.New("end timestamp must not be before start time"))
		return
	}
	step, err := parseStepDefault(r.Form.Get("step"), time.Minute)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	limit, err := parseLimitDefault(r.Form.Get("limit"), 100)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	direction := lokiDirection(r.Form.Get("direction"))
	expr, err := parseLogQL(query)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	if expr.logSelector != nil {
		rows, err := s.queryLogQLRows(ctx, *expr.logSelector, start.UnixNano(), end.UnixNano(), limit, direction)
		if err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		writeLokiSuccess(w, lokiQueryData{ResultType: "streams", Result: logStreamResults(rows, limit, direction), Stats: lokiEmptyStats()})
		return
	}
	metricStart, metricEnd := alignLogQLMetricRange(start, end, step)
	startNS, endNS := logQLMetricFetchBounds(expr, metricStart.UnixNano(), metricEnd.UnixNano())
	if data, ok, err := s.tryLogQLRangeMetricSQL(ctx, expr, metricStart, metricEnd, step); ok {
		if err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		writeLokiSuccess(w, data)
		return
	}
	rows, err := s.queryLogQLMetricRows(ctx, expr, startNS, endNS)
	if err != nil {
		writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	writeLokiSuccess(w, lokiQueryData{ResultType: "matrix", Result: evaluateLogQLRangeMetric(expr, rows, metricStart.UnixNano(), metricEnd.UnixNano(), step), Stats: lokiEmptyStats()})
}

func (s *Server) queryLogQLRows(ctx context.Context, selector logQLSelector, startNS, endNS int64, limit int, direction string) ([]logRow, error) {
	maxRows := s.cfg.LogQueryMaxRows
	if maxRows <= 0 {
		maxRows = 100000
	}
	if limit <= 0 || limit > maxRows {
		limit = maxRows
	}
	candidateLimit := logQLCandidateLimit(selector, limit, maxRows)
	for {
		sql := logQLRawSelectSQL(s.cfg, selector, startNS, endNS, candidateLimit, direction)
		scan := scanLogRows
		if logQLSelectorFullyPushed(selector) {
			sql = logQLCompactSelectSQL(s.cfg, selector, startNS, endNS, candidateLimit, direction)
			scan = scanCompactLogRows
		}
		rows, err := scan(ctx, s.client, sql)
		if err != nil {
			return nil, err
		}
		rawCount := len(rows)
		rows = applyLogQLSelector(dedupLogRows(rows), selector)
		if len(rows) >= limit || rawCount < candidateLimit || candidateLimit >= maxRows {
			if len(rows) > limit {
				rows = rows[:limit]
			}
			return rows, nil
		}
		next := candidateLimit * 2
		if next <= candidateLimit {
			next = maxRows
		}
		if next > maxRows {
			next = maxRows
		}
		candidateLimit = next
	}
}

func (s *Server) queryLogQLMetricRows(ctx context.Context, expr *logQLExpr, startNS, endNS int64) ([]logRow, error) {
	selectors := collectLogQLSelectors(expr)
	if len(selectors) == 0 {
		return nil, nil
	}
	allRows := make([]logRow, 0)
	seen := map[string]struct{}{}
	for _, selector := range selectors {
		key := logQLSelectorKey(selector)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		sql := logQLSelectSQL(s.cfg, selector, startNS, endNS, s.cfg.LogQueryMaxRows, "forward")
		rows, err := scanLogRows(ctx, s.client, sql)
		if err != nil {
			return nil, err
		}
		allRows = append(allRows, rows...)
	}
	return dedupLogRows(allRows), nil
}

func firstLogQLSelector(expr *logQLExpr) *logQLSelector {
	if expr == nil {
		return nil
	}
	switch {
	case expr.logSelector != nil:
		return expr.logSelector
	case expr.rangeAgg != nil:
		return &expr.rangeAgg.selector
	case expr.aggregation != nil:
		return firstLogQLSelector(expr.aggregation.expr)
	case expr.topK != nil:
		return firstLogQLSelector(expr.topK.expr)
	case expr.labelFunc != nil:
		return firstLogQLSelector(expr.labelFunc.expr)
	case expr.binaryOp != nil:
		if selector := firstLogQLSelector(expr.binaryOp.lhs); selector != nil {
			return selector
		}
		return firstLogQLSelector(expr.binaryOp.rhs)
	default:
		return nil
	}
}

func collectLogQLSelectors(expr *logQLExpr) []logQLSelector {
	if expr == nil {
		return nil
	}
	switch {
	case expr.logSelector != nil:
		return []logQLSelector{*expr.logSelector}
	case expr.rangeAgg != nil:
		return []logQLSelector{expr.rangeAgg.selector}
	case expr.aggregation != nil:
		return collectLogQLSelectors(expr.aggregation.expr)
	case expr.topK != nil:
		return collectLogQLSelectors(expr.topK.expr)
	case expr.labelFunc != nil:
		return collectLogQLSelectors(expr.labelFunc.expr)
	case expr.binaryOp != nil:
		left := collectLogQLSelectors(expr.binaryOp.lhs)
		return append(left, collectLogQLSelectors(expr.binaryOp.rhs)...)
	default:
		return nil
	}
}

func logQLSelectorKey(selector logQLSelector) string {
	var b strings.Builder
	for _, matcher := range selector.matchers {
		b.WriteString(matcher.name)
		b.WriteByte('\x00')
		b.WriteString(matcher.op)
		b.WriteByte('\x00')
		b.WriteString(matcher.value)
		b.WriteByte('\x00')
	}
	b.WriteByte('\xff')
	for _, stage := range selector.stages {
		b.WriteString(stage.kind)
		b.WriteByte('\x00')
		if stage.lineFilter != nil {
			b.WriteString(stage.lineFilter.op)
			b.WriteByte('\x00')
			b.WriteString(stage.lineFilter.value)
		}
		b.WriteByte('\x00')
		b.WriteString(stage.parser)
		b.WriteByte('\x00')
		b.WriteString(stage.parserParam)
		b.WriteByte('\x00')
		b.WriteString(stage.lineFormat)
		b.WriteByte('\xff')
	}
	return b.String()
}

func logQLCandidateLimit(selector logQLSelector, limit, maxRows int) int {
	if limit <= 0 || limit > maxRows {
		return maxRows
	}
	if !logQLSelectorFullyPushed(selector) {
		return maxRows
	}
	extra := 256
	if limit < 256 {
		extra = limit
	}
	candidateLimit := limit + extra
	if candidateLimit > maxRows {
		return maxRows
	}
	return candidateLimit
}

func logQLSelectorFullyPushed(selector logQLSelector) bool {
	for _, stage := range selector.stages {
		if stage.kind != "line_filter" {
			return false
		}
	}
	return true
}

func dedupLogRows(rows []logRow) []logRow {
	byKey := make(map[string]int, len(rows))
	out := rows[:0]
	for _, row := range rows {
		key := logRowDedupKey(row)
		if idx, ok := byKey[key]; ok {
			if logRowDedupWeight(row) > logRowDedupWeight(out[idx]) {
				out[idx] = row
			}
			continue
		}
		byKey[key] = len(out)
		out = append(out, row)
	}
	return out
}

func logRowDedupKey(row logRow) string {
	return strconv.FormatInt(row.tsNS, 10) + "\xff" + row.line + "\xff" + labelsKey(row.streamLabels)
}

func logRowDedupWeight(row logRow) int {
	weight := len(row.labels) + len(row.fields)
	if row.labels["trace_id"] != "" {
		weight += 1000
	}
	if row.labels["span_id"] != "" {
		weight += 1000
	}
	return weight
}

func logQLMetricFetchBounds(expr *logQLExpr, startNS, endNS int64) (int64, int64) {
	window := maxLogQLWindow(expr)
	offset := maxLogQLOffset(expr)
	return startNS - window.Nanoseconds() - offset.Nanoseconds(), endNS - offset.Nanoseconds()
}

func maxLogQLWindow(expr *logQLExpr) time.Duration {
	if expr == nil {
		return 0
	}
	switch {
	case expr.rangeAgg != nil:
		return expr.rangeAgg.window
	case expr.aggregation != nil:
		return maxLogQLWindow(expr.aggregation.expr)
	case expr.topK != nil:
		return maxLogQLWindow(expr.topK.expr)
	case expr.labelFunc != nil:
		return maxLogQLWindow(expr.labelFunc.expr)
	case expr.binaryOp != nil:
		return maxDuration(maxLogQLWindow(expr.binaryOp.lhs), maxLogQLWindow(expr.binaryOp.rhs))
	default:
		return 0
	}
}

func maxLogQLOffset(expr *logQLExpr) time.Duration {
	if expr == nil {
		return 0
	}
	switch {
	case expr.rangeAgg != nil:
		return expr.rangeAgg.offset
	case expr.aggregation != nil:
		return maxLogQLOffset(expr.aggregation.expr)
	case expr.topK != nil:
		return maxLogQLOffset(expr.topK.expr)
	case expr.labelFunc != nil:
		return maxLogQLOffset(expr.labelFunc.expr)
	case expr.binaryOp != nil:
		return maxDuration(maxLogQLOffset(expr.binaryOp.lhs), maxLogQLOffset(expr.binaryOp.rhs))
	default:
		return 0
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func alignLogQLMetricRange(start, end time.Time, step time.Duration) (time.Time, time.Time) {
	if step <= 0 {
		step = time.Minute
	}
	stepNS := step.Nanoseconds()
	return time.Unix(0, ceilInt64(start.UnixNano(), stepNS)).UTC(), time.Unix(0, ceilInt64(end.UnixNano(), stepNS)).UTC()
}

func ceilInt64(value, multiple int64) int64 {
	if multiple <= 0 {
		return value
	}
	rem := value % multiple
	if rem == 0 {
		return value
	}
	if value >= 0 {
		return value + multiple - rem
	}
	return value - rem
}

func (s *Server) handleLokiLabels(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	start, end, err := parseLokiStartEnd(r)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	limit, err := parseLimit(r.Form.Get("limit"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	names := map[string]struct{}{}
	if s.cfg.LogsTable != "" {
		sql := fmt.Sprintf(
			"SELECT DISTINCT arrayJoin(JSONExtractKeys(attributes_map_str[%s])) AS label_name FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s AND mapContains(attributes_map_str, %s) ORDER BY label_name%s",
			sqlString(lokiStreamLabelsAttributeKey),
			tableName(s.cfg.CHDatabase, s.cfg.LogsTable),
			teamFilter(s.cfg),
			chTimeNanos(start.UnixNano()),
			chTimeNanos(end.UnixNano()),
			sqlString(lokiStreamLabelsAttributeKey),
			sqlLimit(limit),
		)
		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
		defer cancel()
		if err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
			var name string
			if err := row.Scan(&name); err != nil {
				return err
			}
			if name != "" {
				names[name] = struct{}{}
			}
			return nil
		}); err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
	}
	if len(names) == 0 && s.cfg.LogAttributesTable != "" {
		names = map[string]struct{}{
			"service_name":   {},
			"service.name":   {},
			"level":          {},
			"severity_text":  {},
			"trace_id":       {},
			"span_id":        {},
			"detected_level": {},
		}
		sql := fmt.Sprintf(
			"SELECT DISTINCT attribute_key FROM %s WHERE %s AND time_bucket >= toStartOfInterval(%s, toIntervalMinute(10)) AND time_bucket <= toStartOfInterval(%s, toIntervalMinute(10)) AND attribute_type IN ('log', 'resource') ORDER BY attribute_key%s",
			tableName(s.cfg.CHDatabase, s.cfg.LogAttributesTable),
			teamFilter(s.cfg),
			chTimeNanos(start.UnixNano()),
			chTimeNanos(end.UnixNano()),
			sqlLimit(limit),
		)
		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
		defer cancel()
		if err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
			var name string
			if err := row.Scan(&name); err != nil {
				return err
			}
			names[strings.TrimSuffix(name, "__str")] = struct{}{}
			return nil
		}); err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
	}
	writeLokiSuccess(w, sortedLimited(names, limit))
}

func (s *Server) handleLokiLabelValues(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/loki/api/v1/label/") || !strings.HasSuffix(r.URL.Path, "/values") {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/loki/api/v1/label/"), "/values")
	name = strings.Trim(name, "/")
	if name == "" {
		writeLokiError(w, http.StatusBadRequest, "bad_data", errors.New("missing label name"))
		return
	}
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	start, end, err := parseLokiStartEnd(r)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	limit, err := parseLimit(r.Form.Get("limit"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	values := make(map[string]struct{})
	var sql string
	sql = fmt.Sprintf(
		"SELECT DISTINCT JSONExtractString(attributes_map_str[%s], %s) AS label_value FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s AND mapContains(attributes_map_str, %s) AND label_value != '' ORDER BY label_value%s",
		sqlString(lokiStreamLabelsAttributeKey),
		sqlString(name),
		tableName(s.cfg.CHDatabase, s.cfg.LogsTable),
		teamFilter(s.cfg),
		chTimeNanos(start.UnixNano()),
		chTimeNanos(end.UnixNano()),
		sqlString(lokiStreamLabelsAttributeKey),
		sqlLimit(limit),
	)
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	if err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var value string
		if err := row.Scan(&value); err != nil {
			return err
		}
		if value != "" {
			values[value] = struct{}{}
		}
		return nil
	}); err != nil {
		writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	if len(values) == 0 && s.cfg.LogAttributesTable != "" {
		switch name {
		case "service_name", "service.name":
			sql = fmt.Sprintf("SELECT DISTINCT service_name FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s ORDER BY service_name%s", tableName(s.cfg.CHDatabase, s.cfg.LogsTable), teamFilter(s.cfg), chTimeNanos(start.UnixNano()), chTimeNanos(end.UnixNano()), sqlLimit(limit))
		case "level", "severity", "severity_text":
			sql = fmt.Sprintf("SELECT DISTINCT severity_text FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s ORDER BY severity_text%s", tableName(s.cfg.CHDatabase, s.cfg.LogsTable), teamFilter(s.cfg), chTimeNanos(start.UnixNano()), chTimeNanos(end.UnixNano()), sqlLimit(limit))
		case "trace_id", "span_id":
			sql = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s ORDER BY %s%s", quoteIdent(name), tableName(s.cfg.CHDatabase, s.cfg.LogsTable), teamFilter(s.cfg), chTimeNanos(start.UnixNano()), chTimeNanos(end.UnixNano()), quoteIdent(name), sqlLimit(limit))
		default:
			sql = fmt.Sprintf(
				"SELECT DISTINCT attribute_value FROM %s WHERE %s AND attribute_key = %s AND time_bucket >= toStartOfInterval(%s, toIntervalMinute(10)) AND time_bucket <= toStartOfInterval(%s, toIntervalMinute(10)) AND attribute_type IN ('log', 'resource') ORDER BY attribute_value%s",
				tableName(s.cfg.CHDatabase, s.cfg.LogAttributesTable),
				teamFilter(s.cfg),
				sqlString(name),
				chTimeNanos(start.UnixNano()),
				chTimeNanos(end.UnixNano()),
				sqlLimit(limit),
			)
		}
		if err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
			var value string
			if err := row.Scan(&value); err != nil {
				return err
			}
			if value != "" {
				values[value] = struct{}{}
			}
			return nil
		}); err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
	}
	writeLokiSuccess(w, sortedLimited(values, limit))
}

func (s *Server) handleLokiSeries(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	start, end, err := parseLokiStartEnd(r)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	matches := r.Form["match[]"]
	if len(matches) == 0 {
		matches = r.Form["match"]
	}
	if len(matches) == 0 {
		matches = []string{"{}"}
	}
	seen := map[string]map[string]string{}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	for _, match := range matches {
		selector, err := parseLogQLSelector(match)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "bad_data", err)
			return
		}
		sql := logQLSelectSQL(s.cfg, *selector, start.UnixNano(), end.UnixNano(), s.cfg.MaxSeries, "forward")
		rows, err := scanLogRows(ctx, s.client, sql)
		if err != nil {
			writeLokiError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		for _, row := range rows {
			if !selector.matchesLabels(row.labels) {
				continue
			}
			labels := stableLabelMap(row.labels)
			seen[labelsKey(labels)] = labels
			if len(seen) >= s.cfg.MaxSeries {
				break
			}
		}
	}
	result := make([]map[string]string, 0, len(seen))
	for _, labels := range seen {
		result = append(result, labels)
	}
	sort.Slice(result, func(i, j int) bool { return labelsKey(result[i]) < labelsKey(result[j]) })
	writeLokiSuccess(w, result)
}

func (s *Server) handleLokiIndexStats(w http.ResponseWriter, r *http.Request) {
	writeLokiSuccess(w, map[string]any{
		"streams": 0,
		"chunks":  0,
		"bytes":   0,
		"entries": 0,
	})
}

func (s *Server) handleLokiBuildInfo(w http.ResponseWriter, r *http.Request) {
	writeLokiSuccess(w, map[string]any{
		"version":   "snuffle",
		"revision":  "",
		"branch":    "",
		"buildDate": "",
	})
}

func (s *Server) decodeLokiPushRows(r *http.Request) ([]lokiLogInsertRow, error) {
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	if contentType == "application/x-protobuf" {
		decoded, _, err := readSnappyBody(r, "loki push")
		if err != nil {
			return nil, err
		}
		return s.decodeLokiPushProto(decoded)
	}
	reader := io.Reader(r.Body)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("decode gzip loki push request: %w", err)
		}
		defer gz.Close()
		reader = gz
	}
	var req lokiPushRequest
	if err := json.NewDecoder(reader).Decode(&req); err != nil {
		return nil, fmt.Errorf("decode loki push JSON: %w", err)
	}
	return s.rowsFromLokiPushRequest(req)
}

func (s *Server) rowsFromLokiPushRequest(req lokiPushRequest) ([]lokiLogInsertRow, error) {
	rows := make([]lokiLogInsertRow, 0)
	for _, stream := range req.Streams {
		labels := cloneStringMap(stream.Stream)
		if len(labels) == 0 && stream.Labels != "" {
			parsed, err := parseLokiLabelString(stream.Labels)
			if err != nil {
				return nil, err
			}
			labels = parsed
		}
		for _, value := range stream.Values {
			if len(value) < 2 {
				continue
			}
			ts, err := parseLokiPushTimestamp(fmt.Sprint(value[0]))
			if err != nil {
				return nil, err
			}
			line := fmt.Sprint(value[1])
			metadata := map[string]string{}
			if len(value) >= 3 {
				if raw, ok := value[2].(map[string]any); ok {
					for key, val := range raw {
						metadata[key] = fmt.Sprint(val)
					}
				}
			}
			rows = append(rows, s.lokiInsertRow(labels, metadata, ts, line))
		}
		for _, entry := range stream.Entries {
			tsRaw := entry.Timestamp
			if tsRaw == "" {
				tsRaw = entry.Time
			}
			ts, err := parseLokiPushTimestamp(tsRaw)
			if err != nil {
				return nil, err
			}
			rows = append(rows, s.lokiInsertRow(labels, nil, ts, entry.Line))
		}
	}
	return rows, nil
}

type lokiProtoStream struct {
	labels  map[string]string
	entries []lokiProtoEntry
}

type lokiProtoEntry struct {
	timestamp time.Time
	line      string
	metadata  map[string]string
}

func (s *Server) decodeLokiPushProto(data []byte) ([]lokiLogInsertRow, error) {
	var rows []lokiLogInsertRow
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return nil, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return nil, fmt.Errorf("decode loki push protobuf: stream has wire type %d", wireType)
			}
			message, err := readProtoBytes(data, &i)
			if err != nil {
				return nil, err
			}
			stream, err := parseLokiProtoStream(message)
			if err != nil {
				return nil, err
			}
			for _, entry := range stream.entries {
				rows = append(rows, s.lokiInsertRow(stream.labels, entry.metadata, entry.timestamp, entry.line))
			}
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return nil, err
			}
		}
	}
	return rows, nil
}

func parseLokiProtoStream(data []byte) (lokiProtoStream, error) {
	var stream lokiProtoStream
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return stream, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return stream, fmt.Errorf("decode loki stream protobuf: labels has wire type %d", wireType)
			}
			raw, err := readProtoBytes(data, &i)
			if err != nil {
				return stream, err
			}
			stream.labels, err = parseLokiLabelString(string(raw))
			if err != nil {
				return stream, err
			}
		case 2:
			if wireType != protoWireBytes {
				return stream, fmt.Errorf("decode loki stream protobuf: entry has wire type %d", wireType)
			}
			raw, err := readProtoBytes(data, &i)
			if err != nil {
				return stream, err
			}
			entry, err := parseLokiProtoEntry(raw)
			if err != nil {
				return stream, err
			}
			stream.entries = append(stream.entries, entry)
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return stream, err
			}
		}
	}
	if stream.labels == nil {
		stream.labels = map[string]string{}
	}
	return stream, nil
}

func parseLokiProtoEntry(data []byte) (lokiProtoEntry, error) {
	entry := lokiProtoEntry{metadata: map[string]string{}}
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return entry, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return entry, fmt.Errorf("decode loki entry protobuf: timestamp has wire type %d", wireType)
			}
			raw, err := readProtoBytes(data, &i)
			if err != nil {
				return entry, err
			}
			entry.timestamp, err = parseLokiProtoTimestamp(raw)
			if err != nil {
				return entry, err
			}
		case 2:
			if wireType != protoWireBytes {
				return entry, fmt.Errorf("decode loki entry protobuf: line has wire type %d", wireType)
			}
			raw, err := readProtoBytes(data, &i)
			if err != nil {
				return entry, err
			}
			entry.line = string(raw)
		case 3:
			if wireType != protoWireBytes {
				return entry, fmt.Errorf("decode loki entry protobuf: metadata has wire type %d", wireType)
			}
			raw, err := readProtoBytes(data, &i)
			if err != nil {
				return entry, err
			}
			name, value, err := parseLokiProtoLabelPair(raw)
			if err != nil {
				return entry, err
			}
			if name != "" {
				entry.metadata[name] = value
			}
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return entry, err
			}
		}
	}
	return entry, nil
}

func parseLokiProtoTimestamp(data []byte) (time.Time, error) {
	var seconds int64
	var nanos int64
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return time.Time{}, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireVarint {
				return time.Time{}, fmt.Errorf("decode timestamp protobuf: seconds has wire type %d", wireType)
			}
			value, err := readProtoVarint(data, &i)
			if err != nil {
				return time.Time{}, err
			}
			seconds = int64(value)
		case 2:
			if wireType != protoWireVarint {
				return time.Time{}, fmt.Errorf("decode timestamp protobuf: nanos has wire type %d", wireType)
			}
			value, err := readProtoVarint(data, &i)
			if err != nil {
				return time.Time{}, err
			}
			nanos = int64(value)
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return time.Time{}, err
			}
		}
	}
	return time.Unix(seconds, nanos).UTC(), nil
}

func parseLokiProtoLabelPair(data []byte) (string, string, error) {
	var name string
	var value string
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return "", "", err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		if wireType != protoWireBytes {
			if err := skipProtoField(data, &i, wireType); err != nil {
				return "", "", err
			}
			continue
		}
		raw, err := readProtoBytes(data, &i)
		if err != nil {
			return "", "", err
		}
		switch fieldNum {
		case 1:
			name = string(raw)
		case 2:
			value = string(raw)
		}
	}
	return name, value, nil
}

func (s *Server) lokiInsertRow(labels, metadata map[string]string, timestamp time.Time, line string) lokiLogInsertRow {
	resourceAttributes := map[string]string{}
	attributes := map[string]string{}
	serviceName := ""
	severityText := ""
	traceID := ""
	spanID := ""
	addLabel := func(key, value string) {
		if key == "" {
			return
		}
		switch key {
		case "service_name", "service.name":
			serviceName = value
		case "level", "severity", "severity_text", "detected_level":
			severityText = value
			attributes["detected_level__str"] = value
		case "trace_id":
			traceID = value
			attributes[key+"__str"] = value
		case "span_id":
			spanID = value
			attributes[key+"__str"] = value
		default:
			attributes[key+"__str"] = value
		}
	}
	for key, value := range labels {
		addLabel(key, value)
	}
	if len(labels) > 0 {
		attributes[lokiStreamLabelsAttributeKey] = stableJSONMap(labels)
	}
	if len(metadata) > 0 {
		attributes[lokiEntryMetadataAttributeKey] = stableJSONMap(metadata)
	}
	for key, value := range metadata {
		if strings.HasPrefix(key, "resource_") {
			resourceAttributes[strings.TrimPrefix(key, "resource_")] = value
			continue
		}
		if strings.HasPrefix(key, "resource.") {
			resourceAttributes[strings.TrimPrefix(key, "resource.")] = value
			continue
		}
		addLabel(key, value)
	}
	if serviceName == "" {
		if value := resourceAttributes["service.name"]; value != "" {
			serviceName = value
		}
	}
	if severityText == "" {
		severityText = inferLogSeverity(line)
	}
	return lokiLogInsertRow{
		teamID:                  int32(s.cfg.TeamID),
		originalExpiryTimestamp: timestamp.Add(s.cfg.LogRetention),
		uuid:                    lokiLogUUID(labels, timestamp, line),
		traceID:                 traceID,
		spanID:                  spanID,
		timestamp:               timestamp.UTC(),
		observedTimestamp:       time.Now().UTC(),
		body:                    line,
		severityText:            severityText,
		severityNumber:          severityNumber(severityText),
		serviceName:             serviceName,
		resourceAttributes:      resourceAttributes,
		attributes:              attributes,
	}
}

func (s *Server) insertLokiLogRows(ctx context.Context, rows []lokiLogInsertRow) error {
	if len(rows) == 0 {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, original_expiry_timestamp, uuid, trace_id, span_id, trace_flags, timestamp, observed_timestamp, body, severity_text, severity_number, service_name, resource_attributes, instrumentation_scope, event_name, attributes_map_str)", tableName(s.cfg.CHDatabase, s.cfg.LogsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]int32, len(rows))
		expiry := make([]time.Time, len(rows))
		uuids := make([]string, len(rows))
		traceIDs := make([]string, len(rows))
		spanIDs := make([]string, len(rows))
		traceFlags := make([]int32, len(rows))
		timestamps := make([]time.Time, len(rows))
		observed := make([]time.Time, len(rows))
		bodies := make([]string, len(rows))
		severityTexts := make([]string, len(rows))
		severityNumbers := make([]int32, len(rows))
		serviceNames := make([]string, len(rows))
		resourceAttrs := make([]map[string]string, len(rows))
		scopes := make([]string, len(rows))
		events := make([]string, len(rows))
		attrs := make([]map[string]string, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.teamID
			expiry[i] = row.originalExpiryTimestamp
			uuids[i] = row.uuid
			traceIDs[i] = row.traceID
			spanIDs[i] = row.spanID
			traceFlags[i] = row.traceFlags
			timestamps[i] = row.timestamp
			observed[i] = row.observedTimestamp
			bodies[i] = row.body
			severityTexts[i] = row.severityText
			severityNumbers[i] = row.severityNumber
			serviceNames[i] = row.serviceName
			resourceAttrs[i] = row.resourceAttributes
			if resourceAttrs[i] == nil {
				resourceAttrs[i] = map[string]string{}
			}
			scopes[i] = row.instrumentationScope
			events[i] = row.eventName
			attrs[i] = row.attributes
			if attrs[i] == nil {
				attrs[i] = map[string]string{}
			}
		}
		columns := []any{teamIDs, expiry, uuids, traceIDs, spanIDs, traceFlags, timestamps, observed, bodies, severityTexts, severityNumbers, serviceNames, resourceAttrs, scopes, events, attrs}
		for i, column := range columns {
			if err := batch.Column(i).Append(column); err != nil {
				return 0, err
			}
		}
		return len(rows), nil
	})
}

func parseLokiLabelString(input string) (map[string]string, error) {
	input = strings.TrimSpace(input)
	if strings.HasPrefix(input, "{") && strings.HasSuffix(input, "}") {
		input = strings.TrimSpace(input[1 : len(input)-1])
	}
	matchers, err := parseLogQLMatchers(input)
	if err != nil {
		return nil, err
	}
	labels := make(map[string]string, len(matchers))
	for _, matcher := range matchers {
		if matcher.op != "=" {
			return nil, fmt.Errorf("push labels must use equality matchers, got %s", matcher.op)
		}
		labels[matcher.name] = matcher.value
	}
	return labels, nil
}

func parseLokiPushTimestamp(input string) (time.Time, error) {
	input = strings.Trim(strings.TrimSpace(input), `"`)
	if input == "" {
		return time.Time{}, errors.New("missing log timestamp")
	}
	if ns, err := strconv.ParseInt(input, 10, 64); err == nil {
		return time.Unix(0, ns).UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, input); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("cannot parse log timestamp %q", input)
}

func parseLokiQueryTime(value string, fallback time.Time) (time.Time, error) {
	if value == "" {
		if fallback.IsZero() {
			return time.Time{}, errors.New("missing timestamp")
		}
		return fallback.UTC(), nil
	}
	if ns, err := strconv.ParseInt(value, 10, 64); err == nil && len(strings.TrimLeft(value, "-")) > 11 {
		return time.Unix(0, ns).UTC(), nil
	}
	return parseAPITime(value, fallback)
}

func parseLokiStartEnd(r *http.Request) (time.Time, time.Time, error) {
	now := time.Now()
	start, err := parseLokiQueryTime(r.Form.Get("start"), now.Add(-6*time.Hour))
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseLokiQueryTime(r.Form.Get("end"), now)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return start, end, nil
}

func parseLimitDefault(value string, fallback int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	return parseLimit(value)
}

func parseStepDefault(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	return parseStep(value)
}

func lokiDirection(value string) string {
	if strings.EqualFold(value, "forward") {
		return "forward"
	}
	return "backward"
}

func inferLogSeverity(line string) string {
	if value := explicitLogSeverity(line); value != "" {
		return value
	}
	return detectLogSeverityFromLine(line)
}

func explicitLogSeverity(line string) string {
	for _, fields := range []map[string]string{parseFlatJSONFields(line, ""), parseLogfmtFields(line)} {
		for _, key := range []string{"detected_level", "level", "severity", "severity_text", "lvl"} {
			if value := normalizeLogSeverity(fields[key]); value != "" {
				return value
			}
		}
	}
	return ""
}

func normalizeLogSeverity(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "trace":
		return "trace"
	case "debug":
		return "debug"
	case "info", "information":
		return "info"
	case "warn", "warning":
		return "warn"
	case "error", "err":
		return "error"
	case "critical", "crit":
		return "critical"
	case "fatal":
		return "fatal"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func severityNumber(text string) int32 {
	switch strings.ToLower(text) {
	case "trace":
		return 1
	case "debug":
		return 5
	case "info", "", "unknown":
		return 9
	case "warn", "warning":
		return 13
	case "error":
		return 17
	case "fatal", "critical":
		return 21
	default:
		return 0
	}
}

var logSeverityPatterns = []struct {
	word  string
	level string
}{
	{"fatal", "fatal"},
	{"critical", "critical"},
	{"error", "error"},
	{"err", "error"},
	{"info", "info"},
}

func detectLogSeverityFromLine(line string) string {
	lower := strings.ToLower(line)
	idx := len(lower)
	best := "unknown"
	for _, pattern := range logSeverityPatterns {
		pos := indexOfBoundedLogSeverity(lower, pattern.word)
		if pos == -1 || pos >= idx {
			continue
		}
		idx = pos
		best = pattern.level
		if idx == 0 {
			break
		}
	}
	return best
}

func indexOfBoundedLogSeverity(line, word string) int {
	offset := 0
	for {
		pos := strings.Index(line[offset:], word)
		if pos == -1 {
			return -1
		}
		abs := offset + pos
		if isLeftLogSeverityBoundary(line, abs-1) && isRightLogSeverityBoundary(line, abs+len(word)) {
			return abs
		}
		offset = abs + 1
	}
}

func isLeftLogSeverityBoundary(s string, pos int) bool {
	if pos < 0 || pos >= len(s) {
		return true
	}
	switch s[pos] {
	case ' ', '\t', '\n', '[', '(', '{', '"', '\'', '=', '|':
		return true
	default:
		return false
	}
}

func isRightLogSeverityBoundary(s string, pos int) bool {
	if pos < 0 || pos >= len(s) {
		return true
	}
	switch s[pos] {
	case ' ', '\t', '\n', '[', ']', '(', ')', '{', '}', ':', ',', '!', '"', '\'', '=', '|':
		return true
	default:
		return false
	}
}

func newLogUUID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(raw[:])
}

func lokiLogUUID(labels map[string]string, timestamp time.Time, line string) string {
	h := sha256.New()
	h.Write([]byte(stableJSONMap(labels)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(timestamp.UTC().UnixNano(), 10)))
	h.Write([]byte{0})
	h.Write([]byte(line))
	return "loki:" + hex.EncodeToString(h.Sum(nil))
}

func writeLokiSuccess(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, lokiResponse{Status: "success", Data: data})
}

func writeLokiError(w http.ResponseWriter, code int, errorType string, err error) {
	recordResponseError(w, errorType, err)
	message := ""
	if err != nil {
		message = err.Error()
	}
	writeJSON(w, code, lokiResponse{Status: "error", ErrorType: errorType, Error: message})
}

func lokiEmptyStats() map[string]any {
	return map[string]any{
		"summary": map[string]any{
			"bytesProcessedPerSecond": 0,
			"linesProcessedPerSecond": 0,
			"totalBytesProcessed":     0,
			"totalLinesProcessed":     0,
			"execTime":                0,
			"queueTime":               0,
		},
	}
}
