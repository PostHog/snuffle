package snuffle

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type promRequestStatsKey struct{}

type promRequestStats struct {
	clickHouseQueries atomic.Int64
	readRows          atomic.Int64
	scannedRows       atomic.Int64
	readBytes         atomic.Int64
	clickHouseInserts atomic.Int64
	writtenRows       atomic.Int64
}

var failedQueryLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

type queryLogMetadata struct {
	language  string
	queryType string
	query     string
	backend   string
	time      string
	start     string
	end       string
	step      string
	limit     string
	direction string
}

func withPromRequestStats(ctx context.Context, stats *promRequestStats) context.Context {
	return context.WithValue(ctx, promRequestStatsKey{}, stats)
}

func promRequestStatsFromContext(ctx context.Context) *promRequestStats {
	stats, _ := ctx.Value(promRequestStatsKey{}).(*promRequestStats)
	return stats
}

func recordClickHouseRead(ctx context.Context, rows, scannedRows, readBytes int64) {
	if stats := promRequestStatsFromContext(ctx); stats != nil {
		stats.clickHouseQueries.Add(1)
		stats.readRows.Add(rows)
		stats.scannedRows.Add(scannedRows)
		stats.readBytes.Add(readBytes)
	}
}

func recordClickHouseWrite(ctx context.Context, rows int64) {
	if stats := promRequestStatsFromContext(ctx); stats != nil {
		stats.clickHouseInserts.Add(1)
		stats.writtenRows.Add(rows)
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status       int
	bytes        int64
	errorType    string
	errorMessage string
	queryMeta    *queryLogMetadata
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(payload)
	w.bytes += int64(n)
	return n, err
}

func (w *loggingResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *loggingResponseWriter) recordAPIError(errorType string, err error) {
	w.errorType = errorType
	if err != nil {
		w.errorMessage = err.Error()
	}
}

func (w *loggingResponseWriter) recordQueryLogMetadata(meta queryLogMetadata) {
	w.queryMeta = &meta
}

func (w *loggingResponseWriter) recordQueryLogBackend(backend string) {
	if w.queryMeta == nil {
		w.queryMeta = &queryLogMetadata{}
	}
	w.queryMeta.backend = backend
}

func recordResponseError(w http.ResponseWriter, errorType string, err error) {
	if recorder, ok := w.(interface {
		recordAPIError(string, error)
	}); ok {
		recorder.recordAPIError(errorType, err)
	}
}

func recordQueryLogMetadata(w http.ResponseWriter, meta queryLogMetadata) {
	if recorder, ok := w.(interface {
		recordQueryLogMetadata(queryLogMetadata)
	}); ok {
		recorder.recordQueryLogMetadata(meta)
	}
}

func recordQueryLogBackend(w http.ResponseWriter, backend string) {
	if recorder, ok := w.(interface {
		recordQueryLogBackend(string)
	}); ok {
		recorder.recordQueryLogBackend(backend)
	}
}

func writeHTTPError(w http.ResponseWriter, code int, errorType string, err error) {
	recordResponseError(w, errorType, err)
	message := http.StatusText(code)
	if err != nil {
		message = err.Error()
	}
	http.Error(w, message, code)
}

func logPromRequestReceived(r *http.Request) {
	attrs := requestAttrs(r)
	attrs = append(attrs,
		slog.Int64("content_length", r.ContentLength),
	)
	slog.LogAttrs(r.Context(), slog.LevelInfo, "prom request received", attrs...)
}

func logPromRequestCompleted(r *http.Request, w *loggingResponseWriter, stats *promRequestStats, started time.Time, teamID uint64, haveTeamID bool) {
	status := w.statusCode()
	attrs := requestAttrs(r)
	if haveTeamID {
		attrs = append(attrs, slog.Uint64("team_id", teamID))
	}
	attrs = append(attrs,
		slog.Int("status", status),
		slog.String("status_text", http.StatusText(status)),
		slog.Int64("duration_ms", time.Since(started).Milliseconds()),
		slog.Int64("response_bytes", w.bytes),
		slog.Int64("clickhouse_queries", stats.clickHouseQueries.Load()),
		slog.Int64("read_rows", stats.readRows.Load()),
		slog.Int64("scanned_rows", stats.scannedRows.Load()),
		slog.Int64("read_bytes", stats.readBytes.Load()),
		slog.Int64("clickhouse_inserts", stats.clickHouseInserts.Load()),
		slog.Int64("written_rows", stats.writtenRows.Load()),
	)
	if w.errorType != "" {
		attrs = append(attrs, slog.String("error_type", w.errorType))
	}
	errorMessage := w.errorMessage
	if errorMessage == "" && status >= http.StatusBadRequest {
		errorMessage = http.StatusText(status)
	}
	if errorMessage != "" {
		attrs = append(attrs, slog.String("error", errorMessage))
	}
	if status >= http.StatusBadRequest && w.queryMeta != nil {
		logFailedQuery(r, w, stats, started, teamID, haveTeamID, status, errorMessage)
	}

	level := slog.LevelInfo
	switch {
	case status >= http.StatusInternalServerError:
		level = slog.LevelError
	case status >= http.StatusBadRequest:
		level = slog.LevelWarn
	}
	slog.LogAttrs(r.Context(), level, "prom request completed", attrs...)
}

func logFailedQuery(r *http.Request, w *loggingResponseWriter, stats *promRequestStats, started time.Time, teamID uint64, haveTeamID bool, status int, errorMessage string) {
	meta := w.queryMeta
	if meta == nil {
		return
	}
	attrs := requestAttrs(r)
	if haveTeamID {
		attrs = append(attrs, slog.Uint64("team_id", teamID))
	}
	attrs = append(attrs,
		slog.Int("status", status),
		slog.String("status_text", http.StatusText(status)),
		slog.Int64("duration_ms", time.Since(started).Milliseconds()),
		slog.Int64("response_bytes", w.bytes),
		slog.Int64("clickhouse_queries", stats.clickHouseQueries.Load()),
		slog.Int64("read_rows", stats.readRows.Load()),
		slog.Int64("scanned_rows", stats.scannedRows.Load()),
		slog.Int64("read_bytes", stats.readBytes.Load()),
		slog.String("query_language", meta.language),
		slog.String("query_type", meta.queryType),
		slog.String("query", meta.query),
	)
	if meta.backend != "" {
		attrs = append(attrs, slog.String("query_backend", meta.backend))
	}
	if meta.time != "" {
		attrs = append(attrs, slog.String("query_time", meta.time))
	}
	if meta.start != "" {
		attrs = append(attrs, slog.String("query_start", meta.start))
	}
	if meta.end != "" {
		attrs = append(attrs, slog.String("query_end", meta.end))
	}
	if meta.step != "" {
		attrs = append(attrs, slog.String("query_step", meta.step))
	}
	if meta.limit != "" {
		attrs = append(attrs, slog.String("query_limit", meta.limit))
	}
	if meta.direction != "" {
		attrs = append(attrs, slog.String("query_direction", meta.direction))
	}
	if w.errorType != "" {
		attrs = append(attrs, slog.String("error_type", w.errorType))
	}
	if errorMessage != "" {
		attrs = append(attrs, slog.String("error", errorMessage))
	}
	failedQueryLogger.LogAttrs(r.Context(), slog.LevelWarn, "query failed", attrs...)
}

func requestAttrs(r *http.Request) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("remote_addr", r.RemoteAddr),
	}
	if r.URL.RawQuery != "" {
		attrs = append(attrs, slog.String("raw_query", r.URL.RawQuery))
	}
	if userAgent := r.UserAgent(); userAgent != "" {
		attrs = append(attrs, slog.String("user_agent", userAgent))
	}
	return attrs
}
