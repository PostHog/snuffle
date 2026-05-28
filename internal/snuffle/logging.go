package snuffle

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

type promRequestStatsKey struct{}

type promRequestStats struct {
	clickHouseQueries atomic.Int64
	readRows          atomic.Int64
	clickHouseInserts atomic.Int64
	writtenRows       atomic.Int64
}

func withPromRequestStats(ctx context.Context, stats *promRequestStats) context.Context {
	return context.WithValue(ctx, promRequestStatsKey{}, stats)
}

func promRequestStatsFromContext(ctx context.Context) *promRequestStats {
	stats, _ := ctx.Value(promRequestStatsKey{}).(*promRequestStats)
	return stats
}

func recordClickHouseRead(ctx context.Context, rows int64) {
	if stats := promRequestStatsFromContext(ctx); stats != nil {
		stats.clickHouseQueries.Add(1)
		stats.readRows.Add(rows)
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

func recordResponseError(w http.ResponseWriter, errorType string, err error) {
	if recorder, ok := w.(interface {
		recordAPIError(string, error)
	}); ok {
		recorder.recordAPIError(errorType, err)
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

	level := slog.LevelInfo
	switch {
	case status >= http.StatusInternalServerError:
		level = slog.LevelError
	case status >= http.StatusBadRequest:
		level = slog.LevelWarn
	}
	slog.LogAttrs(r.Context(), level, "prom request completed", attrs...)
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
