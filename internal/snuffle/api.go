package snuffle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

type Server struct {
	cfg       Config
	client    *ClickHouseClient
	queryable *CHQueryable
	engine    *promql.Engine
	parser    parser.Parser
	keyMu     *sync.Mutex
	bitmapMax map[uint64]uint64
}

func Run(cfg Config) error {
	server := newServer(cfg)

	mux := http.NewServeMux()
	server.routes(mux)

	addr := cfg.HTTPHost + ":" + cfg.HTTPPort
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	slog.Info("starting PromQL ClickHouse sidecar", "addr", addr, "series_table", cfg.SeriesTable, "samples_table", cfg.SamplesTable, "label_index_table", cfg.LabelIndexTable, "lookback_delta", cfg.LookbackDelta.String(), "max_samples", cfg.MaxSamples)
	return srv.ListenAndServe()
}

func newServer(cfg Config) *Server {
	client := NewClickHouseClient(cfg)
	queryable := NewCHQueryable(client, cfg)
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:               slog.Default(),
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.QueryTimeout,
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			if rangeMillis <= 0 {
				return int64(time.Minute / time.Millisecond)
			}
			step := rangeMillis / 10
			if step < int64(time.Second/time.Millisecond) {
				return int64(time.Second / time.Millisecond)
			}
			return step
		},
	})

	return &Server{
		cfg:       cfg,
		client:    client,
		queryable: queryable,
		engine:    engine,
		parser:    parser.NewParser(parser.Options{}),
		keyMu:     &sync.Mutex{},
		bitmapMax: make(map[uint64]uint64),
	}
}

func (s *Server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/-/healthy", s.handleHealthy)
	mux.HandleFunc("/-/ready", s.handleHealthy)

	api := s.apiRoutes()
	mux.Handle("/api/v1/", api)
	mux.HandleFunc("/t/", s.handleTeamPath(api))
	mux.HandleFunc("/team/", s.handleTeamPath(api))
}

func (s *Server) apiRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", s.teamHandler((*Server).handleQuery))
	mux.HandleFunc("/api/v1/query_range", s.teamHandler((*Server).handleQueryRange))
	mux.HandleFunc("/api/v1/write", s.teamHandler((*Server).handleRemoteWrite))
	mux.HandleFunc("/api/v1/read", s.teamHandler((*Server).handleRemoteRead))
	mux.HandleFunc("/api/v1/labels", s.teamHandler((*Server).handleLabels))
	mux.HandleFunc("/api/v1/label/", s.teamHandler((*Server).handleLabelValues))
	mux.HandleFunc("/api/v1/series", s.teamHandler((*Server).handleSeries))
	mux.HandleFunc("/api/v1/metadata", s.teamHandler((*Server).handleMetadata))
	mux.HandleFunc("/api/v1/rules", s.teamHandler((*Server).handleRules))
	mux.HandleFunc("/api/v1/alerts", s.teamHandler((*Server).handleAlerts))
	mux.HandleFunc("/api/v1/query_exemplars", s.teamHandler((*Server).handleQueryExemplars))
	return mux
}

type requestTeamIDKey struct{}

func (s *Server) teamHandler(handler func(*Server, http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := &promRequestStats{}
		wrapped := &loggingResponseWriter{ResponseWriter: w}
		withStats := r.WithContext(withPromRequestStats(r.Context(), stats))
		started := time.Now()
		var teamID uint64
		var haveTeamID bool

		logPromRequestReceived(withStats)
		defer func() {
			logPromRequestCompleted(withStats, wrapped, stats, started, teamID, haveTeamID)
		}()

		var err error
		teamID, err = s.teamIDFromRequest(withStats)
		if err != nil {
			writeAPIError(wrapped, http.StatusBadRequest, "bad_data", err)
			return
		}
		haveTeamID = true

		handler(s.withTeamID(teamID), wrapped, withStats)
	}
}

func (s *Server) handleTeamPath(api http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		teamID, strippedPath, err := parseTeamPath(r.URL.Path)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "bad_data", err)
			return
		}
		if strippedPath == "" {
			http.NotFound(w, r)
			return
		}
		cloned := r.Clone(context.WithValue(r.Context(), requestTeamIDKey{}, teamID))
		urlCopy := *r.URL
		urlCopy.Path = strippedPath
		urlCopy.RawPath = ""
		cloned.URL = &urlCopy
		api.ServeHTTP(w, cloned)
	}
}

func parseTeamPath(path string) (uint64, string, error) {
	for _, prefix := range []string{"/t/", "/team/"} {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		rest := strings.TrimPrefix(path, prefix)
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			return 0, "", fmt.Errorf("tenant path must be %s{team_id}/api/v1/...", prefix)
		}
		teamID, err := parseTeamID(parts[0])
		if err != nil {
			return 0, "", err
		}
		return teamID, "/" + parts[1], nil
	}
	return 0, "", nil
}

func (s *Server) teamIDFromRequest(r *http.Request) (uint64, error) {
	if value, ok := r.Context().Value(requestTeamIDKey{}).(uint64); ok {
		return value, nil
	}
	if s.cfg.TeamHeader != "" {
		if value := strings.TrimSpace(r.Header.Get(s.cfg.TeamHeader)); value != "" {
			return parseTeamID(value)
		}
	}
	if s.cfg.TeamQueryParam != "" {
		if value := strings.TrimSpace(r.URL.Query().Get(s.cfg.TeamQueryParam)); value != "" {
			return parseTeamID(value)
		}
	}
	return s.cfg.DefaultTeamID, nil
}

func parseTeamID(value string) (uint64, error) {
	teamID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid team_id %q", value)
	}
	return teamID, nil
}

func (s *Server) withTeamID(teamID uint64) *Server {
	cfg := s.cfg
	cfg.TeamID = teamID
	return &Server{
		cfg:       cfg,
		client:    s.client,
		queryable: NewCHQueryable(s.client, cfg),
		engine:    s.engine,
		parser:    s.parser,
		keyMu:     s.keyMu,
		bitmapMax: s.bitmapMax,
	}
}

func (s *Server) handleHealthy(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.CHTimeout)
	defer cancel()
	if err := s.client.Ping(ctx); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	query := r.Form.Get("query")
	if query == "" {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("missing query parameter"))
		return
	}
	ts, err := parseAPITime(r.Form.Get("time"), time.Now())
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()

	if data, ok, err := s.tryFastInstantQuery(ctx, query, ts); ok {
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		writeAPISuccess(w, data)
		return
	}

	q, err := s.engine.NewInstantQuery(ctx, s.queryable, promql.NewPrometheusQueryOpts(false, s.cfg.LookbackDelta), query, ts)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	defer q.Close()

	result := q.Exec(ctx)
	if result.Err != nil {
		writeAPIError(w, http.StatusUnprocessableEntity, "execution", result.Err)
		return
	}
	writeAPISuccess(w, responseDataFromValue(result.Value))
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	query := r.Form.Get("query")
	if query == "" {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("missing query parameter"))
		return
	}
	start, err := parseAPITime(r.Form.Get("start"), time.Time{})
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("invalid start: %w", err))
		return
	}
	end, err := parseAPITime(r.Form.Get("end"), time.Time{})
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("invalid end: %w", err))
		return
	}
	step, err := parseStep(r.Form.Get("step"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	if end.Before(start) {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("end timestamp must not be before start time"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()

	if data, ok, err := s.tryFastRangeQuery(ctx, query, start, end, step); ok {
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		writeAPISuccess(w, data)
		return
	}

	q, err := s.engine.NewRangeQuery(ctx, s.queryable, promql.NewPrometheusQueryOpts(false, s.cfg.LookbackDelta), query, start, end, step)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	defer q.Close()

	result := q.Exec(ctx)
	if result.Err != nil {
		writeAPIError(w, http.StatusUnprocessableEntity, "execution", result.Err)
		return
	}
	writeAPISuccess(w, responseDataFromValue(result.Value))
}

func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	start, end, err := parseStartEnd(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	limit, err := parseLimit(r.Form.Get("limit"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	matchers, err := s.parseMatcherParams(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	q, err := s.queryable.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "execution", err)
		return
	}
	defer q.Close()
	namesSet := make(map[string]struct{})
	hints := &storage.LabelHints{Limit: limit}
	for _, matcherSet := range matchers {
		names, _, err := q.LabelNames(ctx, hints, matcherSet...)
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		for _, name := range names {
			namesSet[name] = struct{}{}
		}
	}
	writeAPISuccess(w, sortedLimited(namesSet, limit))
}

func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/v1/label/") || !strings.HasSuffix(r.URL.Path, "/values") {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/label/"), "/values")
	name = strings.Trim(name, "/")
	if name == "" {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("missing label name"))
		return
	}
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	start, end, err := parseStartEnd(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	limit, err := parseLimit(r.Form.Get("limit"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	matchers, err := s.parseMatcherParams(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	q, err := s.queryable.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "execution", err)
		return
	}
	defer q.Close()
	valuesSet := make(map[string]struct{})
	hints := &storage.LabelHints{Limit: limit}
	for _, matcherSet := range matchers {
		values, _, err := q.LabelValues(ctx, name, hints, matcherSet...)
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		for _, value := range values {
			valuesSet[value] = struct{}{}
		}
	}
	writeAPISuccess(w, sortedLimited(valuesSet, limit))
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	start, end, err := parseStartEnd(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	matcherSets, err := s.parseMatcherParams(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()

	seen := map[uint64]map[string]string{}
	q := &CHQuerier{queryable: s.queryable, mint: start.UnixMilli(), maxt: end.UnixMilli()}
	for _, matchers := range matcherSets {
		series, err := q.selectSeries(ctx, start.UnixMilli(), end.UnixMilli(), matchers...)
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		for _, s := range series {
			seen[stableSeriesID(s.labels)] = s.labels.Map()
		}
	}
	result := make([]map[string]string, 0, len(seen))
	for _, labels := range seen {
		result = append(result, labels)
	}
	writeAPISuccess(w, result)
}

type metadataResult struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	if s.cfg.MetricsTable == "" {
		writeAPISuccess(w, map[string][]metadataResult{})
		return
	}
	limit, err := parseLimit(r.Form.Get("limit"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	metric := r.Form.Get("metric")
	whereParts := []string{teamFilter(s.cfg)}
	if metric != "" {
		whereParts = append(whereParts, "metric_family_name = "+sqlString(metric))
	}
	sql := fmt.Sprintf(
		"SELECT metric_family_name, argMax(type, updated_at) AS type, argMax(unit, updated_at) AS unit, argMax(help, updated_at) AS help FROM %s WHERE %s GROUP BY metric_family_name ORDER BY metric_family_name%s",
		tableName(s.cfg.CHDatabase, s.cfg.MetricsTable),
		strings.Join(whereParts, " AND "),
		sqlLimit(limit),
	)

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	result := map[string][]metadataResult{}
	err = s.client.QueryJSONEachRow(ctx, sql, func(raw json.RawMessage) error {
		var row struct {
			MetricFamilyName string `json:"metric_family_name"`
			Type             string `json:"type"`
			Unit             string `json:"unit"`
			Help             string `json:"help"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		result[row.MetricFamilyName] = []metadataResult{{
			Type: row.Type,
			Help: row.Help,
			Unit: row.Unit,
		}}
		return nil
	})
	if err != nil {
		writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	writeAPISuccess(w, result)
}

type exemplarQueryResult struct {
	SeriesLabels map[string]string   `json:"seriesLabels"`
	Exemplars    []exemplarAPIResult `json:"exemplars"`
}

type exemplarAPIResult struct {
	Labels    map[string]string `json:"labels"`
	Value     string            `json:"value"`
	Timestamp float64           `json:"timestamp"`
}

func (s *Server) handleQueryExemplars(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	query := r.Form.Get("query")
	if query == "" {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("missing query parameter"))
		return
	}
	start, end, err := parseStartEnd(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	if end.Before(start) {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("end timestamp must not be before start time"))
		return
	}
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	selectors := parser.ExtractSelectors(expr)
	if len(selectors) == 0 {
		writeAPISuccess(w, []exemplarQueryResult{})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	q := &CHQuerier{queryable: s.queryable, mint: start.UnixMilli(), maxt: end.UnixMilli()}
	seen := map[uint64]*seriesMeta{}
	for _, selector := range selectors {
		series, err := q.selectSeries(ctx, start.UnixMilli(), end.UnixMilli(), selector...)
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
			return
		}
		for _, s := range series {
			seen[s.id] = s
		}
	}
	series := make([]*seriesMeta, 0, len(seen))
	for _, s := range seen {
		series = append(series, s)
	}
	if err := q.loadExemplars(ctx, series, start.UnixMilli(), end.UnixMilli()); err != nil {
		writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].labels, series[j].labels) < 0
	})

	result := make([]exemplarQueryResult, 0, len(series))
	for _, s := range series {
		if len(s.exemplars) == 0 {
			continue
		}
		exemplars := make([]exemplarAPIResult, 0, len(s.exemplars))
		for _, exemplar := range s.exemplars {
			exemplars = append(exemplars, exemplarAPIResult{
				Labels:    exemplar.labels.Map(),
				Value:     formatSample(exemplar.value),
				Timestamp: float64(exemplar.t) / 1000,
			})
		}
		result = append(result, exemplarQueryResult{
			SeriesLabels: s.labels.Map(),
			Exemplars:    exemplars,
		})
	}
	writeAPISuccess(w, result)
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request) {
	writeAPISuccess(w, map[string]any{"groups": []any{}})
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	writeAPISuccess(w, map[string]any{"alerts": []any{}})
}

func (s *Server) parseMatcherParams(r *http.Request) ([][]*labels.Matcher, error) {
	values := r.Form["match[]"]
	if len(values) == 0 {
		values = r.Form["match"]
	}
	if len(values) == 0 {
		return [][]*labels.Matcher{{}}, nil
	}
	return s.parser.ParseMetricSelectors(values)
}

type apiResponse struct {
	Status    string `json:"status"`
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

type queryData struct {
	ResultType string `json:"resultType"`
	Result     any    `json:"result"`
}

type sampleResult struct {
	Metric     map[string]string `json:"metric"`
	Value      []any             `json:"value,omitempty"`
	Values     [][]any           `json:"values,omitempty"`
	Histogram  any               `json:"histogram,omitempty"`
	Histograms []any             `json:"histograms,omitempty"`
}

func responseDataFromValue(value parser.Value) queryData {
	switch v := value.(type) {
	case promql.Vector:
		result := make([]sampleResult, 0, len(v))
		for _, sample := range v {
			row := sampleResult{Metric: sample.Metric.Map()}
			if sample.H == nil {
				row.Value = []any{float64(sample.T) / 1000, formatSample(sample.F)}
			} else {
				row.Histogram = histogramPointAny(promql.HPoint{T: sample.T, H: sample.H})
			}
			result = append(result, row)
		}
		return queryData{ResultType: string(parser.ValueTypeVector), Result: result}
	case promql.Matrix:
		result := make([]sampleResult, 0, len(v))
		for _, series := range v {
			values := make([][]any, 0, len(series.Floats))
			for _, point := range series.Floats {
				values = append(values, []any{float64(point.T) / 1000, formatSample(point.F)})
			}
			histograms := make([]any, 0, len(series.Histograms))
			for _, point := range series.Histograms {
				histograms = append(histograms, histogramPointAny(point))
			}
			result = append(result, sampleResult{
				Metric:     series.Metric.Map(),
				Values:     values,
				Histograms: histograms,
			})
		}
		return queryData{ResultType: string(parser.ValueTypeMatrix), Result: result}
	case promql.Scalar:
		return queryData{ResultType: string(parser.ValueTypeScalar), Result: []any{float64(v.T) / 1000, formatSample(v.V)}}
	case promql.String:
		return queryData{ResultType: string(parser.ValueTypeString), Result: []any{float64(v.T) / 1000, v.V}}
	default:
		return queryData{ResultType: string(parser.ValueTypeNone), Result: []any{}}
	}
}

func histogramPointAny(point promql.HPoint) any {
	payload, err := json.Marshal(point)
	if err != nil {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil
	}
	return decoded
}

func writeAPISuccess(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: data})
}

func writeAPIError(w http.ResponseWriter, code int, errorType string, err error) {
	recordResponseError(w, errorType, err)
	message := ""
	if err != nil {
		message = err.Error()
	}
	writeJSON(w, code, apiResponse{Status: "error", ErrorType: errorType, Error: message})
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func formatSample(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	default:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
}

func parseAPITime(value string, fallback time.Time) (time.Time, error) {
	if value == "" {
		if fallback.IsZero() {
			return time.Time{}, errors.New("missing timestamp")
		}
		return fallback, nil
	}
	if parsed, err := strconv.ParseFloat(value, 64); err == nil {
		seconds, frac := math.Modf(parsed)
		return time.Unix(int64(seconds), int64(frac*1e9)).UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", value)
}

func parseStartEnd(r *http.Request) (time.Time, time.Time, error) {
	now := time.Now()
	start, err := parseAPITime(r.Form.Get("start"), now.Add(-6*time.Hour))
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseAPITime(r.Form.Get("end"), now)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return start, end, nil
}

func parseStep(value string) (time.Duration, error) {
	if value == "" {
		return 0, errors.New("missing step parameter")
	}
	if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
		return parsed, nil
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second)), nil
	}
	return 0, fmt.Errorf("invalid step %q", value)
}

func parseLimit(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 0 {
		return 0, fmt.Errorf("invalid limit %q", value)
	}
	return limit, nil
}

var _ storage.Queryable = (*CHQueryable)(nil)
