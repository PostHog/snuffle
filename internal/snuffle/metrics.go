package snuffle

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

type bridgeMetrics struct {
	registry *prometheus.Registry

	httpRequests      *prometheus.CounterVec
	httpDuration      *prometheus.HistogramVec
	httpResponseBytes *prometheus.CounterVec
	httpInflight      *prometheus.GaugeVec

	promQLQueries          *prometheus.CounterVec
	promQLQueryDuration    *prometheus.HistogramVec
	promQLResultSeries     *prometheus.CounterVec
	promQLResultSamples    *prometheus.CounterVec
	promQLResultHistograms *prometheus.CounterVec

	remoteWriteRequests        *prometheus.CounterVec
	remoteWriteDuration        *prometheus.HistogramVec
	remoteWriteCompressedBytes prometheus.Counter
	remoteWriteDecodedBytes    prometheus.Counter
	remoteWriteReceivedRows    *prometheus.CounterVec

	remoteReadRequests        *prometheus.CounterVec
	remoteReadDuration        *prometheus.HistogramVec
	remoteReadCompressedBytes prometheus.Counter
	remoteReadDecodedBytes    prometheus.Counter
	remoteReadResponseBytes   prometheus.Counter
	remoteReadReturnedRows    *prometheus.CounterVec

	clickHouseQueries       *prometheus.CounterVec
	clickHouseQueryLatency  *prometheus.HistogramVec
	clickHouseReturnedRows  prometheus.Counter
	clickHouseScannedRows   prometheus.Counter
	clickHouseReadBytes     prometheus.Counter
	clickHouseQueryInflight prometheus.Gauge

	clickHouseInserts        *prometheus.CounterVec
	clickHouseInsertLatency  *prometheus.HistogramVec
	clickHouseInsertedRows   *prometheus.CounterVec
	clickHouseInsertBytes    *prometheus.CounterVec
	clickHouseInsertInflight prometheus.Gauge

	selfScrapes       *prometheus.CounterVec
	selfScrapeLatency *prometheus.HistogramVec
	selfScrapeSeries  prometheus.Gauge
	selfScrapeSamples prometheus.Gauge
	selfScrapeLast    prometheus.Gauge
}

func newBridgeMetrics() *bridgeMetrics {
	registry := prometheus.NewRegistry()
	m := &bridgeMetrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "http_requests_total",
			Help:      "Total HTTP requests handled by the bridge.",
		}, []string{"method", "endpoint", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds.",
			Buckets:   requestDurationBuckets(),
		}, []string{"method", "endpoint", "code"}),
		httpResponseBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "http_response_bytes_total",
			Help:      "Total HTTP response bytes written by the bridge.",
		}, []string{"method", "endpoint", "code"}),
		httpInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "snuffle",
			Name:      "http_requests_in_flight",
			Help:      "HTTP requests currently being handled by the bridge.",
		}, []string{"method", "endpoint"}),

		promQLQueries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "promql_queries_total",
			Help:      "Total PromQL query executions.",
		}, []string{"type", "engine", "status"}),
		promQLQueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "promql_query_duration_seconds",
			Help:      "PromQL query execution duration in seconds.",
			Buckets:   queryDurationBuckets(),
		}, []string{"type", "engine", "status"}),
		promQLResultSeries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "promql_result_series_total",
			Help:      "Total series returned by PromQL queries.",
		}, []string{"type", "engine"}),
		promQLResultSamples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "promql_result_samples_total",
			Help:      "Total float samples returned by PromQL queries.",
		}, []string{"type", "engine"}),
		promQLResultHistograms: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "promql_result_histograms_total",
			Help:      "Total histogram samples returned by PromQL queries.",
		}, []string{"type", "engine"}),

		remoteWriteRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_write_requests_total",
			Help:      "Total remote-write requests handled by the bridge.",
		}, []string{"status"}),
		remoteWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "remote_write_duration_seconds",
			Help:      "Remote-write request duration in seconds.",
			Buckets:   queryDurationBuckets(),
		}, []string{"status"}),
		remoteWriteCompressedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_write_compressed_bytes_total",
			Help:      "Total compressed remote-write request bytes received.",
		}),
		remoteWriteDecodedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_write_decoded_bytes_total",
			Help:      "Total decoded remote-write request bytes received.",
		}),
		remoteWriteReceivedRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_write_received_rows_total",
			Help:      "Total rows received in remote-write batches by row type.",
		}, []string{"row_type"}),

		remoteReadRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_read_requests_total",
			Help:      "Total remote-read requests handled by the bridge.",
		}, []string{"status"}),
		remoteReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "remote_read_duration_seconds",
			Help:      "Remote-read request duration in seconds.",
			Buckets:   queryDurationBuckets(),
		}, []string{"status"}),
		remoteReadCompressedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_read_compressed_bytes_total",
			Help:      "Total compressed remote-read request bytes received.",
		}),
		remoteReadDecodedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_read_decoded_bytes_total",
			Help:      "Total decoded remote-read request bytes received.",
		}),
		remoteReadResponseBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_read_response_bytes_total",
			Help:      "Total compressed remote-read response bytes written.",
		}),
		remoteReadReturnedRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "remote_read_returned_rows_total",
			Help:      "Total rows returned in remote-read responses by row type.",
		}, []string{"row_type"}),

		clickHouseQueries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_queries_total",
			Help:      "Total ClickHouse queries issued by the bridge.",
		}, []string{"status"}),
		clickHouseQueryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_query_duration_seconds",
			Help:      "ClickHouse query duration in seconds.",
			Buckets:   queryDurationBuckets(),
		}, []string{"status"}),
		clickHouseReturnedRows: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_returned_rows_total",
			Help:      "Total ClickHouse result rows returned to the bridge.",
		}),
		clickHouseScannedRows: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_scanned_rows_total",
			Help:      "Total rows ClickHouse reported scanning while serving bridge queries.",
		}),
		clickHouseReadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_read_bytes_total",
			Help:      "Total bytes ClickHouse reported reading while serving bridge queries.",
		}),
		clickHouseQueryInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_queries_in_flight",
			Help:      "ClickHouse queries currently in flight.",
		}),

		clickHouseInserts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_inserts_total",
			Help:      "Total ClickHouse native inserts issued by the bridge.",
		}, []string{"table", "mode", "status"}),
		clickHouseInsertLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_insert_duration_seconds",
			Help:      "ClickHouse native insert duration in seconds.",
			Buckets:   queryDurationBuckets(),
		}, []string{"table", "mode", "status"}),
		clickHouseInsertedRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_inserted_rows_total",
			Help:      "Total rows successfully inserted into ClickHouse by table.",
		}, []string{"table", "mode"}),
		clickHouseInsertBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_inserted_bytes_total",
			Help:      "Total bytes ClickHouse reported written by bridge inserts.",
		}, []string{"table", "mode"}),
		clickHouseInsertInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "snuffle",
			Name:      "clickhouse_inserts_in_flight",
			Help:      "ClickHouse native inserts currently in flight.",
		}),

		selfScrapes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "snuffle",
			Name:      "self_scrapes_total",
			Help:      "Total bridge self-scrapes attempted.",
		}, []string{"status"}),
		selfScrapeLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "snuffle",
			Name:      "self_scrape_duration_seconds",
			Help:      "Bridge self-scrape gather and downstream write duration in seconds.",
			Buckets:   queryDurationBuckets(),
		}, []string{"status"}),
		selfScrapeSeries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "snuffle",
			Name:      "self_scrape_series",
			Help:      "Number of time series produced by the last bridge self-scrape.",
		}),
		selfScrapeSamples: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "snuffle",
			Name:      "self_scrape_samples",
			Help:      "Number of samples produced by the last bridge self-scrape.",
		}),
		selfScrapeLast: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "snuffle",
			Name:      "self_scrape_last_timestamp_seconds",
			Help:      "Unix timestamp of the last successful bridge self-scrape downstream write.",
		}),
	}

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.httpRequests,
		m.httpDuration,
		m.httpResponseBytes,
		m.httpInflight,
		m.promQLQueries,
		m.promQLQueryDuration,
		m.promQLResultSeries,
		m.promQLResultSamples,
		m.promQLResultHistograms,
		m.remoteWriteRequests,
		m.remoteWriteDuration,
		m.remoteWriteCompressedBytes,
		m.remoteWriteDecodedBytes,
		m.remoteWriteReceivedRows,
		m.remoteReadRequests,
		m.remoteReadDuration,
		m.remoteReadCompressedBytes,
		m.remoteReadDecodedBytes,
		m.remoteReadResponseBytes,
		m.remoteReadReturnedRows,
		m.clickHouseQueries,
		m.clickHouseQueryLatency,
		m.clickHouseReturnedRows,
		m.clickHouseScannedRows,
		m.clickHouseReadBytes,
		m.clickHouseQueryInflight,
		m.clickHouseInserts,
		m.clickHouseInsertLatency,
		m.clickHouseInsertedRows,
		m.clickHouseInsertBytes,
		m.clickHouseInsertInflight,
		m.selfScrapes,
		m.selfScrapeLatency,
		m.selfScrapeSeries,
		m.selfScrapeSamples,
		m.selfScrapeLast,
	)
	return m
}

func requestDurationBuckets() []float64 {
	return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
}

func queryDurationBuckets() []float64 {
	return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
}

func (m *bridgeMetrics) handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *bridgeMetrics) observeHTTPRequest(method, endpoint, code string, duration time.Duration, responseBytes int64) {
	if m == nil {
		return
	}
	labels := []string{method, endpoint, code}
	m.httpRequests.WithLabelValues(labels...).Inc()
	m.httpDuration.WithLabelValues(labels...).Observe(duration.Seconds())
	if responseBytes > 0 {
		m.httpResponseBytes.WithLabelValues(labels...).Add(float64(responseBytes))
	}
}

func (m *bridgeMetrics) observePromQLQuery(queryType, engine, status string, duration time.Duration, series, samples, histograms int) {
	if m == nil {
		return
	}
	m.promQLQueries.WithLabelValues(queryType, engine, status).Inc()
	m.promQLQueryDuration.WithLabelValues(queryType, engine, status).Observe(duration.Seconds())
	if status == "ok" {
		m.promQLResultSeries.WithLabelValues(queryType, engine).Add(float64(series))
		m.promQLResultSamples.WithLabelValues(queryType, engine).Add(float64(samples))
		m.promQLResultHistograms.WithLabelValues(queryType, engine).Add(float64(histograms))
	}
}

func (m *bridgeMetrics) observeRemoteWrite(status string, duration time.Duration, compressedBytes, decodedBytes int, batch remoteWriteBatch) {
	if m == nil {
		return
	}
	m.remoteWriteRequests.WithLabelValues(status).Inc()
	m.remoteWriteDuration.WithLabelValues(status).Observe(duration.Seconds())
	if compressedBytes > 0 {
		m.remoteWriteCompressedBytes.Add(float64(compressedBytes))
	}
	if decodedBytes > 0 {
		m.remoteWriteDecodedBytes.Add(float64(decodedBytes))
	}
	m.remoteWriteReceivedRows.WithLabelValues("series").Add(float64(batch.seriesCount))
	m.remoteWriteReceivedRows.WithLabelValues("sample").Add(float64(batch.sampleCount))
	m.remoteWriteReceivedRows.WithLabelValues("histogram").Add(float64(batch.histogramCount))
	m.remoteWriteReceivedRows.WithLabelValues("exemplar").Add(float64(batch.exemplarCount))
	m.remoteWriteReceivedRows.WithLabelValues("metadata").Add(float64(batch.metadataCount))
}

func (m *bridgeMetrics) observeRemoteRead(status string, duration time.Duration, compressedBytes, decodedBytes, responseBytes int, queries int, rows remoteReadRows) {
	if m == nil {
		return
	}
	m.remoteReadRequests.WithLabelValues(status).Inc()
	m.remoteReadDuration.WithLabelValues(status).Observe(duration.Seconds())
	if compressedBytes > 0 {
		m.remoteReadCompressedBytes.Add(float64(compressedBytes))
	}
	if decodedBytes > 0 {
		m.remoteReadDecodedBytes.Add(float64(decodedBytes))
	}
	if responseBytes > 0 {
		m.remoteReadResponseBytes.Add(float64(responseBytes))
	}
	m.remoteReadReturnedRows.WithLabelValues("query").Add(float64(queries))
	m.remoteReadReturnedRows.WithLabelValues("series").Add(float64(rows.series))
	m.remoteReadReturnedRows.WithLabelValues("sample").Add(float64(rows.samples))
	m.remoteReadReturnedRows.WithLabelValues("histogram").Add(float64(rows.histograms))
	m.remoteReadReturnedRows.WithLabelValues("exemplar").Add(float64(rows.exemplars))
}

func (m *bridgeMetrics) observeClickHouseQuery(status string, duration time.Duration, returnedRows, scannedRows, readBytes int64) {
	if m == nil {
		return
	}
	m.clickHouseQueries.WithLabelValues(status).Inc()
	m.clickHouseQueryLatency.WithLabelValues(status).Observe(duration.Seconds())
	if returnedRows > 0 {
		m.clickHouseReturnedRows.Add(float64(returnedRows))
	}
	if scannedRows > 0 {
		m.clickHouseScannedRows.Add(float64(scannedRows))
	}
	if readBytes > 0 {
		m.clickHouseReadBytes.Add(float64(readBytes))
	}
}

func (m *bridgeMetrics) observeClickHouseInsert(table, mode, status string, duration time.Duration, rows int, bytes uint64) {
	if m == nil {
		return
	}
	m.clickHouseInserts.WithLabelValues(table, mode, status).Inc()
	m.clickHouseInsertLatency.WithLabelValues(table, mode, status).Observe(duration.Seconds())
	if status == "ok" && rows > 0 {
		m.clickHouseInsertedRows.WithLabelValues(table, mode).Add(float64(rows))
	}
	if status == "ok" && bytes > 0 {
		m.clickHouseInsertBytes.WithLabelValues(table, mode).Add(float64(bytes))
	}
}

func valueStats(value parser.Value) (series, samples, histograms int) {
	switch v := value.(type) {
	case promql.Vector:
		series = len(v)
		for _, sample := range v {
			if sample.H == nil {
				samples++
			} else {
				histograms++
			}
		}
	case promql.Matrix:
		series = len(v)
		for _, s := range v {
			samples += len(s.Floats)
			histograms += len(s.Histograms)
		}
	case promql.Scalar, promql.String:
		samples = 1
	}
	return series, samples, histograms
}

func queryDataStats(data queryData) (series, samples, histograms int) {
	switch result := data.Result.(type) {
	case []sampleResult:
		series = len(result)
		for _, row := range result {
			if len(row.Value) > 0 {
				samples++
			}
			samples += len(row.Values)
			if row.Histogram != nil {
				histograms++
			}
			histograms += len(row.Histograms)
		}
	case []any:
		if len(result) > 0 {
			samples = 1
		}
	}
	return series, samples, histograms
}

type remoteReadRows struct {
	series     int
	samples    int
	histograms int
	exemplars  int
}

func countRemoteReadRows(resp *prompb.ReadResponse) remoteReadRows {
	var rows remoteReadRows
	if resp == nil {
		return rows
	}
	for _, result := range resp.GetResults() {
		for _, ts := range result.GetTimeseries() {
			rows.series++
			rows.samples += len(ts.GetSamples())
			rows.histograms += len(ts.GetHistograms())
			rows.exemplars += len(ts.GetExemplars())
		}
	}
	return rows
}

func (s *Server) instrumentHandler(endpoint string, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrapped := &loggingResponseWriter{ResponseWriter: w}
		method := r.Method
		started := time.Now()
		if s.metrics != nil {
			s.metrics.httpInflight.WithLabelValues(method, endpoint).Inc()
			defer s.metrics.httpInflight.WithLabelValues(method, endpoint).Dec()
		}
		defer func() {
			if s.metrics != nil {
				s.metrics.observeHTTPRequest(method, endpoint, strconv.Itoa(wrapped.statusCode()), time.Since(started), wrapped.bytes)
			}
		}()
		handler.ServeHTTP(wrapped, r)
	})
}

func normalizedEndpoint(path string) string {
	if strings.HasPrefix(path, "/api/v1/label/") && strings.HasSuffix(path, "/values") {
		return "/api/v1/label/:name/values"
	}
	return path
}

func (s *Server) startSelfScraper(ctx context.Context) {
	if s.metrics == nil || !s.cfg.SelfScrapeEnabled || s.cfg.SelfScrapeInterval <= 0 {
		return
	}
	go s.selfScrapeLoop(ctx)
}

func (s *Server) selfScrapeLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.scrapeAndWriteSelfMetrics(ctx)
			timer.Reset(s.cfg.SelfScrapeInterval)
		}
	}
}

func (s *Server) scrapeAndWriteSelfMetrics(parent context.Context) {
	started := time.Now()
	status := "ok"
	seriesCount := 0
	sampleCount := 0
	defer func() {
		if s.metrics == nil {
			return
		}
		s.metrics.selfScrapes.WithLabelValues(status).Inc()
		s.metrics.selfScrapeLatency.WithLabelValues(status).Observe(time.Since(started).Seconds())
		s.metrics.selfScrapeSeries.Set(float64(seriesCount))
		s.metrics.selfScrapeSamples.Set(float64(sampleCount))
		if status == "ok" {
			s.metrics.selfScrapeLast.Set(float64(time.Now().Unix()))
		}
	}()

	ctx, cancel := context.WithTimeout(parent, s.cfg.CHTimeout)
	defer cancel()

	req, samples, err := s.selfScrapeWriteRequest(time.Now())
	if err != nil {
		status = "error"
		slog.Warn("self scrape gather failed", "error", err)
		return
	}
	seriesCount = len(req.GetTimeseries())
	sampleCount = samples
	if seriesCount == 0 {
		return
	}
	batch, err := buildRemoteWriteBatch(req, 0, s.cfg.SelfScrapeTeamID, s.cfg.SampleAttributes || s.cfg.postHogSchemaLayout())
	if err != nil {
		status = "error"
		slog.Warn("self scrape encode failed", "error", err)
		return
	}
	if err := s.withTeamID(s.cfg.SelfScrapeTeamID).insertRemoteWriteBatch(ctx, batch); err != nil {
		status = "error"
		slog.Warn("self scrape downstream write failed", "error", err)
		return
	}
}

func (s *Server) selfScrapeWriteRequest(now time.Time) (*prompb.WriteRequest, int, error) {
	if s.metrics == nil || s.metrics.registry == nil {
		return nil, 0, errors.New("metrics registry is not configured")
	}
	families, err := s.metrics.registry.Gather()
	if err != nil {
		return nil, 0, err
	}
	externalLabels := map[string]string{
		"job":      s.cfg.SelfScrapeJob,
		"instance": s.cfg.SelfScrapeInstance,
	}
	req := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(families)),
		Metadata:   make([]prompb.MetricMetadata, 0, len(families)),
	}
	timestampMS := now.UnixMilli()
	sampleCount := 0
	for _, family := range families {
		name := family.GetName()
		if name == "" {
			continue
		}
		req.Metadata = append(req.Metadata, prompb.MetricMetadata{
			MetricFamilyName: name,
			Type:             remoteWriteMetadataType(family.GetType()),
			Help:             family.GetHelp(),
		})
		for _, metric := range family.GetMetric() {
			added := appendMetricFamilySamples(req, family, metric, timestampMS, externalLabels)
			sampleCount += added
		}
	}
	return req, sampleCount, nil
}

func appendMetricFamilySamples(req *prompb.WriteRequest, family *dto.MetricFamily, metric *dto.Metric, timestampMS int64, externalLabels map[string]string) int {
	name := family.GetName()
	switch family.GetType() {
	case dto.MetricType_COUNTER:
		req.Timeseries = append(req.Timeseries, floatTimeSeries(name, metric, nil, metric.GetCounter().GetValue(), timestampMS, externalLabels))
		return 1
	case dto.MetricType_GAUGE:
		req.Timeseries = append(req.Timeseries, floatTimeSeries(name, metric, nil, metric.GetGauge().GetValue(), timestampMS, externalLabels))
		return 1
	case dto.MetricType_UNTYPED:
		req.Timeseries = append(req.Timeseries, floatTimeSeries(name, metric, nil, metric.GetUntyped().GetValue(), timestampMS, externalLabels))
		return 1
	case dto.MetricType_HISTOGRAM:
		histogram := metric.GetHistogram()
		count := 0
		for _, bucket := range histogram.GetBucket() {
			req.Timeseries = append(req.Timeseries, floatTimeSeries(name+"_bucket", metric, map[string]string{"le": prometheusBoundLabel(bucket.GetUpperBound())}, float64(bucket.GetCumulativeCount()), timestampMS, externalLabels))
			count++
		}
		req.Timeseries = append(req.Timeseries, floatTimeSeries(name+"_bucket", metric, map[string]string{"le": "+Inf"}, float64(histogram.GetSampleCount()), timestampMS, externalLabels))
		count++
		req.Timeseries = append(req.Timeseries,
			floatTimeSeries(name+"_sum", metric, nil, histogram.GetSampleSum(), timestampMS, externalLabels),
			floatTimeSeries(name+"_count", metric, nil, float64(histogram.GetSampleCount()), timestampMS, externalLabels),
		)
		return count + 2
	case dto.MetricType_SUMMARY:
		summary := metric.GetSummary()
		count := 0
		for _, quantile := range summary.GetQuantile() {
			req.Timeseries = append(req.Timeseries, floatTimeSeries(name, metric, map[string]string{"quantile": prometheusBoundLabel(quantile.GetQuantile())}, quantile.GetValue(), timestampMS, externalLabels))
			count++
		}
		req.Timeseries = append(req.Timeseries,
			floatTimeSeries(name+"_sum", metric, nil, summary.GetSampleSum(), timestampMS, externalLabels),
			floatTimeSeries(name+"_count", metric, nil, float64(summary.GetSampleCount()), timestampMS, externalLabels),
		)
		return count + 2
	default:
		return 0
	}
}

func floatTimeSeries(metricName string, metric *dto.Metric, extraLabels map[string]string, value float64, timestampMS int64, externalLabels map[string]string) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels:  metricLabels(metricName, metric, extraLabels, externalLabels),
		Samples: []prompb.Sample{{Value: value, Timestamp: timestampMS}},
	}
}

func metricLabels(metricName string, metric *dto.Metric, extraLabels map[string]string, externalLabels map[string]string) []prompb.Label {
	builder := newPromLabelBuilder()
	builder.add("__name__", metricName)
	for _, label := range metric.GetLabel() {
		name := label.GetName()
		if _, ok := externalLabels[name]; ok {
			name = "exported_" + name
		}
		builder.add(name, label.GetValue())
	}
	for name, value := range extraLabels {
		builder.add(name, value)
	}
	for name, value := range externalLabels {
		if value != "" {
			builder.add(name, value)
		}
	}
	return builder.sorted()
}

type promLabelBuilder struct {
	seen   map[string]struct{}
	labels []prompb.Label
}

func newPromLabelBuilder() *promLabelBuilder {
	return &promLabelBuilder{seen: make(map[string]struct{}, 4)}
}

func (b *promLabelBuilder) add(name, value string) {
	if name == "" {
		return
	}
	for {
		if _, exists := b.seen[name]; !exists {
			break
		}
		name = "exported_" + name
	}
	b.seen[name] = struct{}{}
	b.labels = append(b.labels, prompb.Label{Name: name, Value: value})
}

func (b *promLabelBuilder) sorted() []prompb.Label {
	sort.Slice(b.labels, func(i, j int) bool {
		return b.labels[i].Name < b.labels[j].Name
	})
	return b.labels
}

func prometheusBoundLabel(value float64) string {
	if math.IsInf(value, 1) {
		return "+Inf"
	}
	if math.IsInf(value, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func remoteWriteMetadataType(input dto.MetricType) prompb.MetricMetadata_MetricType {
	switch input {
	case dto.MetricType_COUNTER:
		return prompb.MetricMetadata_COUNTER
	case dto.MetricType_GAUGE:
		return prompb.MetricMetadata_GAUGE
	case dto.MetricType_HISTOGRAM:
		return prompb.MetricMetadata_HISTOGRAM
	case dto.MetricType_SUMMARY:
		return prompb.MetricMetadata_SUMMARY
	default:
		return prompb.MetricMetadata_UNKNOWN
	}
}
