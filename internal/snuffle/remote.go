package snuffle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/cespare/xxhash/v2"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func (s *Server) handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req prompb.WriteRequest
	if err := readSnappyProto(r, &req, "remote write"); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	batch, err := buildRemoteWriteBatch(&req)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.CHTimeout)
	defer cancel()
	if err := s.insertRemoteWriteBatch(ctx, batch); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "execution", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRemoteRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req prompb.ReadRequest
	if err := readSnappyProto(r, &req, "remote read"); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	if !remoteReadAcceptsSamples(&req) {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("remote read streamed chunk responses are not supported"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	resp, err := s.remoteReadSamples(ctx, &req)
	if err != nil {
		writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	payload, err := resp.Marshal()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "execution", err)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	_, _ = w.Write(snappy.Encode(nil, payload))
}

func readSnappyProto(r *http.Request, dst interface{ Unmarshal([]byte) error }, name string) error {
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	decoded, err := snappy.Decode(nil, compressed)
	if err != nil {
		return fmt.Errorf("decode snappy %s request: %w", name, err)
	}
	if err := dst.Unmarshal(decoded); err != nil {
		return fmt.Errorf("decode %s protobuf: %w", name, err)
	}
	return nil
}

type remoteWriteBatch struct {
	seriesRows     bytes.Buffer
	labelIndexRows bytes.Buffer
	sampleRows     bytes.Buffer
	histogramRows  bytes.Buffer
	exemplarRows   bytes.Buffer
	metadataRows   bytes.Buffer
	seriesCount    int
	labelCount     int
	sampleCount    int
	histogramCount int
	exemplarCount  int
	metadataCount  int
}

type remoteWriteSeriesRow struct {
	ID         uint64 `json:"id"`
	MetricName string `json:"metric_name"`
	LabelsJSON string `json:"labels_json"`
	MinMS      int64  `json:"min_ms"`
	MaxMS      int64  `json:"max_ms"`
}

type remoteWriteLabelIndexRow struct {
	MetricName string `json:"metric_name"`
	LabelName  string `json:"label_name"`
	LabelValue string `json:"label_value"`
	ID         uint64 `json:"id"`
}

type remoteWriteSampleRow struct {
	TimestampMS int64   `json:"timestamp_ms"`
	ID          uint64  `json:"id"`
	Value       float64 `json:"value"`
}

type remoteWriteHistogramRow struct {
	TimestampMS  int64  `json:"timestamp_ms"`
	ID           uint64 `json:"id"`
	HistogramB64 string `json:"histogram_b64"`
}

type remoteWriteExemplarRow struct {
	TimestampMS int64   `json:"timestamp_ms"`
	ID          uint64  `json:"id"`
	Value       float64 `json:"value"`
	LabelsJSON  string  `json:"labels_json"`
}

type remoteWriteMetadataRow struct {
	MetricFamilyName string `json:"metric_family_name"`
	Type             string `json:"type"`
	Unit             string `json:"unit"`
	Help             string `json:"help"`
}

func buildRemoteWriteBatch(req *prompb.WriteRequest) (remoteWriteBatch, error) {
	var batch remoteWriteBatch
	seriesByID := make(map[uint64]remoteWriteSeriesRow, len(req.GetTimeseries()))
	labelRows := make(map[remoteWriteLabelIndexRow]struct{})
	sampleEncoder := json.NewEncoder(&batch.sampleRows)
	histogramEncoder := json.NewEncoder(&batch.histogramRows)
	exemplarEncoder := json.NewEncoder(&batch.exemplarRows)
	metadataEncoder := json.NewEncoder(&batch.metadataRows)

	for _, metadata := range req.GetMetadata() {
		if metadata.GetMetricFamilyName() == "" {
			continue
		}
		if err := metadataEncoder.Encode(remoteWriteMetadataRow{
			MetricFamilyName: metadata.GetMetricFamilyName(),
			Type:             remoteMetadataType(metadata.GetType()),
			Unit:             metadata.GetUnit(),
			Help:             metadata.GetHelp(),
		}); err != nil {
			return remoteWriteBatch{}, err
		}
		batch.metadataCount++
	}

	for _, ts := range req.GetTimeseries() {
		if len(ts.GetSamples()) == 0 && len(ts.GetHistograms()) == 0 && len(ts.GetExemplars()) == 0 {
			continue
		}

		labelMap, metricName, err := remoteWriteLabelMap(ts.GetLabels())
		if err != nil {
			return remoteWriteBatch{}, err
		}
		lbls := labels.FromMap(labelMap)
		id := stableSeriesID(lbls)

		outputLabels := make(map[string]string, len(labelMap)-1)
		for k, v := range labelMap {
			if k == labels.MetricName {
				continue
			}
			outputLabels[k] = v
			labelRows[remoteWriteLabelIndexRow{
				MetricName: metricName,
				LabelName:  k,
				LabelValue: v,
				ID:         id,
			}] = struct{}{}
		}
		labelsJSON, err := json.Marshal(outputLabels)
		if err != nil {
			return remoteWriteBatch{}, err
		}

		var minMS int64
		var maxMS int64
		var haveTime bool
		observeTime := func(timestamp int64) {
			if !haveTime {
				minMS = timestamp
				maxMS = timestamp
				haveTime = true
				return
			}
			if timestamp < minMS {
				minMS = timestamp
			}
			if timestamp > maxMS {
				maxMS = timestamp
			}
		}

		for _, sample := range ts.GetSamples() {
			observeTime(sample.Timestamp)
			if err := sampleEncoder.Encode(remoteWriteSampleRow{
				TimestampMS: sample.Timestamp,
				ID:          id,
				Value:       sample.Value,
			}); err != nil {
				return remoteWriteBatch{}, err
			}
			batch.sampleCount++
		}
		for _, histogram := range ts.GetHistograms() {
			observeTime(histogram.Timestamp)
			payload, err := histogram.Marshal()
			if err != nil {
				return remoteWriteBatch{}, err
			}
			if err := histogramEncoder.Encode(remoteWriteHistogramRow{
				TimestampMS:  histogram.Timestamp,
				ID:           id,
				HistogramB64: base64.StdEncoding.EncodeToString(payload),
			}); err != nil {
				return remoteWriteBatch{}, err
			}
			batch.histogramCount++
		}
		for _, exemplar := range ts.GetExemplars() {
			observeTime(exemplar.Timestamp)
			exemplarLabels, err := remoteWriteAuxLabelMap(exemplar.GetLabels())
			if err != nil {
				return remoteWriteBatch{}, err
			}
			labelsJSON, err := json.Marshal(exemplarLabels)
			if err != nil {
				return remoteWriteBatch{}, err
			}
			if err := exemplarEncoder.Encode(remoteWriteExemplarRow{
				TimestampMS: exemplar.Timestamp,
				ID:          id,
				Value:       exemplar.Value,
				LabelsJSON:  string(labelsJSON),
			}); err != nil {
				return remoteWriteBatch{}, err
			}
			batch.exemplarCount++
		}

		row := remoteWriteSeriesRow{
			ID:         id,
			MetricName: metricName,
			LabelsJSON: string(labelsJSON),
			MinMS:      minMS,
			MaxMS:      maxMS,
		}
		if existing, ok := seriesByID[id]; ok {
			if row.MinMS > existing.MinMS {
				row.MinMS = existing.MinMS
			}
			if row.MaxMS < existing.MaxMS {
				row.MaxMS = existing.MaxMS
			}
		}
		seriesByID[id] = row
	}

	seriesEncoder := json.NewEncoder(&batch.seriesRows)
	for _, row := range seriesByID {
		if err := seriesEncoder.Encode(row); err != nil {
			return remoteWriteBatch{}, err
		}
		batch.seriesCount++
	}
	labelEncoder := json.NewEncoder(&batch.labelIndexRows)
	for row := range labelRows {
		if err := labelEncoder.Encode(row); err != nil {
			return remoteWriteBatch{}, err
		}
		batch.labelCount++
	}
	return batch, nil
}

func remoteMetadataType(input prompb.MetricMetadata_MetricType) string {
	switch input {
	case prompb.MetricMetadata_COUNTER:
		return "counter"
	case prompb.MetricMetadata_GAUGE:
		return "gauge"
	case prompb.MetricMetadata_HISTOGRAM:
		return "histogram"
	case prompb.MetricMetadata_GAUGEHISTOGRAM:
		return "gaugehistogram"
	case prompb.MetricMetadata_SUMMARY:
		return "summary"
	case prompb.MetricMetadata_INFO:
		return "info"
	case prompb.MetricMetadata_STATESET:
		return "stateset"
	default:
		return "unknown"
	}
}

func stableSeriesID(lbls labels.Labels) uint64 {
	hash := xxhash.New()
	var length [8]byte
	lbls.Range(func(label labels.Label) {
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Name)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Name)
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Value)
	})
	return hash.Sum64()
}

func remoteWriteLabelMap(input []prompb.Label) (map[string]string, string, error) {
	if len(input) == 0 {
		return nil, "", errors.New("remote write time series has no labels")
	}
	labelMap, err := prompbLabelMap(input, "remote write label")
	if err != nil {
		return nil, "", err
	}
	metricName := labelMap[labels.MetricName]
	if metricName == "" {
		return nil, "", errors.New("remote write time series is missing __name__")
	}
	return labelMap, metricName, nil
}

func remoteWriteAuxLabelMap(input []prompb.Label) (map[string]string, error) {
	return prompbLabelMap(input, "remote write auxiliary label")
}

func prompbLabelMap(input []prompb.Label, kind string) (map[string]string, error) {
	labelMap := make(map[string]string, len(input))
	for _, label := range input {
		if label.Name == "" {
			return nil, errors.New(kind + " name must not be empty")
		}
		if _, exists := labelMap[label.Name]; exists {
			return nil, fmt.Errorf("duplicate %s %q", kind, label.Name)
		}
		labelMap[label.Name] = label.Value
	}
	return labelMap, nil
}

func (s *Server) insertRemoteWriteBatch(ctx context.Context, batch remoteWriteBatch) error {
	if batch.seriesCount == 0 && batch.sampleCount == 0 && batch.histogramCount == 0 && batch.exemplarCount == 0 && batch.metadataCount == 0 {
		return nil
	}
	for _, insert := range []remoteWriteInsert{
		{batch.metadataCount, s.cfg.MetricsTable, "metric_family_name, type, unit, help, now64(3, 'UTC')", "metric_family_name String, type String, unit String, help String", &batch.metadataRows, true},
		{batch.seriesCount, s.cfg.SeriesTable, "id, metric_name, labels_json, fromUnixTimestamp64Milli(min_ms, 'UTC'), fromUnixTimestamp64Milli(max_ms, 'UTC')", "id UInt64, metric_name String, labels_json String, min_ms Int64, max_ms Int64", &batch.seriesRows, false},
		{batch.labelCount, s.cfg.LabelIndexTable, "metric_name, label_name, label_value, id", "metric_name String, label_name String, label_value String, id UInt64", &batch.labelIndexRows, false},
		{batch.sampleCount, s.cfg.SamplesTable, "fromUnixTimestamp64Milli(timestamp_ms, 'UTC'), id, value", "timestamp_ms Int64, id UInt64, value Float64", &batch.sampleRows, false},
		{batch.histogramCount, s.cfg.HistogramsTable, "fromUnixTimestamp64Milli(timestamp_ms, 'UTC'), id, base64Decode(histogram_b64)", "timestamp_ms Int64, id UInt64, histogram_b64 String", &batch.histogramRows, true},
		{batch.exemplarCount, s.cfg.ExemplarsTable, "fromUnixTimestamp64Milli(timestamp_ms, 'UTC'), id, value, labels_json", "timestamp_ms Int64, id UInt64, value Float64, labels_json String", &batch.exemplarRows, true},
	} {
		if err := s.insertRemoteRows(ctx, insert); err != nil {
			return err
		}
	}
	return nil
}

type remoteWriteInsert struct {
	count      int
	table      string
	selectExpr string
	input      string
	rows       *bytes.Buffer
	optional   bool
}

func (s *Server) insertRemoteRows(ctx context.Context, insert remoteWriteInsert) error {
	if insert.count == 0 || insert.optional && insert.table == "" {
		return nil
	}
	sql := fmt.Sprintf(
		"INSERT INTO %s SELECT %s FROM input(%s) FORMAT JSONEachRow",
		tableName(s.cfg.CHDatabase, insert.table),
		insert.selectExpr,
		sqlString(insert.input),
	)
	return s.client.ExecWithBody(ctx, sql, bytes.NewReader(insert.rows.Bytes()))
}

func remoteReadAcceptsSamples(req *prompb.ReadRequest) bool {
	accepted := req.GetAcceptedResponseTypes()
	if len(accepted) == 0 {
		return true
	}
	for _, responseType := range accepted {
		if responseType == prompb.ReadRequest_SAMPLES {
			return true
		}
	}
	return false
}

func (s *Server) remoteReadSamples(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resp := &prompb.ReadResponse{Results: make([]*prompb.QueryResult, 0, len(req.GetQueries()))}
	for _, query := range req.GetQueries() {
		matchers, err := remoteReadMatchers(query.GetMatchers())
		if err != nil {
			return nil, err
		}
		q := &CHQuerier{queryable: s.queryable, mint: query.GetStartTimestampMs(), maxt: query.GetEndTimestampMs()}
		series, err := q.selectSeries(ctx, query.GetStartTimestampMs(), query.GetEndTimestampMs(), matchers...)
		if err != nil {
			return nil, err
		}
		if err := q.loadSamples(ctx, series, query.GetStartTimestampMs(), query.GetEndTimestampMs(), false, matchers); err != nil {
			return nil, err
		}
		if err := q.loadHistograms(ctx, series, query.GetStartTimestampMs(), query.GetEndTimestampMs(), matchers); err != nil {
			return nil, err
		}
		if err := q.loadExemplars(ctx, series, query.GetStartTimestampMs(), query.GetEndTimestampMs()); err != nil {
			return nil, err
		}

		result := &prompb.QueryResult{Timeseries: make([]*prompb.TimeSeries, 0, len(series))}
		for _, seriesMeta := range series {
			if len(seriesMeta.samples) == 0 && len(seriesMeta.histograms) == 0 && len(seriesMeta.exemplars) == 0 {
				continue
			}
			timeSeries := &prompb.TimeSeries{
				Labels:     labelsToPrompb(seriesMeta.labels),
				Samples:    make([]prompb.Sample, 0, len(seriesMeta.samples)),
				Histograms: make([]prompb.Histogram, 0, len(seriesMeta.histograms)),
				Exemplars:  make([]prompb.Exemplar, 0, len(seriesMeta.exemplars)),
			}
			for _, sample := range seriesMeta.samples {
				timeSeries.Samples = append(timeSeries.Samples, prompb.Sample{
					Timestamp: sample.t,
					Value:     sample.v,
				})
			}
			for _, histogram := range seriesMeta.histograms {
				timeSeries.Histograms = append(timeSeries.Histograms, histogram.h)
			}
			for _, exemplar := range seriesMeta.exemplars {
				timeSeries.Exemplars = append(timeSeries.Exemplars, exemplar.pb)
			}
			result.Timeseries = append(result.Timeseries, timeSeries)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp, nil
}

func remoteReadMatchers(input []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(input))
	for _, matcher := range input {
		matchType, err := remoteReadMatcherType(matcher.GetType())
		if err != nil {
			return nil, err
		}
		converted, err := labels.NewMatcher(matchType, matcher.GetName(), matcher.GetValue())
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, nil
}

func remoteReadMatcherType(input prompb.LabelMatcher_Type) (labels.MatchType, error) {
	switch input {
	case prompb.LabelMatcher_EQ:
		return labels.MatchEqual, nil
	case prompb.LabelMatcher_NEQ:
		return labels.MatchNotEqual, nil
	case prompb.LabelMatcher_RE:
		return labels.MatchRegexp, nil
	case prompb.LabelMatcher_NRE:
		return labels.MatchNotRegexp, nil
	default:
		return labels.MatchEqual, fmt.Errorf("unsupported remote read matcher type %s", input.String())
	}
}

func labelsToPrompb(input labels.Labels) []prompb.Label {
	output := make([]prompb.Label, 0, input.Len())
	input.Range(func(label labels.Label) {
		output = append(output, prompb.Label{Name: label.Name, Value: label.Value})
	})
	return output
}
