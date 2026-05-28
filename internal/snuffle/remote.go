package snuffle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func (s *Server) handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeHTTPError(w, http.StatusMethodNotAllowed, "bad_method", errors.New("method not allowed"))
		return
	}

	var req prompb.WriteRequest
	if err := readSnappyProto(r, &req, "remote write"); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	batch, err := buildRemoteWriteBatch(&req, s.cfg.RemoteWriteInterval, s.cfg.TeamID)
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
		writeHTTPError(w, http.StatusMethodNotAllowed, "bad_method", errors.New("method not allowed"))
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
	sampleRows     []remoteWriteSampleRow
	histogramRows  []remoteWriteHistogramRow
	exemplarRows   []remoteWriteExemplarRow
	metadataRows   []remoteWriteMetadataRow
	seriesRecords  []remoteWriteSeriesRow
	seriesCount    int
	sampleCount    int
	histogramCount int
	exemplarCount  int
	metadataCount  int
}

type remoteWriteSeriesRow struct {
	TeamID     uint64
	ID         uint64
	MetricName string
	LabelsJSON string
	Labels     []prompb.Label
	MinMS      int64
	MaxMS      int64
}

type remoteWriteSampleRow struct {
	TeamID      uint64
	MetricName  string
	TimestampMS int64
	ID          uint64
	Value       float64
	Version     uint64
}

type remoteWriteHistogramRow struct {
	TeamID      uint64
	MetricName  string
	TimestampMS int64
	ID          uint64
	Histogram   []byte
	Version     uint64
}

type remoteWriteExemplarRow struct {
	TeamID      uint64
	TimestampMS int64
	ID          uint64
	Value       float64
	LabelsJSON  string
}

type remoteWriteMetadataRow struct {
	TeamID           uint64
	MetricFamilyName string
	Type             string
	Unit             string
	Help             string
	UpdatedAtMS      int64
}

func buildRemoteWriteBatch(req *prompb.WriteRequest, sampleInterval time.Duration, teamID uint64) (remoteWriteBatch, error) {
	var batch remoteWriteBatch
	seriesByID := make(map[uint64]remoteWriteSeriesRow, len(req.GetTimeseries()))
	updatedAtMS := time.Now().UnixMilli()

	for _, metadata := range req.GetMetadata() {
		if metadata.GetMetricFamilyName() == "" {
			continue
		}
		batch.metadataRows = append(batch.metadataRows, remoteWriteMetadataRow{
			TeamID:           teamID,
			MetricFamilyName: metadata.GetMetricFamilyName(),
			Type:             remoteMetadataType(metadata.GetType()),
			Unit:             metadata.GetUnit(),
			Help:             metadata.GetHelp(),
			UpdatedAtMS:      updatedAtMS,
		})
	}

	for _, ts := range req.GetTimeseries() {
		if len(ts.GetSamples()) == 0 && len(ts.GetHistograms()) == 0 && len(ts.GetExemplars()) == 0 {
			continue
		}

		id, metricName, err := remoteWriteSeriesIdentity(ts.GetLabels())
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
			if math.IsNaN(sample.Value) {
				continue
			}
			bucketMS := bucketTimestampMS(sample.Timestamp, sampleInterval)
			observeTime(bucketMS)
			batch.sampleRows = append(batch.sampleRows, remoteWriteSampleRow{
				TeamID:      teamID,
				MetricName:  metricName,
				TimestampMS: bucketMS,
				ID:          id,
				Value:       sample.Value,
				Version:     remoteWriteVersion(sample.Timestamp),
			})
		}
		for _, histogram := range ts.GetHistograms() {
			originalTimestamp := histogram.Timestamp
			bucketMS := bucketTimestampMS(originalTimestamp, sampleInterval)
			observeTime(bucketMS)
			histogram.Timestamp = bucketMS
			payload, err := histogram.Marshal()
			if err != nil {
				return remoteWriteBatch{}, err
			}
			batch.histogramRows = append(batch.histogramRows, remoteWriteHistogramRow{
				TeamID:      teamID,
				MetricName:  metricName,
				TimestampMS: bucketMS,
				ID:          id,
				Histogram:   payload,
				Version:     remoteWriteVersion(originalTimestamp),
			})
		}
		for _, exemplar := range ts.GetExemplars() {
			if math.IsNaN(exemplar.Value) {
				continue
			}
			observeTime(exemplar.Timestamp)
			exemplarLabels, err := remoteWriteAuxLabelMap(exemplar.GetLabels())
			if err != nil {
				return remoteWriteBatch{}, err
			}
			labelsJSON, err := json.Marshal(exemplarLabels)
			if err != nil {
				return remoteWriteBatch{}, err
			}
			batch.exemplarRows = append(batch.exemplarRows, remoteWriteExemplarRow{
				TeamID:      teamID,
				TimestampMS: exemplar.Timestamp,
				ID:          id,
				Value:       exemplar.Value,
				LabelsJSON:  string(labelsJSON),
			})
		}
		if !haveTime {
			continue
		}
		row := remoteWriteSeriesRow{
			TeamID:     teamID,
			ID:         id,
			MetricName: metricName,
			Labels:     ts.GetLabels(),
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

	for _, row := range seriesByID {
		batch.seriesRecords = append(batch.seriesRecords, row)
	}
	sort.Slice(batch.seriesRecords, func(i, j int) bool { return batch.seriesRecords[i].ID < batch.seriesRecords[j].ID })
	batch.seriesCount = len(batch.seriesRecords)
	batch.sampleCount = len(batch.sampleRows)
	batch.histogramCount = len(batch.histogramRows)
	batch.exemplarCount = len(batch.exemplarRows)
	batch.metadataCount = len(batch.metadataRows)
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

func remoteWriteSeriesIdentity(input []prompb.Label) (uint64, string, error) {
	if len(input) == 0 {
		return 0, "", errors.New("remote write time series has no labels")
	}
	lbls := input
	if !prompbLabelsSorted(input) {
		lbls = append([]prompb.Label(nil), input...)
		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].Name < lbls[j].Name
		})
	}

	hash := xxhash.New()
	var length [8]byte
	var metricName string
	var previous string
	for i, label := range lbls {
		if label.Name == "" {
			return 0, "", errors.New("remote write label name must not be empty")
		}
		if i > 0 && label.Name == previous {
			return 0, "", fmt.Errorf("duplicate remote write label %q", label.Name)
		}
		previous = label.Name
		if label.Name == labels.MetricName {
			metricName = label.Value
		}
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Name)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Name)
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Value)
	}
	if metricName == "" {
		return 0, "", errors.New("remote write time series is missing __name__")
	}
	return hash.Sum64(), metricName, nil
}

func prompbLabelsSorted(input []prompb.Label) bool {
	for i := 1; i < len(input); i++ {
		if input[i-1].Name > input[i].Name {
			return false
		}
	}
	return true
}

func labelsJSONFromPrompb(input []prompb.Label) (string, error) {
	if len(input) == 0 {
		return "{}", nil
	}
	buf := make([]byte, 0, len(input)*32)
	buf = append(buf, '{')
	first := true
	for _, label := range input {
		if label.Name == labels.MetricName {
			continue
		}
		if label.Name == "" {
			return "", errors.New("remote write label name must not be empty")
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = strconv.AppendQuote(buf, label.Name)
		buf = append(buf, ':')
		buf = strconv.AppendQuote(buf, label.Value)
	}
	buf = append(buf, '}')
	return string(buf), nil
}

func bucketTimestampMS(timestamp int64, interval time.Duration) int64 {
	step := interval.Milliseconds()
	if step <= 0 {
		return timestamp
	}
	remainder := timestamp % step
	if remainder < 0 {
		remainder += step
	}
	return timestamp - remainder
}

func remoteWriteVersion(timestamp int64) uint64 {
	if timestamp < 0 {
		return 0
	}
	return uint64(timestamp)
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
	batchSummary := remoteWriteBatchSummary(batch)
	if batch.seriesCount > 0 {
		inserted, err := s.insertMissingRemoteSeriesRows(ctx, batch.seriesRecords, batchSummary)
		if err != nil {
			return err
		}
		batch.seriesRecords = nil
		batch.seriesCount = inserted
		batchSummary = remoteWriteBatchSummary(batch)
	}
	started := time.Now()
	if err := s.insertRemoteMetadataRows(ctx, batch.metadataRows); err != nil {
		return remoteWritePhaseError("insert metadata", s.cfg.MetricsTable, len(batch.metadataRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	started = time.Now()
	if err := s.insertRemoteSampleRows(ctx, batch.sampleRows); err != nil {
		return remoteWritePhaseError("insert samples", s.cfg.SamplesTable, len(batch.sampleRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	started = time.Now()
	if err := s.insertRemoteHistogramRows(ctx, batch.histogramRows); err != nil {
		return remoteWritePhaseError("insert histograms", s.cfg.HistogramsTable, len(batch.histogramRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	started = time.Now()
	if err := s.insertRemoteExemplarRows(ctx, batch.exemplarRows); err != nil {
		return remoteWritePhaseError("insert exemplars", s.cfg.ExemplarsTable, len(batch.exemplarRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	return nil
}

func (s *Server) insertMissingRemoteSeriesRows(ctx context.Context, rows []remoteWriteSeriesRow, batchSummary string) (int, error) {
	started := time.Now()
	newRows, err := s.filterNewSeriesRows(ctx, rows)
	if err != nil {
		return 0, remoteWritePhaseError("lookup existing series", s.cfg.SeriesTable, len(rows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	if len(newRows) == 0 {
		return 0, nil
	}

	if s.seriesMu != nil {
		s.seriesMu.Lock()
		defer s.seriesMu.Unlock()
	}

	started = time.Now()
	newRows, err = s.filterNewSeriesRows(ctx, newRows)
	if err != nil {
		return 0, remoteWritePhaseError("recheck existing series", s.cfg.SeriesTable, len(newRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	if len(newRows) == 0 {
		return 0, nil
	}

	started = time.Now()
	if err := populateSeriesLabelsJSON(newRows); err != nil {
		return 0, remoteWritePhaseError("encode new series labels", s.cfg.SeriesTable, len(newRows), s.cfg.CHTimeout, batchSummary, started, err)
	}

	started = time.Now()
	if err := s.insertRemoteSeriesRows(ctx, newRows); err != nil {
		return 0, remoteWritePhaseError("insert series", s.cfg.SeriesTable, len(newRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	return len(newRows), nil
}

func remoteWriteBatchSummary(batch remoteWriteBatch) string {
	return fmt.Sprintf(
		"series=%d samples=%d histograms=%d exemplars=%d metadata=%d",
		batch.seriesCount,
		batch.sampleCount,
		batch.histogramCount,
		batch.exemplarCount,
		batch.metadataCount,
	)
}

func remoteWritePhaseError(phase, table string, rows int, timeout time.Duration, batchSummary string, started time.Time, err error) error {
	if err == nil {
		return nil
	}
	elapsed := time.Since(started).Round(time.Millisecond)
	details := fmt.Sprintf("after %s (clickhouse_timeout=%s table=%s rows=%d batch=%s)", elapsed, timeout, table, rows, batchSummary)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("remote write %s timed out %s: %w", phase, details, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("remote write %s canceled %s: %w", phase, details, err)
	default:
		return fmt.Errorf("remote write %s failed %s: %w", phase, details, err)
	}
}

func populateSeriesLabelsJSON(rows []remoteWriteSeriesRow) error {
	for i := range rows {
		if rows[i].LabelsJSON != "" {
			continue
		}
		labelsJSON, err := labelsJSONFromPrompb(rows[i].Labels)
		if err != nil {
			return err
		}
		rows[i].LabelsJSON = labelsJSON
	}
	return nil
}

func (s *Server) filterNewSeriesRows(ctx context.Context, rows []remoteWriteSeriesRow) ([]remoteWriteSeriesRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]uint64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	existing := make(map[uint64]struct{}, len(rows))
	sql := fmt.Sprintf(
		"SELECT DISTINCT series.id FROM %s AS series INNER JOIN remote_write_series_ids AS lookup USING id WHERE series.team_id = %d",
		tableName(s.cfg.CHDatabase, s.cfg.SeriesTable),
		s.cfg.TeamID,
	)
	if err := s.client.QueryRowsWithExternalUInt64s(ctx, "remote_write_series_ids", "id", ids, sql, func(row clickHouseRow) error {
		var id uint64
		if err := row.Scan(&id); err != nil {
			return err
		}
		existing[id] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	newRows := rows[:0]
	for _, row := range rows {
		if _, ok := existing[row.ID]; ok {
			continue
		}
		newRows = append(newRows, row)
	}
	return newRows, nil
}

func (s *Server) insertRemoteSeriesRows(ctx context.Context, rows []remoteWriteSeriesRow) error {
	if len(rows) == 0 {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, id, metric_name, labels_json, min_time, max_time)", tableName(s.cfg.CHDatabase, s.cfg.SeriesTable))
	return s.client.InsertColumnsSync(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		ids := make([]uint64, len(rows))
		metricNames := make([]string, len(rows))
		labelsJSON := make([]string, len(rows))
		minTimes := make([]int64, len(rows))
		maxTimes := make([]int64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			ids[i] = row.ID
			metricNames[i] = row.MetricName
			labelsJSON[i] = row.LabelsJSON
			minTimes[i] = row.MinMS
			maxTimes[i] = row.MaxMS
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(metricNames); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(labelsJSON); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(minTimes); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(maxTimes); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteSampleRows(ctx context.Context, rows []remoteWriteSampleRow) error {
	if len(rows) == 0 {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_name, timestamp, id, value, version)", tableName(s.cfg.CHDatabase, s.cfg.SamplesTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		metricNames := make([]string, len(rows))
		timestamps := make([]int64, len(rows))
		ids := make([]uint64, len(rows))
		values := make([]float64, len(rows))
		versions := make([]uint64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			metricNames[i] = row.MetricName
			timestamps[i] = row.TimestampMS
			ids[i] = row.ID
			values[i] = row.Value
			versions[i] = row.Version
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(metricNames); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(values); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(versions); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteHistogramRows(ctx context.Context, rows []remoteWriteHistogramRow) error {
	if len(rows) == 0 || s.cfg.HistogramsTable == "" {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_name, timestamp, id, histogram, version)", tableName(s.cfg.CHDatabase, s.cfg.HistogramsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		metricNames := make([]string, len(rows))
		timestamps := make([]int64, len(rows))
		ids := make([]uint64, len(rows))
		histograms := make([][]byte, len(rows))
		versions := make([]uint64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			metricNames[i] = row.MetricName
			timestamps[i] = row.TimestampMS
			ids[i] = row.ID
			histograms[i] = row.Histogram
			versions[i] = row.Version
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(metricNames); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(histograms); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(versions); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteExemplarRows(ctx context.Context, rows []remoteWriteExemplarRow) error {
	if len(rows) == 0 || s.cfg.ExemplarsTable == "" {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, timestamp, id, value, labels_json)", tableName(s.cfg.CHDatabase, s.cfg.ExemplarsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		timestamps := make([]int64, len(rows))
		ids := make([]uint64, len(rows))
		values := make([]float64, len(rows))
		labelsJSON := make([]string, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			timestamps[i] = row.TimestampMS
			ids[i] = row.ID
			values[i] = row.Value
			labelsJSON[i] = row.LabelsJSON
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(values); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(labelsJSON); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteMetadataRows(ctx context.Context, rows []remoteWriteMetadataRow) error {
	if len(rows) == 0 || s.cfg.MetricsTable == "" {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_family_name, type, unit, help, updated_at)", tableName(s.cfg.CHDatabase, s.cfg.MetricsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		names := make([]string, len(rows))
		types := make([]string, len(rows))
		units := make([]string, len(rows))
		helps := make([]string, len(rows))
		updatedAt := make([]int64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			names[i] = row.MetricFamilyName
			types[i] = row.Type
			units[i] = row.Unit
			helps[i] = row.Help
			updatedAt[i] = row.UpdatedAtMS
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(names); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(types); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(units); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(helps); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(updatedAt); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
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
