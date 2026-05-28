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
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
	labelIndexRows   bytes.Buffer
	sampleRows       bytes.Buffer
	histogramRows    bytes.Buffer
	exemplarRows     bytes.Buffer
	metadataRows     bytes.Buffer
	seriesRecords    []remoteWriteSeriesRow
	labelBitmapRows  []remoteWriteLabelIndexRow
	activityRecords  []remoteWriteActivityRow
	seriesCount      int
	labelCount       int
	labelBitmapCount int
	activityCount    int
	sampleCount      int
	histogramCount   int
	exemplarCount    int
	metadataCount    int
}

type remoteWriteSeriesRow struct {
	TeamID     uint64 `json:"team_id"`
	ID         uint64 `json:"id"`
	MetricName string `json:"metric_name"`
	LabelsJSON string `json:"labels_json"`
	MinMS      int64  `json:"min_ms"`
	MaxMS      int64  `json:"max_ms"`
}

type remoteWriteSeriesInsertRow struct {
	TeamID     uint64 `json:"team_id"`
	ID         uint64 `json:"id"`
	BitmapID   uint64 `json:"bitmap_id"`
	MetricName string `json:"metric_name"`
	LabelsJSON string `json:"labels_json"`
	MinMS      int64  `json:"min_ms"`
	MaxMS      int64  `json:"max_ms"`
}

type remoteWriteLabelIndexRow struct {
	TeamID     uint64 `json:"team_id"`
	MetricName string `json:"metric_name"`
	LabelName  string `json:"label_name"`
	LabelValue string `json:"label_value"`
	ID         uint64 `json:"id"`
}

type remoteWriteLabelBitmapRow struct {
	TeamID     uint64 `json:"team_id"`
	MetricName string `json:"metric_name"`
	LabelName  string `json:"label_name"`
	LabelValue string `json:"label_value"`
	BitmapID   uint64 `json:"bitmap_id"`
}

type remoteWriteActivityRow struct {
	TeamID     uint64 `json:"team_id"`
	MetricName string `json:"metric_name"`
	BucketMS   int64  `json:"bucket_ms"`
	ID         uint64 `json:"id"`
}

type remoteWriteActivityBitmapRow struct {
	TeamID     uint64 `json:"team_id"`
	MetricName string `json:"metric_name"`
	BucketMS   int64  `json:"bucket_ms"`
	BitmapID   uint64 `json:"bitmap_id"`
}

type remoteWriteSampleRow struct {
	TeamID      uint64  `json:"team_id"`
	TimestampMS int64   `json:"timestamp_ms"`
	ID          uint64  `json:"id"`
	Value       float64 `json:"value"`
	Version     uint64  `json:"version"`
}

type remoteWriteHistogramRow struct {
	TeamID       uint64 `json:"team_id"`
	TimestampMS  int64  `json:"timestamp_ms"`
	ID           uint64 `json:"id"`
	HistogramB64 string `json:"histogram_b64"`
	Version      uint64 `json:"version"`
}

type remoteWriteExemplarRow struct {
	TeamID      uint64  `json:"team_id"`
	TimestampMS int64   `json:"timestamp_ms"`
	ID          uint64  `json:"id"`
	Value       float64 `json:"value"`
	LabelsJSON  string  `json:"labels_json"`
}

type remoteWriteMetadataRow struct {
	TeamID           uint64 `json:"team_id"`
	MetricFamilyName string `json:"metric_family_name"`
	Type             string `json:"type"`
	Unit             string `json:"unit"`
	Help             string `json:"help"`
}

func buildRemoteWriteBatch(req *prompb.WriteRequest, sampleInterval time.Duration, teamID uint64) (remoteWriteBatch, error) {
	var batch remoteWriteBatch
	seriesByID := make(map[uint64]remoteWriteSeriesRow, len(req.GetTimeseries()))
	labelRows := make(map[remoteWriteLabelIndexRow]struct{})
	labelBitmapRows := make(map[remoteWriteLabelIndexRow]struct{})
	activityRows := make(map[remoteWriteActivityRow]struct{})
	exemplarEncoder := json.NewEncoder(&batch.exemplarRows)
	metadataEncoder := json.NewEncoder(&batch.metadataRows)
	sampleRows := make(map[remoteWriteSampleKey]remoteWriteSampleRow)
	histogramRows := make(map[remoteWriteSampleKey]remoteWriteHistogramRow)

	for _, metadata := range req.GetMetadata() {
		if metadata.GetMetricFamilyName() == "" {
			continue
		}
		if err := metadataEncoder.Encode(remoteWriteMetadataRow{
			TeamID:           teamID,
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
			row := remoteWriteSampleRow{
				TeamID:      teamID,
				TimestampMS: bucketMS,
				ID:          id,
				Value:       sample.Value,
				Version:     remoteWriteVersion(sample.Timestamp),
			}
			key := remoteWriteSampleKey{id: id, timestampMS: bucketMS}
			if existing, ok := sampleRows[key]; !ok || row.Version >= existing.Version {
				sampleRows[key] = row
			}
			activityRows[remoteWriteActivityRow{
				TeamID:     teamID,
				MetricName: metricName,
				BucketMS:   bucketMS,
				ID:         id,
			}] = struct{}{}
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
			row := remoteWriteHistogramRow{
				TeamID:       teamID,
				TimestampMS:  bucketMS,
				ID:           id,
				HistogramB64: base64.StdEncoding.EncodeToString(payload),
				Version:      remoteWriteVersion(originalTimestamp),
			}
			key := remoteWriteSampleKey{id: id, timestampMS: bucketMS}
			if existing, ok := histogramRows[key]; !ok || row.Version >= existing.Version {
				histogramRows[key] = row
			}
			activityRows[remoteWriteActivityRow{
				TeamID:     teamID,
				MetricName: metricName,
				BucketMS:   bucketMS,
				ID:         id,
			}] = struct{}{}
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
			if err := exemplarEncoder.Encode(remoteWriteExemplarRow{
				TeamID:      teamID,
				TimestampMS: exemplar.Timestamp,
				ID:          id,
				Value:       exemplar.Value,
				LabelsJSON:  string(labelsJSON),
			}); err != nil {
				return remoteWriteBatch{}, err
			}
			batch.exemplarCount++
		}
		if !haveTime {
			continue
		}
		outputLabels := make(map[string]string, len(labelMap)-1)
		for k, v := range labelMap {
			if k == labels.MetricName {
				continue
			}
			outputLabels[k] = v
			labelRows[remoteWriteLabelIndexRow{
				TeamID:     teamID,
				MetricName: metricName,
				LabelName:  k,
				LabelValue: v,
				ID:         id,
			}] = struct{}{}
			labelBitmapRows[remoteWriteLabelIndexRow{
				TeamID:     teamID,
				MetricName: metricName,
				LabelName:  k,
				LabelValue: v,
				ID:         id,
			}] = struct{}{}
		}
		labelBitmapRows[remoteWriteLabelIndexRow{
			TeamID:     teamID,
			MetricName: metricName,
			LabelName:  labels.MetricName,
			LabelValue: metricName,
			ID:         id,
		}] = struct{}{}
		labelsJSON, err := json.Marshal(outputLabels)
		if err != nil {
			return remoteWriteBatch{}, err
		}

		row := remoteWriteSeriesRow{
			TeamID:     teamID,
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

	for _, row := range seriesByID {
		batch.seriesRecords = append(batch.seriesRecords, row)
		batch.seriesCount++
	}
	labelEncoder := json.NewEncoder(&batch.labelIndexRows)
	for row := range labelRows {
		if err := labelEncoder.Encode(row); err != nil {
			return remoteWriteBatch{}, err
		}
		batch.labelCount++
	}
	for row := range labelBitmapRows {
		batch.labelBitmapRows = append(batch.labelBitmapRows, row)
		batch.labelBitmapCount++
	}
	for row := range activityRows {
		batch.activityRecords = append(batch.activityRecords, row)
		batch.activityCount++
	}
	sampleEncoder := json.NewEncoder(&batch.sampleRows)
	for _, row := range sampleRows {
		if err := sampleEncoder.Encode(row); err != nil {
			return remoteWriteBatch{}, err
		}
		batch.sampleCount++
	}
	histogramEncoder := json.NewEncoder(&batch.histogramRows)
	for _, row := range histogramRows {
		if err := histogramEncoder.Encode(row); err != nil {
			return remoteWriteBatch{}, err
		}
		batch.histogramCount++
	}
	return batch, nil
}

type remoteWriteSampleKey struct {
	id          uint64
	timestampMS int64
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
	if batch.seriesCount == 0 && batch.sampleCount == 0 && batch.histogramCount == 0 && batch.exemplarCount == 0 && batch.metadataCount == 0 && batch.labelBitmapCount == 0 && batch.activityCount == 0 {
		return nil
	}
	var bitmapIDs map[uint64]uint64
	if batch.seriesCount > 0 {
		s.keyMu.Lock()
		assigned, err := s.assignSeriesBitmapIDs(ctx, batch.seriesRecords)
		if err == nil {
			err = s.insertRemoteSeriesRows(ctx, batch.seriesRecords, assigned)
		}
		s.keyMu.Unlock()
		if err != nil {
			return err
		}
		bitmapIDs = assigned
	}
	for _, insert := range []remoteWriteInsert{
		{count: batch.metadataCount, table: s.cfg.MetricsTable, selectExpr: "team_id, metric_family_name, type, unit, help, now64(3, 'UTC')", input: "team_id UInt64, metric_family_name String, type String, unit String, help String", rows: &batch.metadataRows, optional: true},
		{count: batch.labelCount, table: s.cfg.LabelIndexTable, selectExpr: "team_id, metric_name, label_name, label_value, id", input: "team_id UInt64, metric_name String, label_name String, label_value String, id UInt64", rows: &batch.labelIndexRows},
		{count: batch.sampleCount, table: s.cfg.SamplesTable, selectExpr: "team_id, fromUnixTimestamp64Milli(timestamp_ms, 'UTC'), id, value, version", input: "team_id UInt64, timestamp_ms Int64, id UInt64, value Float64, version UInt64", rows: &batch.sampleRows},
		{count: batch.histogramCount, table: s.cfg.HistogramsTable, selectExpr: "team_id, fromUnixTimestamp64Milli(timestamp_ms, 'UTC'), id, base64Decode(histogram_b64), version", input: "team_id UInt64, timestamp_ms Int64, id UInt64, histogram_b64 String, version UInt64", rows: &batch.histogramRows, optional: true},
		{count: batch.exemplarCount, table: s.cfg.ExemplarsTable, selectExpr: "team_id, fromUnixTimestamp64Milli(timestamp_ms, 'UTC'), id, value, labels_json", input: "team_id UInt64, timestamp_ms Int64, id UInt64, value Float64, labels_json String", rows: &batch.exemplarRows, optional: true},
	} {
		if err := s.insertRemoteRows(ctx, insert); err != nil {
			return err
		}
	}
	if batch.labelBitmapCount > 0 || batch.activityCount > 0 {
		if len(bitmapIDs) == 0 {
			return errors.New("remote write bitmap rows are missing series bitmap ids")
		}
		labelBitmapRows, err := labelBitmapRowsWithBitmapIDs(batch.labelBitmapRows, bitmapIDs)
		if err != nil {
			return err
		}
		activityRows, err := activityRowsWithBitmapIDs(batch.activityRecords, bitmapIDs)
		if err != nil {
			return err
		}
		for _, insert := range []remoteWriteInsert{
			{count: batch.labelBitmapCount, table: s.cfg.LabelPostingsTable, selectExpr: "team_id, metric_name, label_name, label_value, groupBitmapState(bitmap_id)", input: "team_id UInt64, metric_name String, label_name String, label_value String, bitmap_id UInt64", rows: &labelBitmapRows, optional: true, groupBy: "team_id, metric_name, label_name, label_value"},
			{count: batch.activityCount, table: s.cfg.ActivityTable, selectExpr: "team_id, metric_name, fromUnixTimestamp64Milli(bucket_ms, 'UTC'), groupBitmapState(bitmap_id)", input: "team_id UInt64, metric_name String, bucket_ms Int64, bitmap_id UInt64", rows: &activityRows, optional: true, groupBy: "team_id, metric_name, bucket_ms"},
		} {
			if err := s.insertRemoteRows(ctx, insert); err != nil {
				return err
			}
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
	groupBy    string
}

func (s *Server) insertRemoteRows(ctx context.Context, insert remoteWriteInsert) error {
	if insert.count == 0 || insert.optional && insert.table == "" {
		return nil
	}
	source := "input(" + sqlString(insert.input) + ") AS input_rows"
	sql := fmt.Sprintf("INSERT INTO %s SELECT %s FROM %s", tableName(s.cfg.CHDatabase, insert.table), insert.selectExpr, source)
	if insert.groupBy != "" {
		sql += " GROUP BY " + insert.groupBy
	}
	sql += " FORMAT JSONEachRow"
	if err := s.client.ExecWithBody(ctx, sql, bytes.NewReader(insert.rows.Bytes())); err != nil {
		return err
	}
	recordClickHouseWrite(ctx, int64(insert.count))
	return nil
}

func (s *Server) insertRemoteSeriesRows(ctx context.Context, rows []remoteWriteSeriesRow, bitmapIDs map[uint64]uint64) error {
	if len(rows) == 0 {
		return nil
	}
	encoded, err := seriesRowsWithBitmapIDs(rows, bitmapIDs)
	if err != nil {
		return err
	}
	insert := remoteWriteInsert{
		count:      len(rows),
		table:      s.cfg.SeriesTable,
		selectExpr: "team_id, id, bitmap_id, metric_name, labels_json, fromUnixTimestamp64Milli(min_ms, 'UTC'), fromUnixTimestamp64Milli(max_ms, 'UTC')",
		input:      "team_id UInt64, id UInt64, bitmap_id UInt64, metric_name String, labels_json String, min_ms Int64, max_ms Int64",
		rows:       &encoded,
	}
	return s.insertRemoteRows(ctx, insert)
}

func (s *Server) assignSeriesBitmapIDs(ctx context.Context, rows []remoteWriteSeriesRow) (map[uint64]uint64, error) {
	ids := uniqueRemoteWriteSeriesIDs(rows)
	if len(ids) == 0 {
		return nil, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if s.bitmapMax == nil {
		s.bitmapMax = make(map[uint64]uint64)
	}
	bitmapIDs := make(map[uint64]uint64, len(ids))
	maxBitmapID, haveMax := s.bitmapMax[s.cfg.TeamID]
	includeMax := !haveMax
	for _, batch := range idBatches(ids, s.cfg.IDChunkSize) {
		sql := seriesBitmapIDLookupSQL(s.cfg, batch, includeMax)
		includeMax = false
		if err := s.client.QueryJSONEachRow(ctx, sql, func(raw json.RawMessage) error {
			var row map[string]json.RawMessage
			if err := json.Unmarshal(raw, &row); err != nil {
				return err
			}
			kind, err := rawString(row["kind"])
			if err != nil {
				return err
			}
			id, err := rawUint64(row["id"])
			if err != nil {
				return err
			}
			bitmapID, err := rawUint64(row["bitmap_id"])
			if err != nil {
				return err
			}
			if kind == "max" {
				maxBitmapID = bitmapID
				haveMax = true
				return nil
			}
			bitmapIDs[id] = bitmapID
			if bitmapID > maxBitmapID {
				maxBitmapID = bitmapID
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if !haveMax {
		maxBitmapID = 0
	}

	nextBitmapID := maxBitmapID + 1
	for _, id := range ids {
		if _, ok := bitmapIDs[id]; ok {
			continue
		}
		bitmapIDs[id] = nextBitmapID
		nextBitmapID++
	}
	if nextBitmapID > 0 {
		s.bitmapMax[s.cfg.TeamID] = nextBitmapID - 1
	}
	return bitmapIDs, nil
}

func uniqueRemoteWriteSeriesIDs(rows []remoteWriteSeriesRow) []uint64 {
	seen := make(map[uint64]struct{}, len(rows))
	ids := make([]uint64, 0, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.ID]; ok {
			continue
		}
		seen[row.ID] = struct{}{}
		ids = append(ids, row.ID)
	}
	return ids
}

func seriesBitmapIDLookupSQL(cfg Config, ids []uint64, includeMax bool) string {
	table := tableName(cfg.CHDatabase, cfg.SeriesTable)
	parts := make([]string, 0, 2)
	if includeMax {
		parts = append(parts, fmt.Sprintf(
			"SELECT 'max' AS kind, toUInt64(0) AS id, toUInt64(ifNull(max(bitmap_id), 0)) AS bitmap_id FROM %s WHERE %s",
			table,
			teamFilter(cfg),
		))
	}
	parts = append(parts, fmt.Sprintf(
		"SELECT 'series' AS kind, id, max(bitmap_id) AS bitmap_id FROM %s WHERE %s AND id IN (%s) GROUP BY id",
		table,
		teamFilter(cfg),
		joinUint64(ids),
	))
	return strings.Join(parts, " UNION ALL ")
}

func seriesRowsWithBitmapIDs(rows []remoteWriteSeriesRow, bitmapIDs map[uint64]uint64) (bytes.Buffer, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	for _, row := range rows {
		bitmapID, ok := bitmapIDs[row.ID]
		if !ok {
			return bytes.Buffer{}, fmt.Errorf("missing bitmap id for series %d", row.ID)
		}
		if err := encoder.Encode(remoteWriteSeriesInsertRow{
			TeamID:     row.TeamID,
			ID:         row.ID,
			BitmapID:   bitmapID,
			MetricName: row.MetricName,
			LabelsJSON: row.LabelsJSON,
			MinMS:      row.MinMS,
			MaxMS:      row.MaxMS,
		}); err != nil {
			return bytes.Buffer{}, err
		}
	}
	return encoded, nil
}

func labelBitmapRowsWithBitmapIDs(rows []remoteWriteLabelIndexRow, bitmapIDs map[uint64]uint64) (bytes.Buffer, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	for _, row := range rows {
		bitmapID, ok := bitmapIDs[row.ID]
		if !ok {
			return bytes.Buffer{}, fmt.Errorf("missing bitmap id for label index series %d", row.ID)
		}
		if err := encoder.Encode(remoteWriteLabelBitmapRow{
			TeamID:     row.TeamID,
			MetricName: row.MetricName,
			LabelName:  row.LabelName,
			LabelValue: row.LabelValue,
			BitmapID:   bitmapID,
		}); err != nil {
			return bytes.Buffer{}, err
		}
	}
	return encoded, nil
}

func activityRowsWithBitmapIDs(rows []remoteWriteActivityRow, bitmapIDs map[uint64]uint64) (bytes.Buffer, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	for _, row := range rows {
		bitmapID, ok := bitmapIDs[row.ID]
		if !ok {
			return bytes.Buffer{}, fmt.Errorf("missing bitmap id for activity series %d", row.ID)
		}
		if err := encoder.Encode(remoteWriteActivityBitmapRow{
			TeamID:     row.TeamID,
			MetricName: row.MetricName,
			BucketMS:   row.BucketMS,
			BitmapID:   bitmapID,
		}); err != nil {
			return bytes.Buffer{}, err
		}
	}
	return encoded, nil
}

func rawUint64(raw json.RawMessage) (uint64, error) {
	var n uint64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return uint64(f), nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, err
	}
	return strconv.ParseUint(s, 10, 64)
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
