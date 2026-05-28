package snuffle

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const (
	protoWireVarint  = 0
	protoWireFixed64 = 1
	protoWireBytes   = 2
	protoWireFixed32 = 5
)

type remoteWriteFastSample struct {
	Timestamp int64
	Value     float64
}

type remoteWriteFastSeriesIndex struct {
	keys   []uint64
	values []int
	used   []bool
	count  int
	mask   uint64
}

func newRemoteWriteFastSeriesIndex(capHint int) remoteWriteFastSeriesIndex {
	size := 16
	for size < capHint*2 {
		size <<= 1
	}
	return remoteWriteFastSeriesIndex{
		keys:   make([]uint64, size),
		values: make([]int, size),
		used:   make([]bool, size),
		mask:   uint64(size - 1),
	}
}

func (m *remoteWriteFastSeriesIndex) get(key uint64) (int, bool) {
	slot := key & m.mask
	for {
		if !m.used[slot] {
			return 0, false
		}
		if m.keys[slot] == key {
			return m.values[slot], true
		}
		slot = (slot + 1) & m.mask
	}
}

func (m *remoteWriteFastSeriesIndex) set(key uint64, value int) {
	if (m.count+1)*10 >= len(m.keys)*7 {
		m.grow()
	}
	slot := key & m.mask
	for {
		if !m.used[slot] {
			m.used[slot] = true
			m.keys[slot] = key
			m.values[slot] = value
			m.count++
			return
		}
		if m.keys[slot] == key {
			m.values[slot] = value
			return
		}
		slot = (slot + 1) & m.mask
	}
}

func (m *remoteWriteFastSeriesIndex) grow() {
	oldKeys := m.keys
	oldValues := m.values
	oldUsed := m.used

	size := len(oldKeys) * 2
	m.keys = make([]uint64, size)
	m.values = make([]int, size)
	m.used = make([]bool, size)
	m.count = 0
	m.mask = uint64(size - 1)

	for i, ok := range oldUsed {
		if ok {
			m.set(oldKeys[i], oldValues[i])
		}
	}
}

type remoteWriteFastIdentity struct {
	hash       xxhash.Digest
	length     [8]byte
	stack      [512]byte
	encoded    []byte
	previous   string
	metricName string
	useStack   bool
	labelCount int
}

func (s *remoteWriteFastIdentity) reset() {
	s.hash.Reset()
	s.encoded = s.stack[:0]
	s.previous = ""
	s.metricName = ""
	s.useStack = true
	s.labelCount = 0
}

func (s *remoteWriteFastIdentity) add(label remoteWriteLabel) (bool, error) {
	if label.Name == "" {
		return false, fmt.Errorf("remote write label name must not be empty")
	}
	if s.labelCount > 0 && label.Name <= s.previous {
		if label.Name == s.previous {
			return false, fmt.Errorf("duplicate remote write label %q", label.Name)
		}
		return false, nil
	}
	s.previous = label.Name
	s.labelCount++
	if label.Name == "__name__" {
		s.metricName = label.Value
	}
	needed := 16 + len(label.Name) + len(label.Value)
	if s.useStack && len(s.encoded)+needed <= cap(s.encoded) {
		binary.LittleEndian.PutUint64(s.length[:], uint64(len(label.Name)))
		s.encoded = append(s.encoded, s.length[:]...)
		s.encoded = append(s.encoded, label.Name...)
		binary.LittleEndian.PutUint64(s.length[:], uint64(len(label.Value)))
		s.encoded = append(s.encoded, s.length[:]...)
		s.encoded = append(s.encoded, label.Value...)
		return true, nil
	}
	if s.useStack {
		s.useStack = false
		_, _ = s.hash.Write(s.encoded)
		s.encoded = nil
	}
	binary.LittleEndian.PutUint64(s.length[:], uint64(len(label.Name)))
	_, _ = s.hash.Write(s.length[:])
	_, _ = s.hash.WriteString(label.Name)
	binary.LittleEndian.PutUint64(s.length[:], uint64(len(label.Value)))
	_, _ = s.hash.Write(s.length[:])
	_, _ = s.hash.WriteString(label.Value)
	return true, nil
}

func (s *remoteWriteFastIdentity) finish() (uint64, string, error) {
	if s.labelCount == 0 {
		return 0, "", fmt.Errorf("remote write time series has no labels")
	}
	if s.metricName == "" {
		return 0, "", fmt.Errorf("remote write time series is missing __name__")
	}
	if s.useStack {
		return xxhash.Sum64(s.encoded), s.metricName, nil
	}
	return s.hash.Sum64(), s.metricName, nil
}

func buildRemoteWriteBatchFromProto(data []byte, sampleInterval time.Duration, teamID uint64) (remoteWriteBatch, bool, error) {
	capHint := len(data) / 256
	if capHint < 1 {
		capHint = 1
	}
	batch := remoteWriteBatch{
		sampleColumns: remoteWriteSampleColumns{
			TeamIDs:     make([]uint64, 0, capHint),
			MetricNames: make([]string, 0, capHint),
			Timestamps:  make([]int64, 0, capHint),
			IDs:         make([]uint64, 0, capHint),
			Values:      make([]float64, 0, capHint),
		},
		histogramRows: make([]remoteWriteHistogramRow, 0),
		exemplarRows:  make([]remoteWriteExemplarRow, 0),
		metadataRows:  make([]remoteWriteMetadataRow, 0),
		seriesRecords: make([]remoteWriteSeriesRow, 0, capHint),
		fastProto:     data,
	}
	seriesIndexByID := newRemoteWriteFastSeriesIndex(capHint)
	sampleScratch := make([]remoteWriteFastSample, 0, 1)
	var identity remoteWriteFastIdentity
	sampleIntervalMS := sampleInterval.Milliseconds()

	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return remoteWriteBatch{}, true, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		if fieldNum <= 0 {
			return remoteWriteBatch{}, true, fmt.Errorf("decode remote write protobuf: invalid field number %d", fieldNum)
		}

		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return remoteWriteBatch{}, true, fmt.Errorf("decode remote write protobuf: timeseries has wire type %d", wireType)
			}
			message, err := readProtoBytes(data, &i)
			if err != nil {
				return remoteWriteBatch{}, true, err
			}
			ok, err := parseFastTimeSeries(message, &batch, &seriesIndexByID, &identity, &sampleScratch, sampleIntervalMS, teamID)
			if err != nil {
				return remoteWriteBatch{}, true, err
			}
			if !ok {
				return remoteWriteBatch{}, false, nil
			}
		case 3:
			return remoteWriteBatch{}, false, nil
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return remoteWriteBatch{}, true, err
			}
		}
	}

	batch.seriesCount = len(batch.seriesRecords)
	batch.sampleCount = batch.sampleColumns.count()
	return batch, true, nil
}

func parseFastTimeSeries(data []byte, batch *remoteWriteBatch, seriesIndexByID *remoteWriteFastSeriesIndex, identity *remoteWriteFastIdentity, sampleScratch *[]remoteWriteFastSample, sampleIntervalMS int64, teamID uint64) (bool, error) {
	samples := (*sampleScratch)[:0]
	sawSample := false
	identity.reset()
	defer func() {
		*sampleScratch = samples[:0]
	}()

	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return true, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		if fieldNum <= 0 {
			return true, fmt.Errorf("decode remote write timeseries protobuf: invalid field number %d", fieldNum)
		}

		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return true, fmt.Errorf("decode remote write timeseries protobuf: label has wire type %d", wireType)
			}
			message, err := readProtoBytes(data, &i)
			if err != nil {
				return true, err
			}
			label, err := parseFastLabel(message)
			if err != nil {
				return true, err
			}
			ok, err := identity.add(label)
			if err != nil || !ok {
				return ok, err
			}
		case 2:
			if wireType != protoWireBytes {
				return true, fmt.Errorf("decode remote write timeseries protobuf: sample has wire type %d", wireType)
			}
			message, err := readProtoBytes(data, &i)
			if err != nil {
				return true, err
			}
			sample, err := parseFastSample(message)
			if err != nil {
				return true, err
			}
			sawSample = true
			if !math.IsNaN(sample.Value) {
				samples = append(samples, sample)
			}
		case 3, 4:
			return false, nil
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return true, err
			}
		}
	}

	if !sawSample {
		return true, nil
	}
	id, metricName, err := identity.finish()
	if err != nil {
		return true, err
	}
	if len(samples) == 0 {
		return true, nil
	}

	var minMS int64
	var maxMS int64
	haveTime := false
	for _, sample := range samples {
		bucketMS := bucketTimestampForStepMS(sample.Timestamp, sampleIntervalMS)
		if !haveTime {
			minMS = bucketMS
			maxMS = bucketMS
			haveTime = true
		} else {
			if bucketMS < minMS {
				minMS = bucketMS
			}
			if bucketMS > maxMS {
				maxMS = bucketMS
			}
		}
		if count := batch.sampleColumns.count(); count > 0 && batch.sampleColumns.IDs[count-1] == id && batch.sampleColumns.Timestamps[count-1] == bucketMS {
			batch.sampleColumns.Values[count-1] = sample.Value
			continue
		}
		appendFastSample(batch, teamID, metricName, bucketMS, id, sample.Value)
	}

	addFastSeriesRecord(batch, seriesIndexByID, id, teamID, metricName, nil, minMS, maxMS)
	return true, nil
}

func addFastSeriesRecord(batch *remoteWriteBatch, indexByID *remoteWriteFastSeriesIndex, id, teamID uint64, metricName string, labels []remoteWriteLabel, minMS, maxMS int64) {
	if index, ok := indexByID.get(id); ok {
		existing := &batch.seriesRecords[index]
		if minMS < existing.MinMS {
			existing.MinMS = minMS
		}
		if maxMS > existing.MaxMS {
			existing.MaxMS = maxMS
		}
		return
	}
	indexByID.set(id, len(batch.seriesRecords))
	batch.seriesRecords = append(batch.seriesRecords, remoteWriteSeriesRow{
		TeamID:     teamID,
		ID:         id,
		MetricName: metricName,
		MinMS:      minMS,
		MaxMS:      maxMS,
	})
}

func appendFastSample(batch *remoteWriteBatch, teamID uint64, metricName string, timestampMS int64, id uint64, value float64) {
	batch.sampleColumns.TeamIDs = append(batch.sampleColumns.TeamIDs, teamID)
	batch.sampleColumns.MetricNames = append(batch.sampleColumns.MetricNames, metricName)
	batch.sampleColumns.Timestamps = append(batch.sampleColumns.Timestamps, timestampMS)
	batch.sampleColumns.IDs = append(batch.sampleColumns.IDs, id)
	batch.sampleColumns.Values = append(batch.sampleColumns.Values, value)
}

func parseFastLabel(data []byte) (remoteWriteLabel, error) {
	if len(data) > 0 && data[0] == 0x0a {
		i := 1
		name, err := readProtoBytes(data, &i)
		if err != nil {
			return remoteWriteLabel{}, err
		}
		if i < len(data) && data[i] == 0x12 {
			i++
			value, err := readProtoBytes(data, &i)
			if err != nil {
				return remoteWriteLabel{}, err
			}
			if i == len(data) {
				return remoteWriteLabel{Name: protoUnsafeString(name), Value: protoUnsafeString(value)}, nil
			}
		}
	}

	var label remoteWriteLabel
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return remoteWriteLabel{}, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return remoteWriteLabel{}, fmt.Errorf("decode remote write label protobuf: name has wire type %d", wireType)
			}
			value, err := readProtoBytes(data, &i)
			if err != nil {
				return remoteWriteLabel{}, err
			}
			label.Name = protoUnsafeString(value)
		case 2:
			if wireType != protoWireBytes {
				return remoteWriteLabel{}, fmt.Errorf("decode remote write label protobuf: value has wire type %d", wireType)
			}
			value, err := readProtoBytes(data, &i)
			if err != nil {
				return remoteWriteLabel{}, err
			}
			label.Value = protoUnsafeString(value)
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return remoteWriteLabel{}, err
			}
		}
	}
	return label, nil
}

func parseFastSample(data []byte) (remoteWriteFastSample, error) {
	if len(data) >= 10 && data[0] == 0x09 && data[9] == 0x10 {
		i := 10
		timestamp, err := readProtoVarint(data, &i)
		if err != nil {
			return remoteWriteFastSample{}, err
		}
		if i == len(data) {
			return remoteWriteFastSample{
				Timestamp: int64(timestamp),
				Value:     math.Float64frombits(binary.LittleEndian.Uint64(data[1:])),
			}, nil
		}
	}

	var sample remoteWriteFastSample
	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return remoteWriteFastSample{}, err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireFixed64 {
				return remoteWriteFastSample{}, fmt.Errorf("decode remote write sample protobuf: value has wire type %d", wireType)
			}
			if len(data)-i < 8 {
				return remoteWriteFastSample{}, fmt.Errorf("decode remote write sample protobuf: truncated fixed64")
			}
			sample.Value = math.Float64frombits(binary.LittleEndian.Uint64(data[i:]))
			i += 8
		case 2:
			if wireType != protoWireVarint {
				return remoteWriteFastSample{}, fmt.Errorf("decode remote write sample protobuf: timestamp has wire type %d", wireType)
			}
			value, err := readProtoVarint(data, &i)
			if err != nil {
				return remoteWriteFastSample{}, err
			}
			sample.Timestamp = int64(value)
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return remoteWriteFastSample{}, err
			}
		}
	}
	return sample, nil
}

func readProtoBytes(data []byte, i *int) ([]byte, error) {
	length, err := readProtoVarint(data, i)
	if err != nil {
		return nil, err
	}
	if length > uint64(len(data)-*i) {
		return nil, fmt.Errorf("decode remote write protobuf: length-delimited field length %d exceeds remaining %d", length, len(data)-*i)
	}
	start := *i
	*i += int(length)
	return data[start:*i], nil
}

func readProtoVarint(data []byte, i *int) (uint64, error) {
	if *i >= len(data) {
		return 0, fmt.Errorf("decode remote write protobuf: truncated varint")
	}
	b := data[*i]
	*i++
	if b < 0x80 {
		return uint64(b), nil
	}
	return readProtoVarintSlow(data, i, uint64(b&0x7f), 7)
}

func readProtoVarintSlow(data []byte, i *int, value uint64, shift uint) (uint64, error) {
	for ; shift < 64; shift += 7 {
		if *i >= len(data) {
			return 0, fmt.Errorf("decode remote write protobuf: truncated varint")
		}
		b := data[*i]
		*i++
		value |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("decode remote write protobuf: varint overflows uint64")
}

func skipProtoField(data []byte, i *int, wireType int) error {
	switch wireType {
	case protoWireVarint:
		_, err := readProtoVarint(data, i)
		return err
	case protoWireFixed64:
		if len(data)-*i < 8 {
			return fmt.Errorf("decode remote write protobuf: truncated fixed64")
		}
		*i += 8
		return nil
	case protoWireBytes:
		_, err := readProtoBytes(data, i)
		return err
	case protoWireFixed32:
		if len(data)-*i < 4 {
			return fmt.Errorf("decode remote write protobuf: truncated fixed32")
		}
		*i += 4
		return nil
	default:
		return fmt.Errorf("decode remote write protobuf: unsupported wire type %d", wireType)
	}
}

func protoUnsafeString(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(data), len(data))
}

func populateSeriesLabelsJSONFromProto(data []byte, rows []remoteWriteSeriesRow) error {
	needed := make(map[uint64]int, len(rows))
	for i := range rows {
		if rows[i].LabelsJSON == "" {
			needed[rows[i].ID] = i
		}
	}
	if len(needed) == 0 {
		return nil
	}

	labelScratch := make([]remoteWriteLabel, 0, 16)
	for i := 0; i < len(data) && len(needed) > 0; {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		if fieldNum == 1 {
			if wireType != protoWireBytes {
				return fmt.Errorf("decode remote write protobuf: timeseries has wire type %d", wireType)
			}
			message, err := readProtoBytes(data, &i)
			if err != nil {
				return err
			}
			if err := populateSeriesLabelsJSONFromTimeSeriesProto(message, rows, needed, &labelScratch); err != nil {
				return err
			}
			continue
		}
		if err := skipProtoField(data, &i, wireType); err != nil {
			return err
		}
	}
	if len(needed) != 0 {
		return fmt.Errorf("remote write fast path could not find labels for %d new series", len(needed))
	}
	return nil
}

func populateSeriesLabelsJSONFromTimeSeriesProto(data []byte, rows []remoteWriteSeriesRow, needed map[uint64]int, labelScratch *[]remoteWriteLabel) error {
	labels := (*labelScratch)[:0]
	defer func() {
		*labelScratch = labels[:0]
	}()

	for i := 0; i < len(data); {
		key, err := readProtoVarint(data, &i)
		if err != nil {
			return err
		}
		fieldNum := int(key >> 3)
		wireType := int(key & 0x7)
		switch fieldNum {
		case 1:
			if wireType != protoWireBytes {
				return fmt.Errorf("decode remote write timeseries protobuf: label has wire type %d", wireType)
			}
			message, err := readProtoBytes(data, &i)
			if err != nil {
				return err
			}
			label, err := parseFastLabel(message)
			if err != nil {
				return err
			}
			labels = append(labels, label)
		case 2:
			if _, err := readProtoBytes(data, &i); err != nil {
				return err
			}
		case 3, 4:
			return nil
		default:
			if err := skipProtoField(data, &i, wireType); err != nil {
				return err
			}
		}
	}

	id, _, err := remoteWriteSeriesIdentityLabels(labels)
	if err != nil {
		return err
	}
	index, ok := needed[id]
	if !ok {
		return nil
	}
	labelsJSON, err := labelsJSONFromRemoteWriteLabels(labels)
	if err != nil {
		return err
	}
	rows[index].LabelsJSON = labelsJSON
	delete(needed, id)
	return nil
}
