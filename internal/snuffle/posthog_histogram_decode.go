package snuffle

import (
	"encoding/binary"
	"fmt"
	"math"
)

func (sample *postHogHistogramSample) scanPacked(row clickHouseRow) error {
	var payload []byte
	if err := row.Scan(&sample.id, &sample.timestamp, &sample.sum, &sample.count, &payload, &sample.temporality); err != nil {
		return err
	}
	var err error
	sample.bounds, sample.counts, err = decodePostHogHistogramArrays(payload, sample.bounds, sample.counts)
	if err != nil {
		return err
	}
	sample.bounds, sample.counts = normalizePostHogHistogramBounds(sample.bounds, sample.counts)
	return nil
}

// normalizePostHogHistogramBounds removes trailing non-finite bounds and adds
// their bucket counts to the bucket before them. A producer that declares an
// explicit Infinity bucket, such as a prom-client histogram, stores +Inf as
// its last bound. The overflow bucket after that bound is always empty, so the
// result is the histogram that Prometheus reads from the duplicate +Inf. The
// function changes the slices in place. It does not change a histogram whose
// counts do not match its bounds or whose fold overflows uint64, so that the
// validation reports it.
func normalizePostHogHistogramBounds(bounds []float64, counts []uint64) ([]float64, []uint64) {
	if len(counts) != len(bounds)+1 {
		return bounds, counts
	}
	for len(bounds) > 0 {
		last := bounds[len(bounds)-1]
		if !math.IsInf(last, 1) && !math.IsNaN(last) {
			break
		}
		overflow, previous := counts[len(counts)-1], counts[len(counts)-2]
		if overflow > math.MaxUint64-previous {
			break
		}
		counts[len(counts)-2] = previous + overflow
		bounds, counts = bounds[:len(bounds)-1], counts[:len(counts)-1]
	}
	return bounds, counts
}

func decodePostHogHistogramArrays(payload []byte, bounds []float64, counts []uint64) ([]float64, []uint64, error) {
	readArray := func() ([]byte, error) {
		length, prefix := binary.Uvarint(payload)
		if prefix <= 0 || length > uint64((len(payload)-prefix)/8) {
			return nil, fmt.Errorf("invalid packed histogram array length")
		}
		payload = payload[prefix:]
		size := int(length) * 8
		values := payload[:size]
		payload = payload[size:]
		return values, nil
	}
	boundBytes, err := readArray()
	if err != nil {
		return nil, nil, err
	}
	countBytes, err := readArray()
	if err != nil {
		return nil, nil, err
	}
	if len(payload) != 0 {
		return nil, nil, fmt.Errorf("unexpected trailing packed histogram data")
	}
	if cap(bounds) < len(boundBytes)/8 {
		bounds = make([]float64, len(boundBytes)/8)
	} else {
		bounds = bounds[:len(boundBytes)/8]
	}
	if cap(counts) < len(countBytes)/8 {
		counts = make([]uint64, len(countBytes)/8)
	} else {
		counts = counts[:len(countBytes)/8]
	}
	for index := range bounds {
		bounds[index] = math.Float64frombits(binary.LittleEndian.Uint64(boundBytes[index*8:]))
	}
	for index := range counts {
		counts[index] = binary.LittleEndian.Uint64(countBytes[index*8:])
	}
	return bounds, counts, nil
}
