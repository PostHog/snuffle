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
	return err
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
