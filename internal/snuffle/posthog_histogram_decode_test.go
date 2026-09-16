package snuffle

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

func packedHistogramFixture(bounds []float64, counts []uint64) []byte {
	payload := binary.AppendUvarint(nil, uint64(len(bounds)))
	for _, bound := range bounds {
		payload = binary.LittleEndian.AppendUint64(payload, math.Float64bits(bound))
	}
	payload = binary.AppendUvarint(payload, uint64(len(counts)))
	for _, count := range counts {
		payload = binary.LittleEndian.AppendUint64(payload, count)
	}
	return payload
}

func TestDecodePostHogHistogramArrays(test *testing.T) {
	bounds := []float64{math.Copysign(0, -1), 0.5, 1}
	counts := []uint64{1, 2, 3, 0x0a00000000000000}
	payload := packedHistogramFixture(bounds, counts)
	actualBounds, actualCounts, err := decodePostHogHistogramArrays(payload, nil, nil)
	if err != nil || !reflect.DeepEqual(actualBounds, bounds) || !reflect.DeepEqual(actualCounts, counts) || !math.Signbit(actualBounds[0]) {
		test.Fatalf("decode: %v %v %v", actualBounds, actualCounts, err)
	}
	boundStorage, countStorage := &actualBounds[0], &actualCounts[0]
	actualBounds, actualCounts, err = decodePostHogHistogramArrays(packedHistogramFixture([]float64{2}, []uint64{3, 4}), actualBounds, actualCounts)
	if err != nil || &actualBounds[0] != boundStorage || &actualCounts[0] != countStorage || len(actualBounds) != 1 || len(actualCounts) != 2 {
		test.Fatal("arrays did not reuse storage")
	}
	for length := 0; length < len(payload); length++ {
		if _, _, err := decodePostHogHistogramArrays(payload[:length], nil, nil); err == nil {
			test.Fatalf("accepted truncated payload of length %d", length)
		}
	}
	for _, invalid := range [][]byte{{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}, append(payload, 0)} {
		if _, _, err := decodePostHogHistogramArrays(invalid, nil, nil); err == nil {
			test.Fatal("accepted invalid packed data")
		}
	}
}

func FuzzDecodePostHogHistogramArrays(fuzz *testing.F) {
	fuzz.Add(packedHistogramFixture([]float64{1}, []uint64{2, 3}))
	fuzz.Add([]byte{0, 0})
	fuzz.Fuzz(func(test *testing.T, payload []byte) {
		bounds, counts, err := decodePostHogHistogramArrays(payload, nil, nil)
		if err == nil && len(bounds)+len(counts) > len(payload)/8 {
			test.Fatal("decoder allocated beyond input size")
		}
	})
}
