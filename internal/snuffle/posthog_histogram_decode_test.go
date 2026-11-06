package snuffle

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
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

type packedHistogramRow struct {
	payload []byte
	count   uint64
}

func (row packedHistogramRow) Scan(dest ...any) error {
	*dest[0].(*uint64) = 1
	*dest[1].(*int64) = 1000
	*dest[2].(*float64) = 0
	*dest[3].(*uint64) = row.count
	*dest[4].(*[]byte) = row.payload
	*dest[5].(*string) = "cumulative"
	return nil
}

func TestScanPackedDropsTrailingInfiniteBound(test *testing.T) {
	for _, testcase := range []struct {
		name       string
		bounds     []float64
		counts     []uint64
		count      uint64
		wantBounds []float64
		wantCounts []uint64
	}{
		{"prom-client explicit Infinity", []float64{16, 32, math.Inf(1)}, []uint64{1, 2, 3, 0}, 6, []float64{16, 32}, []uint64{1, 2, 3}},
		{"repeated Infinity", []float64{16, math.Inf(1), math.Inf(1)}, []uint64{1, 2, 0, 0}, 3, []float64{16}, []uint64{1, 2}},
		{"only Infinity", []float64{math.Inf(1)}, []uint64{4, 0}, 4, []float64{}, []uint64{4}},
		{"finite bounds unchanged", []float64{16, 32}, []uint64{1, 2, 3}, 6, []float64{16, 32}, []uint64{1, 2, 3}},
		{"length mismatch unchanged", []float64{16, math.Inf(1)}, []uint64{1, 2}, 3, []float64{16, math.Inf(1)}, []uint64{1, 2}},
		{"inner Infinity unchanged", []float64{math.Inf(1), 16}, []uint64{1, 2, 3}, 6, []float64{math.Inf(1), 16}, []uint64{1, 2, 3}},
		{"trailing -Inf unchanged", []float64{16, math.Inf(-1)}, []uint64{1, 2, 0}, 3, []float64{16, math.Inf(-1)}, []uint64{1, 2, 0}},
		{"count above Infinity unchanged", []float64{16, math.Inf(1)}, []uint64{1, 2, 5}, 8, []float64{16, math.Inf(1)}, []uint64{1, 2, 5}},
	} {
		test.Run(testcase.name, func(test *testing.T) {
			var sample postHogHistogramSample
			if err := sample.scanPacked(packedHistogramRow{payload: packedHistogramFixture(testcase.bounds, testcase.counts), count: testcase.count}); err != nil {
				test.Fatal(err)
			}
			if !reflect.DeepEqual(sample.bounds, testcase.wantBounds) || !reflect.DeepEqual(sample.counts, testcase.wantCounts) {
				test.Fatalf("got %v %v, want %v %v", sample.bounds, sample.counts, testcase.wantBounds, testcase.wantCounts)
			}
		})
	}
}

func TestPostHogHistogramSeriesAcceptsExplicitInfinityBucket(test *testing.T) {
	var sample postHogHistogramSample
	payload := packedHistogramFixture([]float64{16, 32, math.Inf(1)}, []uint64{1, 2, 3, 0})
	if err := sample.scanPacked(packedHistogramRow{payload: payload, count: 6}); err != nil {
		test.Fatal(err)
	}
	sample.metricName = "cdp_cyclotron_job_size_kb"
	exact := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cdp_cyclotron_job_size_kb_bucket")}
	builder := newPostHogHistogramSeriesBuilder(histogramTestConfig(), exact, true)
	if !builder.strict {
		test.Fatal("an exact selector must be strict")
	}
	if err := builder.add(sample, []string{"_bucket"}); err != nil {
		test.Fatal(err)
	}
	got := map[string]float64{}
	for _, meta := range builder.finish() {
		got[meta.labelMap["le"]] = meta.samples[0].v
	}
	if want := map[string]float64{"16": 1, "32": 3, "+Inf": 6}; !reflect.DeepEqual(got, want) {
		test.Fatalf("buckets = %v, want %v", got, want)
	}
}
