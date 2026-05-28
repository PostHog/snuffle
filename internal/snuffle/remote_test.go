package snuffle

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func TestBuildRemoteWriteBatch(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "http_requests_total"},
				{Name: "job", Value: "api"},
				{Name: "instance", Value: "host-1"},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1000, Value: 1},
				{Timestamp: 2000, Value: 2},
			},
			Exemplars: []prompb.Exemplar{{
				Labels:    []prompb.Label{{Name: "trace_id", Value: "abc"}},
				Value:     2,
				Timestamp: 2000,
			}},
		},
	}, Metadata: []prompb.MetricMetadata{{
		MetricFamilyName: "http_requests",
		Type:             prompb.MetricMetadata_COUNTER,
		Help:             "request count",
		Unit:             "requests",
	}}}

	batch, err := buildRemoteWriteBatch(req, 0, 7)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.seriesCount != 1 || batch.sampleCount != 2 || batch.labelCount != 2 || batch.exemplarCount != 1 || batch.metadataCount != 1 {
		t.Fatalf("counts = series %d samples %d labels %d exemplars %d metadata %d", batch.seriesCount, batch.sampleCount, batch.labelCount, batch.exemplarCount, batch.metadataCount)
	}
	if len(batch.seriesRecords) != 1 {
		t.Fatalf("seriesRecords length = %d, want 1", len(batch.seriesRecords))
	}
	seriesRow := batch.seriesRecords[0]
	if seriesRow.TeamID != 7 || seriesRow.MetricName != "http_requests_total" || seriesRow.MinMS != 1000 || seriesRow.MaxMS != 2000 || !strings.HasPrefix(seriesRow.LabelsJSON, "{") {
		t.Fatalf("unexpected series row: %#v", seriesRow)
	}
	for _, want := range []string{"job", "instance"} {
		if !remoteWriteLabelRowsContain(batch.labelIndexRows, want) {
			t.Fatalf("label rows %#v do not contain %q", batch.labelIndexRows, want)
		}
	}
	for _, row := range batch.labelIndexRows {
		if row.LabelName == labels.MetricName {
			t.Fatalf("label index rows should not include metric name: %#v", batch.labelIndexRows)
		}
	}
	if len(batch.exemplarRows) != 1 || !strings.Contains(batch.exemplarRows[0].LabelsJSON, `"trace_id":"abc"`) {
		t.Fatalf("exemplar rows should contain exemplar labels: %#v", batch.exemplarRows)
	}
	if len(batch.metadataRows) != 1 || batch.metadataRows[0].Type != "counter" {
		t.Fatalf("metadata rows should contain metadata: %#v", batch.metadataRows)
	}
}

func remoteWriteLabelRowsContain(rows []remoteWriteLabelIndexRow, want string) bool {
	for _, row := range rows {
		if row.LabelName == want {
			return true
		}
	}
	return false
}

func TestBuildRemoteWriteBatchDropsNaNSamples(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "api"}},
			Samples: []prompb.Sample{
				{Timestamp: 1_000, Value: math.NaN()},
				{Timestamp: 2_000, Value: 2},
			},
		},
		{
			Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "gone"}},
			Samples: []prompb.Sample{
				{Timestamp: 3_000, Value: math.NaN()},
			},
		},
	}}

	batch, err := buildRemoteWriteBatch(req, 0, 0)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.seriesCount != 1 || batch.sampleCount != 1 || batch.labelCount != 1 {
		t.Fatalf("counts = series %d samples %d labels %d", batch.seriesCount, batch.sampleCount, batch.labelCount)
	}
	if len(batch.sampleRows) != 1 || math.IsNaN(batch.sampleRows[0].Value) || batch.sampleRows[0].Value != 2 || batch.sampleRows[0].TimestampMS != 2000 {
		t.Fatalf("unexpected sample rows: %#v", batch.sampleRows)
	}
	if batch.sampleRows[0].MetricName != "up" {
		t.Fatalf("sample rows should carry metric name for activity MVs: %#v", batch.sampleRows)
	}
	for _, row := range batch.labelIndexRows {
		if row.LabelValue == "gone" {
			t.Fatalf("all-NaN series should not produce label rows: %#v", batch.labelIndexRows)
		}
	}
	if len(batch.seriesRecords) != 1 || batch.seriesRecords[0].MinMS != 2000 || batch.seriesRecords[0].MaxMS != 2000 {
		t.Fatalf("unexpected series records: %#v", batch.seriesRecords)
	}
}

func TestBuildRemoteWriteBatchAcceptsNativeHistograms(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{{Name: labels.MetricName, Value: "latency"}},
		Histograms: []prompb.Histogram{{
			Count:     &prompb.Histogram_CountInt{CountInt: 1},
			Sum:       1,
			ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
			Timestamp: 1000,
		}},
	}}}
	batch, err := buildRemoteWriteBatch(req, 0, 0)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.seriesCount != 1 || batch.histogramCount != 1 || batch.sampleCount != 0 {
		t.Fatalf("counts = series %d histograms %d samples %d", batch.seriesCount, batch.histogramCount, batch.sampleCount)
	}
	if len(batch.histogramRows) != 1 || len(batch.histogramRows[0].Histogram) == 0 {
		t.Fatalf("unexpected histogram rows: %#v", batch.histogramRows)
	}
	if batch.histogramRows[0].MetricName != "latency" {
		t.Fatalf("histogram rows should carry metric name for activity MVs: %#v", batch.histogramRows)
	}
}

func TestBuildRemoteWriteBatchBucketsSamples(t *testing.T) {
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{{Name: labels.MetricName, Value: "up"}, {Name: "job", Value: "api"}},
		Samples: []prompb.Sample{
			{Timestamp: 1_000, Value: 1},
			{Timestamp: 14_999, Value: 2},
			{Timestamp: 15_000, Value: 3},
		},
		Histograms: []prompb.Histogram{
			{
				Count:     &prompb.Histogram_CountInt{CountInt: 1},
				Sum:       1,
				ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
				Timestamp: 14_500,
			},
			{
				Count:     &prompb.Histogram_CountInt{CountInt: 2},
				Sum:       2,
				ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: 1},
				Timestamp: 14_900,
			},
		},
	}}}

	batch, err := buildRemoteWriteBatch(req, 15*time.Second, 0)
	if err != nil {
		t.Fatalf("buildRemoteWriteBatch returned error: %v", err)
	}
	if batch.sampleCount != 2 {
		t.Fatalf("sampleCount = %d, want 2", batch.sampleCount)
	}
	if batch.histogramCount != 1 {
		t.Fatalf("histogramCount = %d, want 1", batch.histogramCount)
	}
	if len(batch.sampleRows) != 2 ||
		batch.sampleRows[0].TimestampMS != 0 ||
		batch.sampleRows[0].Value != 2 ||
		batch.sampleRows[0].Version != 14999 ||
		batch.sampleRows[1].TimestampMS != 15000 ||
		batch.sampleRows[1].Value != 3 {
		t.Fatalf("unexpected sample rows: %#v", batch.sampleRows)
	}
	if len(batch.histogramRows) != 1 || batch.histogramRows[0].TimestampMS != 0 || batch.histogramRows[0].Version != 14900 {
		t.Fatalf("unexpected histogram rows: %#v", batch.histogramRows)
	}
}

func TestRemoteWritePhaseErrorIncludesUsefulContext(t *testing.T) {
	err := remoteWritePhaseError(
		"insert samples",
		"metrics_samples",
		123,
		30*time.Second,
		"series=1 labels=2 samples=123 histograms=0 exemplars=0 metadata=0",
		time.Now().Add(-1500*time.Millisecond),
		context.DeadlineExceeded,
	)
	got := err.Error()
	for _, want := range []string{
		"remote write insert samples timed out",
		"clickhouse_timeout=30s",
		"table=metrics_samples",
		"rows=123",
		"batch=series=1 labels=2 samples=123 histograms=0 exemplars=0 metadata=0",
		"context deadline exceeded",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("error %q does not contain %q", got, want)
		}
	}
}

func TestStableSeriesID(t *testing.T) {
	first := labels.FromMap(map[string]string{
		labels.MetricName: "up",
		"job":             "api",
		"instance":        "host-1",
	})
	second := labels.FromMap(map[string]string{
		"instance":        "host-1",
		labels.MetricName: "up",
		"job":             "api",
	})
	different := labels.FromMap(map[string]string{
		labels.MetricName: "up",
		"job":             "api",
		"instance":        "host-2",
	})

	if stableSeriesID(first) != stableSeriesID(second) {
		t.Fatal("series fingerprint should be independent of input label order")
	}
	if stableSeriesID(first) == stableSeriesID(different) {
		t.Fatal("series fingerprint should change when labels change")
	}
}

func TestRemoteReadMatcherConversion(t *testing.T) {
	matchers, err := remoteReadMatchers([]*prompb.LabelMatcher{
		{Type: prompb.LabelMatcher_EQ, Name: labels.MetricName, Value: "up"},
		{Type: prompb.LabelMatcher_NRE, Name: "job", Value: "debug.*"},
	})
	if err != nil {
		t.Fatalf("remoteReadMatchers returned error: %v", err)
	}
	if len(matchers) != 2 || matchers[0].Type != labels.MatchEqual || matchers[1].Type != labels.MatchNotRegexp {
		t.Fatalf("unexpected matchers: %#v", matchers)
	}
}

func TestRemoteReadAcceptsSamples(t *testing.T) {
	if !remoteReadAcceptsSamples(&prompb.ReadRequest{}) {
		t.Fatal("empty accepted response types should default to samples")
	}
	if !remoteReadAcceptsSamples(&prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES}}) {
		t.Fatal("samples response type should be accepted")
	}
	if remoteReadAcceptsSamples(&prompb.ReadRequest{AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS}}) {
		t.Fatal("chunk-only remote read should not be accepted")
	}
}
