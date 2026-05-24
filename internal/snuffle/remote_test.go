package snuffle

import (
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
	if batch.seriesCount != 1 || batch.sampleCount != 2 || batch.labelCount != 2 || batch.labelBitmapCount != 3 || batch.activityCount != 2 || batch.exemplarCount != 1 || batch.metadataCount != 1 {
		t.Fatalf("counts = series %d samples %d labels %d label bitmaps %d activity %d exemplars %d metadata %d", batch.seriesCount, batch.sampleCount, batch.labelCount, batch.labelBitmapCount, batch.activityCount, batch.exemplarCount, batch.metadataCount)
	}
	if len(batch.seriesRecords) != 1 {
		t.Fatalf("seriesRecords length = %d, want 1", len(batch.seriesRecords))
	}
	seriesRow := batch.seriesRecords[0]
	if seriesRow.TeamID != 7 || seriesRow.MetricName != "http_requests_total" || seriesRow.MinMS != 1000 || seriesRow.MaxMS != 2000 || !strings.HasPrefix(seriesRow.LabelsJSON, "{") {
		t.Fatalf("unexpected series row: %#v", seriesRow)
	}
	labelRows := batch.labelIndexRows.String()
	for _, want := range []string{`"label_name":"job"`, `"label_name":"instance"`} {
		if !strings.Contains(labelRows, want) {
			t.Fatalf("label rows %q do not contain %q", labelRows, want)
		}
	}
	if strings.Contains(labelRows, labels.MetricName) {
		t.Fatalf("label index rows should not include metric name: %q", labelRows)
	}
	for _, want := range []string{"job", "instance", labels.MetricName} {
		if !remoteWriteLabelRowsContain(batch.labelBitmapRows, want) {
			t.Fatalf("label bitmap rows %#v do not contain %q", batch.labelBitmapRows, want)
		}
	}
	if !remoteWriteActivityRowsContain(batch.activityRecords, "http_requests_total", 1000) || !remoteWriteActivityRowsContain(batch.activityRecords, "http_requests_total", 2000) {
		t.Fatalf("unexpected activity rows: %#v", batch.activityRecords)
	}
	if !strings.Contains(batch.exemplarRows.String(), `\"trace_id\":\"abc\"`) {
		t.Fatalf("exemplar rows should contain exemplar labels: %q", batch.exemplarRows.String())
	}
	if !strings.Contains(batch.metadataRows.String(), `"type":"counter"`) {
		t.Fatalf("metadata rows should contain metadata: %q", batch.metadataRows.String())
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

func remoteWriteActivityRowsContain(rows []remoteWriteActivityRow, metricName string, bucketMS int64) bool {
	for _, row := range rows {
		if row.MetricName == metricName && row.BucketMS == bucketMS {
			return true
		}
	}
	return false
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
	if batch.seriesCount != 1 || batch.histogramCount != 1 || batch.sampleCount != 0 || batch.labelBitmapCount != 1 || batch.activityCount != 1 {
		t.Fatalf("counts = series %d histograms %d samples %d label bitmaps %d activity %d", batch.seriesCount, batch.histogramCount, batch.sampleCount, batch.labelBitmapCount, batch.activityCount)
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
	if batch.activityCount != 2 {
		t.Fatalf("activityCount = %d, want 2", batch.activityCount)
	}
	samples := batch.sampleRows.String()
	for _, want := range []string{`"timestamp_ms":0`, `"value":2`, `"version":14999`, `"timestamp_ms":15000`, `"value":3`} {
		if !strings.Contains(samples, want) {
			t.Fatalf("sample rows %q do not contain %q", samples, want)
		}
	}
	histograms := batch.histogramRows.String()
	for _, want := range []string{`"timestamp_ms":0`, `"version":14900`} {
		if !strings.Contains(histograms, want) {
			t.Fatalf("histogram rows %q do not contain %q", histograms, want)
		}
	}
}

func TestSeriesBitmapIDLookupSQLUsesSeriesTable(t *testing.T) {
	cfg := Config{CHDatabase: "default", SeriesTable: "metrics_series", TeamID: 42}
	sql := seriesBitmapIDLookupSQL(cfg, []uint64{11, 22}, true)
	for _, want := range []string{"metrics_series", "bitmap_id", "team_id = 42", "id IN (11,22)", "UNION ALL"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("lookup SQL %q does not contain %q", sql, want)
		}
	}
	if strings.Contains(sql, "series_keys") {
		t.Fatalf("lookup SQL should not use the removed series key table: %q", sql)
	}
}

func TestBitmapRowsUseAssignedBitmapIDs(t *testing.T) {
	seriesRows := []remoteWriteSeriesRow{{
		TeamID:     7,
		ID:         99,
		MetricName: "up",
		LabelsJSON: "{}",
		MinMS:      1000,
		MaxMS:      1000,
	}}
	encodedSeries, err := seriesRowsWithBitmapIDs(seriesRows, map[uint64]uint64{99: 3})
	if err != nil {
		t.Fatalf("seriesRowsWithBitmapIDs returned error: %v", err)
	}
	if !strings.Contains(encodedSeries.String(), `"bitmap_id":3`) {
		t.Fatalf("series row did not include assigned bitmap id: %q", encodedSeries.String())
	}

	labelRows, err := labelBitmapRowsWithBitmapIDs([]remoteWriteLabelIndexRow{{TeamID: 7, MetricName: "up", LabelName: labels.MetricName, LabelValue: "up", ID: 99}}, map[uint64]uint64{99: 3})
	if err != nil {
		t.Fatalf("labelBitmapRowsWithBitmapIDs returned error: %v", err)
	}
	if !strings.Contains(labelRows.String(), `"bitmap_id":3`) || strings.Contains(labelRows.String(), `"id":99`) {
		t.Fatalf("label bitmap row should contain bitmap_id only: %q", labelRows.String())
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
