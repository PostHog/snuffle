package snuffle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func BenchmarkRemoteWriteFromClickHouse(b *testing.B) {
	targetURL := strings.TrimSpace(os.Getenv("REMOTE_WRITE_BENCH_URL"))
	if targetURL == "" {
		b.Skip("set REMOTE_WRITE_BENCH_URL to run remote-write replay benchmark")
	}
	client := &http.Client{Timeout: envDuration("REMOTE_WRITE_BENCH_TIMEOUT", 5*time.Minute)}
	ctx, cancel := context.WithTimeout(context.Background(), envDuration("REMOTE_WRITE_SOURCE_TIMEOUT", 5*time.Minute))
	defer cancel()

	series, sampleCount, err := loadRemoteWriteSourceSeries(ctx)
	if err != nil {
		b.Fatalf("load source series: %v", err)
	}
	if len(series) == 0 {
		b.Fatal("source query returned no series")
	}
	batchSize := envInt("REMOTE_WRITE_BENCH_SERIES_PER_REQUEST", 200, 1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		requests := 0
		for start := 0; start < len(series); start += batchSize {
			end := start + batchSize
			if end > len(series) {
				end = len(series)
			}
			if err := postRemoteWrite(client, targetURL, series[start:end]); err != nil {
				b.Fatalf("remote write batch failed: %v", err)
			}
			requests++
		}
		elapsed := time.Since(started).Seconds()
		b.ReportMetric(float64(sampleCount)/elapsed, "samples_per_s")
		b.ReportMetric(float64(len(series))/elapsed, "series_per_s")
		b.ReportMetric(float64(requests), "requests/op")
	}
	b.StopTimer()
	b.ReportMetric(float64(sampleCount), "samples/op")
	b.ReportMetric(float64(len(series)), "series/op")
}

func loadRemoteWriteSourceSeries(ctx context.Context) ([]prompb.TimeSeries, int, error) {
	chAddr := envString("REMOTE_WRITE_SOURCE_CH_ADDR", "localhost:9000")
	database := envString("REMOTE_WRITE_SOURCE_DATABASE", "snuffle_perf")
	user := envString("REMOTE_WRITE_SOURCE_USER", "default")
	password := os.Getenv("REMOTE_WRITE_SOURCE_PASSWORD")
	teamID := envString("REMOTE_WRITE_SOURCE_TEAM_ID", "42")
	metric := envString("REMOTE_WRITE_SOURCE_METRIC", "ClickHouseMetrics_Query")
	start := envString("REMOTE_WRITE_SOURCE_START", "1778398980000")
	end := envString("REMOTE_WRITE_SOURCE_END", "1778402580000")
	limit := envInt("REMOTE_WRITE_SOURCE_SERIES_LIMIT", 500, 1)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol:    clickhouse.Native,
		Addr:        []string{chAddr},
		Auth:        clickhouse.Auth{Database: database, Username: user, Password: password},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	sql := fmt.Sprintf(`
WITH selected_series AS
(
    SELECT
        id,
        any(metric_name) AS selected_metric_name,
        any(labels_json) AS labels_json,
        min(min_time) AS min_seen,
        max(max_time) AS max_seen
    FROM metrics_series
    WHERE team_id = %s
      AND metric_name = %s
      AND max_time >= fromUnixTimestamp64Milli(%s, 'UTC')
      AND min_time <= fromUnixTimestamp64Milli(%s, 'UTC')
    GROUP BY id
    ORDER BY dateDiff('second', min_seen, max_seen) DESC
    LIMIT %d
),
points AS
(
    SELECT id, timestamp, argMax(value, version) AS value
    FROM metrics_samples
    WHERE team_id = %s
      AND timestamp >= fromUnixTimestamp64Milli(%s, 'UTC')
      AND timestamp <= fromUnixTimestamp64Milli(%s, 'UTC')
      AND id IN (SELECT id FROM selected_series)
    GROUP BY id, timestamp
)
SELECT selected_series.selected_metric_name AS metric_name,
       selected_series.labels_json AS labels_json,
       groupArray(toUnixTimestamp64Milli(timestamp)) AS timestamps,
       groupArray(value) AS values
FROM points
INNER JOIN selected_series USING id
GROUP BY id, metric_name, labels_json`,
		teamID, sqlStringLiteral(metric), start, end, limit, teamID, start, end)

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var series []prompb.TimeSeries
	sampleCount := 0
	for rows.Next() {
		var row remoteWriteSourceRow
		if err := rows.Scan(&row.MetricName, &row.LabelsJSON, &row.Timestamps, &row.Values); err != nil {
			return nil, 0, err
		}
		ts, err := row.timeSeries()
		if err != nil {
			return nil, 0, err
		}
		sampleCount += len(ts.Samples)
		series = append(series, ts)
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	return series, sampleCount, rows.Err()
}

type remoteWriteSourceRow struct {
	MetricName string
	LabelsJSON string
	Timestamps []int64
	Values     []float64
}

func parseJSONMap(raw string, target map[string]string) error {
	if err := json.Unmarshal([]byte(raw), &target); err == nil {
		return nil
	}
	var encoded string
	if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
		return err
	}
	if encoded == "" {
		return nil
	}
	return json.Unmarshal([]byte(encoded), &target)
}

func (r remoteWriteSourceRow) timeSeries() (prompb.TimeSeries, error) {
	labelMap := map[string]string{}
	if r.LabelsJSON != "" {
		if err := parseJSONMap(r.LabelsJSON, labelMap); err != nil {
			return prompb.TimeSeries{}, err
		}
	}
	labelMap[labels.MetricName] = r.MetricName
	names := make([]string, 0, len(labelMap))
	for name := range labelMap {
		names = append(names, name)
	}
	sort.Strings(names)
	ts := prompb.TimeSeries{
		Labels:  make([]prompb.Label, 0, len(names)),
		Samples: make([]prompb.Sample, 0, len(r.Timestamps)),
	}
	for _, name := range names {
		ts.Labels = append(ts.Labels, prompb.Label{Name: name, Value: labelMap[name]})
	}
	if len(r.Timestamps) != len(r.Values) {
		return prompb.TimeSeries{}, fmt.Errorf("timestamp/value length mismatch: %d/%d", len(r.Timestamps), len(r.Values))
	}
	for i, timestamp := range r.Timestamps {
		ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: timestamp, Value: r.Values[i]})
	}
	sort.Slice(ts.Samples, func(i, j int) bool {
		return ts.Samples[i].Timestamp < ts.Samples[j].Timestamp
	})
	return ts, nil
}

func postRemoteWrite(client *http.Client, targetURL string, series []prompb.TimeSeries) error {
	req := prompb.WriteRequest{Timeseries: series}
	payload, err := req.Marshal()
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(snappy.Encode(nil, payload)))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func sqlStringLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return "'" + value + "'"
}
