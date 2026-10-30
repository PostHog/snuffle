package snuffle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// assertQueryLogComments checks that ClickHouse records the request's tags
// in system.query_log.log_comment for reads, Loki reads, and remote write.
func assertQueryLogComments(t *testing.T, ctx context.Context, root *ClickHouseClient, baseURL string) {
	t.Helper()
	clientQueryID := fmt.Sprintf("snuffle-e2e-%d", time.Now().UnixNano())
	header := fmt.Sprintf(`{"client_query_id":%q,"user_id":7,"team_id":999}`, clientQueryID)

	send := func(method, path string, body []byte, wantStatus int) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-PostHog-Query-Tags", header)
		if body != nil {
			req.Header.Set("Content-Type", "application/x-protobuf")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		payload, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != wantStatus {
			t.Fatalf("%s %s status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(payload)))
		}
	}
	rangeParams := url.Values{"query": {"sum(rate(" + e2eCounterMetric + "[1m]))"}, "start": {"1700000010"}, "end": {"1700000070"}, "step": {"30"}}
	send(http.MethodGet, e2eAPIPath("/api/v1/query_range")+"?"+rangeParams.Encode(), nil, http.StatusOK)
	lokiParams := url.Values{"query": {`{app="snuffle-e2e-tags"}`}, "start": {"1700000010"}, "end": {"1700000070"}}
	send(http.MethodGet, e2eAPIPath("/loki/api/v1/query_range")+"?"+lokiParams.Encode(), nil, http.StatusOK)
	write, err := (&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "snuffle_e2e_tagged_total"}},
		Samples: []prompb.Sample{{Timestamp: e2eStartMS, Value: 1}},
	}}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	send(http.MethodPost, e2eAPIPath("/api/v1/write"), snappy.Encode(nil, write), http.StatusNoContent)

	if err := root.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	got := map[string]string{}
	err = root.QueryRows(ctx, `SELECT JSONExtractString(log_comment, 'query_type') AS query_type,
		any(JSONExtractString(log_comment, 'product')),
		any(JSONExtractString(log_comment, 'service_name')),
		max(JSONExtractInt(log_comment, 'team_id')),
		min(JSONExtractInt(log_comment, 'team_id')),
		any(JSONExtractInt(log_comment, 'user_id'))
		FROM system.query_log
		WHERE event_date >= yesterday() AND type = 'QueryFinish'
			AND JSONExtractString(log_comment, 'client_query_id') = `+sqlString(clientQueryID)+`
		GROUP BY query_type`, func(row clickHouseRow) error {
		var queryType, product, service string
		var maxTeam, minTeam, user int64
		if err := row.Scan(&queryType, &product, &service, &maxTeam, &minTeam, &user); err != nil {
			return err
		}
		got[queryType] = fmt.Sprintf("%s/%s/%d-%d/%d", product, service, minTeam, maxTeam, user)
		return nil
	})
	if err != nil {
		t.Fatalf("read query_log: %v", err)
	}
	want := map[string]string{
		"query_range":      fmt.Sprintf("metrics/snuffle/%d-%d/7", e2eTeamID, e2eTeamID),
		"loki_query_range": fmt.Sprintf("logs/snuffle/%d-%d/7", e2eTeamID, e2eTeamID),
		"remote_write":     fmt.Sprintf("metrics/snuffle/%d-%d/7", e2eTeamID, e2eTeamID),
	}
	for queryType, wantTags := range want {
		if got[queryType] != wantTags {
			t.Errorf("query_log %s tags = %q, want %q (all: %v)", queryType, got[queryType], wantTags, got)
		}
	}
}
