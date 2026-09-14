package snuffle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func assertMetricsTenantIsolation(t *testing.T, baseURL string) {
	t.Helper()
	otherTeamID := e2eTeamID + 1
	sharedSeries := e2eWriteRequest().Timeseries[0]
	sharedSeries.Samples = []prompb.Sample{{Timestamp: e2eEndMS, Value: 900}}
	sharedSeries.Exemplars = nil
	postRemoteWriteForTeam(t, baseURL, otherTeamID, &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			sharedSeries,
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "other_tenant_metric"},
					{Name: "job", Value: "other-tenant"},
				},
				Samples: []prompb.Sample{{Timestamp: e2eEndMS, Value: 1}},
			},
		},
	})

	for _, team := range []struct {
		id    uint64
		value string
	}{
		{id: e2eTeamID, value: "25"},
		{id: otherTeamID, value: "900"},
	} {
		for _, query := range []string{e2eCounterMetric, e2eCounterMetric + " + 0"} {
			data := apiGetForTeam[queryDataDTO](t, baseURL, team.id, "/api/v1/query", url.Values{
				"query": {query},
				"time":  {"1700000070"},
			})
			if len(data.Result) != 1 || sampleString(data.Result[0].Value) != team.value {
				t.Fatalf("team %d query %q = %#v, want %s", team.id, query, data, team.value)
			}
		}
	}

	for _, teamID := range []uint64{e2eTeamID, otherTeamID, otherTeamID + 1} {
		series := apiGetForTeam[[]map[string]string](t, baseURL, teamID, "/api/v1/series", url.Values{
			"match[]": {"other_tenant_metric"},
			"start":   {"1700000010"},
			"end":     {"1700000070"},
		})
		wantCount := 0
		if teamID == otherTeamID {
			wantCount = 1
		}
		if len(series) != wantCount {
			t.Fatalf("team %d series = %#v, want %d series", teamID, series, wantCount)
		}
	}
}

func assertLokiRoundTrip(t *testing.T, baseURL string, cfg Config) {
	t.Helper()
	start := time.Now().UTC().Truncate(time.Minute).Add(-3*time.Minute + 15*time.Second)
	firstTimestamp := strconv.FormatInt(start.UnixNano(), 10)
	lastTimestamp := strconv.FormatInt(start.Add(time.Minute).UnixNano(), 10)
	endTimestamp := strconv.FormatInt(start.Add(time.Minute+time.Second).UnixNano(), 10)
	selector := `{service_name="snuffle-e2e"}`

	for _, teamID := range []uint64{e2eTeamID, e2eTeamID + 1} {
		stream := lokiPushStream{
			Stream: map[string]string{"service_name": "snuffle-e2e", "level": "info"},
			Values: [][]any{
				{firstTimestamp, fmt.Sprintf("first request for team %d", teamID)},
				{lastTimestamp, fmt.Sprintf("last request for team %d", teamID)},
			},
		}
		for _, entry := range stream.Values {
			payload, err := json.Marshal(lokiPushRequest{Streams: []lokiPushStream{{
				Stream: stream.Stream,
				Values: [][]any{entry},
			}}})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.Post(fmt.Sprintf("%s/t/%d/loki/api/v1/push", baseURL, teamID), "application/json", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil || resp.StatusCode != http.StatusNoContent {
				t.Fatalf("team %d Loki push status %d: %s (read error: %v)", teamID, resp.StatusCode, body, readErr)
			}
		}
	}

	for _, teamID := range []uint64{e2eTeamID, e2eTeamID + 1} {
		for _, test := range []struct {
			name      string
			query     string
			direction string
			limit     string
			want      [][]string
		}{
			{
				name: "forward", query: selector, direction: "forward", limit: "10",
				want: [][]string{
					{firstTimestamp, fmt.Sprintf("first request for team %d", teamID)},
					{lastTimestamp, fmt.Sprintf("last request for team %d", teamID)},
				},
			},
			{
				name: "backward limit", query: selector, direction: "backward", limit: "1",
				want: [][]string{{lastTimestamp, fmt.Sprintf("last request for team %d", teamID)}},
			},
			{
				name: "line filter", query: selector + ` |= "last"`, direction: "forward", limit: "10",
				want: [][]string{{lastTimestamp, fmt.Sprintf("last request for team %d", teamID)}},
			},
		} {
			t.Run(fmt.Sprintf("team %d %s", teamID, test.name), func(t *testing.T) {
				data := apiGetForTeam[struct {
					ResultType string            `json:"resultType"`
					Result     []logStreamResult `json:"result"`
				}](t, baseURL, teamID, "/loki/api/v1/query_range", url.Values{
					"query": {test.query}, "direction": {test.direction}, "limit": {test.limit},
					"start": {firstTimestamp}, "end": {endTimestamp},
				})
				if data.ResultType != "streams" || len(data.Result) != 1 {
					t.Fatalf("Loki result = %#v, want one stream", data)
				}
				if !reflect.DeepEqual(data.Result[0].Values, test.want) {
					t.Fatalf("Loki values = %#v, want %#v", data.Result[0].Values, test.want)
				}
			})
		}

		data := apiGetForTeam[queryDataDTO](t, baseURL, teamID, "/loki/api/v1/query", url.Values{
			"query": {"sum(count_over_time(" + selector + "[5m]))"},
			"time":  {endTimestamp},
		})
		if data.ResultType != "vector" || len(data.Result) != 1 || sampleString(data.Result[0].Value) != "2" {
			t.Fatalf("team %d Loki count = %#v, want 2", teamID, data)
		}
	}

	for _, teamID := range []uint64{e2eTeamID, e2eTeamID + 1, e2eTeamID + 2} {
		values := apiGetForTeam[[]string](t, baseURL, teamID, "/loki/api/v1/label/service_name/values", url.Values{
			"start": {firstTimestamp}, "end": {endTimestamp},
		})
		if teamID == e2eTeamID+2 {
			if len(values) != 0 {
				t.Fatalf("empty tenant has Loki label values: %#v", values)
			}
		} else if !reflect.DeepEqual(values, []string{"snuffle-e2e"}) {
			t.Fatalf("team %d Loki label values = %#v", teamID, values)
		}
	}

	statsTime := strconv.FormatInt(start.Truncate(time.Minute).Add(2*time.Minute).UnixNano(), 10)
	assertLokiRangeCount(t, baseURL, selector, statsTime)
	if !cfg.postHogLogSchemaLayout() {
		cfg.LogStreamLabelsTable = ""
		mux := http.NewServeMux()
		newServer(cfg).routes(mux)
		withoutLabelIndex := httptest.NewServer(mux)
		defer withoutLabelIndex.Close()
		assertLokiRangeCount(t, withoutLabelIndex.URL, selector, statsTime)
	}
}

func assertLokiRangeCount(t *testing.T, baseURL, selector, timestamp string) {
	t.Helper()
	for _, teamID := range []uint64{e2eTeamID, e2eTeamID + 1} {
		data := apiGetForTeam[queryDataDTO](t, baseURL, teamID, "/loki/api/v1/query_range", url.Values{
			"query": {"sum(count_over_time(" + selector + "[5m]))"},
			"start": {timestamp}, "end": {timestamp}, "step": {"60s"},
		})
		if data.ResultType != "matrix" || len(data.Result) != 1 || len(data.Result[0].Values) != 1 || sampleString(data.Result[0].Values[0]) != "2" {
			t.Fatalf("team %d Loki range count = %#v, want 2", teamID, data)
		}
	}
}

func assertInvalidClickHouseCredentials(t *testing.T, baseURL string) {
	t.Helper()
	for _, invalid := range []bool{true, false} {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/-/ready", nil)
		if err != nil {
			t.Fatal(err)
		}
		if invalid {
			req.SetBasicAuth("snuffle_e2e_nonexistent_user", "invalid-password")
		} else {
			req.SetBasicAuth("default", "")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		wantStatus := http.StatusOK
		if invalid {
			wantStatus = http.StatusServiceUnavailable
		}
		if readErr != nil || resp.StatusCode != wantStatus {
			t.Fatalf("readiness status = %d, want %d: %s (read error: %v)", resp.StatusCode, wantStatus, body, readErr)
		}
	}
}
