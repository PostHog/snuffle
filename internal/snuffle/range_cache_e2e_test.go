package snuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	rangeCacheE2EMetric = "snuffle_e2e_cache_total"
	rangeCacheE2EBase   = int64(1_700_100_000_000)
)

// assertRangeCacheMatchesUncachedResults moves a query window forward, as a
// dashboard does. A server with the range result cache must return the results
// of a server without it, while it evaluates only the steps that are not settled.
func assertRangeCacheMatchesUncachedResults(t *testing.T, ctx context.Context, root *ClickHouseClient, cfg Config) {
	t.Helper()
	uncachedMux := http.NewServeMux()
	newServer(cfg).routes(uncachedMux)
	uncached := httptest.NewServer(uncachedMux)
	defer uncached.Close()

	cachedCfg := cfg
	cachedCfg.RangeCacheMaxBytes = 64 << 20
	cachedCfg.RangeCacheOffset = 5 * time.Minute
	cachedCfg.RangeCacheTTL = time.Hour
	cachedServer := newServer(cachedCfg)
	var clock time.Time
	cachedServer.rangeCache.now = func() time.Time { return clock }
	cachedMux := http.NewServeMux()
	cachedServer.routes(cachedMux)
	cached := httptest.NewServer(cachedMux)
	defer cached.Close()

	postRemoteWrite(t, uncached.URL, rangeCacheE2EWriteRequest())

	const step = time.Minute
	window := func(endMinutes int) url.Values {
		end := rangeCacheE2EBase + int64(endMinutes)*time.Minute.Milliseconds()
		return url.Values{
			"start": {strconv.FormatInt((end-30*time.Minute.Milliseconds())/1000, 10)},
			"end":   {strconv.FormatInt(end/1000, 10)},
			"step":  {step.String()},
		}
	}
	queries := []string{
		`sum(rate(M[2m])) by (code)`,
		`sum(increase(M[5m]))`,
		`M`,
		`count(M) by (job)`,
		`max_over_time(M[3m])`,
		`sum(rate(M[1m])) by (code) / 2`,
	}
	for _, query := range queries {
		query = strings.ReplaceAll(query, "M", rangeCacheE2EMetric)
		t.Run(query, func(t *testing.T) {
			for index, endMinutes := range []int{60, 65, 70, 75, 80, 85, 90} {
				values := window(endMinutes)
				values.Set("query", query)
				// The request arrives just after its last step, as a live dashboard request does.
				clock = time.UnixMilli(rangeCacheE2EBase + int64(endMinutes)*time.Minute.Milliseconds()).Add(30 * time.Second)
				want, _, _ := rangeCacheE2EGet(t, uncached.URL, values, "", "")
				got, status, _ := rangeCacheE2EGet(t, cached.URL, values, "", "")
				wantStatus := rangeCachePartial
				if index == 0 {
					wantStatus = rangeCacheMiss
				}
				if status != wantStatus {
					t.Fatalf("window ending at minute %d: cache status %q, want %q", endMinutes, status, wantStatus)
				}
				if len(want.Result) == 0 {
					t.Fatalf("window ending at minute %d: the server without a cache returned no series", endMinutes)
				}
				assertSameRangeResult(t, fmt.Sprintf("window ending at minute %d", endMinutes), got, want)
			}

			// Every step of this earlier window is settled, so the cache answers it without an evaluation.
			values := window(70)
			values.Set("query", query)
			want, _, _ := rangeCacheE2EGet(t, uncached.URL, values, "", "")
			got, status, _ := rangeCacheE2EGet(t, cached.URL, values, "", "")
			if status != rangeCacheHit {
				t.Fatalf("settled window: cache status %q, want %q", status, rangeCacheHit)
			}
			assertSameRangeResult(t, "settled window", got, want)
		})
	}

	t.Run("a query that depends on the range is not cached", func(t *testing.T) {
		values := window(60)
		values.Set("query", rangeCacheE2EMetric+" @ end()")
		for range 2 {
			want, _, _ := rangeCacheE2EGet(t, uncached.URL, values, "", "")
			got, status, _ := rangeCacheE2EGet(t, cached.URL, values, "", "")
			if status != rangeCacheBypass {
				t.Fatalf("cache status %q, want %q", status, rangeCacheBypass)
			}
			assertSameRangeResult(t, "@ end()", got, want)
		}
	})

	// ClickHouse owns authentication. A cached result must not answer a user that ClickHouse no longer accepts.
	t.Run("a cached result needs credentials that ClickHouse accepts", func(t *testing.T) {
		const user, password = "snuffle_e2e_cache_user", "snuffle-e2e-cache-password"
		for _, statement := range []string{
			"DROP USER IF EXISTS " + quoteIdent(user),
			"CREATE USER " + quoteIdent(user) + " IDENTIFIED BY " + sqlString(password),
			"GRANT ALL ON " + quoteIdent(cfg.CHDatabase) + ".* TO " + quoteIdent(user),
		} {
			if err := root.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
		defer func() { _ = root.Exec(ctx, "DROP USER IF EXISTS "+quoteIdent(user)) }()

		values := window(70)
		values.Set("query", "sum("+rangeCacheE2EMetric+") by (code)")
		if _, status, code := rangeCacheE2EGet(t, cached.URL, values, user, password); code != http.StatusOK || status != rangeCacheMiss {
			t.Fatalf("first request: HTTP %d with cache status %q, want a miss", code, status)
		}
		if _, status, code := rangeCacheE2EGet(t, cached.URL, values, user, password); code != http.StatusOK || status != rangeCacheHit {
			t.Fatalf("second request: HTTP %d with cache status %q, want a hit", code, status)
		}
		if _, _, code := rangeCacheE2EGet(t, cached.URL, values, user, "wrong-password"); code == http.StatusOK {
			t.Fatal("a request with a wrong password read the cached result")
		}
		if err := root.Exec(ctx, "REVOKE ALL ON "+quoteIdent(cfg.CHDatabase)+".* FROM "+quoteIdent(user)); err != nil {
			t.Fatal(err)
		}
		if _, _, code := rangeCacheE2EGet(t, cached.URL, values, user, password); code == http.StatusOK {
			t.Fatal("a user without a grant on the samples read the cached result")
		}
		if err := root.Exec(ctx, "DROP USER "+quoteIdent(user)); err != nil {
			t.Fatal(err)
		}
		if _, _, code := rangeCacheE2EGet(t, cached.URL, values, user, password); code == http.StatusOK {
			t.Fatal("a dropped user read the cached result")
		}
	})
}

// rangeCacheE2EGet returns the result, the cache status header and the HTTP
// status of a range query. A request without a user must succeed. A request
// with a user returns its HTTP status, so the caller can expect a refusal.
func rangeCacheE2EGet(t *testing.T, baseURL string, values url.Values, user, password string) (queryDataDTO, string, int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/t/%d/api/v1/query_range?%s", baseURL, e2eTeamID, values.Encode()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if user != "" {
		req.SetBasicAuth(user, password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded apiResponseDTO[queryDataDTO]
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("decode range response %s: %v", body, err)
		}
	} else if user == "" {
		t.Fatalf("range query %s: HTTP %d: %s", values.Get("query"), resp.StatusCode, body)
	}
	return decoded.Data, resp.Header.Get(rangeCacheHeader), resp.StatusCode
}

func assertSameRangeResult(t *testing.T, name string, got, want queryDataDTO) {
	t.Helper()
	key := func(metric map[string]string) string {
		parts := make([]string, 0, len(metric))
		for label, value := range metric {
			parts = append(parts, label+"="+value)
		}
		slices.Sort(parts)
		return strings.Join(parts, ",")
	}
	wantByKey := make(map[string][][]any, len(want.Result))
	for _, series := range want.Result {
		wantByKey[key(series.Metric)] = series.Values
	}
	if got.ResultType != want.ResultType || len(got.Result) != len(want.Result) {
		t.Fatalf("%s: cached result has type %q and %d series, uncached has type %q and %d series", name, got.ResultType, len(got.Result), want.ResultType, len(want.Result))
	}
	for _, series := range got.Result {
		wantValues, ok := wantByKey[key(series.Metric)]
		if !ok {
			t.Fatalf("%s: cached series %v is missing from the uncached result", name, series.Metric)
		}
		if len(series.Values) != len(wantValues) {
			t.Fatalf("%s: series %v has %d cached points and %d uncached points\ncached:   %v\nuncached: %v", name, series.Metric, len(series.Values), len(wantValues), series.Values, wantValues)
		}
		for i := range series.Values {
			gotValue, _ := strconv.ParseFloat(sampleString(series.Values[i]), 64)
			wantValue, _ := strconv.ParseFloat(sampleString(wantValues[i]), 64)
			if series.Values[i][0] != wantValues[i][0] || math.Abs(gotValue-wantValue) > 1e-9*math.Max(1, math.Abs(wantValue)) {
				t.Fatalf("%s: series %v point %d: cached %v, uncached %v", name, series.Metric, i, series.Values[i], wantValues[i])
			}
		}
	}
}

// rangeCacheE2EWriteRequest holds 90 minutes of counters with one sample every 15 seconds.
func rangeCacheE2EWriteRequest() *prompb.WriteRequest {
	series := func(code string, fromMinute, toMinute int, value func(minute float64) float64) prompb.TimeSeries {
		out := prompb.TimeSeries{Labels: []prompb.Label{
			{Name: labels.MetricName, Value: rangeCacheE2EMetric},
			{Name: "code", Value: code},
			{Name: "job", Value: "api"},
		}}
		for offset := time.Duration(fromMinute) * time.Minute; offset <= time.Duration(toMinute)*time.Minute; offset += 15 * time.Second {
			out.Samples = append(out.Samples, prompb.Sample{Timestamp: rangeCacheE2EBase + offset.Milliseconds(), Value: value(offset.Minutes())})
		}
		return out
	}
	return &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		series("200", 0, 90, func(minute float64) float64 { return 100 + 8*minute }),
		// This counter resets while the window moves over it.
		series("500", 0, 90, func(minute float64) float64 {
			if minute >= 68 {
				return 4 * (minute - 68)
			}
			return 1000 + 4*minute
		}),
		// This series starts in a step that only a later window holds.
		series("404", 72, 90, func(minute float64) float64 { return 12 * (minute - 72) }),
		// This series stops, so later windows hold it only in their first steps.
		series("302", 0, 52, func(minute float64) float64 { return 20 * minute }),
	}}
}
