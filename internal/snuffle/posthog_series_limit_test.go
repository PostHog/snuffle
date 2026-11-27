package snuffle

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/model/labels"
)

const testSeriesLimitError = "series limit exceeded (10); tighten matchers or increase CH_MAX_SERIES"

// The selected_series CTE stops at CH_MAX_SERIES rows in no particular order.
// Every query built on it must fail when the cap is reached, or it reads an
// arbitrary subset of the series and returns low numbers that change from
// run to run.
func TestPostHogSelectedSeriesQueriesFailAtSeriesLimit(t *testing.T) {
	recorder := &sqlRecorder{}
	withClickHouseConn(t, recorder)
	t.Setenv("CH_SCHEMA_LAYOUT", "posthog")
	t.Setenv("CH_MAX_SERIES", "10")
	mux := http.NewServeMux()
	newServer(ConfigFromEnv()).routes(mux)

	guard := "throwIf(count() >= 10, '" + testSeriesLimitError + "')"
	for _, test := range []struct{ path, query string }{
		{"/api/v1/query", `sum by (region) (up)`},
		{"/api/v1/query", `count(up{region="eu"})`},
		{"/api/v1/query", `topk(3, up{region="eu"})`},
		{"/api/v1/query", `count(count by (region) (up))`},
		{"/api/v1/query", `sum by (region) (up{host="a"} / 5 or up{host="b"} / 5)`},
		{"/api/v1/query_range", `sum by (region) (rate(up[5m]))`},
		{"/api/v1/query_range", `sum(rate(up{region="eu"}[5m]))`},
	} {
		recorder.reset()
		params := url.Values{"query": {test.query}, "time": {"1700003600"}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"60"}}
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, test.path+"?"+params.Encode(), nil))
		planned := 0
		for _, sql := range recorder.sqls() {
			if !strings.Contains(sql, "selected_series") {
				continue
			}
			planned++
			if !strings.Contains(sql, guard) {
				t.Errorf("%s %s: selected_series query has no series limit check:\n%s", test.path, test.query, sql)
			}
			if strings.Contains(sql, "IN (SELECT series_id FROM selected_series)") {
				t.Errorf("%s %s: samples follow selected_series without the series limit check:\n%s", test.path, test.query, sql)
			}
		}
		if planned == 0 {
			t.Errorf("%s %s: no selected_series query ran; the case no longer covers a series lookup plan: %q", test.path, test.query, recorder.sqls())
		}
	}
}

func TestPostHogSelectedSeriesLimitOffWithoutCap(t *testing.T) {
	cfg := Config{CHDatabase: "default", SchemaLayout: "posthog", SeriesTable: "metrics4_series", SamplesTable: "metrics4_samples"}
	plan := newPostHogQueryPlan(cfg, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")}, []string{"region"}, 1000, 2000, false)
	where := strings.Join(plan.sampleWhere(), " AND ")
	if strings.Contains(where, "throwIf") || !strings.Contains(where, "series_fingerprint IN (SELECT series_id FROM selected_series)") {
		t.Fatalf("an uncapped series lookup needs no limit check: %s", where)
	}
}

// ClickHouse reports the failed check as a FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
// exception. The API returns the same 422 the Go-side series checks return.
func TestPostHogSeriesLimitExceptionIsA422(t *testing.T) {
	message := testSeriesLimitError + ": while executing 'FUNCTION throwIf(greaterOrEquals(count(), 10_UInt16) :: 5, '" + testSeriesLimitError + "'_String :: 3) -> throwIf(greaterOrEquals(count(), 10_UInt16), 'series limit exceeded (10); tighten matchers or increas... UInt8 : 2'"
	for name, conn := range map[string]*failingConn{
		"query start": {queryErr: &clickhouse.Exception{Code: 395, Name: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO", Message: message}},
		"result read": {rowsErr: &clickhouse.Exception{Code: 395, Name: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO", Message: message}},
	} {
		t.Run(name, func(t *testing.T) {
			withClickHouseConn(t, conn)
			t.Setenv("CH_SCHEMA_LAYOUT", "posthog")
			t.Setenv("CH_MAX_SERIES", "10")
			mux := http.NewServeMux()
			newServer(ConfigFromEnv()).routes(mux)
			response := httptest.NewRecorder()
			params := url.Values{"query": {`sum by (region) (up)`}, "time": {"1700003600"}}
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/query?"+params.Encode(), nil))
			var body apiResponseDTO[json.RawMessage]
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %s: %v", response.Body.String(), err)
			}
			if response.Code != http.StatusUnprocessableEntity || body.Error != testSeriesLimitError {
				t.Fatalf("status %d error %q, want 422 %q", response.Code, body.Error, testSeriesLimitError)
			}
		})
	}
	other := &clickhouse.Exception{Code: 395, Name: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO", Message: "Value passed to 'throwIf' function is non-zero"}
	if err := clickHouseQueryError(other, "read clickhouse query result"); !errors.Is(err, other) {
		t.Fatalf("an unrelated throwIf must keep the ClickHouse error, got %v", err)
	}
}

// The series scan stops at CH_MAX_SERIES rows, so the check counts rows read,
// not the series kept after Go-side matching. Otherwise a matcher evaluated in
// Go (such as an empty-value matcher) or rows split between histogram sources
// and component series hide a truncated scan.
func TestPostHogSeriesScansCountRowsRead(t *testing.T) {
	cfg := Config{CHDatabase: "default", SchemaLayout: "posthog", SeriesTable: "metrics4_series", SamplesTable: "metrics4_samples", MaxSeries: 4, TeamID: 42}
	rows := func(names ...string) *failingConn {
		conn := &failingConn{}
		for _, name := range names {
			conn.rows = append(conn.rows, []any{uint64(len(conn.rows) + 1), name, "api", map[string]string{"region": "eu"}, map[string]string{}})
		}
		return conn
	}
	t.Run("float series filtered in Go", func(t *testing.T) {
		withClickHouseConn(t, rows("up", "up", "up", "up"))
		querier := &CHQuerier{queryable: NewCHQueryable(NewClickHouseClient(cfg), cfg)}
		_, err := querier.selectPostHogFloatSeries(context.Background(), 1000, 2000,
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
			labels.MustNewMatcher(labels.MatchEqual, "region", ""),
		)
		if err == nil || !strings.Contains(err.Error(), "series limit exceeded (4)") {
			t.Fatalf("a full scan whose rows were all filtered out must fail, got %v", err)
		}
	})
	t.Run("histogram sources and component series", func(t *testing.T) {
		withClickHouseConn(t, rows("duration", "duration_count", "duration", "duration_count"))
		querier := &CHQuerier{queryable: NewCHQueryable(NewClickHouseClient(cfg), cfg)}
		_, _, _, err := querier.selectPostHogExactHistogramMetadata(context.Background(), 1000, 2000, postHogHistogramAlias{baseName: "duration", suffix: "_count"},
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "duration_count")})
		if err == nil || !strings.Contains(err.Error(), "series limit exceeded (4)") {
			t.Fatalf("a full scan split between sources and series must fail, got %v", err)
		}
	})
	t.Run("under the limit", func(t *testing.T) {
		withClickHouseConn(t, rows("up", "up", "up"))
		querier := &CHQuerier{queryable: NewCHQueryable(NewClickHouseClient(cfg), cfg)}
		series, err := querier.selectPostHogFloatSeries(context.Background(), 1000, 2000, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
		if err != nil || len(series) != 3 {
			t.Fatalf("got %d series, %v; want 3", len(series), err)
		}
	})
}

func withClickHouseConn(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	previousOpen := openClickHouse
	openClickHouse = func(Config, string, string) (clickhouse.Conn, error) { return conn, nil }
	t.Cleanup(func() { openClickHouse = previousOpen })
}

// failingConn answers every query with the same rows, then an optional error,
// or fails the query outright.
type failingConn struct {
	driver.Conn
	queryErr error
	rowsErr  error
	rows     [][]any
}

func (c *failingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fixedRows{rows: c.rows, err: c.rowsErr, index: -1}, nil
}

func (c *failingConn) Close() error { return nil }

type fixedRows struct {
	driver.Rows
	rows  [][]any
	err   error
	index int
}

func (r *fixedRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fixedRows) Scan(dest ...any) error {
	for i, value := range r.rows[r.index] {
		switch target := dest[i].(type) {
		case *uint64:
			*target = value.(uint64)
		case *uint8:
			*target = value.(uint8)
		case *string:
			*target = value.(string)
		case *map[string]string:
			*target = value.(map[string]string)
		default:
			return errors.New("fixedRows: unsupported scan target")
		}
	}
	return nil
}

func (r *fixedRows) Err() error   { return r.err }
func (r *fixedRows) Close() error { return nil }
