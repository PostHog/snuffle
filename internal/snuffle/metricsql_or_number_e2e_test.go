package snuffle

import (
	"net/url"
	"reflect"
	"strconv"
	"testing"
)

// assertOrWithNumber checks MetricsQL's `x or 0`: the number is a series
// without labels, so a missing aggregate becomes 0.
func assertOrWithNumber(t *testing.T, baseURL string) {
	t.Helper()
	const missing = "snuffle_e2e_does_not_exist"
	running := `sum(` + e2eRunningMetric + `)`
	for _, c := range []struct {
		query string
		want  float64
		// same, when set, is a query whose result c.query must match point for
		// point instead of the constant want.
		same string
	}{
		{query: `sum(` + missing + `) or 0`, want: 0},
		{query: running + ` or 0`, same: running},
		{query: `0 or ` + running, want: 0},
		{query: `(0/0) or ` + running, same: running},
		{query: running + ` / ((sum(` + missing + `) or 0) + ` + running + `)`, want: 1},
	} {
		t.Run(c.query, func(t *testing.T) {
			if c.same != "" {
				assertSameResult(t, baseURL, c.query, c.same)
				return
			}
			instant, errText := queryOrError(t, baseURL, "/api/v1/query", url.Values{"query": {c.query}, "time": {"1700000070"}})
			if errText != "" {
				t.Fatalf("instant: %s", errText)
			}
			if len(instant.Result) != 1 {
				t.Fatalf("instant: %d series, want 1: %#v", len(instant.Result), instant.Result)
			}
			if got, _ := strconv.ParseFloat(instant.Result[0].Value[1].(string), 64); got != c.want {
				t.Errorf("instant = %v, want %v", got, c.want)
			}
			ranged, errText := queryOrError(t, baseURL, "/api/v1/query_range", url.Values{"query": {c.query}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"10"}})
			if errText != "" {
				t.Fatalf("range: %s", errText)
			}
			if len(ranged.Result) != 1 || len(ranged.Result[0].Values) != 4 {
				t.Fatalf("range: want one series with 4 points: %#v", ranged.Result)
			}
			for _, point := range ranged.Result[0].Values {
				if got, _ := strconv.ParseFloat(point[1].(string), 64); got != c.want {
					t.Errorf("range at %v = %v, want %v", point[0], got, c.want)
				}
			}
		})
	}
}

// assertSameResult checks that query returns the same single series values as
// reference, both instant and over a range.
func assertSameResult(t *testing.T, baseURL, query, reference string) {
	t.Helper()
	instantParams := func(q string) url.Values { return url.Values{"query": {q}, "time": {"1700000070"}} }
	rangeParams := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {"1700000040"}, "end": {"1700000070"}, "step": {"10"}}
	}
	for _, kind := range []struct {
		path   string
		params func(string) url.Values
		points func(sampleResultDTO) [][]any
	}{
		{"/api/v1/query", instantParams, func(r sampleResultDTO) [][]any { return [][]any{r.Value} }},
		{"/api/v1/query_range", rangeParams, func(r sampleResultDTO) [][]any { return r.Values }},
	} {
		got, errText := queryOrError(t, baseURL, kind.path, kind.params(query))
		if errText != "" {
			t.Fatalf("%s %s: %s", kind.path, query, errText)
		}
		want, errText := queryOrError(t, baseURL, kind.path, kind.params(reference))
		if errText != "" {
			t.Fatalf("%s %s: %s", kind.path, reference, errText)
		}
		if len(got.Result) != 1 || len(want.Result) != 1 {
			t.Fatalf("%s: want one series each, got %#v and %#v", kind.path, got.Result, want.Result)
		}
		gotPoints, wantPoints := kind.points(got.Result[0]), kind.points(want.Result[0])
		if len(gotPoints) == 0 || !reflect.DeepEqual(gotPoints, wantPoints) {
			t.Errorf("%s: %s = %v, want %v (from %s)", kind.path, query, gotPoints, wantPoints, reference)
		}
	}
}
