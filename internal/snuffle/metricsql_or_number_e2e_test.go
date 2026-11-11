package snuffle

import (
	"net/url"
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
	}{
		{`sum(` + missing + `) or 0`, 0},
		{running + ` or 0`, 7},
		{`0 or ` + running, 0},
		{running + ` / ((sum(` + missing + `) or 0) + ` + running + `)`, 1},
	} {
		t.Run(c.query, func(t *testing.T) {
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
