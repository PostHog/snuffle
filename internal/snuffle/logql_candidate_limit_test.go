package snuffle

import "testing"

// ENG-024: a pipeline whose stages cannot drop a row (parsers, relabelling)
// fetches only the query limit and parses those rows; only real filters that
// run outside SQL fall back to reading up to the row cap.
func TestLogQLCandidateLimitOnlyOverfetchesForGoFilters(t *testing.T) {
	const limit, maxRows = 1000, 100000
	for _, tt := range []struct {
		query string
		want  int
	}{
		{`{app="api"} |= "error"`, limit + 256},
		{`{app="api"} | json`, limit + 256},
		{`{app="api"} |= "error" | json | line_format "{{.msg}}"`, limit + 256},
		{`{app="api"} | logfmt | label_format svc=app | drop pod`, limit + 256},
		{`{app="api"} | json | status="500"`, maxRows},
		{`{app="api"} | json | __error__=""`, maxRows},
		{`{app="api"} | line_format "{{.msg}}" |= "error"`, maxRows},
	} {
		selector, err := parseLogQLSelector(tt.query)
		if err != nil {
			t.Fatalf("%s: %v", tt.query, err)
		}
		if got := logQLCandidateLimit(*selector, limit, maxRows); got != tt.want {
			t.Errorf("%s: candidate limit = %d, want %d", tt.query, got, tt.want)
		}
	}
}
