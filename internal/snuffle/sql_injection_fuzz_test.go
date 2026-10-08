package snuffle

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// sqlInjectionCanary is spliced into every payload. It may appear in SQL only
// inside a string literal or a quoted identifier.
const sqlInjectionCanary = "zqxinj"

// sqlInjectionTargets put fuzzed text where a query language accepts user text.
// {{q}} is the value as a quoted string. {{r}} is the name as-is and {{p}} is
// the name as a path segment. The canary goes into the value, or into the name
// when the template has no value.
var sqlInjectionTargets = []struct {
	path, param, template string
}{
	{"/api/v1/query", "query", `m{job={{q}}}`},
	{"/api/v1/query", "query", `m{job=~{{q}}}`},
	{"/api/v1/query", "query", `m{job!~{{q}}}`},
	{"/api/v1/query", "query", `{__name__={{q}}}`},
	{"/api/v1/query", "query", `{__name__=~{{q}}}`},
	{"/api/v1/query", "query", `m{{{r}}="x"}`},
	{"/api/v1/query", "query", `{{r}}`},
	{"/api/v1/query", "query", `sum by ({{r}}) (m)`},
	{"/api/v1/query", "query", `count(count by (job) (m{job={{q}}}))`},
	{"/api/v1/query", "query", `topk(3, sum by (job) (m{job!={{q}}}))`},
	{"/api/v1/query_range", "query", `sum(rate(m{job={{q}}}[5m]))`},
	{"/api/v1/query_range", "query", `sum by (job) (increase(m{job=~{{q}}}[5m]))`},
	{"/api/v1/query_range", "query", `sum by ({{r}}) (rate(m[5m]))`},
	{"/api/v1/query_range", "query", `histogram_quantile(0.9, sum by (le) (rate(m_bucket{job={{q}}}[5m])))`},
	{"/api/v1/series", "match[]", `m{job={{q}}}`},
	{"/api/v1/labels", "match[]", `{__name__=~{{q}}}`},
	{"/api/v1/label/{{p}}/values", "match[]", `m`},
	{"/api/v1/label/job/values", "match[]", `m{job=~{{q}}}`},
	{"/api/v1/metadata", "metric", `{{r}}`},
	{"/api/v1/query_exemplars", "query", `m{job={{q}}}`},
	{"/t/7/api/v1/query", "query", `m{job={{q}}}`},
	{"/loki/api/v1/query_range", "query", `{app={{q}}}`},
	{"/loki/api/v1/query_range", "query", `{app=~{{q}}}`},
	{"/loki/api/v1/query_range", "query", `{app!~{{q}}}`},
	{"/loki/api/v1/query_range", "query", `{app="api"} |= {{q}}`},
	{"/loki/api/v1/query_range", "query", `{app="api"} |~ {{q}}`},
	{"/loki/api/v1/query_range", "query", `{app="api"} |> {{q}}`},
	{"/loki/api/v1/query_range", "query", `{app="api"} | json | status={{q}}`},
	{"/loki/api/v1/query_range", "query", `{app="api"} | logfmt | level=~{{q}}`},
	{"/loki/api/v1/query_range", "query", `{app="api"} | json | {{r}}="x"`},
	{"/loki/api/v1/query_range", "query", `sum by ({{r}}) (count_over_time({app="api"}[1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(count_over_time({app={{q}}} |= {{q}} [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(rate({app="api"} | json | status=~{{q}} [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(sum_over_time({app="api"} | regexp {{q}} | unwrap v [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(sum_over_time({app="api"} | regexp {{q}} | unwrap {{r}} [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(sum_over_time({app="api"} | regexp "(?P<v>[0-9]+)" | {{r}}={{q}} | unwrap v [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(sum_over_time({app="api"} | pattern {{q}} | unwrap v [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum(sum_over_time({app="api"} | json v={{q}} | unwrap v [1m]))`},
	{"/loki/api/v1/query_range", "query", `sum by (app) (avg_over_time({app="api"} | logfmt | unwrap duration({{r}}) [1m]))`},
	{"/loki/api/v1/query_range", "query", `quantile_over_time(0.9, {app="api"} | json | unwrap {{r}} [1m])`},
	{"/loki/api/v1/query_range", "query", `sum(bytes_over_time({app="api"} |= {{q}} [1m]))`},
	{"/loki/api/v1/query", "query", `sum by (app) (count_over_time({app={{q}}}[1m]))`},
	{"/loki/api/v1/label/{{p}}/values", "query", `{app="api"}`},
	{"/loki/api/v1/label/app/values", "query", `{app=~{{q}}}`},
	{"/loki/api/v1/labels", "query", `{app={{q}}}`},
	{"/loki/api/v1/series", "match[]", `{app={{q}}}`},
	{"/loki/api/v1/index/stats", "query", `{app={{q}}}`},
	{"/t/7/loki/api/v1/query_range", "query", `{app={{q}}}`},
}

var sqlInjectionNameSeeds = []string{`job`, `__regex`, `__name__`, `__error__`, `v`, `le`}

var sqlInjectionSeeds = []string{
	``,
	`x`,
	`'`,
	`\'`,
	`\\'`,
	`'' OR 1=1 --`,
	`') OR 1=1 --`,
	`'), (SELECT 1) --`,
	`\'), (SELECT * FROM url('http://example.com')) /*`,
	`(?P<v>[0-9]+)`,
	`(?P<v>[0-9]+)'), 1) --`,
	`<v> <_>`,
	`a.b`,
	"x`y",
	`x"y`,
	"\x00",
}

func FuzzHTTPQueriesKeepUserTextInSQLLiterals(f *testing.F) {
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	f.Cleanup(func() { slog.SetDefault(previousLogger) })
	recorder := &sqlRecorder{}
	previousOpen := openClickHouse
	openClickHouse = func(Config, string, string) (clickhouse.Conn, error) { return recorder, nil }
	f.Cleanup(func() { openClickHouse = previousOpen })

	var muxes []*http.ServeMux
	for _, layout := range []string{"current", "posthog"} {
		f.Setenv("CH_SCHEMA_LAYOUT", layout)
		mux := http.NewServeMux()
		newServer(ConfigFromEnv()).routes(mux)
		muxes = append(muxes, mux)
	}
	for target := range sqlInjectionTargets {
		for _, name := range sqlInjectionNameSeeds {
			for _, seed := range sqlInjectionSeeds {
				f.Add(uint8(target), name, seed, uint8(len(seed)))
			}
		}
	}

	f.Fuzz(func(t *testing.T, target uint8, name, value string, canaryAt uint8) {
		spec := sqlInjectionTargets[int(target)%len(sqlInjectionTargets)]
		if strings.Contains(spec.template, "{{q}}") {
			value = withCanary(value, canaryAt)
		} else {
			name = withCanary(name, canaryAt)
		}
		fill := strings.NewReplacer("{{q}}", strconv.Quote(value), "{{r}}", name, "{{p}}", url.PathEscape(name))
		params := url.Values{
			spec.param: {fill.Replace(spec.template)},
			"start":    {"1700000000"},
			"end":      {"1700003600"},
			"step":     {"60"},
		}
		for _, mux := range muxes {
			recorder.reset()
			req := httptest.NewRequest(http.MethodGet, fill.Replace(spec.path)+"?"+params.Encode(), nil)
			mux.ServeHTTP(httptest.NewRecorder(), req)
			for _, sql := range recorder.sqls() {
				if problem := sqlOutsideLiteralsProblem(sql); problem != "" {
					t.Fatalf("%s %s: %s\nSQL: %s", req.URL.Path, params.Get(spec.param), problem, sql)
				}
			}
		}
	})
}

func withCanary(text string, at uint8) string {
	i := int(at) % (len(text) + 1)
	return text[:i] + sqlInjectionCanary + text[i:]
}

// sqlOutsideLiteralsProblem lexes ClickHouse SQL and checks the text outside
// string literals and quoted identifiers.
func sqlOutsideLiteralsProblem(sql string) string {
	var code strings.Builder
	for i := 0; i < len(sql); i++ {
		quote := sql[i]
		if quote != '\'' && quote != '"' && quote != '`' {
			code.WriteByte(quote)
			continue
		}
		closed := false
		for i++; i < len(sql); i++ {
			if sql[i] == '\\' {
				i++
				continue
			}
			if sql[i] == quote {
				if i+1 < len(sql) && sql[i+1] == quote {
					i++
					continue
				}
				closed = true
				break
			}
		}
		if !closed {
			return "unterminated quoted text"
		}
		code.WriteString("?")
	}
	outside := strings.ToLower(code.String())
	if strings.Contains(outside, sqlInjectionCanary) {
		return "user text outside a SQL literal"
	}
	for _, token := range []string{";", "--", "/*", "#"} {
		if strings.Contains(outside, token) {
			return "statement separator or comment " + strconv.Quote(token) + " outside a SQL literal"
		}
	}
	for _, b := range []byte(outside) {
		if (b < 0x20 && b != '\n' && b != '\t') || b >= 0x7f {
			return "control or non-ASCII byte outside a SQL literal"
		}
	}
	return ""
}

func TestSQLOutsideLiteralsProblem(t *testing.T) {
	for sql, bad := range map[string]bool{
		`SELECT 'zqxinj' FROM t`:                  false,
		`SELECT 'a\'zqxinj' FROM t`:               false,
		`SELECT 'a''zqxinj' FROM t`:               false,
		"SELECT `zqxinj` FROM t":                  false,
		`SELECT 'a\\' zqxinj FROM t`:              true,
		`SELECT 'a'' FROM t`:                      true,
		`SELECT 1 -- 'x'`:                         true,
		`SELECT extractGroups(body, 'x'), zqxinj`: true,
	} {
		if got := sqlOutsideLiteralsProblem(sql) != ""; got != bad {
			t.Errorf("sqlOutsideLiteralsProblem(%q) flagged = %v, want %v", sql, got, bad)
		}
	}
}

// sqlRecorder is a ClickHouse connection that records each statement and
// returns no rows, so handlers continue to any later queries.
type sqlRecorder struct {
	driver.Conn
	mu        sync.Mutex
	statement []string
}

func (r *sqlRecorder) record(sql string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statement = append(r.statement, sql)
}

func (r *sqlRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statement = nil
}

func (r *sqlRecorder) sqls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.statement...)
}

func (r *sqlRecorder) Query(_ context.Context, sql string, _ ...any) (driver.Rows, error) {
	r.record(sql)
	return emptyRows{}, nil
}

func (r *sqlRecorder) Exec(_ context.Context, sql string, _ ...any) error {
	r.record(sql)
	return nil
}

func (r *sqlRecorder) Close() error { return nil }

type emptyRows struct{ driver.Rows }

func (emptyRows) Next() bool   { return false }
func (emptyRows) Err() error   { return nil }
func (emptyRows) Close() error { return nil }
