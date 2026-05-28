package snuffle

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ClickHouseClient struct {
	cfg         Config
	endpoint    string
	endpointErr error
	httpClient  *http.Client
}

func NewClickHouseClient(cfg Config) *ClickHouseClient {
	endpoint, err := clickHouseEndpoint(cfg)
	return &ClickHouseClient{
		cfg:         cfg,
		endpoint:    endpoint,
		endpointErr: err,
		httpClient: &http.Client{
			Timeout: cfg.CHTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        512,
				MaxIdleConnsPerHost: 256,
				MaxConnsPerHost:     256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func clickHouseEndpoint(cfg Config) (string, error) {
	u, err := url.Parse(cfg.CHURL)
	if err != nil {
		return "", fmt.Errorf("parse CH_URL: %w", err)
	}
	q := u.Query()
	if cfg.CHDatabase != "" {
		q.Set("database", cfg.CHDatabase)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *ClickHouseClient) QueryJSONEachRow(ctx context.Context, sql string, handle func(json.RawMessage) error) error {
	if !strings.Contains(strings.ToUpper(sql), "FORMAT JSONEACHROW") {
		sql += "\nFORMAT JSONEachRow"
	}
	resp, err := c.doQuery(ctx, sql, "text/plain; charset=utf-8", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var rows int64
	defer func() {
		recordClickHouseRead(ctx, rows)
	}()
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		rows++
		if err := handle(json.RawMessage(line)); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (c *ClickHouseClient) ExecWithBody(ctx context.Context, sql string, body io.Reader) error {
	resp, err := c.doQuery(ctx, sql, "text/plain; charset=utf-8", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *ClickHouseClient) doQuery(ctx context.Context, sql, contentType string, body io.Reader) (*http.Response, error) {
	if c.endpointErr != nil {
		return nil, c.endpointErr
	}
	var requestBody io.Reader
	if body == nil {
		requestBody = strings.NewReader(sql)
	} else {
		requestBody = io.MultiReader(strings.NewReader(sql+"\n"), body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, requestBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	if c.cfg.CHUser != "" {
		req.SetBasicAuth(c.cfg.CHUser, c.cfg.CHPassword)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("clickhouse status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func (c *ClickHouseClient) Ping(ctx context.Context) error {
	return c.QueryJSONEachRow(ctx, "SELECT 1 AS ok FORMAT JSONEachRow", func(raw json.RawMessage) error {
		return nil
	})
}

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func quoteIdent(ident string) string {
	if !identifierRE.MatchString(ident) {
		panic(fmt.Sprintf("invalid ClickHouse identifier %q", ident))
	}
	return "`" + ident + "`"
}

func tableName(database, table string) string {
	if strings.Contains(table, ".") {
		parts := strings.Split(table, ".")
		quoted := make([]string, 0, len(parts))
		for _, part := range parts {
			quoted = append(quoted, quoteIdent(part))
		}
		return strings.Join(quoted, ".")
	}
	if database == "" {
		return quoteIdent(table)
	}
	return quoteIdent(database) + "." + quoteIdent(table)
}

func sqlString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return "'" + value + "'"
}

func chTimeMillis(ms int64) string {
	return "fromUnixTimestamp64Milli(" + strconv.FormatInt(ms, 10) + ", 'UTC')"
}

func chTimeMillisExpr(expr string) string {
	return "fromUnixTimestamp64Milli(" + expr + ", 'UTC')"
}

func teamFilter(cfg Config) string {
	return "team_id = " + strconv.FormatUint(cfg.TeamID, 10)
}

func formatDurationSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
}
