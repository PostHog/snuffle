package snuffle

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseClient struct {
	conn    clickhouse.Conn
	connErr error
}

func NewClickHouseClient(cfg Config) *ClickHouseClient {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     clickHouseAddrs(cfg),
		Auth: clickhouse.Auth{
			Database: cfg.CHDatabase,
			Username: cfg.CHUser,
			Password: cfg.CHPassword,
		},
		Settings: clickhouse.Settings{
			"allow_experimental_time_series_aggregate_functions": 1,
		},
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout:     cfg.CHTimeout,
		MaxOpenConns:    32,
		MaxIdleConns:    8,
		ConnMaxLifetime: time.Hour,
	})
	return &ClickHouseClient{
		conn:    conn,
		connErr: err,
	}
}

func clickHouseAddrs(cfg Config) []string {
	addr := strings.TrimSpace(cfg.CHAddr)
	if addr == "" {
		addr = "localhost:9000"
	}
	parts := strings.Split(addr, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			addrs = append(addrs, part)
		}
	}
	if len(addrs) == 0 {
		return []string{"localhost:9000"}
	}
	return addrs
}

type clickHouseRow interface {
	Scan(dest ...any) error
}

func (c *ClickHouseClient) QueryRows(ctx context.Context, sql string, handle func(clickHouseRow) error) error {
	return c.queryRows(ctx, sql, handle)
}

func (c *ClickHouseClient) QueryRowsWithExternalUInt64s(ctx context.Context, table, column string, values []uint64, sql string, handle func(clickHouseRow) error) error {
	external, err := ext.NewTable(table, ext.Column(column, "UInt64"))
	if err != nil {
		return err
	}
	for _, value := range values {
		if err := external.Append(value); err != nil {
			return err
		}
	}
	return c.queryRows(clickhouse.Context(ctx, clickhouse.WithExternalTable(external)), sql, handle)
}

func (c *ClickHouseClient) queryRows(ctx context.Context, sql string, handle func(clickHouseRow) error) error {
	if c.connErr != nil {
		return fmt.Errorf("connect clickhouse: %w", c.connErr)
	}
	result, err := c.conn.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("start clickhouse query: %w", err)
	}
	defer result.Close()

	var rowCount int64
	defer func() {
		recordClickHouseRead(ctx, rowCount)
	}()
	for result.Next() {
		rowCount++
		if err := handle(result); err != nil {
			return fmt.Errorf("handle clickhouse query row %d: %w", rowCount, err)
		}
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("read clickhouse query result after %d rows: %w", rowCount, err)
	}
	return nil
}

func (c *ClickHouseClient) Exec(ctx context.Context, sql string) error {
	if c.connErr != nil {
		return fmt.Errorf("connect clickhouse: %w", c.connErr)
	}
	if err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("execute clickhouse statement: %w", err)
	}
	return nil
}

type clickHouseBatch = driver.Batch

func (c *ClickHouseClient) InsertColumns(ctx context.Context, sql string, appendColumns func(clickHouseBatch) (int, error)) error {
	return c.insertColumns(ctx, sql, true, appendColumns)
}

func (c *ClickHouseClient) InsertColumnsSync(ctx context.Context, sql string, appendColumns func(clickHouseBatch) (int, error)) error {
	return c.insertColumns(ctx, sql, false, appendColumns)
}

func (c *ClickHouseClient) insertColumns(ctx context.Context, sql string, async bool, appendColumns func(clickHouseBatch) (int, error)) error {
	if c.connErr != nil {
		return fmt.Errorf("connect clickhouse: %w", c.connErr)
	}
	insertMode := "native insert"
	if async {
		ctx = clickhouse.Context(ctx, clickhouse.WithAsync(false))
		insertMode = "native async insert (async_insert=1 wait_for_async_insert=0)"
	}
	batch, err := c.conn.PrepareBatch(ctx, sql)
	if err != nil {
		return fmt.Errorf("prepare %s: %w", insertMode, err)
	}
	sent := false
	defer func() {
		if !sent {
			_ = batch.Close()
		}
	}()
	count, err := appendColumns(batch)
	if err != nil {
		return fmt.Errorf("append native insert columns: %w", err)
	}
	if count == 0 {
		sent = true
		if err := batch.Close(); err != nil {
			return fmt.Errorf("close empty %s batch: %w", insertMode, err)
		}
		return nil
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send %s rows=%d: %w", insertMode, count, err)
	}
	sent = true
	recordClickHouseWrite(ctx, int64(count))
	return nil
}

func (c *ClickHouseClient) Ping(ctx context.Context) error {
	if c.connErr != nil {
		return c.connErr
	}
	return c.conn.Ping(ctx)
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
