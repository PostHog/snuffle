package snuffle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// busyConn rejects its first `busy` queries the way ClickHouse does when a
// user is over max_concurrent_queries_for_user, then runs them.
type busyConn struct {
	driver.Conn
	busy  int32
	err   error
	calls atomic.Int32
}

func (c *busyConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if c.calls.Add(1) <= c.busy {
		if c.err != nil {
			return nil, c.err
		}
		return nil, &clickhouse.Exception{Code: 202, Name: "TOO_MANY_SIMULTANEOUS_QUERIES", Message: "Too many simultaneous queries for user default"}
	}
	return emptyRows{}, nil
}

func (c *busyConn) Close() error { return nil }

func withBusyConn(t *testing.T, conn *busyConn) *ClickHouseClient {
	t.Helper()
	previousOpen, previousBackoff := openClickHouse, clickHouseBusyBackoff
	openClickHouse = func(Config, string, string) (clickhouse.Conn, error) { return conn, nil }
	clickHouseBusyBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { openClickHouse, clickHouseBusyBackoff = previousOpen, previousBackoff })
	return NewClickHouseClient(Config{})
}

// ENG-007: a query rejected for too many simultaneous queries never started,
// so it is retried instead of failing the request.
func TestQueryRowsRetriesTooManySimultaneousQueries(t *testing.T) {
	conn := &busyConn{busy: 2}
	client := withBusyConn(t, conn)
	if err := client.QueryRows(context.Background(), "SELECT 1", func(clickHouseRow) error { return nil }); err != nil {
		t.Fatalf("QueryRows returned %v, want success after retries", err)
	}
	if got := conn.calls.Load(); got != 3 {
		t.Fatalf("queries sent = %d, want 3", got)
	}
}

func TestQueryRowsGivesUpWhenClickHouseStaysBusy(t *testing.T) {
	conn := &busyConn{busy: 100}
	client := withBusyConn(t, conn)
	err := client.QueryRows(context.Background(), "SELECT 1", func(clickHouseRow) error { return nil })
	var exception *clickhouse.Exception
	if !errors.As(err, &exception) || exception.Code != 202 {
		t.Fatalf("QueryRows returned %v, want the ClickHouse 202 error", err)
	}
	if got := conn.calls.Load(); got != 4 {
		t.Fatalf("queries sent = %d, want 4 (one try and three retries)", got)
	}
}

func TestQueryRowsDoesNotRetryOtherErrors(t *testing.T) {
	conn := &busyConn{busy: 1, err: &clickhouse.Exception{Code: 241, Name: "MEMORY_LIMIT_EXCEEDED"}}
	client := withBusyConn(t, conn)
	if err := client.QueryRows(context.Background(), "SELECT 1", func(clickHouseRow) error { return nil }); err == nil {
		t.Fatal("QueryRows succeeded, want the memory limit error")
	}
	if got := conn.calls.Load(); got != 1 {
		t.Fatalf("queries sent = %d, want 1", got)
	}
}

func TestQueryRowsStopsRetryingWhenTheRequestEnds(t *testing.T) {
	conn := &busyConn{busy: 100}
	client := withBusyConn(t, conn)
	clickHouseBusyBackoff = []time.Duration{time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := client.QueryRows(ctx, "SELECT 1", func(clickHouseRow) error { return nil }); err == nil {
		t.Fatal("QueryRows succeeded, want an error")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("QueryRows waited %s after the request ended", elapsed)
	}
}
