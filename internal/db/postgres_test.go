package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

// The dashboard's request timeout relies on an expired context stopping the
// query in Postgres, not only in the client.
func TestDashboardQueryStopsOnTheServer(t *testing.T) {
	t.Parallel()
	admin := testdb.New(t)
	ctx := context.Background()
	pool, err := db.ConnectDashboard(ctx, admin.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if got := pool.Config().MaxConns; got > 3 {
		t.Errorf("MaxConns = %d", got)
	}

	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := pool.Exec(short, "SELECT pg_sleep(30)"); err == nil {
		t.Fatal("query outlived its context")
	}

	const running = `SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND state = 'active'
		  AND query = 'SELECT pg_sleep(30)'`
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		if err := admin.QueryRow(ctx, running).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("query still running on the server")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
