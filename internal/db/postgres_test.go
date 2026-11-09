package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

// The dashboard's request timeout relies on an expired context stopping the
// query in Postgres, and on the error matching context.DeadlineExceeded.
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

	waitFor := func(want int) {
		t.Helper()
		const running = `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND state = 'active'
			  AND query = 'SELECT pg_sleep(30)'`
		var n int
		for range 500 {
			if err := admin.QueryRow(ctx, running).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%d queries running, want %d", n, want)
	}

	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := pool.Exec(short, "SELECT pg_sleep(30)")
		errc <- err
	}()
	waitFor(1)
	if err := <-errc; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	waitFor(0)
}
