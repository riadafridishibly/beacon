package retention

import (
	"context"
	"testing"
	"time"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func TestPurge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.New(t)
	q := db.New(pool)
	cutoff := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)

	var events []db.InsertEventsParams
	add := func(app string, ts time.Time, n int) {
		for range n {
			events = append(events, db.InsertEventsParams{AppID: app, Name: "a", Ts: ts, DeviceID: "d1"})
		}
	}
	for _, app := range []string{"one", "two"} {
		if _, err := apps.Create(ctx, pool, app, ""); err != nil {
			t.Fatal(err)
		}
	}
	// More than one batch for app one.
	add("one", cutoff.Add(-time.Microsecond), batchSize+1)
	add("one", cutoff, 2)
	add("two", cutoff.AddDate(0, -1, 0), 3)
	add("two", cutoff.AddDate(0, 0, 1), 4)
	if _, err := q.InsertEvents(ctx, events); err != nil {
		t.Fatal(err)
	}

	n, err := Purge(ctx, q, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != batchSize+4 {
		t.Errorf("deleted %d, want %d", n, batchSize+4)
	}
	rows, err := pool.Query(ctx, `SELECT app_id, count(*), min(ts) FROM events GROUP BY app_id ORDER BY app_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]int{"one": 2, "two": 4}
	got := map[string]int{}
	for rows.Next() {
		var app string
		var count int
		var oldest time.Time
		if err := rows.Scan(&app, &count, &oldest); err != nil {
			t.Fatal(err)
		}
		got[app] = count
		if oldest.Before(cutoff) {
			t.Errorf("app %s kept an event from %v", app, oldest)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for app, w := range want {
		if got[app] != w {
			t.Errorf("app %s has %d events, want %d", app, got[app], w)
		}
	}
}
