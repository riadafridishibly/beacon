package apps

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func TestCreateAndResolve(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.New(t)
	q := db.New(pool)

	key, err := Create(ctx, pool, "demo", "Demo")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, keyPrefix) {
		t.Errorf("key %q lacks prefix %q", key, keyPrefix)
	}
	second, err := NewKey(ctx, q, "demo")
	if err != nil {
		t.Fatal(err)
	}

	r := NewResolver(q)
	for _, k := range []string{key, second} {
		if got, err := r.App(ctx, k); err != nil || got != "demo" {
			t.Errorf("App(%q) = %q, %v; want demo", k, got, err)
		}
	}
	if got, err := r.App(ctx, "bk_unknown"); err != nil || got != "" {
		t.Errorf("unknown key: %q, %v", got, err)
	}

	if _, err := Create(ctx, pool, "Bad_ID", ""); !errors.Is(err, ErrBadID) {
		t.Errorf("bad id: err = %v, want ErrBadID", err)
	}
	if _, err := Create(ctx, pool, "demo", ""); err == nil {
		t.Error("duplicate id: no error")
	}
}

func TestRevoke(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.New(t)
	q := db.New(pool)

	key, err := Create(ctx, pool, "demo", "Demo")
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewKey(ctx, q, "demo")
	if err != nil {
		t.Fatal(err)
	}
	prefix := key[:len(keyPrefix)+shownChars]

	if got, err := Revoke(ctx, q, prefix); err != nil || got != "demo" {
		t.Fatalf("Revoke = %q, %v; want demo", got, err)
	}

	r := NewResolver(q)
	if got, err := r.App(ctx, key); err != nil || got != "" {
		t.Errorf("revoked key: %q, %v", got, err)
	}
	if got, err := r.App(ctx, other); err != nil || got != "demo" {
		t.Errorf("other key: %q, %v", got, err)
	}

	list, err := q.ListAppKeys(ctx, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].RevokedAt != nil || list[1].Prefix != prefix || list[1].RevokedAt == nil {
		t.Fatalf("keys = %+v, want the active key first", list)
	}
	first := *list[1].RevokedAt

	if _, err := Revoke(ctx, q, prefix); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
	list, err = q.ListAppKeys(ctx, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if !list[1].RevokedAt.Equal(first) {
		t.Errorf("second revoke moved revoked_at from %v to %v", first, *list[1].RevokedAt)
	}

	for _, p := range []string{"bk_222222", "bk_short", "", "bk_\x00\x00\x00\x00\x00\x00"} {
		if _, err := Revoke(ctx, q, p); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("Revoke(%q) = %v, want ErrKeyNotFound", p, err)
		}
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.New(t)
	q := db.New(pool)

	key, err := Create(ctx, pool, "demo", "Demo")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(ctx, pool, "other", "Other"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.InsertEvents(ctx, []db.InsertEventsParams{
		{AppID: "demo", Name: "app.open", Ts: time.Now(), DeviceID: "d1"},
		{AppID: "other", Name: "app.open", Ts: time.Now(), DeviceID: "d1"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.CreateView(ctx, db.CreateViewParams{
		AppID: "demo", Title: "Opens", Event: "app.open", Measures: []string{"count"},
		Filter: []byte("{}"), Shape: "table", MaxViews: 20,
	}); err != nil {
		t.Fatal(err)
	}

	if err := Delete(ctx, q, "demo"); err != nil {
		t.Fatal(err)
	}
	if got, err := NewResolver(q).App(ctx, key); err != nil || got != "" {
		t.Errorf("key of deleted app: %q, %v", got, err)
	}
	var demo, other int
	if err := pool.QueryRow(ctx, "SELECT count(*) FILTER (WHERE app_id = 'demo'), count(*) FILTER (WHERE app_id = 'other') FROM events").Scan(&demo, &other); err != nil {
		t.Fatal(err)
	}
	if demo != 0 || other != 1 {
		t.Errorf("events left: demo %d, other %d; want 0 and 1", demo, other)
	}

	if views, err := q.ListViews(ctx, "demo"); err != nil || len(views) != 0 {
		t.Errorf("views of deleted app: %d, %v", len(views), err)
	}

	for _, id := range []string{"demo", "Bad_ID", "\xff"} {
		if err := Delete(ctx, q, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Delete(%q) = %v, want ErrNotFound", id, err)
		}
	}
}
