package apps

import (
	"context"
	"errors"
	"strings"
	"testing"

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
