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
