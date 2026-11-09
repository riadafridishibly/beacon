package dashboard

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
)

func TestKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, c, pw := setup(t)
	q := db.New(pool)
	key, err := apps.Create(ctx, pool, "alpha", "Alpha")
	if err != nil {
		t.Fatal(err)
	}
	prefix := key[:9]
	revokePath := "/keys/" + prefix + "/revoke"

	if w := c.do(http.MethodPost, revokePath, url.Values{}); w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/login") {
		t.Fatalf("signed out: %d %s", w.Code, w.Header().Get("Location"))
	}
	if got, err := apps.NewResolver(q).App(ctx, key); err != nil || got != "alpha" {
		t.Fatalf("signed-out revoke changed the key: %q, %v", got, err)
	}

	c.login(pw)
	w := c.do(http.MethodGet, "/apps/alpha/keys", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), prefix) || !strings.Contains(w.Body.String(), revokePath) {
		t.Fatalf("keys page: %d %s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), key) {
		t.Error("keys page shows the full key")
	}

	w = c.do(http.MethodPost, revokePath, url.Values{})
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/apps/alpha/keys" {
		t.Fatalf("revoke: %d %s", w.Code, w.Header().Get("Location"))
	}
	if got, err := apps.NewResolver(q).App(ctx, key); err != nil || got != "" {
		t.Errorf("revoked key still resolves: %q, %v", got, err)
	}

	w = c.do(http.MethodGet, "/apps/alpha/keys", nil)
	if !strings.Contains(w.Body.String(), "Revoked") || strings.Contains(w.Body.String(), revokePath) {
		t.Errorf("after revoke: %s", w.Body)
	}

	if w := c.do(http.MethodPost, "/keys/bk_222222/revoke", url.Values{}); w.Code != http.StatusNotFound {
		t.Errorf("unknown prefix: %d", w.Code)
	}
	if w := c.do(http.MethodGet, "/apps/nope/keys", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown app: %d", w.Code)
	}
}
