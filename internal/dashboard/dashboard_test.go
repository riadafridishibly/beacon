package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

var now = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

type client struct {
	t      *testing.T
	h      http.Handler
	cookie *http.Cookie
}

func setup(t *testing.T) (*pgxpool.Pool, *client, string) {
	t.Helper()
	pool := testdb.New(t)
	h, err := New(pool, Config{ViteURL: "http://localhost:8341", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	pw, err := auth.CreateAccount(context.Background(), db.New(pool), "ops@example.com")
	if err != nil {
		t.Fatal(err)
	}
	return pool, &client{t: t, h: h}, pw
}

func (c *client) do(method, path string, form url.Values) *httptest.ResponseRecorder {
	c.t.Helper()
	var r *http.Request
	if form != nil {
		r = httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	if c.cookie != nil {
		r.AddCookie(c.cookie)
	}
	w := httptest.NewRecorder()
	c.h.ServeHTTP(w, r)
	for _, ck := range w.Result().Cookies() {
		if ck.Name == auth.CookieName {
			if ck.MaxAge < 0 {
				c.cookie = nil
			} else {
				c.cookie = ck
			}
		}
	}
	return w
}

func (c *client) login(password string) *httptest.ResponseRecorder {
	c.t.Helper()
	return c.do(http.MethodPost, "/login", url.Values{"email": {"ops@example.com"}, "password": {password}, "next": {"/apps/new"}})
}

func TestLogin(t *testing.T) {
	t.Parallel()
	_, c, pw := setup(t)

	w := c.do(http.MethodGet, "/apps/new", nil)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/login?next=%2Fapps%2Fnew" {
		t.Fatalf("signed out: %d %s", w.Code, w.Header().Get("Location"))
	}
	if !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'self' http://localhost:8341") {
		t.Errorf("CSP = %q", w.Header().Get("Content-Security-Policy"))
	}

	if w := c.login("wrong"); w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "incorrect") {
		t.Fatalf("wrong password: %d", w.Code)
	}
	if c.cookie != nil {
		t.Fatal("wrong password set a session cookie")
	}

	w = c.login(pw)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/apps/new" {
		t.Fatalf("login: %d %s", w.Code, w.Header().Get("Location"))
	}
	if c.cookie == nil || !c.cookie.Secure || !c.cookie.HttpOnly {
		t.Fatalf("session cookie = %+v", c.cookie)
	}
	if w := c.do(http.MethodGet, "/", nil); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "ops@example.com") {
		t.Fatalf("signed in: %d", w.Code)
	}
	if w := c.do(http.MethodGet, "/login", nil); w.Code != http.StatusSeeOther {
		t.Errorf("login page while signed in: %d", w.Code)
	}

	old := c.cookie
	c.do(http.MethodPost, "/logout", url.Values{})
	c.cookie = old
	if w := c.do(http.MethodGet, "/", nil); w.Code != http.StatusSeeOther {
		t.Errorf("old cookie after logout: %d", w.Code)
	}
}

func TestCrossOriginPostRejected(t *testing.T) {
	t.Parallel()
	_, c, pw := setup(t)
	c.login(pw)
	r := httptest.NewRequest(http.MethodPost, "/apps", strings.NewReader("id=x"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	r.AddCookie(c.cookie)
	w := httptest.NewRecorder()
	c.h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Errorf("cross-site POST: %d, want 403", w.Code)
	}
}

func TestAppList(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	ctx := context.Background()
	c.login(pw)

	if w := c.do(http.MethodGet, "/", nil); !strings.Contains(w.Body.String(), "No apps yet") {
		t.Errorf("empty list:\n%s", w.Body)
	}

	for _, id := range []string{"alpha", "beta"} {
		if _, err := apps.Create(ctx, pool, id, strings.ToUpper(id)); err != nil {
			t.Fatal(err)
		}
	}
	ev := func(name, device string, age time.Duration) db.InsertEventsParams {
		return db.InsertEventsParams{AppID: "alpha", Name: name, Ts: now.Add(-age), DeviceID: device}
	}
	_, err := db.New(pool).InsertEvents(ctx, []db.InsertEventsParams{
		ev("app.open", "d1", time.Hour),
		ev("app.open", "d2", time.Hour),
		ev("error.sync", "d1", 2*time.Hour),
		ev("app.open", "d3", 25*time.Hour), // outside the last 24 h
	})
	if err != nil {
		t.Fatal(err)
	}

	list, err := db.New(pool).AppSummaries(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0] != (db.AppSummariesRow{ID: "alpha", Name: "ALPHA", Events: 3, Devices: 2, Errors: 1}) ||
		list[1] != (db.AppSummariesRow{ID: "beta", Name: "BETA"}) {
		t.Errorf("summaries = %+v", list)
	}

	body := c.do(http.MethodGet, "/", nil).Body.String()
	for _, s := range []string{"ALPHA", "BETA", "alpha", "beta"} {
		if !strings.Contains(body, s) {
			t.Errorf("app list lacks %q", s)
		}
	}
}

func TestCreateApp(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	c.login(pw)

	if w := c.do(http.MethodGet, "/apps/new", nil); w.Code != http.StatusOK {
		t.Fatalf("form: %d", w.Code)
	}
	w := c.do(http.MethodPost, "/apps", url.Values{"id": {"my-app"}, "name": {"My App"}})
	if w.Code != http.StatusCreated || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("create: %d %q", w.Code, w.Header().Get("Cache-Control"))
	}
	key := regexp.MustCompile(`bk_[A-Z0-9]+`).FindString(w.Body.String())
	if key == "" {
		t.Fatalf("no key on the page:\n%s", w.Body)
	}
	id, err := apps.NewResolver(db.New(pool)).App(context.Background(), key)
	if err != nil || id != "my-app" {
		t.Errorf("shown key resolves to %q, %v", id, err)
	}

	for _, tt := range []struct {
		form url.Values
		want string
	}{
		{url.Values{"id": {"my-app"}}, "already exists"},
		{url.Values{"id": {"My App"}}, "lowercase letters"},
		{url.Values{"id": {"ok"}, "name": {strings.Repeat("x", 101)}}, "at most 100"},
	} {
		w := c.do(http.MethodPost, "/apps", tt.form)
		if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), tt.want) {
			t.Errorf("%v: %d, body lacks %q", tt.form, w.Code, tt.want)
		}
	}
}

func TestNotFound(t *testing.T) {
	t.Parallel()
	_, c, pw := setup(t)
	c.login(pw)
	if w := c.do(http.MethodGet, "/nope", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown page: %d", w.Code)
	}
}
