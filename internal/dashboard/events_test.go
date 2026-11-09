package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func seedEvents(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"alpha", "beta", "quiet"} {
		if _, err := apps.Create(ctx, pool, id, strings.ToUpper(id)); err != nil {
			t.Fatal(err)
		}
	}
	props, err := json.Marshal(map[string]any{"message": "<b>boom</b>", "retries": 2})
	if err != nil {
		t.Fatal(err)
	}
	value := 12.5
	_, err = db.New(pool).InsertEvents(ctx, []db.InsertEventsParams{
		{AppID: "alpha", Name: "app.open", DeviceID: "d1", Ts: now.Add(-time.Minute), Country: sp("BD"), AppVersion: sp("1.0"), Value: &value},
		{AppID: "alpha", Name: "app.open", DeviceID: "d2", Ts: now.Add(-2 * time.Minute), Country: sp("US"), AppVersion: sp("1.1"), UserID: sp("u1")},
		{AppID: "alpha", Name: "error.sync", DeviceID: "d1", Ts: now.Add(-3 * time.Minute), Country: sp("BD"), AppVersion: sp("1.1"), UserID: sp("u1"), Props: props},
		{AppID: "beta", Name: "app.open", DeviceID: "d1", Ts: now.Add(-time.Minute), Country: sp("BD")},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func eventNames(v eventsView) string {
	var s []string
	for _, r := range v.Rows {
		s = append(s, r.Name+"/"+r.Device)
	}
	return strings.Join(s, " ")
}

func TestLoadEventsFilters(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedEvents(t, pool)
	d := &dashboard{pool: pool, q: db.New(pool)}

	for _, tt := range []struct {
		name string
		f    eventFilter
		want string
	}{
		{"none, newest first", eventFilter{}, "app.open/d1 app.open/d2 error.sync/d1"},
		{"name", eventFilter{Name: "error.sync"}, "error.sync/d1"},
		{"name is exact", eventFilter{Name: "error"}, ""},
		{"device", eventFilter{Device: "d2"}, "app.open/d2"},
		{"user", eventFilter{User: "u1"}, "app.open/d2 error.sync/d1"},
		{"country", eventFilter{Country: "BD"}, "app.open/d1 error.sync/d1"},
		{"version", eventFilter{Version: "1.0"}, "app.open/d1"},
		{"combined", eventFilter{Device: "d1", Version: "1.1"}, "error.sync/d1"},
	} {
		v, err := d.loadEvents(context.Background(), "alpha", tt.f, "")
		if err != nil {
			t.Fatal(err)
		}
		if got := eventNames(v); got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestLoadEventsPaging(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := apps.Create(ctx, pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	// One timestamp across the page boundary, so only the id can order them.
	var rows []db.InsertEventsParams
	for i := range 2*eventsPageSize + 3 {
		rows = append(rows, db.InsertEventsParams{AppID: "alpha", Name: "tick", DeviceID: fmt.Sprintf("d%d", i), Ts: now})
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	d := &dashboard{pool: pool, q: db.New(pool)}

	seen := map[string]bool{}
	before, pages := "", 0
	for {
		v, err := d.loadEvents(ctx, "alpha", eventFilter{Name: "tick"}, before)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, r := range v.Rows {
			if seen[r.Device] {
				t.Errorf("%s appears twice", r.Device)
			}
			seen[r.Device] = true
		}
		if v.Older == "" {
			break
		}
		if !strings.Contains(v.Older, "name=tick") {
			t.Errorf("older link drops the filter: %s", v.Older)
		}
		_, query, _ := strings.Cut(v.Older, "before=")
		before, _, _ = strings.Cut(query, "&")
	}
	if pages != 3 || len(seen) != len(rows) {
		t.Errorf("pages = %d, events = %d, want 3 and %d", pages, len(seen), len(rows))
	}
}

func TestParseEventCursor(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 11, 8, 12, 0, 0, 123456000, time.UTC)
	got, id, ok := parseEventCursor(eventCursor(db.Event{ID: 42, Ts: ts}))
	if !ok || id != 42 || !got.Equal(ts) {
		t.Errorf("round trip = %v %d %v", got, id, ok)
	}
	for _, s := range []string{"", "x", "1", "1_", "_1", "a_1", "1_b", "-1_1", "1_-1", "0_1", "1_0"} {
		if _, _, ok := parseEventCursor(s); ok {
			t.Errorf("%q parsed", s)
		}
	}
}

func TestEventsURL(t *testing.T) {
	t.Parallel()
	if got := eventsURL("a", eventFilter{}, ""); got != "/apps/a/events" {
		t.Errorf("no filters = %q", got)
	}
	got := eventsURL("a", eventFilter{Name: "x&y", User: "u 1"}, "5_6")
	if want := "/apps/a/events?before=5_6&name=x%26y&user=u+1"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestEventsPage(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedEvents(t, pool)
	c.login(pw)

	w := c.do(http.MethodGet, "/apps/alpha/events", nil)
	body := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(body, "<html") {
		t.Fatalf("page: %d", w.Code)
	}
	for _, s := range []string{
		"error.sync",
		"2026-11-08 11:59:00",
		"12.5",
		// Props are shown as text, never as markup.
		"&lt;b&gt;boom&lt;/b&gt;",
		"&#34;retries&#34;: 2",
		"/apps/alpha/events?device=d2",
		`aria-current="page"`,
	} {
		if !strings.Contains(body, s) {
			t.Errorf("page lacks %q", s)
		}
	}
	if strings.Contains(body, "<b>boom") {
		t.Error("props rendered as markup")
	}
	if strings.Contains(body, ` style="`) {
		t.Error("page has an inline style, which the CSP blocks")
	}

	r := httptest.NewRequest(http.MethodGet, "/apps/alpha/events?name=&device=d2&user=&country=&version=", nil)
	r.Header.Set("HX-Request", "true")
	r.AddCookie(c.cookie)
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "<html") {
		t.Errorf("htmx fragment: %d", rec.Code)
	}
	if got := rec.Header().Get("HX-Push-Url"); got != "/apps/alpha/events?device=d2" {
		t.Errorf("pushed URL = %q", got)
	}

	for _, tt := range []struct {
		path, want string
		code       int
	}{
		{"/apps/alpha/events?country=bd", "error.sync", http.StatusOK},
		{"/apps/alpha/events?before=garbage", "error.sync", http.StatusOK},
		{"/apps/alpha/events?before=9223372036854775807_9223372036854775807", "error.sync", http.StatusOK},
		{"/apps/alpha/events?device=%00", "error.sync", http.StatusOK},
		{"/apps/alpha/events?device=%FF", "error.sync", http.StatusOK},
		{"/apps/alpha/events?name=nope", "No events match", http.StatusOK},
		{"/apps/quiet/events", "No events yet", http.StatusOK},
		{"/apps/missing/events", "", http.StatusNotFound},
		{"/apps/%00/events", "", http.StatusNotFound},
		{"/apps/%00", "", http.StatusNotFound},
	} {
		w := c.do(http.MethodGet, tt.path, nil)
		if w.Code != tt.code || !strings.Contains(w.Body.String(), tt.want) {
			t.Errorf("%s: %d, want %d with %q", tt.path, w.Code, tt.code, tt.want)
		}
	}

	if w := c.do(http.MethodGet, "/apps/alpha", nil); !strings.Contains(w.Body.String(), `href="/apps/alpha/events"`) {
		t.Error("overview doesn't link to events")
	}
}
