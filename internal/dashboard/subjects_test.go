package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func seedSubjects(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"alpha", "beta"} {
		if _, err := apps.Create(ctx, pool, id, strings.ToUpper(id)); err != nil {
			t.Fatal(err)
		}
	}
	_, err := db.New(pool).InsertEvents(ctx, []db.InsertEventsParams{
		{AppID: "alpha", Name: "app.open", DeviceID: "d1", UserID: sp("u1"), Ts: now.Add(-time.Hour), AppVersion: sp("2.0"), Country: sp("BD"), City: sp("Dhaka"), Platform: sp("ios"), OSVersion: sp("18")},
		{AppID: "alpha", Name: "error.sync", DeviceID: "d1", UserID: sp("u1"), Ts: now.Add(-25 * time.Hour), AppVersion: sp("1.0"), Country: sp("US")},
		{AppID: "alpha", Name: "error.net", DeviceID: "d1", UserID: sp("u2"), Ts: now.Add(-26 * time.Hour), AppVersion: sp("1.0"), Country: sp("US")},
		{AppID: "alpha", Name: "app.open", DeviceID: "d2", UserID: sp("u1"), Ts: now.Add(-2 * time.Hour), AppVersion: sp("1.5"), Country: sp("DE")},
		{AppID: "alpha", Name: "app.open", DeviceID: "../odd/id", Ts: now.Add(-3 * time.Hour), AppVersion: sp("1.5")},
		{AppID: "beta", Name: "error.sync", DeviceID: "d1", UserID: sp("u1"), Ts: now, AppVersion: sp("9.9")},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func subjectsSummary(v subjectsView) string {
	var s []string
	for _, r := range v.Rows {
		s = append(s, fmt.Sprintf("%s/%s/%s/%d", r.ID, deref(r.AppVersion), deref(r.Country), r.Errors))
	}
	return strings.Join(s, " ")
}

func TestLoadSubjects(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedSubjects(t, pool)
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	for _, tt := range []struct {
		kind subjectKind
		want string
	}{
		{deviceSubject, "d1/2.0/BD/2 d2/1.5/DE/0 ../odd/id/1.5//0"},
		{userSubject, "u1/2.0/BD/1 u2/1.0/US/1"},
	} {
		v, err := d.loadSubjects(context.Background(), "alpha", tt.kind, "")
		if err != nil {
			t.Fatal(err)
		}
		if got := subjectsSummary(v); got != tt.want {
			t.Errorf("%s = %q, want %q", tt.kind.tab(), got, tt.want)
		}
	}
}

func TestLoadSubjectsPaging(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := apps.Create(ctx, pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	// One timestamp across the page boundary, so only the id can order them.
	var rows []db.InsertEventsParams
	for i := range 2*subjectsPageSize + 3 {
		rows = append(rows, db.InsertEventsParams{
			AppID: "alpha", Name: "tick", Ts: now,
			DeviceID: fmt.Sprintf("d_%d", i), UserID: sp(fmt.Sprintf("u_%d", i)),
		})
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	d := &dashboard{pool: pool, q: db.New(pool)}

	for _, k := range []subjectKind{deviceSubject, userSubject} {
		seen := map[string]bool{}
		before, pages := "", 0
		for {
			v, err := d.loadSubjects(ctx, "alpha", k, before)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			for _, r := range v.Rows {
				if seen[r.ID] {
					t.Errorf("%s appears twice", r.ID)
				}
				seen[r.ID] = true
			}
			if v.Older == "" {
				break
			}
			u, err := url.Parse(v.Older)
			if err != nil {
				t.Fatal(err)
			}
			before = u.Query().Get("before")
		}
		if pages != 3 || len(seen) != 2*subjectsPageSize+3 {
			t.Errorf("%s: %d pages, %d rows", k.tab(), pages, len(seen))
		}
	}
}

func TestParseSubjectCursor(t *testing.T) {
	t.Parallel()
	ts := time.UnixMicro(1_700_000_000_000_001)
	got, id, ok := parseSubjectCursor(subjectCursor(ts, "a_b/c"))
	if !ok || !got.Equal(ts) || id != "a_b/c" {
		t.Errorf("round trip = %v %q %v", got, id, ok)
	}
	for _, s := range []string{"", "x", "12", "12_", "x_a", "-1_a", "12_a\x00", "12_\xff"} {
		if _, _, ok := parseSubjectCursor(s); ok {
			t.Errorf("%q parsed", s)
		}
	}
}

func TestLoadSubject(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedSubjects(t, pool)
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}
	ctx := context.Background()

	v, err := d.loadSubject(ctx, "alpha", deviceSubject, "d1", "")
	if err != nil {
		t.Fatal(err)
	}
	s := v.Summary
	if s.Events != 3 || s.Errors != 2 || !s.LastSeen.Equal(now.Add(-time.Hour)) || !s.FirstSeen.Equal(now.Add(-26*time.Hour)) {
		t.Errorf("summary = %+v", s)
	}
	if deref(s.AppVersion) != "2.0" || v.location() != "Dhaka, BD" || v.platform() != "ios 18" {
		t.Errorf("latest = %q %q %q", deref(s.AppVersion), v.location(), v.platform())
	}
	if len(v.Related) != 2 || v.Related[0].ID != "u1" || v.Related[1].ID != "u2" {
		t.Errorf("users = %+v", v.Related)
	}
	if len(v.Days) != 2 || len(v.Days[0].Rows) != 1 || len(v.Days[1].Rows) != 2 {
		t.Fatalf("days = %+v", v.Days)
	}
	first := v.Days[0].Rows[0]
	if v.Days[0].Label != "Sun, 8 Nov 2026" || first.Clock != "11:00:00" || first.Other != "u1" {
		t.Errorf("first entry = %q %+v", v.Days[0].Label, first)
	}
	self := subjectURL("alpha", deviceSubject, "d1", "")
	var userLink bool
	for _, f := range first.Event.Fields {
		if f.Timeline == self {
			t.Error("a timeline links to itself")
		}
		userLink = userLink || f.Timeline == subjectURL("alpha", userSubject, "u1", "")
	}
	if !userLink {
		t.Error("no link to the user's timeline")
	}

	u, err := d.loadSubject(ctx, "alpha", userSubject, "u1", "")
	if err != nil {
		t.Fatal(err)
	}
	if u.Summary.Events != 3 || u.Summary.Errors != 1 || len(u.Related) != 2 || u.Related[0].ID != "d1" || u.Days[0].Rows[0].Other != "d1" {
		t.Errorf("user = %+v", u)
	}
}

func TestLoadSubjectTimelinePaging(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := apps.Create(ctx, pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	var rows []db.InsertEventsParams
	for i := range eventsPageSize + 1 {
		rows = append(rows, db.InsertEventsParams{AppID: "alpha", Name: "tick", DeviceID: "d/1", Ts: now.Add(-time.Duration(i) * time.Minute)})
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	v, err := d.loadSubject(ctx, "alpha", deviceSubject, "d/1", "")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(v.Older)
	if err != nil || u.Query().Get("id") != "d/1" {
		t.Fatalf("older = %q, %v", v.Older, err)
	}
	next, err := d.loadSubject(ctx, "alpha", deviceSubject, "d/1", u.Query().Get("before"))
	if err != nil {
		t.Fatal(err)
	}
	if next.Older != "" || len(next.Days) != 1 || len(next.Days[0].Rows) != 1 {
		t.Errorf("last page = %+v", next)
	}
}

func TestSubjectPages(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedSubjects(t, pool)
	c.login(pw)

	for _, tt := range []struct {
		path string
		code int
		want string
	}{
		{"/apps/alpha/devices", http.StatusOK, subjectURLHTML("alpha", deviceSubject, "d1")},
		{"/apps/alpha/users", http.StatusOK, subjectURLHTML("alpha", userSubject, "u2")},
		{subjectURL("alpha", deviceSubject, "d1", ""), http.StatusOK, "Users on this device"},
		{subjectURL("alpha", userSubject, "u1", ""), http.StatusOK, "Devices of this user"},
		{subjectURL("alpha", deviceSubject, "../odd/id", ""), http.StatusOK, "../odd/id"},
		{subjectURL("alpha", deviceSubject, "missing", ""), http.StatusNotFound, ""},
		{subjectURL("beta", userSubject, "u2", ""), http.StatusNotFound, ""},
		{"/apps/alpha/device", http.StatusNotFound, ""},
		{"/apps/alpha/device?id=%00", http.StatusNotFound, ""},
		{"/apps/alpha/device?id=%FF", http.StatusNotFound, ""},
		{"/apps/nope/devices", http.StatusNotFound, ""},
		{"/apps/alpha/devices?before=garbage", http.StatusOK, "d1"},
	} {
		rec := c.do(http.MethodGet, tt.path, nil)
		if rec.Code != tt.code {
			t.Errorf("%s = %d, want %d", tt.path, rec.Code, tt.code)
			continue
		}
		if !strings.Contains(rec.Body.String(), tt.want) {
			t.Errorf("%s misses %q", tt.path, tt.want)
		}
	}

	rec := c.do(http.MethodGet, "/apps/alpha/events", nil)
	if body := rec.Body.String(); !strings.Contains(body, `hx-boost="false"`) || !strings.Contains(body, subjectURLHTML("alpha", deviceSubject, "d1")) {
		t.Error("events rows don't link to timelines outside the boosted list")
	}
}

func subjectURLHTML(appID string, k subjectKind, id string) string {
	return strings.ReplaceAll(subjectURL(appID, k, id, ""), "&", "&amp;")
}
