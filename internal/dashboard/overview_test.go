package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func sp(s string) *string { return &s }

type evOpt func(*db.InsertEventsParams)

func withValue(v float64) evOpt  { return func(e *db.InsertEventsParams) { e.Value = &v } }
func withUser(u string) evOpt    { return func(e *db.InsertEventsParams) { e.UserID = &u } }
func withSession(s string) evOpt { return func(e *db.InsertEventsParams) { e.SessionID = &s } }
func withMessage(m string) evOpt {
	return func(e *db.InsertEventsParams) {
		e.Props, _ = json.Marshal(map[string]string{"message": m})
	}
}

func seedOverview(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"alpha", "beta", "quiet", "new"} {
		if _, err := apps.Create(ctx, pool, id, strings.ToUpper(id)); err != nil {
			t.Fatal(err)
		}
	}
	ev := func(app, name, device string, age time.Duration, opts ...evOpt) db.InsertEventsParams {
		e := db.InsertEventsParams{AppID: app, Name: name, DeviceID: device, Ts: now.Add(-age)}
		e.Country, e.Platform, e.AppVersion = sp("BD"), sp("macos"), sp("1.0")
		if device == "d2" {
			e.Country, e.AppVersion = nil, sp("1.1")
		}
		for _, o := range opts {
			o(&e)
		}
		return e
	}
	const day = 24 * time.Hour
	_, err := db.New(pool).InsertEvents(ctx, []db.InsertEventsParams{
		// Today, the last bucket of 7d.
		ev("alpha", "app.open", "d1", time.Hour, withValue(100)),
		ev("alpha", "app.open", "d2", time.Hour, withValue(300), withUser("u1")),
		ev("alpha", "error.sync", "d1", 2*time.Hour, withUser("u1"), withMessage("boom")),
		ev("alpha", "error.sync", "d2", 3*time.Hour, withMessage("boom")),
		ev("alpha", "error.auth", "d1", 4*time.Hour),
		// d1 sent heartbeats and no end; its length is the sum of its heartbeats.
		ev("alpha", "session.start", "d1", 5*time.Hour, withSession("s1")),
		ev("alpha", "session.heartbeat", "d1", 5*time.Hour-time.Minute, withSession("s1"), withValue(60)),
		ev("alpha", "session.heartbeat", "d1", 5*time.Hour-2*time.Minute, withSession("s1"), withValue(60)),
		ev("alpha", "session.end", "d2", 5*time.Hour, withSession("s2"), withValue(240)),
		// Two days ago: an open without a value.
		ev("alpha", "app.open", "d1", 2*day),
		// The first day of 7d, and the day before it.
		ev("alpha", "app.open", "d3", 6*day+12*time.Hour),
		ev("alpha", "app.open", "d4", 7*day+time.Hour),
		// Another app's events never count.
		ev("beta", "app.open", "x1", time.Hour, withValue(9000)),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadOverview(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedOverview(t, pool)
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	v, err := d.loadOverview(context.Background(), "alpha", "7d")
	if err != nil {
		t.Fatal(err)
	}
	want := db.OverviewTotalsRow{Devices: 3, Users: 1, Events: 11, Opens: 4, Errors: 3}
	if v.Totals != want {
		t.Errorf("totals = %+v, want %+v", v.Totals, want)
	}
	if len(v.Active.Rows) != 7 {
		t.Fatalf("buckets = %d, want 7", len(v.Active.Rows))
	}

	today, twoDaysAgo, emptyDay, firstDay := 6, 4, 5, 0
	for _, tt := range []struct {
		name string
		c    chartView
		row  int
		want []string
	}{
		{"active today", v.Active, today, []string{"2", "1"}},
		{"active on an empty day", v.Active, emptyDay, []string{"0", "0"}},
		{"active first day", v.Active, firstDay, []string{"1", "0"}},
		{"opens today", v.Opens, today, []string{"2"}},
		{"opens without a value", v.Opens, twoDaysAgo, []string{"1"}},
		{"startup today", v.Startup, today, []string{"200 ms", "290 ms"}},
		{"startup without a value", v.Startup, twoDaysAgo, []string{"–", "–"}},
		// (120 + 240) seconds over 2 active devices.
		{"usage today", v.Usage, today, []string{"3m"}},
		{"usage without sessions", v.Usage, twoDaysAgo, []string{"–"}},
		{"errors today", v.Errors, today, []string{"3"}},
	} {
		if got := tt.c.Rows[tt.row].Values; !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s = %v, want %v", tt.name, got, tt.want)
		}
	}

	if len(v.TopErrors) != 2 {
		t.Fatalf("top errors = %+v", v.TopErrors)
	}
	top := v.TopErrors[0]
	if top.Name != "error.sync" || top.Message != "boom" || top.Devices != 2 || top.Users != 1 || !top.LastSeen.Equal(now.Add(-2*time.Hour)) {
		t.Errorf("top error = %+v", top)
	}
	if e := v.TopErrors[1]; e.Name != "error.auth" || e.Message != "" || e.Devices != 1 {
		t.Errorf("second error = %+v", e)
	}

	countries := []db.OverviewBreakdownRow{{Label: "BD", Devices: 2}, {Label: "", Devices: 1}}
	versions := []db.OverviewBreakdownRow{{Label: "1.0", Devices: 2}, {Label: "1.1", Devices: 1}}
	if !reflect.DeepEqual(v.Breakdown[0].Rows, countries) || v.Breakdown[0].Top != 2 {
		t.Errorf("countries = %+v", v.Breakdown[0])
	}
	if got := v.Breakdown[1].Rows; len(got) != 1 || got[0].Label != "macos" || got[0].Devices != 3 {
		t.Errorf("platforms = %+v", got)
	}
	if !reflect.DeepEqual(v.Breakdown[2].Rows, versions) {
		t.Errorf("versions = %+v", v.Breakdown[2].Rows)
	}

	day, err := d.loadOverview(context.Background(), "alpha", "24h")
	if err != nil {
		t.Fatal(err)
	}
	if day.Totals.Devices != 2 || len(day.Active.Rows) != 2 {
		t.Errorf("24h: %+v, %d buckets", day.Totals, len(day.Active.Rows))
	}
}

func TestOverviewPage(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedOverview(t, pool)
	c.login(pw)

	w := c.do(http.MethodGet, "/apps/alpha", nil)
	body := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(body, "<html") {
		t.Fatalf("page: %d", w.Code)
	}
	for _, s := range []string{"Daily active devices and users", "Top errors", "boom", "App versions", `aria-current="true"`} {
		if !strings.Contains(body, s) {
			t.Errorf("page lacks %q", s)
		}
	}
	if strings.Contains(body, ` style="`) {
		t.Error("page has an inline style, which the CSP blocks")
	}

	r := httptest.NewRequest(http.MethodGet, "/apps/alpha?range=30d", nil)
	r.Header.Set("HX-Request", "true")
	r.AddCookie(c.cookie)
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), "Top errors") {
		t.Errorf("htmx fragment: %d, html page: %v", rec.Code, strings.Contains(rec.Body.String(), "<html"))
	}

	if w := c.do(http.MethodGet, "/apps/alpha?range=bogus", nil); !strings.Contains(w.Body.String(), "/apps/alpha?range=7d") || w.Code != http.StatusOK {
		t.Errorf("bad range: %d", w.Code)
	}
	if w := c.do(http.MethodGet, "/apps/quiet", nil); !strings.Contains(w.Body.String(), "No events in this range") {
		t.Errorf("app without events:\n%s", w.Body)
	}
	if w := c.do(http.MethodGet, "/apps/missing", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown app: %d", w.Code)
	}
	if w := c.do(http.MethodGet, "/apps/new", nil); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "NEW") {
		t.Errorf("an app named new: %d", w.Code)
	}
	if w := c.do(http.MethodGet, "/", nil); !strings.Contains(w.Body.String(), `href="/apps/alpha"`) {
		t.Error("app card doesn't link to the overview")
	}
}
