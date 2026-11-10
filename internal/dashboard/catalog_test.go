package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func props(t *testing.T, m map[string]any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func seedCatalog(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"alpha", "beta"} {
		if _, err := apps.Create(ctx, pool, id, strings.ToUpper(id)); err != nil {
			t.Fatal(err)
		}
	}
	day := 24 * time.Hour
	var rows []db.InsertEventsParams
	for i, v := range []float64{10, 20, 30, 40} {
		rows = append(rows, db.InsertEventsParams{
			AppID: "alpha", Name: "app.open", DeviceID: "d1", Ts: now.Add(-time.Duration(i) * time.Minute), Value: fp(v),
			Props: props(t, map[string]any{"screen": "home", "cold": true}),
		})
	}
	rows = append(rows,
		db.InsertEventsParams{AppID: "alpha", Name: "app.open", DeviceID: "d1", Ts: now.Add(-day), Value: fp(100), Props: props(t, map[string]any{"screen": "<b>settings</b>"})},
		db.InsertEventsParams{AppID: "alpha", Name: "app.open", DeviceID: "d1", Ts: now.Add(-20 * day), Value: fp(500), Props: props(t, map[string]any{"old": 1})},
		db.InsertEventsParams{AppID: "alpha", Name: "tap", DeviceID: "d1", Ts: now.Add(-time.Hour)},
		db.InsertEventsParams{AppID: "alpha", Name: "error.sync", DeviceID: "d1", Ts: now.Add(-40 * day), Props: props(t, map[string]any{"message": "boom"})},
		db.InsertEventsParams{AppID: "beta", Name: "beta.only", DeviceID: "d1", Ts: now, Props: props(t, map[string]any{"secret": 1})},
	)
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
}

func TestLoadCatalog(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedCatalog(t, pool)
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	v, err := d.loadCatalog(context.Background(), "alpha", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Rows) != 3 {
		t.Fatalf("rows = %+v", v.Rows)
	}
	open := v.Rows[0]
	if open.Name != "app.open" || open.Events != 6 || !open.LastSeen.Equal(now) ||
		!reflect.DeepEqual(open.Keys, []string{"cold", "old", "screen"}) {
		t.Errorf("first = %+v", open)
	}
	if last := v.Rows[1]; last.Name != "tap" || len(last.Keys) != 0 {
		t.Errorf("second = %+v", last)
	}
}

func TestLoadCatalogKeysAndPaging(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := apps.Create(ctx, pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	many := map[string]any{}
	for i := range maxListedKeys + 3 {
		many[fmt.Sprintf("k%02d", i)] = i
	}
	// Every name has the same count, so only the name can order the pages.
	var rows []db.InsertEventsParams
	for i := range 2*catalogPageSize + 3 {
		rows = append(rows, db.InsertEventsParams{AppID: "alpha", Name: fmt.Sprintf("name_%d", i), DeviceID: "d", Ts: now, Props: props(t, many)})
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	seen := map[string]bool{}
	before, pages := "", 0
	for {
		v, err := d.loadCatalog(ctx, "alpha", before)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, r := range v.Rows {
			if seen[r.Name] {
				t.Errorf("%s appears twice", r.Name)
			}
			seen[r.Name] = true
			if len(r.Keys) != maxListedKeys || r.MoreKeys != 3 {
				t.Errorf("%s keys = %d, more %d", r.Name, len(r.Keys), r.MoreKeys)
			}
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
	if pages != 3 || len(seen) != 2*catalogPageSize+3 {
		t.Errorf("%d pages, %d names", pages, len(seen))
	}
}

func TestPropKeySampleCountsRowsWithoutProps(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := apps.Create(ctx, pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	rows := []db.InsertEventsParams{{AppID: "alpha", Name: "beat", DeviceID: "d", Ts: now.Add(-time.Hour), Props: props(t, map[string]any{"old": 1})}}
	for i := range propKeySample {
		rows = append(rows, db.InsertEventsParams{AppID: "alpha", Name: "beat", DeviceID: "d", Ts: now.Add(-time.Duration(i) * time.Second)})
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	list, err := d.loadCatalog(ctx, "alpha", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Rows) != 1 || len(list.Rows[0].Keys) != 0 {
		t.Errorf("list = %+v", list.Rows)
	}
	v, err := d.loadCatalogEvent(ctx, "alpha", "beat", "7d", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if v.Values.Key != "" {
		t.Errorf("key = %q", v.Values.Key)
	}
}

func TestParseCatalogCursor(t *testing.T) {
	t.Parallel()
	n, name, ok := parseCatalogCursor(catalogCursor(12, "a_b.c"))
	if !ok || n != 12 || name != "a_b.c" {
		t.Errorf("round trip = %d %q %v", n, name, ok)
	}
	for _, s := range []string{"", "x", "12", "12_", "x_a", "-1_a", "0_a", "12_a\x00", "12_\xff"} {
		if _, _, ok := parseCatalogCursor(s); ok {
			t.Errorf("%q parsed", s)
		}
	}
}

func TestLoadCatalogEvent(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedCatalog(t, pool)
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}
	ctx := context.Background()

	v, err := d.loadCatalogEvent(ctx, "alpha", "app.open", "7d", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if v.Total.Events != 5 || v.Total.WithValue != 5 || v.Total.P50 != 30 || !v.LastSeen.Equal(now) {
		t.Errorf("total = %+v, last seen %v", v.Total, v.LastSeen)
	}
	if len(v.Count.Rows) != 7 || !v.Value.HasData {
		t.Errorf("charts = %d rows, value %v", len(v.Count.Rows), v.Value.HasData)
	}
	today := v.Value.Series[0].Values[6]
	if today == nil || *today != 25 {
		t.Errorf("today's p50 = %v", today)
	}
	if empty := v.Value.Series[0].Values[0]; empty != nil {
		t.Errorf("a day without events has p50 %v", *empty)
	}

	if v.Values.Requested != "" {
		t.Errorf("requested = %q with no key asked for", v.Values.Requested)
	}
	if v.Values.Key != "screen" || v.Values.Keys[1] != "cold" {
		t.Errorf("keys = %q, chosen %q", v.Values.Keys, v.Values.Key)
	}
	want := []db.EventTopValuesRow{
		{Label: "home", Events: 4, WithValue: 4, P50: 25, P95: 38.5},
		{Label: "<b>settings</b>", Events: 1, WithValue: 1, P50: 100, P95: 100},
	}
	if !reflect.DeepEqual(v.Values.Rows, want) || v.Values.Top != 4 || !v.Values.rowsHaveValue() {
		t.Errorf("values = %+v", v.Values)
	}

	chosen, err := d.loadCatalogEvent(ctx, "alpha", "app.open", "7d", "cold", false)
	if err != nil {
		t.Fatal(err)
	}
	if chosen.Values.Key != "cold" || len(chosen.Values.Rows) != 1 || chosen.Values.Rows[0].Label != "true" {
		t.Errorf("chosen = %+v", chosen.Values)
	}
	unknown, err := d.loadCatalogEvent(ctx, "alpha", "app.open", "7d", "old", false)
	if err != nil || unknown.Values.Key != "screen" || unknown.Values.Requested != "old" {
		t.Errorf("key missing from the range = %+v, %v", unknown.Values, err)
	}

	bare, err := d.loadCatalogEvent(ctx, "alpha", "tap", "7d", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if bare.hasValues() || bare.Values.Key != "" || bare.Total.Events != 1 {
		t.Errorf("bare = %+v", bare)
	}

	old, err := d.loadCatalogEvent(ctx, "alpha", "error.sync", "7d", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if old.Total.Events != 0 || old.Values.Key != "" || !old.LastSeen.Equal(now.Add(-40*24*time.Hour)) {
		t.Errorf("old = %+v", old)
	}

	unvalued, err := d.loadCatalogEvent(ctx, "alpha", "error.sync", "90d", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if rows := unvalued.Values.Rows; len(rows) != 1 || rows[0].WithValue != 0 || unvalued.Values.rowsHaveValue() {
		t.Errorf("values without a value = %+v", unvalued.Values)
	}
}

func TestCatalogPages(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedCatalog(t, pool)
	c.login(pw)

	for _, tt := range []struct {
		path string
		code int
		want string
	}{
		{"/apps/alpha/catalog", http.StatusOK, "/apps/alpha/catalog/app.open"},
		{"/apps/alpha/catalog?before=garbage", http.StatusOK, "app.open"},
		{"/apps/alpha/catalog/app.open", http.StatusOK, "&lt;b&gt;settings&lt;/b&gt;"},
		{"/apps/alpha/catalog/app.open", http.StatusOK, "p50 25 · p95 38.5"},
		{"/apps/alpha/catalog/app.open?range=90d", http.StatusOK, "old"},
		{"/apps/alpha/catalog/tap", http.StatusOK, "No events with props in this range."},
		{"/apps/alpha/catalog/error.sync", http.StatusOK, "No events in this range."},
		{"/apps/alpha/catalog/beta.only", http.StatusNotFound, ""},
		{"/apps/alpha/catalog/missing", http.StatusNotFound, ""},
		{"/apps/alpha/catalog/%FF", http.StatusNotFound, ""},
		{"/apps/nope/catalog", http.StatusNotFound, ""},
		{"/apps/nope/catalog/app.open", http.StatusNotFound, ""},
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
}

func TestCatalogEventFragments(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedCatalog(t, pool)
	c.login(pw)

	fragment := func(target string) string {
		r := httptest.NewRequest(http.MethodGet, "/apps/alpha/catalog/app.open?key=cold", nil)
		r.Header.Set("HX-Request", "true")
		r.Header.Set("HX-Target", target)
		r.AddCookie(c.cookie)
		rec := httptest.NewRecorder()
		c.h.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d", target, rec.Code)
		}
		if vary := rec.Header().Values("Vary"); !slices.Contains(vary, "HX-Target") {
			t.Errorf("%s: Vary = %q", target, vary)
		}
		return rec.Body.String()
	}

	body := fragment(valuesTarget)
	if !strings.Contains(body, "Top props values") || strings.Contains(body, "Events per day") || strings.Contains(body, "<html") {
		t.Errorf("key change returned the wrong fragment: %.200s", body)
	}
	if !strings.Contains(body, `hx-swap-oob="true"`) || !strings.Contains(body, "/apps/alpha/catalog/app.open?key=cold&amp;range=30d") {
		t.Error("key change leaves the range links on the old key")
	}
	body = fragment("event")
	if !strings.Contains(body, "Events per day") || strings.Contains(body, "<html") {
		t.Errorf("range change returned the wrong fragment: %.200s", body)
	}
}

func TestValueText(t *testing.T) {
	t.Parallel()
	for v, want := range map[float64]string{38.5: "38.5", 1840: "1,840", math.Inf(1): "–", math.NaN(): "–"} {
		if got := valueText(v); got != want {
			t.Errorf("valueText(%v) = %q, want %q", v, got, want)
		}
	}
}

func TestTopValuesPercentilesCoverTopLabels(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := apps.Create(ctx, pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	ev := func(server string, value *float64) db.InsertEventsParams {
		return db.InsertEventsParams{
			AppID: "alpha", Name: "vpn.connect", DeviceID: "d", Ts: now, Value: value,
			Props: props(t, map[string]any{"server": server}),
		}
	}
	rows := []db.InsertEventsParams{ev("fra", fp(100)), ev("fra", fp(300)), ev("fra", nil), ev("fra", nil), ev("ams", nil), ev("ams", nil), ev("ams", nil)}
	for i := range 12 {
		rows = append(rows, ev(fmt.Sprintf("s%02d", i), fp(1)))
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	v, err := d.loadCatalogEvent(ctx, "alpha", "vpn.connect", "7d", "server", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Values.Rows) != 10 {
		t.Fatalf("rows = %d", len(v.Values.Rows))
	}
	want := []db.EventTopValuesRow{
		{Label: "fra", Events: 4, WithValue: 2, P50: 200, P95: 290},
		{Label: "ams", Events: 3},
		{Label: "s00", Events: 1, WithValue: 1, P50: 1, P95: 1},
	}
	if got := v.Values.Rows[:3]; !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %+v", got)
	}

	var b strings.Builder
	if err := propValues(v.Values).Render(ctx, &b); err != nil {
		t.Fatal(err)
	}
	if html := b.String(); !strings.Contains(html, "2 with value") || strings.Count(html, "with value") != 1 {
		t.Errorf("only fra has some events without a value: %s", html)
	}
}
