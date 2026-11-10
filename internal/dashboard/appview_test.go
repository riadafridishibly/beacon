package dashboard

import (
	"cmp"
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

func TestParseFilter(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"":   "{}",
		"{}": "{}",
		`{"platform": "macos", "props.result": "ok"}`:         `{"platform":"macos","props.result":"ok"}`,
		`{"props.cold": true, "props.n": 1}`:                  `{"props.cold":true,"props.n":1}`,
		`{"props.path": "C:\\u0000x"}`:                        `{"props.path":"C:\\u0000x"}`,
		`{"props.city": "` + strings.Repeat("ж", 1500) + `"}`: `{"props.city":"` + strings.Repeat("ж", 1500) + `"}`,
	} {
		got, msg := parseFilter(in)
		if msg != "" || string(got) != want {
			t.Errorf("parseFilter(%q) = %s, %q", in, got, msg)
		}
	}
	for _, in := range []string{
		"null", "[]", `"x"`, "{", `{"result": "ok"}`, `{"props.": 1}`, `{"platform": 1}`, `{"platform": null}`,
		`{"props.x": "\u0000"}`, `{"props.x": {"a": ["\u0000"]}}`, `{"props.x": {"\u0000": 1}}`, `{"props.x": "` + strings.Repeat("a", maxFilterLen) + `"}`,
	} {
		if _, msg := parseFilter(in); msg == "" {
			t.Errorf("parseFilter(%q) accepted", in)
		}
	}
}

func TestValidateTitle(t *testing.T) {
	t.Parallel()
	for title, want := range map[string]string{
		"Connect time":           "",
		"":                       "Enter a title.",
		"a\x00b":                 "Remove the NUL character.",
		strings.Repeat("a", 101): "Use at most 100 characters.",
	} {
		f := viewForm{Title: title, Event: "vpn.connect", Agg: "count"}
		f.validate()
		if f.TitleError != want {
			t.Errorf("title %.20q: error %q, want %q", title, f.TitleError, want)
		}
	}
}

func TestParseField(t *testing.T) {
	t.Parallel()
	for in, want := range map[string][2]string{
		"platform":     {"platform", ""},
		"country":      {"country", ""},
		"props.server": {"", "server"},
		"props.a.b":    {"", "a.b"},
	} {
		column, key, ok := parseField(in)
		if !ok || column != want[0] || key != want[1] {
			t.Errorf("parseField(%q) = %q, %q, %v", in, column, key, ok)
		}
	}
	for _, in := range []string{"", "city", "props", "props.", "server", "props.\x00"} {
		if _, _, ok := parseField(in); ok {
			t.Errorf("parseField(%q) accepted", in)
		}
	}
}

func seedViews(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"alpha", "beta"} {
		if _, err := apps.Create(ctx, pool, id, strings.ToUpper(id)); err != nil {
			t.Fatal(err)
		}
	}
	macos, windows := "macos", "windows"
	connect := func(i int, platform *string, value float64, p map[string]any) db.InsertEventsParams {
		return db.InsertEventsParams{
			AppID: "alpha", Name: "vpn.connect", DeviceID: fmt.Sprintf("d%d", i%2), Ts: now.Add(-time.Duration(i) * time.Minute),
			Platform: platform, Value: fp(value), Props: props(t, p),
		}
	}
	rows := []db.InsertEventsParams{
		connect(0, &macos, 100, map[string]any{"server": "fra", "result": "ok"}),
		connect(1, &macos, 300, map[string]any{"server": "fra", "result": "ok"}),
		connect(2, &macos, 5000, map[string]any{"server": "fra", "result": "timeout"}),
		connect(3, &windows, 50, map[string]any{"server": "ams", "result": "ok"}),
		connect(4, &macos, 70, map[string]any{"server": "ams", "result": "ok"}),
		connect(5, &macos, 10, map[string]any{"result": "ok"}),
		{AppID: "alpha", Name: "app.open", DeviceID: "d0", Ts: now},
	}
	if _, err := db.New(pool).InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
}

func createView(t *testing.T, pool *pgxpool.Pool, p db.CreateViewParams) {
	t.Helper()
	p.AppID, p.MaxViews, p.Shape = "alpha", maxViews, cmp.Or(p.Shape, "table")
	if p.Filter == nil {
		p.Filter = []byte("{}")
	}
	if _, err := db.New(pool).CreateView(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

func TestOverviewViews(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	seedViews(t, pool)
	ok := []byte(`{"props.result":"ok"}`)
	createView(t, pool, db.CreateViewParams{Title: "Connect time", Event: "vpn.connect", Agg: "p95", GroupBy: "props.server", Filter: ok})
	createView(t, pool, db.CreateViewParams{Title: "Windows", Event: "vpn.connect", Agg: "count", GroupBy: "props.server", Filter: []byte(`{"platform":"windows"}`)})
	createView(t, pool, db.CreateViewParams{Title: "Daily", Event: "vpn.connect", Agg: "count", Filter: ok})
	createView(t, pool, db.CreateViewParams{Title: "Sum", Event: "vpn.connect", Agg: "sum", GroupBy: "platform"})
	createView(t, pool, db.CreateViewParams{Title: "Broken", Event: "vpn.connect", Agg: "count", GroupBy: "city"})
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	v, err := d.loadOverview(context.Background(), "alpha", "7d")
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Views) != 5 {
		t.Fatalf("views = %d", len(v.Views))
	}
	type row struct {
		label  string
		events int64
		p95    float64
	}
	var got []row
	for _, r := range v.Views[0].Groups.Rows {
		got = append(got, row{r.Label, r.Events, r.P95})
	}
	if want := []row{{"fra", 2, 290}, {"ams", 2, 69}, {"", 1, 10}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("by p95 = %v, want %v", got, want)
	}
	if s := v.Views[0].Summary; s != `vpn.connect · p95 of value by props.server · props.result = "ok"` {
		t.Errorf("summary = %q", s)
	}
	if rows := v.Views[1].Groups.Rows; len(rows) != 1 || rows[0].Label != "ams" || rows[0].Events != 1 {
		t.Errorf("windows = %+v", rows)
	}
	daily := v.Views[2]
	if daily.Groups != nil || len(daily.Chart.Series) != 1 {
		t.Fatalf("daily = %+v", daily)
	}
	if today := daily.Chart.Series[0].Values[6]; today == nil || *today != 5 {
		t.Errorf("today's count = %v", today)
	}
	if rows := v.Views[3].Groups.Rows; len(rows) != 2 || rows[0].Label != "macos" || rows[0].Total != 5480 || rows[1].Total != 50 {
		t.Errorf("sum = %+v", rows)
	}
	if broken := v.Views[4]; !broken.Failed || broken.Title != "Broken" || v.Views[3].Failed {
		t.Errorf("a view that can't be computed = %+v", broken)
	}
}

func TestViewEditor(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedViews(t, pool)
	c.login(pw)

	if w := c.do(http.MethodGet, "/apps/alpha/views", nil); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "No views yet.") {
		t.Fatalf("empty list: %d", w.Code)
	}
	w := c.do(http.MethodGet, "/apps/alpha/views/new", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `<option value="vpn.connect">`) ||
		!strings.Contains(w.Body.String(), `<option value="props.server">`) {
		t.Fatalf("new view form: %d", w.Code)
	}

	form := url.Values{
		"title": {"Connect time"}, "event": {"vpn.connect"}, "agg": {"p95"},
		"group_by": {"props.server"}, "filter": {`{"props.result": "ok"}`},
	}
	if w := c.do(http.MethodPost, "/apps/alpha/views", form); w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/apps/alpha/views" {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	views, err := db.New(pool).ListViews(context.Background(), "alpha")
	if err != nil || len(views) != 1 || string(views[0].Filter) != `{"props.result": "ok"}` {
		t.Fatalf("stored = %+v, %v", views, err)
	}
	id := views[0].ID
	path := viewURL("alpha", id)
	if w := c.do(http.MethodGet, path, nil); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Connect time") {
		t.Errorf("edit form: %d", w.Code)
	}
	if body := c.do(http.MethodGet, "/apps/alpha", nil).Body.String(); !strings.Contains(body, "Connect time") || !strings.Contains(body, "fra") {
		t.Error("overview misses the view")
	}

	bad := url.Values{"title": {""}, "event": {"Bad Name"}, "agg": {"mean"}, "group_by": {"city"}, "filter": {"[1]"}, "shape": {"pie"}}
	w = c.do(http.MethodPost, path, bad)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid update: %d", w.Code)
	}
	for _, msg := range []string{"Enter a title.", "Use an event name", "Choose a measure.", "Use platform, app_version", "Use a JSON object", "Choose a table or lines."} {
		if !strings.Contains(w.Body.String(), msg) {
			t.Errorf("invalid update misses %q", msg)
		}
	}
	ungrouped := url.Values{"title": {"Lines"}, "event": {"vpn.connect"}, "agg": {"count"}, "shape": {"line"}}
	if w := c.do(http.MethodPost, path, ungrouped); w.Code != http.StatusSeeOther {
		t.Fatalf("lines without a group: %d", w.Code)
	}
	if v, err := db.New(pool).GetView(context.Background(), db.GetViewParams{AppID: "alpha", ID: id}); err != nil || v.Shape != "table" {
		t.Errorf("lines without a group stored %+v, %v", v, err)
	}

	form.Set("shape", "line")
	if w := c.do(http.MethodPost, path, form); w.Code != http.StatusSeeOther {
		t.Fatalf("update to lines: %d", w.Code)
	}
	if w := c.do(http.MethodGet, path, nil); !strings.Contains(w.Body.String(), `<option value="line" selected>`) {
		t.Error("edit form misses the stored shape")
	}
	// The tooltip names groups only on a line chart, and the table titles
	// them only in a table.
	if body := c.do(http.MethodGet, "/apps/alpha", nil).Body.String(); !strings.Contains(body, `<tspan class="fill-base-content/60">fra</tspan>`) ||
		strings.Contains(body, `title="fra"`) {
		t.Error("overview misses the lines")
	}
	form.Del("shape")

	form.Set("title", "Connect p50")
	form.Set("agg", "p50")
	form.Set("filter", "")
	if w := c.do(http.MethodPost, path, form); w.Code != http.StatusSeeOther {
		t.Fatalf("update: %d", w.Code)
	}
	v, err := db.New(pool).GetView(context.Background(), db.GetViewParams{AppID: "alpha", ID: id})
	if err != nil || v.Title != "Connect p50" || v.Agg != "p50" || string(v.Filter) != "{}" || v.Shape != "table" {
		t.Errorf("updated = %+v, %v", v, err)
	}

	for _, p := range []string{
		viewURL("beta", id), "/apps/alpha/views/0", "/apps/alpha/views/x", "/apps/nope/views", "/apps/alpha/views/999",
	} {
		if w := c.do(http.MethodGet, p, nil); w.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d", p, w.Code)
		}
	}
	if w := c.do(http.MethodPost, viewURL("beta", id), form); w.Code != http.StatusNotFound {
		t.Errorf("update through another app = %d", w.Code)
	}
	if w := c.do(http.MethodPost, viewURL("beta", id)+"/delete", url.Values{}); w.Code != http.StatusNotFound {
		t.Errorf("delete through another app = %d", w.Code)
	}
	if w := c.do(http.MethodPost, path+"/delete", url.Values{}); w.Code != http.StatusSeeOther {
		t.Errorf("delete = %d", w.Code)
	}
	if w := c.do(http.MethodGet, path, nil); w.Code != http.StatusNotFound {
		t.Errorf("deleted view = %d", w.Code)
	}
}

func TestViewLimit(t *testing.T) {
	t.Parallel()
	pool, c, pw := setup(t)
	seedViews(t, pool)
	c.login(pw)
	for range maxViews {
		createView(t, pool, db.CreateViewParams{Title: "v", Event: "vpn.connect", Agg: "count"})
	}
	form := url.Values{"title": {"one more"}, "event": {"vpn.connect"}, "agg": {"count"}}
	w := c.do(http.MethodPost, "/apps/alpha/views", form)
	if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "at most 20 views") {
		t.Errorf("over the limit: %d", w.Code)
	}
	if body := c.do(http.MethodGet, "/apps/alpha/views", nil).Body.String(); strings.Contains(body, "New view") {
		t.Error("full list still offers New view")
	}
}

func TestViewLines(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	if _, err := apps.Create(context.Background(), pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("x", 40)
	// Group i has 8-i events today; a and long also have one yesterday.
	var rows []db.InsertEventsParams
	for i, server := range []string{"a", "b", "c", long, "", "f", "g"} {
		for j := range 8 - i {
			rows = append(rows, db.InsertEventsParams{
				AppID: "alpha", Name: "vpn.connect", DeviceID: "d", Ts: now.Add(-time.Duration(j) * time.Minute),
				Value: fp(float64(10 * (i + 1))), Props: props(t, map[string]any{"server": server}),
			})
		}
		if server == "a" || server == long {
			rows = append(rows, db.InsertEventsParams{
				AppID: "alpha", Name: "vpn.connect", DeviceID: "d", Ts: now.Add(-24 * time.Hour),
				Value: fp(1), Props: props(t, map[string]any{"server": server}),
			})
		}
	}
	if _, err := db.New(pool).InsertEvents(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	createView(t, pool, db.CreateViewParams{Title: "Count", Event: "vpn.connect", Agg: "count", GroupBy: "props.server", Shape: "line"})
	createView(t, pool, db.CreateViewParams{Title: "p95", Event: "vpn.connect", Agg: "p95", GroupBy: "props.server", Shape: "line"})
	d := &dashboard{pool: pool, q: db.New(pool), nowFunc: func() time.Time { return now }}

	v, err := d.loadOverview(context.Background(), "alpha", "7d")
	if err != nil {
		t.Fatal(err)
	}
	count := v.Views[0].Chart
	if v.Views[0].Groups != nil || count.Kind != lineChart {
		t.Fatalf("count = %+v", v.Views[0])
	}
	type line struct {
		name, full, stroke string
		yesterday, today   any
	}
	val := func(p *float64) any {
		if p == nil {
			return nil
		}
		return *p
	}
	var got []line
	for _, s := range count.Series {
		got = append(got, line{s.Name, s.FullName, s.Stroke, val(s.Values[5]), val(s.Values[6])})
	}
	want := []line{
		{"a", "", "stroke-chart-1", 1.0, 8.0},
		{"b", "", "stroke-chart-2", 0.0, 7.0},
		{"c", "", "stroke-chart-3", 0.0, 6.0},
		{strings.Repeat("x", maxSeriesName-1) + "…", long, "stroke-chart-4", 1.0, 5.0},
		{"Unknown", "", "stroke-chart-5", 0.0, 4.0},
		{"Other", "", "stroke-chart-other", 0.0, 5.0},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("lines =\n%v\nwant\n%v", got, want)
	}
	if !strings.HasSuffix(count.Note, "per day · Other pools the remaining groups") {
		t.Errorf("note = %q", count.Note)
	}
	var placeholders []string
	for _, s := range count.Series {
		if s.Placeholder {
			placeholders = append(placeholders, s.Name)
		}
	}
	if fmt.Sprint(placeholders) != "[Unknown Other]" {
		t.Errorf("placeholders = %v", placeholders)
	}
	if first := count.Lines[0].Series.Name; first != "Other" || len(count.Lines) != 6 {
		t.Errorf("drawn first: %q of %d lines", first, len(count.Lines))
	}
	if !count.Legend {
		t.Error("grouped lines have no legend")
	}

	p95 := v.Views[1].Chart
	if b := p95.Series[1]; b.Values[5] != nil || b.Values[6] == nil || *b.Values[6] != 20 {
		t.Errorf("p95 of b = %v, %v", val(b.Values[5]), val(b.Values[6]))
	}
	if other := p95.Series[5]; other.Values[6] == nil || *other.Values[6] != 70 {
		t.Errorf("p95 of Other = %v", val(other.Values[6]))
	}
}

func TestLineNeedsGroup(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	if _, err := apps.Create(context.Background(), pool, "alpha", "Alpha"); err != nil {
		t.Fatal(err)
	}
	_, err := db.New(pool).CreateView(context.Background(), db.CreateViewParams{
		AppID: "alpha", Title: "v", Event: "vpn.connect", Agg: "count", Filter: []byte("{}"), Shape: "line", MaxViews: maxViews,
	})
	if err == nil {
		t.Error("stored lines without a group")
	}
}
