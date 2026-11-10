package dashboard

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/ingest"
	"github.com/riadafridishibly/beacon/internal/web"
)

const (
	// maxViews bounds the queries the overview runs.
	maxViews       = 20
	viewGroupLimit = 20
	maxTitleLen    = 100
	maxPropKeyLen  = 100
	maxFilterLen   = 2000
)

type aggregate struct{ ID, Label string }

var aggregates = []aggregate{
	{"count", "Events"},
	{"distinct_devices", "Devices"},
	{"sum", "Sum of value"},
	{"p50", "p50 of value"},
	{"p95", "p95 of value"},
}

func aggLabel(id string) string {
	for _, a := range aggregates {
		if a.ID == id {
			return a.Label
		}
	}
	return id
}

var viewColumns = []string{"platform", "app_version", "country"}

// parseField reads a group_by or filter key: a column, or props.<key>.
func parseField(s string) (column, propKey string, ok bool) {
	if slices.Contains(viewColumns, s) {
		return s, "", true
	}
	key, found := strings.CutPrefix(s, "props.")
	if !found || key == "" || utf8.RuneCountInString(key) > maxPropKeyLen || !validText(key) {
		return "", "", false
	}
	return "", key, true
}

// parseFilter checks a filter typed into the form and returns it as compact
// JSON with sorted keys.
func parseFilter(s string) ([]byte, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return []byte("{}"), ""
	}
	if len(s) > maxFilterLen {
		return nil, "Use at most 2,000 characters."
	}
	// PostgreSQL rejects NUL in jsonb.
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(s), &m); err != nil || m == nil || strings.Contains(s, `\u0000`) {
		return nil, `Use a JSON object, such as {"props.result": "ok"}.`
	}
	for k, raw := range m {
		column, _, ok := parseField(k)
		if !ok {
			return nil, fmt.Sprintf("%q is not a column (platform, app_version, country) or props.<key>.", k)
		}
		var text string
		if column != "" && json.Unmarshal(raw, &text) != nil {
			return nil, fmt.Sprintf("%s takes a string.", k)
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, `Use a JSON object, such as {"props.result": "ok"}.`
	}
	return b, ""
}

// viewQuery is a view's grouping and filter as query parameters.
type viewQuery struct {
	Dimension, PropKey            string
	PropsFilter                   []byte
	Platform, AppVersion, Country *string
}

func newViewQuery(v db.AppView) (viewQuery, error) {
	var q viewQuery
	if v.GroupBy != "" {
		column, key, ok := parseField(v.GroupBy)
		if !ok {
			return q, fmt.Errorf("view %d: group_by %q", v.ID, v.GroupBy)
		}
		q.Dimension, q.PropKey = cmp.Or(column, "props"), key
	}
	var filter map[string]json.RawMessage
	if err := json.Unmarshal(v.Filter, &filter); err != nil {
		return q, fmt.Errorf("view %d: filter: %w", v.ID, err)
	}
	props := map[string]json.RawMessage{}
	for k, raw := range filter {
		column, key, ok := parseField(k)
		if !ok {
			return q, fmt.Errorf("view %d: filter key %q", v.ID, k)
		}
		if column == "" {
			props[key] = raw
			continue
		}
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return q, fmt.Errorf("view %d: filter %s: %w", v.ID, k, err)
		}
		switch column {
		case "platform":
			q.Platform = &text
		case "app_version":
			q.AppVersion = &text
		case "country":
			q.Country = &text
		}
	}
	var err error
	q.PropsFilter, err = json.Marshal(props)
	return q, err
}

// describeView is a view's definition in one line.
func describeView(v db.AppView) string {
	parts := []string{v.Event, aggLabel(v.Agg)}
	if v.GroupBy != "" {
		parts[1] += " by " + v.GroupBy
	}
	var filter map[string]json.RawMessage
	if json.Unmarshal(v.Filter, &filter) == nil && len(filter) > 0 {
		var conds []string
		for _, k := range slices.Sorted(maps.Keys(filter)) {
			conds = append(conds, k+" = "+string(filter[k]))
		}
		parts = append(parts, strings.Join(conds, " and "))
	}
	return strings.Join(parts, " · ")
}

// aggValue is the measure a view shows, or nil when it is a measure of value
// and no event had one.
func aggValue(agg string, events, devices, withValue int64, total, p50, p95 float64) *float64 {
	switch agg {
	case "count":
		return count(events)
	case "distinct_devices":
		return count(devices)
	}
	if withValue == 0 {
		return nil
	}
	switch agg {
	case "sum":
		return finite(total)
	case "p50":
		return finite(p50)
	}
	return finite(p95)
}

// viewCard is a view computed over the overview's range. Chart is set for a
// view without group_by, Groups otherwise.
type viewCard struct {
	ID      int64
	Title   string
	Summary string
	Chart   chartView
	Groups  *viewGroups
}

type viewGroups struct {
	Field  string
	Agg    string
	Values bool
	Rows   []db.ViewGroupsRow
}

func (d *dashboard) loadViewCards(ctx context.Context, appID string, since, until time.Time, days []time.Time) ([]viewCard, error) {
	views, err := d.q.ListViews(ctx, appID)
	if err != nil {
		return nil, err
	}
	cards := make([]viewCard, 0, len(views))
	for _, v := range views {
		q, err := newViewQuery(v)
		if err != nil {
			return nil, err
		}
		c := viewCard{ID: v.ID, Title: v.Title, Summary: describeView(v)}
		if v.GroupBy == "" {
			rows, err := d.q.ViewDaily(ctx, db.ViewDailyParams{
				AppID: appID, Name: v.Event, Since: since, Until: until,
				PropsFilter: q.PropsFilter, Platform: q.Platform, AppVersion: q.AppVersion, Country: q.Country,
			})
			if err != nil {
				return nil, fmt.Errorf("view %d: %w", v.ID, err)
			}
			c.Chart = viewChart(v, days, rows)
		} else {
			rows, err := d.q.ViewGroups(ctx, db.ViewGroupsParams{
				AppID: appID, Name: v.Event, Since: since, Until: until,
				Dimension: q.Dimension, PropKey: q.PropKey,
				PropsFilter: q.PropsFilter, Platform: q.Platform, AppVersion: q.AppVersion, Country: q.Country,
				GroupLimit: viewGroupLimit,
			})
			if err != nil {
				return nil, fmt.Errorf("view %d: %w", v.ID, err)
			}
			c.Groups = groupsFor(v, rows)
		}
		cards = append(cards, c)
	}
	return cards, nil
}

func viewChart(v db.AppView, days []time.Time, rows []db.ViewDailyRow) chartView {
	byDay := make(map[string]db.ViewDailyRow, len(rows))
	for _, r := range rows {
		byDay[dayKey(r.Day.Time)] = r
	}
	values := make([]*float64, len(days))
	for i, day := range days {
		r := byDay[dayKey(day)]
		values[i] = aggValue(v.Agg, r.Events, r.Devices, r.WithValue, r.Total, r.P50, r.P95)
	}
	c := chartData{
		Title: v.Title,
		Note:  describeView(v),
		Empty: "No matching " + v.Event + " events in this range.",
		Kind:  columnChart, Days: days, Format: countFormat,
		Series: []series{withValues(slot1, aggLabel(v.Agg), values)},
	}
	switch v.Agg {
	case "sum":
		c.Format = numberFormat
		c.Empty = "No matching " + v.Event + " events with a value in this range."
	case "p50", "p95":
		c.Kind, c.Format, c.PlotZeros = lineChart, numberFormat, true
		c.Empty = "No matching " + v.Event + " events with a value in this range."
	}
	return layout(c)
}

// groupsFor orders the groups by the view's measure. Ties keep the query's
// order, by count.
func groupsFor(v db.AppView, rows []db.ViewGroupsRow) *viewGroups {
	key := func(r db.ViewGroupsRow) float64 {
		if p := aggValue(v.Agg, r.Events, r.Devices, r.WithValue, r.Total, r.P50, r.P95); p != nil {
			return *p
		}
		return math.Inf(-1)
	}
	slices.SortStableFunc(rows, func(a, b db.ViewGroupsRow) int { return cmp.Compare(key(b), key(a)) })
	g := &viewGroups{Field: v.GroupBy, Agg: v.Agg, Rows: rows}
	g.Values = slices.ContainsFunc(rows, func(r db.ViewGroupsRow) bool { return r.WithValue > 0 })
	return g
}

func viewsURL(appID string) string { return "/apps/" + appID + "/views" }

func viewURL(appID string, id int64) string {
	return viewsURL(appID) + "/" + strconv.FormatInt(id, 10)
}

// viewForm is the view editor. ID is zero for a new view. Events and Fields
// are suggestions from what the app has sent.
type viewForm struct {
	ID                                 int64
	Title, Event, Agg, GroupBy, Filter string
	TitleError, EventError, AggError   string
	GroupByError, FilterError          string
	FormError                          string
	Events, Fields                     []string
}

func (f viewForm) action(appID string) string {
	if f.ID == 0 {
		return viewsURL(appID)
	}
	return viewURL(appID, f.ID)
}

// validate sets the form's errors and returns the filter to store.
func (f *viewForm) validate() []byte {
	switch n := utf8.RuneCountInString(f.Title); {
	case n == 0:
		f.TitleError = "Enter a title."
	case n > maxTitleLen || !validText(f.Title):
		f.TitleError = "Use at most 100 characters."
	}
	if !ingest.ValidName(f.Event) {
		f.EventError = "Use an event name, such as vpn.connect."
	}
	if !slices.ContainsFunc(aggregates, func(a aggregate) bool { return a.ID == f.Agg }) {
		f.AggError = "Choose a measure."
	}
	if f.GroupBy != "" {
		if _, _, ok := parseField(f.GroupBy); !ok {
			f.GroupByError = "Use platform, app_version, country or props.<key>."
		}
	}
	filter, msg := parseFilter(f.Filter)
	f.FilterError = msg
	return filter
}

func (f viewForm) valid() bool {
	return f.TitleError == "" && f.EventError == "" && f.AggError == "" &&
		f.GroupByError == "" && f.FilterError == ""
}

// suggest fills the editor's suggestions from the catalog's first page.
func (d *dashboard) suggest(ctx context.Context, appID string, f *viewForm) error {
	rows, err := d.q.ListEventNames(ctx, db.ListEventNamesParams{
		AppID: appID, BeforeCount: math.MaxInt64, BeforeName: "",
		PageLimit: catalogPageSize, KeySample: propKeySample,
	})
	if err != nil {
		return err
	}
	keys := map[string]bool{}
	for _, r := range rows {
		f.Events = append(f.Events, r.Name)
		for _, k := range r.Keys {
			keys[k] = true
		}
	}
	slices.Sort(f.Events)
	f.Fields = slices.Clone(viewColumns)
	for k := range keys {
		f.Fields = append(f.Fields, "props."+k)
	}
	slices.Sort(f.Fields[len(viewColumns):])
	return nil
}

// view returns web.ErrNotFound for an id that isn't one of the app's views.
func (d *dashboard) view(ctx context.Context, appID, id string) (db.AppView, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return db.AppView{}, web.ErrNotFound
	}
	v, err := d.q.GetView(ctx, db.GetViewParams{AppID: appID, ID: n})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.AppView{}, web.ErrNotFound
	}
	return v, err
}

func (d *dashboard) viewList(w http.ResponseWriter, r *http.Request) error {
	app, err := d.app(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	views, err := d.q.ListViews(r.Context(), app.ID)
	if err != nil {
		return err
	}
	return web.Render(w, r, http.StatusOK, viewsPage(auth.Nav(r.Context()), app, views))
}

func (d *dashboard) newView(w http.ResponseWriter, r *http.Request) error {
	app, err := d.app(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	f := viewForm{Agg: "count"}
	if err := d.suggest(r.Context(), app.ID, &f); err != nil {
		return err
	}
	return web.Render(w, r, http.StatusOK, viewFormPage(auth.Nav(r.Context()), app, f))
}

func (d *dashboard) editView(w http.ResponseWriter, r *http.Request) error {
	app, err := d.app(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	v, err := d.view(r.Context(), app.ID, r.PathValue("view"))
	if err != nil {
		return err
	}
	f := viewForm{ID: v.ID, Title: v.Title, Event: v.Event, Agg: v.Agg, GroupBy: v.GroupBy}
	if string(v.Filter) != "{}" {
		f.Filter = string(v.Filter)
	}
	if err := d.suggest(r.Context(), app.ID, &f); err != nil {
		return err
	}
	return web.Render(w, r, http.StatusOK, viewFormPage(auth.Nav(r.Context()), app, f))
}

// saveView creates a view, or updates the one in the path.
func (d *dashboard) saveView(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	app, err := d.app(ctx, r.PathValue("id"))
	if err != nil {
		return err
	}
	f := viewForm{
		Title:   strings.TrimSpace(r.PostFormValue("title")),
		Event:   strings.TrimSpace(r.PostFormValue("event")),
		Agg:     r.PostFormValue("agg"),
		GroupBy: strings.TrimSpace(r.PostFormValue("group_by")),
		Filter:  strings.TrimSpace(r.PostFormValue("filter")),
	}
	if id := r.PathValue("view"); id != "" {
		v, err := d.view(ctx, app.ID, id)
		if err != nil {
			return err
		}
		f.ID = v.ID
	}
	if filter := f.validate(); f.valid() {
		saved, err := d.storeView(ctx, app.ID, &f, filter)
		if err != nil {
			return err
		}
		if saved {
			web.Redirect(w, r, viewsURL(app.ID))
			return nil
		}
	}
	if err := d.suggest(ctx, app.ID, &f); err != nil {
		return err
	}
	return web.Render(w, r, http.StatusUnprocessableEntity, viewFormPage(auth.Nav(ctx), app, f))
}

// storeView reports false with f.FormError set when the app has no room for
// another view.
func (d *dashboard) storeView(ctx context.Context, appID string, f *viewForm, filter []byte) (bool, error) {
	if f.ID == 0 {
		_, err := d.q.CreateView(ctx, db.CreateViewParams{
			AppID: appID, Title: f.Title, Event: f.Event, Agg: f.Agg, GroupBy: f.GroupBy,
			Filter: filter, MaxViews: maxViews,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			f.FormError = fmt.Sprintf("An app can have at most %d views.", maxViews)
			return false, nil
		}
		return err == nil, err
	}
	n, err := d.q.UpdateView(ctx, db.UpdateViewParams{
		AppID: appID, ID: f.ID, Title: f.Title, Event: f.Event, Agg: f.Agg, GroupBy: f.GroupBy,
		Filter: filter,
	})
	if err == nil && n == 0 {
		return false, web.ErrNotFound
	}
	return err == nil, err
}

func (d *dashboard) deleteView(w http.ResponseWriter, r *http.Request) error {
	app, err := d.app(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	v, err := d.view(r.Context(), app.ID, r.PathValue("view"))
	if err != nil {
		return err
	}
	if _, err := d.q.DeleteView(r.Context(), db.DeleteViewParams{AppID: app.ID, ID: v.ID}); err != nil {
		return err
	}
	web.Redirect(w, r, viewsURL(app.ID))
	return nil
}
