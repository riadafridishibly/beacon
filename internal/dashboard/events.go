package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/web"
)

const (
	eventsPageSize = 50
	eventTimeUTC   = "2006-01-02 15:04:05.000 UTC"
)

// eventFilter holds the exact values a list is narrowed to; empty means any.
type eventFilter struct {
	Name, Device, User, Country, Version string
}

func parseEventFilter(q url.Values) eventFilter {
	return eventFilter{
		Name:    cleanFilter(q.Get("name")),
		Device:  cleanFilter(q.Get("device")),
		User:    cleanFilter(q.Get("user")),
		Country: strings.ToUpper(cleanFilter(q.Get("country"))),
		Version: cleanFilter(q.Get("version")),
	}
}

// cleanFilter drops values PostgreSQL would reject as text. No stored event
// can hold them.
func cleanFilter(s string) string {
	s = strings.TrimSpace(s)
	if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
		return ""
	}
	return s
}

func eventsURL(appID string, f eventFilter, before string) string {
	q := url.Values{}
	for _, p := range []struct{ key, value string }{
		{"name", f.Name},
		{"device", f.Device},
		{"user", f.User},
		{"country", f.Country},
		{"version", f.Version},
		{"before", before},
	} {
		if p.value != "" {
			q.Set(p.key, p.value)
		}
	}
	u := "/apps/" + appID + "/events"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

func eventCursor(e db.Event) string {
	return strconv.FormatInt(e.Ts.UnixMicro(), 10) + "_" + strconv.FormatInt(e.ID, 10)
}

func parseEventCursor(s string) (ts time.Time, id int64, ok bool) {
	micro, rest, found := strings.Cut(s, "_")
	if !found {
		return time.Time{}, 0, false
	}
	m, err := strconv.ParseInt(micro, 10, 64)
	if err != nil || m <= 0 {
		return time.Time{}, 0, false
	}
	id, err = strconv.ParseInt(rest, 10, 64)
	if err != nil || id <= 0 {
		return time.Time{}, 0, false
	}
	return time.UnixMicro(m), id, true
}

type eventsView struct {
	App    db.App
	Filter eventFilter
	Before string
	Older  string
	Rows   []eventRow
}

type eventRow struct {
	Time, Name, Device, User, Country, Version string
	Fields                                     []eventField
	Props                                      string
}

// eventField is one line of an expanded row. Filter, when set, is the URL of
// the list narrowed to this value.
type eventField struct {
	Label, Value, Filter string
}

func (d *dashboard) events(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	v, err := d.loadEvents(r.Context(), r.PathValue("id"), parseEventFilter(q), q.Get("before"))
	if err != nil {
		return err
	}
	if web.IsHTMX(r) {
		// A submitted form would otherwise push every empty field.
		w.Header().Set("HX-Push-Url", eventsURL(v.App.ID, v.Filter, v.Before))
		return web.Render(w, r, http.StatusOK, eventsBody(v))
	}
	return web.Render(w, r, http.StatusOK, eventsPage(auth.Nav(r.Context()), v))
}

func (d *dashboard) loadEvents(ctx context.Context, appID string, f eventFilter, before string) (eventsView, error) {
	app, err := d.app(ctx, appID)
	if err != nil {
		return eventsView{}, err
	}
	beforeTS, beforeID, ok := parseEventCursor(before)
	if !ok {
		before = ""
		beforeTS, beforeID = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), math.MaxInt64
	}
	rows, err := d.q.ListEvents(ctx, db.ListEventsParams{
		AppID:      appID,
		BeforeTs:   beforeTS,
		BeforeID:   beforeID,
		Name:       optional(f.Name),
		DeviceID:   optional(f.Device),
		UserID:     optional(f.User),
		Country:    optional(f.Country),
		AppVersion: optional(f.Version),
		PageLimit:  eventsPageSize + 1,
	})
	if err != nil {
		return eventsView{}, err
	}
	v := eventsView{App: app, Filter: f, Before: before}
	if len(rows) > eventsPageSize {
		rows = rows[:eventsPageSize]
		v.Older = eventsURL(appID, f, eventCursor(rows[eventsPageSize-1]))
	}
	for _, e := range rows {
		v.Rows = append(v.Rows, newEventRow(appID, f, e))
	}
	return v, nil
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func newEventRow(appID string, f eventFilter, e db.Event) eventRow {
	r := eventRow{
		Time:    e.Ts.UTC().Format(time.DateTime),
		Name:    e.Name,
		Device:  e.DeviceID,
		User:    deref(e.UserID),
		Country: deref(e.Country),
		Version: deref(e.AppVersion),
		Props:   prettyProps(e.Props),
	}
	add := func(label, value string, narrow func(*eventFilter, string)) {
		if value == "" {
			return
		}
		field := eventField{Label: label, Value: value}
		if narrow != nil {
			g := f
			narrow(&g, value)
			field.Filter = eventsURL(appID, g, "")
		}
		r.Fields = append(r.Fields, field)
	}
	add("Event", e.Name, func(g *eventFilter, v string) { g.Name = v })
	add("Time", e.Ts.UTC().Format(eventTimeUTC), nil)
	add("Received", e.ReceivedAt.UTC().Format(eventTimeUTC), nil)
	add("Device", e.DeviceID, func(g *eventFilter, v string) { g.Device = v })
	add("User", r.User, func(g *eventFilter, v string) { g.User = v })
	add("Session", deref(e.SessionID), nil)
	add("Country", r.Country, func(g *eventFilter, v string) { g.Country = v })
	add("City", deref(e.City), nil)
	add("App version", r.Version, func(g *eventFilter, v string) { g.Version = v })
	add("Platform", deref(e.Platform), nil)
	add("OS version", deref(e.OSVersion), nil)
	if e.Value != nil {
		add("Value", strconv.FormatFloat(*e.Value, 'g', -1, 64), nil)
	}
	return r
}

func prettyProps(props json.RawMessage) string {
	if len(props) == 0 || string(props) == "null" {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, props, "", "  "); err != nil {
		return string(props)
	}
	return buf.String()
}
