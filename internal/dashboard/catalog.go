package dashboard

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/web"
)

const (
	catalogPageSize = 50
	propKeySample   = 1000
	maxListedKeys   = 8
	maxPickerKeys   = 30
	valuesTarget    = "values"
)

func catalogURL(appID, before string) string {
	u := "/apps/" + appID + "/catalog"
	if before != "" {
		u += "?" + url.Values{"before": {before}}.Encode()
	}
	return u
}

func catalogEventURL(appID, name, rng, key string) string {
	u := "/apps/" + appID + "/catalog/" + url.PathEscape(name)
	q := url.Values{}
	if rng != "" && rng != defaultRange {
		q.Set("range", rng)
	}
	if key != "" {
		q.Set("key", key)
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

func catalogCursor(events int64, name string) string {
	return strconv.FormatInt(events, 10) + "_" + name
}

func parseCatalogCursor(s string) (events int64, name string, ok bool) {
	n, name, found := strings.Cut(s, "_")
	if !found || name == "" || !validText(name) {
		return 0, "", false
	}
	events, err := strconv.ParseInt(n, 10, 64)
	if err != nil || events <= 0 {
		return 0, "", false
	}
	return events, name, true
}

type catalogView struct {
	App    db.App
	Now    time.Time
	Before string
	Older  string
	Rows   []catalogRow
}

// catalogRow is one event name. MoreKeys counts the props keys left out of
// Keys.
type catalogRow struct {
	Name     string
	Events   int64
	LastSeen time.Time
	Keys     []string
	MoreKeys int
}

// catalogEventView is one event name's page. Total is the whole range.
type catalogEventView struct {
	App      db.App
	Name     string
	Range    string
	Now      time.Time
	LastSeen time.Time
	Total    db.EventDailyRow
	Count    chartView
	Value    chartView
	Values   propValuesView
}

func (v catalogEventView) hasValues() bool { return v.Total.WithValue > 0 }

// propValuesView is the top values of one props key. Key is empty when no
// event in the range has props.
type propValuesView struct {
	AppID, Name, Range, Key string
	Keys                    []string
	Rows                    []db.EventTopValuesRow
	Top                     int64
}

func (d *dashboard) catalog(w http.ResponseWriter, r *http.Request) error {
	v, err := d.loadCatalog(r.Context(), r.PathValue("id"), r.URL.Query().Get("before"))
	if err != nil {
		return err
	}
	if web.IsHTMX(r) {
		return web.Render(w, r, http.StatusOK, catalogBody(v))
	}
	return web.Render(w, r, http.StatusOK, catalogPage(auth.Nav(r.Context()), v))
}

func (d *dashboard) loadCatalog(ctx context.Context, appID, before string) (catalogView, error) {
	app, err := d.app(ctx, appID)
	if err != nil {
		return catalogView{}, err
	}
	beforeCount, beforeName, ok := parseCatalogCursor(before)
	if !ok {
		before, beforeCount, beforeName = "", math.MaxInt64, ""
	}
	rows, err := d.q.ListEventNames(ctx, db.ListEventNamesParams{
		AppID:       appID,
		BeforeCount: beforeCount,
		BeforeName:  beforeName,
		PageLimit:   catalogPageSize + 1,
		KeySample:   propKeySample,
	})
	if err != nil {
		return catalogView{}, err
	}
	v := catalogView{App: app, Now: d.now(), Before: before}
	if len(rows) > catalogPageSize {
		rows = rows[:catalogPageSize]
		last := rows[catalogPageSize-1]
		v.Older = catalogURL(appID, catalogCursor(last.Events, last.Name))
	}
	for _, r := range rows {
		row := catalogRow{Name: r.Name, Events: r.Events, LastSeen: r.LastSeen, Keys: r.Keys}
		if len(row.Keys) > maxListedKeys {
			row.MoreKeys = len(row.Keys) - maxListedKeys
			row.Keys = row.Keys[:maxListedKeys]
		}
		v.Rows = append(v.Rows, row)
	}
	return v, nil
}

func (d *dashboard) catalogEvent(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	appID, name, rng, key := r.PathValue("id"), r.PathValue("name"), parseRange(q.Get("range")), q.Get("key")
	if web.IsHTMX(r) && r.Header.Get("HX-Target") == valuesTarget {
		v, err := d.loadCatalogEvent(r.Context(), appID, name, rng, key, false)
		if err != nil {
			return err
		}
		return web.Render(w, r, http.StatusOK, keyChange(v.Values))
	}
	v, err := d.loadCatalogEvent(r.Context(), appID, name, rng, key, true)
	if err != nil {
		return err
	}
	if web.IsHTMX(r) {
		return web.Render(w, r, http.StatusOK, catalogEventBody(v))
	}
	return web.Render(w, r, http.StatusOK, catalogEventPage(auth.Nav(r.Context()), v))
}

// loadCatalogEvent skips the charts when only the top values are shown again,
// since the percentiles are what cost the most.
func (d *dashboard) loadCatalogEvent(ctx context.Context, appID, name, rng, key string, charts bool) (catalogEventView, error) {
	app, err := d.app(ctx, appID)
	if err != nil {
		return catalogEventView{}, err
	}
	if !validText(name) {
		return catalogEventView{}, web.ErrNotFound
	}
	lastSeen, err := d.q.EventLastSeen(ctx, db.EventLastSeenParams{AppID: appID, Name: name})
	if errors.Is(err, pgx.ErrNoRows) {
		return catalogEventView{}, web.ErrNotFound
	}
	if err != nil {
		return catalogEventView{}, err
	}
	now := d.now()
	since, until, days := window(rng, now)
	v := catalogEventView{App: app, Name: name, Range: rng, Now: now, LastSeen: lastSeen}
	if charts {
		rows, err := d.q.EventDaily(ctx, db.EventDailyParams{AppID: appID, Name: name, Since: since, Until: until})
		if err != nil {
			return catalogEventView{}, err
		}
		v.buildCharts(days, rows)
	}
	v.Values, err = d.loadPropValues(ctx, appID, name, rng, key, since, until)
	return v, err
}

func (d *dashboard) loadPropValues(ctx context.Context, appID, name, rng, key string, since, until time.Time) (propValuesView, error) {
	v := propValuesView{AppID: appID, Name: name, Range: rng}
	var err error
	v.Keys, err = d.q.EventPropKeys(ctx, db.EventPropKeysParams{
		AppID: appID, Name: name, Since: since, Until: until,
		KeySample: propKeySample, KeyLimit: maxPickerKeys,
	})
	if err != nil || len(v.Keys) == 0 {
		return v, err
	}
	v.Key = v.Keys[0]
	for _, k := range v.Keys {
		if k == key {
			v.Key = k
		}
	}
	v.Rows, err = d.q.EventTopValues(ctx, db.EventTopValuesParams{AppID: appID, Name: name, Since: since, Until: until, Key: v.Key})
	if err != nil {
		return v, err
	}
	if len(v.Rows) > 0 {
		v.Top = v.Rows[0].Events
	}
	return v, nil
}

// buildCharts lays the rows out over days, so a day without events still has
// a slot on the axis.
func (v *catalogEventView) buildCharts(days []time.Time, rows []db.EventDailyRow) {
	byDay := make(map[string]db.EventDailyRow, len(rows))
	for _, r := range rows {
		if !r.Day.Valid {
			v.Total = r
			continue
		}
		byDay[dayKey(r.Day.Time)] = r
	}
	n := len(days)
	counts, p50, p95 := make([]*float64, n), make([]*float64, n), make([]*float64, n)
	for i, day := range days {
		r := byDay[dayKey(day)]
		counts[i] = count(r.Events)
		if r.WithValue > 0 {
			p50[i], p95[i] = finite(r.P50), finite(r.P95)
		}
	}
	v.Count = layout(chartData{
		Title: "Events per day",
		Empty: "No events in this range.",
		Kind:  columnChart, Days: days, Format: countFormat,
		Series: []series{withValues(slot1, "Events", counts)},
	})
	v.Value = layout(chartData{
		Title: "Value",
		Note:  "p50 and p95 per day",
		Empty: "No events with a value in this range.",
		Kind:  lineChart, Days: days, Format: numberFormat,
		Series: []series{withValues(slot1, "p50", p50), withValues(slot2, "p95", p95)},
	})
}
