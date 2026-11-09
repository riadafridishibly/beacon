package dashboard

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/web"
)

const defaultRange = "7d"

var ranges = []string{"24h", "7d", "30d", "90d"}

var rangeDays = map[string]int{"7d": 7, "30d": 30, "90d": 90}

// window is the span a range covers and its UTC day buckets. 24h is rolling,
// which matches the app list, and so touches two days. The others are whole
// UTC days ending today, so no bucket but today's is partial. until is the
// end of today, because client clocks can run up to an hour ahead.
func window(rng string, now time.Time) (since, until time.Time, days []time.Time) {
	now = now.UTC()
	today := now.Truncate(24 * time.Hour)
	until = today.Add(24 * time.Hour)
	if n, ok := rangeDays[rng]; ok {
		since = today.AddDate(0, 0, 1-n)
	} else {
		since = now.Add(-24 * time.Hour)
	}
	for d := since.Truncate(24 * time.Hour); !d.After(today); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	return since, until, days
}

func parseRange(rng string) string {
	if _, ok := rangeDays[rng]; !ok && rng != "24h" {
		return defaultRange
	}
	return rng
}

type overviewView struct {
	App     db.App
	Range   string
	Now     time.Time
	Totals  db.OverviewTotalsRow
	Active  chartView
	Usage   chartView
	Opens   chartView
	Startup chartView
	Errors  chartView

	TopErrors []db.OverviewTopErrorsRow
	Breakdown []breakdownView
}

type breakdownView struct {
	Title string
	Rows  []db.OverviewBreakdownRow
	Top   int64
}

func (d *dashboard) overview(w http.ResponseWriter, r *http.Request) error {
	v, err := d.loadOverview(r.Context(), r.PathValue("id"), parseRange(r.URL.Query().Get("range")))
	if err != nil {
		return err
	}
	if web.IsHTMX(r) {
		return web.Render(w, r, http.StatusOK, overviewBody(v))
	}
	return web.Render(w, r, http.StatusOK, overviewPage(auth.Nav(r.Context()), v))
}

func (d *dashboard) loadOverview(ctx context.Context, appID, rng string) (overviewView, error) {
	app, err := d.app(ctx, appID)
	if err != nil {
		return overviewView{}, err
	}
	now := d.now()
	since, until, days := window(rng, now)
	v := overviewView{App: app, Range: rng, Now: now}

	v.Totals, err = d.q.OverviewTotals(ctx, db.OverviewTotalsParams{AppID: appID, Since: since, Until: until})
	if err != nil {
		return overviewView{}, fmt.Errorf("totals: %w", err)
	}
	if v.Totals.Events == 0 {
		return v, nil
	}
	daily, err := d.q.OverviewDaily(ctx, db.OverviewDailyParams{AppID: appID, Since: since, Until: until})
	if err != nil {
		return overviewView{}, fmt.Errorf("daily: %w", err)
	}
	usage, err := d.q.OverviewUsage(ctx, db.OverviewUsageParams{AppID: appID, Since: since, Until: until})
	if err != nil {
		return overviewView{}, fmt.Errorf("usage: %w", err)
	}
	v.buildCharts(days, daily, usage)

	v.TopErrors, err = d.q.OverviewTopErrors(ctx, db.OverviewTopErrorsParams{AppID: appID, Since: since, Until: until})
	if err != nil {
		return overviewView{}, fmt.Errorf("top errors: %w", err)
	}
	for _, b := range []struct{ title, dimension string }{
		{"Countries", "country"},
		{"Platforms", "platform"},
		{"App versions", "app_version"},
	} {
		rows, err := d.q.OverviewBreakdown(ctx, db.OverviewBreakdownParams{
			AppID: appID, Since: since, Until: until, Dimension: b.dimension,
		})
		if err != nil {
			return overviewView{}, fmt.Errorf("breakdown %s: %w", b.dimension, err)
		}
		bv := breakdownView{Title: b.title, Rows: rows}
		if len(rows) > 0 {
			bv.Top = rows[0].Devices
		}
		v.Breakdown = append(v.Breakdown, bv)
	}
	return v, nil
}

func dayKey(t time.Time) string { return t.UTC().Format(time.DateOnly) }

// buildCharts lays the rows out over days, so a day without events still has
// a slot on the axis.
func (v *overviewView) buildCharts(days []time.Time, daily []db.OverviewDailyRow, usage []db.OverviewUsageRow) {
	byDay := make(map[string]db.OverviewDailyRow, len(daily))
	for _, r := range daily {
		byDay[dayKey(r.Day.Time)] = r
	}
	seconds := make(map[string]float64, len(usage))
	for _, r := range usage {
		seconds[dayKey(r.Day.Time)] = r.Seconds
	}

	n := len(days)
	devices, users, opens, errs := make([]*float64, n), make([]*float64, n), make([]*float64, n), make([]*float64, n)
	p50, p95, perDevice := make([]*float64, n), make([]*float64, n), make([]*float64, n)
	for i, day := range days {
		r := byDay[dayKey(day)]
		devices[i], users[i] = count(r.Devices), count(r.Users)
		opens[i], errs[i] = count(r.Opens), count(r.Errors)
		if r.TimedOpens > 0 {
			p50[i], p95[i] = finite(r.OpenP50), finite(r.OpenP95)
		}
		if s, ok := seconds[dayKey(day)]; ok && r.Devices > 0 {
			perDevice[i] = finite(s / float64(r.Devices))
		}
	}

	v.Active = layout(chartData{
		Title: "Daily active devices and users",
		Empty: "No activity in this range.",
		Kind:  lineChart, Days: days, Format: countFormat,
		Series: []series{withValues(slot1, "Devices", devices), withValues(slot2, "Users", users)},
	})
	v.Usage = layout(chartData{
		Title: "Usage per active device",
		Note:  "Session time per day, divided by that day's active devices",
		Empty: "No session.heartbeat or session.end events in this range.",
		Kind:  columnChart, Days: days, Format: durationFormat,
		Series: []series{withValues(slot1, "Per device", perDevice)},
	})
	v.Opens = layout(chartData{
		Title: "Opens per day",
		Empty: "No app.open events in this range.",
		Kind:  columnChart, Days: days, Format: countFormat,
		Series: []series{withValues(slot1, "Opens", opens)},
	})
	v.Startup = layout(chartData{
		Title: "Startup time",
		Note:  "app.open value",
		Empty: "No app.open events with a value in this range.",
		Kind:  lineChart, Days: days, Format: millisFormat,
		Series: []series{withValues(slot1, "p50", p50), withValues(slot2, "p95", p95)},
	})
	v.Errors = layout(chartData{
		Title: "Errors per day",
		Empty: "No errors in this range.",
		Kind:  columnChart, Days: days, Format: countFormat,
		Series: []series{withValues(slot1, "Errors", errs)},
	})
}

func withValues(s series, name string, values []*float64) series {
	s.Name, s.Values = name, values
	return s
}

func count(n int64) *float64 {
	v := float64(n)
	return &v
}

// finite drops values no axis can scale to; ingest accepts any JSON number.
func finite(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

func ago(t, now time.Time) string {
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d/time.Minute))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d/time.Hour))
	}
	return fmt.Sprintf("%dd ago", int(d/(24*time.Hour)))
}
