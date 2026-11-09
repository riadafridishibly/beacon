package dashboard

import (
	"context"
	"errors"
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
	subjectsPageSize = 50
	relatedLimit     = 20
)

// subjectKind picks between the two things a timeline can follow.
type subjectKind int

const (
	deviceSubject subjectKind = iota
	userSubject
)

func (k subjectKind) noun() string {
	if k == userSubject {
		return "user"
	}
	return "device"
}

func (k subjectKind) tab() string { return k.noun() + "s" }

func (k subjectKind) title() string {
	if k == userSubject {
		return "Users"
	}
	return "Devices"
}

func (k subjectKind) other() subjectKind {
	if k == userSubject {
		return deviceSubject
	}
	return userSubject
}

func (k subjectKind) filter(id string) eventFilter {
	if k == userSubject {
		return eventFilter{User: id}
	}
	return eventFilter{Device: id}
}

func subjectsURL(appID string, k subjectKind, before string) string {
	u := "/apps/" + appID + "/" + k.tab()
	if before != "" {
		u += "?" + url.Values{"before": {before}}.Encode()
	}
	return u
}

// subjectURL carries the id in the query because clients choose device and
// user ids, and "/" or ".." would not survive in a path.
func subjectURL(appID string, k subjectKind, id, before string) string {
	q := url.Values{"id": {id}}
	if before != "" {
		q.Set("before", before)
	}
	return "/apps/" + appID + "/" + k.noun() + "?" + q.Encode()
}

func subjectCursor(lastSeen time.Time, id string) string {
	return strconv.FormatInt(lastSeen.UnixMicro(), 10) + "_" + id
}

func parseSubjectCursor(s string) (lastSeen time.Time, id string, ok bool) {
	micro, id, found := strings.Cut(s, "_")
	if !found || id == "" || !validText(id) {
		return time.Time{}, "", false
	}
	m, err := strconv.ParseInt(micro, 10, 64)
	if err != nil || m <= 0 {
		return time.Time{}, "", false
	}
	return time.UnixMicro(m), id, true
}

type subjectsView struct {
	App    db.App
	Kind   subjectKind
	Now    time.Time
	Before string
	Older  string
	Rows   []db.ListDevicesRow
}

// subjectView is one device's or user's page. Days is the timeline, newest
// first.
type subjectView struct {
	App     db.App
	Kind    subjectKind
	ID      string
	Now     time.Time
	Summary db.DeviceSummaryRow
	Related []db.UsersOfDeviceRow
	More    bool
	Before  string
	Older   string
	Days    []timelineDay
}

type timelineDay struct {
	Label string
	Rows  []timelineRow
}

// timelineRow is an event as a timeline shows it. Other is the user on a
// device's timeline and the device on a user's.
type timelineRow struct {
	Event        eventRow
	Clock, Other string
}

func (d *dashboard) subjectList(k subjectKind) func(http.ResponseWriter, *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		v, err := d.loadSubjects(r.Context(), r.PathValue("id"), k, r.URL.Query().Get("before"))
		if err != nil {
			return err
		}
		if web.IsHTMX(r) {
			return web.Render(w, r, http.StatusOK, subjectsBody(v))
		}
		return web.Render(w, r, http.StatusOK, subjectsPage(auth.Nav(r.Context()), v))
	}
}

func (d *dashboard) subjectDetail(k subjectKind) func(http.ResponseWriter, *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		q := r.URL.Query()
		v, err := d.loadSubject(r.Context(), r.PathValue("id"), k, q.Get("id"), q.Get("before"))
		if err != nil {
			return err
		}
		if web.IsHTMX(r) {
			return web.Render(w, r, http.StatusOK, timeline(v))
		}
		return web.Render(w, r, http.StatusOK, subjectPage(auth.Nav(r.Context()), v))
	}
}

func (d *dashboard) loadSubjects(ctx context.Context, appID string, k subjectKind, before string) (subjectsView, error) {
	app, err := d.app(ctx, appID)
	if err != nil {
		return subjectsView{}, err
	}
	beforeTS, beforeID, ok := parseSubjectCursor(before)
	if !ok {
		before, beforeTS, beforeID = "", farFuture, ""
	}
	rows, err := d.listSubjects(ctx, k, db.ListDevicesParams{
		AppID:     appID,
		BeforeTs:  beforeTS,
		BeforeID:  beforeID,
		PageLimit: subjectsPageSize + 1,
	})
	if err != nil {
		return subjectsView{}, err
	}
	v := subjectsView{App: app, Kind: k, Now: d.now(), Before: before, Rows: rows}
	if len(rows) > subjectsPageSize {
		v.Rows = rows[:subjectsPageSize]
		last := v.Rows[subjectsPageSize-1]
		v.Older = subjectsURL(appID, k, subjectCursor(last.LastSeen, last.ID))
	}
	return v, nil
}

func (d *dashboard) listSubjects(ctx context.Context, k subjectKind, p db.ListDevicesParams) ([]db.ListDevicesRow, error) {
	if k == deviceSubject {
		return d.q.ListDevices(ctx, p)
	}
	rows, err := d.q.ListUsers(ctx, db.ListUsersParams(p))
	if err != nil {
		return nil, err
	}
	out := make([]db.ListDevicesRow, len(rows))
	for i, r := range rows {
		out[i] = db.ListDevicesRow(r)
	}
	return out, nil
}

func (d *dashboard) subjectSummary(ctx context.Context, k subjectKind, p db.DeviceSummaryParams) (db.DeviceSummaryRow, error) {
	if k == deviceSubject {
		return d.q.DeviceSummary(ctx, p)
	}
	row, err := d.q.UserSummary(ctx, db.UserSummaryParams(p))
	return db.DeviceSummaryRow(row), err
}

// relatedSubjects returns the users seen on a device or the devices a user
// was seen on, most recent first.
func (d *dashboard) relatedSubjects(ctx context.Context, k subjectKind, p db.UsersOfDeviceParams) ([]db.UsersOfDeviceRow, error) {
	if k == deviceSubject {
		return d.q.UsersOfDevice(ctx, p)
	}
	rows, err := d.q.DevicesOfUser(ctx, db.DevicesOfUserParams(p))
	if err != nil {
		return nil, err
	}
	out := make([]db.UsersOfDeviceRow, len(rows))
	for i, r := range rows {
		out[i] = db.UsersOfDeviceRow(r)
	}
	return out, nil
}

func (d *dashboard) loadSubject(ctx context.Context, appID string, k subjectKind, id, before string) (subjectView, error) {
	app, err := d.app(ctx, appID)
	if err != nil {
		return subjectView{}, err
	}
	if id == "" || !validText(id) {
		return subjectView{}, web.ErrNotFound
	}
	summary, err := d.subjectSummary(ctx, k, db.DeviceSummaryParams{AppID: appID, ID: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return subjectView{}, web.ErrNotFound
	}
	if err != nil {
		return subjectView{}, err
	}
	related, err := d.relatedSubjects(ctx, k, db.UsersOfDeviceParams{AppID: appID, ID: id, RowLimit: relatedLimit + 1})
	if err != nil {
		return subjectView{}, err
	}
	f := k.filter(id)
	rows, before, older, err := d.eventPage(ctx, appID, f, before)
	if err != nil {
		return subjectView{}, err
	}
	v := subjectView{App: app, Kind: k, ID: id, Now: d.now(), Summary: summary, Related: related, Before: before}
	if len(related) > relatedLimit {
		v.Related, v.More = related[:relatedLimit], true
	}
	if older != "" {
		v.Older = subjectURL(appID, k, id, older)
	}
	v.Days = timelineDays(appID, k, id, f, rows)
	return v, nil
}

func timelineDays(appID string, k subjectKind, id string, f eventFilter, events []db.Event) []timelineDay {
	self := subjectURL(appID, k, id, "")
	var days []timelineDay
	for _, e := range events {
		ts := e.Ts.UTC()
		label := ts.Format("Mon, 2 Jan 2006")
		if len(days) == 0 || days[len(days)-1].Label != label {
			days = append(days, timelineDay{Label: label})
		}
		row := newEventRow(appID, f, e)
		for i := range row.Fields {
			if row.Fields[i].Timeline == self {
				row.Fields[i].Timeline = ""
			}
		}
		other := row.User
		if k == userSubject {
			other = row.Device
		}
		day := &days[len(days)-1]
		day.Rows = append(day.Rows, timelineRow{Event: row, Clock: ts.Format(time.TimeOnly), Other: other})
	}
	return days
}

func (v subjectView) location() string {
	return strings.Join(nonEmpty(deref(v.Summary.City), deref(v.Summary.Country)), ", ")
}

func (v subjectView) platform() string {
	return strings.Join(nonEmpty(deref(v.Summary.Platform), deref(v.Summary.OSVersion)), " ")
}

func nonEmpty(values ...string) []string {
	out := values[:0]
	for _, s := range values {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
