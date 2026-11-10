// Command seed fills a development database with a demo app and 90 days of
// generated events. Running it again replaces that app and nothing else.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/config"
	"github.com/riadafridishibly/beacon/internal/db"
)

const (
	appID          = "demo"
	appName        = "Demo (seed data)"
	days           = 90
	deviceCount    = 200
	userCount      = 90
	heartbeatEvery = 60
	day            = 24 * time.Hour
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	n, err := seed(ctx, pool, time.Now())
	if err != nil {
		return err
	}
	fmt.Printf("app %q: %d events\n", appID, n)
	return nil
}

// seed replaces the demo app. The name check stops it from deleting a real
// app that happens to have the id.
func seed(ctx context.Context, pool *pgxpool.Pool, now time.Time) (int64, error) {
	q := db.New(pool)
	app, err := q.GetApp(ctx, appID)
	switch {
	case err == nil && app.Name != appName:
		return 0, fmt.Errorf("app %q already exists and wasn't created by seed", appID)
	case err == nil:
		if err := q.DeleteApp(ctx, appID); err != nil {
			return 0, err
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, err
	}
	if _, err := apps.Create(ctx, pool, appID, appName); err != nil {
		return 0, err
	}
	if err := rotateKey(ctx, q); err != nil {
		return 0, err
	}
	if err := addViews(ctx, q); err != nil {
		return 0, err
	}
	return q.InsertEvents(ctx, generate(now, rand.New(rand.NewPCG(1, 2))))
}

// addViews adds one view of each kind the overview draws.
func addViews(ctx context.Context, q *db.Queries) error {
	for _, v := range []db.CreateViewParams{
		{Title: "Export time by format", Event: "export.finished", Measures: []string{"p95"}, GroupBy: "props.format"},
		{Title: "Screen views", Event: "screen.view", Measures: []string{"count", "distinct_devices"}, GroupBy: "props.screen"},
		{Title: "PDF exports per day", Event: "export.finished", Measures: []string{"count"}, Filter: []byte(`{"props.format": "pdf"}`)},
		{Title: "Export time", Event: "export.finished", Measures: []string{"p50", "p95"}},
		{Title: "Screen views per day", Event: "screen.view", Measures: []string{"count"}, GroupBy: "props.screen", Shape: "line"},
	} {
		v.AppID, v.MaxViews, v.Shape = appID, 100, cmp.Or(v.Shape, "table")
		if v.Filter == nil {
			v.Filter = []byte("{}")
		}
		if _, err := q.CreateView(ctx, v); err != nil {
			return err
		}
	}
	return nil
}

// rotateKey revokes the demo app's key and adds a new one, so the keys page
// shows both states.
func rotateKey(ctx context.Context, q *db.Queries) error {
	keys, err := q.ListAppKeys(ctx, appID)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := apps.Revoke(ctx, q, k.Prefix); err != nil {
			return err
		}
	}
	_, err = apps.NewKey(ctx, q, appID)
	return err
}

type place struct {
	country, city string
	utcOffset     int
}

// Repeated entries make a value more common.
var places = []place{
	{"BD", "Dhaka", 6}, {"BD", "Dhaka", 6}, {"BD", "Chattogram", 6},
	{"US", "New York", -5}, {"US", "New York", -5}, {"US", "San Francisco", -8},
	{"IN", "Bengaluru", 5}, {"IN", "Mumbai", 5},
	{"DE", "Berlin", 1}, {"GB", "London", 0}, {"BR", "São Paulo", -3}, {"JP", "Tokyo", 9},
}

type platform struct {
	name      string
	versions  []string
	startupMS float64
}

var platforms = []platform{
	{"macos", []string{"14.7", "15.1"}, 420},
	{"macos", []string{"14.7", "15.1"}, 420},
	{"windows", []string{"10", "11"}, 780},
	{"windows", []string{"10", "11"}, 780},
	{"linux", []string{"6.8", "6.11"}, 520},
	{"ios", []string{"17.6", "18.1"}, 610},
	{"android", []string{"14", "15"}, 950},
}

var releases = []struct {
	version string
	day     int
}{
	{"1.8.0", 0},
	{"1.9.0", 35},
	// 2.0.0 ships a sync bug that 2.0.1 fixes, which shows as an error spike.
	{"2.0.0", 70},
	{"2.0.1", 75},
}

var (
	screens = []string{"home", "search", "settings", "library", "profile"}
	formats = []string{"pdf", "csv", "png"}
)

type device struct {
	id                     string
	user, nextUser         *string
	signInDay, nextUserDay int
	place                  place
	platform               platform
	osVersion              string
	firstDay, lastDay      int
	activity               float64
	updateLag              int
}

func pick[T any](rng *rand.Rand, list []T) T { return list[rng.IntN(len(list))] }

func hexID(rng *rand.Rand) string {
	return fmt.Sprintf("%08x-%04x-%04x", rng.Uint32(), rng.Uint32()&0xffff, rng.Uint32()&0xffff)
}

func logNormal(rng *rand.Rand, median, sigma float64) float64 {
	return median * math.Exp(sigma*rng.NormFloat64())
}

func newDevice(rng *rand.Rand) device {
	p := pick(rng, platforms)
	d := device{
		id:        hexID(rng),
		place:     pick(rng, places),
		platform:  p,
		osVersion: pick(rng, p.versions),
		lastDay:   days,
		activity:  0.25 + 0.45*rng.Float64(),
		updateLag: rng.IntN(8),
	}
	if rng.Float64() < 0.55 {
		d.firstDay = rng.IntN(days - 5)
	}
	if rng.Float64() < 0.15 {
		d.lastDay = d.firstDay + rng.IntN(days-d.firstDay)
	}
	if rng.Float64() < 0.6 {
		d.user = newUser(rng)
		// Clients can set the user at any time, so a device can start out
		// anonymous or pass to someone else.
		if rng.Float64() < 0.3 {
			d.signInDay = d.firstDay + rng.IntN(d.lastDay-d.firstDay+1)
		}
		if rng.Float64() < 0.2 {
			from := max(d.firstDay, d.signInDay)
			d.nextUser = newUser(rng)
			d.nextUserDay = from + rng.IntN(d.lastDay-from+1)
		}
	}
	return d
}

// newUser picks from fewer users than there are devices, so some users have
// several devices.
func newUser(rng *rand.Rand) *string {
	u := fmt.Sprintf("user-%02d", rng.IntN(userCount))
	return &u
}

func (d device) userOn(dayIndex int) *string {
	switch {
	case dayIndex < d.signInDay:
		return nil
	case d.nextUser != nil && dayIndex >= d.nextUserDay:
		return d.nextUser
	}
	return d.user
}

func (d device) versionOn(dayIndex int) string {
	version := releases[0].version
	for _, r := range releases[1:] {
		if dayIndex >= r.day+d.updateLag {
			version = r.version
		}
	}
	return version
}

func generate(now time.Time, rng *rand.Rand) []db.InsertEventsParams {
	now = now.UTC()
	first := now.Truncate(day).AddDate(0, 0, 1-days)
	devices := make([]device, deviceCount)
	for i := range devices {
		devices[i] = newDevice(rng)
	}
	s := session{now: now, rng: rng}
	for i := range days {
		date := first.AddDate(0, 0, i)
		chance := 1.0
		if wd := date.Weekday(); wd == time.Saturday || wd == time.Sunday {
			chance = 0.65
		}
		for _, d := range devices {
			if i < d.firstDay || i > d.lastDay || rng.Float64() >= d.activity*chance {
				continue
			}
			for range 1 + rng.IntN(2) {
				s.run(d, d.userOn(i), date, d.versionOn(i))
			}
		}
	}
	return s.events
}

// session collects the events of generated sessions.
type session struct {
	now    time.Time
	rng    *rand.Rand
	events []db.InsertEventsParams
}

func (s *session) run(d device, user *string, date time.Time, version string) {
	rng := s.rng
	localHour := 9 + 14*rng.Float64()
	start := date.Add(time.Duration((localHour - float64(d.place.utcOffset)) * float64(time.Hour)))
	id := hexID(rng)
	add := func(name string, at time.Duration, value *float64, props map[string]any) {
		ts := start.Add(at)
		if ts.After(s.now) {
			return
		}
		e := db.InsertEventsParams{
			AppID: appID, Name: name, Ts: ts, DeviceID: d.id, UserID: user, SessionID: &id,
			AppVersion: &version, Platform: &d.platform.name, OSVersion: &d.osVersion,
			Country: &d.place.country, City: &d.place.city, Value: value,
		}
		if props != nil {
			e.Props, _ = json.Marshal(props)
		}
		s.events = append(s.events, e)
	}

	startup := d.platform.startupMS
	if version >= "2.0.0" {
		startup *= 0.8
	}
	add("app.open", 0, num(math.Round(logNormal(rng, startup, 0.35))), map[string]any{"cold": rng.Float64() < 0.4})
	add("session.start", 100*time.Millisecond, nil, nil)

	length := min(max(logNormal(rng, 300, 0.9), 20), 3600)
	seconds := func(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }
	for t := float64(heartbeatEvery); t < length; t += heartbeatEvery {
		add("session.heartbeat", seconds(t), num(t), nil)
	}
	during := func() time.Duration { return seconds(length * rng.Float64()) }

	for range rng.IntN(3) {
		add("screen.view", during(), nil, map[string]any{"screen": pick(rng, screens)})
	}
	if rng.Float64() < 0.2 {
		add("export.finished", during(), num(math.Round(logNormal(rng, 1500, 0.7))), map[string]any{"format": pick(rng, formats)})
	}
	if user != nil && rng.Float64() < 0.01 {
		add("feedback", during(), nil, map[string]any{"text": "The new search is much easier to use."})
	}

	syncRate := 0.04
	if version == "2.0.0" {
		syncRate = 0.7
	}
	if rng.Float64() < syncRate {
		add("error.sync", during(), nil, map[string]any{"message": "connection reset by peer", "code": "ECONNRESET"})
	}
	if rng.Float64() < 0.03 {
		add("error.auth", during(), nil, map[string]any{"message": "token expired", "code": "401"})
	}
	if rng.Float64() < 0.02 {
		add("error.render", during(), nil, map[string]any{"message": "cannot read layout of an empty view", "stack": "render@view:88\ndraw@screen:12"})
	}

	// Some sessions never send an end, such as when the app is killed.
	if rng.Float64() < 0.88 {
		add("session.end", seconds(length), num(math.Round(length)), nil)
	}
}

func num(v float64) *float64 { return &v }
