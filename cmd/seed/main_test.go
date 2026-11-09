package main

import (
	"context"
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

var now = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func TestGenerate(t *testing.T) {
	t.Parallel()
	events := generate(now, rand.New(rand.NewPCG(1, 2)))
	if len(events) < 50_000 || len(events) > 200_000 {
		t.Errorf("events = %d", len(events))
	}
	namePattern := regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)*$`)
	oldest := now.AddDate(0, 0, -days-1)
	seen := map[string]int{}
	spike, normal := 0, 0
	usersOfDevice := map[string]map[string]bool{}
	anonymous := map[string]bool{}
	for _, e := range events {
		seen[e.Name]++
		if e.UserID == nil {
			anonymous[e.DeviceID] = true
		} else {
			if usersOfDevice[e.DeviceID] == nil {
				usersOfDevice[e.DeviceID] = map[string]bool{}
			}
			usersOfDevice[e.DeviceID][*e.UserID] = true
		}
		if !namePattern.MatchString(e.Name) {
			t.Fatalf("bad name %q", e.Name)
		}
		if e.Ts.After(now) || e.Ts.Before(oldest) {
			t.Fatalf("%s at %v is outside the window", e.Name, e.Ts)
		}
		if e.Name == "error.sync" {
			if *e.AppVersion == "2.0.0" {
				spike++
			} else {
				normal++
			}
		}
	}
	for _, name := range []string{
		"app.open", "session.start", "session.heartbeat", "session.end",
		"error.sync", "error.auth", "error.render", "feedback", "screen.view", "export.finished",
	} {
		if seen[name] == 0 {
			t.Errorf("no %s events", name)
		}
	}
	if spike == 0 || normal == 0 {
		t.Errorf("error.sync: %d on 2.0.0, %d on other versions", spike, normal)
	}
	shared, signedInLater := 0, 0
	for id, users := range usersOfDevice {
		if len(users) > 1 {
			shared++
		}
		if anonymous[id] {
			signedInLater++
		}
	}
	if shared == 0 || signedInLater == 0 {
		t.Errorf("%d shared devices, %d devices signed in later", shared, signedInLater)
	}
}

func demoEvents(t *testing.T, q *db.Queries, id string) int64 {
	t.Helper()
	rows, err := q.AppSummaries(context.Background(), now.AddDate(-1, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.ID == id {
			return r.Events
		}
	}
	t.Fatalf("no app %q", id)
	return 0
}

func TestSeedReplacesOnlyItsOwnApp(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	q := db.New(pool)
	if _, err := apps.Create(ctx, pool, "other", "Other"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.InsertEvents(ctx, []db.InsertEventsParams{{AppID: "other", Name: "app.open", DeviceID: "d1", Ts: now}}); err != nil {
		t.Fatal(err)
	}

	first, err := seed(ctx, pool, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := seed(ctx, pool, now)
	if err != nil {
		t.Fatal(err)
	}
	if first == 0 || first != second || demoEvents(t, q, appID) != second {
		t.Errorf("first = %d, second = %d, stored = %d", first, second, demoEvents(t, q, appID))
	}
	if n := demoEvents(t, q, "other"); n != 1 {
		t.Errorf("other app has %d events, want 1", n)
	}
	keys, err := q.ListAppKeys(ctx, appID)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].RevokedAt != nil || keys[1].RevokedAt == nil {
		t.Errorf("keys = %+v, want one active and one revoked", keys)
	}
}

func TestSeedRefusesARealApp(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	ctx := context.Background()
	q := db.New(pool)
	if _, err := apps.Create(ctx, pool, appID, "Production"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.InsertEvents(ctx, []db.InsertEventsParams{{AppID: appID, Name: "app.open", DeviceID: "d1", Ts: now}}); err != nil {
		t.Fatal(err)
	}
	if _, err := seed(ctx, pool, now); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("err = %v", err)
	}
	if n := demoEvents(t, q, appID); n != 1 {
		t.Errorf("real app has %d events, want 1", n)
	}
}
