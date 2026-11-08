package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

var now = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func setup(t *testing.T) (*pgxpool.Pool, http.Handler, string) {
	t.Helper()
	pool := testdb.New(t)
	key, err := apps.Create(context.Background(), pool, "demo", "Demo")
	if err != nil {
		t.Fatal(err)
	}
	q := db.New(pool)
	h := &Handler{Apps: apps.NewResolver(q), DB: q, Now: func() time.Time { return now }}
	return pool, h, key
}

func post(t *testing.T, h http.Handler, key string, body []byte, gz bool) *httptest.ResponseRecorder {
	t.Helper()
	if gz {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		_, _ = w.Write(body)
		_ = w.Close()
		body = buf.Bytes()
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if gz {
		r.Header.Set("Content-Encoding", "gzip")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

type stored struct {
	Name, DeviceID               string
	UserID, AppVersion, Platform *string
	Ts                           time.Time
	Value                        *float64
	Props                        []byte
}

func load(t *testing.T, pool *pgxpool.Pool) []stored {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT name, device_id, user_id, app_version, platform, ts, value, props FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []stored
	for rows.Next() {
		var s stored
		if err := rows.Scan(&s.Name, &s.DeviceID, &s.UserID, &s.AppVersion, &s.Platform, &s.Ts, &s.Value, &s.Props); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func TestIngest(t *testing.T) {
	t.Parallel()
	pool, h, key := setup(t)

	body := `{
		"context": {"device_id": "d1", "user_id": "tester@example.com", "app_version": "1.2.0", "platform": "macos"},
		"events": [
			{"name": "app.open", "ts": "2026-11-08T11:59:00Z", "value": 812.5, "props": {"cold": true}},
			{"name": "error.sync", "device_id": "d2", "platform": "ios", "props": {"message": "timeout"}},
			{"name": "session.heartbeat", "ts": "2026-11-09T12:00:00Z", "props": null}
		]
	}`
	rec := post(t, h, key, []byte(body), true)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"accepted":3`) {
		t.Errorf("body %s", rec.Body)
	}

	got := load(t, pool)
	if len(got) != 3 {
		t.Fatalf("%d rows, want 3", len(got))
	}
	open, sync, beat := got[0], got[1], got[2]
	if open.Name != "app.open" || open.DeviceID != "d1" || *open.UserID != "tester@example.com" ||
		*open.AppVersion != "1.2.0" || *open.Value != 812.5 || !open.Ts.Equal(now.Add(-time.Minute)) {
		t.Errorf("app.open row %+v", open)
	}
	var props map[string]any
	if err := json.Unmarshal(open.Props, &props); err != nil || props["cold"] != true {
		t.Errorf("app.open props %s", open.Props)
	}
	if sync.DeviceID != "d2" || *sync.Platform != "ios" || *sync.UserID != "tester@example.com" || sync.Value != nil {
		t.Errorf("event fields should override context: %+v", sync)
	}
	if !sync.Ts.Equal(now) {
		t.Errorf("missing ts: got %v, want arrival time", sync.Ts)
	}
	if !beat.Ts.Equal(now) || beat.Props != nil {
		t.Errorf("future ts or null props: %+v", beat)
	}
}

func TestIngestRejects(t *testing.T) {
	t.Parallel()
	pool, h, key := setup(t)

	many := `{"context":{"device_id":"d"},"events":[` + strings.Repeat(`{"name":"a"},`, maxEvents) + `{"name":"a"}]}`
	for _, tt := range []struct {
		name, key, body string
		status          int
		wantIndex       bool
	}{
		{"no key", "", `{}`, http.StatusUnauthorized, false},
		{"unknown key", "bk_nope", `{}`, http.StatusUnauthorized, false},
		{"bad json", key, `{`, http.StatusBadRequest, false},
		{"no events", key, `{"events":[]}`, http.StatusBadRequest, false},
		{"too many", key, many, http.StatusBadRequest, false},
		{"bad name", key, `{"events":[{"name":"App Open","device_id":"d"}]}`, http.StatusBadRequest, true},
		{"no device", key, `{"events":[{"name":"app.open"}]}`, http.StatusBadRequest, true},
		{"props array", key, `{"events":[{"name":"x","device_id":"d","props":[1]}]}`, http.StatusBadRequest, true},
	} {
		rec := post(t, h, tt.key, []byte(tt.body), false)
		if rec.Code != tt.status {
			t.Errorf("%s: status %d, want %d: %s", tt.name, rec.Code, tt.status, rec.Body)
		}
		if got := strings.Contains(rec.Body.String(), `"index"`); got != tt.wantIndex {
			t.Errorf("%s: index in body = %v, want %v: %s", tt.name, got, tt.wantIndex, rec.Body)
		}
	}

	// One bad event rejects the whole batch.
	rec := post(t, h, key, []byte(`{"context":{"device_id":"d"},"events":[{"name":"ok"},{"name":"BAD"}]}`), false)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"index":1`) {
		t.Errorf("mixed batch: %d %s", rec.Code, rec.Body)
	}
	if n := len(load(t, pool)); n != 0 {
		t.Errorf("%d rows stored from rejected batches", n)
	}
}

func TestIngestBodyLimit(t *testing.T) {
	t.Parallel()
	_, h, key := setup(t)
	big := []byte(`{"events":[{"name":"x","device_id":"d","props":{"p":"` + strings.Repeat("a", maxInflated) + `"}}]}`)
	for _, gz := range []bool{false, true} {
		rec := post(t, h, key, big, gz)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "too large") {
			t.Errorf("gzip=%v: %d %s", gz, rec.Code, rec.Body)
		}
	}
}
