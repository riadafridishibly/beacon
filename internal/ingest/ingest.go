// Package ingest serves POST /v1/events, which stores batches of events
// from any app.
package ingest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"regexp"
	"strings"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/geo"
)

const (
	maxBody     = 1 << 20
	maxInflated = 4 << 20
	maxEvents   = 1000
	maxProps    = 16 << 10
	maxFutureTS = time.Hour
	maxNameLen  = 64
	maxIDLen    = 128
	maxUserLen  = 256
	maxAttrLen  = 64
)

var namePattern = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)*$`)

// Batch is the request body. Fields set in Context apply to every event
// that doesn't set them itself.
type Batch struct {
	Context Fields  `json:"context"`
	Events  []Event `json:"events"`
}

// Fields are the attributes an event shares with the others from the same
// device.
type Fields struct {
	DeviceID   string `json:"device_id,omitempty"`
	UserID     string `json:"user_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
	AppVersion string `json:"app_version,omitempty"`
	Platform   string `json:"platform,omitempty"`
	OSVersion  string `json:"os_version,omitempty"`
}

// Event is one thing that happened. Value is its single numeric measure,
// such as a duration in milliseconds. Props holds everything else, as a
// JSON object.
type Event struct {
	Fields
	Name  string          `json:"name"`
	TS    *time.Time      `json:"ts,omitempty"`
	Value *float64        `json:"value,omitempty"`
	Props json.RawMessage `json:"props,omitempty"`
}

// Apps resolves an ingest key to an app id, "" when the key is unknown.
type Apps interface {
	App(ctx context.Context, key string) (string, error)
}

type Handler struct {
	Apps Apps
	DB   *db.Queries
	Geo  *geo.Locator
	Now  func() time.Time
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || key == "" {
		writeError(w, http.StatusUnauthorized, "missing ingest key", -1)
		return
	}
	appID, err := h.Apps.App(r.Context(), key)
	if err != nil {
		slog.ErrorContext(r.Context(), "resolve ingest key", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", -1)
		return
	}
	if appID == "" {
		writeError(w, http.StatusUnauthorized, "unknown ingest key", -1)
		return
	}

	batch, err := decode(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), -1)
		return
	}
	if len(batch.Events) == 0 {
		writeError(w, http.StatusBadRequest, "no events", -1)
		return
	}
	if len(batch.Events) > maxEvents {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("at most %d events per batch", maxEvents), -1)
		return
	}

	now := h.now()
	country, city := h.Geo.Lookup(clientAddr(r))
	rows := make([]db.InsertEventsParams, len(batch.Events))
	for i, e := range batch.Events {
		row, err := toRow(appID, batch.Context, e, now)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), i)
			return
		}
		row.Country, row.City = opt(country), opt(city)
		rows[i] = row
	}
	if _, err := h.DB.InsertEvents(r.Context(), rows); err != nil {
		slog.ErrorContext(r.Context(), "insert events", "app", appID, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", -1)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(rows)})
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func decode(w http.ResponseWriter, r *http.Request) (Batch, error) {
	var body io.Reader = http.MaxBytesReader(w, r.Body, maxBody)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(body)
		if err != nil {
			return Batch{}, errors.New("invalid gzip body")
		}
		defer zr.Close()
		body = &limitReader{r: zr, n: maxInflated}
	}
	var b Batch
	dec := json.NewDecoder(body)
	if err := dec.Decode(&b); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) || errors.Is(err, errInflatedTooLarge) {
			return Batch{}, errors.New("body too large: the limit is 1 MiB, or 4 MiB after gunzip")
		}
		return Batch{}, fmt.Errorf("invalid JSON: %w", err)
	}
	return b, nil
}

var errInflatedTooLarge = errors.New("inflated body too large")

type limitReader struct {
	r io.Reader
	n int64
}

func (l *limitReader) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, errInflatedTooLarge
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= int64(n)
	return n, err
}

func toRow(appID string, shared Fields, e Event, now time.Time) (db.InsertEventsParams, error) {
	f := e.Fields
	f.DeviceID = or(f.DeviceID, shared.DeviceID)
	f.UserID = or(f.UserID, shared.UserID)
	f.SessionID = or(f.SessionID, shared.SessionID)
	f.AppVersion = or(f.AppVersion, shared.AppVersion)
	f.Platform = or(f.Platform, shared.Platform)
	f.OSVersion = or(f.OSVersion, shared.OSVersion)

	switch {
	case !namePattern.MatchString(e.Name) || len(e.Name) > maxNameLen:
		return db.InsertEventsParams{}, fmt.Errorf("name %q must be at most %d lowercase letters, digits and underscores, in dot-separated parts", e.Name, maxNameLen)
	case f.DeviceID == "":
		return db.InsertEventsParams{}, errors.New("device_id is required")
	case len(f.DeviceID) > maxIDLen || len(f.SessionID) > maxIDLen:
		return db.InsertEventsParams{}, fmt.Errorf("device_id and session_id must be at most %d bytes", maxIDLen)
	case len(f.UserID) > maxUserLen:
		return db.InsertEventsParams{}, fmt.Errorf("user_id must be at most %d bytes", maxUserLen)
	case len(f.AppVersion) > maxAttrLen || len(f.Platform) > maxAttrLen || len(f.OSVersion) > maxAttrLen:
		return db.InsertEventsParams{}, fmt.Errorf("app_version, platform and os_version must be at most %d bytes", maxAttrLen)
	}

	props := e.Props
	if len(props) > 0 && string(props) == "null" {
		props = nil
	}
	if len(props) > 0 {
		if props[0] != '{' {
			return db.InsertEventsParams{}, errors.New("props must be a JSON object")
		}
		if len(props) > maxProps {
			return db.InsertEventsParams{}, fmt.Errorf("props must be at most %d bytes", maxProps)
		}
	}

	// Client clocks drift. A time far in the future would sort ahead of
	// everything else, so it is replaced with the time of arrival.
	ts := now
	if e.TS != nil && !e.TS.IsZero() && e.TS.Before(now.Add(maxFutureTS)) {
		ts = *e.TS
	}

	return db.InsertEventsParams{
		AppID:      appID,
		Name:       e.Name,
		Ts:         ts,
		DeviceID:   f.DeviceID,
		UserID:     opt(f.UserID),
		SessionID:  opt(f.SessionID),
		AppVersion: opt(f.AppVersion),
		Platform:   opt(f.Platform),
		OSVersion:  opt(f.OSVersion),
		Value:      e.Value,
		Props:      props,
	}, nil
}

// clientAddr is the address RealIP left in RemoteAddr. The address itself
// is never stored.
func clientAddr(r *http.Request) netip.Addr {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}
	}
	return ap.Addr()
}

func writeError(w http.ResponseWriter, status int, msg string, index int) {
	body := map[string]any{"error": msg}
	if index >= 0 {
		body["index"] = index
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func opt(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
