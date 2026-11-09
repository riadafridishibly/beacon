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
	"math"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/geo"
	"github.com/riadafridishibly/beacon/internal/web"
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

// Handler serves POST /v1/events. ByIP and ByKey are required.
type Handler struct {
	Apps  Apps
	DB    *db.Queries
	Geo   *geo.Locator
	ByIP  *web.Limiter
	ByKey *web.Limiter
	Now   func() time.Time
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Before the key lookup, so guessing keys can't load the database, and
	// so one address with a leaked key can't drain the key's bucket.
	if !allow(w, h.ByIP, web.ClientKey(r)) {
		return
	}
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
	if !allow(w, h.ByKey, key) {
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
		// A value toRow can't foresee, such as a number too large for jsonb.
		// A 500 would make the client resend this batch forever.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
			slog.WarnContext(r.Context(), "rejected batch", "app", appID, "err", err)
			writeError(w, http.StatusBadRequest, "batch contains a value that can't be stored", -1)
			return
		}
		slog.ErrorContext(r.Context(), "insert events", "app", appID, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", -1)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(rows)})
}

// allow writes a 429 and returns false when l refuses key.
func allow(w http.ResponseWriter, l *web.Limiter, key string) bool {
	ok, wait := l.Take(key)
	if !ok {
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(wait.Seconds())))))
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded", -1)
	}
	return ok
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
	case hasNUL(f.DeviceID, f.UserID, f.SessionID, f.AppVersion, f.Platform, f.OSVersion):
		return db.InsertEventsParams{}, errors.New("device_id, user_id, session_id, app_version, platform and os_version must not contain NUL")
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
		if !storableJSON(props) {
			return db.InsertEventsParams{}, errors.New(`props must be valid UTF-8, without \u0000 or unpaired surrogates`)
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

// storableJSON reports whether valid JSON can be stored as jsonb, which
// refuses \u0000, unpaired surrogates and invalid UTF-8.
func storableJSON(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for i := 0; i+1 < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		i++
		if b[i] != 'u' {
			continue
		}
		r, ok := hex4(b[i+1:])
		if !ok {
			return false
		}
		i += 4
		switch {
		case r == 0 || (r >= 0xDC00 && r <= 0xDFFF):
			return false
		case r >= 0xD800 && r <= 0xDBFF:
			if i+2 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			low, ok := hex4(b[i+3:])
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}

func hex4(b []byte) (uint64, bool) {
	if len(b) < 4 {
		return 0, false
	}
	r, err := strconv.ParseUint(string(b[:4]), 16, 16)
	return r, err == nil
}

func hasNUL(fields ...string) bool {
	for _, f := range fields {
		if strings.IndexByte(f, 0) >= 0 {
			return true
		}
	}
	return false
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
