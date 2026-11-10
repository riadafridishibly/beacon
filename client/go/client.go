// Package beacon sends telemetry events to a beacon server. Events wait in
// an on-disk queue and go out in gzipped batches.
package beacon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMaxQueueBytes = 10 << 20
	defaultBatchEvents   = 100
	defaultFlushEvery    = 30 * time.Second
	defaultHeartbeat     = 60 * time.Second
	defaultMinBackoff    = time.Second
	defaultMaxBackoff    = 5 * time.Minute
	requestTimeout       = 30 * time.Second
	closeWait            = 5 * time.Second

	// maxBatchBytes keeps a batch under the server's 1 MiB limit even when
	// gzip gains nothing.
	maxBatchBytes = 900 << 10
)

// processStart is the closest a Go package can get to process start.
var processStart = time.Now()

// ErrClosed is returned after Close.
var ErrClosed = errors.New("beacon: client closed")

// Config configures a Client. Endpoint, Key and Dir are required.
type Config struct {
	// Endpoint is the server's base URL.
	Endpoint string
	// Key is the app's ingest key.
	Key string
	// Dir holds the queue and the device id. Use one Client per Dir.
	Dir string

	AppVersion string
	// Platform defaults to the operating system: macos, windows, linux, ios
	// or android.
	Platform  string
	OSVersion string

	// MaxQueueBytes caps the queue on disk, 10 MiB by default. Past it the
	// oldest events are dropped.
	MaxQueueBytes int64
	// ProcessStart overrides the time AppOpened measures from.
	ProcessStart time.Time
	// Log receives messages about delivery problems and dropped data. It is
	// optional and must not call the Client.
	Log func(message string)
	// AllowProps lists the props keys that may leave the app. Others are
	// dropped.
	AllowProps []string

	// Set by tests.
	flushEvery, heartbeatEvery, minBackoff, maxBackoff time.Duration
	batchEvents                                        int
	now                                                func() time.Time
	newID                                              func() string
}

// Client queues and sends events. Its methods are safe for concurrent use,
// but a Props map must not change during the call it is passed to.
type Client struct {
	url, key, dir          string
	http                   *http.Client
	q                      *queue
	log                    func(string)
	allowed                map[string]bool
	now                    func() time.Time
	newID                  func() string
	started                time.Time
	flushEvery, beatEvery  time.Duration
	minBackoff, maxBackoff time.Duration
	failures               int // owned by the run goroutine

	deviceID, appVersion, platform, osVersion string

	opened atomic.Bool

	mu        sync.Mutex
	userID    string
	sessionID string
	// sessionStart is when the session began, and reported the whole seconds
	// of it already sent.
	sessionStart time.Time
	reported     float64
	closed       bool

	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
}

// NewClient opens the queue in cfg.Dir and starts sending in the background.
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil || cfg.Endpoint == "" || cfg.Key == "" || cfg.Dir == "" {
		return nil, errors.New("beacon: Endpoint, Key and Dir are required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("beacon: invalid Endpoint %q", cfg.Endpoint)
	}

	allowed := make(map[string]bool, len(cfg.AllowProps))
	for _, key := range cfg.AllowProps {
		allowed[key] = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		url:        strings.TrimRight(cfg.Endpoint, "/") + "/v1/events",
		key:        cfg.Key,
		dir:        cfg.Dir,
		http:       &http.Client{Timeout: requestTimeout},
		log:        cfg.Log,
		allowed:    allowed,
		now:        cfg.now,
		newID:      cfg.newID,
		started:    processStart,
		flushEvery: or(cfg.flushEvery, defaultFlushEvery),
		beatEvery:  or(cfg.heartbeatEvery, defaultHeartbeat),
		minBackoff: or(cfg.minBackoff, defaultMinBackoff),
		maxBackoff: or(cfg.maxBackoff, defaultMaxBackoff),
		appVersion: cfg.AppVersion,
		platform:   or(cfg.Platform, platformName(runtime.GOOS)),
		osVersion:  cfg.OSVersion,
		ctx:        ctx,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.newID == nil {
		c.newID = randomID
	}
	if !cfg.ProcessStart.IsZero() {
		c.started = cfg.ProcessStart
	}

	fail := func(err error) (*Client, error) {
		cancel()
		return nil, err
	}
	if c.deviceID, err = loadDeviceID(cfg.Dir, c.newID); err != nil {
		return fail(fmt.Errorf("beacon: device id: %w", err))
	}
	probe := event{Name: "probe", DeviceID: c.deviceID, AppVersion: c.appVersion, Platform: c.platform, OSVersion: c.osVersion}
	if err := probe.validate(); err != nil {
		return fail(err)
	}
	onDrop := func(n int64) { c.logf("queue full: dropped %d oldest events", n) }
	c.q, err = openQueue(filepath.Join(cfg.Dir, "queue"), or(cfg.MaxQueueBytes, defaultMaxQueueBytes),
		or(cfg.batchEvents, defaultBatchEvents), maxBatchBytes, onDrop)
	if err != nil {
		return fail(fmt.Errorf("beacon: open queue: %w", err))
	}

	c.endPreviousSession()
	go c.run()
	c.notify() // events left by the previous run
	return c, nil
}

// DeviceID returns the id generated for this install.
func (c *Client) DeviceID() string { return c.deviceID }

// SetUserID sets the user_id of later events, "" for none. Only tester
// builds should call it.
func (c *Client) SetUserID(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.userID = id
}

// Track queues an event. props may be nil. Report errors with TrackError: the
// dashboard counts only names starting "error.".
func (c *Client) Track(name string, props Props) error {
	return c.track(name, nil, props)
}

// TrackError queues error.<area>, counted as an error by the dashboard. A
// non-empty message, cut to 2 KiB, replaces props["message"] if allowed.
func (c *Client) TrackError(area, message string, props Props) error {
	// A message that isn't allowed stays out of props, or every error would
	// log a drop.
	if message = cleanMessage(message); message != "" && c.allowed["message"] {
		props = maps.Clone(props)
		if props == nil {
			props = Props{}
		}
		props["message"] = message
	}
	return c.track("error."+area, nil, props)
}

// TrackValue queues an event with its one numeric measure, such as a
// duration in milliseconds.
func (c *Client) TrackValue(name string, value float64, props Props) error {
	return c.track(name, &value, props)
}

func (c *Client) track(name string, value *float64, props Props) error {
	encoded, dropped, err := filterProps(props, c.allowed)
	if err != nil {
		return err
	}
	if len(dropped) > 0 {
		c.logf("%s: dropped props not allowed: %s", name, strings.Join(dropped, ", "))
	}
	return c.enqueue(name, value, encoded)
}

// AppOpened records app.open once per process, with the time since process
// start. Call it when the first screen is usable.
func (c *Client) AppOpened() error {
	if !c.opened.CompareAndSwap(false, true) {
		return nil
	}
	ms := float64(max(c.now().Sub(c.started), 0).Milliseconds())
	return c.enqueue("app.open", &ms, []byte(`{"cold":true}`))
}

// StartSession begins a session and sends session.start, then a heartbeat
// each minute. It does nothing while a session is open. A session the app
// never ends, because it crashed or was killed, is ended by the next Client
// opened on the same Dir.
func (c *Client) StartSession() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID != "" {
		return nil
	}
	c.sessionID = c.newID()
	e := c.eventLocked("session.start", nil, nil)
	c.sessionStart, c.reported = *e.TS, 0
	if err := c.addLocked(e); err != nil {
		return err
	}
	c.saveSessionLocked(*e.TS)
	return nil
}

// EndSession sends session.end and closes the session.
func (c *Client) EndSession() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID == "" {
		return nil
	}
	e := c.eventLocked("session.end", nil, nil)
	secs := c.reportLocked(*e.TS)
	e.Value = &secs
	err := c.addLocked(e)
	c.sessionID = ""
	c.removeSession()
	return err
}

func (c *Client) heartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID == "" {
		return
	}
	e := c.eventLocked("session.heartbeat", nil, nil)
	secs := c.reportLocked(*e.TS)
	e.Value = &secs
	if err := c.addLocked(e); err != nil {
		if !errors.Is(err, ErrClosed) {
			c.logf("heartbeat: %v", err)
		}
		return
	}
	c.saveSessionLocked(*e.TS)
}

// reportLocked returns the session's seconds since its last report.
// Rounding the running total instead of each report keeps their sum equal
// to the session's length.
func (c *Client) reportLocked(now time.Time) float64 {
	total := math.Round(max(now.Sub(c.sessionStart), 0).Seconds())
	secs := max(total-c.reported, 0)
	c.reported = max(total, c.reported)
	return secs
}

// savedSession is the open session on disk, for the next run to end if this
// one stops without ending it. TS is the last time the session was known to
// be active, in Unix milliseconds.
type savedSession struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id,omitempty"`
	TS        int64  `json:"ts"`
}

func (c *Client) sessionPath() string { return filepath.Join(c.dir, "session") }

func (c *Client) saveSessionLocked(ts time.Time) {
	b, err := json.Marshal(savedSession{SessionID: c.sessionID, UserID: c.userID, TS: ts.UnixMilli()})
	if err == nil {
		err = writeFileAtomic(c.sessionPath(), b)
	}
	if err != nil {
		c.logf("save session: %v", err)
	}
}

func (c *Client) removeSession() {
	if err := os.Remove(c.sessionPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		c.logf("remove session: %v", err)
	}
}

// endPreviousSession sends session.end, marked abnormal, for a session the
// previous run left open.
func (c *Client) endPreviousSession() {
	b, err := os.ReadFile(c.sessionPath())
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	defer c.removeSession()
	var saved savedSession
	if err == nil {
		err = json.Unmarshal(b, &saved)
	}
	if err == nil && saved.SessionID == "" {
		err = errors.New("no session_id")
	}
	if err != nil {
		c.logf("previous session: %v", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.eventLocked("session.end", nil, []byte(`{"abnormal":true}`))
	secs := 0.0
	e.SessionID, e.UserID, e.Value = saved.SessionID, saved.UserID, &secs
	// A time that can't be right is left out, and the server uses the time
	// the event arrives.
	if ts := time.UnixMilli(saved.TS).UTC(); saved.TS > 0 && !ts.After(*e.TS) {
		e.TS = &ts
	} else {
		e.TS = nil
	}
	if err := c.addLocked(e); err != nil {
		c.logf("previous session: %v", err)
	}
}

// Flush starts sending queued events now, without waiting for them. Call it
// when a mobile app moves to the background.
func (c *Client) Flush() {
	c.q.seal()
	c.notify()
}

// Close ends any open session and makes one last attempt to send, for a few
// seconds at most. Unsent events stay on disk for the next run.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.EndSession()
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		close(c.stop)
		select {
		case <-c.done:
		case <-time.After(closeWait):
			c.cancel()
			<-c.done
		}
		c.cancel()
	})
	return err
}

func (c *Client) enqueue(name string, value *float64, props []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enqueueLocked(name, value, props)
}

func (c *Client) enqueueLocked(name string, value *float64, props []byte) error {
	return c.addLocked(c.eventLocked(name, value, props))
}

func (c *Client) eventLocked(name string, value *float64, props []byte) event {
	ts := c.now().UTC()
	return event{
		Name: name, TS: &ts, DeviceID: c.deviceID, UserID: c.userID, SessionID: c.sessionID,
		AppVersion: c.appVersion, Platform: c.platform, OSVersion: c.osVersion, Value: value, Props: props,
	}
}

// addLocked queues e under a new event_id, which the event keeps when it is
// sent again.
func (c *Client) addLocked(e event) error {
	if c.closed {
		return ErrClosed
	}
	e.EventID = c.newID()
	if err := e.validate(); err != nil {
		return err
	}
	line, err := marshal(e)
	if err != nil {
		return err
	}
	ready, err := c.q.add(line)
	if ready {
		c.notify()
	}
	return err
}

func (c *Client) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) run() {
	defer close(c.done)
	flush := time.NewTicker(c.flushEvery)
	defer flush.Stop()
	beat := time.NewTicker(c.beatEvery)
	defer beat.Stop()
	retry := time.NewTimer(time.Hour)
	retry.Stop()
	defer retry.Stop()

	waiting := false
	for {
		select {
		case <-c.stop:
			c.q.seal()
			if !waiting {
				c.drain()
			}
			return
		case <-beat.C:
			c.heartbeat()
			continue
		case <-flush.C:
			c.q.sealIdle()
		case <-c.wake:
		case <-retry.C:
			waiting = false
		}
		if waiting {
			continue
		}
		if wait := c.drain(); wait > 0 {
			retry.Reset(wait)
			waiting = true
		}
	}
}

func (c *Client) logf(format string, args ...any) {
	if c.log == nil {
		return
	}
	// Most logging happens on the background goroutine, where a panic would
	// end the app.
	defer func() { _ = recover() }()
	c.log("beacon: " + fmt.Sprintf(format, args...))
}

// loadDeviceID returns the id stored in dir, creating it on first use. It
// is random and never derived from hardware.
func loadDeviceID(dir string, newID func() string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "device_id")
	b, err := os.ReadFile(path)
	if id := strings.TrimSpace(string(b)); err == nil && id != "" {
		return id, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	id := newID()
	if err := writeFileAtomic(path, []byte(id)); err != nil {
		return "", err
	}
	return id, nil
}

// platformName matches the names the TypeScript client sends.
func platformName(goos string) string {
	if goos == "darwin" {
		return "macos"
	}
	return goos
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand.Read never fails
	return hex.EncodeToString(b)
}

func or[T comparable](v, fallback T) T {
	var zero T
	if v != zero {
		return v
	}
	return fallback
}
