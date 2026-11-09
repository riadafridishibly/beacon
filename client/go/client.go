// Package beacon sends telemetry events to a beacon server. Events wait in
// an on-disk queue and go out in gzipped batches.
package beacon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// Logger receives messages about delivery problems and dropped data.
type Logger interface {
	Log(message string)
}

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
	// ProcessStartUnixMs overrides the time AppOpened measures from.
	ProcessStartUnixMs int64
	// Log is optional. It must not call the Client.
	Log Logger

	allowed map[string]bool

	// Set by tests.
	flushEvery, heartbeatEvery, minBackoff, maxBackoff time.Duration
	batchEvents                                        int
	now                                                func() time.Time
	newID                                              func() string
}

// AllowProp permits a props key. Keys that were not allowed are dropped
// from events passed to Track, TrackValue and TrackError.
func (c *Config) AllowProp(key string) {
	if c.allowed == nil {
		c.allowed = make(map[string]bool)
	}
	c.allowed[key] = true
}

// Client queues and sends events. Its methods are safe for concurrent use.
type Client struct {
	url, key               string
	http                   *http.Client
	q                      *queue
	log                    Logger
	allowed                map[string]bool
	now                    func() time.Time
	newID                  func() string
	started                time.Time
	flushEvery, beatEvery  time.Duration
	minBackoff, maxBackoff time.Duration
	failures               int // owned by the run goroutine

	deviceID, appVersion, platform, osVersion string

	opened atomic.Bool

	mu           sync.Mutex
	userID       string
	sessionID    string
	sessionStart time.Time
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

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		url:        strings.TrimRight(cfg.Endpoint, "/") + "/v1/events",
		key:        cfg.Key,
		http:       &http.Client{Timeout: requestTimeout},
		log:        cfg.Log,
		allowed:    maps.Clone(cfg.allowed),
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
	if cfg.ProcessStartUnixMs != 0 {
		c.started = time.UnixMilli(cfg.ProcessStartUnixMs)
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

// TrackError queues the event error.<area>, which the dashboard counts as an
// error. message is sent as the "message" prop only if AllowProp permits it,
// and "" sends none.
func (c *Client) TrackError(area, message string, props Props) error {
	if message != "" {
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
// each minute. It does nothing while a session is open.
func (c *Client) StartSession() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID != "" {
		return nil
	}
	c.sessionID = c.newID()
	c.sessionStart = c.now()
	return c.enqueueLocked("session.start", nil, nil)
}

// EndSession sends session.end and closes the session.
func (c *Client) EndSession() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID == "" {
		return nil
	}
	secs := c.sessionSecondsLocked()
	err := c.enqueueLocked("session.end", &secs, nil)
	c.sessionID = ""
	return err
}

func (c *Client) heartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID == "" {
		return
	}
	secs := c.sessionSecondsLocked()
	if err := c.enqueueLocked("session.heartbeat", &secs, nil); err != nil && !errors.Is(err, ErrClosed) {
		c.logf("heartbeat: %v", err)
	}
}

func (c *Client) sessionSecondsLocked() float64 {
	return math.Round(c.now().Sub(c.sessionStart).Seconds())
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
	if c.closed {
		return ErrClosed
	}
	ts := c.now().UTC()
	e := event{
		Name: name, TS: &ts, DeviceID: c.deviceID, UserID: c.userID, SessionID: c.sessionID,
		AppVersion: c.appVersion, Platform: c.platform, OSVersion: c.osVersion, Value: value, Props: props,
	}
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
	if c.log != nil {
		c.log.Log("beacon: " + fmt.Sprintf(format, args...))
	}
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
