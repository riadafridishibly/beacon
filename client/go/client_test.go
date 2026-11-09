package beacon

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// server records the bodies it accepts. respond, when set, picks the
// response to each request by number, starting at 0; -1 means accept.
type server struct {
	*httptest.Server
	mu      sync.Mutex
	bodies  [][]byte
	calls   int
	respond func(call int, body []byte) (int, string)
	changed chan struct{}
}

func newServer(t *testing.T) *server {
	t.Helper()
	s := &server{changed: make(chan struct{}, 1)}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.Close)
	return s
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/events" || r.Method != http.MethodPost ||
		r.Header.Get("Authorization") != "Bearer key" ||
		r.Header.Get("Content-Encoding") != "gzip" ||
		r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, fmt.Sprintf(`{"error":"bad request shape: %s %s %v"}`, r.Method, r.URL.Path, r.Header), http.StatusTeapot)
		return
	}
	zr, err := gzip.NewReader(r.Body)
	if err != nil {
		http.Error(w, `{"error":"not gzip"}`, http.StatusTeapot)
		return
	}
	body, _ := io.ReadAll(zr)

	s.mu.Lock()
	call := s.calls
	s.calls++
	respond := s.respond
	s.mu.Unlock()
	status, resp := http.StatusAccepted, `{"accepted":1}`
	if respond != nil {
		if st, msg := respond(call, body); st != -1 {
			status, resp = st, msg
		}
	}
	if status == http.StatusAccepted {
		s.mu.Lock()
		s.bodies = append(s.bodies, body)
		s.mu.Unlock()
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, resp)
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *server) config(t *testing.T) *Config {
	t.Helper()
	return &Config{
		Endpoint:   s.URL + "/",
		Key:        "key",
		Dir:        t.TempDir(),
		flushEvery: time.Hour, heartbeatEvery: time.Hour,
		minBackoff: time.Millisecond, maxBackoff: 5 * time.Millisecond,
	}
}

// wait returns the accepted bodies once there are at least n.
func (s *server) wait(t *testing.T, n int) [][]byte {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		s.mu.Lock()
		got := append([][]byte(nil), s.bodies...)
		s.mu.Unlock()
		if len(got) >= n {
			return got
		}
		select {
		case <-s.changed:
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatalf("got %d bodies, want %d", len(got), n)
		}
	}
}

func newClient(t *testing.T, cfg *Config) *Client {
	t.Helper()
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type wireEvent struct {
	Name      string          `json:"name"`
	DeviceID  string          `json:"device_id"`
	UserID    string          `json:"user_id"`
	SessionID string          `json:"session_id"`
	Value     *float64        `json:"value"`
	Props     json.RawMessage `json:"props"`
}

func decodeEvents(t *testing.T, bodies [][]byte) []wireEvent {
	t.Helper()
	var all []wireEvent
	for _, b := range bodies {
		var batch struct {
			Events []wireEvent `json:"events"`
		}
		if err := json.Unmarshal(b, &batch); err != nil {
			t.Fatalf("%v: %s", err, b)
		}
		all = append(all, batch.Events...)
	}
	return all
}

func names(events []wireEvent) string {
	var s []string
	for _, e := range events {
		s = append(s, e.Name)
	}
	return strings.Join(s, " ")
}

func TestSendsWhenBatchIsFull(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.batchEvents = 3
	c := newClient(t, cfg)
	for _, name := range []string{"a", "b", "c"} {
		must(t, c.Track(name, nil))
	}
	if got := names(decodeEvents(t, srv.wait(t, 1))); got != "a b c" {
		t.Errorf("events %q", got)
	}
}

func TestSendsOnInterval(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.flushEvery = 5 * time.Millisecond
	c := newClient(t, cfg)
	must(t, c.Track("a", nil))
	if got := names(decodeEvents(t, srv.wait(t, 1))); got != "a" {
		t.Errorf("events %q", got)
	}
}

func TestRetriesAfterServerError(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	srv.respond = func(call int, _ []byte) (int, string) {
		if call < 3 {
			return http.StatusInternalServerError, `{"error":"internal error"}`
		}
		return -1, ""
	}
	c := newClient(t, srv.config(t))
	must(t, c.Track("a", nil))
	c.Flush()
	if got := names(decodeEvents(t, srv.wait(t, 1))); got != "a" {
		t.Errorf("events %q", got)
	}
}

func TestBadEventIsDroppedAndRestResent(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	srv.respond = func(call int, _ []byte) (int, string) {
		if call == 0 {
			return http.StatusBadRequest, `{"error":"bad","index":1}`
		}
		return -1, ""
	}
	cfg := srv.config(t)
	cfg.batchEvents = 3
	c := newClient(t, cfg)
	for _, name := range []string{"a", "b", "c"} {
		must(t, c.Track(name, nil))
	}
	if got := names(decodeEvents(t, srv.wait(t, 1))); got != "a c" {
		t.Errorf("events %q", got)
	}
}

func TestBadBatchIsDropped(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	srv.respond = func(call int, _ []byte) (int, string) {
		if call == 0 {
			return http.StatusBadRequest, `{"error":"invalid JSON"}`
		}
		return -1, ""
	}
	c := newClient(t, srv.config(t))
	must(t, c.Track("a", nil))
	c.Flush()
	deadline := time.Now().Add(10 * time.Second)
	for c.q.oldest() != nil {
		if time.Now().After(deadline) {
			t.Fatal("batch still queued")
		}
		time.Sleep(time.Millisecond)
	}
	must(t, c.Track("b", nil))
	c.Flush()
	if got := names(decodeEvents(t, srv.wait(t, 1))); got != "b" {
		t.Errorf("events %q", got)
	}
}

func TestQueueSurvivesRestart(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	srv.respond = func(int, []byte) (int, string) {
		return http.StatusServiceUnavailable, ""
	}
	cfg := srv.config(t)
	c := newClient(t, cfg)
	must(t, c.Track("a", nil))
	must(t, c.Close())
	if err := c.Track("b", nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Track after Close: %v", err)
	}

	srv.mu.Lock()
	srv.respond = nil
	srv.mu.Unlock()
	newClient(t, cfg)
	if got := names(decodeEvents(t, srv.wait(t, 1))); got != "a" {
		t.Errorf("events %q", got)
	}
}

func TestDeviceIDPersists(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	a := newClient(t, cfg)
	must(t, a.Close())
	b := newClient(t, cfg)
	if a.DeviceID() == "" || a.DeviceID() != b.DeviceID() {
		t.Errorf("device ids %q and %q", a.DeviceID(), b.DeviceID())
	}
}

func TestSessionEvents(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.heartbeatEvery = 5 * time.Millisecond
	var mu sync.Mutex
	current := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	cfg.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		current = current.Add(time.Second)
		return current
	}
	c := newClient(t, cfg)
	must(t, c.Track("before", nil))
	must(t, c.StartSession())
	must(t, c.StartSession())
	time.Sleep(30 * time.Millisecond)
	must(t, c.EndSession())
	must(t, c.Track("after", nil))
	must(t, c.Close())

	events := decodeEvents(t, srv.wait(t, 1))
	got := names(events)
	if !strings.HasPrefix(got, "before session.start session.heartbeat") || !strings.HasSuffix(got, "session.end after") {
		t.Fatalf("events %q", got)
	}
	start, end := events[1], events[len(events)-2]
	if events[0].SessionID != "" || events[len(events)-1].SessionID != "" {
		t.Errorf("events outside a session have a session id: %+v", events)
	}
	if start.SessionID == "" || start.SessionID != end.SessionID {
		t.Errorf("session ids %q and %q", start.SessionID, end.SessionID)
	}
	if end.Value == nil || *end.Value <= 0 {
		t.Errorf("session.end value %v", end.Value)
	}
}

func TestAppOpenedOnce(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.ProcessStart = time.UnixMilli(1000)
	cfg.now = func() time.Time { return time.UnixMilli(1800) }
	c := newClient(t, cfg)
	must(t, c.AppOpened())
	must(t, c.AppOpened())
	must(t, c.Close())

	events := decodeEvents(t, srv.wait(t, 1))
	if len(events) != 1 || events[0].Name != "app.open" || *events[0].Value != 800 || string(events[0].Props) != `{"cold":true}` {
		t.Errorf("events %+v", events)
	}
}

func TestPropsMustBeAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.AllowProps = []string{"code"}
	c := newClient(t, cfg)
	props := Props{"code": 42, "message": "secret"}
	must(t, c.Track("error.sync", props))
	if len(props) != 2 {
		t.Errorf("caller's props changed: %v", props)
	}
	must(t, c.Track("x", Props{"message": "secret"}))
	must(t, c.Track("big", Props{"code": int64(1<<53 + 1)}))
	if err := c.Track("x", Props{"code": make(chan int)}); err == nil {
		t.Error("unencodable props accepted")
	}
	if err := c.Track("Bad Name", nil); err == nil {
		t.Error("bad name accepted")
	}
	must(t, c.Close())

	events := decodeEvents(t, srv.wait(t, 1))
	if len(events) != 3 || string(events[0].Props) != `{"code":42}` || events[1].Props != nil ||
		string(events[2].Props) != `{"code":9007199254740993}` {
		t.Errorf("events %+v", events)
	}
}

func TestTrackError(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.AllowProps = []string{"code", "message"}
	c := newClient(t, cfg)
	props := Props{"code": 42, "message": "old", "other": 1}
	must(t, c.TrackError("engine", "boom", props))
	if props["message"] != "old" {
		t.Errorf("caller's props changed: %v", props)
	}
	must(t, c.TrackError("sync", "", nil))
	must(t, c.TrackError("auth", "denied", nil))
	for _, area := range []string{"", "Bad Area"} {
		if err := c.TrackError(area, "m", nil); err == nil {
			t.Errorf("area %q accepted", area)
		}
	}
	if err := c.TrackError("x", "m", Props{"code": make(chan int)}); err == nil {
		t.Error("unencodable props accepted")
	}
	must(t, c.Close())

	events := decodeEvents(t, srv.wait(t, 1))
	if len(events) != 3 {
		t.Fatalf("events %+v", events)
	}
	if e := events[0]; e.Name != "error.engine" || string(e.Props) != `{"code":42,"message":"boom"}` {
		t.Errorf("event %s %s", e.Name, e.Props)
	}
	if e := events[1]; e.Name != "error.sync" || e.Props != nil {
		t.Errorf("event %s %s", e.Name, e.Props)
	}
	if e := events[2]; e.Name != "error.auth" || string(e.Props) != `{"message":"denied"}` {
		t.Errorf("event %s %s", e.Name, e.Props)
	}
}

func TestTrackErrorMessageMustBeAllowed(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	c := newClient(t, srv.config(t))
	must(t, c.TrackError("engine", "secret", nil))
	must(t, c.Close())
	if e := decodeEvents(t, srv.wait(t, 1))[0]; e.Name != "error.engine" || e.Props != nil {
		t.Errorf("event %s %s", e.Name, e.Props)
	}
}

func TestTrackErrorCleansMessage(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.AllowProps = []string{"message"}
	c := newClient(t, cfg)
	tests := []struct{ in, want string }{
		{"a\x00b", "ab"},
		{"a\xffb", "a�b"},
		{strings.Repeat("€", 5000), strings.Repeat("€", 682)},
		{strings.Repeat("\x01", 5000), strings.Repeat("\x01", maxMessage)},
	}
	for _, tt := range tests {
		must(t, c.TrackError("x", tt.in, nil))
	}
	must(t, c.TrackError("only_nul", "\x00", Props{"message": "kept"}))
	must(t, c.Close())

	events := decodeEvents(t, srv.wait(t, 1))
	if len(events) != len(tests)+1 {
		t.Fatalf("got %d events", len(events))
	}
	for i, tt := range tests {
		var got struct{ Message string }
		if err := json.Unmarshal(events[i].Props, &got); err != nil {
			t.Fatal(err)
		}
		if got.Message != tt.want {
			t.Errorf("message %d: %d bytes %q", i, len(got.Message), got.Message[:min(len(got.Message), 20)])
		}
	}
	if p := string(events[len(tests)].Props); p != `{"message":"kept"}` {
		t.Errorf("props %s", p)
	}
}

func TestErrorValuesAreSentAsText(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	cfg.AllowProps = []string{"cause"}
	c := newClient(t, cfg)
	var nilErr *os.PathError
	must(t, c.Track("a", Props{"cause": errors.New("boom")}))
	must(t, c.Track("b", Props{"cause": nilErr}))
	must(t, c.Close())

	events := decodeEvents(t, srv.wait(t, 1))
	if string(events[0].Props) != `{"cause":"boom"}` || string(events[1].Props) != `{"cause":null}` {
		t.Errorf("props %s, %s", events[0].Props, events[1].Props)
	}
}

func TestAllowPropsAfterNewClientHaveNoEffect(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	cfg := srv.config(t)
	c := newClient(t, cfg)
	cfg.AllowProps = []string{"late"}
	must(t, c.Track("x", Props{"late": 1}))
	must(t, c.Close())
	if events := decodeEvents(t, srv.wait(t, 1)); events[0].Props != nil {
		t.Errorf("props %s", events[0].Props)
	}
}

func TestPlatformDefault(t *testing.T) {
	t.Parallel()
	for goos, want := range map[string]string{"darwin": "macos", "windows": "windows", "linux": "linux", "ios": "ios", "android": "android"} {
		if got := platformName(goos); got != want {
			t.Errorf("%s: %s, want %s", goos, got, want)
		}
	}
}

func TestUserIDIsOptIn(t *testing.T) {
	t.Parallel()
	srv := newServer(t)
	c := newClient(t, srv.config(t))
	must(t, c.Track("a", nil))
	c.SetUserID("tester")
	must(t, c.Track("b", nil))
	must(t, c.Close())
	events := decodeEvents(t, srv.wait(t, 1))
	if events[0].UserID != "" || events[1].UserID != "tester" {
		t.Errorf("events %+v", events)
	}
}

func TestNewClientRejectsBadConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, cfg := range []*Config{
		nil,
		{Key: "k", Dir: dir},
		{Endpoint: "ftp://x", Key: "k", Dir: dir},
		{Endpoint: "http://x", Dir: dir},
		{Endpoint: "http://x", Key: "k"},
		{Endpoint: "http://x", Key: "k", Dir: dir, AppVersion: strings.Repeat("a", 65)},
	} {
		if c, err := NewClient(cfg); err == nil {
			_ = c.Close()
			t.Errorf("accepted %+v", cfg)
		}
	}
}

func openTestQueue(t *testing.T, maxBytes int64, maxEvents int) (*queue, *int64) {
	t.Helper()
	var dropped int64
	q, err := openQueue(t.TempDir(), maxBytes, maxEvents, maxBatchBytes, func(n int64) { dropped += n })
	if err != nil {
		t.Fatal(err)
	}
	return q, &dropped
}

func TestQueueDropsOldestPastCap(t *testing.T) {
	t.Parallel()
	q, dropped := openTestQueue(t, 100, 2)
	for i := range 20 {
		if _, err := q.add(fmt.Appendf(nil, `{"name":"e%02d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	if *dropped == 0 {
		t.Error("nothing dropped")
	}
	if q.total > q.maxBytes {
		t.Errorf("queue holds %d bytes, cap %d", q.total, q.maxBytes)
	}
	lines, err := q.read(q.oldest())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(lines[0]), "e00") {
		t.Errorf("oldest event survived: %s", lines[0])
	}
}

func TestQueueSkipsTornLine(t *testing.T) {
	t.Parallel()
	q, _ := openTestQueue(t, 1<<20, 10)
	if _, err := q.add([]byte(`{"name":"a"}`)); err != nil {
		t.Fatal(err)
	}
	q.seal()
	seg := q.oldest()
	f, err := os.OpenFile(seg.path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"name":"b`)
	_ = f.Close()

	lines, err := q.read(seg)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || string(lines[0]) != `{"name":"a"}` {
		t.Errorf("lines %q", lines)
	}
}

func TestQueueReopensOldestFirst(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "q")
	q, err := openQueue(dir, 1<<20, 1, maxBatchBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := q.add(fmt.Appendf(nil, `{"name":%q}`, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "leftover.tmp"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	q2, err := openQueue(dir, 1<<20, 1, maxBatchBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(q2.segs) != 2 {
		t.Fatalf("%d segments", len(q2.segs))
	}
	lines, _ := q2.read(q2.oldest())
	if string(lines[0]) != `{"name":"a"}` {
		t.Errorf("oldest %q", lines[0])
	}
	if _, err := q2.add([]byte(`{"name":"c"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "leftover.tmp")); !os.IsNotExist(err) {
		t.Errorf("temp file kept: %v", err)
	}
}

func TestBatchFitsBodyLimit(t *testing.T) {
	t.Parallel()
	q, _ := openTestQueue(t, 100<<20, 100)
	big := []byte(`{"name":"x","props":{"p":"` + strings.Repeat("a", maxProps) + `"}}`)
	for range 300 {
		if _, err := q.add(big); err != nil {
			t.Fatal(err)
		}
	}
	if len(q.segs) < 2 {
		t.Fatalf("%d segments", len(q.segs))
	}
	for _, seg := range q.segs {
		if seg.size > maxBatchBytes {
			t.Errorf("segment of %d bytes", seg.size)
		}
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	t.Parallel()
	c := &Client{minBackoff: time.Second, maxBackoff: 8 * time.Second}
	for i, limit := range []time.Duration{1, 2, 4, 8, 8, 8} {
		d := c.nextBackoff()
		if limit *= time.Second; d < limit/2 || d > limit {
			t.Errorf("attempt %d: %v, want between %v and %v", i, d, limit/2, limit)
		}
	}
	c.failures = 100
	if d := c.nextBackoff(); d > c.maxBackoff {
		t.Errorf("after many failures: %v", d)
	}
}
