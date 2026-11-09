package beacon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func readFixture(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "conformance", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func TestConformanceEvents(t *testing.T) {
	t.Parallel()
	var cases []struct {
		Name      string `json:"name"`
		Event     event  `json:"event"`
		PropsSize int    `json:"props_size"`
		Valid     bool   `json:"valid"`
	}
	readFixture(t, "events.json", &cases)
	allowed := map[string]bool{"p": true, "p\x00": true, "cold": true}

	for _, tt := range cases {
		raw := tt.Event.Props
		if tt.PropsSize > 0 {
			raw = fmt.Appendf(nil, `{"p":%q}`, strings.Repeat("a", tt.PropsSize-len(`{"p":""}`)))
		}
		filtered, err := filterRaw(raw, allowed)
		e := tt.Event
		e.Props = filtered
		if err == nil {
			err = e.validate()
		}
		if got := err == nil; got != tt.Valid {
			t.Errorf("%s: valid = %v, want %v: %v", tt.Name, got, tt.Valid, err)
		}
	}
}

// filterRaw keeps each value as raw JSON so that the unpaired surrogates in
// the cases reach validate. A Go string can't hold them.
func filterRaw(raw json.RawMessage, allowed map[string]bool) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	props := make(Props, len(values))
	for k, v := range values {
		props[k] = v
	}
	filtered, _, err := filterProps(props, allowed)
	return filtered, err
}

func TestConformanceResponses(t *testing.T) {
	t.Parallel()
	var cases []struct {
		Name        string `json:"name"`
		Status      int    `json:"status"`
		RetryAfter  string `json:"retry_after"`
		Body        string `json:"body"`
		Action      action `json:"action"`
		Index       int    `json:"index"`
		WaitSeconds int    `json:"wait_seconds"`
	}
	readFixture(t, "responses.json", &cases)
	for _, tt := range cases {
		got := classify(tt.Status, tt.RetryAfter, []byte(tt.Body))
		want := outcome{action: tt.Action, index: tt.Index, wait: time.Duration(tt.WaitSeconds) * time.Second}
		if got != want {
			t.Errorf("%s: %+v, want %+v", tt.Name, got, want)
		}
	}
}

// The client must produce the body in batch.json, which the server's test
// posts to the real handler.
func TestConformanceBatch(t *testing.T) {
	t.Parallel()
	var want any
	readFixture(t, "batch.json", &want)

	srv := newServer(t)
	cfg := srv.config(t)
	cfg.AppVersion, cfg.Platform, cfg.OSVersion = "1.4.0", "macos", "15.1"
	cfg.newID = func() string { return "fixed-id" }
	cfg.now = func() time.Time { return time.Date(2026, 11, 8, 11, 59, 0, 0, time.UTC) }
	cfg.AllowProps = []string{"cold"}
	c := newClient(t, cfg)

	c.SetUserID("tester@example.com")
	must(t, c.StartSession())
	must(t, c.TrackValue("sync.done", 812, Props{"cold": true}))
	must(t, c.Close())

	var got any
	if err := json.Unmarshal(srv.wait(t, 1)[0], &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body\n got %v\nwant %v", got, want)
	}
}
