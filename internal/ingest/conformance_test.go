package ingest

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

const conformanceDir = "../../client/conformance/"

func readFixture(t *testing.T, name string, v any) []byte {
	t.Helper()
	b, err := os.ReadFile(conformanceDir + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestConformanceEvents holds the server to the validation rules that every
// client copies.
func TestConformanceEvents(t *testing.T) {
	t.Parallel()
	_, h, key := setup(t)
	var cases []struct {
		Name      string         `json:"name"`
		Event     map[string]any `json:"event"`
		PropsSize int            `json:"props_size"`
		Valid     bool           `json:"valid"`
	}
	readFixture(t, "events.json", &cases)

	for _, tt := range cases {
		if tt.PropsSize > 0 {
			tt.Event["props"] = map[string]string{"p": strings.Repeat("a", tt.PropsSize-len(`{"p":""}`))}
		}
		body, err := json.Marshal(map[string]any{"events": []any{tt.Event}})
		if err != nil {
			t.Fatal(err)
		}
		rec := post(t, h, key, body, false)
		if tt.Valid && rec.Code != http.StatusAccepted {
			t.Errorf("%s: status %d, want 202: %s", tt.Name, rec.Code, rec.Body)
		}
		if !tt.Valid && (rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"index":0`)) {
			t.Errorf("%s: %d %s, want 400 with index 0", tt.Name, rec.Code, rec.Body)
		}
	}
}

// TestConformanceBatch posts the body that clients are tested to produce.
func TestConformanceBatch(t *testing.T) {
	t.Parallel()
	pool, h, key := setup(t)
	var batch struct {
		Events []json.RawMessage `json:"events"`
	}
	body := readFixture(t, "batch.json", &batch)

	rec := post(t, h, key, body, true)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	got := load(t, pool)
	if len(got) != len(batch.Events) {
		t.Fatalf("%d rows, want %d", len(got), len(batch.Events))
	}
	if got[1].Name != "sync.done" || got[1].DeviceID != "fixed-id" || *got[1].UserID != "tester@example.com" ||
		*got[1].AppVersion != "1.4.0" || *got[1].Platform != "macos" || *got[1].Value != 812 {
		t.Errorf("row %+v", got[1])
	}
}
