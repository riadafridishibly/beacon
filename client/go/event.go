package beacon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// These limits match internal/ingest. client/conformance/events.json is run
// against both.
const (
	maxNameLen = 64
	maxIDLen   = 128
	maxUserLen = 256
	maxAttrLen = 64
	maxProps   = 16 << 10
)

var namePattern = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)*$`)

// Props is the JSON object an event carries beside its one numeric value.
type Props map[string]any

type event struct {
	Name       string          `json:"name"`
	TS         *time.Time      `json:"ts,omitempty"`
	DeviceID   string          `json:"device_id,omitempty"`
	UserID     string          `json:"user_id,omitempty"`
	SessionID  string          `json:"session_id,omitempty"`
	AppVersion string          `json:"app_version,omitempty"`
	Platform   string          `json:"platform,omitempty"`
	OSVersion  string          `json:"os_version,omitempty"`
	Value      *float64        `json:"value,omitempty"`
	Props      json.RawMessage `json:"props,omitempty"`
}

// validate applies the server's rules, so that an event the server would
// reject never reaches the queue.
func (e *event) validate() error {
	switch {
	case !namePattern.MatchString(e.Name) || len(e.Name) > maxNameLen:
		return fmt.Errorf("beacon: name %q must be at most %d lowercase letters, digits and underscores, in dot-separated parts", e.Name, maxNameLen)
	case e.DeviceID == "":
		return errors.New("beacon: device_id is required")
	case len(e.DeviceID) > maxIDLen || len(e.SessionID) > maxIDLen:
		return fmt.Errorf("beacon: device_id and session_id must be at most %d bytes", maxIDLen)
	case len(e.UserID) > maxUserLen:
		return fmt.Errorf("beacon: user_id must be at most %d bytes", maxUserLen)
	case len(e.AppVersion) > maxAttrLen || len(e.Platform) > maxAttrLen || len(e.OSVersion) > maxAttrLen:
		return fmt.Errorf("beacon: app_version, platform and os_version must be at most %d bytes", maxAttrLen)
	case hasNUL(e.DeviceID, e.UserID, e.SessionID, e.AppVersion, e.Platform, e.OSVersion):
		return errors.New("beacon: device_id, user_id, session_id, app_version, platform and os_version must not contain NUL")
	case len(e.Props) > maxProps:
		return fmt.Errorf("beacon: props must be at most %d bytes", maxProps)
	case len(e.Props) > 0 && e.Props[0] != '{':
		return errors.New("beacon: props must be a JSON object")
	case !storableJSON(e.Props):
		return errors.New(`beacon: props must be valid UTF-8, without \u0000 or unpaired surrogates`)
	}
	return nil
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

// marshal encodes v without HTML escaping, which would inflate props past
// the size that validate measured.
func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// filterProps encodes the allowed keys of props and returns the names of the
// others. It leaves props unchanged because the map is the caller's.
func filterProps(props Props, allowed map[string]bool) (json.RawMessage, []string, error) {
	kept := make(Props, len(props))
	var dropped []string
	for k, v := range props {
		if allowed[k] {
			kept[k] = v
		} else {
			dropped = append(dropped, k)
		}
	}
	if len(kept) == 0 {
		return nil, dropped, nil
	}
	b, err := marshal(kept)
	if err != nil {
		return nil, dropped, fmt.Errorf("beacon: props: %w", err)
	}
	return b, dropped, nil
}
