package beacon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
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
	case len(e.Props) > maxProps:
		return fmt.Errorf("beacon: props must be at most %d bytes", maxProps)
	case len(e.Props) > 0 && e.Props[0] != '{':
		return errors.New("beacon: props must be a JSON object")
	}
	return nil
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

// filterProps keeps the allowed keys of a JSON object and returns the names
// of the others.
func filterProps(propsJSON string, allowed map[string]bool) (json.RawMessage, []string, error) {
	if propsJSON == "" {
		return nil, nil, nil
	}
	var props map[string]json.RawMessage
	if err := json.Unmarshal([]byte(propsJSON), &props); err != nil {
		return nil, nil, errors.New("beacon: props must be a JSON object")
	}
	var dropped []string
	for k := range props {
		if !allowed[k] {
			dropped = append(dropped, k)
			delete(props, k)
		}
	}
	if len(props) == 0 {
		return nil, dropped, nil
	}
	b, err := marshal(props)
	return b, dropped, err
}
