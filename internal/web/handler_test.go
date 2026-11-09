package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleErrors(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	boom := errors.New("boom")

	for _, tc := range []struct {
		name   string
		ctx    context.Context
		err    error
		status int
		body   string
	}{
		{"client went away", canceled, fmt.Errorf("query: %w", context.Canceled), http.StatusOK, ""},
		{"real error after the client went away", canceled, boom, http.StatusInternalServerError, "Something went wrong"},
		{"deadline", expired, fmt.Errorf("query: %w", context.DeadlineExceeded), http.StatusServiceUnavailable, "took too long"},
		{"real error after the deadline", expired, boom, http.StatusInternalServerError, "Something went wrong"},
		{"not found after the deadline", expired, ErrNotFound, http.StatusNotFound, "Page not found"},
		{"deadline error from elsewhere", context.Background(), context.DeadlineExceeded, http.StatusInternalServerError, "Something went wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := Handle(func(http.ResponseWriter, *http.Request) error { return tc.err })
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(tc.ctx))
			if w.Code != tc.status {
				t.Errorf("status %d, want %d", w.Code, tc.status)
			}
			if !strings.Contains(w.Body.String(), tc.body) || (tc.body == "" && w.Body.Len() != 0) {
				t.Errorf("body %.200q, want %q", w.Body.String(), tc.body)
			}
		})
	}
}
