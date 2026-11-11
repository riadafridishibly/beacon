package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/a-h/templ"
)

func TestTiming(t *testing.T) {
	t.Parallel()
	if _, _, ok := Timing(context.Background()); ok {
		t.Error("ok outside WithTiming")
	}

	var page, tmpl time.Duration
	var ok bool
	c := templ.ComponentFunc(func(ctx context.Context, _ io.Writer) error {
		page, tmpl, ok = Timing(ctx)
		return nil
	})
	h := WithTiming(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		if _, _, ok := Timing(r.Context()); ok {
			t.Error("ok outside Render")
		}
		_ = Render(w, r, http.StatusOK, c)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !ok || page < 5*time.Millisecond || tmpl > page {
		t.Errorf("page %v, template %v, ok %v", page, tmpl, ok)
	}
}

func TestMillis(t *testing.T) {
	t.Parallel()
	for d, want := range map[time.Duration]string{
		340 * time.Microsecond:                      "0.3ms",
		122*time.Millisecond + 449*time.Microsecond: "122.4ms",
		2 * time.Second:                             "2000.0ms",
	} {
		if got := millis(d); got != want {
			t.Errorf("%v: %q, want %q", d, got, want)
		}
	}
}
