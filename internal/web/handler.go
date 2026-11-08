package web

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/a-h/templ"
)

// ErrNotFound makes Handle render the 404 page.
var ErrNotFound = errors.New("not found")

// HandlerFunc is an http.HandlerFunc that returns an error instead of writing
// error responses itself.
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// Handle adapts h to http.Handler. ErrNotFound renders the 404 page; any
// other error is logged and renders the 500 page, unless the client went
// away.
func Handle(h HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := h(w, r)
		if err == nil || errors.Is(err, context.Canceled) && r.Context().Err() != nil {
			return
		}
		if errors.Is(err, ErrNotFound) {
			_ = Render(w, r, http.StatusNotFound, NotFoundPage())
			return
		}
		slog.ErrorContext(r.Context(), "handler failed", "method", r.Method, "path", r.URL.Path, "err", err)
		_ = Render(w, r, http.StatusInternalServerError, ErrorPage())
	})
}

// Render renders c into a buffer, then writes it with the given status, so a
// template error never leaves a half-written page.
func Render(w http.ResponseWriter, r *http.Request, status int, c templ.Component) error {
	var buf bytes.Buffer
	if err := c.Render(r.Context(), &buf); err != nil {
		return fmt.Errorf("render: %w", err)
	}
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	// The same URL returns a fragment or a full page depending on this header.
	h.Add("Vary", "HX-Request")
	w.WriteHeader(status)
	_, err := buf.WriteTo(w)
	return err
}

// IsHTMX reports whether r came from htmx and expects a fragment rather than
// a full page.
func IsHTMX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true" &&
		r.Header.Get("HX-History-Restore-Request") != "true"
}

// Redirect sends the browser to url after a successful POST: an HX-Redirect
// header for htmx requests, a 303 otherwise.
func Redirect(w http.ResponseWriter, r *http.Request, url string) {
	if IsHTMX(r) {
		w.Header().Set("HX-Redirect", url)
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, url, http.StatusSeeOther)
}
