// Package web holds HTTP middleware shared by the API and, later, the
// dashboard.
package web

import (
	"errors"
	"log/slog"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

// Logger logs one line per request.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		slog.InfoContext(r.Context(), "request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", time.Since(start),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Recoverer turns a panic in a handler into a logged 500.
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v)
				}
				slog.ErrorContext(r.Context(), "panic", "method", r.Method, "path", r.URL.Path, "panic", v)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// RealIP sets r.RemoteAddr to the client address from X-Forwarded-For when
// the connection comes from a trusted proxy. It walks the header from the
// right and takes the first address that isn't a trusted proxy, because
// clients can put anything on the left.
func RealIP(trusted []netip.Prefix) func(http.Handler) http.Handler {
	isTrusted := func(a netip.Addr) bool {
		for _, p := range trusted {
			if p.Contains(a) {
				return true
			}
		}
		return false
	}
	return func(next http.Handler) http.Handler {
		if len(trusted) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			peer, err := netip.ParseAddrPort(r.RemoteAddr)
			if err != nil || !isTrusted(peer.Addr().Unmap()) {
				next.ServeHTTP(w, r)
				return
			}
			hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
			for i := len(hops) - 1; i >= 0; i-- {
				a, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
				if err != nil {
					break
				}
				a = a.Unmap()
				if !isTrusted(a) {
					r2 := r.Clone(r.Context())
					r2.RemoteAddr = netip.AddrPortFrom(a, 0).String()
					next.ServeHTTP(w, r2)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
