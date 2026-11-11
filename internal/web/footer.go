package web

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

type (
	versionKey     struct{}
	requestTimeKey struct{}
	renderTimeKey  struct{}
)

// WithVersion makes v the server version layouts show in the footer.
func WithVersion(v string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), versionKey{}, v)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Version returns the server version to show, or "" outside WithVersion.
func Version(ctx context.Context) string {
	v, _ := ctx.Value(versionKey{}).(string)
	return v
}

// WithTiming makes layouts show in the footer how long the request and its
// template have taken so far.
func WithTiming(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), requestTimeKey{}, time.Now())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Timing returns the time since the request and its template started. ok is
// false outside WithTiming or outside Render.
func Timing(ctx context.Context) (page, template time.Duration, ok bool) {
	req, ok1 := ctx.Value(requestTimeKey{}).(time.Time)
	tpl, ok2 := ctx.Value(renderTimeKey{}).(time.Time)
	if !ok1 || !ok2 {
		return 0, 0, false
	}
	return time.Since(req), time.Since(tpl), true
}

func withRenderTime(ctx context.Context) context.Context {
	if ctx.Value(requestTimeKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, renderTimeKey{}, time.Now())
}

func millis(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 1, 64) + "ms"
}
