package web

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
)

const (
	defaultBrandName = "Beacon"
	brandLogoPath    = "/static/brand/logo.svg"
	brandFaviconPath = "/static/brand/favicon.svg"
)

//go:embed brand/logo.svg
var defaultBrandLogo []byte

// Brand is the name and images a deployment shows in the dashboard.
type Brand struct {
	name    string
	logo    []byte
	favicon []byte
}

// NewBrand returns the brand for name and dir. An empty name means Beacon and
// an empty dir means the built-in mark. A dir must hold logo.svg; favicon.svg
// is optional and defaults to the logo.
func NewBrand(name, dir string) (*Brand, error) {
	if name == "" {
		name = defaultBrandName
	}
	b := &Brand{name: name, logo: defaultBrandLogo, favicon: defaultBrandLogo}
	if dir == "" {
		return b, nil
	}
	logo, err := os.ReadFile(filepath.Join(dir, "logo.svg"))
	if err != nil {
		return nil, fmt.Errorf("brand: %w", err)
	}
	b.logo, b.favicon = logo, logo
	favicon, err := os.ReadFile(filepath.Join(dir, "favicon.svg"))
	if errors.Is(err, fs.ErrNotExist) {
		return b, nil
	}
	if err != nil {
		return nil, fmt.Errorf("brand: %w", err)
	}
	b.favicon = favicon
	return b, nil
}

// Routes registers the brand images on mux. They are public because the login
// page shows them.
func (b *Brand) Routes(mux *http.ServeMux) {
	mux.Handle("GET "+brandLogoPath, svgHandler(b.logo))
	mux.Handle("GET "+brandFaviconPath, svgHandler(b.favicon))
}

// svgHandler uses a short cache lifetime because the URLs carry no content
// hash, and a deployment can change its images.
func svgHandler(svg []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(svg)
	})
}

type brandKey struct{}

// Middleware makes the brand available to layouts through the request context.
func (b *Brand) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), brandKey{}, b)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// BrandName returns the name to show. Outside Brand.Middleware, as in handler
// tests, it is the default.
func BrandName(ctx context.Context) string {
	if b, ok := ctx.Value(brandKey{}).(*Brand); ok {
		return b.name
	}
	return defaultBrandName
}
