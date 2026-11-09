package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func serveBrand(t *testing.T, b *Brand, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	b.Routes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("%s: status %d", path, w.Code)
	}
	return w
}

func writeBrandFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultBrand(t *testing.T) {
	t.Parallel()
	b, err := NewBrand("", "")
	if err != nil {
		t.Fatal(err)
	}
	if b.name != "Beacon" {
		t.Errorf("name = %q", b.name)
	}
	if len(b.logo) == 0 {
		t.Error("no built-in logo")
	}
	w := serveBrand(t, b, brandFaviconPath)
	if w.Header().Get("Content-Type") != "image/svg+xml" || w.Body.String() != string(defaultBrandLogo) {
		t.Errorf("favicon: %s, %.40s", w.Header().Get("Content-Type"), w.Body.String())
	}
}

func TestBrandImagesFromDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBrandFile(t, dir, "logo.svg", "<svg>logo</svg>")

	b, err := NewBrand("Acme", dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := serveBrand(t, b, brandFaviconPath).Body.String(); got != "<svg>logo</svg>" {
		t.Errorf("favicon without its own file = %q", got)
	}

	writeBrandFile(t, dir, "favicon.svg", "<svg>favicon</svg>")
	b, err = NewBrand("Acme", dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := serveBrand(t, b, brandLogoPath).Body.String(); got != "<svg>logo</svg>" {
		t.Errorf("logo = %q", got)
	}
	if got := serveBrand(t, b, brandFaviconPath).Body.String(); got != "<svg>favicon</svg>" {
		t.Errorf("favicon = %q", got)
	}
}

func TestBrandDirNeedsLogo(t *testing.T) {
	t.Parallel()
	if _, err := NewBrand("Acme", t.TempDir()); err == nil {
		t.Error("no error for a directory without logo.svg")
	}
}

func TestBrandName(t *testing.T) {
	t.Parallel()
	if got := BrandName(context.Background()); got != "Beacon" {
		t.Errorf("outside middleware: %q", got)
	}
	b, err := NewBrand("Acme", "")
	if err != nil {
		t.Fatal(err)
	}
	var got string
	h := b.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = BrandName(r.Context())
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if got != "Acme" {
		t.Errorf("in middleware: %q", got)
	}
}
