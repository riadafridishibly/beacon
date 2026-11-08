package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"net/http"
	"strings"

	"github.com/a-h/templ"
)

// dist is the Vite build output (`pnpm run build`). It holds only .gitkeep
// until the frontend has been built.
//
//go:embed all:dist
var dist embed.FS

// Vite entry points, relative to the Vite root (frontend/). The stylesheet is
// a separate entry so both dev and production load it with a render-blocking
// <link>; imported from JS, Vite's dev server would apply it after first
// paint.
const (
	scriptEntry = "src/main.ts"
	styleEntry  = "src/app.css"
)

// Assets holds the <link> and <script> tags that load the frontend.
type Assets struct {
	tags string
}

// NewAssets points the tags at the Vite dev server when viteURL is set, and
// at the hashed files listed in the embedded Vite manifest otherwise.
func NewAssets(viteURL string) (*Assets, error) {
	if viteURL != "" {
		u := html.EscapeString(strings.TrimSuffix(viteURL, "/"))
		return &Assets{tags: fmt.Sprintf(
			`<link rel="stylesheet" href="%[1]s/%[2]s">`+
				`<script type="module" src="%[1]s/@vite/client"></script>`+
				`<script type="module" src="%[1]s/%[3]s"></script>`,
			u, styleEntry, scriptEntry,
		)}, nil
	}
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil, err
	}
	return assetsFromManifest(sub)
}

type manifestChunk struct {
	File    string   `json:"file"`
	CSS     []string `json:"css"`
	Imports []string `json:"imports"`
}

func assetsFromManifest(fsys fs.FS) (*Assets, error) {
	data, err := fs.ReadFile(fsys, ".vite/manifest.json")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, errors.New("frontend not built: run `pnpm run build`, or `make dev` for the Vite dev server")
	}
	if err != nil {
		return nil, err
	}
	var manifest map[string]manifestChunk
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse vite manifest: %w", err)
	}
	for _, e := range []string{scriptEntry, styleEntry} {
		if _, ok := manifest[e]; !ok {
			return nil, fmt.Errorf("vite manifest has no entry %q", e)
		}
	}

	// CSS the script's static imports pull in, and the imports to preload.
	var css, preload []string
	seen := map[string]bool{}
	var walk func(key string)
	walk = func(key string) {
		if seen[key] {
			return
		}
		seen[key] = true
		chunk := manifest[key]
		for _, c := range chunk.CSS {
			if !seen["css:"+c] {
				seen["css:"+c] = true
				css = append(css, c)
			}
		}
		for _, imp := range chunk.Imports {
			preload = append(preload, manifest[imp].File)
			walk(imp)
		}
	}
	walk(scriptEntry)

	var b strings.Builder
	style := manifest[styleEntry].File
	fmt.Fprintf(&b, `<link rel="stylesheet" href="/static/%s">`, html.EscapeString(style))
	for _, c := range css {
		if c != style {
			fmt.Fprintf(&b, `<link rel="stylesheet" href="/static/%s">`, html.EscapeString(c))
		}
	}
	fmt.Fprintf(&b, `<script type="module" src="/static/%s"></script>`, html.EscapeString(manifest[scriptEntry].File))
	for _, p := range preload {
		fmt.Fprintf(&b, `<link rel="modulepreload" href="/static/%s">`, html.EscapeString(p))
	}
	return &Assets{tags: b.String()}, nil
}

type assetsKey struct{}

// Middleware makes the asset tags available to layouts through the request
// context.
func (a *Assets) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), assetsKey{}, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AssetTags renders the tags that load the stylesheet and the script. It
// renders nothing outside Assets.Middleware, as in handler tests.
func AssetTags() templ.Component {
	return templ.ComponentFunc(func(ctx context.Context, w io.Writer) error {
		a, ok := ctx.Value(assetsKey{}).(*Assets)
		if !ok {
			return nil
		}
		_, err := io.WriteString(w, a.tags)
		return err
	})
}

// StaticHandler serves the built frontend at /static/assets/. File names
// carry a content hash, so they can be cached forever.
func StaticHandler() http.Handler {
	sub, err := fs.Sub(dist, "dist/assets")
	if err != nil {
		panic(err) // fs.Sub only fails on an invalid path
	}
	files := http.FileServerFS(sub)
	return http.StripPrefix("/static/assets/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "" || strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		files.ServeHTTP(w, r)
	}))
}
