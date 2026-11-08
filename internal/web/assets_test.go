package web

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestAssetsFromManifest(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{".vite/manifest.json": {Data: []byte(`{
		"src/app.css": {"file": "assets/app-000.css", "isEntry": true},
		"src/main.ts": {"file": "assets/main-abc.js", "isEntry": true, "css": ["assets/main-abc.css"], "imports": ["_shared.js"]},
		"_shared.js": {"file": "assets/shared-def.js", "css": ["assets/shared-def.css"]}
	}`)}}

	a, err := assetsFromManifest(fsys)
	if err != nil {
		t.Fatal(err)
	}
	want := `<link rel="stylesheet" href="/static/assets/app-000.css">` +
		`<link rel="stylesheet" href="/static/assets/main-abc.css">` +
		`<link rel="stylesheet" href="/static/assets/shared-def.css">` +
		`<script type="module" src="/static/assets/main-abc.js"></script>` +
		`<link rel="modulepreload" href="/static/assets/shared-def.js">`
	if a.tags != want {
		t.Errorf("tags:\n got %s\nwant %s", a.tags, want)
	}
}

func TestAssetsFromManifestMissing(t *testing.T) {
	t.Parallel()
	_, err := assetsFromManifest(fstest.MapFS{})
	if err == nil || !strings.Contains(err.Error(), "pnpm run build") {
		t.Errorf("err = %v, want hint to run pnpm run build", err)
	}
}

func TestDevAssets(t *testing.T) {
	t.Parallel()
	a, err := NewAssets("http://localhost:8341/")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a.tags, `<link rel="stylesheet" href="http://localhost:8341/src/app.css">`) ||
		!strings.Contains(a.tags, `src="http://localhost:8341/@vite/client"`) ||
		!strings.Contains(a.tags, `src="http://localhost:8341/src/main.ts"`) {
		t.Errorf("dev tags = %s", a.tags)
	}
}
