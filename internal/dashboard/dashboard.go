// Package dashboard serves the web UI: sign-in, the app list, creating apps,
// and each app's overview and events.
package dashboard

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/web"
)

const maxNameLen = 100

// Config is how New builds the dashboard.
type Config struct {
	// ViteURL is the Vite dev server origin. Empty serves the embedded build.
	ViteURL string
	// Now is the current time; nil means time.Now.
	Now func() time.Time
}

// New returns the dashboard handler, with the login page, session checks,
// the CSP and cross-origin protection for its forms.
func New(pool *pgxpool.Pool, cfg Config) (http.Handler, error) {
	assets, err := web.NewAssets(cfg.ViteURL)
	if err != nil {
		return nil, err
	}
	q := db.New(pool)
	d := &dashboard{pool: pool, q: q, nowFunc: cfg.Now}
	a := auth.New(q)

	pages := http.NewServeMux()
	pages.Handle("GET /{$}", web.Handle(d.appList))
	pages.Handle("GET /new-app", web.Handle(d.newApp))
	pages.Handle("GET /apps/{id}", web.Handle(d.overview))
	pages.Handle("GET /apps/{id}/events", web.Handle(d.events))
	pages.Handle("POST /apps", web.Handle(d.createApp))
	pages.Handle("/", web.Handle(func(http.ResponseWriter, *http.Request) error { return web.ErrNotFound }))

	mux := http.NewServeMux()
	mux.Handle("GET /static/assets/", web.StaticHandler())
	a.Routes(mux)
	mux.Handle("/", a.RequireLogin(pages))

	var h http.Handler = mux
	h = assets.Middleware(h)
	h = http.NewCrossOriginProtection().Handler(h)
	h = web.SecureHeaders(cfg.ViteURL)(h)
	return h, nil
}

type dashboard struct {
	pool    *pgxpool.Pool
	q       *db.Queries
	nowFunc func() time.Time
}

func (d *dashboard) now() time.Time {
	if d.nowFunc != nil {
		return d.nowFunc()
	}
	return time.Now()
}

// app returns web.ErrNotFound for an unknown id, including one PostgreSQL
// couldn't accept as text.
func (d *dashboard) app(ctx context.Context, id string) (db.App, error) {
	if !apps.ValidID(id) {
		return db.App{}, web.ErrNotFound
	}
	app, err := d.q.GetApp(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.App{}, web.ErrNotFound
	}
	return app, err
}

func (d *dashboard) appList(w http.ResponseWriter, r *http.Request) error {
	list, err := d.q.AppSummaries(r.Context(), d.now().Add(-24*time.Hour))
	if err != nil {
		return err
	}
	return web.Render(w, r, http.StatusOK, appListPage(auth.Nav(r.Context()), list))
}

func (d *dashboard) newApp(w http.ResponseWriter, r *http.Request) error {
	return web.Render(w, r, http.StatusOK, newAppPage(auth.Nav(r.Context()), newAppForm{}))
}

func (d *dashboard) createApp(w http.ResponseWriter, r *http.Request) error {
	f := newAppForm{
		ID:   strings.TrimSpace(r.PostFormValue("id")),
		Name: strings.TrimSpace(r.PostFormValue("name")),
	}
	if utf8.RuneCountInString(f.Name) > maxNameLen {
		f.NameError = "Use at most 100 characters."
		return web.Render(w, r, http.StatusUnprocessableEntity, newAppPage(auth.Nav(r.Context()), f))
	}
	key, err := apps.Create(r.Context(), d.pool, f.ID, f.Name)
	switch {
	case errors.Is(err, apps.ErrBadID):
		f.IDError = "Use lowercase letters, digits and dashes, starting with a letter or digit."
	case errors.Is(err, apps.ErrExists):
		f.IDError = "An app with this id already exists."
	case err != nil:
		return err
	}
	if f.IDError != "" {
		return web.Render(w, r, http.StatusUnprocessableEntity, newAppPage(auth.Nav(r.Context()), f))
	}
	// The key is shown in this response and nowhere else, so the page is
	// rendered here rather than after a redirect, and kept out of caches.
	w.Header().Set("Cache-Control", "no-store")
	return web.Render(w, r, http.StatusCreated, appCreatedPage(auth.Nav(r.Context()), f.ID, key))
}
