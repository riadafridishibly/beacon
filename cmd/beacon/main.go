// Command beacon is a telemetry server: it stores events from many apps and
// shows them per app.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/riadafridishibly/beacon/internal/apps"
	"github.com/riadafridishibly/beacon/internal/config"
	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/geo"
	"github.com/riadafridishibly/beacon/internal/ingest"
	"github.com/riadafridishibly/beacon/internal/web"
)

const usage = `usage:
  beacon serve                   run the server (the default)
  beacon migrate                 apply database migrations
  beacon app create <id> [name]  add an app and print its ingest key
  beacon app key <id>            print a new ingest key for an app
  beacon app list                list apps`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "beacon:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	cmd := "serve"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		return serve(ctx, cfg)
	case "migrate":
		pool, err := db.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		return db.Migrate(ctx, pool)
	case "app":
		return appCmd(ctx, cfg, args)
	case "help", "-h", "--help":
		fmt.Println(usage)
		return nil
	}
	return fmt.Errorf("unknown command %q\n%s", cmd, usage)
}

func serve(ctx context.Context, cfg config.Config) error {
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	var loc *geo.Locator
	if cfg.GeoIPPath != "" {
		if loc, err = geo.Open(cfg.GeoIPPath); err != nil {
			return fmt.Errorf("open geoip database: %w", err)
		}
		defer loc.Close()
	} else {
		slog.Warn("BEACON_GEOIP_DB is not set; events get no country or city")
	}

	q := db.New(pool)
	mux := http.NewServeMux()
	mux.Handle("POST /v1/events", &ingest.Handler{Apps: apps.NewResolver(q), DB: q, Geo: loc})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           web.Recoverer(web.Logger(web.RealIP(cfg.TrustedProxies)(mux))),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.Listen)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func appCmd(ctx context.Context, cfg config.Config, args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	q := db.New(pool)

	switch {
	case args[0] == "create" && (len(args) == 2 || len(args) == 3):
		name := ""
		if len(args) == 3 {
			name = args[2]
		}
		key, err := apps.Create(ctx, pool, args[1], name)
		if err != nil {
			return err
		}
		fmt.Printf("app %s created\ningest key: %s\n", args[1], key)
		return nil
	case args[0] == "key" && len(args) == 2:
		key, err := apps.NewKey(ctx, q, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("ingest key: %s\n", key)
		return nil
	case args[0] == "list" && len(args) == 1:
		list, err := q.ListApps(ctx)
		if err != nil {
			return err
		}
		for _, a := range list {
			fmt.Printf("%s\t%s\n", a.ID, a.Name)
		}
		return nil
	}
	return errors.New(usage)
}
