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
	"github.com/riadafridishibly/beacon/internal/auth"
	"github.com/riadafridishibly/beacon/internal/config"
	"github.com/riadafridishibly/beacon/internal/dashboard"
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
  beacon app list                list apps and their ingest keys
  beacon app revoke <key prefix> stop accepting an ingest key
  beacon user create <email>     add a dashboard account and print its password
  beacon user reset <email>      print a new password and sign the account out`

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
	case "user":
		return userCmd(ctx, cfg, args)
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

	if len(cfg.TrustedProxies) == 0 {
		slog.Warn("BEACON_TRUSTED_PROXIES is not set; behind a reverse proxy, all clients share one rate limit and get no country or city")
	}

	dashPool, err := db.ConnectDashboard(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer dashPool.Close()
	dash, err := dashboard.New(dashPool, dashboard.Config{ViteURL: cfg.ViteURL})
	if err != nil {
		return err
	}

	q := db.New(pool)
	mux := http.NewServeMux()
	mux.Handle("/", dash)
	mux.Handle("POST /v1/events", &ingest.Handler{
		Apps: apps.NewResolver(q),
		DB:   q,
		Geo:  loc,
		// The default of one request a second covers about 30 devices
		// behind one NAT, at the two batches a minute a client sends.
		ByIP: web.NewLimiter(time.Second/time.Duration(cfg.IngestIPRate), 60*cfg.IngestIPRate),
		// Every install of an app shares its key, so this limit grows with
		// the number of installs.
		ByKey: web.NewLimiter(time.Second/time.Duration(cfg.IngestKeyRate), 10*cfg.IngestKeyRate),
	})
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
			keys, err := q.ListAppKeys(ctx, a.ID)
			if err != nil {
				return err
			}
			for _, k := range keys {
				status := "active"
				if k.RevokedAt != nil {
					status = "revoked " + k.RevokedAt.UTC().Format(time.DateTime) + " UTC"
				}
				fmt.Printf("  %s\t%s\n", k.Prefix, status)
			}
		}
		return nil
	case args[0] == "revoke" && len(args) == 2:
		appID, err := apps.Revoke(ctx, q, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("revoked %s of app %s; a running server can accept it for up to a minute\n", args[1], appID)
		return nil
	}
	return errors.New(usage)
}

func userCmd(ctx context.Context, cfg config.Config, args []string) error {
	if len(args) != 2 || (args[0] != "create" && args[0] != "reset") {
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
	var password string
	if args[0] == "create" {
		password, err = auth.CreateAccount(ctx, db.New(pool), args[1])
	} else {
		password, err = auth.ResetPassword(ctx, pool, args[1])
	}
	if err != nil {
		return err
	}
	fmt.Printf("password: %s\n", password)
	return nil
}
