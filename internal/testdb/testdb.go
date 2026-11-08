// Package testdb gives each test its own PostgreSQL database, copied from a
// migrated template.
package testdb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/db"
)

const envURL = "BEACON_TEST_DATABASE_URL"

var (
	once    sync.Once
	tplName string
	tplErr  error
)

// New returns a pool for a fresh, migrated database, dropped after the test.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin := adminURL(t)

	once.Do(func() { tplName, tplErr = template(ctx, admin) })
	if tplErr != nil {
		t.Fatal(tplErr)
	}
	name := "beacon_test_" + strings.ToLower(rand.Text()[:12])
	if err := exec(ctx, admin, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, tplName)); err != nil {
		t.Fatal(err)
	}
	pool, err := db.Connect(ctx, withDB(admin, name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := exec(ctx, admin, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name)); err != nil {
			t.Error(err)
		}
	})
	return pool
}

// template returns a migrated database to copy, named by a hash of the
// migrations so a changed migration gets a new one. Test binaries for
// different packages run at once, so creation holds an advisory lock.
func template(ctx context.Context, admin string) (string, error) {
	h := sha256.New()
	files, err := fs.Glob(db.MigrationsFS, "migrations/*.sql")
	if err != nil {
		return "", err
	}
	for _, f := range files {
		b, err := fs.ReadFile(db.MigrationsFS, f)
		if err != nil {
			return "", err
		}
		h.Write([]byte(f))
		h.Write(b)
	}
	name := "beacon_tpl_" + hex.EncodeToString(h.Sum(nil))[:12]

	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext('beacon_testdb'))"); err != nil {
		return "", err
	}
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists); err != nil {
		return "", err
	}
	if exists {
		return name, nil
	}

	building := name + "_tmp"
	if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+building); err != nil {
		return "", err
	}
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+building); err != nil {
		return "", err
	}
	pool, err := db.Connect(ctx, withDB(admin, building))
	if err != nil {
		return "", err
	}
	err = db.Migrate(ctx, pool)
	pool.Close()
	if err != nil {
		return "", err
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", building, name)); err != nil {
		return "", err
	}
	return name, nil
}

func adminURL(t testing.TB) string {
	if u := os.Getenv(envURL); u != "" {
		return u
	}
	return "postgres://localhost:5432/postgres?sslmode=disable"
}

func withDB(base, name string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

func exec(ctx context.Context, url, sql string) error {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, sql)
	return err
}
