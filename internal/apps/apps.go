// Package apps manages the applications that send events and their ingest
// keys.
package apps

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/db"
)

const (
	keyPrefix = "bk_"
	// shownChars is how much of the random part of a key is stored as its
	// prefix.
	shownChars = 6
	cacheTTL   = time.Minute
	cacheMax   = 10_000
)

var (
	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	prefixPattern = regexp.MustCompile(fmt.Sprintf(`^%s[A-Z2-7]{%d}$`, keyPrefix, shownChars))
)

// ErrBadID is returned for an app id that isn't lowercase letters, digits
// and dashes.
var ErrBadID = errors.New("app id must be 1-63 lowercase letters, digits or dashes, starting with a letter or digit")

// ErrExists is returned when creating an app whose id is taken.
var ErrExists = errors.New("an app with that id already exists")

// ErrNotFound is returned for an app id that matches no app.
var ErrNotFound = errors.New("no app has that id")

// ErrKeyNotFound is returned for a key prefix that matches no ingest key.
var ErrKeyNotFound = errors.New("no ingest key has that prefix")

// ValidID reports whether id is a well-formed app id.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// Create adds an app with its first ingest key and returns the key.
func Create(ctx context.Context, pool *pgxpool.Pool, id, name string) (string, error) {
	if !ValidID(id) {
		return "", ErrBadID
	}
	if name == "" {
		name = id
	}
	var key string
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		_, err := q.CreateApp(ctx, db.CreateAppParams{ID: id, Name: name})
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
			return ErrExists
		}
		if err != nil {
			return fmt.Errorf("create app: %w", err)
		}
		key, err = addKey(ctx, q, id)
		return err
	})
	return key, err
}

// NewKey adds an ingest key to an app and returns it. Older keys keep
// working.
func NewKey(ctx context.Context, q *db.Queries, appID string) (string, error) {
	return addKey(ctx, q, appID)
}

func addKey(ctx context.Context, q *db.Queries, appID string) (string, error) {
	key := keyPrefix + rand.Text()
	_, err := q.CreateAppKey(ctx, db.CreateAppKeyParams{
		AppID:   appID,
		KeyHash: hashKey(key),
		Prefix:  key[:len(keyPrefix)+shownChars],
	})
	if err != nil {
		return "", fmt.Errorf("create key: %w", err)
	}
	return key, nil
}

// Revoke stops accepting the key with the given prefix and returns its app
// id. Revoking a key twice keeps the first revocation time.
func Revoke(ctx context.Context, q *db.Queries, prefix string) (string, error) {
	if !prefixPattern.MatchString(prefix) {
		return "", ErrKeyNotFound
	}
	appID, err := q.RevokeKey(ctx, prefix)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("revoke key: %w", err)
	}
	return appID, nil
}

// Delete removes an app with its ingest keys, views and events.
func Delete(ctx context.Context, q *db.Queries, id string) error {
	if !ValidID(id) {
		return ErrNotFound
	}
	n, err := q.DeleteApp(ctx, id)
	if err != nil {
		return fmt.Errorf("delete app: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func hashKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// Resolver maps ingest keys to app ids. It caches answers, including
// unknown keys, for a minute, so a revoked key can work for up to a minute.
type Resolver struct {
	q     *db.Queries
	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	appID   string
	expires time.Time
}

func NewResolver(q *db.Queries) *Resolver {
	return &Resolver{q: q, cache: map[string]cached{}}
}

// App returns the app id for key, or "" for an unknown or revoked key.
func (r *Resolver) App(ctx context.Context, key string) (string, error) {
	h := hashKey(key)
	now := time.Now()
	r.mu.Lock()
	c, ok := r.cache[string(h)]
	r.mu.Unlock()
	if ok && now.Before(c.expires) {
		return c.appID, nil
	}

	appID, err := r.q.AppForKey(ctx, h)
	if errors.Is(err, pgx.ErrNoRows) {
		appID, err = "", nil
	}
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	if len(r.cache) >= cacheMax {
		clear(r.cache)
	}
	r.cache[string(h)] = cached{appID: appID, expires: now.Add(cacheTTL)}
	r.mu.Unlock()
	return appID, nil
}
