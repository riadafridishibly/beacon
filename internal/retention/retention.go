// Package retention deletes events older than the retention window.
package retention

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
)

const batchSize = 5000

// Purge deletes every app's events with a ts before cutoff and returns how
// many it deleted. An app that fails doesn't stop the others.
func Purge(ctx context.Context, q *db.Queries, cutoff time.Time) (int64, error) {
	apps, err := q.ListApps(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	var errs []error
	for _, a := range apps {
		n, err := purgeApp(ctx, q, a.ID, cutoff)
		total += n
		if err != nil {
			if ctx.Err() != nil {
				return total, err
			}
			errs = append(errs, fmt.Errorf("app %s: %w", a.ID, err))
		}
	}
	return total, errors.Join(errs...)
}

func purgeApp(ctx context.Context, q *db.Queries, appID string, cutoff time.Time) (int64, error) {
	var total int64
	for {
		n, err := q.DeleteEventsBefore(ctx, db.DeleteEventsBeforeParams{
			AppID: appID, Before: cutoff, BatchSize: batchSize,
		})
		total += n
		if err != nil || n < batchSize {
			return total, err
		}
	}
}

// Run purges events older than keep now and then once a day, until ctx is
// done.
func Run(ctx context.Context, q *db.Queries, keep time.Duration) {
	for {
		start := time.Now()
		n, err := Purge(ctx, q, start.Add(-keep))
		switch {
		case err != nil && ctx.Err() == nil:
			slog.ErrorContext(ctx, "delete old events", "deleted", n, "err", err)
		case n > 0:
			slog.InfoContext(ctx, "deleted old events", "events", n, "took", time.Since(start).Round(time.Millisecond))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(24 * time.Hour):
		}
	}
}
