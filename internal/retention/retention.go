// Package retention deletes events older than the retention window.
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
)

const batchSize = 5000

// Purge deletes every app's events with a ts before cutoff and returns how
// many it deleted.
func Purge(ctx context.Context, q *db.Queries, cutoff time.Time) (int64, error) {
	apps, err := q.ListApps(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, a := range apps {
		for {
			n, err := q.DeleteEventsBefore(ctx, db.DeleteEventsBeforeParams{
				AppID: a.ID, Before: cutoff, BatchSize: batchSize,
			})
			total += n
			if err != nil {
				return total, fmt.Errorf("app %s: %w", a.ID, err)
			}
			if n < batchSize {
				break
			}
		}
	}
	return total, nil
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
