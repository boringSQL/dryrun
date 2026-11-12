package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

const defaultSnapshotStaleAfter = 72 * time.Hour

func snapshotStaleAfter(w io.Writer) time.Duration {
	_, cfg, err := loadProjectConfig()
	if err != nil || cfg.History == nil {
		return defaultSnapshotStaleAfter
	}
	raw := strings.TrimSpace(cfg.History.StaleAfter)
	if raw == "" {
		return defaultSnapshotStaleAfter
	}
	after, err := parseRelative(raw)
	if err != nil {
		fmt.Fprintf(w, "warning: invalid [history] stale_after %q: %v; using %s\n",
			raw, err, fmtLived(defaultSnapshotStaleAfter))
		return defaultSnapshotStaleAfter
	}
	return after
}

// A pull refreshes the payload without moving the snapshots.timestamp column.
func newestSnapshotTime(ctx context.Context, store *history.Store, key history.SnapshotKey, schemaCapturedAt time.Time) time.Time {
	newest := schemaCapturedAt
	spans, err := store.Inventory(ctx, key)
	if err != nil {
		slog.Debug("snapshot freshness: inventory failed", "error", err)
		return newest
	}
	for _, span := range spans {
		for _, at := range []*time.Time{span.Newest, span.LastAttempt} {
			if at != nil && at.After(newest) {
				newest = *at
			}
		}
	}
	return newest
}

func warnIfSnapshotStale(ctx context.Context, w io.Writer, store *history.Store, key history.SnapshotKey, schemaCapturedAt time.Time) {
	if os.Getenv("DRYRUN_NO_STALE_WARNING") != "" {
		return
	}
	after := snapshotStaleAfter(w)
	if after <= 0 {
		return
	}
	newest := newestSnapshotTime(ctx, store, key, schemaCapturedAt)
	now := time.Now().UTC()
	// zero: fixture without a measurement; future: clock skew
	if newest.IsZero() || newest.After(now) {
		return
	}
	if age := now.Sub(newest); age > after {
		fmt.Fprintf(w, "warning: the newest snapshot for %s/%s is %s old (%s); refresh it with 'dryrun snapshot pull' or 'dryrun snapshot capture'\n",
			key.ProjectID, key.DatabaseID, fmtLived(age), newest.UTC().Format("2006-01-02"))
	}
}
