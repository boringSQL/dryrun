package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

const (
	// rides every tool call, so it is answered from cache between checks
	freshnessTTL = 20 * time.Second

	baselineHistory = "history"
	baselineLoaded  = "loaded_schema"
)

type (
	// throttles the history.db read that picks up a newer snapshot
	freshness struct {
		mu        sync.Mutex
		checkedAt time.Time
		servedAt  time.Time
	}
)

// driftBaseline answers drift against what history.db holds for this key, not
// whatever this process is serving.
func (s *Server) driftBaseline(ctx context.Context) (*schema.SchemaSnapshot, string, error) {
	hist, key := s.historyKey()
	if hist != nil {
		if note := s.historyNote(); note != nil {
			return nil, "", errors.New(*note)
		}
		snap, err := hist.GetSchema(ctx, key, history.NewRefLatest())
		switch {
		case err == nil && snap != nil:
			return snap, baselineHistory, nil
		case err != nil && !errors.Is(err, history.ErrSnapshotNotFound):
			// falling back would measure drift against the baseline itself
			return nil, "", fmt.Errorf("cannot read the stored snapshot: %w", err)
		}
		slog.Debug("no stored snapshot for key", "project", key.ProjectID, "database", key.DatabaseID)
	}

	snap, err := s.getSchema()
	if err != nil {
		return nil, "", err
	}
	return snap, baselineLoaded, nil
}

func (s *Server) historyKey() (*history.Store, history.SnapshotKey) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.history == nil || s.snapshotKey.ProjectID == "" {
		return nil, history.SnapshotKey{}
	}
	return s.history, s.snapshotKey
}

// adoptNewerSnapshot swaps in the newest history.db bundle when any stream
// (schema, planner, activity, query stats) advanced. Throttled by freshnessTTL.
func (s *Server) adoptNewerSnapshot(ctx context.Context) {
	hist, key := s.historyKey()
	// a history.db this build cannot read is not a source to serve from
	if hist == nil || s.historyNote() != nil {
		return
	}

	// lock order: freshness.mu before s.mu, never the reverse
	s.freshness.mu.Lock()
	defer s.freshness.mu.Unlock()

	served := s.servedBundle()
	var servedAt time.Time
	if served != nil {
		servedAt = served.Schema.Timestamp
	}
	if s.freshness.servedAt.Equal(servedAt) && !s.freshness.checkedAt.IsZero() && time.Since(s.freshness.checkedAt) < freshnessTTL {
		return
	}
	s.freshness.checkedAt = time.Now()
	s.freshness.servedAt = servedAt

	// shorter than the store's busy_timeout: the next tick retries a contended read
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	a, err := hist.GetAnnotated(ctx, key, history.NewRefLatest())
	if err != nil || a == nil || a.Schema == nil {
		if err != nil && !errors.Is(err, history.ErrSnapshotNotFound) {
			slog.Warn("cannot read the newest snapshot from history", "error", err)
		}
		return
	}
	if served != nil {
		if !bundleAdvanced(a, served) {
			// retry a failed pendingStamps computation: a stats-free bundle
			// never advances again. Gate: served bundle must be history's latest.
			if a.Schema.ContentHash == served.Schema.ContentHash {
				s.mu.Lock()
				s.pendingStamps = pendingReschemaStamps(ctx, hist, key, served)
				s.mu.Unlock()
			}
			return
		}
		// key and --db url resolve independently; history can hold another database entirely
		if served.Schema.Database != "" && a.Schema.Database != "" && a.Schema.Database != served.Schema.Database {
			return
		}
	}

	// outside s.mu: freshness.mu serializes adopts; the reads must not stall tool calls
	pending := pendingReschemaStamps(ctx, hist, key, a)

	s.mu.Lock()
	s.annotated = a
	s.pendingStamps = pending
	s.uninitialized = false
	s.mu.Unlock()
	s.freshness.servedAt = a.Schema.Timestamp
	slog.Info("picked up a newer snapshot from history",
		"captured_at", a.Schema.Timestamp.UTC().Format(time.RFC3339), "content_hash", a.Schema.ContentHash)
}

// the prior-hash captures _meta shows beside stats_pending_reschema; a failed
// lookup is no stats, never a failed adoption — the freshness tick retries
func pendingReschemaStamps(ctx context.Context, hist *history.Store, key history.SnapshotKey, a *schema.AnnotatedSchema) captureStamps {
	var c captureStamps
	if a == nil || (a.Planner != nil && a.Merged != nil) {
		return c
	}
	if a.Planner == nil {
		if p, err := hist.LatestPlanner(ctx, key); err == nil && p != nil {
			c.planner = stamp(p.Timestamp)
			c.pendingReschema = true
		}
	}
	if a.Merged == nil {
		acts, err := hist.LatestActivity(ctx, key)
		if err == nil && len(acts) > 0 {
			nodes := make([]schema.NodeActivity, len(acts))
			for i := range acts {
				nodes[i] = schema.NodeActivity{Node: acts[i].Node}
			}
			if oldest, source := oldestActivityStamp(nodes); !oldest.IsZero() {
				c.activity, c.activityNode = stamp(oldest), source
				c.pendingReschema = true
			}
		}
	}
	return c
}

// a content twin (A -> B -> A) is not newer; fresh stats under an unchanged schema are
func bundleAdvanced(cand, served *schema.AnnotatedSchema) bool {
	if cand.Schema.ContentHash != served.Schema.ContentHash {
		// with --db the served schema is stamped now; stats alone must not revert to older DDL
		if cand.Schema.Timestamp.Before(served.Schema.Timestamp) {
			return false
		}
		if cand.Schema.Timestamp.After(served.Schema.Timestamp) {
			return true
		}
	}
	if plannerAt(cand).After(plannerAt(served)) {
		return true
	}
	return statsAt(cand).After(statsAt(served))
}

func plannerAt(a *schema.AnnotatedSchema) time.Time {
	if a == nil || a.Planner == nil {
		return time.Time{}
	}
	return a.Planner.Timestamp
}

// newest capture across activity nodes and query stats
func statsAt(a *schema.AnnotatedSchema) time.Time {
	var newest time.Time
	if a == nil {
		return newest
	}
	if a.Merged != nil {
		for _, n := range a.Merged.Nodes {
			if n.Node.Timestamp.After(newest) {
				newest = n.Node.Timestamp
			}
		}
	}
	for _, q := range a.QueryStats {
		if q.Node.Timestamp.After(newest) {
			newest = q.Node.Timestamp
		}
	}
	return newest
}

// what is loaded, without triggering a freshness check
func (s *Server) servedBundle() *schema.AnnotatedSchema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uninitialized || s.annotated == nil || s.annotated.Schema == nil || s.annotated.Schema.Timestamp.IsZero() {
		return nil
	}
	return s.annotated
}
