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
	// whether history.db holds a schema snapshot newer than the one served
	freshness struct {
		mu        sync.Mutex
		checkedAt time.Time
		servedAt  string
		newerAt   string
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

// newerSnapshotAt returns the RFC3339 timestamp of the newest history.db
// snapshot when it is newer than the one served, else "". Offline only: with
// --db the served schema shares the database clock, so it is never behind.
func (s *Server) newerSnapshotAt() string {
	s.mu.RLock()
	live := s.pool != nil
	s.mu.RUnlock()
	if live {
		return ""
	}

	hist, key := s.historyKey()
	if hist == nil {
		return ""
	}
	loaded, err := s.getSchema()
	if err != nil || loaded == nil || loaded.Timestamp.IsZero() {
		return ""
	}

	// keyed on what is served, so reload_schema clears the answer
	servedAt := loaded.Timestamp.UTC().Format(time.RFC3339Nano)

	s.freshness.mu.Lock()
	defer s.freshness.mu.Unlock()
	if s.freshness.servedAt == servedAt && time.Since(s.freshness.checkedAt) < freshnessTTL {
		return s.freshness.newerAt
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	newerAt := ""
	if latest, err := hist.LatestSchema(ctx, key); err == nil && latest != nil {
		// A -> B -> A stores a content twin; a later timestamp alone would
		// send the agent to reload the snapshot it already has
		if latest.ContentHash != loaded.ContentHash && latest.Timestamp.After(loaded.Timestamp) {
			newerAt = latest.Timestamp.UTC().Format(time.RFC3339)
		}
	} else if err != nil {
		slog.Debug("history.LatestSchema miss", "error", err)
	}

	s.freshness.checkedAt = time.Now()
	s.freshness.servedAt = servedAt
	s.freshness.newerAt = newerAt
	return newerAt
}
