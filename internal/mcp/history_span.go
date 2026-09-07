package mcp

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	// The counts move only when a capture lands, and adoptNewerSnapshot
	// invalidates on exactly that, so this is a backstop for the streams a
	// schema adoption does not cover rather than a polling interval.
	historySpanTTL = 5 * time.Minute
)

type (
	// throttles the aggregate reads behind _meta.history
	historyInventory struct {
		mu     sync.Mutex
		readAt time.Time
		spans  []historySpan
		// the read failed, as against there being no store to read: absence is
		// already spoken for, and it means something stronger
		failed bool
	}
)

// invalidate forces the next read, called when a snapshot is adopted. The
// mutex is a leaf -- historySpans takes no other lock while holding it -- so
// this is safe under freshness.mu.
func (h *historyInventory) invalidate() {
	h.mu.Lock()
	h.readAt = time.Time{}
	h.mu.Unlock()
}

// How much history stands behind this answer, and whether the question could be
// asked at all. The tools serve one snapshot, so without this an agent cannot
// tell a store holding three months from one holding this morning's first
// capture -- and the only way to find out was to shell out to
// `dryrun snapshot list`.
//
// The returned slice is shared with every concurrent caller: read it, never
// append to it.
func (s *Server) historySpans() ([]historySpan, bool) {
	hist, key := s.historyKey()
	// a history.db this build cannot read is not a source to count from
	if hist == nil || s.historyNote() != nil {
		return nil, true
	}

	// Read OUTSIDE the lock. getSchema runs the freshness check, which takes
	// freshness.mu and, on an adopt, invalidates this cache -- so calling it
	// under inventory.mu closes a cycle, and re-enters this non-reentrant
	// mutex on the single-goroutine path. inventory.mu stays a leaf.
	served := s.databaseName()

	s.inventory.mu.Lock()
	defer s.inventory.mu.Unlock()
	if !s.inventory.readAt.IsZero() && time.Since(s.inventory.readAt) < historySpanTTL {
		return s.inventory.spans, !s.inventory.failed
	}
	// shorter than the store's busy_timeout: the next call retries a contended read
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.inventory.spans, s.inventory.failed = nil, false
	// stamped after the read, so a slow one does not shorten the window it
	// bought; whatever the outcome, so a store that keeps failing is read once
	// per TTL rather than once per tool call
	defer func() { s.inventory.readAt = time.Now() }()

	// key and --db url resolve independently, so history can hold another
	// database entirely -- the same guard adoptNewerSnapshot applies before it
	// serves that bundle. Counting its rows beside this database's answer would
	// state another database's history as this one's.
	stored, err := hist.LatestDatabaseName(ctx, key)
	if err != nil {
		slog.Warn("cannot identify the history database", "error", err)
		s.inventory.failed = true
		return nil, false
	}
	// An unidentifiable store -- stats rows but no schema row -- is counted
	// rather than suppressed: narrow, since every capture path writes one.
	if stored != "" && served != "" && stored != served {
		slog.Debug("history holds another database; no inventory reported",
			"served", served, "history", stored)
		return nil, true
	}

	streams, err := hist.Inventory(ctx, key)
	if err != nil {
		slog.Warn("cannot read the history inventory", "error", err)
		s.inventory.failed = true
		return nil, false
	}
	spans := make([]historySpan, 0, len(streams))
	for _, st := range streams {
		spans = append(spans, historySpan{
			Stream:      st.Stream,
			Rows:        st.Rows,
			Oldest:      stampPtr(st.Oldest),
			Newest:      stampPtr(st.Newest),
			LastAttempt: stampPtr(st.LastAttempt),
			Nodes:       st.Nodes,
			CorruptRows: st.CorruptRows,
		})
	}
	s.inventory.spans = spans
	return spans, true
}

// An unknown instant is an absent field, never the zero date.
func stampPtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return stamp(*t)
}
