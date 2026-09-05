package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/buildinfo"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/pgmustard"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

type (
	Server struct {
		pool            *pgxpool.Pool
		dbURL           string
		annotated       *schema.AnnotatedSchema
		mu              sync.RWMutex
		history         *history.Store
		snapshotKey     history.SnapshotKey
		lintConfig      lint.Config
		pgmustardClient *pgmustard.Client
		uninitialized   bool
		freshness       freshness
	}
)

func NewServer(pool *pgxpool.Pool, dbURL string, snap *schema.SchemaSnapshot, hist *history.Store, lintCfg lint.Config, pgMustardAPIKey string) *Server {
	return &Server{
		pool:            pool,
		dbURL:           dbURL,
		annotated:       &schema.AnnotatedSchema{Schema: snap},
		history:         hist,
		lintConfig:      lintCfg,
		pgmustardClient: pgmustard.NewClient(pgMustardAPIKey),
	}
}

func NewOfflineServer(snap *schema.SchemaSnapshot, lintCfg lint.Config) *Server {
	slog.Debug("offline server", "tables", len(snap.Tables), "database", snap.Database)
	return NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: snap}, lintCfg)
}

// NewOfflineServerAnnotated builds an offline server from a pre-assembled
// AnnotatedSchema (schema + planner + activity), so a hosted caller can supply
// the join from blobs instead of a local history.db.
func NewOfflineServerAnnotated(a *schema.AnnotatedSchema, lintCfg lint.Config) *Server {
	if a == nil {
		a = &schema.AnnotatedSchema{}
	}
	return &Server{
		annotated:       a,
		lintConfig:      lintCfg,
		pgmustardClient: pgmustard.NewClient(""),
	}
}

func (s *Server) SetUninitialized() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uninitialized = true
}

func (s *Server) SetSnapshotKey(key history.SnapshotKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotKey = key
}

// Lets offline mode wire in history.db after construction
func (s *Server) SetHistory(hist *history.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = hist
}

func (s *Server) BootstrapFromHistory(ctx context.Context) bool {
	a, ok := s.loadAnnotatedFromHistory(ctx)
	if !ok || a == nil || a.Schema == nil {
		return false
	}
	s.mu.Lock()
	s.annotated = a
	s.uninitialized = false
	s.mu.Unlock()
	return true
}

// SchemaCounts reports the loaded snapshot's size; zeros when uninitialized.
func (s *Server) SchemaCounts() (tables, views, functions int) {
	snap, err := s.getSchema()
	if err != nil || snap == nil {
		return 0, 0, 0
	}
	return len(snap.Tables), len(snap.Views), len(snap.Functions)
}

func (s *Server) loadAnnotatedFromHistory(ctx context.Context) (*schema.AnnotatedSchema, bool) {
	s.mu.RLock()
	hist := s.history
	key := s.snapshotKey
	s.mu.RUnlock()
	if hist == nil || key.ProjectID == "" {
		return nil, false
	}
	a, err := hist.GetAnnotated(ctx, key, history.NewRefLatest())
	if err != nil {
		slog.Debug("history.GetAnnotated miss", "error", err)
		return nil, false
	}
	return a, true
}

// describes a history.db compat problem for the user, or nil if fine
func (s *Server) historyNote() *string {
	s.mu.RLock()
	hist := s.history
	s.mu.RUnlock()
	if hist == nil {
		return nil
	}
	if hist.Compat() == history.CompatNewer {
		note := "the history database was written by a newer dryrun; upgrade dryrun"
		return &note
	}
	return nil
}

func (s *Server) getAnnotated() (*schema.AnnotatedSchema, error) {
	s.adoptNewerSnapshot(context.Background())

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.annotated == nil || s.annotated.Schema == nil || s.uninitialized {
		return nil, errors.New(s.noSchemaMsg())
	}
	return s.annotated, nil
}

// Takes no lock of its own: reads snapshotKey under the caller's s.mu.
func (s *Server) noSchemaMsg() string {
	msg := "no schema loaded — initialize first: run `dryrun init --db <DATABASE_URL>` (or `dryrun snapshot take`) in a terminal, then retry. The snapshot is picked up without restarting the server"
	if s.snapshotKey.ProjectID != "" {
		// an empty history and a project/database mismatch fail identically
		msg += fmt.Sprintf(" (looked under project=%s database=%s)", s.snapshotKey.ProjectID, s.snapshotKey.DatabaseID)
	}
	return msg
}

func (s *Server) getSchema() (*schema.SchemaSnapshot, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return nil, err
	}
	return a.Schema, nil
}

func (s *Server) modeStr() string {
	if s.pool != nil {
		return "live"
	}
	return "offline"
}

func (s *Server) pgDisplay() string {
	snap, err := s.getSchema()
	if err != nil || snap.PgVersion == "" {
		return ""
	}
	if v, err := dryrun.ParsePgVersion(snap.PgVersion); err == nil {
		return v.String()
	}
	return snap.PgVersion
}

func (s *Server) databaseName() string {
	snap, err := s.getSchema()
	if err != nil {
		return ""
	}
	return snap.Database
}

type (
	// DDL, planner stats and activity reach history.db as separate rows on
	// separate schedules, so each carries its own capture time.
	captureStamps struct {
		schema   string
		planner  string
		activity string
		// which node is the laggard: "three weeks stale" is not actionable
		// without knowing whose capture is old
		activityNode string
	}
)

// When each part of the snapshot was taken, so an agent can judge staleness
// itself. No age and no threshold: dryrun does not know what stale means for a
// given database.
func (s *Server) captureTimes() captureStamps {
	a, err := s.getAnnotated()
	if err != nil {
		return captureStamps{}
	}

	var c captureStamps
	if a.Schema != nil {
		c.schema = stamp(a.Schema.Timestamp)
	}
	if a.Planner != nil {
		c.planner = stamp(a.Planner.Timestamp)
	}
	if a.Merged != nil {
		// the oldest node, not the newest: a fresh primary beside a three-week
		// replica is three weeks stale for anything read across nodes
		var oldest time.Time
		for _, n := range a.Merged.Nodes {
			if n.Node.Timestamp.IsZero() {
				continue
			}
			if oldest.IsZero() || n.Node.Timestamp.Before(oldest) {
				oldest, c.activityNode = n.Node.Timestamp, n.Node.Source
			}
		}
		c.activity = stamp(oldest)
	}
	return c
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func (c captureStamps) fields() map[string]any {
	out := map[string]any{}
	for k, v := range map[string]string{
		"schema_captured_at":   c.schema,
		"planner_captured_at":  c.planner,
		"activity_captured_at": c.activity,
		"activity_oldest_node": c.activityNode,
	} {
		if v != "" {
			out[k] = v
		}
	}
	return out
}

// Thin clients read only the text body, so the header carries them too.
func (c captureStamps) suffix() string {
	var parts []string
	for _, p := range []struct{ label, at string }{
		{"schema", c.schema}, {"planner", c.planner}, {"activity", c.activity},
	} {
		if p.at == "" {
			continue
		}
		part := p.label + " " + p.at
		if p.label == "activity" && c.activityNode != "" {
			part += " (" + c.activityNode + ")"
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return ""
	}
	return " | captured: " + strings.Join(parts, ", ")
}

func (s *Server) requirePool() (*pgxpool.Pool, error) {
	if s.pool == nil {
		return nil, fmt.Errorf("this tool requires a live database connection (--db)")
	}
	return s.pool, nil
}

func (s *Server) Instructions() string {
	metaNote := "Each tool response includes _meta.hint (prose) and may include _meta.next: an array of {tool, args} entries that are pre-validated follow-up calls — copy the args verbatim instead of inferring them from the hint. _meta.schema_captured_at, _meta.planner_captured_at and _meta.activity_captured_at say when each part of the snapshot was taken -- DDL, planner stats and per-node counters are captured by separate commands, so they age at different rates. They describe the snapshot, not a live read. Activity is the oldest node, named in _meta.activity_oldest_node. A snapshot taken while this server runs (`dryrun snapshot take`, `snapshot pull`) is picked up on the next tool call; nothing needs reloading."

	// surface a history.db compat problem here so MCP-only users see it
	if note := s.historyNote(); note != nil {
		metaNote += "\n\nWarning: " + *note + "."
	}

	dv := buildinfo.Get()

	snap, err := s.getSchema()
	if err != nil || snap.PgVersion == "" {
		return fmt.Sprintf("dryrun %s, PostgreSQL schema advisor. No schema loaded yet.\n\n%s", dv, metaNote)
	}

	ver, err := dryrun.ParsePgVersion(snap.PgVersion)
	if err != nil {
		return fmt.Sprintf("dryrun %s, PostgreSQL schema advisor. Database: %s\n\n%s", dv, snap.Database, metaNote)
	}

	return fmt.Sprintf("dryrun %s, PostgreSQL schema advisor. PostgreSQL %s; database: %s\n\n%s", dv, ver, snap.Database, metaNote)
}
