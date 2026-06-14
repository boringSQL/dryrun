package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

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
		pool             *pgxpool.Pool
		dbURL            string
		annotated        *schema.AnnotatedSchema
		mu               sync.RWMutex
		history          *history.Store
		snapshotKey      history.SnapshotKey
		lintConfig       lint.Config
		pgmustardClient  *pgmustard.Client
		schemaCandidates []string
		uninitialized    bool
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
	slog.Info("loaded schema from file", "tables", len(snap.Tables), "database", snap.Database)
	return &Server{
		annotated:       &schema.AnnotatedSchema{Schema: snap},
		lintConfig:      lintCfg,
		pgmustardClient: pgmustard.NewClient(""),
	}
}

func (s *Server) SetSchemaCandidates(paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaCandidates = paths
}

func (s *Server) SetUninitialized(paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaCandidates = paths
	s.uninitialized = true
}

// Required before reload_schema can prefer history.db over schema.json
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
	var note string
	switch hist.Compat() {
	case history.CompatLegacy:
		note = "the history database is from an older dryrun and cannot be read; re-run `dryrun init` to recapture its snapshots"
	case history.CompatNewer:
		note = "the history database was written by a newer dryrun; upgrade dryrun"
	default:
		return nil
	}
	return &note
}

func (s *Server) getAnnotated() (*schema.AnnotatedSchema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.annotated == nil || s.annotated.Schema == nil || s.uninitialized {
		return nil, fmt.Errorf("no schema loaded — initialize first:\n\n1. Run `dryrun dump-schema --db <DATABASE_URL>` in a terminal\n2. Call the `reload_schema` tool in this session\n\nThe schema will be picked up without restarting the server.")
	}
	return s.annotated, nil
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

func (s *Server) requirePool() (*pgxpool.Pool, error) {
	if s.pool == nil {
		return nil, fmt.Errorf("this tool requires a live database connection (--db)")
	}
	return s.pool, nil
}

func (s *Server) Instructions() string {
	metaNote := "Each tool response includes _meta.hint (prose) and may include _meta.next: an array of {tool, args} entries that are pre-validated follow-up calls — copy the args verbatim instead of inferring them from the hint."

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
