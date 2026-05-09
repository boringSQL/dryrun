package mcp

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/pgmustard"
	"github.com/boringsql/dryrun/internal/schema"
)

type (
	Server struct {
		pool             *pgxpool.Pool
		dbURL            string
		snap             *schema.SchemaSnapshot
		mu               sync.RWMutex
		history          *history.Store
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
		snap:            snap,
		history:         hist,
		lintConfig:      lintCfg,
		pgmustardClient: pgmustard.NewClient(pgMustardAPIKey),
	}
}

func NewOfflineServer(snap *schema.SchemaSnapshot, lintCfg lint.Config) *Server {
	slog.Info("loaded schema from file", "tables", len(snap.Tables), "database", snap.Database)
	return &Server{snap: snap, lintConfig: lintCfg, pgmustardClient: pgmustard.NewClient("")}
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

func (s *Server) getSchema() (*schema.SchemaSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snap == nil || s.uninitialized {
		return nil, fmt.Errorf("no schema loaded — initialize first:\n\n1. Run `dryrun dump-schema --db <DATABASE_URL>` in a terminal\n2. Call the `reload_schema` tool in this session\n\nThe schema will be picked up without restarting the server.")
	}
	return s.snap, nil
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
	snap, err := s.getSchema()
	if err != nil || snap.PgVersion == "" {
		return "dryrun PostgreSQL schema advisor. No schema loaded yet."
	}

	ver, err := dryrun.ParsePgVersion(snap.PgVersion)
	if err != nil {
		return fmt.Sprintf("dryrun PostgreSQL schema advisor. Database: %s", snap.Database)
	}

	return fmt.Sprintf("dryrun PostgreSQL schema advisor. PostgreSQL %s; database: %s", ver, snap.Database)
}
