package schema

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/dryrun"
)

// satisfied by both *pgxpool.Pool and pgx.Tx, so capture can run straight on
// the pool or inside a read-only tx
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type (
	DryRun struct {
		pool *pgxpool.Pool
	}

	// session-level defense: read-only default plus timeouts, applied to every
	// connection the pool opens. Independent of the capture tx's READ ONLY mode.
	SessionGuards struct {
		ReadOnly         bool
		StatementTimeout time.Duration
		LockTimeout      time.Duration
		IdleInTxTimeout  time.Duration
	}
)

func DefaultSessionGuards() SessionGuards {
	return SessionGuards{
		ReadOnly:         true,
		StatementTimeout: 30 * time.Second,
		LockTimeout:      2 * time.Second,
		IdleInTxTimeout:  10 * time.Second,
	}
}

func (g SessionGuards) apply(ctx context.Context, c *pgx.Conn) error {
	var b strings.Builder
	if g.ReadOnly {
		b.WriteString("SET default_transaction_read_only = on;\n")
	}
	// zero disables; values are Go durations, not user input, so formatting is safe
	if g.StatementTimeout > 0 {
		fmt.Fprintf(&b, "SET statement_timeout = %d;\n", g.StatementTimeout.Milliseconds())
	}
	if g.LockTimeout > 0 {
		fmt.Fprintf(&b, "SET lock_timeout = %d;\n", g.LockTimeout.Milliseconds())
	}
	if g.IdleInTxTimeout > 0 {
		fmt.Fprintf(&b, "SET idle_in_transaction_session_timeout = %d;\n", g.IdleInTxTimeout.Milliseconds())
	}
	if b.Len() == 0 {
		return nil
	}
	_, err := c.Exec(ctx, b.String())
	return err
}

type ProbeResult struct {
	Version       dryrun.PgVersion `json:"version"`
	VersionString string           `json:"version_string"`
	Flavor        Flavor           `json:"flavor"`
	Capabilities  Capabilities     `json:"capabilities"`
}

type PrivilegeReport struct {
	PgCatalog         bool `json:"pg_catalog"`
	InformationSchema bool `json:"information_schema"`
	PgStatUserTables  bool `json:"pg_stat_user_tables"`
}

func Connect(ctx context.Context, url string) (*DryRun, error) {
	return ConnectWithGuards(ctx, url, DefaultSessionGuards())
}

func ConnectWithGuards(ctx context.Context, url string, guards SessionGuards) (*DryRun, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("connection failed: %w", err)
	}

	config.MaxConns = 5
	config.MaxConnLifetime = 30 * time.Minute
	config.AfterConnect = guards.apply

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, classifyConnError(err, url)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, classifyConnError(err, url)
	}

	slog.Debug("connected to PostgreSQL")
	return &DryRun{pool: pool}, nil
}

func (d *DryRun) Probe(ctx context.Context) (*ProbeResult, error) {
	var versionStr string
	err := d.pool.QueryRow(ctx, "SELECT version()").Scan(&versionStr)
	if err != nil {
		return nil, fmt.Errorf("probe failed: %w", err)
	}

	version, err := dryrun.ParsePgVersion(versionStr)
	if err != nil {
		return nil, err
	}

	// advisory: a failed flavor probe defaults to postgres, not an error
	flavor, err := DetectFlavorLive(ctx, d.pool)
	if err != nil {
		slog.Warn("flavor probe failed, reporting as postgres", "err", err)
	}

	slog.Info("probed PostgreSQL", "pg_version", version.String(), "flavor", flavor)
	return &ProbeResult{
		Version:       version,
		VersionString: versionStr,
		Flavor:        flavor,
		Capabilities:  flavor.Capabilities(),
	}, nil
}

// Probes access to key system catalogs
func (d *DryRun) CheckPrivileges(ctx context.Context) (*PrivilegeReport, error) {
	report := &PrivilegeReport{
		PgCatalog:         checkAccess(ctx, d.pool, "SELECT 1 FROM pg_catalog.pg_tables LIMIT 1"),
		InformationSchema: checkAccess(ctx, d.pool, "SELECT 1 FROM information_schema.columns LIMIT 1"),
		PgStatUserTables:  checkAccess(ctx, d.pool, "SELECT 1 FROM pg_stat_user_tables LIMIT 1"),
	}
	slog.Info("privilege check complete",
		"pg_catalog", report.PgCatalog,
		"information_schema", report.InformationSchema,
		"pg_stat_user_tables", report.PgStatUserTables,
	)
	return report, nil
}

func (d *DryRun) Introspect(ctx context.Context) (*SchemaSnapshot, error) {
	return IntrospectSchema(ctx, d.pool)
}

func (d *DryRun) Pool() *pgxpool.Pool {
	return d.pool
}

func (d *DryRun) Close() {
	d.pool.Close()
}

func checkAccess(ctx context.Context, pool *pgxpool.Pool, query string) bool {
	_, err := pool.Exec(ctx, query)
	return err == nil
}

func classifyConnError(err error, url string) error {
	return fmt.Errorf("connection failed to %s: %w", url, err)
}
