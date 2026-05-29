package history

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/boringsql/dryrun/internal/schema"
)

// PRAGMA user_version; 1-2 were the rust codebase, need to restart from 3
const HistorySchemaVersion = 3

type (
	Compat int

	Store struct {
		db     *sql.DB
		compat Compat
	}

	SnapshotSummary struct {
		ID            int64        `json:"id"`
		Kind          SnapshotKind `json:"-"`
		DBURLHash     string       `json:"db_url_hash,omitempty"`
		Timestamp     time.Time    `json:"timestamp"`
		ContentHash   string       `json:"content_hash"`
		Database      string       `json:"database,omitempty"`
		SchemaRefHash string       `json:"schema_ref_hash,omitempty"`
		NodeLabel     string       `json:"node_label,omitempty"`
		ProjectID     *string      `json:"project_id,omitempty"`
		DatabaseID    *string      `json:"database_id,omitempty"`
	}
)

const (
	CompatOK     Compat = iota // this build's format
	CompatLegacy               // old rust history.db
	CompatNewer                // written by a newer dryrun
)

func (c Compat) String() string {
	switch c {
	case CompatLegacy:
		return "legacy"
	case CompatNewer:
		return "newer"
	default:
		return "ok"
	}
}

func (s *Store) Compat() Compat { return s.compat }

// Opens (or creates) sqlite history db at path
func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("cannot open history db: %w", err)
	}

	s := &Store{db: db}

	// old rust db, don't migrate it
	foreign, err := isForeignStore(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if foreign {
		s.compat = CompatLegacy
		slog.Debug("history store: incompatible format", "path", path)
		return s, nil
	}

	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	s.compat = stampVersion(db)

	slog.Debug("history store opened", "path", path, "compat", s.compat)
	return s, nil
}

// true if this looks like an old rust history.db, not ours
func isForeignStore(db *sql.DB) (bool, error) {
	var name string
	err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'`).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect history db: %w", err)
	}

	rows, err := db.Query(`PRAGMA table_info(snapshots)`)
	if err != nil {
		return false, fmt.Errorf("inspect snapshots table: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			cname   string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &cname, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if cname == "db_url_hash" {
			return false, nil
		}
	}
	return true, rows.Err()
}

// save our schema version into the db
func stampVersion(db *sql.DB) Compat {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return CompatOK
	}
	if v > HistorySchemaVersion {
		return CompatNewer
	}
	if v != HistorySchemaVersion {
		_, _ = db.Exec(fmt.Sprintf("PRAGMA user_version = %d", HistorySchemaVersion))
	}
	return CompatOK
}

// Opens .dryrun/history.db in cwd
func OpenDefault() (*Store, error) {
	path, err := DefaultHistoryPath()
	if err != nil {
		return nil, err
	}
	return Open(path)
}

func scanSchemaSummary(rows interface{ Scan(...any) error }) (SnapshotSummary, error) {
	var (
		ss    SnapshotSummary
		tsStr string
		pid   sql.NullString
		did   sql.NullString
	)
	if err := rows.Scan(&ss.ID, &ss.DBURLHash, &tsStr, &ss.ContentHash, &ss.Database, &pid, &did); err != nil {
		return ss, err
	}
	ss.Kind = SchemaKind()
	ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
	ss.SchemaRefHash = ss.ContentHash
	if pid.Valid {
		v := pid.String
		ss.ProjectID = &v
	}
	if did.Valid {
		v := did.String
		ss.DatabaseID = &v
	}
	return ss, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			db_url_hash   TEXT NOT NULL,
			timestamp     TEXT NOT NULL,
			content_hash  TEXT NOT NULL,
			database_name TEXT NOT NULL,
			snapshot_json TEXT NOT NULL,
			project_id    TEXT,
			database_id   TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_snapshots_content_hash
			ON snapshots(content_hash);
		CREATE INDEX IF NOT EXISTS snapshots_by_key_taken_at
			ON snapshots(project_id, database_id, timestamp DESC);

		CREATE TABLE IF NOT EXISTS planner_stats (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			project_id      TEXT,
			database_id     TEXT,
			schema_ref_hash TEXT NOT NULL,
			content_hash    TEXT NOT NULL,
			timestamp       TEXT NOT NULL,
			payload_json    TEXT NOT NULL,
			UNIQUE(schema_ref_hash, content_hash)
		);
		CREATE INDEX IF NOT EXISTS planner_stats_by_key_taken_at
			ON planner_stats(project_id, database_id, timestamp DESC);
		CREATE INDEX IF NOT EXISTS planner_stats_by_schema_ref
			ON planner_stats(schema_ref_hash);

		CREATE TABLE IF NOT EXISTS activity_stats (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			project_id      TEXT,
			database_id     TEXT,
			schema_ref_hash TEXT NOT NULL,
			content_hash    TEXT NOT NULL,
			node_source     TEXT NOT NULL,
			timestamp       TEXT NOT NULL,
			payload_json    TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS activity_stats_by_key_taken_at
			ON activity_stats(project_id, database_id, timestamp DESC);
		CREATE INDEX IF NOT EXISTS activity_stats_by_schema_ref
			ON activity_stats(schema_ref_hash, node_source, timestamp DESC);
	`)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// in-place upgrade for legacy history.db: columns added nullable; legacy rows stay NULL
	for _, col := range []string{"project_id", "database_id"} {
		if _, err := s.db.Exec("ALTER TABLE snapshots ADD COLUMN " + col + " TEXT"); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("migration failed (%s): %w", col, err)
			}
		}
	}
	// db_url_hash predates project_id/database_id but legacy DBs from the very first cut lack it
	if _, err := s.db.Exec("ALTER TABLE snapshots ADD COLUMN db_url_hash TEXT NOT NULL DEFAULT ''"); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("migration failed (db_url_hash): %w", err)
		}
	}
	if _, err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_snapshots_db_url_hash
		ON snapshots(db_url_hash, timestamp DESC)`); err != nil {
		return fmt.Errorf("migration failed (idx_snapshots_db_url_hash): %w", err)
	}
	return nil
}

// synthetic db_url_hash for SnapshotStore rows that lack a real db_url
func syntheticDBURLHash(key SnapshotKey) string {
	h := sha256.Sum256([]byte("dryrun-key:" + string(key.ProjectID) + ":" + string(key.DatabaseID)))
	return fmt.Sprintf("%x", h)[:16]
}

// schema-specific wrapper; mirror of Rust's `put_schema` default method.
func (s *Store) PutSchema(ctx context.Context, key SnapshotKey, snap *schema.SchemaSnapshot) (PutOutcome, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var latest sql.NullString
	_ = s.db.QueryRowContext(ctx,
		`SELECT content_hash FROM snapshots
		  WHERE project_id = ? AND database_id = ?
		  ORDER BY timestamp DESC LIMIT 1`,
		pid, did,
	).Scan(&latest)

	if latest.Valid && latest.String == snap.ContentHash {
		slog.Debug("schema unchanged, skipping put", "hash", snap.ContentHash)
		return PutDeduped, nil
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot serialize snapshot: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO snapshots (db_url_hash, timestamp, content_hash, database_name,
		                        snapshot_json, project_id, database_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		syntheticDBURLHash(key), snap.Timestamp.Format(time.RFC3339),
		snap.ContentHash, snap.Database, string(data), pid, did,
	)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot save snapshot: %w", err)
	}

	slog.Info("snapshot put", "hash", snap.ContentHash, "project", pid, "database", did)
	return PutInserted, nil
}

var ErrSnapshotNotFound = errors.New("snapshot not found")

func (s *Store) GetSchema(ctx context.Context, key SnapshotKey, at SnapshotRef) (*schema.SchemaSnapshot, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var (
		jsonStr string
		err     error
		detail  string
	)
	switch at.Kind {
	case RefLatest:
		detail = "latest"
		err = s.db.QueryRowContext(ctx,
			`SELECT snapshot_json FROM snapshots
			  WHERE project_id = ? AND database_id = ?
			  ORDER BY timestamp DESC LIMIT 1`,
			pid, did,
		).Scan(&jsonStr)
	case RefAt:
		detail = fmt.Sprintf("at-or-before %s", at.At.Format(time.RFC3339))
		err = s.db.QueryRowContext(ctx,
			`SELECT snapshot_json FROM snapshots
			  WHERE project_id = ? AND database_id = ? AND timestamp <= ?
			  ORDER BY timestamp DESC LIMIT 1`,
			pid, did, at.At.Format(time.RFC3339),
		).Scan(&jsonStr)
	case RefHash:
		detail = "hash " + at.Hash
		// git-style prefix match; ambiguous prefixes are rejected
		rows, qerr := s.db.QueryContext(ctx,
			`SELECT snapshot_json FROM snapshots
			  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?
			  LIMIT 2`,
			pid, did, at.Hash+"%",
		)
		if qerr != nil {
			return nil, qerr
		}
		defer rows.Close()
		matches := 0
		for rows.Next() {
			matches++
			if matches == 1 {
				if scanErr := rows.Scan(&jsonStr); scanErr != nil {
					return nil, scanErr
				}
			}
		}
		if matches == 0 {
			err = sql.ErrNoRows
		} else if matches > 1 {
			return nil, fmt.Errorf("ambiguous snapshot hash prefix %q (matches multiple)", at.Hash)
		}
	default:
		return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (%s)", ErrSnapshotNotFound, detail)
	}
	if err != nil {
		return nil, err
	}

	var snap schema.SchemaSnapshot
	if err := json.Unmarshal([]byte(jsonStr), &snap); err != nil {
		return nil, fmt.Errorf("corrupt snapshot JSON: %w", err)
	}
	return &snap, nil
}

func (s *Store) ListSchema(ctx context.Context, key SnapshotKey, rng TimeRange) ([]SnapshotSummary, error) {
	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(`SELECT id, db_url_hash, timestamp, content_hash, database_name, project_id, database_id
	                  FROM snapshots WHERE project_id = ? AND database_id = ?`)
	args = append(args, string(key.ProjectID), string(key.DatabaseID))
	if rng.From != nil {
		sb.WriteString(" AND timestamp >= ?")
		args = append(args, rng.From.Format(time.RFC3339))
	}
	if rng.To != nil {
		sb.WriteString(" AND timestamp < ?")
		args = append(args, rng.To.Format(time.RFC3339))
	}
	sb.WriteString(" ORDER BY timestamp DESC")

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotSummary
	for rows.Next() {
		ss, err := scanSchemaSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ss)
	}
	return out, rows.Err()
}

func (s *Store) LatestSchema(ctx context.Context, key SnapshotKey) (*SnapshotSummary, error) {
	list, err := s.ListSchema(ctx, key, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (s *Store) DeleteSchemaBefore(ctx context.Context, key SnapshotKey, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM snapshots
		  WHERE project_id = ? AND database_id = ? AND timestamp < ?`,
		string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339),
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// rows with NULL project/database are legacy and not exportable as keyed streams
func (s *Store) ListKeys(ctx context.Context) ([]SnapshotKey, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT project_id, database_id FROM snapshots
		  WHERE project_id IS NOT NULL AND database_id IS NOT NULL
		  ORDER BY project_id, database_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotKey
	for rows.Next() {
		var pid, did string
		if err := rows.Scan(&pid, &did); err != nil {
			return nil, err
		}
		out = append(out, SnapshotKey{ProjectID: ProjectId(pid), DatabaseID: DatabaseId(did)})
	}
	return out, rows.Err()
}

// Put dispatches on the StoredSnapshot variant to the right kind-specific path.
func (s *Store) Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error) {
	switch {
	case snap.AsSchema() != nil:
		return s.PutSchema(ctx, key, snap.AsSchema())
	case snap.AsPlanner() != nil:
		return s.PutPlanner(ctx, key, snap.AsPlanner())
	case snap.AsActivity() != nil:
		return s.PutActivity(ctx, key, snap.AsActivity())
	}
	return PutInserted, fmt.Errorf("empty StoredSnapshot")
}

func (s *Store) Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error) {
	switch kind.Tag {
	case KindSchema:
		snap, err := s.GetSchema(ctx, key, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapSchema(snap), nil
	case KindPlanner:
		p, err := s.getPlannerRef(ctx, key, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapPlanner(p), nil
	case KindActivity:
		a, err := s.getActivityRef(ctx, key, kind.NodeLabel, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapActivity(a), nil
	}
	return StoredSnapshot{}, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

func (s *Store) List(ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error) {
	switch kind.Tag {
	case KindSchema:
		return s.ListSchema(ctx, key, rng)
	case KindPlanner:
		return s.listPlanner(ctx, key, rng)
	case KindActivity:
		return s.listActivity(ctx, key, kind.NodeLabel, rng)
	}
	return nil, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

func (s *Store) Latest(ctx context.Context, key SnapshotKey, kind SnapshotKind) (*SnapshotSummary, error) {
	list, err := s.List(ctx, key, kind, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (s *Store) DeleteBefore(ctx context.Context, key SnapshotKey, kind SnapshotKind, cutoff time.Time) (int64, error) {
	switch kind.Tag {
	case KindSchema:
		return s.DeleteSchemaBefore(ctx, key, cutoff)
	case KindPlanner:
		res, err := s.db.ExecContext(ctx,
			`DELETE FROM planner_stats
			  WHERE project_id = ? AND database_id = ? AND timestamp < ?`,
			string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339),
		)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	case KindActivity:
		query := `DELETE FROM activity_stats
			  WHERE project_id = ? AND database_id = ? AND timestamp < ?`
		args := []any{string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339)}
		if kind.NodeLabel != "" {
			query += " AND node_source = ?"
			args = append(args, kind.NodeLabel)
		}
		res, err := s.db.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	return 0, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

func (s *Store) ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var out []SnapshotKind
	var n int

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM snapshots WHERE project_id = ? AND database_id = ?`,
		pid, did).Scan(&n); err != nil {
		return nil, err
	}
	if n > 0 {
		out = append(out, SchemaKind())
	}

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM planner_stats WHERE project_id = ? AND database_id = ?`,
		pid, did).Scan(&n); err != nil {
		return nil, err
	}
	if n > 0 {
		out = append(out, PlannerKind())
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT node_source FROM activity_stats
		  WHERE project_id = ? AND database_id = ?
		  ORDER BY node_source`,
		pid, did)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, err
		}
		out = append(out, ActivityKind(label))
	}
	return out, rows.Err()
}

// compile-time check that *Store satisfies SnapshotStore
var _ SnapshotStore = (*Store)(nil)

func DefaultHistoryPath() (string, error) {
	dir, err := DefaultDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "history.db"), nil
}

func DefaultDataDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cannot determine working directory: %w", err)
	}
	return filepath.Join(cwd, ".dryrun"), nil
}
