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

// PRAGMA user_version; 1-2 were the rust codebase, need to restart from 3.
// Purely additive tables (CREATE TABLE IF NOT EXISTS, no rename/drop) don't bump this.
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

		CREATE TABLE IF NOT EXISTS query_stats (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			project_id      TEXT,
			database_id     TEXT,
			schema_ref_hash TEXT NOT NULL,
			content_hash    TEXT NOT NULL,
			node_source     TEXT NOT NULL,
			timestamp       TEXT NOT NULL,
			payload_json    TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS query_stats_by_key_taken_at
			ON query_stats(project_id, database_id, timestamp DESC);
		CREATE INDEX IF NOT EXISTS query_stats_by_schema_ref
			ON query_stats(schema_ref_hash, node_source, timestamp DESC);
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

// ResolveSchemaSnapshot maps a content-hash prefix to one schema row.
// More than one match (prefix collision or content twin) is rejected.
func (s *Store) ResolveSchemaSnapshot(ctx context.Context, key SnapshotKey, hashPrefix string) (SnapshotSummary, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, db_url_hash, timestamp, content_hash, database_name, project_id, database_id
		   FROM snapshots
		  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?
		  ORDER BY timestamp DESC LIMIT 2`,
		string(key.ProjectID), string(key.DatabaseID), hashPrefix+"%")
	if err != nil {
		return SnapshotSummary{}, err
	}
	defer rows.Close()

	var (
		matches []SnapshotSummary
	)
	for rows.Next() {
		ss, serr := scanSchemaSummary(rows)
		if serr != nil {
			return SnapshotSummary{}, serr
		}
		matches = append(matches, ss)
	}
	if err := rows.Err(); err != nil {
		return SnapshotSummary{}, err
	}
	switch len(matches) {
	case 0:
		return SnapshotSummary{}, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, hashPrefix)
	case 1:
		return matches[0], nil
	default:
		return SnapshotSummary{}, fmt.Errorf("ambiguous snapshot hash prefix %q (matches multiple snapshots; use a longer prefix or --latest)", hashPrefix)
	}
}

// table is a caller-side literal, never user input.
func (s *Store) nodeStatsHashMatches(ctx context.Context, pid, did, like, table string, mk func(string) SnapshotKind) ([]SnapshotSummary, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, schema_ref_hash, content_hash, node_source, timestamp FROM "+table+
			" WHERE project_id = ? AND database_id = ? AND content_hash LIKE ? ORDER BY timestamp DESC LIMIT 2",
		pid, did, like)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotSummary
	for rows.Next() {
		var (
			ss    SnapshotSummary
			label string
			tsStr string
		)
		if err := rows.Scan(&ss.ID, &ss.SchemaRefHash, &ss.ContentHash, &label, &tsStr); err != nil {
			return nil, err
		}
		ss.Kind = mk(label)
		ss.NodeLabel = label
		ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		out = append(out, ss)
	}
	return out, rows.Err()
}

// ResolveSnapshot maps a content-hash prefix to one snapshot of any kind
// (schema, planner, activity, query), the same set `snapshot list` prints.
// More than one match across the four tables is rejected so a delete can't
// be misdirected. The returned summary carries its Kind.
func (s *Store) ResolveSnapshot(ctx context.Context, key SnapshotKey, hashPrefix string) (SnapshotSummary, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)
	like := hashPrefix + "%"

	var matches []SnapshotSummary

	srows, err := s.db.QueryContext(ctx,
		`SELECT id, db_url_hash, timestamp, content_hash, database_name, project_id, database_id
		   FROM snapshots
		  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?
		  ORDER BY timestamp DESC LIMIT 2`,
		pid, did, like)
	if err != nil {
		return SnapshotSummary{}, err
	}
	for srows.Next() {
		ss, serr := scanSchemaSummary(srows)
		if serr != nil {
			srows.Close()
			return SnapshotSummary{}, serr
		}
		matches = append(matches, ss)
	}
	if err := srows.Err(); err != nil {
		srows.Close()
		return SnapshotSummary{}, err
	}
	srows.Close()

	prows, err := s.db.QueryContext(ctx,
		`SELECT id, schema_ref_hash, content_hash, timestamp
		   FROM planner_stats
		  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?
		  ORDER BY timestamp DESC LIMIT 2`,
		pid, did, like)
	if err != nil {
		return SnapshotSummary{}, err
	}
	for prows.Next() {
		var (
			ss    SnapshotSummary
			tsStr string
		)
		if err := prows.Scan(&ss.ID, &ss.SchemaRefHash, &ss.ContentHash, &tsStr); err != nil {
			prows.Close()
			return SnapshotSummary{}, err
		}
		ss.Kind = PlannerKind()
		ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		matches = append(matches, ss)
	}
	if err := prows.Err(); err != nil {
		prows.Close()
		return SnapshotSummary{}, err
	}
	prows.Close()

	amatches, err := s.nodeStatsHashMatches(ctx, pid, did, like, "activity_stats", ActivityKind)
	if err != nil {
		return SnapshotSummary{}, err
	}
	matches = append(matches, amatches...)

	qmatches, err := s.nodeStatsHashMatches(ctx, pid, did, like, "query_stats", QueryKind)
	if err != nil {
		return SnapshotSummary{}, err
	}
	matches = append(matches, qmatches...)

	switch len(matches) {
	case 0:
		return SnapshotSummary{}, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, hashPrefix)
	case 1:
		return matches[0], nil
	default:
		return SnapshotSummary{}, fmt.Errorf("ambiguous snapshot hash prefix %q (matches multiple snapshots; use a longer prefix or --latest)", hashPrefix)
	}
}

type DeletedSnapshot struct {
	Snapshot          SnapshotSummary
	PlannerRemoved    int64
	ActivityRemoved   int64
	QueryStatsRemoved int64
	Cascaded          bool // false when a content twin kept the bound stats
}

// DeleteSnapshot removes one snapshot of any kind. Schema rows cascade to their
// bound stats (see DeleteSchemaSnapshot); planner/activity/query rows delete alone.
func (s *Store) DeleteSnapshot(ctx context.Context, key SnapshotKey, snap SnapshotSummary) (DeletedSnapshot, error) {
	switch snap.Kind.Tag {
	case KindSchema:
		return s.DeleteSchemaSnapshot(ctx, key, snap)
	case KindPlanner:
		return s.deleteStatsRow(ctx, key, snap, "planner_stats")
	case KindActivity:
		return s.deleteStatsRow(ctx, key, snap, "activity_stats")
	case KindQuery:
		return s.deleteStatsRow(ctx, key, snap, "query_stats")
	default:
		return DeletedSnapshot{}, fmt.Errorf("unknown SnapshotKind tag: %d", snap.Kind.Tag)
	}
}

// table is a caller-side literal, never user input.
func (s *Store) deleteStatsRow(ctx context.Context, key SnapshotKey, snap SnapshotSummary, table string) (DeletedSnapshot, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM "+table+" WHERE id = ? AND project_id = ? AND database_id = ?",
		snap.ID, pid, did)
	if err != nil {
		return DeletedSnapshot{}, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return DeletedSnapshot{}, fmt.Errorf("%w (id %d)", ErrSnapshotNotFound, snap.ID)
	}
	slog.Info("snapshot deleted", "kind", snap.Kind.String(), "hash", snap.ContentHash,
		"project", pid, "database", did)
	return DeletedSnapshot{Snapshot: snap}, nil
}

// DeleteSchemaSnapshot removes one schema row by rowid and, unless a content
// twin remains, the planner/activity/query stats bound to it. Atomic.
func (s *Store) DeleteSchemaSnapshot(ctx context.Context, key SnapshotKey, snap SnapshotSummary) (DeletedSnapshot, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DeletedSnapshot{}, err
	}
	defer tx.Rollback()

	// cascade off the hash from the deleted row, not the caller struct
	var hash string
	err = tx.QueryRowContext(ctx,
		`DELETE FROM snapshots WHERE id = ? AND project_id = ? AND database_id = ?
		 RETURNING content_hash`,
		snap.ID, pid, did).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return DeletedSnapshot{}, fmt.Errorf("%w (id %d)", ErrSnapshotNotFound, snap.ID)
	}
	if err != nil {
		return DeletedSnapshot{}, err
	}

	out := DeletedSnapshot{Snapshot: snap}

	// a surviving content twin still binds these stats
	var twins int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM snapshots
		  WHERE project_id = ? AND database_id = ? AND content_hash = ?`,
		pid, did, hash).Scan(&twins); err != nil {
		return DeletedSnapshot{}, err
	}
	if twins == 0 {
		pr, err := tx.ExecContext(ctx,
			`DELETE FROM planner_stats
			  WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?`,
			pid, did, hash)
		if err != nil {
			return DeletedSnapshot{}, err
		}
		ar, err := tx.ExecContext(ctx,
			`DELETE FROM activity_stats
			  WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?`,
			pid, did, hash)
		if err != nil {
			return DeletedSnapshot{}, err
		}
		qr, err := tx.ExecContext(ctx,
			`DELETE FROM query_stats
			  WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?`,
			pid, did, hash)
		if err != nil {
			return DeletedSnapshot{}, err
		}
		out.PlannerRemoved, _ = pr.RowsAffected()
		out.ActivityRemoved, _ = ar.RowsAffected()
		out.QueryStatsRemoved, _ = qr.RowsAffected()
		out.Cascaded = true
	}

	if err := tx.Commit(); err != nil {
		return DeletedSnapshot{}, err
	}
	slog.Info("snapshot deleted", "hash", hash, "project", pid, "database", did,
		"planner_removed", out.PlannerRemoved, "activity_removed", out.ActivityRemoved,
		"query_stats_removed", out.QueryStatsRemoved)
	return out, nil
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
	case snap.AsQueryStats() != nil:
		return s.PutQueryStats(ctx, key, snap.AsQueryStats())
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
	case KindQuery:
		q, err := s.getQueryStatsRef(ctx, key, kind.NodeLabel, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapQueryStats(q), nil
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
	case KindQuery:
		return s.listQueryStats(ctx, key, kind.NodeLabel, rng)
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
		return s.deleteNodeStatsBefore(ctx, key, kind.NodeLabel, "activity_stats", cutoff)
	case KindQuery:
		return s.deleteNodeStatsBefore(ctx, key, kind.NodeLabel, "query_stats", cutoff)
	}
	return 0, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

// table is a caller-side literal, never user input.
func (s *Store) deleteNodeStatsBefore(ctx context.Context, key SnapshotKey, nodeLabel, table string, cutoff time.Time) (int64, error) {
	query := "DELETE FROM " + table + " WHERE project_id = ? AND database_id = ? AND timestamp < ?"
	args := []any{string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339)}
	if nodeLabel != "" {
		query += " AND node_source = ?"
		args = append(args, nodeLabel)
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Prune drops activity/query rows older than cutoff, keeping the newest row per (node, kind).
func (s *Store) Prune(ctx context.Context, key SnapshotKey, cutoff time.Time) (int64, error) {
	var total int64
	for _, table := range []string{"activity_stats", "query_stats"} {
		// survivor is the newest per node; timestamps are second-resolution and
		// pull can insert older rows with higher ids, so id only breaks ties.
		res, err := s.db.ExecContext(ctx,
			"DELETE FROM "+table+
				" WHERE project_id = ? AND database_id = ? AND timestamp < ?"+
				" AND id NOT IN (SELECT id FROM "+table+" AS keep"+
				" WHERE project_id = ? AND database_id = ?"+
				" AND id = (SELECT id FROM "+table+
				" WHERE project_id = keep.project_id AND database_id = keep.database_id"+
				" AND node_source = keep.node_source"+
				" ORDER BY timestamp DESC, id DESC LIMIT 1))",
			string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339),
			string(key.ProjectID), string(key.DatabaseID))
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	qrows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT node_source FROM query_stats
		  WHERE project_id = ? AND database_id = ?
		  ORDER BY node_source`,
		pid, did)
	if err != nil {
		return nil, err
	}
	defer qrows.Close()
	for qrows.Next() {
		var label string
		if err := qrows.Scan(&label); err != nil {
			return nil, err
		}
		out = append(out, QueryKind(label))
	}
	return out, qrows.Err()
}

// Maps a content-hash prefix to its kind for the `snapshot diff` same-kind guard.
func (s *Store) ResolveKind(ctx context.Context, key SnapshotKey, hashPrefix string) (SnapshotKind, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var matches []SnapshotKind
	// table is a caller-side literal, never user input.
	count := func(table string) (int, error) {
		var n int
		err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+table+" WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?",
			pid, did, hashPrefix+"%").Scan(&n)
		return n, err
	}

	if n, err := count("snapshots"); err != nil {
		return SnapshotKind{}, err
	} else if n > 0 {
		matches = append(matches, SchemaKind())
	}
	if n, err := count("planner_stats"); err != nil {
		return SnapshotKind{}, err
	} else if n > 0 {
		matches = append(matches, PlannerKind())
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT node_source FROM activity_stats
		  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?`,
		pid, did, hashPrefix+"%")
	if err != nil {
		return SnapshotKind{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return SnapshotKind{}, err
		}
		matches = append(matches, ActivityKind(label))
	}
	if err := rows.Err(); err != nil {
		return SnapshotKind{}, err
	}
	qrows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT node_source FROM query_stats
		  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?`,
		pid, did, hashPrefix+"%")
	if err != nil {
		return SnapshotKind{}, err
	}
	defer qrows.Close()
	for qrows.Next() {
		var label string
		if err := qrows.Scan(&label); err != nil {
			return SnapshotKind{}, err
		}
		matches = append(matches, QueryKind(label))
	}
	if err := qrows.Err(); err != nil {
		return SnapshotKind{}, err
	}

	switch len(matches) {
	case 0:
		return SnapshotKind{}, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, hashPrefix)
	case 1:
		return matches[0], nil
	default:
		return SnapshotKind{}, fmt.Errorf("ambiguous snapshot hash prefix %q (matches multiple kinds)", hashPrefix)
	}
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
