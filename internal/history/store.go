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
// 4: UNIQUE(project_id, database_id, content_hash) on query_stats
// 5: captured_locally on every row table, so --due ignores pulled rows
// 6: planner_stats rebuilt, UNIQUE scoped to (project_id, database_id, content_hash)
const HistorySchemaVersion = 6

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
	CompatOK    Compat = iota // this build's format
	CompatNewer               // written by a newer dryrun
)

func (c Compat) String() string {
	if c == CompatNewer {
		return "newer"
	}
	return "ok"
}

func (s *Store) Compat() Compat { return s.compat }

// Opens (or creates) sqlite history db at path; refuses a foreign (rust-era) file
func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create directory: %w", err)
	}

	// the MCP server polls this file while the CLI writes it; the pragma rides
	// the DSN so every pooled connection gets it, not just the first one
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("cannot open history db: %w", err)
	}

	s := &Store{db: db}

	// old rust db: refuse before migrate() can bolt columns onto a schema we don't own
	foreign, err := isForeignStore(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if foreign {
		db.Close()
		return nil, fmt.Errorf("history db %s was created by an older dryrun and cannot be read; "+
			"move it aside and re-run 'dryrun init' or 'dryrun snapshot pull'", path)
	}

	// journal_mode rewrites the file — must run after the legacy-db check above
	journal := enableWAL(db)

	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	s.compat = stampVersion(db)

	slog.Debug("history store opened", "path", path, "compat", s.compat, "journal", journal)
	return s, nil
}

// WAL keeps mcp-serve reads off SQLITE_BUSY during captures; a fs that refuses
// the switch stays on the rollback journal.
func enableWAL(db *sql.DB) string {
	var journal string
	if err := db.QueryRow("PRAGMA journal_mode = WAL").Scan(&journal); err != nil {
		slog.Debug("history db stays on the rollback journal", "err", err)
		if err := db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
			return "unknown"
		}
	}
	return journal
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

type (
	// tableDesc collapses the four near-identical snapshot tables into one shape.
	// snapshots carries database_name/db_url_hash and self-refs its content hash;
	// the stats tables carry schema_ref_hash and (activity/query) node_source.
	tableDesc struct {
		tag        SnapshotKindTag
		table      string
		payloadCol string // snapshot_json or payload_json
		perNode    bool   // true for activity & query
		detailNoun string // error-message noun
	}

	rowScanner interface {
		Scan(...any) error
	}
)

var (
	allKinds = []tableDesc{
		{tag: KindSchema, table: "snapshots", payloadCol: "snapshot_json", detailNoun: "schema"},
		{tag: KindPlanner, table: "planner_stats", payloadCol: "payload_json", detailNoun: "planner"},
		{tag: KindActivity, table: "activity_stats", payloadCol: "payload_json", perNode: true, detailNoun: "activity"},
		{tag: KindQuery, table: "query_stats", payloadCol: "payload_json", perNode: true, detailNoun: "query stats"},
	}
)

// descFor maps a kind tag to its descriptor; callers pass in a known tag.
func descFor(tag SnapshotKindTag) (tableDesc, bool) {
	if int(tag) >= 0 && int(tag) < len(allKinds) && allKinds[tag].tag == tag {
		return allKinds[tag], true
	}
	return tableDesc{}, false
}

// mustDesc is descFor for call sites holding a compile-time-known tag.
func mustDesc(tag SnapshotKindTag) tableDesc {
	return allKinds[tag]
}

// summarySelect projects every table onto the same nine columns, padding the
// ones a table lacks with empty literals. Aliases keep scanSummary single.
func (d tableDesc) summarySelect() string {
	var (
		schemaRef string
		nodeSrc   string
		dbName    string
		dbHash    string
	)
	if d.tag == KindSchema {
		schemaRef = "content_hash"
	} else {
		schemaRef = "schema_ref_hash"
	}
	if d.perNode {
		nodeSrc = "node_source"
	} else {
		nodeSrc = "''"
	}
	if d.tag == KindSchema {
		dbName = "database_name"
		dbHash = "db_url_hash"
	} else {
		dbName = "''"
		dbHash = "''"
	}
	return "SELECT id, " + dbHash + " AS db_url_hash, timestamp, content_hash, " +
		dbName + " AS database_name, " + schemaRef + " AS schema_ref_hash, " +
		nodeSrc + " AS node_source, project_id, database_id FROM " + d.table
}

// scanSummary reads the nine-column projection above. The timestamp is parsed
// strictly: callers (inventory, LatestSchema) rely on a corrupt one surfacing.
func scanSummary(rows rowScanner, tag SnapshotKindTag) (SnapshotSummary, error) {
	var (
		ss    SnapshotSummary
		tsStr string
		label string
		pid   sql.NullString
		did   sql.NullString
	)
	if err := rows.Scan(&ss.ID, &ss.DBURLHash, &tsStr, &ss.ContentHash, &ss.Database,
		&ss.SchemaRefHash, &label, &pid, &did); err != nil {
		return ss, err
	}
	t, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return ss, fmt.Errorf("bad timestamp %q: %w", tsStr, err)
	}
	ss.Timestamp = t
	switch tag {
	case KindSchema:
		ss.Kind = SchemaKind()
	case KindPlanner:
		ss.Kind = PlannerKind()
	case KindActivity:
		ss.Kind = ActivityKind(label)
		ss.NodeLabel = label
	case KindQuery:
		ss.Kind = QueryKind(label)
		ss.NodeLabel = label
	}
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

// refPayload resolves a SnapshotRef to the raw payload JSON of one row. The
// caller unmarshals into its own concrete type.
func (s *Store) refPayload(ctx context.Context, key SnapshotKey, d tableDesc, nodeLabel string, at SnapshotRef) (string, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	where := " FROM " + d.table + " WHERE project_id = ? AND database_id = ?"
	args := []any{pid, did}
	if d.perNode && nodeLabel != "" {
		where += " AND node_source = ?"
		args = append(args, nodeLabel)
	}
	base := "SELECT " + d.payloadCol + where

	var (
		jsonStr string
		err     error
		detail  string
	)
	switch at.Kind {
	case RefLatest:
		detail = "latest " + d.detailNoun
		err = s.db.QueryRowContext(ctx, base+" ORDER BY timestamp DESC, id DESC LIMIT 1", args...).Scan(&jsonStr)
	case RefAt:
		detail = fmt.Sprintf("%s at-or-before %s", d.detailNoun, at.At.Format(time.RFC3339))
		args = append(args, formatHistoryTS(at.At))
		err = s.db.QueryRowContext(ctx,
			base+" AND timestamp <= ? ORDER BY timestamp DESC, id DESC LIMIT 1", args...).Scan(&jsonStr)
	case RefHash:
		detail = fmt.Sprintf("%s hash %s", d.detailNoun, at.Hash)
		// git-style prefix match; content twins resolve newest-wins
		var hash string
		if hash, err = resolveHashPrefix(ctx, s.db, at.Hash,
			"SELECT DISTINCT content_hash"+where+" AND content_hash LIKE ? ESCAPE '\\' LIMIT 2",
			append(append([]any{}, args...), likePrefix(at.Hash))...); err == nil {
			err = s.db.QueryRowContext(ctx,
				base+" AND content_hash = ? ORDER BY timestamp DESC, id DESC LIMIT 1",
				append(args, hash)...).Scan(&jsonStr)
		}
	case RefIndex:
		detail = fmt.Sprintf("%s latest~%d", d.detailNoun, at.Index)
		args = append(args, at.Index)
		err = s.db.QueryRowContext(ctx,
			base+" ORDER BY timestamp DESC, id DESC LIMIT 1 OFFSET ?", args...).Scan(&jsonStr)
	default:
		return "", fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w (%s)", ErrSnapshotNotFound, detail)
	}
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

// listKind scans newest-first summaries of one kind, optionally node-scoped.
func (s *Store) listKind(ctx context.Context, key SnapshotKey, d tableDesc, nodeLabel string, rng TimeRange) ([]SnapshotSummary, error) {
	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(d.summarySelect())
	sb.WriteString(" WHERE project_id = ? AND database_id = ?")
	args = append(args, string(key.ProjectID), string(key.DatabaseID))
	if d.perNode && nodeLabel != "" {
		sb.WriteString(" AND node_source = ?")
		args = append(args, nodeLabel)
	}
	if rng.From != nil {
		sb.WriteString(" AND timestamp >= ?")
		args = append(args, formatHistoryTS(*rng.From))
	}
	if rng.To != nil {
		sb.WriteString(" AND timestamp < ?")
		args = append(args, formatHistoryTS(*rng.To))
	}
	sb.WriteString(" ORDER BY timestamp DESC, id DESC")

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotSummary
	for rows.Next() {
		ss, err := scanSummary(rows, d.tag)
		if err != nil {
			return nil, err
		}
		out = append(out, ss)
	}
	return out, rows.Err()
}

// latestKind reads one row instead of scanning the whole history.
func (s *Store) latestKind(ctx context.Context, key SnapshotKey, d tableDesc, nodeLabel string) (*SnapshotSummary, error) {
	sb := d.summarySelect() + " WHERE project_id = ? AND database_id = ?"
	args := []any{string(key.ProjectID), string(key.DatabaseID)}
	if d.perNode && nodeLabel != "" {
		sb += " AND node_source = ?"
		args = append(args, nodeLabel)
	}
	sb += " ORDER BY timestamp DESC, id DESC LIMIT 1"

	ss, err := scanSummary(s.db.QueryRowContext(ctx, sb, args...), d.tag)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ss, nil
}

// resolvePrefixMatches gathers prefix matches across the requested tables
// (defaults to all four), newest-first, and collapses content twins.
func (s *Store) resolvePrefixMatches(ctx context.Context, key SnapshotKey, hashPrefix string, kinds ...tableDesc) ([]SnapshotSummary, error) {
	if len(kinds) == 0 {
		kinds = allKinds
	}
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)
	like := likePrefix(hashPrefix)

	var matches []SnapshotSummary
	for _, d := range kinds {
		err := func() error {
			rows, err := s.db.QueryContext(ctx,
				d.summarySelect()+
					" WHERE project_id = ? AND database_id = ? AND content_hash LIKE ? ESCAPE '\\'"+
					" ORDER BY timestamp DESC, id DESC",
				pid, did, like)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				ss, serr := scanSummary(rows, d.tag)
				if serr != nil {
					return serr
				}
				matches = append(matches, ss)
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	return dedupeContentTwins(matches), nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	// A newer dryrun may have dropped constraints this build still creates, so
	// touching its schema either fails the open or silently re-imposes them.
	// Open() classifies it CompatNewer right after this returns.
	var userVersion int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&userVersion); err == nil && userVersion > HistorySchemaVersion {
		return nil
	}

	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			db_url_hash   TEXT NOT NULL,
			timestamp     TEXT NOT NULL,
			content_hash  TEXT NOT NULL,
			database_name TEXT NOT NULL,
			snapshot_json TEXT NOT NULL,
			project_id    TEXT,
			database_id   TEXT,
			captured_locally INTEGER NOT NULL DEFAULT 1
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
			captured_locally INTEGER NOT NULL DEFAULT 1
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
			payload_json    TEXT NOT NULL,
			captured_locally INTEGER NOT NULL DEFAULT 1
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
			payload_json    TEXT NOT NULL,
			captured_locally INTEGER NOT NULL DEFAULT 1
		);
		CREATE INDEX IF NOT EXISTS query_stats_by_key_taken_at
			ON query_stats(project_id, database_id, timestamp DESC);
		CREATE INDEX IF NOT EXISTS query_stats_by_schema_ref
			ON query_stats(schema_ref_hash, node_source, timestamp DESC);

		CREATE TABLE IF NOT EXISTS capture_attempts (
			project_id   TEXT NOT NULL,
			database_id  TEXT NOT NULL,
			node_label   TEXT NOT NULL,
			stream       TEXT NOT NULL,
			attempted_at TEXT NOT NULL,
			PRIMARY KEY (project_id, database_id, node_label, stream)
		);
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

	// v4 -> v5: existing rows backfill as local via DEFAULT 1; table is a
	// caller-side literal, never user input.
	for _, tbl := range []string{"snapshots", "planner_stats", "activity_stats", "query_stats"} {
		if _, err := s.db.Exec("ALTER TABLE " + tbl +
			" ADD COLUMN captured_locally INTEGER NOT NULL DEFAULT 1"); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("migration failed (%s.captured_locally): %w", tbl, err)
			}
		}
	}

	// v5 -> v6: inline UNIQUE(schema_ref_hash, content_hash) ignored the
	// database; SQLite can't drop it, so rebuild the table scoped per database.
	if userVersion < HistorySchemaVersion {
		if err := s.rebuildPlannerStats(); err != nil {
			return err
		}
	}

	// v3 -> v4: pre-constraint DBs can hold duplicate query_stats hashes (an idle
	// node captured twice). Keep the first observation, matching what
	// INSERT OR IGNORE does from here on. NULL-keyed rows are skipped: a unique
	// index treats their keys as distinct, so deduping them would over-delete.
	if userVersion < HistorySchemaVersion {
		res, err := s.db.Exec(`DELETE FROM query_stats WHERE id IN (
			  SELECT id FROM (
			    SELECT id, ROW_NUMBER() OVER (
			           PARTITION BY project_id, database_id, content_hash
			           ORDER BY timestamp ASC, id ASC) AS rn
			      FROM query_stats
			     WHERE project_id IS NOT NULL AND database_id IS NOT NULL)
			   WHERE rn > 1)`)
		if err != nil {
			return fmt.Errorf("migration failed (query_stats dedupe): %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("history migration removed duplicate query stats", "rows", n)
		}

		// Pre-members rows can't be rehashed and would collide on push; drop
		// them. Empty captures survive; json_valid guards corrupt payloads.
		res, err = s.db.Exec(`DELETE FROM query_stats
			 WHERE json_valid(payload_json)
			   AND EXISTS (SELECT 1 FROM json_each(payload_json, '$.queries') e
			                WHERE json_type(e.value) = 'object'
			                  AND json_type(e.value, '$.members') IS NULL)`)
		if err != nil {
			return fmt.Errorf("migration failed (query_stats pre-members): %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("history migration removed pre-members query stats", "rows", n)
		}
	}
	if _, err := s.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS query_stats_by_content_key
		ON query_stats(project_id, database_id, content_hash)`); err != nil {
		return fmt.Errorf("migration failed (query_stats_by_content_key): %w", err)
	}
	if _, err := s.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS planner_stats_by_content_key
		ON planner_stats(project_id, database_id, content_hash)`); err != nil {
		return fmt.Errorf("migration failed (planner_stats_by_content_key): %w", err)
	}

	// Best-effort read-only index: a read-only or busy history.db must still open.
	// Ascending timestamp + rowid tail yields "timestamp DESC, id DESC" without a sort.
	for _, table := range []string{"activity_stats", "query_stats"} {
		if _, err := s.db.Exec("CREATE INDEX IF NOT EXISTS " + table + "_by_node_taken_at ON " + table +
			"(project_id, database_id, node_source, timestamp)"); err != nil {
			slog.Debug("label index not created", "table", table, "err", err)
		}
	}
	return nil
}

// rebuildPlannerStats rebuilds planner_stats scoped per database, deduping rows
// that would violate the new key (first observation wins, like INSERT OR IGNORE).
func (s *Store) rebuildPlannerStats() error {
	// origin 'u' is an inline constraint's autoindex; our explicit index is 'c'.
	var inlineUnique int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM pragma_index_list('planner_stats') WHERE origin = 'u'`).Scan(&inlineUnique); err != nil {
		return fmt.Errorf("migration failed (inspect planner_stats): %w", err)
	}
	if inlineUnique == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("migration failed (planner_stats rebuild): %w", err)
	}
	defer tx.Rollback()

	for _, stmt := range []string{
		`ALTER TABLE planner_stats RENAME TO planner_stats_old`,
		`CREATE TABLE planner_stats (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			project_id      TEXT,
			database_id     TEXT,
			schema_ref_hash TEXT NOT NULL,
			content_hash    TEXT NOT NULL,
			timestamp       TEXT NOT NULL,
			payload_json    TEXT NOT NULL,
			captured_locally INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE UNIQUE INDEX planner_stats_by_content_key
		   ON planner_stats(project_id, database_id, content_hash)`,
		// OR IGNORE drops rows the old global key allowed to collide; earliest
		// (timestamp, id) wins, and NULL keys stay distinct like the index.
		`INSERT OR IGNORE INTO planner_stats
		   (id, project_id, database_id, schema_ref_hash, content_hash, timestamp, payload_json, captured_locally)
		 SELECT id, project_id, database_id, schema_ref_hash, content_hash, timestamp, payload_json, captured_locally
		   FROM planner_stats_old
		  ORDER BY timestamp ASC, id ASC`,
		`DROP TABLE planner_stats_old`,
		`CREATE INDEX planner_stats_by_key_taken_at
		   ON planner_stats(project_id, database_id, timestamp DESC)`,
		`CREATE INDEX planner_stats_by_schema_ref
		   ON planner_stats(schema_ref_hash)`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed (planner_stats rebuild): %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migration failed (planner_stats rebuild): %w", err)
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
	return s.putSchema(ctx, key, snap, true)
}

// capturedLocally is false on the pull path; see Store.Put.
func (s *Store) putSchema(ctx context.Context, key SnapshotKey, snap *schema.SchemaSnapshot, capturedLocally bool) (PutOutcome, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var latest sql.NullString
	_ = s.db.QueryRowContext(ctx,
		`SELECT content_hash FROM snapshots
		  WHERE project_id = ? AND database_id = ?
		  ORDER BY timestamp DESC, id DESC LIMIT 1`,
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
		                        snapshot_json, project_id, database_id, captured_locally)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		syntheticDBURLHash(key), formatHistoryTS(snap.Timestamp),
		snap.ContentHash, snap.Database, string(data), pid, did, localFlag(capturedLocally),
	)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot save snapshot: %w", err)
	}

	slog.Info("snapshot put", "hash", snap.ContentHash, "project", pid, "database", did)
	return PutInserted, nil
}

func localFlag(capturedLocally bool) int {
	if capturedLocally {
		return 1
	}
	return 0
}

var ErrSnapshotNotFound = errors.New("snapshot not found")

func (s *Store) GetSchema(ctx context.Context, key SnapshotKey, at SnapshotRef) (*schema.SchemaSnapshot, error) {
	if err := at.validate(); err != nil {
		return nil, err
	}
	jsonStr, err := s.refPayload(ctx, key, mustDesc(KindSchema), "", at)
	if err != nil {
		return nil, err
	}
	var snap schema.SchemaSnapshot
	if err := json.Unmarshal([]byte(jsonStr), &snap); err != nil {
		return nil, fmt.Errorf("corrupt snapshot JSON: %w", err)
	}
	return &snap, nil
}

// GetSchemaByExactHash resolves a full content hash from an internal join
// (e.g. schema_ref_hash). No prefix matching; on content twins it picks the
// newest row.
func (s *Store) GetSchemaByExactHash(ctx context.Context, key SnapshotKey, hash string) (*schema.SchemaSnapshot, error) {
	if hash == "" {
		return nil, fmt.Errorf("%w (empty schema hash)", ErrSnapshotNotFound)
	}
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var jsonStr string
	err := s.db.QueryRowContext(ctx,
		`SELECT snapshot_json FROM snapshots
		  WHERE project_id = ? AND database_id = ? AND content_hash = ?
		  ORDER BY timestamp DESC, id DESC LIMIT 1`,
		pid, did, hash,
	).Scan(&jsonStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, hash)
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
	return s.listKind(ctx, key, mustDesc(KindSchema), "", rng)
}

// LatestSchema reads one row instead of scanning the history.
func (s *Store) LatestSchema(ctx context.Context, key SnapshotKey) (*SnapshotSummary, error) {
	return s.latestKind(ctx, key, mustDesc(KindSchema), "")
}

// dedupeContentTwins keeps the newest row per (kind, node, content_hash):
// byte-identical re-captures are one addressable snapshot. Input must be newest-first.
func dedupeContentTwins(matches []SnapshotSummary) []SnapshotSummary {
	seen := make(map[string]struct{}, len(matches))
	out := make([]SnapshotSummary, 0, len(matches))
	for _, m := range matches {
		k := fmt.Sprintf("%d\x00%s\x00%s", m.Kind.Tag, m.Kind.NodeLabel, m.ContentHash)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, m)
	}
	return out
}

// ResolveSchemaSnapshot maps a content-hash prefix to one schema row.
// More than one distinct match is rejected.
func (s *Store) ResolveSchemaSnapshot(ctx context.Context, key SnapshotKey, hashPrefix string) (SnapshotSummary, error) {
	matches, err := s.resolvePrefixMatches(ctx, key, hashPrefix, mustDesc(KindSchema))
	if err != nil {
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

// ResolveSnapshot maps a content-hash prefix to one snapshot of any kind
// (schema, planner, activity, query), the same set `snapshot list` prints.
// More than one match across the four tables is rejected so a delete can't
// be misdirected. The returned summary carries its Kind.
func (s *Store) ResolveSnapshot(ctx context.Context, key SnapshotKey, hashPrefix string) (SnapshotSummary, error) {
	matches, err := s.resolvePrefixMatches(ctx, key, hashPrefix)
	if err != nil {
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

type (
	CascadeCounts struct {
		Planner    int
		Activity   int
		QueryStats int
		Blocked    bool // a surviving content twin keeps these rows; nothing cascades
	}
)

func (c CascadeCounts) Total() int { return c.Planner + c.Activity + c.QueryStats }

// CountCascade counts the stats rows bound to this snapshot, so the delete
// prompt can say how many go with it. Non-schema kinds never cascade, so they
// count zero. Blocked means a content twin survives the delete and keeps the
// rows: the counts then describe what stays, not what goes.
//
// Advisory only, and read outside the delete transaction: `snap` is assumed to
// have been resolved from this store, and a capture landing between the count
// and the delete can add a twin, turning a promised cascade into a blocked one.
// The delete recounts inside its transaction and reports the actuals.
func (s *Store) CountCascade(ctx context.Context, key SnapshotKey, snap SnapshotSummary) (CascadeCounts, error) {
	if snap.Kind.Tag != KindSchema {
		return CascadeCounts{}, nil
	}
	twins, err := s.CountContentTwins(ctx, key, snap)
	if err != nil {
		return CascadeCounts{}, err
	}

	out := CascadeCounts{Blocked: twins > 1}
	for _, t := range []struct {
		table string
		into  *int
	}{
		{"planner_stats", &out.Planner},
		{"activity_stats", &out.Activity},
		{"query_stats", &out.QueryStats},
	} {
		// table is a caller-side literal, never user input.
		if err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+t.table+
				" WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?",
			string(key.ProjectID), string(key.DatabaseID), snap.ContentHash,
		).Scan(t.into); err != nil {
			return CascadeCounts{}, err
		}
	}
	return out, nil
}

// CountContentTwins counts rows of this kind carrying the content hash,
// so delete can report removing 1 of N identical rows.
func (s *Store) CountContentTwins(ctx context.Context, key SnapshotKey, snap SnapshotSummary) (int, error) {
	d, ok := descFor(snap.Kind.Tag)
	if !ok {
		return 0, fmt.Errorf("unknown SnapshotKind tag: %d", snap.Kind.Tag)
	}
	var (
		clause string
		args   = []any{string(key.ProjectID), string(key.DatabaseID), snap.ContentHash}
	)
	if d.perNode {
		clause = " AND node_source = ?"
		args = append(args, snap.Kind.NodeLabel)
	}
	var n int
	// table/clause are caller-side literals, never user input.
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+d.table+
			" WHERE project_id = ? AND database_id = ? AND content_hash = ?"+clause,
		args...).Scan(&n)
	return n, err
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
	if snap.Kind.Tag == KindSchema {
		return s.DeleteSchemaSnapshot(ctx, key, snap)
	}
	d, ok := descFor(snap.Kind.Tag)
	if !ok {
		return DeletedSnapshot{}, fmt.Errorf("unknown SnapshotKind tag: %d", snap.Kind.Tag)
	}
	return s.deleteStatsRow(ctx, key, snap, d.table)
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

// Put is the sync/pull entry point, so rows land captured_locally=0; local
// capture uses the typed PutX methods.
func (s *Store) Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error) {
	switch {
	case snap.AsSchema() != nil:
		return s.putSchema(ctx, key, snap.AsSchema(), false)
	case snap.AsPlanner() != nil:
		return s.putPlanner(ctx, key, snap.AsPlanner(), false)
	case snap.AsActivity() != nil:
		return s.putActivity(ctx, key, snap.AsActivity(), false)
	case snap.AsQueryStats() != nil:
		return s.putQueryStats(ctx, key, snap.AsQueryStats(), false)
	}
	return PutInserted, fmt.Errorf("empty StoredSnapshot")
}

func (s *Store) Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error) {
	if err := at.validate(); err != nil {
		return StoredSnapshot{}, err
	}
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
	d, ok := descFor(kind.Tag)
	if !ok {
		return nil, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
	}
	return s.latestKind(ctx, key, d, kind.NodeLabel)
}

func (s *Store) ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var out []SnapshotKind
	for _, d := range allKinds {
		if !d.perNode {
			var n int
			if err := s.db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM "+d.table+" WHERE project_id = ? AND database_id = ?",
				pid, did).Scan(&n); err != nil {
				return nil, err
			}
			if n > 0 {
				out = append(out, SnapshotKind{Tag: d.tag})
			}
			continue
		}

		err := func() error {
			rows, err := s.db.QueryContext(ctx,
				"SELECT DISTINCT node_source FROM "+d.table+
					" WHERE project_id = ? AND database_id = ? ORDER BY node_source",
				pid, did)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var label string
				if err := rows.Scan(&label); err != nil {
					return err
				}
				out = append(out, SnapshotKind{Tag: d.tag, NodeLabel: label})
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Maps a content-hash prefix to its kind for the `snapshot diff` same-kind guard.
func (s *Store) ResolveKind(ctx context.Context, key SnapshotKey, hashPrefix string) (SnapshotKind, error) {
	matches, err := s.resolvePrefixMatches(ctx, key, hashPrefix)
	if err != nil {
		return SnapshotKind{}, err
	}
	// Dedupe by kind identity: several rows of the same node are one kind.
	seen := make(map[string]struct{}, len(matches))
	var kinds []SnapshotKind
	for _, m := range matches {
		k := fmt.Sprintf("%d\x00%s", m.Kind.Tag, m.Kind.NodeLabel)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		kinds = append(kinds, SnapshotKind{Tag: m.Kind.Tag, NodeLabel: m.Kind.NodeLabel})
	}

	switch len(kinds) {
	case 0:
		return SnapshotKind{}, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, hashPrefix)
	case 1:
		return kinds[0], nil
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
