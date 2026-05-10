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

type Store struct {
	db *sql.DB
}

type SnapshotSummary struct {
	ID          int64     `json:"id"`
	DBURLHash   string    `json:"db_url_hash"`
	Timestamp   time.Time `json:"timestamp"`
	ContentHash string    `json:"content_hash"`
	Database    string    `json:"database"`
	ProjectID   *string   `json:"project_id,omitempty"`
	DatabaseID  *string   `json:"database_id,omitempty"`
}

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
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}

	slog.Debug("history store opened", "path", path)
	return s, nil
}

// Opens .dryrun/history.db in cwd
func OpenDefault() (*Store, error) {
	path, err := DefaultHistoryPath()
	if err != nil {
		return nil, err
	}
	return Open(path)
}

func scanSummary(rows interface{ Scan(...any) error }) (SnapshotSummary, error) {
	var (
		ss    SnapshotSummary
		tsStr string
		pid   sql.NullString
		did   sql.NullString
	)
	if err := rows.Scan(&ss.ID, &ss.DBURLHash, &tsStr, &ss.ContentHash, &ss.Database, &pid, &did); err != nil {
		return ss, err
	}
	ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
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
		CREATE INDEX IF NOT EXISTS idx_snapshots_db_url_hash
			ON snapshots(db_url_hash, timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_snapshots_content_hash
			ON snapshots(content_hash);
		CREATE INDEX IF NOT EXISTS snapshots_by_key_taken_at
			ON snapshots(project_id, database_id, timestamp DESC);
	`)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// in-place upgrade for pre-v0.6 history.db: columns added nullable; legacy rows stay NULL
	for _, col := range []string{"project_id", "database_id"} {
		if _, err := s.db.Exec("ALTER TABLE snapshots ADD COLUMN " + col + " TEXT"); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("migration failed (%s): %w", col, err)
			}
		}
	}
	return nil
}

// synthetic db_url_hash for SnapshotStore rows that lack a real db_url
func syntheticDBURLHash(key SnapshotKey) string {
	h := sha256.Sum256([]byte("dryrun-key:" + string(key.ProjectID) + ":" + string(key.DatabaseID)))
	return fmt.Sprintf("%x", h)[:16]
}

func (s *Store) Put(ctx context.Context, key SnapshotKey, snap *schema.SchemaSnapshot) (PutOutcome, error) {
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

func (s *Store) Get(ctx context.Context, key SnapshotKey, at SnapshotRef) (*schema.SchemaSnapshot, error) {
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
		err = s.db.QueryRowContext(ctx,
			`SELECT snapshot_json FROM snapshots
			  WHERE project_id = ? AND database_id = ? AND content_hash = ?
			  LIMIT 1`,
			pid, did, at.Hash,
		).Scan(&jsonStr)
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

func (s *Store) List(ctx context.Context, key SnapshotKey, rng TimeRange) ([]SnapshotSummary, error) {
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
		ss, err := scanSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ss)
	}
	return out, rows.Err()
}

func (s *Store) Latest(ctx context.Context, key SnapshotKey) (*SnapshotSummary, error) {
	list, err := s.List(ctx, key, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (s *Store) DeleteBefore(ctx context.Context, key SnapshotKey, cutoff time.Time) (int64, error) {
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

