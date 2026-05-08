package history

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// Returns false if content_hash matches the latest stored
func (s *Store) SaveSnapshot(dbURL string, snap *schema.SchemaSnapshot) (bool, error) {
	urlHash := hashURL(dbURL)

	var latestHash sql.NullString
	_ = s.db.QueryRow(
		"SELECT content_hash FROM snapshots WHERE db_url_hash = ? ORDER BY timestamp DESC LIMIT 1",
		urlHash,
	).Scan(&latestHash)

	if latestHash.Valid && latestHash.String == snap.ContentHash {
		slog.Debug("schema unchanged, skipping save", "hash", snap.ContentHash)
		return false, nil
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return false, fmt.Errorf("cannot serialize snapshot: %w", err)
	}

	_, err = s.db.Exec(
		"INSERT INTO snapshots (db_url_hash, timestamp, content_hash, database_name, snapshot_json) VALUES (?, ?, ?, ?, ?)",
		urlHash, snap.Timestamp.Format(time.RFC3339), snap.ContentHash, snap.Database, string(data),
	)
	if err != nil {
		return false, fmt.Errorf("cannot save snapshot: %w", err)
	}

	slog.Info("snapshot saved", "hash", snap.ContentHash, "database", snap.Database)
	return true, nil
}

func (s *Store) LoadSnapshot(contentHash string) (*schema.SchemaSnapshot, error) {
	var jsonStr string
	err := s.db.QueryRow(
		"SELECT snapshot_json FROM snapshots WHERE content_hash = ? LIMIT 1",
		contentHash,
	).Scan(&jsonStr)
	if err == sql.ErrNoRows {
		return nil, nil
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

func (s *Store) ListSnapshots(dbURL string) ([]SnapshotSummary, error) {
	urlHash := hashURL(dbURL)
	rows, err := s.db.Query(
		"SELECT id, db_url_hash, timestamp, content_hash, database_name FROM snapshots WHERE db_url_hash = ? ORDER BY timestamp DESC",
		urlHash,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []SnapshotSummary
	for rows.Next() {
		var (
			ss    SnapshotSummary
			tsStr string
		)
		if err := rows.Scan(&ss.ID, &ss.DBURLHash, &tsStr, &ss.ContentHash, &ss.Database); err != nil {
			return nil, err
		}
		ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		summaries = append(summaries, ss)
	}
	return summaries, rows.Err()
}

func (s *Store) LatestSnapshot(dbURL string) (*schema.SchemaSnapshot, error) {
	urlHash := hashURL(dbURL)
	var jsonStr string
	err := s.db.QueryRow(
		"SELECT snapshot_json FROM snapshots WHERE db_url_hash = ? ORDER BY timestamp DESC LIMIT 1",
		urlHash,
	).Scan(&jsonStr)
	if err == sql.ErrNoRows {
		return nil, nil
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

func (s *Store) SnapshotsSince(dbURL string, since time.Time) ([]schema.SchemaSnapshot, error) {
	urlHash := hashURL(dbURL)
	rows, err := s.db.Query(
		"SELECT snapshot_json FROM snapshots WHERE db_url_hash = ? AND timestamp >= ? ORDER BY timestamp ASC",
		urlHash, since.Format(time.RFC3339),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []schema.SchemaSnapshot
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, err
		}
		var snap schema.SchemaSnapshot
		if err := json.Unmarshal([]byte(jsonStr), &snap); err != nil {
			return nil, fmt.Errorf("corrupt snapshot JSON: %w", err)
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, rows.Err()
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
			snapshot_json TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_snapshots_db_url_hash
			ON snapshots(db_url_hash, timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_snapshots_content_hash
			ON snapshots(content_hash);
	`)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}

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

func hashURL(url string) string {
	h := sha256.Sum256([]byte(url))
	return fmt.Sprintf("%x", h)[:16]
}
