package history

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// userVersion reads back the PRAGMA user_version that Open is supposed to
// stamp. The test reaches into the unexported db handle directly — it lives in
// package history, and going through a public accessor would only obscure what
// is being checked.
func userVersion(t *testing.T, s *Store) int {
	t.Helper()
	var v int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	return v
}

// tableExists reports whether a table is present — used to prove that opening
// a foreign store does NOT scribble Go-only tables into it.
func tableExists(t *testing.T, path, table string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	var name string
	err = db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	return true
}

// TestOpenFreshStoreIsCompatOK: a brand-new store must come back CompatOK and
// must have HistorySchemaVersion stamped into PRAGMA user_version, so the very
// next open can recognise it as its own without re-inspecting table shapes.
func TestOpenFreshStoreIsCompatOK(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if s.Compat() != CompatOK {
		t.Errorf("fresh store: got Compat %v, want ok", s.Compat())
	}
	if got := userVersion(t, s); got != HistorySchemaVersion {
		t.Errorf("user_version: got %d, want %d", got, HistorySchemaVersion)
	}
}

// TestOpenForeignStoreIsLegacy is the core of the Rust-to-Go detection. We
// hand-build a SQLite file whose snapshots table looks like the pre-Go (Rust)
// schema — it carries a `kind` column and, crucially, lacks `db_url_hash`.
// Open must flag it CompatLegacy AND leave it completely untouched: no
// migrate, so no empty planner_stats table gets created inside someone's old
// database. Mutating a foreign store would be the rude, data-risking move.
func TestOpenForeignStoreIsLegacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rust.db")

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("create raw db: %v", err)
	}
	// Rust-era snapshots table: single table, kind-discriminated, no db_url_hash.
	_, err = raw.Exec(`CREATE TABLE snapshots (
		id            INTEGER PRIMARY KEY,
		kind          TEXT NOT NULL,
		content_hash  TEXT NOT NULL,
		snapshot_json TEXT NOT NULL,
		project_id    TEXT,
		database_id   TEXT
	)`)
	if err != nil {
		t.Fatalf("seed foreign schema: %v", err)
	}
	raw.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open foreign store should not error: %v", err)
	}
	defer s.Close()

	if s.Compat() != CompatLegacy {
		t.Errorf("foreign store: got Compat %v, want legacy", s.Compat())
	}
	// Open must not have run migrate against the foreign file.
	if tableExists(t, path, "planner_stats") {
		t.Error("Open created planner_stats inside a foreign store; it must be left untouched")
	}
}

// TestOpenNewerStoreIsCompatNewer: a store stamped with a user_version above
// this build's HistorySchemaVersion was written by a newer dryrun. Open must
// report CompatNewer rather than silently treating it as its own, since a
// newer build may have changed the payload format.
func TestOpenNewerStoreIsCompatNewer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "newer.db")

	// first open creates a normal Go store...
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// ...then we forge a version from the future and reopen.
	if _, err := s.db.Exec("PRAGMA user_version = 999"); err != nil {
		t.Fatalf("bump user_version: %v", err)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if s2.Compat() != CompatNewer {
		t.Errorf("future-versioned store: got Compat %v, want newer", s2.Compat())
	}
}

// TestOpenAdoptsPreMarkerGoStore: a Go-shaped store from before the
// user_version marker existed has the right tables but user_version 0. Open
// must recognise it as its own (db_url_hash present), adopt it by stamping the
// current version, and report CompatOK — never CompatLegacy.
func TestOpenAdoptsPreMarkerGoStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "premarker.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// simulate a pre-marker Go store: correct tables, but no version stamp.
	if _, err := s.db.Exec("PRAGMA user_version = 0"); err != nil {
		t.Fatalf("clear user_version: %v", err)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if s2.Compat() != CompatOK {
		t.Errorf("pre-marker Go store: got Compat %v, want ok", s2.Compat())
	}
	if got := userVersion(t, s2); got != HistorySchemaVersion {
		t.Errorf("pre-marker store not re-stamped: user_version %d, want %d", got, HistorySchemaVersion)
	}
}
