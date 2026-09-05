package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
)

// TestOpenHistoryStoreSurfacesLegacyRefusal: history.Open refuses a rust-era
// file, and the CLI opener must pass that error (with its remedy) through
// untouched -- before Open refused, commands died deep in a query with a bare
// "no such table: query_stats".
func TestOpenHistoryStoreSurfacesLegacyRefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	// Rust-shaped db: snapshots table without db_url_hash.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("create raw db: %v", err)
	}
	_, err = raw.Exec(`CREATE TABLE snapshots (
		id            INTEGER PRIMARY KEY,
		kind          TEXT NOT NULL,
		content_hash  TEXT NOT NULL,
		snapshot_json TEXT NOT NULL,
		project_id    TEXT,
		database_id   TEXT
	)`)
	if err != nil {
		t.Fatalf("seed rust schema: %v", err)
	}
	raw.Close()

	s, err := openHistoryStore(path)
	if err == nil {
		s.Close()
		t.Fatal("legacy history.db must fail at open")
	}
	if !strings.Contains(err.Error(), "created by an older dryrun") {
		t.Errorf("error should name the cause, got: %v", err)
	}
	if !strings.Contains(err.Error(), "move it aside") {
		t.Errorf("error should give the working remedy (init/pull crash against the legacy file too), got: %v", err)
	}
}

// TestOpenHistoryStoreNewerWarnsButOpens: a db from a newer dryrun is mostly
// readable (additive evolution), so it warns on stderr and continues rather
// than refusing every command.
func TestOpenHistoryStoreNewerWarnsButOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	s, err := history.Open(path)
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	s.Close()

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen raw: %v", err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 999"); err != nil {
		t.Fatalf("forge future version: %v", err)
	}
	raw.Close()

	s2, err := openHistoryStore(path)
	if err != nil {
		t.Fatalf("newer db should warn, not fail: %v", err)
	}
	defer s2.Close()
	if s2.Compat() != history.CompatNewer {
		t.Errorf("got Compat %v, want newer", s2.Compat())
	}
}
