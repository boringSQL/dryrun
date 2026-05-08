package history

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testSnapshot(hash, db string) *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    db,
		Timestamp:   time.Now().UTC(),
		ContentHash: hash,
		Tables: []schema.Table{
			{Schema: "public", Name: "users"},
		},
	}
}

func TestSaveAndLoadSnapshot(t *testing.T) {
	store := testStore(t)
	snap := testSnapshot("abc123", "testdb")

	saved, err := store.SaveSnapshot("postgres://localhost/testdb", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !saved {
		t.Error("expected save to succeed (new snapshot)")
	}

	loaded, err := store.LoadSnapshot("abc123")
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("expected to load snapshot")
	}
	if loaded.Database != "testdb" {
		t.Errorf("got database %q, want testdb", loaded.Database)
	}
}

func TestSaveSkipsDuplicate(t *testing.T) {
	store := testStore(t)
	snap := testSnapshot("dup_hash", "testdb")

	saved1, _ := store.SaveSnapshot("postgres://localhost/testdb", snap)
	if !saved1 {
		t.Error("first save should succeed")
	}

	saved2, _ := store.SaveSnapshot("postgres://localhost/testdb", snap)
	if saved2 {
		t.Error("second save with same hash should be skipped")
	}
}

func TestListSnapshots(t *testing.T) {
	store := testStore(t)
	dbURL := "postgres://localhost/listdb"

	for i := 0; i < 3; i++ {
		snap := testSnapshot(time.Now().Format(time.RFC3339Nano), "listdb")
		time.Sleep(time.Millisecond) // ensure unique timestamps
		store.SaveSnapshot(dbURL, snap)
	}

	summaries, err := store.ListSnapshots(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 3 {
		t.Errorf("expected 3 snapshots, got %d", len(summaries))
	}
}

func TestLatestSnapshot(t *testing.T) {
	store := testStore(t)
	dbURL := "postgres://localhost/latestdb"

	snap1 := testSnapshot("first", "latestdb")
	snap1.Timestamp = time.Now().UTC().Add(-time.Hour)
	store.SaveSnapshot(dbURL, snap1)

	snap2 := testSnapshot("second", "latestdb")
	snap2.Timestamp = time.Now().UTC()
	store.SaveSnapshot(dbURL, snap2)

	latest, err := store.LatestSnapshot(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil {
		t.Fatal("expected latest snapshot")
	}
	if latest.ContentHash != "second" {
		t.Errorf("got hash %q, want second", latest.ContentHash)
	}
}

func TestLoadNonexistentSnapshot(t *testing.T) {
	store := testStore(t)
	snap, err := store.LoadSnapshot("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if snap != nil {
		t.Error("expected nil for nonexistent snapshot")
	}
}

func TestDefaultHistoryPath(t *testing.T) {
	path, err := DefaultHistoryPath()
	if err != nil {
		t.Fatal(err)
	}
	cwd, _ := os.Getwd()
	expected := filepath.Join(cwd, ".dryrun", "history.db")
	if path != expected {
		t.Errorf("got %q, want %q", path, expected)
	}
}
