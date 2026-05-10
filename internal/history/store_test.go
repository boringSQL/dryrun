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
