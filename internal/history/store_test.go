package history

import (
	"context"
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

// TestListKeysReturnsDistinctKeyedRows seeds two streams with multiple
// snapshots each plus a legacy NULL-keyed row, then asserts ListKeys returns
// exactly the two real (project, database) pairs in stable sorted order. The
// legacy row must be skipped because it can't address a stream.
func TestListKeysReturnsDistinctKeyedRows(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	seed := func(k SnapshotKey, hashes ...string) {
		for i, h := range hashes {
			s := testSnapshot(h, string(k.DatabaseID))
			s.Timestamp = now.Add(time.Duration(i) * time.Minute)
			if _, err := store.Put(ctx, k, s); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed(key("acme", "primary"), "h-a1", "h-a2")
	seed(key("zeta", "replica"), "h-z1")

	// legacy row predates project/database columns — must not appear in ListKeys
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO snapshots (db_url_hash, timestamp, content_hash, database_name, snapshot_json)
		 VALUES (?, ?, ?, ?, ?)`,
		"legacy-hash", now.Format(time.RFC3339), "h-legacy", "old", "{}"); err != nil {
		t.Fatal(err)
	}

	got, err := store.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := []SnapshotKey{key("acme", "primary"), key("zeta", "replica")}
	if len(got) != len(want) {
		t.Fatalf("got %d keys (%+v), want %d (%+v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("keys[%d]: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestListKeysEmpty asserts that an empty history.db produces a nil/empty
// slice with no error — export against a virgin store must succeed and emit
// nothing rather than panic on a missing row.
func TestListKeysEmpty(t *testing.T) {
	store := testStore(t)
	got, err := store.ListKeys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want empty", got)
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
