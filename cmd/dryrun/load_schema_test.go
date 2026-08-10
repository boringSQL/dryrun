package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// seedHistorySchema puts a one-table snapshot into dir/.dryrun/history.db under
// key and returns the table name it wrote.
func seedHistorySchema(t *testing.T, dir string, key history.SnapshotKey, table string) {
	t.Helper()
	store, err := history.Open(filepath.Join(dir, ".dryrun", "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	snap := &schema.SchemaSnapshot{
		Database:    "seeded",
		PgVersion:   "16.4",
		ContentHash: "hist-" + table,
		Tables:      []schema.Table{{Schema: "public", Name: table}},
	}
	if _, err := store.PutSchema(context.Background(), key, snap); err != nil {
		t.Fatal(err)
	}
}

// lint/drift read the schema from history.db. Before v0.15 they read
// .dryrun/schema.json, and a profile's schema_file was consulted first.
func TestLoadSchemaForLintReadsHistory(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"

[profiles.local]
db_url = "postgres://unused/x"

[default]
profile = "local"
`)
	seedHistorySchema(t, dir, history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}, "t_from_history")
	withCWD(t, dir)

	snap, err := loadSchemaForLint()
	if err != nil {
		t.Fatalf("loadSchemaForLint: %v", err)
	}
	if len(snap.Tables) != 1 || snap.Tables[0].Name != "t_from_history" {
		t.Errorf("got %+v, want the snapshot from history.db", snap.Tables)
	}
}

// A leftover .dryrun/schema.json must not win over history.db. The profile here
// still carries schema_file, now an unknown key: a config written before v0.15
// has to keep loading, and the key has to stay inert.
func TestLoadSchemaForLintIgnoresSchemaFile(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"

[profiles.local]
schema_file = ".dryrun/schema.json"

[default]
profile = "local"
`)
	seedHistorySchema(t, dir, history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}, "t_from_history")
	stale := `{"database":"stale","tables":[{"schema":"public","name":"t_from_file"}]}`
	if err := os.WriteFile(filepath.Join(dir, ".dryrun", "schema.json"), []byte(stale), 0o644); err != nil {
		t.Fatal(err)
	}
	withCWD(t, dir)

	snap, err := loadSchemaForLint()
	if err != nil {
		t.Fatalf("loadSchemaForLint: %v", err)
	}
	if len(snap.Tables) != 1 || snap.Tables[0].Name != "t_from_history" {
		t.Errorf("got %+v, want history.db to win over schema.json", snap.Tables)
	}
}

// `drift` compares loadSavedSchema against a live introspection of the same
// database. If the saved side could fall back to a live connection, drift would
// compare live against live and report a clean bill of health forever.
func TestLoadSavedSchemaNeverGoesLive(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"
`)
	withCWD(t, dir)
	// a URL that would fail loudly if anything tried to dial it
	flagDB = "postgres://drift-must-not-connect@127.0.0.1:1/x"

	_, err := loadSavedSchema()
	if err == nil {
		t.Fatal("expected an error; loadSavedSchema must not fall back to --db")
	}
	// a live fallback would fail too (connection refused), so pin the message:
	// only the history-miss error proves no dial was attempted
	if !strings.Contains(err.Error(), "no schema snapshot") {
		t.Errorf("expected the history-miss error, got: %v", err)
	}
}

// With no snapshot and no --db, the error has to name the key it looked under
// and the two ways to get one, since `import` no longer exists.
func TestLoadSchemaForLintNoSourceError(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"
`)
	withCWD(t, dir)

	_, err := loadSchemaForLint()
	if err == nil {
		t.Fatal("expected an error with no snapshot and no --db")
	}
	for _, want := range []string{"project=demo", "database=demo", "snapshot pull", "init"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

