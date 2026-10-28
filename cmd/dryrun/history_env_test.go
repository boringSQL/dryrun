package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func seedHistoryAt(t *testing.T, path string, key history.SnapshotKey, table string) {
	t.Helper()
	store, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snap := &schema.SchemaSnapshot{
		Database:    "seeded",
		PgVersion:   "16.4",
		ContentHash: "env-" + table,
		Tables:      []schema.Table{{Schema: "public", Name: table}},
	}
	if _, err := store.PutSchema(context.Background(), key, snap); err != nil {
		t.Fatal(err)
	}
}

func assertNoDotDryrun(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, ".dryrun")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s/.dryrun must not exist (stat err: %v)", dir, err)
	}
}

// A server spawned in a PR checkout (no .dryrun/) reads the shared db named by
// DRYRUN_HISTORY_DB.
func TestHistoryEnvReadsSharedDBFromForeignCWD(t *testing.T) {
	resetFlags(t)
	shared := filepath.Join(t.TempDir(), "history.db")
	seedHistoryAt(t, shared, history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}, "t_shared")

	checkout := writeTOML(t, t.TempDir(), `
[project]
id = "demo"
`)
	withCWD(t, checkout)
	t.Setenv(history.HistoryDBEnv, shared)

	snap, err := loadSavedSchema()
	if err != nil {
		t.Fatalf("loadSavedSchema: %v", err)
	}
	if len(snap.Tables) != 1 || snap.Tables[0].Name != "t_shared" {
		t.Errorf("got %+v, want the snapshot from %s", snap.Tables, shared)
	}
	assertNoDotDryrun(t, checkout)
}

// A missing env target means uninitialized, and nothing gets created --
// neither the file nor a .dryrun/ in cwd.
func TestHistoryEnvMissingFileCreatesNothing(t *testing.T) {
	resetFlags(t)
	missing := filepath.Join(t.TempDir(), "not-yet", "history.db")
	cwd := t.TempDir()
	withCWD(t, cwd)
	t.Setenv(history.HistoryDBEnv, missing)

	_, err := openExistingHistoryStore()
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want ErrNotExist (uninitialized mode), got %v", err)
	}
	if _, serr := os.Stat(filepath.Dir(missing)); !errors.Is(serr, os.ErrNotExist) {
		t.Errorf("parent dir of %s was created", missing)
	}
	assertNoDotDryrun(t, cwd)
}

// A relative value must not read as ErrNotExist; mcp-serve prints "unusable
// (<reason>)" for other errors, so the operator sees why.
func TestHistoryEnvRelativeIsNotSilentlyMissing(t *testing.T) {
	resetFlags(t)
	cwd := t.TempDir()
	withCWD(t, cwd)
	t.Setenv(history.HistoryDBEnv, "~/dryrun/history.db")

	_, err := openExistingHistoryStore()
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want a non-ErrNotExist error naming %s, got %v", history.HistoryDBEnv, err)
	}
	if !strings.Contains(err.Error(), history.HistoryDBEnv) {
		t.Errorf("error should name %s: %v", history.HistoryDBEnv, err)
	}
}

// --history-db beats the env var on a real read path.
func TestHistoryFlagBeatsEnv(t *testing.T) {
	resetFlags(t)
	key := history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}
	envDB := filepath.Join(t.TempDir(), "env.db")
	flagDB := filepath.Join(t.TempDir(), "flag.db")
	seedHistoryAt(t, envDB, key, "t_env")
	seedHistoryAt(t, flagDB, key, "t_flag")

	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"
`)
	withCWD(t, dir)
	t.Setenv(history.HistoryDBEnv, envDB)

	a, err := loadAnnotatedForCheck(context.Background(), flagDB)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.Schema.Tables; len(got) != 1 || got[0].Name != "t_flag" {
		t.Errorf("got %+v, want t_flag from --history-db", got)
	}
}

// Captures sharing one db contend for one lock, whatever cwd each started in.
func TestCaptureLockFollowsHistoryEnv(t *testing.T) {
	resetFlags(t)
	shared := filepath.Join(t.TempDir(), "history.db")
	t.Setenv(history.HistoryDBEnv, shared)
	if got, want := captureLockPath(""), shared+".capture.lock"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := captureLockPath("/flag/h.db"), "/flag/h.db.capture.lock"; got != want {
		t.Errorf("--history-db: got %q, want %q", got, want)
	}
	t.Setenv(history.HistoryDBEnv, "")
	if got, want := captureLockPath(""), filepath.Join(".dryrun", "capture.lock"); got != want {
		t.Errorf("no env: got %q, want %q", got, want)
	}
}

// pull/capture create the db at the env path, not under cwd.
func TestHistoryEnvWritersCreateAtEnvPath(t *testing.T) {
	resetFlags(t)
	target := filepath.Join(t.TempDir(), "cache", "history.db")
	cwd := t.TempDir()
	withCWD(t, cwd)
	t.Setenv(history.HistoryDBEnv, target)

	s, err := openHistoryStore("")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := os.Stat(target); err != nil {
		t.Errorf("db not created at %s: %v", target, err)
	}
	assertNoDotDryrun(t, cwd)
}

func TestHistoryEnvNamedInMissingSnapshotError(t *testing.T) {
	resetFlags(t)
	shared := filepath.Join(t.TempDir(), "history.db")
	seedHistoryAt(t, shared, history.SnapshotKey{ProjectID: "other", DatabaseID: "other"}, "t")

	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"
`)
	withCWD(t, dir)
	t.Setenv(history.HistoryDBEnv, shared)

	_, err := loadSavedSchema()
	if err == nil {
		t.Fatal("expected a missing-snapshot error")
	}
	if !strings.Contains(err.Error(), shared) {
		t.Errorf("error should name %s: %v", shared, err)
	}
}
