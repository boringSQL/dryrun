package main

import (
	"os"
	"path/filepath"
	"testing"
)

// withCWD chdirs into dir for the duration of the test and restores cwd on
// cleanup. Config discovery walks up from cwd, so tests that want to pin a
// specific dryrun.toml must control where they run from.
func withCWD(t *testing.T, dir string) {
	t.Helper()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })
}

// resetFlags restores the package-level CLI flag globals so test cases do not
// bleed state into each other. cobra wires these via PersistentFlags so they
// stay set between command runs in-process.
func resetFlags(t *testing.T) {
	t.Helper()
	prevDB, prevProfile, prevConfig, prevSchema := flagDB, flagProfile, flagConfig, flagSchemaFile
	flagDB, flagProfile, flagConfig, flagSchemaFile = "", "", "", ""
	t.Cleanup(func() {
		flagDB, flagProfile, flagConfig, flagSchemaFile = prevDB, prevProfile, prevConfig, prevSchema
	})
	os.Unsetenv("PROFILE")
	os.Unsetenv("DATABASE_URL")
}

// writeTOML drops a dryrun.toml + .git marker into dir so config.Discover
// stops there. Returns dir for chaining.
func writeTOML(t *testing.T, dir, body string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "dryrun.toml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestRequireDBFromProfile: when --db is empty but --profile points at a
// profile with db_url, requireDB resolves through the profile rather than
// erroring out. This is the L5 wiring that lets `dryrun --profile staging
// drift` work without re-typing the connection string.
func TestRequireDBFromProfile(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[profiles.staging]
db_url = "postgres://stg/x"
database_id = "stg-a"

[profiles.dev]
db_url = "postgres://dev/x"
`)
	withCWD(t, dir)

	flagProfile = "staging"
	got, err := requireDB()
	if err != nil {
		t.Fatal(err)
	}
	if got != "postgres://stg/x" {
		t.Errorf("got %q, want postgres://stg/x", got)
	}
}

// TestRequireDBCLIOverridesProfile: --db beats --profile even when both
// resolve to a connection string. Matches the documented precedence ladder.
func TestRequireDBCLIOverridesProfile(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[profiles.staging]
db_url = "postgres://stg/x"
`)
	withCWD(t, dir)

	flagDB = "postgres://override/x"
	flagProfile = "staging"
	got, err := requireDB()
	if err != nil {
		t.Fatal(err)
	}
	if got != "postgres://override/x" {
		t.Errorf("--db should win: got %q", got)
	}
}

// TestRequireDBMissingProfile: a typo'd profile name plus no --db must error
// rather than silently falling back to "" (which would leak through to a
// pgx.Connect call and produce a confusing error downstream).
func TestRequireDBMissingProfile(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[profiles.staging]
db_url = "postgres://stg/x"
`)
	withCWD(t, dir)

	flagProfile = "stagign" // typo
	if _, err := requireDB(); err == nil {
		t.Fatal("expected error for missing profile")
	}
}

// TestResolveSnapshotKeyFromProfile: `dryrun --profile staging` snapshots
// must land under (project_id, staging-shard-a), not the bare project id.
// Otherwise history.db reads/writes drift between commands invoked with and
// without the flag.
func TestResolveSnapshotKeyFromProfile(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"

[profiles.staging]
db_url = "postgres://stg/x"
database_id = "staging-shard-a"
`)
	withCWD(t, dir)

	flagProfile = "staging"
	key := resolveSnapshotKey()
	if string(key.ProjectID) != "demo" || string(key.DatabaseID) != "staging-shard-a" {
		t.Errorf("got %+v, want demo/staging-shard-a", key)
	}
}
