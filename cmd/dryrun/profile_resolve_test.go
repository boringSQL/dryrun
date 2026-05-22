package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
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
	prevMasksFile, prevMaskPolicy, prevNoMasks := flagMasksFile, flagMaskPolicy, flagNoMasks
	flagDB, flagProfile, flagConfig, flagSchemaFile = "", "", "", ""
	flagMasksFile, flagMaskPolicy, flagNoMasks = "", nil, false
	t.Cleanup(func() {
		flagDB, flagProfile, flagConfig, flagSchemaFile = prevDB, prevProfile, prevConfig, prevSchema
		flagMasksFile, flagMaskPolicy, flagNoMasks = prevMasksFile, prevMaskPolicy, prevNoMasks
	})
	os.Unsetenv("PROFILE")
	os.Unsetenv("DATABASE_URL")
}

// writeMasks drops a masks YAML file at dir/name and returns its path. It is
// the data-masking counterpart of writeTOML — the resolveMaskPolicy tests need
// a real file on disk because masking.LoadSharedMasks reads from the
// filesystem rather than from an in-memory string.
func writeMasks(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
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

// TestResolveMaskPolicyNoMasks: the --no-masks flag is a hard opt-out and must
// short-circuit before any file is read. Even with a perfectly good
// data-masking-policy.yml sitting right there in the working directory,
// resolveMaskPolicy must return a nil Policy — nil is the contract for
// "masking disabled", and ApplyPlanner treats a nil Policy as a no-op.
func TestResolveMaskPolicyNoMasks(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()
	writeMasks(t, dir, "data-masking-policy.yml", `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	writeTOML(t, dir, `
[profiles.dev]
db_url = "postgres://dev/x"
`)
	withCWD(t, dir)

	flagProfile = "dev"
	flagNoMasks = true
	pol, err := resolveMaskPolicy()
	if err != nil {
		t.Fatalf("--no-masks should not error: %v", err)
	}
	if pol != nil {
		t.Error("--no-masks must short-circuit to a nil Policy, even when a masks file exists")
	}
}

// TestResolveMaskPolicyDiscovers: with no --masks-file flag and no masks_file
// key in the profile, resolveMaskPolicy falls through to auto-discovery and
// picks up a data-masking-policy.yml found in the working directory. The
// profile here has no explicit database_id, so it defaults to the profile
// name ("dev") — which is exactly the block the fixture defines.
func TestResolveMaskPolicyDiscovers(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()
	writeMasks(t, dir, "data-masking-policy.yml", `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	writeTOML(t, dir, `
[profiles.dev]
db_url = "postgres://dev/x"
`)
	withCWD(t, dir)

	flagProfile = "dev"
	pol, err := resolveMaskPolicy()
	if err != nil {
		t.Fatalf("resolveMaskPolicy: %v", err)
	}
	if pol == nil {
		t.Fatal("expected discovery to find data-masking-policy.yml")
	}
	if !pol.IsSensitive("public", "users", "email") {
		t.Error("discovered masks file should mark users.email sensitive")
	}
}

// TestResolveMaskPolicyCLIOverridesProfile: when both an explicit --masks-file
// flag and a profile masks_file are present, the CLI flag wins. The two files
// list different columns (cli_col vs profile_col), so the resolved Policy's
// IsSensitive answers reveal unambiguously which file was actually loaded.
func TestResolveMaskPolicyCLIOverridesProfile(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()

	// the file the profile points at — masks users.profile_col
	writeMasks(t, dir, "profile-masks.yml", `version: 1
databases:
  dev:
    columns:
      users.profile_col: { expr: "x", tags: [pii] }
`)
	// the file the CLI flag points at — masks users.cli_col
	cliPath := writeMasks(t, dir, "cli-masks.yml", `version: 1
databases:
  dev:
    columns:
      users.cli_col: { expr: "x", tags: [pii] }
`)
	writeTOML(t, dir, `
[profiles.dev]
db_url = "postgres://dev/x"
masks_file = "profile-masks.yml"
`)
	withCWD(t, dir)

	flagProfile = "dev"
	flagMasksFile = cliPath
	pol, err := resolveMaskPolicy()
	if err != nil {
		t.Fatalf("resolveMaskPolicy: %v", err)
	}
	if pol == nil {
		t.Fatal("expected a Policy")
	}
	if !pol.IsSensitive("public", "users", "cli_col") {
		t.Error("--masks-file should win: cli_col is not masked")
	}
	if pol.IsSensitive("public", "users", "profile_col") {
		t.Error("profile masks_file should have been overridden by --masks-file")
	}
}

// TestResolveMaskPolicyForKeyPerDatabase: the per-key resolver is what the
// snapshot-export loop calls once per database it is exporting. Two keys that
// share one masks file but carry different database_ids must each select their
// own block — db_a's policy masks accounts.ssn and nothing else, db_b's masks
// leads.email and nothing else. This guards against a multi-database export
// cross-contaminating one database's stats with another's masking rules.
func TestResolveMaskPolicyForKeyPerDatabase(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()
	writeMasks(t, dir, "data-masking-policy.yml", `version: 1
databases:
  db_a:
    columns:
      accounts.ssn: { expr: "x", tags: [pii] }
  db_b:
    columns:
      leads.email: { expr: "x", tags: [pii] }
`)
	writeTOML(t, dir, `
[project]
id = "demo"

[profiles.a]
db_url = "postgres://a/x"
database_id = "db_a"

[profiles.b]
db_url = "postgres://b/x"
database_id = "db_b"
`)
	withCWD(t, dir)

	keyA := history.SnapshotKey{ProjectID: "demo", DatabaseID: "db_a"}
	keyB := history.SnapshotKey{ProjectID: "demo", DatabaseID: "db_b"}

	polA, err := resolveMaskPolicyForKey(keyA)
	if err != nil {
		t.Fatalf("resolveMaskPolicyForKey(db_a): %v", err)
	}
	if polA == nil || !polA.IsSensitive("public", "accounts", "ssn") {
		t.Error("db_a key should resolve the db_a block (accounts.ssn)")
	}
	if polA != nil && polA.IsSensitive("public", "leads", "email") {
		t.Error("db_a key must not see db_b's columns")
	}

	polB, err := resolveMaskPolicyForKey(keyB)
	if err != nil {
		t.Fatalf("resolveMaskPolicyForKey(db_b): %v", err)
	}
	if polB == nil || !polB.IsSensitive("public", "leads", "email") {
		t.Error("db_b key should resolve the db_b block (leads.email)")
	}
	if polB != nil && polB.IsSensitive("public", "accounts", "ssn") {
		t.Error("db_b key must not see db_a's columns")
	}
}

// TestResolveMaskPolicyProfileMasksFileSurvivesDBOverride is the regression
// guard for the bug where a profile's masks_file was silently ignored whenever
// --db was also supplied. This combination is not a corner case: `dryrun init`
// only captures when --db (or DATABASE_URL) is set, so for the init command it
// is the *normal* case. Under the bug, config.ResolveProfile sees a non-nil
// cliDB, short-circuits to a bare "<cli>" profile, and the named profile's
// masks_file/mask_policies never reach resolveMaskPolicyForKey.
//
// The fixture deliberately names the masks file "profile-masks.yml" rather
// than the canonical "data-masking-policy.yml" — that keeps it OUT of reach of
// auto-discovery, so the profile's masks_file key is the *only* path that can
// produce a non-nil policy. Without that precaution, discovery would mask the
// bug: the test would pass via the discovery fallback even with the profile
// path broken.
func TestResolveMaskPolicyProfileMasksFileSurvivesDBOverride(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()
	writeMasks(t, dir, "profile-masks.yml", `version: 1
databases:
  prod:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	writeTOML(t, dir, `
[project]
id = "demo"

[profiles.prod]
db_url = "postgres://prod/x"
database_id = "prod"
masks_file = "profile-masks.yml"
`)
	withCWD(t, dir)

	// --db set, exactly as `dryrun init` always runs it, plus the profile.
	flagDB = "postgres://override/x"
	flagProfile = "prod"

	pol, err := resolveMaskPolicyForKey(history.SnapshotKey{ProjectID: "demo", DatabaseID: "prod"})
	if err != nil {
		t.Fatalf("resolveMaskPolicyForKey: %v", err)
	}
	if pol == nil {
		t.Fatal("profile masks_file must still resolve when --db is supplied")
	}
	if !pol.IsSensitive("public", "users", "email") {
		t.Error("expected the profile's masks_file (users.email) to apply")
	}
}

// TestResolveSnapshotKeyDBOverrideKeepsProfileDatabaseID is the regression
// guard for the second facet of the --db-override bug: a profile's explicit
// database_id was discarded whenever --db was also supplied, so snapshots
// landed under (project, project) instead of (project, database_id). For
// `dryrun init` that is the normal case, since init always runs with --db.
// Under the bug, config.ResolveProfile saw a non-nil cliDB, returned a bare
// "<cli>" profile with no database_id, and SnapshotKey() fell back to the
// project id — silently routing reads and writes to the wrong history bucket.
//
// resolveSnapshotKey feeds every history-touching command (init, snapshot
// take/list/diff, stats apply, mcp-serve), so a divergent key here means a
// command run with --db cannot see snapshots a command run without it stored,
// and vice versa.
func TestResolveSnapshotKeyDBOverrideKeepsProfileDatabaseID(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"

[profiles.staging]
db_url = "postgres://stg/x"
database_id = "staging-shard-a"
`)
	withCWD(t, dir)

	// --db set, exactly as `dryrun init` always runs it, plus the profile.
	flagDB = "postgres://override/x"
	flagProfile = "staging"

	key := resolveSnapshotKey()
	if string(key.ProjectID) != "demo" {
		t.Errorf("project id: got %q, want demo", key.ProjectID)
	}
	if string(key.DatabaseID) != "staging-shard-a" {
		t.Errorf("database id: got %q, want staging-shard-a "+
			"(--db override must not collapse it to the project id)", key.DatabaseID)
	}
}
