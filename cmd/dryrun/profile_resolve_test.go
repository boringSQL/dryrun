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
	prevDB, prevProfile, prevConfig := flagDB, flagProfile, flagConfig
	prevMasksFile, prevMaskPolicy, prevNoMasks := flagMasksFile, flagMaskPolicy, flagNoMasks
	flagDB, flagProfile, flagConfig = "", "", ""
	flagMasksFile, flagMaskPolicy, flagNoMasks = "", nil, false
	t.Cleanup(func() {
		flagDB, flagProfile, flagConfig = prevDB, prevProfile, prevConfig
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

// Several profiles and none selected makes ResolveProfile fail. The fallback
// must still honour [project].id: keying off the directory name instead would
// point mcp-serve at a different history than every capture command wrote to.
func TestResolveSnapshotKeyAmbiguousProfileKeepsProjectID(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"

[profiles.staging]
db_url = "postgres://stg/x"

[profiles.prod]
db_url = "postgres://prod/x"
`)
	withCWD(t, dir)

	key := resolveSnapshotKey()
	if string(key.ProjectID) != "demo" || string(key.DatabaseID) != "demo" {
		t.Errorf("got %+v, want demo/demo", key)
	}
}

// TestBuildMaskerNoMasks: --no-masks is the hard opt-out. buildMasker must
// short-circuit to nil before touching the filesystem, even when a perfectly
// good data-masking-policy.yml is sitting right there.
func TestBuildMaskerNoMasks(t *testing.T) {
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
	p, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "dev"})
	if err != nil {
		t.Fatalf("--no-masks should not error: %v", err)
	}
	if p != nil {
		t.Errorf("--no-masks must short-circuit to nil, got %v", p)
	}
}

// TestBuildMaskerDiscovers: with no --masks-file flag and no masks_file in the
// profile, buildMasker falls through to auto-discovery and picks up a
// data-masking-policy.yml in the working directory.
func TestBuildMaskerDiscovers(t *testing.T) {
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
	m, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "dev"})
	if err != nil {
		t.Fatalf("buildMasker: %v", err)
	}
	if !m.IsSensitive("public", "users", "email") {
		t.Error("discovered masks file should mark users.email sensitive")
	}
}

// TestBuildMaskerNoFileNoRequire: when no masks file resolves and
// require_masks is unset, buildMasker must succeed with a nil Policy so
// `dryrun init` keeps working out of the box. Operators who want the
// refuse-to-capture-unmasked guard opt in via require_masks=true (covered by
// TestBuildMaskerRequireMasksRefusesMissingFile).
func TestBuildMaskerNoFileNoRequire(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
[profiles.dev]
db_url = "postgres://dev/x"
`)
	withCWD(t, dir)

	flagProfile = "dev"
	p, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "dev"})
	if err != nil {
		t.Fatalf("no-file + no-require should not error: %v", err)
	}
	if p != nil {
		t.Errorf("no resolved masks file must yield a nil Policy, got %v", p)
	}
}

// TestBuildMaskerCLIOverridesProfile: when both --masks-file and the profile's
// masks_file are set, the flag wins. The two files list different columns so
// IsSensitive on the resolved Policy reveals which file actually loaded.
func TestBuildMaskerCLIOverridesProfile(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()
	writeMasks(t, dir, "profile-masks.yml", `version: 1
databases:
  dev:
    columns:
      users.profile_col: { expr: "x", tags: [pii] }
`)
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
	m, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "dev"})
	if err != nil {
		t.Fatalf("buildMasker: %v", err)
	}
	pol := m
	if !pol.IsSensitive("public", "users", "cli_col") {
		t.Error("--masks-file should win: cli_col is not masked")
	}
	if pol.IsSensitive("public", "users", "profile_col") {
		t.Error("profile masks_file should have been overridden by --masks-file")
	}
}

// TestBuildMaskerPerDatabase: two keys sharing one masks file but carrying
// different database_ids must each select their own block. Guards against a
// multi-database setup cross-contaminating one database's stats with
// another's masking rules.
func TestBuildMaskerPerDatabase(t *testing.T) {
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

	mA, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "db_a"})
	if err != nil {
		t.Fatalf("buildMasker(db_a): %v", err)
	}
	polA := mA
	if !polA.IsSensitive("public", "accounts", "ssn") {
		t.Error("db_a key should resolve the db_a block (accounts.ssn)")
	}
	if polA.IsSensitive("public", "leads", "email") {
		t.Error("db_a key must not see db_b's columns")
	}

	mB, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "db_b"})
	if err != nil {
		t.Fatalf("buildMasker(db_b): %v", err)
	}
	polB := mB
	if !polB.IsSensitive("public", "leads", "email") {
		t.Error("db_b key should resolve the db_b block (leads.email)")
	}
	if polB.IsSensitive("public", "accounts", "ssn") {
		t.Error("db_b key must not see db_a's columns")
	}
}

// TestBuildMaskerProfileMasksFileSurvivesDBOverride: integration regression
// for the bug where a profile's masks_file was silently ignored whenever --db
// was supplied. `dryrun init` always runs with --db, so this is the normal
// case. The refactor made the bug structurally impossible (buildMasker
// resolves the profile by name only, never threading flagDB), but the
// end-to-end coverage stays as a guardrail against future shortcuts.
//
// Fixture names the masks file "profile-masks.yml" so auto-discovery cannot
// mask the bug — the profile's masks_file is the only path that can produce
// a real Policy.
func TestBuildMaskerProfileMasksFileSurvivesDBOverride(t *testing.T) {
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

	flagDB = "postgres://override/x"
	flagProfile = "prod"

	m, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "prod"})
	if err != nil {
		t.Fatalf("buildMasker: %v", err)
	}
	if !m.IsSensitive("public", "users", "email") {
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

// TestBuildMaskerRequireMasksRefusesNoMasks: require_masks=true in dryrun.toml
// is a project-level safety assertion that init must mask. --no-masks would
// silently bypass the assertion, so buildMasker rejects it loudly. The check
// fires before any resolution / discovery work and even when a perfectly valid
// masks file is sitting next to dryrun.toml — the toggle is what's contested,
// not the file's existence.
func TestBuildMaskerRequireMasksRefusesNoMasks(t *testing.T) {
	resetFlags(t)
	dir := t.TempDir()
	writeMasks(t, dir, "data-masking-policy.yml", `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	writeTOML(t, dir, `
require_masks = true

[profiles.dev]
db_url = "postgres://dev/x"
`)
	withCWD(t, dir)

	flagProfile = "dev"
	flagNoMasks = true
	_, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "dev"})
	if err == nil {
		t.Fatal("require_masks=true must refuse --no-masks")
	}
}

// TestBuildMaskerRequireMasksRefusesMissingFile: with require_masks=true and
// no masks file resolvable from flag / profile / discovery, init must fail
// with the stricter message that names the project-level setting. The default
// (require_masks unset) already fails in this case, but the wording is
// different — this test pins the stricter branch.
func TestBuildMaskerRequireMasksRefusesMissingFile(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), `
require_masks = true

[profiles.dev]
db_url = "postgres://dev/x"
`)
	withCWD(t, dir)

	flagProfile = "dev"
	_, err := buildMasker(history.SnapshotKey{ProjectID: "demo", DatabaseID: "dev"})
	if err == nil {
		t.Fatal("require_masks=true must refuse missing masks file")
	}
	if !contains(err.Error(), "require_masks") {
		t.Errorf("error should name require_masks: %v", err)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
