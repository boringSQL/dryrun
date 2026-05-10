package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
)

func TestParseFullConfig(t *testing.T) {
	toml := `
[default]
profile = "production"

[profiles.development]
db_url = "${DEV_DATABASE_URL}"

[profiles.staging]
schema_file = ".dryrun/staging-schema.json"

[profiles.production]
schema_file = ".dryrun/schema.json"

[conventions]
table_name = "snake_singular"
column_name = "snake_case"
pk_type = "bigint_identity"
require_timestamps = true
prefer_text_over_varchar = true

[conventions.disabled_rules]
rules = ["naming/table_style"]

[conventions.custom]
table_name_regex = "^[a-z][a-z0-9_]*$"
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Default == nil || cfg.Default.Profile == nil || *cfg.Default.Profile != "production" {
		t.Error("expected default profile = production")
	}
	if len(cfg.Profiles) != 3 {
		t.Errorf("expected 3 profiles, got %d", len(cfg.Profiles))
	}
	if cfg.Conventions == nil {
		t.Fatal("expected conventions")
	}
	if cfg.Conventions.DisabledRules == nil || len(cfg.Conventions.DisabledRules.Rules) != 1 {
		t.Error("expected 1 disabled rule")
	}
}

func TestParseEmptyConfig(t *testing.T) {
	cfg, err := Parse("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Default != nil {
		t.Error("expected nil default")
	}
}

func TestParseInvalidConfig(t *testing.T) {
	_, err := Parse("not valid toml [[[")
	if err == nil {
		t.Error("expected error")
	}
}

func TestExpandEnvVars(t *testing.T) {
	os.Setenv("DRYRUN_TEST_VAR", "hello")
	defer os.Unsetenv("DRYRUN_TEST_VAR")

	if got := ExpandEnvVars("${DRYRUN_TEST_VAR}"); got != "hello" {
		t.Errorf("got %q, want hello", got)
	}
	if got := ExpandEnvVars("postgres://${DRYRUN_TEST_VAR}:5432/db"); got != "postgres://hello:5432/db" {
		t.Errorf("got %q", got)
	}
}

func TestExpandEnvVarsMissing(t *testing.T) {
	os.Unsetenv("DRYRUN_MISSING_VAR")
	if got := ExpandEnvVars("${DRYRUN_MISSING_VAR}"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// TestProjectIDFromConfig: an explicit [project] id wins over any
// fallback. The basename of project_root is ignored when the user has
// pinned an ID — that's the whole point of letting them set it.
func TestProjectIDFromConfig(t *testing.T) {
	toml := `
[project]
id = "acme-monolith"
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ProjectID("/tmp/some-other-dir"); got != "acme-monolith" {
		t.Errorf("got %q, want acme-monolith", got)
	}
}

// TestProjectIDFallback: with no [project] block, ProjectID falls back to
// the basename of the project root. Same input → same output (stable across
// invocations). Different roots → different IDs.
func TestProjectIDFallback(t *testing.T) {
	cfg, err := Parse("")
	if err != nil {
		t.Fatal(err)
	}

	a1 := cfg.ProjectID("/home/u/projects/acme")
	a2 := cfg.ProjectID("/home/u/projects/acme")
	b := cfg.ProjectID("/home/u/projects/beta")

	if a1 != a2 {
		t.Errorf("ProjectID not stable: %q vs %q", a1, a2)
	}
	if a1 != "acme" {
		t.Errorf("basename fallback: got %q, want acme", a1)
	}
	if a1 == b {
		t.Errorf("different roots collapsed to same ID: %q", a1)
	}
}

// TestProjectIDEmptyBasename: degenerate project roots ("/", "", ".") must
// not produce an empty ProjectID — that would break SnapshotKey lookups.
// We expect the literal "default" sentinel, matching Rust default_project_id.
func TestProjectIDEmptyBasename(t *testing.T) {
	cfg, _ := Parse("")
	for _, root := range []string{"/", "", "."} {
		if got := cfg.ProjectID(root); got != "default" {
			t.Errorf("root %q: got %q, want default", root, got)
		}
	}
}

// TestResolveCLIOverridesPreserveProjectID: --db and --schema both shortcut
// past profile resolution, but they still belong to a project — so the
// resolver must populate ProjectID from project_root and leave DatabaseID
// nil (no profile means no per-profile database_id).
func TestResolveCLIOverridesPreserveProjectID(t *testing.T) {
	cfg, _ := Parse(`[project]
id = "demo"`)
	root := "/tmp/whatever"

	dbURL := "postgres://localhost/x"
	rp, err := cfg.ResolveProfile(&dbURL, nil, nil, root)
	if err != nil {
		t.Fatal(err)
	}
	if rp.ProjectID != "demo" {
		t.Errorf("--db ProjectID: got %q, want demo", rp.ProjectID)
	}
	if rp.DatabaseID != nil {
		t.Errorf("--db DatabaseID: got %v, want nil", rp.DatabaseID)
	}

	schemaPath := "/tmp/schema.json"
	rp, err = cfg.ResolveProfile(nil, &schemaPath, nil, root)
	if err != nil {
		t.Fatal(err)
	}
	if rp.ProjectID != "demo" || rp.DatabaseID != nil {
		t.Errorf("--schema resolve: got (%q, %v), want (demo, nil)", rp.ProjectID, rp.DatabaseID)
	}
}

// TestProfileDatabaseIDRoundTrip: TOML parsing for the new database_id
// field on profiles. When set, ResolveProfile must surface it verbatim;
// when omitted, it must fall back to the profile name (Rust parity).
func TestProfileDatabaseIDRoundTrip(t *testing.T) {
	toml := `
[project]
id = "demo"

[profiles.staging]
db_url = "postgres://stg/x"
database_id = "staging-shard-a"

[profiles.dev]
db_url = "postgres://dev/x"
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}

	staging := "staging"
	rp, err := cfg.ResolveProfile(nil, nil, &staging, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	if rp.DatabaseID == nil || *rp.DatabaseID != "staging-shard-a" {
		t.Errorf("staging DatabaseID: got %v, want staging-shard-a", rp.DatabaseID)
	}

	dev := "dev"
	rp, err = cfg.ResolveProfile(nil, nil, &dev, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	if rp.DatabaseID == nil || *rp.DatabaseID != "dev" {
		t.Errorf("dev DatabaseID (fallback): got %v, want dev", rp.DatabaseID)
	}
}

// TestSnapshotKey: SnapshotKey() is the single bridge from config-land to
// history-land. CLI-only resolves (no DatabaseID) must still produce a
// usable key by mirroring ProjectID — otherwise SnapshotStore reads/writes
// would land under empty-string IDs.
func TestSnapshotKey(t *testing.T) {
	cfg, _ := Parse(`[project]
id = "demo"

[profiles.staging]
db_url = "postgres://stg/x"
database_id = "shard-a"`)

	// profile path: explicit project + database IDs flow through
	staging := "staging"
	rp, _ := cfg.ResolveProfile(nil, nil, &staging, "/tmp/demo")
	k := rp.SnapshotKey()
	want := history.SnapshotKey{ProjectID: "demo", DatabaseID: "shard-a"}
	if k != want {
		t.Errorf("profile key: got %+v, want %+v", k, want)
	}

	// CLI override path: DatabaseID nil → SnapshotKey mirrors ProjectID
	dbURL := "postgres://localhost/x"
	rp, _ = cfg.ResolveProfile(&dbURL, nil, nil, "/tmp/demo")
	k = rp.SnapshotKey()
	want = history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}
	if k != want {
		t.Errorf("CLI-override key: got %+v, want %+v", k, want)
	}
}

// TestResolveMissingProfile: the error returned for a typo'd profile name
// must include the requested name so users can tell which one they
// fat-fingered without re-reading the config.
func TestResolveMissingProfile(t *testing.T) {
	cfg, _ := Parse(`
[profiles.dev]
db_url = "postgres://dev/x"
`)
	bogus := "stagign"
	_, err := cfg.ResolveProfile(nil, nil, &bogus, "/tmp/whatever")
	if err == nil {
		t.Fatal("expected error for missing profile")
	}
	if want := "stagign"; !contains(err.Error(), want) {
		t.Errorf("error %q does not mention %q", err.Error(), want)
	}
}

// TestParseDemoFixture: smoke-checks that the example TOML in
// examples/demo/dryrun.toml still parses with the v0.6 schema. It is the
// reference document users start from; breaking it means breaking onboarding.
func TestParseDemoFixture(t *testing.T) {
	demo := filepath.Join("..", "..", "examples", "demo", "dryrun.toml")
	cfg, err := Load(demo)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Project == nil || cfg.Project.ID == nil || *cfg.Project.ID != "demo" {
		t.Errorf("demo project id: got %+v, want demo", cfg.Project)
	}
	dev, ok := cfg.Profiles["dev"]
	if !ok {
		t.Fatal("demo missing [profiles.dev]")
	}
	if dev.DatabaseID == nil || *dev.DatabaseID != "demo-dev" {
		t.Errorf("demo dev database_id: got %v, want demo-dev", dev.DatabaseID)
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

func TestLintConfigFromConventions(t *testing.T) {
	toml := `
[conventions]
table_name = "snake_plural"
prefer_text_over_varchar = false

[conventions.disabled_rules]
rules = ["pk/exists"]
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}
	lintCfg := cfg.LintConfig()
	if lintCfg.TableNameStyle != "snake_plural" {
		t.Errorf("got %q, want snake_plural", lintCfg.TableNameStyle)
	}
	if lintCfg.PreferTextOverVarchar {
		t.Error("expected prefer_text_over_varchar = false")
	}
	if len(lintCfg.DisabledRules) != 1 || lintCfg.DisabledRules[0] != "pk/exists" {
		t.Error("expected disabled rule pk/exists")
	}
}
