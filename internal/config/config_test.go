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

// TestResolvePrecedence walks each rung of the documented precedence ladder:
// --db > --schema > --profile > [default].profile > single-profile fallback.
// Each sub-test removes the higher-priority input and verifies the next rung
// takes over.
func TestResolvePrecedence(t *testing.T) {
	toml := `
[default]
profile = "prod"

[profiles.dev]
db_url = "postgres://dev/x"

[profiles.prod]
db_url = "postgres://prod/x"
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}
	root := "/tmp/demo"
	os.Unsetenv("PROFILE")

	cliDB := "postgres://cli/x"
	cliSchema := "/tmp/schema.json"
	dev := "dev"

	// rung 1: --db wins over everything
	rp, err := cfg.ResolveProfile(&cliDB, &cliSchema, &dev, root)
	if err != nil {
		t.Fatal(err)
	}
	if rp.DBURL == nil || *rp.DBURL != cliDB {
		t.Errorf("--db rung: got %v, want %s", rp.DBURL, cliDB)
	}

	// rung 2: --schema wins when --db absent
	rp, err = cfg.ResolveProfile(nil, &cliSchema, &dev, root)
	if err != nil {
		t.Fatal(err)
	}
	if rp.SchemaFile == nil || *rp.SchemaFile != cliSchema || rp.DBURL != nil {
		t.Errorf("--schema rung: got schema=%v db=%v", rp.SchemaFile, rp.DBURL)
	}

	// rung 3: --profile wins over [default].profile
	rp, err = cfg.ResolveProfile(nil, nil, &dev, root)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Name != "dev" {
		t.Errorf("--profile rung: got %q, want dev", rp.Name)
	}

	// rung 4: [default].profile when no CLI selector
	rp, err = cfg.ResolveProfile(nil, nil, nil, root)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Name != "prod" {
		t.Errorf("[default].profile rung: got %q, want prod", rp.Name)
	}
}

// TestResolveSingleProfileFallback: with no [default] and a single profile,
// resolution implicitly picks it. Adding a second profile breaks the fallback
// (resolver must require an explicit selector).
func TestResolveSingleProfileFallback(t *testing.T) {
	one, _ := Parse(`
[profiles.only]
db_url = "postgres://only/x"
`)
	rp, err := one.ResolveProfile(nil, nil, nil, "/tmp/demo")
	if err != nil {
		t.Fatalf("single-profile fallback: %v", err)
	}
	if rp.Name != "only" || rp.DBURL == nil || *rp.DBURL != "postgres://only/x" {
		t.Errorf("got %+v", rp)
	}

	two, _ := Parse(`
[profiles.a]
db_url = "postgres://a/x"

[profiles.b]
db_url = "postgres://b/x"
`)
	tmp := t.TempDir() // ensure no .dryrun/schema.json under cwd
	if _, err := two.ResolveProfile(nil, nil, nil, tmp); err == nil {
		t.Error("expected error with two profiles and no selector")
	}
}

// TestResolveProfilePlusDB: --db must beat --profile even when the profile
// exists and has its own db_url. The CLI override is a hard short-circuit.
func TestResolveProfilePlusDB(t *testing.T) {
	cfg, _ := Parse(`
[profiles.staging]
db_url = "postgres://stg/x"
`)
	cliDB := "postgres://override/x"
	staging := "staging"
	rp, err := cfg.ResolveProfile(&cliDB, nil, &staging, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	if rp.DBURL == nil || *rp.DBURL != cliDB {
		t.Errorf("--db should override --profile: got %v", rp.DBURL)
	}
	if rp.Name != "<cli>" {
		t.Errorf("expected Name=<cli>, got %q", rp.Name)
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

// TestProfileMasksRoundTrip exercises the C3 config plumbing for data masking.
// The masks_file and mask_policies keys must survive TOML decoding onto
// ProfileConfig, and then flow through ResolveProfile onto ResolvedProfile.
// masks_file additionally inherits the same relative-path treatment as
// schema_file: a bare filename in the config is resolved against project_root,
// so `dryrun init` works the same whether it is invoked from the project root
// or a nested subdirectory.
func TestProfileMasksRoundTrip(t *testing.T) {
	toml := `
[project]
id = "demo"

[profiles.dev]
db_url = "postgres://dev/x"
masks_file = "data-masking-policy.yml"
mask_policies = ["pii", "internal"]
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}

	// raw decode: both keys land on ProfileConfig verbatim, untransformed.
	dev := cfg.Profiles["dev"]
	if dev.MasksFile == nil || *dev.MasksFile != "data-masking-policy.yml" {
		t.Errorf("ProfileConfig.MasksFile: got %v, want data-masking-policy.yml", dev.MasksFile)
	}
	if len(dev.MaskPolicies) != 2 || dev.MaskPolicies[0] != "pii" || dev.MaskPolicies[1] != "internal" {
		t.Errorf("ProfileConfig.MaskPolicies: got %v, want [pii internal]", dev.MaskPolicies)
	}

	// resolved: the relative masks_file is rebased onto project_root, while
	// the policy list passes through unchanged.
	name := "dev"
	rp, err := cfg.ResolveProfile(nil, nil, &name, "/tmp/demo-root")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join("/tmp/demo-root", "data-masking-policy.yml")
	if rp.MasksFile == nil || *rp.MasksFile != want {
		t.Errorf("ResolvedProfile.MasksFile: got %v, want %s", rp.MasksFile, want)
	}
	if len(rp.MaskPolicies) != 2 || rp.MaskPolicies[0] != "pii" {
		t.Errorf("ResolvedProfile.MaskPolicies: got %v, want [pii internal]", rp.MaskPolicies)
	}
}

// TestProfileMasksAbsolutePathPreserved: an absolute masks_file must be passed
// through verbatim. filepath.Join-ing an already-absolute path under
// project_root would silently corrupt it, so the resolver explicitly skips the
// rebase when the path is absolute — this test pins that branch.
func TestProfileMasksAbsolutePathPreserved(t *testing.T) {
	cfg, err := Parse(`
[profiles.dev]
db_url = "postgres://dev/x"
masks_file = "/etc/dryrun/masks.yml"
`)
	if err != nil {
		t.Fatal(err)
	}
	name := "dev"
	rp, err := cfg.ResolveProfile(nil, nil, &name, "/tmp/demo-root")
	if err != nil {
		t.Fatal(err)
	}
	if rp.MasksFile == nil || *rp.MasksFile != "/etc/dryrun/masks.yml" {
		t.Errorf("absolute masks_file should be untouched: got %v", rp.MasksFile)
	}
}

// TestProfileMasksOmitted: a profile that declares neither key must leave both
// ResolvedProfile fields at their zero values — a nil *string and a nil slice.
// resolveMaskPolicy treats nil as the signal to "fall through to discovery",
// so fabricating an empty-but-non-nil default here would quietly break the
// auto-discovery path.
func TestProfileMasksOmitted(t *testing.T) {
	cfg, err := Parse(`
[profiles.dev]
db_url = "postgres://dev/x"
`)
	if err != nil {
		t.Fatal(err)
	}
	name := "dev"
	rp, err := cfg.ResolveProfile(nil, nil, &name, "/tmp/demo-root")
	if err != nil {
		t.Fatal(err)
	}
	if rp.MasksFile != nil {
		t.Errorf("expected nil MasksFile when omitted, got %q", *rp.MasksFile)
	}
	if rp.MaskPolicies != nil {
		t.Errorf("expected nil MaskPolicies when omitted, got %v", rp.MaskPolicies)
	}
}

// TestParseRemotes: the [[remote]] array-of-tables must decode onto
// ProjectConfig.Remotes with every field surfaced. A remote that omits
// `type` leaves it empty (the OCI store treats "" as oci) and `default`
// defaults to false — both are exercised here so a half-specified remote
// can't silently acquire fields it didn't ask for.
func TestParseRemotes(t *testing.T) {
	toml := `
[[remote]]
name = "gar"
type = "oci"
ref = "us-docker.pkg.dev/proj/dryrun"
token_env = "GAR_TOKEN"
default = true

[[remote]]
name = "ghcr"
ref = "ghcr.io/org/dryrun"
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Remotes) != 2 {
		t.Fatalf("expected 2 remotes, got %d", len(cfg.Remotes))
	}
	gar := cfg.Remotes[0]
	if gar.Name != "gar" || gar.Type != "oci" || gar.Ref != "us-docker.pkg.dev/proj/dryrun" {
		t.Errorf("gar fields: %+v", gar)
	}
	if gar.TokenEnv != "GAR_TOKEN" || !gar.Default {
		t.Errorf("gar token_env/default: %+v", gar)
	}
	ghcr := cfg.Remotes[1]
	if ghcr.Type != "" || ghcr.TokenEnv != "" || ghcr.Default {
		t.Errorf("ghcr should leave optional fields zero: %+v", ghcr)
	}
}

// TestProfileRemoteStreamBinding: the per-profile `remote` and `stream` keys
// must decode onto ProfileConfig and then flow through ResolveProfile onto
// ResolvedProfile. `stream` is a storage-location override only, so it must
// not perturb the SnapshotKey — that stays keyed by (project, database).
func TestProfileRemoteStreamBinding(t *testing.T) {
	toml := `
[project]
id = "demo"

[profiles.auth]
db_url = "postgres://auth/x"
database_id = "auth"
remote = "gar"
stream = "shared/auth"
`
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatal(err)
	}

	// raw decode lands verbatim on ProfileConfig
	p := cfg.Profiles["auth"]
	if p.Remote == nil || *p.Remote != "gar" {
		t.Errorf("ProfileConfig.Remote: got %v, want gar", p.Remote)
	}
	if p.Stream == nil || *p.Stream != "shared/auth" {
		t.Errorf("ProfileConfig.Stream: got %v, want shared/auth", p.Stream)
	}

	name := "auth"
	rp, err := cfg.ResolveProfile(nil, nil, &name, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Remote == nil || *rp.Remote != "gar" {
		t.Errorf("ResolvedProfile.Remote: got %v, want gar", rp.Remote)
	}
	if rp.Stream() != "shared/auth" {
		t.Errorf("Stream() override: got %q, want shared/auth", rp.Stream())
	}
	// the override must not leak into the local key
	if k := rp.SnapshotKey(); k != (history.SnapshotKey{ProjectID: "demo", DatabaseID: "auth"}) {
		t.Errorf("stream override perturbed SnapshotKey: %+v", k)
	}
}

// TestStreamDefault: with no `stream` set, Stream() must reproduce
// history.StreamSuffix(key) byte-for-byte — the default <project>/<database>
// layout that BundleDir already uses, so anything already pushed keeps
// resolving. An empty-string `stream` is treated as "unset", not as a literal
// empty repo path.
func TestStreamDefault(t *testing.T) {
	cfg, err := Parse(`
[project]
id = "demo"

[profiles.auth]
db_url = "postgres://auth/x"
database_id = "auth"

[profiles.empty]
db_url = "postgres://e/x"
database_id = "e"
stream = ""
`)
	if err != nil {
		t.Fatal(err)
	}

	name := "auth"
	rp, err := cfg.ResolveProfile(nil, nil, &name, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	want := history.StreamSuffix(rp.SnapshotKey())
	if want != "demo/auth" {
		t.Fatalf("precondition: StreamSuffix got %q, want demo/auth", want)
	}
	if rp.Stream() != want {
		t.Errorf("default Stream(): got %q, want %q", rp.Stream(), want)
	}

	empty := "empty"
	rp, err = cfg.ResolveProfile(nil, nil, &empty, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Stream() != history.StreamSuffix(rp.SnapshotKey()) {
		t.Errorf("empty stream should fall back to default, got %q", rp.Stream())
	}
}

// TestResolveRemote walks the three-way selection rule:
//   - explicit name → that remote (or a name-bearing error on a typo)
//   - no name, single remote → the sole remote
//   - no name, many remotes → the one marked default, else an error
//   - no remotes at all → an error
func TestResolveRemote(t *testing.T) {
	many, _ := Parse(`
[[remote]]
name = "gar"
ref = "us-docker.pkg.dev/p/dryrun"
default = true

[[remote]]
name = "ghcr"
ref = "ghcr.io/org/dryrun"
`)
	if r, err := many.ResolveRemote("ghcr"); err != nil || r.Name != "ghcr" {
		t.Errorf("by name: got %v, %v", r, err)
	}
	if r, err := many.ResolveRemote(""); err != nil || r.Name != "gar" {
		t.Errorf("sole default: got %v, %v", r, err)
	}
	if _, err := many.ResolveRemote("nope"); err == nil || !contains(err.Error(), "nope") {
		t.Errorf("typo must name the remote: %v", err)
	}

	one, _ := Parse(`
[[remote]]
name = "only"
ref = "ghcr.io/org/dryrun"
`)
	if r, err := one.ResolveRemote(""); err != nil || r.Name != "only" {
		t.Errorf("single remote: got %v, %v", r, err)
	}

	ambiguous, _ := Parse(`
[[remote]]
name = "a"
ref = "ghcr.io/a"

[[remote]]
name = "b"
ref = "ghcr.io/b"
`)
	if _, err := ambiguous.ResolveRemote(""); err == nil {
		t.Error("expected error for many remotes and no default")
	}

	none, _ := Parse(`[project]
id = "x"`)
	if _, err := none.ResolveRemote(""); err == nil {
		t.Error("expected error when no remotes configured")
	}
}

// TestRemoteStreamBackwardsCompat: a config with none of the new fields must
// resolve exactly as before — no Remote pinned, Stream() at the default. This
// pins the additive guarantee: existing dryrun.toml files keep their behavior.
func TestRemoteStreamBackwardsCompat(t *testing.T) {
	cfg, err := Parse(`
[project]
id = "demo"

[profiles.dev]
db_url = "postgres://dev/x"
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Remotes) != 0 {
		t.Errorf("expected no remotes, got %d", len(cfg.Remotes))
	}
	name := "dev"
	rp, err := cfg.ResolveProfile(nil, nil, &name, "/tmp/demo")
	if err != nil {
		t.Fatal(err)
	}
	if rp.Remote != nil {
		t.Errorf("expected nil Remote, got %q", *rp.Remote)
	}
	if rp.Stream() != history.StreamSuffix(rp.SnapshotKey()) {
		t.Errorf("Stream() should equal the default, got %q", rp.Stream())
	}
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
