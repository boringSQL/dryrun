package config

import (
	"os"
	"testing"
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
