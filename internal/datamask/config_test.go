package datamask

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuildConfigCLIBeatsProfile pins the precedence rule: a CLI flag wins,
// the profile fills gaps. Two paths and two policy lists on the same field
// surface the merge behaviour unambiguously.
func TestBuildConfigCLIBeatsProfile(t *testing.T) {
	cfg := BuildConfig(
		Flags{MasksFile: "/cli/path.yml", MaskPolicies: []string{"cli-pol"}},
		ProfileMasks{File: "/profile/path.yml", Policies: []string{"profile-pol"}},
	)
	if cfg.Path != "/cli/path.yml" {
		t.Errorf("Path: got %q, want /cli/path.yml", cfg.Path)
	}
	if len(cfg.Policies) != 1 || cfg.Policies[0] != "cli-pol" {
		t.Errorf("Policies: got %v, want [cli-pol]", cfg.Policies)
	}
}

// TestBuildConfigProfileFallback: when the CLI is silent, every field is
// taken from the profile. This is the common dryrun.toml-driven workflow.
func TestBuildConfigProfileFallback(t *testing.T) {
	cfg := BuildConfig(
		Flags{},
		ProfileMasks{File: "/profile/path.yml", Policies: []string{"pii"}},
	)
	if cfg.Path != "/profile/path.yml" {
		t.Errorf("Path: got %q, want /profile/path.yml", cfg.Path)
	}
	if len(cfg.Policies) != 1 || cfg.Policies[0] != "pii" {
		t.Errorf("Policies: got %v, want [pii]", cfg.Policies)
	}
}

// TestBuildConfigFlagsPropagate: the two flag-only knobs (Disabled,
// AllowMissingDB) have no profile-level equivalents and must round-trip
// from Flags into MaskConfig untouched.
func TestBuildConfigFlagsPropagate(t *testing.T) {
	cfg := BuildConfig(
		Flags{NoMasks: true, AllowMissingDB: true},
		ProfileMasks{},
	)
	if !cfg.Disabled {
		t.Error("NoMasks must propagate to Disabled")
	}
	if !cfg.AllowMissingDB {
		t.Error("AllowMissingDB must propagate")
	}
}

// TestBuildMaskerDisabledReturnsNil: Disabled is the hard opt-out. No
// filesystem touch, no Load call — straight to nil (MaskPlanner nil-guards).
func TestBuildMaskerDisabledReturnsNil(t *testing.T) {
	m, err := MaskConfig{Disabled: true}.BuildMasker("dev", t.TempDir())
	if err != nil {
		t.Fatalf("Disabled BuildMasker should not error: %v", err)
	}
	if m != nil {
		t.Errorf("want nil Policy, got %v", m)
	}
}

// TestBuildMaskerRefusesWithoutFile: the refuse-to-capture guard. No flag,
// no profile, no discovery, no opt-out → hard error. The error text must
// name the escape hatches so an operator can fix it without guessing.
func TestBuildMaskerRefusesWithoutFile(t *testing.T) {
	_, err := MaskConfig{}.BuildMasker("dev", t.TempDir())
	if err == nil {
		t.Fatal("expected error when no masks file resolves and masking is not disabled")
	}
	for _, want := range []string{"--masks-file", "--no-masks"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error must mention %q escape hatch, got: %v", want, err)
		}
	}
}

// TestBuildMaskerExplicitPath: an explicit Path skips discovery and loads
// the named file directly. The returned Masker must be the concrete *Policy
// so callers can inspect column membership.
func TestBuildMaskerExplicitPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yml")
	body := `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	p, err := MaskConfig{Path: path}.BuildMasker("dev", dir)
	if err != nil {
		t.Fatalf("BuildMasker: %v", err)
	}
	if p == nil {
		t.Fatal("want loaded Policy, got nil")
	}
	if !p.IsSensitive("public", "users", "email") {
		t.Error("loaded policy should mark users.email sensitive")
	}
}

// TestBuildMaskerAllowMissingDB: AllowMissingDB threads through to Load so a
// missing database_id downgrades to an empty Policy instead of erroring.
// Without this, multi-DB projects with partial YAML coverage break at init.
func TestBuildMaskerAllowMissingDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yml")
	body := `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	// strict default: a missing dbID is an error.
	if _, err := (MaskConfig{Path: path}).BuildMasker("ghost", dir); err == nil {
		t.Error("strict mode must error on missing database_id")
	}
	// permissive: same call with AllowMissingDB returns an empty Policy.
	p, err := MaskConfig{Path: path, AllowMissingDB: true}.BuildMasker("ghost", dir)
	if err != nil {
		t.Fatalf("AllowMissingDB BuildMasker: %v", err)
	}
	if p == nil {
		t.Fatal("permissive mode must return empty Policy, not nil")
	}
	if p.IsSensitive("public", "users", "email") {
		t.Error("permissive Policy for missing dbID must match nothing")
	}
}
