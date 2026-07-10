package main

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

// Policy tests for the role-safety preflight on prod-reading commands: a
// privileged role is refused fail-closed, --allow-privileged downgrades the
// refusal to a warning (nil error), and a plain role passes silently.
func TestRolePreflight(t *testing.T) {
	privileged := &schema.RoleReport{Rolname: "postgres", Super: true}
	plain := &schema.RoleReport{Rolname: "dryrun_readonly"}

	t.Run("privileged role refused without --allow-privileged", func(t *testing.T) {
		err := rolePreflight(privileged, false)
		if err == nil {
			t.Fatal("expected refusal for superuser role, got nil")
		}
		for _, want := range []string{"postgres", "superuser", "--allow-privileged", "dryrun-readonly-role.sql"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal message missing %q: %v", want, err)
			}
		}
	})

	t.Run("privileged role allowed with --allow-privileged", func(t *testing.T) {
		if err := rolePreflight(privileged, true); err != nil {
			t.Fatalf("expected warning-only pass with --allow-privileged, got %v", err)
		}
	})

	t.Run("plain role passes", func(t *testing.T) {
		if err := rolePreflight(plain, false); err != nil {
			t.Fatalf("expected pass for unprivileged role, got %v", err)
		}
	})

	t.Run("every privilege bit trips the refusal", func(t *testing.T) {
		reports := []*schema.RoleReport{
			{Rolname: "r", Replication: true},
			{Rolname: "b", BypassRLS: true},
		}
		for _, r := range reports {
			if err := rolePreflight(r, false); err == nil {
				t.Errorf("report %+v should be refused", r)
			}
		}
	})
}
