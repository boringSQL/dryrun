package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/lint"
)

// Smoke tests for lint_schema scopes and filters. Each subtest exercises a
// scope/filter combination and asserts the expected top-level keys appear.
func TestLintHandlers_OfflineSmoke(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("lint_schema_default_all", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", nil)
		assertContains(t, out, "conventions")
		assertContains(t, out, "audit")
	})

	t.Run("lint_schema_scope_conventions", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "conventions"})
		assertContains(t, out, "conventions")
		assertContains(t, out, "rule_groups")
	})

	t.Run("lint_schema_scope_audit", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "audit"})
		assertContains(t, out, "audit")
		assertContains(t, out, "findings")
	})

	t.Run("lint_schema_scope_all", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "all"})
		assertContains(t, out, "conventions")
		assertContains(t, out, "audit")
	})

	t.Run("lint_schema_with_schema_filter", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"schema": "public"})
		assertContains(t, out, "conventions")
	})
}

// auditRulePrefixes are rule prefixes that only appear from audit scope.
var auditRulePrefixes = []string{"indexes/", "fk/circular", "fk/orphan", "fk/type_mismatch", "docs/", "vacuum/", "naming/bool_prefix", "naming/reserved", "naming/id_mismatch", "pk/non_sequential"}

// conventionRulePrefixes are rule prefixes that only appear from conventions scope.
var conventionRulePrefixes = []string{"types/", "timestamps/", "constraints/", "partition/"}

// Pins the scope isolation contract: rules from the audit family must not
// surface in conventions output and vice versa. Without this, a stray rule
// could leak across scopes and confuse callers that key off the response
// shape.
func TestLintSchemaScopeIsolation(t *testing.T) {
	c := setupOfflineTest(t)

	type lintOut struct {
		Conventions *lint.CompactReport `json:"conventions,omitempty"`
		Audit       *lint.Report        `json:"audit,omitempty"`
	}
	parse := func(t *testing.T, out string) lintOut {
		t.Helper()
		var lo lintOut
		if err := json.Unmarshal([]byte(out), &lo); err != nil {
			t.Fatalf("failed to parse lint output: %v", err)
		}
		return lo
	}

	conventionsHasPrefix := func(lo lintOut, prefix string) bool {
		if lo.Conventions == nil {
			return false
		}
		for _, g := range lo.Conventions.RuleGroups {
			if strings.HasPrefix(g.Rule, prefix) || g.Rule == prefix {
				return true
			}
		}
		return false
	}
	auditHasPrefix := func(lo lintOut, prefix string) bool {
		if lo.Audit == nil {
			return false
		}
		for _, f := range lo.Audit.Findings {
			if strings.HasPrefix(f.Rule, prefix) || f.Rule == prefix {
				return true
			}
		}
		return false
	}

	t.Run("conventions_excludes_audit_rules", func(t *testing.T) {
		lo := parse(t, callTool(t, c, "lint_schema", map[string]any{"scope": "conventions"}))
		for _, prefix := range auditRulePrefixes {
			if conventionsHasPrefix(lo, prefix) {
				t.Errorf("conventions scope should not contain audit rule %q", prefix)
			}
		}
	})

	t.Run("audit_excludes_convention_rules", func(t *testing.T) {
		lo := parse(t, callTool(t, c, "lint_schema", map[string]any{"scope": "audit"}))
		for _, prefix := range conventionRulePrefixes {
			if auditHasPrefix(lo, prefix) {
				t.Errorf("audit scope should not contain convention rule %q", prefix)
			}
		}
	})

	t.Run("all_has_both_branches", func(t *testing.T) {
		allLo := parse(t, callTool(t, c, "lint_schema", map[string]any{"scope": "all"}))
		if allLo.Conventions == nil {
			t.Error("all scope should include conventions")
		}
		if allLo.Audit == nil {
			t.Error("all scope should include audit")
		}
	})

	t.Run("schema_filter_reduces_findings", func(t *testing.T) {
		allLo := parse(t, callTool(t, c, "lint_schema", nil))
		filteredLo := parse(t, callTool(t, c, "lint_schema", map[string]any{"schema": "nonexistent_schema"}))

		var allCount, filteredCount int
		if allLo.Audit != nil {
			allCount = len(allLo.Audit.Findings)
		}
		if filteredLo.Audit != nil {
			filteredCount = len(filteredLo.Audit.Findings)
		}

		if filteredCount >= allCount && allCount > 0 {
			t.Errorf("filtering by nonexistent schema should reduce findings, got %d vs %d", filteredCount, allCount)
		}
	})
}

// verifies that the table filter passed through lint_schema actually reaches
// the audit layer: filtering to a nonexistent table should reduce
// tables_checked compared to filtering for a real one.
func TestLintSchema_TableFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{"table": "users"})
	outNone := callTool(t, c, "lint_schema", map[string]any{"table": "definitely_not_a_table_xyz"})

	type lintOut struct {
		Audit *lint.Report `json:"audit,omitempty"`
	}
	parse := func(s string) lintOut {
		var lo lintOut
		_ = json.Unmarshal([]byte(s), &lo)
		return lo
	}
	a := parse(out)
	b := parse(outNone)
	aCount := 0
	if a.Audit != nil {
		aCount = a.Audit.TablesChecked
	}
	bCount := 0
	if b.Audit != nil {
		bCount = b.Audit.TablesChecked
	}
	if bCount >= aCount && aCount > 0 {
		t.Errorf("expected nonexistent filter to reduce tables_checked, got a=%d b=%d", aCount, bCount)
	}
}
