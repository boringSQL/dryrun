package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/lint"
)

// Smoke tests for lint_schema scopes and filters. Each subtest exercises a
// scope/filter combination and asserts the expected top-level keys appear.
// Subtests that read the audit `findings` array pass verbosity=full because
// the default summary view collapses findings into by_rule counts.
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
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "audit", "verbosity": "full"})
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
// shape. All subtests pass verbosity=full because the assertions parse
// `audit.findings` directly — that array only exists in full mode.
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
		lo := parse(t, callTool(t, c, "lint_schema", map[string]any{"scope": "conventions", "verbosity": "full"}))
		for _, prefix := range auditRulePrefixes {
			if conventionsHasPrefix(lo, prefix) {
				t.Errorf("conventions scope should not contain audit rule %q", prefix)
			}
		}
	})

	t.Run("audit_excludes_convention_rules", func(t *testing.T) {
		lo := parse(t, callTool(t, c, "lint_schema", map[string]any{"scope": "audit", "verbosity": "full"}))
		for _, prefix := range conventionRulePrefixes {
			if auditHasPrefix(lo, prefix) {
				t.Errorf("audit scope should not contain convention rule %q", prefix)
			}
		}
	})

	t.Run("all_has_both_branches", func(t *testing.T) {
		allLo := parse(t, callTool(t, c, "lint_schema", map[string]any{"scope": "all", "verbosity": "full"}))
		if allLo.Conventions == nil {
			t.Error("all scope should include conventions")
		}
		if allLo.Audit == nil {
			t.Error("all scope should include audit")
		}
	})

	t.Run("schema_filter_reduces_findings", func(t *testing.T) {
		allLo := parse(t, callTool(t, c, "lint_schema", map[string]any{"verbosity": "full"}))
		filteredLo := parse(t, callTool(t, c, "lint_schema", map[string]any{"schema": "nonexistent_schema", "verbosity": "full"}))

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
// tables_checked compared to filtering for a real one. Pinned to
// verbosity=full so audit.tables_checked is populated (the summary shape
// renames it to tables_analyzed and lives under a different key).
func TestLintSchema_TableFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{"table": "users", "verbosity": "full"})
	outNone := callTool(t, c, "lint_schema", map[string]any{"table": "definitely_not_a_table_xyz", "verbosity": "full"})

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

// Default verbosity is now "summary": the audit branch collapses findings
// into by_rule counts; the full findings array and ddl_fix payloads are not
// surfaced (hint prose may still mention them).
func TestLintSchema_DefaultIsSummary(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{"scope": "audit"})

	var payload struct {
		Audit map[string]any `json:"audit"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if _, ok := payload.Audit["findings"]; ok {
		t.Errorf("summary mode must not surface 'findings' key, got: %v", payload.Audit)
	}
	if _, ok := payload.Audit["by_rule"]; !ok {
		t.Errorf("summary mode must emit 'by_rule', got: %v", payload.Audit)
	}
}

// verbosity=full opts back into the legacy shape — the raw findings array,
// recommendations, and ddl_fix payloads are all surfaced. This is the
// escape hatch for clients that were built against the pre-summary
// response and need time to migrate.
func TestLintSchema_VerbosityFullRestoresFindings(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{
		"scope":     "audit",
		"verbosity": "full",
	})

	var payload struct {
		Audit *lint.Report `json:"audit"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if payload.Audit == nil || payload.Audit.Findings == nil {
		t.Fatalf("verbosity=full must restore findings array, got: %+v", payload.Audit)
	}
}

// Summary mode with audit findings ≤ 50 surfaces a _meta.next that points
// back at lint_schema with verbosity=full plus any filter args the caller
// passed (so the client doesn't have to remember them).
func TestLintSchema_SummaryNextPreservesFilters(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{
		"scope":  "audit",
		"schema": "public",
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	meta, _ := payload["_meta"].(map[string]any)
	next, ok := meta["next"].([]any)
	if !ok || len(next) == 0 {
		t.Fatalf("expected _meta.next with one entry, got: %v", meta)
	}
	first, _ := next[0].(map[string]any)
	if first["tool"] != "lint_schema" {
		t.Errorf("expected tool=lint_schema, got %v", first["tool"])
	}
	args, _ := first["args"].(map[string]any)
	if args["verbosity"] != "full" {
		t.Errorf("expected verbosity=full, got %v", args["verbosity"])
	}
	if args["schema"] != "public" {
		t.Errorf("expected schema=public preserved, got %v", args["schema"])
	}
	if args["scope"] != "audit" {
		t.Errorf("expected scope=audit preserved, got %v", args["scope"])
	}
}

// verbosity=full means the client has the findings array already, so
// suggesting a follow-up `lint_schema verbosity=full` call would just loop.
// _meta.next must be absent in this mode — only summary responses chain
// forward.
func TestLintSchema_FullModeOmitsNext(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{
		"scope":     "audit",
		"verbosity": "full",
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	meta, _ := payload["_meta"].(map[string]any)
	if _, present := meta["next"]; present {
		t.Errorf("verbosity=full must omit _meta.next, got: %v", meta)
	}
}

// `fields` is a whitelist that wins over `scope`: even when scope=all asks
// for both branches, fields=[conventions] should drop the audit branch
// entirely. This contract lets callers narrow the response without having
// to also adjust scope, which would be awkward when fields is the more
// expressive knob.
func TestLintSchema_FieldsWhitelistOverridesScope(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "lint_schema", map[string]any{
		"scope":  "all",
		"fields": []any{"conventions"},
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if _, ok := payload["conventions"]; !ok {
		t.Errorf("expected 'conventions' key, got: %v", keys(payload))
	}
	if _, ok := payload["audit"]; ok {
		t.Errorf("fields=[conventions] must drop audit, got: %v", keys(payload))
	}
}
