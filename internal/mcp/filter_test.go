package mcp

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// A filter is not a lookup: it may legitimately match nothing. But an agent
// that passes the qualified name list_tables printed used to get an empty
// result with no error and no hint, which reads as "nothing wrong here".
func TestTableFilterAcceptsQualifiedNames(t *testing.T) {
	a := withActivity(annotate(multiSchemaSnap(), 500_000),
		schema.QualifiedName{Schema: "app", Name: "events"},
		schema.TableActivity{SeqScan: 500_000, NLiveTup: 500_000})
	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))

	t.Run("qualified_filter_matches", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies", "table": "app.events"})
		assertContains(t, out, `"table": "events"`)
		assertContains(t, out, `"schema": "app"`)
	})

	t.Run("bare_filter_still_matches", func(t *testing.T) {
		assertContains(t, callTool(t, c, "detect", map[string]any{"kind": "anomalies", "table": "events"}),
			`"schema": "app"`)
	})

	// a filter has an empty result to fall back on, so it refuses to widen the
	// scope the way the lookup does -- but it says which argument lost
	t.Run("qualified_filter_respects_schema_argument", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies", "table": "app.events", "schema": "public"})
		assertContains(t, out, "schema=public and table=app.events name different schemas")
	})

	// the literal name is tried first here too: public."foo.bar" exists, and so
	// does foo.bar, so a split would filter to the wrong one
	t.Run("literal_dotted_name_outranks_the_split", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"table": "foo.bar", "verbosity": "full"})
		if strings.Contains(out, "matched nothing") {
			t.Errorf("public.\"foo.bar\" exists and must match: %.300s", out)
		}
	})

	t.Run("unmatched_filter_says_so", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies", "table": "event"})
		assertContains(t, out, "matched nothing")
		assertContains(t, out, "did you mean app.events")
	})

	// near matches are substring only, so a transposition finds none; the note
	// still has to leave somewhere to go
	t.Run("transposition_still_points_somewhere", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies", "table": "evnts"})
		assertContains(t, out, "matched nothing")
		assertContains(t, out, `search_schema {"query":"evnts"}`)
	})

	t.Run("all_kinds_carry_it", func(t *testing.T) {
		for _, kind := range []string{"all", "stale_stats", "unused_indexes", "bloated_indexes", "bloated_tables"} {
			out := callTool(t, c, "detect", map[string]any{"kind": kind, "table": "nosuch"})
			if !strings.Contains(out, "matched nothing") {
				t.Errorf("kind=%s dropped the note: %.300s", kind, out)
			}
		}
	})

	// a real table with no findings is not an unmatched filter
	t.Run("matched_filter_with_no_findings_stays_quiet", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies", "table": "public.orders"})
		assertContains(t, out, "No anomalies detected.")
		if strings.Contains(out, "matched nothing") {
			t.Errorf("a table that exists must not be reported as unmatched: %.300s", out)
		}
	})

	// lint_schema's follow-up must re-run with the split filter, not the name
	// that needed splitting
	t.Run("lint_next_call_carries_the_split_filter", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"table": "app.orders"})
		if strings.Contains(out, `"table": "app.orders"`) {
			t.Errorf("follow-up re-passes the qualified name: %.400s", out)
		}
	})

	t.Run("vacuum_health_and_lint_schema_split_too", func(t *testing.T) {
		assertContains(t, callTool(t, c, "vacuum_health", map[string]any{"table": "nosuch"}), "matched nothing")
		assertContains(t, callTool(t, c, "lint_schema", map[string]any{"table": "nosuch"}), "matched nothing")
	})
}

// The note is computed from the schema snapshot, but the detectors read node
// stats, which can carry a table captured after the schema was. Emitting the
// note next to a finding would tell the agent its filter matched nothing while
// showing it what the filter matched.
func TestTableFilterNoteNeverContradictsAFinding(t *testing.T) {
	a := withActivity(annotate(multiSchemaSnap(), 500_000),
		schema.QualifiedName{Schema: "public", Name: "ghost"},
		schema.TableActivity{SeqScan: 500_000, NLiveTup: 500_000})
	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))

	for _, kind := range []string{"anomalies", "all"} {
		out := callTool(t, c, "detect", map[string]any{"kind": kind, "table": "ghost"})
		if !strings.Contains(out, "ghost") {
			t.Fatalf("kind=%s: fixture no longer produces the finding: %.300s", kind, out)
		}
		if strings.Contains(out, "matched nothing") {
			t.Errorf("kind=%s: note contradicts the finding beside it: %.400s", kind, out)
		}
	}
}
