package mcp

import (
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// Both fixtures must survive the declared input/output schemas production runs
// under, or tests built on them fail for the wrong reason.
func TestFixturesServeThroughMCP(t *testing.T) {
	t.Run("multi_schema", func(t *testing.T) {
		c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))

		out := callTool(t, c, "find_objects", nil)
		for _, want := range []string{"public.orders", "app.orders", "app.events", "public.foo.bar"} {
			assertContains(t, out, want)
		}

		// a name containing a dot must keep resolving as itself, not as schema.table
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "foo.bar"}), `"name": "foo.bar"`)
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "orders", "schema": "app"}), `"tenant_id"`)

		// FKTable is nspname.relname, so an FK to public."foo.bar" and one to a
		// table bar in a schema public.foo are the same string
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "foo.bar", "detail": "relations"}),
			`"join": "JOIN public.orders ON public.orders.ref_id = public.\"foo.bar\".id"`)
	})

	t.Run("wide_and_partitioned", func(t *testing.T) {
		c := serveOffline(t, NewOfflineServerAnnotated(annotate(wideSnap(400, 60), 1000), lint.DefaultConfig()))

		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "wide"}), `"wide_c0400_idx"`)
		// detail=full marshals the raw table; the compact path keeps only the
		// first and last five children, and a middle child's bound appears
		// nowhere else (its name does, via the parent index's children)
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "events", "detail": "full"}),
			"FOR VALUES FROM ('2026-01-31 00:00:00+00')")
	})
}

// The demo snapshot carries no activity at all, so nothing else pins that a
// finding outside public reaches detect.
func TestFixtureActivityOutsidePublic(t *testing.T) {
	a := withActivity(annotate(multiSchemaSnap(), 500_000),
		schema.QualifiedName{Schema: "app", Name: "events"},
		schema.TableActivity{SeqScan: 500_000, IdxScan: 0, NLiveTup: 500_000})
	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))

	out := callTool(t, c, "detect", map[string]any{"kind": "anomalies"})
	assertContains(t, out, `"schema": "app"`)
	assertContains(t, out, `"table": "events"`)
}
