package mcp

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// Builds a fixture snapshot with two schemas (public, billing) and two
// nodes (primary + replica), each carrying overlapping table and index stats.
func filterTestSnap(t *testing.T) *schema.SchemaSnapshot {
	t.Helper()
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 18", Database: "db", Timestamp: time.Now().UTC(),
		Tables: []schema.Table{
			{Schema: "public", Name: "users"},
			{Schema: "public", Name: "orders"},
			{Schema: "billing", Name: "invoices"},
			{Schema: "billing", Name: "orders"},
		},
		NodeStats: []schema.NodeStats{
			{
				Source: "primary",
				TableStats: []schema.NodeTableStats{
					{Schema: "public", Table: "users"},
					{Schema: "public", Table: "orders"},
					{Schema: "billing", Table: "invoices"},
				},
				IndexStats: []schema.NodeIndexStats{
					{Schema: "public", Table: "users", IndexName: "users_pkey"},
					{Schema: "billing", Table: "invoices", IndexName: "invoices_pkey"},
				},
			},
			{
				Source: "replica",
				TableStats: []schema.NodeTableStats{
					{Schema: "public", Table: "users"},
					{Schema: "billing", Table: "invoices"},
				},
				IndexStats: []schema.NodeIndexStats{
					{Schema: "public", Table: "users", IndexName: "users_pkey"},
				},
			},
		},
	}
}

// Pins the fast-path in filterSnap: when both schema and table filters are
// empty, the original pointer is returned unchanged with no copy.
func TestFilterSnap_EmptyFiltersReturnsSame(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "", "")
	if out != snap {
		t.Error("expected same pointer when no filters")
	}
}

// verifies that schema-only filter narrows Tables, plus per-node TableStats
// and IndexStats, to only the requested schema. Entries from other schemas
// must not leak through any of these three projections.
func TestFilterSnap_SchemaOnly(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "public", "")
	if len(out.Tables) != 2 {
		t.Fatalf("expected 2 public tables, got %d", len(out.Tables))
	}
	for _, ta := range out.Tables {
		if ta.Schema != "public" {
			t.Errorf("unexpected schema %q", ta.Schema)
		}
	}
	for _, ns := range out.NodeStats {
		for _, ts := range ns.TableStats {
			if ts.Schema != "public" {
				t.Errorf("node %s: TableStats has non-public schema %q", ns.Source, ts.Schema)
			}
		}
		for _, is := range ns.IndexStats {
			if is.Schema != "public" {
				t.Errorf("node %s: IndexStats has non-public schema %q", ns.Source, is.Schema)
			}
		}
	}
}

// Pins table-only filter: matches by table name across all schemas, so a
// filter for "orders" keeps both public.orders and billing.orders.
func TestFilterSnap_TableOnly(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "", "orders")
	if len(out.Tables) != 2 {
		t.Fatalf("expected 2 orders tables (public+billing), got %d", len(out.Tables))
	}
	for _, ta := range out.Tables {
		if ta.Name != "orders" {
			t.Errorf("unexpected table %q", ta.Name)
		}
	}
	for _, ns := range out.NodeStats {
		for _, ts := range ns.TableStats {
			if ts.Table != "orders" {
				t.Errorf("node %s: TableStats has non-orders %q", ns.Source, ts.Table)
			}
		}
	}
}

// Verifies that combining schema and table filters does AND-narrowing: the
// only surviving table is the unique (schema, name) pair, here public.orders.
func TestFilterSnap_SchemaAndTable(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "public", "orders")
	if len(out.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(out.Tables))
	}
	if out.Tables[0].Schema != "public" || out.Tables[0].Name != "orders" {
		t.Errorf("unexpected table: %+v", out.Tables[0])
	}
}

// pins that filterSnap applies the schema filter to every NodeStats entry,
// not just the first one, and importantly that the original snapshot is not
// mutated in the process. The latter is critical because callers share the
// snap pointer across concurrent MCP tool calls.
func TestFilterSnap_MultiNodeFilters(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "billing", "")
	if len(out.NodeStats) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(out.NodeStats))
	}
	for _, ns := range out.NodeStats {
		for _, ts := range ns.TableStats {
			if ts.Schema != "billing" {
				t.Errorf("node %s: schema %q leaked", ns.Source, ts.Schema)
			}
		}
		for _, is := range ns.IndexStats {
			if is.Schema != "billing" {
				t.Errorf("node %s: index schema %q leaked", ns.Source, is.Schema)
			}
		}
	}
	// original snap untouched
	if len(snap.NodeStats[0].TableStats) != 3 {
		t.Errorf("original snap mutated: primary TableStats len=%d", len(snap.NodeStats[0].TableStats))
	}
}
