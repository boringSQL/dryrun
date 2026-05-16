package mcp

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
)

// Two schemas (public, billing) with overlapping table names so we can pin
// the AND-narrowing behaviour of filterSnap across both filter axes.
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
	}
}

// Empty filters short-circuit: filterSnap returns the original pointer, so
// downstream callers can rely on equality comparison to detect "no filter".
func TestFilterSnap_EmptyFiltersReturnsSame(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "", "")
	if out != snap {
		t.Error("expected same pointer when no filters")
	}
}

// Schema-only filter keeps every table whose Schema matches. Tables from
// other schemas must not leak.
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
}

// Table-only filter keeps every table whose Name matches — across schemas.
// A filter for "orders" should keep both public.orders and billing.orders.
func TestFilterSnap_TableOnly(t *testing.T) {
	snap := filterTestSnap(t)
	out := filterSnap(snap, "", "orders")
	if len(out.Tables) != 2 {
		t.Fatalf("expected 2 orders tables, got %d", len(out.Tables))
	}
	for _, ta := range out.Tables {
		if ta.Name != "orders" {
			t.Errorf("unexpected table %q", ta.Name)
		}
	}
}

// Combined filters AND-narrow to the unique (schema, name) pair.
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

// The _meta block carries mode + database + pg_version derived from the
// snapshot; the optional hint field is present only when non-empty.
func TestInjectMeta_OfflineMode(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2 on x86_64", Database: "appdb",
		Timestamp: time.Now().UTC(),
	}
	srv := NewOfflineServer(snap, lint.DefaultConfig())

	t.Run("with_hint", func(t *testing.T) {
		out := map[string]any{"foo": "bar"}
		srv.injectMeta(out, "do the thing", nil)
		meta, ok := out["_meta"].(map[string]any)
		if !ok {
			t.Fatalf("expected _meta map, got %T", out["_meta"])
		}
		if meta["mode"] != "offline" {
			t.Errorf("expected mode=offline, got %v", meta["mode"])
		}
		if meta["database"] != "appdb" {
			t.Errorf("expected database=appdb, got %v", meta["database"])
		}
		if _, has := meta["pg_version"]; !has {
			t.Error("expected pg_version key")
		}
		if meta["hint"] != "do the thing" {
			t.Errorf("expected hint set, got %v", meta["hint"])
		}
	})

	t.Run("empty_hint_omitted", func(t *testing.T) {
		out := map[string]any{}
		srv.injectMeta(out, "", nil)
		meta, _ := out["_meta"].(map[string]any)
		if _, has := meta["hint"]; has {
			t.Error("expected no hint key when empty")
		}
	})
}

// metaJSONResult merges the payload at the top level and injects _meta below
// it; the body must remain valid JSON for downstream MCP transport.
func TestMetaJSONResult_ProducesValidJSON(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2 on x86_64", Database: "appdb",
		Timestamp: time.Now().UTC(),
	}
	srv := NewOfflineServer(snap, lint.DefaultConfig())

	payload := map[string]any{"valid": true, "warnings": []string{"w1"}}
	res := srv.metaJSONResult(payload, "", "use advise", nil)
	if res == nil || len(res.Content) == 0 {
		t.Fatal("expected non-empty result")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, tc.Text)
	}
	meta, ok := decoded["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected _meta object, got %T", decoded["_meta"])
	}
	if meta["mode"] != "offline" {
		t.Errorf("expected offline mode, got %v", meta["mode"])
	}
	if meta["hint"] != "use advise" {
		t.Errorf("expected hint set, got %v", meta["hint"])
	}
	if decoded["valid"] != true {
		t.Errorf("expected payload merged: valid=true")
	}
}
