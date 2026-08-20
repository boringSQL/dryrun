package mcp

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// loadDemoSchema loads the checked-in demo fixture used by offline MCP tests.
func loadDemoSchema(t *testing.T) *schema.SchemaSnapshot {
	t.Helper()
	data, err := os.ReadFile("../../examples/demo/.dryrun/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var snap schema.SchemaSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	return &snap
}

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

// The compact index serializer deliberately treats is_valid/is_ready as
// "exception-only" signals. A healthy database has every index valid and
// ready, so emitting `"is_valid": true` on all 200 indexes would be pure
// noise that buries the one index that actually matters. Instead the compact
// shape uses *bool pointers with omitempty and only writes the keys when the
// underlying index is in a bad state. This test pins both halves of that
// contract: the healthy index must omit the keys entirely, and the broken
// index must surface them as explicit false so a diagnosing agent can see it.
func TestToCompactTable_IndexValidityIsExceptionOnly(t *testing.T) {
	tbl := &schema.Table{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			// A perfectly normal, healthy index — valid and ready. We expect
			// the serializer to stay completely silent about its flags.
			{Name: "idx_healthy", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true, IsReady: true},
			// A broken index — the kind of carcass a failed CREATE INDEX
			// CONCURRENTLY leaves behind. Both flags are false and the
			// serializer must explicitly surface them so it isn't silently
			// mistaken for a healthy index.
			{Name: "idx_broken", Columns: []string{"email"}, IndexType: "btree", IsValid: false, IsReady: false},
		},
	}

	out := toCompactTable(tbl, nil)

	// Round-trip through JSON because the omitempty behaviour we care about is
	// a property of the wire encoding, not of the in-memory struct. Asserting
	// on the decoded map is the most faithful representation of what an MCP
	// client on the other end of the transport will actually observe.
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshalling compact table failed: %v", err)
	}
	var decoded struct {
		Indexes []map[string]any `json:"indexes"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshalling compact table failed: %v\n%s", err, raw)
	}
	if len(decoded.Indexes) != 2 {
		t.Fatalf("expected 2 indexes in compact output, got %d", len(decoded.Indexes))
	}

	healthy, broken := decoded.Indexes[0], decoded.Indexes[1]

	// The healthy index must NOT carry either key. Their mere presence — even
	// set to true — would defeat the entire point of the exception-only design.
	if _, has := healthy["is_valid"]; has {
		t.Errorf("healthy index should omit is_valid, but key was present: %v", healthy["is_valid"])
	}
	if _, has := healthy["is_ready"]; has {
		t.Errorf("healthy index should omit is_ready, but key was present: %v", healthy["is_ready"])
	}

	// The broken index must carry both keys, and they must both be false so the
	// downstream consumer can tell exactly what kind of broken it is dealing with.
	if v, has := broken["is_valid"]; !has || v != false {
		t.Errorf("broken index should surface is_valid=false, got present=%v value=%v", has, v)
	}
	if v, has := broken["is_ready"]; !has || v != false {
		t.Errorf("broken index should surface is_ready=false, got present=%v value=%v", has, v)
	}
}

// capItems is the primitive behind every §6 response cap. The contract has
// four corners and each one matters for a different reason, so we pin all four:
// the happy "everything fits" path (no omission, slice returned untouched), the
// "spills over" path (exactly max kept, the rest counted as omitted so the
// caller can advertise how much it hid), and the two opt-out encodings — max of
// zero and a negative max — which both mean "the user disabled the cap, give me
// everything" and must never drop a single element.
func TestCapItems(t *testing.T) {
	items := []int{1, 2, 3, 4, 5}

	t.Run("under_cap_keeps_all", func(t *testing.T) {
		kept, omitted := capItems(items, 10)
		if len(kept) != 5 || omitted != 0 {
			t.Fatalf("expected all 5 kept and 0 omitted, got kept=%d omitted=%d", len(kept), omitted)
		}
	})

	t.Run("equal_to_cap_keeps_all", func(t *testing.T) {
		kept, omitted := capItems(items, 5)
		if len(kept) != 5 || omitted != 0 {
			t.Fatalf("boundary: expected 5 kept and 0 omitted, got kept=%d omitted=%d", len(kept), omitted)
		}
	})

	t.Run("over_cap_truncates_and_counts", func(t *testing.T) {
		kept, omitted := capItems(items, 2)
		if len(kept) != 2 || omitted != 3 {
			t.Fatalf("expected 2 kept and 3 omitted, got kept=%d omitted=%d", len(kept), omitted)
		}
	})

	t.Run("zero_max_disables_cap", func(t *testing.T) {
		kept, omitted := capItems(items, 0)
		if len(kept) != 5 || omitted != 0 {
			t.Fatalf("max=0 means uncapped: expected 5 kept and 0 omitted, got kept=%d omitted=%d", len(kept), omitted)
		}
	})

	t.Run("negative_max_disables_cap", func(t *testing.T) {
		kept, omitted := capItems(items, -1)
		if len(kept) != 5 || omitted != 0 {
			t.Fatalf("negative max means uncapped: expected 5 kept and 0 omitted, got kept=%d omitted=%d", len(kept), omitted)
		}
	})
}

// entryBlock is the JSON shape the detect tool emits per category. The crucial
// invariant is that `count` is ALWAYS the full pre-cap total — a model staring
// at a capped `entries` array still needs to know the true size of the haystack
// to decide whether to narrow or page. The truncated/omitted keys are the
// inverse signal: present (and honest) only when something was actually hidden,
// absent entirely when the whole set fit so the common case stays clean.
func TestEntryBlock(t *testing.T) {
	t.Run("within_cap_omits_truncation_keys", func(t *testing.T) {
		block := entryBlock([]int{1, 2, 3}, 50)
		if block["count"] != 3 {
			t.Errorf("expected count=3 (the full total), got %v", block["count"])
		}
		if _, has := block["truncated"]; has {
			t.Error("nothing was hidden, so truncated must be absent")
		}
		if _, has := block["omitted"]; has {
			t.Error("nothing was hidden, so omitted must be absent")
		}
	})

	t.Run("over_cap_reports_full_count_and_omission", func(t *testing.T) {
		block := entryBlock([]int{1, 2, 3, 4, 5}, 2)
		if block["count"] != 5 {
			t.Errorf("count must stay the full total (5) even though entries is capped, got %v", block["count"])
		}
		entries, ok := block["entries"].([]int)
		if !ok || len(entries) != 2 {
			t.Fatalf("expected 2 capped entries, got %v", block["entries"])
		}
		if block["truncated"] != true {
			t.Error("expected truncated=true when entries were dropped")
		}
		if block["omitted"] != 3 {
			t.Errorf("expected omitted=3, got %v", block["omitted"])
		}
	})
}

// filterByQual is what finally makes detect/vacuum_health's long-declared
// schema/table parameters real rather than decorative. It mirrors filterSnap's
// AND-narrowing semantics but operates on already-computed result entries
// (because the detectors draw from different sources and can't all be filtered
// upstream). We reuse a tiny (schema, table) pair type and the same overlapping
// names trick from filterTestSnap so the cross-axis behaviour is unambiguous.
func TestFilterByQual(t *testing.T) {
	type qual struct {
		s string
		n string
	}
	key := func(q qual) (string, string) { return q.s, q.n }
	items := []qual{
		{"public", "users"},
		{"public", "orders"},
		{"billing", "invoices"},
		{"billing", "orders"},
	}

	t.Run("empty_filters_passthrough", func(t *testing.T) {
		out := filterByQual(items, "", "", key)
		if len(out) != 4 {
			t.Fatalf("expected all 4 entries unfiltered, got %d", len(out))
		}
	})

	t.Run("schema_only", func(t *testing.T) {
		out := filterByQual(items, "public", "", key)
		if len(out) != 2 {
			t.Fatalf("expected 2 public entries, got %d", len(out))
		}
		for _, q := range out {
			if q.s != "public" {
				t.Errorf("leaked non-public entry %+v", q)
			}
		}
	})

	t.Run("table_only_crosses_schemas", func(t *testing.T) {
		out := filterByQual(items, "", "orders", key)
		if len(out) != 2 {
			t.Fatalf("expected both orders entries across schemas, got %d", len(out))
		}
	})

	t.Run("schema_and_table_and_narrow", func(t *testing.T) {
		out := filterByQual(items, "billing", "orders", key)
		if len(out) != 1 || out[0].s != "billing" || out[0].n != "orders" {
			t.Fatalf("expected the unique billing.orders entry, got %+v", out)
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
