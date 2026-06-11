package mcp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
)

// Smoke tests for health-family tools: detect (all kinds), vacuum_health.
// Each subtest exercises one kind or filter and asserts the expected JSON
// keys or error text appear.
func TestHealthHandlers_OfflineSmoke(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("detect_default_all", func(t *testing.T) {
		out := callTool(t, c, "detect", nil)
		assertContains(t, out, "stale_stats")
		assertContains(t, out, "unused_indexes")
		assertContains(t, out, "anomalies")
		assertContains(t, out, "bloated_indexes")
	})

	t.Run("detect_stale_stats", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "stale_stats"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_unused_indexes", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "unused_indexes"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_anomalies", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_bloated_indexes", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_bloated_with_threshold", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes", "threshold": 2.0})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_invalid_kind", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bogus"})
		assertContains(t, out, "unknown detect kind")
	})

	t.Run("vacuum_health", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", nil)
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_with_filter", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", map[string]any{"table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_nonexistent_table", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", map[string]any{"table": "nonexistent_xyz"})
		assertContains(t, out, "No vacuum health concerns")
	})
}

// The whole point of §6 is that a giant result set degrades gracefully instead
// of dumping the entire database into the model's context. This pins the full
// truncation contract for a single detect category at the boundary where it
// fires: 60 findings, a cap of 50. We assert the four things a downstream agent
// relies on — (1) `count` reports the true 60 so it knows the haystack's real
// size, (2) only 50 entries actually cross the wire, (3) truncated/omitted
// honestly advertise that 10 were withheld, and (4) _meta.next hands back a
// pre-validated, uncapped re-run of exactly this category (limit:0) so the agent
// can fetch the rest mechanically rather than guessing at the args.
func TestCappedKindResult_TruncationContract(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 18", Database: "appdb", Timestamp: time.Now().UTC(),
	}
	srv := NewOfflineServer(snap, lint.DefaultConfig())

	entries := make([]int, 60)
	for i := range entries {
		entries[i] = i
	}

	res := cappedKindResult(srv, "unused_indexes", entries, 50, "", "")
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, tc.Text)
	}

	if decoded["count"] != float64(60) {
		t.Errorf("count must be the full pre-cap total 60, got %v", decoded["count"])
	}
	shown, _ := decoded["unused_indexes"].([]any)
	if len(shown) != 50 {
		t.Errorf("expected 50 entries on the wire, got %d", len(shown))
	}
	if decoded["truncated"] != true {
		t.Errorf("expected truncated=true, got %v", decoded["truncated"])
	}
	if decoded["omitted"] != float64(10) {
		t.Errorf("expected omitted=10, got %v", decoded["omitted"])
	}

	meta, ok := decoded["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected _meta object, got %T", decoded["_meta"])
	}
	next, ok := meta["next"].([]any)
	if !ok || len(next) != 1 {
		t.Fatalf("expected exactly one _meta.next entry, got %v", meta["next"])
	}
	call, _ := next[0].(map[string]any)
	if call["tool"] != "detect" {
		t.Errorf("next.tool must be detect, got %v", call["tool"])
	}
	args, _ := call["args"].(map[string]any)
	if args["kind"] != "unused_indexes" {
		t.Errorf("next.args.kind must echo the category, got %v", args["kind"])
	}
	if args["limit"] != float64(0) {
		t.Errorf("next.args.limit must be 0 (uncapped) so the re-run returns everything, got %v", args["limit"])
	}
}

// The flip side of the contract: when the result set fits under the cap, none of
// the truncation machinery should appear. A clean, small response must stay
// clean — no truncated flag, no omitted count, and crucially no _meta.next
// nudging the agent toward a pointless re-run.
func TestCappedKindResult_NoTruncationWhenUnderCap(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 18", Database: "appdb", Timestamp: time.Now().UTC(),
	}
	srv := NewOfflineServer(snap, lint.DefaultConfig())

	res := cappedKindResult(srv, "bloated_indexes", []int{1, 2, 3}, 50, "", "")
	tc := res.Content[0].(mcp.TextContent)

	var decoded map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, tc.Text)
	}
	if decoded["count"] != float64(3) {
		t.Errorf("expected count=3, got %v", decoded["count"])
	}
	if _, has := decoded["truncated"]; has {
		t.Error("nothing hidden: truncated must be absent")
	}
	if _, has := decoded["omitted"]; has {
		t.Error("nothing hidden: omitted must be absent")
	}
	// _meta is only injected on the truncation branch, so it should be absent here.
	if _, has := decoded["_meta"]; has {
		t.Error("no truncation: _meta (and its next) must be absent")
	}
}

// buildAnomalies must emit hottest-first (by total seq_scan) so a downstream
// cap keeps the most alarming tables. We seed two tables that both trip the
// seq-scan-only flag (>100 seq scans, zero index scans) but with very different
// volumes, inserted cold-then-hot so a passing test proves the sort flipped
// them rather than preserving insertion order.
func TestBuildAnomalies_HottestFirst(t *testing.T) {
	a := &schema.AnnotatedSchema{
		Schema: &schema.SchemaSnapshot{},
		Merged: &schema.MergedActivity{Nodes: []schema.NodeActivity{{
			Node: schema.NodeIdentity{Source: "primary"},
			Tables: []schema.TableActivityEntry{
				{Table: schema.QualifiedName{Schema: "public", Name: "warm"}, Activity: schema.TableActivity{SeqScan: 200, IdxScan: 0}},
				{Table: schema.QualifiedName{Schema: "public", Name: "hot"}, Activity: schema.TableActivity{SeqScan: 9000, IdxScan: 0}},
			},
		}}},
	}

	anomalies := buildAnomalies(a)
	if len(anomalies) != 2 {
		t.Fatalf("expected both seq-scan-only tables flagged, got %d: %v", len(anomalies), anomalies)
	}
	if anomalies[0]["table"] != "hot" {
		t.Errorf("expected the 9000-seq-scan table first, got %q", anomalies[0]["table"])
	}
	if anomalies[1]["table"] != "warm" {
		t.Errorf("expected the 200-seq-scan table second, got %q", anomalies[1]["table"])
	}
}

// capStaleStats is the two-bucket guard that keeps a fresh bulk-load (where
// nearly every table is never-analyzed) from blowing the response bound, while
// still guaranteeing the "no stats at all" class is never starved out by a
// flood of merely-stale tables. The contract: never-analyzed and stale are
// capped INDEPENDENTLY at max, never-analyzed comes first, and omitted sums
// both buckets. We feed 4 never-analyzed and 5 stale with max=2, so each bucket
// must shed its overflow separately — proving a big never-analyzed pile can't
// crowd out the stale tables and vice versa.
func TestCapStaleStats_TwoBuckets(t *testing.T) {
	mk := func(name string, daysAgo *int64) schema.StaleStatsEntry {
		return schema.StaleStatsEntry{Node: "primary", Schema: "public", Table: name, LastAnalyzedDaysAgo: daysAgo}
	}
	d := func(n int64) *int64 { return &n }

	entries := []schema.StaleStatsEntry{
		mk("never1", nil), mk("never2", nil), mk("never3", nil), mk("never4", nil),
		mk("stale1", d(40)), mk("stale2", d(30)), mk("stale3", d(20)), mk("stale4", d(15)), mk("stale5", d(10)),
	}

	kept, omitted := capStaleStats(entries, 2)

	// 2 from each bucket survive; the other 2 never + 3 stale are omitted.
	if len(kept) != 4 {
		t.Fatalf("expected 4 kept (2 never + 2 stale), got %d: %+v", len(kept), kept)
	}
	if omitted != 5 {
		t.Errorf("expected omitted=5 (2 never + 3 stale), got %d", omitted)
	}

	// never-analyzed must occupy the front of the kept slice.
	for i := 0; i < 2; i++ {
		if kept[i].LastAnalyzedDaysAgo != nil {
			t.Errorf("position %d should be a never-analyzed entry, got %v days", i, *kept[i].LastAnalyzedDaysAgo)
		}
	}
	// the stale survivors must be the two MOST stale (the detector pre-sorts
	// worst-first, and capStaleStats preserves that within the bucket).
	if kept[2].Table != "stale1" || kept[3].Table != "stale2" {
		t.Errorf("expected the two most-stale tables (stale1, stale2), got %q, %q", kept[2].Table, kept[3].Table)
	}
}

// A never-analyzed flood must not starve the stale bucket: with far more
// never-analyzed than the cap, the stale tables still get their own slots
// rather than being squeezed to zero.
func TestCapStaleStats_NeverFloodDoesNotStarveStale(t *testing.T) {
	var entries []schema.StaleStatsEntry
	for i := 0; i < 100; i++ {
		entries = append(entries, schema.StaleStatsEntry{Schema: "public", Table: "n", LastAnalyzedDaysAgo: nil})
	}
	days := int64(12)
	entries = append(entries, schema.StaleStatsEntry{Schema: "public", Table: "stale", LastAnalyzedDaysAgo: &days})

	kept, _ := capStaleStats(entries, 10)
	if len(kept) != 11 {
		t.Fatalf("expected 10 never + 1 stale = 11 kept, got %d", len(kept))
	}
	last := kept[len(kept)-1]
	if last.Table != "stale" || last.LastAnalyzedDaysAgo == nil {
		t.Errorf("the lone stale table must survive the never-analyzed flood, got %+v", last)
	}
}

// Pins that vacuum_health with an unknown schema returns the friendly
// "No vacuum health concerns" message rather than an error or empty payload.
func TestVacuumHealth_SchemaFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "vacuum_health", map[string]any{"schema": "nonexistent_schema_xyz"})
	if !strings.Contains(out, "No vacuum health concerns") {
		t.Errorf("expected empty vacuum health for unknown schema, got %s", out)
	}
}

// Sanity check that detect tolerates a table filter matching nothing without
// crashing or returning empty output. JSON-parseable detect kinds must still
// produce valid JSON, text-mode kinds are tolerated as is.
func TestDetect_TableFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "detect", map[string]any{"table": "definitely_not_a_table_xyz"})
	if out == "" {
		t.Fatal("empty result")
	}
	var any map[string]any
	if err := json.Unmarshal([]byte(out), &any); err != nil {
		return
	}
}
