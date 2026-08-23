package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/vacuum"
)

// Smoke tests for the health family: detect, across every kind.
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
		assertContains(t, out, "bloated_tables")
		assertContains(t, out, "vacuum_health")
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

	t.Run("detect_bloated_tables", func(t *testing.T) {
		// this kind was missing from the declared enum when input validation
		// landed, so the validator rejected a working handler path (and the
		// _meta.next follow-ups the server itself emits for it) — going through
		// the validated client here is the regression tripwire
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_tables"})
		if out == "" {
			t.Fatal("empty result")
		}
		if strings.Contains(out, "input schema validation failed") {
			t.Fatalf("declared enum rejects bloated_tables: %s", out)
		}
	})

	t.Run("detect_bloated_with_threshold", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes", "threshold": 2.0})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_invalid_kind", func(t *testing.T) {
		// with WithInputSchemaValidation enabled, the declared enum rejects the
		// bogus kind before the handler runs — this is the server-side schema
		// validation contract (plan item B), so we assert the validator's error
		// rather than the handler's "unknown detect kind" fallback (which still
		// guards direct handler calls that bypass the server layer)
		out := callTool(t, c, "detect", map[string]any{"kind": "bogus"})
		assertContains(t, out, "input schema validation failed")
	})

	t.Run("vacuum_health", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "vacuum_health"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_with_filter", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "vacuum_health", "table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_nonexistent_table", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "vacuum_health", "table": "nonexistent_xyz"})
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
	// _meta rides every kind result -- it carries the capture stamps -- but with
	// nothing truncated it must not carry a next.
	meta, has := decoded["_meta"].(map[string]any)
	if !has {
		t.Fatal("_meta must be present: it carries the capture stamps")
	}
	if _, has := meta["next"]; has {
		t.Error("no truncation: _meta.next must be absent")
	}
}

// AnalyzeVacuumHealth returns a row per large table whether or not it found
// anything, so the kind=all block must filter to rows that actually carry a
// finding -- otherwise the default detect call ships a fat block of healthy
// tables, which is the opposite of why vacuum_health was merged into detect.
func TestWithFindings_DropsHealthyTables(t *testing.T) {
	in := []vacuum.VacuumHealth{
		{Schema: "public", Table: "healthy"},
		{Schema: "public", Table: "sick", Findings: []vacuum.VacuumFinding{{Code: vacuum.CodeAutovacuumDisabled}}},
		{Schema: "public", Table: "also_healthy", Findings: []vacuum.VacuumFinding{}},
	}
	got := withFindings(in)
	if len(got) != 1 {
		t.Fatalf("want only the table with findings, got %d: %+v", len(got), got)
	}
	if got[0].Table != "sick" {
		t.Errorf("kept the wrong table: %q", got[0].Table)
	}
	if withFindings(nil) == nil {
		t.Error("must return an empty slice, not nil, so it marshals as [] not null")
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

	anomalies, _ := buildAnomalies(a)
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

// Pins that detect kind=vacuum_health with an unknown schema returns the
// friendly "No vacuum health concerns" message rather than an error or empty
// payload.
func TestVacuumHealth_SchemaFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "detect", map[string]any{"kind": "vacuum_health", "schema": "nonexistent_schema_xyz"})
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

// The unattributed_scans interpretation used to live in the detect tool
// description, where every agent saw it before deciding to call detect at all.
// It is now emitted with the finding: absent unless a returned anomaly carries
// the flag, present (and naming the top-level-only capture) when one does.
func TestUnattributedScansHint(t *testing.T) {
	if got := unattributedScansHint(nil); got != "" {
		t.Errorf("no anomalies: want empty hint, got %q", got)
	}

	other := []map[string]any{{"table": "orders", "flags": []string{"seq_scan_only"}}}
	if got := unattributedScansHint(other); got != "" {
		t.Errorf("unrelated flag: want empty hint, got %q", got)
	}

	flagged := []map[string]any{
		{"table": "orders", "flags": []string{"seq_scan_only"}},
		{"table": "events", "flags": []string{string(schema.FlagUnattributedScans)}},
	}
	got := unattributedScansHint(flagged)
	if got == "" {
		t.Fatal("flagged anomaly: want the interpretation hint, got empty")
	}
	for _, want := range []string{"unattributed_scans", "pg_stat_statements.track", "log_nested_statements"} {
		if !strings.Contains(got, want) {
			t.Errorf("hint missing %q: %s", want, got)
		}
	}
}

// anomalySchema builds the narrowest snapshot that trips (or does not trip)
// unattributed_scans: one very hot table, and a top-level-only pg_stat_statements
// capture whose statements reference referenced instead.
func anomalySchema(referenced string) *schema.AnnotatedSchema {
	return anomalySchemaIn("public", referenced)
}

func anomalySchemaIn(schemaName, referenced string) *schema.AnnotatedSchema {
	track := "top"
	return &schema.AnnotatedSchema{
		Schema: &schema.SchemaSnapshot{},
		Merged: &schema.MergedActivity{Nodes: []schema.NodeActivity{{
			Node: schema.NodeIdentity{Source: "primary"},
			Tables: []schema.TableActivityEntry{{
				Table:    schema.QualifiedName{Schema: schemaName, Name: "events"},
				Activity: schema.TableActivity{SeqScan: 200_000, IdxScan: 1},
			}},
		}}},
		QueryStats: []schema.QueryStatsSnapshot{{
			PgssTrack: &track,
			RawRows:   1,
			Queries:   []schema.QueryStatsEntry{{Canonical: "SELECT id FROM " + referenced + " WHERE x = $1"}},
		}},
	}
}

// detect must carry the interpretation in _meta.hint when a returned anomaly
// is flagged, and stay silent when the captured statements account for the
// scans.
func TestDetect_AnomaliesCarriesUnattributedScansHint(t *testing.T) {
	hintFor := func(t *testing.T, referenced string) string {
		t.Helper()
		srv := NewOfflineServerAnnotated(anomalySchema(referenced), lint.DefaultConfig())

		var req mcp.CallToolRequest
		req.Params.Name = "detect"
		req.Params.Arguments = map[string]any{"kind": "anomalies"}
		res, err := srv.handleDetect(context.Background(), req)
		if err != nil {
			t.Fatalf("handleDetect: %v", err)
		}
		text, ok := res.Content[0].(mcp.TextContent)
		if !ok {
			t.Fatalf("expected TextContent, got %T", res.Content[0])
		}

		var decoded struct {
			Meta struct {
				Hint string `json:"hint"`
			} `json:"_meta"`
		}
		if err := json.Unmarshal([]byte(text.Text), &decoded); err != nil {
			t.Fatalf("unmarshal detect result: %v\n%s", err, text.Text)
		}
		return decoded.Meta.Hint
	}

	if got := hintFor(t, "orders"); !strings.Contains(got, "unattributed_scans") {
		t.Errorf("flagged anomaly: _meta.hint must explain the flag, got %q", got)
	}
	if got := hintFor(t, "events"); strings.Contains(got, "unattributed_scans") {
		t.Errorf("statements account for the scans: want no interpretation, got %q", got)
	}
}

// A schema argument defaults to "public" for lookup tools, which need one to
// resolve against. detect is a filter: it used to default the same way, so an
// application living in its own schema got "no findings" with no error and no
// hint -- a confidently wrong answer. Absent must mean every schema.
func TestDetect_NoSchemaArgCoversAllSchemas(t *testing.T) {
	srv := NewOfflineServerAnnotated(anomalySchemaIn("app", "orders"), lint.DefaultConfig())

	call := func(t *testing.T, args map[string]any) string {
		t.Helper()
		var req mcp.CallToolRequest
		req.Params.Name = "detect"
		req.Params.Arguments = args
		res, err := srv.handleDetect(context.Background(), req)
		if err != nil {
			t.Fatalf("handleDetect: %v", err)
		}
		text, ok := res.Content[0].(mcp.TextContent)
		if !ok {
			t.Fatalf("expected TextContent, got %T", res.Content[0])
		}
		return text.Text
	}

	// kind=all is the default and the path an agent actually hits
	for _, kind := range []string{"all", "anomalies"} {
		if got := call(t, map[string]any{"kind": kind}); !strings.Contains(got, `"app"`) {
			t.Errorf("kind=%s: no schema argument must cover every schema, got %s", kind, got)
		}
	}
	if got := call(t, map[string]any{"kind": "anomalies", "schema": "app"}); !strings.Contains(got, `"app"`) {
		t.Errorf("explicit schema=app must still match, got %s", got)
	}
	// assert the empty result, not merely the absence of "app": a panic or an
	// error result would satisfy a bare NotContains
	got := call(t, map[string]any{"kind": "anomalies", "schema": "public"})
	if !strings.Contains(got, "No anomalies detected.") {
		t.Errorf("explicit schema=public must exclude app, got %s", got)
	}
}

// setupOfflineTest wraps a bare SchemaSnapshot, so Planner is nil, SizingFor
// returns nil for every table and AnalyzeVacuumHealth bails before producing a
// row -- every other vacuum assertion in this file only ever sees the empty
// path. This builds a server with planner + activity so the filtering is
// exercised for real: one table with a finding, one large and healthy.
func setupVacuumOfflineTest(t *testing.T) *client.Client {
	t.Helper()
	// non-nil slices: describe_table detail=full marshals these straight through
	// and its output schema requires arrays, so nil would fail validation
	tbl := func(name string, reloptions []string) schema.Table {
		return schema.Table{
			Schema: "public", Name: name, Reloptions: reloptions,
			Columns:     []schema.Column{col("id")},
			Indexes:     []schema.Index{},
			Constraints: []schema.Constraint{},
		}
	}
	sick := tbl("sick", []string{"autovacuum_enabled=false"})
	healthy := tbl("healthy", nil)
	a := &schema.AnnotatedSchema{
		Schema: &schema.SchemaSnapshot{
			PgVersion: "PostgreSQL 17.0", Database: "test",
			Timestamp: time.Now().UTC(), ContentHash: "test",
			Tables: []schema.Table{sick, healthy},
		},
		Planner: &schema.PlannerStatsSnapshot{Tables: []schema.TableSizingEntry{
			// >=10k so both enter the scan, <1M so "healthy" does not trip
			// default_knobs_large_table just for being big
			{Table: sick.Qual(), Sizing: schema.TableSizing{Reltuples: 50_000}},
			{Table: healthy.Qual(), Sizing: schema.TableSizing{Reltuples: 50_000}},
		}},
		Merged: &schema.MergedActivity{Nodes: []schema.NodeActivity{{
			Node: schema.NodeIdentity{Source: "primary"},
			Tables: []schema.TableActivityEntry{
				{Table: sick.Qual(), Activity: schema.TableActivity{NDeadTup: 10}},
				{Table: healthy.Qual(), Activity: schema.TableActivity{NDeadTup: 0}},
			},
		}}},
	}
	return serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))
}

// detect reports offenders. A large table with no vacuum concern must not ride
// along -- that is the whole reason vacuum_health could be folded into detect
// without inflating the default payload.
func TestDetectVacuumHealth_ExcludesHealthyTables(t *testing.T) {
	c := setupVacuumOfflineTest(t)

	// scoped to the vacuum_health block: under kind=all the healthy table
	// legitimately shows up in stale_stats (never analyzed), which is a
	// different category's business
	for _, tc := range []struct {
		name string
		args map[string]any
		all  bool
	}{
		{"kind=vacuum_health", map[string]any{"kind": "vacuum_health"}, false},
		{"kind=all", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var decoded map[string]any
			if err := json.Unmarshal([]byte(callTool(t, c, "detect", tc.args)), &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			block := decoded
			if tc.all {
				b, ok := decoded["vacuum_health"].(map[string]any)
				if !ok {
					t.Fatalf("kind=all must carry a vacuum_health block, got %v", decoded["vacuum_health"])
				}
				block = b
			}
			entries, _ := json.Marshal(block)
			if !strings.Contains(string(entries), `"sick"`) {
				t.Errorf("the table with a finding must appear: %s", entries)
			}
			if strings.Contains(string(entries), `"healthy"`) {
				t.Errorf("a table with no vacuum finding must not appear: %s", entries)
			}
		})
	}
}
