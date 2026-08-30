package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// The names here are deliberately overlapping: "order" is a table name, part of
// two other table names, a column name, an index name, an enum label and a word
// inside a view definition. Ranking is the whole point of the tool, and a
// fixture where each term matches one thing proves nothing about it.
func findSnap() *schema.SchemaSnapshot {
	orders := testTable("public", "orders", testCol("status", "text", false))
	orders.Comment = strPtr("customer orders, one row per checkout")
	orders.Columns[1].Comment = strPtr("order_status enum values, denormalised")
	orders.Indexes = append(orders.Indexes, schema.Index{
		Name: "orders_status_idx", Columns: []string{"status"}, IndexType: "btree",
		IsValid: true, IsReady: true,
		Definition: indexDef(false, "orders_status_idx", "public", "orders", "status"),
	})

	orderItems := testTable("public", "order_items", testCol("qty", "integer", false))
	reorders := testTable("public", "reorders", testCol("note", "text", true))
	audit := testTable("audit", "trail", testCol("payload", "jsonb", true))
	audit.Columns[1].Comment = strPtr("raw order payload as received")

	return finish(&schema.SchemaSnapshot{
		Tables: []schema.Table{orders, orderItems, reorders, audit},
		Views: []schema.View{
			{
				Schema: "public", Name: "revenue_by_day",
				// the matched term sits well past maxMatchDetail, so an
				// excerpt that shows the head of the body proves nothing
				Definition: "SELECT\n" + strings.Repeat("  -- padding padding padding\n", 20) +
					"  date_trunc('day', created_at),\n  sum(total)\nFROM public.orders\nWHERE status <> 'cancelled'",
			},
			{
				Schema: "public", Name: "orders_mv", IsMaterialized: true,
				Definition: "SELECT 1",
				Comment:    strPtr("nightly rollup"),
			},
		},
		Functions: []schema.Function{{
			Schema: "public", Name: "recalculate_totals", IdentityArgs: "bigint",
			ReturnType: "numeric", Language: "plpgsql",
			Comment: strPtr("recomputes an order total from its items"),
		}},
		Enums: []schema.EnumType{{
			Schema: "public", Name: "order_status",
			Labels: []string{"pending", "paid", "cancelled"},
		}},
	})
}

func findServer(t *testing.T) *Server {
	t.Helper()
	return NewOfflineServerAnnotated(annotate(findSnap(), 1000), lint.DefaultConfig())
}

func find(t *testing.T, srv *Server, args map[string]any) findObjectsResult {
	t.Helper()
	var req mcp.CallToolRequest
	req.Params.Name = "find_objects"
	req.Params.Arguments = args
	res, err := srv.handleFindObjects(context.Background(), req)
	if err != nil {
		t.Fatalf("handleFindObjects: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool error: %v", res.Content)
	}
	out, ok := res.StructuredContent.(findObjectsResult)
	if !ok {
		t.Fatalf("want findObjectsResult, got %T", res.StructuredContent)
	}
	return out
}

func objectsOf(res findObjectsResult) []string {
	out := make([]string, len(res.Objects))
	for i, m := range res.Objects {
		out[i] = m.Object
	}
	return out
}

// No query is an inventory, and an inventory of every column of every table is
// not orientation: it lists tables, with the sizes the repo cannot supply.
func TestFindObjects_InventoryDefaultsToTables(t *testing.T) {
	res := find(t, findServer(t), nil)

	want := []string{"audit.trail", "public.order_items", "public.orders", "public.reorders"}
	if got := objectsOf(res); len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, w := range want {
		if res.Objects[i].Object != w {
			t.Errorf("position %d: got %s, want %s", i, res.Objects[i].Object, w)
		}
		if res.Objects[i].Kind != "table" {
			t.Errorf("%s: kind %q in a default inventory", w, res.Objects[i].Kind)
		}
		if res.Objects[i].RowsEstimate == nil || res.Objects[i].SizeBytes == nil {
			t.Errorf("%s: no sizing, which is the reason to call this without a query", w)
		}
	}
}

// The merge's payoff: matches carry the sizes that used to need a second call.
func TestFindObjects_MatchesCarrySizing(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "orders", "kind": "table"})
	if len(res.Objects) == 0 {
		t.Fatal("no matches")
	}
	for _, m := range res.Objects {
		if m.RowsEstimate == nil || m.SizeBytes == nil {
			t.Errorf("%s: a table match without sizing", m.Object)
		}
	}
}

// Exact name, then prefix, then substring, then a hit in prose. An agent that
// searches "orders" wants public.orders first, not order_items.qty.
func TestFindObjects_RanksExactNamesFirst(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "orders"})
	if len(res.Objects) < 3 {
		t.Fatalf("want several matches, got %v", objectsOf(res))
	}
	if res.Objects[0].Object != "public.orders" || res.Objects[0].MatchedOn != matchedName {
		t.Errorf("first match is %+v, want the exact table name", res.Objects[0])
	}

	// prefix beats substring: orders_mv before reorders
	pos := map[string]int{}
	for i, m := range res.Objects {
		pos[m.Object] = i
	}
	if pos["public.orders_mv"] > pos["public.reorders"] {
		t.Errorf("substring outranked prefix: %v", objectsOf(res))
	}
	// and everything that matched only prose comes after everything named
	lastName := -1
	firstMeta := len(res.Objects)
	for i, m := range res.Objects {
		if m.MatchedOn == matchedName {
			lastName = i
		} else if i < firstMeta {
			firstMeta = i
		}
	}
	if firstMeta < lastName {
		t.Errorf("a prose match outranked a name match: %v", objectsOf(res))
	}
}

// The headline: a value that exists only as an enum label, or only in a
// comment or a view body, was unfindable before this tool.
func TestFindObjects_SearchesBeyondNames(t *testing.T) {
	srv := findServer(t)

	for _, tc := range []struct {
		name, query, object, matchedOn string
	}{
		{"enum label", "cancelled", "public.order_status", matchedLabels},
		{"view definition", "date_trunc", "public.revenue_by_day", matchedDefinition},
		{"table comment", "checkout", "public.orders", matchedComment},
		{"column comment", "denormalised", "public.orders.status", matchedComment},
		{"function comment", "recomputes", "public.recalculate_totals(bigint)", matchedComment},
		{"index definition", "btree", "public.orders.orders_status_idx", matchedDefinition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := find(t, srv, map[string]any{"query": tc.query})
			for _, m := range res.Objects {
				if m.Object != tc.object {
					continue
				}
				if m.MatchedOn != tc.matchedOn {
					t.Errorf("matched_on = %q, want %q", m.MatchedOn, tc.matchedOn)
				}
				return
			}
			t.Errorf("%q did not find %s: %v", tc.query, tc.object, objectsOf(res))
		})
	}
}

// public.orders matches on its name and on its comment ("customer orders").
// That is one row, reported as the stronger of the two.
func TestFindObjects_NameWinsOverProse(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "orders", "kind": "table"})

	var seen int
	for _, m := range res.Objects {
		if m.Object != "public.orders" {
			continue
		}
		seen++
		if m.MatchedOn != matchedName {
			t.Errorf("matched_on = %q, want name", m.MatchedOn)
		}
	}
	if seen != 1 {
		t.Errorf("public.orders appeared %d times: %v", seen, objectsOf(res))
	}
}

// An inventory matched nothing, so it must not claim a field.
func TestFindObjects_InventoryHasNoMatchedOn(t *testing.T) {
	res := find(t, findServer(t), nil)
	for _, m := range res.Objects {
		if m.MatchedOn != "" {
			t.Errorf("%s: matched_on = %q with no query", m.Object, m.MatchedOn)
		}
	}
}

// A view body can be arbitrarily long. The row says why it matched; it is not
// a way to read the view.
func TestFindObjects_ViewDefinitionIsBoundedAndOneLine(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "date_trunc", "kind": "view"})
	if len(res.Objects) != 1 {
		t.Fatalf("want the view, got %v", objectsOf(res))
	}
	d := res.Objects[0].Detail
	if strings.Contains(d, "\n") {
		t.Errorf("newlines survived into detail: %q", d)
	}
	if len([]rune(d)) > maxMatchDetail {
		t.Errorf("detail is %d runes, cap is %d", len([]rune(d)), maxMatchDetail)
	}
	if !strings.Contains(d, "date_trunc") {
		t.Errorf("detail does not show the match: %q", d)
	}
}

// A multi-byte definition must not be cut mid-rune. Case folding is a separate
// hazard, covered by TestFindObjects_ExcerptSurvivesCaseFoldingWidthChange.
func TestFindObjects_TruncationIsRuneSafe(t *testing.T) {
	snap := findSnap()
	snap.Views[0].Definition = "SELECT '" + strings.Repeat("é", 400) + "' AS marker"
	srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

	res := find(t, srv, map[string]any{"query": "marker", "kind": "view"})
	if len(res.Objects) != 1 {
		t.Fatalf("want the view, got %v", objectsOf(res))
	}
	if strings.ContainsRune(res.Objects[0].Detail, '�') {
		t.Error("truncation split a rune")
	}
}

func TestFindObjects_KindAndSchemaFilters(t *testing.T) {
	srv := findServer(t)

	t.Run("kind", func(t *testing.T) {
		res := find(t, srv, map[string]any{"query": "order", "kind": "index"})
		for _, m := range res.Objects {
			if m.Kind != "index" {
				t.Errorf("kind filter leaked %s (%s)", m.Kind, m.Object)
			}
		}
		if len(res.Objects) == 0 {
			t.Error("expected the status index")
		}
	})

	t.Run("schema", func(t *testing.T) {
		res := find(t, srv, map[string]any{"query": "order", "schema": "audit"})
		for _, m := range res.Objects {
			if m.Schema != "audit" {
				t.Errorf("schema filter leaked %s", m.Object)
			}
		}
		// audit.trail.payload's comment mentions an order payload
		if len(res.Objects) == 0 {
			t.Error("expected the audit column comment match")
		}
	})

	t.Run("kind with no query inventories that kind", func(t *testing.T) {
		res := find(t, srv, map[string]any{"kind": "enum"})
		if len(res.Objects) != 1 || res.Objects[0].Object != "public.order_status" {
			t.Errorf("got %v", objectsOf(res))
		}
	})

	t.Run("unknown kind is an error naming the kinds", func(t *testing.T) {
		var req mcp.CallToolRequest
		req.Params.Arguments = map[string]any{"kind": "sequence"}
		res, err := srv.handleFindObjects(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if !res.IsError {
			t.Fatal("unknown kind accepted")
		}
		if txt, ok := res.Content[0].(mcp.TextContent); !ok || !strings.Contains(txt.Text, "materialized_view") {
			t.Errorf("error should list the kinds: %v", res.Content)
		}
	})
}

// Sorting by size has to be total and stable, and objects that have no table
// cannot be ordered by one — they go last rather than sorting as zero.
func TestFindObjects_SortBySizePutsUnsizedLast(t *testing.T) {
	snap := findSnap()
	a := annotate(snap, 1000)
	// give the tables distinct sizes: orders biggest, trail smallest
	for i := range a.Planner.Tables {
		switch a.Planner.Tables[i].Table.Name {
		case "orders":
			a.Planner.Tables[i].Sizing.TableSize = 900
			a.Planner.Tables[i].Sizing.Reltuples = 900
		case "order_items":
			a.Planner.Tables[i].Sizing.TableSize = 500
			a.Planner.Tables[i].Sizing.Reltuples = 500
		default:
			a.Planner.Tables[i].Sizing.TableSize = 10
			a.Planner.Tables[i].Sizing.Reltuples = 10
		}
	}
	srv := NewOfflineServerAnnotated(a, lint.DefaultConfig())

	res := find(t, srv, map[string]any{"query": "order", "sort": "size"})
	var sized, unsized []string
	seenUnsized := false
	for _, m := range res.Objects {
		if m.Kind == "view" || m.Kind == "materialized_view" || m.Kind == "function" || m.Kind == "enum" {
			seenUnsized = true
			unsized = append(unsized, m.Object)
			continue
		}
		if seenUnsized {
			t.Errorf("a sized object (%s) came after an unsized one: %v", m.Object, objectsOf(res))
		}
		sized = append(sized, m.Object)
	}
	if len(sized) == 0 || len(unsized) == 0 {
		t.Fatalf("fixture did not produce both kinds: sized=%v unsized=%v", sized, unsized)
	}
	if sized[0] != "public.orders" {
		t.Errorf("biggest table is not first: %v", sized)
	}
}

// Paging must be deterministic: the same query, walked in pages, must visit
// every object exactly once.
func TestFindObjects_PagingIsStable(t *testing.T) {
	srv := findServer(t)
	all := find(t, srv, map[string]any{"query": "order", "limit": float64(0)})

	seen := make([]string, 0, all.Count)
	for off := 0; off < all.Count; off += 2 {
		page := find(t, srv, map[string]any{"query": "order", "limit": float64(2), "offset": float64(off)})
		if page.Count != all.Count {
			t.Fatalf("count changed between pages: %d then %d", all.Count, page.Count)
		}
		seen = append(seen, objectsOf(page)...)
	}
	if strings.Join(seen, ",") != strings.Join(objectsOf(all), ",") {
		t.Errorf("paged walk differs from the whole list:\n%v\n%v", seen, objectsOf(all))
	}
}

func TestFindObjects_TruncationOffersTheUncappedRerun(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "order", "limit": float64(1)})
	if res.Meta == nil || len(res.Meta.Next) == 0 {
		t.Fatal("truncated result carries no follow-up")
	}
	var rerun *NextCall
	for i, n := range res.Meta.Next {
		if n.Tool == "find_objects" {
			rerun = &res.Meta.Next[i]
		}
	}
	if rerun == nil {
		t.Fatalf("no find_objects re-run in %+v", res.Meta.Next)
	}
	if rerun.Args["limit"] != 0 {
		t.Errorf("re-run is not uncapped: %+v", rerun.Args)
	}
	if rerun.Args["query"] != "order" {
		t.Errorf("re-run dropped the query: %+v", rerun.Args)
	}
}

// A table match is a question about that table; hand over the call that reads it.
func TestFindObjects_PointsAtDescribeTable(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "orders", "kind": "table"})
	if res.Meta == nil {
		t.Fatal("no meta")
	}
	for _, n := range res.Meta.Next {
		if n.Tool == "describe_table" && n.Args["table"] == "orders" && n.Args["schema"] == "public" {
			return
		}
	}
	t.Errorf("no describe_table follow-up: %+v", res.Meta.Next)
}

// A column match belongs to a table, and the follow-up has to name that table
// rather than the column.
func TestFindObjects_ColumnFollowupNamesItsTable(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "qty", "kind": "column"})
	if len(res.Objects) != 1 || res.Objects[0].Table != "order_items" {
		t.Fatalf("got %+v", res.Objects)
	}
	for _, n := range res.Meta.Next {
		if n.Tool == "describe_table" && n.Args["table"] == "order_items" {
			return
		}
	}
	t.Errorf("follow-up does not name the owning table: %+v", res.Meta.Next)
}

// A miss has to say what was searched, or the agent cannot tell "not there"
// from "not looked for".
func TestFindObjects_MissExplainsTheSearch(t *testing.T) {
	srv := findServer(t)

	res := find(t, srv, map[string]any{"query": "nosuchthing"})
	if res.Count != 0 {
		t.Fatalf("expected no matches, got %v", objectsOf(res))
	}
	if !strings.Contains(res.Meta.Hint, "Function bodies") {
		t.Errorf("miss should name the boundary that explains it: %q", res.Meta.Hint)
	}

	narrowed := find(t, srv, map[string]any{"query": "cancelled", "kind": "table"})
	if narrowed.Count != 0 {
		t.Fatalf("kind=table matched an enum label: %v", objectsOf(narrowed))
	}
	if !strings.Contains(narrowed.Meta.Hint, "kind=table") {
		t.Errorf("a filtered miss should name the filter: %q", narrowed.Meta.Hint)
	}
}

func TestFindObjects_OffsetBeyondTheEnd(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "order", "offset": float64(9999)})
	if len(res.Objects) != 0 {
		t.Errorf("want an empty page, got %v", objectsOf(res))
	}
	if res.Count == 0 {
		t.Error("count must stay the pre-paging total so the caller can correct the offset")
	}
	// the recovery instruction has to reach _meta.hint, which is where the
	// instructions block tells agents to read prose
	if !strings.Contains(res.Meta.Hint, "beyond the end") {
		t.Errorf("hint does not explain the empty page: %q", res.Meta.Hint)
	}
}

// A negative offset used to slice the result out of range.
func TestFindObjects_NegativeOffsetIsClamped(t *testing.T) {
	res := find(t, findServer(t), map[string]any{"query": "order", "offset": float64(-5)})
	if len(res.Objects) == 0 {
		t.Fatal("no results")
	}
	if res.Offset != 0 {
		t.Errorf("offset = %d, want 0", res.Offset)
	}
}

// An inventory names what it listed; "13 indexs" is not a word.
func TestFindObjects_InventoryNounIsThePluralOfTheKind(t *testing.T) {
	c := setupOfflineTest(t)
	for kind, want := range map[string]string{"index": "indexes", "view": "views", "table": "tables"} {
		out := callTool(t, c, "find_objects", map[string]any{"kind": kind})
		if !strings.Contains(out, want) {
			t.Errorf("kind=%s: no %q in\n%.200s", kind, want, out)
		}
	}
}

// A daily-partitioned table is one object with 365 physical pieces. The
// inventory that lists all 366 is unusable, and the parent already carries
// their rolled-up size (RollUpPartitionSizing runs at snapshot assembly).
func TestFindObjects_InventoryFoldsPartitionChildren(t *testing.T) {
	snap := wideSnap(3, 40)
	a := annotateRolled(snap, 1000)
	srv := NewOfflineServerAnnotated(a, lint.DefaultConfig())

	res := find(t, srv, nil)
	for _, m := range res.Objects {
		if strings.HasPrefix(m.Name, "events_p") {
			t.Errorf("partition child in the inventory: %s", m.Object)
		}
	}
	if res.Count != 2 {
		t.Errorf("count = %d, want the two real tables: %v", res.Count, objectsOf(res))
	}
	if !strings.Contains(res.Meta.Hint, "40 partition children") {
		t.Errorf("folding has to be said out loud: %q", res.Meta.Hint)
	}

	var parent *objectMatch
	for i, m := range res.Objects {
		if m.Name == "events" {
			parent = &res.Objects[i]
		}
	}
	if parent == nil {
		t.Fatalf("the partitioned parent is missing: %v", objectsOf(res))
	}
	if parent.Partitioned == nil || parent.Partitioned.Children != 40 {
		t.Errorf("parent does not report its children: %+v", parent.Partitioned)
	}
	// the parent holds no heap of its own; the rolled-up total is the answer
	if parent.RowsEstimate == nil || *parent.RowsEstimate == 0 {
		t.Errorf("parent reports %v rows, not the roll-up of its children", parent.RowsEstimate)
	}
	if parent.SizeBytes == nil || *parent.SizeBytes == 0 {
		t.Errorf("parent reports %v bytes, so sort=size would bury it", parent.SizeBytes)
	}

	// the regression that started this: sorted by size, the partitioned table
	// must outrank an ordinary one
	bySize := find(t, srv, map[string]any{"sort": "size"})
	if len(bySize.Objects) == 0 || bySize.Objects[0].Name != "events" {
		t.Errorf("sort=size put %v first, not the partitioned parent", objectsOf(bySize))
	}
}

// Folding is an inventory concern; a child asked for by name still exists.
func TestFindObjects_SearchStillFindsPartitionChildren(t *testing.T) {
	snap := wideSnap(3, 40)
	srv := NewOfflineServerAnnotated(annotateRolled(snap, 1000), lint.DefaultConfig())

	res := find(t, srv, map[string]any{"query": "events_p003"})
	if len(res.Objects) == 0 {
		t.Fatal("a partition child became unreachable")
	}
	if res.Objects[0].Object != "public.events_p003" {
		t.Errorf("got %v", objectsOf(res))
	}
}

// A comment can hold a design document. It must not become the response.
func TestFindObjects_CommentsAreBounded(t *testing.T) {
	snap := findSnap()
	long := "line one\n" + strings.Repeat("padding ", 400)
	snap.Tables[0].Comment = &long
	srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

	res := find(t, srv, map[string]any{"query": "orders", "kind": "table"})
	for _, m := range res.Objects {
		if m.Comment == nil {
			continue
		}
		if len([]rune(*m.Comment)) > maxMatchDetail {
			t.Errorf("%s: comment is %d runes", m.Object, len([]rune(*m.Comment)))
		}
		if strings.Contains(*m.Comment, "\n") {
			t.Errorf("%s: newline in a one-line-per-object listing", m.Object)
		}
	}
}

// Overloads share schema.name; the object identity has to tell them apart or a
// caller keying on it silently loses one.
func TestFindObjects_OverloadsAreDistinct(t *testing.T) {
	snap := findSnap()
	snap.Functions = append(snap.Functions, schema.Function{
		Schema: "public", Name: "recalculate_totals", IdentityArgs: "bigint, boolean",
		ReturnType: "numeric", Language: "plpgsql",
	})
	srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

	res := find(t, srv, map[string]any{"kind": "function"})
	seen := map[string]bool{}
	for _, m := range res.Objects {
		if seen[m.Object] {
			t.Errorf("two functions share the identity %q", m.Object)
		}
		seen[m.Object] = true
	}
	if len(res.Objects) != 2 {
		t.Errorf("want both overloads, got %v", objectsOf(res))
	}
}

func TestFindObjects_SortByRows(t *testing.T) {
	snap := findSnap()
	a := annotate(snap, 1000)
	for i := range a.Planner.Tables {
		if a.Planner.Tables[i].Table.Name == "reorders" {
			a.Planner.Tables[i].Sizing.Reltuples = 99999
		}
	}
	res := find(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()), map[string]any{"sort": "rows"})
	if len(res.Objects) == 0 || res.Objects[0].Name != "reorders" {
		t.Errorf("row order ignored: %v", objectsOf(res))
	}
}

// A materialized view is its own kind, and kind=view must not return one.
func TestFindObjects_ViewKindsAreDistinct(t *testing.T) {
	srv := findServer(t)

	plain := find(t, srv, map[string]any{"kind": "view"})
	for _, m := range plain.Objects {
		if m.Kind != "view" {
			t.Errorf("kind=view returned %s (%s)", m.Kind, m.Object)
		}
	}
	mat := find(t, srv, map[string]any{"kind": "materialized_view"})
	if len(mat.Objects) != 1 || mat.Objects[0].Object != "public.orders_mv" {
		t.Errorf("kind=materialized_view got %v", objectsOf(mat))
	}
}

// The schema filter has to apply to every kind, not just the table walk.
func TestFindObjects_SchemaFilterAppliesToEveryKind(t *testing.T) {
	snap := findSnap()
	snap.Views = append(snap.Views, schema.View{Schema: "audit", Name: "trail_v", Definition: "SELECT 1"})
	snap.Enums = append(snap.Enums, schema.EnumType{Schema: "audit", Name: "trail_kind", Labels: []string{"x"}})
	snap.Functions = append(snap.Functions, schema.Function{
		Schema: "audit", Name: "trail_fn", IdentityArgs: "", ReturnType: "void", Language: "sql",
	})
	// a same-named object in another schema, so the filter has something to exclude
	snap.Views = append(snap.Views, schema.View{Schema: "public", Name: "trail_v", Definition: "SELECT 1"})
	srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

	res := find(t, srv, map[string]any{"query": "trail", "schema": "audit"})
	kinds := map[string]bool{}
	for _, m := range res.Objects {
		if m.Schema != "audit" {
			t.Errorf("leaked %s", m.Object)
		}
		kinds[m.Kind] = true
	}
	for _, want := range []string{"table", "view", "enum", "function"} {
		if !kinds[want] {
			t.Errorf("schema filter dropped every %s: %v", want, objectsOf(res))
		}
	}

	// and it must exclude the other schema's objects entirely
	all := find(t, srv, map[string]any{"query": "trail"})
	if all.Count <= res.Count {
		t.Errorf("filtered count %d is not smaller than unfiltered %d", res.Count, all.Count)
	}
}

// Paging has to stay stable under an explicit sort, not just the default.
func TestFindObjects_PagingIsStableUnderSort(t *testing.T) {
	srv := findServer(t)
	all := find(t, srv, map[string]any{"query": "order", "sort": "size", "limit": float64(0)})

	var seen []string
	for off := 0; off < all.Count; off += 3 {
		page := find(t, srv, map[string]any{
			"query": "order", "sort": "size", "limit": float64(3), "offset": float64(off),
		})
		seen = append(seen, objectsOf(page)...)
	}
	if strings.Join(seen, ",") != strings.Join(objectsOf(all), ",") {
		t.Errorf("sorted paging is not stable:\n%v\n%v", seen, objectsOf(all))
	}
}

// The truncation hint counts what this page showed, not where it ended.
func TestFindObjects_TruncationHintCountsThePage(t *testing.T) {
	res := find(t, findServer(t), map[string]any{
		"query": "order", "limit": float64(2), "offset": float64(2),
	})
	if !strings.Contains(res.Meta.Hint, "Showing 2 of") {
		t.Errorf("hint miscounts a later page: %q", res.Meta.Hint)
	}
	// a later page has no "top match" to hand over
	for _, n := range res.Meta.Next {
		if n.Tool == "describe_table" {
			t.Errorf("page 2 suggested a top match: %+v", n)
		}
	}
}

// An unknown sort must fail the way an unknown kind does.
func TestFindObjects_UnknownSortIsAnError(t *testing.T) {
	var req mcp.CallToolRequest
	req.Params.Arguments = map[string]any{"sort": "relevance"}
	res, err := findServer(t).handleFindObjects(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("unknown sort accepted")
	}
	if txt, ok := res.Content[0].(mcp.TextContent); !ok || !strings.Contains(txt.Text, "rows") {
		t.Errorf("error should list the sorts: %v", res.Content)
	}
}

// A view comment is as unbounded as a table comment, and it reaches the text
// body, which promises one line per object.
func TestFindObjects_ViewCommentIsBoundedInBothPaths(t *testing.T) {
	snap := findSnap()
	long := "first line\n" + strings.Repeat("padding ", 400)
	snap.Views[1].Comment = &long
	srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

	res := find(t, srv, map[string]any{"kind": "materialized_view"})
	if len(res.Objects) != 1 || res.Objects[0].Comment == nil {
		t.Fatalf("got %+v", res.Objects)
	}
	if len([]rune(*res.Objects[0].Comment)) > maxMatchDetail {
		t.Errorf("view comment is %d runes", len([]rune(*res.Objects[0].Comment)))
	}

	line := renderMatch(res.Objects[0])
	if strings.Contains(line, "\n") {
		t.Errorf("newline in the rendered line: %q", line)
	}
}

// Every excerpt, wherever the match lands, stays inside the budget.
func TestFindObjects_ExcerptStaysWithinTheCap(t *testing.T) {
	body := strings.Repeat("a", 500) + "needle" + strings.Repeat("b", 500)
	for _, tc := range []struct{ name, def string }{
		{"match in the middle", "SELECT " + body},
		{"match at the head", "SELECT needle " + strings.Repeat("b", 900)},
		{"match at the tail", "SELECT " + strings.Repeat("a", 900) + " needle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := findSnap()
			snap.Views[0].Definition = tc.def
			srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

			res := find(t, srv, map[string]any{"query": "needle", "kind": "view"})
			if len(res.Objects) != 1 {
				t.Fatalf("got %v", objectsOf(res))
			}
			d := res.Objects[0].Detail
			if n := len([]rune(d)); n > maxMatchDetail {
				t.Errorf("detail is %d runes, cap is %d: %q", n, maxMatchDetail, d)
			}
			if !strings.Contains(d, "needle") {
				t.Errorf("window missed the match: %q", d)
			}
		})
	}
}

// pg_partman puts partitions in a schema of their own. Folding them into a
// parent the filter excluded would drop them from every answer.
func TestFindObjects_FoldRespectsTheSchemaFilter(t *testing.T) {
	snap := findSnap()
	parent := testTable("public", "events", testCol("at", "timestamptz", false))
	parent.PartitionInfo = &schema.PartitionInfo{
		Strategy: schema.PartitionRange, Key: "RANGE (at)",
		Children: []schema.PartitionChild{{Schema: "parts", Name: "events_p01", Bound: "..."}},
	}
	child := testTable("parts", "events_p01", testCol("at", "timestamptz", false))
	snap.Tables = append(snap.Tables, parent, child)
	srv := NewOfflineServerAnnotated(annotateRolled(snap, 100), lint.DefaultConfig())

	// unfiltered, the child is noise beside its parent: fold it, and say so
	all := find(t, srv, map[string]any{})
	for _, m := range all.Objects {
		if m.Name == "events_p01" {
			t.Errorf("child listed beside its parent: %v", objectsOf(all))
		}
	}
	if !strings.Contains(all.Meta.Hint, "1 partition children") {
		t.Errorf("fold not reported: %q", all.Meta.Hint)
	}

	// the child's schema, with no parent in sight: it is the answer
	inParts := find(t, srv, map[string]any{"schema": "parts"})
	if len(inParts.Objects) != 1 || inParts.Objects[0].Object != "parts.events_p01" {
		t.Fatalf("a schema of partitions inventoried as %v", objectsOf(inParts))
	}
	if strings.Contains(inParts.Meta.Hint, "folded") {
		t.Errorf("nothing was folded, but the hint says so: %q", inParts.Meta.Hint)
	}
}

// A fold the caller cannot see is a count they cannot trust, including on the
// paths that return no rows at all.
func TestFindObjects_FoldNoteSurvivesEmptyPages(t *testing.T) {
	snap := wideSnap(3, 40)
	srv := NewOfflineServerAnnotated(annotateRolled(snap, 1000), lint.DefaultConfig())

	past := find(t, srv, map[string]any{"offset": float64(999)})
	if !strings.Contains(past.Meta.Hint, "folded") {
		t.Errorf("past-the-end page dropped the fold note: %q", past.Meta.Hint)
	}

	// an enum inventory folded no tables, so it must not claim to have
	enums := find(t, srv, map[string]any{"kind": "enum"})
	if enums.Count != 0 {
		t.Fatalf("fixture has enums: %v", objectsOf(enums))
	}
	if strings.Contains(enums.Meta.Hint, "folded") {
		t.Errorf("an enum listing reported a partition fold: %q", enums.Meta.Hint)
	}
}

// strings.ToLower keeps the rune count but not the byte length: Ⱥ is two bytes
// and lowers to three. A byte offset taken in the lowered copy therefore points
// somewhere else in the original, and past its end once the growth outruns the
// tail.
func TestFindObjects_ExcerptSurvivesCaseFoldingByteGrowth(t *testing.T) {
	for _, tc := range []struct{ name, def string }{
		{"match past the end of the original", "SELECT '" + strings.Repeat("Ⱥ", 300) + "needle_here"},
		{"match in the middle", "SELECT '" + strings.Repeat("Ⱥ", 300) + "needle_here" + strings.Repeat("x", 300)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := findSnap()
			snap.Views[0].Definition = tc.def
			srv := NewOfflineServerAnnotated(annotate(snap, 10), lint.DefaultConfig())

			res := find(t, srv, map[string]any{"query": "needle_here", "kind": "view"})
			if len(res.Objects) != 1 {
				t.Fatalf("got %v", objectsOf(res))
			}
			if !strings.Contains(res.Objects[0].Detail, "needle_here") {
				t.Errorf("window missed the match: %q", res.Objects[0].Detail)
			}
		})
	}
}
