package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/internal/snapdiff"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/lint"
)

func reqWith(args map[string]any) mcp.CallToolRequest {
	var req mcp.CallToolRequest
	req.Params.Name = "snapshot_diff"
	req.Params.Arguments = args
	return req
}

// limitArg is what makes the "re-run with limit=0" contract real for every
// capped tool (detect, vacuum_health, snapshot_diff). Historically only
// snapshot_diff honored it (via a local capArg); the shared helper leaned on
// getFloatArg, which treats any value <= 0 as "unset" and substitutes the
// fallback — so an agent that followed the hint (or replayed the limit:0 call
// _meta.next emits) silently got the default cap back and could loop on the
// same truncated result forever. These cases nail the contract corner by
// corner — an explicit 0 is honored as "no cap", a real number passes straight
// through, and the genuinely-unset paths (absent key, nonsense negative) fall
// back to the sane default. If someone ever "simplifies" limitArg back to
// getFloatArg, the explicit-zero case here is the tripwire.
func TestLimitArg(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
		want int
	}{
		{"explicit zero means all", map[string]any{"limit": float64(0)}, 0},
		{"explicit value passes through", map[string]any{"limit": float64(5)}, 5},
		{"absent falls back to default", nil, defaultMaxItems},
		{"negative falls back to default", map[string]any{"limit": float64(-1)}, defaultMaxItems},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := limitArg(reqWith(tc.args)); got != tc.want {
				t.Fatalf("limitArg = %d, want %d", got, tc.want)
			}
		})
	}
}

// nextPage is replayed verbatim by an agent, so it must reproduce the same diff:
// endpoints pinned by hash (latest moves between pages), the same kind, node,
// filters and window, with only offset advanced. Defaults are elided to keep it short.
func TestNextPage(t *testing.T) {
	res := &snapdiff.Result{FromHash: "aaa", ToHash: "bbb"}
	payload := &snapdiff.Result{Offset: 20, LimitApplied: 10, Truncated: true}

	full := nextPage(res, snapdiff.Options{
		From: "latest~2", To: "latest", Kind: "planner", Node: "replica",
		Schema: "public", Table: "orders", Window: time.Hour,
	}, "full", 0, payload)
	for k, want := range map[string]any{
		"from": "aaa", "to": "bbb", "limit": 0, "offset": 30, "kind": "planner", "node": "replica",
		"schema": "public", "table": "orders", "view": "full", "window_minutes": 60.0,
	} {
		if full[k] != want {
			t.Errorf("arg %q = %v, want %v", k, full[k], want)
		}
	}

	min := nextPage(res, snapdiff.Options{Kind: "schema", Window: snapdiff.DefaultWindow}, "summary", 50, payload)
	for _, k := range []string{"kind", "view", "window_minutes", "schema", "table", "node"} {
		if _, ok := min[k]; ok {
			t.Errorf("default arg %q should be omitted, got %v", k, min[k])
		}
	}
	if min["limit"] != 50 || min["offset"] != 30 {
		t.Errorf("limit/offset = %v/%v, want 50/30", min["limit"], min["offset"])
	}
}

func schemaWithTables(hash string, ts time.Time, names ...string) *schema.SchemaSnapshot {
	var tbls []schema.Table
	for _, n := range names {
		tbls = append(tbls, schema.Table{
			Schema: "public", Name: n,
			Columns: []schema.Column{{Name: "id", TypeName: "int"}},
		})
	}
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "appdb",
		Timestamp: ts, ContentHash: hash, Tables: tbls,
	}
}

// TestSnapshotDiff_TruncationEmitsNextPage drives the whole path: seed a store
// with a three-object diff, call the handler with limit=1, and parse the JSON
// body the client receives. It must admit the cut (truncated=true) and carry a
// snapshot_diff call in _meta.next that pins the compared pair by hash and
// advances the offset. A capture landing before the replay must not change the
// pair, which is the reason for pinning at all.
func TestSnapshotDiff_TruncationEmitsNextPage(t *testing.T) {
	ctx := context.Background()
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	// three tables dropped between the two captures -> three objects
	if _, err := store.Put(ctx, key, history.WrapSchema(schemaWithTables("a1a1", t0, "t1", "t2", "t3"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key, history.WrapSchema(schemaWithTables("b2b2", t0.Add(time.Hour)))); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(nil, "", schemaWithTables("b2b2", t0.Add(time.Hour)), store, lint.DefaultConfig(), "")
	srv.SetSnapshotKey(key)

	res, err := srv.handleSnapshotDiff(ctx, reqWith(map[string]any{"limit": float64(1)}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
		t.Fatalf("result is not JSON: %v\n%s", err, text.Text)
	}
	if out["truncated"] != true {
		t.Errorf("expected truncated=true, got %v", out["truncated"])
	}

	meta, _ := out["_meta"].(map[string]any)
	next, _ := meta["next"].([]any)
	var args map[string]any
	for _, n := range next {
		nc, _ := n.(map[string]any)
		if nc["tool"] == "snapshot_diff" {
			args, _ = nc["args"].(map[string]any)
		}
	}
	if args == nil {
		t.Fatalf("expected a snapshot_diff next page in _meta.next, got %v", next)
	}
	if args["from"] != out["from_hash"] || args["to"] != out["to_hash"] {
		t.Errorf("next page must pin the compared pair by hash: %v vs %v/%v", args, out["from_hash"], out["to_hash"])
	}
	if args["offset"] != float64(1) || args["limit"] != float64(1) {
		t.Errorf("next page should advance offset by the page size: %v", args)
	}
	if hint, _ := meta["hint"].(string); strings.Contains(hint, "limit=0") {
		t.Errorf("hint must not send the agent to limit=0: %q", hint)
	}

	// a newer capture lands between pages
	if _, err := store.Put(ctx, key, history.WrapSchema(schemaWithTables("c3c3", t0.Add(90*time.Minute), "t9"))); err != nil {
		t.Fatal(err)
	}

	// replaying it yields the next object, from the same pair
	res2, err := srv.handleSnapshotDiff(ctx, reqWith(args))
	if err != nil {
		t.Fatal(err)
	}
	var out2 struct {
		Objects  []struct{ Name string } `json:"objects"`
		FromHash string                  `json:"from_hash"`
		ToHash   string                  `json:"to_hash"`
	}
	if err := json.Unmarshal([]byte(res2.Content[0].(mcp.TextContent).Text), &out2); err != nil {
		t.Fatal(err)
	}
	var first []struct{ Name string }
	b, _ := json.Marshal(out["objects"])
	_ = json.Unmarshal(b, &first)
	if len(out2.Objects) != 1 || len(first) != 1 || out2.Objects[0].Name == first[0].Name ||
		out2.FromHash != out["from_hash"] || out2.ToHash != out["to_hash"] {
		t.Errorf("replayed page: %+v after %+v", out2, first)
	}
}

func TestSnapshotDiff_OffsetPages(t *testing.T) {
	ctx := context.Background()
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	if _, err := store.Put(ctx, key, history.WrapSchema(schemaWithTables("s-a", t0, "t1", "t2", "t3"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key, history.WrapSchema(schemaWithTables("s-b", t0.Add(time.Hour)))); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(nil, "", schemaWithTables("s-b", t0.Add(time.Hour)), store, lint.DefaultConfig(), "")
	srv.SetSnapshotKey(key)

	page := func(offset float64) (names []string, truncated bool) {
		res, err := srv.handleSnapshotDiff(ctx, reqWith(map[string]any{"limit": float64(1), "offset": offset}))
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Objects   []struct{ Name string } `json:"objects"`
			Truncated bool                    `json:"truncated"`
		}
		if err := json.Unmarshal([]byte(res.Content[0].(mcp.TextContent).Text), &out); err != nil {
			t.Fatal(err)
		}
		for _, o := range out.Objects {
			names = append(names, o.Name)
		}
		return names, out.Truncated
	}

	seen := map[string]bool{}
	for off := float64(0); off < 3; off++ {
		names, truncated := page(off)
		if len(names) != 1 || seen[names[0]] {
			t.Fatalf("offset %v: got %v, want one object not seen before", off, names)
		}
		seen[names[0]] = true
		if truncated != (off < 2) {
			t.Errorf("offset %v: truncated=%v", off, truncated)
		}
	}
}

func bigQueryResult(n int) *snapdiff.Result {
	entries := make([]diff.QueryEntryDelta, n)
	for i := range entries {
		entries[i] = diff.QueryEntryDelta{
			Fingerprint: fmt.Sprintf("sha1:%06d", i), Status: diff.QueryGrew,
			Canonical: strings.Repeat("SELECT 1 ", 30), Calls: int64(i), CallsDelta: int64(i),
		}
	}
	return &snapdiff.Result{
		Summary:    snapdiff.Summary{Headline: "x"},
		QueryDelta: []snapdiff.NodeQueryDelta{{Node: "primary", Delta: &diff.QueryDelta{Entries: entries}}},
	}
}

func TestFitSnapshotDiff(t *testing.T) {
	const budget = 32 << 10
	res := bigQueryResult(2000)

	for _, limit := range []int{0, 1500} {
		p, err := fitSnapshotDiff(res, "full", limit, 0, budget)
		if err != nil {
			t.Fatalf("limit=%d: %v", limit, err)
		}
		b, _ := json.MarshalIndent(p, "", "  ")
		if len(b) > budget {
			t.Errorf("limit=%d: %d bytes, over the %d budget", limit, len(b), budget)
		}
		got := len(p.QueryDelta[0].Delta.Entries)
		if !p.Truncated || p.TruncatedBy != "bytes" || p.LimitApplied != got || got == 0 || got >= 1500 {
			t.Errorf("limit=%d: truncated=%v by=%q applied=%d entries=%d", limit, p.Truncated, p.TruncatedBy, p.LimitApplied, got)
		}
	}

	// a limit that already fits the budget is the caller's cut, not the budget's
	p, err := fitSnapshotDiff(res, "full", 5, 0, budget)
	if err != nil || p.TruncatedBy != "limit" || p.LimitApplied != 5 {
		t.Errorf("small limit: err=%v by=%q applied=%d", err, p.TruncatedBy, p.LimitApplied)
	}

	// everything fits: nothing to report
	p, err = fitSnapshotDiff(bigQueryResult(3), "full", 0, 0, budget)
	if err != nil || p.Truncated || p.TruncatedBy != "" || p.LimitApplied != 0 {
		t.Errorf("fitting result: err=%v truncated=%v by=%q", err, p.Truncated, p.TruncatedBy)
	}
}

func TestFitSnapshotDiff_PagesCoverEveryEntryOnce(t *testing.T) {
	res := bigQueryResult(400)
	seen := map[string]bool{}
	for off := 0; off < 1000; {
		p, err := fitSnapshotDiff(res, "full", 0, off, 16<<10)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range p.QueryDelta[0].Delta.Entries {
			if seen[e.Fingerprint] {
				t.Fatalf("%s repeated at offset %d", e.Fingerprint, off)
			}
			seen[e.Fingerprint] = true
		}
		if !p.Truncated {
			break
		}
		off += p.LimitApplied
	}
	if len(seen) != 400 {
		t.Errorf("paged through %d of 400 entries", len(seen))
	}
}

func TestFitSnapshotDiff_RefusesWhenOneEntryDoesNotFit(t *testing.T) {
	empty, _ := json.MarshalIndent(bigQueryResult(0).ForPage("full", 0, 0), "", "  ")
	// room for the envelope but not for one entry
	if _, err := fitSnapshotDiff(bigQueryResult(10), "full", 0, 0, len(empty)+10); err == nil {
		t.Error("expected an error when even limit=1 is over budget")
	}
}

func TestFitSnapshotDiff_HugeLimitIsClamped(t *testing.T) {
	p, err := fitSnapshotDiff(bigQueryResult(2000), "full", 1<<50, 0, 32<<10)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p.QueryDelta[0].Delta.Entries); got < 10 {
		t.Errorf("an absurd limit left %d entries; the ratio step must not overflow", got)
	}
}

// a byte-cut page continues from where it stopped and never sends the agent to limit=0
func TestSnapshotDiffFollowups_ByteCut(t *testing.T) {
	res := bigQueryResult(3)
	res.FromHash, res.ToHash = "aaa", "bbb"
	payload := &snapdiff.Result{Truncated: true, TruncatedBy: "bytes", LimitApplied: 8, Offset: 16}

	hint, next := snapshotDiffFollowups(res, snapdiff.Options{Kind: "query"}, "full", 0, payload)
	var args map[string]any
	for _, n := range next {
		if n.Tool == "snapshot_diff" {
			args = n.Args
		}
	}
	if args == nil || args["offset"] != 24 || args["from"] != "aaa" || args["to"] != "bbb" {
		t.Errorf("next page = %v, want offset 24 pinned to aaa..bbb", args)
	}
	if _, ok := args["node"]; ok {
		t.Errorf("node was not given, so the pinned hashes alone must carry it: %v", args)
	}
	for _, want := range []string{"limit_applied=8", "offset=24", "node="} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint %q lacks %q", hint, want)
		}
	}
	if strings.Contains(hint, "limit=0") {
		t.Errorf("hint sends the agent to limit=0: %q", hint)
	}

	payload = &snapdiff.Result{Truncated: true, TruncatedBy: "limit", LimitApplied: 1}
	hint, _ = snapshotDiffFollowups(res, snapdiff.Options{Kind: "query"}, "full", 1, payload)
	if !strings.Contains(hint, "node=") || strings.Contains(hint, "schema=") {
		t.Errorf("limit-cut hint for kind=query should name node=, got %q", hint)
	}

	_, next = snapshotDiffFollowups(res, snapdiff.Options{Kind: "query"}, "full", 0, &snapdiff.Result{})
	for _, n := range next {
		if n.Tool == "snapshot_diff" {
			t.Errorf("a complete result offered a next page: %v", n.Args)
		}
	}
}
