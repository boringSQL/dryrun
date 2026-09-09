package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// Three paths through list_top_queries end in an empty array, and only one of
// them is an absence of evidence. An agent that cannot tell them apart answers
// "what does this database spend its time on" with "nothing" — which is why the
// project's own trust-signal rule is an explicit unknown with a reason rather
// than an empty array.

func queryStatsSnap(t *testing.T, hist *history.Store, schemaHash string, calls ...int64) {
	t.Helper()
	track := "all"
	entries := make([]schema.QueryStatsEntry, len(calls))
	for i, c := range calls {
		entries[i] = schema.QueryStatsEntry{
			Canonical: "SELECT " + strings.Repeat("x", i+1), Calls: c, TotalExecTimeMs: float64(c) * 10,
			Members: []schema.QueryStatsMember{{QueryID: int64(i + 1), Calls: c, TotalExecTimeMs: float64(c) * 10}},
		}
	}
	// ContentHash left empty: PutQueryStats derives it from the members, and a
	// hardcoded one makes a second call a silent INSERT OR IGNORE, so a test
	// asserting on two captures would be asserting against one
	out, err := hist.PutQueryStats(context.Background(), testKey, &schema.QueryStatsSnapshot{
		SchemaRefHash: schemaHash,
		PgssTrack:     &track,
		RawRows:       len(entries),
		Node:          schema.NodeIdentity{Source: "primary", Timestamp: time.Now().UTC()},
		Queries:       entries,
	})
	if err != nil {
		t.Fatalf("PutQueryStats: %v", err)
	}
	if out == history.PutDeduped {
		t.Fatal("the fixture deduped; this store holds fewer captures than the test believes")
	}
}

func topQueries(t *testing.T, srv *Server, args map[string]any) listTopQueriesResult {
	t.Helper()
	var req mcp.CallToolRequest
	req.Params.Name = "list_top_queries"
	req.Params.Arguments = args
	res, err := srv.handleListTopQueries(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTopQueries: %v", err)
	}
	out, ok := res.StructuredContent.(listTopQueriesResult)
	if !ok {
		t.Fatalf("want listTopQueriesResult, got %T (%v)", res.StructuredContent, res.Content)
	}
	return out
}

// Nobody looked: the corpus was never observed.
func TestListTopQueriesSaysUnknownWhenNothingWasCaptured(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)

	got := topQueries(t, serverWithHistory(t, snap, hist), nil)
	if !got.Unknown {
		t.Error("no query capture exists, but the result does not say unknown")
	}
	if got.Reason == "" {
		t.Error("unknown without a reason is the empty array again, one field over")
	}
	if got.Count != 0 || len(got.Queries) != 0 {
		t.Errorf("want no queries, got count=%d len=%d", got.Count, len(got.Queries))
	}
}

// Measured, and nothing cleared the floor. That is an answer, so it must NOT
// wear the word reserved for having no evidence.
func TestListTopQueriesIsNotUnknownWhenTheFloorFilteredEverything(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)
	queryStatsSnap(t, hist, snap.ContentHash, 1, 1)

	got := topQueries(t, serverWithHistory(t, snap, hist), map[string]any{"min_calls": float64(1000)})
	if got.Unknown {
		t.Error("captures exist and were read; an empty page is a measurement, not an absence of one")
	}
	if got.Reason != "" {
		t.Errorf("a measured result needs no reason, got %q", got.Reason)
	}
}

// Paging past the end is neither: Count carries what was measured.
func TestListTopQueriesIsNotUnknownPastTheLastPage(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)
	queryStatsSnap(t, hist, snap.ContentHash, 10, 5)

	got := topQueries(t, serverWithHistory(t, snap, hist), map[string]any{"offset": float64(99)})
	if got.Unknown {
		t.Error("an offset past the end is a paging answer, not an unmeasured one")
	}
	if got.Count != 2 {
		t.Errorf("the pre-paging total still stands: want 2, got %d", got.Count)
	}
}

// A capture that holds no statements at all is measured — so not unknown — but
// blaming the call floor for it names a cause that filtered nothing.
func TestListTopQueriesDoesNotBlameTheFloorForAnEmptyCapture(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)
	queryStatsSnap(t, hist, snap.ContentHash)

	var req mcp.CallToolRequest
	req.Params.Name = "list_top_queries"
	res, err := serverWithHistory(t, snap, hist).handleListTopQueries(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTopQueries: %v", err)
	}
	out, ok := res.StructuredContent.(listTopQueriesResult)
	if !ok {
		t.Fatalf("want listTopQueriesResult, got %T", res.StructuredContent)
	}
	if out.Unknown {
		t.Error("a capture was read; that is a measurement, not an absence of one")
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("want TextContent, got %T", res.Content[0])
	}
	if strings.Contains(text.Text, "calls.") {
		t.Errorf("the floor filtered nothing here, so it cannot be the reason:\n%s", text.Text)
	}
	if !strings.Contains(text.Text, "holds no statements") {
		t.Errorf("want the capture named as the reason, got:\n%s", text.Text)
	}
}

// The description promises the word; a rename that drops it leaves the tool
// telling an agent to look for something it never sends.
func TestTheToolDescriptionMatchesWhatTheEmptyResultCarries(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)
	srv := serverWithHistory(t, snap, hist)
	c := serveOffline(t, srv)

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var found string
	for _, tool := range list.Tools {
		if tool.Name == "list_top_queries" {
			found = tool.Description
		}
	}
	if found == "" {
		t.Fatal("list_top_queries is not registered")
	}
	if !strings.Contains(found, "unknown: true") {
		t.Errorf("the description does not name the field the empty result carries:\n%s", found)
	}
	if !strings.Contains(found, "_meta.history") {
		t.Errorf("the description does not point at where the horizon is stated:\n%s", found)
	}

	// serveOffline is the only place output-schema validation runs, and the
	// generated schema sets additionalProperties:false, so the two new keys are
	// proven servable by calling the tool through it rather than by compiling
	var req mcp.CallToolRequest
	req.Params.Name = "list_top_queries"
	res, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	obj, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content is %T", res.StructuredContent)
	}
	if obj["unknown"] != true {
		t.Errorf("unknown did not survive to the wire: %v", obj)
	}
	if r, _ := obj["reason"].(string); r == "" {
		t.Errorf("reason did not survive to the wire: %v", obj)
	}
}

// Step 3 is "every local history tool states its horizon", and snapshot_diff is
// the other one: without a test its sentence can be dropped with everything
// still green. The `window` it names is the interval a delta spans, which is
// what an agent needs to turn one into a rate — and it is a different field
// from window_minutes, the correlation tolerance.
func TestSnapshotDiffStatesThatItIsNotARate(t *testing.T) {
	hist := historyStore(t)
	c := serveOffline(t, serverWithHistory(t, datedSnap(t, time.Now().UTC(), "t"), hist))

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var found string
	for _, tool := range list.Tools {
		if tool.Name == "snapshot_diff" {
			found = tool.Description
		}
	}
	if found == "" {
		t.Fatal("snapshot_diff is not registered")
	}
	// "window" alone is a substring of window_minutes and would assert nothing
	for _, want := range []string{"not a rate", "as `window` in nanoseconds", "window_minutes", "not_subtractable", "_meta.history"} {
		if !strings.Contains(found, want) {
			t.Errorf("the description does not say %q:\n%s", want, found)
		}
	}
}
