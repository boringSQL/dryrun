package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

// Three paged handlers slice with the offset this returns, so the clamp is a
// safety property, not a preference: entries[-1:total] panics. Nothing declares
// a minimum on the argument, so this is the only thing enforcing it.
func TestGetFloatArgClampsNonPositive(t *testing.T) {
	for _, tc := range []struct {
		name string
		args map[string]any
		want float64
	}{
		{"absent", nil, 7},
		{"positive passes through", map[string]any{"n": float64(3)}, 3},
		{"negative falls back", map[string]any{"n": float64(-1)}, 7},
		{"zero falls back", map[string]any{"n": float64(0)}, 7},
		{"wrong type falls back", map[string]any{"n": "three"}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req mcp.CallToolRequest
			req.Params.Arguments = tc.args
			if got := getFloatArg(req, "n", 7); got != tc.want {
				t.Errorf("getFloatArg = %v, want %v", got, tc.want)
			}
		})
	}
}

// End to end, because the clamp lives one layer away from the slice it
// protects: a negative offset has to read as "the first page", not as an error
// and not as a panic recovered into one.
func TestListTopQueries_NegativeOffsetIsTheFirstPage(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)

	track := "all"
	if _, err := hist.PutQueryStats(context.Background(), testKey, &schema.QueryStatsSnapshot{
		SchemaRefHash: snap.ContentHash,
		ContentHash:   "qs-1",
		PgssTrack:     &track,
		RawRows:       2,
		Node:          schema.NodeIdentity{Source: "primary", Timestamp: time.Now().UTC()},
		Queries: []schema.QueryStatsEntry{
			{
				Canonical: "SELECT 1", Calls: 10, TotalExecTimeMs: 100,
				Members: []schema.QueryStatsMember{{QueryID: 1, Calls: 10, TotalExecTimeMs: 100}},
			},
			{
				Canonical: "SELECT 2", Calls: 5, TotalExecTimeMs: 50,
				Members: []schema.QueryStatsMember{{QueryID: 2, Calls: 5, TotalExecTimeMs: 50}},
			},
		},
	}); err != nil {
		t.Fatalf("PutQueryStats: %v", err)
	}

	srv := serverWithHistory(t, snap, hist)

	var req mcp.CallToolRequest
	req.Params.Name = "list_top_queries"
	req.Params.Arguments = map[string]any{"offset": float64(-1)}

	res, err := srv.handleListTopQueries(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTopQueries: %v", err)
	}
	out, ok := res.StructuredContent.(listTopQueriesResult)
	if !ok {
		t.Fatalf("want listTopQueriesResult, got %T (%v)", res.StructuredContent, res.Content)
	}
	if out.Offset != 0 {
		t.Errorf("offset = %d, want 0", out.Offset)
	}
	if len(out.Queries) != 2 {
		t.Errorf("want the first page, got %d queries", len(out.Queries))
	}
}
