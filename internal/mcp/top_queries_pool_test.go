package mcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// One label over two servers with different reset epochs, captured A B A B.
// The newest capture is B's; the second-newest is A's. Comparing B with A reads
// A's reset epoch as "counters were reset", which never happened on B. The tool
// must compare B with B's own earlier capture, and say the totals are B's.

type poolCapture struct {
	server  string
	qshape  int
	calls   int64
	atHours int
}

// idBase keeps member queryids distinct across labels, which the content digest hashes
func putPoolCaptures(t *testing.T, hist *history.Store, schemaRef, label string, idBase int64, t0 time.Time, caps []poolCapture) {
	t.Helper()
	boots := map[string]time.Time{"a": t0.Add(-72 * time.Hour), "b": t0.Add(-48 * time.Hour), "p": t0.Add(-96 * time.Hour)}
	resets := map[string]time.Time{"a": t0.Add(-70 * time.Hour), "b": t0.Add(-40 * time.Hour), "p": t0.Add(-90 * time.Hour)}
	for i, c := range caps {
		boot, reset := boots[c.server], resets[c.server]
		if _, err := hist.PutQueryStats(context.Background(), testKey, &schema.QueryStatsSnapshot{
			SchemaRefHash: schemaRef,
			ContentHash:   fmt.Sprintf("%s-%s%d", label, c.server, i),
			QshapeVersion: c.qshape, RowCap: 500, RawRows: 1,
			Node: schema.NodeIdentity{
				Source: label, Timestamp: t0.Add(time.Duration(c.atHours) * time.Hour), PostmasterStartTime: &boot,
			},
			InfoBefore: &schema.QueryStatsInfo{StatsReset: reset},
			InfoAfter:  &schema.QueryStatsInfo{StatsReset: reset},
			Queries: []schema.QueryStatsEntry{{
				Fingerprint: "fp-" + label, Canonical: "SELECT 1", Calls: c.calls, TotalExecTimeMs: float64(c.calls),
				Members: []schema.QueryStatsMember{{QueryID: idBase + int64(i), Calls: c.calls}},
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func topQueriesText(t *testing.T, srv *Server, args map[string]any) string {
	t.Helper()
	var req mcp.CallToolRequest
	req.Params.Name = "list_top_queries"
	req.Params.Arguments = args
	res, err := srv.handleListTopQueries(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTopQueries: %v", err)
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("want TextContent, got %T", res.Content[0])
	}
	return text.Text
}

func TestListTopQueriesOnARotatingLabel(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	putPoolCaptures(t, hist, snap.ContentHash, "pool", 100, t0, []poolCapture{
		{"a", 3, 10, 0}, {"b", 3, 20, 1}, {"a", 3, 30, 2}, {"b", 3, 40, 3},
	})

	text := topQueriesText(t, serverWithHistory(t, snap, hist), nil)
	if strings.Contains(text, "counters were reset") {
		t.Errorf("another server's reset epoch was reported as a reset:\n%s", text)
	}
	if !strings.Contains(text, "label pool rotates between servers") {
		t.Errorf("the totals were not attributed to one server:\n%s", text)
	}
}

// B's two captures straddle a qshape regrouping while A's do not: the caveat
// fires only if B is really compared with B. A node filter keeps another
// label's caveats out.
func TestListTopQueriesPairsWithinServerAndHonoursNodeFilter(t *testing.T) {
	hist := historyStore(t)
	snap := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	put(t, hist, snap)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	putPoolCaptures(t, hist, snap.ContentHash, "pool", 100, t0, []poolCapture{
		{"a", 4, 10, 0}, {"b", 3, 20, 1}, {"a", 4, 30, 2}, {"b", 4, 40, 3},
	})
	// a plain label whose own captures regroup too
	putPoolCaptures(t, hist, snap.ContentHash, "other", 200, t0, []poolCapture{
		{"p", 3, 10, 0}, {"p", 5, 20, 1},
	})
	srv := serverWithHistory(t, snap, hist)

	text := topQueriesText(t, srv, map[string]any{"node": "pool"})
	if !strings.Contains(text, "previous capture of pool: query shapes were regrouped between captures (qshape v3 -> v4)") {
		t.Errorf("B was not compared with its own earlier capture:\n%s", text)
	}
	if strings.Contains(text, "previous capture of other") {
		t.Errorf("the node filter let another label's caveats through:\n%s", text)
	}
}
