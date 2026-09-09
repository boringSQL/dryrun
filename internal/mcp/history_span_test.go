package mcp

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"

	_ "modernc.org/sqlite"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

func activityAt(at time.Time, label string) *schema.ActivityStatsSnapshot {
	return &schema.ActivityStatsSnapshot{
		SchemaRefHash: "three",
		ContentHash:   "act-" + label,
		Node: schema.NodeIdentity{
			Source: label, PgVersion: "PostgreSQL 17.0", Timestamp: at,
		},
		Tables: []schema.TableActivityEntry{{
			Table:    schema.QualifiedName{Schema: "public", Name: "three"},
			Activity: schema.TableActivity{SeqScan: 1},
		}},
	}
}

// The agent asking "what changed" has one snapshot in hand and no way to know
// whether anything stands behind it. These are the numbers that answer that
// without a shell-out to `dryrun snapshot list`.

// find_objects renders prose in its text content, so the typed envelope has to
// be read off StructuredContent. Going through the client is the point: the
// server validates the payload against the generated schema on the way out.
func structuredMetaOf(t *testing.T, c *client.Client, name string, args map[string]any) map[string]any {
	t.Helper()
	var req mcp.CallToolRequest
	req.Params.Name = name
	req.Params.Arguments = args
	res, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("CallTool(%s): %v", name, err)
	}
	obj, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("CallTool(%s): structured content is %T", name, res.StructuredContent)
	}
	meta, ok := obj["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("CallTool(%s): no _meta in %v", name, obj)
	}
	return meta
}

func spansOf(t *testing.T, meta map[string]any) map[string]map[string]any {
	t.Helper()
	raw, ok := meta["history"].([]any)
	if !ok {
		t.Fatalf("no _meta.history, got %v", meta["history"])
	}
	out := map[string]map[string]any{}
	for _, e := range raw {
		span, ok := e.(map[string]any)
		if !ok {
			t.Fatalf("history entry is %T, want an object", e)
		}
		name, ok := span["stream"].(string)
		if !ok {
			t.Fatalf("history entry has no stream name: %v", span)
		}
		out[name] = span
	}
	return out
}

func TestMetaCarriesTheHistorySpan(t *testing.T) {
	hist := historyStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for i, table := range []string{"one", "two", "three"} {
		put(t, hist, datedSnap(t, base.Add(time.Duration(i)*24*time.Hour), table))
	}
	a := activityAt(base.Add(48*time.Hour), "primary")
	if _, err := hist.PutActivity(ctx, testKey, a); err != nil {
		t.Fatal(err)
	}

	// a capture that stored nothing still moved this clock: it is what tells a
	// quiet stream from an abandoned one
	attempted := base.Add(72 * time.Hour)
	if err := hist.MarkCaptureAttempt(ctx, testKey, "", "schema", attempted); err != nil {
		t.Fatal(err)
	}

	srv := serverWithHistory(t, datedSnap(t, base.Add(48*time.Hour), "three"), hist)
	c := serveOffline(t, srv)

	spans := spansOf(t, metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "public.three"})))

	sc, ok := spans["schema"]
	if !ok {
		t.Fatalf("no schema stream in %v", spans)
	}
	if sc["rows"] != float64(3) {
		t.Errorf("schema rows: want 3, got %v", sc["rows"])
	}
	if sc["oldest"] != "2026-08-01T12:00:00Z" || sc["newest"] != "2026-08-03T12:00:00Z" {
		t.Errorf("schema span: got %v..%v", sc["oldest"], sc["newest"])
	}
	if sc["last_attempt"] != "2026-08-04T12:00:00Z" {
		t.Errorf("last_attempt: want the attempt clock, got %v", sc["last_attempt"])
	}
	// schema rows are not node-scoped; claiming a node count would invent one
	if _, ok := sc["nodes"]; ok {
		t.Errorf("schema stream claims %v nodes", sc["nodes"])
	}

	act, ok := spans["activity"]
	if !ok {
		t.Fatalf("no activity stream in %v", spans)
	}
	// nothing recorded an attempt on activity; an absent clock is not a zero one
	if _, ok := act["last_attempt"]; ok {
		t.Errorf("activity claims an attempt at %v", act["last_attempt"])
	}
	if act["rows"] != float64(1) || act["nodes"] != float64(1) {
		t.Errorf("activity: want 1 row on 1 node, got %v", act)
	}
	// nothing was captured on these two; a zero row would read as "measured none"
	for _, stream := range []string{"planner", "query"} {
		if _, ok := spans[stream]; ok {
			t.Errorf("%s has no rows but appears as %v", stream, spans[stream])
		}
	}
}

// find_objects answers through the typed toolMeta rather than injectMeta, so
// the field has to be on both paths — and it goes through the client, because
// the generated output schema sets additionalProperties:false and a new nested
// struct is exactly what output validation exists to catch.
func TestTypedMetaCarriesTheHistorySpan(t *testing.T) {
	hist := historyStore(t)
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	c := serveOffline(t, serverWithHistory(t, datedSnap(t, at, "one"), hist))

	spans := spansOf(t, structuredMetaOf(t, c, "find_objects", nil))
	sc, ok := spans["schema"]
	if !ok {
		t.Fatalf("no schema stream in %v", spans)
	}
	if sc["rows"] != float64(1) || sc["newest"] != "2026-08-01T12:00:00Z" {
		t.Errorf("got %v", sc)
	}
}

// A store that cannot be read is not a deployment without history: absence
// already means the second thing, so the first needs its own word or the agent
// concludes there is nothing to ask and goes back to the shell.
func TestAFailedInventoryReadIsNotSilence(t *testing.T) {
	hist := historyStore(t)
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	srv := serverWithHistory(t, datedSnap(t, at, "one"), hist)
	c := serveOffline(t, srv)

	hist.Close()
	srv.inventory.invalidate()

	meta := metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "public.one"}))
	if _, ok := meta["history"]; ok {
		t.Errorf("the read failed, but meta reports %v", meta["history"])
	}
	if meta["history_unavailable"] != true {
		t.Errorf("want history_unavailable, got %v", meta["history_unavailable"])
	}
}

// Retention leaves the attempt clock behind, so a stream can reach the wire
// with no rows. It must read as "captured, nothing kept" — the zero belongs to
// rows and nodes, and the date to the clock.
func TestAPrunedStreamRendersAsCapturedNotAbsent(t *testing.T) {
	hist := historyStore(t)
	ctx := context.Background()
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	if _, err := hist.PutActivity(ctx, testKey, activityAt(at, "primary")); err != nil {
		t.Fatal(err)
	}
	attempted := at.Add(48 * time.Hour)
	if err := hist.MarkCaptureAttempt(ctx, testKey, "primary", "activity", attempted); err != nil {
		t.Fatal(err)
	}
	if _, err := hist.DeleteBefore(ctx, testKey, history.ActivityKind(""), at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	c := serveOffline(t, serverWithHistory(t, datedSnap(t, at, "one"), hist))
	act, ok := spansOf(t, metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "public.one"})))["activity"]
	if !ok {
		t.Fatal("a pruned stream that was captured must not vanish from the wire")
	}
	if act["rows"] != float64(0) {
		t.Errorf("rows: want an explicit 0, got %v", act["rows"])
	}
	if act["nodes"] != float64(0) {
		t.Errorf("nodes: a node-scoped stream with nothing left says 0, got %v", act["nodes"])
	}
	if act["last_attempt"] != "2026-08-03T12:00:00Z" {
		t.Errorf("last_attempt: want the clock that kept this entry, got %v", act["last_attempt"])
	}
	if _, ok := act["newest"]; ok {
		t.Errorf("no rows left, so no span: got %v", act["newest"])
	}
	if _, ok := act["oldest"]; ok {
		t.Errorf("no rows left, so no span: got %v", act["oldest"])
	}
}

// A history.db keyed to another database must not have its counts quoted
// beside this one's answer — the guard adoptNewerSnapshot applies before it
// will serve such a bundle.
func TestNoInventoryFromAnotherDatabasesHistory(t *testing.T) {
	hist := historyStore(t)
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	stored := datedSnap(t, at, "one")
	stored.Database = "otherdb"
	put(t, hist, stored)

	served := datedSnap(t, at, "one")
	served.Database = "appdb"
	c := serveOffline(t, serverWithHistory(t, served, hist))

	meta := metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "public.one"}))
	if _, ok := meta["history"]; ok {
		t.Errorf("counts from another database reached the wire: %v", meta["history"])
	}
	// the store answered; nothing failed
	if _, ok := meta["history_unavailable"]; ok {
		t.Error("a foreign history is not an unreadable one")
	}
}

// A hosted server holds no local store. "No history" and "an empty history"
// are different answers, and only one of them may be printed as a number.
func TestMetaOmitsTheHistorySpanWithNoStore(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))
	meta := metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "app.orders"}))
	if _, ok := meta["history"]; ok {
		t.Errorf("no store, but meta claims %v", meta["history"])
	}
}

// The count rides every tool call, so it is read once per TTL and can be that
// stale. Worth pinning: it is the reason a capture taken mid-session does not
// show up immediately.
func TestTheHistorySpanIsCachedForTheTTL(t *testing.T) {
	hist := historyStore(t)
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	srv := serverWithHistory(t, datedSnap(t, at, "one"), hist)

	got, ok := srv.historySpans()
	if !ok || len(got) != 1 || got[0].Rows != 1 {
		t.Fatalf("first read: ok=%t %+v", ok, got)
	}
	put(t, hist, datedSnap(t, at.Add(time.Hour), "two"))
	if got, _ = srv.historySpans(); len(got) != 1 || got[0].Rows != 1 {
		t.Errorf("within the TTL: want the cached 1, got %+v", got)
	}

	srv.inventory.mu.Lock()
	srv.inventory.readAt = time.Now().Add(-historySpanTTL - time.Second)
	srv.inventory.mu.Unlock()
	if got, _ = srv.historySpans(); len(got) != 1 || got[0].Rows != 2 {
		t.Errorf("after the TTL: want 2, got %+v", got)
	}
}

// The inventory read once called getSchema while holding its own mutex, and
// getSchema runs the freshness check, which invalidates that cache on an adopt
// — a cycle, and a re-entry of a non-reentrant mutex on one goroutine. The
// concurrent shape is the one that hangs forever, so this runs both.
func TestTheInventoryCacheTakesNoLockWhileHoldingItsOwn(t *testing.T) {
	hist := historyStore(t)
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	srv := serverWithHistory(t, datedSnap(t, at, "one"), hist)

	// a newer snapshot waiting to be adopted, so the freshness path does real
	// work on the next read rather than returning from its throttle
	put(t, hist, datedSnap(t, at.Add(time.Hour), "two"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 20 {
					srv.inventory.invalidate()
					if _, ok := srv.historySpans(); !ok {
						t.Error("inventory read failed")
					}
					srv.adoptNewerSnapshot(context.Background())
				}
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlocked: the inventory cache is taking another lock while holding its own")
	}
}

// A store this build cannot read is present and refusing, which is what
// history_unavailable exists to say. Plain absence would report the stronger
// claim — that the deployment has no local history — on the one server that
// does have it, and the selection note is telling the agent to look there.
func TestACompatNewerStoreReportsUnavailableRatherThanAbsent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.db")
	hist, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	srv := serverWithHistory(t, datedSnap(t, at, "one"), hist)
	if _, ok := srv.historySpans(); !ok {
		t.Fatal("the fixture store is readable; this test would prove nothing")
	}
	hist.Close()

	// what a newer dryrun leaves behind
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 9999"); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	newer, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { newer.Close() })
	if newer.Compat() != history.CompatNewer {
		t.Fatalf("want CompatNewer, got %v", newer.Compat())
	}
	srv.SetHistory(newer)
	srv.inventory.invalidate()

	spans, ok := srv.historySpans()
	if len(spans) != 0 {
		t.Errorf("an unreadable store must report no counts, got %+v", spans)
	}
	if ok {
		t.Error("want history_unavailable; plain absence says there is no local history at all")
	}
}

// _meta is built two ways -- by hand for the map payloads, from toolMeta for
// the typed ones -- so the unreadable-store answer is checked on both wires
// rather than only through historySpans.
func TestUnavailableReachesTheWire(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.db")
	hist, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	put(t, hist, datedSnap(t, at, "one"))
	srv := serverWithHistory(t, datedSnap(t, at, "one"), hist)
	c := serveOffline(t, srv)
	hist.Close()

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 9999"); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	newer, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { newer.Close() })
	srv.SetHistory(newer)
	srv.inventory.invalidate()

	for name, meta := range map[string]map[string]any{
		// injectMeta
		"describe_table": metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "public.one"})),
		// newMeta, through the generated output schema
		"find_objects": structuredMetaOf(t, c, "find_objects", nil),
	} {
		if meta["history_unavailable"] != true {
			t.Errorf("%s: want history_unavailable on the wire, got %v", name, meta)
		}
		if _, ok := meta["history"]; ok {
			t.Errorf("%s: an unreadable store must report no counts, got %v", name, meta["history"])
		}
	}
}
