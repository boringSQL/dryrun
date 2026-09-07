package history

import (
	"context"
	"testing"
	"time"
)

// Inventory is what a tool response quotes to say how much history stands
// behind it, so its failures are the quiet ones: a stream reported as zero when
// it was never captured, another key's rows folded in, a span invented from a
// timestamp nobody can parse, or a count that means something different on each
// of the four streams.

func ptr(n int) *int { return &n }

func inventoryOf(t *testing.T, s *Store, k SnapshotKey) map[string]StreamSpan {
	t.Helper()
	got, err := s.Inventory(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]StreamSpan{}
	for _, span := range got {
		out[span.Stream] = span
	}
	return out
}

func TestInventoryOmitsStreamsWithNoRows(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	got, err := store.Inventory(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty store: want no streams, got %+v", got)
	}

	if _, err := store.PutSchema(ctx, k, testSnapshot("h1", "acme")); err != nil {
		t.Fatal(err)
	}
	got, err = store.Inventory(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Stream != "schema" || got[0].Rows != 1 {
		t.Fatalf("one schema capture: want exactly that stream, got %+v", got)
	}
	if got[0].Nodes != nil {
		t.Errorf("schema rows are not node-scoped: want no node count, got %d", *got[0].Nodes)
	}
}

func TestInventoryCountsRowsSpansAndNodes(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	for i, h := range []string{"h1", "h2", "h3"} {
		s := testSnapshot(h, "acme")
		s.Timestamp = base.Add(time.Duration(i) * 24 * time.Hour)
		if _, err := store.PutSchema(ctx, k, s); err != nil {
			t.Fatal(err)
		}
	}
	for i, h := range []string{"p1", "p2"} {
		p := plannerFixture("h3", h, "acme")
		p.Timestamp = base.Add(time.Duration(i) * time.Hour)
		if _, err := store.PutPlanner(ctx, k, p); err != nil {
			t.Fatal(err)
		}
	}
	putNodeActivityAt(t, store, k, "a1", "primary", false, base)
	putNodeActivityAt(t, store, k, "a2", "primary", false, base.Add(time.Hour))
	putNodeActivityAt(t, store, k, "a3", "replica", true, base.Add(2*time.Hour))
	putNodeQueryStatsAt(t, store, k, "q1", "primary", base.Add(3*time.Hour))

	got, err := store.Inventory(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	// capture order, so the list reads the way the pipeline runs
	want := []string{"schema", "planner", "activity", "query"}
	if len(got) != len(want) {
		t.Fatalf("streams: want %v, got %+v", want, got)
	}
	for i := range want {
		if got[i].Stream != want[i] {
			t.Fatalf("stream order: want %v, got %+v", want, got)
		}
	}

	byStream := inventoryOf(t, store, k)
	for _, tc := range []struct {
		stream         string
		rows           int
		nodes          *int
		oldest, newest time.Time
	}{
		{"schema", 3, nil, base, base.Add(48 * time.Hour)},
		{"planner", 2, nil, base, base.Add(time.Hour)},
		{"activity", 3, ptr(2), base, base.Add(2 * time.Hour)},
		{"query", 1, ptr(1), base.Add(3 * time.Hour), base.Add(3 * time.Hour)},
	} {
		span := byStream[tc.stream]
		if span.Rows != tc.rows {
			t.Errorf("%s rows: want %d, got %d", tc.stream, tc.rows, span.Rows)
		}
		switch {
		case tc.nodes == nil && span.Nodes != nil:
			t.Errorf("%s: not node-scoped, but claims %d nodes", tc.stream, *span.Nodes)
		case tc.nodes != nil && (span.Nodes == nil || *span.Nodes != *tc.nodes):
			t.Errorf("%s nodes: want %d, got %v", tc.stream, *tc.nodes, span.Nodes)
		}
		if span.CorruptRows != 0 {
			t.Errorf("%s: nothing is corrupt here, got %d", tc.stream, span.CorruptRows)
		}
		if span.Oldest == nil || span.Newest == nil {
			t.Fatalf("%s: want a span, got %+v", tc.stream, span)
		}
		if !span.Oldest.Equal(tc.oldest) || !span.Newest.Equal(tc.newest) {
			t.Errorf("%s span: want %s..%s, got %s..%s",
				tc.stream, tc.oldest, tc.newest, *span.Oldest, *span.Newest)
		}
	}
}

// The field is Rows, not Captures, because three of the four streams dedup on
// content: an hourly capture of a database nobody migrated stores one schema
// row. A reader taking the count for capture cadence, or Newest for "when we
// last looked", is the misreading this naming exists to prevent — and the
// number that does answer "when did we last look" is LastAttempt.
func TestInventoryCountsStoredRowsNotCaptureAttempts(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	first := time.Now().UTC().Truncate(time.Second).Add(-48 * time.Hour)
	unchanged := testSnapshot("same-hash", "acme")
	unchanged.Timestamp = first
	if _, err := store.PutSchema(ctx, k, unchanged); err != nil {
		t.Fatal(err)
	}
	again := testSnapshot("same-hash", "acme")
	again.Timestamp = first.Add(24 * time.Hour)
	if out, err := store.PutSchema(ctx, k, again); err != nil {
		t.Fatal(err)
	} else if out != PutDeduped {
		t.Fatalf("fixture must dedup for this test to mean anything, got %v", out)
	}

	span := inventoryOf(t, store, k)["schema"]
	if span.Rows != 1 {
		t.Errorf("an unchanged schema stores no row: want 1, got %d", span.Rows)
	}
	if !span.Newest.Equal(first) {
		t.Errorf("newest is when the content last changed: want %s, got %s", first, *span.Newest)
	}
	if span.LastAttempt != nil {
		t.Errorf("nothing recorded an attempt, but got %s", *span.LastAttempt)
	}

	attempted := first.Add(36 * time.Hour)
	if err := store.MarkCaptureAttempt(ctx, k, "", "schema", attempted); err != nil {
		t.Fatal(err)
	}
	span = inventoryOf(t, store, k)["schema"]
	if span.LastAttempt == nil || !span.LastAttempt.Equal(attempted) {
		t.Fatalf("want the attempt clock at %s, got %+v", attempted, span.LastAttempt)
	}
	if !span.Newest.Equal(first) {
		t.Errorf("an attempt that stored nothing must not move the span: got %s", *span.Newest)
	}
}

func TestInventoryIsScopedToOneKey(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	mine, theirs := key("acme", "primary"), key("zeta", "primary")

	if _, err := store.PutSchema(ctx, mine, testSnapshot("h1", "acme")); err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"z1", "z2"} {
		if _, err := store.PutSchema(ctx, theirs, testSnapshot(h, "zeta")); err != nil {
			t.Fatal(err)
		}
	}
	putNodeActivityAt(t, store, theirs, "za", "primary", false, time.Now().UTC())
	if err := store.MarkCaptureAttempt(ctx, theirs, "", "schema", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	got := inventoryOf(t, store, mine)
	if len(got) != 1 || got["schema"].Rows != 1 {
		t.Fatalf("another key's rows leaked in: %+v", got)
	}
	if got["schema"].LastAttempt != nil {
		t.Errorf("another key's attempt leaked in: %s", *got["schema"].LastAttempt)
	}
}

// The production shape: one row an older binary wrote with an unreadable
// timestamp, among rows that are fine. SQLite's MIN/MAX are lexical and the
// garbage sorts above every real date, so an unfiltered MAX ends the span at an
// unknown — reporting the newest capture as missing on a store that has one.
func TestInventoryExcludesUnreadableTimestampsFromTheSpan(t *testing.T) {
	store := testStore(t)
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second).Add(-24 * time.Hour)
	putNodeActivityAt(t, store, k, "a1", "primary", false, base)
	putNodeActivityAt(t, store, k, "a2", "primary", false, base.Add(time.Hour))
	insertRawActivity(t, store, k, "bad", "primary", "not-a-timestamp", nodePayload(false, "PostgreSQL 17.0"))

	span := inventoryOf(t, store, k)["activity"]
	if span.Rows != 3 {
		t.Errorf("rows: want all 3 counted, got %d", span.Rows)
	}
	if span.CorruptRows != 1 {
		t.Errorf("corrupt rows: want 1 reported, got %d", span.CorruptRows)
	}
	if span.Oldest == nil || span.Newest == nil {
		t.Fatalf("want the span of the readable rows, got %+v", span)
	}
	if !span.Oldest.Equal(base) || !span.Newest.Equal(base.Add(time.Hour)) {
		t.Errorf("span: want %s..%s, got %s..%s", base, base.Add(time.Hour), *span.Oldest, *span.Newest)
	}
}

// Every row unreadable: the count stands, the span goes unknown rather than
// becoming the zero date, which on the wire reads as a capture taken in year 1.
func TestInventoryLeavesAWhollyUnreadableSpanUnknown(t *testing.T) {
	store := testStore(t)
	k := key("acme", "primary")

	insertRawActivity(t, store, k, "bad", "primary", "not-a-timestamp", nodePayload(false, "PostgreSQL 17.0"))

	span := inventoryOf(t, store, k)["activity"]
	if span.Rows != 1 || span.CorruptRows != 1 {
		t.Fatalf("want one row, reported corrupt: %+v", span)
	}
	if span.Oldest != nil || span.Newest != nil {
		t.Errorf("want an unknown span, got %v..%v", span.Oldest, span.Newest)
	}
}

// A stream added to streamSources and not to inventoryStreams is silently
// absent from every tool response — the failure reads as "nothing captured".
func TestInventoryCoversEveryStream(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range inventoryStreams {
		if seen[s] {
			t.Errorf("stream %q listed twice", s)
		}
		seen[s] = true
		if _, ok := streamSources[s]; !ok {
			t.Errorf("stream %q is not in streamSources", s)
		}
	}
	for s := range streamSources {
		if !seen[s] {
			t.Errorf("stream %q has rows but no inventory entry", s)
		}
	}
}

// Retention deletes stats rows and leaves capture_attempts alone, so a stream
// pruned to nothing that captured this morning would otherwise vanish from the
// inventory — reported as never captured, the one reading the omission rule
// exists to reserve for streams that really were never captured.
func TestInventoryKeepsAPrunedStreamWithAnAttemptClock(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	old := time.Now().UTC().Truncate(time.Second).Add(-90 * 24 * time.Hour)
	putNodeActivityAt(t, store, k, "a1", "primary", false, old)
	recent := time.Now().UTC().Truncate(time.Second)
	if err := store.MarkCaptureAttempt(ctx, k, "primary", "activity", recent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteBefore(ctx, k, ActivityKind(""), old.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	span, ok := inventoryOf(t, store, k)["activity"]
	if !ok {
		t.Fatal("a pruned stream with a live attempt clock must not read as never captured")
	}
	if span.Rows != 0 || span.Oldest != nil || span.Newest != nil {
		t.Errorf("want no rows and no span, got %+v", span)
	}
	// a node-scoped stream with nothing left says zero nodes; only a stream
	// that has no node dimension says nothing at all
	if span.Nodes == nil || *span.Nodes != 0 {
		t.Errorf("want an explicit 0 nodes, got %v", span.Nodes)
	}
	if span.LastAttempt == nil || !span.LastAttempt.Equal(recent) {
		t.Errorf("want the attempt clock at %s, got %v", recent, span.LastAttempt)
	}
}

// The attempt clock folds across node labels: one node still capturing is what
// says the stream is live, and the newest of them is the answer.
func TestInventoryFoldsTheAttemptClockAcrossNodes(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	at := time.Now().UTC().Truncate(time.Second)
	putNodeActivityAt(t, store, k, "a1", "primary", false, at.Add(-time.Hour))
	stale := at.Add(-7 * 24 * time.Hour)
	if err := store.MarkCaptureAttempt(ctx, k, "replica", "activity", stale); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCaptureAttempt(ctx, k, "primary", "activity", at); err != nil {
		t.Fatal(err)
	}

	span := inventoryOf(t, store, k)["activity"]
	if span.LastAttempt == nil || !span.LastAttempt.Equal(at) {
		t.Fatalf("want the newest node's attempt %s, got %v", at, span.LastAttempt)
	}
	// a per-node stream's attempts are keyed per label; one stream's fold must
	// not pick up another stream's clock
	if q, ok := inventoryOf(t, store, k)["query"]; ok {
		t.Errorf("query has neither rows nor attempts but appears as %+v", q)
	}
}

// The SQL filter is looser than time.Parse on purpose. A value it admits and Go
// cannot read must be reported as corrupt, or the span goes absent next to
// corrupt_rows: 0 — an assertion that the store is clean.
func TestInventoryCountsABoundGoCannotParse(t *testing.T) {
	store := testStore(t)
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second).Add(-24 * time.Hour)
	putNodeActivityAt(t, store, k, "a1", "primary", false, base)
	// passes the date-prefix filter, no such month
	insertRawActivity(t, store, k, "bad", "primary", "2026-13-45T99:99:99Z", nodePayload(false, "PostgreSQL 17.0"))

	span := inventoryOf(t, store, k)["activity"]
	if span.Rows != 2 {
		t.Errorf("rows: want 2, got %d", span.Rows)
	}
	if span.Newest != nil {
		t.Errorf("the newest row is unreadable: want no bound, got %s", *span.Newest)
	}
	if span.CorruptRows == 0 {
		t.Error("an absent bound beside corrupt_rows: 0 says the store is clean")
	}
	if span.Oldest == nil || !span.Oldest.Equal(base) {
		t.Errorf("the readable end still stands: want %s, got %v", base, span.Oldest)
	}
}
