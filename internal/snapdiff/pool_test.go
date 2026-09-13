package snapdiff

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
)

// A read pool is one label over several servers. Its previous capture is usually
// another server's, and differencing two servers' cumulative counters is noise.
// DiffNodePair pairs the newer capture with its own server's earlier one, and
// refuses when that server has none. A label that merely restarted is not a pool.

func poolActivity(t *testing.T, store *history.Store, hash string, ts, boot time.Time, seq int64) history.StoredSnapshot {
	t.Helper()
	a := mkActivity("sh", hash, "pool", ts, seq)
	b := boot
	a.Node.PostmasterStartTime = &b
	s := history.WrapActivity(a)
	put(t, store, s)
	return s
}

func poolQuery(t *testing.T, store *history.Store, hash string, ts, boot time.Time, calls int64) history.StoredSnapshot {
	t.Helper()
	q := mkQuery("sh", hash, "pool", ts, calls, float64(calls))
	b := boot
	q.Node.PostmasterStartTime = &b
	s := history.WrapQueryStats(q)
	put(t, store, s)
	return s
}

func seqScan(t *testing.T, d *diff.ActivityDelta) diff.CounterDelta {
	t.Helper()
	for _, c := range d.Counters {
		if c.Identity.Name == "users" && c.Metric == diff.MetricSeqScan {
			return c
		}
	}
	t.Fatalf("no users seq_scan row in %+v", d)
	return diff.CounterDelta{}
}

func hasCaveat(caveats []string, sub string) bool {
	for _, c := range caveats {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

func TestDiffNodePair_PoolPairsWithinMember(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	a3 := poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)
	b4 := poolActivity(t, store, "b4", t0.Add(3*time.Hour), bootB, 1_300)

	d, err := DiffNodePair(ctx, store, key(), a3, b4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.From.ContentHash() != "b2" {
		t.Fatalf("paired with %q, want b4's own earlier capture b2", d.From.ContentHash())
	}
	if d.Activity.Refused() {
		t.Fatalf("same-member pair refused: %s", d.Activity.Incomparable)
	}
	if r := seqScan(t, d.Activity); r.ValueA != 1_000 || r.ValueB != 1_300 {
		t.Errorf("seq_scan %v -> %v, want 1000 -> 1300 from one server", r.ValueA, r.ValueB)
	}
	if !hasCaveat(d.Activity.Caveats, `label "pool" rotates between servers: compared the server`) {
		t.Errorf("pairing not explained: %v", d.Activity.Caveats)
	}
	if hasCaveat(d.Activity.Caveats, "restarted or was replaced") {
		t.Errorf("a same-member pair must not carry the restart caveat: %v", d.Activity.Caveats)
	}
}

func TestDiffNodePair_PoolMemberWithoutBaselineRefuses(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)
	b4 := poolActivity(t, store, "b4", t0.Add(3*time.Hour), bootB, 1_300)
	c5 := poolActivity(t, store, "c5", t0.Add(4*time.Hour), bootC, 20)

	d, err := DiffNodePair(ctx, store, key(), b4, c5, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Activity.Incomparable, "has no capture of its own") {
		t.Fatalf("a member with no earlier capture must refuse, got %+v", d.Activity)
	}
	if len(d.Activity.Counters) != 0 {
		t.Error("a refusal carries no rows")
	}
}

// A→A' never recurs, so it is a restart: today's diff with the restart caveat.
func TestDiffNodePair_RestartIsNotAPool(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootA2 := t0.Add(-72*time.Hour), t0.Add(90*time.Minute)

	poolActivity(t, store, "a1", t0, bootA, 100)
	a2 := poolActivity(t, store, "a2", t0.Add(time.Hour), bootA, 150)
	a3 := poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA2, 170)

	d, err := DiffNodePair(ctx, store, key(), a2, a3, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.From.ContentHash() != "a2" {
		t.Fatalf("a restart must diff the captures asked for, paired with %q", d.From.ContentHash())
	}
	if !hasCaveat(d.Activity.Caveats, "restarted or was replaced") || hasCaveat(d.Activity.Caveats, "rotates between servers") {
		t.Errorf("want only the restart caveat, got %v", d.Activity.Caveats)
	}
}

func TestDiffNodePair_PoolQueryPairsWithinMember(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolQuery(t, store, "qa1", t0, bootA, 10)
	poolQuery(t, store, "qb2", t0.Add(time.Hour), bootB, 500)
	qa3 := poolQuery(t, store, "qa3", t0.Add(2*time.Hour), bootA, 20)
	qb4 := poolQuery(t, store, "qb4", t0.Add(3*time.Hour), bootB, 800)

	d, err := DiffNodePair(ctx, store, key(), qa3, qb4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.From.ContentHash() != "qb2" || d.Query.Incomparable != "" {
		t.Fatalf("want pairing with qb2, got from=%q incomparable=%q", d.From.ContentHash(), d.Query.Incomparable)
	}
	if d.Query.CallsDelta != 300 {
		t.Errorf("calls delta %d, want 300 from one server", d.Query.CallsDelta)
	}
	if !hasCaveat(d.Query.Caveats, "rotates between servers") {
		t.Errorf("pairing not explained: %v", d.Query.Caveats)
	}
}

// pairing looks backwards from `to`; reversed operands would subtract servers
func TestDiffNodePair_PoolReversedOperandsRefuse(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	a3 := poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)
	b4 := poolActivity(t, store, "b4", t0.Add(3*time.Hour), bootB, 1_300)

	d, err := DiffNodePair(ctx, store, key(), b4, a3, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Activity.Incomparable, "pass the older capture first") || len(d.Activity.Counters) != 0 {
		t.Errorf("reversed operands on a rotating label must refuse, got %+v", d.Activity)
	}
}

// the default MCP/CLI range on a pool: latest~1 is the other server
func TestBuild_PoolLatestPairsWithinMember(t *testing.T) {
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)
	poolActivity(t, store, "b4", t0.Add(3*time.Hour), bootB, 1_300)

	res, err := Build(context.Background(), store, key(), Options{From: "latest~1", To: "latest", Kind: "activity"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.ActivityDelta) != 1 {
		t.Fatalf("want one pool delta, got %+v", res.ActivityDelta)
	}
	if r := seqScan(t, res.ActivityDelta[0].Delta); r.ValueA != 1_000 || r.ValueB != 1_300 {
		t.Errorf("seq_scan %v -> %v, want b2 -> b4", r.ValueA, r.ValueB)
	}
	if !strings.Contains(strings.Join(res.ForView("summary", 0).Correlation.Notes, " | "), `activity: label "pool" rotates between servers`) {
		t.Errorf("pairing note missing from summary: %v", res.Correlation.Notes)
	}
}

func labelQuery(t *testing.T, store *history.Store, label, hash string, ts time.Time, boot *time.Time, calls int64) history.StoredSnapshot {
	t.Helper()
	q := mkQuery("sh", hash, label, ts, calls, float64(calls))
	q.Node.PostmasterStartTime = boot
	s := history.WrapQueryStats(q)
	put(t, store, s)
	return s
}

// a cross-label pair stays DiffQueryStats' refusal even when `to` rotates
func TestDiffNodePair_PoolIgnoresOtherLabel(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB, bootR := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-24*time.Hour)

	poolQuery(t, store, "qa1", t0, bootA, 10)
	poolQuery(t, store, "qb2", t0.Add(time.Hour), bootB, 500)
	poolQuery(t, store, "qa3", t0.Add(2*time.Hour), bootA, 20)
	qb4 := poolQuery(t, store, "qb4", t0.Add(3*time.Hour), bootB, 800)
	r := labelQuery(t, store, "replica1", "r1", t0.Add(150*time.Minute), &bootR, 5)

	d, err := DiffNodePair(ctx, store, key(), r, qb4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Query.Incomparable, "different nodes") || d.From.ContentHash() != "r1" {
		t.Fatalf("cross-label pair must refuse as different nodes, got from=%q %q", d.From.ContentHash(), d.Query.Incomparable)
	}
}

// C's baseline sits behind an A and a B capture
func TestDiffNodePair_PoolThreeMembers(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-24*time.Hour)

	poolQuery(t, store, "qa1", t0, bootA, 10)
	poolQuery(t, store, "qb2", t0.Add(time.Hour), bootB, 500)
	poolQuery(t, store, "qc3", t0.Add(2*time.Hour), bootC, 30)
	poolQuery(t, store, "qa4", t0.Add(3*time.Hour), bootA, 40)
	qb5 := poolQuery(t, store, "qb5", t0.Add(4*time.Hour), bootB, 700)
	qc6 := poolQuery(t, store, "qc6", t0.Add(5*time.Hour), bootC, 90)

	d, err := DiffNodePair(ctx, store, key(), qb5, qc6, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.From.ContentHash() != "qc3" {
		t.Fatalf("paired with %q, want qc3", d.From.ContentHash())
	}
	if d.Query.CallsDelta != 60 || d.Query.Window != 3*time.Hour {
		t.Errorf("calls %d over %s, want 60 over 3h", d.Query.CallsDelta, d.Query.Window)
	}
}

func TestDiffNodePair_PoolQueryNoBaselineRefuses(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-time.Hour)

	poolQuery(t, store, "qa1", t0, bootA, 10)
	poolQuery(t, store, "qb2", t0.Add(time.Hour), bootB, 500)
	poolQuery(t, store, "qa3", t0.Add(2*time.Hour), bootA, 20)
	qb4 := poolQuery(t, store, "qb4", t0.Add(3*time.Hour), bootB, 800)
	qc5 := poolQuery(t, store, "qc5", t0.Add(4*time.Hour), bootC, 3)

	d, err := DiffNodePair(ctx, store, key(), qb4, qc5, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Query.Incomparable, "no capture of its own") || d.Query.Window != time.Hour {
		t.Fatalf("want a refusal over the requested 1h window, got %q over %s", d.Query.Incomparable, d.Query.Window)
	}
}

// from predates fingerprints; to's server is known and the label rotates
func TestDiffNodePair_PoolUnfingerprintedFromPairs(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)
	x := history.WrapActivity(mkActivity("sh", "x", "pool", t0.Add(150*time.Minute), 120))
	put(t, store, x)
	b4 := poolActivity(t, store, "b4", t0.Add(3*time.Hour), bootB, 1_300)

	d, err := DiffNodePair(ctx, store, key(), x, b4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.From.ContentHash() != "b2" {
		t.Fatalf("paired with %q, want b2", d.From.ContentHash())
	}
}

// the substituted row's schema is gone: rollup is skipped on both sides
func TestDiffNodePair_PoolBaselineWithoutSchemaSkipsRollup(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)
	parts := partitionedSchema("parts", t0)
	put(t, store, history.WrapSchema(parts))

	child := func(ref, hash string, ts, boot time.Time, seq int64) history.StoredSnapshot {
		a := childActivity(hash, ts, seq)
		a.SchemaRefHash, a.Node.Source = ref, "pool"
		b := boot
		a.Node.PostmasterStartTime = &b
		s := history.WrapActivity(a)
		put(t, store, s)
		return s
	}
	child("parts", "a1", t0, bootA, 10)
	child("gone", "b2", t0.Add(time.Hour), bootB, 100)
	a3 := child("parts", "a3", t0.Add(2*time.Hour), bootA, 20)
	b4 := child("parts", "b4", t0.Add(3*time.Hour), bootB, 250)

	d, err := DiffNodePair(ctx, store, key(), a3, b4, parts, parts)
	if err != nil {
		t.Fatal(err)
	}
	if d.From.ContentHash() != "b2" {
		t.Fatalf("paired with %q, want b2", d.From.ContentHash())
	}
	if hasCounter(d.Activity, "events") || !hasCounter(d.Activity, "events_2026") {
		t.Errorf("want child counters only, no rolled-up parent: %+v", d.Activity.Counters)
	}
}

func TestBuild_PoolCorrelationNamesPairedRow(t *testing.T) {
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)
	poolActivity(t, store, "b4", t0.Add(3*time.Hour), bootB, 1_300)

	res, err := Build(context.Background(), store, key(), Options{From: "latest~1", To: "latest", Kind: "activity"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.Correlation.From.Activity) != 1 {
		t.Fatalf("want one from match, got %+v", res.Correlation.From.Activity)
	}
	if mi := res.Correlation.From.Activity[0]; mi.Hash != "b2" || mi.Source != "member" || !mi.TakenAt.Equal(t0.Add(time.Hour)) {
		t.Errorf("from match names %+v, want b2 via member", mi)
	}
}

func TestBuild_PoolQueryNotesReachSummary(t *testing.T) {
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolQuery(t, store, "qa1", t0, bootA, 10)
	poolQuery(t, store, "qb2", t0.Add(time.Hour), bootB, 500)
	poolQuery(t, store, "qa3", t0.Add(2*time.Hour), bootA, 20)
	poolQuery(t, store, "qb4", t0.Add(3*time.Hour), bootB, 800)

	res, err := Build(context.Background(), store, key(), Options{From: "latest~1", To: "latest", Kind: "query"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	joined := strings.Join(res.ForView("summary", 0).Correlation.Notes, " | ")
	if !strings.Contains(joined, `query: label "pool" rotates between servers: compared`) {
		t.Errorf("query pairing note missing from summary: %q", joined)
	}
}

// schema anchors match b2 and a4; pairing must use those rows' own times and
// find a1, not reason from the schema captures' timestamps
func TestBuild_PoolSchemaAnchoredUsesRowTimes(t *testing.T) {
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	member := func(ref, hash string, ts, boot time.Time, seq int64) {
		a := mkActivity(ref, hash, "pool", ts, seq)
		b := boot
		a.Node.PostmasterStartTime = &b
		put(t, store, history.WrapActivity(a))
	}
	put(t, store, history.WrapSchema(mkSchema("schema-a", t0, table("users", "id"))))
	member("schema-a", "a1", t0.Add(time.Minute), bootA, 100)
	member("schema-a", "b2", t0.Add(time.Hour), bootB, 1_000)
	put(t, store, history.WrapSchema(mkSchema("schema-b", t0.Add(2*time.Hour), table("users", "id"))))
	member("schema-b", "b3", t0.Add(2*time.Hour+time.Minute), bootB, 1_100)
	member("schema-b", "a4", t0.Add(3*time.Hour), bootA, 400)

	res, err := Build(context.Background(), store, key(), Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.ActivityDelta) != 1 || res.ActivityDelta[0].Delta.Refused() {
		t.Fatalf("want one paired pool delta, got %+v", res.ActivityDelta)
	}
	if r := seqScan(t, res.ActivityDelta[0].Delta); r.ValueA != 100 || r.ValueB != 400 {
		t.Errorf("seq_scan %v -> %v, want a1 -> a4", r.ValueA, r.ValueB)
	}
}

// as of b2 the label has not rotated yet; as of a3 it has, so the check must
// read the later operand
func TestDiffNodePair_PoolReversedUsesLaterOperandForDetection(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	poolActivity(t, store, "a1", t0, bootA, 100)
	b2 := poolActivity(t, store, "b2", t0.Add(time.Hour), bootB, 1_000)
	a3 := poolActivity(t, store, "a3", t0.Add(2*time.Hour), bootA, 150)

	d, err := DiffNodePair(ctx, store, key(), a3, b2, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Activity.Incomparable, "pass the older capture first") {
		t.Fatalf("want the reversed refusal, got %+v", d.Activity)
	}
}

func TestDiffNodePair_PoolLookupFailure(t *testing.T) {
	t0 := time.Now().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)
	mk := func(hash string, ts, boot time.Time) history.StoredSnapshot {
		a := mkActivity("sh", hash, "pool", ts, 1)
		b := boot
		a.Node.PostmasterStartTime = &b
		return history.WrapActivity(a)
	}
	from, to := mk("a1", t0, bootA), mk("b2", t0.Add(time.Hour), bootB)

	t.Run("a store error refuses", func(t *testing.T) {
		store := openStore(t)
		store.Close()
		d, err := DiffNodePair(context.Background(), store, key(), from, to, nil, nil)
		if err != nil {
			t.Fatalf("a failed check must refuse, not error: %v", err)
		}
		if !strings.Contains(d.Activity.Incomparable, `could not check whether label "pool" rotates`) {
			t.Fatalf("want a lookup refusal, got %+v", d.Activity)
		}
	})

	t.Run("a cancelled request stays an error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := DiffNodePair(ctx, openStore(t), key(), from, to, nil, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	})
}

// row-cap caveats fire on most busy servers; they must not flood summary notes
func TestBuild_QueryNotesSkipRoutineCaveats(t *testing.T) {
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-3 * time.Hour)
	for i, calls := range []int64{10, 40} {
		q := mkQuery("sh", fmt.Sprintf("capped-%d", i), "primary", t0.Add(time.Duration(i)*time.Hour), calls, float64(calls))
		q.RawRows = q.RowCap
		put(t, store, history.WrapQueryStats(q))
	}

	res, err := Build(context.Background(), store, key(), Options{From: "latest~1", To: "latest", Kind: "query"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.QueryDelta) != 1 || len(res.QueryDelta[0].Delta.Caveats) == 0 {
		t.Fatalf("fixture should produce a capped query delta with caveats, got %+v", res.QueryDelta)
	}
	for _, n := range res.Correlation.Notes {
		if strings.HasPrefix(n, "query: ") {
			t.Errorf("routine caveat leaked into notes: %q", n)
		}
	}
}
