package history

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// IsPool and MemberBaseline are how a diff over a read pool keeps both sides on
// one server. IsPool must see the label as of the capture being diffed, not as
// of now, and MemberBaseline must find a server's own earlier capture however
// its start time was spelled.

func putNodeQueryFingerprintAt(t *testing.T, s *Store, key SnapshotKey, hash, label string, rowTS, started time.Time) {
	t.Helper()
	q := queryStatsFixture("sr", hash, label)
	q.Node.Timestamp = rowTS
	q.Node.PostmasterStartTime = &started
	if _, err := s.PutQueryStats(context.Background(), key, q); err != nil {
		t.Fatal(err)
	}
}

func TestIsPool(t *testing.T) {
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	t.Run("alternating servers are a pool", func(t *testing.T) {
		s := testStore(t)
		putNodeActivityFingerprintAt(t, s, key, "a1", "pool", t0, bootA, "")
		putNodeActivityFingerprintAt(t, s, key, "b2", "pool", t0.Add(time.Hour), bootB, "")
		putNodeActivityFingerprintAt(t, s, key, "a3", "pool", t0.Add(2*time.Hour), bootA, "")
		if ok, err := s.IsPool(ctx, key, "pool", t0.Add(2*time.Hour)); err != nil || !ok {
			t.Fatalf("IsPool = %v, %v; want true", ok, err)
		}
	})

	t.Run("a one-way change is a restart", func(t *testing.T) {
		s := testStore(t)
		putNodeActivityFingerprintAt(t, s, key, "a1", "node", t0, bootA, "")
		putNodeActivityFingerprintAt(t, s, key, "a2", "node", t0.Add(time.Hour), bootA, "")
		putNodeActivityFingerprintAt(t, s, key, "b3", "node", t0.Add(2*time.Hour), bootB, "")
		if ok, err := s.IsPool(ctx, key, "node", t0.Add(2*time.Hour)); err != nil || ok {
			t.Fatalf("IsPool = %v, %v; want false", ok, err)
		}
	})

	// the recurrence lands after upTo, so as of upTo this was only a change
	t.Run("rows after upTo are not evidence", func(t *testing.T) {
		s := testStore(t)
		putNodeActivityFingerprintAt(t, s, key, "a1", "later", t0, bootA, "")
		putNodeActivityFingerprintAt(t, s, key, "b2", "later", t0.Add(time.Hour), bootB, "")
		putNodeActivityFingerprintAt(t, s, key, "a3", "later", t0.Add(2*time.Hour), bootA, "")
		if ok, err := s.IsPool(ctx, key, "later", t0.Add(time.Hour)); err != nil || ok {
			t.Fatalf("IsPool as of b2 = %v, %v; want false", ok, err)
		}
	})

	// both streams feed one window, as they do for snapshot nodes
	t.Run("oscillation across streams counts", func(t *testing.T) {
		s := testStore(t)
		putNodeActivityFingerprintAt(t, s, key, "a1", "mixed", t0, bootA, "")
		putNodeQueryFingerprintAt(t, s, key, "qb2", "mixed", t0.Add(time.Hour), bootB)
		putNodeActivityFingerprintAt(t, s, key, "a3", "mixed", t0.Add(2*time.Hour), bootA, "")
		if ok, err := s.IsPool(ctx, key, "mixed", t0.Add(2*time.Hour)); err != nil || !ok {
			t.Fatalf("IsPool = %v, %v; want true", ok, err)
		}
	})
}

func TestMemberBaseline(t *testing.T) {
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := testStore(t)
	putNodeActivityFingerprintAt(t, s, key, "a1", "pool", t0, bootA, "")
	putNodeActivityFingerprintAt(t, s, key, "b2", "pool", t0.Add(time.Hour), bootB, "")
	putNodeActivityFingerprintAt(t, s, key, "a3", "pool", t0.Add(2*time.Hour), bootA, "")
	putNodeActivityFingerprintAt(t, s, key, "b4", "pool", t0.Add(3*time.Hour), bootB, "")
	putNodeQueryFingerprintAt(t, s, key, "qb2", "pool", t0.Add(time.Hour), bootB)

	get := func(t *testing.T, kind SnapshotKind, started, before, notAfter time.Time) (string, bool) {
		t.Helper()
		snap, found, err := s.MemberBaseline(ctx, key, kind, started, before, notAfter)
		if err != nil {
			t.Fatal(err)
		}
		return snap.ContentHash(), found
	}

	t.Run("newest same-server capture no later than notAfter", func(t *testing.T) {
		if h, ok := get(t, ActivityKind("pool"), bootB, t0.Add(3*time.Hour), t0.Add(2*time.Hour)); !ok || h != "b2" {
			t.Fatalf("got %q, %v; want b2", h, ok)
		}
	})

	t.Run("the to capture itself is never its own baseline", func(t *testing.T) {
		if h, ok := get(t, ActivityKind("pool"), bootB, t0.Add(3*time.Hour), t0.Add(3*time.Hour)); !ok || h != "b2" {
			t.Fatalf("got %q, %v; want b2, not b4", h, ok)
		}
	})

	t.Run("no earlier capture from that server", func(t *testing.T) {
		if h, ok := get(t, ActivityKind("pool"), bootB, t0.Add(time.Hour), t0.Add(time.Hour)); ok {
			t.Fatalf("got %q; want none", h)
		}
	})

	// the payload keeps the driver's zone offset, so the stored text is not UTC
	t.Run("start time stored with an offset still matches", func(t *testing.T) {
		bootC := t0.Add(-24 * time.Hour)
		putNodeActivityFingerprintAt(t, s, key, "c1", "zoned", t0, bootC.In(time.FixedZone("CEST", 2*60*60)), "")
		if h, ok := get(t, ActivityKind("zoned"), bootC.UTC(), t0.Add(time.Hour), t0.Add(time.Hour)); !ok || h != "c1" {
			t.Fatalf("got %q, %v; want c1", h, ok)
		}
	})

	t.Run("query stream reads its own table", func(t *testing.T) {
		if h, ok := get(t, QueryKind("pool"), bootB, t0.Add(3*time.Hour), t0.Add(2*time.Hour)); !ok || h != "qb2" {
			t.Fatalf("got %q, %v; want qb2", h, ok)
		}
	})

	t.Run("non-node kinds are an error", func(t *testing.T) {
		if _, _, err := s.MemberBaseline(ctx, key, SchemaKind(), bootB, t0, t0); err == nil {
			t.Fatal("schema has no members")
		}
	})
}

// The window is anchored at upTo, not at now: a label that rotated two months
// ago is a pool as of its own captures, and a recurrence older than the
// window before upTo is not evidence.
func TestIsPool_WindowFollowsUpTo(t *testing.T) {
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	s := testStore(t)
	old := time.Now().UTC().Truncate(time.Second).Add(-60 * 24 * time.Hour)
	bootA, bootB := old.Add(-72*time.Hour), old.Add(-48*time.Hour)

	putNodeActivityFingerprintAt(t, s, key, "a1", "pool", old, bootA, "")
	putNodeActivityFingerprintAt(t, s, key, "b2", "pool", old.Add(time.Hour), bootB, "")
	putNodeActivityFingerprintAt(t, s, key, "a3", "pool", old.Add(2*time.Hour), bootA, "")

	if ok, err := s.IsPool(ctx, key, "pool", old.Add(2*time.Hour)); err != nil || !ok {
		t.Fatalf("IsPool as of a3 = %v, %v; want true", ok, err)
	}
	// a1 has left the window; b2 and a3 alone are one change, not a rotation
	if ok, err := s.IsPool(ctx, key, "pool", old.Add(30*24*time.Hour+30*time.Minute)); err != nil || ok {
		t.Fatalf("IsPool once a1 left the window = %v, %v; want false", ok, err)
	}
}

func TestIsPool_UnfingerprintedRowsAndTooFewRows(t *testing.T) {
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	t.Run("legacy rows between members are not a server", func(t *testing.T) {
		s := testStore(t)
		putNodeActivityFingerprintAt(t, s, key, "a1", "pool", t0, bootA, "")
		putNodeActivityAt(t, s, key, "x2", "pool", false, t0.Add(time.Hour))
		putNodeActivityFingerprintAt(t, s, key, "b3", "pool", t0.Add(2*time.Hour), bootB, "")
		putNodeActivityAt(t, s, key, "x4", "pool", false, t0.Add(3*time.Hour))
		putNodeActivityFingerprintAt(t, s, key, "a5", "pool", t0.Add(4*time.Hour), bootA, "")
		if ok, err := s.IsPool(ctx, key, "pool", t0.Add(4*time.Hour)); err != nil || !ok {
			t.Fatalf("IsPool = %v, %v; want true", ok, err)
		}
	})

	t.Run("two servers seen once each have not rotated yet", func(t *testing.T) {
		s := testStore(t)
		putNodeActivityFingerprintAt(t, s, key, "a1", "young", t0, bootA, "")
		putNodeActivityFingerprintAt(t, s, key, "b2", "young", t0.Add(time.Hour), bootB, "")
		if ok, err := s.IsPool(ctx, key, "young", t0.Add(time.Hour)); err != nil || ok {
			t.Fatalf("IsPool = %v, %v; want false", ok, err)
		}
	})
}

// the scan is bounded, and rows without a start time count against the bound
func TestMemberBaseline_ScanLimit(t *testing.T) {
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)
	end := t0.Add(5 * time.Hour)

	fill := func(t *testing.T, s *Store, others int, legacy bool) {
		putNodeActivityFingerprintAt(t, s, key, "b0", "pool", t0, bootB, "")
		for i := range others {
			ts := t0.Add(time.Duration(i+1) * time.Minute)
			if legacy && i == others-1 {
				putNodeActivityAt(t, s, key, "x", "pool", false, ts)
				continue
			}
			putNodeActivityFingerprintAt(t, s, key, fmt.Sprintf("a%d", i), "pool", ts, bootA, "")
		}
	}

	t.Run("within the scan", func(t *testing.T) {
		s := testStore(t)
		fill(t, s, memberBaselineScan-1, false)
		if _, found, err := s.MemberBaseline(ctx, key, ActivityKind("pool"), bootB, end, end); err != nil || !found {
			t.Fatalf("found=%v err=%v; want b0 inside the scan", found, err)
		}
	})

	t.Run("past the scan", func(t *testing.T) {
		s := testStore(t)
		fill(t, s, memberBaselineScan, false)
		if _, found, err := s.MemberBaseline(ctx, key, ActivityKind("pool"), bootB, end, end); err != nil || found {
			t.Fatalf("found=%v err=%v; want none past the scan", found, err)
		}
	})

	t.Run("an unfingerprinted row takes a slot", func(t *testing.T) {
		s := testStore(t)
		fill(t, s, memberBaselineScan, true)
		if _, found, err := s.MemberBaseline(ctx, key, ActivityKind("pool"), bootB, end, end); err != nil || found {
			t.Fatalf("found=%v err=%v; want none, the legacy row fills the last slot", found, err)
		}
	})
}

// a cancelled request must surface as an error, however it was wrapped; every
// other failure of the pool pass only drops a label
func TestCtxErr(t *testing.T) {
	for err, want := range map[error]bool{
		context.Canceled:                                 true,
		context.DeadlineExceeded:                         true,
		fmt.Errorf("query stats: %w", context.Canceled):  true,
		fmt.Errorf("no such table: activity_stats"):      false,
		fmt.Errorf("wrapped: %w", fmt.Errorf("corrupt")): false,
	} {
		if got := ctxErr(err); got != want {
			t.Errorf("ctxErr(%v) = %v, want %v", err, got, want)
		}
	}
}
