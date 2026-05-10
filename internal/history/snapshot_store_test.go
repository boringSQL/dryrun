package history

import (
	"context"
	"errors"
	"testing"
	"time"
)

func key(project, database string) SnapshotKey {
	return SnapshotKey{ProjectID: ProjectId(project), DatabaseID: DatabaseId(database)}
}

// TestPutInserts verifies that putting a fresh snapshot under a new key
// returns PutInserted and the row becomes visible via Latest.
func TestPutInserts(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	outcome, err := store.Put(ctx, k, testSnapshot("hash-1", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	if outcome != PutInserted {
		t.Errorf("first put: got %v, want PutInserted", outcome)
	}

	latest, err := store.Latest(ctx, k)
	if err != nil || latest == nil {
		t.Fatalf("Latest after Put: got (%v, %v), want non-nil summary", latest, err)
	}
	if latest.ContentHash != "hash-1" {
		t.Errorf("latest hash: got %q, want hash-1", latest.ContentHash)
	}
}

// TestPutDedupesSameHash exercises the dedup contract: putting the same
// content hash a second time under the same key must short-circuit and
// return PutDeduped without inserting a duplicate row.
func TestPutDedupesSameHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	snap := testSnapshot("dup-hash", "acme")

	if o, err := store.Put(ctx, k, snap); err != nil || o != PutInserted {
		t.Fatalf("first put: got (%v, %v)", o, err)
	}
	if o, err := store.Put(ctx, k, snap); err != nil || o != PutDeduped {
		t.Fatalf("second put: got (%v, %v), want PutDeduped", o, err)
	}

	list, err := store.List(ctx, k, TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 row after dedup, got %d", len(list))
	}
}

// TestPutIsKeyScoped guards against cross-key collisions: identical content
// hashes under different (project, database) pairs must each insert their own
// row instead of being deduped against each other.
func TestPutIsKeyScoped(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	snap := testSnapshot("same-hash", "shared")

	k1 := key("acme", "primary")
	k2 := key("acme", "replica")
	if _, err := store.Put(ctx, k1, snap); err != nil {
		t.Fatal(err)
	}
	if o, err := store.Put(ctx, k2, snap); err != nil || o != PutInserted {
		t.Fatalf("put under second key: got (%v, %v), want PutInserted", o, err)
	}

	for _, k := range []SnapshotKey{k1, k2} {
		got, err := store.List(ctx, k, TimeRange{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Errorf("key %+v: got %d rows, want 1", k, len(got))
		}
	}
}

// TestGetByLatestAtHash covers all three SnapshotRef variants against a
// three-snapshot history: Latest returns the newest, At(t) returns the most
// recent row at or before t, and Hash addresses a specific row.
func TestGetByLatestAtHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	mk := func(hash string, offset time.Duration) {
		s := testSnapshot(hash, "acme")
		s.Timestamp = now.Add(offset)
		if _, err := store.Put(ctx, k, s); err != nil {
			t.Fatal(err)
		}
	}
	mk("h-old", -2*time.Hour)
	mk("h-mid", -1*time.Hour)
	mk("h-new", 0)

	t.Run("Latest", func(t *testing.T) {
		s, err := store.Get(ctx, k, NewRefLatest())
		if err != nil || s == nil {
			t.Fatalf("got (%v, %v)", s, err)
		}
		if s.ContentHash != "h-new" {
			t.Errorf("got %q, want h-new", s.ContentHash)
		}
	})

	t.Run("At", func(t *testing.T) {
		// asking for "30 minutes ago" should resolve to the mid row (latest <= cutoff)
		s, err := store.Get(ctx, k, NewRefAt(now.Add(-30*time.Minute)))
		if err != nil || s == nil {
			t.Fatalf("got (%v, %v)", s, err)
		}
		if s.ContentHash != "h-mid" {
			t.Errorf("got %q, want h-mid", s.ContentHash)
		}
	})

	t.Run("Hash", func(t *testing.T) {
		s, err := store.Get(ctx, k, NewRefHash("h-old"))
		if err != nil || s == nil {
			t.Fatalf("got (%v, %v)", s, err)
		}
		if s.ContentHash != "h-old" {
			t.Errorf("got %q, want h-old", s.ContentHash)
		}
	})
}

// TestGetNotFound asserts that all three SnapshotRef variants surface a
// wrapped ErrSnapshotNotFound when no row matches; callers rely on this
// sentinel to distinguish "missing" from "corrupt".
func TestGetNotFound(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	cases := []struct {
		name string
		ref  SnapshotRef
	}{
		{"Latest", NewRefLatest()},
		{"At", NewRefAt(time.Now().UTC())},
		{"Hash", NewRefHash("does-not-exist")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := store.Get(ctx, k, c.ref)
			if !errors.Is(err, ErrSnapshotNotFound) {
				t.Errorf("got %v, want ErrSnapshotNotFound", err)
			}
		})
	}
}

// TestListWithTimeRange seeds three rows across a six-hour window and asserts
// the half-open semantics of TimeRange.From / TimeRange.To (>= from, < to).
func TestListWithTimeRange(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	for hash, offset := range map[string]time.Duration{
		"h-3h": -3 * time.Hour,
		"h-2h": -2 * time.Hour,
		"h-1h": -1 * time.Hour,
	} {
		s := testSnapshot(hash, "acme")
		s.Timestamp = now.Add(offset)
		if _, err := store.Put(ctx, k, s); err != nil {
			t.Fatal(err)
		}
	}

	from := now.Add(-2*time.Hour - time.Minute) // just before h-2h
	to := now.Add(-30 * time.Minute)            // just after h-1h
	list, err := store.List(ctx, k, TimeRange{From: &from, To: &to})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("got %d rows, want 2 (h-2h, h-1h)", len(list))
	}
	// List returns newest-first
	if list[0].ContentHash != "h-1h" || list[1].ContentHash != "h-2h" {
		t.Errorf("ordering: got [%s, %s], want [h-1h, h-2h]", list[0].ContentHash, list[1].ContentHash)
	}
}

// TestDeleteBeforeCutoff verifies the cutoff is exclusive: a row whose
// timestamp lies before the cutoff is removed, and a row whose timestamp
// equals or exceeds the cutoff is retained — which is how the v0.6 retention
// path keeps the latest snapshot alive while pruning history.
func TestDeleteBeforeCutoff(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	oldSnap := testSnapshot("h-old", "acme")
	oldSnap.Timestamp = now.Add(-24 * time.Hour)
	newSnap := testSnapshot("h-new", "acme")
	newSnap.Timestamp = now
	if _, err := store.Put(ctx, k, oldSnap); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, k, newSnap); err != nil {
		t.Fatal(err)
	}

	deleted, err := store.DeleteBefore(ctx, k, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Errorf("got %d deleted, want 1", deleted)
	}

	list, err := store.List(ctx, k, TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ContentHash != "h-new" {
		t.Errorf("survivors: got %+v, want [h-new]", list)
	}
}

// TestLatestEmpty: Latest on a key with no rows must return (nil, nil)
// rather than ErrSnapshotNotFound — it's a survey method, not a lookup.
func TestLatestEmpty(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	got, err := store.Latest(ctx, key("acme", "primary"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}
