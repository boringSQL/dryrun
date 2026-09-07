package history

import (
	"context"
	"strings"
	"testing"
	"time"
)

// An idle node captured twice writes two byte-identical activity_stats rows
// (the content hash excludes the timestamp, and activity_stats has no unique
// index). Everything that resolves a snapshot has to treat those twins as one
// addressable snapshot instead of calling the prefix ambiguous — with
// `capture --all --due` on a cron, quiet nodes are the common case.
func seedActivityTwins(t *testing.T, store *Store, k SnapshotKey, node, hash string, at ...time.Time) {
	t.Helper()
	ctx := context.Background()
	for _, ts := range at {
		a := activityFixture("sref-A", hash, node, false)
		a.Node.Timestamp = ts
		if _, err := store.PutActivity(ctx, k, a); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetActivityByHashPrefixPrefersNewestTwin(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "ac-idle", now.Add(-time.Hour), now)

	got, err := store.getActivityRef(ctx, k, "primary", NewRefHash("ac-idl"))
	if err != nil {
		t.Fatalf("prefix over two identical rows must resolve, got %v", err)
	}
	if !got.Node.Timestamp.Equal(now) {
		t.Errorf("resolved the %s twin, want the newest (%s)", got.Node.Timestamp, now)
	}
}

// A prefix spanning two *different* hashes is still ambiguous: newest-wins is
// a twin tiebreak, not a general "pick something" rule.
func TestGetActivityByHashPrefixStillRejectsDistinctHashes(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "ac-one", now.Add(-time.Hour))
	seedActivityTwins(t, store, k, "primary", "ac-two", now)

	_, err := store.getActivityRef(ctx, k, "primary", NewRefHash("ac-"))
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("distinct hashes under one prefix must stay ambiguous, got %v", err)
	}
}

// latest~N is positional now, so it addresses either twin instead of
// round-tripping through a hash that matches both.
func TestResolveTokenLatestTildeIsPositionalOverTwins(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "ac-idle", now.Add(-time.Hour), now)

	kind, ref, err := store.ResolveToken(ctx, k, "latest~1", "activity", "")
	if err != nil {
		t.Fatalf("resolve latest~1: %v", err)
	}
	if ref.Kind != RefIndex || ref.Index != 1 {
		t.Fatalf("latest~1 = %+v, want RefIndex(1)", ref)
	}
	got, err := store.Get(ctx, k, kind, ref)
	if err != nil {
		t.Fatalf("get latest~1: %v", err)
	}
	if ts := got.Timestamp(); !ts.Equal(now.Add(-time.Hour)) {
		t.Errorf("latest~1 landed on %s, want the older twin %s", ts, now.Add(-time.Hour))
	}

	// and latest~2 is out of range with only two rows
	if _, _, err := store.ResolveToken(ctx, k, "latest~2", "activity", ""); err == nil {
		t.Error("latest~2 over two captures should fail")
	}
}

func TestResolveSnapshotCollapsesTwins(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "ac-idle", now.Add(-time.Hour), now)

	sum, err := store.ResolveSnapshot(ctx, k, "ac-idl")
	if err != nil {
		t.Fatalf("twins must resolve to one snapshot, got %v", err)
	}
	if sum.Kind.Tag != KindActivity || sum.ContentHash != "ac-idle" {
		t.Fatalf("resolved %+v, want the activity twin", sum)
	}
	if !sum.Timestamp.Equal(now) {
		t.Errorf("resolved the %s row, want the newest (%s)", sum.Timestamp, now)
	}

	// a second, distinct hash under the same prefix is still ambiguous
	seedActivityTwins(t, store, k, "primary", "ac-idle-other", now)
	if _, err := store.ResolveSnapshot(ctx, k, "ac-idl"); err == nil ||
		!strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("distinct hashes must stay ambiguous, got %v", err)
	}
}

// Schema twins arise too: a->b->a puts the same content hash twice, since
// PutSchema only dedupes against the latest row.
func TestResolveSchemaSnapshotCollapsesTwins(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	for i, h := range []string{"sh-a", "sh-b", "sh-a"} {
		s := testSnapshot(h, "appdb")
		s.Timestamp = now.Add(time.Duration(i) * time.Minute)
		if _, err := store.PutSchema(ctx, k, s); err != nil {
			t.Fatal(err)
		}
	}

	sum, err := store.ResolveSchemaSnapshot(ctx, k, "sh-a")
	if err != nil {
		t.Fatalf("schema twins must resolve, got %v", err)
	}
	if !sum.Timestamp.Equal(now.Add(2 * time.Minute)) {
		t.Errorf("resolved the %s row, want the newest twin", sum.Timestamp)
	}

	snap, err := store.GetSchema(ctx, k, NewRefHash("sh-a"))
	if err != nil {
		t.Fatalf("GetSchema over twins must resolve, got %v", err)
	}
	if snap.ContentHash != "sh-a" {
		t.Errorf("got %q, want sh-a", snap.ContentHash)
	}
	if _, err := store.GetSchema(ctx, k, NewRefHash("sh-")); err == nil ||
		!strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("distinct schema hashes must stay ambiguous, got %v", err)
	}
}

// The bug as reported: `snapshot diff --latest --kind activity` on a node that
// was quiet between two captures.
func TestGetActivityLatestTildeAfterIdleCapture(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "ac-idle", now.Add(-time.Hour), now)

	for _, tok := range []string{"latest", "latest~1"} {
		kind, ref, err := store.ResolveToken(ctx, k, tok, "activity", "")
		if err != nil {
			t.Fatalf("%s: %v", tok, err)
		}
		if _, err := store.Get(ctx, k, kind, ref); err != nil {
			t.Fatalf("%s: %v", tok, err)
		}
	}
}

// RefIndex is implemented by every backend, so the bundle stores get the same
// positional contract the sqlite store does: nth-newest, out of range is a
// not-found, and a negative index never indexes a slice.
func TestFilesystemStoreRefIndex(t *testing.T) {
	store, _ := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	hashes := []string{"sh-old", "sh-mid", "sh-new"}
	for i, h := range hashes {
		s := testSnapshot(h, "appdb")
		s.Timestamp = now.Add(time.Duration(i) * time.Minute)
		if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
			t.Fatalf("put %s: %v", h, err)
		}
		a := activityFixture(h, "ac-"+h, "primary", false)
		a.Node.Timestamp = s.Timestamp
		if _, err := store.Put(ctx, k, WrapActivity(a)); err != nil {
			t.Fatalf("put activity %s: %v", h, err)
		}
	}

	// newest-first: latest~0 is sh-new
	for n, want := range map[int]string{0: "sh-new", 1: "sh-mid", 2: "sh-old"} {
		got, err := store.Get(ctx, k, SchemaKind(), NewRefIndex(n))
		if err != nil {
			t.Fatalf("latest~%d: %v", n, err)
		}
		if got.ContentHash() != want {
			t.Errorf("latest~%d = %q, want %q", n, got.ContentHash(), want)
		}
		gotA, err := store.Get(ctx, k, ActivityKind("primary"), NewRefIndex(n))
		if err != nil {
			t.Fatalf("activity latest~%d: %v", n, err)
		}
		if gotA.ContentHash() != "ac-"+want {
			t.Errorf("activity latest~%d = %q, want ac-%s", n, gotA.ContentHash(), want)
		}
	}

	for _, n := range []int{3, -1} {
		if _, err := store.Get(ctx, k, SchemaKind(), NewRefIndex(n)); err == nil {
			t.Errorf("schema latest~%d should not resolve", n)
		}
		if _, err := store.Get(ctx, k, ActivityKind("primary"), NewRefIndex(n)); err == nil {
			t.Errorf("activity latest~%d should not resolve", n)
		}
	}
}

func TestStoreRefIndexRejectsNegative(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	if _, err := store.PutSchema(ctx, k, testSnapshot("sh-1", "appdb")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSchema(ctx, k, NewRefIndex(-1)); err == nil {
		t.Error("a negative index must not fall through to OFFSET 0")
	}
	if _, err := store.Get(ctx, k, SchemaKind(), NewRefIndex(-1)); err == nil {
		t.Error("a negative index must not fall through to OFFSET 0")
	}
}

// Deletes address one row by id. Over twins that means the newest row goes and
// identical ones remain, so the caller has to be told the count rather than be
// left thinking the snapshot is gone.
func TestDeleteOverTwinsRemovesOneRowAndReportsTheCount(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "ac-idle", now.Add(-time.Hour), now)

	target, err := store.ResolveSnapshot(ctx, k, "ac-idl")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	twins, err := store.CountContentTwins(ctx, k, target)
	if err != nil {
		t.Fatalf("count twins: %v", err)
	}
	if twins != 2 {
		t.Fatalf("twin count = %d, want 2", twins)
	}

	if _, err := store.DeleteSnapshot(ctx, k, target); err != nil {
		t.Fatalf("delete: %v", err)
	}
	left, err := store.List(ctx, k, ActivityKind("primary"), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 {
		t.Fatalf("%d rows left, want 1", len(left))
	}
	// the newest row is the one that went
	if !left[0].Timestamp.Equal(now.Add(-time.Hour)) {
		t.Errorf("survivor is %s, want the older twin %s", left[0].Timestamp, now.Add(-time.Hour))
	}
	if n, _ := store.CountContentTwins(ctx, k, target); n != 1 {
		t.Errorf("twin count after delete = %d, want 1", n)
	}
}

// A schema twin count must not be confused by a same-hash row on another kind.
func TestCountContentTwinsIsKindAndNodeScoped(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	seedActivityTwins(t, store, k, "primary", "shared-hash", now)
	seedActivityTwins(t, store, k, "replica", "shared-hash", now, now.Add(time.Minute))
	if _, err := store.PutSchema(ctx, k, testSnapshot("shared-hash", "appdb")); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		kind SnapshotKind
		want int
	}{
		{SchemaKind(), 1},
		{ActivityKind("primary"), 1},
		{ActivityKind("replica"), 2},
	} {
		n, err := store.CountContentTwins(ctx, k,
			SnapshotSummary{Kind: tc.kind, ContentHash: "shared-hash"})
		if err != nil {
			t.Fatalf("%s: %v", tc.kind, err)
		}
		if n != tc.want {
			t.Errorf("%s twin count = %d, want %d", tc.kind, n, tc.want)
		}
	}
}
