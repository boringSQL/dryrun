package history

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func poolBundleStore(t *testing.T) (SnapshotStore, string) {
	t.Helper()
	root := t.TempDir()
	s, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return s, root
}

func poolBundleActivity(hash string, ts, started time.Time, pool bool) *schema.ActivityStatsSnapshot {
	a := activityFixture("sref", hash, "pool", true)
	a.Node.Timestamp = ts
	a.Node.PostmasterStartTime = &started
	a.Node.Pool = pool
	return a
}

func seedPoolBundle(t *testing.T, s SnapshotStore, k SnapshotKey, rows ...*schema.ActivityStatsSnapshot) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Put(ctx, k, WrapSchema(testSnapshot("sref", "appdb"))); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := s.Put(ctx, k, WrapActivity(r)); err != nil {
			t.Fatal(err)
		}
	}
}

func activityHashes(t *testing.T, s SnapshotStore, k SnapshotKey) map[string]bool {
	t.Helper()
	list, err := s.List(context.Background(), k, ActivityKind(""), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, l := range list {
		out[l.ContentHash] = true
	}
	return out
}

func TestPoolBundleKeepsNewestRowPerServer(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("a1", t0, bootA, true),
		poolBundleActivity("b1", t0.Add(time.Hour), bootB, true),
		poolBundleActivity("a2", t0.Add(2*time.Hour), bootA, true),
	)

	got := activityHashes(t, s, k)
	if len(got) != 2 || !got["a2"] || !got["b1"] {
		t.Fatalf("rows = %v, want a2 and b1", got)
	}
	latest, err := s.Latest(ctx, k, ActivityKind("pool"))
	if err != nil || latest == nil || latest.ContentHash != "a2" {
		t.Fatalf("Latest = %+v, %v; want a2", latest, err)
	}
	members, err := s.ListPoolMembers(ctx, k, ActivityKind("pool"), "sref")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].ContentHash != "b1" || members[0].SchemaRefHash != "sref" {
		t.Fatalf("members = %+v, want b1", members)
	}
	stored, err := s.Get(ctx, k, ActivityKind("pool"), NewRefHash("b1"))
	if err != nil {
		t.Fatalf("Get member by hash: %v", err)
	}
	if stored.ContentHash() != "b1" {
		t.Errorf("got %s", stored.ContentHash())
	}
}

func TestPoolBundleDropsServerUnseenPastMaxAge(t *testing.T) {
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("gone", t0, bootA, true),
		poolBundleActivity("live", t0.Add(poolMemberMaxAge+time.Hour), bootB, true),
	)
	if got := activityHashes(t, s, k); len(got) != 1 || !got["live"] {
		t.Errorf("rows = %v, want only live", got)
	}
}

// Pushes go oldest first, but a --full or a re-push can replay an older row.
func TestPoolBundleOlderRowOfKnownServerIsDedup(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA := t0.Add(-72 * time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k, poolBundleActivity("new", t0.Add(time.Hour), bootA, true))
	out, err := s.Put(ctx, k, WrapActivity(poolBundleActivity("old", t0, bootA, true)))
	if err != nil {
		t.Fatal(err)
	}
	if out != PutDeduped {
		t.Error("an older row of a server already held must dedup")
	}
	if got := activityHashes(t, s, k); len(got) != 1 || !got["new"] {
		t.Errorf("rows = %v, want only new", got)
	}
}

func TestPoolBundleDedupsHeadAndMember(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("a", t0, bootA, true),
		poolBundleActivity("b", t0.Add(time.Hour), bootB, true),
	)
	for _, r := range []*schema.ActivityStatsSnapshot{
		poolBundleActivity("a", t0, bootA, true),                // member
		poolBundleActivity("b", t0.Add(time.Hour), bootB, true), // head
	} {
		out, err := s.Put(ctx, k, WrapActivity(r))
		if err != nil {
			t.Fatal(err)
		}
		if out != PutDeduped {
			t.Errorf("%s should dedup", r.ContentHash)
		}
	}
}

// Removing pool = true must not leave the old servers travelling forever.
func TestPoolBundleUnmarkedRowClearsMembers(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("a", t0, bootA, true),
		poolBundleActivity("b", t0.Add(time.Hour), bootB, true),
		poolBundleActivity("plain", t0.Add(2*time.Hour), bootB, false),
	)
	if got := activityHashes(t, s, k); len(got) != 1 || !got["plain"] {
		t.Errorf("rows = %v, want only plain", got)
	}
	members, err := s.ListPoolMembers(ctx, k, ActivityKind("pool"), "sref")
	if err != nil || len(members) != 0 {
		t.Errorf("members = %+v, %v; want none", members, err)
	}
}

func TestPlainLabelNeverGrowsMembers(t *testing.T) {
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s, root := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("a", t0, bootA, false),
		poolBundleActivity("b", t0.Add(time.Hour), bootB, false),
	)
	if got := activityHashes(t, s, k); len(got) != 1 || !got["b"] {
		t.Errorf("rows = %v, want only b", got)
	}
	// unchanged on the wire for anyone not using a pool
	raw := readOnlyBundleJSON(t, root, k)
	if _, ok := raw["activity_members"]; ok {
		t.Error("activity_members must be omitted when empty")
	}
}

func readOnlyBundleJSON(t *testing.T, root string, k SnapshotKey) map[string]json.RawMessage {
	t.Helper()
	dir := BundleDir(root, k)
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("bundle dir: %v, %d entries", err, len(entries))
	}
	compressed, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	b, err := DecodeBundle(compressed)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(plain, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// An older dryrun decodes the same bundle into a struct without the members
// fields and must still find the label's newest row in `activity`.
func TestPoolBundleOlderReaderSeesLabelsNewestRow(t *testing.T) {
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s, root := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("a", t0, bootA, true),
		poolBundleActivity("b", t0.Add(time.Hour), bootB, true),
	)
	raw := readOnlyBundleJSON(t, root, k)
	if _, ok := raw["activity_members"]; !ok {
		t.Fatal("expected activity_members in a pool bundle")
	}
	var oldShape map[string]*schema.ActivityStatsSnapshot
	if err := json.Unmarshal(raw["activity"], &oldShape); err != nil {
		t.Fatal(err)
	}
	if len(oldShape) != 1 || oldShape["pool"] == nil || oldShape["pool"].ContentHash != "b" {
		t.Errorf("activity = %+v, want pool -> b", oldShape)
	}
}

func TestStoreListPoolMembers(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * 24 * time.Hour)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-24*time.Hour)

	put := func(s *Store, hash string, ts, started time.Time, pool bool) {
		t.Helper()
		if _, err := s.PutActivity(ctx, k, poolBundleActivity(hash, ts, started, pool)); err != nil {
			t.Fatal(err)
		}
	}
	hashes := func(s *Store) map[string]bool {
		t.Helper()
		list, err := s.ListPoolMembers(ctx, k, ActivityKind("pool"), "sref")
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, l := range list {
			out[l.ContentHash] = true
		}
		return out
	}

	t.Run("newest row of each other server", func(t *testing.T) {
		s := testStore(t)
		put(s, "a1", t0, bootA, true)
		put(s, "b1", t0.Add(time.Hour), bootB, true)
		put(s, "a2", t0.Add(2*time.Hour), bootA, true)
		put(s, "b2", t0.Add(3*time.Hour), bootB, true)
		if got := hashes(s); len(got) != 1 || !got["a2"] {
			t.Errorf("got %v, want a2", got)
		}
	})
	t.Run("server past the age limit is left out", func(t *testing.T) {
		s := testStore(t)
		put(s, "gone", t0, bootC, true)
		put(s, "a", t0.Add(poolMemberMaxAge+time.Hour), bootA, true)
		put(s, "b", t0.Add(poolMemberMaxAge+2*time.Hour), bootB, true)
		if got := hashes(s); len(got) != 1 || !got["a"] {
			t.Errorf("got %v, want a", got)
		}
	})
	t.Run("label whose newest row is not a pool has none", func(t *testing.T) {
		s := testStore(t)
		put(s, "a", t0, bootA, true)
		put(s, "plain", t0.Add(time.Hour), bootB, false)
		if got := hashes(s); len(got) != 0 {
			t.Errorf("got %v, want none", got)
		}
	})
	t.Run("other schema's rows are left out", func(t *testing.T) {
		s := testStore(t)
		other := poolBundleActivity("other", t0, bootA, true)
		other.SchemaRefHash = "sref-old"
		if _, err := s.PutActivity(ctx, k, other); err != nil {
			t.Fatal(err)
		}
		put(s, "b", t0.Add(time.Hour), bootB, true)
		if got := hashes(s); len(got) != 0 {
			t.Errorf("got %v, want none", got)
		}
	})
	t.Run("older unmarked rows are left out", func(t *testing.T) {
		s := testStore(t)
		put(s, "u", t0, bootA, false)
		put(s, "m1", t0.Add(time.Hour), bootB, true)
		put(s, "m2", t0.Add(2*time.Hour), bootC, true)
		if got := hashes(s); len(got) != 1 || !got["m1"] {
			t.Errorf("got %v, want m1", got)
		}
	})
	t.Run("schema and planner are refused", func(t *testing.T) {
		s := testStore(t)
		if _, err := s.ListPoolMembers(ctx, k, SchemaKind(), "sref"); err == nil {
			t.Error("want an error")
		}
	})
}

// A declared pool counts as rotating from its second server on; nothing recurs
// in a fresh clone. A restart on an undeclared label still reads as a restart.
func TestSummariseMembers_DeclaredPool(t *testing.T) {
	a, b := time.Unix(1000, 0), time.Unix(2000, 0)
	cases := []struct {
		name string
		fps  []NodeFingerprint
		want bool
	}{
		{"declared, two servers", []NodeFingerprint{{StartedAt: b, Pool: true}, {StartedAt: a, Pool: true}}, true},
		{"newest row no longer declared", []NodeFingerprint{{StartedAt: b}, {StartedAt: a, Pool: true}}, false},
		{"declared, one server", []NodeFingerprint{{StartedAt: a, Pool: true}, {StartedAt: a, Pool: true}}, false},
		{"undeclared restart", []NodeFingerprint{{StartedAt: b}, {StartedAt: a}}, false},
		{"undeclared recurrence", []NodeFingerprint{{StartedAt: a}, {StartedAt: b}, {StartedAt: a}}, true},
	}
	for _, c := range cases {
		if _, got := summariseMembers(c.fps); got != c.want {
			t.Errorf("%s: oscillating = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestIsPool_DeclaredWithTwoRows(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := testStore(t)
	for _, r := range []*schema.ActivityStatsSnapshot{
		poolBundleActivity("a", t0, bootA, true),
		poolBundleActivity("b", t0.Add(time.Minute), bootB, true),
	} {
		if _, err := s.PutActivity(ctx, k, r); err != nil {
			t.Fatal(err)
		}
	}
	pool, err := s.IsPool(ctx, k, "pool", t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !pool {
		t.Error("a declared pool with two servers must read as a pool")
	}
}

// Replays of an older unmarked row leave a plain label's newest row in place too.
func TestBundleOlderUnmarkedRowDoesNotWalkLabelBack(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	boot := t0.Add(-72 * time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k, poolBundleActivity("new", t0.Add(time.Hour), boot, false))
	out, err := s.Put(ctx, k, WrapActivity(poolBundleActivity("old", t0, boot, false)))
	if err != nil {
		t.Fatal(err)
	}
	if out != PutDeduped {
		t.Error("an older row must not count as a change")
	}
	if got := activityHashes(t, s, k); len(got) != 1 || !got["new"] {
		t.Errorf("rows = %v, want only new", got)
	}
}

func putIndexedPoolActivity(t *testing.T, s *Store, k SnapshotKey, hash string, ts, started time.Time, idxScan int64) {
	t.Helper()
	a := poolBundleActivity(hash, ts, started, true)
	a.Indexes = []schema.IndexActivityEntry{{
		Table:    schema.QualifiedName{Schema: "public", Name: "users"},
		Index:    "users_email_idx",
		Activity: schema.IndexActivity{IdxScan: idxScan},
	}}
	if _, err := s.PutActivity(context.Background(), k, a); err != nil {
		t.Fatal(err)
	}
}

// One row per server: the newest server's older capture must not count again.
func TestDeclaredPoolTotalsCountEachServerOnce(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := testStore(t)
	putIndexedSchema(t, s, k, "sref")
	putIndexedPoolActivity(t, s, k, "a1", t0, bootA, 5)
	putIndexedPoolActivity(t, s, k, "b1", t0.Add(time.Hour), bootB, 7)
	putIndexedPoolActivity(t, s, k, "b2", t0.Add(2*time.Hour), bootB, 9)
	putIndexedPoolActivity(t, s, k, "a2", t0.Add(3*time.Hour), bootA, 40)

	a, err := s.GetAnnotated(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	users := schema.QualifiedName{Schema: "public", Name: "users"}
	if n := a.TotalIndexScans(users, "users_email_idx"); n != 49 {
		t.Errorf("TotalIndexScans = %d, want 49 (a2 40 + b2 9)", n)
	}
}

// The scan has no early exit and runs on the MCP tick; the bound keeps it cheap.
func TestDeclaredPoolScanIsBounded(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-60 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := testStore(t)
	putIndexedSchema(t, s, k, "sref")
	putIndexedPoolActivity(t, s, k, "a", t0, bootA, 5)
	for i := range poolMemberScan {
		ts := t0.Add(time.Duration(i+1) * time.Minute)
		putIndexedPoolActivity(t, s, k, ts.Format("b150405"), ts, bootB, 0)
	}
	a, err := s.GetAnnotated(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	users := schema.QualifiedName{Schema: "public", Name: "users"}
	if n := a.TotalIndexScans(users, "users_email_idx"); n != 0 {
		t.Errorf("TotalIndexScans = %d: a server past the scan bound was read", n)
	}
}

// Documented limit: a max_age shorter than the member age deletes what a pull just added.
func TestShortMaxAgePrunesPoolMemberRow(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := now.Add(-720*time.Hour), now.Add(-480*time.Hour)

	s := testStore(t)
	putIndexedSchema(t, s, k, "sref")
	putIndexedPoolActivity(t, s, k, "a", now.Add(-96*time.Hour), bootA, 5)
	putIndexedPoolActivity(t, s, k, "b", now.Add(-time.Hour), bootB, 0)
	if _, err := s.Prune(ctx, k, PruneOptions{Cutoff: now.Add(-72 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if got := activityHashes(t, s, k); len(got) != 1 || !got["b"] {
		t.Errorf("rows = %v; if a is kept, drop the max_age note from multi-node-stats.md", got)
	}
}

// A server gone past the member age must stop counting: its counters would only inflate.
func TestDeclaredPoolIgnoresServerPastMaxAge(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * 24 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := testStore(t)
	putIndexedSchema(t, s, k, "sref")
	putIndexedPoolActivity(t, s, k, "gone", t0, bootA, 5)
	putIndexedPoolActivity(t, s, k, "live", t0.Add(poolMemberMaxAge+time.Hour), bootB, 0)

	a, err := s.GetAnnotated(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	users := schema.QualifiedName{Schema: "public", Name: "users"}
	if n := a.TotalIndexScans(users, "users_email_idx"); n != 0 {
		t.Errorf("TotalIndexScans = %d: a server past the age limit was added", n)
	}
}

// A marked row replayed after a newer unmarked head pairs them in one bundle;
// the newest row decides, so the label answers as one server everywhere.
func TestPoolBundleMarkedReplayUnderUnmarkedHeadStaysPlain(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-24*time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k,
		poolBundleActivity("a", t0, bootA, true),
		poolBundleActivity("plain", t0.Add(2*time.Hour), bootB, false),
	)
	if _, err := s.Put(ctx, k, WrapActivity(poolBundleActivity("b", t0.Add(time.Hour), bootC, true))); err != nil {
		t.Fatal(err)
	}
	members, err := s.ListPoolMembers(ctx, k, ActivityKind("pool"), "sref")
	if err != nil || len(members) != 0 {
		t.Errorf("members = %+v, %v; want none under an unmarked head", members, err)
	}
	if got := activityHashes(t, s, k); len(got) != 1 || !got["plain"] {
		t.Errorf("rows = %v, want only plain", got)
	}
}

// Whole-second timestamps tie; the tie breaks on hash the way newestPerNode
// does, or store and sync pick different heads for the same rows.
func TestBundlePlainLabelEqualSecondTieBreaksOnHash(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second)
	boot := t0.Add(-72 * time.Hour)

	s, _ := poolBundleStore(t)
	seedPoolBundle(t, s, k, poolBundleActivity("z-high", t0, boot, false))
	out, err := s.Put(ctx, k, WrapActivity(poolBundleActivity("a-low", t0, boot, false)))
	if err != nil {
		t.Fatal(err)
	}
	if out != PutDeduped {
		t.Error("a same-second row with a lower hash must not replace the head")
	}
	if got := activityHashes(t, s, k); len(got) != 1 || !got["z-high"] {
		t.Errorf("rows = %v, want only z-high", got)
	}
}

// Documented: a restart inside the limits counts twice, which inflates and never hides use.
func TestDeclaredPoolRestartCountsOldBootToo(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootOld, bootNew := t0.Add(-72*time.Hour), t0.Add(time.Minute)

	s := testStore(t)
	putIndexedSchema(t, s, k, "sref")
	putIndexedPoolActivity(t, s, k, "before", t0, bootOld, 5)
	putIndexedPoolActivity(t, s, k, "after", t0.Add(time.Hour), bootNew, 1)

	a, err := s.GetAnnotated(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	users := schema.QualifiedName{Schema: "public", Name: "users"}
	if n := a.TotalIndexScans(users, "users_email_idx"); n != 6 {
		t.Errorf("TotalIndexScans = %d, want 6 (old boot 5 + new boot 1)", n)
	}
}
