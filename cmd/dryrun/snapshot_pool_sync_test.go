package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

const (
	poolTestIndex = "users_email_idx"
)

func poolSyncSchema(hash string, ts time.Time) *schema.SchemaSnapshot {
	s := syncTestSchema(hash, "appdb", ts)
	s.Tables[0].Indexes = []schema.Index{{Name: poolTestIndex, IndexType: "btree", Columns: []string{"email"}}}
	return s
}

func poolSyncActivity(schemaRef, hash string, ts, started time.Time, idxScan int64, pool bool) *schema.ActivityStatsSnapshot {
	a := syncTestActivity(schemaRef, hash, "read-pool", ts, true)
	a.Node.PostmasterStartTime = &started
	a.Node.Pool = pool
	a.Indexes = []schema.IndexActivityEntry{{
		Table:    schema.QualifiedName{Schema: "public", Name: "users"},
		Index:    poolTestIndex,
		Activity: schema.IndexActivity{IdxScan: idxScan},
	}}
	return a
}

func poolSyncQuery(schemaRef, hash string, ts, started time.Time, pool bool) *schema.QueryStatsSnapshot {
	q := syncTestQueryStats(schemaRef, hash, "read-pool", ts)
	q.Node.PostmasterStartTime = &started
	q.Node.Pool = pool
	return q
}

func openPoolStore(t *testing.T) *history.Store {
	t.Helper()
	s, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func putPoolRows(t *testing.T, s history.SnapshotStore, k history.SnapshotKey, rows ...history.StoredSnapshot) {
	t.Helper()
	for _, r := range rows {
		if _, err := s.Put(context.Background(), k, r); err != nil {
			t.Fatal(err)
		}
	}
}

// writer -> push -> pull into an empty db, the way a fresh clone gets a pool
func freshReader(t *testing.T, writer *history.Store, k history.SnapshotKey) *history.Store {
	t.Helper()
	ctx := context.Background()
	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := syncKeys(ctx, writer, remote, []history.SnapshotKey{k}, pullScope{latest: true}); err != nil {
		t.Fatalf("push: %v", err)
	}
	reader := openPoolStore(t)
	if _, err := syncKeys(ctx, remote, reader, []history.SnapshotKey{k}, pullScope{latest: true}); err != nil {
		t.Fatalf("pull: %v", err)
	}
	return reader
}

func unusedIndexReported(t *testing.T, s *history.Store, k history.SnapshotKey) bool {
	t.Helper()
	a, err := s.GetAnnotated(context.Background(), k, history.NewRefLatest())
	if err != nil {
		t.Fatalf("GetAnnotated: %v", err)
	}
	for _, u := range schema.DetectUnusedIndexes(a) {
		if u.IndexName == poolTestIndex {
			return true
		}
	}
	return false
}

// The index is used only through server A; server B answered last.
func TestFreshCloneOfPoolSeesEveryServer(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	writer := openPoolStore(t)
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a1", t0, bootA, 40, true)),
		history.WrapActivity(poolSyncActivity("sh", "b1", t0.Add(15*time.Minute), bootB, 0, true)),
	)
	if unusedIndexReported(t, writer, k) {
		t.Fatal("precondition: the writer itself must see server A's scans")
	}

	reader := freshReader(t, writer, k)
	if unusedIndexReported(t, reader, k) {
		t.Error("fresh clone reported an index used through another pool server as unused")
	}
}

// Query rows follow the same rule, so a reader can pair a server with its own
// earlier capture as soon as that server shows up again.
func TestFreshCloneOfPoolGetsQueryRowPerServer(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	writer := openPoolStore(t)
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapQueryStats(poolSyncQuery("sh", "qa", t0, bootA, true)),
		history.WrapQueryStats(poolSyncQuery("sh", "qb", t0.Add(15*time.Minute), bootB, true)),
	)

	reader := freshReader(t, writer, k)
	got, err := reader.List(ctx, k, history.QueryKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("got %d query rows, want one per server: %+v", len(got), got)
	}
}

// Only the newest row per server travels: a timer pull must not drag the
// writer's whole series along.
func TestPoolPushCarriesNewestRowPerServerOnly(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	writer := openPoolStore(t)
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a1", t0, bootA, 10, true)),
		history.WrapActivity(poolSyncActivity("sh", "b1", t0.Add(time.Hour), bootB, 0, true)),
		history.WrapActivity(poolSyncActivity("sh", "a2", t0.Add(2*time.Hour), bootA, 20, true)),
		history.WrapActivity(poolSyncActivity("sh", "b2", t0.Add(3*time.Hour), bootB, 0, true)),
	)

	reader := freshReader(t, writer, k)
	got, err := reader.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]bool{}
	for _, s := range got {
		hashes[s.ContentHash] = true
	}
	if len(got) != 2 || !hashes["a2"] || !hashes["b2"] {
		t.Errorf("reader rows = %v, want a2 and b2", hashes)
	}
}

// A label that never declared a pool keeps today's newest-per-label behaviour.
func TestUnmarkedLabelStillSyncsNewestRowOnly(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	bootOld, bootNew := t0.Add(-72*time.Hour), t0.Add(-time.Hour)

	writer := openPoolStore(t)
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "before-restart", t0, bootOld, 40, false)),
		history.WrapActivity(poolSyncActivity("sh", "after-restart", t0.Add(time.Hour), bootNew, 0, false)),
	)

	reader := freshReader(t, writer, k)
	got, err := reader.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ContentHash != "after-restart" {
		t.Errorf("got %+v, want only after-restart", got)
	}
}

func TestCaptureMarksRowsOfDeclaredPool(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	boot := t0.Add(-72 * time.Hour)

	cases := []struct {
		name string
		tgt  captureTarget
		opts captureRunOptions
		want bool
	}{
		{"pool = true", captureTarget{Label: "read-pool", Pool: true}, captureRunOptions{}, true},
		{"--allow-rotation", captureTarget{Label: "read-pool"}, captureRunOptions{AllowRotation: true}, true},
		{"plain node", captureTarget{Label: "read-pool"}, captureRunOptions{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := openPoolStore(t)
			putPoolRows(t, s, k, history.WrapSchema(poolSyncSchema("sh", t0)))
			reads := []streamRead{
				{stream: "activity", activity: poolSyncActivity("sh", "a1", t0, boot, 1, false)},
				{stream: "query", query: poolSyncQuery("sh", "q1", t0, boot, false)},
			}
			for _, r := range reads {
				if _, err := persistStream(ctx, s, k, c.tgt, r, nil, c.opts); err != nil {
					t.Fatalf("persist %s: %v", r.stream, err)
				}
			}
			a, err := s.Get(ctx, k, history.ActivityKind("read-pool"), history.NewRefLatest())
			if err != nil {
				t.Fatal(err)
			}
			q, err := s.Get(ctx, k, history.QueryKind("read-pool"), history.NewRefLatest())
			if err != nil {
				t.Fatal(err)
			}
			if a.Node().Pool != c.want || q.Node().Pool != c.want {
				t.Errorf("activity pool = %v, query pool = %v, want %v", a.Node().Pool, q.Node().Pool, c.want)
			}
		})
	}
}

func TestNodeWarningsSkipRotationOnDeclaredPool(t *testing.T) {
	label := func(pool bool) string {
		return captureStderr(t, func() {
			printNodeWarnings([]history.NodeSummary{{
				Label: "read-pool", Role: history.NodeRoleStandby, Members: 2, Oscillating: true, Pool: pool,
			}})
		})
	}
	if out := label(true); strings.Contains(out, "alternating") {
		t.Errorf("declared pool warned about rotation: %s", out)
	}
	if out := label(false); !strings.Contains(out, "alternating") {
		t.Errorf("undeclared rotation must still warn: %q", out)
	}
}

func poolPush(t *testing.T, writer *history.Store, remote history.SnapshotStore, k history.SnapshotKey, scope pullScope) {
	t.Helper()
	if _, err := syncKeys(context.Background(), writer, remote, []history.SnapshotKey{k}, scope); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func listHashes(t *testing.T, s history.SnapshotStore, k history.SnapshotKey) map[string]bool {
	t.Helper()
	list, err := s.List(context.Background(), k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, l := range list {
		out[l.ContentHash] = true
	}
	return out
}

// Turning pool = true on for a label with history: old unmarked rows must not
// overwrite the remote or evict servers, on any push.
func TestEnablingPoolOnLabelWithHistory(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-6 * time.Hour)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-24*time.Hour)

	writer := openPoolStore(t)
	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "u1", t0, bootA, 1, false)),
		history.WrapActivity(poolSyncActivity("sh", "u2", t0.Add(time.Hour), bootB, 2, false)),
	)
	poolPush(t, writer, remote, k, pullScope{latest: true})

	putPoolRows(t, writer, k, history.WrapActivity(poolSyncActivity("sh", "m3", t0.Add(2*time.Hour), bootC, 3, true)))
	poolPush(t, writer, remote, k, pullScope{latest: true})
	if got := listHashes(t, remote, k); !got["m3"] || !got["u2"] {
		t.Fatalf("after first marked push: %v, want m3 and u2", got)
	}

	putPoolRows(t, writer, k, history.WrapActivity(poolSyncActivity("sh", "m4", t0.Add(3*time.Hour), bootC, 4, true)))
	poolPush(t, writer, remote, k, pullScope{latest: true})
	if got := listHashes(t, remote, k); !got["m4"] || !got["u2"] {
		t.Fatalf("after second marked push: %v, want m4 and u2", got)
	}
}

// --full replays old rows; the bundle must keep its newest row and its servers.
func TestFullPushOfOldRowsKeepsPoolBundle(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-6 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	writer := openPoolStore(t)
	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "old", t0, bootA, 1, false)),
		history.WrapActivity(poolSyncActivity("sh", "ma", t0.Add(time.Hour), bootA, 2, true)),
		history.WrapActivity(poolSyncActivity("sh", "mb", t0.Add(2*time.Hour), bootB, 3, true)),
	)
	poolPush(t, writer, remote, k, pullScope{latest: true})
	poolPush(t, writer, remote, k, pullScope{})

	got := listHashes(t, remote, k)
	if !got["ma"] || !got["mb"] {
		t.Errorf("rows = %v, want ma and mb kept", got)
	}
	latest, err := remote.Latest(ctx, k, history.ActivityKind("read-pool"))
	if err != nil || latest == nil || latest.ContentHash != "mb" {
		t.Errorf("Latest = %+v, %v; want mb", latest, err)
	}
}

// A long-lived store: server A answered long ago, more than a fingerprint window
// of B's rows sit on top of it, and its row is still inside the member age limit.
func TestLongLivedStoreStillAddsUpQuietServer(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-48 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := openPoolStore(t)
	putPoolRows(t, s, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a", t0, bootA, 40, true)),
	)
	for i := range history.NodeFingerprintWindow + 5 {
		ts := t0.Add(time.Duration(i+1) * time.Hour)
		putPoolRows(t, s, k, history.WrapActivity(poolSyncActivity("sh", "b"+ts.Format("150405"), ts, bootB, 0, true)))
	}
	if unusedIndexReported(t, s, k) {
		t.Error("server A's row is within the age limit but its scans were not added up")
	}
}

// Writer, remote and reader over several cycles, servers answering in turn.
func TestPoolConvergesOverRepeatedPushAndPull(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-12 * time.Hour)
	boots := []time.Time{t0.Add(-72 * time.Hour), t0.Add(-48 * time.Hour), t0.Add(-24 * time.Hour)}

	writer, reader := openPoolStore(t), openPoolStore(t)
	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, writer, k, history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))))
	for i := range 9 {
		srv := i % 3
		// the index is only used on server 0
		scans := int64(0)
		if srv == 0 {
			scans = int64(i + 1)
		}
		hash := string(rune('a'+srv)) + string(rune('0'+i))
		putPoolRows(t, writer, k, history.WrapActivity(poolSyncActivity("sh", hash, t0.Add(time.Duration(i)*time.Hour), boots[srv], scans, true)))
		poolPush(t, writer, remote, k, pullScope{latest: true})
		if _, err := syncKeys(context.Background(), remote, reader, []history.SnapshotKey{k}, pullScope{latest: true}); err != nil {
			t.Fatalf("pull: %v", err)
		}
		if got := listHashes(t, remote, k); i >= 2 && len(got) != 3 {
			t.Errorf("cycle %d: remote holds %v, want one row per server", i, got)
		}
		if unusedIndexReported(t, reader, k) {
			t.Errorf("cycle %d: reader reported an index used on server 0 as unused", i)
		}
	}
}

// Removing pool = true: the newest row decides, so a later restart is a restart.
func TestRemovingPoolFlagEndsPoolReading(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-6 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := openPoolStore(t)
	putPoolRows(t, s, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a", t0, bootA, 1, true)),
		history.WrapActivity(poolSyncActivity("sh", "b", t0.Add(time.Hour), bootB, 1, false)),
	)
	pool, err := s.IsPool(ctx, k, "read-pool", t0.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if pool {
		t.Error("newest row is unmarked: this is a restart, not a pool")
	}
	nodes, err := s.ListNodes(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Pool {
		t.Errorf("nodes = %+v, want Pool false", nodes)
	}
}

func TestListNodesReportsDeclaredPool(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-6 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	s := openPoolStore(t)
	putPoolRows(t, s, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a", t0, bootA, 1, true)),
		history.WrapActivity(poolSyncActivity("sh", "b", t0.Add(time.Hour), bootB, 1, true)),
	)
	nodes, err := s.ListNodes(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || !nodes[0].Pool || !nodes[0].Oscillating || nodes[0].Members != 2 {
		t.Errorf("nodes = %+v, want a pool of 2", nodes)
	}
}

type countingStore struct {
	history.SnapshotStore
	memberCalls int
}

func (c *countingStore) ListPoolMembers(ctx context.Context, key history.SnapshotKey, kind history.SnapshotKind, schemaRef string) ([]history.SnapshotSummary, error) {
	c.memberCalls++
	return c.SnapshotStore.ListPoolMembers(ctx, key, kind, schemaRef)
}

// On a registry each members lookup fetches a whole bundle: a label with one
// row under its schema must not pay for it.
func TestPullOfPlainLabelSkipsMemberLookup(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, remote, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a", t0, t0.Add(-72*time.Hour), 1, false)),
	)
	c := &countingStore{SnapshotStore: remote}
	if _, err := selectSnapshots(ctx, c, k, pullScope{latest: true}); err != nil {
		t.Fatal(err)
	}
	if c.memberCalls != 0 {
		t.Errorf("ListPoolMembers called %d times, want 0", c.memberCalls)
	}
}

func TestPullSinceFiltersPoolMembers(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-48 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, remote, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a", t0, bootA, 1, true)),
		history.WrapActivity(poolSyncActivity("sh", "b0", t0.Add(24*time.Hour), bootB, 1, true)),
		history.WrapActivity(poolSyncActivity("sh", "c", t0.Add(40*time.Hour), t0.Add(-time.Hour), 1, true)),
	)
	from := t0.Add(12 * time.Hour)
	sel, err := selectSnapshots(ctx, remote, k, pullScope{latest: true, rng: history.TimeRange{From: &from}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, s := range sel[history.KindActivity] {
		got[s.ContentHash] = true
	}
	if len(got) != 2 || !got["b0"] || !got["c"] {
		t.Errorf("selected %v, want b0 and c", got)
	}
}

// The bundle keeps the old head when pool = true is turned on, and the reader
// counts it even though it is unmarked: scans must not vanish in the migration.
func TestEnablingPoolKeepsOldHeadsScansOnReader(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-6 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	writer, reader := openPoolStore(t), openPoolStore(t)
	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "u", t0, bootA, 40, false)),
	)
	poolPush(t, writer, remote, k, pullScope{latest: true})
	putPoolRows(t, writer, k, history.WrapActivity(poolSyncActivity("sh", "m", t0.Add(time.Hour), bootB, 0, true)))
	poolPush(t, writer, remote, k, pullScope{latest: true})

	if _, err := syncKeys(ctx, remote, reader, []history.SnapshotKey{k}, pullScope{latest: true}); err != nil {
		t.Fatalf("pull: %v", err)
	}
	if unusedIndexReported(t, reader, k) {
		t.Error("the reader lost the scans of the row that was the head before pool = true")
	}
}

// Documented: an unmarked capture clears the remote's servers, and the next
// marked capture and push puts them back.
func TestUnmarkedCaptureClearsMembersUntilNextMarkedPush(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-6 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	writer := openPoolStore(t)
	remote, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	putPoolRows(t, writer, k,
		history.WrapSchema(poolSyncSchema("sh", t0.Add(-time.Hour))),
		history.WrapActivity(poolSyncActivity("sh", "a", t0, bootA, 40, true)),
		history.WrapActivity(poolSyncActivity("sh", "b", t0.Add(time.Hour), bootB, 0, true)),
	)
	poolPush(t, writer, remote, k, pullScope{latest: true})

	putPoolRows(t, writer, k, history.WrapActivity(poolSyncActivity("sh", "unmarked", t0.Add(2*time.Hour), bootB, 1, false)))
	poolPush(t, writer, remote, k, pullScope{latest: true})
	if got := listHashes(t, remote, k); len(got) != 1 || !got["unmarked"] {
		t.Fatalf("after unmarked push: %v, want only unmarked", got)
	}

	putPoolRows(t, writer, k, history.WrapActivity(poolSyncActivity("sh", "b2", t0.Add(3*time.Hour), bootB, 2, true)))
	poolPush(t, writer, remote, k, pullScope{latest: true})
	if got := listHashes(t, remote, k); !got["a"] || !got["b2"] {
		t.Errorf("after marked push: %v, want a back alongside b2", got)
	}
}
