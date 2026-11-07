package history

import (
	"context"
	"errors"
	"testing"
	"time"
)

// countingBackend wraps a real backend and counts the transport calls, so the
// tests can pin how often a sync really goes to disk / registry / bucket.
type countingBackend struct {
	bundleBackend
	lists, saves int
	listErr      error // returned once, then cleared
	saveErr      error
}

func (c *countingBackend) list(ctx context.Context, key SnapshotKey) ([]*Bundle, error) {
	c.lists++
	if err := c.listErr; err != nil {
		c.listErr = nil
		return nil, err
	}
	return c.bundleBackend.list(ctx, key)
}

func (c *countingBackend) save(ctx context.Context, key SnapshotKey, b *Bundle) error {
	c.saves++
	if c.saveErr != nil {
		// write first, then fail: a partial write must still drop the cache
		if err := c.bundleBackend.save(ctx, key, b); err != nil {
			return err
		}
		return c.saveErr
	}
	return c.bundleBackend.save(ctx, key, b)
}

func countingFsStore(t *testing.T) (*bundleStore, *countingBackend) {
	t.Helper()
	cb := &countingBackend{bundleBackend: &fsBackend{root: t.TempDir()}}
	return &bundleStore{backend: cb}, cb
}

// seedBundles writes n schema versions, each with planner, activity and query
// rows, an hour apart so the filenames and newest-first order are stable.
func seedBundles(t *testing.T, s *bundleStore, k SnapshotKey, n int) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Duration(n) * time.Hour)
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * time.Hour)
		sh := "sh-" + string(rune('a'+i))
		snap := testSnapshot(sh, "appdb")
		snap.Timestamp = ts
		p := plannerFixture(sh, "pl-"+sh, "appdb")
		p.Timestamp = ts
		a := activityFixture(sh, "ac-"+sh, "primary", false)
		a.Node.Timestamp = ts
		q := queryStatsFixture(sh, "qs-"+sh, "primary")
		q.Node.Timestamp = ts
		for _, put := range []StoredSnapshot{WrapSchema(snap), WrapPlanner(p), WrapActivity(a), WrapQueryStats(q)} {
			if _, err := s.Put(ctx, k, put); err != nil {
				t.Fatalf("seed put %s: %v", put.Kind(), err)
			}
		}
	}
}

// A pull reads the source as selectSnapshots + syncKindList do: one List per
// kind, then a Get per copied row. Before the cache every one of those calls
// re-listed (and decoded) every bundle of the key.
func TestBundleStoreListsOncePerKeyAcrossReads(t *testing.T) {
	s, cb := countingFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	seedBundles(t, s, k, 5)
	// schema put + one merge each for planner, activity and query
	if cb.saves != 20 {
		t.Fatalf("seed saves = %d, want 20", cb.saves)
	}
	cb.lists = 0

	var gets int
	for _, kind := range []SnapshotKind{PlannerKind(), ActivityKind(""), QueryKind(""), SchemaKind()} {
		list, err := s.List(ctx, k, kind, TimeRange{})
		if err != nil {
			t.Fatalf("list %s: %v", kind, err)
		}
		if len(list) != 5 {
			t.Fatalf("list %s: got %d rows, want 5", kind, len(list))
		}
		for _, sum := range list {
			got, err := s.Get(ctx, k, sum.Kind, NewRefHash(sum.ContentHash))
			if err != nil {
				t.Fatalf("get %s %s: %v", sum.Kind, sum.ContentHash, err)
			}
			if got.ContentHash() != sum.ContentHash {
				t.Fatalf("get %s: got hash %s, want %s", sum.Kind, got.ContentHash(), sum.ContentHash)
			}
			gets++
		}
	}
	if _, err := s.ListKinds(ctx, k); err != nil {
		t.Fatalf("list kinds: %v", err)
	}
	if _, err := s.Latest(ctx, k, SchemaKind()); err != nil {
		t.Fatalf("latest: %v", err)
	}

	if gets != 20 {
		t.Fatalf("gets = %d, want 20", gets)
	}
	if cb.lists != 1 {
		t.Fatalf("backend.list called %d times for one key, want 1", cb.lists)
	}

	// the cache is per key: a second key is its own list
	if _, err := s.List(ctx, key("acme", "other"), SchemaKind(), TimeRange{}); err != nil {
		t.Fatalf("list other key: %v", err)
	}
	if cb.lists != 2 {
		t.Fatalf("backend.list = %d after a second key, want 2", cb.lists)
	}
}

// A push writes into a bundle store between its reads: each merge rewrites a
// bundle under the same name, so a read after a save must see the new rows.
func TestBundleStoreSaveDropsCache(t *testing.T) {
	s, cb := countingFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	if _, err := s.Put(ctx, k, WrapSchema(testSnapshot("sh-1", "appdb"))); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	if list, err := s.List(ctx, k, PlannerKind(), TimeRange{}); err != nil || len(list) != 0 {
		t.Fatalf("planner before merge: %v rows, err %v", len(list), err)
	}

	if _, err := s.Put(ctx, k, WrapPlanner(plannerFixture("sh-1", "pl-1", "appdb"))); err != nil {
		t.Fatalf("put planner: %v", err)
	}
	list, err := s.List(ctx, k, PlannerKind(), TimeRange{})
	if err != nil {
		t.Fatalf("list planner: %v", err)
	}
	if len(list) != 1 || list[0].ContentHash != "pl-1" {
		t.Fatalf("planner after merge = %+v, want pl-1 (stale cache)", list)
	}

	if _, err := s.Put(ctx, k, WrapActivity(activityFixture("sh-1", "ac-1", "primary", false))); err != nil {
		t.Fatalf("put activity: %v", err)
	}
	got, err := s.Get(ctx, k, ActivityKind("primary"), NewRefLatest())
	if err != nil {
		t.Fatalf("get activity after merge: %v", err)
	}
	if got.ContentHash() != "ac-1" {
		t.Fatalf("activity after merge = %s, want ac-1", got.ContentHash())
	}

	// a dedup writes nothing, so the cached list stays valid
	listsBefore := cb.lists
	if out, err := s.Put(ctx, k, WrapPlanner(plannerFixture("sh-1", "pl-1", "appdb"))); err != nil || out != PutDeduped {
		t.Fatalf("re-put planner: outcome %v, err %v", out, err)
	}
	if _, err := s.List(ctx, k, PlannerKind(), TimeRange{}); err != nil {
		t.Fatalf("list after dedup: %v", err)
	}
	if cb.lists != listsBefore {
		t.Fatalf("a deduped put dropped the cache: lists %d -> %d", listsBefore, cb.lists)
	}
}

func TestBundleStoreFailedSaveDropsCache(t *testing.T) {
	s, cb := countingFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	if _, err := s.Put(ctx, k, WrapSchema(testSnapshot("sh-1", "appdb"))); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	if _, err := s.List(ctx, k, PlannerKind(), TimeRange{}); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	cb.saveErr = errors.New("connection reset after upload")
	if _, err := s.Put(ctx, k, WrapPlanner(plannerFixture("sh-1", "pl-1", "appdb"))); err == nil {
		t.Fatal("put planner: want the save error")
	}
	cb.saveErr = nil

	list, err := s.List(ctx, k, PlannerKind(), TimeRange{})
	if err != nil {
		t.Fatalf("list planner: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("planner rows = %d, want the partially written row to be visible", len(list))
	}
}

// a transient backend error must not stick for the rest of the command
func TestBundleStoreDoesNotCacheListError(t *testing.T) {
	s, cb := countingFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	seedBundles(t, s, k, 2)

	cb.listErr = errors.New("registry 503")
	if _, err := s.List(ctx, k, SchemaKind(), TimeRange{}); err == nil {
		t.Fatal("list: want the backend error")
	}
	list, err := s.List(ctx, k, SchemaKind(), TimeRange{})
	if err != nil {
		t.Fatalf("list after error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list after error = %d rows, want 2", len(list))
	}
}
