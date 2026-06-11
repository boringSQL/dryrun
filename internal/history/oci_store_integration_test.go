//go:build integration

package history

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// This is the registry-backed half of the OCIStore tests. It only runs under
// `go test -tags integration` AND when DRYRUN_TEST_REGISTRY points at a live,
// push-able OCI registry (e.g. a local `registry:2` or `zot` over plain HTTP):
//
//	docker run -d -p 5000:5000 registry:2
//	DRYRUN_TEST_REGISTRY=localhost:5000 go test -tags integration ./internal/history/
//
// Without that env var the whole file skips, so CI that hasn't provisioned a
// registry stays green. We talk to the registry anonymously over plain HTTP:
// OCIStore leaves remote.Repository.Client nil, which makes oras-go fall back
// to its default (anonymous) client, exactly what an unauthenticated local
// registry wants.

// newIntegrationStore builds an OCIStore against the test registry, rooted at a
// per-run-unique repo prefix so repeated runs (and parallel packages) never
// collide on tags. The uniqueness comes from a nanosecond stamp baked into the
// project segment of every key's stream — see uniqueKey below.
func newIntegrationStore(t *testing.T) *OCIStore {
	t.Helper()
	addr := os.Getenv("DRYRUN_TEST_REGISTRY")
	if addr == "" {
		t.Skip("DRYRUN_TEST_REGISTRY not set; skipping OCI registry integration test")
	}
	store, err := NewOCIStore(OCIConfig{
		Base:      addr + "/dryrun-test",
		PlainHTTP: true, // local registry:2 / zot listen on http, not https
	})
	if err != nil {
		t.Fatalf("NewOCIStore: %v", err)
	}
	return store
}

// uniqueKey namespaces a (project, database) pair under a per-run stamp so the
// registry repo path is fresh every run. Reusing a path across runs would let a
// previous run's tags leak into this run's List/Latest assertions.
func uniqueKey(stamp int64, project, database string) SnapshotKey {
	return key(fmt.Sprintf("run-%d-%s", stamp, project), database)
}

// TestOCIStoreConformance drives the same put/get/list/latest/delete/dedup
// contract the SQLite and filesystem stores are held to, but against a real
// registry. If OCIStore's tag scheme, manifest annotations, or merge logic
// diverge from the shared SnapshotStore semantics, one of these sub-tests
// breaks.
func TestOCIStoreConformance(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	stamp := time.Now().UnixNano()

	t.Run("PutGetSchema", func(t *testing.T) {
		k := uniqueKey(stamp, "acme", "primary")
		s := testSnapshot("sh-1", "appdb")
		if o, err := store.Put(ctx, k, WrapSchema(s)); err != nil || o != PutInserted {
			t.Fatalf("put schema: got (%v, %v), want PutInserted", o, err)
		}
		got, err := store.Get(ctx, k, SchemaKind(), NewRefHash("sh-1"))
		if err != nil || got.AsSchema() == nil || got.AsSchema().ContentHash != "sh-1" {
			t.Fatalf("get schema: got %+v err=%v", got.AsSchema(), err)
		}
	})

	t.Run("SchemaDedup", func(t *testing.T) {
		k := uniqueKey(stamp, "dedup", "primary")
		s := testSnapshot("dup-hash", "appdb")
		if o, err := store.Put(ctx, k, WrapSchema(s)); err != nil || o != PutInserted {
			t.Fatalf("first put: got (%v, %v)", o, err)
		}
		// Same content hash a second time must short-circuit to PutDeduped — the
		// version tag already resolves, so no new manifest is pushed.
		if o, err := store.Put(ctx, k, WrapSchema(s)); err != nil || o != PutDeduped {
			t.Fatalf("second put: got (%v, %v), want PutDeduped", o, err)
		}
	})

	t.Run("PlannerActivityMergeIntoBundle", func(t *testing.T) {
		k := uniqueKey(stamp, "merge", "primary")
		s := testSnapshot("sh-merge", "appdb")
		if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
			t.Fatalf("put schema: %v", err)
		}
		// planner and activity locate the schema bundle by schema_ref_hash,
		// pull-merge-repush; both must then be retrievable from the same stream.
		p := plannerFixture("sh-merge", "pl-1", "appdb")
		if _, err := store.Put(ctx, k, WrapPlanner(p)); err != nil {
			t.Fatalf("put planner: %v", err)
		}
		a := activityFixture("sh-merge", "ac-1", "primary", false)
		if _, err := store.Put(ctx, k, WrapActivity(a)); err != nil {
			t.Fatalf("put activity: %v", err)
		}

		gotP, err := store.Get(ctx, k, PlannerKind(), NewRefHash("pl-1"))
		if err != nil || gotP.AsPlanner().ContentHash != "pl-1" {
			t.Errorf("get planner: got %+v err=%v", gotP.AsPlanner(), err)
		}
		gotA, err := store.Get(ctx, k, ActivityKind("primary"), NewRefHash("ac-1"))
		if err != nil || gotA.AsActivity().ContentHash != "ac-1" {
			t.Errorf("get activity: got %+v err=%v", gotA.AsActivity(), err)
		}
	})

	t.Run("OrphanPlannerRejected", func(t *testing.T) {
		k := uniqueKey(stamp, "orphan", "primary")
		// No schema bundle pushed first, so the planner has nothing to bind to.
		p := plannerFixture("no-such-schema", "pl-x", "appdb")
		if _, err := store.Put(ctx, k, WrapPlanner(p)); err == nil {
			t.Error("put orphan planner: want error, got nil")
		}
	})

	t.Run("ListLatestAndTimeRange", func(t *testing.T) {
		k := uniqueKey(stamp, "history", "primary")
		now := time.Now().UTC().Truncate(time.Second)
		mk := func(hash string, offset time.Duration) {
			s := testSnapshot(hash, "appdb")
			s.Timestamp = now.Add(offset)
			if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
				t.Fatalf("put %s: %v", hash, err)
			}
		}
		mk("h-old", -2*time.Hour)
		mk("h-mid", -1*time.Hour)
		mk("h-new", 0)

		list, err := store.List(ctx, k, SchemaKind(), TimeRange{})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(list) != 3 {
			t.Fatalf("list len: got %d, want 3", len(list))
		}
		// List is newest-first, same as every other backend.
		if list[0].ContentHash != "h-new" {
			t.Errorf("list[0]: got %q, want h-new", list[0].ContentHash)
		}

		latest, err := store.Latest(ctx, k, SchemaKind())
		if err != nil || latest == nil || latest.ContentHash != "h-new" {
			t.Fatalf("latest: got %+v err=%v, want h-new", latest, err)
		}

		// half-open window [mid-ε, new): should surface exactly h-mid.
		from := now.Add(-time.Hour - time.Minute)
		to := now.Add(-30 * time.Minute)
		windowed, err := store.List(ctx, k, SchemaKind(), TimeRange{From: &from, To: &to})
		if err != nil {
			t.Fatalf("windowed list: %v", err)
		}
		if len(windowed) != 1 || windowed[0].ContentHash != "h-mid" {
			t.Errorf("windowed list: got %+v, want [h-mid]", windowed)
		}
	})

	t.Run("DeleteBeforeCutoff", func(t *testing.T) {
		k := uniqueKey(stamp, "retention", "primary")
		now := time.Now().UTC().Truncate(time.Second)
		old := testSnapshot("h-old", "appdb")
		old.Timestamp = now.Add(-24 * time.Hour)
		fresh := testSnapshot("h-new", "appdb")
		fresh.Timestamp = now
		if _, err := store.Put(ctx, k, WrapSchema(old)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(ctx, k, WrapSchema(fresh)); err != nil {
			t.Fatal(err)
		}

		deleted, err := store.DeleteBefore(ctx, k, SchemaKind(), now.Add(-time.Hour))
		if err != nil {
			t.Fatalf("delete before: %v", err)
		}
		if deleted != 1 {
			t.Errorf("deleted: got %d, want 1", deleted)
		}
		list, err := store.List(ctx, k, SchemaKind(), TimeRange{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].ContentHash != "h-new" {
			t.Errorf("survivors: got %+v, want [h-new]", list)
		}
	})

	t.Run("KeyIsolation", func(t *testing.T) {
		// Identical content under two different streams must not dedup against
		// each other — each stream is its own repository.
		k1 := uniqueKey(stamp, "iso", "primary")
		k2 := uniqueKey(stamp, "iso", "replica")
		s := testSnapshot("same-hash", "appdb")
		if _, err := store.Put(ctx, k1, WrapSchema(s)); err != nil {
			t.Fatal(err)
		}
		if o, err := store.Put(ctx, k2, WrapSchema(s)); err != nil || o != PutInserted {
			t.Fatalf("put under second key: got (%v, %v), want PutInserted", o, err)
		}
		for _, k := range []SnapshotKey{k1, k2} {
			list, err := store.List(ctx, k, SchemaKind(), TimeRange{})
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 {
				t.Errorf("key %+v: got %d rows, want 1", k, len(list))
			}
		}
	})
}

// TestOCIStoreCrossStoreParity is the round-trip that proves the OCI backend is
// wire-compatible with the filesystem one: seed a FilesystemStore, sync each
// kind into OCIStore, sync back into a second FilesystemStore, and assert the
// content hashes survive unchanged. content_hash is the identity of a snapshot,
// so equal hashes end-to-end means the bytes never drifted in transit.
func TestOCIStoreCrossStoreParity(t *testing.T) {
	oci := newIntegrationStore(t)
	ctx := context.Background()
	stamp := time.Now().UnixNano()
	k := uniqueKey(stamp, "parity", "primary")

	src, _ := testFsStore(t)
	dst, _ := testFsStore(t)

	// Seed the source filesystem store with one of each kind, all bound to the
	// same schema_ref_hash so planner/activity have a schema to merge into.
	s := testSnapshot("sh-parity", "appdb")
	p := plannerFixture("sh-parity", "pl-parity", "appdb")
	a := activityFixture("sh-parity", "ac-parity", "primary", false)
	for _, snap := range []StoredSnapshot{WrapSchema(s), WrapPlanner(p), WrapActivity(a)} {
		if _, err := src.Put(ctx, k, snap); err != nil {
			t.Fatalf("seed src: %v", err)
		}
	}

	// Hop: filesystem -> OCI -> filesystem. Schema must lead so the merges land.
	copyKind := func(from, to SnapshotStore, kind SnapshotKind, ref SnapshotRef) {
		got, err := from.Get(ctx, k, kind, ref)
		if err != nil {
			t.Fatalf("get %v from source: %v", kind, err)
		}
		if _, err := to.Put(ctx, k, got); err != nil {
			t.Fatalf("put %v into dest: %v", kind, err)
		}
	}
	for _, hop := range []struct {
		from, to SnapshotStore
	}{
		{src, oci},
		{oci, dst},
	} {
		copyKind(hop.from, hop.to, SchemaKind(), NewRefHash("sh-parity"))
		copyKind(hop.from, hop.to, PlannerKind(), NewRefHash("pl-parity"))
		copyKind(hop.from, hop.to, ActivityKind("primary"), NewRefHash("ac-parity"))
	}

	// The hashes that came out the far end must equal what we put in.
	gotS, err := dst.Get(ctx, k, SchemaKind(), NewRefHash("sh-parity"))
	if err != nil || gotS.AsSchema().ContentHash != s.ContentHash {
		t.Errorf("schema parity: got %+v err=%v, want hash %s", gotS.AsSchema(), err, s.ContentHash)
	}
	gotP, err := dst.Get(ctx, k, PlannerKind(), NewRefHash("pl-parity"))
	if err != nil || gotP.AsPlanner().ContentHash != p.ContentHash {
		t.Errorf("planner parity: got %+v err=%v, want hash %s", gotP.AsPlanner(), err, p.ContentHash)
	}
	gotA, err := dst.Get(ctx, k, ActivityKind("primary"), NewRefHash("ac-parity"))
	if err != nil || gotA.AsActivity().ContentHash != a.ContentHash {
		t.Errorf("activity parity: got %+v err=%v, want hash %s", gotA.AsActivity(), err, a.ContentHash)
	}
}
