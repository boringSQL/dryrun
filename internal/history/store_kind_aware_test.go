package history

import (
	"context"
	"testing"
	"time"
)

// TestStoredSnapshotRoundTrip drives a StoredSnapshot of each variant
// (Schema, Planner, Activity) through the generic Put -> Get -> List path
// on the SQLite Store. This is the contract the V2 FilesystemStore will
// implement against the same interface, so kind dispatch must be lossless:
// what goes in via Put(WrapX) must come back out via Get(kind) and surface
// in List(kind) with the right ContentHash and SchemaRefHash.
func TestStoredSnapshotRoundTrip(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("schema-hash-1", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(schemaSnap)); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	plannerSnap := plannerFixture(schemaSnap.ContentHash, "planner-hash-1", "appdb")
	if _, err := store.Put(ctx, k, WrapPlanner(plannerSnap)); err != nil {
		t.Fatalf("put planner: %v", err)
	}
	activitySnap := activityFixture(schemaSnap.ContentHash, "activity-hash-1", "primary", false)
	if _, err := store.Put(ctx, k, WrapActivity(activitySnap)); err != nil {
		t.Fatalf("put activity: %v", err)
	}

	t.Run("Get_Schema", func(t *testing.T) {
		got, err := store.Get(ctx, k, SchemaKind(), NewRefHash("schema-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsSchema() == nil || got.AsSchema().ContentHash != "schema-hash-1" {
			t.Errorf("got %+v, want schema-hash-1", got.AsSchema())
		}
	})

	t.Run("Get_Planner", func(t *testing.T) {
		got, err := store.Get(ctx, k, PlannerKind(), NewRefHash("planner-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsPlanner() == nil || got.AsPlanner().ContentHash != "planner-hash-1" {
			t.Errorf("got %+v, want planner-hash-1", got.AsPlanner())
		}
		if got.SchemaRefHash() != schemaSnap.ContentHash {
			t.Errorf("schema_ref_hash mismatch: got %q, want %q", got.SchemaRefHash(), schemaSnap.ContentHash)
		}
	})

	t.Run("Get_Activity_ByNodeLabel", func(t *testing.T) {
		got, err := store.Get(ctx, k, ActivityKind("primary"), NewRefHash("activity-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsActivity() == nil || got.AsActivity().ContentHash != "activity-hash-1" {
			t.Errorf("got %+v, want activity-hash-1", got.AsActivity())
		}
		if got.Kind().NodeLabel != "primary" {
			t.Errorf("node label: got %q, want primary", got.Kind().NodeLabel)
		}
	})

	t.Run("List_per_kind", func(t *testing.T) {
		sl, err := store.List(ctx, k, SchemaKind(), TimeRange{})
		if err != nil || len(sl) != 1 || sl[0].ContentHash != "schema-hash-1" {
			t.Errorf("schema list: got %+v err=%v", sl, err)
		}
		pl, err := store.List(ctx, k, PlannerKind(), TimeRange{})
		if err != nil || len(pl) != 1 || pl[0].ContentHash != "planner-hash-1" {
			t.Errorf("planner list: got %+v err=%v", pl, err)
		}
		al, err := store.List(ctx, k, ActivityKind(""), TimeRange{})
		if err != nil || len(al) != 1 || al[0].ContentHash != "activity-hash-1" || al[0].NodeLabel != "primary" {
			t.Errorf("activity list: got %+v err=%v", al, err)
		}
	})
}

// TestListKindsReportsPopulatedSubset seeds only schema + one activity node
// (no planner row), then asserts ListKinds returns exactly those kinds and
// that activity entries carry the node label. The omission of planner is
// the discriminating case: a partially-populated key must not advertise the
// empty stream.
func TestListKindsReportsPopulatedSubset(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("sh-1", "appdb")
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}
	a := activityFixture(schemaSnap.ContentHash, "ac-1", "replica-a", true)
	if _, err := store.PutActivity(ctx, k, a); err != nil {
		t.Fatal(err)
	}

	kinds, err := store.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 2 {
		t.Fatalf("got %d kinds (%+v), want 2 (schema, activity:replica-a)", len(kinds), kinds)
	}
	if kinds[0].Tag != KindSchema {
		t.Errorf("kinds[0] = %v, want schema", kinds[0])
	}
	if kinds[1].Tag != KindActivity || kinds[1].NodeLabel != "replica-a" {
		t.Errorf("kinds[1] = %v, want activity:replica-a", kinds[1])
	}
}

// TestListKindsActivityMultiNode confirms each distinct node_source surfaces
// as its own ActivityKind entry, matching the bundle-by-node semantics V2's
// FilesystemStore must preserve.
func TestListKindsActivityMultiNode(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("sh-1", "appdb")
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}
	for _, src := range []string{"replica-b", "replica-a", "primary"} {
		a := activityFixture(schemaSnap.ContentHash, "ac-"+src, src, src != "primary")
		if _, err := store.PutActivity(ctx, k, a); err != nil {
			t.Fatal(err)
		}
	}

	kinds, err := store.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	var labels []string
	for _, kk := range kinds {
		if kk.Tag == KindActivity {
			labels = append(labels, kk.NodeLabel)
		}
	}
	want := []string{"primary", "replica-a", "replica-b"}
	if len(labels) != len(want) {
		t.Fatalf("activity labels: got %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Errorf("labels[%d]: got %q, want %q", i, labels[i], want[i])
		}
	}
}

// TestLatestPicksPerKind: with planner and activity rows that are newer than
// the schema row, Latest(SchemaKind) must still return the schema timestamp,
// not the most-recent-across-kinds. Kind dispatch on Latest must isolate
// streams; otherwise V3's per-kind sync diff would compare apples to oranges.
func TestLatestPicksPerKind(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	schemaSnap := testSnapshot("sh-old", "appdb")
	schemaSnap.Timestamp = now.Add(-2 * time.Hour)
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}

	planner := plannerFixture(schemaSnap.ContentHash, "pl-newer", "appdb")
	planner.Timestamp = now.Add(-30 * time.Minute)
	if _, err := store.PutPlanner(ctx, k, planner); err != nil {
		t.Fatal(err)
	}

	activity := activityFixture(schemaSnap.ContentHash, "ac-newest", "primary", false)
	activity.Node.Timestamp = now
	if _, err := store.PutActivity(ctx, k, activity); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		kind     SnapshotKind
		wantHash string
	}{
		{"schema", SchemaKind(), "sh-old"},
		{"planner", PlannerKind(), "pl-newer"},
		{"activity", ActivityKind("primary"), "ac-newest"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := store.Latest(ctx, k, c.kind)
			if err != nil || got == nil {
				t.Fatalf("got (%+v, %v)", got, err)
			}
			if got.ContentHash != c.wantHash {
				t.Errorf("got %q, want %q", got.ContentHash, c.wantHash)
			}
		})
	}
}

// TestDeleteBeforePerKindIsolated: DeleteBefore on planner must not affect
// schema or activity rows. The retention path in V3 will iterate per kind,
// so cross-kind cascade would silently prune unrelated streams.
func TestDeleteBeforePerKindIsolated(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := func(t time.Time) time.Time { return t.Add(-24 * time.Hour) }

	s := testSnapshot("sh-1", "appdb")
	s.Timestamp = old(now)
	if _, err := store.PutSchema(ctx, k, s); err != nil {
		t.Fatal(err)
	}
	p := plannerFixture("sh-1", "pl-1", "appdb")
	p.Timestamp = old(now)
	if _, err := store.PutPlanner(ctx, k, p); err != nil {
		t.Fatal(err)
	}
	a := activityFixture("sh-1", "ac-1", "primary", false)
	a.Node.Timestamp = old(now)
	if _, err := store.PutActivity(ctx, k, a); err != nil {
		t.Fatal(err)
	}

	n, err := store.DeleteBefore(ctx, k, PlannerKind(), now)
	if err != nil || n != 1 {
		t.Fatalf("delete planner: n=%d err=%v", n, err)
	}

	sl, _ := store.List(ctx, k, SchemaKind(), TimeRange{})
	pl, _ := store.List(ctx, k, PlannerKind(), TimeRange{})
	al, _ := store.List(ctx, k, ActivityKind(""), TimeRange{})
	if len(sl) != 1 || len(pl) != 0 || len(al) != 1 {
		t.Errorf("after delete planner: schema=%d planner=%d activity=%d, want 1/0/1",
			len(sl), len(pl), len(al))
	}
}
