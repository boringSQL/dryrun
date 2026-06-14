package history

import (
	"context"
	"errors"
	"testing"
)

// ResolveKind is what lets `snapshot diff` enforce its same-kind guard: given a
// content-hash prefix, it has to say which of the three snapshot tables
// (schema / planner / activity) that hash was stored in, because the three live
// in separate tables keyed only by content hash. This test seeds one snapshot of
// each kind under a single key (the planner and activity rows reference the
// schema's hash as their schema_ref, mirroring how a real capture fans out) and
// then drives the resolver through the cases that matter.
func TestResolveKind(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("schemahash01", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(schemaSnap)); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	plannerSnap := plannerFixture(schemaSnap.ContentHash, "plannerhash01", "appdb")
	if _, err := store.Put(ctx, k, WrapPlanner(plannerSnap)); err != nil {
		t.Fatalf("put planner: %v", err)
	}
	activitySnap := activityFixture(schemaSnap.ContentHash, "activityhash01", "primary", false)
	if _, err := store.Put(ctx, k, WrapActivity(activitySnap)); err != nil {
		t.Fatalf("put activity: %v", err)
	}

	// Each kind must resolve from its full hash, and (the "schema prefix" case)
	// a git-style truncated prefix has to work too, since the CLI lets users pass
	// short hashes to `diff`.
	cases := []struct {
		name   string
		prefix string
		want   SnapshotKindTag
	}{
		{"schema full", "schemahash01", KindSchema},
		{"schema prefix", "schemah", KindSchema},
		{"planner", "plannerhash01", KindPlanner},
		{"activity", "activityhash01", KindActivity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ResolveKind(ctx, k, tc.prefix)
			if err != nil {
				t.Fatalf("ResolveKind(%q): %v", tc.prefix, err)
			}
			if got.Tag != tc.want {
				t.Errorf("ResolveKind(%q) = %v, want tag %v", tc.prefix, got, tc.want)
			}
		})
	}

	// A prefix that matches nothing must come back as the well-known
	// ErrSnapshotNotFound sentinel (not some opaque error), so the diff command
	// can report "no such snapshot" cleanly instead of leaking a SQL error.
	t.Run("not found", func(t *testing.T) {
		_, err := store.ResolveKind(ctx, k, "deadbeef")
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Errorf("expected ErrSnapshotNotFound, got %v", err)
		}
	})

	// Activity snapshots are per-node, so resolving one must also recover the node
	// label off the row (here "primary") rather than just the bare kind tag; the
	// caller needs the label to load the right activity series back out.
	t.Run("activity carries node label", func(t *testing.T) {
		got, err := store.ResolveKind(ctx, k, "activityhash01")
		if err != nil {
			t.Fatal(err)
		}
		if got.NodeLabel != "primary" {
			t.Errorf("expected node label 'primary', got %q", got.NodeLabel)
		}
	})
}
