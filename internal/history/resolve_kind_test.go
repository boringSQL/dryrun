package history

import (
	"context"
	"errors"
	"testing"
)

// ResolveKind is what lets `snapshot diff` enforce its same-kind guard: given a
// content-hash prefix, it has to say which of the four snapshot tables
// (schema / planner / activity / query) that hash was stored in, because the
// four live in separate tables keyed only by content hash. This test seeds one
// snapshot of each kind under a single key (the planner, activity, and query
// rows all reference the schema's hash as their schema_ref, mirroring how a
// real capture fans out) and then drives the resolver through the cases that
// matter. Query joined the other three after ResolveKind already existed for
// schema/planner/activity, added as a fourth `SELECT DISTINCT node_source FROM
// query_stats ... LIKE ?` block appended to the same function — exactly the
// kind of change where forgetting to append to `matches`, or querying the
// wrong table, would compile fine and only show up here.
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
	queryStatsSnap := queryStatsFixture(schemaSnap.ContentHash, "queryhash01", "primary")
	if _, err := store.Put(ctx, k, WrapQueryStats(queryStatsSnap)); err != nil {
		t.Fatalf("put query stats: %v", err)
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
		{"query", "queryhash01", KindQuery},
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

	// Same story for query stats — it is also a per-node table, and the query
	// branch has its own node_source scan, so this is not implied by the
	// activity subtest above.
	t.Run("query carries node label", func(t *testing.T) {
		got, err := store.ResolveKind(ctx, k, "queryhash01")
		if err != nil {
			t.Fatal(err)
		}
		if got.NodeLabel != "primary" {
			t.Errorf("expected node label 'primary', got %q", got.NodeLabel)
		}
	})
}

// ResolveKindFlag is the --kind flag parser shared by `snapshot diff` and the
// MCP history tool. "activity" and "query" are per-node kinds that resolve
// through the same resolveNodeKind helper (they used to be two independent,
// near-identical functions before that helper was extracted) — so this test
// exercises both the "exactly one node, pick it automatically" path and the
// "--node was given explicitly" path for each, plus the unknown-kind error.
func TestResolveKindFlag(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("sh-1", "appdb")
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, activityFixture(schemaSnap.ContentHash, "ac-1", "primary", false)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, queryStatsFixture(schemaSnap.ContentHash, "qh-1", "primary")); err != nil {
		t.Fatal(err)
	}

	t.Run("schema and planner are position-independent of node data", func(t *testing.T) {
		got, err := store.ResolveKindFlag(ctx, k, "schema", "")
		if err != nil || got.Tag != KindSchema {
			t.Errorf("schema: got (%+v, %v)", got, err)
		}
		got, err = store.ResolveKindFlag(ctx, k, "", "") // "" defaults to schema
		if err != nil || got.Tag != KindSchema {
			t.Errorf("default (empty string): got (%+v, %v)", got, err)
		}
		got, err = store.ResolveKindFlag(ctx, k, "planner", "")
		if err != nil || got.Tag != KindPlanner {
			t.Errorf("planner: got (%+v, %v)", got, err)
		}
	})

	t.Run("activity with the sole node picked automatically", func(t *testing.T) {
		got, err := store.ResolveKindFlag(ctx, k, "activity", "")
		if err != nil {
			t.Fatal(err)
		}
		if got.Tag != KindActivity || got.NodeLabel != "primary" {
			t.Errorf("got %+v, want activity:primary", got)
		}
	})

	t.Run("query with the sole node picked automatically", func(t *testing.T) {
		got, err := store.ResolveKindFlag(ctx, k, "query", "")
		if err != nil {
			t.Fatal(err)
		}
		if got.Tag != KindQuery || got.NodeLabel != "primary" {
			t.Errorf("got %+v, want query:primary", got)
		}
	})

	t.Run("multiple query nodes with no --node is rejected, listing the candidates", func(t *testing.T) {
		if _, err := store.PutQueryStats(ctx, k, queryStatsFixture(schemaSnap.ContentHash, "qh-2", "replica-a")); err != nil {
			t.Fatal(err)
		}
		_, err := store.ResolveKindFlag(ctx, k, "query", "")
		if err == nil {
			t.Fatal("expected an error when auto-picking among multiple query nodes")
		}
	})

	t.Run("explicit --node bypasses auto-pick entirely, even for an unseen node", func(t *testing.T) {
		// This is deliberate: an explicit --node is trusted as-is, not validated
		// against ListKinds, so a caller can name a node that hasn't reported yet.
		got, err := store.ResolveKindFlag(ctx, k, "query", "replica-not-seen-yet")
		if err != nil {
			t.Fatal(err)
		}
		if got.NodeLabel != "replica-not-seen-yet" {
			t.Errorf("got %+v, want node label to pass through verbatim", got)
		}
	})

	t.Run("unknown kind is rejected", func(t *testing.T) {
		_, err := store.ResolveKindFlag(ctx, k, "bogus", "")
		if err == nil {
			t.Fatal("expected an error for an unrecognized --kind value")
		}
	})
}
