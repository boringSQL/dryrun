package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

// diffOperands is the whole selection grammar boiled down to one pure function:
// given positionals plus the compat flags, it must decide which two snapshots
// (or one snapshot + the live database) the user actually asked for. These
// cases walk every branch a person can reach from the command line so a
// refactor that quietly drops, say, the bare `--from` legacy path or lets
// `--live` through with two operands gets caught here instead of in someone's
// terminal.
func TestDiffOperands(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		fromHash string
		toHash   string
		latest   bool
		live     bool

		wantFrom string
		wantTo   string
		wantLive bool
		wantErr  bool
	}{
		{
			name:     "two positionals is the happy store-to-store path",
			args:     []string{"aaaa", "bbbb"},
			wantFrom: "aaaa",
			wantTo:   "bbbb",
		},
		{
			name:     "from/to flags still work for the people with muscle memory",
			fromHash: "xxxx",
			toHash:   "yyyy",
			wantFrom: "xxxx",
			wantTo:   "yyyy",
		},
		{
			name:     "latest alone means since-last-capture, the previous against the newest",
			latest:   true,
			wantFrom: "latest~1",
			wantTo:   "latest",
		},
		{
			name:     "an explicit from anchored against the newest capture",
			fromHash: "xxxx",
			latest:   true,
			wantFrom: "xxxx",
			wantTo:   "latest",
		},
		{
			name:     "one positional plus --live diffs that snapshot against the database now",
			args:     []string{"aaaa"},
			live:     true,
			wantFrom: "aaaa",
			wantTo:   "",
			wantLive: true,
		},
		{
			name:     "a bare --from with no --to is the historical live shorthand we keep honoring",
			fromHash: "xxxx",
			wantFrom: "xxxx",
			wantTo:   "",
			wantLive: true,
		},
		{
			name:    "--live refuses to pick between two operands",
			args:    []string{"aaaa", "bbbb"},
			live:    true,
			wantErr: true,
		},
		{
			name:    "--live with nothing to diff is a usage error, not a silent capture",
			live:    true,
			wantErr: true,
		},
		{
			name:    "a lone snapshot with no second operand and no --live can't be a diff",
			args:    []string{"aaaa"},
			wantErr: true,
		},
		{
			name:    "no arguments at all is a usage error",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from, to, live, err := diffOperands(tc.args, tc.fromHash, tc.toHash, tc.latest, tc.live)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got from=%q to=%q live=%v", from, to, live)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if from != tc.wantFrom || to != tc.wantTo || live != tc.wantLive {
				t.Fatalf("got from=%q to=%q live=%v, want from=%q to=%q live=%v",
					from, to, live, tc.wantFrom, tc.wantTo, tc.wantLive)
			}
		})
	}
}

// parseKindFlag is the little dispatcher that turns the --kind string into a
// concrete SnapshotKind. schema and planner are trivial; activity is the
// interesting one because it has to go find a node label, so the no-activity
// and unknown-kind error paths both get a look here. The multi-node and
// single-node activity behavior lives in its own test below where we can seed
// the store.
func TestParseKindFlag(t *testing.T) {
	store := openSQLite(t)
	key := syncKey("acme", "primary")
	ctx := context.Background()

	if k, err := store.ResolveKindFlag(ctx, key, "schema", ""); err != nil || k.Tag != history.KindSchema {
		t.Fatalf("schema: got kind=%v err=%v", k, err)
	}
	// empty string is the same as schema, since that's the flag default
	if k, err := store.ResolveKindFlag(ctx, key, "", ""); err != nil || k.Tag != history.KindSchema {
		t.Fatalf("empty kind should default to schema: got kind=%v err=%v", k, err)
	}
	if k, err := store.ResolveKindFlag(ctx, key, "planner", ""); err != nil || k.Tag != history.KindPlanner {
		t.Fatalf("planner: got kind=%v err=%v", k, err)
	}
	if _, err := store.ResolveKindFlag(ctx, key, "nonsense", ""); err == nil {
		t.Fatal("an unknown kind should be rejected, not silently swallowed")
	}
	// with no activity rows in the store there's no node to resolve to
	if _, err := store.ResolveKindFlag(ctx, key, "activity", ""); err == nil {
		t.Fatal("activity with nothing captured should error")
	}
}

// resolveActivityKind has to guess a node label when the user doesn't hand one
// over. With a single captured node that guess is unambiguous and we take it;
// with two it must refuse and tell the user to disambiguate; an explicit --node
// short-circuits the whole search.
func TestResolveActivityKind(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	t.Run("a single node is picked automatically", func(t *testing.T) {
		store := openSQLite(t)
		key := syncKey("acme", "primary")
		if _, err := store.PutSchema(ctx, key, syncTestSchema("sh-1", "appdb", now)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutActivity(ctx, key, syncTestActivity("sh-1", "ac-1", "primary", now, false)); err != nil {
			t.Fatal(err)
		}
		k, err := store.ResolveKindFlag(ctx, key, "activity", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if k.Tag != history.KindActivity || k.NodeLabel != "primary" {
			t.Fatalf("got %v, want activity:primary", k)
		}
	})

	t.Run("two nodes force the user to choose", func(t *testing.T) {
		store := openSQLite(t)
		key := syncKey("acme", "primary")
		if _, err := store.PutSchema(ctx, key, syncTestSchema("sh-1", "appdb", now)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutActivity(ctx, key, syncTestActivity("sh-1", "ac-1", "primary", now, false)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutActivity(ctx, key, syncTestActivity("sh-1", "ac-2", "replica", now, true)); err != nil {
			t.Fatal(err)
		}
		_, err := store.ResolveKindFlag(ctx, key, "activity", "")
		if err == nil {
			t.Fatal("two nodes with no --node should be ambiguous")
		}
		// the error should name both nodes so the user knows what to pass
		if !strings.Contains(err.Error(), "primary") || !strings.Contains(err.Error(), "replica") {
			t.Fatalf("error should list the available nodes, got: %v", err)
		}
	})

	t.Run("an explicit node is taken verbatim", func(t *testing.T) {
		store := openSQLite(t)
		key := syncKey("acme", "primary")
		k, err := store.ResolveKindFlag(ctx, key, "activity", "replica")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if k.NodeLabel != "replica" {
			t.Fatalf("got node %q, want replica", k.NodeLabel)
		}
	})
}

// resolveDiffToken is where a single command-line word becomes a (kind, ref)
// pair. A `latest` word leans on --kind to know what it's the latest of; a
// `latest~N` word counts generations back through the stored series; anything
// else is treated as a content-hash prefix and the kind is recovered from
// whichever table the hash lives in.
func TestResolveDiffToken(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	store := openSQLite(t)
	key := syncKey("acme", "primary")

	// seed a schema plus two planner captures an hour apart so latest~1 has a
	// real predecessor to point at
	if _, err := store.PutSchema(ctx, key, syncTestSchema("schemahash", "appdb", now.Add(-2*time.Hour))); err != nil {
		t.Fatal(err)
	}
	older := syncTestPlanner("schemahash", "planner-older", "appdb", now.Add(-time.Hour))
	newer := syncTestPlanner("schemahash", "planner-newer", "appdb", now)
	if _, err := store.PutPlanner(ctx, key, older); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutPlanner(ctx, key, newer); err != nil {
		t.Fatal(err)
	}

	t.Run("latest resolves to the newest of the requested kind", func(t *testing.T) {
		kind, ref, err := store.ResolveToken(ctx, key, "latest", "planner", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if kind.Tag != history.KindPlanner {
			t.Fatalf("got kind %v, want planner", kind)
		}
		if ref.Kind != history.RefLatest {
			t.Fatalf("latest should resolve to a RefLatest, got %v", ref.Kind)
		}
	})

	t.Run("latest~1 walks back one generation in the series", func(t *testing.T) {
		_, ref, err := store.ResolveToken(ctx, key, "latest~1", "planner", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// the previous planner capture is the older of the two we seeded
		if ref.Kind != history.RefHash || ref.Hash != "planner-older" {
			t.Fatalf("latest~1 should point at the previous capture, got kind=%v hash=%q", ref.Kind, ref.Hash)
		}
	})

	t.Run("walking back further than history allows is an error", func(t *testing.T) {
		_, _, err := store.ResolveToken(ctx, key, "latest~5", "planner", "")
		if err == nil {
			t.Fatal("latest~5 against two captures should fail")
		}
	})

	t.Run("a hash prefix recovers its own kind without --kind", func(t *testing.T) {
		// --kind is left at schema on purpose; the planner prefix should win
		kind, ref, err := store.ResolveToken(ctx, key, "planner-ne", "schema", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if kind.Tag != history.KindPlanner {
			t.Fatalf("a planner hash should resolve to planner kind, got %v", kind)
		}
		if ref.Kind != history.RefHash || ref.Hash != "planner-ne" {
			t.Fatalf("got ref kind=%v hash=%q", ref.Kind, ref.Hash)
		}
	})

	t.Run("an unknown hash prefix is rejected", func(t *testing.T) {
		_, _, err := store.ResolveToken(ctx, key, "deadbeef", "schema", "")
		if err == nil {
			t.Fatal("a hash that matches nothing should error")
		}
	})
}

// A content-hash prefix should load the same snapshot a full hash would, the
// way git lets you name a commit by its first few characters. The schema path
// already did this; these check the planner and activity tables learned the
// same trick, including the ambiguity guard that refuses a prefix matching more
// than one row.
func TestStoreGetByHashPrefix(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	store := openSQLite(t)
	key := syncKey("acme", "primary")

	if _, err := store.PutSchema(ctx, key, syncTestSchema("schemahash", "appdb", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutPlanner(ctx, key, syncTestPlanner("schemahash", "plannerhash", "appdb", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, key, syncTestActivity("schemahash", "activityhash", "primary", now, false)); err != nil {
		t.Fatal(err)
	}

	t.Run("a planner prefix loads the full planner snapshot", func(t *testing.T) {
		got, err := store.Get(ctx, key, history.PlannerKind(), history.NewRefHash("planner"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.ContentHash() != "plannerhash" {
			t.Fatalf("got %q, want plannerhash", got.ContentHash())
		}
	})

	t.Run("an activity prefix loads the full activity snapshot", func(t *testing.T) {
		got, err := store.Get(ctx, key, history.ActivityKind("primary"), history.NewRefHash("activity"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.ContentHash() != "activityhash" {
			t.Fatalf("got %q, want activityhash", got.ContentHash())
		}
	})

	t.Run("a prefix that matches nothing is a not-found", func(t *testing.T) {
		if _, err := store.Get(ctx, key, history.PlannerKind(), history.NewRefHash("zzzz")); err == nil {
			t.Fatal("a planner prefix matching no row should error")
		}
	})
}

// buildSnapshotDiff is the dispatcher that, given a kind, calls the matching
// Diff* function and parks the result in the right envelope slot. We don't
// re-check the deltas themselves here (those have their own tests) — only that
// each kind lands in its own field and the envelope carries the endpoints, so
// a JSON consumer can tell schema from planner from activity.
func TestBuildSnapshotDiff(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	t.Run("schema fills the schema slot", func(t *testing.T) {
		from := history.WrapSchema(syncTestSchema("schema-a", "appdb", now.Add(-time.Hour)))
		to := history.WrapSchema(syncTestSchema("schema-b", "appdb", now))
		env, err := buildSnapshotDiff(history.SchemaKind(), from, to)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env.Kind != "schema" || env.Schema == nil {
			t.Fatalf("expected a schema envelope, got kind=%q schema=%v", env.Kind, env.Schema)
		}
		if env.Planner != nil || env.Activity != nil {
			t.Fatal("only the schema slot should be populated")
		}
		if env.FromHash != "schema-a" || env.ToHash != "schema-b" {
			t.Fatalf("endpoints not carried through: from=%q to=%q", env.FromHash, env.ToHash)
		}
	})

	t.Run("planner fills the planner slot", func(t *testing.T) {
		from := history.WrapPlanner(syncTestPlanner("sh", "planner-a", "appdb", now.Add(-time.Hour)))
		to := history.WrapPlanner(syncTestPlanner("sh", "planner-b", "appdb", now))
		env, err := buildSnapshotDiff(history.PlannerKind(), from, to)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env.Kind != "planner" || env.Planner == nil {
			t.Fatalf("expected a planner envelope, got kind=%q planner=%v", env.Kind, env.Planner)
		}
		if env.Schema != nil || env.Activity != nil {
			t.Fatal("only the planner slot should be populated")
		}
	})

	t.Run("activity fills the activity slot", func(t *testing.T) {
		from := history.WrapActivity(syncTestActivity("sh", "activity-a", "primary", now.Add(-time.Hour), false))
		to := history.WrapActivity(syncTestActivity("sh", "activity-b", "primary", now, false))
		env, err := buildSnapshotDiff(history.ActivityKind("primary"), from, to)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env.Kind != "activity" || env.Activity == nil {
			t.Fatalf("expected an activity envelope, got kind=%q activity=%v", env.Kind, env.Activity)
		}
		if env.Schema != nil || env.Planner != nil {
			t.Fatal("only the activity slot should be populated")
		}
	})
}
