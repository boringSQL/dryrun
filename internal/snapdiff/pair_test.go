package snapdiff

import (
	"context"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// DiffNodePair is the single seam both diff paths (CLI `snapshot diff` and the
// MCP correlator) go through for per-node streams. These tests pin the contract
// callers rely on: dispatch by kind, partition rollup only when BOTH schemas are
// supplied, and refusal of mismatched or non-node kinds.

// partitioned parent public.events with one child public.events_2026; activity
// is only captured on the child, so the parent exists only after rollup
func partitionedSchema(hash string, ts time.Time) *snapshot.SchemaSnapshot {
	parent := table("events", "id")
	parent.PartitionInfo = &snapshot.PartitionInfo{
		Strategy: snapshot.PartitionRange,
		Key:      "id",
		Children: []snapshot.PartitionChild{{Schema: "public", Name: "events_2026"}},
	}
	return mkSchema(hash, ts, parent, table("events_2026", "id"))
}

func childActivity(hash string, ts time.Time, seqScan int64) *snapshot.ActivityStatsSnapshot {
	a := mkActivity("sh", hash, "primary", ts, 0)
	a.Tables = []snapshot.TableActivityEntry{{
		Table:    snapshot.QualifiedName{Schema: "public", Name: "events_2026"},
		Activity: snapshot.TableActivity{SeqScan: seqScan},
	}}
	return a
}

func hasCounter(d *diff.ActivityDelta, name string) bool {
	for _, c := range d.Counters {
		if c.Identity.Name == name {
			return true
		}
	}
	return false
}

func TestDiffNodePair_ActivityRollsUpOnlyWithBothSchemas(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	from := history.WrapActivity(childActivity("a1", now.Add(-time.Hour), 10))
	to := history.WrapActivity(childActivity("a2", now, 25))
	s := partitionedSchema("sh", now)

	t.Run("both schemas roll the parent up", func(t *testing.T) {
		d, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, from, to, s, s)
		if err != nil {
			t.Fatal(err)
		}
		if d.Query != nil || d.Activity == nil {
			t.Fatalf("activity pair must fill only Activity, got %+v", d)
		}
		if !hasCounter(d.Activity, "events") {
			t.Fatal("parent events should be synthesized from its child")
		}
	})

	t.Run("one schema missing skips rollup on both sides", func(t *testing.T) {
		for name, pair := range map[string][2]*snapshot.SchemaSnapshot{
			"from nil": {nil, s},
			"to nil":   {s, nil},
			"both nil": {nil, nil},
		} {
			d, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, from, to, pair[0], pair[1])
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if hasCounter(d.Activity, "events") {
				t.Fatalf("%s: a one-sided rollup would read the absent parent as zero", name)
			}
			if !hasCounter(d.Activity, "events_2026") {
				t.Fatalf("%s: the child's own counters must still diff", name)
			}
		}
	})

	// each schema rolls up its own side: a swap would put the parent's 10 on
	// the to side instead
	t.Run("schemas apply to their own side", func(t *testing.T) {
		plain := mkSchema("plain", now, table("events", "id"), table("events_2026", "id"))
		d, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, from, to, s, plain)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range d.Activity.Counters {
			if c.Identity.Name == "events" && c.Metric == diff.MetricSeqScan {
				if c.ValueA != 10 || c.ValueB != 0 {
					t.Fatalf("events seq_scan = %v -> %v, want 10 -> 0", c.ValueA, c.ValueB)
				}
				return
			}
		}
		t.Fatal("from side should carry the rolled-up parent")
	})
}

// the kind check is tag-only; two labels still reach DiffQueryStats, which
// refuses them as Incomparable rather than erroring
func TestDiffNodePair_DifferentLabelsAreIncomparableNotErrors(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	from := history.WrapQueryStats(mkQuery("sh", "q1", "primary", now.Add(-time.Hour), 10, 100))
	to := history.WrapQueryStats(mkQuery("sh", "q2", "replica", now, 30, 400))

	d, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, from, to, nil, nil)
	if err != nil {
		t.Fatalf("different labels must not error: %v", err)
	}
	if d.Query == nil || d.Query.Incomparable == "" {
		t.Fatalf("expected an Incomparable query delta, got %+v", d.Query)
	}
}

func TestDiffNodePair_QueryFillsQuery(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	from := history.WrapQueryStats(mkQuery("sh", "q1", "primary", now.Add(-time.Hour), 10, 100))
	to := history.WrapQueryStats(mkQuery("sh", "q2", "primary", now, 30, 400))

	d, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, from, to, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.Activity != nil || d.Query == nil {
		t.Fatalf("query pair must fill only Query, got %+v", d)
	}
	if d.Query.CallsDelta != 20 {
		t.Fatalf("calls delta = %d, want 20", d.Query.CallsDelta)
	}
}

func TestDiffNodePair_RefusesMismatchedAndNonNodeKinds(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	act := history.WrapActivity(mkActivity("sh", "a1", "primary", now, 1))
	qry := history.WrapQueryStats(mkQuery("sh", "q1", "primary", now, 1, 1))
	sch := history.WrapSchema(mkSchema("sh", now, table("users", "id")))

	if _, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, act, qry, nil, nil); err == nil {
		t.Fatal("activity vs query must not be comparable")
	}
	if _, err := DiffNodePair(context.Background(), nil, history.SnapshotKey{}, sch, sch, nil, nil); err == nil {
		t.Fatal("schema is not a per-node stream")
	}
}

func TestRowSchema(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	now := time.Now().Truncate(time.Second)
	put(t, store, history.WrapSchema(mkSchema("sh", now, table("users", "id"))))

	if got := RowSchema(ctx, store, key(), mkActivity("sh", "a1", "primary", now, 1)); got == nil || got.ContentHash != "sh" {
		t.Fatalf("expected the row's schema, got %v", got)
	}
	if got := RowSchema(ctx, store, key(), mkActivity("missing", "a2", "primary", now, 1)); got != nil {
		t.Fatal("an unknown schema_ref must resolve to nil, not error")
	}
	if got := RowSchema(ctx, store, key(), mkActivity("", "a3", "primary", now, 1)); got != nil {
		t.Fatal("a row without a schema_ref must resolve to nil")
	}
	if got := RowSchema(ctx, store, key(), nil); got != nil {
		t.Fatal("nil activity must resolve to nil")
	}
}
