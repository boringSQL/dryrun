package snapdiff

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// The whole point of snapdiff is that a user asks "what changed between these two
// moments" and gets one answer that braids the three capture streams together,
// not three disconnected per-kind diffs. These tests seed a store the way a real
// `dryrun snapshot take` would — schema, planner, and activity rows written
// separately with their own slightly-skewed timestamps — and then assert that
// Build re-joins them by capture time and tells the truth about how it did so.

func openStore(t *testing.T) *history.Store {
	t.Helper()
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func key() history.SnapshotKey {
	return history.SnapshotKey{ProjectID: "acme", DatabaseID: "primary"}
}

func table(name string, cols ...string) snapshot.Table {
	var cs []snapshot.Column
	for i, c := range cols {
		cs = append(cs, snapshot.Column{Name: c, Ordinal: int16(i + 1), TypeName: "text", Nullable: true})
	}
	return snapshot.Table{Schema: "public", Name: name, Columns: cs}
}

func withIndex(t snapshot.Table, idx string) snapshot.Table {
	t.Indexes = append(t.Indexes, snapshot.Index{Name: idx, IndexType: "btree", Definition: "CREATE INDEX " + idx})
	return t
}

func mkSchema(hash string, ts time.Time, tables ...snapshot.Table) *snapshot.SchemaSnapshot {
	return &snapshot.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "appdb",
		Timestamp: ts, ContentHash: hash, Tables: tables,
	}
}

func mkPlanner(schemaRef, hash string, ts time.Time, reltuples float64) *snapshot.PlannerStatsSnapshot {
	return &snapshot.PlannerStatsSnapshot{
		SchemaRefHash: schemaRef, ContentHash: hash, Database: "appdb", Timestamp: ts,
		Tables: []snapshot.TableSizingEntry{{
			Table:  snapshot.QualifiedName{Schema: "public", Name: "users"},
			Sizing: snapshot.TableSizing{Reltuples: reltuples, Relpages: 10, TableSize: 81920},
		}},
	}
}

func mkActivity(schemaRef, hash, node string, ts time.Time, seqScan int64) *snapshot.ActivityStatsSnapshot {
	return &snapshot.ActivityStatsSnapshot{
		SchemaRefHash: schemaRef, ContentHash: hash,
		Node: snapshot.NodeIdentity{Source: node, PgVersion: "PostgreSQL 17.0", Timestamp: ts},
		Tables: []snapshot.TableActivityEntry{{
			Table:    snapshot.QualifiedName{Schema: "public", Name: "users"},
			Activity: snapshot.TableActivity{SeqScan: seqScan, IdxScan: 5},
		}},
	}
}

func put(t *testing.T, s *history.Store, snap history.StoredSnapshot) {
	t.Helper()
	if _, err := s.Put(context.Background(), key(), snap); err != nil {
		t.Fatal(err)
	}
}

func findObject(objs []ObjectChange, name string) *ObjectChange {
	for i := range objs {
		if objs[i].Name == name {
			return &objs[i]
		}
	}
	return nil
}

// TestBuild_CorrelatesAcrossKinds is the headline behavior: a schema change to
// `users` plus sizing growth plus an activity spike, all captured at two moments
// two hours apart, must come back as ONE users object carrying structural,
// sizing, and activity lines together. That co-location is the moat.
func TestBuild_CorrelatesAcrossKinds(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-3 * time.Hour)
	t1 := t0.Add(2 * time.Hour)

	// moment 0: users(id), 1k rows, 10 seq scans
	put(t, store, history.WrapSchema(mkSchema("schema-a", t0, table("users", "id"))))
	put(t, store, history.WrapPlanner(mkPlanner("schema-a", "planner-a", t0.Add(2*time.Minute), 1000)))
	put(t, store, history.WrapActivity(mkActivity("schema-a", "activity-a", "primary", t0.Add(3*time.Minute), 10)))

	// moment 1: users(id,email)+index, 5k rows, 100 seq scans
	put(t, store, history.WrapSchema(mkSchema("schema-b", t1, withIndex(table("users", "id", "email"), "users_email_idx"))))
	put(t, store, history.WrapPlanner(mkPlanner("schema-b", "planner-b", t1.Add(2*time.Minute), 5000)))
	put(t, store, history.WrapActivity(mkActivity("schema-b", "activity-b", "primary", t1.Add(3*time.Minute), 100)))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if res.IsEmpty() {
		t.Fatal("expected a non-empty diff")
	}

	users := findObject(res.Objects, "users")
	if users == nil {
		t.Fatalf("no users object in %+v", res.Objects)
	}
	if len(users.Structural) == 0 {
		t.Error("users should carry structural changes (added column + index)")
	}
	if len(users.Sizing) == 0 {
		t.Error("users should carry sizing drift (reltuples grew 5x)")
	}
	if len(users.Activity) == 0 {
		t.Error("users should carry activity drift (seq_scan spike)")
	}

	// and the join must be auditable: both sides matched a planner and an activity
	// capture within the window, with a small recorded skew.
	if res.Correlation.From.Planner == nil || res.Correlation.To.Planner == nil {
		t.Fatal("both sides should have matched a planner capture")
	}
	if res.Correlation.From.Planner.Source != "window" {
		t.Errorf("planner match should come from the window join, got %q", res.Correlation.From.Planner.Source)
	}
	if len(res.Correlation.From.Activity) != 1 {
		t.Errorf("from side should have matched one activity node, got %d", len(res.Correlation.From.Activity))
	}
}

// TestBuild_WindowExcludesFarCaptures: a planner row captured well outside the
// window is NOT silently treated as belonging to the moment. The diff has no
// planner delta and the correlation notes say so, instead of quietly pairing a
// stale capture.
func TestBuild_WindowExcludesFarCaptures(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-5 * time.Hour)
	t1 := t0.Add(2 * time.Hour)

	put(t, store, history.WrapSchema(mkSchema("schema-a", t0, table("users", "id"))))
	put(t, store, history.WrapSchema(mkSchema("schema-b", t1, table("users", "id", "email"))))
	// the only planner capture sits an hour off the t1 anchor — outside 30m
	put(t, store, history.WrapPlanner(mkPlanner("schema-b", "planner-far", t1.Add(time.Hour), 5000)))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema", Window: 30 * time.Minute})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if res.PlannerDelta != nil {
		t.Error("a planner capture outside the window must not be correlated")
	}
	joined := strings.Join(res.Correlation.Notes, " | ")
	if !strings.Contains(joined, "no planner capture within") {
		t.Errorf("correlation should note the missing planner, got: %q", joined)
	}
}

// TestBuild_PlannerAnchorWhenSchemaStable is the case a plain schema diff can't
// answer: the DDL is unchanged (one deduped schema row) but the table grew. The
// user anchors on the planner timeline; schema is still resolved exactly via
// schema_ref, and the schema delta is correctly empty.
func TestBuild_PlannerAnchorWhenSchemaStable(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-3 * time.Hour)

	put(t, store, history.WrapSchema(mkSchema("schema-x", t0, table("users", "id"))))
	put(t, store, history.WrapPlanner(mkPlanner("schema-x", "planner-1", t0.Add(5*time.Minute), 1000)))
	put(t, store, history.WrapPlanner(mkPlanner("schema-x", "planner-2", t0.Add(2*time.Hour), 5000)))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "planner"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if res.PrimaryKind != "planner" {
		t.Errorf("primary kind should be planner, got %q", res.PrimaryKind)
	}
	if res.PlannerDelta.IsEmpty() {
		t.Fatal("planner delta should show the reltuples growth")
	}
	if !res.SchemaDelta.IsEmpty() {
		t.Error("schema is stable, so the schema delta must be empty")
	}
	// schema was recovered through the exact content link, not a time window
	if res.Correlation.From.Schema == nil || res.Correlation.From.Schema.Source != "schema_ref" {
		t.Errorf("schema should be correlated via schema_ref, got %+v", res.Correlation.From.Schema)
	}
}

// TestBuild_RanksDestructiveFirst: a dropped table outranks a freshly added one,
// so the agent reads the scariest change at the top of the list.
func TestBuild_RanksDestructiveFirst(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	t1 := t0.Add(time.Hour)

	put(t, store, history.WrapSchema(mkSchema("s-a", t0, table("orders", "id"))))
	put(t, store, history.WrapSchema(mkSchema("s-b", t1, table("invoices", "id"))))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.Objects) < 2 {
		t.Fatalf("expected both the dropped and added table, got %d objects", len(res.Objects))
	}
	if res.Objects[0].Name != "orders" {
		t.Errorf("the dropped table should rank first, got %q", res.Objects[0].Name)
	}
}

// TestBuild_Empty: two identical schema captures produce a clean "no changes"
// result, not a noisy empty payload.
func TestBuild_Empty(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)

	put(t, store, history.WrapSchema(mkSchema("same-1", t0, table("users", "id"))))
	put(t, store, history.WrapSchema(mkSchema("same-2", t0.Add(time.Hour), table("users", "id"))))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !res.IsEmpty() {
		t.Errorf("identical schemas should diff empty, got objects=%+v", res.Objects)
	}
	if !strings.Contains(res.Summary.Headline, "no changes") {
		t.Errorf("headline should say no changes, got %q", res.Summary.Headline)
	}
}

// TestForView: summary drops the raw deltas (token-thrift for an LLM context);
// full keeps them.
func TestForView(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	t1 := t0.Add(time.Hour)
	put(t, store, history.WrapSchema(mkSchema("s-a", t0, table("users", "id"))))
	put(t, store, history.WrapSchema(mkSchema("s-b", t1, table("users", "id", "email"))))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := res.ForView("summary"); got.SchemaDelta != nil {
		t.Error("summary view must omit the raw schema delta")
	}
	if got := res.ForView("full"); got.SchemaDelta == nil {
		t.Error("full view must keep the raw schema delta")
	}
}
