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

func tableIn(schemaName, name string, cols ...string) snapshot.Table {
	t := table(name, cols...)
	t.Schema = schemaName
	return t
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

	// and the join must be auditable: with schema as the anchor, the planner and
	// activity are pulled through the exact schema_ref link (not the time window),
	// so the source must say so on both sides.
	if res.Correlation.From.Planner == nil || res.Correlation.To.Planner == nil {
		t.Fatal("both sides should have matched a planner capture")
	}
	if res.Correlation.From.Planner.Source != "schema_ref" {
		t.Errorf("planner match should come from the exact schema_ref link, got %q", res.Correlation.From.Planner.Source)
	}
	if len(res.Correlation.From.Activity) != 1 || res.Correlation.From.Activity[0].Source != "schema_ref" {
		t.Errorf("from side should have one schema_ref-matched activity node, got %+v", res.Correlation.From.Activity)
	}
}

// TestBuild_SchemaRefJoinIgnoresWindow is the point of the exact link: a planner
// captured two hours after its schema snapshot — far outside any window — is
// still the planner for that schema and must be correlated. The old time-window
// heuristic would have dropped it; the schema_ref join keys on identity, not
// proximity, so the diff comes back populated with a schema_ref source even
// though no capture sits anywhere near the anchor timestamp.
func TestBuild_SchemaRefJoinIgnoresWindow(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-6 * time.Hour)
	t1 := t0.Add(3 * time.Hour)

	put(t, store, history.WrapSchema(mkSchema("schema-a", t0, table("users", "id"))))
	put(t, store, history.WrapSchema(mkSchema("schema-b", t1, table("users", "id", "email"))))
	// each planner sits 2h off its own schema anchor — way outside a 30m window
	put(t, store, history.WrapPlanner(mkPlanner("schema-a", "planner-a", t0.Add(2*time.Hour), 1000)))
	put(t, store, history.WrapPlanner(mkPlanner("schema-b", "planner-b", t1.Add(2*time.Hour), 5000)))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema", Window: 30 * time.Minute})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if res.PlannerDelta.IsEmpty() {
		t.Fatal("the exact schema_ref join must correlate the planner regardless of window")
	}
	if res.Correlation.From.Planner == nil || res.Correlation.From.Planner.Source != "schema_ref" {
		t.Errorf("planner should be matched via schema_ref, got %+v", res.Correlation.From.Planner)
	}
}

// TestBuild_WindowExcludesFarCaptures covers the path where the time window still
// governs: a planner-anchored diff correlates the *other* kind (activity) by
// capture proximity, since there's no exact link for it. An activity row sitting
// hours from both planner anchors must not be silently adopted — the diff carries
// no activity and the notes say so, rather than pairing a stale capture.
func TestBuild_WindowExcludesFarCaptures(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-6 * time.Hour)

	put(t, store, history.WrapSchema(mkSchema("schema-x", t0, table("users", "id"))))
	put(t, store, history.WrapPlanner(mkPlanner("schema-x", "planner-1", t0.Add(5*time.Minute), 1000)))
	put(t, store, history.WrapPlanner(mkPlanner("schema-x", "planner-2", t0.Add(2*time.Hour), 5000)))
	// the only activity capture is hours from either planner anchor — outside 30m
	put(t, store, history.WrapActivity(mkActivity("schema-x", "activity-far", "primary", t0.Add(5*time.Hour), 100)))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "planner", Window: 30 * time.Minute})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.ActivityDelta) != 0 {
		t.Error("an activity capture outside the window must not be correlated")
	}
	joined := strings.Join(res.Correlation.Notes, " | ")
	if !strings.Contains(joined, "no activity capture within") {
		t.Errorf("correlation should note the missing activity, got: %q", joined)
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
	if got := res.ForView("summary", 0); got.SchemaDelta != nil {
		t.Error("summary view must omit the raw schema delta")
	}
	if got := res.ForView("full", 0); got.SchemaDelta == nil {
		t.Error("full view must keep the raw schema delta")
	}
}

// threeDrops gives the cap tests a deterministic, easy-to-count diff: a from
// snapshot with three tables and a to snapshot with none, which a schema diff
// turns into exactly three dropped-table objects. Three is the smallest count
// that lets a limit of 1 leave an unambiguous remainder of 2, so every "kept N,
// omitted M" assertion below has a single correct answer and a failure points
// straight at the capping math rather than at fixture noise.
func threeDrops(t *testing.T) *Result {
	t.Helper()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	put(t, store, history.WrapSchema(mkSchema("s-a", t0, table("t1", "id"), table("t2", "id"), table("t3", "id"))))
	put(t, store, history.WrapSchema(mkSchema("s-b", t0.Add(time.Hour))))
	res, err := Build(context.Background(), store, key(), Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.Objects) != 3 {
		t.Fatalf("fixture should yield 3 objects, got %d", len(res.Objects))
	}
	return res
}

// TestForView_LimitCapsObjects pins the headline promise of the §6 work: the
// response size is something the caller controls, not something the database
// dictates. With a limit of 1 the object list must come back trimmed to a
// single entry AND admit that it hid the rest (truncated, omitted_objects=2) so
// an agent never mistakes a capped view for the whole story. The limit=0 case is
// the escape hatch — "give me everything" — and there the truncation flag has to
// stay off, otherwise a caller asking for the full set would be told it was
// clipped when it wasn't.
func TestForView_LimitCapsObjects(t *testing.T) {
	res := threeDrops(t)

	capped := res.ForView("summary", 1)
	if len(capped.Objects) != 1 {
		t.Fatalf("limit=1 should keep one object, got %d", len(capped.Objects))
	}
	if !capped.Truncated || capped.OmittedObjects != 2 {
		t.Errorf("expected truncated with 2 omitted, got truncated=%v omitted=%d", capped.Truncated, capped.OmittedObjects)
	}

	all := res.ForView("summary", 0)
	if len(all.Objects) != 3 || all.Truncated {
		t.Errorf("limit=0 should keep all and not truncate, got %d objects truncated=%v", len(all.Objects), all.Truncated)
	}
}

// TestForView_FullCapsRawRows guards the sneaky payload leak: the summary view
// is small by construction, but the full view also ships the raw per-row deltas,
// and those grow with the size of the diff. If the cap only applied to the
// object list, a giant migration could still return a megabyte of raw changes
// and quietly blow the context budget the whole feature is supposed to protect.
// So full view must clip the raw rows to the same limit and report the overflow
// separately as omitted_rows, and limit=0 must once again mean "all of it".
func TestForView_FullCapsRawRows(t *testing.T) {
	res := threeDrops(t)

	full := res.ForView("full", 1)
	if full.SchemaDelta == nil || len(full.SchemaDelta.Changes) != 1 {
		t.Fatalf("full view should cap raw changes to 1, got %+v", full.SchemaDelta)
	}
	if full.OmittedRows != 2 || !full.Truncated {
		t.Errorf("expected 2 omitted rows and truncated, got omitted_rows=%d truncated=%v", full.OmittedRows, full.Truncated)
	}

	allFull := res.ForView("full", 0)
	if len(allFull.SchemaDelta.Changes) != 3 || allFull.OmittedRows != 0 {
		t.Errorf("limit=0 should keep all rows, got %d changes omitted_rows=%d", len(allFull.SchemaDelta.Changes), allFull.OmittedRows)
	}
}

// TestBuild_FilterBySchema covers the other half of "narrow it down": before you
// reach for a cap, you should be able to ask the question more precisely. A diff
// that touches both public and audit, filtered to audit, must come back with
// only the audit object — and crucially the raw delta has to be scoped too, not
// just the ranked object list. A filter that trimmed the headline but left the
// raw rows full would be a filter in name only, and the truncation it's meant to
// avoid would sneak back in through the delta.
func TestBuild_FilterBySchema(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	put(t, store, history.WrapSchema(mkSchema("s-a", t0,
		tableIn("public", "users", "id"), tableIn("audit", "log", "id"))))
	put(t, store, history.WrapSchema(mkSchema("s-b", t0.Add(time.Hour))))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema", Schema: "audit"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.Objects) != 1 || res.Objects[0].Schema != "audit" {
		t.Fatalf("schema filter should keep only audit objects, got %+v", res.Objects)
	}
	for _, c := range res.SchemaDelta.Changes {
		if c.Object.Schema == nil || *c.Object.Schema != "audit" {
			t.Errorf("raw delta should be scoped to audit, found %v", c.Object)
		}
	}
}

// TestBuild_FilterByTable is the finest-grained narrowing knob: two tables
// changed, the user only cares about orders, so orders is the only object that
// survives. This is the call an agent makes after a broad diff points it at one
// hot table and it wants to drill in without re-reading everything else.
func TestBuild_FilterByTable(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	t0 := time.Now().Truncate(time.Second).Add(-2 * time.Hour)
	put(t, store, history.WrapSchema(mkSchema("s-a", t0, table("orders", "id"), table("invoices", "id"))))
	put(t, store, history.WrapSchema(mkSchema("s-b", t0.Add(time.Hour))))

	res, err := Build(ctx, store, key(), Options{From: "latest~1", To: "latest", Kind: "schema", Table: "orders"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(res.Objects) != 1 || res.Objects[0].Name != "orders" {
		t.Fatalf("table filter should keep only orders, got %+v", res.Objects)
	}
}
