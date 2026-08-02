package snapshot

import (
	"encoding/json"
	"testing"
)

// Baseline snapshot used by hash sensitivity tests below.
func baselineSnap() *SchemaSnapshot {
	return &SchemaSnapshot{
		PgVersion: "16.0",
		Tables: []Table{{
			Schema: "public",
			Name:   "users",
			Columns: []Column{
				{Name: "id", Ordinal: 1, TypeName: "bigint", Nullable: false},
				{Name: "email", Ordinal: 2, TypeName: "text", Nullable: false},
			},
		}},
	}
}

// statistics_target and generated participate in DDL identity:
// setting them must shift the content_hash so introspect runs that
// observe ALTER ... SET STATISTICS / GENERATED ALWAYS create a new
// snapshot row instead of dedup'ing against the previous one.
func TestContentHash_SensitiveToStatisticsTargetAndGenerated(t *testing.T) {
	base := ComputeContentHash(baselineSnap())

	target := int16(500)
	snapWithTarget := baselineSnap()
	snapWithTarget.Tables[0].Columns[1].StatisticsTarget = &target
	if h := ComputeContentHash(snapWithTarget); h == base {
		t.Errorf("hash did not change after setting statistics_target")
	}

	gen := "stored"
	snapWithGen := baselineSnap()
	snapWithGen.Tables[0].Columns[1].Generated = &gen
	if h := ComputeContentHash(snapWithGen); h == base {
		t.Errorf("hash did not change after setting generated")
	}
}

// After the DDL-only refactor SchemaSnapshot no longer carries stats, so
// stats-only mutation is impossible by construction. We keep a smoke test
// that the hash itself is deterministic across two identical snapshots.
func TestContentHash_DeterministicOnIdenticalSnapshots(t *testing.T) {
	a := ComputeContentHash(baselineSnap())
	b := ComputeContentHash(baselineSnap())
	if a != b {
		t.Errorf("hash non-deterministic: %s vs %s", a, b)
	}
}

// Adding or removing a column is a DDL change and MUST shift the hash;
// otherwise an introspect re-run after a migration would dedup against the
// pre-migration snapshot and we'd lose the diff for drift reports.
func TestContentHash_SensitiveToAddRemoveColumn(t *testing.T) {
	base := ComputeContentHash(baselineSnap())

	added := baselineSnap()
	added.Tables[0].Columns = append(added.Tables[0].Columns, Column{
		Name: "created_at", Ordinal: 3, TypeName: "timestamptz", Nullable: false,
	})
	if h := ComputeContentHash(added); h == base {
		t.Errorf("hash didn't change when adding a column")
	}

	removed := baselineSnap()
	removed.Tables[0].Columns = removed.Tables[0].Columns[:1]
	if h := ComputeContentHash(removed); h == base {
		t.Errorf("hash didn't change when removing a column")
	}
}

// Index DDL participates in the hash; adding or removing one needs to be
// visible to drift, otherwise CREATE INDEX CONCURRENTLY runs go unnoticed.
func TestContentHash_SensitiveToAddRemoveIndex(t *testing.T) {
	base := ComputeContentHash(baselineSnap())

	withIdx := baselineSnap()
	withIdx.Tables[0].Indexes = []Index{{
		Name:       "users_email_idx",
		Columns:    []string{"email"},
		IndexType:  "btree",
		IsUnique:   true,
		Definition: "CREATE UNIQUE INDEX users_email_idx ON public.users (email)",
		IsValid:    true,
	}}
	h1 := ComputeContentHash(withIdx)
	if h1 == base {
		t.Errorf("hash didn't change when adding an index")
	}

	dropped := withIdx
	dropped.Tables[0].Indexes = nil
	if h := ComputeContentHash(dropped); h == h1 {
		t.Errorf("hash didn't change when removing an index")
	}
}

// HasExpressions is a transient capture-time field (json:"-") that feeds the bloat
// estimator's confidence flag; it must not be part of the schema content hash, or
// adding it would mint a one-time spurious schema diff for every tracked database.
func TestContentHash_IgnoresHasExpressions(t *testing.T) {
	withIdx := baselineSnap()
	withIdx.Tables[0].Indexes = []Index{{
		Name:       "users_email_idx",
		Columns:    []string{"email"},
		IndexType:  "btree",
		Definition: "CREATE INDEX users_email_idx ON public.users (lower(email))",
		IsValid:    true,
	}}
	base := ComputeContentHash(withIdx)

	flagged := withIdx
	flagged.Tables = append([]Table(nil), withIdx.Tables...)
	flagged.Tables[0].Indexes = append([]Index(nil), withIdx.Tables[0].Indexes...)
	flagged.Tables[0].Indexes[0].HasExpressions = true
	if h := ComputeContentHash(flagged); h != base {
		t.Errorf("content hash changed when HasExpressions toggled; it must be excluded")
	}
}

// Changing a primary key (kind or column list) is a DDL change. Sensitivity
// here guards the most common "promote a candidate key to PK" migration.
func TestContentHash_SensitiveToPrimaryKeyChange(t *testing.T) {
	withPK := baselineSnap()
	withPK.Tables[0].Constraints = []Constraint{{
		Name: "users_pkey", Kind: ConstraintPrimaryKey, Columns: []string{"id"},
	}}
	base := ComputeContentHash(withPK)

	movedPK := baselineSnap()
	movedPK.Tables[0].Constraints = []Constraint{{
		Name: "users_pkey", Kind: ConstraintPrimaryKey, Columns: []string{"email"},
	}}
	if h := ComputeContentHash(movedPK); h == base {
		t.Errorf("hash didn't change after moving PK columns")
	}
}

// Foreign keys define cross-table invariants; adding one MUST shift the hash
// so referential-integrity changes show up in drift output.
func TestContentHash_SensitiveToAddForeignKey(t *testing.T) {
	base := ComputeContentHash(baselineSnap())

	withFK := baselineSnap()
	fkTable := "tenants"
	withFK.Tables[0].Constraints = []Constraint{{
		Name: "users_tenant_fkey", Kind: ConstraintForeignKey,
		Columns: []string{"tenant_id"}, FKTable: &fkTable, FKColumns: []string{"id"},
	}}
	if h := ComputeContentHash(withFK); h == base {
		t.Errorf("hash didn't change when adding a foreign key")
	}
}

// Toggling RLS rewrites the table's security model — drift consumers must
// see this even though no columns or indexes moved.
func TestContentHash_SensitiveToRLSToggle(t *testing.T) {
	base := ComputeContentHash(baselineSnap())

	rlsOn := baselineSnap()
	rlsOn.Tables[0].RLSEnabled = true
	if h := ComputeContentHash(rlsOn); h == base {
		t.Errorf("hash didn't change when enabling RLS")
	}
}

// The schema_ref_hash a planner/activity snapshot stores MUST be the same
// content_hash a fresh ComputeContentHash produces for the underlying schema.
// This is what L7c's GetAnnotated relies on to join the three shapes.
func TestSchemaRefHash_PlannerBindsToSchemaContentHash(t *testing.T) {
	snap := baselineSnap()
	snap.ContentHash = ComputeContentHash(snap)

	planner := &PlannerStatsSnapshot{
		SchemaRefHash: snap.ContentHash,
		Tables:        []TableSizingEntry{{Table: QualifiedName{Schema: "public", Name: "users"}}},
	}
	planner.ContentHash = ComputePlannerContentHash(planner)

	if planner.SchemaRefHash != ComputeContentHash(snap) {
		t.Errorf("planner.SchemaRefHash drifted from schema.ContentHash: %s vs %s",
			planner.SchemaRefHash, ComputeContentHash(snap))
	}

}

// The volatile reference counters (DatabaseXid/DatabaseMxid) advance on every
// capture even when nothing about the schema/sizing changed. They MUST stay out
// of the planner content hash, or bundle dedup (filesystem_store keys on
// content_hash) would write a fresh bundle on every single capture. This guards
// that invariant directly: two snapshots differing only in the reference counters
// hash identically. The stable raw relfrozenxid/relminmxid, by contrast, DO belong
// in the hash and are exercised by the other tests via the Tables payload.
func TestPlannerContentHash_IgnoresReferenceCounters(t *testing.T) {
	base := &PlannerStatsSnapshot{
		SchemaRefHash: "ddl-hash",
		Tables: []TableSizingEntry{{
			Table:  QualifiedName{Schema: "public", Name: "events"},
			Sizing: TableSizing{Reltuples: 1_000_000, RelfrozenXid: 100, RelminMxid: 1},
		}},
	}
	drifted := *base
	drifted.DatabaseXid = base.DatabaseXid + 5_000_000
	drifted.DatabaseMxid = base.DatabaseMxid + 5_000_000

	if got, want := ComputePlannerContentHash(&drifted), ComputePlannerContentHash(base); got != want {
		t.Errorf("reference counters leaked into planner content hash: %s vs %s", got, want)
	}

	// sanity: the stable frozen xids still move the hash, so they aren't silently dropped.
	moved := *base
	moved.Tables = []TableSizingEntry{{
		Table:  base.Tables[0].Table,
		Sizing: TableSizing{Reltuples: 1_000_000, RelfrozenXid: 200, RelminMxid: 1},
	}}
	if ComputePlannerContentHash(&moved) == ComputePlannerContentHash(base) {
		t.Errorf("relfrozenxid change did not affect planner content hash")
	}
}

// Bloat is derived from sizing+schema, so it must not perturb the planner
// content hash — otherwise a re-capture of unchanged stats post-bloat would
// look "new" and dedup would break across the format change. An entry with a
// populated Bloat must hash identically to the same entry without one.
func TestPlannerContentHash_IgnoresBloat(t *testing.T) {
	qual := QualifiedName{Schema: "public", Name: "users"}
	base := &PlannerStatsSnapshot{
		SchemaRefHash: "ddl-hash",
		Tables: []TableSizingEntry{{
			Table:  qual,
			Sizing: TableSizing{Relpages: 500, Reltuples: 10_000, TableSize: 4_096_000},
		}},
		Indexes: []IndexSizingEntry{{
			Table:  qual,
			Index:  "users_pkey",
			Sizing: IndexSizing{Relpages: 100, Reltuples: 10_000, Size: 819_200},
		}},
	}
	annotated := *base
	annotated.Tables = []TableSizingEntry{{
		Table:  base.Tables[0].Table,
		Sizing: base.Tables[0].Sizing,
		Bloat:  &BloatEstimate{BloatRatio: 2.1, ExpectedPages: 238, ActualPages: 500, AvgTupleWidth: 12, SizeBytes: 4_096_000},
	}}
	annotated.Indexes = []IndexSizingEntry{{
		Table:  base.Indexes[0].Table,
		Index:  base.Indexes[0].Index,
		Sizing: base.Indexes[0].Sizing,
		Bloat:  &BloatEstimate{BloatRatio: 4.2, ExpectedPages: 24, ActualPages: 100, AvgTupleWidth: 4, SizeBytes: 819_200},
	}}

	if got, want := ComputePlannerContentHash(&annotated), ComputePlannerContentHash(base); got != want {
		t.Errorf("bloat leaked into planner content hash: %s vs %s", got, want)
	}

	// sanity: the underlying sizing still moves the hash, so it isn't silently dropped.
	moved := *base
	moved.Indexes = []IndexSizingEntry{{
		Table:  base.Indexes[0].Table,
		Index:  base.Indexes[0].Index,
		Sizing: IndexSizing{Relpages: 200, Reltuples: 10_000, Size: 819_200},
	}}
	if ComputePlannerContentHash(&moved) == ComputePlannerContentHash(base) {
		t.Errorf("relpages change did not affect planner content hash")
	}
}

// Same invariant for activity snapshots. Two nodes producing different
// schema_ref values mean the cluster has drifted; under matched DDL the
// binding must be stable across nodes.
func TestSchemaRefHash_ActivityBindsToSchemaContentHash(t *testing.T) {
	snap := baselineSnap()
	snap.ContentHash = ComputeContentHash(snap)

	a := &ActivityStatsSnapshot{
		SchemaRefHash: snap.ContentHash,
		Node:          NodeIdentity{Source: "replica-1", IsStandby: true},
	}
	a.ContentHash = ComputeActivityContentHash(a)

	if a.SchemaRefHash != ComputeContentHash(snap) {
		t.Errorf("activity.SchemaRefHash drifted: %s vs %s",
			a.SchemaRefHash, ComputeContentHash(snap))
	}
}

// Two activity snapshots from different nodes against the same DDL must
// produce different content_hash values; node.source is in the canonical
// representation precisely so two replicas don't collide in the dedup index.
func TestActivityContentHash_DifferentiatesNodes(t *testing.T) {
	a := &ActivityStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "replica-1"},
		Tables:        []TableActivityEntry{},
	}
	b := *a
	b.Node = NodeIdentity{Source: "replica-2"}

	if ComputeActivityContentHash(a) == ComputeActivityContentHash(&b) {
		t.Errorf("activity hash didn't distinguish replicas")
	}
}

// Same node.source discrimination guarantee as activity stats: two nodes running the
// same workload must not collide in the dedup index.
func TestQueryStatsContentHash_DifferentiatesNodes(t *testing.T) {
	q := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "replica-1"},
		Queries: []QueryStatsEntry{{
			Fingerprint: "sha1:abc",
			Members:     []QueryStatsMember{{QueryID: 123, Calls: 5}},
			Calls:       5,
		}},
	}
	b := *q
	b.Node = NodeIdentity{Source: "replica-2"}

	if ComputeQueryStatsContentHash(q) == ComputeQueryStatsContentHash(&b) {
		t.Errorf("query stats hash didn't distinguish replicas")
	}
}

// Derived fields (fingerprint, canonical, cluster totals) must not move the digest.
func TestQueryStatsContentHash_IgnoresDerivedFields(t *testing.T) {
	members := []QueryStatsMember{{QueryID: 123, Calls: 10, TotalExecTimeMs: 100, Rows: 10}}
	q := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{{
			Fingerprint:     "sha1:abc",
			Canonical:       "SELECT id FROM users WHERE id = $1",
			Members:         members,
			Calls:           10,
			TotalExecTimeMs: 100,
			MeanExecTimeMs:  10,
			Rows:            10,
		}},
	}
	b := *q
	b.Queries = []QueryStatsEntry{{
		Fingerprint:     "sha1:zzz",
		Canonical:       "SELECT u.id FROM users u WHERE u.id = $1",
		Members:         members,
		Calls:           10,
		TotalExecTimeMs: 100,
		MeanExecTimeMs:  10.0000001,
		Rows:            10,
	}}

	if ComputeQueryStatsContentHash(q) != ComputeQueryStatsContentHash(&b) {
		t.Errorf("query stats hash moved on a derived-field-only change")
	}
}

// v1 froze reloptions at the last DDL change: a settings-only ALTER hashed identically,
// so the new body deduped away and vacuum advice kept reading stale storage parameters.
// v2 covers them; the structural hash still must not.
func TestContentHashV2_SensitiveToReloptions(t *testing.T) {
	base := baselineSnap()
	tuned := baselineSnap()
	tuned.Tables[0].Reloptions = []string{"autovacuum_vacuum_scale_factor=0.007"}

	if ComputeStructuralHash(tuned) != ComputeStructuralHash(base) {
		t.Errorf("structural hash moved on a reloptions-only change; it is the DDL identity")
	}
	if ComputeContentHashV2(tuned) == ComputeContentHashV2(base) {
		t.Errorf("v2 hash did not change after setting reloptions")
	}
}

// The compatibility promise: a database that sets no storage parameters hashes the same
// under both, so upgrading the binary never re-hashes it.
func TestContentHashV2_MatchesStructuralWithoutReloptions(t *testing.T) {
	snap := baselineSnap()
	if ComputeContentHashV2(snap) != ComputeStructuralHash(snap) {
		t.Errorf("v2 hash diverged from structural for a table with no reloptions")
	}

	// An empty (non-nil) slice must omit the key too, not marshal as [].
	snap.Tables[0].Reloptions = []string{}
	if ComputeContentHashV2(snap) != ComputeStructuralHash(snap) {
		t.Errorf("empty reloptions slice changed the v2 hash")
	}
}

// pg_class.reloptions orders by when each option was set, so two tables with the same
// settings can list them differently. That is not an identity difference.
func TestContentHashV2_ReloptionsOrderInsensitive(t *testing.T) {
	a := baselineSnap()
	a.Tables[0].Reloptions = []string{"autovacuum_vacuum_threshold=5000", "fillfactor=70"}
	b := baselineSnap()
	b.Tables[0].Reloptions = []string{"fillfactor=70", "autovacuum_vacuum_threshold=5000"}

	if ComputeContentHashV2(a) != ComputeContentHashV2(b) {
		t.Errorf("v2 hash depends on reloptions order")
	}
}

// Sorting must not reorder the caller's snapshot: the body is serialized after hashing,
// and reordering it would change the stored bytes.
func TestContentHashV2_DoesNotMutateReloptions(t *testing.T) {
	snap := baselineSnap()
	snap.Tables[0].Reloptions = []string{"fillfactor=70", "autovacuum_vacuum_threshold=5000"}
	ComputeContentHashV2(snap)

	if snap.Tables[0].Reloptions[0] != "fillfactor=70" {
		t.Errorf("hashing sorted the caller's reloptions in place: %v", snap.Tables[0].Reloptions)
	}
}

// The digest algorithm is a wire contract keyed on the doc's own format_version, not on
// the build that happens to be reading it: an old body must keep hashing the old way.
func TestDigestFor_DispatchesOnFormatVersion(t *testing.T) {
	for _, fv := range []int{0, 1} {
		snap := baselineSnap()
		snap.FormatVersion = fv
		snap.Tables[0].Reloptions = []string{"autovacuum_vacuum_scale_factor=0.007"}
		if DigestFor(snap) != ComputeStructuralHash(snap) {
			t.Errorf("format_version %d did not hash structurally", fv)
		}
	}

	snap := baselineSnap()
	snap.FormatVersion = 2
	snap.Tables[0].Reloptions = []string{"autovacuum_vacuum_scale_factor=0.007"}
	if DigestFor(snap) != ComputeContentHashV2(snap) {
		t.Errorf("format_version 2 did not hash with reloptions")
	}
}

// ComputeContentHash is what every current writer calls; it must stay the v1 digest until
// the writers move to DigestFor, or existing stores would silently re-hash.
func TestContentHash_StillStructural(t *testing.T) {
	snap := baselineSnap()
	snap.Tables[0].Reloptions = []string{"autovacuum_vacuum_scale_factor=0.007"}
	if ComputeContentHash(snap) != ComputeStructuralHash(snap) {
		t.Errorf("ComputeContentHash changed meaning; existing stores would re-hash")
	}
}

// GUCs ride the planner doc because the schema digest only moves on DDL: a
// postgresql.conf change would never rotate it, freezing the settings a reader sees.
// The planner doc re-hashes every capture anyway (reltuples move), so this costs no rows.
func TestPlannerContentHash_SensitiveToGUCs(t *testing.T) {
	base := &PlannerStatsSnapshot{SchemaRefHash: "abc"}
	tuned := &PlannerStatsSnapshot{
		SchemaRefHash: "abc",
		GUCs:          []GucSetting{{Name: "autovacuum_vacuum_scale_factor", Setting: "0.1"}},
	}
	if ComputePlannerContentHash(base) == ComputePlannerContentHash(tuned) {
		t.Error("planner hash ignored gucs")
	}

	other := &PlannerStatsSnapshot{
		SchemaRefHash: "abc",
		GUCs:          []GucSetting{{Name: "autovacuum_vacuum_scale_factor", Setting: "0.2"}},
	}
	if ComputePlannerContentHash(tuned) == ComputePlannerContentHash(other) {
		t.Error("planner hash ignored a guc value change")
	}
}

// The compatibility promise: planner docs captured before GUCs moved here carry none, so
// they must hash exactly as they did or every historical re-push 422s on digest mismatch.
func TestPlannerContentHash_OmitsEmptyGUCs(t *testing.T) {
	noGUCs := &PlannerStatsSnapshot{SchemaRefHash: "abc"}
	emptyGUCs := &PlannerStatsSnapshot{SchemaRefHash: "abc", GUCs: []GucSetting{}}
	if ComputePlannerContentHash(noGUCs) != ComputePlannerContentHash(emptyGUCs) {
		t.Error("an empty gucs slice changed the planner hash")
	}
}

// ---------------------------------------------------------------------------
// Query-stats digest: raw pg_stat_statements rows, not qshape's grouping.
//
// The digest is computed from QueryStatsEntry.Members — the (queryid, calls,
// total_exec_time, rows) tuples exactly as Postgres reported them — flattened
// out of their clusters and sorted by queryid. Everything qshape decides
// (fingerprint, canonical text, which rows merge, the per-cluster totals that
// follow from that) is deliberately excluded.
//
// The reason is that this digest is an identity, not a checksum: it is the
// dedup key in query_stats, the have-set key in sync, and the blob digest
// predict stores. If it moved every time qshape improved its normalizer, an
// unchanged workload would re-push in full and the old digests would strand on
// every remote that already held them.
//
// The tests below split into two halves that have to hold together: things
// that MUST NOT move the digest (grouping, ordering, derived text) and things
// that MUST (any number Postgres actually reported).
// ---------------------------------------------------------------------------

// Helper: a snapshot carrying exactly the given raw rows, all in one cluster.
// The cluster's own fingerprint and totals are left deliberately wrong in some
// callers below, to prove they are not consulted.
func queryStatsWith(members ...QueryStatsMember) *QueryStatsSnapshot {
	return &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries:       []QueryStatsEntry{{Fingerprint: "sha1:whatever", Members: members}},
	}
}

// The property the whole change exists for. An improved qshape normalizer
// collapses two shapes that used to be separate — an ORM alias variant, say —
// so the same two pg_stat_statements rows arrive as one cluster instead of
// two. Postgres reported identical numbers, so the capture is identical, so
// the digest must be identical. Before this, the merge rewrote every digest in
// the capture and dedup-busted the entire history.
func TestQueryStatsContentHash_SurvivesRegrouping(t *testing.T) {
	rowA := QueryStatsMember{QueryID: 100, Calls: 5, TotalExecTimeMs: 50, Rows: 5}
	rowB := QueryStatsMember{QueryID: 200, Calls: 7, TotalExecTimeMs: 70, Rows: 7}

	// before: two clusters, each with its own fingerprint and totals
	split := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{
			{Fingerprint: "sha1:aaa", Members: []QueryStatsMember{rowA}, Calls: 5, TotalExecTimeMs: 50, Rows: 5},
			{Fingerprint: "sha1:bbb", Members: []QueryStatsMember{rowB}, Calls: 7, TotalExecTimeMs: 70, Rows: 7},
		},
	}
	// after: one cluster, new fingerprint, summed totals — same raw rows
	merged := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{
			{Fingerprint: "sha1:ccc", Members: []QueryStatsMember{rowA, rowB}, Calls: 12, TotalExecTimeMs: 120, Rows: 12},
		},
	}

	if ComputeQueryStatsContentHash(split) != ComputeQueryStatsContentHash(merged) {
		t.Errorf("digest moved when clustering changed but the raw rows did not:\n split  = %s\n merged = %s",
			ComputeQueryStatsContentHash(split), ComputeQueryStatsContentHash(merged))
	}
}

// The mirror image: a normalizer that gets stricter and splits one cluster
// into two must not move the digest either. Same argument, opposite direction —
// worth pinning separately because a hash that folded on merge but not on
// split would pass the test above and still be wrong.
func TestQueryStatsContentHash_SurvivesSplitting(t *testing.T) {
	rowA := QueryStatsMember{QueryID: 100, Calls: 5, TotalExecTimeMs: 50, Rows: 5}
	rowB := QueryStatsMember{QueryID: 200, Calls: 7, TotalExecTimeMs: 70, Rows: 7}

	merged := queryStatsWith(rowA, rowB)
	split := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{
			{Fingerprint: "sha1:aaa", Members: []QueryStatsMember{rowA}},
			{Fingerprint: "sha1:bbb", Members: []QueryStatsMember{rowB}},
		},
	}

	if ComputeQueryStatsContentHash(merged) != ComputeQueryStatsContentHash(split) {
		t.Error("digest moved when one cluster split into two carrying the same rows")
	}
}

// Member order inside a cluster is qshape's business and, before qshape v0.3.0,
// followed pg_stat_statements' untied ORDER BY — equal-timing rows could
// permute between captures. The digest sorts by queryid precisely so that a
// permutation is not a change. qshape now sorts too, so this is belt and
// braces: it keeps the guarantee local rather than borrowed from a dependency.
func TestQueryStatsContentHash_IgnoresMemberOrder(t *testing.T) {
	rowA := QueryStatsMember{QueryID: 100, Calls: 5, TotalExecTimeMs: 50}
	rowB := QueryStatsMember{QueryID: 200, Calls: 7, TotalExecTimeMs: 70}

	if ComputeQueryStatsContentHash(queryStatsWith(rowA, rowB)) !=
		ComputeQueryStatsContentHash(queryStatsWith(rowB, rowA)) {
		t.Error("digest moved on member order alone")
	}
}

// Cluster order is equally arbitrary: qshape sorts clusters by total exec time,
// so a workload where two shapes swap places between captures reorders the
// Queries slice without changing a single number. Flattening before the sort is
// what makes that invisible.
func TestQueryStatsContentHash_IgnoresClusterOrder(t *testing.T) {
	rowA := QueryStatsMember{QueryID: 100, Calls: 5, TotalExecTimeMs: 50}
	rowB := QueryStatsMember{QueryID: 200, Calls: 7, TotalExecTimeMs: 70}

	first := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{
			{Fingerprint: "sha1:aaa", Members: []QueryStatsMember{rowA}},
			{Fingerprint: "sha1:bbb", Members: []QueryStatsMember{rowB}},
		},
	}
	second := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{
			{Fingerprint: "sha1:bbb", Members: []QueryStatsMember{rowB}},
			{Fingerprint: "sha1:aaa", Members: []QueryStatsMember{rowA}},
		},
	}

	if ComputeQueryStatsContentHash(first) != ComputeQueryStatsContentHash(second) {
		t.Error("digest moved when clusters were listed in a different order")
	}
}

// Counters are the payload. Each one has to bust the digest on its own,
// otherwise an active workload would dedup into a single history row forever
// and the whole point of repeated capture disappears. Tested field by field
// because a digest that tracked calls but silently dropped rows would look
// healthy on a busy database and lose data on a read-heavy one.
func TestQueryStatsContentHash_TracksEachCounter(t *testing.T) {
	base := QueryStatsMember{QueryID: 100, Calls: 5, TotalExecTimeMs: 50, Rows: 5}
	baseHash := ComputeQueryStatsContentHash(queryStatsWith(base))

	changed := map[string]QueryStatsMember{
		"calls":           {QueryID: 100, Calls: 6, TotalExecTimeMs: 50, Rows: 5},
		"total_exec_time": {QueryID: 100, Calls: 5, TotalExecTimeMs: 51, Rows: 5},
		"rows":            {QueryID: 100, Calls: 5, TotalExecTimeMs: 50, Rows: 6},
		"queryid":         {QueryID: 101, Calls: 5, TotalExecTimeMs: 50, Rows: 5},
	}
	for field, m := range changed {
		if ComputeQueryStatsContentHash(queryStatsWith(m)) == baseHash {
			t.Errorf("digest ignored a change to %s", field)
		}
	}
}

// Membership changes are real changes: a query entering or leaving the top 500
// alters what the capture observed, even if every surviving row is untouched.
// Dedup must not swallow that, or the capture where a new hot query appeared
// would never be written.
func TestQueryStatsContentHash_TracksMembershipChange(t *testing.T) {
	rowA := QueryStatsMember{QueryID: 100, Calls: 5}
	rowB := QueryStatsMember{QueryID: 200, Calls: 7}

	one := ComputeQueryStatsContentHash(queryStatsWith(rowA))
	two := ComputeQueryStatsContentHash(queryStatsWith(rowA, rowB))
	if one == two {
		t.Error("digest ignored a row appearing in the capture")
	}
}

// Two rows can legitimately share a queryid within one capture only in odd
// cases, but the sort must stay total regardless, or Go's unstable sort could
// order them differently between two runs over identical data. Calls is the
// tiebreak; this pins that a same-queryid pair hashes deterministically.
func TestQueryStatsContentHash_DeterministicOnDuplicateQueryIDs(t *testing.T) {
	a := QueryStatsMember{QueryID: 100, Calls: 5}
	b := QueryStatsMember{QueryID: 100, Calls: 9}

	if ComputeQueryStatsContentHash(queryStatsWith(a, b)) !=
		ComputeQueryStatsContentHash(queryStatsWith(b, a)) {
		t.Error("digest is order-dependent when two rows share a queryid")
	}
}

// schema_ref_hash binds a capture to the DDL it was taken against. The same
// workload observed before and after a migration is not the same fact, and
// GetAnnotated joins on this, so it belongs in the digest.
func TestQueryStatsContentHash_DifferentiatesSchemaRef(t *testing.T) {
	q := queryStatsWith(QueryStatsMember{QueryID: 100, Calls: 5})
	b := *q
	b.SchemaRefHash = "different-ddl"

	if ComputeQueryStatsContentHash(q) == ComputeQueryStatsContentHash(&b) {
		t.Error("digest ignored the bound schema_ref_hash")
	}
}

// An idle node with no qualifying queries still produces a capture, and two
// such captures must agree — this is the case where dedup is supposed to fire.
// nil and empty members are the same fact and must not hash apart.
func TestQueryStatsContentHash_EmptyCaptureIsStable(t *testing.T) {
	nilMembers := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries:       []QueryStatsEntry{{Fingerprint: "sha1:aaa"}},
	}
	emptyMembers := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries:       []QueryStatsEntry{{Fingerprint: "sha1:aaa", Members: []QueryStatsMember{}}},
	}
	noQueries := &QueryStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
	}

	if ComputeQueryStatsContentHash(nilMembers) != ComputeQueryStatsContentHash(emptyMembers) {
		t.Error("nil and empty member slices hashed differently")
	}
	if ComputeQueryStatsContentHash(nilMembers) != ComputeQueryStatsContentHash(noQueries) {
		t.Error("a cluster with no members did not hash as an empty capture")
	}
}

// The wire contract. HTTPStore.Put does not push the stored ContentHash — it
// recomputes the digest from the blob it is about to serialize, because predict
// re-derives the hash from the posted body and 422s on a mismatch. That only
// works if every input to the digest survives a JSON round trip: the moment the
// raw rows are dropped from the payload (json:"-", or computed capture-side and
// discarded) the remote sees a different capture than the one we hashed.
//
// This is the test that would have caught keeping Members out of the payload,
// which was the tempting version of this change.
func TestQueryStatsContentHash_ReproducibleFromPayload(t *testing.T) {
	original := &QueryStatsSnapshot{
		FormatVersion: FormatVersion,
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Queries: []QueryStatsEntry{{
			Fingerprint:     "sha1:abc",
			Canonical:       "SELECT id FROM users WHERE id = $1",
			Members:         []QueryStatsMember{{QueryID: 100, Calls: 5, TotalExecTimeMs: 50, Rows: 5}},
			Calls:           5,
			TotalExecTimeMs: 50,
			MeanExecTimeMs:  10,
			Rows:            5,
		}},
	}
	want := ComputeQueryStatsContentHash(original)

	body, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var received QueryStatsSnapshot
	if err := json.Unmarshal(body, &received); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := ComputeQueryStatsContentHash(&received); got != want {
		t.Errorf("digest not reproducible from the posted body:\n  local  %s\n  remote %s", want, got)
	}
}

// Temp blocks are content: a capture whose statements spilled differently is a different
// read, and the digest has to say so or a dedup would drop it. Absent (an older CLI, a
// pgss read that did not collect them) must leave the digest exactly as it was, or every
// stored blob's digest moves the day the field lands.
func TestQueryStatsContentHashCoversTempBlocks(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	base := queryStatsWith(QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3})
	baseDigest := ComputeQueryStatsContentHash(base)

	// Captured from the build before the temp-block fields existed. Anything that moves
	// this rewrites the digest of every already-stored blob.
	const preTempBlocks = "4d0278345588c74f401a22f8a36ccc30adcd229846647e9cfa617698354529c7"
	if baseDigest != preTempBlocks {
		t.Fatalf("a capture with no temp blocks must hash exactly as it did before the field existed:\n  got  %s\n  want %s", baseDigest, preTempBlocks)
	}

	for _, tc := range []struct {
		name   string
		member QueryStatsMember
	}{
		{"read", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, TempBlksRead: n(64)}},
		{"written", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, TempBlksWritten: n(64)}},
		// Zero is a real reading — this statement ran and spilled nothing — and must not
		// collapse onto the nil digest, which is the whole reason these are pointers.
		{"explicit zero read", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, TempBlksRead: n(0)}},
		{"explicit zero written", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, TempBlksWritten: n(0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ComputeQueryStatsContentHash(queryStatsWith(tc.member)) == baseDigest {
				t.Fatal("temp blocks do not affect the digest; a differing capture would dedup away")
			}
		})
	}

	// The two counters are distinct content, not one number under two names.
	read := queryStatsWith(QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(64)})
	written := queryStatsWith(QueryStatsMember{QueryID: 1, Calls: 10, TempBlksWritten: n(64)})
	if ComputeQueryStatsContentHash(read) == ComputeQueryStatsContentHash(written) {
		t.Fatal("read and written temp blocks hash alike; one of them is missing from the digest")
	}
}

// Members identical in every other counter must still sort deterministically, or the
// digest moves between captures of identical content.
func TestQueryStatsContentHashIsStableAcrossMemberOrder(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	for _, tc := range []struct {
		name string
		a, b QueryStatsMember
	}{
		{
			"both known",
			QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(1)},
			QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(2)},
		},
		// One member read the counters and the other did not, which is how a mixed
		// capture arrives. Ordering unknown against known is the branch that decides it.
		{
			"read unknown against known",
			QueryStatsMember{QueryID: 1, Calls: 10},
			QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(2)},
		},
		{
			"written unknown against known, read tied",
			QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(1)},
			QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(1), TempBlksWritten: n(2)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ComputeQueryStatsContentHash(queryStatsWith(tc.a, tc.b)) != ComputeQueryStatsContentHash(queryStatsWith(tc.b, tc.a)) {
				t.Fatal("member order moves the digest")
			}
		})
	}
}

// The cluster-level temp sums are derived from the members, so they must not move the
// digest: two decodes of the same rows differ only in what qshape computed from them.
func TestQueryStatsContentHashIgnoresClusterTempSums(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	base := queryStatsWith(QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(64)})
	derived := queryStatsWith(QueryStatsMember{QueryID: 1, Calls: 10, TempBlksRead: n(64)})
	derived.Queries[0].TempBlksRead = n(64)
	derived.Queries[0].TempBlksWritten = n(8)
	if ComputeQueryStatsContentHash(base) != ComputeQueryStatsContentHash(derived) {
		t.Fatal("a cluster-level sum moved the digest; only the raw members are content")
	}
}

// The shared-block fields are inside the digest, so each must be a tiebreaker in the
// comparator: the sort is unstable, and a field it does not order can move the digest of
// identical content. The two timings and stddev are the ones added last.
func TestQueryStatsContentHashCoversBlockTimings(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	base := queryStatsWith(QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3})
	baseDigest := ComputeQueryStatsContentHash(base)

	for _, tc := range []struct {
		name   string
		member QueryStatsMember
	}{
		{"read time", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, SharedBlkReadTimeMs: f(12.5)}},
		{"write time", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, SharedBlkWriteTimeMs: f(12.5)}},
		{"stddev", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, StddevExecTimeMs: f(3)}},
		// An explicit 0 is a reading: with track_io_timing ON, a statement that never
		// waited on a block genuinely spent 0 ms there, and a statement executed once
		// genuinely has no deviation. Neither may collapse onto the nil digest.
		{"explicit zero read time", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, SharedBlkReadTimeMs: f(0)}},
		{"explicit zero stddev", QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, StddevExecTimeMs: f(0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ComputeQueryStatsContentHash(queryStatsWith(tc.member)) == baseDigest {
				t.Fatal("field does not affect the digest; a differing capture would dedup away")
			}
		})
	}

	// Two members alike in every ordered counter but one timing must still hash the same
	// whichever order they arrive in. This is the property the tiebreakers exist for.
	for _, tie := range []struct {
		name string
		a, b QueryStatsMember
	}{
		{"read time",
			QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, SharedBlkReadTimeMs: f(1)},
			QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, SharedBlkReadTimeMs: f(2)}},
		// stddev is the LAST comparator arm, so it is the one whose absence would be silent.
		{"stddev",
			QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, StddevExecTimeMs: f(1)},
			QueryStatsMember{QueryID: 1, Calls: 10, TotalExecTimeMs: 5, Rows: 3, StddevExecTimeMs: f(2)}},
	} {
		t.Run("order/"+tie.name, func(t *testing.T) {
			if ComputeQueryStatsContentHash(queryStatsWith(tie.a, tie.b)) !=
				ComputeQueryStatsContentHash(queryStatsWith(tie.b, tie.a)) {
				t.Fatal("member order moves the digest; the field is not breaking the tie")
			}
		})
	}
}
