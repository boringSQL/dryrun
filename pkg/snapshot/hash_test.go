package snapshot

import "testing"

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
