package schema

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
