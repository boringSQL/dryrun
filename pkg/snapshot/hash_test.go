package snapshot

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
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

// Compat pin: an unconditional key would rewrite every stored blob's digest and 422 historical re-push.
func TestActivityContentHash_OmitsAbsentFieldsSoOldDigestsSurvive(t *testing.T) {
	a := &ActivityStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "replica-1"},
		Tables:        []TableActivityEntry{},
	}
	const preDatabaseScopedFields = "28e0808538e6e6ba47026a7959ec42866daeb8797f4a788dc7aa6482fbde52de"
	if got := ComputeActivityContentHash(a); got != preDatabaseScopedFields {
		t.Fatalf("a capture with none of the three new fields must hash exactly as it did before they existed:\n  got  %s\n  want %s", got, preDatabaseScopedFields)
	}
}

func TestActivityContentHash_IncludesDatabaseScopedFields(t *testing.T) {
	base := func() *ActivityStatsSnapshot {
		return &ActivityStatsSnapshot{SchemaRefHash: "sref", Node: NodeIdentity{Source: "primary"}}
	}

	a := base()
	baseHash := ComputeActivityContentHash(a)

	withDB := base()
	withDB.Database = &DatabaseActivity{Deadlocks: 1}
	if ComputeActivityContentHash(withDB) == baseHash {
		t.Error("Database field change did not affect the content hash")
	}

	slotsOK := true
	withSlots := base()
	withSlots.ReplicationSlots = []ReplicationSlotActivity{{SlotName: "s1", Active: true}}
	withSlots.ReplicationSlotsReadOK = &slotsOK
	if ComputeActivityContentHash(withSlots) == baseHash {
		t.Error("ReplicationSlots field change did not affect the content hash")
	}

	withSlotsCheckedEmpty := base()
	withSlotsCheckedEmpty.ReplicationSlotsReadOK = &slotsOK
	if ComputeActivityContentHash(withSlotsCheckedEmpty) == baseHash {
		t.Error("ReplicationSlotsReadOK alone (zero slots, but checked) did not affect the content hash")
	}

	withCheckpointer := base()
	withCheckpointer.Checkpointer = &CheckpointerActivity{CheckpointsTimed: 1, View: "pg_stat_bgwriter"}
	if ComputeActivityContentHash(withCheckpointer) == baseHash {
		t.Error("Checkpointer field change did not affect the content hash")
	}

	peersOK := true
	streaming := "streaming"
	withPeers := base()
	withPeers.ReplicationPeers = []ReplicationPeerActivity{{ApplicationName: "r1", State: &streaming}}
	withPeers.ReplicationPeersReadOK = &peersOK
	if ComputeActivityContentHash(withPeers) == baseHash {
		t.Error("ReplicationPeers field change did not affect the content hash")
	}

	withPeersCheckedEmpty := base()
	withPeersCheckedEmpty.ReplicationPeersReadOK = &peersOK
	if ComputeActivityContentHash(withPeersCheckedEmpty) == baseHash {
		t.Error("ReplicationPeersReadOK alone (zero peers, but checked) did not affect the content hash")
	}
}

func TestActivityContentHash_NilAndEmptyReplicationPeersHashIdentically(t *testing.T) {
	readOK := true
	nilPeers := &ActivityStatsSnapshot{
		SchemaRefHash: "sref", Node: NodeIdentity{Source: "primary"},
		ReplicationPeers: nil, ReplicationPeersReadOK: &readOK,
	}
	emptyPeers := &ActivityStatsSnapshot{
		SchemaRefHash: "sref", Node: NodeIdentity{Source: "primary"},
		ReplicationPeers: []ReplicationPeerActivity{}, ReplicationPeersReadOK: &readOK,
	}
	if ComputeActivityContentHash(nilPeers) != ComputeActivityContentHash(emptyPeers) {
		t.Error("nil vs non-nil-but-empty ReplicationPeers produced different digests; a decode round-trip cannot reproduce this")
	}
}

func TestActivityContentHash_NilAndEmptyReplicationSlotsHashIdentically(t *testing.T) {
	readOK := true
	nilSlots := &ActivityStatsSnapshot{
		SchemaRefHash: "sref", Node: NodeIdentity{Source: "primary"},
		ReplicationSlots: nil, ReplicationSlotsReadOK: &readOK,
	}
	emptySlots := &ActivityStatsSnapshot{
		SchemaRefHash: "sref", Node: NodeIdentity{Source: "primary"},
		ReplicationSlots: []ReplicationSlotActivity{}, ReplicationSlotsReadOK: &readOK,
	}
	if ComputeActivityContentHash(nilSlots) != ComputeActivityContentHash(emptySlots) {
		t.Error("nil vs non-nil-but-empty ReplicationSlots produced different digests; a decode round-trip cannot reproduce this")
	}
}

func TestActivityContentHash_ReproducibleFromPayload(t *testing.T) {
	readOK := true
	resetAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	original := &ActivityStatsSnapshot{
		SchemaRefHash: "sref",
		Node:          NodeIdentity{Source: "primary"},
		Tables:        []TableActivityEntry{},
		Database:      &DatabaseActivity{Deadlocks: 3, XactCommit: 100, StatsReset: &resetAt},
		ReplicationSlots: []ReplicationSlotActivity{
			{SlotName: "s1", SlotType: "physical", Active: true},
		},
		ReplicationSlotsReadOK: &readOK,
		Checkpointer:           &CheckpointerActivity{CheckpointsTimed: 5, CheckpointsReq: 1, View: "pg_stat_bgwriter"},
	}
	want := ComputeActivityContentHash(original)

	body, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var received ActivityStatsSnapshot
	if err := json.Unmarshal(body, &received); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := ComputeActivityContentHash(&received); got != want {
		t.Errorf("digest not reproducible from the posted body:\n  local  %s\n  remote %s", want, got)
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

// Membership changes are real changes: a query entering or leaving the row cap
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

// RowCap is a capture parameter, not an observed counter; it must not move the digest.
func TestQueryStatsContentHash_IgnoresRowCap(t *testing.T) {
	row := QueryStatsMember{QueryID: 100, Calls: 5, TotalExecTimeMs: 50, Rows: 5}

	withoutCap := queryStatsWith(row)
	withCap := queryStatsWith(row)
	withCap.RowCap = 1000

	if ComputeQueryStatsContentHash(withoutCap) != ComputeQueryStatsContentHash(withCap) {
		t.Error("digest moved when only RowCap changed")
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

// CaptureRuleVersion is a header field, so it must not participate in the
// content hash: hashing it would move every stored digest and break dedup
// against rows already written locally and pushed to cloud.
func TestCaptureRuleVersionIsNotHashed(t *testing.T) {
	mk := func(v int) *QueryStatsSnapshot {
		return &QueryStatsSnapshot{
			SchemaRefHash:      "abc",
			CaptureRuleVersion: v,
			Node:               NodeIdentity{Source: "primary"},
			Queries: []QueryStatsEntry{{
				Fingerprint: "f", Calls: 1, TotalExecTimeMs: 1,
				Members: []QueryStatsMember{{QueryID: 7, Calls: 1, TotalExecTimeMs: 1}},
			}},
		}
	}
	if a, b := ComputeQueryStatsContentHash(mk(0)), ComputeQueryStatsContentHash(mk(2)); a != b {
		t.Errorf("the header field moved the content hash:\n  v0: %s\n  v2: %s", a, b)
	}
}

// 0 must stay absent on the wire so payloads written before the field existed
// remain byte-identical, and must read back as 0 = unknown rather than as a
// version that happens to match.
func TestCaptureRuleVersionWireRoundTrip(t *testing.T) {
	for _, v := range []int{0, 2} {
		data, err := json.Marshal(&QueryStatsSnapshot{CaptureRuleVersion: v})
		if err != nil {
			t.Fatal(err)
		}
		present := strings.Contains(string(data), "capture_rule_version")
		if present != (v != 0) {
			t.Errorf("v=%d: key present=%v, want %v (omitempty keeps legacy bytes identical)", v, present, v != 0)
		}
		var back QueryStatsSnapshot
		if err := json.Unmarshal(data, &back); err != nil {
			t.Fatal(err)
		}
		if back.CaptureRuleVersion != v {
			t.Errorf("v=%d round-tripped to %d", v, back.CaptureRuleVersion)
		}
	}
}

// The node identity fingerprint (postmaster_start_time / server_addr) rides
// inside the payload but must stay OUTSIDE both content hashes. If it ever
// entered one, a node restart would change the digest of otherwise-identical
// stats: local dedup would stop collapsing them, and every remote push would
// upload the same content under a new digest forever. Both hash functions
// canonicalize an explicit field list taking only Node.Source, and these tests
// exist so that stays true when fields are added.
func TestActivityContentHash_IgnoresNodeFingerprint(t *testing.T) {
	started := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	a := &ActivityStatsSnapshot{
		SchemaRefHash: "sr",
		Node:          NodeIdentity{Source: "primary", PgVersion: "17.0"},
		Tables:        []TableActivityEntry{{Table: QualifiedName{Schema: "public", Name: "users"}}},
	}
	before := ComputeActivityContentHash(a)

	a.Node.PostmasterStartTime = &started
	a.Node.ServerAddr = "10.0.0.1"
	if got := ComputeActivityContentHash(a); got != before {
		t.Errorf("fingerprint changed the activity digest:\n before %s\n after  %s", before, got)
	}

	// a restart on the same node must not move it either
	restarted := started.Add(time.Hour)
	a.Node.PostmasterStartTime = &restarted
	a.Node.ServerAddr = "10.0.0.2"
	if got := ComputeActivityContentHash(a); got != before {
		t.Errorf("a changed fingerprint moved the activity digest: %s != %s", got, before)
	}

	// the label is identity and must still count
	a.Node.Source = "replica"
	if got := ComputeActivityContentHash(a); got == before {
		t.Error("node source no longer affects the activity digest")
	}
}

func TestQueryStatsContentHash_IgnoresNodeFingerprint(t *testing.T) {
	started := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	q := &QueryStatsSnapshot{
		SchemaRefHash: "sr",
		Node:          NodeIdentity{Source: "primary", PgVersion: "17.0"},
		Queries: []QueryStatsEntry{{
			Fingerprint: "sha1:abc",
			Members:     []QueryStatsMember{{QueryID: 42, Calls: 5}},
			Calls:       5,
		}},
	}
	before := ComputeQueryStatsContentHash(q)

	q.Node.PostmasterStartTime = &started
	q.Node.ServerAddr = "10.0.0.1"
	if got := ComputeQueryStatsContentHash(q); got != before {
		t.Errorf("fingerprint changed the query-stats digest:\n before %s\n after  %s", before, got)
	}
}

// The wire shape has to survive a round trip, since a pulled snapshot is
// decoded before its digest is recomputed by the receiving store.
func TestNodeFingerprint_SurvivesJSONRoundTrip(t *testing.T) {
	started := time.Date(2026, 8, 20, 9, 0, 0, 123456000, time.UTC)
	in := NodeIdentity{Source: "primary", PostmasterStartTime: &started, ServerAddr: "10.0.0.1"}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out NodeIdentity
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.PostmasterStartTime == nil || !out.PostmasterStartTime.Equal(started) {
		t.Errorf("start time did not round trip: %v", out.PostmasterStartTime)
	}
	if out.ServerAddr != in.ServerAddr {
		t.Errorf("server addr %q, want %q", out.ServerAddr, in.ServerAddr)
	}

	// absent on an older capture: nil, never a zero time that would read as a
	// real observation
	var legacy NodeIdentity
	if err := json.Unmarshal([]byte(`{"source":"primary","is_standby":false}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.PostmasterStartTime != nil {
		t.Errorf("missing field decoded to %v, want nil", legacy.PostmasterStartTime)
	}
}

// --- v3: partition children out of the digest -------------------------------

// Capture-shaped: introspect.sql's fetch-tables takes relkind 'r' and 'p' with no
// relispartition filter, so a real snapshot carries each partition twice — once in the
// parent's PartitionInfo.Children and once as its own tables[] entry, columns and all.
// A fixture that only sets PartitionInfo tests a schema no capture ever produces.
func partitionedSnap(children ...PartitionChild) *SchemaSnapshot {
	snap := baselineSnap()
	snap.FormatVersion = 3
	snap.Tables[0].PartitionInfo = &PartitionInfo{
		Strategy: PartitionRange,
		Key:      "created_at",
		Children: children,
	}
	for _, c := range children {
		snap.Tables = append(snap.Tables, Table{
			Schema:  c.Schema,
			Name:    c.Name,
			Columns: snap.Tables[0].Columns,
			Indexes: []Index{{
				Name:       c.Name + "_pkey",
				Columns:    []string{"id"},
				IndexType:  "btree",
				IsUnique:   true,
				IsPrimary:  true,
				Definition: "CREATE UNIQUE INDEX " + c.Name + "_pkey ON " + c.Schema + "." + c.Name + " USING btree (id)",
				IsValid:    true,
				IsReady:    true,
			}},
		})
	}
	return snap
}

func child(name, bound string) PartitionChild {
	return PartitionChild{Schema: "public", Name: name, Bound: bound}
}

// The reason v3 exists: a pg_partman rotation is not DDL, and under v2 it minted a
// full uncompressed schema row per rotation in a table nothing prunes cheaply.
func TestContentHashV3_IgnoresPartitionChildren(t *testing.T) {
	before := partitionedSnap(child("users_2026_01", "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"))
	after := partitionedSnap(
		child("users_2026_01", "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"),
		child("users_2026_02", "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"),
	)

	if ComputeContentHashV3(before) != ComputeContentHashV3(after) {
		t.Errorf("adding a partition moved the v3 digest")
	}
	// Guard the fixture: v2 must still see the rotation, or the test proves nothing.
	if ComputeContentHashV2(before) == ComputeContentHashV2(after) {
		t.Fatal("fixture is not exercising the change; v2 digest did not move either")
	}
}

// The child's own tables[] entry must leave the digest alone too; stripping only the
// parent's Children list would still re-hash on every rotation.
func TestContentHashV3_IgnoresPartitionChildTableEntries(t *testing.T) {
	rotated := partitionedSnap(child("users_2026_01", "a"), child("users_2026_02", "b"))
	// same doc, but with the parent's Children list already stripped: if the child
	// tables[] entries still counted, this would differ from the one-child schema.
	unpartitioned := baselineSnap()
	unpartitioned.FormatVersion = 3

	bare := partitionedSnap()
	if ComputeContentHashV3(rotated) != ComputeContentHashV3(bare) {
		t.Errorf("a partition's own tables[] entry moved the v3 digest")
	}
	// Guard: the child entries are really in the fixture and really move v2.
	if len(rotated.Tables) != 3 {
		t.Fatalf("fixture lost the child tables: %d tables", len(rotated.Tables))
	}
	if ComputeContentHashV2(rotated) == ComputeContentHashV2(bare) {
		t.Fatal("fixture is not exercising the change; v2 digest did not move either")
	}
	// The parent still participates: this is not "partitioned tables are unhashed".
	if ComputeContentHashV3(rotated) == ComputeContentHashV3(unpartitioned) {
		t.Errorf("v3 dropped the partitioned parent itself, not just its children")
	}
}

// A table that merely shares a name with nothing is untouched; only tables actually
// listed as some parent's child are dropped, and the list is derived from the doc so a
// stored snapshot always re-hashes identically.
func TestContentHashV3_KeepsUnrelatedTables(t *testing.T) {
	base := partitionedSnap(child("users_2026_01", "a"))
	withOther := partitionedSnap(child("users_2026_01", "a"))
	withOther.Tables = append(withOther.Tables, Table{Schema: "public", Name: "orders"})

	if ComputeContentHashV3(base) == ComputeContentHashV3(withOther) {
		t.Errorf("adding an ordinary table did not move the v3 digest")
	}
}

// Detaching every child is still not DDL on the parent.
func TestContentHashV3_IgnoresDroppedPartitions(t *testing.T) {
	full := partitionedSnap(child("users_2026_01", "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"))
	empty := partitionedSnap()

	if ComputeContentHashV3(full) != ComputeContentHashV3(empty) {
		t.Errorf("dropping a partition moved the v3 digest")
	}
}

// Strategy and key are the partition DDL; changing either is a real schema change and
// must still rotate the digest, or PARTITION BY changes would dedup away.
func TestContentHashV3_SensitiveToStrategyAndKey(t *testing.T) {
	base := ComputeContentHashV3(partitionedSnap())

	rekeyed := partitionedSnap()
	rekeyed.Tables[0].PartitionInfo.Key = "tenant_id"
	if ComputeContentHashV3(rekeyed) == base {
		t.Errorf("partition key change did not move the v3 digest")
	}

	restrategied := partitionedSnap()
	restrategied.Tables[0].PartitionInfo.Strategy = PartitionHash
	if ComputeContentHashV3(restrategied) == base {
		t.Errorf("partition strategy change did not move the v3 digest")
	}
}

// A partitioned table is the only thing v3 changes: every unpartitioned schema keeps
// its v2 bytes, so the one-time re-hash is scoped to partitioned projects. Pinned to a
// literal, not to ComputeContentHashV2 — a change to tableToStructural would move both
// and a relative comparison would still pass while every stored digest re-derived.
func TestContentHashV3_MatchesV2WithoutPartitions(t *testing.T) {
	snap := baselineSnap()
	snap.Tables[0].Reloptions = []string{"fillfactor=70"}

	// Captured from the build before v3 existed.
	const preV3 = "b6d505183d5cce0e1009143c9cdce0f0cdc16f91252bfbf0ed21a47f95304393"
	if got := ComputeContentHashV2(snap); got != preV3 {
		t.Fatalf("the v2 digest itself moved; every stored schema row re-derives:\n  got  %s\n  want %s", got, preV3)
	}
	if ComputeContentHashV3(snap) != preV3 {
		t.Errorf("v3 re-hashed a schema with no partitioned table")
	}
}

// v3 inherits v2's reloptions coverage; it strips children, it does not revert to v1.
func TestContentHashV3_StillCoversReloptions(t *testing.T) {
	base := partitionedSnap()
	tuned := partitionedSnap()
	tuned.Tables[0].Reloptions = []string{"autovacuum_enabled=off"}

	if ComputeContentHashV3(base) == ComputeContentHashV3(tuned) {
		t.Errorf("v3 lost the v2 reloptions coverage")
	}
}

// The wire contract: a doc stamped v2 keeps hashing children in even on a build that
// knows v3, or every stored digest would silently re-derive on read.
func TestDigestFor_V2DocKeepsChildrenInDigest(t *testing.T) {
	before := partitionedSnap(child("users_2026_01", "a"))
	before.FormatVersion = 2
	after := partitionedSnap(child("users_2026_01", "a"), child("users_2026_02", "b"))
	after.FormatVersion = 2

	if DigestFor(before) == DigestFor(after) {
		t.Errorf("a v2 doc was hashed under the v3 rules")
	}
	if DigestFor(before) != ComputeContentHashV2(before) {
		t.Errorf("format_version 2 did not hash with the v2 algorithm")
	}
}

func TestDigestFor_V3DispatchesToV3(t *testing.T) {
	snap := partitionedSnap(child("users_2026_01", "a"))
	if DigestFor(snap) != ComputeContentHashV3(snap) {
		t.Errorf("format_version 3 did not hash with the v3 algorithm")
	}
	// Forward compatibility: an unknown newer generation falls back to the newest
	// algorithm this build has, matching how the v2 branch behaved before v3 landed.
	snap.FormatVersion = 99
	if DigestFor(snap) != ComputeContentHashV3(snap) {
		t.Errorf("format_version 99 did not fall back to the newest known algorithm")
	}
}

// What a fresh capture stamps must be what DigestFor hashes it with; a mismatch is
// exactly the case predict answers 422 on (internal/history/http_store.go:107).
func TestFormatVersionConstant_MatchesNewestDigest(t *testing.T) {
	snap := partitionedSnap(child("users_2026_01", "a"))
	snap.FormatVersion = FormatVersion
	if DigestFor(snap) != ComputeContentHashV3(snap) {
		t.Errorf("FormatVersion %d does not dispatch to the newest digest", FormatVersion)
	}
}

// The mid-level of a sub-partitioned tree is both a child and a parent. pg_inherits
// emits parent→mid and mid→leaf, so both land in the strip set and a rotation at any
// level is invisible. Nothing else exercises the mid-level claim.
func TestContentHashV3_IgnoresSubPartitionRotation(t *testing.T) {
	tree := func(leaves ...string) *SchemaSnapshot {
		snap := baselineSnap()
		snap.FormatVersion = 3
		snap.Tables[0].PartitionInfo = &PartitionInfo{
			Strategy: PartitionRange,
			Key:      "created_at",
			Children: []PartitionChild{child("users_2026", "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")},
		}
		mid := Table{
			Schema:  "public",
			Name:    "users_2026",
			Columns: snap.Tables[0].Columns,
			PartitionInfo: &PartitionInfo{
				Strategy: PartitionHash,
				Key:      "tenant_id",
			},
		}
		for _, l := range leaves {
			mid.PartitionInfo.Children = append(mid.PartitionInfo.Children, child(l, "FOR VALUES WITH (MODULUS 4, REMAINDER 0)"))
		}
		snap.Tables = append(snap.Tables, mid)
		for _, l := range leaves {
			snap.Tables = append(snap.Tables, Table{Schema: "public", Name: l, Columns: snap.Tables[0].Columns})
		}
		return snap
	}

	one, two := tree("users_2026_p0"), tree("users_2026_p0", "users_2026_p1")
	if ComputeContentHashV3(one) != ComputeContentHashV3(two) {
		t.Errorf("a leaf added under a mid-level partition moved the v3 digest")
	}
	if ComputeContentHashV2(one) == ComputeContentHashV2(two) {
		t.Fatal("fixture is not exercising the change; v2 digest did not move either")
	}
}

// The strip set is built from every table before any is filtered, so a document that
// lists a child before its parent strips identically. A future single-pass rewrite of
// partitionChildTables would break this silently.
func TestContentHashV3_IndependentOfTableOrder(t *testing.T) {
	forward := partitionedSnap(child("users_2026_01", "a"), child("users_2026_02", "b"))
	reversed := partitionedSnap(child("users_2026_01", "a"), child("users_2026_02", "b"))
	slices.Reverse(reversed.Tables)

	if ComputeContentHashV3(forward) != ComputeContentHashV3(reversed) {
		t.Errorf("v3 digest depends on the order tables appear in the document")
	}
}

// DETACH PARTITION, which is what pg_partman retention does by default
// (retention_keep_table = true). The child leaves pg_inherits and therefore leaves
// PartitionInfo.Children, but stays in fetch-tables as a plain relkind='r' row — so it
// re-enters the digest as an ordinary table. That is the right semantics (a detached
// partition IS a standalone table) but it means retention still rotates the digest.
// Pinned so nobody "fixes" it by name-matching partition children heuristically.
func TestContentHashV3_DetachedPartitionCountsAsAnOrdinaryTable(t *testing.T) {
	attached := partitionedSnap(child("users_2026_01", "a"))

	detached := partitionedSnap()
	detached.Tables = append(detached.Tables, attached.Tables[1]) // same Table, no longer listed as a child

	if ComputeContentHashV3(attached) == ComputeContentHashV3(detached) {
		t.Errorf("DETACH PARTITION did not move the v3 digest; the child is no longer a partition")
	}
	if ComputeContentHashV3(detached) == ComputeContentHashV3(partitionedSnap()) {
		t.Errorf("a detached partition was stripped from the digest; it is an ordinary table now")
	}
}
