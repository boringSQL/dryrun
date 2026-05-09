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

// Runtime stats must stay outside the hash — confirms the new fields
// didn't accidentally inherit through some stats-bearing path.
func TestContentHash_StableAcrossStatsOnlyChanges(t *testing.T) {
	base := ComputeContentHash(baselineSnap())

	snap := baselineSnap()
	snap.Tables[0].Stats = &TableStats{Reltuples: 1234, DeadTuples: 9}
	snap.Tables[0].Columns[0].Stats = &ColumnStats{}

	if h := ComputeContentHash(snap); h != base {
		t.Errorf("hash drifted on stats-only change: base=%s got=%s", base, h)
	}
}
