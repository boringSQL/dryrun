package audit

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func testSnap() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: "test",
	}
}

func TestDuplicateIndexes(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			{Name: "idx_a", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true},
			{Name: "idx_b", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true},
		},
	}}
	findings := checkDuplicateIndexes(snap)
	if len(findings) != 1 {
		t.Errorf("expected 1 duplicate finding, got %d", len(findings))
	}
}

func TestRedundantIndexes(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			{Name: "idx_short", Columns: []string{"user_id"}, IndexType: "btree"},
			{Name: "idx_long", Columns: []string{"user_id", "created_at"}, IndexType: "btree"},
		},
	}}
	findings := checkRedundantIndexes(snap)
	if len(findings) != 1 {
		t.Errorf("expected 1 redundant finding, got %d", len(findings))
	}
}

func TestCircularFK(t *testing.T) {
	a := new("public.b")
	b := new("public.a")
	snap := testSnap()
	snap.Tables = []schema.Table{
		{Schema: "public", Name: "a", Constraints: []schema.Constraint{
			{Kind: schema.ConstraintForeignKey, Columns: []string{"b_id"}, FKTable: a, FKColumns: []string{"id"}},
		}},
		{Schema: "public", Name: "b", Constraints: []schema.Constraint{
			{Kind: schema.ConstraintForeignKey, Columns: []string{"a_id"}, FKTable: b, FKColumns: []string{"id"}},
		}},
	}
	findings := checkCircularFKs(snap)
	if len(findings) == 0 {
		t.Error("expected circular FK finding")
	}
}

func TestOrphanTable(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{
		{Schema: "public", Name: "isolated"},
	}
	findings := checkOrphanTables(snap)
	if len(findings) != 1 {
		t.Errorf("expected 1 orphan, got %d", len(findings))
	}
}

func TestFKTypeMismatch(t *testing.T) {
	fkTable := new("public.users")
	snap := testSnap()
	snap.Tables = []schema.Table{
		{Schema: "public", Name: "users", Columns: []schema.Column{{Name: "id", TypeName: "bigint"}}},
		{Schema: "public", Name: "orders",
			Columns: []schema.Column{{Name: "user_id", TypeName: "integer"}},
			Constraints: []schema.Constraint{{
				Kind: schema.ConstraintForeignKey, Columns: []string{"user_id"},
				FKTable: fkTable, FKColumns: []string{"id"},
			}},
		},
	}
	findings := checkFKTypeMismatch(snap)
	if len(findings) != 1 {
		t.Errorf("expected 1 type mismatch, got %d", len(findings))
	}
}

func TestBoolPrefix(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "user",
		Columns: []schema.Column{
			{Name: "active", TypeName: "boolean"},
			{Name: "is_admin", TypeName: "boolean"},
		},
	}}
	findings := checkBoolPrefix(snap)
	if len(findings) != 1 || findings[0].Message == "" {
		t.Errorf("expected 1 bool prefix finding for 'active', got %d", len(findings))
	}
}

func TestReservedWord(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "user",
		Columns: []schema.Column{{Name: "order", TypeName: "text"}},
	}}
	findings := checkReservedWords(snap)
	found := false
	for _, f := range findings {
		if f.Message != "" && f.Rule == "naming/reserved" {
			found = true
		}
	}
	if !found {
		t.Error("expected reserved word finding for 'user' or 'order'")
	}
}

func TestBloatedIndexRule(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Columns: []schema.Column{{Name: "id", TypeName: "integer"}},
		Indexes: []schema.Index{{
			Name: "idx_orders_id", Columns: []string{"id"}, IndexType: "btree",
			Stats: &schema.IndexStats{Relpages: 5000, Reltuples: 100000},
		}},
	}}
	config := DefaultConfig()
	findings := checkBloatedIndexes(snap, &config)
	if len(findings) != 1 {
		t.Fatalf("expected 1 bloated finding, got %d", len(findings))
	}
	if findings[0].Rule != "indexes/bloated" {
		t.Errorf("expected rule indexes/bloated, got %s", findings[0].Rule)
	}
	if findings[0].DDLFix == nil || *findings[0].DDLFix == "" {
		t.Error("expected DDL fix")
	}
}

func TestBloatedIndexRule_BelowThreshold(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Columns: []schema.Column{{Name: "id", TypeName: "integer"}},
		Indexes: []schema.Index{{
			Name: "idx_orders_id", Columns: []string{"id"}, IndexType: "btree",
			// ~163 expected pages for 100k int tuples, 200 actual → ratio ~1.2
			Stats: &schema.IndexStats{Relpages: 200, Reltuples: 100000},
		}},
	}}
	config := DefaultConfig()
	findings := checkBloatedIndexes(snap, &config)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings below threshold, got %d", len(findings))
	}
}

func TestVacuumLargeTableDefaults_LargeTableNoOverrides(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "events",
		Stats: &schema.TableStats{Reltuples: 5_000_000, DeadTuples: 100},
	}}
	findings := checkVacuumLargeTableDefaults(snap)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	f := findings[0]
	if f.Rule != "vacuum/large_table_defaults" {
		t.Errorf("expected rule vacuum/large_table_defaults, got %s", f.Rule)
	}
	if f.DDLFix == nil || *f.DDLFix == "" {
		t.Error("expected DDL fix")
	}
	if len(f.Tables) != 1 || f.Tables[0] != "public.events" {
		t.Errorf("expected tables [public.events], got %v", f.Tables)
	}
}

func TestVacuumLargeTableDefaults_SmallTable(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "small",
		Stats: &schema.TableStats{Reltuples: 500_000, DeadTuples: 100},
	}}
	findings := checkVacuumLargeTableDefaults(snap)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings for <1M rows, got %d", len(findings))
	}
}

func TestVacuumLargeTableDefaults_HasOverrides(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "tuned",
		Stats:      &schema.TableStats{Reltuples: 5_000_000, DeadTuples: 100},
		Reloptions: []string{"autovacuum_vacuum_scale_factor=0.01"},
	}}
	findings := checkVacuumLargeTableDefaults(snap)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings for table with overrides, got %d", len(findings))
	}
}

func TestVacuumLargeTableDefaults_VeryLargeTableWarning(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "huge",
		Stats: &schema.TableStats{Reltuples: 50_000_000, DeadTuples: 0},
	}}
	findings := checkVacuumLargeTableDefaults(snap)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Severity != "warning" {
		t.Errorf("expected warning severity for >10M rows, got %s", findings[0].Severity)
	}
}

func TestRunRules(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Columns: []schema.Column{{Name: "id", TypeName: "bigint"}},
	}}
	config := DefaultConfig()
	findings := RunRules(snap, &config)
	_ = findings
}
