package audit

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/lint"
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

// Pins the four-way branching in checkDuplicateIndexes based on which duplicate
// backs a constraint. Both-back yields a warning with no DDL fix; single-back
// drops the non-backing index; neither-back drops idx_b as sufficient. Also
// covers skip cases: different columns, different IndexType, invalid index.
func TestDuplicateIndexes_Branching(t *testing.T) {
	mkSnap := func(t *testing.T, a, b schema.Index) *schema.SchemaSnapshot {
		t.Helper()
		s := testSnap()
		s.Tables = []schema.Table{{
			Schema: "public", Name: "orders",
			Indexes: []schema.Index{a, b},
		}}
		return s
	}

	idx := func(name string, cols []string, kind string, backs, valid bool) schema.Index {
		return schema.Index{Name: name, Columns: cols, IndexType: kind, IsValid: valid, BacksConstraint: backs}
	}

	t.Run("both_back_constraints_warning_no_ddl", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", true, true),
			idx("idx_b", []string{"user_id"}, "btree", true, true),
		)
		findings := checkDuplicateIndexes(snap)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding, got %d", len(findings))
		}
		f := findings[0]
		if f.Severity != lint.SeverityWarning {
			t.Errorf("expected warning severity, got %s", f.Severity)
		}
		if f.DDLFix != nil {
			t.Errorf("expected no DDLFix, got %v", *f.DDLFix)
		}
		if !strings.Contains(f.Message, "both back constraints") {
			t.Errorf("expected message about both back constraints, got %q", f.Message)
		}
		if !strings.Contains(f.Recommendation, "FK") || !strings.Contains(f.Recommendation, "re-create") {
			t.Errorf("expected recommendation mentioning FK + re-create, got %q", f.Recommendation)
		}
	})

	t.Run("only_a_backs_constraint_drops_b", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", true, true),
			idx("idx_b", []string{"user_id"}, "btree", false, true),
		)
		findings := checkDuplicateIndexes(snap)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding, got %d", len(findings))
		}
		f := findings[0]
		if f.Severity != lint.SeverityError {
			t.Errorf("expected error severity, got %s", f.Severity)
		}
		if f.DDLFix == nil || !strings.Contains(*f.DDLFix, "DROP INDEX idx_b") {
			t.Errorf("expected DDL to drop idx_b, got %v", f.DDLFix)
		}
		if !strings.Contains(f.Recommendation, "backs a constraint") {
			t.Errorf("expected recommendation to mention backs a constraint, got %q", f.Recommendation)
		}
	})

	t.Run("only_b_backs_constraint_drops_a", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", false, true),
			idx("idx_b", []string{"user_id"}, "btree", true, true),
		)
		findings := checkDuplicateIndexes(snap)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding, got %d", len(findings))
		}
		f := findings[0]
		if f.Severity != lint.SeverityError {
			t.Errorf("expected error severity, got %s", f.Severity)
		}
		if f.DDLFix == nil || !strings.Contains(*f.DDLFix, "DROP INDEX idx_a") {
			t.Errorf("expected DDL to drop idx_a, got %v", f.DDLFix)
		}
		if !strings.Contains(f.Recommendation, "backs a constraint") {
			t.Errorf("expected recommendation to mention backs a constraint, got %q", f.Recommendation)
		}
	})

	t.Run("neither_backs_constraint_drops_b_sufficient", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", false, true),
			idx("idx_b", []string{"user_id"}, "btree", false, true),
		)
		findings := checkDuplicateIndexes(snap)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding, got %d", len(findings))
		}
		f := findings[0]
		if f.Severity != lint.SeverityError {
			t.Errorf("expected error severity, got %s", f.Severity)
		}
		if f.DDLFix == nil || !strings.Contains(*f.DDLFix, "DROP INDEX idx_b") {
			t.Errorf("expected DDL to drop idx_b, got %v", f.DDLFix)
		}
		if !strings.Contains(f.Recommendation, "is sufficient") {
			t.Errorf("expected recommendation 'is sufficient', got %q", f.Recommendation)
		}
	})

	t.Run("different_columns_no_finding", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", false, true),
			idx("idx_b", []string{"order_id"}, "btree", false, true),
		)
		if findings := checkDuplicateIndexes(snap); len(findings) != 0 {
			t.Errorf("expected 0 findings for different columns, got %d", len(findings))
		}
	})

	t.Run("different_index_type_no_finding", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", false, true),
			idx("idx_b", []string{"user_id"}, "hash", false, true),
		)
		if findings := checkDuplicateIndexes(snap); len(findings) != 0 {
			t.Errorf("expected 0 findings for different IndexType, got %d", len(findings))
		}
	})

	t.Run("invalid_index_skipped", func(t *testing.T) {
		snap := mkSnap(t,
			idx("idx_a", []string{"user_id"}, "btree", false, true),
			idx("idx_b", []string{"user_id"}, "btree", false, false),
		)
		if findings := checkDuplicateIndexes(snap); len(findings) != 0 {
			t.Errorf("expected 0 findings when one index invalid, got %d", len(findings))
		}
	})
}

// verifies the DDL fix for vacuum/large_table_defaults sets all four knobs
// (vacuum + analyze scale factor and threshold) and that the recommendation
// explains why scale factors alone aren't enough. Also sanity-checks that
// SuggestedVacuumKnobs returns a sensible scale factor for a 10M row table.
func TestVacuumLargeTableDefaults_FourKnobDDL(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "events",
		Stats: &schema.TableStats{Reltuples: 10_000_000, DeadTuples: 0},
	}}
	findings := checkVacuumLargeTableDefaults(snap)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	f := findings[0]
	if f.DDLFix == nil {
		t.Fatal("expected DDLFix")
	}
	ddl := *f.DDLFix
	for _, knob := range []string{
		"autovacuum_vacuum_scale_factor",
		"autovacuum_vacuum_threshold",
		"autovacuum_analyze_scale_factor",
		"autovacuum_analyze_threshold",
	} {
		if !strings.Contains(ddl, knob) {
			t.Errorf("expected DDL to contain %s, got %s", knob, ddl)
		}
	}
	if !strings.Contains(f.Recommendation, "scale factors alone aren't enough") {
		t.Errorf("expected recommendation mentioning scale factors alone aren't enough, got %q", f.Recommendation)
	}

	vacSF, _, _, _ := schema.SuggestedVacuumKnobs(10_000_000)
	if vacSF <= 0 || vacSF > 0.1 {
		t.Errorf("expected scale factor in (0, 0.1] for 10M rows, got %v", vacSF)
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
