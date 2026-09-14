package query

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func f64p(f float64) *float64 { return &f }

// Sized so the existing verdict tests still hold: users is large (stays
// DANGEROUS), orders is small (downgrades to CAUTION), events is empty.
func migrationTestAnnotated() *schema.AnnotatedSchema {
	return &schema.AnnotatedSchema{
		Schema: &schema.SchemaSnapshot{
			PgVersion:   "PostgreSQL 17.0",
			Database:    "test",
			Timestamp:   time.Now().UTC(),
			ContentHash: "test",
			Tables: []schema.Table{
				{
					Schema: "public", Name: "users",
					Columns: []schema.Column{
						{Name: "id", TypeName: "bigint"},
						{Name: "email", TypeName: "text"},
					},
				},
				{
					Schema: "public", Name: "orders",
					Columns: []schema.Column{
						{Name: "id", TypeName: "bigint"},
						{Name: "user_id", TypeName: "bigint"},
						{Name: "status", TypeName: "text"},
						{Name: "total", TypeName: "numeric"},
					},
					Indexes: []schema.Index{{Name: "orders_user_id_idx", Columns: []string{"user_id"}}},
				},
				{
					Schema: "app", Name: "orders",
					Columns: []schema.Column{{Name: "status", TypeName: "text"}},
				},
				{
					Schema: "public", Name: "legacy",
					Columns:     []schema.Column{{Name: "total", TypeName: "numeric"}},
					Constraints: []schema.Constraint{{Name: "legacy_total_check", Kind: schema.ConstraintCheck}},
				},
				{
					Schema: "public", Name: "events",
					Columns: []schema.Column{{Name: "created_at", TypeName: "timestamptz"}},
					PartitionInfo: &schema.PartitionInfo{
						Strategy: schema.PartitionRange,
						Key:      "RANGE (created_at)",
					},
				},
			},
		},
		Planner: &schema.PlannerStatsSnapshot{
			Tables: []schema.TableSizingEntry{
				{Table: schema.QualifiedName{Schema: "public", Name: "users"}, Sizing: schema.TableSizing{Reltuples: 2_000_000, TableSize: 512 << 20}},
				{Table: schema.QualifiedName{Schema: "public", Name: "orders"}, Sizing: schema.TableSizing{Reltuples: 2_000, TableSize: 128 << 10}},
				{Table: schema.QualifiedName{Schema: "app", Name: "orders"}, Sizing: schema.TableSizing{Reltuples: 2_000, TableSize: 128 << 10}},
				{Table: schema.QualifiedName{Schema: "public", Name: "events"}, Sizing: schema.TableSizing{Reltuples: 0, TableSize: 0}},
			},
			Columns: []schema.ColumnStatsEntry{
				{Table: schema.QualifiedName{Schema: "public", Name: "orders"}, Column: "status", Stats: schema.ColumnStats{NullFrac: f64p(0)}},
				{Table: schema.QualifiedName{Schema: "public", Name: "users"}, Column: "email", Stats: schema.ColumnStats{NullFrac: f64p(0.2)}},
			},
		},
	}
}

func TestCheckMigrationAddColumn(t *testing.T) {
	snap := migrationTestAnnotated()
	checks, err := CheckMigration("ALTER TABLE users ADD COLUMN age integer", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Operation != "ADD COLUMN" {
		t.Errorf("got %q, want ADD COLUMN", checks[0].Operation)
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("nullable column without default should be safe, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationAddColumnWithDefault(t *testing.T) {
	snap := migrationTestAnnotated()
	checks, err := CheckMigration("ALTER TABLE users ADD COLUMN age integer DEFAULT 0", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("column with default on PG17 should be caution, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationCreateIndex(t *testing.T) {
	snap := migrationTestAnnotated()
	checks, err := CheckMigration("CREATE INDEX idx_users_email ON users(email)", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("non-concurrent index on a large table should be dangerous, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationCreateIndexConcurrently(t *testing.T) {
	snap := migrationTestAnnotated()
	checks, err := CheckMigration("CREATE INDEX CONCURRENTLY idx_users_email ON users(email)", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("concurrent index should be safe, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationDropTable(t *testing.T) {
	snap := migrationTestAnnotated()
	checks, err := CheckMigration("DROP TABLE users", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("drop table should be dangerous, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationRename(t *testing.T) {
	snap := migrationTestAnnotated()
	checks, err := CheckMigration("ALTER TABLE users RENAME TO customers", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("rename should be dangerous, got %q", checks[0].Safety)
	}
}

// Regression for the fdd7995 stub: a known table must carry its sizing.
func TestCheckMigrationCarriesTableSize(t *testing.T) {
	checks, err := CheckMigration("CREATE INDEX idx_o ON orders (status)", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	c := checks[0]
	if c.TableSize == nil || *c.TableSize != "128.0 KB" {
		t.Errorf("expected table_size 128.0 KB, got %v", c.TableSize)
	}
	if c.RowEstimate == nil || *c.RowEstimate != 2000 {
		t.Errorf("expected row_estimate 2000, got %v", c.RowEstimate)
	}
}

// No sizing means no evidence and no downgrade: the worst case stands.
func TestCheckMigrationUnknownTableHasNoSizing(t *testing.T) {
	checks, err := CheckMigration("CREATE INDEX idx_l ON legacy (total)", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].TableSize != nil || checks[0].RowEstimate != nil {
		t.Errorf("expected no sizing for an unknown table, got %v/%v", checks[0].TableSize, checks[0].RowEstimate)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("unknown sizing must keep the worst case, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationSmallTableDowngrades(t *testing.T) {
	for _, ddl := range []string{
		"CREATE INDEX idx_o ON orders (status)",
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"ALTER TABLE orders ALTER COLUMN total TYPE bigint",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if c.Safety != SafetyCaution {
			t.Errorf("%s: small table should be caution, got %q", ddl, c.Safety)
		}
		if c.Safety == SafetySafe {
			t.Errorf("%s: small table must never be safe", ddl)
		}
		if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "Table is small") {
			t.Errorf("%s: expected small-table note, got %v", ddl, c.Rationale)
		}
		if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "lock_timeout") {
			t.Errorf("%s: expected lock_timeout guidance, got %v", ddl, c.Rationale)
		}
	}
}

// reltuples = -1 (never analyzed) is unknown, not small.
func TestCheckMigrationNeverAnalyzedRowsKeepWorstCase(t *testing.T) {
	a := migrationTestAnnotated()
	a.Planner.Tables = append(a.Planner.Tables, schema.TableSizingEntry{
		Table:  schema.QualifiedName{Schema: "public", Name: "legacy"},
		Sizing: schema.TableSizing{Reltuples: -1, TableSize: 8192},
	})
	checks, err := CheckMigration("CREATE INDEX idx_l ON legacy (total)", a)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("never-analyzed rows are unknown, expected dangerous, got %q", checks[0].Safety)
	}
}

// Size never softens operations whose danger is not size-proportional.
func TestCheckMigrationSmallTableMetaOpsStayDangerous(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE orders RENAME TO orders_old",
		"DROP TABLE orders",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Safety != SafetyDangerous {
			t.Errorf("%s: expected dangerous regardless of size, got %q", ddl, checks[0].Safety)
		}
	}
}

// The SET NOT NULL null_frac refinement dropped by fdd7995 is restored.
func TestCheckMigrationSetNotNullNullFracZero(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE orders ALTER COLUMN status SET NOT NULL", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(checks[0].Recommendation, "0% NULLs") {
		t.Errorf("expected the 0%% NULLs data check, got %q", checks[0].Recommendation)
	}
}

func TestCheckMigrationSetNotNullNullFracBackfill(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE users ALTER COLUMN email SET NOT NULL", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	rec := checks[0].Recommendation
	if !strings.Contains(rec, "20% NULLs") || !strings.Contains(rec, "400000 rows") {
		t.Errorf("expected the backfill data check (20%% NULLs, ~400000 rows), got %q", rec)
	}
}

// reltuples = -1 is unknown: the NULL fraction still matters, but no row count
// may be derived from it.
func TestCheckMigrationSetNotNullNullFracUnknownRows(t *testing.T) {
	a := migrationTestAnnotated()
	for i := range a.Planner.Tables {
		if a.Planner.Tables[i].Table.Name == "users" {
			a.Planner.Tables[i].Sizing.Reltuples = -1
		}
	}
	checks, err := CheckMigration("ALTER TABLE users ALTER COLUMN email SET NOT NULL", a)
	if err != nil {
		t.Fatal(err)
	}
	rec := checks[0].Recommendation
	if !strings.Contains(rec, "20% NULLs") {
		t.Errorf("expected the NULL fraction to survive, got %q", rec)
	}
	if strings.Contains(rec, "rows)") {
		t.Errorf("no row count may be derived from unknown rows: %q", rec)
	}
}
