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
			Timestamp: time.Now().UTC(),
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
		if !strings.HasPrefix(c.Recommendation, "Table is small") {
			t.Errorf("%s: small-table note should lead the recommendation, got %q", ddl, c.Recommendation)
		}
		if strings.Contains(c.Recommendation, "DANGEROUS") {
			t.Errorf("%s: caution verdict recommends with DANGEROUS prose:\n%s", ddl, c.Recommendation)
		}
	}
}

// A2/D3: prose must agree with the verdict. Every analyzer that can soften a
// verdict to caution/safe must not ship a recommendation that still opens
// DANGEROUS -- a small table (ALTER TYPE, ADD CONSTRAINT, CREATE INDEX) or SET
// NOT NULL, which is caution at any size.
func TestCheckMigrationProseNeverContradictsVerdict(t *testing.T) {
	for _, ddl := range []string{
		// ADD COLUMN
		"ALTER TABLE users ADD COLUMN age integer",
		"ALTER TABLE orders ADD COLUMN seen_at timestamptz DEFAULT now()",
		// ALTER COLUMN TYPE (caution on a small table, dangerous on a large one)
		"ALTER TABLE orders ALTER COLUMN total TYPE bigint",
		"ALTER TABLE users ALTER COLUMN email TYPE citext",
		// SET NOT NULL -- caution at any size
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
		"ALTER TABLE ONLY users ALTER COLUMN email SET NOT NULL",
		// ADD CONSTRAINT, all forms
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)",
		"ALTER TABLE orders ADD PRIMARY KEY (id)",
		"ALTER TABLE orders ADD CONSTRAINT u UNIQUE (user_id)",
		"ALTER TABLE orders ADD EXCLUDE USING gist (status WITH =)",
		"ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID",
		"ALTER TABLE orders VALIDATE CONSTRAINT legacy_total_check",
		// CREATE INDEX
		"CREATE INDEX idx_o ON orders (status)",
		"CREATE INDEX idx_u ON users (email)",
		"CREATE INDEX CONCURRENTLY idx_u ON users (email)",
		// table lifecycle, including the created-empty shortcut
		"CREATE TABLE fresh (id bigint)",
		"CREATE TABLE fresh AS SELECT 1 AS id",
		"CREATE TABLE fresh (id bigint); CREATE INDEX idx_fresh ON fresh (id);",
		"DROP TABLE orders",
		"ALTER TABLE orders RENAME TO orders_old",
		// DROP INDEX
		"DROP INDEX orders_user_id_idx",
		"DROP INDEX CONCURRENTLY orders_user_id_idx",
		"DROP INDEX orders_user_id_idx CASCADE",
		// passthrough / unmodeled
		"SET statement_timeout = '5s'",
		"DO $$ BEGIN NULL; END $$",
		"INSERT INTO users (id) VALUES (1)",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if len(checks) == 0 {
			t.Errorf("%s: expected at least one check", ddl)
			continue
		}
		for _, c := range checks {
			if c.Safety != SafetyDangerous && strings.Contains(c.Recommendation, "DANGEROUS") {
				t.Errorf("%s: %s verdict recommends with DANGEROUS prose:\n%s", ddl, c.Safety, c.Recommendation)
			}
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

// A8: a planner capture older than 7 days is not evidence of "small": the
// worst case stands and no sizing is reported.
func TestCheckMigrationStalePlannerKeepsWorstCase(t *testing.T) {
	for _, ddl := range []string{
		"CREATE INDEX idx_o ON orders (status)",
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"ALTER TABLE orders ALTER COLUMN total TYPE bigint",
	} {
		a := migrationTestAnnotated()
		a.Planner.Timestamp = time.Now().Add(-30 * 24 * time.Hour)
		checks, err := CheckMigration(ddl, a)
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if c.Safety != SafetyDangerous {
			t.Errorf("%s: stale planner must keep the worst case, got %q", ddl, c.Safety)
		}
		if c.TableSize != nil || c.RowEstimate != nil {
			t.Errorf("%s: stale planner must report no sizing, got %v/%v", ddl, c.TableSize, c.RowEstimate)
		}
		if c.Rationale != nil && strings.Contains(c.Rationale.Note, "Table is small") {
			t.Errorf("%s: stale planner must not carry the small-table note, got %v", ddl, c.Rationale)
		}
	}
}

// The 7-day threshold itself: just over is stale, just under still downgrades.
func TestCheckMigrationStalePlannerBoundary(t *testing.T) {
	ddl := "CREATE INDEX idx_o ON orders (status)"

	a := migrationTestAnnotated()
	a.Planner.Timestamp = time.Now().Add(-7*24*time.Hour - time.Hour)
	checks, err := CheckMigration(ddl, a)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("7d+1h old is stale, expected dangerous, got %q", checks[0].Safety)
	}

	b := migrationTestAnnotated()
	b.Planner.Timestamp = time.Now().Add(-7*24*time.Hour + time.Hour)
	checks, err = CheckMigration(ddl, b)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("7d-1h old is fresh, expected caution, got %q", checks[0].Safety)
	}
}

// An undated planner cannot be trusted to soften a verdict.
func TestCheckMigrationZeroPlannerTimestampIsStale(t *testing.T) {
	a := migrationTestAnnotated()
	a.Planner.Timestamp = time.Time{}
	checks, err := CheckMigration("CREATE INDEX idx_o ON orders (status)", a)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("zero planner timestamp must keep the worst case, got %q", checks[0].Safety)
	}
	if checks[0].TableSize != nil || checks[0].RowEstimate != nil {
		t.Errorf("zero planner timestamp must report no sizing, got %v/%v", checks[0].TableSize, checks[0].RowEstimate)
	}
}

// Stale sizing must not derive a row count for the SET NOT NULL backfill, but
// the NULL fraction still matters.
func TestCheckMigrationStalePlannerSetNotNullNoRowCount(t *testing.T) {
	a := migrationTestAnnotated()
	a.Planner.Timestamp = time.Now().Add(-30 * 24 * time.Hour)
	checks, err := CheckMigration("ALTER TABLE users ALTER COLUMN email SET NOT NULL", a)
	if err != nil {
		t.Fatal(err)
	}
	rec := checks[0].Recommendation
	if !strings.Contains(rec, "20% NULLs") {
		t.Errorf("expected the NULL fraction to survive, got %q", rec)
	}
	if strings.Contains(rec, "rows)") {
		t.Errorf("no row count may be derived from stale rows: %q", rec)
	}
}
