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

// A10: the verdict follows the object kind. Table, column and view renames
// break every caller that names them; an index is never named by a query; a
// constraint or sequence only breaks callers that spell the old name out.
func TestCheckMigrationRenameByKind(t *testing.T) {
	tests := []struct {
		ddl    string
		safety SafetyRating
		prose  string
	}{
		{"ALTER TABLE users RENAME TO customers", SafetyDangerous, ""},
		{"ALTER TABLE users RENAME COLUMN email TO login", SafetyDangerous, ""},
		{"ALTER VIEW users_view RENAME TO accounts_view", SafetyDangerous, ""},
		{"ALTER MATERIALIZED VIEW users_mv RENAME TO accounts_mv", SafetyDangerous, ""},
		{"ALTER INDEX users_email_idx RENAME TO users_login_idx", SafetySafe, "pg_indexes"},
		{"ALTER TABLE users RENAME CONSTRAINT users_email_key TO users_login_key", SafetyCaution, "ON CONFLICT"},
		{"ALTER SEQUENCE users_id_seq RENAME TO customers_id_seq", SafetyCaution, "nextval"},
		{"ALTER TYPE mood RENAME VALUE 'sad' TO 'blue'", SafetyCaution, "old literal"},
	}
	for _, tt := range tests {
		checks, err := CheckMigration(tt.ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", tt.ddl, err)
		}
		c := checkByOp(t, checks, "RENAME")
		if c.Safety != tt.safety {
			t.Errorf("%s: got %q, want %q", tt.ddl, c.Safety, tt.safety)
		}
		if c.LockType != "ACCESS EXCLUSIVE" {
			t.Errorf("%s: got lock %q, want ACCESS EXCLUSIVE", tt.ddl, c.LockType)
		}
		if tt.safety != SafetyDangerous && strings.Contains(c.Recommendation, "DANGEROUS") {
			t.Errorf("%s: %s verdict recommends with DANGEROUS prose:\n%s", tt.ddl, tt.safety, c.Recommendation)
		}
		if tt.prose != "" && !strings.Contains(c.Recommendation, tt.prose) {
			t.Errorf("%s: recommendation should mention %q, got:\n%s", tt.ddl, tt.prose, c.Recommendation)
		}
	}
}

// A rename kind A10 does not model (a trigger, a type) keeps the worst case
// rather than falling through to a softened verdict.
func TestCheckMigrationRenameUnknownKindStaysDangerous(t *testing.T) {
	checks, err := CheckMigration("ALTER TRIGGER trg ON users RENAME TO trg2", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("an unmodeled rename kind must stay dangerous, got %q", checks[0].Safety)
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
		"ALTER INDEX orders_user_id_idx RENAME TO orders_user_created_idx",
		"ALTER TABLE orders RENAME CONSTRAINT orders_total_check TO orders_total_nonneg",
		"ALTER SEQUENCE orders_id_seq RENAME TO sales_id_seq",
		// DROP INDEX
		"DROP INDEX orders_user_id_idx",
		"DROP INDEX CONCURRENTLY orders_user_id_idx",
		"DROP INDEX orders_user_id_idx CASCADE",
		// passthrough / unmodeled
		"SET statement_timeout = '5s'",
		"DO $$ BEGIN NULL; END $$",
		"INSERT INTO users (id) VALUES (1)",
		// A5
		"COMMENT ON TABLE users IS 'x'",
		"UPDATE users SET email = 'x'",
		"DELETE FROM users",
		"UPDATE orders SET status = 'x'",
		"DELETE FROM orders",
		"DROP TRIGGER trg ON users",
		"DROP TRIGGER trg ON users CASCADE",
		"DROP FUNCTION my_func(integer)",
		"DROP FUNCTION my_func(integer) CASCADE",
		"REINDEX INDEX users_email_idx",
		"REINDEX TABLE orders",
		"REINDEX INDEX CONCURRENTLY users_email_idx",
		"ALTER TABLE users DISABLE TRIGGER ALL",
		"ALTER TABLE users ENABLE ROW LEVEL SECURITY",
		"ALTER TABLE users SET LOGGED",
		"ALTER TABLE orders SET LOGGED",
		"ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
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

// A5: COMMENT ON takes SHARE UPDATE EXCLUSIVE, which blocks neither reads nor
// writes -- metadata-only.
func TestCheckMigrationCommentOn(t *testing.T) {
	for _, ddl := range []string{
		"COMMENT ON TABLE users IS 'all users'",
		"COMMENT ON COLUMN users.email IS 'login'",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Operation != "COMMENT" {
			t.Errorf("%s: got %q, want COMMENT", ddl, checks[0].Operation)
		}
		if checks[0].Safety != SafetySafe {
			t.Errorf("%s: COMMENT is metadata-only, got %q", ddl, checks[0].Safety)
		}
		if checks[0].LockType != "SHARE UPDATE EXCLUSIVE" {
			t.Errorf("%s: got lock %q, want SHARE UPDATE EXCLUSIVE", ddl, checks[0].LockType)
		}
	}
}

// COMMENT ON TABLE must carry the table's sizing so the with_size metric holds.
func TestCheckMigrationCommentOnCarriesSize(t *testing.T) {
	checks, err := CheckMigration("COMMENT ON TABLE users IS 'x'", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Table == nil || *checks[0].Table != "users" {
		t.Errorf("expected table users, got %v", checks[0].Table)
	}
	if checks[0].TableSize == nil || checks[0].RowEstimate == nil {
		t.Errorf("expected sizing for a known table, got %v/%v", checks[0].TableSize, checks[0].RowEstimate)
	}
}

// A5: DML is ROW EXCLUSIVE; INSERT always passes, an UPDATE/DELETE with a
// WHERE is bounded, and a full-table write is a caution on anything not known
// small.
func TestCheckMigrationDML(t *testing.T) {
	tests := []struct {
		ddl      string
		safety   SafetyRating
		op       string
		wantLock string
	}{
		{"INSERT INTO users (id) VALUES (1)", SafetySafe, "INSERT", "ROW EXCLUSIVE"},
		{"INSERT INTO users (id) SELECT id FROM legacy", SafetySafe, "INSERT", "ROW EXCLUSIVE"},
		{"UPDATE users SET email = 'x' WHERE id = 1", SafetySafe, "UPDATE", "ROW EXCLUSIVE"},
		{"DELETE FROM users WHERE id = 1", SafetySafe, "DELETE", "ROW EXCLUSIVE"},
		{"UPDATE orders SET status = 'x'", SafetySafe, "UPDATE", "ROW EXCLUSIVE"},
		{"DELETE FROM orders", SafetySafe, "DELETE", "ROW EXCLUSIVE"},
		{"UPDATE users SET email = 'x'", SafetyCaution, "UPDATE", "ROW EXCLUSIVE"},
		{"DELETE FROM users", SafetyCaution, "DELETE", "ROW EXCLUSIVE"},
		{"UPDATE legacy SET total = 0", SafetyCaution, "UPDATE", "ROW EXCLUSIVE"},
	}
	for _, tt := range tests {
		checks, err := CheckMigration(tt.ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", tt.ddl, err)
		}
		c := checks[0]
		if c.Operation != tt.op {
			t.Errorf("%s: got op %q, want %q", tt.ddl, c.Operation, tt.op)
		}
		if c.Safety != tt.safety {
			t.Errorf("%s: got %q, want %q", tt.ddl, c.Safety, tt.safety)
		}
		if c.LockType != tt.wantLock {
			t.Errorf("%s: got lock %q, want %q", tt.ddl, c.LockType, tt.wantLock)
		}
	}
}

// A full-table UPDATE/DELETE on a large table must recommend batching and
// must not ship DANGEROUS prose under a caution verdict.
func TestCheckMigrationFullTableDMLRecommendsBatching(t *testing.T) {
	for _, ddl := range []string{"UPDATE users SET email = 'x'", "DELETE FROM users"} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		rec := checks[0].Recommendation
		if !strings.Contains(rec, "atch") {
			t.Errorf("%s: expected a batching recommendation, got %q", ddl, rec)
		}
		if strings.Contains(rec, "DANGEROUS") {
			t.Errorf("%s: caution recommends with DANGEROUS prose: %q", ddl, rec)
		}
	}
}

// A table created empty earlier in the file makes a full-table write a 0-row
// no-op, the same shortcut the other analyzers use.
func TestCheckMigrationDMLOnCreatedEmptyTable(t *testing.T) {
	ddl := "CREATE TABLE fresh (id bigint); UPDATE fresh SET id = 1"
	checks, err := CheckMigration(ddl, migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	update := checks[len(checks)-1]
	if update.Operation != "UPDATE" {
		t.Fatalf("got %q, want UPDATE", update.Operation)
	}
	if update.Safety != SafetySafe {
		t.Errorf("0-row update on a table created empty should be safe, got %q", update.Safety)
	}
}

// A5: DO is its own operation with its own reason -- unanalyzable, caution.
func TestCheckMigrationDoBlock(t *testing.T) {
	checks, err := CheckMigration("DO $$ BEGIN NULL; END $$", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Operation != "DO" {
		t.Errorf("got %q, want DO (no longer UNRECOGNIZED)", checks[0].Operation)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("DO should be caution, got %q", checks[0].Safety)
	}
	if strings.Contains(checks[0].Recommendation, "DANGEROUS") {
		t.Errorf("caution recommends with DANGEROUS prose: %q", checks[0].Recommendation)
	}
}

// A5: DROP TRIGGER is a brief ACCESS EXCLUSIVE catalog unlink; CASCADE widens
// the blast radius.
func TestCheckMigrationDropTrigger(t *testing.T) {
	checks, err := CheckMigration("DROP TRIGGER trg ON users", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Operation != "DROP TRIGGER" {
		t.Errorf("got %q, want DROP TRIGGER", checks[0].Operation)
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("plain DROP TRIGGER should be safe, got %q", checks[0].Safety)
	}
	if checks[0].LockType != "ACCESS EXCLUSIVE" {
		t.Errorf("got lock %q, want ACCESS EXCLUSIVE", checks[0].LockType)
	}

	checks, err = CheckMigration("DROP TRIGGER trg ON users CASCADE", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("DROP TRIGGER CASCADE should be caution, got %q", checks[0].Safety)
	}
}

// A5: DROP FUNCTION/PROCEDURE only touches the catalog; CASCADE is caution.
func TestCheckMigrationDropFunction(t *testing.T) {
	for _, ddl := range []string{
		"DROP FUNCTION my_func(integer)",
		"DROP PROCEDURE my_proc(integer)",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Safety != SafetySafe {
			t.Errorf("%s: should be safe, got %q", ddl, checks[0].Safety)
		}
		if checks[0].LockType != "none (catalog only)" {
			t.Errorf("%s: got lock %q, want catalog-only", ddl, checks[0].LockType)
		}
	}

	checks, err := CheckMigration("DROP FUNCTION my_func(integer) CASCADE", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("DROP FUNCTION CASCADE should be caution, got %q", checks[0].Safety)
	}
}

// A5: REINDEX blocks writes at any size and blocks reads through the index;
// CONCURRENTLY is safe but cannot run in a transaction.
func TestCheckMigrationReindex(t *testing.T) {
	tests := []struct {
		ddl    string
		safety SafetyRating
	}{
		{"REINDEX INDEX users_email_idx", SafetyDangerous},
		{"REINDEX TABLE users", SafetyDangerous},
		{"REINDEX SCHEMA public", SafetyDangerous},
		{"REINDEX DATABASE test", SafetyDangerous},
		{"REINDEX TABLE orders", SafetyCaution},
		{"REINDEX INDEX CONCURRENTLY users_email_idx", SafetySafe},
		{"REINDEX TABLE CONCURRENTLY users", SafetySafe},
	}
	for _, tt := range tests {
		checks, err := CheckMigration(tt.ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", tt.ddl, err)
		}
		if checks[0].Safety != tt.safety {
			t.Errorf("%s: got %q, want %q", tt.ddl, checks[0].Safety, tt.safety)
		}
	}
}

// A concurrent REINDEX names itself CONCURRENTLY so A4's transaction flip
// matches it.
func TestCheckMigrationReindexConcurrentNamed(t *testing.T) {
	checks, err := CheckMigration("REINDEX INDEX CONCURRENTLY users_email_idx", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Operation != "REINDEX CONCURRENTLY" {
		t.Errorf("got %q, want REINDEX CONCURRENTLY", checks[0].Operation)
	}
	if checks[0].LockType != "SHARE UPDATE EXCLUSIVE" {
		t.Errorf("got lock %q, want SHARE UPDATE EXCLUSIVE", checks[0].LockType)
	}
}

// A5: the metadata-only ALTER TABLE subcommands must be safe, not UNRECOGNIZED.
func TestCheckMigrationAlterSubcommandsSafe(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users ALTER COLUMN email SET DEFAULT 'x'",
		"ALTER TABLE users ALTER COLUMN email DROP DEFAULT",
		"ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
		"ALTER TABLE users ALTER COLUMN email SET STATISTICS 100",
		"ALTER TABLE users ALTER COLUMN email SET STORAGE PLAIN",
		"ALTER TABLE users ALTER COLUMN email SET COMPRESSION pglz",
		"ALTER TABLE users OWNER TO postgres",
		"ALTER TABLE users CLUSTER ON users_email_idx",
		"ALTER TABLE users SET WITHOUT CLUSTER",
		"ALTER TABLE users REPLICA IDENTITY FULL",
		"ALTER TABLE users SET (fillfactor = 70)",
		"ALTER TABLE users RESET (fillfactor)",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if c.Operation == "UNRECOGNIZED" {
			t.Errorf("%s: still UNRECOGNIZED", ddl)
		}
		if c.Safety != SafetySafe {
			t.Errorf("%s: should be safe, got %q", ddl, c.Safety)
		}
		if c.Statement == "" {
			t.Errorf("%s: safe check must carry its Statement for passthrough", ddl)
		}
	}
}

// A5: behavior-flipping ALTER TABLE subcommands are caution, not safe.
func TestCheckMigrationAlterSubcommandsCaution(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users DISABLE TRIGGER trg",
		"ALTER TABLE users DISABLE TRIGGER ALL",
		"ALTER TABLE users ENABLE ROW LEVEL SECURITY",
		"ALTER TABLE users FORCE ROW LEVEL SECURITY",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if c.Operation == "UNRECOGNIZED" {
			t.Errorf("%s: still UNRECOGNIZED", ddl)
		}
		if c.Safety != SafetyCaution {
			t.Errorf("%s: should be caution, got %q", ddl, c.Safety)
		}
		if strings.Contains(c.Recommendation, "DANGEROUS") {
			t.Errorf("%s: caution recommends with DANGEROUS prose: %q", ddl, c.Recommendation)
		}
	}
}

// A5: rewrite-inducing ALTER TABLE subcommands are dangerous at size, caution
// on a small table.
func TestCheckMigrationAlterSubcommandsRewrite(t *testing.T) {
	tests := []struct {
		ddl    string
		safety SafetyRating
	}{
		{"ALTER TABLE users SET LOGGED", SafetyDangerous},
		{"ALTER TABLE users SET UNLOGGED", SafetyDangerous},
		{"ALTER TABLE users SET TABLESPACE fastspace", SafetyDangerous},
		{"ALTER TABLE orders SET LOGGED", SafetyCaution},
		{"ALTER TABLE orders SET TABLESPACE fastspace", SafetyCaution},
	}
	for _, tt := range tests {
		checks, err := CheckMigration(tt.ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", tt.ddl, err)
		}
		c := checks[0]
		if c.Operation == "UNRECOGNIZED" {
			t.Errorf("%s: still UNRECOGNIZED", tt.ddl)
		}
		if c.Safety != tt.safety {
			t.Errorf("%s: got %q, want %q", tt.ddl, c.Safety, tt.safety)
		}
		if tt.safety != SafetyDangerous && strings.Contains(c.Recommendation, "DANGEROUS") {
			t.Errorf("%s: caution recommends with DANGEROUS prose: %q", tt.ddl, c.Recommendation)
		}
	}
}

// A5: ATTACH PARTITION is caution -- it locks the attached table exclusively
// and may scan it.
func TestCheckMigrationAttachPartition(t *testing.T) {
	ddl := "ALTER TABLE events ATTACH PARTITION events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
	checks, err := CheckMigration(ddl, migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Operation != "ATTACH PARTITION" {
		t.Errorf("got %q, want ATTACH PARTITION", checks[0].Operation)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("ATTACH PARTITION should be caution, got %q", checks[0].Safety)
	}
}

// A9: PostgreSQL rejects DROP INDEX CONCURRENTLY combined with CASCADE or with
// several index names, in any transaction mode. Both get a split safer_sql.
func TestCheckMigrationDropIndexConcurrentlyRejectedForms(t *testing.T) {
	tests := []struct {
		name   string
		ddl    string
		reason string
		safer  []string
	}{
		{
			name:   "cascade",
			ddl:    "DROP INDEX CONCURRENTLY users_email_idx CASCADE",
			reason: "does not support CASCADE",
			safer:  []string{"DROP INDEX CONCURRENTLY users_email_idx;"},
		},
		{
			name:   "several names",
			ddl:    "DROP INDEX CONCURRENTLY users_email_idx, users_login_idx",
			reason: "does not support dropping multiple objects",
			safer: []string{
				"DROP INDEX CONCURRENTLY users_email_idx;",
				"DROP INDEX CONCURRENTLY users_login_idx;",
			},
		},
		{
			name:   "cascade several names",
			ddl:    "DROP INDEX CONCURRENTLY users_email_idx, users_login_idx CASCADE",
			reason: "does not support CASCADE or dropping multiple objects",
			safer: []string{
				"DROP INDEX CONCURRENTLY users_email_idx;",
				"DROP INDEX CONCURRENTLY users_login_idx;",
			},
		},
		{
			// IF EXISTS does not bypass the rejection, so the split keeps it.
			name:   "if exists several names",
			ddl:    "DROP INDEX CONCURRENTLY IF EXISTS users_email_idx, users_login_idx",
			reason: "does not support dropping multiple objects",
			safer: []string{
				"DROP INDEX CONCURRENTLY IF EXISTS users_email_idx;",
				"DROP INDEX CONCURRENTLY IF EXISTS users_login_idx;",
			},
		},
		{
			// names render qualified and quoted, PG-folding aside
			name:   "qualified and quoted names",
			ddl:    `DROP INDEX CONCURRENTLY myschema."Weird Name", users_login_idx`,
			reason: "does not support dropping multiple objects",
			safer: []string{
				`DROP INDEX CONCURRENTLY myschema."Weird Name";`,
				"DROP INDEX CONCURRENTLY users_login_idx;",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checks, err := CheckMigration(tc.ddl, migrationTestAnnotated())
			if err != nil {
				t.Fatal(err)
			}
			if len(checks) != 1 {
				t.Fatalf("got %d checks, want 1", len(checks))
			}
			c := checks[0]
			if c.Operation != "DROP INDEX CONCURRENTLY" {
				t.Errorf("got operation %q, want DROP INDEX CONCURRENTLY", c.Operation)
			}
			if c.Safety != SafetyDangerous {
				t.Errorf("got safety %q, want dangerous", c.Safety)
			}
			if !strings.Contains(c.Rationale.Reason, tc.reason) {
				t.Errorf("reason %q should contain %q", c.Rationale.Reason, tc.reason)
			}
			if got := strings.Join(c.SaferSQL, "|"); got != strings.Join(tc.safer, "|") {
				t.Errorf("safer_sql = %v, want %v", c.SaferSQL, tc.safer)
			}
		})
	}
}

// A9: the reason survives the transaction flip -- the form fails either way, so
// the wrapper message must not replace "PostgreSQL rejects this form".
func TestCheckMigrationFileDropIndexConcurrentlyRejectedForms(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		reason  string
	}{
		{
			name:    "transactional goose",
			content: "-- +goose Up\nDROP INDEX CONCURRENTLY users_email_idx CASCADE;\n-- +goose Down\nDROP INDEX users_email_idx;\n",
			reason:  "does not support CASCADE",
		},
		{
			name:    "no transaction goose",
			content: "-- +goose NO TRANSACTION\n-- +goose Up\nDROP INDEX CONCURRENTLY users_email_idx, users_login_idx;\n-- +goose Down\nDROP INDEX users_email_idx;\n",
			reason:  "does not support dropping multiple objects",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, err := CheckMigrationFile(tc.content, "up", migrationTestAnnotated())
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Checks) != 1 {
				t.Fatalf("got %d checks, want 1", len(report.Checks))
			}
			c := report.Checks[0]
			if c.Safety != SafetyDangerous {
				t.Errorf("got safety %q, want dangerous", c.Safety)
			}
			if !strings.Contains(c.Rationale.Reason, tc.reason) {
				t.Errorf("reason %q should contain %q", c.Rationale.Reason, tc.reason)
			}
			if strings.Contains(c.Rationale.Reason, "wraps this file in") {
				t.Errorf("wrapper message masked the real reason: %q", c.Rationale.Reason)
			}
			if len(c.SaferSQL) == 0 {
				t.Error("expected a split safer_sql")
			}
		})
	}
}

// A9: only CONCURRENTLY is rejected -- plain multi-name DROP INDEX stays safe.
func TestCheckMigrationDropIndexNonConcurrentMultiName(t *testing.T) {
	checks, err := CheckMigration("DROP INDEX users_email_idx, users_login_idx", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("plain multi-name DROP INDEX should be safe, got %q", checks[0].Safety)
	}
}

// A9 end to end: the split remedy runs under NO TRANSACTION, one DROP INDEX
// CONCURRENTLY per transaction. Verified live on postgres:18.6 -- the two
// statements cannot share a psql invocation (one implicit transaction blocks
// CONCURRENTLY), which is what the NO TRANSACTION marker and the multi-statement
// header guard against.
func TestCheckMigrationFileSplitRunsUnderNoTransaction(t *testing.T) {
	report, err := CheckMigrationFile(
		"-- +goose NO TRANSACTION\n-- +goose Up\nDROP INDEX CONCURRENTLY users_email_idx, users_login_idx;\n-- +goose Down\nSELECT 1;\n",
		"up", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if report.MigrationSQL == "" {
		t.Fatal("expected a migration_sql with the split drops")
	}
	for _, want := range []string{
		"-- +goose NO TRANSACTION",
		"DROP INDEX CONCURRENTLY users_email_idx;",
		"DROP INDEX CONCURRENTLY users_login_idx;",
		"Run each statement in its own transaction",
	} {
		if !strings.Contains(report.MigrationSQL, want) {
			t.Errorf("migration_sql missing %q:\n%s", want, report.MigrationSQL)
		}
	}
}

// Lock levels must match PG16 ALTER TABLE: SET STATISTICS, attribute options,
// CLUSTER ON and fillfactor take SHARE UPDATE EXCLUSIVE, not ACCESS EXCLUSIVE.
func TestCheckMigrationAlterSubcommandLockTypes(t *testing.T) {
	tests := []struct {
		ddl  string
		lock string
	}{
		{"ALTER TABLE users ALTER COLUMN email SET STATISTICS 100", "SHARE UPDATE EXCLUSIVE"},
		{"ALTER TABLE users ALTER COLUMN email SET (n_distinct = 0.5)", "SHARE UPDATE EXCLUSIVE"},
		{"ALTER TABLE users CLUSTER ON users_email_idx", "SHARE UPDATE EXCLUSIVE"},
		{"ALTER TABLE users SET WITHOUT CLUSTER", "SHARE UPDATE EXCLUSIVE"},
		{"ALTER TABLE users SET (fillfactor = 70)", "SHARE UPDATE EXCLUSIVE"},
		{"ALTER TABLE users RESET (fillfactor)", "SHARE UPDATE EXCLUSIVE"},
		{"ALTER TABLE users OWNER TO postgres", "ACCESS EXCLUSIVE"},
		{"ALTER TABLE users REPLICA IDENTITY FULL", "ACCESS EXCLUSIVE"},
	}
	for _, tt := range tests {
		checks, err := CheckMigration(tt.ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", tt.ddl, err)
		}
		if checks[0].LockType != tt.lock {
			t.Errorf("%s: got lock %q, want %q", tt.ddl, checks[0].LockType, tt.lock)
		}
	}
}

// DETACH PARTITION is safe metadata; CONCURRENTLY names itself so A4 can flip
// it inside a transactional file.
func TestCheckMigrationDetachPartition(t *testing.T) {
	plain, err := CheckMigration("ALTER TABLE events DETACH PARTITION events_2025", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].Safety != SafetySafe {
		t.Errorf("DETACH PARTITION should be safe, got %q", plain[0].Safety)
	}
	if !strings.Contains(plain[0].LockType, "ACCESS EXCLUSIVE (partition)") {
		t.Errorf("plain DETACH should report the partition lock, got %q", plain[0].LockType)
	}

	conc, err := CheckMigration("ALTER TABLE events DETACH PARTITION events_2025 CONCURRENTLY", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if conc[0].Operation != "DETACH PARTITION CONCURRENTLY" {
		t.Errorf("got %q, want DETACH PARTITION CONCURRENTLY", conc[0].Operation)
	}
	if conc[0].LockType != "SHARE UPDATE EXCLUSIVE" {
		t.Errorf("got lock %q, want SHARE UPDATE EXCLUSIVE", conc[0].LockType)
	}
}

// COMMENT ON COLUMN on a schema-qualified table carries the qualified table and its size.
func TestCheckMigrationCommentOnSchemaQualifiedColumn(t *testing.T) {
	checks, err := CheckMigration("COMMENT ON COLUMN public.users.email IS 'login'", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Table == nil || *checks[0].Table != "public.users" {
		t.Errorf("expected table public.users, got %v", checks[0].Table)
	}
	if checks[0].TableSize == nil || checks[0].RowEstimate == nil {
		t.Errorf("expected sizing for public.users, got %v/%v", checks[0].TableSize, checks[0].RowEstimate)
	}
}

// DROP TRIGGER on a known table must carry the table and its sizing.
func TestCheckMigrationDropTriggerCarriesSize(t *testing.T) {
	checks, err := CheckMigration("DROP TRIGGER trg ON users", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Table == nil || *checks[0].Table != "users" {
		t.Errorf("expected table users, got %v", checks[0].Table)
	}
	if checks[0].TableSize == nil || checks[0].RowEstimate == nil {
		t.Errorf("expected sizing for users, got %v/%v", checks[0].TableSize, checks[0].RowEstimate)
	}
}

// REINDEX TABLE on a table created empty in the same migration is safe and 0 rows.
func TestCheckMigrationReindexTableCreatedEmpty(t *testing.T) {
	checks, err := CheckMigration("CREATE TABLE fresh (id bigint); REINDEX TABLE fresh;", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	reindex := checks[len(checks)-1]
	if reindex.Operation != "REINDEX TABLE" {
		t.Fatalf("got %q, want REINDEX TABLE", reindex.Operation)
	}
	if reindex.Safety != SafetySafe {
		t.Errorf("reindex on table created empty should be safe, got %q", reindex.Safety)
	}
	if reindex.RowEstimate == nil || *reindex.RowEstimate != 0 {
		t.Errorf("expected 0 row estimate, got %v", reindex.RowEstimate)
	}
}

// An unqualified REINDEX INDEX finds its parent table in the snapshot and carries its sizing.
func TestCheckMigrationReindexIndexFindsParentTable(t *testing.T) {
	checks, err := CheckMigration("REINDEX INDEX orders_user_id_idx", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	c := checks[0]
	if c.Safety != SafetyCaution {
		t.Errorf("REINDEX on small table orders should be caution, got %q", c.Safety)
	}
	if c.TableSize == nil || *c.TableSize != "128.0 KB" {
		t.Errorf("expected table_size 128.0 KB, got %v", c.TableSize)
	}
}

// Stale planner stats (> 7 days) must not allow a full-table write to be
// treated as empty/safe; it must stay caution.
func TestCheckMigrationStalePlannerDMLKeepsCaution(t *testing.T) {
	a := migrationTestAnnotated()
	a.Planner.Timestamp = time.Now().Add(-30 * 24 * time.Hour)
	checks, err := CheckMigration("UPDATE events SET created_at = now()", a)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("stale planner stats on full-table UPDATE must keep caution, got %q", checks[0].Safety)
	}
}
