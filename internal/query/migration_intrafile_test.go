package query

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func checkByOp(t *testing.T, checks []MigrationCheck, op string) MigrationCheck {
	t.Helper()
	for _, c := range checks {
		if c.Operation == op {
			return c
		}
	}
	t.Fatalf("no check with operation %q in %v", op, opsOf(checks))
	return MigrationCheck{}
}

func opsOf(checks []MigrationCheck) []string {
	ops := make([]string, 0, len(checks))
	for _, c := range checks {
		ops = append(ops, c.Operation)
	}
	return ops
}

func mustCheck(t *testing.T, ddl string) []MigrationCheck {
	t.Helper()
	checks, err := CheckMigration(ddl, migrationTestAnnotated())
	if err != nil {
		t.Fatalf("%s: %v", ddl, err)
	}
	return checks
}

// CREATE TABLE is modeled now: safe, zero rows, and it passes through.
func TestCheckMigrationCreateTable(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE new_orders (id bigint, status text)")
	c := checkByOp(t, checks, "CREATE TABLE")
	if c.Safety != SafetySafe {
		t.Errorf("CREATE TABLE should be safe, got %q", c.Safety)
	}
	if c.RowEstimate == nil || *c.RowEstimate != 0 {
		t.Errorf("expected 0 rows, got %v", c.RowEstimate)
	}
	if c.Statement == "" {
		t.Error("CREATE TABLE needs a Statement to pass through migration_sql")
	}
	// all-safe input composes to nothing, but the statement itself is usable
	if got := ComposeMigrationSQL(checks); got != "" {
		t.Errorf("all-safe input should not produce migration_sql, got:\n%s", got)
	}
}

// A table created in the same file is empty, so statements that would
// normally scan or rewrite it are safe and need no multi-step rewrite.
func TestCheckMigrationCreatedTableIsEmpty(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
		op   string
	}{
		{
			name: "foreign key",
			ddl:  "CREATE TABLE new_orders (id bigint, user_id bigint);\nALTER TABLE new_orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id)",
			op:   "ADD FOREIGN KEY",
		},
		{
			name: "primary key",
			ddl:  "CREATE TABLE new_orders (id bigint);\nALTER TABLE new_orders ADD PRIMARY KEY (id)",
			op:   "ADD PRIMARY KEY",
		},
		{
			name: "check",
			ddl:  "CREATE TABLE new_orders (id bigint);\nALTER TABLE new_orders ADD CHECK (id > 0)",
			op:   "ADD CHECK CONSTRAINT",
		},
		{
			name: "index",
			ddl:  "CREATE TABLE new_orders (id bigint);\nCREATE INDEX ON new_orders (id)",
			op:   "CREATE INDEX",
		},
		{
			name: "set not null",
			ddl:  "CREATE TABLE new_orders (id bigint);\nALTER TABLE new_orders ALTER COLUMN id SET NOT NULL",
			op:   "SET NOT NULL",
		},
		{
			name: "alter type",
			ddl:  "CREATE TABLE new_orders (id bigint);\nALTER TABLE new_orders ALTER COLUMN id TYPE text",
			op:   "ALTER COLUMN TYPE",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := checkByOp(t, mustCheck(t, tc.ddl), tc.op)
			if c.Safety != SafetySafe {
				t.Errorf("expected safe on an empty created table, got %q", c.Safety)
			}
			if len(c.SaferSQL) != 0 {
				t.Errorf("no rewrite is needed on an empty table, got %v", c.SaferSQL)
			}
			if c.Statement == "" {
				t.Error("statement must pass through into migration_sql")
			}
			if c.Rationale == nil || !strings.Contains(c.Rationale.Reason, "created empty earlier") {
				t.Errorf("expected the empty-table reason, got %v", c.Rationale)
			}
			if c.Rationale != nil && !strings.Contains(c.Rationale.Reason, "lock_timeout") {
				t.Errorf("expected lock_timeout caveat, got %q", c.Rationale.Reason)
			}
		})
	}
}

// The FK rewrite normally locks the referenced table too; that must survive.
func TestCheckMigrationEmptyTableForeignKeyNotesReferencedTable(t *testing.T) {
	c := checkByOp(t, mustCheck(t,
		"CREATE TABLE new_orders (id bigint, user_id bigint);\nALTER TABLE new_orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id)"),
		"ADD FOREIGN KEY")
	if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "users") ||
		!strings.Contains(c.Rationale.Note, "SHARE ROW EXCLUSIVE") {
		t.Errorf("expected a referenced-table lock note naming users, got %v", c.Rationale)
	}
}

// IF NOT EXISTS on a table the snapshot already has is a no-op: never empty.
func TestCheckMigrationCreateTableIfNotExistsNotTracked(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE IF NOT EXISTS orders (id bigint);\nCREATE INDEX ON orders (id)")
	if got := checkByOp(t, checks, "CREATE TABLE").Safety; got != SafetySafe {
		t.Errorf("CREATE TABLE IF NOT EXISTS itself is safe, got %q", got)
	}
	c := checkByOp(t, checks, "CREATE INDEX")
	if c.Safety == SafetySafe {
		t.Errorf("orders already exists with rows; index build must not be safe, got %q", c.Safety)
	}
}

// IF NOT EXISTS on a table absent from the snapshot creates it, so later
// statements see an empty table and carry the snapshot-currency caveat.
func TestCheckMigrationIntrafileIfNotExistsAbsent(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
		op   string
	}{
		{
			name: "index",
			ddl:  "CREATE TABLE IF NOT EXISTS new_t (id bigint);\nCREATE INDEX IF NOT EXISTS idx_new_t ON new_t (id)",
			op:   "CREATE INDEX",
		},
		{
			name: "primary key",
			ddl:  "CREATE TABLE IF NOT EXISTS new_t (id bigint);\nALTER TABLE new_t ADD PRIMARY KEY (id)",
			op:   "ADD PRIMARY KEY",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := checkByOp(t, mustCheck(t, tc.ddl), tc.op)
			if c.Safety != SafetySafe {
				t.Errorf("expected safe on a table created by IF NOT EXISTS, got %q", c.Safety)
			}
			if c.RowEstimate == nil || *c.RowEstimate != 0 {
				t.Errorf("expected 0 rows, got %v", c.RowEstimate)
			}
			if c.Rationale == nil || !strings.Contains(c.Rationale.Reason, "created empty earlier") {
				t.Errorf("expected the empty-table reason, got %v", c.Rationale)
			}
			if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "snapshot is current") {
				t.Errorf("expected the snapshot-currency caveat, got %v", c.Rationale)
			}
			if !strings.Contains(c.Recommendation, "snapshot is current") {
				t.Errorf("expected the recommendation to carry the snapshot caveat, got %q", c.Recommendation)
			}
		})
	}
}

// Unconditional CREATE TABLE does not carry the snapshot-currency caveat.
func TestCheckMigrationIntrafileNonConditionalHasNoCaveat(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE new_t (id bigint);\nCREATE INDEX ON new_t (id)")
	c := checkByOp(t, checks, "CREATE INDEX")
	if c.Safety != SafetySafe {
		t.Fatalf("expected safe, got %q", c.Safety)
	}
	if strings.Contains(c.Recommendation, "snapshot is current") {
		t.Errorf("unconditional CREATE TABLE must not carry snapshot caveat, got %q", c.Recommendation)
	}
	if c.Rationale != nil && strings.Contains(c.Rationale.Note, "snapshot is current") {
		t.Errorf("unconditional CREATE TABLE must not carry snapshot caveat in note, got %q", c.Rationale.Note)
	}
}

// A second CREATE TABLE IF NOT EXISTS after a data load is a no-op: it cannot
// re-empty the table.
func TestCheckMigrationIntrafileIfNotExistsDataLoad(t *testing.T) {
	ddl := "CREATE TABLE IF NOT EXISTS t (id bigint);\n" +
		"INSERT INTO t VALUES (1);\n" +
		"CREATE TABLE IF NOT EXISTS t (id bigint);\n" +
		"CREATE INDEX ON t (id)"
	checks := mustCheck(t, ddl)
	c := checkByOp(t, checks, "CREATE INDEX")
	if c.Safety == SafetySafe {
		t.Errorf("second CREATE TABLE IF NOT EXISTS after INSERT must not re-empty the table, got safe")
	}
}

// A table the snapshot already has stays unsafe: the CREATE is a no-op.
func TestCheckMigrationIntrafileIfNotExistsPresent(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE IF NOT EXISTS users (id bigint);\nCREATE INDEX ON users (id)")
	if c := checkByOp(t, checks, "CREATE INDEX"); c.Safety != SafetyDangerous {
		t.Errorf("users already exists with 2M rows; index build must stay dangerous, got %q", c.Safety)
	}
}

// Without a snapshot, absence is unknowable, so IF NOT EXISTS cannot imply empty.
func TestCheckMigrationIntrafileIfNotExistsNoSnapshot(t *testing.T) {
	checks, err := CheckMigration("CREATE TABLE IF NOT EXISTS new_t (id bigint);\nCREATE INDEX ON new_t (id)", nil)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if c := checkByOp(t, checks, "CREATE INDEX"); c.Safety == SafetySafe {
		t.Errorf("without a snapshot the table cannot be assumed absent, got safe")
	}
}

// A data load between CREATE and ALTER breaks the emptiness assumption.
func TestCheckMigrationDataLoadInvalidatesEmpty(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
	}{
		{"insert", "CREATE TABLE t (id bigint);\nINSERT INTO t SELECT id FROM users;\nALTER TABLE t ADD PRIMARY KEY (id)"},
		{"copy", "CREATE TABLE t (id bigint);\nCOPY t FROM '/tmp/t.csv';\nALTER TABLE t ADD PRIMARY KEY (id)"},
		{"do block", "CREATE TABLE t (id bigint);\nDO $$ BEGIN INSERT INTO t VALUES (1); END $$;\nALTER TABLE t ADD PRIMARY KEY (id)"},
		{"call procedure", "CREATE TABLE t (id bigint);\nCALL load_t();\nALTER TABLE t ADD PRIMARY KEY (id)"},
		{"merge", "CREATE TABLE t (id bigint);\nMERGE INTO t USING users u ON t.id = u.id WHEN NOT MATCHED THEN INSERT (id) VALUES (u.id);\nALTER TABLE t ADD PRIMARY KEY (id)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := checkByOp(t, mustCheck(t, tc.ddl), "ADD PRIMARY KEY")
			if c.Safety == SafetySafe {
				t.Errorf("a table loaded after CREATE must not be treated as empty, got safe")
			}
			if c.RowEstimate != nil && *c.RowEstimate == 0 {
				t.Errorf("row estimate must not be pinned to 0 after a data load")
			}
		})
	}
}

// Attaching a partition can bring rows into a parent created in the same file.
func TestCheckMigrationAttachPartitionInvalidatesEmpty(t *testing.T) {
	ddl := "CREATE TABLE p (id int) PARTITION BY RANGE (id);\n" +
		"CREATE TABLE c (id int);\n" +
		"ALTER TABLE p ATTACH PARTITION c FOR VALUES FROM (1) TO (2);\n" +
		"ALTER TABLE p ADD PRIMARY KEY (id)"
	c := checkByOp(t, mustCheck(t, ddl), "ADD PRIMARY KEY")
	if c.Safety == SafetySafe {
		t.Errorf("parent had a partition attached; must not be treated as empty, got safe")
	}
}

// CREATE TABLE AS is born populated: caution, and later statements there are
// not softened.
func TestCheckMigrationCreateTableAs(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE recent AS SELECT * FROM users;\nCREATE INDEX ON recent (id)")
	c := checkByOp(t, checks, "CREATE TABLE AS")
	if c.Safety != SafetyCaution {
		t.Errorf("CTAS should be caution, got %q", c.Safety)
	}
	if got := checkByOp(t, checks, "CREATE INDEX").Safety; got == SafetySafe {
		t.Errorf("CTAS output is not empty; index build must not be safe, got %q", got)
	}
}

// A partitioned parent created in the file is empty: the plain index build is
// safe, and no CONCURRENTLY rewrite (which Postgres rejects there) is offered.
func TestCheckMigrationPartitionedParentCreatedInFile(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE p (id int) PARTITION BY RANGE (id);\nCREATE INDEX ON p (id)")
	c := checkByOp(t, checks, "CREATE INDEX")
	if c.Safety != SafetySafe {
		t.Errorf("empty partitioned parent index build should be safe, got %q", c.Safety)
	}
	if len(c.SaferSQL) != 0 {
		t.Errorf("no CONCURRENTLY rewrite for a partitioned parent, got %v", c.SaferSQL)
	}
}

// RENAME re-keys the catalog, so statements on the new name still see empty.
func TestCheckMigrationRenameFollowsCreatedTable(t *testing.T) {
	checks := mustCheck(t, "CREATE TABLE t (id bigint);\nALTER TABLE t RENAME TO t2;\nCREATE INDEX ON t2 (id)")
	if got := checkByOp(t, checks, "CREATE INDEX").Safety; got != SafetySafe {
		t.Errorf("index on the renamed created table should be safe, got %q", got)
	}
}

// An inline FK on CREATE TABLE still locks the referenced table; say so.
func TestCheckMigrationCreateTableInlineFKNote(t *testing.T) {
	c := checkByOp(t, mustCheck(t,
		"CREATE TABLE new_orders (id bigint, user_id bigint REFERENCES users(id))"),
		"CREATE TABLE")
	if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "users") {
		t.Errorf("expected an inline FK note naming users, got %v", c.Rationale)
	}
}

// migration_sql carries the CREATE TABLE and the untouched empty-table
// statements alongside the rewrite of a genuinely unsafe one.
func TestCheckMigrationComposeIncludesCreateTablePassthrough(t *testing.T) {
	checks := mustCheck(t,
		"CREATE TABLE t (id bigint);\nCREATE INDEX ON t (id);\nALTER TABLE orders ADD CHECK (total >= 0)")
	got := ComposeMigrationSQL(checks)
	if got == "" {
		t.Fatal("expected migration_sql")
	}
	if !strings.Contains(got, "CREATE TABLE t (id bigint);") {
		t.Errorf("CREATE TABLE did not pass through:\n%s", got)
	}
	if !strings.Contains(got, "CREATE INDEX ON t") {
		t.Errorf("empty-table index did not pass through unchanged:\n%s", got)
	}
	if _, err := pg_query.Parse(got); err != nil {
		t.Fatalf("migration_sql does not parse: %v\n%s", err, got)
	}
}
