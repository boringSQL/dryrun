package query

import (
	"strings"
	"testing"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/pkg/jit"
)

func rewriteFor(t *testing.T, ddl string) []string {
	t.Helper()
	checks, err := CheckMigration(ddl, migrationTestSchema())
	if err != nil {
		t.Fatalf("%s: %v", ddl, err)
	}
	var out []string
	for _, c := range checks {
		out = append(out, c.SaferSQL...)
	}
	return out
}

func TestSaferSQL(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want []string
	}{
		{
			name: "foreign key keeps its name and its referential action",
			ddl:  "ALTER TABLE orders ADD CONSTRAINT fk_o_u FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE",
			want: []string{
				"ALTER TABLE orders ADD CONSTRAINT fk_o_u FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT fk_o_u;",
			},
		},
		{
			// an unnamed constraint cannot be validated, so the rewrite names it
			name: "composite foreign key across schemas",
			ddl:  "ALTER TABLE public.orders ADD FOREIGN KEY (user_id, tenant_id) REFERENCES app.users(id, tenant_id)",
			want: []string{
				"ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_tenant_id_fkey FOREIGN KEY (user_id, tenant_id) REFERENCES app.users (id, tenant_id) NOT VALID;",
				"ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_tenant_id_fkey;",
			},
		},
		{
			name: "check constraint",
			ddl:  "ALTER TABLE orders ADD CHECK (total >= 0)",
			want: []string{
				"ALTER TABLE orders ADD CONSTRAINT orders_total_check CHECK (total >= 0) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_total_check;",
			},
		},
		{
			name: "index keeps its predicate and its method",
			ddl:  "CREATE INDEX idx_o ON orders USING btree (user_id) WHERE status = 'open'",
			want: []string{"CREATE INDEX CONCURRENTLY idx_o ON orders USING btree (user_id) WHERE status = 'open';"},
		},
		{
			// a failed concurrent build leaves an INVALID index behind, so the
			// rewrite has to name what it creates
			name: "unique index without a name gets one",
			ddl:  "CREATE UNIQUE INDEX ON public.orders (user_id, created_at DESC)",
			want: []string{"CREATE UNIQUE INDEX CONCURRENTLY orders_user_id_created_at_idx ON public.orders USING btree (user_id, created_at DESC);"},
		},
		{
			name: "set not null on pg 12+",
			ddl:  "ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
			want: []string{
				"ALTER TABLE orders ADD CONSTRAINT orders_status_not_null CHECK (status IS NOT NULL) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_status_not_null;",
				"ALTER TABLE orders ALTER COLUMN status SET NOT NULL;",
				"ALTER TABLE orders DROP CONSTRAINT orders_status_not_null;",
			},
		},
		{
			name: "quoted identifiers stay quoted",
			ddl:  `ALTER TABLE "My Orders" ALTER COLUMN "Status" SET NOT NULL`,
			want: []string{
				`ALTER TABLE "My Orders" ADD CONSTRAINT "My Orders_Status_not_null" CHECK ("Status" IS NOT NULL) NOT VALID;`,
				`ALTER TABLE "My Orders" VALIDATE CONSTRAINT "My Orders_Status_not_null";`,
				`ALTER TABLE "My Orders" ALTER COLUMN "Status" SET NOT NULL;`,
				`ALTER TABLE "My Orders" DROP CONSTRAINT "My Orders_Status_not_null";`,
			},
		},
		{
			// one ALTER TABLE, two commands: the rewrite covers the reported one
			// and does not drag the other along
			name: "only the command being reported",
			ddl:  "ALTER TABLE orders ADD COLUMN a integer, ADD CONSTRAINT c CHECK (a > 0)",
			want: []string{
				"ALTER TABLE orders ADD CONSTRAINT c CHECK (a > 0) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT c;",
			},
		},

		{
			// statement 2 has to name the table exactly the way statement 1 does
			name: "quoted table on the constraint path",
			ddl:  `ALTER TABLE "My Orders" ADD FOREIGN KEY (user_id) REFERENCES users(id)`,
			want: []string{
				`ALTER TABLE "My Orders" ADD CONSTRAINT "My Orders_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users (id) NOT VALID;`,
				`ALTER TABLE "My Orders" VALIDATE CONSTRAINT "My Orders_user_id_fkey";`,
			},
		},
		{
			name: "only and if exists survive on the constraint path",
			ddl:  "ALTER TABLE IF EXISTS ONLY orders ADD CHECK (total >= 0)",
			want: []string{
				"ALTER TABLE IF EXISTS ONLY orders ADD CONSTRAINT orders_total_check CHECK (total >= 0) NOT VALID;",
				"ALTER TABLE IF EXISTS ONLY orders VALIDATE CONSTRAINT orders_total_check;",
			},
		},
		{
			// the CHECK would only prove something about the parent
			name: "set not null on only",
			ddl:  "ALTER TABLE ONLY orders ALTER COLUMN status SET NOT NULL",
			want: nil,
		},
		{
			name: "set not null on a table that may not exist",
			ddl:  "ALTER TABLE IF EXISTS orders ALTER COLUMN status SET NOT NULL",
			want: nil,
		},
		{
			name: "schema-qualified set not null",
			ddl:  "ALTER TABLE app.orders ALTER COLUMN status SET NOT NULL",
			want: []string{
				"ALTER TABLE app.orders ADD CONSTRAINT orders_status_not_null CHECK (status IS NOT NULL) NOT VALID;",
				"ALTER TABLE app.orders VALIDATE CONSTRAINT orders_status_not_null;",
				"ALTER TABLE app.orders ALTER COLUMN status SET NOT NULL;",
				"ALTER TABLE app.orders DROP CONSTRAINT orders_status_not_null;",
			},
		},
		{
			name: "one column named once",
			ddl:  "ALTER TABLE orders ADD CHECK (total > 0 AND total < 10)",
			want: []string{
				"ALTER TABLE orders ADD CONSTRAINT orders_total_check CHECK (total > 0 AND total < 10) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_total_check;",
			},
		},
		{
			name: "two derived names in one statement",
			ddl:  "ALTER TABLE orders ADD CHECK (total >= 0), ADD CHECK (total <= 100)",
			want: []string{
				"ALTER TABLE orders ADD CONSTRAINT orders_total_check CHECK (total >= 0) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_total_check;",
				"ALTER TABLE orders ADD CONSTRAINT orders_total_check1 CHECK (total <= 100) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_total_check1;",
			},
		},
		{
			name: "derived name already taken in the snapshot",
			ddl:  "ALTER TABLE legacy ADD CHECK (total >= 0)",
			want: []string{
				"ALTER TABLE legacy ADD CONSTRAINT legacy_total_check1 CHECK (total >= 0) NOT VALID;",
				"ALTER TABLE legacy VALIDATE CONSTRAINT legacy_total_check1;",
			},
		},
		{
			name: "generated index name avoids one the schema already has",
			ddl:  "CREATE INDEX ON orders (user_id)",
			want: []string{"CREATE INDEX CONCURRENTLY orders_user_id_idx1 ON orders USING btree (user_id);"},
		},

		{
			name: "exclusion constraint has no not valid form",
			ddl:  "ALTER TABLE orders ADD EXCLUDE USING gist (status WITH =)",
			want: nil,
		},
		{
			name: "concurrently is rejected on a partitioned table",
			ddl:  "CREATE INDEX ON events (created_at)",
			want: nil,
		},
		{
			name: "already not valid",
			ddl:  "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID",
			want: nil,
		},
		{
			name: "already concurrent",
			ddl:  "CREATE INDEX CONCURRENTLY idx_o ON orders (user_id)",
			want: nil,
		},
		{
			// the backfill needs a batching predicate nobody here can know
			name: "add column with a volatile default",
			ddl:  "ALTER TABLE orders ADD COLUMN seen_at timestamptz DEFAULT now()",
			want: nil,
		},
		{
			name: "column type change needs expand and swap",
			ddl:  "ALTER TABLE orders ALTER COLUMN total TYPE bigint",
			want: nil,
		},
		{
			name: "primary key is not a validate-later constraint",
			ddl:  "ALTER TABLE orders ADD PRIMARY KEY (id)",
			want: nil,
		},
		{
			name: "unique constraint is not a validate-later constraint",
			ddl:  "ALTER TABLE orders ADD CONSTRAINT u UNIQUE (user_id)",
			want: nil,
		},
		{
			name: "drop table",
			ddl:  "DROP TABLE orders",
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteFor(t, tc.ddl)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d statements, want %d:\n got: %#v\nwant: %#v", len(got), len(tc.want), got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("statement %d:\n got: %s\nwant: %s", i+1, got[i], tc.want[i])
				}
			}
		})
	}
}

// SET NOT NULL's CHECK shortcut is available on every supported major, so the
// rewrite is offered unconditionally -- CheckMigration no longer takes a
// version at all. Kept as a regression guard: this rewrite used to be gated on
// PG >= 12, and a snapshot with no parseable version silently lost it.
func TestSetNotNullRewriteIsAlwaysOffered(t *testing.T) {
	got := rewriteFor(t, "ALTER TABLE orders ALTER COLUMN status SET NOT NULL")
	if len(got) != 4 {
		t.Fatalf("expected the four-step rewrite, got %v", got)
	}
}

// The whole point of the field: what it hands back is not the thing it warned
// about. Every statement in a rewrite must itself come back rated safer.
func TestSaferSQLIsActuallySafer(t *testing.T) {
	ddls := []string{
		"ALTER TABLE orders ADD CONSTRAINT fk_o_u FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE",
		"ALTER TABLE public.orders ADD FOREIGN KEY (user_id, tenant_id) REFERENCES app.users(id, tenant_id)",
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"CREATE INDEX idx_o ON orders USING btree (user_id) WHERE status = 'open'",
		"CREATE UNIQUE INDEX ON public.orders (user_id, created_at DESC)",
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
		`ALTER TABLE "My Orders" ALTER COLUMN "Status" SET NOT NULL`,
	}
	for _, ddl := range ddls {
		steps := rewriteFor(t, ddl)
		if len(steps) == 0 {
			t.Fatalf("%s: expected a rewrite", ddl)
		}
		if len(steps) == 1 && steps[0] == ddl+";" {
			t.Fatalf("%s: the rewrite is the input", ddl)
		}
		rechecked, err := CheckMigration(strings.Join(steps, "\n"), migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: the rewrite does not parse: %v", ddl, err)
		}
		for _, c := range rechecked {
			if c.Safety == SafetyDangerous {
				t.Errorf("%s\n  rewrite still dangerous at %q: %s", ddl, c.Operation, strings.Join(steps, " "))
			}
			// a rewrite step check_migration doesn't recognize is just as much
			// a regression as one it rates dangerous -- both mean the rewrite
			// emitted something this tool cannot vouch for
			if c.Operation == "UNRECOGNIZED" {
				t.Errorf("%s\n  rewrite step unrecognized: %s", ddl, strings.Join(steps, " "))
			}
		}
	}
}

// safer_sql is the alternative to an unsafe statement; a safe one has nothing
// to be safer than.
func TestSaferSQLOnlyOnUnsafeChecks(t *testing.T) {
	ddls := []string{
		"ALTER TABLE users ADD COLUMN age integer",
		"ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID",
		"CREATE INDEX CONCURRENTLY idx_o ON orders (user_id)",
		"ALTER TABLE orders VALIDATE CONSTRAINT fk",
		"ALTER TABLE orders DROP COLUMN legacy",
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
	}
	for _, ddl := range ddls {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range checks {
			if c.Safety == SafetySafe && len(c.SaferSQL) > 0 {
				t.Errorf("%s: safe check %q carries safer_sql %v", ddl, c.Operation, c.SaferSQL)
			}
		}
	}
}

// Postgres truncates identifiers at 63 bytes; if the generated name is not
// truncated the same way, the VALIDATE names a constraint that does not exist.
func TestGeneratedConstraintNameFitsPostgres(t *testing.T) {
	long := strings.Repeat("o", 60)
	steps := rewriteFor(t, "ALTER TABLE "+long+" ALTER COLUMN status SET NOT NULL")
	if len(steps) != 4 {
		t.Fatalf("expected a rewrite, got %v", steps)
	}
	name := long + "_status_not_null"
	if len(name) <= pgIdentMax {
		t.Fatalf("test no longer exercises truncation")
	}
	want := name[:pgIdentMax]
	for _, s := range steps {
		if strings.Contains(s, name) {
			t.Fatalf("untruncated name survived: %s", s)
		}
	}
	if !strings.Contains(steps[1], want) {
		t.Errorf("VALIDATE does not name the truncated constraint: %s", steps[1])
	}
}

func TestGeneratedConstraintNameClipsWholeRunes(t *testing.T) {
	table := strings.Repeat("t", 41)
	col := strings.Repeat("é", 20)
	steps := rewriteFor(t, `ALTER TABLE `+table+` ALTER COLUMN "`+col+`" SET NOT NULL`)
	if len(steps) != 4 {
		t.Fatalf("expected a rewrite, got %v", steps)
	}
	for _, s := range steps {
		if !utf8.ValidString(s) {
			t.Fatalf("rewrite is not valid utf-8: %q", s)
		}
	}
}

func TestIndexBackedConstraintRecommendation(t *testing.T) {
	for _, tc := range []struct {
		ddl       string
		operation string
		mentions  []string
	}{
		{"ALTER TABLE orders ADD PRIMARY KEY (id)", "ADD PRIMARY KEY",
			[]string{"CREATE UNIQUE INDEX CONCURRENTLY", "USING INDEX", "NOT NULL"}},
		{"ALTER TABLE orders ADD CONSTRAINT u UNIQUE (user_id)", "ADD UNIQUE CONSTRAINT",
			[]string{"CREATE UNIQUE INDEX CONCURRENTLY", "USING INDEX"}},
		{"ALTER TABLE orders ADD EXCLUDE USING gist (status WITH =)", "ADD EXCLUSION CONSTRAINT",
			[]string{"CREATE INDEX CONCURRENTLY"}},
	} {
		checks, err := CheckMigration(tc.ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", tc.ddl, err)
		}
		if checks[0].Operation != tc.operation {
			t.Errorf("%s: operation %q, want %q", tc.ddl, checks[0].Operation, tc.operation)
		}
		rec := checks[0].Recommendation
		// none of these has a NOT VALID form, so prescribing one is wrong advice;
		// saying so in the reason is not
		if strings.Contains(rec, "NOT VALID;") {
			t.Errorf("%s: recommendation prescribes NOT VALID:\n%s", tc.ddl, rec)
		}
		for _, want := range tc.mentions {
			if !strings.Contains(rec, want) {
				t.Errorf("%s: recommendation does not mention %q:\n%s", tc.ddl, want, rec)
			}
		}
	}
}

// The recommendation is the fallback for when there is no rewrite. Where there
// is one, prose describing a second, differently-named migration beside it is
// worse than no prose.
func TestRecommendationDropsTheFixWhereThereIsARewrite(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)",
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"CREATE INDEX idx_o ON orders (user_id)",
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
	} {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if len(c.SaferSQL) == 0 {
			t.Fatalf("%s: expected a rewrite", ddl)
		}
		if strings.Contains(c.Recommendation, "FIX:") {
			t.Errorf("%s: recommendation still carries a fix beside safer_sql:\n%s", ddl, c.Recommendation)
		}
		if !strings.Contains(c.Recommendation, "REASON:") {
			t.Errorf("%s: recommendation lost the reason:\n%s", ddl, c.Recommendation)
		}
	}
}

// Every construction site must fill Rationale -- an agent reading rationale
// instead of parsing Recommendation prose should never get an empty reason.
func TestRationalePresentAcrossConstructionSites(t *testing.T) {
	ddls := []string{
		"ALTER TABLE orders DROP COLUMN legacy",                                                     // DROP COLUMN
		"ALTER TABLE orders ALTER COLUMN total TYPE bigint",                                         // ALTER COLUMN TYPE
		"ALTER TABLE orders VALIDATE CONSTRAINT fk",                                                 // VALIDATE CONSTRAINT
		"ALTER TABLE users ADD COLUMN age integer",                                                  // ADD COLUMN, no default
		"ALTER TABLE orders ADD COLUMN seen_at timestamptz DEFAULT now()",                           // ADD COLUMN, volatile default
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",                                       // SET NOT NULL, rewrite offered
		"ALTER TABLE ONLY orders ALTER COLUMN status SET NOT NULL",                                  // SET NOT NULL, no rewrite
		"ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID", // ADD CONSTRAINT, already NOT VALID
		"ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)",                         // ADD CONSTRAINT, rewrite offered
		"ALTER TABLE orders ADD PRIMARY KEY (id)",                                                   // ADD CONSTRAINT, no NOT VALID form at all
		"CREATE INDEX CONCURRENTLY idx_o ON orders (user_id)",                                       // CREATE INDEX, already concurrent
		"CREATE INDEX idx_o ON orders (user_id)",                                                    // CREATE INDEX, rewrite offered
		"ALTER TABLE orders RENAME COLUMN total TO amount",                                          // RENAME
		"DROP TABLE orders", // fallback keyword check
	}
	for _, ddl := range ddls {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if len(checks) == 0 {
			t.Fatalf("%s: expected a check", ddl)
		}
		for _, c := range checks {
			if c.Rationale == nil {
				t.Errorf("%s: %s has no rationale", ddl, c.Operation)
				continue
			}
			if c.Rationale.Reason == "" {
				t.Errorf("%s: %s rationale has an empty reason", ddl, c.Operation)
			}
		}
	}
}

// Where a jit.Entry backs the check, rationale must be the Entry's own
// Reason/Note verbatim -- not prose re-extracted from Recommendation.
func TestRationaleMatchesJitEntry(t *testing.T) {
	tests := []struct {
		ddl   string
		entry jit.Entry
	}{
		{"ALTER TABLE orders ALTER COLUMN total TYPE bigint", jit.AlterColumnType("orders", "total", "<new_type>")},
		{"ALTER TABLE orders ALTER COLUMN status SET NOT NULL", jit.SetNotNull("orders", "status")},
		{"ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)", jit.AddForeignKeyUnsafe("orders", "<col>", "<ref_table>", "<ref_col>")},
		{"ALTER TABLE orders ADD CHECK (total >= 0)", jit.AddCheckConstraintUnsafe("orders", "<expr>")},
		{"ALTER TABLE orders ADD PRIMARY KEY (id)", jit.AddIndexBackedConstraint("orders", "PRIMARY KEY", "id")},
		{"CREATE INDEX idx_o ON orders (user_id)", jit.CreateIndexBlocking("orders", "idx_o", "btree", "user_id")},
		{"ALTER TABLE orders RENAME COLUMN total TO amount", jit.Rename("<old_name>", "<new_name>")},
	}
	for _, tc := range tests {
		checks, err := CheckMigration(tc.ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", tc.ddl, err)
		}
		if len(checks) == 0 {
			t.Fatalf("%s: expected a check", tc.ddl)
		}
		c := checks[0]
		if c.Rationale == nil {
			t.Fatalf("%s: no rationale", tc.ddl)
		}
		if c.Rationale.Reason != tc.entry.Reason {
			t.Errorf("%s: rationale.reason = %q, want %q", tc.ddl, c.Rationale.Reason, tc.entry.Reason)
		}
		if c.Rationale.Note != tc.entry.Note {
			t.Errorf("%s: rationale.note = %q, want %q", tc.ddl, c.Rationale.Note, tc.entry.Note)
		}
	}
}

// SET NOT NULL downgrades Recommendation from String() to Warning() once a
// rewrite exists (migration.go's analyzeSetNotNull); rationale must not move
// with it -- it comes from the Entry either way. ADD CONSTRAINT has the same
// downgrade (analyzeAddConstraint), but FK/CHECK always get a mechanical
// rewrite through CheckMigration, so there is no reachable no-rewrite
// counterpart to compare against; SET NOT NULL is the one site where both
// sides of the downgrade are actually observable.
func TestRationaleSurvivesStringToWarningDowngrade(t *testing.T) {
	withRewrite, err := CheckMigration("ALTER TABLE orders ALTER COLUMN status SET NOT NULL", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	withoutRewrite, err := CheckMigration("ALTER TABLE ONLY orders ALTER COLUMN status SET NOT NULL", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if len(withRewrite[0].SaferSQL) == 0 {
		t.Fatal("expected a rewrite on the non-ONLY statement")
	}
	if len(withoutRewrite[0].SaferSQL) != 0 {
		t.Fatal("expected no rewrite on the ONLY statement")
	}
	if withRewrite[0].Rationale == nil || withoutRewrite[0].Rationale == nil {
		t.Fatal("expected rationale in both cases")
	}
	if withRewrite[0].Rationale.Reason != withoutRewrite[0].Rationale.Reason {
		t.Errorf("rationale.reason differs across the downgrade: %q vs %q",
			withRewrite[0].Rationale.Reason, withoutRewrite[0].Rationale.Reason)
	}
}

// ADD COLUMN with a DEFAULT cannot tell from the parse tree whether the
// default is volatile, so Recommendation hedges ("safe for immutable
// defaults... If the default IS volatile"). jit.AddColumnVolatileDefault's
// Reason asserts an unconditional rewrite, which is only true in the volatile
// case -- it must land in Note as detail, not as Rationale.Reason, or an
// agent reading only rationale gets an answer stronger than the evidence.
func TestRationaleHedgesAddColumnDefault(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE orders ADD COLUMN seen_at timestamptz DEFAULT now()", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	rationale := checks[0].Rationale
	if rationale == nil {
		t.Fatal("expected rationale")
	}
	if strings.Contains(rationale.Reason, "rewrites every row") {
		t.Errorf("rationale.reason asserts an unconditional rewrite the code cannot prove: %q", rationale.Reason)
	}
	if !strings.Contains(rationale.Reason, "immutable defaults") {
		t.Errorf("rationale.reason lost the hedge: %q", rationale.Reason)
	}
	if !strings.Contains(rationale.Note, "rewrites every row") {
		t.Errorf("rationale.note dropped the volatile-case detail: %q", rationale.Note)
	}
}

// The one branch where two notes merge: a non-concurrent CREATE INDEX on a
// partitioned table carries both CreateIndexBlocking's own note (cannot run
// in a transaction) and the partition-specific one appended to Recommendation
// at the same site. Both must survive in rationale.Note; neither may clobber
// the other.
func TestRationaleJoinsPartitionNote(t *testing.T) {
	checks, err := CheckMigration("CREATE INDEX ON events (created_at)", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if len(checks[0].SaferSQL) != 0 {
		t.Fatal("expected no rewrite on a partitioned table")
	}
	rationale := checks[0].Rationale
	if rationale == nil {
		t.Fatal("expected rationale")
	}
	if !strings.Contains(rationale.Note, "Cannot run inside a transaction") {
		t.Errorf("rationale.note lost the jit entry's own note: %q", rationale.Note)
	}
	if !strings.Contains(rationale.Note, "CONCURRENTLY is rejected on a partitioned table") {
		t.Errorf("rationale.note lost the partition-specific note: %q", rationale.Note)
	}
}

// Present for an all-rewritable multi-statement DDL, in input order.
func TestComposeMigrationSQLPresentWhenAllRewritable(t *testing.T) {
	checks, err := CheckMigration(
		"ALTER TABLE orders ADD CHECK (total >= 0);\nCREATE INDEX ON orders (status)",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	got := ComposeMigrationSQL(checks)
	if got == "" {
		t.Fatal("expected migration_sql, got none")
	}
	if _, err := pg_query.Parse(got); err != nil {
		t.Fatalf("migration_sql does not parse: %v\n%s", err, got)
	}
	checkIdx := strings.Index(got, "ADD CONSTRAINT orders_total_check")
	indexIdx := strings.Index(got, "CREATE INDEX CONCURRENTLY")
	if checkIdx == -1 || indexIdx == -1 || checkIdx > indexIdx {
		t.Errorf("statements out of input order:\n%s", got)
	}
}

// Absent when any unsafe statement has no mechanical rewrite -- a file that
// looks saveable but still holds an unsafe statement is worse than no file.
func TestComposeMigrationSQLAbsentWhenAnyUnsafeHasNoRewrite(t *testing.T) {
	checks, err := CheckMigration(
		"ALTER TABLE orders ADD CHECK (total >= 0);\nALTER TABLE orders ALTER COLUMN total TYPE bigint",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if got := ComposeMigrationSQL(checks); got != "" {
		t.Errorf("expected no migration_sql, got:\n%s", got)
	}
}

// RENAME never carries a rewrite, so it suppresses migration_sql for the
// whole input the same way ALTER COLUMN TYPE does -- not just for itself.
func TestComposeMigrationSQLAbsentWhenRenamePresent(t *testing.T) {
	checks, err := CheckMigration(
		"ALTER TABLE orders ADD CHECK (total >= 0);\nALTER TABLE orders RENAME COLUMN total TO amount",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if got := ComposeMigrationSQL(checks); got != "" {
		t.Errorf("expected no migration_sql, got:\n%s", got)
	}
}

// A statement type CheckMigration cannot analyze (e.g. CREATE TABLE, or a
// DROP that isn't DROP TABLE) must not silently vanish from migration_sql: it
// becomes its own unrewritable check, which suppresses the whole file rather
// than emitting one with that statement quietly missing.
func TestComposeMigrationSQLAbsentWhenStatementIsUnrecognized(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE orders ADD CHECK (total >= 0);\nCREATE TABLE audit_log (id bigint)",
		"ALTER TABLE orders ADD CHECK (total >= 0);\nDROP VIEW some_view", // a DROP that isn't DROP TABLE or DROP INDEX
	} {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		found := false
		for _, c := range checks {
			if c.Operation == "UNRECOGNIZED" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: expected an UNRECOGNIZED check, got %v", ddl, checks)
		}
		if got := ComposeMigrationSQL(checks); got != "" {
			t.Errorf("%s: expected no migration_sql -- the unrecognized statement would be missing from it, got:\n%s", ddl, got)
		}
	}
}

// A migration file commonly sets lock_timeout at the top; that must not be
// misjudged as unsafe and must not block migration_sql the way an
// actually-unmodeled statement does. BEGIN/COMMIT is a separate case (below)
// since, unlike SET, it cannot be composed around a rewrite at all.
func TestComposeMigrationSQLPassesThroughSetStatement(t *testing.T) {
	checks, err := CheckMigration(
		"SET lock_timeout = '3s';\nALTER TABLE orders ADD CHECK (total >= 0);",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Operation != "SET" || checks[0].Safety != SafetySafe {
		t.Fatalf("expected SET to be safe, got %s/%s", checks[0].Operation, checks[0].Safety)
	}
	if !strings.Contains(checks[0].Recommendation, "one session") {
		t.Errorf("SET recommendation should tie its effect to running in one session: %s", checks[0].Recommendation)
	}
	got := ComposeMigrationSQL(checks)
	if got == "" {
		t.Fatal("expected migration_sql")
	}
	if !strings.Contains(got, "SET lock_timeout") {
		t.Errorf("migration_sql lost the SET statement:\n%s", got)
	}
	// the header advises per-statement transactions; it must also say one
	// session, or that advice silently discards this very SET
	if !strings.Contains(got, "one session") {
		t.Errorf("migration_sql header does not call for one session:\n%s", got)
	}
	if _, err := pg_query.Parse(got); err != nil {
		t.Fatalf("migration_sql does not parse: %v\n%s", err, got)
	}
}

// BEGIN/COMMIT must NOT be bundled around a rewrite: CREATE/DROP INDEX
// CONCURRENTLY cannot run inside a transaction at all, and a multi-step
// rewrite (e.g. SET NOT NULL's four statements) needs its own lock released
// between steps -- wrapping it in the caller's transaction is exactly what
// the composed file's own header warns against. migration_sql must suppress
// entirely rather than compose something that either fails outright or
// quietly reintroduces the lock-holding problem the rewrite exists to avoid.
func TestComposeMigrationSQLAbsentWhenTransactionControlPresent(t *testing.T) {
	for _, ddl := range []string{
		"BEGIN;\nCREATE INDEX ON orders (user_id);\nCOMMIT;",
		"BEGIN;\nALTER TABLE orders ADD CHECK (total >= 0);\nCOMMIT;",
		"BEGIN;\nSAVEPOINT sp1;\nALTER TABLE orders ADD CHECK (total >= 0);\nROLLBACK;",
	} {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if got := ComposeMigrationSQL(checks); got != "" {
			t.Errorf("%s: expected no migration_sql, got:\n%s", ddl, got)
		}
	}
}

// ALTER SYSTEM SET is a server-wide config change, not the session-scoped
// SET this tool passes through -- pg_query parses it as a distinct
// AlterSystemStmt node, not VariableSetStmt, so it must fall through to
// unmodeledCheck (and so suppress the gate) rather than ride along as a safe
// passthrough.
func TestAlterSystemSetIsUnrecognizedNotSetPassthrough(t *testing.T) {
	checks, err := CheckMigration(
		"ALTER SYSTEM SET work_mem = '1GB';\nALTER TABLE orders ADD CHECK (total >= 0);",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range checks {
		if c.Operation == "SET" {
			t.Fatalf("ALTER SYSTEM SET must not be rated as the session-scoped SET passthrough: %v", checks)
		}
		if c.Operation == "UNRECOGNIZED" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an UNRECOGNIZED check for ALTER SYSTEM SET, got %v", checks)
	}
	if got := ComposeMigrationSQL(checks); got != "" {
		t.Errorf("expected no migration_sql, got:\n%s", got)
	}
}

// DROP INDEX (with or without CONCURRENTLY) is safe and must pass through,
// not get swept into the same bucket as a statement check_migration cannot
// analyze at all.
func TestComposeMigrationSQLPassesThroughDropIndex(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE orders ADD CHECK (total >= 0);\nDROP INDEX orders_user_id_idx",
		"ALTER TABLE orders ADD CHECK (total >= 0);\nDROP INDEX CONCURRENTLY orders_user_id_idx",
	} {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		got := ComposeMigrationSQL(checks)
		if got == "" {
			t.Fatalf("%s: expected migration_sql", ddl)
		}
		if !strings.Contains(got, "DROP INDEX") {
			t.Errorf("%s: migration_sql lost the DROP INDEX passthrough:\n%s", ddl, got)
		}
	}
}

// CASCADE on DROP INDEX has the same reach as on DROP CONSTRAINT (it takes
// the constraint the index backs, and foreign keys referencing it), so it
// gets the same caution rating rather than the plain form's safe.
func TestDropIndexCascadeIsCaution(t *testing.T) {
	plain, err := CheckMigration("DROP INDEX orders_user_id_idx", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].Safety != SafetySafe {
		t.Fatalf("plain DROP INDEX should be safe, got %s", plain[0].Safety)
	}
	cascade, err := CheckMigration("DROP INDEX orders_user_id_idx CASCADE", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if cascade[0].Safety != SafetyCaution {
		t.Errorf("CASCADE DROP INDEX should be caution, got %s", cascade[0].Safety)
	}
	if !strings.Contains(cascade[0].Recommendation, "CASCADE") {
		t.Errorf("recommendation should call out CASCADE: %s", cascade[0].Recommendation)
	}
	// caution with no rewrite blocks migration_sql, same as CASCADE on
	// DROP CONSTRAINT -- the file must not look saveable with it inside
	mixed, err := CheckMigration(
		"ALTER TABLE orders ADD CHECK (total >= 0);\nDROP INDEX orders_user_id_idx CASCADE",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if got := ComposeMigrationSQL(mixed); got != "" {
		t.Errorf("expected no migration_sql with a CASCADE DROP INDEX in the input, got:\n%s", got)
	}
}

// CASCADE on DROP CONSTRAINT reaches beyond this table (e.g. foreign keys in
// other tables), so it is downgraded from the plain, safe case rather than
// passed through with the same "metadata-only" framing.
func TestDropConstraintCascadeIsCaution(t *testing.T) {
	plain, err := CheckMigration("ALTER TABLE orders DROP CONSTRAINT old_check", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].Safety != SafetySafe {
		t.Fatalf("plain DROP CONSTRAINT should be safe, got %s", plain[0].Safety)
	}
	cascade, err := CheckMigration("ALTER TABLE orders DROP CONSTRAINT old_check CASCADE", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if cascade[0].Safety != SafetyCaution {
		t.Errorf("CASCADE DROP CONSTRAINT should be caution, got %s", cascade[0].Safety)
	}
	if !strings.Contains(cascade[0].Recommendation, "CASCADE") {
		t.Errorf("recommendation should call out CASCADE: %s", cascade[0].Recommendation)
	}
}

// DROP CONSTRAINT is metadata-only and safe; it must pass through into
// migration_sql like any other safe statement, not vanish (it is the cleanup
// step of SET NOT NULL's own rewrite, so this also guards that rewrite's
// self-consistency -- TestSaferSQLIsActuallySafer re-parses it separately).
func TestComposeMigrationSQLPassesThroughDropConstraint(t *testing.T) {
	checks, err := CheckMigration(
		"ALTER TABLE orders ADD CHECK (total >= 0);\nALTER TABLE orders DROP CONSTRAINT old_check",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	got := ComposeMigrationSQL(checks)
	if got == "" {
		t.Fatal("expected migration_sql")
	}
	if !strings.Contains(got, "DROP CONSTRAINT old_check") {
		t.Errorf("migration_sql lost the DROP CONSTRAINT passthrough:\n%s", got)
	}
}

// Absent when nothing is unsafe -- there is nothing to rewrite, so a bundle
// would just be the input back.
func TestComposeMigrationSQLAbsentWhenNothingUnsafe(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE orders DROP COLUMN legacy", migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	if got := ComposeMigrationSQL(checks); got != "" {
		t.Errorf("expected no migration_sql, got:\n%s", got)
	}
}

// A safe statement in a mixed file comes out unchanged -- quoting, ONLY and
// IF EXISTS intact -- beside the rewrite for the unsafe one.
func TestComposeMigrationSQLPassThroughFidelity(t *testing.T) {
	checks, err := CheckMigration(
		"ALTER TABLE IF EXISTS ONLY orders VALIDATE CONSTRAINT fk, ADD CHECK (total >= 0)",
		migrationTestSchema())
	if err != nil {
		t.Fatal(err)
	}
	got := ComposeMigrationSQL(checks)
	if got == "" {
		t.Fatal("expected migration_sql")
	}
	if !strings.Contains(got, "ALTER TABLE IF EXISTS ONLY orders VALIDATE CONSTRAINT fk;") {
		t.Errorf("safe statement did not pass through verbatim:\n%s", got)
	}
	if !strings.Contains(got, "ALTER TABLE IF EXISTS ONLY orders ADD CONSTRAINT orders_total_check CHECK (total >= 0) NOT VALID;") {
		t.Errorf("unsafe statement's rewrite missing or lost ONLY/IF EXISTS:\n%s", got)
	}
	if _, err := pg_query.Parse(got); err != nil {
		t.Fatalf("migration_sql does not parse: %v\n%s", err, got)
	}
}

// jit placeholders (<col>, <expr>, <ref_table>, ...) live only in
// Recommendation/RollbackDDL prose; one reaching a saveable file is the
// failure mode the rewritten==unsafe gate exists to prevent. The FK case is
// the meaningful one: its own RollbackDDL genuinely contains "<name>", so this
// proves ComposeMigrationSQL draws from Statement/SaferSQL and not from a
// sibling field that happens to carry a placeholder.
func TestComposeMigrationSQLHasNoPlaceholders(t *testing.T) {
	ddls := []string{
		"ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)",
		"ALTER TABLE orders ADD CHECK (total >= 0)",
		"CREATE INDEX ON orders (user_id)",
		"ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
	}
	for _, ddl := range ddls {
		checks, err := CheckMigration(ddl, migrationTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if ddl == ddls[0] && (checks[0].RollbackDDL == nil || !strings.Contains(*checks[0].RollbackDDL, "<name>")) {
			t.Fatalf("%s: test fixture no longer carries a placeholder to guard against", ddl)
		}
		got := ComposeMigrationSQL(checks)
		if got == "" {
			t.Fatalf("%s: expected migration_sql", ddl)
		}
		for _, placeholder := range []string{"<col>", "<expr>", "<ref_table>", "<ref_col>", "<default>", "<new_type>", "<auto>", "<name>"} {
			if strings.Contains(got, placeholder) {
				t.Errorf("%s: migration_sql carries placeholder %q:\n%s", ddl, placeholder, got)
			}
		}
	}
}
