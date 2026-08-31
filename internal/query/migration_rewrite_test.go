package query

import (
	"strings"
	"testing"
	"unicode/utf8"
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
