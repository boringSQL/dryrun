package query

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

// An inline constraint on ADD COLUMN is the standalone ADD CONSTRAINT form:
// it must not read as an unconstrained, metadata-only column add.
func TestAddColumnInlineConstraintVerdicts(t *testing.T) {
	tests := []struct {
		name   string
		ddl    string
		safety SafetyRating
	}{
		{"not null without default fails on populated table", "ALTER TABLE users ADD COLUMN active boolean NOT NULL", SafetyDangerous},
		{"not null without default fails even on a small table", "ALTER TABLE orders ADD COLUMN active boolean NOT NULL", SafetyDangerous},
		{"unique builds an index under lock", "ALTER TABLE users ADD COLUMN code text UNIQUE", SafetyDangerous},
		{"unique is caution on a small table", "ALTER TABLE orders ADD COLUMN code text UNIQUE", SafetyCaution},
		{"foreign key scans under lock", "ALTER TABLE users ADD COLUMN org_id int REFERENCES users(id)", SafetyDangerous},
		{"check scans under lock", "ALTER TABLE users ADD COLUMN age int CHECK (age >= 0)", SafetyDangerous},
		{"stored generated rewrites the table", "ALTER TABLE users ADD COLUMN x int GENERATED ALWAYS AS (id * 2) STORED", SafetyDangerous},
		{"primary key without default fails on populated table", "ALTER TABLE orders ADD COLUMN id2 int PRIMARY KEY", SafetyDangerous},
		{"primary key with default is index-backed", "ALTER TABLE orders ADD COLUMN id2 int PRIMARY KEY DEFAULT 0", SafetyCaution},
		{"not null with default is metadata-only", "ALTER TABLE orders ADD COLUMN n int NOT NULL DEFAULT 0", SafetyCaution},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checks, err := CheckMigration(tt.ddl, migrationTestAnnotated())
			if err != nil {
				t.Fatalf("%s: %v", tt.ddl, err)
			}
			c := checks[0]
			if c.Operation != "ADD COLUMN" {
				t.Fatalf("%s: operation %q, want ADD COLUMN", tt.ddl, c.Operation)
			}
			if c.Safety != tt.safety {
				t.Errorf("%s: safety %q, want %q\n%s", tt.ddl, c.Safety, tt.safety, c.Recommendation)
			}
		})
	}
}

func TestAddColumnInlineConstraintRewrites(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want []string
	}{
		{
			name: "foreign key splits into NOT VALID + VALIDATE",
			ddl:  "ALTER TABLE orders ADD COLUMN org_id int REFERENCES users(id)",
			want: []string{
				"ALTER TABLE orders ADD COLUMN org_id int;",
				"ALTER TABLE orders ADD CONSTRAINT orders_org_id_fkey FOREIGN KEY (org_id) REFERENCES users (id) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_org_id_fkey;",
			},
		},
		{
			name: "check splits into NOT VALID + VALIDATE",
			ddl:  "ALTER TABLE users ADD COLUMN age int CHECK (age >= 0)",
			want: []string{
				"ALTER TABLE users ADD COLUMN age int;",
				"ALTER TABLE users ADD CONSTRAINT users_age_check CHECK (age >= 0) NOT VALID;",
				"ALTER TABLE users VALIDATE CONSTRAINT users_age_check;",
			},
		},
		{
			name: "unique splits into a concurrent index + USING INDEX",
			ddl:  "ALTER TABLE users ADD COLUMN code text UNIQUE",
			want: []string{
				"ALTER TABLE users ADD COLUMN code text;",
				"CREATE UNIQUE INDEX CONCURRENTLY users_code_idx ON users USING btree (code);",
				"ALTER TABLE users ADD CONSTRAINT users_code_key UNIQUE USING INDEX users_code_idx;",
			},
		},
		{
			name: "named inline constraints keep their names",
			ddl:  "ALTER TABLE orders ADD COLUMN x int CONSTRAINT x_pos CHECK (x > 0)",
			want: []string{
				"ALTER TABLE orders ADD COLUMN x int;",
				"ALTER TABLE orders ADD CONSTRAINT x_pos CHECK (x > 0) NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT x_pos;",
			},
		},
		{
			name: "named inline unique keeps its name",
			ddl:  "ALTER TABLE orders ADD COLUMN x int CONSTRAINT x_uni UNIQUE",
			want: []string{
				"ALTER TABLE orders ADD COLUMN x int;",
				"CREATE UNIQUE INDEX CONCURRENTLY x_uni ON orders USING btree (x);",
				"ALTER TABLE orders ADD CONSTRAINT x_uni UNIQUE USING INDEX x_uni;",
			},
		},
		{
			name: "only survives the split",
			ddl:  "ALTER TABLE ONLY orders ADD COLUMN org_id int REFERENCES users(id)",
			want: []string{
				"ALTER TABLE ONLY orders ADD COLUMN org_id int;",
				"ALTER TABLE ONLY orders ADD CONSTRAINT orders_org_id_fkey FOREIGN KEY (org_id) REFERENCES users (id) NOT VALID;",
				"ALTER TABLE ONLY orders VALIDATE CONSTRAINT orders_org_id_fkey;",
			},
		},
		{
			name: "quoted identifiers stay quoted",
			ddl:  `ALTER TABLE "My Orders" ADD COLUMN "Status" text UNIQUE`,
			want: []string{
				`ALTER TABLE "My Orders" ADD COLUMN "Status" text;`,
				`CREATE UNIQUE INDEX CONCURRENTLY "My Orders_Status_idx" ON "My Orders" USING btree ("Status");`,
				`ALTER TABLE "My Orders" ADD CONSTRAINT "My Orders_Status_key" UNIQUE USING INDEX "My Orders_Status_idx";`,
			},
		},
		{
			name: "check and unique both split, in order",
			ddl:  "ALTER TABLE orders ADD COLUMN c2 text UNIQUE CHECK (c2 <> '')",
			want: []string{
				"ALTER TABLE orders ADD COLUMN c2 text;",
				"ALTER TABLE orders ADD CONSTRAINT orders_c2_check CHECK (c2 <> '') NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_c2_check;",
				"CREATE UNIQUE INDEX CONCURRENTLY orders_c2_idx ON orders USING btree (c2);",
				"ALTER TABLE orders ADD CONSTRAINT orders_c2_key UNIQUE USING INDEX orders_c2_idx;",
			},
		},
		{
			name: "deferrable foreign key keeps its attributes",
			ddl:  "ALTER TABLE orders ADD COLUMN x int REFERENCES users(id) ON DELETE CASCADE DEFERRABLE",
			want: []string{
				"ALTER TABLE orders ADD COLUMN x int;",
				"ALTER TABLE orders ADD CONSTRAINT orders_x_fkey FOREIGN KEY (x) REFERENCES users (id) ON DELETE CASCADE DEFERRABLE NOT VALID;",
				"ALTER TABLE orders VALIDATE CONSTRAINT orders_x_fkey;",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rewriteFor(t, tt.ddl)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d statements, want %d:\n got: %#v\nwant: %#v", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("statement %d:\n got: %s\nwant: %s", i+1, got[i], tt.want[i])
				}
			}
		})
	}
}

// NOT NULL, PRIMARY KEY and STORED generated have no safe split: they must not
// offer a rewrite that would not run.
func TestAddColumnInlineConstraintNoRewrite(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users ADD COLUMN active boolean NOT NULL",
		"ALTER TABLE users ADD COLUMN x int GENERATED ALWAYS AS (id * 2) STORED",
		// CONCURRENTLY is rejected on a partitioned parent, so no unique split
		"ALTER TABLE events ADD COLUMN x int UNIQUE",
		// DEFERRABLE cannot survive the USING INDEX split
		"ALTER TABLE orders ADD COLUMN x int UNIQUE DEFERRABLE INITIALLY DEFERRED",
		// IF NOT EXISTS skips the inline constraint with the column; a split
		// ADD CONSTRAINT would still run against an existing column
		"ALTER TABLE users ADD COLUMN IF NOT EXISTS b int CHECK (b > 0)",
		// fixture is PG17: an FK blocks the whole split, even with a CHECK
		"ALTER TABLE events ADD COLUMN x int REFERENCES users(id) CHECK (x > 0)",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if len(checks[0].SaferSQL) != 0 {
			t.Errorf("%s: expected no SaferSQL, got %#v", ddl, checks[0].SaferSQL)
		}
		if !strings.Contains(checks[0].Recommendation, "FIX:") {
			t.Errorf("%s: no rewrite, so the recommendation must carry the fix:\n%s", ddl, checks[0].Recommendation)
		}
	}
}

// On a table this file just created, an inline constraint scans zero rows and
// stays safe -- the empty-table shortcut must win over the inline analysis.
func TestAddColumnInlineConstraintOnCreatedEmptyTable(t *testing.T) {
	ddl := "CREATE TABLE t (id bigint); ALTER TABLE t ADD COLUMN x int REFERENCES users(id)"
	checks, err := CheckMigration(ddl, migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	last := checks[len(checks)-1]
	if last.Operation != "ADD COLUMN" || last.Safety != SafetySafe {
		t.Errorf("%s: last check = %q/%q, want ADD COLUMN/safe", ddl, last.Operation, last.Safety)
	}
	if len(last.SaferSQL) != 0 {
		t.Errorf("%s: empty-table add must need no rewrite, got %#v", ddl, last.SaferSQL)
	}
}

func annotatedWithVersion(v string) *schema.AnnotatedSchema {
	a := migrationTestAnnotated()
	a.Schema.PgVersion = v
	return a
}

// PostgreSQL before 18 rejects NOT VALID foreign keys on a partitioned table
// ("cannot add NOT VALID foreign key on partitioned table"). Every FK path
// must agree: no NOT VALID split, per-partition advice, and a user-written
// NOT VALID must not pass as safe. An unknown version counts as older.
func TestPartitionedForeignKeyNotValid(t *testing.T) {
	const (
		inline     = "ALTER TABLE events ADD COLUMN x int REFERENCES users(id)"
		standalone = "ALTER TABLE events ADD CONSTRAINT events_x_fkey FOREIGN KEY (x) REFERENCES users(id)"
		notValid   = "ALTER TABLE events ADD CONSTRAINT events_x_fkey FOREIGN KEY (x) REFERENCES users(id) NOT VALID"
	)
	for _, v := range []string{"PostgreSQL 17.0", "garbage"} {
		a := annotatedWithVersion(v)
		for _, ddl := range []string{inline, standalone} {
			checks, err := CheckMigration(ddl, a)
			if err != nil {
				t.Fatalf("%s: %v", ddl, err)
			}
			c := checks[0]
			if len(c.SaferSQL) != 0 {
				t.Errorf("[%s] %s: NOT VALID split would fail, got %#v", v, ddl, c.SaferSQL)
			}
			if !strings.Contains(c.Recommendation, "For each partition") {
				t.Errorf("[%s] %s: expected per-partition fix:\n%s", v, ddl, c.Recommendation)
			}
		}
		checks, err := CheckMigration(notValid, a)
		if err != nil {
			t.Fatal(err)
		}
		if c := checks[0]; c.Safety != SafetyDangerous || !strings.Contains(c.Recommendation, "before PostgreSQL 18") {
			t.Errorf("[%s] user-written NOT VALID FK on partitioned table: %q\n%s", v, c.Safety, c.Recommendation)
		}
	}

	a := annotatedWithVersion("PostgreSQL 18.1")
	for _, ddl := range []string{inline, standalone} {
		checks, err := CheckMigration(ddl, a)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(checks[0].SaferSQL, "\n"); !strings.Contains(got, "NOT VALID") {
			t.Errorf("[18] %s: NOT VALID works on 18, expected the split, got %q", ddl, got)
		}
	}
	checks, err := CheckMigration(notValid, a)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("[18] user-written NOT VALID FK should stay safe, got %q", checks[0].Safety)
	}
}

// CHECK NOT VALID is fine on a partitioned table; only the FK is refused.
func TestPartitionedCheckStillSplits(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE events ADD COLUMN x int CHECK (x > 0)", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(checks[0].SaferSQL, "\n"); !strings.Contains(got, "CHECK (x > 0) NOT VALID") {
		t.Errorf("expected CHECK split on partitioned table, got %q", got)
	}
}

// A partitioned parent created earlier in the file, no longer empty, is still
// partitioned: the file catalog must feed the same guard as the snapshot.
func TestPartitionedForeignKeyFromFileCatalog(t *testing.T) {
	ddl := "CREATE TABLE p (id int, x int) PARTITION BY RANGE (id);\n" +
		"INSERT INTO p SELECT 1, 1;\n" +
		"ALTER TABLE p ADD CONSTRAINT p_x_fkey FOREIGN KEY (x) REFERENCES users(id);"
	checks, err := CheckMigration(ddl, migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	last := checks[len(checks)-1]
	if last.Operation != "ADD FOREIGN KEY" {
		t.Fatalf("last check %q, want ADD FOREIGN KEY", last.Operation)
	}
	// not safe: proves the empty-table shortcut did not answer instead
	if last.Safety == SafetySafe {
		t.Fatalf("populated in-file table must reach the FK analysis, got safe")
	}
	if len(last.SaferSQL) != 0 {
		t.Errorf("in-file partitioned parent: NOT VALID split would fail, got %#v", last.SaferSQL)
	}
	if !strings.Contains(last.Recommendation, "For each partition") {
		t.Errorf("expected per-partition fix:\n%s", last.Recommendation)
	}
}
