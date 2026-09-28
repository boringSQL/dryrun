package query

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// The size-dependent verdicts name why the worst case was assumed, and only
// when a trusted small-table reading could have softened them.
func TestCheckMigrationSizingContext(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want string
	}{
		{"create index", "CREATE INDEX idx_o ON orders (status)", "missing_sizing"},
		{"alter column type", "ALTER TABLE orders ALTER COLUMN total TYPE bigint", "missing_sizing"},
		{"add check constraint", "ALTER TABLE orders ADD CONSTRAINT ck CHECK (total >= 0)", "missing_sizing"},
		{"add unique column", "ALTER TABLE orders ADD COLUMN code text UNIQUE", "missing_sizing"},
		{"set logged", "ALTER TABLE orders SET LOGGED", "missing_sizing"},
		{"reindex table", "REINDEX TABLE orders", "missing_sizing"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := migrationTestAnnotated()
			// orders keeps its schema but loses its sizing entry
			var kept []schema.TableSizingEntry
			for _, e := range a.Planner.Tables {
				if e.Table.Name != "orders" {
					kept = append(kept, e)
				}
			}
			a.Planner.Tables = kept

			checks, err := CheckMigration(tc.ddl, a)
			if err != nil {
				t.Fatal(err)
			}
			c := checks[0]
			if c.Safety != SafetyDangerous {
				t.Errorf("%s: sizing missing must keep the worst case, got %q", tc.ddl, c.Safety)
			}
			if c.SizingContext != tc.want {
				t.Errorf("%s: sizing_context %q, want %q", tc.ddl, c.SizingContext, tc.want)
			}
			if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "assumes a large table") {
				t.Errorf("%s: expected the worst-case caveat in rationale.note, got %#v", tc.ddl, c.Rationale)
			}
		})
	}
}

// A known-large table softens nothing and carries no caveat: the reading exists,
// it just is not small.
func TestCheckMigrationSizingContextLargeTableHasNone(t *testing.T) {
	checks, err := CheckMigration("CREATE INDEX idx_u ON users (email)", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].SizingContext != "" {
		t.Errorf("a genuine large-table reading must not carry sizing_context, got %q", checks[0].SizingContext)
	}
}

// A known-small table carries the small-table note, not the missing-sizing one.
func TestCheckMigrationSizingContextSmallTableHasNone(t *testing.T) {
	checks, err := CheckMigration("CREATE INDEX idx_o ON orders (status)", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].SizingContext != "" {
		t.Errorf("a known-small table must not carry sizing_context, got %q", checks[0].SizingContext)
	}
	if checks[0].Safety != SafetyCaution {
		t.Errorf("small table should downgrade to caution, got %q", checks[0].Safety)
	}
}

// The four ways a reading can be unusable are named distinctly.
func TestCheckMigrationSizingContextReasons(t *testing.T) {
	ddl := "CREATE INDEX idx_o ON orders (status)"

	noSnap := &schema.AnnotatedSchema{}
	checks, err := CheckMigration(ddl, noSnap)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].SizingContext != "no_snapshot" {
		t.Errorf("no schema should read as no_snapshot, got %q", checks[0].SizingContext)
	}

	noPlan := migrationTestAnnotated()
	noPlan.Planner = nil
	checks, err = CheckMigration(ddl, noPlan)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].SizingContext != "missing_planner" {
		t.Errorf("schema without planner stats should read as missing_planner, got %q", checks[0].SizingContext)
	}

	stale := migrationTestAnnotated()
	stale.Planner.Timestamp = time.Now().Add(-30 * 24 * time.Hour)
	checks, err = CheckMigration(ddl, stale)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].SizingContext != "stale_planner" {
		t.Errorf("stale planner should read as stale_planner, got %q", checks[0].SizingContext)
	}

	never := migrationTestAnnotated()
	never.Planner.Tables = append(never.Planner.Tables, schema.TableSizingEntry{
		Table:  schema.QualifiedName{Schema: "public", Name: "legacy"},
		Sizing: schema.TableSizing{Reltuples: -1, TableSize: 8192},
	})
	checks, err = CheckMigration("CREATE INDEX idx_l ON legacy (total)", never)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].SizingContext != "missing_sizing" {
		t.Errorf("reltuples=-1 should read as missing_sizing, got %q", checks[0].SizingContext)
	}
}

// A non-downgradable hazard is dangerous at any table size, so a snapshot would
// not change the verdict and there is nothing to caveat.
func TestCheckMigrationSizingContextNonDowngradableHasNone(t *testing.T) {
	a := migrationTestAnnotated()
	a.Planner = nil
	checks, err := CheckMigration("ALTER TABLE orders ADD COLUMN active boolean NOT NULL", a)
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Fatalf("NOT NULL without default fails at any size, got %q", checks[0].Safety)
	}
	if checks[0].SizingContext != "" {
		t.Errorf("a size-invariant failure must not carry sizing_context, got %q", checks[0].SizingContext)
	}
}

// An unbounded UPDATE/DELETE is caution only because the table is not known
// small, so the missing reading is named like any other size-dependent verdict.
func TestCheckMigrationSizingContextDML(t *testing.T) {
	a := migrationTestAnnotated()
	var kept []schema.TableSizingEntry
	for _, e := range a.Planner.Tables {
		if e.Table.Name != "orders" {
			kept = append(kept, e)
		}
	}
	a.Planner.Tables = kept

	checks, err := CheckMigration("UPDATE orders SET total = 0", a)
	if err != nil {
		t.Fatal(err)
	}
	c := checks[0]
	if c.Safety != SafetyCaution {
		t.Errorf("unbounded UPDATE on an unsized table should stay caution, got %q", c.Safety)
	}
	if c.SizingContext != "missing_sizing" {
		t.Errorf("sizing_context %q, want missing_sizing", c.SizingContext)
	}
	if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "assumes a large table") {
		t.Errorf("expected the worst-case caveat, got %#v", c.Rationale)
	}
}

// A table created earlier in the same file is known empty, so no missing-sizing
// caveat should appear even where the empty-table path did not cover the subtype.
func TestCheckMigrationSizingContextFileCreatedTable(t *testing.T) {
	ddl := "CREATE TABLE t (id bigint);\nALTER TABLE t SET LOGGED;"
	checks, err := CheckMigration(ddl, &schema.AnnotatedSchema{})
	if err != nil {
		t.Fatal(err)
	}
	last := checks[len(checks)-1]
	if last.SizingContext != "" {
		t.Errorf("a file-created table must not carry a missing-sizing caveat, got %q", last.SizingContext)
	}
}

func TestCheckMigrationSizingContextAbsentWhenSizeIrrelevant(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE orders ADD COLUMN active boolean NOT NULL",
		"ALTER TABLE orders ADD COLUMN note text",
		"CREATE INDEX CONCURRENTLY idx_o ON orders (status)",
		"ALTER TABLE orders ADD CONSTRAINT ck CHECK (total >= 0) NOT VALID",
		"COMMENT ON TABLE orders IS 'x'",
		"ANALYZE orders",
		"CREATE TABLE fresh (id bigint)",
	} {
		for _, reason := range []string{"no-planner", "no-snapshot"} {
			a := migrationTestAnnotated()
			if reason == "no-planner" {
				a.Planner = nil
			} else {
				a = &schema.AnnotatedSchema{}
			}
			checks, err := CheckMigration(ddl, a)
			if err != nil {
				t.Fatalf("%s: %v", ddl, err)
			}
			if checks[0].SizingContext != "" {
				t.Errorf("[%s] %s: size-irrelevant verdict must not carry sizing_context, got %q", reason, ddl, checks[0].SizingContext)
			}
		}
	}
}
