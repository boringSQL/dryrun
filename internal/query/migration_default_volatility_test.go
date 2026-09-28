package query

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

// The default expression's volatility decides the verdict: a constant or
// STABLE/IMMUTABLE default is metadata-only, a VOLATILE one rewrites the table.
func TestAddColumnDefaultVolatility(t *testing.T) {
	safe := []string{
		"ALTER TABLE users ADD COLUMN n int DEFAULT 0",
		"ALTER TABLE users ADD COLUMN n int DEFAULT -1",
		"ALTER TABLE users ADD COLUMN b boolean DEFAULT true",
		"ALTER TABLE users ADD COLUMN s text DEFAULT 'x'",
		"ALTER TABLE users ADD COLUMN s text DEFAULT NULL",
		"ALTER TABLE users ADD COLUMN j jsonb DEFAULT '{}'::jsonb",
		"ALTER TABLE users ADD COLUMN a int[] DEFAULT ARRAY[1, 2]",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT now()",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT current_timestamp",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT pg_catalog.now()",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT timezone('utc', now())",
		"ALTER TABLE users ADD COLUMN s text DEFAULT lower('X')",
		"ALTER TABLE users ADD COLUMN n int DEFAULT 1 + 2",
	}
	for _, ddl := range safe {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Safety != SafetySafe {
			t.Errorf("%s: non-volatile default should be safe, got %q\n%s", ddl, checks[0].Safety, checks[0].Recommendation)
		}
	}

	volatile := []string{
		"ALTER TABLE users ADD COLUMN u uuid DEFAULT gen_random_uuid()",
		"ALTER TABLE users ADD COLUMN n bigint DEFAULT nextval('s')",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT clock_timestamp()",
		"ALTER TABLE users ADD COLUMN f float8 DEFAULT random()",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT timezone('utc', clock_timestamp())",
	}
	for _, ddl := range volatile {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if c.Safety != SafetyDangerous {
			t.Errorf("%s: volatile default on a large table should be dangerous, got %q\n%s", ddl, c.Safety, c.Recommendation)
		}
		if !strings.Contains(c.Rationale.Reason, "rewrites every row") {
			t.Errorf("%s: expected the rewrite reason, got %q", ddl, c.Rationale.Reason)
		}
	}
}

// Unprovable defaults keep the hedge: caution, with the rewrite in Note only.
func TestAddColumnUnknownDefaultKeepsHedge(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT app.custom_now()",
		"ALTER TABLE users ADD COLUMN t timestamptz DEFAULT custom_now()",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		c := checks[0]
		if c.Safety != SafetyCaution {
			t.Errorf("%s: unprovable default should stay caution, got %q", ddl, c.Safety)
		}
		if !strings.Contains(c.Rationale.Reason, "not provable") {
			t.Errorf("%s: expected the hedge reason, got %q", ddl, c.Rationale.Reason)
		}
	}
}

// A volatile default rewrites proportional to size, so a known-small table
// softens it and a missing reading is named.
func TestAddColumnVolatileDefaultSizeAware(t *testing.T) {
	small, err := CheckMigration("ALTER TABLE orders ADD COLUMN u uuid DEFAULT gen_random_uuid()", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if small[0].Safety != SafetyCaution {
		t.Errorf("volatile default on a small table should be caution, got %q", small[0].Safety)
	}
	if small[0].SizingContext != "" {
		t.Errorf("known-small table must not carry sizing_context, got %q", small[0].SizingContext)
	}

	a := migrationTestAnnotated()
	a.Planner = nil
	unknown, err := CheckMigration("ALTER TABLE users ADD COLUMN u uuid DEFAULT gen_random_uuid()", a)
	if err != nil {
		t.Fatal(err)
	}
	if unknown[0].Safety != SafetyDangerous {
		t.Errorf("volatile default with no reading should stay dangerous, got %q", unknown[0].Safety)
	}
	if unknown[0].SizingContext != "missing_planner" {
		t.Errorf("expected missing_planner, got %q", unknown[0].SizingContext)
	}
}

// STABLE built-ins do not rewrite: they must not be volatile.
func TestAddColumnStableDefaultFuncsAreSafe(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE users ADD COLUMN n int DEFAULT pg_backend_pid()",
		"ALTER TABLE users ADD COLUMN n bigint DEFAULT txid_current()",
		"ALTER TABLE users ADD COLUMN n bigint DEFAULT pg_current_xact_id()",
		"ALTER TABLE users ADD COLUMN s text DEFAULT pg_current_snapshot()::text",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Safety != SafetySafe {
			t.Errorf("%s: STABLE default should be safe, got %q\n%s", ddl, checks[0].Safety, checks[0].Recommendation)
		}
	}
}

// timezone(text, timetz) is VOLATILE through PG14; the timestamptz form is safe.
func TestAddColumnDefaultTimezoneOverloads(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE users ADD COLUMN t timestamptz DEFAULT timezone('utc', now())", migrationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("timezone(text, timestamptz) should be safe, got %q", checks[0].Safety)
	}

	for _, ddl := range []string{
		"ALTER TABLE users ADD COLUMN t timetz DEFAULT timezone('utc', current_time)",
		"ALTER TABLE users ADD COLUMN t timetz DEFAULT timezone('utc', '12:00'::timetz)",
	} {
		checks, err := CheckMigration(ddl, migrationTestAnnotated())
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Safety != SafetyDangerous {
			t.Errorf("%s: timezone(text, timetz) rewrites on PG14, should be dangerous, got %q", ddl, checks[0].Safety)
		}
	}
}

func annotatedWithDomain(checks bool) *schema.AnnotatedSchema {
	a := migrationTestAnnotated()
	d := schema.DomainType{
		Schema: "public", Name: "pos", BaseType: "integer",
	}
	if checks {
		d.CheckConstraints = []string{"VALUE > 0"}
	}
	a.Schema.Domains = append(a.Schema.Domains, d)
	return a
}

// A domain with CHECK constraints rewrites the table for a constant default;
// one without checks does not.
func TestAddColumnConstrainedDomainRewrites(t *testing.T) {
	ddl := "ALTER TABLE users ADD COLUMN c pos DEFAULT 1"

	checks, err := CheckMigration(ddl, annotatedWithDomain(true))
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("constrained domain default should be dangerous, got %q\n%s", checks[0].Safety, checks[0].Recommendation)
	}
	if !strings.Contains(checks[0].Rationale.Reason, "domain") {
		t.Errorf("expected the domain reason, got %q", checks[0].Rationale.Reason)
	}

	plain, err := CheckMigration(ddl, annotatedWithDomain(false))
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].Safety != SafetySafe {
		t.Errorf("unconstrained domain default should be safe, got %q", plain[0].Safety)
	}

	qualified, err := CheckMigration("ALTER TABLE users ADD COLUMN c public.pos DEFAULT 1", annotatedWithDomain(true))
	if err != nil {
		t.Fatal(err)
	}
	if qualified[0].Safety != SafetyDangerous {
		t.Errorf("schema-qualified domain must resolve, got %q", qualified[0].Safety)
	}
}

// Without a snapshot the column type cannot be cleared of being a domain: keep
// the hedge rather than say safe.
func TestAddColumnUnverifiedTypeWithoutSnapshot(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TABLE t ADD COLUMN s text DEFAULT 'x'",
		"ALTER TABLE t ADD COLUMN c pos DEFAULT 1",
	} {
		checks, err := CheckMigration(ddl, &schema.AnnotatedSchema{})
		if err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
		if checks[0].Safety != SafetyCaution {
			t.Errorf("%s: an unverified column type must stay caution, got %q", ddl, checks[0].Safety)
		}
	}

	// a pg_catalog-qualified builtin is known not to be a domain even offline
	checks, err := CheckMigration("ALTER TABLE t ADD COLUMN n int DEFAULT 0", &schema.AnnotatedSchema{})
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].Safety != SafetySafe {
		t.Errorf("pg_catalog builtin default should be safe offline, got %q", checks[0].Safety)
	}
}
