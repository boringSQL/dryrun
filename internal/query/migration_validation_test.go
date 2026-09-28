package query

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

func sp(s string) *string { return &s }

// its own table: touching users/orders would shift constraint name allocation
func validationTestAnnotated() *schema.AnnotatedSchema {
	a := migrationTestAnnotated()
	a.Schema.Composites = []schema.CompositeType{{Schema: "public", Name: "addr"}}
	a.Schema.Tables = append(a.Schema.Tables, schema.Table{
		Schema: "public", Name: "accounts",
		Columns: []schema.Column{
			{Name: "id", TypeName: "bigint"},
			{Name: "ok", TypeName: "text"},
			{Name: "pending", TypeName: "text"},
			{Name: "either", TypeName: "text"},
			{Name: "a", TypeName: "text"},
			{Name: "b", TypeName: "text"},
			{Name: "home", TypeName: "public.addr"},
			{Name: "enf", TypeName: "text"},
			{Name: "MixedCase", TypeName: "text"},
		},
		Constraints: []schema.Constraint{
			{Name: "accounts_ok_nn", Kind: schema.ConstraintCheck, Definition: sp("CHECK ((ok IS NOT NULL))")},
			{Name: "accounts_pending_nn", Kind: schema.ConstraintCheck, Definition: sp("CHECK ((pending IS NOT NULL)) NOT VALID")},
			{Name: "accounts_either", Kind: schema.ConstraintCheck, Definition: sp("CHECK (((either IS NOT NULL) OR (id > 0)))")},
			{Name: "accounts_ab", Kind: schema.ConstraintCheck, Definition: sp("CHECK (((a IS NOT NULL) AND (b IS NOT NULL)))")},
			{Name: "accounts_home_nn", Kind: schema.ConstraintCheck, Definition: sp("CHECK ((home IS NOT NULL))")},
			{Name: "accounts_enf_nn", Kind: schema.ConstraintCheck, Definition: sp("CHECK ((enf IS NOT NULL)) NOT ENFORCED")},
			{Name: "accounts_mixed_nn", Kind: schema.ConstraintCheck, Definition: sp(`CHECK (("MixedCase" IS NOT NULL))`)},
			{Name: "accounts_user_fk", Kind: schema.ConstraintForeignKey, FKTable: sp("public.users"), Definition: sp("FOREIGN KEY (id) REFERENCES users(id) NOT VALID")},
		},
	})
	return a
}

func TestSetNotNullUsesExistingCheck(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		safety    SafetyRating
		safer     int
		mention   string
	}{
		{"validated check", "ALTER TABLE accounts ALTER COLUMN ok SET NOT NULL", SafetySafe, 0, "accounts_ok_nn"},
		{"not valid check", "ALTER TABLE accounts ALTER COLUMN pending SET NOT NULL", SafetyCaution, 2, "NOT VALID"},
		{"multi column and", "ALTER TABLE accounts ALTER COLUMN b SET NOT NULL", SafetySafe, 0, "accounts_ab"},
		{"or proves nothing", "ALTER TABLE accounts ALTER COLUMN either SET NOT NULL", SafetyCaution, 4, ""},
		{"composite column", "ALTER TABLE accounts ALTER COLUMN home SET NOT NULL", SafetyCaution, 4, ""},
		{"not enforced", "ALTER TABLE accounts ALTER COLUMN enf SET NOT NULL", SafetyCaution, 4, ""},
		{"mixed case", `ALTER TABLE accounts ALTER COLUMN "MixedCase" SET NOT NULL`, SafetySafe, 0, "accounts_mixed_nn"},
		{"no check", "ALTER TABLE accounts ALTER COLUMN id SET NOT NULL", SafetyCaution, 4, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checks, err := CheckMigration(tc.ddl, validationTestAnnotated())
			if err != nil {
				t.Fatal(err)
			}
			c := checks[0]
			if c.Safety != tc.safety {
				t.Errorf("safety = %s, want %s", c.Safety, tc.safety)
			}
			if len(c.SaferSQL) != tc.safer {
				t.Errorf("safer_sql = %v, want %d steps", c.SaferSQL, tc.safer)
			}
			if !strings.Contains(c.Recommendation, tc.mention) {
				t.Errorf("recommendation %q lacks %q", c.Recommendation, tc.mention)
			}
			for _, s := range c.SaferSQL {
				if strings.Contains(s, "DROP CONSTRAINT") && tc.safer != 4 {
					t.Errorf("must not drop a constraint this migration did not add: %s", s)
				}
			}
		})
	}
}

func TestSetNotNullSameFileSequence(t *testing.T) {
	ddl := `ALTER TABLE orders ADD CONSTRAINT orders_status_nn CHECK (status IS NOT NULL) NOT VALID;
ALTER TABLE orders VALIDATE CONSTRAINT orders_status_nn;
ALTER TABLE orders ALTER COLUMN status SET NOT NULL;
ALTER TABLE orders DROP CONSTRAINT orders_status_nn;`
	checks, err := CheckMigration(ddl, validationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 4 {
		t.Fatalf("checks = %d, want 4", len(checks))
	}
	if checks[2].Operation != "SET NOT NULL" || checks[2].Safety != SafetySafe {
		t.Errorf("SET NOT NULL = %s %s, want safe", checks[2].Operation, checks[2].Safety)
	}
	if !strings.Contains(checks[1].Recommendation, "Safe") {
		t.Errorf("validate of a file-added NOT VALID constraint should stay safe: %s", checks[1].Recommendation)
	}
}

func TestSetNotNullSameFileNotValidOnly(t *testing.T) {
	ddl := `ALTER TABLE orders ADD CONSTRAINT orders_status_nn CHECK (status IS NOT NULL) NOT VALID;
ALTER TABLE orders ALTER COLUMN status SET NOT NULL;`
	checks, err := CheckMigration(ddl, validationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[1].Safety != SafetyCaution || !strings.Contains(checks[1].Recommendation, "NOT VALID") {
		t.Errorf("SET NOT NULL after an unvalidated CHECK = %s: %s", checks[1].Safety, checks[1].Recommendation)
	}
}

func TestSetNotNullDroppedCheckIsNoProof(t *testing.T) {
	ddl := `ALTER TABLE accounts DROP CONSTRAINT accounts_ok_nn;
ALTER TABLE accounts ALTER COLUMN ok SET NOT NULL;`
	checks, err := CheckMigration(ddl, validationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if checks[1].Safety != SafetyCaution {
		t.Errorf("safety = %s, want caution once the proof is dropped", checks[1].Safety)
	}
}

func TestValidateConstraintVerdict(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		safety    SafetyRating
		mention   string
	}{
		{"already validated", "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_ok_nn", SafetySafe, "No-op"},
		{"not valid check", "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_pending_nn", SafetySafe, "autovacuum"},
		{"not valid fk", "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_user_fk", SafetySafe, "ROW SHARE on public.users"},
		{"not enforced", "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_enf_nn", SafetyCaution, "NOT ENFORCED"},
		{"unknown", "ALTER TABLE accounts VALIDATE CONSTRAINT nope", SafetySafe, "not in the snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checks, err := CheckMigration(tc.ddl, validationTestAnnotated())
			if err != nil {
				t.Fatal(err)
			}
			c := checks[0]
			if c.Safety != tc.safety || !strings.Contains(c.Recommendation, tc.mention) {
				t.Errorf("got %s %q, want %s containing %q", c.Safety, c.Recommendation, tc.safety, tc.mention)
			}
		})
	}
}

func TestUnvalidatedConstraintsReportedOncePerTable(t *testing.T) {
	ddl := `ALTER TABLE accounts ADD COLUMN x int;
ALTER TABLE accounts ADD COLUMN y int;`
	checks, err := CheckMigration(ddl, validationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"accounts_pending_nn", "accounts_user_fk"}
	if got := checks[0].UnvalidatedConstraints; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("first check lists %v, want %v (NOT ENFORCED excluded)", got, want)
	}
	if len(checks[1].UnvalidatedConstraints) != 0 {
		t.Errorf("second check repeats the list: %v", checks[1].UnvalidatedConstraints)
	}
	if !strings.Contains(checks[0].Recommendation, "Unvalidated on this table") {
		t.Errorf("recommendation lacks the note: %s", checks[0].Recommendation)
	}
}

func TestValidateExcludedFromItsOwnUnvalidatedList(t *testing.T) {
	checks, err := CheckMigration("ALTER TABLE accounts VALIDATE CONSTRAINT accounts_pending_nn", validationTestAnnotated())
	if err != nil {
		t.Fatal(err)
	}
	if got := checks[0].UnvalidatedConstraints; len(got) != 1 || got[0] != "accounts_user_fk" {
		t.Errorf("unvalidated = %v, want only accounts_user_fk", got)
	}
}

func TestConstraintValidityFromDefinition(t *testing.T) {
	for _, tc := range []struct {
		def                   string
		notValid, notEnforced bool
	}{
		{"CHECK ((a > 0))", false, false},
		{"CHECK ((a > 0)) NOT VALID", true, false},
		{"FOREIGN KEY (a) REFERENCES t(id) DEFERRABLE INITIALLY DEFERRED NOT VALID", true, false},
		{"CHECK ((a > 0)) NOT ENFORCED", false, true},
		{"CHECK ((note <> 'NOT VALID'))", false, false},
	} {
		c := schema.Constraint{Definition: sp(tc.def)}
		if c.IsNotValid() != tc.notValid || c.IsNotEnforced() != tc.notEnforced {
			t.Errorf("%q: notValid=%v notEnforced=%v", tc.def, c.IsNotValid(), c.IsNotEnforced())
		}
	}
	if (schema.Constraint{}).IsNotValid() {
		t.Error("nil definition must read as validated")
	}
}
