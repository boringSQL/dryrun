package query

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

func step0Annotated() *schema.AnnotatedSchema {
	a := migrationTestAnnotated()
	for i := range a.Schema.Tables {
		if tb := &a.Schema.Tables[i]; tb.Schema == "public" && tb.Name == "users" {
			tb.Columns = append(tb.Columns,
				schema.Column{Name: "code", TypeName: "character varying(20)", Nullable: true},
				schema.Column{Name: "tag", TypeName: "character(4)"})
			tb.Indexes = append(tb.Indexes,
				schema.Index{Name: "users_id_uidx", Columns: []string{"id"}, IsUnique: true, IsValid: true},
				schema.Index{Name: "users_code_uidx", Columns: []string{"code"}, IsUnique: true, IsValid: true})
		}
	}
	return a
}

func TestStep0Verdicts(t *testing.T) {
	const (
		add   = "ADD COLUMN"
		alter = "ALTER COLUMN TYPE"
		cic   = "CREATE CONCURRENTLY INDEX"
	)
	tests := []struct {
		ddl, op string
		safety  SafetyRating
	}{
		{"ALTER TABLE users ADD COLUMN n bigserial", add, SafetyDangerous},
		{"ALTER TABLE users ADD COLUMN n bigint GENERATED ALWAYS AS IDENTITY", add, SafetyDangerous},
		{"ALTER TABLE users ALTER COLUMN code TYPE varchar(50)", alter, SafetySafe},
		{"ALTER TABLE users ALTER COLUMN code TYPE text", alter, SafetySafe},
		{"ALTER TABLE users ALTER COLUMN code TYPE varchar(10)", alter, SafetyDangerous},
		{"ALTER TABLE users ALTER COLUMN tag TYPE text", alter, SafetyDangerous},
		{"CREATE INDEX CONCURRENTLY i ON events (created_at)", cic, SafetyDangerous},
		{"CREATE INDEX CONCURRENTLY i ON users (email)", cic, SafetySafe},
		{"ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY USING INDEX users_id_uidx", "ADD PRIMARY KEY", SafetySafe},
		{"ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY USING INDEX users_code_uidx", "ADD PRIMARY KEY", SafetyCaution},
		{"ALTER TABLE users ALTER COLUMN email SET NOT NULL", "SET NOT NULL", SafetyDangerous},
		{"ALTER TABLE orders ALTER COLUMN status SET NOT NULL", "SET NOT NULL", SafetyCaution},
		{"VACUUM FULL users", "VACUUM FULL", SafetyDangerous},
		{"VACUUM users", "VACUUM", SafetyCaution},
		{"TRUNCATE users CASCADE", "TRUNCATE", SafetyDangerous},
		{"CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION f()", "CREATE TRIGGER", SafetySafe},
		{"REFRESH MATERIALIZED VIEW mv", "REFRESH MATERIALIZED VIEW", SafetyDangerous},
	}
	for _, tt := range tests {
		checks, err := CheckMigration(tt.ddl, step0Annotated())
		if err != nil {
			t.Fatalf("%s: %v", tt.ddl, err)
		}
		if c := checks[0]; c.Operation != tt.op || c.Safety != tt.safety {
			t.Errorf("%s: got %s/%s, want %s/%s", tt.ddl, c.Operation, c.Safety, tt.op, tt.safety)
		}
	}
}

func TestStep0TransactionRules(t *testing.T) {
	find := func(checks []MigrationCheck, op string) MigrationCheck {
		for _, c := range checks {
			if c.Operation == op {
				return c
			}
		}
		t.Fatalf("no %s check in %v", op, checks)
		return MigrationCheck{}
	}

	checks, _ := CheckMigration("BEGIN;\nCREATE INDEX CONCURRENTLY i ON users (email);\nCOMMIT;", step0Annotated())
	if c := find(checks, "CREATE CONCURRENTLY INDEX"); c.Safety != SafetyDangerous || !strings.Contains(c.Recommendation, "transaction block") {
		t.Errorf("concurrent index inside BEGIN..COMMIT: %s: %s", c.Safety, c.Recommendation)
	}

	// ACCESS EXCLUSIVE from the DDL is held to COMMIT, across the full-table write
	checks, _ = CheckMigration("BEGIN;\nALTER TABLE users ADD COLUMN f boolean;\nUPDATE users SET f = false;\nCOMMIT;", step0Annotated())
	if c := find(checks, "UPDATE"); c.Safety != SafetyDangerous {
		t.Errorf("UPDATE after DDL in one transaction: %s: %s", c.Safety, c.Recommendation)
	}
	report, err := CheckMigrationFile("-- +goose Up\nALTER TABLE users ADD COLUMN f boolean;\nUPDATE users SET f = false;\n-- +goose Down\nSELECT 1;\n", "up", step0Annotated())
	if err != nil {
		t.Fatal(err)
	}
	if c := find(report.Checks, "UPDATE"); c.Safety != SafetyDangerous {
		t.Errorf("goose wraps the file in a transaction, UPDATE: %s", c.Safety)
	}

	ddl := "ALTER TABLE users ADD COLUMN a int;\nALTER TABLE users ADD COLUMN b int;"
	checks, _ = CheckMigration(ddl, step0Annotated())
	if checks[0].LockNote == "" || checks[1].LockNote != "" {
		t.Errorf("lock_note goes on the first ACCESS EXCLUSIVE check only: %q / %q", checks[0].LockNote, checks[1].LockNote)
	}
	checks, _ = CheckMigration("SET lock_timeout = '2s';\n"+ddl, step0Annotated())
	if checks[1].LockNote != "" {
		t.Errorf("lock_timeout is set, got %q", checks[1].LockNote)
	}
}
