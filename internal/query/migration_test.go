package query

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/schema"
)

func migrationTestSchema() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
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
				Stats: &schema.TableStats{Reltuples: 1_000_000, TableSize: 100_000_000},
			},
		},
	}
}

func TestCheckMigrationAddColumn(t *testing.T) {
	snap := migrationTestSchema()
	pgVer := &dryrun.PgVersion{Major: 17, Minor: 0}
	checks, err := CheckMigration("ALTER TABLE users ADD COLUMN age integer", snap, pgVer)
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
	snap := migrationTestSchema()
	pgVer := &dryrun.PgVersion{Major: 17, Minor: 0}
	checks, err := CheckMigration("ALTER TABLE users ADD COLUMN age integer DEFAULT 0", snap, pgVer)
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
	snap := migrationTestSchema()
	checks, err := CheckMigration("CREATE INDEX idx_users_email ON users(email)", snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Safety != SafetyDangerous {
		t.Errorf("non-concurrent index should be dangerous, got %q", checks[0].Safety)
	}
}

func TestCheckMigrationCreateIndexConcurrently(t *testing.T) {
	snap := migrationTestSchema()
	checks, err := CheckMigration("CREATE INDEX CONCURRENTLY idx_users_email ON users(email)", snap, nil)
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
	snap := migrationTestSchema()
	checks, err := CheckMigration("DROP TABLE users", snap, nil)
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
	snap := migrationTestSchema()
	checks, err := CheckMigration("ALTER TABLE users RENAME TO customers", snap, nil)
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
