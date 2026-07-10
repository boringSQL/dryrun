package schema

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Live tests for the connection-layer read-only enforcement (PLAN-compliance
// Item 1). These assert the compliance guarantee itself: every session dryrun
// opens is read-only with defensive timeouts, an attempted write fails with
// SQLSTATE 25006, and the one sanctioned writer (InjectStats) can still write
// through a guarded connection. Gated on TEST_DATABASE_URL like stats_test.go.

func liveDryRun(t *testing.T) (*DryRun, context.Context) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live guards test")
	}
	ctx := context.Background()
	conn, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn, ctx
}

func settingValue(t *testing.T, ctx context.Context, conn *DryRun, name string) string {
	t.Helper()
	var v string
	if err := conn.Pool().QueryRow(ctx,
		"SELECT setting FROM pg_settings WHERE name = $1", name).Scan(&v); err != nil {
		t.Fatalf("pg_settings(%s): %v", name, err)
	}
	return v
}

// The default connection path must set all four session guards. pg_settings
// reports timeouts in milliseconds, so the expectations are the raw ms values
// from DefaultSessionGuards.
func TestConnect_SetsSessionGuards(t *testing.T) {
	conn, ctx := liveDryRun(t)

	want := map[string]string{
		"default_transaction_read_only":       "on",
		"statement_timeout":                   "30000",
		"lock_timeout":                        "2000",
		"idle_in_transaction_session_timeout": "10000",
	}
	for name, expected := range want {
		if got := settingValue(t, ctx, conn, name); got != expected {
			t.Errorf("%s = %q, want %q", name, got, expected)
		}
	}
}

// The compliance deliverable: a write through dryrun's default connection path
// must fail with SQLSTATE 25006 (read_only_sql_transaction), not silently
// succeed and not fail with some unrelated error.
func TestConnect_RefusesWrites(t *testing.T) {
	conn, ctx := liveDryRun(t)

	_, err := conn.Pool().Exec(ctx, "CREATE TABLE guards_write_probe (id int)")
	if err == nil {
		conn.Pool().Exec(ctx, "DROP TABLE guards_write_probe")
		t.Fatal("write succeeded through the default read-only connection path")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError, got %T: %v", err, err)
	}
	if pgErr.Code != "25006" {
		t.Errorf("SQLSTATE = %s, want 25006 (read_only_sql_transaction): %v", pgErr.Code, err)
	}
}

// A guard with everything disabled must leave the session untouched — the
// opt-out path for local-dev connections must not half-apply defaults.
func TestConnectWithGuards_ZeroDisables(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live guards test")
	}
	ctx := context.Background()
	conn, err := ConnectWithGuards(ctx, url, SessionGuards{})
	if err != nil {
		t.Fatalf("ConnectWithGuards: %v", err)
	}
	defer conn.Close()

	if got := settingValue(t, ctx, conn, "default_transaction_read_only"); got != "off" {
		t.Errorf("default_transaction_read_only = %q, want off (guards disabled)", got)
	}
}

// Regression guard for the sanctioned writer: InjectStats runs through a
// guarded (read-only-by-default) connection and must still be able to write,
// because it opts out with SET TRANSACTION READ WRITE at the top of its tx.
// Fixture setup/teardown uses a raw unguarded pool, mirroring the reality that
// only dryrun's own sessions carry the guards.
func TestInjectStats_WritesThroughGuardedConnection(t *testing.T) {
	raw := livePool(t)
	conn, ctx := liveDryRun(t)
	major := serverMajor(t, ctx, raw)

	if _, err := raw.Exec(ctx, `
		DROP TABLE IF EXISTS guards_inject;
		CREATE TABLE guards_inject (id int PRIMARY KEY, v text);
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { raw.Exec(ctx, "DROP TABLE IF EXISTS guards_inject") })

	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{Schema: "public", Name: "guards_inject", Columns: []Column{{Name: "id"}, {Name: "v"}}},
		}},
		Planner: &PlannerStatsSnapshot{Tables: []TableSizingEntry{
			{Table: qn("public", "guards_inject"), Sizing: TableSizing{Reltuples: 10_000, Relpages: 100}},
		}},
	}

	res, err := InjectStats(ctx, conn.Pool(), a, major)
	if err != nil {
		t.Fatalf("InjectStats through guarded connection: %v", err)
	}
	if res.TablesUpdated != 1 {
		t.Errorf("TablesUpdated = %d, want 1 (read-only opt-out broken?), warnings: %v",
			res.TablesUpdated, res.Warnings)
	}
}

// RoleReport must reflect pg_roles for current_user exactly — the preflight's
// fail-closed decision hangs off these three booleans.
func TestRoleReport_MatchesCatalog(t *testing.T) {
	conn, ctx := liveDryRun(t)

	report, err := conn.RoleReport(ctx)
	if err != nil {
		t.Fatalf("RoleReport: %v", err)
	}

	var (
		rolname                string
		super, repl, bypassrls bool
	)
	err = conn.Pool().QueryRow(ctx, `
		SELECT rolname, rolsuper, rolreplication, rolbypassrls
		FROM pg_roles WHERE rolname = current_user`).
		Scan(&rolname, &super, &repl, &bypassrls)
	if err != nil {
		t.Fatalf("pg_roles: %v", err)
	}

	if report.Rolname != rolname || report.Super != super ||
		report.Replication != repl || report.BypassRLS != bypassrls {
		t.Errorf("RoleReport = %+v, want {%s %t %t %t}", report, rolname, super, repl, bypassrls)
	}
}

// Pure classification tests: which combinations count as privileged and how
// they render in the refusal message.
func TestRoleReport_Privileges(t *testing.T) {
	cases := []struct {
		name       string
		report     RoleReport
		privileged bool
		privs      []string
	}{
		{"plain role", RoleReport{Rolname: "dryrun_readonly"}, false, nil},
		{"superuser", RoleReport{Rolname: "postgres", Super: true}, true, []string{"superuser"}},
		{"replication", RoleReport{Replication: true}, true, []string{"replication"}},
		{"bypassrls", RoleReport{BypassRLS: true}, true, []string{"bypassrls"}},
		{"all three", RoleReport{Super: true, Replication: true, BypassRLS: true}, true,
			[]string{"superuser", "replication", "bypassrls"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.report.Privileged(); got != tc.privileged {
				t.Errorf("Privileged() = %t, want %t", got, tc.privileged)
			}
			got := tc.report.Privileges()
			if len(got) != len(tc.privs) {
				t.Fatalf("Privileges() = %v, want %v", got, tc.privs)
			}
			for i := range got {
				if got[i] != tc.privs[i] {
					t.Errorf("Privileges()[%d] = %q, want %q", i, got[i], tc.privs[i])
				}
			}
		})
	}
}
