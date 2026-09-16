package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/query"
	"github.com/boringsql/dryrun/internal/schema"
)

// checkFixture seeds a temp project: dryrun.toml, an initialized history.db,
// and a schema with a large `orders` and a small `users`, so size-aware
// verdicts are reachable offline.
func checkFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "dryrun.toml"), []byte("[project]\nid = \"p\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	// a leaked global from another test would key a different project
	flagConfig, flagProfile = "", ""
	t.Cleanup(func() { flagConfig, flagProfile = "", "" })

	store := testStoreAt(t, dir)
	ctx := context.Background()

	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0",
		Database:  "app",
		Timestamp: time.Now().UTC(),
		Tables: []schema.Table{
			{Schema: "public", Name: "orders", Columns: []schema.Column{
				{Name: "id", TypeName: "bigint"},
				{Name: "status", TypeName: "text"},
				{Name: "customer_id", TypeName: "bigint"},
			}},
			{Schema: "public", Name: "users", Columns: []schema.Column{
				{Name: "id", TypeName: "bigint"},
				{Name: "email", TypeName: "text"},
			}},
		},
	}
	snap.ContentHash = schema.DigestFor(snap)

	key := resolveSnapshotKey()
	if _, err := store.PutSchema(ctx, key, snap); err != nil {
		t.Fatal(err)
	}
	planner := &schema.PlannerStatsSnapshot{
		SchemaRefHash: snap.ContentHash,
		ContentHash:   "planner-p",
		Timestamp:     time.Now().UTC(),
		Tables: []schema.TableSizingEntry{
			{Table: schema.QualifiedName{Schema: "public", Name: "orders"},
				Sizing: schema.TableSizing{Reltuples: 2_000_000, Relpages: 250_000, TableSize: 2_000_000_000}},
			{Table: schema.QualifiedName{Schema: "public", Name: "users"},
				Sizing: schema.TableSizing{Reltuples: 500, Relpages: 10, TableSize: 81_920}},
		},
	}
	if _, err := store.PutPlanner(ctx, key, planner); err != nil {
		t.Fatal(err)
	}

	return filepath.Join(dir, "history.db")
}

func writeSQL(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func runMigrationCheck(t *testing.T, historyPath string, args ...string) (string, string, error) {
	t.Helper()
	cmd := checkMigrationCmd(&historyPath)
	var out, errBuf bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errBuf.String(), err
}

func runQueryCheck(t *testing.T, historyPath string, args ...string) (string, string, error) {
	t.Helper()
	cmd := checkQueryCmd(&historyPath)
	var out, errBuf bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errBuf.String(), err
}

func exitCodeOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exitError
	if errors.As(err, &exitErr) {
		return exitErr.code
	}
	t.Fatalf("unexpected error type %T: %v", err, err)
	return -1
}

func TestCheckMigration_DangerousExitsOne(t *testing.T) {
	hist := checkFixture(t)
	mig := writeSQL(t, filepath.Dir(hist), "large.sql", "CREATE INDEX idx_orders_status ON orders (status);\n")

	out, _, err := runMigrationCheck(t, hist, "--json", mig)
	if code := exitCodeOf(t, err); code != 1 {
		t.Fatalf("exit %d, want 1 for a blocking index on a 2M-row table", code)
	}

	var report query.MigrationReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &report); err != nil {
		t.Fatalf("single-file --json must be one object: %v\n%s", err, out)
	}
	if report.File != mig {
		t.Errorf("file = %q, want %q", report.File, mig)
	}
	if report.Framework != "plain" || report.Direction != "up" {
		t.Errorf("framework/direction = %q/%q, want plain/up", report.Framework, report.Direction)
	}
	if len(report.Checks) != 1 || report.Checks[0].Safety != query.SafetyDangerous {
		t.Fatalf("checks = %+v, want one dangerous", report.Checks)
	}
	if report.Checks[0].TableSize == nil {
		t.Error("expected the planner sizing to reach the CLI verdict")
	}
}

func TestCheckMigration_SafeExitsZero(t *testing.T) {
	hist := checkFixture(t)
	mig := writeSQL(t, filepath.Dir(hist), "safe.sql",
		"-- +goose Up\nCREATE TABLE new_t (id bigint);\nCREATE INDEX ON new_t (id);\n")

	out, _, err := runMigrationCheck(t, hist, "--json", mig)
	if code := exitCodeOf(t, err); code != 0 {
		t.Fatalf("exit %d, want 0: a table created in the same file is empty\n%s", code, out)
	}
	var report query.MigrationReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &report); err != nil {
		t.Fatal(err)
	}
	for _, c := range report.Checks {
		if c.Safety == query.SafetyDangerous {
			t.Errorf("%s is dangerous on a same-file table: %s", c.Operation, c.Recommendation)
		}
	}
}

// Caution is advice, not a gate: a small-table index must not fail CI.
func TestCheckMigration_CautionExitsZero(t *testing.T) {
	hist := checkFixture(t)
	mig := writeSQL(t, filepath.Dir(hist), "small.sql", "CREATE INDEX idx_users_email ON users (email);\n")

	out, _, err := runMigrationCheck(t, hist, "--json", mig)
	if code := exitCodeOf(t, err); code != 0 {
		t.Fatalf("exit %d, want 0 for a caution verdict", code)
	}
	var report query.MigrationReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Checks) != 1 || report.Checks[0].Safety != query.SafetyCaution {
		t.Fatalf("checks = %+v, want one caution", report.Checks)
	}
}

func TestCheckMigration_DirectionDown(t *testing.T) {
	hist := checkFixture(t)
	mig := writeSQL(t, filepath.Dir(hist), "rollback.sql",
		"-- +goose Up\nCREATE TABLE gone (id bigint);\n-- +goose Down\nDROP TABLE gone;\n")

	out, _, err := runMigrationCheck(t, hist, "--direction", "down", "--json", mig)
	if code := exitCodeOf(t, err); code != 1 {
		t.Fatalf("exit %d, want 1: DROP TABLE in the down section\n%s", code, out)
	}
	var report query.MigrationReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &report); err != nil {
		t.Fatal(err)
	}
	if report.Direction != "down" {
		t.Errorf("direction = %q, want down", report.Direction)
	}
}

// A bad flag, and a down check on a file with no down section, are tool
// errors -- the caller's input could not be analyzed, not a failed verdict.
func TestCheckMigration_InvalidInputExitsTwo(t *testing.T) {
	hist := checkFixture(t)
	dir := filepath.Dir(hist)

	plain := writeSQL(t, dir, "plain.sql", "CREATE TABLE t (id bigint);\n")

	_, stderr, err := runMigrationCheck(t, hist, "--direction", "sideways", plain)
	if code := exitCodeOf(t, err); code != 2 {
		t.Fatalf("exit %d, want 2 for an invalid direction", code)
	}
	if !strings.Contains(stderr, "direction") {
		t.Errorf("stderr %q should name the bad direction", stderr)
	}

	_, stderr, err = runMigrationCheck(t, hist, "--direction", "down", plain)
	if code := exitCodeOf(t, err); code != 2 {
		t.Fatalf("exit %d, want 2 for a down check on plain SQL", code)
	}
	if !strings.Contains(stderr, "down") {
		t.Errorf("stderr %q should explain the missing down section", stderr)
	}
}

func TestCheckMigration_MultipleFilesEmitArray(t *testing.T) {
	hist := checkFixture(t)
	dir := filepath.Dir(hist)
	safe := writeSQL(t, dir, "a_safe.sql", "ALTER TABLE orders ADD COLUMN note text;\n")
	danger := writeSQL(t, dir, "b_danger.sql", "CREATE INDEX idx_orders_status ON orders (status);\n")

	out, _, err := runMigrationCheck(t, hist, "--json", safe, danger)
	if code := exitCodeOf(t, err); code != 1 {
		t.Fatalf("exit %d, want 1 when any file is dangerous", code)
	}
	var reports []query.MigrationReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &reports); err != nil {
		t.Fatalf("multi-file --json must be an array: %v\n%s", err, out)
	}
	if len(reports) != 2 {
		t.Fatalf("got %d reports, want 2", len(reports))
	}
}

// A tool error on one file (2) outranks a dangerous verdict on another (1), and
// the good files are still reported.
func TestCheckMigration_UnreadableFileOutranksDanger(t *testing.T) {
	hist := checkFixture(t)
	dir := filepath.Dir(hist)
	danger := writeSQL(t, dir, "danger.sql", "CREATE INDEX idx_orders_status ON orders (status);\n")

	out, stderr, err := runMigrationCheck(t, hist, "--json", danger, filepath.Join(dir, "missing.sql"))
	if code := exitCodeOf(t, err); code != 2 {
		t.Fatalf("exit %d, want 2 when a file cannot be read", code)
	}
	if !strings.Contains(stderr, "missing.sql") {
		t.Errorf("stderr %q should name the unreadable file", stderr)
	}
	// two file arguments, one reportable: the shape follows the argument
	// count, not how many files survived
	var reports []query.MigrationReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &reports); err != nil {
		t.Fatalf("the readable file should still be reported as an array: %v\n%s", err, out)
	}
	if len(reports) != 1 || reports[0].File != danger {
		t.Errorf("reports = %+v, want the one readable file", reports)
	}
}

func TestCheckQuery_ValidAndInvalid(t *testing.T) {
	hist := checkFixture(t)
	dir := filepath.Dir(hist)

	valid := writeSQL(t, dir, "valid.sql", "SELECT id FROM orders WHERE status = 'open';\n")
	out, _, err := runQueryCheck(t, hist, "--json", valid)
	if code := exitCodeOf(t, err); code != 0 {
		t.Fatalf("exit %d, want 0 for a valid query\n%s", code, out)
	}
	var report queryReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Valid {
		t.Fatalf("valid query reported invalid: %+v", report.Errors)
	}

	invalid := writeSQL(t, dir, "invalid.sql", "SELECT id FROM missing_table;\n")
	out, _, err = runQueryCheck(t, hist, "--json", invalid)
	if code := exitCodeOf(t, err); code != 1 {
		t.Fatalf("exit %d, want 1 for an unknown table\n%s", code, out)
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &report); err != nil {
		t.Fatal(err)
	}
	if report.Valid || len(report.Errors) == 0 {
		t.Errorf("unknown table should be invalid with errors: %+v", report)
	}
}

// A query that does not parse is a tool error, matching the MCP tool's error
// channel, not a valid:false verdict.
func TestCheckQuery_ParseErrorExitsTwo(t *testing.T) {
	hist := checkFixture(t)
	bad := writeSQL(t, filepath.Dir(hist), "bad.sql", "SELECT FROM WHERE;\n")

	_, stderr, err := runQueryCheck(t, hist, bad)
	if code := exitCodeOf(t, err); code != 2 {
		t.Fatalf("exit %d, want 2 for a parse error", code)
	}
	if !strings.Contains(stderr, "bad.sql") {
		t.Errorf("stderr %q should name the file", stderr)
	}
}

// An explicit --history-db that does not exist is a typo: refuse before
// history.Open materializes an empty database next to it.
func TestCheck_UnknownHistoryDBNeverCreates(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "typo.db")

	_, stderr, err := runMigrationCheck(t, missing, writeSQL(t, dir, "m.sql", "CREATE TABLE t (id bigint);\n"))
	if code := exitCodeOf(t, err); code != 2 {
		t.Fatalf("exit %d, want 2 for a missing history db", code)
	}
	if !strings.Contains(stderr, "typo.db") {
		t.Errorf("stderr %q should name the missing path", stderr)
	}
	if _, serr := os.Stat(missing); !os.IsNotExist(serr) {
		t.Errorf("a read-only check created %s", missing)
	}
}

func TestCheck_MissingSnapshotExitsTwo(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "dryrun.toml"), []byte("[project]\nid = \"p\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	flagConfig, flagProfile = "", ""
	t.Cleanup(func() { flagConfig, flagProfile = "", "" })

	store := testStoreAt(t, dir) // initializes an empty history.db
	_ = store
	hist := filepath.Join(dir, "history.db")
	mig := writeSQL(t, dir, "m.sql", "CREATE TABLE t (id bigint);\n")

	_, stderr, err := runMigrationCheck(t, hist, mig)
	if code := exitCodeOf(t, err); code != 2 {
		t.Fatalf("exit %d, want 2 with no snapshot", code)
	}
	if !strings.Contains(stderr, "no schema snapshot") {
		t.Errorf("stderr %q should explain the missing snapshot and the fix", stderr)
	}
}
