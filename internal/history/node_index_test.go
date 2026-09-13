package history

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Per-label reads (fingerprint windows, member baselines, counter regressions,
// --due) filter on one node_source and order by time. The label index serves
// them. history.db is never analyzed, so the planner picks indexes by equality
// prefix alone; these tests pin both that the label index is taken where it
// should be and that it does not steal lookups it would make worse. Empty
// tables are enough: without sqlite_stat1 row counts do not enter the choice.

func explainPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var (
			id, parent, unused int
			detail             string
		)
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, " | ")
}

func TestLabelQueriesUseLabelIndex(t *testing.T) {
	s := testStore(t)
	for _, table := range []string{"activity_stats", "query_stats"} {
		for name, q := range map[string]struct {
			sql  string
			args []any
		}{
			"fingerprint window": {fingerprintArmSQL(table, " AND timestamp <= ?"),
				[]any{"p", "d", "pool", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z", nodeFingerprintRows}},
			// RecentNodeFingerprints, on every capture
			"unbounded fingerprint window": {fingerprintArmSQL(table, ""),
				[]any{"p", "d", "pool", "2026-01-01T00:00:00Z", nodeFingerprintRows}},
			"member baseline": {memberBaselineSQL(table),
				[]any{"p", "d", "pool", "2026-02-01T00:00:00Z", memberBaselineScan}},
		} {
			plan := explainPlan(t, s.db, q.sql, q.args...)
			if !strings.Contains(plan, table+"_by_node_taken_at") {
				t.Errorf("%s on %s does not use the label index: %s", name, table, plan)
			}
			if strings.Contains(plan, "TEMP B-TREE") {
				t.Errorf("%s on %s sorts instead of scanning the index in order: %s", name, table, plan)
			}
		}
	}
	// activity only: pool servers for GetAnnotated
	plan := explainPlan(t, s.db, activityMembersSQL(),
		"p", "d", "pool", "sh", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z", memberBaselineScan)
	if !strings.Contains(plan, "activity_stats_by_node_taken_at") || strings.Contains(plan, "TEMP B-TREE") {
		t.Errorf("pool server scan left the label index or sorts: %s", plan)
	}
}

// The label index has the longer equality prefix, so without the unary plus
// it would win this lookup and walk every row of the key.
func TestLatestPerNodeByRefKeepsSchemaRefIndex(t *testing.T) {
	s := testStore(t)
	for _, table := range []string{"activity_stats", "query_stats"} {
		plan := explainPlan(t, s.db, latestPerNodeByRefSQL(table), "p", "d", "sh")
		// outer query and correlated subquery each
		if strings.Contains(plan, table+"_by_node_taken_at") || strings.Count(plan, table+"_by_schema_ref") != 2 {
			t.Errorf("%s per-ref lookup left the schema_ref index: %s", table, plan)
		}
	}
}

// listActivity/listQueryStats without a node filter: the key index still serves
func TestKeyWideListKeepsKeyIndex(t *testing.T) {
	s := testStore(t)
	for _, table := range []string{"activity_stats", "query_stats"} {
		plan := explainPlan(t, s.db,
			`SELECT id FROM `+table+` WHERE project_id = ? AND database_id = ? ORDER BY timestamp DESC, id DESC`, "p", "d")
		if !strings.Contains(plan, table+"_by_key_taken_at") {
			t.Errorf("%s key-wide list left the key index: %s", table, plan)
		}
	}
}

// migrate runs on every open, so a history.db created before the index gets it
// on the next open rather than needing a version bump
func TestOpenAddsLabelIndexToExistingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	dropLabelIndexes(t, s)
	s.Close()

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, idx := range labelIndexes {
		var name string
		if err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, idx).Scan(&name); err != nil {
			t.Errorf("%s missing after reopen: %v", idx, err)
		}
	}
}

// A read-only mount is supported. The index is an optimization, so a store that
// predates it and cannot be written must still open and answer reads.
func TestOpenReadOnlyStoreWithoutLabelIndex(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
	path := filepath.Join(t.TempDir(), "history.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	dropLabelIndexes(t, s)
	// a read-only mount cannot switch to WAL, so the file keeps a rollback journal
	if _, err := s.db.Exec("PRAGMA journal_mode = DELETE"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	s, err = Open(path)
	if err != nil {
		t.Fatalf("read-only store without the label index must open: %v", err)
	}
	defer s.Close()
	if _, err := s.RecentNodeFingerprints(context.Background(), SnapshotKey{ProjectID: "p", DatabaseID: "d"}, "pool"); err != nil {
		t.Fatalf("read without the label index: %v", err)
	}
}

var (
	labelIndexes = []string{"activity_stats_by_node_taken_at", "query_stats_by_node_taken_at"}
)

func dropLabelIndexes(t *testing.T, s *Store) {
	t.Helper()
	for _, idx := range labelIndexes {
		if _, err := s.db.Exec("DROP INDEX " + idx); err != nil {
			t.Fatal(err)
		}
	}
}
