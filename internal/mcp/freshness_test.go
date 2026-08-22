package mcp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

func historyStore(t *testing.T) *history.Store {
	t.Helper()
	hist, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	t.Cleanup(func() { hist.Close() })
	return hist
}

var testKey = history.SnapshotKey{ProjectID: "proj", DatabaseID: "db"}

// Distinct content, so ContentHash differs and PutSchema does not dedup.
func datedSnap(t *testing.T, at time.Time, table string) *schema.SchemaSnapshot {
	t.Helper()
	return &schema.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    "appdb",
		Timestamp:   at,
		ContentHash: table,
		Tables: []schema.Table{{
			Schema: "public", Name: table,
			Columns: []schema.Column{{Name: "id", Ordinal: 1, TypeName: "bigint"}},
		}},
	}
}

func put(t *testing.T, hist *history.Store, snap *schema.SchemaSnapshot) {
	t.Helper()
	if _, err := hist.PutSchema(context.Background(), testKey, snap); err != nil {
		t.Fatalf("PutSchema: %v", err)
	}
}

func serverWithHistory(t *testing.T, loaded *schema.SchemaSnapshot, hist *history.Store) *Server {
	t.Helper()
	srv := NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: loaded}, lint.DefaultConfig())
	srv.SetHistory(hist)
	srv.SetSnapshotKey(testKey)
	return srv
}

// The bug this exists to prevent: with --db the served schema is an
// introspection taken at process start, so drift measured against it answers
// "did DDL change since startup" and never "did someone migrate".
func TestDriftBaselineIsTheStoredSnapshot(t *testing.T) {
	hist := historyStore(t)
	stored := datedSnap(t, time.Now().Add(-48*time.Hour).UTC(), "from_history")
	put(t, hist, stored)

	// what a --db server would be serving: a fresh introspection
	srv := serverWithHistory(t, datedSnap(t, time.Now().UTC(), "from_startup_introspection"), hist)

	got, baseline, err := srv.driftBaseline(context.Background())
	if err != nil {
		t.Fatalf("driftBaseline: %v", err)
	}
	if got == nil || len(got.Tables) != 1 {
		t.Fatalf("no baseline: %+v", got)
	}
	if got.Tables[0].Name != "from_history" {
		t.Errorf("drift measured against %q, not the stored snapshot", got.Tables[0].Name)
	}
	if baseline != baselineHistory {
		t.Errorf("baseline = %q", baseline)
	}
}

func TestDriftBaselineFallsBackToTheLoadedSchema(t *testing.T) {
	loaded := datedSnap(t, time.Now().UTC(), "loaded")
	for _, tc := range []struct {
		name string
		srv  *Server
	}{
		{"no history at all", NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: loaded}, lint.DefaultConfig())},
		{"history with nothing for this key", serverWithHistory(t, loaded, historyStore(t))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, baseline, err := tc.srv.driftBaseline(context.Background())
			if err != nil {
				t.Fatalf("driftBaseline: %v", err)
			}
			if got == nil || got.Tables[0].Name != "loaded" {
				t.Fatalf("want the loaded schema, got %+v", got)
			}
			if baseline != baselineLoaded {
				t.Errorf("baseline should name the fallback, got %q", baseline)
			}
		})
	}
}

func TestNewerSnapshotAt(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	newer := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)

	t.Run("history holds a newer one", func(t *testing.T) {
		hist := historyStore(t)
		put(t, hist, datedSnap(t, newer, "new"))
		srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)
		if got := srv.newerSnapshotAt(); got != newer.Format(time.RFC3339) {
			t.Errorf("got %q, want %s", got, newer.Format(time.RFC3339))
		}
	})

	t.Run("the served snapshot is the newest", func(t *testing.T) {
		hist := historyStore(t)
		put(t, hist, datedSnap(t, older, "old"))
		srv := serverWithHistory(t, datedSnap(t, newer, "new"), hist)
		if got := srv.newerSnapshotAt(); got != "" {
			t.Errorf("reported a newer snapshot that is older: %q", got)
		}
	})

	t.Run("same snapshot", func(t *testing.T) {
		hist := historyStore(t)
		snap := datedSnap(t, newer, "same")
		put(t, hist, snap)
		srv := serverWithHistory(t, snap, hist)
		if got := srv.newerSnapshotAt(); got != "" {
			t.Errorf("reported the served snapshot as newer than itself: %q", got)
		}
	})

	t.Run("no history", func(t *testing.T) {
		srv := NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: datedSnap(t, older, "x")}, lint.DefaultConfig())
		if got := srv.newerSnapshotAt(); got != "" {
			t.Errorf("got %q with no history", got)
		}
	})
}

// reload_schema swaps the served snapshot, and the answer has to change with
// it rather than waiting out the cache.
func TestNewerSnapshotClearsWhenTheServedSnapshotChanges(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	newer := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)

	hist := historyStore(t)
	latest := datedSnap(t, newer, "new")
	put(t, hist, latest)

	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)
	if srv.newerSnapshotAt() == "" {
		t.Fatal("expected a newer snapshot before the reload")
	}

	srv.mu.Lock()
	srv.annotated = &schema.AnnotatedSchema{Schema: latest}
	srv.mu.Unlock()

	if got := srv.newerSnapshotAt(); got != "" {
		t.Errorf("still reporting %q after the newer snapshot was loaded", got)
	}
}

// Both meta paths: newMeta for tools with generated output schemas, injectMeta
// for the map-based ones.
func TestNewerSnapshotReachesBothMetaPaths(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	newer := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)
	want := newer.Format(time.RFC3339)

	hist := historyStore(t)
	put(t, hist, datedSnap(t, newer, "new"))
	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)

	if got := srv.newMeta("", nil); got.NewerSnapshotAt != want {
		t.Errorf("typed meta: got %q, want %q", got.NewerSnapshotAt, want)
	}

	wrapper := map[string]any{}
	srv.injectMeta(wrapper, "", nil)
	meta, _ := wrapper["_meta"].(map[string]any)
	if meta["newer_snapshot_at"] != want {
		t.Errorf("map meta: got %v, want %q", meta["newer_snapshot_at"], want)
	}

	// and it must be absent, not empty, when there is nothing newer
	quiet := serverWithHistory(t, datedSnap(t, newer, "new"), hist)
	out, err := json.Marshal(quiet.newMeta("", nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "newer_snapshot_at") {
		t.Errorf("empty field serialized: %s", out)
	}
}

// PutSchema only dedups against the newest row, so A -> B -> A stores a twin of
// A. Reloading into a snapshot you already serve is wasted work.
func TestNewerSnapshotIgnoresAContentTwin(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)

	served := datedSnap(t, t0, "a")
	put(t, hist, served)
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "b"))

	twin := datedSnap(t, t0.Add(2*time.Hour), "a") // same ContentHash as served
	put(t, hist, twin)

	srv := serverWithHistory(t, served, hist)
	if got := srv.newerSnapshotAt(); got != "" {
		t.Errorf("sent the agent to reload the snapshot it already has: %q", got)
	}
}

// The stored timestamp is second-granularity RFC3339, and imported snapshots
// need not be UTC.
func TestNewerSnapshotToleratesOddTimestamps(t *testing.T) {
	zone := time.FixedZone("CEST", 2*60*60)
	older := time.Now().Add(-2 * time.Hour).In(zone)
	newer := older.Add(90 * time.Minute)

	hist := historyStore(t)
	put(t, hist, datedSnap(t, newer, "new"))
	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)

	if got := srv.newerSnapshotAt(); got != newer.UTC().Truncate(time.Second).Format(time.RFC3339) {
		t.Errorf("got %q, want the newer snapshot in UTC", got)
	}
}

// A read failure must not read as "nothing stored": the fallback in live mode
// is the very schema the drift is measured against.
func TestDriftBaselineRefusesOnAReadError(t *testing.T) {
	hist := historyStore(t)
	put(t, hist, datedSnap(t, time.Now().UTC(), "stored"))
	if err := hist.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	srv := serverWithHistory(t, datedSnap(t, time.Now().UTC(), "loaded"), hist)
	got, baseline, err := srv.driftBaseline(context.Background())
	if err == nil {
		t.Fatalf("a broken history read fell back to %q (%v)", baseline, got)
	}
	if !strings.Contains(err.Error(), "stored snapshot") {
		t.Errorf("error should name what failed: %v", err)
	}
}

// A history.db this build cannot read is not a baseline.
func TestDriftBaselineRefusesOnIncompatibleHistory(t *testing.T) {
	srv := serverWithHistory(t, datedSnap(t, time.Now().UTC(), "loaded"), legacyHistoryStore(t))
	_, _, err := srv.driftBaseline(context.Background())
	if err == nil {
		t.Fatal("a legacy history.db was accepted as a drift baseline")
	}
	// the compat problem is the thing to report, not whatever the read did
	if !strings.Contains(err.Error(), "older dryrun") {
		t.Errorf("error should name the compatibility problem: %v", err)
	}
}

// The snapshot key and the --db url resolve independently, so history can hold
// another database entirely.
func TestNewerSnapshotIsOfflineOnly(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)
	put(t, hist, datedSnap(t, time.Now().UTC(), "new"))

	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)
	if srv.newerSnapshotAt() == "" {
		t.Fatal("expected the field offline")
	}

	// with a pool the served schema is a startup introspection stamped by the
	// same clock every stored snapshot used, so the read is pure cost
	srv.mu.Lock()
	srv.pool = &pgxpool.Pool{}
	srv.mu.Unlock()
	if got := srv.newerSnapshotAt(); got != "" {
		t.Errorf("read history in live mode: %q", got)
	}
}

func TestDatabaseMismatch(t *testing.T) {
	snapOf := func(db string) *schema.SchemaSnapshot {
		return &schema.SchemaSnapshot{Database: db}
	}
	if msg := databaseMismatch(snapOf("prod"), snapOf("staging")); msg == "" {
		t.Error("prod snapshot against a staging connection reported as drift")
	} else if !strings.Contains(msg, "prod") || !strings.Contains(msg, "staging") {
		t.Errorf("message should name both: %s", msg)
	}
	for _, tc := range [][2]string{{"app", "app"}, {"", "app"}, {"app", ""}} {
		if msg := databaseMismatch(snapOf(tc[0]), snapOf(tc[1])); msg != "" {
			t.Errorf("databaseMismatch(%q, %q) = %s", tc[0], tc[1], msg)
		}
	}
}

func driftText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatal("empty result")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("want text content, got %T", res.Content[0])
	}
	return tc.Text
}

// The regression this whole change exists to prevent: drift measured against
// the schema this process introspected at startup always reads as clean.
func TestCheckDriftMeasuresAgainstTheStoredSnapshot(t *testing.T) {
	hist := historyStore(t)
	stored := datedSnap(t, time.Now().Add(-48*time.Hour).UTC(), "old_table")
	put(t, hist, stored)

	// live has a table the stored snapshot does not: real drift
	live := datedSnap(t, time.Now().UTC(), "old_table")
	live.Tables = append(live.Tables, schema.Table{
		Schema: "public", Name: "added_by_a_migration",
		Columns: []schema.Column{{Name: "id", Ordinal: 1, TypeName: "bigint"}},
	})

	// what a --db server serves: an introspection of that same live database
	srv := serverWithHistory(t, live, hist)

	out := driftText(t, srv.driftAgainst(context.Background(), live))
	if strings.Contains(out, "No drift detected") {
		t.Fatalf("drift measured against the startup introspection:\n%s", out)
	}
	if !strings.Contains(out, `"baseline": "history"`) {
		t.Errorf("response should name the baseline:\n%.400s", out)
	}
	if !strings.Contains(out, "added_by_a_migration") {
		t.Errorf("the migrated table should appear:\n%.400s", out)
	}
}

// With nothing stored the comparison is introspection against introspection,
// which cannot show a migration that ran before startup. Say so.
func TestCheckDriftAdmitsAVacuousBaseline(t *testing.T) {
	live := datedSnap(t, time.Now().UTC(), "t")
	srv := NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: live}, lint.DefaultConfig())

	out := driftText(t, srv.driftAgainst(context.Background(), live))
	if !strings.Contains(out, "No drift detected") {
		t.Fatalf("identical snapshots should read as no drift:\n%s", out)
	}
	if !strings.Contains(out, "cannot show a migration that ran before it") {
		t.Errorf("a vacuous baseline has to say so:\n%s", out)
	}
}

func TestCheckDriftRefusesAnotherDatabase(t *testing.T) {
	hist := historyStore(t)
	stored := datedSnap(t, time.Now().Add(-time.Hour).UTC(), "t")
	stored.Database = "prod"
	put(t, hist, stored)

	live := datedSnap(t, time.Now().UTC(), "t")
	live.Database = "staging"

	srv := serverWithHistory(t, live, hist)
	out := driftText(t, srv.driftAgainst(context.Background(), live))
	if !strings.Contains(out, "not drift") {
		t.Errorf("comparing prod history against a staging connection:\n%s", out)
	}
}
