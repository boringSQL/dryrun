package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

func servedTable(t *testing.T, srv *Server) string {
	t.Helper()
	snap, err := srv.getSchema()
	if err != nil {
		t.Fatalf("getSchema: %v", err)
	}
	if len(snap.Tables) == 0 {
		t.Fatal("served snapshot has no tables")
	}
	return snap.Tables[0].Name
}

func TestAdoptNewerSnapshot(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	newer := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)

	t.Run("history holds a newer one", func(t *testing.T) {
		hist := historyStore(t)
		put(t, hist, datedSnap(t, newer, "new"))
		srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)
		if got := servedTable(t, srv); got != "new" {
			t.Errorf("still serving %q", got)
		}
	})

	t.Run("the served snapshot is the newest", func(t *testing.T) {
		hist := historyStore(t)
		put(t, hist, datedSnap(t, older, "old"))
		srv := serverWithHistory(t, datedSnap(t, newer, "new"), hist)
		if got := servedTable(t, srv); got != "new" {
			t.Errorf("adopted an older snapshot: %q", got)
		}
	})

	t.Run("no history", func(t *testing.T) {
		srv := NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: datedSnap(t, older, "x")}, lint.DefaultConfig())
		if got := servedTable(t, srv); got != "x" {
			t.Errorf("got %q with no history", got)
		}
	})

	t.Run("nothing loaded yet", func(t *testing.T) {
		hist := historyStore(t)
		put(t, hist, datedSnap(t, newer, "new"))
		srv := serverWithHistory(t, nil, hist)
		srv.SetUninitialized()
		if got := servedTable(t, srv); got != "new" {
			t.Errorf("a server started before `dryrun init` never picked the snapshot up: %q", got)
		}
	})
}

// Within freshnessTTL the served snapshot is pinned, so one call cannot answer from two.
func TestAdoptFollowsSuccessiveSnapshots(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "second"))

	srv := serverWithHistory(t, datedSnap(t, t0, "first"), hist)
	if got := servedTable(t, srv); got != "second" {
		t.Fatalf("first adoption failed: %q", got)
	}

	put(t, hist, datedSnap(t, t0.Add(2*time.Hour), "third"))
	if got := servedTable(t, srv); got != "second" {
		t.Errorf("snapshot changed inside the throttle window: %q", got)
	}

	srv.freshness.mu.Lock()
	srv.freshness.checkedAt = time.Now().Add(-2 * freshnessTTL)
	srv.freshness.mu.Unlock()
	if got := servedTable(t, srv); got != "third" {
		t.Errorf("second snapshot never picked up: %q", got)
	}
}

// _meta must date the snapshot being served, not the one loaded at startup.
func TestAdoptedSnapshotDatesTheMeta(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	newer := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)

	hist := historyStore(t)
	put(t, hist, datedSnap(t, newer, "new"))
	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)

	if got := srv.newMeta("", nil).SchemaCapturedAt; got != newer.Format(time.RFC3339) {
		t.Errorf("typed meta: got %q, want %q", got, newer.Format(time.RFC3339))
	}

	wrapper := map[string]any{}
	srv.injectMeta(wrapper, "", nil)
	meta, _ := wrapper["_meta"].(map[string]any)
	if meta["schema_captured_at"] != newer.Format(time.RFC3339) {
		t.Errorf("map meta: got %v", meta["schema_captured_at"])
	}
	if _, ok := meta["newer_snapshot_at"]; ok {
		t.Error("the served snapshot cannot be behind history, so the field must be gone")
	}
	out, err := json.Marshal(srv.newMeta("", nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "newer_snapshot_at") {
		t.Errorf("stale field serialized: %s", out)
	}
}

// PutSchema only dedups against the newest row, so A -> B -> A stores a twin of
// A. Reloading into a snapshot already served is wasted work.
func TestAdoptIgnoresAContentTwin(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)

	served := datedSnap(t, t0, "a")
	put(t, hist, served)
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "b"))

	twin := datedSnap(t, t0.Add(2*time.Hour), "a") // same ContentHash as served
	put(t, hist, twin)

	srv := serverWithHistory(t, served, hist)
	if got := servedTable(t, srv); got != "a" {
		t.Errorf("reloaded the snapshot it already had: %q", got)
	}
}

// The common take stores no new schema row; planner and activity land under the same ref hash.
func TestAdoptPicksUpStatsUnderAnUnchangedSchema(t *testing.T) {
	t0 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)

	snap := datedSnap(t, t0, "t")
	put(t, hist, snap)

	srv := serverWithHistory(t, snap, hist)
	if got := srv.newMeta("", nil).PlannerCapturedAt; got != "" {
		t.Fatalf("planner stats before any capture: %q", got)
	}

	plannerAt := t0.Add(time.Hour)
	if _, err := hist.PutPlanner(context.Background(), testKey, &schema.PlannerStatsSnapshot{
		SchemaRefHash: snap.ContentHash,
		ContentHash:   "planner-1",
		Database:      snap.Database,
		Timestamp:     plannerAt,
	}); err != nil {
		t.Fatalf("PutPlanner: %v", err)
	}

	srv.freshness.mu.Lock()
	srv.freshness.checkedAt = time.Now().Add(-2 * freshnessTTL)
	srv.freshness.mu.Unlock()

	if got := srv.newMeta("", nil).PlannerCapturedAt; got != plannerAt.Format(time.RFC3339) {
		t.Errorf("planner capture not picked up: %q", got)
	}
}

func putPlannerAt(t *testing.T, hist *history.Store, refHash string, at time.Time) {
	t.Helper()
	if _, err := hist.PutPlanner(context.Background(), testKey, &schema.PlannerStatsSnapshot{
		SchemaRefHash: refHash,
		ContentHash:   "planner-" + at.Format(time.RFC3339),
		Database:      "appdb",
		Timestamp:     at,
	}); err != nil {
		t.Fatalf("PutPlanner: %v", err)
	}
}

func putActivityAt(t *testing.T, hist *history.Store, refHash, source string, at time.Time) {
	t.Helper()
	if _, err := hist.PutActivity(context.Background(), testKey, &schema.ActivityStatsSnapshot{
		SchemaRefHash: refHash,
		ContentHash:   "activity-" + source + "-" + at.Format(time.RFC3339),
		Node:          schema.NodeIdentity{Source: source, Timestamp: at},
	}); err != nil {
		t.Fatalf("PutActivity: %v", err)
	}
}

func expireFreshness(srv *Server) {
	srv.freshness.mu.Lock()
	srv.freshness.checkedAt = time.Now().Add(-2 * freshnessTTL)
	srv.freshness.mu.Unlock()
}

// §5.5: a new schema hash lands and every stat row is still bound to the prior
// one. _meta used to drop planner_captured_at/activity_captured_at entirely,
// which an agent cannot tell apart from a project that never captured stats.
func TestMetaMarksAReschemaWindow(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	statsAt := t0.Add(time.Hour)
	reschemaAt := t0.Add(2 * time.Hour)
	recapturedAt := t0.Add(3 * time.Hour)

	hist := historyStore(t)
	oldSnap := datedSnap(t, t0, "old")
	put(t, hist, oldSnap)
	putPlannerAt(t, hist, oldSnap.ContentHash, statsAt)
	putActivityAt(t, hist, oldSnap.ContentHash, "primary", statsAt)

	srv := serverWithHistory(t, oldSnap, hist)
	meta := srv.newMeta("", nil)
	if meta.PlannerCapturedAt != statsAt.Format(time.RFC3339) || meta.StatsPendingReschema {
		t.Fatalf("baseline: planner %q, pending %v", meta.PlannerCapturedAt, meta.StatsPendingReschema)
	}

	// the migration: a new hash with no stats under it yet
	newSnap := datedSnap(t, reschemaAt, "new")
	put(t, hist, newSnap)
	expireFreshness(srv)

	meta = srv.newMeta("", nil)
	if meta.SchemaCapturedAt != reschemaAt.Format(time.RFC3339) {
		t.Errorf("serving the new schema: %q", meta.SchemaCapturedAt)
	}
	if meta.PlannerCapturedAt != statsAt.Format(time.RFC3339) {
		t.Errorf("planner stamp dropped instead of dated to the prior hash: %q", meta.PlannerCapturedAt)
	}
	if meta.ActivityCapturedAt != statsAt.Format(time.RFC3339) {
		t.Errorf("activity stamp dropped instead of dated to the prior hash: %q", meta.ActivityCapturedAt)
	}
	if meta.ActivityOldestNode != "primary" {
		t.Errorf("laggard node lost: %q", meta.ActivityOldestNode)
	}
	if !meta.StatsPendingReschema {
		t.Error("the re-schema window is not flagged")
	}

	// the map-based _meta path (dynamic-payload tools) carries the same marker
	wrapper := map[string]any{}
	srv.injectMeta(wrapper, "", nil)
	if got, _ := wrapper["_meta"].(map[string]any); got["stats_pending_reschema"] != true {
		t.Errorf("map _meta missing the marker: %v", got)
	}
	// and thin clients, which cannot see _meta at all, read the text header
	if suffix := srv.captureTimes().suffix(); !strings.Contains(suffix, "stats pending reschema") {
		t.Errorf("header hides the window: %q", suffix)
	}

	// planner re-captures first: its stamp advances, activity is still pending
	putPlannerAt(t, hist, newSnap.ContentHash, recapturedAt)
	expireFreshness(srv)
	meta = srv.newMeta("", nil)
	if !meta.StatsPendingReschema {
		t.Error("activity is still prior-hash; the window is not over")
	}
	if meta.PlannerCapturedAt != recapturedAt.Format(time.RFC3339) {
		t.Errorf("planner stamp did not advance: %q", meta.PlannerCapturedAt)
	}
	if meta.ActivityCapturedAt != statsAt.Format(time.RFC3339) {
		t.Errorf("activity stamp should still be the prior hash's: %q", meta.ActivityCapturedAt)
	}

	// activity lands under the new hash and the window closes
	putActivityAt(t, hist, newSnap.ContentHash, "primary", recapturedAt)
	expireFreshness(srv)
	meta = srv.newMeta("", nil)
	if meta.StatsPendingReschema {
		t.Error("window closed but still flagged")
	}
	if meta.PlannerCapturedAt != recapturedAt.Format(time.RFC3339) || meta.ActivityCapturedAt != recapturedAt.Format(time.RFC3339) {
		t.Errorf("stamps did not advance: planner %q activity %q", meta.PlannerCapturedAt, meta.ActivityCapturedAt)
	}
}

// A server restarted mid-window (migration landed, next capture not yet) must
// flag it through the bootstrap path, not only through adoption.
func TestMetaMarksAReschemaWindowAfterRestart(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	statsAt := t0.Add(time.Hour)
	reschemaAt := t0.Add(2 * time.Hour)

	hist := historyStore(t)
	oldSnap := datedSnap(t, t0, "old")
	put(t, hist, oldSnap)
	putPlannerAt(t, hist, oldSnap.ContentHash, statsAt)
	putActivityAt(t, hist, oldSnap.ContentHash, "primary", statsAt)
	put(t, hist, datedSnap(t, reschemaAt, "new"))

	srv := NewOfflineServerAnnotated(nil, lint.DefaultConfig())
	srv.SetHistory(hist)
	srv.SetSnapshotKey(testKey)
	if !srv.BootstrapFromHistory(context.Background()) {
		t.Fatal("bootstrap failed")
	}

	meta := srv.newMeta("", nil)
	if meta.SchemaCapturedAt != reschemaAt.Format(time.RFC3339) {
		t.Errorf("latest schema not served: %q", meta.SchemaCapturedAt)
	}
	if !meta.StatsPendingReschema {
		t.Error("restart inside the window lost the marker")
	}
	if meta.PlannerCapturedAt != statsAt.Format(time.RFC3339) {
		t.Errorf("prior planner stamp lost: %q", meta.PlannerCapturedAt)
	}
}

// The marker exists to tell "not yet re-captured under this schema" from
// "never captured"; it must not fire for the latter.
func TestMetaNoReschemaMarkerWhenStatsNeverCaptured(t *testing.T) {
	hist := historyStore(t)
	put(t, hist, datedSnap(t, time.Now().Add(-time.Hour).UTC(), "solo"))

	srv := NewOfflineServerAnnotated(nil, lint.DefaultConfig())
	srv.SetHistory(hist)
	srv.SetSnapshotKey(testKey)
	if !srv.BootstrapFromHistory(context.Background()) {
		t.Fatal("bootstrap failed")
	}

	meta := srv.newMeta("", nil)
	if meta.StatsPendingReschema {
		t.Error("no stats ever captured, but the meta claims a pending window")
	}
	if meta.PlannerCapturedAt != "" || meta.ActivityCapturedAt != "" {
		t.Errorf("stamps invented from nothing: planner %q activity %q", meta.PlannerCapturedAt, meta.ActivityCapturedAt)
	}
}

// pendingStamps are computed at adoption and used to be final: a lookup that
// failed under a contended read left the window unflagged until the first
// re-capture, because a stats-free bundle never advances again. The expired
// freshness tick refreshes them instead.
func TestMetaReschemaWindowRefreshesPendingStamps(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	statsAt := t0.Add(time.Hour)
	reschemaAt := t0.Add(2 * time.Hour)

	hist := historyStore(t)
	oldSnap := datedSnap(t, t0, "old")
	put(t, hist, oldSnap)
	putPlannerAt(t, hist, oldSnap.ContentHash, statsAt)
	putActivityAt(t, hist, oldSnap.ContentHash, "primary", statsAt)
	newSnap := datedSnap(t, reschemaAt, "new")
	put(t, hist, newSnap)

	srv := serverWithHistory(t, newSnap, hist)
	if meta := srv.newMeta("", nil); !meta.StatsPendingReschema {
		t.Fatal("window not flagged on the first tick; fixture broken")
	}

	// simulate the transient failure: the stamps computed for the window are lost
	srv.mu.Lock()
	srv.pendingStamps = captureStamps{}
	srv.mu.Unlock()
	if meta := srv.newMeta("", nil); meta.StatsPendingReschema {
		t.Fatal("pendingStamps cleared but the flag survived the throttle window")
	}

	// the bundle does not advance; the next expired tick must restore the stamps
	expireFreshness(srv)
	meta := srv.newMeta("", nil)
	if !meta.StatsPendingReschema {
		t.Error("flag missing until the next re-capture")
	}
	if meta.PlannerCapturedAt != statsAt.Format(time.RFC3339) {
		t.Errorf("planner stamp = %q, want the prior capture %q", meta.PlannerCapturedAt, statsAt.Format(time.RFC3339))
	}
	if meta.ActivityCapturedAt != statsAt.Format(time.RFC3339) {
		t.Errorf("activity stamp = %q, want the prior capture %q", meta.ActivityCapturedAt, statsAt.Format(time.RFC3339))
	}
}

// ... but only when history's latest IS the served bundle: a schema history
// does not hold (a --db introspection ahead of the last capture) must not
// borrow stamps bound to another hash.
func TestMetaReschemaRefreshRequiresTheServedHash(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	statsAt := t0.Add(time.Hour)

	hist := historyStore(t)
	oldSnap := datedSnap(t, t0, "old")
	put(t, hist, oldSnap)
	putPlannerAt(t, hist, oldSnap.ContentHash, statsAt)

	// served schema is newer than anything in history and never captured there
	local := datedSnap(t, t0.Add(2*time.Hour), "local")
	srv := serverWithHistory(t, local, hist)
	meta := srv.newMeta("", nil)
	if meta.SchemaCapturedAt != local.Timestamp.Format(time.RFC3339) {
		t.Errorf("not serving the local schema: %q", meta.SchemaCapturedAt)
	}
	if meta.StatsPendingReschema {
		t.Error("borrowed prior-hash stamps for a schema history does not hold")
	}
	if meta.PlannerCapturedAt != "" || meta.ActivityCapturedAt != "" {
		t.Errorf("invented stamps: planner %q activity %q", meta.PlannerCapturedAt, meta.ActivityCapturedAt)
	}
}

// "newest" is decided on the instant, not the string: stored timestamps need not be UTC.
func TestAdoptToleratesOddTimestamps(t *testing.T) {
	zone := time.FixedZone("CEST", 2*60*60)
	older := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	newer := older.Add(90 * time.Minute).In(zone)

	hist := historyStore(t)
	put(t, hist, datedSnap(t, newer, "new"))
	// later text (+02:00 sorts after Z), earlier instant
	put(t, hist, datedSnap(t, older.Add(time.Hour).UTC(), "middle"))

	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)
	if got := servedTable(t, srv); got != "new" {
		t.Errorf("got %q, want the newest instant", got)
	}
}

// With --db the served schema is stamped now; fresher stats are no reason to serve older DDL.
func TestAdoptNeverGoesBackwards(t *testing.T) {
	stored := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)
	old := datedSnap(t, stored, "from_history")
	put(t, hist, old)
	if _, err := hist.PutPlanner(context.Background(), testKey, &schema.PlannerStatsSnapshot{
		SchemaRefHash: old.ContentHash,
		ContentHash:   "planner-1",
		Database:      old.Database,
		Timestamp:     stored,
	}); err != nil {
		t.Fatalf("PutPlanner: %v", err)
	}

	srv := serverWithHistory(t, datedSnap(t, time.Now().UTC(), "from_startup_introspection"), hist)
	if got := servedTable(t, srv); got != "from_startup_introspection" {
		t.Errorf("adopted an older schema for its stats: %q", got)
	}
}

// A newer dryrun's history.db reads without error, so only the explicit gate stops it.
func TestAdoptRefusesNewerHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	hist, err := history.Open(path)
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	put(t, hist, datedSnap(t, time.Now().UTC(), "from_the_future"))
	hist.Close()

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 9999"); err != nil {
		t.Fatalf("bump user_version: %v", err)
	}
	raw.Close()

	hist, err = history.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { hist.Close() })
	if hist.Compat() != history.CompatNewer {
		t.Fatalf("fixture is %v, not CompatNewer", hist.Compat())
	}

	srv := serverWithHistory(t, datedSnap(t, time.Now().Add(-time.Hour).UTC(), "old"), hist)
	if got := servedTable(t, srv); got != "old" {
		t.Errorf("served from a history.db this build cannot read: %q", got)
	}
}

// Adoption swaps a pointer every handler reads. Run under -race.
func TestAdoptUnderConcurrentReaders(t *testing.T) {
	t0 := time.Now().Add(-4 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "second"))
	srv := serverWithHistory(t, datedSnap(t, t0, "first"), hist)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := range 20 {
				if i == 0 && n%5 == 0 {
					put(t, hist, datedSnap(t, t0.Add(time.Duration(2+n)*time.Hour), fmt.Sprintf("s%d", n)))
					srv.freshness.mu.Lock()
					srv.freshness.checkedAt = time.Time{}
					srv.freshness.mu.Unlock()
				}
				if _, err := srv.getSchema(); err != nil {
					t.Errorf("getSchema: %v", err)
					return
				}
				srv.newMeta("", nil)
			}
		}(i)
	}
	wg.Wait()
}

// The snapshot key and the --db url resolve independently, so history can hold another database.
func TestAdoptRefusesAnotherDatabase(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	hist := historyStore(t)
	other := datedSnap(t, time.Now().UTC(), "new")
	other.Database = "somewhere_else"
	put(t, hist, other)

	srv := serverWithHistory(t, datedSnap(t, older, "old"), hist)
	if got := servedTable(t, srv); got != "old" {
		t.Errorf("adopted another database's snapshot: %q", got)
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
