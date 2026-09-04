package history

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCaptureAttempts_Roundtrip(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	if _, ok, err := s.LastCaptureAttemptAt(ctx, key, "primary", "query"); err != nil || ok {
		t.Fatalf("ok=%t err=%v, want no attempt on an empty store", ok, err)
	}

	first := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if err := s.MarkCaptureAttempt(ctx, key, "primary", "query", first); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LastCaptureAttemptAt(ctx, key, "primary", "query")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if !got.Equal(first) {
		t.Errorf("attempt %s, want %s", got, first)
	}

	// the clock advances rather than accumulating a row per tick
	second := first.Add(30 * time.Minute)
	if err := s.MarkCaptureAttempt(ctx, key, "primary", "query", second); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.LastCaptureAttemptAt(ctx, key, "primary", "query")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(second) {
		t.Errorf("attempt %s, want the upsert to win with %s", got, second)
	}
	var rows int
	if err := s.db.QueryRow(`SELECT count(*) FROM capture_attempts`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("%d rows, want the upsert to keep exactly one", rows)
	}
}

// activity and query rows carry a node_source, so their clocks are per node.
func TestCaptureAttempts_PerNodeStreamsAreIndependent(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	at := time.Now().UTC().Truncate(time.Second)

	if err := s.MarkCaptureAttempt(ctx, key, "replica-1", "activity", at); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.LastCaptureAttemptAt(ctx, key, "replica-2", "activity"); err != nil || ok {
		t.Errorf("ok=%t err=%v, want replica-2 unaffected by replica-1", ok, err)
	}
	if _, ok, err := s.LastCaptureAttemptAt(ctx, key, "replica-1", "query"); err != nil || ok {
		t.Errorf("ok=%t err=%v, want each stream to keep its own clock", ok, err)
	}
}

// schema and planner rows are read with no node filter, so their attempt must
// be keyed project-scoped too. Keying per node would compare two different
// scopes: a second node would read the first node's row but not its attempt,
// and re-introspect on every tick.
func TestCaptureAttempts_ProjectScopedStreamsShareOneClock(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	at := time.Now().UTC().Truncate(time.Second)

	for _, stream := range []string{"schema", "planner"} {
		if err := s.MarkCaptureAttempt(ctx, key, "node-a", stream, at); err != nil {
			t.Fatal(err)
		}
		got, ok, err := s.LastCaptureAttemptAt(ctx, key, "node-b", stream)
		if err != nil || !ok {
			t.Fatalf("%s: ok=%t err=%v, want node-b to see the project-scoped attempt", stream, ok, err)
		}
		if !got.Equal(at) {
			t.Errorf("%s: attempt %s, want %s", stream, got, at)
		}
	}

	var label string
	if err := s.db.QueryRow(
		`SELECT node_label FROM capture_attempts WHERE stream = 'schema'`).Scan(&label); err != nil {
		t.Fatal(err)
	}
	if label != "" {
		t.Errorf("node_label %q, want the empty project-scoped key", label)
	}
}

func TestCaptureAttempts_ScopedToKey(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	at := time.Now().UTC().Truncate(time.Second)

	if err := s.MarkCaptureAttempt(ctx, SnapshotKey{ProjectID: "p1", DatabaseID: "d"}, "primary", "query", at); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.LastCaptureAttemptAt(ctx, SnapshotKey{ProjectID: "p2", DatabaseID: "d"}, "primary", "query"); err != nil || ok {
		t.Errorf("ok=%t err=%v, want another project unaffected", ok, err)
	}
	if _, ok, err := s.LastCaptureAttemptAt(ctx, SnapshotKey{ProjectID: "p1", DatabaseID: "d2"}, "primary", "query"); err != nil || ok {
		t.Errorf("ok=%t err=%v, want another database unaffected", ok, err)
	}
}

func TestCaptureAttempts_UnknownStream(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	if err := s.MarkCaptureAttempt(ctx, key, "primary", "bloat", time.Now()); err == nil ||
		!strings.Contains(err.Error(), "unknown stream") {
		t.Errorf("MarkCaptureAttempt error %v, want unknown stream", err)
	}
	if _, _, err := s.LastCaptureAttemptAt(ctx, key, "primary", "bloat"); err == nil ||
		!strings.Contains(err.Error(), "unknown stream") {
		t.Errorf("LastCaptureAttemptAt error %v, want unknown stream", err)
	}
}

// The capture lock serialises writers, so this is belt and braces -- but a
// backwards write would hold a stream back past its interval.
func TestCaptureAttempts_ClockOnlyMovesForward(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	newest := time.Now().UTC().Truncate(time.Second)

	if err := s.MarkCaptureAttempt(ctx, key, "primary", "query", newest); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCaptureAttempt(ctx, key, "primary", "query", newest.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.LastCaptureAttemptAt(ctx, key, "primary", "query")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(newest) {
		t.Errorf("attempt %s, want the older write to be ignored (%s)", got, newest)
	}
}

// The table is additive, so an existing history.db gains it without a version
// bump and an older dryrun keeps reading that file.
func TestCaptureAttempts_AdditiveMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != HistorySchemaVersion {
		t.Fatalf("user_version %d, want %d", version, HistorySchemaVersion)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// re-opening must not stamp anything new, and must not fail on the table
	// it already created
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Compat() != CompatOK {
		t.Errorf("compat %v, want CompatOK", s2.Compat())
	}
	if err := s2.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != HistorySchemaVersion {
		t.Errorf("user_version %d after reopen, want %d", version, HistorySchemaVersion)
	}
	if err := s2.MarkCaptureAttempt(context.Background(),
		SnapshotKey{ProjectID: "p", DatabaseID: "d"}, "primary", "query", time.Now()); err != nil {
		t.Errorf("MarkCaptureAttempt on a reopened store: %v", err)
	}
}
