package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func TestSnapshotStaleAfterDefault(t *testing.T) {
	resetFlags(t)
	withCWD(t, t.TempDir())

	var buf bytes.Buffer
	if got := snapshotStaleAfter(&buf); got != defaultSnapshotStaleAfter {
		t.Errorf("got %s, want the %s default", got, defaultSnapshotStaleAfter)
	}
	if buf.Len() != 0 {
		t.Errorf("unexpected warning: %q", buf.String())
	}
}

func TestSnapshotStaleAfterConfig(t *testing.T) {
	cases := []struct {
		name    string
		toml    string
		want    time.Duration
		warning string
	}{
		{"duration", "[history]\nstale_after = \"3d\"\n", 72 * time.Hour, ""},
		{"hours", "[history]\nstale_after = \"12h\"\n", 12 * time.Hour, ""},
		{"zero disables", "[history]\nstale_after = \"0\"\n", 0, ""},
		{"empty falls back", "[history]\nstale_after = \"\"\n", defaultSnapshotStaleAfter, ""},
		{"invalid falls back", "[history]\nstale_after = \"soon\"\n", defaultSnapshotStaleAfter, "invalid [history] stale_after"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resetFlags(t)
			withCWD(t, writeTOML(t, t.TempDir(), c.toml))

			var buf bytes.Buffer
			if got := snapshotStaleAfter(&buf); got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
			if c.warning == "" && buf.Len() != 0 {
				t.Errorf("unexpected warning: %q", buf.String())
			}
			if c.warning != "" && !strings.Contains(buf.String(), c.warning) {
				t.Errorf("warning = %q, want %q", buf.String(), c.warning)
			}
		})
	}
}

func putSchemaAt(t *testing.T, store *history.Store, key history.SnapshotKey, hash string, at time.Time) *schema.SchemaSnapshot {
	t.Helper()
	snap := &schema.SchemaSnapshot{
		Database:    "seeded",
		PgVersion:   "16.4",
		ContentHash: hash,
		Timestamp:   at,
		Tables:      []schema.Table{{Schema: "public", Name: "t_" + hash}},
	}
	if _, err := store.PutSchema(context.Background(), key, snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestWarnIfSnapshotStale(t *testing.T) {
	resetFlags(t)
	withCWD(t, t.TempDir())

	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}
	now := time.Now().UTC()

	t.Run("old schema warns", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		snap := putSchemaAt(t, store, key, "old", now.Add(-30*24*time.Hour))

		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, snap.Timestamp)
		got := buf.String()
		if !strings.Contains(got, "demo/demo") || !strings.Contains(got, "old") {
			t.Errorf("warning = %q, want the key and the age", got)
		}
		if !strings.HasPrefix(got, "warning: ") || !strings.Contains(got, "dryrun snapshot pull") {
			t.Errorf("warning = %q, want a refresh hint", got)
		}
	})

	t.Run("fresh schema silent", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		snap := putSchemaAt(t, store, key, "fresh", now.Add(-time.Hour))
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, snap.Timestamp)
		if buf.Len() != 0 {
			t.Errorf("unexpected warning: %q", buf.String())
		}
	})

	t.Run("zero timestamp silent", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		// fixtures written without a timestamp must not read as year 1
		putSchemaAt(t, store, key, "zero", time.Time{})
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, time.Time{})
		if buf.Len() != 0 {
			t.Errorf("unexpected warning: %q", buf.String())
		}
	})

	t.Run("future timestamp silent", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		snap := putSchemaAt(t, store, key, "skew", now.Add(48*time.Hour))
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, snap.Timestamp)
		if buf.Len() != 0 {
			t.Errorf("unexpected warning: %q", buf.String())
		}
	})

	t.Run("pull refresh updates the payload, not the column", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		old := putSchemaAt(t, store, key, "same", now.Add(-30*24*time.Hour))
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, old.Timestamp)
		if buf.Len() == 0 {
			t.Fatal("pre-pull snapshot should warn")
		}

		// same content hash, newer capture time: the pull path (capturedLocally
		// false) refreshes the payload while snapshots.timestamp stays behind
		refreshed := *old
		refreshed.Timestamp = now.Add(-time.Minute)
		if _, err := store.Put(ctx, key, history.WrapSchema(&refreshed)); err != nil {
			t.Fatal(err)
		}
		buf.Reset()
		warnIfSnapshotStale(ctx, &buf, store, key, refreshed.Timestamp)
		if buf.Len() != 0 {
			t.Errorf("a just-pulled snapshot warned: %q", buf.String())
		}
	})

	t.Run("recent capture attempt suppresses an old schema", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		snap := putSchemaAt(t, store, key, "quiet", now.Add(-30*24*time.Hour))
		if err := store.MarkCaptureAttempt(ctx, key, "primary", "schema", now.Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, snap.Timestamp)
		if buf.Len() != 0 {
			t.Errorf("a quiet but actively captured schema warned: %q", buf.String())
		}
	})

	t.Run("threshold disables", func(t *testing.T) {
		resetFlags(t)
		withCWD(t, writeTOML(t, t.TempDir(), "[history]\nstale_after = \"0\"\n"))
		store := testStoreAt(t, t.TempDir())
		snap := putSchemaAt(t, store, key, "old2", now.Add(-30*24*time.Hour))
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, snap.Timestamp)
		if buf.Len() != 0 {
			t.Errorf("stale_after = 0 warned: %q", buf.String())
		}
	})

	t.Run("env silences", func(t *testing.T) {
		resetFlags(t)
		withCWD(t, t.TempDir())
		t.Setenv("DRYRUN_NO_STALE_WARNING", "1")
		store := testStoreAt(t, t.TempDir())
		snap := putSchemaAt(t, store, key, "old3", now.Add(-30*24*time.Hour))
		var buf bytes.Buffer
		warnIfSnapshotStale(ctx, &buf, store, key, snap.Timestamp)
		if buf.Len() != 0 {
			t.Errorf("DRYRUN_NO_STALE_WARNING warned: %q", buf.String())
		}
	})
}

// The loaders behind lint and check must carry the warning to the command's
// stderr, and stay silent when the snapshot is fresh.
func TestLoadersWarnOnStaleSnapshot(t *testing.T) {
	resetFlags(t)
	dir := writeTOML(t, t.TempDir(), "[project]\nid = \"demo\"\n")
	withCWD(t, dir)
	key := history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}

	store := testStoreAt(t, filepath.Join(dir, ".dryrun"))
	snap := putSchemaAt(t, store, key, "stale", time.Now().UTC().Add(-30*24*time.Hour))
	store.Close()

	var buf bytes.Buffer
	if _, err := loadSchemaForLint(&buf); err != nil {
		t.Fatalf("loadSchemaForLint: %v", err)
	}
	if !strings.Contains(buf.String(), "warning: the newest snapshot") {
		t.Errorf("lint loader stderr = %q, want the stale warning", buf.String())
	}

	buf.Reset()
	if _, err := loadAnnotatedForCheck(context.Background(), "", &buf); err != nil {
		t.Fatalf("loadAnnotatedForCheck: %v", err)
	}
	if !strings.Contains(buf.String(), "warning: the newest snapshot") {
		t.Errorf("check loader stderr = %q, want the stale warning", buf.String())
	}

	// refresh the payload so both loaders go quiet
	store, err := history.Open(filepath.Join(dir, ".dryrun", "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	refreshed := *snap
	refreshed.Timestamp = time.Now().UTC().Add(-time.Hour)
	if _, err := store.Put(context.Background(), key, history.WrapSchema(&refreshed)); err != nil {
		t.Fatal(err)
	}

	buf.Reset()
	if _, err := loadSchemaForLint(&buf); err != nil {
		t.Fatalf("loadSchemaForLint: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("lint loader warned on a fresh pull: %q", buf.String())
	}
}

func TestNewestSnapshotTimePicksTheFreshestStream(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "demo", DatabaseID: "demo"}
	store := testStoreAt(t, t.TempDir())
	now := time.Now().UTC()

	snap := putSchemaAt(t, store, key, "schema", now.Add(-20*24*time.Hour))
	if _, err := store.PutActivity(ctx, key, &schema.ActivityStatsSnapshot{
		SchemaRefHash: snap.ContentHash,
		ContentHash:   "activity-1",
		Node:          schema.NodeIdentity{Source: "primary", Timestamp: now.Add(-time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	got := newestSnapshotTime(ctx, store, key, snap.Timestamp)
	if age := now.Sub(got); age > time.Hour {
		t.Errorf("newest = %s (%s old), want the fresh activity row", got, age)
	}
}
