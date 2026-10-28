package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

// seedTake writes one schema + one activity take for node at ts, under k.
func seedTake(t *testing.T, store history.SnapshotStore, k history.SnapshotKey, schemaHash, actHash, node string, ts time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.Put(ctx, k, history.WrapSchema(syncTestSchema(schemaHash, "appdb", ts))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, k, history.WrapActivity(syncTestActivity(schemaHash, actHash, node, ts, false))); err != nil {
		t.Fatal(err)
	}
}

// pullInto seeds a filesystem source with one fresh take, a local db with one
// 30-day-old take, writes the [history] block when wantHistory, runs the real
// pull command and returns the local activity takes afterward.
func pullInto(t *testing.T, wantHistory bool, extraArgs ...string) []history.SnapshotSummary {
	t.Helper()
	resetFlags(t)
	ctx := context.Background()
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	srcDir := t.TempDir()
	src, err := history.NewFilesystemStore(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	seedTake(t, src, k, "sh-new", "ac-new", "primary", now.Add(-time.Hour))

	localPath := filepath.Join(t.TempDir(), "history.db")
	local, err := history.Open(localPath)
	if err != nil {
		t.Fatal(err)
	}
	seedTake(t, local, k, "sh-old", "ac-old", "primary", now.Add(-30*24*time.Hour))
	local.Close()

	body := ""
	if wantHistory {
		body = "[history]\nauto_prune = true\nmax_age = \"14d\"\n"
	}
	withCWD(t, writeTOML(t, t.TempDir(), body))

	cmd := snapshotPullCmd()
	cmd.SetArgs(append([]string{"--from-path", srcDir, "--history-db", localPath, "--all"}, extraArgs...))
	if err := cmd.Execute(); err != nil {
		t.Fatalf("pull: %v", err)
	}

	db, err := history.Open(localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sums, err := db.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	return sums
}

// The reader case: the pull lands the new take and retention drops the aged one.
func TestPullAutoPrunes(t *testing.T) {
	sums := pullInto(t, true)
	if len(sums) != 1 || sums[0].ContentHash != "ac-new" {
		t.Errorf("got %d takes (%v), want only ac-new", len(sums), sums)
	}
}

func TestPullWithoutHistoryConfigKeepsRows(t *testing.T) {
	if sums := pullInto(t, false); len(sums) != 2 {
		t.Errorf("got %d takes, want 2 (no prune without [history])", len(sums))
	}
}

// --full asks for the whole history, so the pull must not prune what it just copied.
func TestPullFullDoesNotPrune(t *testing.T) {
	if sums := pullInto(t, true, "--full"); len(sums) != 2 {
		t.Errorf("got %d takes, want 2 (--full skips prune)", len(sums))
	}
}
