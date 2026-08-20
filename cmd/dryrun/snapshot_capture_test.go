package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func TestCaptureTargets_OneOff(t *testing.T) {
	got, err := captureTargets("", "postgres://u@h/db", "replica-1", []string{"query"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d targets, want 1", len(got))
	}
	if got[0].Label != "replica-1" || got[0].URL != "postgres://u@h/db" {
		t.Errorf("target %+v", got[0])
	}
	if got[0].Role != "auto" {
		t.Errorf("role %q, want auto for an ad-hoc capture", got[0].Role)
	}
}

func TestCaptureTargets_Errors(t *testing.T) {
	cases := []struct {
		name              string
		node, from, label string
		all               bool
		wantErr           string
	}{
		{name: "all with node", node: "primary", all: true, wantErr: "drop --node"},
		{name: "all with from", from: "postgres://x", all: true, wantErr: "drop --node"},
		{name: "from with node", node: "primary", from: "postgres://x", wantErr: "does not combine"},
		{name: "from without label", from: "postgres://x", wantErr: "--label is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := captureTargets(tc.node, tc.from, tc.label, nil, tc.all)
			if err == nil {
				t.Fatalf("want an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// --streams is the operator overriding config for this run.
func TestTargetFromNode_CLIStreamsWin(t *testing.T) {
	n := config.ResolvedNode{Name: "primary", Streams: []string{"activity", "query"}, Interval: time.Hour, Pool: true}

	if got := targetFromNode(n, nil); strings.Join(got.Streams, ",") != "activity,query" {
		t.Errorf("streams %v, want the config's", got.Streams)
	}
	got := targetFromNode(n, []string{"query"})
	if strings.Join(got.Streams, ",") != "query" {
		t.Errorf("streams %v, want the CLI's", got.Streams)
	}
	if !got.Pool || got.Interval != time.Hour {
		t.Errorf("pool/interval lost: %+v", got)
	}
}

func TestCandidateStreams(t *testing.T) {
	// with no configured streams the role decides later, so every stream is a
	// candidate for the cadence pre-check
	got := candidateStreams(captureTarget{})
	for _, want := range []string{"planner", "activity", "query"} {
		if !strings.Contains(strings.Join(got, ","), want) {
			t.Errorf("candidates %v omit %q", got, want)
		}
	}
	if got := candidateStreams(captureTarget{Streams: []string{"query"}}); len(got) != 1 {
		t.Errorf("candidates %v, want only the configured stream", got)
	}
}

func testStoreAt(t *testing.T, dir string) *history.Store {
	t.Helper()
	store, err := history.Open(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestDueStreams(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary", Interval: time.Hour}

	t.Run("no interval means always run", func(t *testing.T) {
		run, skipped, err := dueStreams(ctx, store, key, captureTarget{Label: "primary"}, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 1 || len(skipped) != 0 {
			t.Errorf("run=%v skipped=%v", run, skipped)
		}
	})

	t.Run("without --due nothing is skipped", func(t *testing.T) {
		run, _, err := dueStreams(ctx, store, key, target, []string{"query"}, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 1 {
			t.Errorf("run=%v, want the stream to run", run)
		}
	})

	t.Run("never captured is due", func(t *testing.T) {
		run, _, err := dueStreams(ctx, store, key, target, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 1 {
			t.Errorf("run=%v, want a first capture to be due", run)
		}
	})

	t.Run("recent capture is not due", func(t *testing.T) {
		putQueryAt(t, store, key, "q-1", "primary", time.Now().UTC().Add(-5*time.Minute))
		run, skipped, err := dueStreams(ctx, store, key, target, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 0 || len(skipped) != 1 {
			t.Errorf("run=%v skipped=%v, want it held back", run, skipped)
		}
	})

	t.Run("elapsed interval is due again", func(t *testing.T) {
		putQueryAt(t, store, key, "q-2", "elapsed", time.Now().UTC().Add(-90*time.Minute))
		run, _, err := dueStreams(ctx, store, key, captureTarget{Label: "elapsed", Interval: time.Hour}, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 1 {
			t.Errorf("run=%v, want it due after the interval", run)
		}
	})

	// a row pulled from a host with a skewed clock would otherwise hold the
	// stream back until that future timestamp passed
	t.Run("future-dated row does not starve the stream", func(t *testing.T) {
		putQueryAt(t, store, key, "q-3", "future", time.Now().UTC().Add(48*time.Hour))
		run, _, err := dueStreams(ctx, store, key, captureTarget{Label: "future", Interval: time.Hour}, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 1 {
			t.Errorf("run=%v, want a clamped future timestamp to read as due", run)
		}
	})

	// each stream keeps its own cadence, so a fresh query capture must not
	// hold activity back
	t.Run("streams are independent", func(t *testing.T) {
		putQueryAt(t, store, key, "q-4", "mixed", time.Now().UTC().Add(-time.Minute))
		run, skipped, err := dueStreams(ctx, store, key,
			captureTarget{Label: "mixed", Interval: time.Hour}, []string{"activity", "query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(run, ",") != "activity" {
			t.Errorf("run=%v, want only activity", run)
		}
		if len(skipped) != 1 {
			t.Errorf("skipped=%v, want query held back", skipped)
		}
	})
}

func TestLockCaptures(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "history.db")

	unlock, err := lockCaptures(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockCaptures(db); err == nil {
		t.Fatal("a second capture took the lock while the first held it")
	}
	unlock()

	// released: the next run gets it
	unlock2, err := lockCaptures(db)
	if err != nil {
		t.Fatalf("lock was not released: %v", err)
	}
	unlock2()

	// a crashed run leaves a file behind; it must not block cron forever
	t.Run("stale lock is taken over", func(t *testing.T) {
		path := captureLockPath(db)
		if err := os.WriteFile(path, []byte("1 stale\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-captureLockStale - time.Hour)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
		unlock, err := lockCaptures(db)
		if err != nil {
			t.Fatalf("a stale lock blocked the run: %v", err)
		}
		unlock()
	})
}

// --all puts connection failures in a cron log for every unreachable node.
func TestRedactURLPasswords(t *testing.T) {
	cases := []struct{ in, want string }{
		{
			in:   "connection failed to postgres://postgres:hunter2@10.0.0.1:5432/db: refused",
			want: "postgres://postgres:***@10.0.0.1:5432/db",
		},
		{
			in:   "postgres://nopassword@host/db",
			want: "postgres://nopassword@host/db",
		},
		{
			in:   "two: postgres://a:b@h1/db and postgres://c:d@h2/db",
			want: "postgres://c:***@h2/db",
		},
		{in: "no url here", want: "no url here"},
	}
	for _, tc := range cases {
		got := redactURLPasswords(tc.in)
		if !strings.Contains(got, tc.want) {
			t.Errorf("redact(%q) = %q, want it to contain %q", tc.in, got, tc.want)
		}
		if strings.Contains(got, "hunter2") || strings.Contains(got, ":b@") || strings.Contains(got, ":d@") {
			t.Errorf("redact(%q) = %q still carries a password", tc.in, got)
		}
	}
}

func putQueryAt(t *testing.T, s *history.Store, key history.SnapshotKey, hash, label string, at time.Time) {
	t.Helper()
	q := &schema.QueryStatsSnapshot{
		SchemaRefHash: "sr",
		ContentHash:   hash,
		Node:          schema.NodeIdentity{Source: label, Timestamp: at},
		Queries: []schema.QueryStatsEntry{{
			Fingerprint: "fp-" + hash,
			Members:     []schema.QueryStatsMember{{QueryID: int64(len(hash)), Calls: 1}},
			Calls:       1,
		}},
	}
	if _, err := s.PutQueryStats(context.Background(), key, q); err != nil {
		t.Fatal(err)
	}
}
