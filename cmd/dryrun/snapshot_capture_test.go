package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/fixturize/masking"

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
			t.Errorf("run=%v, want a future timestamp to be ignored, leaving it due", run)
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

	unlock2, err := lockCaptures(db)
	if err != nil {
		t.Fatalf("lock was not released: %v", err)
	}
	unlock2()
}

// A lock left by a crashed run is reported, not silently reclaimed: taking one
// over by path cannot be made race-free, and two processes that both decide a
// lock is stale would both run against production.
func TestLockCaptures_StaleLockIsReportedNotTaken(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "history.db")
	path := captureLockPath(db)

	if err := os.WriteFile(path, []byte("999 crashed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-72 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	_, err := lockCaptures(db)
	if err == nil {
		t.Fatal("an old lock was taken over automatically")
	}
	// the operator needs the age and the way out, not just a refusal
	for _, want := range []string{path, "held for", "rm "} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	// cleared by hand, the next run takes it
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	unlock, err := lockCaptures(db)
	if err != nil {
		t.Fatalf("lock not available after clearing: %v", err)
	}
	unlock()
}

// A lock cleared by hand and retaken by another run must not be deleted by the
// run that lost it.
func TestLockCaptures_UnlockOnlyRemovesItsOwn(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "history.db")
	path := captureLockPath(db)

	mine, err := lockCaptures(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("someone else\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mine()
	if _, err := os.Stat(path); err != nil {
		t.Error("unlock deleted another run's lock")
	}
}

// --all puts connection failures in a cron log for every unreachable node, and
// Postgres takes secrets in three shapes.
func TestRedactSecrets(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   string
		secret string
	}{
		{
			name:   "url credentials",
			in:     "connection failed to postgres://postgres:hunter2@10.0.0.1:5432/db: refused",
			want:   "postgres://postgres:***@10.0.0.1:5432/db",
			secret: "hunter2",
		},
		{
			// an unencoded @ in the password used to leak everything after it
			name:   "unencoded at-sign in the password",
			in:     "postgres://u:p@ss@host/db",
			want:   "postgres://u:***@host/db",
			secret: "p@ss",
		},
		{
			name: "no password to redact",
			in:   "postgres://nopassword@host/db",
			want: "postgres://nopassword@host/db",
		},
		{
			name:   "keyword dsn",
			in:     "host=h port=5432 password=s3cret dbname=x",
			want:   "password=***",
			secret: "s3cret",
		},
		{
			name:   "quoted keyword dsn",
			in:     "password='quoted secret' host=h",
			want:   "password=***",
			secret: "quoted secret",
		},
		{
			name:   "url query parameter",
			in:     "postgres://u@h/db?password=leaked&sslmode=require",
			want:   "password=***",
			secret: "leaked",
		},
		{
			// an address is not a credential
			name: "plain text is untouched",
			in:   "no url here, owner@example.com",
			want: "owner@example.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactSecrets(tc.in)
			if !strings.Contains(got, tc.want) {
				t.Errorf("redactSecrets(%q) = %q, want it to contain %q", tc.in, got, tc.want)
			}
			if tc.secret != "" && strings.Contains(got, tc.secret) {
				t.Errorf("redactSecrets(%q) = %q still carries the secret", tc.in, got)
			}
		})
	}

	t.Run("every url in a message", func(t *testing.T) {
		got := redactSecrets("two: postgres://a:b@h1/db and postgres://c:d@h2/db")
		for _, leaked := range []string{":b@", ":d@"} {
			if strings.Contains(got, leaked) {
				t.Errorf("%q still carries a password", got)
			}
		}
	})

	// sslmode is not a secret; over-redacting hides the cause of the failure
	t.Run("non-secret parameters survive", func(t *testing.T) {
		got := redactSecrets("host=h user=app sslmode=require dbname=x")
		for _, want := range []string{"host=h", "user=app", "sslmode=require"} {
			if !strings.Contains(got, want) {
				t.Errorf("redaction ate %q: %s", want, got)
			}
		}
	})
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

// `capture` writes planner rows that `push` ships to a registry, so it must
// mask exactly as `snapshot take` does. Before this test the planner stream
// bypassed masking, bloat annotation and Masking entirely.
func TestCaptureStream_PlannerMasksLikeTake(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "testdb"}
	target := captureTarget{Label: "primary"}

	cases := []struct {
		name       string
		policy     *masking.Policy
		wantMasked map[string]bool
	}{
		{"no policy leaves stats intact", nil, map[string]bool{"email": false, "id": false}},
		{"policy nulls the matching column only", loadTestPolicy(t, "users.email"), map[string]bool{"email": true, "id": false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{PlannerColumns: []schema.ColumnStatsEntry{
				colWithStats("users", "email"),
				colWithStats("users", "id"),
			}}
			w := &stubWriter{Stored: &schema.SchemaSnapshot{ContentHash: "sr-1"}}

			if _, err := captureStream(ctx, cap, w, key, target, "planner", "sr-1", 0,
				captureRunOptions{MaskPolicy: tc.policy}); err != nil {
				t.Fatalf("captureStream: %v", err)
			}
			if w.LastPlanner == nil {
				t.Fatal("PutPlanner never received a snapshot")
			}
			for _, c := range w.LastPlanner.Columns {
				masked := c.Stats.MostCommonVals == nil
				if masked != tc.wantMasked[c.Column] {
					t.Errorf("column %s masked=%t, want %t", c.Column, masked, tc.wantMasked[c.Column])
				}
			}
			// a row whose Masking is nil is indistinguishable from "unknown"
			if w.LastPlanner.Masking == nil {
				t.Fatal("Masking was not recorded on the row")
			}
			if w.LastPlanner.Masking.Applied != (tc.policy != nil) {
				t.Errorf("Masking.Applied=%t, want %t", w.LastPlanner.Masking.Applied, tc.policy != nil)
			}
		})
	}
}

// A node without pg_stat_statements must be skipped, not fail the fleet run
// every five minutes forever.
func TestCaptureStream_QueryStatsUnavailableIsSkipped(t *testing.T) {
	cap := &stubCapturer{QueryStatsErr: schema.ErrQueryStatsUnavailable}
	w := &stubWriter{Stored: &schema.SchemaSnapshot{ContentHash: "sr-1"}}

	_, err := captureStream(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"},
		captureTarget{Label: "replica"}, "query", "sr-1", 0, captureRunOptions{})
	if !errors.Is(err, errStreamUnavailable) {
		t.Errorf("got %v, want errStreamUnavailable", err)
	}
}

func TestCaptureStream_SchemaIsRefused(t *testing.T) {
	_, err := captureStream(context.Background(), &stubCapturer{}, &stubWriter{},
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"},
		captureTarget{Label: "primary"}, "schema", "sr-1", 0, captureRunOptions{})
	if err == nil || !strings.Contains(err.Error(), "snapshot take") {
		t.Errorf("got %v, want a pointer to snapshot take", err)
	}
}

// A stream that dedups writes no row, so a due clock read from row timestamps
// alone would introspect production on every tick forever.
func TestDueStreams_AttemptSatisfiesTheClock(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary", Interval: time.Hour}

	run, _, err := dueStreams(ctx, store, key, target, []string{"schema"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 1 {
		t.Fatalf("run=%v, want the first tick to be due", run)
	}

	if err := store.MarkCaptureAttempt(ctx, key, target.Label, "schema", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	run, skipped, err := dueStreams(ctx, store, key, target, []string{"schema"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 0 || len(skipped) != 1 {
		t.Errorf("run=%v skipped=%v, want the attempt to hold the second tick back", run, skipped)
	}

	// and it expires like a row does. A separate store, because the marker
	// only moves forward: this attempt has to be the first one written.
	aged := testStoreAt(t, t.TempDir())
	if err := aged.MarkCaptureAttempt(ctx, key, target.Label, "schema", time.Now().UTC().Add(-90*time.Minute)); err != nil {
		t.Fatal(err)
	}
	run, _, err = dueStreams(ctx, aged, key, target, []string{"schema"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 1 {
		t.Errorf("run=%v, want it due again after the interval", run)
	}
}

// The newer of the two markers wins, in both directions.
func TestDueStreams_RowAndAttemptTakeTheMax(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("a recent row outweighs a stale attempt", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
		putQueryAt(t, store, key, "q-1", "primary", now.Add(-5*time.Minute))
		if err := store.MarkCaptureAttempt(ctx, key, "primary", "query", now.Add(-10*time.Hour)); err != nil {
			t.Fatal(err)
		}
		run, _, err := dueStreams(ctx, store, key,
			captureTarget{Label: "primary", Interval: time.Hour}, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 0 {
			t.Errorf("run=%v, want the recent row to hold it back", run)
		}
	})

	t.Run("a recent attempt outweighs a stale row", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
		putQueryAt(t, store, key, "q-1", "primary", now.Add(-10*time.Hour))
		if err := store.MarkCaptureAttempt(ctx, key, "primary", "query", now.Add(-5*time.Minute)); err != nil {
			t.Fatal(err)
		}
		run, _, err := dueStreams(ctx, store, key,
			captureTarget{Label: "primary", Interval: time.Hour}, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 0 {
			t.Errorf("run=%v, want the recent attempt to hold it back", run)
		}
	})

	// a skewed clock, most likely arriving via pull, must not starve a stream
	t.Run("a future attempt is ignored", func(t *testing.T) {
		store := testStoreAt(t, t.TempDir())
		key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
		if err := store.MarkCaptureAttempt(ctx, key, "primary", "query", now.Add(48*time.Hour)); err != nil {
			t.Fatal(err)
		}
		run, _, err := dueStreams(ctx, store, key,
			captureTarget{Label: "primary", Interval: time.Hour}, []string{"query"}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(run) != 1 {
			t.Errorf("run=%v, want a future-dated attempt to read as due", run)
		}
	})
}

// schema is project-scoped: one node's capture already satisfies another's
// clock through the row, so the attempt must cross-satisfy too. Keying it per
// node would leave the second node re-introspecting every tick.
func TestDueStreams_ProjectScopedAttemptCrossesNodes(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	if err := store.MarkCaptureAttempt(ctx, key, "node-a", "schema", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	run, _, err := dueStreams(ctx, store, key,
		captureTarget{Label: "node-b", Interval: time.Hour}, []string{"schema"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 0 {
		t.Errorf("run=%v, want node-a's schema attempt to satisfy node-b", run)
	}

	// per-node streams keep the opposite property
	if err := store.MarkCaptureAttempt(ctx, key, "node-a", "query", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	run, _, err = dueStreams(ctx, store, key,
		captureTarget{Label: "node-b", Interval: time.Hour}, []string{"query"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 1 {
		t.Errorf("run=%v, want node-b's query stream still due", run)
	}
}

// Recording a failed attempt would silence a broken node for a full interval
// instead of retrying on the next tick.
func TestCaptureStreams_AttemptRecording(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary"}

	cases := []struct {
		name    string
		cap     *stubCapturer
		wantErr bool
		want    bool
	}{
		{"a captured row marks the attempt", &stubCapturer{}, false, true},
		{"an unavailable stream marks the attempt",
			&stubCapturer{QueryStatsErr: schema.ErrQueryStatsUnavailable}, false, true},
		{"a real error does not", &stubCapturer{QueryStatsErr: errors.New("connection reset")}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := testStoreAt(t, t.TempDir())
			_, err := captureStreams(ctx, tc.cap, store, key, target,
				[]string{"query"}, "sr-1", 0, captureRunOptions{})
			if (err != nil) != tc.wantErr {
				t.Fatalf("captureStreams err=%v, wantErr=%t", err, tc.wantErr)
			}
			_, ok, err := store.LastCaptureAttemptAt(ctx, key, target.Label, "query")
			if err != nil {
				t.Fatal(err)
			}
			if ok != tc.want {
				t.Errorf("attempt recorded=%t, want %t", ok, tc.want)
			}
		})
	}
}

// An unchanged stream dedups and writes no row; without the attempt marker it
// would stay due forever.
func TestCaptureStreams_DedupMarksTheAttempt(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary", Interval: time.Hour}
	store := testStoreAt(t, t.TempDir())
	cap := &stubCapturer{}

	for i := range 2 {
		done, err := captureStreams(ctx, cap, store, key, target,
			[]string{"query"}, "sr-1", 0, captureRunOptions{})
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if i == 1 && strings.Join(done, ",") != "query=unchanged" {
			t.Fatalf("second run reported %v, want the dedup", done)
		}
	}

	run, _, err := dueStreams(ctx, store, key, target, []string{"query"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 0 {
		t.Errorf("run=%v, want a deduped stream to stop being permanently due", run)
	}
}

// capture_attempts records what this host did, not what the project contains:
// a pulled row must never mark a stream recently attempted here.
func TestCaptureAttempts_NotWrittenByAStoreWrite(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	putQueryAt(t, store, key, "pulled-1", "primary", time.Now().UTC())
	if _, ok, err := store.LastCaptureAttemptAt(ctx, key, "primary", "query"); err != nil || ok {
		t.Errorf("ok=%t err=%v, want a stored row to leave the attempt clock untouched", ok, err)
	}
}
