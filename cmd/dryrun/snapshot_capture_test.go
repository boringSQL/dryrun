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
	n := config.ResolvedNode{Name: "primary", Streams: []string{"activity", "query"},
		Interval: time.Hour, Pool: true,
		Intervals: map[string]time.Duration{"query": 6 * time.Hour}}

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
	// a dropped map is silent: every stream just falls back to the base
	if got.Intervals["query"] != 6*time.Hour {
		t.Errorf("per-stream intervals lost: %+v", got.Intervals)
	}
}

func TestCandidateStreams(t *testing.T) {
	// with no configured streams the role decides later, so every stream is a
	// candidate for the cadence pre-check
	got := candidateStreams(captureTarget{})
	for _, want := range []string{"schema", "planner", "activity", "query"} {
		if !strings.Contains(strings.Join(got, ","), want) {
			t.Errorf("candidates %v omit %q", got, want)
		}
	}
	if got := candidateStreams(captureTarget{Role: "standby"}); strings.Contains(strings.Join(got, ","), "schema") {
		t.Errorf("candidates %v: a declared standby has no schema to originate", got)
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

			if _, _, err := captureStream(ctx, cap, w, key, target, "planner", "sr-1", 0,
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

	_, _, err := captureStream(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"},
		captureTarget{Label: "replica"}, "query", "sr-1", 0, captureRunOptions{})
	if !errors.Is(err, errStreamUnavailable) {
		t.Errorf("got %v, want errStreamUnavailable", err)
	}
}

func TestCaptureStream_Schema(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	t.Run("a primary introspects and stores", func(t *testing.T) {
		cap := &stubCapturer{}
		w := &stubWriter{}
		n, hash, err := captureStream(ctx, cap, w, key,
			captureTarget{Label: "primary", DetectedRole: history.NodeRolePrimary},
			"schema", "", 0, captureRunOptions{})
		if err != nil {
			t.Fatalf("captureStream: %v", err)
		}
		if cap.IntrospectN != 1 || w.SchemaN != 1 {
			t.Errorf("introspects=%d puts=%d, want 1 each", cap.IntrospectN, w.SchemaN)
		}
		if hash != "schema-hash-1" {
			t.Errorf("hash=%q, want the captured snapshot's", hash)
		}
		if n != 3 {
			t.Errorf("n=%d, want the stub's 3 tables", n)
		}
	})

	t.Run("an unchanged schema dedups and still reports its hash", func(t *testing.T) {
		cap := &stubCapturer{}
		w := &stubWriter{SchemaDedups: true}
		_, hash, err := captureStream(ctx, cap, w, key,
			captureTarget{Label: "primary", DetectedRole: history.NodeRolePrimary},
			"schema", "", 0, captureRunOptions{})
		if !errors.Is(err, errStreamUnchanged) {
			t.Fatalf("err=%v, want errStreamUnchanged", err)
		}
		if hash != "schema-hash-1" {
			t.Errorf("hash=%q, want the deduped content's hash", hash)
		}
	})

	// role = "auto" on a replica, or a failover since the config edit
	t.Run("a standby skips instead of failing", func(t *testing.T) {
		cap := &stubCapturer{}
		w := &stubWriter{}
		_, _, err := captureStream(ctx, cap, w, key,
			captureTarget{Label: "replica", DetectedRole: history.NodeRoleStandby},
			"schema", "", 0, captureRunOptions{})
		if !errors.Is(err, errStreamUnavailable) {
			t.Fatalf("err=%v, want errStreamUnavailable", err)
		}
		if cap.IntrospectN != 0 || w.SchemaN != 0 {
			t.Errorf("standby introspected=%d stored=%d, want neither", cap.IntrospectN, w.SchemaN)
		}
	})
}

// A stream that dedups writes no row, so a due clock read from row timestamps
// alone would introspect production on every tick forever.
func TestDueStreams_AttemptSatisfiesTheClock(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary", Interval: time.Hour}

	run, _, err := dueStreams(ctx, store, key, target, []string{"planner"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 1 {
		t.Fatalf("run=%v, want the first tick to be due", run)
	}

	if err := store.MarkCaptureAttempt(ctx, key, target.Label, "planner", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	run, skipped, err := dueStreams(ctx, store, key, target, []string{"planner"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(run) != 0 || len(skipped) != 1 {
		t.Errorf("run=%v skipped=%v, want the attempt to hold the second tick back", run, skipped)
	}

	// and it expires like a row does. A separate store, because the marker
	// only moves forward: this attempt has to be the first one written.
	aged := testStoreAt(t, t.TempDir())
	if err := aged.MarkCaptureAttempt(ctx, key, target.Label, "planner", time.Now().UTC().Add(-90*time.Minute)); err != nil {
		t.Fatal(err)
	}
	run, _, err = dueStreams(ctx, aged, key, target, []string{"planner"}, true)
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

func TestSchemaFirst(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want string
	}{
		{[]string{"query", "schema", "activity"}, "schema,query,activity"},
		{[]string{"schema", "planner"}, "schema,planner"},
		{[]string{"planner", "activity", "query"}, "planner,activity,query"},
		{nil, ""},
	} {
		if got := strings.Join(schemaFirst(tc.in), ","); got != tc.want {
			t.Errorf("schemaFirst(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}

	// the caller's slice must not be reordered under it: capture_check and the
	// report both read the configured order
	in := []string{"query", "schema"}
	schemaFirst(in)
	if in[0] != "query" {
		t.Errorf("schemaFirst mutated its input: %v", in)
	}
}

// Schema writes the hash the other streams bind to, so it leads even when the
// config lists it last.
func TestCaptureStreams_SchemaRunsFirst(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	store := testStoreAt(t, t.TempDir())
	cap := &stubCapturer{}

	done, err := captureStreams(ctx, cap, store, key,
		captureTarget{Label: "primary", DetectedRole: history.NodeRolePrimary},
		[]string{"query", "schema"}, "", 0, captureRunOptions{})
	if err != nil {
		t.Fatalf("captureStreams: %v", err)
	}
	// reported in run order, not the order the caller asked for
	if strings.HasPrefix(strings.Join(done, " "), "query") {
		t.Errorf("done=%v, want schema reported first", done)
	}
	if cap.IntrospectN != 1 {
		t.Fatalf("introspects=%d, want the schema stream to have run", cap.IntrospectN)
	}
	if cap.QueryStatsN != 1 {
		t.Errorf("query stream ran %d times, want 1", cap.QueryStatsN)
	}
	if strings.Join(done, " ") != "schema=3 query=0" {
		t.Errorf("done=%v, want both streams captured", done)
	}
}

// Swaps the stream function for one that records the schemaRef each stream was
// called with, so the threading can be pinned before the schema branch stops
// refusing.
func stubStreams(t *testing.T, fn func(stream, schemaRef string) (int, string, error)) *[]string {
	t.Helper()
	var refs []string
	prev := captureStreamFn
	captureStreamFn = func(_ context.Context, _ initCapturer, _ initWriter, _ history.SnapshotKey,
		_ captureTarget, stream, schemaRef string, _ int, _ captureRunOptions) (int, string, error) {
		refs = append(refs, stream+"@"+schemaRef)
		return fn(stream, schemaRef)
	}
	t.Cleanup(func() { captureStreamFn = prev })
	return &refs
}

// §4.2: a stream captured after schema in the same run must bind to the hash
// that run just wrote, not the one it started with.
func TestCaptureStreams_ThreadsTheSchemaHash(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	for _, tc := range []struct {
		name    string
		schema  func() (int, string, error)
		want    string
		wantErr string
	}{
		{"a written schema rebinds the streams after it",
			func() (int, string, error) { return 139, "new-hash", nil },
			"schema@old-hash,activity@new-hash", ""},
		{"a deduped schema rebinds to the hash the store already held",
			func() (int, string, error) { return 0, "old-hash", errStreamUnchanged },
			"schema@old-hash,activity@old-hash", ""},
		{"a schema that captured nothing to bind to is a real error",
			func() (int, string, error) { return 139, "", nil },
			"schema@old-hash", "no content hash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testStoreAt(t, t.TempDir())
			refs := stubStreams(t, func(stream, _ string) (int, string, error) {
				if stream == "schema" {
					return tc.schema()
				}
				return 1, "", nil
			})
			_, err := captureStreams(ctx, &stubCapturer{}, store, key,
				captureTarget{Label: "primary"}, []string{"activity", "schema"},
				"old-hash", 0, captureRunOptions{})
			if tc.wantErr == "" && err != nil {
				t.Fatalf("captureStreams: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err=%v, want it to mention %q", err, tc.wantErr)
			}
			if got := strings.Join(*refs, ","); got != tc.want {
				t.Errorf("calls = %q, want %q", got, tc.want)
			}
		})
	}
}

// An empty hash is a real error, and a deduped schema writes no row -- so the
// attempt marker would be the only due clock and would silence the node for a
// full interval. §4.1: never record on a real error.
func TestCaptureStreams_NoAttemptWhenSchemaReturnsNoHash(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	store := testStoreAt(t, t.TempDir())
	stubStreams(t, func(stream, _ string) (int, string, error) { return 0, "", nil })

	if _, err := captureStreams(ctx, &stubCapturer{}, store, key,
		captureTarget{Label: "primary"}, []string{"schema"}, "", 0,
		captureRunOptions{}); err == nil {
		t.Fatal("want an error when schema returns no content hash")
	}
	// schema is project-scoped, so its attempt is keyed with an empty label
	if _, ok, err := store.LastCaptureAttemptAt(ctx, key, "", "schema"); err != nil || ok {
		t.Errorf("attempt recorded=%t err=%v, want a real error to leave the clock untouched", ok, err)
	}
}

// captureOneNode waives --allow-orphan because the run is going to write a
// schema; if the schema stream skips, that waiver has to be paid back.
func TestCaptureStreams_OrphanWaiverPayback(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	for _, tc := range []struct {
		name    string
		streams []string
		opts    captureRunOptions
		wantErr bool
	}{
		{"a skipped schema orphans the streams after it", []string{"schema", "activity"}, captureRunOptions{}, true},
		{"schema alone orphans nothing", []string{"schema"}, captureRunOptions{}, false},
		{"--allow-orphan was the operator's call", []string{"schema", "activity"}, captureRunOptions{AllowOrphan: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testStoreAt(t, t.TempDir())
			stubStreams(t, func(stream, _ string) (int, string, error) {
				if stream == "schema" {
					return 0, "", errStreamUnavailable // a standby skips
				}
				return 1, "", nil
			})
			// empty schemaRef: nothing stored for the run to fall back on
			_, err := captureStreams(ctx, &stubCapturer{}, store, key,
				captureTarget{Label: "primary"}, tc.streams, "", 0, tc.opts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v, wantErr=%t", err, tc.wantErr)
			}
		})
	}
}

// A standby that skips schema but has a stored schema to bind to is not an
// orphan: the payback must not fire.
func TestCaptureStreams_SkippedSchemaWithAStoredRefIsFine(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	store := testStoreAt(t, t.TempDir())
	refs := stubStreams(t, func(stream, _ string) (int, string, error) {
		if stream == "schema" {
			return 0, "", errStreamUnavailable
		}
		return 1, "", nil
	})

	done, err := captureStreams(ctx, &stubCapturer{}, store, key,
		captureTarget{Label: "standby"}, []string{"schema", "activity"},
		"stored-hash", 0, captureRunOptions{})
	if err != nil {
		t.Fatalf("captureStreams: %v", err)
	}
	if strings.Join(done, " ") != "schema=n/a activity=1" {
		t.Errorf("done=%v, want the standby to skip schema and still capture activity", done)
	}
	if got := strings.Join(*refs, ","); got != "schema@stored-hash,activity@stored-hash" {
		t.Errorf("calls = %q, want activity to keep the stored ref", got)
	}
}

// §2.4: introspection is expensive and DDL is rare, so schema does not run at
// the stats streams' cadence unless the operator asks for it.
func TestIntervalFor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target captureTarget
		stream string
		want   time.Duration
	}{
		{"a stats stream takes the node's base interval",
			captureTarget{Interval: 5 * time.Minute}, "activity", 5 * time.Minute},
		{"schema is floored above a short base interval",
			captureTarget{Interval: 5 * time.Minute}, "schema", 24 * time.Hour},
		{"a base interval longer than the floor wins",
			captureTarget{Interval: 48 * time.Hour}, "schema", 48 * time.Hour},
		{"the floor applies even with no base interval",
			captureTarget{}, "schema", 24 * time.Hour},
		{"an explicit override beats the floor in both directions",
			captureTarget{Interval: 5 * time.Minute,
				Intervals: map[string]time.Duration{"schema": time.Hour}}, "schema", time.Hour},
		{"an override on a stats stream is honoured",
			captureTarget{Interval: 5 * time.Minute,
				Intervals: map[string]time.Duration{"query": time.Hour}}, "query", time.Hour},
		{"an override for another stream does not leak",
			captureTarget{Interval: 5 * time.Minute,
				Intervals: map[string]time.Duration{"query": time.Hour}}, "activity", 5 * time.Minute},
		{"no interval anywhere leaves the stream ungated",
			captureTarget{}, "activity", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.target.intervalFor(tc.stream); got != tc.want {
				t.Errorf("intervalFor(%q) = %s, want %s", tc.stream, got, tc.want)
			}
		})
	}
}

// One node, two cadences: the stats streams tick while schema stays held back.
func TestDueStreams_PerStreamIntervals(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary", Interval: 5 * time.Minute}
	now := time.Now().UTC()

	// both captured an hour ago: past the 5m base, well inside the 24h floor
	for _, s := range []string{"schema", "activity"} {
		if err := store.MarkCaptureAttempt(ctx, key, target.Label, s, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	run, skipped, err := dueStreams(ctx, store, key, target, []string{"schema", "activity"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(run, ",") != "activity" {
		t.Errorf("run=%v, want only activity due", run)
	}
	if len(skipped) != 1 || !strings.HasPrefix(skipped[0], "schema") {
		t.Errorf("skipped=%v, want schema held back by the floor", skipped)
	}

	// an explicit override opts back in to the short cadence
	target.Intervals = map[string]time.Duration{"schema": time.Minute}
	run, _, err = dueStreams(ctx, store, key, target, []string{"schema", "activity"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(run, ",") != "schema,activity" {
		t.Errorf("run=%v, want the override to make schema due", run)
	}
}

// dueStreams stopped short-circuiting on a zero base interval so the schema
// floor could still apply. For a node with neither an interval nor a floored
// stream that has to stay a no-op: everything runs, nothing is reported
// skipped, and the store is never read.
func TestDueStreams_NoIntervalStaysUngated(t *testing.T) {
	ctx := context.Background()
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	target := captureTarget{Label: "primary"} // no Interval, no Intervals

	// a marker recent enough to hold back any nonzero interval
	if err := store.MarkCaptureAttempt(ctx, key, target.Label, "activity", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	run, skipped, err := dueStreams(ctx, store, key, target, []string{"activity", "query"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(run, ",") != "activity,query" {
		t.Errorf("run=%v, want every stream ungated", run)
	}
	if len(skipped) != 0 {
		t.Errorf("skipped=%v, want nothing reported skipped", skipped)
	}
}

// A project-scoped clock covers every node, so a node that captured nothing
// must not satisfy it -- under the 24h schema floor that would buy the whole
// fleet a day of no schema capture.
func TestCaptureStreams_UnavailableDoesNotSatisfyAProjectClock(t *testing.T) {
	ctx := context.Background()
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	store := testStoreAt(t, t.TempDir())
	stubStreams(t, func(stream, _ string) (int, string, error) {
		return 0, "", errStreamUnavailable
	})

	done, err := captureStreams(ctx, &stubCapturer{}, store, key,
		captureTarget{Label: "standby"}, []string{"schema", "query"}, "stored-hash", 0,
		captureRunOptions{})
	if err != nil {
		t.Fatalf("captureStreams: %v", err)
	}
	if strings.Join(done, " ") != "schema=n/a query=n/a" {
		t.Fatalf("done=%v, want both skipped", done)
	}

	// schema is project-scoped: this node proved nothing about the project
	if _, ok, err := store.LastCaptureAttemptAt(ctx, key, "", "schema"); err != nil || ok {
		t.Errorf("schema attempt recorded=%t err=%v, want an unavailable node to leave the project clock alone", ok, err)
	}
	// query is per-node: "this replica has no pg_stat_statements" is a fact
	// about this node, and must still hold its own clock back
	if _, ok, err := store.LastCaptureAttemptAt(ctx, key, "standby", "query"); err != nil || !ok {
		t.Errorf("query attempt recorded=%t err=%v, want the per-node clock still marked", ok, err)
	}
}

// §4.4: the schema-bearing node goes first so the nodes after it bind to the
// hash this run writes.
func TestSchemaNodesFirst(t *testing.T) {
	targets := []captureTarget{
		{Label: "analytics", Streams: []string{"activity"}},
		{Label: "primary", Streams: []string{"schema", "planner"}},
		{Label: "replica", Streams: []string{"query"}},
	}
	var got []string
	for _, tgt := range schemaNodesFirst(targets) {
		got = append(got, tgt.Label)
	}
	if strings.Join(got, ",") != "primary,analytics,replica" {
		t.Errorf("order = %v, want the schema-bearing node first", got)
	}

	// a node with no explicit streams gets the primary defaults, schema included
	auto := []captureTarget{
		{Label: "b-replica", Role: "standby"},
		{Label: "a-primary"},
	}
	got = nil
	for _, tgt := range schemaNodesFirst(auto) {
		got = append(got, tgt.Label)
	}
	if strings.Join(got, ",") != "a-primary,b-replica" {
		t.Errorf("order = %v, want the role-defaulted primary first", got)
	}

	// stable: two schema-bearing nodes keep their alphabetical order
	two := []captureTarget{
		{Label: "a", Streams: []string{"schema"}},
		{Label: "b", Streams: []string{"schema"}},
		{Label: "c", Streams: []string{"query"}},
	}
	got = nil
	for _, tgt := range schemaNodesFirst(two) {
		got = append(got, tgt.Label)
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Errorf("order = %v, want a stable sort", got)
	}
}
