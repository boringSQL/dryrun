package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// A stateless writer pushes to an empty directory and uploads it, so every run
// adds an object even when the hash is known. Payload timestamps carry
// microseconds, as now() does; the db column has seconds.
func publishSchema(t *testing.T, remoteDir string, k history.SnapshotKey, snap *schema.SchemaSnapshot) {
	t.Helper()
	dir := t.TempDir()
	store, err := history.NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), k, history.WrapSchema(snap)); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(remoteDir, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
}

func schemaWithTables(hash string, ts time.Time, tables ...string) *schema.SchemaSnapshot {
	s := syncTestSchema(hash, "appdb", ts)
	s.FormatVersion = schema.FormatVersion
	for _, name := range tables {
		s.Tables = append(s.Tables, schema.Table{Schema: "public", Name: name})
	}
	return s
}

func newRemote(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

// a store reads its bundles once, as each `dryrun snapshot pull` opens its own;
// a test that publishes behind a store's back pulls through a fresh one
func reopenRemote(t *testing.T, dir string) history.SnapshotStore {
	t.Helper()
	store, err := history.NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func pullOnce(t *testing.T, remote, reader history.SnapshotStore, k history.SnapshotKey, scope pullScope) SyncOutcome {
	t.Helper()
	outs, err := syncKeys(context.Background(), remote, reader, []history.SnapshotKey{k}, scope)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	return outs[0]
}

func readerLatest(t *testing.T, s *history.Store, k history.SnapshotKey) *schema.SchemaSnapshot {
	t.Helper()
	snap, err := s.GetSchema(context.Background(), k, history.NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func schemaRows(t *testing.T, s *history.Store, k history.SnapshotKey) []history.SnapshotSummary {
	t.Helper()
	list, err := s.List(context.Background(), k, history.SchemaKind(), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func hasTable(s *schema.SchemaSnapshot, name string) bool {
	for _, tb := range s.Tables {
		if tb.Name == name {
			return true
		}
	}
	return false
}

func baseT0() time.Time {
	return time.Now().UTC().Truncate(time.Second).Add(-10*time.Hour + 700*time.Microsecond)
}

func TestPullFollowsARevertedMigration(t *testing.T) {
	for name, scope := range map[string]pullScope{"latest": {latest: true}, "full": {}} {
		t.Run(name, func(t *testing.T) {
			k := syncKey("acme", "primary")
			t0 := baseT0()
			dir := newRemote(t)
			remote := func() history.SnapshotStore { return reopenRemote(t, dir) }
			reader := openPoolStore(t)

			publishSchema(t, dir, k, schemaWithTables("hash-a", t0))
			pullOnce(t, remote(), reader, k, scope)
			publishSchema(t, dir, k, schemaWithTables("hash-b", t0.Add(time.Hour), "added"))
			pullOnce(t, remote(), reader, k, scope)
			publishSchema(t, dir, k, schemaWithTables("hash-a", t0.Add(2*time.Hour)))
			out := pullOnce(t, remote(), reader, k, scope)

			if got := readerLatest(t, reader, k); got.ContentHash != "hash-a" || hasTable(got, "added") {
				t.Errorf("reader's latest = %s, want hash-a without the reverted table", got.ContentHash)
			}
			if out.Schema.Copied != 1 {
				t.Errorf("outcome = %+v, want 1 copied", out.Schema)
			}
		})
	}
}

// Digest v3 omits partition children, so this change has the same hash.
func TestPullRefreshesPayloadOfAnUnchangedHash(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := baseT0()
	dir := newRemote(t)
	remote := func() history.SnapshotStore { return reopenRemote(t, dir) }
	reader := openPoolStore(t)

	publishSchema(t, dir, k, schemaWithTables("hash-a", t0))
	pullOnce(t, remote(), reader, k, pullScope{latest: true})
	firstColumn := schemaRows(t, reader, k)[0].Timestamp

	publishSchema(t, dir, k, schemaWithTables("hash-a", t0.Add(time.Hour), "ev_2027"))
	out := pullOnce(t, remote(), reader, k, pullScope{latest: true})

	got := readerLatest(t, reader, k)
	if !hasTable(got, "ev_2027") {
		t.Error("reader kept the old payload")
	}
	if !got.Timestamp.Equal(t0.Add(time.Hour)) {
		t.Errorf("payload timestamp = %s, want the newer capture", got.Timestamp)
	}
	rows := schemaRows(t, reader, k)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want the row updated in place", len(rows))
	}
	if !rows[0].Timestamp.Equal(firstColumn) {
		t.Errorf("column moved to %s; it records when this version appeared", rows[0].Timestamp)
	}
	if out.Schema.Copied != 1 {
		t.Errorf("outcome = %+v, want 1 copied", out.Schema)
	}
}

// The column stores seconds; the payload carries microseconds.
func TestRepeatPullOfOneBundleWritesNothing(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := baseT0()
	dir := newRemote(t)
	remote := func() history.SnapshotStore { return reopenRemote(t, dir) }
	reader := openPoolStore(t)

	publishSchema(t, dir, k, schemaWithTables("hash-a", t0))
	pullOnce(t, remote(), reader, k, pullScope{latest: true})
	publishSchema(t, dir, k, schemaWithTables("hash-a", t0.Add(time.Hour)))
	pullOnce(t, remote(), reader, k, pullScope{latest: true})

	for range 2 {
		out := pullOnce(t, remote(), reader, k, pullScope{latest: true})
		if out.Schema.Copied != 0 || out.Schema.UpToDate != 1 {
			t.Errorf("repeat pull = %+v, want 1 up-to-date", out.Schema)
		}
	}
	if rows := schemaRows(t, reader, k); len(rows) != 1 {
		t.Errorf("got %d rows, want 1", len(rows))
	}
}

func TestPullFromAnOlderSourceLeavesTheReaderAlone(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := baseT0()
	dir := newRemote(t)
	reader := openPoolStore(t)

	publishSchema(t, dir, k, schemaWithTables("hash-a", t0, "old"))
	if _, err := reader.Put(context.Background(), k, history.WrapSchema(schemaWithTables("hash-a", t0.Add(time.Hour), "new"))); err != nil {
		t.Fatal(err)
	}
	pullOnce(t, reopenRemote(t, dir), reader, k, pullScope{latest: true})

	got := readerLatest(t, reader, k)
	if !hasTable(got, "new") || hasTable(got, "old") {
		t.Error("an older bundle replaced a newer payload")
	}
}

func TestPullKeepsPayloadFromANewerFormat(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := baseT0()
	reader := openPoolStore(t)
	ctx := context.Background()

	stored := schemaWithTables("hash-a", t0, "stored")
	if _, err := reader.Put(ctx, k, history.WrapSchema(stored)); err != nil {
		t.Fatal(err)
	}
	older := schemaWithTables("hash-a", t0.Add(time.Hour), "older_format")
	older.FormatVersion = stored.FormatVersion - 1
	out, err := reader.Put(ctx, k, history.WrapSchema(older))
	if err != nil {
		t.Fatal(err)
	}
	if out != history.PutDeduped {
		t.Error("a lower format version must not count as a write")
	}
	if got := readerLatest(t, reader, k); hasTable(got, "older_format") {
		t.Error("a lower format version overwrote the payload")
	}
}

// A store that also captures: a pull must not move --due's clock.
func TestPullRefreshLeavesALocalCapturesClockAlone(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	s := openPoolStore(t)

	if _, err := s.PutSchema(ctx, k, schemaWithTables("hash-a", t0)); err != nil {
		t.Fatal(err)
	}
	before, ok, err := s.LastCaptureAt(ctx, k, "", "schema")
	if err != nil || !ok {
		t.Fatalf("LastCaptureAt: %v, %v", ok, err)
	}
	if _, err := s.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-a", t0.Add(time.Hour), "pulled"))); err != nil {
		t.Fatal(err)
	}
	if !hasTable(readerLatest(t, s, k), "pulled") {
		t.Error("payload not refreshed")
	}
	after, ok, err := s.LastCaptureAt(ctx, k, "", "schema")
	if err != nil || !ok || !after.Equal(before) {
		t.Errorf("--due clock moved from %s to %s (%v, %v)", before, after, ok, err)
	}
}

func TestLocalCaptureOfAnUnchangedHashStillDedups(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	s := openPoolStore(t)

	if _, err := s.PutSchema(ctx, k, schemaWithTables("hash-a", t0)); err != nil {
		t.Fatal(err)
	}
	out, err := s.PutSchema(ctx, k, schemaWithTables("hash-a", t0.Add(time.Hour), "second"))
	if err != nil {
		t.Fatal(err)
	}
	if out != history.PutDeduped || hasTable(readerLatest(t, s, k), "second") {
		t.Error("local capture behaviour changed")
	}
}

func TestLatestPullSelectsANewerSchemaWithNoStats(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	dir := newRemote(t)
	remote := func() history.SnapshotStore { return reopenRemote(t, dir) }
	reader := openPoolStore(t)

	publishSchema(t, dir, k, schemaWithTables("hash-a", t0))
	if _, err := remote().Put(ctx, k, history.WrapActivity(syncTestActivity("hash-a", "act-1", "primary", t0.Add(time.Minute), false))); err != nil {
		t.Fatal(err)
	}
	publishSchema(t, dir, k, schemaWithTables("hash-b", t0.Add(time.Hour)))

	pullOnce(t, remote(), reader, k, pullScope{latest: true})
	if got := readerLatest(t, reader, k); got.ContentHash != "hash-b" {
		t.Errorf("latest = %s, want the newer schema", got.ContentHash)
	}
	acts, err := reader.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(acts) != 1 {
		t.Errorf("got %d activity rows, want the stats kept", len(acts))
	}
}

func TestPushToALongLivedRemoteIsUnchanged(t *testing.T) {
	k := syncKey("acme", "primary")
	t0 := baseT0()
	remote := reopenRemote(t, newRemote(t))
	writer := openPoolStore(t)
	ctx := context.Background()

	if _, err := writer.PutSchema(ctx, k, schemaWithTables("hash-a", t0)); err != nil {
		t.Fatal(err)
	}
	pullOnce(t, writer, remote, k, pullScope{latest: true})
	out := pullOnce(t, writer, remote, k, pullScope{latest: true})
	if out.Schema.Copied != 0 || out.Schema.UpToDate != 1 {
		t.Errorf("second push = %+v, want 1 up-to-date", out.Schema)
	}
}

// Refreshing a payload must not disturb the log: stats keep their version.
func TestSnapshotLogAfterARefresh(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	dir := newRemote(t)
	remote := func() history.SnapshotStore { return reopenRemote(t, dir) }
	reader := openPoolStore(t)

	publishSchema(t, dir, k, schemaWithTables("hash-a", t0))
	if _, err := remote().Put(ctx, k, history.WrapActivity(syncTestActivity("hash-a", "act-1", "primary", t0.Add(time.Minute), false))); err != nil {
		t.Fatal(err)
	}
	pullOnce(t, remote(), reader, k, pullScope{latest: true})
	publishSchema(t, dir, k, schemaWithTables("hash-a", t0.Add(5*time.Hour), "ev_2027"))
	pullOnce(t, remote(), reader, k, pullScope{latest: true})

	schemas := schemaRows(t, reader, k)
	stats, err := reader.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	perVersion, orphans := assignVersions(schemas, stats)
	if len(orphans) != 0 || len(perVersion[0]) != 1 {
		t.Errorf("orphans = %d, version 0 has %d; want the capture kept on its version", len(orphans), len(perVersion[0]))
	}
	if want := t0.Truncate(time.Second); !schemas[0].Timestamp.Equal(want) {
		t.Errorf("column = %s, want %s: Lived and Date read it", schemas[0].Timestamp, want)
	}
}

// After A -> B -> A with a refreshed second A, each A diffs as itself.
func TestSnapshotLogDiffsEachTwinAgainstItsOwnPayload(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	s := openPoolStore(t)

	for _, snap := range []*schema.SchemaSnapshot{
		schemaWithTables("hash-a", t0),
		schemaWithTables("hash-b", t0.Add(time.Hour), "only_in_b"),
		schemaWithTables("hash-a", t0.Add(2*time.Hour)),
		schemaWithTables("hash-a", t0.Add(3*time.Hour), "new_child"),
	} {
		if _, err := s.Put(ctx, k, history.WrapSchema(snap)); err != nil {
			t.Fatal(err)
		}
	}
	rows := schemaRows(t, s, k)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want a b a", len(rows))
	}
	versions := make([]schemaVersion, len(rows))
	for i, r := range rows {
		versions[i] = schemaVersion{Row: r, Ref: i, Oldest: i == len(rows)-1}
	}
	diffVersions(ctx, s, k, rows, versions)
	if versions[1].DiffErr != nil {
		t.Fatal(versions[1].DiffErr)
	}
	// hash-b's parent is the first A, which never had new_child
	if d := fmt.Sprintf("%+v", versions[1].Delta); strings.Contains(d, "new_child") {
		t.Errorf("hash-b's diff mentions a table only the newest A has: %s", d)
	}
	if d := fmt.Sprintf("%+v", versions[1].Delta); !strings.Contains(d, "only_in_b") {
		t.Errorf("hash-b's diff lost its own table: %s", d)
	}
}

func TestPullWarnings(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	k := syncKey("focus", "focus_production")
	cases := []struct {
		name string
		outs []SyncOutcome
		all  bool
		want string
	}{
		{"missing key", []SyncOutcome{{Key: k}}, false, "source has no snapshots for focus/focus_production"},
		{"--all with no keys", nil, true, "source has no snapshots"},
		{"stale", []SyncOutcome{{Key: k, SourceNewest: now.Add(-72 * time.Hour)}}, false, "captured 3d ago"},
		{"just past 48h", []SyncOutcome{{Key: k, SourceNewest: now.Add(-48*time.Hour - time.Minute)}}, false, "the capture job may have stopped"},
		{"at 48h", []SyncOutcome{{Key: k, SourceNewest: now.Add(-48 * time.Hour)}}, false, ""},
		{"fresh", []SyncOutcome{{Key: k, SourceNewest: now.Add(-time.Hour)}}, false, ""},
		{"--all skips a key with nothing", []SyncOutcome{{Key: k}}, true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			warnPullSource(&buf, c.outs, c.all, now)
			got := buf.String()
			if c.want == "" && got != "" {
				t.Errorf("want silence, got %q", got)
			}
			if c.want != "" && (!strings.Contains(got, c.want) || !strings.HasPrefix(got, "warning: ")) {
				t.Errorf("got %q, want a warning containing %q", got, c.want)
			}
		})
	}
}

// A long-lived writer's schema timestamp is its last DDL change; fresh stats
// under it mean the job is healthy.
func TestSourceNewestIncludesStats(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	remote := reopenRemote(t, newRemote(t))
	reader := openPoolStore(t)

	statsAt := t0.Add(9 * time.Hour)
	if _, err := remote.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-a", t0.Add(-30*24*time.Hour)))); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Put(ctx, k, history.WrapActivity(syncTestActivity("hash-a", "act-1", "primary", statsAt, false))); err != nil {
		t.Fatal(err)
	}
	out := pullOnce(t, remote, reader, k, pullScope{latest: true})
	if !out.SourceNewest.Equal(statsAt) {
		t.Errorf("SourceNewest = %s, want the activity row's %s", out.SourceNewest, statsAt)
	}
}

type getCountingStore struct {
	history.SnapshotStore
	gets int
}

func (c *getCountingStore) Get(ctx context.Context, key history.SnapshotKey, kind history.SnapshotKind, at history.SnapshotRef) (history.StoredSnapshot, error) {
	c.gets++
	return c.SnapshotStore.Get(ctx, key, kind, at)
}

// On a registry a Get lists every bundle. Microseconds in the summary against
// seconds in the column must not make an unchanged bundle look newer.
func TestRepeatPullOfTheOnlyBundleFetchesNothing(t *testing.T) {
	k := syncKey("acme", "primary")
	dir := newRemote(t)
	reader := openPoolStore(t)

	publishSchema(t, dir, k, schemaWithTables("hash-a", baseT0()))
	pullOnce(t, reopenRemote(t, dir), reader, k, pullScope{latest: true})

	c := &getCountingStore{SnapshotStore: reopenRemote(t, dir)}
	pullOnce(t, c, reader, k, pullScope{latest: true})
	if c.gets != 0 {
		t.Errorf("repeat pull made %d Get calls, want 0", c.gets)
	}
}

// Through the real command, so the warning can't be left unwired.
func TestPullCommandWarnsOnStaleAndMissingSource(t *testing.T) {
	k := syncKey("acme", "primary")
	pull := func(t *testing.T, srcDir string) string {
		t.Helper()
		resetFlags(t)
		withCWD(t, writeTOML(t, t.TempDir(), "[project]\nid = \"acme\"\n\n[default]\nprofile = \"p\"\n\n[profiles.p]\ndatabase_id = \"primary\"\n"))
		return captureStderr(t, func() {
			cmd := snapshotPullCmd()
			cmd.SetArgs([]string{"--from-path", srcDir, "--history-db", t.TempDir() + "/history.db"})
			if err := cmd.Execute(); err != nil {
				t.Errorf("pull: %v", err)
			}
		})
	}

	t.Run("missing", func(t *testing.T) {
		if out := pull(t, t.TempDir()); !strings.Contains(out, "source has no snapshots for acme/primary") {
			t.Errorf("stderr = %q", out)
		}
	})
	t.Run("stale", func(t *testing.T) {
		dir := newRemote(t)
		publishSchema(t, dir, k, schemaWithTables("hash-a", time.Now().UTC().Add(-72*time.Hour)))
		if out := pull(t, dir); !strings.Contains(out, "the capture job may have stopped") {
			t.Errorf("stderr = %q", out)
		}
	})
	t.Run("fresh", func(t *testing.T) {
		dir := newRemote(t)
		publishSchema(t, dir, k, schemaWithTables("hash-a", time.Now().UTC().Add(-time.Hour)))
		if out := pull(t, dir); strings.Contains(out, "warning:") {
			t.Errorf("stderr = %q", out)
		}
	})
}

// Only the newest source row may refresh: refreshing an older known hash would
// add a twin.
func TestPullRefreshesOnlyTheNewestRow(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	dir := newRemote(t)
	reader := openPoolStore(t)

	for _, snap := range []*schema.SchemaSnapshot{schemaWithTables("hash-a", t0), schemaWithTables("hash-c", t0.Add(time.Hour))} {
		if _, err := reader.Put(ctx, k, history.WrapSchema(snap)); err != nil {
			t.Fatal(err)
		}
	}
	publishSchema(t, dir, k, schemaWithTables("hash-a", t0.Add(2*time.Hour)))
	publishSchema(t, dir, k, schemaWithTables("hash-b", t0.Add(3*time.Hour)))
	pullOnce(t, reopenRemote(t, dir), reader, k, pullScope{})

	rows := schemaRows(t, reader, k)
	if len(rows) != 3 || rows[0].ContentHash != "hash-b" {
		t.Errorf("got %d rows, newest %s; want a, c, b", len(rows), rows[0].ContentHash)
	}
}

// A long-lived remote dedups the reverted A, so its newest schema row is B while
// fresh stats merge into A. B is not current: stats newer than it are bound elsewhere.
func TestFreshPullFromALongLivedRemoteAfterARevertLandsOnA(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	remote := reopenRemote(t, newRemote(t))
	reader := openPoolStore(t)

	for _, snap := range []history.StoredSnapshot{
		history.WrapSchema(schemaWithTables("hash-a", t0)),
		history.WrapSchema(schemaWithTables("hash-b", t0.Add(time.Hour), "added")),
		history.WrapSchema(schemaWithTables("hash-a", t0.Add(2*time.Hour))),
		history.WrapActivity(syncTestActivity("hash-a", "act-after-revert", "primary", t0.Add(2*time.Hour), false)),
	} {
		if _, err := remote.Put(ctx, k, snap); err != nil {
			t.Fatal(err)
		}
	}
	pullOnce(t, remote, reader, k, pullScope{latest: true})
	if got := readerLatest(t, reader, k); got.ContentHash != "hash-a" {
		t.Errorf("fresh reader's latest = %s, want hash-a", got.ContentHash)
	}
}

// A window says nothing about the source: an empty one isn't a missing key, and
// a short one isn't a dead job.
func TestPullSinceStaysQuiet(t *testing.T) {
	resetFlags(t)
	k := syncKey("acme", "primary")
	dir := newRemote(t)
	publishSchema(t, dir, k, schemaWithTables("hash-a", time.Now().UTC().Add(-30*24*time.Hour)))
	withCWD(t, writeTOML(t, t.TempDir(), "[project]\nid = \"acme\"\n\n[default]\nprofile = \"p\"\n\n[profiles.p]\ndatabase_id = \"primary\"\n"))
	for _, args := range [][]string{{"--since", "1h"}, {"--full", "--since", "1h"}} {
		out := captureStderr(t, func() {
			cmd := snapshotPullCmd()
			cmd.SetArgs(append([]string{"--from-path", dir, "--history-db", t.TempDir() + "/history.db"}, args...))
			if err := cmd.Execute(); err != nil {
				t.Errorf("pull: %v", err)
			}
		})
		if strings.Contains(out, "warning:") {
			t.Errorf("%v warned: %q", args, out)
		}
	}
}

// Same DDL from another cluster is not a newer capture of this one: refreshing
// would trip the identity guard on the next local capture.
func TestPullDoesNotRefreshAcrossClusters(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	reader := openPoolStore(t)

	stored := schemaWithTables("hash-a", t0, "ours")
	stored.SystemIdentifier = "111"
	if _, err := reader.Put(ctx, k, history.WrapSchema(stored)); err != nil {
		t.Fatal(err)
	}
	other := schemaWithTables("hash-a", t0.Add(time.Hour), "theirs")
	other.SystemIdentifier = "222"
	out, err := reader.Put(ctx, k, history.WrapSchema(other))
	if err != nil {
		t.Fatal(err)
	}
	if out != history.PutDeduped || hasTable(readerLatest(t, reader, k), "theirs") {
		t.Error("another cluster's payload replaced this one's")
	}
}

// A corrupt latest row must not block a refresh.
func TestPullToleratesAnOddFormatVersionInTheLatestRow(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	path := t.TempDir() + "/history.db"
	s, err := history.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-a", t0))); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`UPDATE snapshots SET snapshot_json = json_set(snapshot_json, '$.format_version', 'x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-a", t0.Add(time.Hour), "healed"))); err != nil {
		t.Fatalf("same hash over the damaged row: %v", err)
	}
	if !hasTable(readerLatest(t, s, k), "healed") {
		t.Error("the damaged row's payload was not refreshed")
	}
	if _, err := s.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-b", t0.Add(2*time.Hour)))); err != nil {
		t.Errorf("a new hash must still land: %v", err)
	}
}

// Two schemas in one second: the refresh gate must pick the same row
// newestPerNode would, not whatever the source's listing order left last.
func TestPullRefreshesTheLargerHashOfEqualSecondRows(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	t1 := t0.Add(time.Hour)
	src := openPoolStore(t)
	reader := openPoolStore(t)

	for _, snap := range []*schema.SchemaSnapshot{schemaWithTables("hash-a", t0), schemaWithTables("hash-b", t0)} {
		if _, err := reader.Put(ctx, k, history.WrapSchema(snap)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-a", t1))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-b", t1, "b_new"))); err != nil {
		t.Fatal(err)
	}
	pullOnce(t, src, reader, k, pullScope{})

	rows := schemaRows(t, reader, k)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: refreshing the wrong row adds a twin", len(rows))
	}
	if !hasTable(readerLatest(t, reader, k), "b_new") {
		t.Error("hash-b's payload was not refreshed")
	}
}

// Stats bound to a schema the source no longer holds must not veto the newest
// schema: only a row the source still has can be the reverted one.
func TestOrphanStatsDoNotVetoTheNewestSchema(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	src := openPoolStore(t)
	reader := openPoolStore(t)

	if _, err := src.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-old", t0))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Put(ctx, k, history.WrapSchema(schemaWithTables("hash-new", t0.Add(time.Hour)))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Put(ctx, k, history.WrapActivity(syncTestActivity("hash-gone", "act-1", "primary", t0.Add(2*time.Hour), false))); err != nil {
		t.Fatal(err)
	}
	pullOnce(t, src, reader, k, pullScope{latest: true})
	if got := readerLatest(t, reader, k); got.ContentHash != "hash-new" {
		t.Errorf("latest = %s, want hash-new: orphan stats are not a revert", got.ContentHash)
	}
}

// A stateless writer leaves one bundle per run, so an unchanged schema has a
// twin per capture. A fresh reader must date the version by its first twin, or
// the earlier captures come out orphaned and Lived shrinks to the last run.
func TestFreshPullDatesAVersionByItsFirstTwin(t *testing.T) {
	for name, scope := range map[string]pullScope{"latest": {latest: true}, "full": {}} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			k := syncKey("acme", "primary")
			t0 := baseT0()
			dir := newRemote(t)
			reader := openPoolStore(t)

			type run struct {
				hash   string
				at     time.Time
				tables []string
			}
			runs := []run{
				{"hash-a", t0, nil},
				{"hash-a", t0.Add(time.Hour), nil},
				{"hash-a", t0.Add(2 * time.Hour), []string{"ev_2027"}},
			}
			if !scope.latest {
				runs = append(runs, run{"hash-b", t0.Add(3 * time.Hour), []string{"added"}})
			}
			for i, r := range runs {
				publishSchema(t, dir, k, schemaWithTables(r.hash, r.at, r.tables...))
				act := syncTestActivity(r.hash, fmt.Sprintf("act-%d", i), "primary", r.at.Add(time.Minute), false)
				if _, err := reopenRemote(t, dir).Put(ctx, k, history.WrapActivity(act)); err != nil {
					t.Fatal(err)
				}
			}
			pullOnce(t, reopenRemote(t, dir), reader, k, scope)

			schemas := schemaRows(t, reader, k)
			a := schemas[len(schemas)-1]
			if a.ContentHash != "hash-a" || !a.Timestamp.Equal(t0.Truncate(time.Second)) {
				t.Errorf("hash-a dated %s, want its first twin's %s", a.Timestamp, t0.Truncate(time.Second))
			}
			got, err := reader.GetSchemaByExactHash(ctx, k, "hash-a")
			if err != nil {
				t.Fatal(err)
			}
			if !hasTable(got, "ev_2027") || !got.Timestamp.Equal(t0.Add(2*time.Hour)) {
				t.Errorf("hash-a payload from %s, want the newest twin's", got.Timestamp)
			}

			stats, err := reader.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
			if err != nil {
				t.Fatal(err)
			}
			if _, orphans := assignVersions(schemas, stats); len(orphans) != 0 {
				t.Errorf("%d of %d captures orphaned", len(orphans), len(stats))
			}
		})
	}
}

// A bundle store keeps one bundle per hash; feeding it the first twin would
// date a reverted schema before the version it replaced.
func TestPushAfterARevertKeepsTheRevertedSchemaNewest(t *testing.T) {
	ctx := context.Background()
	k := syncKey("acme", "primary")
	t0 := baseT0()
	writer := openPoolStore(t)
	remote := reopenRemote(t, newRemote(t))

	for _, snap := range []*schema.SchemaSnapshot{
		schemaWithTables("hash-a", t0),
		schemaWithTables("hash-b", t0.Add(time.Hour), "added"),
		schemaWithTables("hash-a", t0.Add(2*time.Hour)),
	} {
		if _, err := writer.PutSchema(ctx, k, snap); err != nil {
			t.Fatal(err)
		}
	}
	pullOnce(t, writer, remote, k, pullScope{})

	latest, err := remote.Get(ctx, k, history.SchemaKind(), history.NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if got := latest.AsSchema().ContentHash; got != "hash-a" {
		t.Errorf("remote's newest schema = %s, want hash-a", got)
	}
}
