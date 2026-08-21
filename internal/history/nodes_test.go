package history

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Seeds one activity row at an explicit time. The fixtures stamp time.Now(),
// but every ordering question in ListNodes turns on which row is newest, so the
// tests pin timestamps rather than racing the clock.
func putNodeActivityAt(t *testing.T, s *Store, key SnapshotKey, hash, label string, standby bool, ts time.Time) {
	t.Helper()
	a := activityFixture("sr", hash, label, standby)
	a.Node.Timestamp = ts
	if _, err := s.PutActivity(context.Background(), key, a); err != nil {
		t.Fatal(err)
	}
}

func putNodeQueryStatsAt(t *testing.T, s *Store, key SnapshotKey, hash, label string, ts time.Time) {
	t.Helper()
	q := queryStatsFixture("sr", hash, label)
	q.Node.Timestamp = ts
	if _, err := s.PutQueryStats(context.Background(), key, q); err != nil {
		t.Fatal(err)
	}
}

// Rows the Go fixtures cannot express: a standby query-stats row, a payload
// with no recorded role, an unparseable timestamp, malformed JSON. All four are
// reachable in a real history.db -- from older binaries, from a pull, or from a
// partial write -- and each one is a case ListNodes has to survive.
func insertRawActivity(t *testing.T, s *Store, key SnapshotKey, hash, label, ts, payload string) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(),
		`INSERT INTO activity_stats
		   (project_id, database_id, schema_ref_hash, content_hash, node_source, timestamp, payload_json)
		   VALUES (?, ?, 'sr', ?, ?, ?, ?)`,
		string(key.ProjectID), string(key.DatabaseID), hash, label, ts, payload); err != nil {
		t.Fatal(err)
	}
}

func nodePayload(standby bool, pgVersion string) string {
	return fmt.Sprintf(`{"node":{"is_standby":%t,"pg_version":%q}}`, standby, pgVersion)
}

func findNode(t *testing.T, nodes []NodeSummary, label string) NodeSummary {
	t.Helper()
	for _, n := range nodes {
		if n.Label == label {
			return n
		}
	}
	t.Fatalf("node %q not in inventory", label)
	return NodeSummary{}
}

// ListNodes backs `dryrun snapshot nodes`, the command an operator runs when
// history looks wrong. Two properties matter more than the formatting:
//
//   - it must not fail. A single malformed row -- bad timestamp, invalid JSON,
//     no recorded role -- has to degrade that one node and still print the
//     others, because the healthy nodes are what the operator came to see.
//   - it must not contradict the capture guard. The role it prints comes from
//     LatestNodeRole, the same function `snapshot activity` enforces with, so
//     "nodes says primary" and "capture refuses because it thinks standby" can
//     never both be true. TestListNodes_AgreesWithCaptureGuard pins that.
func TestListNodes(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	base := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	// a plain primary feeding both streams
	putNodeActivityAt(t, store, key, "a-primary-1", "primary", false, base)
	putNodeActivityAt(t, store, key, "a-primary-2", "primary", false, base.Add(time.Hour))
	putNodeQueryStatsAt(t, store, key, "q-primary-1", "primary", base.Add(2*time.Hour))
	// a standby feeding activity only, one row orphaned
	putNodeActivityAt(t, store, key, "a-replica-1", "replica", true, base)
	insertRawActivity(t, store, key, "a-replica-orphan", "replica",
		base.Add(time.Minute).Format(time.RFC3339), nodePayload(true, "PostgreSQL 16.4"))
	if _, err := store.db.ExecContext(ctx,
		`UPDATE activity_stats SET schema_ref_hash = '' WHERE content_hash = 'a-replica-orphan'`); err != nil {
		t.Fatal(err)
	}

	nodes, err := store.ListNodes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("one entry per label, sorted", func(t *testing.T) {
		if len(nodes) != 2 {
			t.Fatalf("got %d nodes, want 2: %+v", len(nodes), nodes)
		}
		if nodes[0].Label != "primary" || nodes[1].Label != "replica" {
			t.Errorf("labels %q,%q -- want primary,replica in sorted order",
				nodes[0].Label, nodes[1].Label)
		}
	})

	t.Run("streams and per-stream row counts", func(t *testing.T) {
		p := findNode(t, nodes, "primary")
		if got, want := fmt.Sprint(p.Streams), "[activity query]"; got != want {
			t.Errorf("streams %s, want %s", got, want)
		}
		if p.ActivityRows != 2 || p.QueryRows != 1 {
			t.Errorf("rows activity=%d query=%d, want 2 and 1", p.ActivityRows, p.QueryRows)
		}
		r := findNode(t, nodes, "replica")
		if got, want := fmt.Sprint(r.Streams), "[activity]"; got != want {
			t.Errorf("streams %s, want %s -- a node with no query rows must not claim the stream", got, want)
		}
	})

	// the query-stats row is the newest for this label, so it -- not the older
	// activity row -- decides what the node currently looks like
	t.Run("last capture spans both streams", func(t *testing.T) {
		p := findNode(t, nodes, "primary")
		if want := base.Add(2 * time.Hour); !p.LastCapture.Equal(want) {
			t.Errorf("last capture %s, want %s (the query-stats row)", p.LastCapture, want)
		}
	})

	t.Run("roles", func(t *testing.T) {
		if got := findNode(t, nodes, "primary").Role; got != NodeRolePrimary {
			t.Errorf("primary role %q, want %q", got, NodeRolePrimary)
		}
		if got := findNode(t, nodes, "replica").Role; got != NodeRoleStandby {
			t.Errorf("replica role %q, want %q", got, NodeRoleStandby)
		}
	})

	t.Run("orphan rows counted", func(t *testing.T) {
		r := findNode(t, nodes, "replica")
		if r.OrphanRows != 1 {
			t.Errorf("orphan rows %d, want 1", r.OrphanRows)
		}
		if p := findNode(t, nodes, "primary"); p.OrphanRows != 0 {
			t.Errorf("primary orphan rows %d, want 0", p.OrphanRows)
		}
	})

	t.Run("no flip reported for stable labels", func(t *testing.T) {
		for _, n := range nodes {
			if n.RoleFlipped {
				t.Errorf("%s reported a role flip it never had", n.Label)
			}
		}
	})
}

func TestListNodes_Empty(t *testing.T) {
	nodes, err := testStore(t).ListNodes(context.Background(), SnapshotKey{ProjectID: "p", DatabaseID: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Errorf("got %d nodes from an empty store, want none", len(nodes))
	}
}

// A label is scoped to its (project, database). Two projects using the label
// "primary" -- which is the default, so this is the common case -- must not
// pool their rows into one node.
func TestListNodes_ScopedToKey(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	mine := SnapshotKey{ProjectID: "mine", DatabaseID: "d"}
	theirs := SnapshotKey{ProjectID: "theirs", DatabaseID: "d"}
	ts := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	putNodeActivityAt(t, store, mine, "a-1", "primary", false, ts)
	putNodeActivityAt(t, store, theirs, "a-2", "primary", true, ts)
	putNodeActivityAt(t, store, theirs, "a-3", "extra", true, ts)

	nodes, err := store.ListNodes(ctx, mine)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want only this project's: %+v", len(nodes), nodes)
	}
	if nodes[0].ActivityRows != 1 {
		t.Errorf("activity rows %d, want 1 -- the other project's row leaked in", nodes[0].ActivityRows)
	}
	if nodes[0].Role != NodeRolePrimary {
		t.Errorf("role %q, want %q -- the other project's standby row decided the role",
			nodes[0].Role, NodeRolePrimary)
	}
}

// A label covering two roles is the failure the warning exists to surface:
// usually --label aimed at a rotating endpoint, so two machines' cumulative
// counters are appending into one series. It has to be caught whether the two
// roles landed in one stream or one each.
func TestListNodes_RoleFlipDetection(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	base := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	// both roles inside activity_stats
	putNodeActivityAt(t, store, key, "a-within-1", "within", true, base)
	putNodeActivityAt(t, store, key, "a-within-2", "within", false, base.Add(time.Hour))
	// one role per stream: activity says standby, query says primary (the
	// query-stats fixture is never a standby). Neither table alone shows a
	// flip, so a per-table check misses this entirely.
	putNodeActivityAt(t, store, key, "a-cross", "cross", true, base)
	putNodeQueryStatsAt(t, store, key, "q-cross", "cross", base.Add(time.Minute))
	// never flipped
	putNodeActivityAt(t, store, key, "a-steady", "steady", true, base)

	nodes, err := store.ListNodes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		label string
		want  bool
	}{
		{"within", true},
		{"cross", true},
		{"steady", false},
	} {
		if got := findNode(t, nodes, tc.label).RoleFlipped; got != tc.want {
			t.Errorf("%s: RoleFlipped=%t, want %t", tc.label, got, tc.want)
		}
	}
}

// `snapshot nodes` is run precisely when history is suspect, so a row that
// cannot be read must cost that one node, never the whole inventory. Before
// this behaviour existed, one bad timestamp made the command print nothing at
// all while `snapshot list` kept working.
func TestListNodes_SurvivesUnreadableRows(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	ts := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	putNodeActivityAt(t, store, key, "a-healthy", "healthy", false, ts)
	insertRawActivity(t, store, key, "a-badts", "badts", "not-a-timestamp", nodePayload(false, "PostgreSQL 17.0"))
	// json_extract aborts the whole statement on malformed JSON unless guarded
	insertRawActivity(t, store, key, "a-badjson", "badjson", ts.Format(time.RFC3339), `{"node":{"is_standby"`)
	// a payload with no role at all, as older captures wrote
	insertRawActivity(t, store, key, "a-noRole", "norole", ts.Format(time.RFC3339), `{"tables":[]}`)

	nodes, err := store.ListNodes(ctx, key)
	if err != nil {
		t.Fatalf("one unreadable row failed the whole inventory: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("got %d nodes, want all 4 listed: %+v", len(nodes), nodes)
	}

	t.Run("healthy node is unaffected", func(t *testing.T) {
		h := findNode(t, nodes, "healthy")
		if h.Role != NodeRolePrimary || !h.LastCapture.Equal(ts) {
			t.Errorf("healthy node degraded: role=%q last=%s", h.Role, h.LastCapture)
		}
	})

	// the timestamp is unreadable but the role is not, so the role still shows
	t.Run("bad timestamp zeroes last capture and is counted", func(t *testing.T) {
		b := findNode(t, nodes, "badts")
		if !b.LastCapture.IsZero() {
			t.Errorf("last capture %s, want zero", b.LastCapture)
		}
		if b.CorruptRows == 0 {
			t.Error("CorruptRows=0; the CLI prints no warning without it")
		}
		if b.Role != NodeRolePrimary {
			t.Errorf("role %q, want %q -- the role was readable", b.Role, NodeRolePrimary)
		}
	})

	t.Run("malformed json reads as unknown role", func(t *testing.T) {
		if got := findNode(t, nodes, "badjson").Role; got != NodeRoleUnknown {
			t.Errorf("role %q, want unknown", got)
		}
	})

	// unknown must never be reported as primary: the guard treats unknown as
	// "cannot check", and reporting primary would claim a check that never ran
	t.Run("payload without a role is unknown, not primary", func(t *testing.T) {
		if got := findNode(t, nodes, "norole").Role; got != NodeRoleUnknown {
			t.Errorf("role %q, want unknown", got)
		}
	})
}

// The invariant the whole design turns on. `snapshot nodes` and the capture
// guard answer the same question -- what role is this label -- and an operator
// who reads "primary" from the table, then has a capture refused because the
// guard decided "standby", has no way to reconcile the two.
//
// ListNodes therefore does not derive the role itself; it calls LatestNodeRole.
// This test is what stops a future change from reintroducing a second
// derivation: it seeds every awkward shape found while building the command,
// including a same-second tie and a role that differs between the two streams.
func TestListNodes_AgreesWithCaptureGuard(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	base := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	putNodeActivityAt(t, store, key, "a-plain", "plain", true, base)
	// query stats newer than activity, and disagreeing about the role
	putNodeActivityAt(t, store, key, "a-newer-q", "newerq", true, base)
	putNodeQueryStatsAt(t, store, key, "q-newer-q", "newerq", base.Add(time.Hour))
	// query stats only
	putNodeQueryStatsAt(t, store, key, "q-only", "queryonly", base)
	// two activity rows in the same second, disagreeing: only the id tiebreak
	// separates them, and both readers must break it the same way
	tie := base.Format(time.RFC3339)
	insertRawActivity(t, store, key, "a-tie-1", "tie", tie, nodePayload(true, "PostgreSQL 16.4"))
	insertRawActivity(t, store, key, "a-tie-2", "tie", tie, nodePayload(false, "PostgreSQL 16.4"))
	// unreadable shapes, where disagreement is likeliest
	insertRawActivity(t, store, key, "a-bad-ts", "badts", "not-a-timestamp", nodePayload(false, "PostgreSQL 17.0"))
	insertRawActivity(t, store, key, "a-bad-json", "badjson", tie, `{"node":{"is_standby"`)
	insertRawActivity(t, store, key, "a-no-role", "norole", tie, `{"tables":[]}`)

	nodes, err := store.ListNodes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) == 0 {
		t.Fatal("no nodes seeded")
	}
	for _, n := range nodes {
		guard, err := store.LatestNodeRole(ctx, key, n.Label)
		if err != nil {
			t.Fatalf("%s: %v", n.Label, err)
		}
		if guard != n.Role {
			t.Errorf("%s: snapshot nodes says %q, the capture guard says %q -- "+
				"these must never disagree", n.Label, n.Role, guard)
		}
	}
}

// rowTS (when we captured) and started (when that server booted) are separate
// facts: a returning member has an old start time on a new row.
func putNodeActivityFingerprintAt(t *testing.T, s *Store, key SnapshotKey, hash, label string, rowTS, started time.Time, addr string) {
	t.Helper()
	a := activityFixture("sr", hash, label, false)
	a.Node.Timestamp = rowTS
	a.Node.PostmasterStartTime = &started
	a.Node.ServerAddr = addr
	if _, err := s.PutActivity(context.Background(), key, a); err != nil {
		t.Fatal(err)
	}
}

// RecentNodeFingerprints backs the drift check. It answers "which servers has
// this label captured from lately", newest first, because oscillation (A->B->A)
// is what identifies a rotating endpoint -- a single change is just a restart.
func TestRecentNodeFingerprints(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	// relative to now: the window is computed from the clock, so fixed dates
	// would fall out of it and fail on a date rather than on a change
	base := time.Now().UTC().Truncate(time.Second).Add(-24 * time.Hour)

	t.Run("no rows yields nothing, not an error", func(t *testing.T) {
		seen, err := store.RecentNodeFingerprints(ctx, key, "never-seen")
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 0 {
			t.Errorf("got %d fingerprints, want none -- a first capture must not warn", len(seen))
		}
	})

	t.Run("newest first, with what was captured", func(t *testing.T) {
		putNodeActivityFingerprintAt(t, store, key, "fp-1", "node-a", base, base, "10.0.0.1")
		putNodeActivityFingerprintAt(t, store, key, "fp-2", "node-a", base.Add(time.Hour), base.Add(time.Hour), "10.0.0.2")
		seen, err := store.RecentNodeFingerprints(ctx, key, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 2 {
			t.Fatalf("got %d fingerprints, want 2", len(seen))
		}
		if !seen[0].StartedAt.Equal(base.Add(time.Hour)) || seen[0].ServerAddr != "10.0.0.2" {
			t.Errorf("newest is %s/%s, want the later row", seen[0].StartedAt, seen[0].ServerAddr)
		}
		if !seen[1].StartedAt.Equal(base) {
			t.Errorf("second is %s, want %s", seen[1].StartedAt, base)
		}
	})

	// the whole point: the window has to reach past the previous row, or
	// A->B->A is indistinguishable from a one-way change
	t.Run("window reaches past the previous row", func(t *testing.T) {
		// server A returns: a new capture carrying A's original start time
		putNodeActivityFingerprintAt(t, store, key, "fp-3", "node-a",
			base.Add(2*time.Hour), base, "10.0.0.1")
		seen, err := store.RecentNodeFingerprints(ctx, key, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) < 3 {
			t.Fatalf("got %d fingerprints, want the full history", len(seen))
		}
		var distinct int
		for i, f := range seen {
			if i == 0 || !f.StartedAt.Equal(seen[i-1].StartedAt) {
				distinct++
			}
		}
		if distinct < 3 {
			t.Errorf("alternation is not visible in the window: %+v", seen)
		}
	})

	// rows written before the fingerprint existed carry none; they must drop
	// out rather than appear as a distinct member
	t.Run("rows without a fingerprint are skipped", func(t *testing.T) {
		insertRawActivity(t, store, key, "fp-legacy", "node-legacy",
			base.Format(time.RFC3339), `{"node":{"source":"node-legacy","is_standby":false}}`)
		seen, err := store.RecentNodeFingerprints(ctx, key, "node-legacy")
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 0 {
			t.Errorf("got %d fingerprints from unfingerprinted rows", len(seen))
		}
	})

	// corruption is not evidence the node moved
	t.Run("unparseable start times are skipped, not fatal", func(t *testing.T) {
		insertRawActivity(t, store, key, "fp-bad", "node-bad", base.Format(time.RFC3339),
			`{"node":{"source":"node-bad","postmaster_start_time":"not-a-timestamp"}}`)
		putNodeActivityFingerprintAt(t, store, key, "fp-good", "node-bad", base.Add(time.Hour), base.Add(time.Hour), "10.0.0.9")
		seen, err := store.RecentNodeFingerprints(ctx, key, "node-bad")
		if err != nil {
			t.Fatalf("a corrupt value failed the read: %v", err)
		}
		if len(seen) != 1 || seen[0].ServerAddr != "10.0.0.9" {
			t.Errorf("got %+v, want only the readable row", seen)
		}
	})

	t.Run("scoped to its own label and project", func(t *testing.T) {
		putNodeActivityFingerprintAt(t, store, key, "fp-other", "node-b", base, base, "10.9.9.9")
		seen, _ := store.RecentNodeFingerprints(ctx, key, "node-a")
		for _, f := range seen {
			if f.ServerAddr == "10.9.9.9" {
				t.Error("another label's fingerprint leaked in")
			}
		}
		other := SnapshotKey{ProjectID: "elsewhere", DatabaseID: "d"}
		if s, _ := store.RecentNodeFingerprints(ctx, other, "node-a"); len(s) != 0 {
			t.Error("another project's fingerprints leaked in")
		}
	})

	t.Run("includes query_stats rows", func(t *testing.T) {
		q := queryStatsFixture("sr", "qfp-1", "node-q")
		q.Node.Timestamp = base
		q.Node.PostmasterStartTime = &base
		q.Node.ServerAddr = "10.0.0.7"
		if _, err := store.PutQueryStats(ctx, key, q); err != nil {
			t.Fatal(err)
		}
		seen, _ := store.RecentNodeFingerprints(ctx, key, "node-q")
		if len(seen) != 1 || seen[0].ServerAddr != "10.0.0.7" {
			t.Errorf("got %+v, want the query_stats fingerprint", seen)
		}
	})

	t.Run("rows older than the window are excluded", func(t *testing.T) {
		old := base.Add(-nodeFingerprintAge - 24*time.Hour)
		putNodeActivityFingerprintAt(t, store, key, "fp-ancient", "node-old", old, old, "10.0.0.5")
		if seen, _ := store.RecentNodeFingerprints(ctx, key, "node-old"); len(seen) != 0 {
			t.Errorf("got %d fingerprints from outside the window", len(seen))
		}
		putNodeActivityFingerprintAt(t, store, key, "fp-recent", "node-old", base, base, "10.0.0.6")
		if seen, _ := store.RecentNodeFingerprints(ctx, key, "node-old"); len(seen) != 1 {
			t.Errorf("got %d fingerprints, want only the in-window row", len(seen))
		}
	})

	// the two streams are one series: a query-stats row between two activity
	// rows must sort into place, or oscillation spanning both is invisible
	t.Run("streams interleave by timestamp", func(t *testing.T) {
		putNodeActivityFingerprintAt(t, store, key, "mix-1", "node-mix", base, base, "10.0.0.1")
		q := queryStatsFixture("sr", "mix-2", "node-mix")
		mid := base.Add(time.Hour)
		q.Node.Timestamp, q.Node.PostmasterStartTime, q.Node.ServerAddr = mid, &mid, "10.0.0.2"
		if _, err := store.PutQueryStats(ctx, key, q); err != nil {
			t.Fatal(err)
		}
		putNodeActivityFingerprintAt(t, store, key, "mix-3", "node-mix",
			base.Add(2*time.Hour), base, "10.0.0.1")

		seen, err := store.RecentNodeFingerprints(ctx, key, "node-mix")
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 3 {
			t.Fatalf("got %d fingerprints, want 3 across both streams", len(seen))
		}
		if !seen[1].StartedAt.Equal(mid) {
			t.Errorf("query-stats row did not sort between the activity rows: %+v", seen)
		}
	})

	t.Run("window is bounded", func(t *testing.T) {
		for i := 0; i < nodeFingerprintRows+5; i++ {
			at := base.Add(time.Duration(i) * time.Minute)
			putNodeActivityFingerprintAt(t, store, key, fmt.Sprintf("bulk-%d", i), "node-bulk", at, at, "10.0.0.1")
		}
		seen, err := store.RecentNodeFingerprints(ctx, key, "node-bulk")
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != nodeFingerprintRows {
			t.Errorf("window returned %d rows, want %d", len(seen), nodeFingerprintRows)
		}
	})
}

// `snapshot nodes` has to answer "is this label one machine?" without the user
// running a capture to find out. Members counts distinct servers; oscillation
// separates a rotating endpoint from an ordinary restart.
func TestSummariseMembers(t *testing.T) {
	a := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	b := a.Add(time.Hour)
	c := a.Add(2 * time.Hour)
	fp := func(at time.Time) NodeFingerprint { return NodeFingerprint{StartedAt: at} }

	cases := []struct {
		name        string
		seen        []NodeFingerprint
		wantMembers int
		wantOsc     bool
	}{
		{"nothing recorded", nil, 0, false},
		{"one server", []NodeFingerprint{fp(a), fp(a), fp(a)}, 1, false},
		{"restart: one after another", []NodeFingerprint{fp(b), fp(a)}, 2, false},
		{"replacement chain", []NodeFingerprint{fp(c), fp(b), fp(a)}, 3, false},
		{"rotation A-B-A", []NodeFingerprint{fp(a), fp(b), fp(a)}, 2, true},
		{"rotation with repeats", []NodeFingerprint{fp(a), fp(a), fp(b), fp(b), fp(a)}, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members, osc := summariseMembers(tc.seen)
			if members != tc.wantMembers {
				t.Errorf("members=%d, want %d", members, tc.wantMembers)
			}
			if osc != tc.wantOsc {
				t.Errorf("oscillating=%t, want %t", osc, tc.wantOsc)
			}
		})
	}
}

func TestListNodes_ReportsMembers(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	base := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	a, b := base, base.Add(time.Minute)

	// one label, two servers alternating: the rotating-endpoint case
	putNodeActivityFingerprintAt(t, store, key, "r-1", "rotating", base, a, "10.0.0.1")
	putNodeActivityFingerprintAt(t, store, key, "r-2", "rotating", base.Add(time.Hour), b, "10.0.0.2")
	putNodeActivityFingerprintAt(t, store, key, "r-3", "rotating", base.Add(2*time.Hour), a, "10.0.0.1")
	// one label, one server
	putNodeActivityFingerprintAt(t, store, key, "s-1", "steady", base, a, "10.0.0.3")
	// a label with no fingerprints at all
	insertRawActivity(t, store, key, "legacy-1", "unknown-node",
		base.Format(time.RFC3339), `{"node":{"source":"unknown-node","is_standby":false}}`)

	nodes, err := store.ListNodes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	rotating := findNode(t, nodes, "rotating")
	if rotating.Members != 2 || !rotating.Oscillating {
		t.Errorf("rotating: members=%d osc=%t, want 2 and true", rotating.Members, rotating.Oscillating)
	}
	steady := findNode(t, nodes, "steady")
	if steady.Members != 1 || steady.Oscillating {
		t.Errorf("steady: members=%d osc=%t, want 1 and false", steady.Members, steady.Oscillating)
	}
	// one sighting is not evidence of stability; the CLI renders it as such
	if steady.Fingerprinted != 1 {
		t.Errorf("steady: fingerprinted=%d, want 1", steady.Fingerprinted)
	}
	// no fingerprints must read as unknown, never as "one server"
	if u := findNode(t, nodes, "unknown-node"); u.Fingerprinted != 0 || u.Members != 0 {
		t.Errorf("unfingerprinted node reports members=%d from %d rows", u.Members, u.Fingerprinted)
	}
}

// The per-arm LIMIT once ran before the fingerprint filter, so a label whose
// recent rows came from a pre-fingerprint binary burned the whole window and
// reported no members at all -- which reads as "no rotation".
func TestRecentNodeFingerprints_UnfingerprintedRowsDoNotEatTheWindow(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	started := base.Add(-time.Hour)
	putNodeActivityFingerprintAt(t, store, key, "fp-old", "node", base, started, "10.0.0.1")
	// more recent rows than the window holds, none of them fingerprinted
	for i := 0; i < nodeFingerprintRows+5; i++ {
		insertRawActivity(t, store, key, fmt.Sprintf("legacy-%d", i), "node",
			base.Add(time.Duration(i+1)*time.Minute).Format(time.RFC3339),
			`{"node":{"source":"node","is_standby":false}}`)
	}

	seen, err := store.RecentNodeFingerprints(ctx, key, "node")
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d fingerprints, want the one that exists behind the unfingerprinted rows", len(seen))
	}
	if seen[0].ServerAddr != "10.0.0.1" {
		t.Errorf("got %+v", seen[0])
	}
}
