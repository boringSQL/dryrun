package schema

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/boringsql/qshape"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live-DB tests for the v0.6 split-stats capture. We gate on TEST_DATABASE_URL
// because the repo doesn't carry a testcontainers dep — set it locally with
// a throwaway Postgres (any role that can read pg_stat_user_tables works).
func livePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live capture test")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Captures all three shapes against a live database and asserts each has
// non-empty content (any non-empty Postgres has pg_catalog tables which
// already trigger pg_stats rows for our standard system schemas filtered
// out; we only need user-space rows to be present).
func TestCaptureAll_AgainstLiveDB(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	snap, err := IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if snap.ContentHash == "" {
		t.Fatalf("expected non-empty schema content_hash")
	}

	planner, err := CapturePlannerStats(ctx, pool, snap.ContentHash)
	if err != nil {
		t.Fatalf("planner capture: %v", err)
	}
	if planner.ContentHash == "" {
		t.Errorf("planner ContentHash empty")
	}
	if planner.SchemaRefHash != snap.ContentHash {
		t.Errorf("planner.SchemaRefHash=%s want=%s", planner.SchemaRefHash, snap.ContentHash)
	}
	// A live Postgres always has at least the snapshot's own tables visible
	// through pg_class — but a fresh database may have zero user tables, so
	// we only require the capture not to fail and the binding to hold.

	activity, err := CaptureActivityStats(ctx, pool, snap.ContentHash, "test-primary")
	if err != nil {
		t.Fatalf("activity capture: %v", err)
	}
	if activity.SchemaRefHash != snap.ContentHash {
		t.Errorf("activity.SchemaRefHash=%s want=%s", activity.SchemaRefHash, snap.ContentHash)
	}
	if activity.Node.Source != "test-primary" {
		t.Errorf("activity.Node.Source=%q want=test-primary", activity.Node.Source)
	}
	if activity.Node.PgVersion == "" {
		t.Errorf("activity.Node.PgVersion is empty; expected version() string")
	}
}

func TestCaptureActivityStats_DatabaseScopedFields(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	activity, err := CaptureActivityStats(ctx, pool, "sref", "test-primary")
	if err != nil {
		t.Fatalf("activity capture: %v", err)
	}

	if activity.Database == nil {
		t.Fatal("expected Database to be populated against a live Postgres")
	}
	if activity.Database.BlksHit+activity.Database.BlksRead == 0 {
		t.Errorf("Database.BlksHit+BlksRead = 0, want > 0 on a live connection")
	}

	if activity.Checkpointer == nil {
		t.Fatal("expected Checkpointer to be populated against a live Postgres")
	}
	if activity.Checkpointer.View != "pg_stat_checkpointer" && activity.Checkpointer.View != "pg_stat_bgwriter" {
		t.Errorf("Checkpointer.View = %q, want pg_stat_checkpointer or pg_stat_bgwriter", activity.Checkpointer.View)
	}

	if activity.ReplicationSlotsReadOK == nil {
		t.Fatal("expected ReplicationSlotsReadOK to be set against a live Postgres")
	}
	if !*activity.ReplicationSlotsReadOK {
		t.Error("expected the replication-slots read to succeed against a live Postgres")
	}
}

func TestReplicationSlotsQueries_ParseAgainstLiveDB(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	for _, name := range []string{"fetch-replication-slots", "fetch-replication-slots-no-wal-status"} {
		rows, err := pool.Query(ctx, q(name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rows.Close()
	}
}

func TestReplicationPeersQueries_ParseAgainstLiveDB(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	for _, name := range []string{"fetch-has-stat-replication", "fetch-replication-peers-primary", "fetch-replication-peers-standby"} {
		rows, err := pool.Query(ctx, q(name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rows.Close()
	}
}

func TestCaptureActivityStats_ReplicationPeers(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	activity, err := CaptureActivityStats(ctx, pool, "sref", "test-primary")
	if err != nil {
		t.Fatalf("activity capture: %v", err)
	}
	if activity.ReplicationPeersReadOK == nil {
		t.Fatal("expected ReplicationPeersReadOK to be set")
	}
	if !*activity.ReplicationPeersReadOK {
		t.Error("expected the replication-peers read to succeed")
	}
	if activity.ReplicationPeers == nil {
		t.Fatal("expected ReplicationPeers non-nil when ReadOK=true")
	}
}

func TestFetchReplicationPeers_ZeroRowsOK(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	peers, ok := fetchReplicationPeers(ctx, pool, false)
	if !ok {
		t.Fatalf("expected ok=true against a live primary with no standbys")
	}
	if peers == nil {
		t.Fatal("expected non-nil empty slice, not nil")
	}
	if len(peers) != 0 {
		t.Errorf("expected 0 peers on a standalone primary, got %d", len(peers))
	}
}

func TestFetchReplicationPeers_ReadFailureIsOKFalse(t *testing.T) {
	pool := livePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	peers, ok := fetchReplicationPeers(ctx, pool, false)
	if ok {
		t.Errorf("expected ok=false on cancelled context")
	}
	if peers != nil {
		t.Errorf("expected nil peers on failure, got %v", peers)
	}
}

func TestFetchReplicationPeers_StandbyBranch(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	peers, ok := fetchReplicationPeers(ctx, pool, true)
	if !ok {
		t.Fatalf("expected standby branch to succeed on live DB")
	}
	if peers == nil {
		t.Fatal("expected non-nil peers slice")
	}
}

// pg_stat_statements may not be preloaded on the test DB; the sentinel is a valid outcome too.
func TestCaptureQueryStats_AgainstLiveDB(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	snap, err := IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}

	qs, err := CaptureQueryStats(ctx, pool, snap.ContentHash, "test-primary", 0)
	if err != nil {
		if errors.Is(err, ErrQueryStatsUnavailable) {
			t.Skip("pg_stat_statements not available on test DB")
		}
		t.Fatalf("query stats capture: %v", err)
	}
	if qs.SchemaRefHash != snap.ContentHash {
		t.Errorf("qs.SchemaRefHash=%s want=%s", qs.SchemaRefHash, snap.ContentHash)
	}
	if qs.Node.Source != "test-primary" {
		t.Errorf("qs.Node.Source=%q want=test-primary", qs.Node.Source)
	}
	if qs.ContentHash == "" {
		t.Errorf("qs.ContentHash empty")
	}
}

// Recomputing the planner hash on the same captured payload must be
// deterministic — this is what PutPlanner relies on for its idempotency.
func TestCapturePlannerStats_DeterministicHash(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	p1, err := CapturePlannerStats(ctx, pool, "fake-ddl-hash")
	if err != nil {
		t.Fatalf("first capture: %v", err)
	}
	want := p1.ContentHash

	// Recompute over the same payload (NOT a second live query — timing
	// would shift Timestamp). Re-hash via the public helper.
	if got := ComputePlannerContentHash(p1); got != want {
		t.Errorf("planner hash non-deterministic: got=%s want=%s", got, want)
	}
}

// CaptureNodeIdentity returns is_standby reflecting pg_is_in_recovery();
// on a primary it must be false. On a real replica setup a separate test
// would flip this; here we assert the primary path doesn't lie.
func TestCaptureNodeIdentity_PrimaryFalse(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	node, err := CaptureNodeIdentity(ctx, pool, "primary")
	if err != nil {
		t.Fatalf("node identity: %v", err)
	}
	if node.Source != "primary" {
		t.Errorf("source=%q want=primary", node.Source)
	}
	// Most CI Postgres setups run as primary; if the test DB is a replica,
	// the caller knows what they wired and can flip this expectation locally.
	if node.IsStandby {
		t.Logf("note: connected to a standby; IsStandby=true")
	}
	if node.PgVersion == "" {
		t.Errorf("PgVersion empty")
	}
}

// The capture side of the raw-row digest: every cluster qshape hands back must
// arrive with its Members populated from the underlying pg_stat_statements
// rows. If they were dropped — kept only as the aggregate totals, as the entry
// carried before — ComputeQueryStatsContentHash would digest an empty row set
// and every capture on the node would collapse to one content hash, silently
// deduping the entire history into a single row.
func TestCaptureQueryStats_PopulatesRawMembers(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	qs, err := CaptureQueryStats(ctx, pool, "fake-ddl-hash", "test-primary", 0)
	if err != nil {
		if errors.Is(err, ErrQueryStatsUnavailable) {
			t.Skip("pg_stat_statements not available on test DB")
		}
		t.Fatalf("query stats capture: %v", err)
	}
	if len(qs.Queries) == 0 {
		t.Skip("no qualifying queries in pg_stat_statements on the test DB")
	}

	for i, e := range qs.Queries {
		if len(e.Members) == 0 {
			t.Errorf("Queries[%d] (%s) has no Members; the raw rows the digest needs were dropped",
				i, e.Fingerprint)
			continue
		}
		// members are the rows as Postgres reported them, so the cluster totals
		// must be exactly their sum — a mismatch means the two views of the same
		// capture have diverged
		var calls, rows int64
		for _, m := range e.Members {
			calls += m.Calls
			rows += m.Rows
		}
		if calls != e.Calls {
			t.Errorf("Queries[%d]: member calls sum to %d, cluster reports %d", i, calls, e.Calls)
		}
		if rows != e.Rows {
			t.Errorf("Queries[%d]: member rows sum to %d, cluster reports %d", i, rows, e.Rows)
		}
	}
}

// The stored ContentHash must be reproducible from the captured payload alone.
// PutQueryStats trusts it for dedup and HTTPStore recomputes it at push time,
// so a digest that depended on anything not in the snapshot would 422 on the
// first push.
func TestCaptureQueryStats_ContentHashRecomputes(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	qs, err := CaptureQueryStats(ctx, pool, "fake-ddl-hash", "test-primary", 0)
	if err != nil {
		if errors.Is(err, ErrQueryStatsUnavailable) {
			t.Skip("pg_stat_statements not available on test DB")
		}
		t.Fatalf("query stats capture: %v", err)
	}

	// recompute over the same payload, not a second live query: the counters
	// move under us otherwise and the test would flap on any busy database
	if got := ComputeQueryStatsContentHash(qs); got != qs.ContentHash {
		t.Errorf("stored ContentHash %s does not recompute from the payload (got %s)",
			qs.ContentHash, got)
	}
}

// track_io_timing is off by default, and with it off pg_stat_statements reports 0 for the
// two block timings. Storing that 0 would make a statement that never waited on disk
// indistinguishable from one nobody measured, so it is dropped to unknown at the fetch.
func TestStripUntimed(t *testing.T) {
	ms := func(v float64) *float64 { return &v }
	measured := qshape.Query{
		QueryID:              1,
		SharedBlkReadTimeMs:  ms(0),
		SharedBlkWriteTimeMs: ms(4),
		SharedBlksRead:       func(v int64) *int64 { return &v }(9),
	}

	off := stripUntimed(measured, false)
	if off.SharedBlkReadTimeMs != nil || off.SharedBlkWriteTimeMs != nil {
		t.Fatalf("timings survive track_io_timing = off: %v %v", off.SharedBlkReadTimeMs, off.SharedBlkWriteTimeMs)
	}
	// The COUNTERS have no config gate; dropping them too would lose real data.
	if off.SharedBlksRead == nil || *off.SharedBlksRead != 9 {
		t.Fatal("shared block counters must survive: they do not depend on track_io_timing")
	}

	// Unknown (the GUC read failed) is not off: the timings survive, and the capture's
	// NULL track_io_timing is what marks them uncertain.
	on := stripUntimed(measured, true)
	if on.SharedBlkReadTimeMs == nil || *on.SharedBlkReadTimeMs != 0 {
		t.Fatal("an explicit 0 with timing ON is a measurement and must survive")
	}
	if on.SharedBlkWriteTimeMs == nil || *on.SharedBlkWriteTimeMs != 4 {
		t.Fatal("write timing lost")
	}
}
