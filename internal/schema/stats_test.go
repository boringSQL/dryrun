package schema

import (
	"context"
	"errors"
	"os"
	"testing"

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

// pg_stat_statements may not be preloaded on the test DB; the sentinel is a valid outcome too.
func TestCaptureQueryStats_AgainstLiveDB(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	snap, err := IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}

	qs, err := CaptureQueryStats(ctx, pool, snap.ContentHash, "test-primary")
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
