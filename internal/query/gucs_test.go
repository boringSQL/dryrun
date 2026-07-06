package query

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/schema"
)

// The allowlist decides which captured GUCs get replayed via SET LOCAL before
// EXPLAIN. Two failure directions matter: replaying a non-session-settable GUC
// (shared_buffers is postmaster context, autovacuum_* is sighup) would always
// be rejected by the server and pollute GucsSkipped with noise on every
// explain; NOT replaying a planner knob silently produces a plan production
// would never choose. So we pin both directions.
func TestIsPlannerGUC(t *testing.T) {
	replayed := []string{
		"seq_page_cost", "random_page_cost",
		"cpu_tuple_cost", "cpu_index_tuple_cost", "cpu_operator_cost",
		"effective_cache_size", "effective_io_concurrency",
		"work_mem", "hash_mem_multiplier",
		"jit", "jit_above_cost", "jit_inline_above_cost", "jit_optimize_above_cost",
		"max_parallel_workers", "max_parallel_workers_per_gather",
		"parallel_setup_cost", "parallel_tuple_cost",
		"min_parallel_table_scan_size", "min_parallel_index_scan_size",
		"parallel_leader_participation",
		// the whole enable_* family goes through the prefix match, including
		// toggles added in future majors we cannot enumerate today
		"enable_seqscan", "enable_hashjoin", "enable_nestloop",
		"enable_partition_pruning", "enable_self_join_elimination",
	}
	for _, name := range replayed {
		if !isPlannerGUC(name) {
			t.Errorf("isPlannerGUC(%q) = false, want true", name)
		}
	}

	captureOnly := []string{
		"shared_buffers",              // postmaster: SET LOCAL errors with "cannot be changed without restarting"
		"max_worker_processes",        // postmaster
		"autovacuum",                  // sighup: "cannot be changed now"
		"autovacuum_vacuum_threshold", // sighup
		"maintenance_work_mem",        // settable but not a planner input
		"default_statistics_target",   // affects future ANALYZE, not planning over injected stats
	}
	for _, name := range captureOnly {
		if isPlannerGUC(name) {
			t.Errorf("isPlannerGUC(%q) = true, want false", name)
		}
	}
}

// End-to-end proof that snapshot GUCs actually reach the planner: on a tiny
// indexed table the planner prefers Seq Scan, but a snapshot carrying
// enable_seqscan=off must flip the replayed plan to an index scan. Also proves
// SET LOCAL does not leak: the pool connection must be back to enable_seqscan=on
// after ExplainQuery returns.
func TestExplainQuery_ReplaysPlannerGUCs(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live GUC replay test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS guc_replay_probe;
		CREATE TABLE guc_replay_probe (id int PRIMARY KEY, val text);
		INSERT INTO guc_replay_probe SELECT g, g::text FROM generate_series(1, 100) g;
		ANALYZE guc_replay_probe;
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS guc_replay_probe")
	})

	sql := "SELECT * FROM guc_replay_probe WHERE id > 10"

	// control: no snapshot, tiny table => Seq Scan
	baseline, err := ExplainQuery(ctx, pool, sql, false, nil)
	if err != nil {
		t.Fatalf("baseline explain: %v", err)
	}
	if baseline.Plan.NodeType != "Seq Scan" {
		t.Fatalf("baseline plan = %q, expected Seq Scan on a 100-row table", baseline.Plan.NodeType)
	}

	snap := &schema.SchemaSnapshot{
		GUCs: []schema.GucSetting{
			// a GUC the server rejects must be skipped (savepoint rollback),
			// not abort the whole EXPLAIN — this is the cross-major /
			// cross-platform shape: a snapshot from a newer prod major carries
			// planner toggles the local shadow server has never heard of
			{Name: "enable_bogus_future_toggle", Setting: "on"},
			{Name: "enable_seqscan", Setting: "off"},
			// capture-only entries must be filtered out, not break the tx
			{Name: "shared_buffers", Setting: "16384", Unit: strPtr("8kB")},
			{Name: "autovacuum", Setting: "on"},
		},
	}
	replayed, err := ExplainQuery(ctx, pool, sql, false, snap)
	if err != nil {
		t.Fatalf("replayed explain: %v", err)
	}
	if !strings.Contains(replayed.Plan.NodeType, "Index") {
		t.Errorf("replayed plan = %q, expected an index scan under enable_seqscan=off", replayed.Plan.NodeType)
	}

	// the result must say exactly what planner environment the plan came from
	if len(replayed.GucsReplayed) != 1 || replayed.GucsReplayed[0] != "enable_seqscan" {
		t.Errorf("GucsReplayed = %v, want [enable_seqscan]", replayed.GucsReplayed)
	}
	if len(replayed.GucsSkipped) != 1 || replayed.GucsSkipped[0].Name != "enable_bogus_future_toggle" {
		t.Errorf("GucsSkipped = %v, want the rejected enable_bogus_future_toggle", replayed.GucsSkipped)
	}
	if len(replayed.GucsSkipped) == 1 && replayed.GucsSkipped[0].Reason == "" {
		t.Error("skipped GUC carries no reason")
	}

	// SET LOCAL must die with the rolled-back tx, not poison the pool
	var current string
	if err := pool.QueryRow(ctx, "SHOW enable_seqscan").Scan(&current); err != nil {
		t.Fatalf("show enable_seqscan: %v", err)
	}
	if current != "on" {
		t.Errorf("enable_seqscan leaked to the pool: %q", current)
	}
}
