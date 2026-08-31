package schema

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// The floor is a statement about the SERVER major. pg_stat_statements has its
// own version, and pg_upgrade does not run ALTER EXTENSION UPDATE — so a
// current server can carry pgss 1.8, with no toplevel column and no
// pg_stat_statements_info view. Capture must degrade, not fail.
//
// This is the regression the PG14-floor cleanup could most easily have caused:
// the probes for toplevel/info/shared_blk_read_time look like server-version
// checks and are not. Nothing tested it before.
//
// Needs a throwaway server (it rewrites the extension) and superuser, so it
// rides TEST_DATABASE_URL like the rest of the live-DB tests.
func TestCaptureQueryStatsWithOldPgssExtension(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	var available bool
	if err := pool.QueryRow(ctx,
		"SELECT count(*) > 0 FROM pg_available_extension_versions WHERE name = 'pg_stat_statements' AND version = '1.8'").
		Scan(&available); err != nil {
		t.Skipf("cannot read pg_available_extension_versions: %v", err)
	}
	if !available {
		t.Skip("pg_stat_statements 1.8 not offered by this server; skipping")
	}

	// Restore exactly what was here, including "nothing" -- installing pgss on a
	// developer's database that never had it is a side effect, not a cleanup.
	var priorVersion *string
	if err := pool.QueryRow(ctx,
		"SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'pg_stat_statements'").
		Scan(&priorVersion); err != nil {
		priorVersion = nil
	}

	if _, err := pool.Exec(ctx, "DROP EXTENSION IF EXISTS pg_stat_statements"); err != nil {
		t.Skipf("cannot drop pg_stat_statements (needs superuser): %v", err)
	}
	t.Cleanup(func() {
		restore := context.Background()
		_, _ = pool.Exec(restore, "DROP EXTENSION IF EXISTS pg_stat_statements")
		if priorVersion != nil {
			_, _ = pool.Exec(restore,
				fmt.Sprintf("CREATE EXTENSION pg_stat_statements VERSION %s", quoteLiteral(*priorVersion)))
		}
	})

	if _, err := pool.Exec(ctx, "CREATE EXTENSION pg_stat_statements VERSION '1.8'"); err != nil {
		t.Skipf("cannot install pg_stat_statements 1.8 (shared_preload_libraries?): %v", err)
	}

	// The probe must report the extension present but without 1.9's additions.
	var installed, hasInfoView, hasToplevel, renamedBlkTime bool
	if err := pool.QueryRow(ctx, q("fetch-pg-stat-statements-installed")).
		Scan(&installed, &hasInfoView, &hasToplevel, &renamedBlkTime); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !installed {
		t.Fatal("pgss 1.8 is installed but the probe reported it absent")
	}
	if hasToplevel {
		t.Error("pgss 1.8 has no toplevel column, but the probe claims it does")
	}
	if hasInfoView {
		t.Error("pgss 1.8 has no pg_stat_statements_info, but the probe claims it does")
	}

	snap, err := CaptureQueryStats(ctx, pool, "", "old-pgss", 100)
	if err != nil {
		t.Fatalf("capture against pgss 1.8 failed, it must degrade instead: %v", err)
	}
	if snap == nil {
		t.Fatal("capture returned no snapshot")
	}
	if snap.InfoAfter != nil {
		t.Error("pgss 1.8 has no info view; InfoAfter must be nil, never a zero struct")
	}
	if snap.ToplevelOnly {
		t.Error("pgss 1.8 cannot filter on toplevel; ToplevelOnly must be false")
	}
}
