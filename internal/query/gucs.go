package query

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/boringsql/dryrun/internal/schema"
)

type SkippedGuc struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// session-settable planner knobs; capture-only GUCs (shared_buffers,
// autovacuum_*, max_worker_processes) are excluded
var plannerGUCs = map[string]struct{}{
	"seq_page_cost":                   {},
	"random_page_cost":                {},
	"cpu_tuple_cost":                  {},
	"cpu_index_tuple_cost":            {},
	"cpu_operator_cost":               {},
	"effective_cache_size":            {},
	"effective_io_concurrency":        {},
	"work_mem":                        {},
	"hash_mem_multiplier":             {},
	"jit":                             {},
	"jit_above_cost":                  {},
	"jit_inline_above_cost":           {},
	"jit_optimize_above_cost":         {},
	"max_parallel_workers":            {},
	"max_parallel_workers_per_gather": {},
	"parallel_setup_cost":             {},
	"parallel_tuple_cost":             {},
	"min_parallel_table_scan_size":    {},
	"min_parallel_index_scan_size":    {},
	"parallel_leader_participation":   {},
}

func isPlannerGUC(name string) bool {
	if _, ok := plannerGUCs[name]; ok {
		return true
	}
	return strings.HasPrefix(name, "enable_")
}

// set_config(..., true) == SET LOCAL: lasts until the surrounding tx rolls
// back. Each GUC gets a savepoint so one rejection (unknown parameter on an
// older major, platform-invalid value) skips that GUC instead of aborting
// the tx and the EXPLAIN.
func applyPlannerGUCs(ctx context.Context, tx pgx.Tx, gucs []schema.GucSetting) (replayed []string, skipped []SkippedGuc, err error) {
	for _, g := range gucs {
		if !isPlannerGUC(g.Name) {
			continue
		}
		if _, err := tx.Exec(ctx, "SAVEPOINT guc_replay"); err != nil {
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SELECT set_config($1, $2, true)", g.Name, g.CanonicalValue()); err != nil {
			if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT guc_replay"); rbErr != nil {
				return nil, nil, rbErr
			}
			skipped = append(skipped, SkippedGuc{Name: g.Name, Reason: pgErrMessage(err)})
			continue
		}
		replayed = append(replayed, g.Name)
	}
	return replayed, skipped, nil
}

func pgErrMessage(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Message
	}
	return err.Error()
}
