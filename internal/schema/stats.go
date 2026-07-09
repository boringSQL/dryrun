package schema

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Sizing + per-column pg_stats; schema_ref ties it back to a DDL snapshot
func CapturePlannerStats(ctx context.Context, pool Querier, schemaRefHash string) (*PlannerStatsSnapshot, error) {
	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return nil, fmt.Errorf("query current_database: %w", err)
	}

	// Reference counters for ageing relfrozenxid/relminmxid offline.
	// xmax = next xid (pg_current_snapshot doesn't consume one, safe in our read tx).
	// mxid_age('1')+1 = next_multixact, avoiding superuser-gated pg_control_checkpoint;
	// cast before +1 since mxid_age is int4 and nears INT_MAX at wraparound.
	var databaseXid, databaseMxid int64
	if err := pool.QueryRow(ctx,
		"SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())::text::int8, "+
			"(pg_catalog.mxid_age('1'::xid)::int8 + 1)",
	).Scan(&databaseXid, &databaseMxid); err != nil {
		return nil, fmt.Errorf("query reference counters: %w", err)
	}

	tables, err := fetchPlannerTableSizing(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch table sizing: %w", err)
	}
	indexes, err := fetchPlannerIndexSizing(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch index sizing: %w", err)
	}
	columns, err := fetchPlannerColumnStats(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch column stats: %w", err)
	}
	gucs, err := fetchGUCs(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch gucs: %w", err)
	}

	snap := &PlannerStatsSnapshot{
		FormatVersion: FormatVersion,
		SchemaRefHash: schemaRefHash,
		Database:      database,
		Timestamp:     time.Now().UTC(),
		DatabaseXid:   databaseXid,
		DatabaseMxid:  databaseMxid,
		Tables:        tables,
		Indexes:       indexes,
		Columns:       columns,
		GUCs:          gucs,
	}
	snap.ContentHash = ComputePlannerContentHash(snap)
	return snap, nil
}

// Per-node activity counters; source identifies the producing node
func CaptureActivityStats(ctx context.Context, pool Querier, schemaRefHash, source string) (*ActivityStatsSnapshot, error) {
	node, err := CaptureNodeIdentity(ctx, pool, source)
	if err != nil {
		return nil, err
	}
	tables, err := fetchActivityTables(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch activity tables: %w", err)
	}
	indexes, err := fetchActivityIndexes(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch activity indexes: %w", err)
	}

	snap := &ActivityStatsSnapshot{
		FormatVersion: FormatVersion,
		SchemaRefHash: schemaRefHash,
		Node:          *node,
		Tables:        tables,
		Indexes:       indexes,
	}
	snap.ContentHash = ComputeActivityContentHash(snap)
	return snap, nil
}

func CaptureNodeIdentity(ctx context.Context, pool Querier, source string) (*NodeIdentity, error) {
	var (
		isStandby bool
		pgVersion string
	)
	if err := pool.QueryRow(ctx, q("fetch-node-identity")).Scan(&isStandby, &pgVersion); err != nil {
		return nil, fmt.Errorf("fetch node identity: %w", err)
	}
	return &NodeIdentity{
		Source:    source,
		IsStandby: isStandby,
		PgVersion: pgVersion,
		Timestamp: time.Now().UTC(),
	}, nil
}

func fetchPlannerTableSizing(ctx context.Context, pool Querier) ([]TableSizingEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-planner-table-sizing"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (TableSizingEntry, error) {
		var e TableSizingEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name,
			&e.Sizing.Reltuples, &e.Sizing.Relpages,
			&e.Sizing.TableSize, &e.Sizing.TotalRelationSize,
			&e.Sizing.IndexesSize, &e.Sizing.ToastSize,
			&e.Sizing.RelfrozenXid, &e.Sizing.RelminMxid,
		)
		return e, err
	})
}

func fetchPlannerIndexSizing(ctx context.Context, pool Querier) ([]IndexSizingEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-planner-index-sizing"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (IndexSizingEntry, error) {
		var e IndexSizingEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name, &e.Index,
			&e.Sizing.Relpages, &e.Sizing.Reltuples, &e.Sizing.Size,
		)
		return e, err
	})
}

func fetchPlannerColumnStats(ctx context.Context, pool Querier) ([]ColumnStatsEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-planner-column-stats"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (ColumnStatsEntry, error) {
		var e ColumnStatsEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name, &e.Column,
			&e.Stats.NullFrac, &e.Stats.NDistinct,
			&e.Stats.MostCommonVals, &e.Stats.MostCommonFreqs,
			&e.Stats.HistogramBounds, &e.Stats.Correlation,
			&e.Stats.AvgWidth,
		)
		return e, err
	})
}

func fetchActivityTables(ctx context.Context, pool Querier) ([]TableActivityEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-activity-tables"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (TableActivityEntry, error) {
		var e TableActivityEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name,
			&e.Activity.SeqScan, &e.Activity.SeqTupRead,
			&e.Activity.IdxScan, &e.Activity.IdxTupFetch,
			&e.Activity.NTupIns, &e.Activity.NTupUpd, &e.Activity.NTupDel, &e.Activity.NTupHotUpd,
			&e.Activity.NLiveTup, &e.Activity.NDeadTup, &e.Activity.NModSinceAnalyze,
			&e.Activity.LastVacuum, &e.Activity.LastAutovacuum,
			&e.Activity.LastAnalyze, &e.Activity.LastAutoanalyze,
			&e.Activity.VacuumCount, &e.Activity.AutovacuumCount,
			&e.Activity.AnalyzeCount, &e.Activity.AutoanalyzeCount,
		)
		return e, err
	})
}

func fetchActivityIndexes(ctx context.Context, pool Querier) ([]IndexActivityEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-activity-indexes"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (IndexActivityEntry, error) {
		var e IndexActivityEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name, &e.Index,
			&e.Activity.IdxScan, &e.Activity.IdxTupRead, &e.Activity.IdxTupFetch,
		)
		return e, err
	})
}

func FetchIsStandby(ctx context.Context, pool Querier) (bool, error) {
	var b bool
	err := pool.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&b)
	return b, err
}

func FetchCurrentDatabase(ctx context.Context, pool Querier) (string, error) {
	var db string
	err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&db)
	return db, err
}
