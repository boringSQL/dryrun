package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Per-table, per-index, per-column stats from connected db for multi-node setups
func ExtractNodeStats(ctx context.Context, pool *pgxpool.Pool, source string) (*NodeStats, error) {
	tableStats, err := extractTableStats(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("extract table stats: %w", err)
	}

	indexStats, err := extractIndexStats(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("extract index stats: %w", err)
	}

	columnStats, err := extractColumnStats(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("extract column stats: %w", err)
	}

	return &NodeStats{
		Source:      source,
		TableStats:  tableStats,
		IndexStats:  indexStats,
		ColumnStats: columnStats,
	}, nil
}

func extractTableStats(ctx context.Context, pool *pgxpool.Pool) ([]NodeTableStats, error) {
	rows, err := pool.Query(ctx, q("fetch-table-stats"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []NodeTableStats
	for rows.Next() {
		var (
			oid int32
			rs  rawTableStats
		)
		if err := rows.Scan(
			&oid, &rs.reltuples, &rs.deadTuples,
			&rs.lastVacuum, &rs.lastAutovacuum,
			&rs.lastAnalyze, &rs.lastAutoanalyze,
			&rs.seqScan, &rs.idxScan, &rs.tableSize,
		); err != nil {
			return nil, err
		}
		// TODO: query returns OID but we need schema+table names; separate query needed
	}
	_ = out
	return nil, rows.Err()
}

func extractIndexStats(ctx context.Context, pool *pgxpool.Pool) ([]NodeIndexStats, error) {
	return nil, nil
}

func extractColumnStats(ctx context.Context, pool *pgxpool.Pool) ([]NodeColumnStats, error) {
	return nil, nil
}
