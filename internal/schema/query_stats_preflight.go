package schema

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

type (
	batcher interface {
		SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	}

	queryStatsPreflight struct {
		Installed      bool
		HasInfoView    bool
		HasToplevel    bool
		RenamedBlkTime bool
		Node           nodeIdentityRow
		Env            pgssEnvironment
	}
)

func fetchQueryStatsPreflight(ctx context.Context, pool Querier) (*queryStatsPreflight, error) {
	var p queryStatsPreflight

	pipe, ok := pool.(batcher)
	if !ok {
		if err := pool.QueryRow(ctx, q("fetch-pg-stat-statements-installed")).
			Scan(&p.Installed, &p.HasInfoView, &p.HasToplevel, &p.RenamedBlkTime); err != nil {
			return nil, fmt.Errorf("check pg_stat_statements: %w", err)
		}
		if err := pool.QueryRow(ctx, q("fetch-node-identity")).Scan(p.Node.dest()...); err != nil {
			return nil, fmt.Errorf("fetch node identity: %w", err)
		}
		p.Env = fetchPgssEnvironment(ctx, pool)
		return &p, nil
	}

	batch := &pgx.Batch{}
	batch.Queue(q("fetch-pg-stat-statements-installed"))
	batch.Queue(q("fetch-node-identity"))
	batch.Queue(q("fetch-pgss-environment"))
	results := pipe.SendBatch(ctx, batch)
	defer results.Close()

	if err := results.QueryRow().Scan(&p.Installed, &p.HasInfoView, &p.HasToplevel, &p.RenamedBlkTime); err != nil {
		return nil, fmt.Errorf("check pg_stat_statements: %w", err)
	}
	if err := results.QueryRow().Scan(p.Node.dest()...); err != nil {
		return nil, fmt.Errorf("fetch node identity: %w", err)
	}
	if err := results.QueryRow().Scan(p.Env.dest()...); err != nil {
		slog.Debug("pg_stat_statements environment unavailable", "error", err)
		p.Env = pgssEnvironment{}
	}
	return &p, nil
}
