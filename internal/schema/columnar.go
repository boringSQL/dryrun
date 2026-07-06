package schema

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/boringsql/dryrun/pkg/columnar"
)

// only AlloyDB exposes the g_columnar_* views
var ErrNotAlloyDB = errors.New("columnar engine not present (not an AlloyDB instance)")

func CaptureColumnarState(ctx context.Context, pool Querier) (*columnar.State, error) {
	flavor, err := DetectFlavorLive(ctx, pool)
	if err != nil {
		return nil, err
	}
	if flavor != FlavorAlloyDBOmni && flavor != FlavorAlloyDBManaged {
		return nil, ErrNotAlloyDB
	}

	var enabled *bool
	if err := pool.QueryRow(ctx, q("columnar-engine-enabled")).Scan(&enabled); err != nil {
		return nil, fmt.Errorf("columnar engine setting: %w", err)
	}

	state := &columnar.State{Flavor: string(flavor), EngineEnabled: enabled != nil && *enabled}
	// the g_columnar_* views raise an error when the engine is off, so only read them when enabled
	if !state.EngineEnabled {
		return state, nil
	}

	if state.Columns, err = fetchColumnarColumns(ctx, pool); err != nil {
		return nil, fmt.Errorf("columnar columns: %w", err)
	}
	if state.Relations, err = fetchColumnarRelations(ctx, pool); err != nil {
		return nil, fmt.Errorf("columnar relations: %w", err)
	}
	return state, nil
}

func fetchColumnarColumns(ctx context.Context, pool Querier) ([]columnar.Column, error) {
	rows, err := query(ctx, pool, "columnar-columns")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (columnar.Column, error) {
		var c columnar.Column
		var last *time.Time
		err := r.Scan(&c.Schema, &c.Relation, &c.Column, &c.Type, &c.Status,
			&c.SizeBytes, &c.NumTimesAccessed, &last)
		c.LastAccessed = last
		return c, err
	})
}

func fetchColumnarRelations(ctx context.Context, pool Querier) ([]columnar.Relation, error) {
	rows, err := query(ctx, pool, "columnar-relations")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (columnar.Relation, error) {
		var rel columnar.Relation
		err := r.Scan(&rel.Schema, &rel.Relation, &rel.Status, &rel.SizeBytes,
			&rel.InvalidBlockCount, &rel.TotalBlockCount, &rel.AutoRefreshFailure)
		return rel, err
	})
}
