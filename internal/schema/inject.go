package schema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InjectResult struct {
	TablesUpdated  int      `json:"tables_updated"`
	IndexesUpdated int      `json:"indexes_updated"`
	ColumnsUpdated int      `json:"columns_updated"`
	Warnings       []string `json:"warnings,omitempty"`
	Method         string   `json:"method"`
}

func (r *InjectResult) warn(format string, args ...any) {
	w := fmt.Sprintf(format, args...)
	r.Warnings = append(r.Warnings, w)
	slog.Warn(w)
}

// PG18+ uses pg_restore_*_stats(), older versions fall back to direct catalog manipulation
func InjectStats(ctx context.Context, pool *pgxpool.Pool, a *AnnotatedSchema, pgMajor int) (*InjectResult, error) {
	if a == nil || a.Schema == nil || a.Planner == nil {
		return nil, errors.New("annotated schema with planner stats required")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	result := &InjectResult{}
	if pgMajor >= 18 {
		result.Method = "pg_restore_relation_stats"
	} else {
		result.Method = "pg_class_update"
	}

	for _, t := range a.Schema.Tables {
		qual := t.Qual()

		if sz := a.SizingFor(qual); sz != nil {
			if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
				return injectRelationStats(ctx, tx, pgMajor, t.Schema, t.Name, sz.Relpages, sz.Reltuples)
			}); err != nil {
				result.warn("table %s.%s: %v", t.Schema, t.Name, err)
			} else {
				result.TablesUpdated++
			}
		}

		for _, idx := range t.Indexes {
			isz := a.IndexSizingFor(qual, idx.Name)
			if isz == nil {
				continue
			}
			if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
				return injectRelationStats(ctx, tx, pgMajor, t.Schema, idx.Name, isz.Relpages, isz.Reltuples)
			}); err != nil {
				result.warn("index %s.%s: %v", t.Schema, idx.Name, err)
			} else {
				result.IndexesUpdated++
			}
		}

		colsWithStats := collectColumnsWithStats(a, t)
		if len(colsWithStats) == 0 {
			continue
		}

		if pgMajor >= 18 {
			for _, cs := range colsWithStats {
				if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
					return injectColumnStatsPG18(ctx, tx, pgMajor, t.Schema, t.Name, cs.col, cs.stats)
				}); err != nil {
					result.warn("column %s.%s.%s: %v", t.Schema, t.Name, cs.col.Name, err)
				} else {
					result.ColumnsUpdated++
				}
			}
		} else {
			names := make([]Column, len(colsWithStats))
			for i, cs := range colsWithStats {
				names[i] = cs.col
			}
			var meta map[string]columnMeta
			if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
				var e error
				meta, e = batchLookupColumnMeta(ctx, tx, t.Schema, t.Name, names)
				return e
			}); err != nil {
				result.warn("column metadata lookup %s.%s: %v", t.Schema, t.Name, err)
				continue
			}
			for _, cs := range colsWithStats {
				cm, ok := meta[cs.col.Name]
				if !ok {
					result.warn("column %s.%s.%s: not found in target database", t.Schema, t.Name, cs.col.Name)
					continue
				}
				if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
					return injectColumnStatsLegacy(ctx, tx, cm, cs.stats)
				}); err != nil {
					result.warn("column %s.%s.%s: %v", t.Schema, t.Name, cs.col.Name, err)
				} else {
					result.ColumnsUpdated++
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	slog.Info("stats injection complete",
		"tables", result.TablesUpdated,
		"indexes", result.IndexesUpdated,
		"columns", result.ColumnsUpdated,
		"method", result.Method,
	)
	return result, nil
}

// savepoint per injection: one failed Exec else aborts the whole pgx tx (25P02).
func runSavepoint(ctx context.Context, tx pgx.Tx, fn func(pgx.Tx) error) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(sp); err != nil {
		_ = sp.Rollback(ctx)
		return err
	}
	return sp.Commit(ctx)
}

type colWithStats struct {
	col   Column
	stats *ColumnStats
}

func collectColumnsWithStats(a *AnnotatedSchema, t Table) []colWithStats {
	qual := t.Qual()
	var out []colWithStats
	for _, c := range t.Columns {
		if s := a.ColumnStats(qual, c.Name); s != nil {
			out = append(out, colWithStats{col: c, stats: s})
		}
	}
	return out
}

type columnMeta struct {
	relOID   uint32
	attNum   int16
	typeOID  uint32
	typeName string
	eqOpOID  uint32 // 0 when type has no equality operator
}

// One round-trip instead of one query per column
func batchLookupColumnMeta(ctx context.Context, tx pgx.Tx, schemaName, tableName string, cols []Column) (map[string]columnMeta, error) {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}

	rows, err := tx.Query(ctx, q("lookup-column-meta"), schemaName, tableName, names)
	if err != nil {
		return nil, fmt.Errorf("batch column lookup: %w", err)
	}
	defer rows.Close()

	result := make(map[string]columnMeta, len(cols))
	for rows.Next() {
		var (
			name string
			cm   columnMeta
		)
		if err := rows.Scan(&name, &cm.relOID, &cm.attNum, &cm.typeOID, &cm.typeName, &cm.eqOpOID); err != nil {
			return nil, fmt.Errorf("scan column meta: %w", err)
		}
		result[name] = cm
	}
	return result, rows.Err()
}

func injectRelationStats(ctx context.Context, tx pgx.Tx, pgMajor int, schemaName, relName string, relpages int64, reltuples float64) error {
	if pgMajor >= 18 {
		_, err := tx.Exec(ctx, q("restore-relation-stats-pg18"),
			pgMajor, schemaName, relName, relpages, float32(reltuples))
		return err
	}

	tag, err := tx.Exec(ctx, q("update-relation-stats-legacy"),
		reltuples, relpages, relName, schemaName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("relation %s.%s not found in target database", schemaName, relName)
	}
	return nil
}

func injectColumnStatsPG18(ctx context.Context, tx pgx.Tx, pgMajor int, schemaName, tableName string, col Column, s *ColumnStats) error {
	parts := []string{
		"'version', $1::int",
		"'schemaname', $2::text",
		"'relname', $3::text",
		"'attname', $4::text",
		"'inherited', false",
	}
	args := []any{pgMajor, schemaName, tableName, col.Name}
	idx := 5

	if s.NullFrac != nil {
		parts = append(parts, fmt.Sprintf("'null_frac', $%d::real", idx))
		args = append(args, float32(*s.NullFrac))
		idx++
	}
	if s.NDistinct != nil {
		parts = append(parts, fmt.Sprintf("'n_distinct', $%d::real", idx))
		args = append(args, float32(*s.NDistinct))
		idx++
	}
	// MCV vals + freqs are one slot — emit both or neither
	if s.MostCommonVals != nil && s.MostCommonFreqs != nil {
		parts = append(parts, fmt.Sprintf("'most_common_vals', $%d::text", idx))
		args = append(args, *s.MostCommonVals)
		idx++
		parts = append(parts, fmt.Sprintf("'most_common_freqs', $%d::real[]", idx))
		args = append(args, *s.MostCommonFreqs)
		idx++
	}
	if s.HistogramBounds != nil {
		parts = append(parts, fmt.Sprintf("'histogram_bounds', $%d::text", idx))
		args = append(args, *s.HistogramBounds)
		idx++
	}
	if s.Correlation != nil {
		parts = append(parts, fmt.Sprintf("'correlation', $%d::real", idx))
		args = append(args, float32(*s.Correlation))
		idx++
	}

	sql := "SELECT pg_restore_attribute_stats(" + strings.Join(parts, ", ") + ")"
	_, err := tx.Exec(ctx, sql, args...)
	return err
}

func injectColumnStatsLegacy(ctx context.Context, tx pgx.Tx, cm columnMeta, s *ColumnStats) error {
	_, err := tx.Exec(ctx, q("delete-column-stats-legacy"), cm.relOID, cm.attNum)
	if err != nil {
		return fmt.Errorf("delete old stats: %w", err)
	}

	nullFrac := float32(0)
	if s.NullFrac != nil {
		nullFrac = float32(*s.NullFrac)
	}
	nDistinct := float32(0)
	if s.NDistinct != nil {
		nDistinct = float32(*s.NDistinct)
	}

	// stavalues is anyarray Does not work before PG 18
	kind3 := int16(0)
	var correlation any
	if s.Correlation != nil {
		kind3 = 3
		correlation = fmt.Sprintf("{%v}", *s.Correlation)
	}

	const insertSQL = `INSERT INTO pg_statistic (
		starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct,
		stakind1, staop1, stacoll1, stanumbers1, stavalues1,
		stakind2, staop2, stacoll2, stanumbers2, stavalues2,
		stakind3, staop3, stacoll3, stanumbers3, stavalues3,
		stakind4, staop4, stacoll4, stanumbers4, stavalues4,
		stakind5, staop5, stacoll5, stanumbers5, stavalues5
	) VALUES ($1, $2, false, $3, 0, $4,
		0, 0, 0, NULL, NULL,
		0, 0, 0, NULL, NULL,
		$5, 0, 0, $6::real[], NULL,
		0, 0, 0, NULL, NULL,
		0, 0, 0, NULL, NULL)`

	if _, err := tx.Exec(ctx, insertSQL, cm.relOID, cm.attNum, nullFrac, nDistinct, kind3, correlation); err != nil {
		return fmt.Errorf("insert pg_statistic: %w", err)
	}
	return nil
}

func CanInjectStats(a *AnnotatedSchema) error {
	if a == nil || a.Planner == nil {
		return errors.New("annotated schema has no planner stats to inject")
	}
	if len(a.Planner.Tables) == 0 && len(a.Planner.Columns) == 0 {
		return errors.New("planner snapshot is empty")
	}
	return nil
}
