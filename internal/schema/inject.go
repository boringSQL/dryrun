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
func InjectStats(ctx context.Context, pool *pgxpool.Pool, snap *SchemaSnapshot, pgMajor int) (*InjectResult, error) {
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

	for _, t := range snap.Tables {
		// relation stats -> pg_class
		if t.Stats != nil {
			if err := injectRelationStats(ctx, tx, pgMajor, t.Schema, t.Name, t.Stats.Relpages, t.Stats.Reltuples); err != nil {
				result.warn("table %s.%s: %v", t.Schema, t.Name, err)
			} else {
				result.TablesUpdated++
			}
		}

		// index stats -> pg_class
		for _, idx := range t.Indexes {
			if idx.Stats == nil {
				continue
			}
			if err := injectRelationStats(ctx, tx, pgMajor, t.Schema, idx.Name, idx.Stats.Relpages, idx.Stats.Reltuples); err != nil {
				result.warn("index %s.%s: %v", t.Schema, idx.Name, err)
			} else {
				result.IndexesUpdated++
			}
		}

		// column stats -> pg_statistic; legacy path batches OID lookups
		colsWithStats := columnsWithStats(t.Columns)
		if len(colsWithStats) == 0 {
			continue
		}

		if pgMajor >= 18 {
			for _, col := range colsWithStats {
				if err := injectColumnStatsPG18(ctx, tx, pgMajor, t.Schema, t.Name, col); err != nil {
					result.warn("column %s.%s.%s: %v", t.Schema, t.Name, col.Name, err)
				} else {
					result.ColumnsUpdated++
				}
			}
		} else {
			meta, err := batchLookupColumnMeta(ctx, tx, t.Schema, t.Name, colsWithStats)
			if err != nil {
				result.warn("column metadata lookup %s.%s: %v", t.Schema, t.Name, err)
				continue
			}
			for _, col := range colsWithStats {
				cm, ok := meta[col.Name]
				if !ok {
					result.warn("column %s.%s.%s: not found in target database", t.Schema, t.Name, col.Name)
					continue
				}
				if err := injectColumnStatsLegacy(ctx, tx, cm, col); err != nil {
					result.warn("column %s.%s.%s: %v", t.Schema, t.Name, col.Name, err)
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

func columnsWithStats(cols []Column) []Column {
	var out []Column
	for _, c := range cols {
		if c.Stats != nil {
			out = append(out, c)
		}
	}
	return out
}

type columnMeta struct {
	relOID   uint32
	attNum   int16
	typeOID  uint32
	typeName string // e.g. "integer", "character varying"
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

// PG18+ path; only non-nil stat fields are sent
func injectColumnStatsPG18(ctx context.Context, tx pgx.Tx, pgMajor int, schemaName, tableName string, col Column) error {
	parts := []string{
		"'version', $1::int",
		"'schemaname', $2::name",
		"'relname', $3::name",
		"'attname', $4::name",
		"'inherited', false",
	}
	args := []any{pgMajor, schemaName, tableName, col.Name}
	idx := 5

	if col.Stats.NullFrac != nil {
		parts = append(parts, fmt.Sprintf("'null_frac', $%d::real", idx))
		args = append(args, float32(*col.Stats.NullFrac))
		idx++
	}
	if col.Stats.NDistinct != nil {
		parts = append(parts, fmt.Sprintf("'n_distinct', $%d::real", idx))
		args = append(args, float32(*col.Stats.NDistinct))
		idx++
	}
	if col.Stats.MostCommonVals != nil {
		parts = append(parts, fmt.Sprintf("'most_common_vals', $%d::text", idx))
		args = append(args, *col.Stats.MostCommonVals)
		idx++
	}
	if col.Stats.MostCommonFreqs != nil {
		parts = append(parts, fmt.Sprintf("'most_common_freqs', $%d::text", idx))
		args = append(args, *col.Stats.MostCommonFreqs)
		idx++
	}
	if col.Stats.HistogramBounds != nil {
		parts = append(parts, fmt.Sprintf("'histogram_bounds', $%d::text", idx))
		args = append(args, *col.Stats.HistogramBounds)
		idx++
	}
	if col.Stats.Correlation != nil {
		parts = append(parts, fmt.Sprintf("'correlation', $%d::real", idx))
		args = append(args, float32(*col.Stats.Correlation))
		idx++
	}

	sql := "SELECT pg_restore_attribute_stats(" + strings.Join(parts, ", ") + ")"
	_, err := tx.Exec(ctx, sql, args...)
	return err
}

// PG <18 path: direct pg_statistic manipulation
func injectColumnStatsLegacy(ctx context.Context, tx pgx.Tx, cm columnMeta, col Column) error {
	// remove existing non-inherited stats
	_, err := tx.Exec(ctx, q("delete-column-stats-legacy"), cm.relOID, cm.attNum)
	if err != nil {
		return fmt.Errorf("delete old stats: %w", err)
	}

	nullFrac := float32(0)
	if col.Stats.NullFrac != nil {
		nullFrac = float32(*col.Stats.NullFrac)
	}
	nDistinct := float32(0)
	if col.Stats.NDistinct != nil {
		nDistinct = float32(*col.Stats.NDistinct)
	}

	// build slot values; types without equality op (json, xml, ...) can't have MCV or histogram slots - staop is required there
	type slot struct {
		kind    int16
		op      uint32
		numbers string // empty or real[] literal
		values  string // empty or typed array literal
	}

	hasEqOp := cm.eqOpOID != 0
	slots := [5]slot{}

	// slot 1: MCV (stakind=1), needs equality op
	if hasEqOp && col.Stats.MostCommonVals != nil && col.Stats.MostCommonFreqs != nil {
		slots[0] = slot{kind: 1, op: cm.eqOpOID, numbers: *col.Stats.MostCommonFreqs, values: *col.Stats.MostCommonVals}
	}

	// slot 2: histogram (stakind=2), needs equality op for range comparison
	if hasEqOp && col.Stats.HistogramBounds != nil {
		slots[1] = slot{kind: 2, op: cm.eqOpOID, values: *col.Stats.HistogramBounds}
	}

	// slot 3: correlation (stakind=3), no operator needed
	if col.Stats.Correlation != nil {
		slots[2] = slot{kind: 3, numbers: fmt.Sprintf("{%v}", *col.Stats.Correlation)}
	}

	// types with spaces ("character varying", "timestamp with time zone") need quoting for ::type[] cast
	arrayCast := cm.typeName + "[]"
	if strings.Contains(cm.typeName, " ") {
		arrayCast = fmt.Sprintf(`"%s"[]`, cm.typeName)
	}

	// stavalues are anyarray and need explicit cast to the column's actual type
	var valueParts []string
	var args []any
	argN := 1

	addArg := func(v any) string {
		placeholder := fmt.Sprintf("$%d", argN)
		args = append(args, v)
		argN++
		return placeholder
	}

	// starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct
	valueParts = append(valueParts, addArg(cm.relOID), addArg(cm.attNum), "false", addArg(nullFrac), "0", addArg(nDistinct))

	for _, s := range slots {
		valueParts = append(valueParts, addArg(s.kind))
		valueParts = append(valueParts, addArg(s.op))

		if s.numbers != "" {
			valueParts = append(valueParts, addArg(s.numbers)+"::real[]")
		} else {
			valueParts = append(valueParts, "NULL")
		}

		if s.values != "" {
			valueParts = append(valueParts, addArg(s.values)+"::"+arrayCast)
		} else {
			valueParts = append(valueParts, "NULL")
		}
	}

	insertSQL := `INSERT INTO pg_statistic (
		starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct,
		stakind1, staop1, stanumbers1, stavalues1,
		stakind2, staop2, stanumbers2, stavalues2,
		stakind3, staop3, stanumbers3, stavalues3,
		stakind4, staop4, stanumbers4, stavalues4,
		stakind5, staop5, stanumbers5, stavalues5
	) VALUES (` + strings.Join(valueParts, ", ") + ")"

	_, err = tx.Exec(ctx, insertSQL, args...)
	if err != nil {
		return fmt.Errorf("insert pg_statistic: %w", err)
	}

	return nil
}

func hasColumnStats(snap *SchemaSnapshot) bool {
	for _, t := range snap.Tables {
		for _, c := range t.Columns {
			if c.Stats != nil {
				return true
			}
		}
	}
	return false
}

// Overlays node-specific stats onto tables/indexes/columns in snap
func ApplyNodeStats(snap *SchemaSnapshot, node string) error {
	var ns *NodeStats
	for i := range snap.NodeStats {
		if snap.NodeStats[i].Source == node {
			ns = &snap.NodeStats[i]
			break
		}
	}
	if ns == nil {
		return fmt.Errorf("node %q not found in snapshot (available: %s)", node, nodeSourceList(snap.NodeStats))
	}

	tableIdx := make(map[string]int, len(snap.Tables))
	for i := range snap.Tables {
		key := snap.Tables[i].Schema + "." + snap.Tables[i].Name
		tableIdx[key] = i
	}

	for _, nts := range ns.TableStats {
		key := nts.Schema + "." + nts.Table
		if ti, ok := tableIdx[key]; ok {
			stats := nts.Stats
			snap.Tables[ti].Stats = &stats
		}
	}

	for _, nis := range ns.IndexStats {
		key := nis.Schema + "." + nis.Table
		ti, ok := tableIdx[key]
		if !ok {
			continue
		}
		for j := range snap.Tables[ti].Indexes {
			if snap.Tables[ti].Indexes[j].Name == nis.IndexName {
				stats := nis.Stats
				snap.Tables[ti].Indexes[j].Stats = &stats
				break
			}
		}
	}

	for _, ncs := range ns.ColumnStats {
		key := ncs.Schema + "." + ncs.Table
		ti, ok := tableIdx[key]
		if !ok {
			continue
		}
		for j := range snap.Tables[ti].Columns {
			if snap.Tables[ti].Columns[j].Name == ncs.Column {
				stats := ncs.Stats
				snap.Tables[ti].Columns[j].Stats = &stats
				break
			}
		}
	}

	return nil
}

func CanInjectStats(snap *SchemaSnapshot) error {
	hasRelStats := false
	for _, t := range snap.Tables {
		if t.Stats != nil {
			hasRelStats = true
			break
		}
	}
	if !hasRelStats && !hasColumnStats(snap) {
		return errors.New("snapshot contains no statistics to inject")
	}
	return nil
}

func nodeSourceList(nodes []NodeStats) string {
	if len(nodes) == 0 {
		return "none"
	}
	s := ""
	for i, n := range nodes {
		if i > 0 {
			s += ", "
		}
		s += n.Source
	}
	return s
}
