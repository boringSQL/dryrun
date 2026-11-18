package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/dryrun"
)

type InjectResult struct {
	TablesUpdated        int      `json:"tables_updated"`
	IndexesUpdated       int      `json:"indexes_updated"`
	ColumnsUpdated       int      `json:"columns_updated"`
	ExtendedStatsUpdated int      `json:"extended_stats_updated"`
	AutovacuumDisabled   int      `json:"autovacuum_disabled"`
	PgRegresqlLoaded     bool     `json:"pg_regresql_loaded"`
	Warnings             []string `json:"warnings,omitempty"`
	Method               string   `json:"method"`
	// why no extended statistics were attempted; not a warning
	ExtendedStatsSkipped string `json:"extended_stats_skipped,omitempty"`
}

func (r *InjectResult) warn(format string, args ...any) {
	w := fmt.Sprintf(format, args...)
	r.Warnings = append(r.Warnings, w)
	slog.Warn(w)
}

func (r *InjectResult) warnExtendedStats(e *ExtendedStatsEntry, format string, args ...any) {
	r.warn("extended statistics %s.%s on %s.%s: %s",
		e.Statistics.Schema, e.Statistics.Name, e.Table.Schema, e.Table.Name, fmt.Sprintf(format, args...))
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

	// sessions default to read-only (connection guards); injection is the one
	// sanctioned writer and must opt out before its first query
	if _, err := tx.Exec(ctx, "SET TRANSACTION READ WRITE"); err != nil {
		return nil, fmt.Errorf("set transaction read write: %w", err)
	}

	result := &InjectResult{}
	if pgMajor >= 18 {
		result.Method = "pg_restore_relation_stats"
	} else {
		// legacy path writes pg_class/pg_statistic directly; managed AlloyDB
		// denies that. Skip rather than emit a permission-denied warning per row.
		flavor, err := DetectFlavorLive(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("detect target flavor: %w", err)
		}
		if !flavor.Capabilities().CatalogWritable {
			result.Method = "skipped"
			result.warn("stats injection skipped: %s denies direct catalog writes; use a PostgreSQL 18+ target for the pg_restore_*_stats path", flavor.Display())
			return result, nil
		}
		result.Method = "pg_class_update"
	}

	result.PgRegresqlLoaded = probePgRegresql(ctx, tx)
	srcVer := sourceVersionNum(a.Schema.PgVersion)

	var extStats []extendedStatsTarget
	if len(a.Planner.ExtendedStats) > 0 {
		ok, err := probeExtendedStatsRestore(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("probe pg_restore_extended_stats: %w", err)
		}
		if ok {
			extStats = matchExtendedStats(ctx, tx, a.Planner.ExtendedStats, result)
		} else {
			// Not a warning: it would trip the MCP plan-degraded hint on every
			// explain against a pre-19 target.
			result.ExtendedStatsSkipped = "target lacks pg_restore_extended_stats() (PostgreSQL 19+)"
			slog.Info("extended statistics not injected: "+result.ExtendedStatsSkipped,
				"objects", len(a.Planner.ExtendedStats))
		}
	}
	// ext-only tables still need autovacuum off, or ANALYZE wipes the injection
	tablesWithExtStats := make(map[QualifiedName]bool, len(extStats))
	for _, t := range extStats {
		tablesWithExtStats[t.entry.Table] = true
	}

	relStatsInjected := false
	for _, t := range a.Schema.Tables {
		qual := t.Qual()
		sz := a.SizingFor(qual)
		colsWithStats := collectColumnsWithStats(a, t)

		touched := sz != nil || len(colsWithStats) > 0 || tablesWithExtStats[qual]
		for _, idx := range t.Indexes {
			if touched {
				break
			}
			touched = a.IndexSizingFor(qual, idx.Name) != nil
		}

		// autovacuum would overwrite injected stats; disable before injecting.
		// partitioned parents reject storage params, and have no stats of their own
		if touched && t.PartitionInfo == nil {
			if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
				return disableAutovacuum(ctx, tx, t.Schema, t.Name)
			}); err != nil {
				result.warn("table %s.%s: disable autovacuum: %v", t.Schema, t.Name, err)
			} else {
				result.AutovacuumDisabled++
			}
		}

		if sz != nil {
			if sz.Reltuples <= 0 {
				result.warn("table %s.%s: snapshot reltuples is %g; injected plan will be degenerate", t.Schema, t.Name, sz.Reltuples)
			}
			if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
				return injectRelationStats(ctx, tx, pgMajor, srcVer, t.Schema, t.Name, sz.Relpages, sz.Reltuples, sz.RelAllVisible)
			}); err != nil {
				result.warn("table %s.%s: %v", t.Schema, t.Name, err)
			} else {
				result.TablesUpdated++
				relStatsInjected = true
			}
		}

		for _, idx := range t.Indexes {
			isz := a.IndexSizingFor(qual, idx.Name)
			if isz == nil {
				continue
			}
			if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
				return injectRelationStats(ctx, tx, pgMajor, srcVer, t.Schema, idx.Name, isz.Relpages, isz.Reltuples, nil)
			}); err != nil {
				result.warn("index %s.%s: %v", t.Schema, idx.Name, err)
			} else {
				result.IndexesUpdated++
			}
		}

		if len(colsWithStats) == 0 {
			continue
		}

		if pgMajor >= 18 {
			for _, cs := range colsWithStats {
				if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
					return injectColumnStatsPG18(ctx, tx, srcVer, t.Schema, t.Name, cs.col, cs.stats, cs.inherited)
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
					return injectColumnStatsLegacy(ctx, tx, cm, cs.stats, cs.inherited)
				}); err != nil {
					result.warn("column %s.%s.%s: %v", t.Schema, t.Name, cs.col.Name, err)
				} else {
					result.ColumnsUpdated++
				}
			}
		}
	}

	for _, t := range extStats {
		var out extendedStatsOutcome
		if err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
			var err error
			out, err = injectExtendedStats(ctx, tx, t)
			return err
		}); err != nil {
			result.warnExtendedStats(t.entry, "%v", err)
			continue
		}
		if out.applied > 0 {
			result.ExtendedStatsUpdated++
		}
		switch {
		case len(out.missed) > 0:
			result.warnExtendedStats(t.entry, "%s did not load; see the server log", strings.Join(out.missed, ", "))
		case out.failed:
			result.warnExtendedStats(t.entry, "restore reported a failure; see the server log")
		}
	}

	// without pg_regresql's hook, injected relation stats get rescaled to
	// physical size and collapse row estimates
	if relStatsInjected && !result.PgRegresqlLoaded {
		result.warn("pg_regresql not loaded: injected relpages/reltuples/relallvisible may be ignored (rescaled to physical size); LOAD pg_regresql before EXPLAIN")
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	slog.Info("stats injection complete",
		"tables", result.TablesUpdated,
		"indexes", result.IndexesUpdated,
		"columns", result.ColumnsUpdated,
		"extended_stats", result.ExtendedStatsUpdated,
		"autovacuum_disabled", result.AutovacuumDisabled,
		"pg_regresql_loaded", result.PgRegresqlLoaded,
		"method", result.Method,
	)
	return result, nil
}

func disableAutovacuum(ctx context.Context, tx pgx.Tx, schemaName, relName string) error {
	ident := pgx.Identifier{schemaName, relName}.Sanitize()
	_, err := tx.Exec(ctx, "ALTER TABLE "+ident+" SET (autovacuum_enabled=false)")
	return err
}

func probePgRegresql(ctx context.Context, tx pgx.Tx) bool {
	var loaded bool
	if err := tx.QueryRow(ctx, q("probe-pg-regresql")).Scan(&loaded); err != nil {
		return false
	}
	return loaded
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
	col       Column
	stats     *ColumnStats
	inherited bool
}

func collectColumnsWithStats(a *AnnotatedSchema, t Table) []colWithStats {
	qual := t.Qual()
	var out []colWithStats
	for _, c := range t.Columns {
		if e := a.ColumnStatsEntry(qual, c.Name); e != nil {
			out = append(out, colWithStats{col: c, stats: &e.Stats, inherited: e.Inherited})
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

// 'version' is the server_version_num the stats came from (pg_dump sends the
// source's); nil when the banner doesn't parse, and a NULL pair is skipped
func sourceVersionNum(banner string) *int {
	v, err := dryrun.ParsePgVersion(banner)
	if err != nil {
		return nil
	}
	n := v.Major*10000 + v.Minor
	return &n
}

func injectRelationStats(ctx context.Context, tx pgx.Tx, pgMajor int, srcVer *int, schemaName, relName string, relpages int64, reltuples float64, relAllVisible *int64) error {
	if pgMajor >= 18 {
		_, err := tx.Exec(ctx, q("restore-relation-stats-pg18"),
			srcVer, schemaName, relName, relpages, float32(reltuples), relAllVisible)
		return err
	}

	tag, err := tx.Exec(ctx, q("update-relation-stats-legacy"),
		reltuples, relpages, relName, schemaName, relAllVisible)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("relation %s.%s not found in target database", schemaName, relName)
	}
	return nil
}

func injectColumnStatsPG18(ctx context.Context, tx pgx.Tx, srcVer *int, schemaName, tableName string, col Column, s *ColumnStats, inherited bool) error {
	parts := []string{
		"'version', $1::int",
		"'schemaname', $2::text",
		"'relname', $3::text",
		"'attname', $4::text",
		"'inherited', $5::boolean",
	}
	args := []any{srcVer, schemaName, tableName, col.Name, inherited}
	idx := 6

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
	}

	sql := "SELECT pg_restore_attribute_stats(" + strings.Join(parts, ", ") + ")"
	_, err := tx.Exec(ctx, sql, args...)
	return err
}

func injectColumnStatsLegacy(ctx context.Context, tx pgx.Tx, cm columnMeta, s *ColumnStats, inherited bool) error {
	_, err := tx.Exec(ctx, q("delete-column-stats-legacy"), cm.relOID, cm.attNum, inherited)
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
	) VALUES ($1, $2, $3, $4, 0, $5,
		0, 0, 0, NULL, NULL,
		0, 0, 0, NULL, NULL,
		$6, 0, 0, $7::real[], NULL,
		0, 0, 0, NULL, NULL,
		0, 0, 0, NULL, NULL)`

	if _, err := tx.Exec(ctx, insertSQL, cm.relOID, cm.attNum, inherited, nullFrac, nDistinct, kind3, correlation); err != nil {
		return fmt.Errorf("insert pg_statistic: %w", err)
	}
	return nil
}

func probeExtendedStatsRestore(ctx context.Context, tx pgx.Tx) (bool, error) {
	var ok bool
	err := tx.QueryRow(ctx, q("probe-extended-stats-restore")).Scan(&ok)
	return ok, err
}

type (
	// a snapshot entry proven to match a target statistics object
	extendedStatsTarget struct {
		entry   *ExtendedStatsEntry
		oid     uint32
		stxKeys []int16
	}

	extendedStatsOutcome struct {
		applied int
		missed  []string
		failed  bool
	}
)

// Runs before any write so autovacuum is only disabled for tables whose
// statistics will actually be restored. The restore function has no column
// names, so the key list must match the snapshot exactly; dropped columns may
// still shift attnums, which injection remaps.
func matchExtendedStats(ctx context.Context, tx pgx.Tx, entries []ExtendedStatsEntry, result *InjectResult) []extendedStatsTarget {
	var out []extendedStatsTarget
	for i := range entries {
		e := &entries[i]
		t := extendedStatsTarget{entry: e}
		var definition string
		err := runSavepoint(ctx, tx, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, q("lookup-extended-stats-layout"),
				e.Statistics.Schema, e.Statistics.Name, e.Table.Schema, e.Table.Name,
			).Scan(&t.oid, &definition, &t.stxKeys)
		})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			result.warnExtendedStats(e, "statistics object not found in target database")
		case err != nil:
			result.warnExtendedStats(e, "look up target layout: %v", err)
		case definition != e.Definition:
			result.warnExtendedStats(e, "key layout differs from the target (snapshot %q, target %q)", e.Definition, definition)
		default:
			out = append(out, t)
		}
	}
	return out
}

// Clears the target object first: the restore function leaves unspecified
// kinds alone, so a masked MCV would otherwise keep local data. A false return
// is a partial success, so read back which kinds landed.
func injectExtendedStats(ctx context.Context, tx pgx.Tx, t extendedStatsTarget) (extendedStatsOutcome, error) {
	e := t.entry
	ndistinct, dependencies := e.NDistinct, e.Dependencies
	if !slices.Equal(e.StxKeys, t.stxKeys) {
		var err error
		if ndistinct, err = remapPayload(ndistinct, e.StxKeys, t.stxKeys, remapNDistinct); err != nil {
			return extendedStatsOutcome{}, fmt.Errorf("n_distinct: %w", err)
		}
		if dependencies, err = remapPayload(dependencies, e.StxKeys, t.stxKeys, remapDependencies); err != nil {
			return extendedStatsOutcome{}, fmt.Errorf("dependencies: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, q("clear-extended-stats"),
		e.Table.Schema, e.Table.Name, e.Statistics.Schema, e.Statistics.Name, e.Inherited,
	); err != nil {
		return extendedStatsOutcome{}, fmt.Errorf("clear existing stats: %w", err)
	}
	ok, err := restoreExtendedStats(ctx, tx, e, ndistinct, dependencies)
	if err != nil {
		return extendedStatsOutcome{}, err
	}

	var hasNDistinct, hasDependencies, hasMCV bool
	if err := tx.QueryRow(ctx, q("read-extended-stats-kinds"), t.oid, e.Inherited).
		Scan(&hasNDistinct, &hasDependencies, &hasMCV); err != nil {
		return extendedStatsOutcome{}, fmt.Errorf("read back stats: %w", err)
	}
	out := extendedStatsOutcome{failed: !ok}
	for _, c := range []struct {
		name      string
		want, got bool
	}{
		{"n_distinct", e.NDistinct != nil, hasNDistinct},
		{"dependencies", e.Dependencies != nil, hasDependencies},
		{"mcv", e.MCVValues != nil, hasMCV},
	} {
		switch {
		case c.want && c.got:
			out.applied++
		case c.want:
			out.missed = append(out.missed, c.name)
		}
	}
	return out, nil
}

func restoreExtendedStats(ctx context.Context, tx pgx.Tx, e *ExtendedStatsEntry, ndistinct, dependencies *string) (bool, error) {
	parts := []string{
		"'schemaname', $1::text",
		"'relname', $2::text",
		"'statistics_schemaname', $3::text",
		"'statistics_name', $4::text",
		"'inherited', $5::boolean",
	}
	args := []any{e.Table.Schema, e.Table.Name, e.Statistics.Schema, e.Statistics.Name, e.Inherited}
	add := func(name, typ, value string) {
		parts = append(parts, fmt.Sprintf("'%s', $%d::%s", name, len(args)+1, typ))
		args = append(args, value)
	}
	if ndistinct != nil {
		add("n_distinct", "pg_ndistinct", *ndistinct)
	}
	if dependencies != nil {
		add("dependencies", "pg_dependencies", *dependencies)
	}
	// all three MCV arrays or none: the function ignores a partial set
	if e.MCVValues != nil && e.MCVFreqs != nil && e.MCVBaseFreqs != nil {
		add("most_common_vals", "text[]", *e.MCVValues)
		add("most_common_freqs", "float8[]", *e.MCVFreqs)
		add("most_common_base_freqs", "float8[]", *e.MCVBaseFreqs)
	}

	var ok bool
	err := tx.QueryRow(ctx,
		"SELECT pg_restore_extended_stats("+strings.Join(parts, ", ")+")", args...,
	).Scan(&ok)
	return ok, err
}

type ndistinctPayloadItem struct {
	Attributes []int16 `json:"attributes"`
	NDistinct  int64   `json:"ndistinct"`
}

type dependencyPayloadItem struct {
	Attributes []int16 `json:"attributes"`
	Dependency int16   `json:"dependency"`
	Degree     float64 `json:"degree"`
}

func remapPayload(p *string, src, dst []int16, remap func(string, []int16, []int16) (string, error)) (*string, error) {
	if p == nil {
		return nil, nil
	}
	v, err := remap(*p, src, dst)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// The restore function has no column names: positive values are source
// attnums, so rewrite each one to the target attnum at the same key position.
// Negative values are expression slots and keep their number once the key
// layouts matched.
func remapAttnum(a int16, src, dst []int16) (int16, error) {
	if a < 0 {
		return a, nil
	}
	for i, s := range src {
		if s == a {
			if i >= len(dst) {
				return 0, fmt.Errorf("no target key for source attnum %d", a)
			}
			return dst[i], nil
		}
	}
	return 0, fmt.Errorf("attnum %d is not a key of the statistics object", a)
}

func remapNDistinct(payload string, src, dst []int16) (string, error) {
	var items []ndistinctPayloadItem
	if err := json.Unmarshal([]byte(payload), &items); err != nil {
		return "", fmt.Errorf("parse payload: %w", err)
	}
	for i := range items {
		for j, a := range items[i].Attributes {
			r, err := remapAttnum(a, src, dst)
			if err != nil {
				return "", err
			}
			items[i].Attributes[j] = r
		}
	}
	b, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func remapDependencies(payload string, src, dst []int16) (string, error) {
	var items []dependencyPayloadItem
	if err := json.Unmarshal([]byte(payload), &items); err != nil {
		return "", fmt.Errorf("parse payload: %w", err)
	}
	for i := range items {
		for j, a := range items[i].Attributes {
			r, err := remapAttnum(a, src, dst)
			if err != nil {
				return "", err
			}
			items[i].Attributes[j] = r
		}
		d, err := remapAttnum(items[i].Dependency, src, dst)
		if err != nil {
			return "", err
		}
		items[i].Dependency = d
	}
	b, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func CanInjectStats(a *AnnotatedSchema) error {
	if a == nil || a.Planner == nil {
		return errors.New("annotated schema has no planner stats to inject")
	}
	if len(a.Planner.Tables) == 0 && len(a.Planner.Columns) == 0 && len(a.Planner.ExtendedStats) == 0 {
		return errors.New("planner snapshot is empty")
	}
	return nil
}
