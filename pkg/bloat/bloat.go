package bloat

import (
	"math"
	"strconv"
	"strings"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

const (
	pageSize        = 8192
	btreeFillfactor = 0.9
	tupleOverhead   = 8 // item pointer + tuple header alignment, bytes
	defaultWidth    = 32

	pageHeaderSize    = 24  // PageHeaderData
	heapTupleOverhead = 28  // MAXALIGN'd tuple header (24) + item pointer (4)
	heapFillfactor    = 1.0 // heap default; overridden by reloptions

	toastThreshold    = 2000 // ~TOAST_TUPLE_THRESHOLD
	toastPointerWidth = 18   // varlena TOAST pointer
)

// Avg byte widths per type for btree tuple sizing
var typeWidths = map[string]int{
	"smallint":                    2,
	"int2":                        2,
	"integer":                     4,
	"int":                         4,
	"int4":                        4,
	"bigint":                      8,
	"int8":                        8,
	"real":                        4,
	"float4":                      4,
	"double precision":            8,
	"float8":                      8,
	"boolean":                     1,
	"bool":                        1,
	"date":                        4,
	"timestamp without time zone": 8,
	"timestamp":                   8,
	"timestamp with time zone":    8,
	"timestamptz":                 8,
	"uuid":                        16,
	"inet":                        19,
	"cidr":                        19,
	"macaddr":                     6,
	"oid":                         4,
	"numeric":                     16,
	"text":                        32,
	"character varying":           32,
	"varchar":                     32,
	"character":                   32,
	"char":                        32,
	"bpchar":                      32,
	"bytea":                       32,
	"jsonb":                       64,
	"json":                        64,
	"xml":                         64,
}

// INCLUDE columns live in the leaf tuple too; count them toward tuple width
func EstimateIndexBloat(sizing snapshot.IndexSizing, columns, includeColumns []string, table snapshot.Table, indexType string) (snapshot.BloatEstimate, bool) {
	if indexType != "btree" {
		return snapshot.BloatEstimate{}, false
	}
	if sizing.Reltuples <= 0 || sizing.Relpages <= 0 {
		return snapshot.BloatEstimate{}, false
	}

	colTypes := make(map[string]string, len(table.Columns))
	for _, c := range table.Columns {
		colTypes[c.Name] = c.TypeName
	}

	avgKeyWidth := 0
	for _, col := range columns {
		typeName, ok := colTypes[col]
		if !ok {
			// Expression column (e.g. lower(email)) - use default
			avgKeyWidth += defaultWidth
			continue
		}
		avgKeyWidth += lookupTypeWidth(typeName)
	}
	for _, col := range includeColumns {
		if typeName, ok := colTypes[col]; ok {
			avgKeyWidth += lookupTypeWidth(typeName)
		} else {
			avgKeyWidth += defaultWidth
		}
	}

	if avgKeyWidth == 0 {
		return snapshot.BloatEstimate{}, false
	}

	usable := float64(pageSize) * btreeFillfactor
	tupleSize := float64(tupleOverhead + avgKeyWidth)
	tuplesPerPage := usable / tupleSize
	expectedPages := max(int64(math.Ceil(sizing.Reltuples/tuplesPerPage)), 1)

	return snapshot.BloatEstimate{
		BloatRatio:    float64(sizing.Relpages) / float64(expectedPages),
		ExpectedPages: expectedPages,
		ActualPages:   sizing.Relpages,
		AvgTupleWidth: avgKeyWidth,
		SizeBytes:     sizing.Size,
	}, true
}

// colWidth is measured avg_width per column; absent columns fall back to declared type width.
func EstimateTableBloat(sizing snapshot.TableSizing, table snapshot.Table, colWidth map[string]int) (snapshot.BloatEstimate, bool) {
	if sizing.Reltuples <= 0 || sizing.Relpages <= 0 {
		return snapshot.BloatEstimate{}, false
	}

	rowWidth := 0
	for i := range table.Columns {
		c := &table.Columns[i]
		if w, ok := colWidth[c.Name]; ok && w > 0 {
			rowWidth += heapColWidth(w)
		} else {
			rowWidth += lookupTypeWidth(c.TypeName)
		}
	}
	if rowWidth == 0 {
		return snapshot.BloatEstimate{}, false
	}

	usable := float64(pageSize-pageHeaderSize) * tableFillfactor(table.Reloptions)
	tupleSize := float64(heapTupleOverhead + rowWidth)
	tuplesPerPage := usable / tupleSize
	expectedPages := max(int64(math.Ceil(sizing.Reltuples/tuplesPerPage)), 1)

	return snapshot.BloatEstimate{
		BloatRatio:    float64(sizing.Relpages) / float64(expectedPages),
		ExpectedPages: expectedPages,
		ActualPages:   sizing.Relpages,
		AvgTupleWidth: rowWidth,
		SizeBytes:     sizing.TableSize,
	}, true
}

// fillfactor from reloptions ("fillfactor=70" -> 0.70); heap default 100%.
func tableFillfactor(reloptions []string) float64 {
	for _, opt := range reloptions {
		if v, ok := strings.CutPrefix(opt, "fillfactor="); ok {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
				return float64(n) / 100
			}
		}
	}
	return heapFillfactor
}

// Fills entry.Bloat for tables and btree indexes, joining planner against schema;
// non-btree and pre-ANALYZE entries stay nil.
func Annotate(planner *snapshot.PlannerStatsSnapshot, sch *snapshot.SchemaSnapshot) {
	if planner == nil || sch == nil {
		return
	}

	tables := make(map[snapshot.QualifiedName]*snapshot.Table, len(sch.Tables))
	for i := range sch.Tables {
		tables[sch.Tables[i].Qual()] = &sch.Tables[i]
	}

	// measured column widths per table
	widths := make(map[snapshot.QualifiedName]map[string]int)
	for i := range planner.Columns {
		c := &planner.Columns[i]
		if c.Stats.AvgWidth == nil {
			continue
		}
		m := widths[c.Table]
		if m == nil {
			m = make(map[string]int)
			widths[c.Table] = m
		}
		m[c.Column] = *c.Stats.AvgWidth
	}

	for i := range planner.Tables {
		e := &planner.Tables[i]
		table := tables[e.Table]
		if table == nil {
			continue
		}
		if est, ok := EstimateTableBloat(e.Sizing, *table, widths[e.Table]); ok {
			e.Bloat = &est
		}
	}

	for i := range planner.Indexes {
		e := &planner.Indexes[i]
		table := tables[e.Table]
		if table == nil {
			continue
		}
		var idx *snapshot.Index
		for j := range table.Indexes {
			if table.Indexes[j].Name == e.Index {
				idx = &table.Indexes[j]
				break
			}
		}
		if idx == nil {
			continue
		}
		if est, ok := EstimateIndexBloat(e.Sizing, idx.Columns, idx.IncludeColumns, *table, idx.IndexType); ok {
			est.Approximate = idx.Predicate != nil || idx.HasExpressions
			e.Bloat = &est
		}
	}
}

// heapColWidth caps a TOASTed value to its in-heap pointer width.
func heapColWidth(avgWidth int) int {
	if avgWidth > toastThreshold {
		return toastPointerWidth
	}
	return avgWidth
}

// lookupTypeWidth returns the estimated byte width for a PostgreSQL type name.
func lookupTypeWidth(typeName string) int {
	normalized := strings.ToLower(strings.TrimSpace(typeName))

	// Strip parenthesized suffixes: varchar(255) -> varchar, numeric(10,2) -> numeric
	if idx := strings.IndexByte(normalized, '('); idx >= 0 {
		normalized = strings.TrimSpace(normalized[:idx])
	}

	// Strip array suffix
	normalized = strings.TrimSuffix(normalized, "[]")

	if w, ok := typeWidths[normalized]; ok {
		return w
	}
	return defaultWidth
}
