package query

import (
	"fmt"
	"strings"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/jit"
	"github.com/boringsql/dryrun/internal/schema"
)

type Advice struct {
	Issue            string            `json:"issue"`
	Severity         string            `json:"severity"`
	Table            *string           `json:"table,omitempty"`
	Recommendation   string            `json:"recommendation"`
	DDL              *string           `json:"ddl,omitempty"`
	VersionNote      *string           `json:"version_note,omitempty"`
	IndexSuggestions []IndexSuggestion `json:"index_suggestions,omitempty"`
}

// Walks plan tree, with per-node seq_scan breakdown when node stats present
func Advise(plan *PlanNode, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion) []Advice {
	var advice []Advice
	walkForAdvice(plan, snap, pgVersion, &advice)

	// attach index suggestions to advice entries that have a table
	if suggestions, err := SuggestIndex("", snap, plan, pgVersion); err == nil && len(suggestions) > 0 {
		for i := range advice {
			if advice[i].Table == nil {
				continue
			}
			for _, s := range suggestions {
				if s.Table == *advice[i].Table {
					advice[i].IndexSuggestions = append(advice[i].IndexSuggestions, s)
				}
			}
		}
	}

	if len(snap.NodeStats) > 0 {
		for i := range advice {
			if advice[i].Table != nil && strings.Contains(advice[i].Issue, "sequential scan") {
				breakdown := perNodeBreakdown(snap, *advice[i].Table)
				if breakdown != "" {
					advice[i].Recommendation += "\n\nPer-node breakdown:\n" + breakdown
				}
			}
		}
	}

	return advice
}

func perNodeBreakdown(snap *schema.SchemaSnapshot, qualified string) string {
	parts := strings.SplitN(qualified, ".", 2)
	if len(parts) != 2 {
		return ""
	}
	schemaName, tableName := parts[0], parts[1]

	var lines []string
	for _, ns := range snap.NodeStats {
		for _, ts := range ns.TableStats {
			if ts.Schema == schemaName && ts.Table == tableName {
				lines = append(lines, fmt.Sprintf("  %s: seq_scan=%d, idx_scan=%d", ns.Source, ts.Stats.SeqScan, ts.Stats.IdxScan))
			}
		}
	}
	return strings.Join(lines, "\n")
}

func walkForAdvice(node *PlanNode, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion, advice *[]Advice) {
	adviseSeqScan(node, snap, pgVersion, advice)
	adviseNestedLoopSeqScan(node, pgVersion, advice)
	adviseSort(node, snap, pgVersion, advice)
	adviseIndexScanBloat(node, snap, advice)
	adviseCTE(node, advice)

	for i := range node.Children {
		walkForAdvice(&node.Children[i], snap, pgVersion, advice)
	}
}

func adviseSeqScan(node *PlanNode, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion, advice *[]Advice) {
	if node.NodeType != "Seq Scan" || node.RelationName == nil || node.PlanRows < 10_000 {
		return
	}

	tableName := *node.RelationName
	schemaName := "public"
	if node.Schema != nil {
		schemaName = *node.Schema
	}
	qualified := schemaName + "." + tableName

	var table *schema.Table
	for i := range snap.Tables {
		if snap.Tables[i].Name == tableName && snap.Tables[i].Schema == schemaName {
			table = &snap.Tables[i]
			break
		}
	}

	filterCol := extractColumnFromFilter(node.Filter)

	var matchingIdx *schema.Index
	if table != nil && filterCol != "" {
		for i := range table.Indexes {
			if len(table.Indexes[i].Columns) > 0 && table.Indexes[i].Columns[0] == filterCol {
				matchingIdx = &table.Indexes[i]
				break
			}
		}
	}

	if matchingIdx != nil {
		// bloated index -> REINDEX, not ANALYZE
		if est, ok := schema.EstimateIndexBloat(*matchingIdx, *table); ok && est.BloatRatio > 3.0 {
			*advice = append(*advice, Advice{
				Issue:          fmt.Sprintf("sequential scan on '%s' (~%d rows) - index '%s' exists but appears bloated (%.1fx)", qualified, int64(node.PlanRows), matchingIdx.Name, est.BloatRatio),
				Severity:       "warning",
				Table:          strp(qualified),
				Recommendation: fmt.Sprintf("Index '%s' is estimated at %.1fx bloat. Rebuild it to restore accurate planner cost estimates.", matchingIdx.Name, est.BloatRatio),
				DDL:            strp(fmt.Sprintf("REINDEX CONCURRENTLY %s;", matchingIdx.Name)),
			})
			return
		}

		ddl := fmt.Sprintf("ANALYZE %s.%s;", schemaName, tableName)
		*advice = append(*advice, Advice{
			Issue:          fmt.Sprintf("sequential scan on '%s' (~%d rows) despite existing index", qualified, int64(node.PlanRows)),
			Severity:       "info",
			Table:          strp(qualified),
			Recommendation: "Run ANALYZE to update statistics. The planner may correctly prefer a seq scan if selectivity is low.",
			DDL:            strp(ddl),
		})
		return
	}

	var ddl *string
	recommendation := "Add an index on the filtered column(s) to avoid sequential scan."
	if filterCol != "" {
		var col *schema.Column
		if table != nil {
			for i := range table.Columns {
				if table.Columns[i].Name == filterCol {
					col = &table.Columns[i]
					break
				}
			}
		}
		var colType string
		if col != nil {
			colType = col.TypeName
		}
		idxType, rec := suggestIndexType(qualified, colType, filterCol)
		recommendation = rec

		// stats-aware refinements
		if col != nil && col.Stats != nil {
			tableRows := node.PlanRows
			if table != nil && table.Stats != nil && table.Stats.Reltuples > tableRows {
				tableRows = table.Stats.Reltuples
			}
			recommendation += statsAwareAdvice(col, filterCol, tableRows)
		}

		idxName := fmt.Sprintf("idx_%s_%s", tableName, filterCol)

		// partial index when column is mostly NULL
		if col != nil && col.Stats != nil && col.Stats.NullFrac != nil && *col.Stats.NullFrac > 0.5 {
			ddl = strp(fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s.%s USING %s(%s) WHERE %s IS NOT NULL;",
				idxName, schemaName, tableName, idxType, filterCol, filterCol))
		} else if dominant, freq, skewed := schema.HasSkewedDistribution(col.Stats, 0.5); skewed {
			_ = freq
			ddl = strp(fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s.%s USING %s(%s) WHERE %s != '%s';",
				idxName, schemaName, tableName, idxType, filterCol, filterCol, dominant))
		} else {
			ddl = strp(fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s.%s USING %s(%s);",
				idxName, schemaName, tableName, idxType, filterCol))
		}
	}

	*advice = append(*advice, Advice{
		Issue:          fmt.Sprintf("sequential scan on '%s' (~%d rows)", qualified, int64(node.PlanRows)),
		Severity:       "warning",
		Table:          strp(qualified),
		Recommendation: recommendation,
		DDL:            ddl,
		VersionNote:    versionNoteForIndex(pgVersion),
	})
}

func adviseNestedLoopSeqScan(node *PlanNode, pgVersion *dryrun.PgVersion, advice *[]Advice) {
	if node.NodeType != "Nested Loop" || len(node.Children) < 2 {
		return
	}
	inner := &node.Children[1]
	if inner.NodeType != "Seq Scan" || inner.PlanRows <= 100 {
		return
	}

	tableName := "unknown"
	if inner.RelationName != nil {
		tableName = *inner.RelationName
	}
	schemaName := "public"
	if inner.Schema != nil {
		schemaName = *inner.Schema
	}
	qualified := schemaName + "." + tableName

	filterCol := extractColumnFromFilter(inner.Filter)
	var ddl *string
	if filterCol != "" {
		ddl = strp(fmt.Sprintf("CREATE INDEX CONCURRENTLY idx_%s_%s ON %s.%s(%s);",
			tableName, filterCol, schemaName, tableName, filterCol))
	}

	*advice = append(*advice, Advice{
		Issue:          fmt.Sprintf("nested loop with sequential scan on inner side '%s' (~%d rows per loop)", qualified, int64(inner.PlanRows)),
		Severity:       "warning",
		Table:          strp(qualified),
		Recommendation: "Add an index on the join/filter column of the inner table to convert the seq scan to an index scan.",
		DDL:            ddl,
		VersionNote:    versionNoteForIndex(pgVersion),
	})
}

func adviseSort(node *PlanNode, _ *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion, advice *[]Advice) {
	if node.NodeType != "Sort" || node.PlanRows < 10_000 || len(node.SortKey) == 0 {
		return
	}

	tableInfo := findTableInSubtree(node)
	if tableInfo == nil {
		return
	}
	schemaName, tableName := tableInfo[0], tableInfo[1]
	qualified := schemaName + "." + tableName

	firstKey := strings.Fields(node.SortKey[0])[0]
	ddl := fmt.Sprintf("CREATE INDEX CONCURRENTLY idx_%s_%s ON %s.%s(%s);",
		tableName, firstKey, schemaName, tableName, strings.Join(node.SortKey, ", "))

	*advice = append(*advice, Advice{
		Issue:          fmt.Sprintf("sort on ~%d rows (keys: %s)", int64(node.PlanRows), strings.Join(node.SortKey, ", ")),
		Severity:       "info",
		Table:          strp(qualified),
		Recommendation: "Consider an index matching the sort order to avoid an explicit sort step.",
		DDL:            strp(ddl),
		VersionNote:    versionNoteForIndex(pgVersion),
	})
}

func extractColumnFromFilter(filter *string) string {
	if filter == nil {
		return ""
	}
	trimmed := strings.TrimSpace(*filter)
	trimmed = strings.TrimLeft(trimmed, "(")
	trimmed = strings.TrimRight(trimmed, ")")
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return ""
	}
	token := fields[0]
	if i := strings.LastIndex(token, "."); i >= 0 {
		token = token[i+1:]
	}
	for _, c := range token {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return ""
		}
	}
	if token == "" {
		return ""
	}
	return token
}

func suggestIndexType(table, colType, colName string) (string, string) {
	ct := strings.ToLower(colType)
	switch {
	case ct == "jsonb":
		e := jit.SuggestGIN(table, colName, colType)
		return "gin", e.Reason + "\n" + e.Note
	case ct == "tsvector":
		e := jit.SuggestGIN(table, colName, colType)
		return "gin", e.Reason
	case strings.Contains(ct, "geometry") || strings.Contains(ct, "geography"):
		e := jit.SuggestGiST(table, colName, colType)
		return "gist", e.Reason
	case strings.Contains(ct, "range") || ct == "tsrange" || ct == "daterange" || ct == "int4range":
		e := jit.SuggestGiST(table, colName, colType)
		return "gist", e.Reason
	default:
		return "btree", fmt.Sprintf("Add a B-tree index on '%s' for equality/range lookups.", colName)
	}
}

func versionNoteForIndex(pgVersion *dryrun.PgVersion) *string {
	if pgVersion == nil {
		return nil
	}
	if pgVersion.Major >= 13 {
		return strp("PG 13+: B-tree deduplication is enabled by default, reducing index size for low-cardinality columns.")
	}
	if pgVersion.Major >= 11 {
		return strp("PG 11+: Use INCLUDE for covering indexes to enable index-only scans.")
	}
	return nil
}

func adviseIndexScanBloat(node *PlanNode, snap *schema.SchemaSnapshot, advice *[]Advice) {
	if node.IndexName == nil {
		return
	}
	if node.NodeType != "Index Scan" && node.NodeType != "Index Only Scan" {
		return
	}

	tableName := ""
	schemaName := "public"
	if node.RelationName != nil {
		tableName = *node.RelationName
	}
	if node.Schema != nil {
		schemaName = *node.Schema
	}
	if tableName == "" {
		return
	}

	var table *schema.Table
	for i := range snap.Tables {
		if snap.Tables[i].Name == tableName && snap.Tables[i].Schema == schemaName {
			table = &snap.Tables[i]
			break
		}
	}
	if table == nil {
		return
	}

	indexName := *node.IndexName
	for _, idx := range table.Indexes {
		if idx.Name == indexName {
			est, ok := schema.EstimateIndexBloat(idx, *table)
			if ok && est.BloatRatio > 3.0 {
				qualified := schemaName + "." + tableName
				*advice = append(*advice, Advice{
					Issue:          fmt.Sprintf("index '%s' on '%s' appears bloated (%.1fx) - cost estimates may be inflated", indexName, qualified, est.BloatRatio),
					Severity:       "info",
					Table:          strp(qualified),
					Recommendation: fmt.Sprintf("Rebuild index to improve cost accuracy: REINDEX CONCURRENTLY %s;", indexName),
					DDL:            strp(fmt.Sprintf("REINDEX CONCURRENTLY %s;", indexName)),
				})
			}
			break
		}
	}
}

func statsAwareAdvice(col *schema.Column, filterCol string, tableRows float64) string {
	s := col.Stats
	if s == nil {
		return ""
	}
	var parts []string

	sel := schema.ColumnSelectivity(*col, tableRows)
	if s.NDistinct != nil {
		nd := *s.NDistinct
		if nd > 0 && nd <= 5 {
			parts = append(parts, fmt.Sprintf("\nColumn '%s' has only %.0f distinct values, so a full index has poor selectivity (%.0f%% of rows per value).", filterCol, nd, sel*100))
		} else if nd > 0 && nd <= 20 {
			parts = append(parts, fmt.Sprintf("\nColumn '%s' has %d distinct values (selectivity ~%.1f%%).", filterCol, int64(nd), sel*100))
		}
	}

	if dominant, freq, skewed := schema.HasSkewedDistribution(s, 0.5); skewed {
		parts = append(parts, fmt.Sprintf("Value '%s' dominates at ~%.0f%%. A partial index excluding it would be much smaller and faster.", dominant, freq*100))
	}

	if s.NullFrac != nil && *s.NullFrac > 0.5 {
		nullRows := int64(*s.NullFrac * tableRows)
		parts = append(parts, fmt.Sprintf("Column is %.0f%% NULL (~%d rows). Use a partial index WHERE %s IS NOT NULL to index only the non-null rows.", *s.NullFrac*100, nullRows, filterCol))
	}

	// random correlation hurts range scans
	if s.Correlation != nil {
		c := *s.Correlation
		if c > -0.3 && c < 0.3 && tableRows > 10_000 {
			parts = append(parts, fmt.Sprintf("Physical ordering is random (correlation: %.2f); index range scans will cause random I/O.", c))
		}
	}

	return strings.Join(parts, " ")
}

func adviseCTE(node *PlanNode, advice *[]Advice) {
	if node.NodeType != "CTE Scan" || node.CTEName == nil {
		return
	}
	rows := int64(node.PlanRows)
	if rows < 1000 {
		return
	}
	cteName := *node.CTEName
	e := jit.CTEMaterialized(cteName, rows)
	*advice = append(*advice, Advice{
		Issue:          fmt.Sprintf("materialized CTE '%s' (~%d rows)", cteName, rows),
		Severity:       "info",
		Recommendation: e.Reason + "\n" + e.Fix,
	})
}

func findTableInSubtree(node *PlanNode) []string {
	if node.Schema != nil && node.RelationName != nil {
		return []string{*node.Schema, *node.RelationName}
	}
	for i := range node.Children {
		if result := findTableInSubtree(&node.Children[i]); result != nil {
			return result
		}
	}
	return nil
}
