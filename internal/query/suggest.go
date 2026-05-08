package query

import (
	"fmt"
	"strings"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/schema"
)

type IndexSuggestion struct {
	Table            string   `json:"table"`
	IndexType        string   `json:"index_type"`
	Columns          []string `json:"columns"`
	IncludeColumns   []string `json:"include_columns"`
	PartialPredicate *string  `json:"partial_predicate,omitempty"`
	DDL              string   `json:"ddl"`
	Rationale        string   `json:"rationale"`
	EstimatedImpact  string   `json:"estimated_impact"`
}

func SuggestIndex(sql string, snap *schema.SchemaSnapshot, plan *PlanNode, pgVersion *dryrun.PgVersion) ([]IndexSuggestion, error) {
	parsed, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	var suggestions []IndexSuggestion

	if plan != nil {
		suggestFromPlan(plan, snap, pgVersion, &suggestions)
	}
	suggestFromQueryStructure(parsed, snap, pgVersion, &suggestions)
	dedupSuggestions(&suggestions)

	return suggestions, nil
}

func suggestFromPlan(node *PlanNode, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion, suggestions *[]IndexSuggestion) {
	if node.NodeType == "Seq Scan" && node.PlanRows >= 1000 && node.RelationName != nil {
		tableName := *node.RelationName
		schemaName := "public"
		if node.Schema != nil {
			schemaName = *node.Schema
		}

		var table *schema.Table
		for i := range snap.Tables {
			if snap.Tables[i].Name == tableName && snap.Tables[i].Schema == schemaName {
				table = &snap.Tables[i]
				break
			}
		}

		if node.Filter != nil {
			col := extractSuggestFilterColumn(*node.Filter)
			if col != "" && !hasLeadingIndex(table, col) {
				idxType := chooseIndexType(table, col)
				qualified := schemaName + "." + tableName
				idxName := fmt.Sprintf("idx_%s_%s", tableName, col)
				*suggestions = append(*suggestions, IndexSuggestion{
					Table:     qualified,
					IndexType: idxType,
					Columns:   []string{col},
					DDL: fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s USING %s(%s);",
						idxName, qualified, idxType, col),
					Rationale:       fmt.Sprintf("Seq scan on '%s' filtering on '%s' (~%d rows)", qualified, col, int64(node.PlanRows)),
					EstimatedImpact: estimateImpact(node.PlanRows),
				})
			}
		}
	}

	if node.NodeType == "Sort" && node.PlanRows >= 5000 && len(node.SortKey) > 0 {
		if info := findTableInSubtree(node); info != nil {
			schemaName, tableName := info[0], info[1]
			var cols []string
			for _, k := range node.SortKey {
				cols = append(cols, strings.Fields(k)[0])
			}
			qualified := schemaName + "." + tableName
			colList := strings.Join(cols, ", ")
			idxName := fmt.Sprintf("idx_%s_%s", tableName, cols[0])
			*suggestions = append(*suggestions, IndexSuggestion{
				Table:     qualified,
				IndexType: "btree",
				Columns:   cols,
				DDL: fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s(%s);",
					idxName, qualified, colList),
				Rationale:       fmt.Sprintf("Sort on ~%d rows could be avoided with an index on (%s)", int64(node.PlanRows), colList),
				EstimatedImpact: "eliminates sort step",
			})
		}
	}

	for i := range node.Children {
		suggestFromPlan(&node.Children[i], snap, pgVersion, suggestions)
	}
}

func suggestFromQueryStructure(parsed *ParsedQuery, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion, suggestions *[]IndexSuggestion) {
	for _, fc := range parsed.Info.FilterColumns {
		var ref *ReferencedTable
		if fc.Table != nil {
			alias := *fc.Table
			for i := range parsed.Info.Tables {
				t := &parsed.Info.Tables[i]
				if (t.Alias != nil && *t.Alias == alias) || t.Name == alias {
					ref = t
					break
				}
			}
		} else if len(parsed.Info.Tables) == 1 {
			ref = &parsed.Info.Tables[0]
		}
		if ref == nil {
			continue
		}

		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}

		var table *schema.Table
		for i := range snap.Tables {
			if snap.Tables[i].Name == ref.Name && snap.Tables[i].Schema == schemaName {
				table = &snap.Tables[i]
				break
			}
		}
		if table == nil {
			continue
		}

		isLarge := table.Stats != nil && table.Stats.Reltuples >= 1000
		if isLarge && !hasLeadingIndex(table, fc.Column) {
			idxType := chooseIndexType(table, fc.Column)
			qualified := table.Schema + "." + table.Name
			idxName := fmt.Sprintf("idx_%s_%s", table.Name, fc.Column)
			*suggestions = append(*suggestions, IndexSuggestion{
				Table:     qualified,
				IndexType: idxType,
				Columns:   []string{fc.Column},
				DDL: fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s USING %s(%s);",
					idxName, qualified, idxType, fc.Column),
				Rationale: fmt.Sprintf("WHERE clause filters on '%s' on table '%s' (~%d rows)",
					fc.Column, qualified, int64(table.Stats.Reltuples)),
				EstimatedImpact: estimateImpact(table.Stats.Reltuples),
			})
		}
	}
}

func extractSuggestFilterColumn(filter string) string {
	trimmed := strings.TrimSpace(filter)
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
	return token
}

func hasLeadingIndex(table *schema.Table, col string) bool {
	if table == nil {
		return false
	}
	for _, idx := range table.Indexes {
		if len(idx.Columns) > 0 && idx.Columns[0] == col {
			return true
		}
	}
	return false
}

func chooseIndexType(table *schema.Table, col string) string {
	if table == nil {
		return "btree"
	}
	for _, c := range table.Columns {
		if c.Name == col {
			ct := strings.ToLower(c.TypeName)
			if ct == "jsonb" || ct == "tsvector" {
				return "gin"
			}
			if strings.Contains(ct, "geometry") || strings.Contains(ct, "geography") || strings.Contains(ct, "range") {
				return "gist"
			}
		}
	}
	return "btree"
}

func estimateImpact(rowCount float64) string {
	switch {
	case rowCount >= 1_000_000:
		return "high - large table, index likely reduces query time significantly"
	case rowCount >= 10_000:
		return "medium - moderate table size, index should help"
	default:
		return "low - small table, index may or may not help"
	}
}

func dedupSuggestions(suggestions *[]IndexSuggestion) {
	seen := make(map[string]bool)
	n := 0
	for _, s := range *suggestions {
		key := s.Table + ":" + strings.Join(s.Columns, ",")
		if !seen[key] {
			seen[key] = true
			(*suggestions)[n] = s
			n++
		}
	}
	*suggestions = (*suggestions)[:n]
}
