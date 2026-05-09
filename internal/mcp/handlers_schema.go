package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	// Formatted line plus sortable values for list_tables
	tableEntry struct {
		line string
		name string
		rows float64
		size int64
	}
)

func (s *Server) handleListTables(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaFilter := getArg(req, "schema")
	var entries []tableEntry
	for _, t := range snap.Tables {
		if schemaFilter != "" && t.Schema != schemaFilter {
			continue
		}
		line := t.Schema + "." + t.Name
		var rows float64
		var size int64
		stats := schema.EffectiveTableStats(&t, snap)
		if stats != nil {
			rows = stats.Reltuples
			size = stats.TableSize
			line += fmt.Sprintf(" (~%d rows)", int64(rows))
		}
		if t.PartitionInfo != nil {
			line += fmt.Sprintf(" [partitioned: %s(%s), %d parts]",
				t.PartitionInfo.Strategy, t.PartitionInfo.Key,
				len(t.PartitionInfo.Children))
		}
		if t.Comment != nil {
			line += " - " + *t.Comment
		}
		entries = append(entries, tableEntry{line: line, name: t.Schema + "." + t.Name, rows: rows, size: size})
	}

	switch getArg(req, "sort") {
	case "rows":
		sort.Slice(entries, func(i, j int) bool { return entries[i].rows > entries[j].rows })
	case "size":
		sort.Slice(entries, func(i, j int) bool { return entries[i].size > entries[j].size })
	default:
		sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	}

	total := len(entries)

	if total == 0 {
		return textResult(s.wrapText("No tables found.", "")), nil
	}

	offset := int(getFloatArg(req, "offset", 0))
	limit := int(getFloatArg(req, "limit", 50))

	if offset >= total {
		return textResult(s.wrapText(fmt.Sprintf("%d table(s) total. Offset %d is beyond the end.", total, offset), "")), nil
	}
	end := pageEnd(offset, limit, total)
	entries = entries[offset:end]

	lines := make([]string, len(entries))
	for i, e := range entries {
		lines[i] = e.line
	}

	var body string
	if offset == 0 && end == total {
		body = fmt.Sprintf("%d table(s):\n%s", total, strings.Join(lines, "\n"))
	} else {
		body = fmt.Sprintf("Showing %d-%d of %d table(s):\n%s", offset+1, end, total, strings.Join(lines, "\n"))
	}
	return textResult(s.wrapText(body, "")), nil
}

func (s *Server) handleDescribeTable(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	tableName := getArg(req, "table")
	schemaName := schemaArg(req)
	detail := argOr(req, "detail", "summary")

	for i := range snap.Tables {
		t := &snap.Tables[i]
		if t.Name == tableName && t.Schema == schemaName {
			var tableRows float64
			if stats := schema.EffectiveTableStats(t, snap); stats != nil {
				tableRows = stats.Reltuples
			}

			var profiles []map[string]any
			for _, col := range t.Columns {
				if p := schema.ProfileColumn(col, tableRows); p != nil {
					profiles = append(profiles, map[string]any{
						"column":  col.Name,
						"profile": p,
					})
				}
			}

			result := map[string]any{}

			switch detail {
			case "full":
				result["table"] = t
			case "stats":
				if stats := schema.EffectiveTableStats(t, snap); stats != nil {
					result["table_stats"] = stats
				}
			default:
				result["table"] = toCompactTable(t)
			}

			if len(profiles) > 0 {
				result["column_profiles"] = profiles
			}

			if len(snap.NodeStats) > 0 {
				var nodeBreakdown []map[string]any
				for _, ns := range snap.NodeStats {
					for _, ts := range ns.TableStats {
						if ts.Schema == schemaName && ts.Table == tableName {
							nodeBreakdown = append(nodeBreakdown, map[string]any{
								"source":    ns.Source,
								"timestamp": ns.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
								"stats":     ts.Stats,
							})
						}
					}
				}
				if len(nodeBreakdown) > 0 {
					result["node_breakdown"] = nodeBreakdown
				}
			}
			if t.PartitionInfo != nil {
				result["partition_summary"] = fmt.Sprintf(
					"PARTITIONED BY %s (%s) - %d partitions. "+
						"Always include '%s' in WHERE clauses for partition pruning.",
					t.PartitionInfo.Strategy, t.PartitionInfo.Key,
					len(t.PartitionInfo.Children), t.PartitionInfo.Key)
			}

			hint := ""
			for _, c := range t.Constraints {
				if c.Kind == schema.ConstraintForeignKey {
					hint = "This table has foreign keys — use find_related for JOIN patterns with related tables."
					break
				}
			}
			s.injectMeta(result, hint)
			return jsonResult(result), nil
		}
	}
	return errResult(fmt.Sprintf("table '%s.%s' not found", schemaName, tableName)), nil
}

func (s *Server) handleSearchSchema(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	q := strings.ToLower(getArg(req, "query"))
	var results []string

	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		if strings.Contains(strings.ToLower(t.Name), q) {
			comment := ""
			if t.Comment != nil {
				comment = " - " + *t.Comment
			}
			results = append(results, "TABLE "+qualified+comment)
		}
		for _, col := range t.Columns {
			if strings.Contains(strings.ToLower(col.Name), q) {
				results = append(results, fmt.Sprintf("COLUMN %s.%s (%s)", qualified, col.Name, col.TypeName))
			}
		}
		for _, idx := range t.Indexes {
			if strings.Contains(strings.ToLower(idx.Name), q) || strings.Contains(strings.ToLower(idx.Definition), q) {
				results = append(results, fmt.Sprintf("INDEX %s: %s", qualified, idx.Definition))
			}
		}
	}
	for _, v := range snap.Views {
		if strings.Contains(strings.ToLower(v.Name), q) {
			kind := "VIEW"
			if v.IsMaterialized {
				kind = "MATERIALIZED VIEW"
			}
			results = append(results, fmt.Sprintf("%s %s.%s", kind, v.Schema, v.Name))
		}
	}
	for _, f := range snap.Functions {
		if strings.Contains(strings.ToLower(f.Name), q) {
			results = append(results, fmt.Sprintf("FUNCTION %s.%s(%s)", f.Schema, f.Name, f.IdentityArgs))
		}
	}
	for _, e := range snap.Enums {
		if strings.Contains(strings.ToLower(e.Name), q) {
			results = append(results, fmt.Sprintf("ENUM %s.%s: [%s]", e.Schema, e.Name, strings.Join(e.Labels, ", ")))
		}
	}

	total := len(results)
	if total == 0 {
		return textResult(s.wrapText(fmt.Sprintf("No matches for '%s'.", getArg(req, "query")), "")), nil
	}

	offset := int(getFloatArg(req, "offset", 0))
	limit := int(getFloatArg(req, "limit", 30))

	if offset >= total {
		return textResult(s.wrapText(fmt.Sprintf("%d match(es) for '%s'. Offset %d is beyond the end.", total, getArg(req, "query"), offset), "")), nil
	}
	end := pageEnd(offset, limit, total)
	shown := results[offset:end]

	var body string
	if offset == 0 && end == total {
		body = fmt.Sprintf("%d match(es) for '%s':\n%s", total, getArg(req, "query"), strings.Join(shown, "\n"))
	} else {
		body = fmt.Sprintf("Showing %d-%d of %d match(es) for '%s':\n%s",
			offset+1, end, total, getArg(req, "query"), strings.Join(shown, "\n"))
	}
	return textResult(s.wrapText(body, "")), nil
}

func (s *Server) handleFindRelated(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	tableName := getArg(req, "table")
	schemaName := schemaArg(req)
	qualified := schemaName + "." + tableName

	var table *schema.Table
	for i := range snap.Tables {
		if snap.Tables[i].Name == tableName && snap.Tables[i].Schema == schemaName {
			table = &snap.Tables[i]
			break
		}
	}
	if table == nil {
		return errResult(fmt.Sprintf("table '%s' not found", qualified)), nil
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("Relationships for %s:\n", qualified))

	var outgoing []string
	for _, c := range table.Constraints {
		if c.Kind != schema.ConstraintForeignKey || c.FKTable == nil {
			continue
		}
		outgoing = append(outgoing, fmt.Sprintf("  %s(%s) -> %s(%s)",
			qualified, strings.Join(c.Columns, ", "), *c.FKTable, strings.Join(c.FKColumns, ", ")))
	}
	if len(outgoing) == 0 {
		lines = append(lines, "Outgoing FKs: none")
	} else {
		lines = append(lines, "Outgoing FKs:")
		lines = append(lines, outgoing...)
	}

	var incoming []string
	for _, other := range snap.Tables {
		for _, fk := range other.Constraints {
			if fk.Kind != schema.ConstraintForeignKey || fk.FKTable == nil || *fk.FKTable != qualified {
				continue
			}
			incoming = append(incoming, fmt.Sprintf("  %s.%s(%s) -> %s(%s)",
				other.Schema, other.Name, strings.Join(fk.Columns, ", "), qualified, strings.Join(fk.FKColumns, ", ")))
		}
	}
	lines = append(lines, "")
	if len(incoming) == 0 {
		lines = append(lines, "Incoming FKs: none")
	} else {
		lines = append(lines, "Incoming FKs:")
		lines = append(lines, incoming...)
	}

	return textResult(s.wrapText(strings.Join(lines, "\n"), "")), nil
}
