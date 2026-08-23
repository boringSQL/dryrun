package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/ddl"
	"github.com/boringsql/dryrun/pkg/vacuum"
)

var describeTableFields = []string{
	"columns", "indexes", "constraints", "stats", "partition_info",
	"column_profiles", "comment", "policies", "triggers", "reloptions",
	"rls_enabled", "ddl", "vacuum", "relations",
}

type (
	// Formatted line plus the structured entry and sortable values for list_tables
	tableEntry struct {
		line  string
		name  string
		rows  float64
		size  int64
		entry tableListEntry
	}
)

func (s *Server) handleListTables(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := a.Schema

	schemaFilter := getArg(req, "schema")
	var entries []tableEntry
	for _, t := range snap.Tables {
		if schemaFilter != "" && t.Schema != schemaFilter {
			continue
		}
		structured := tableListEntry{Schema: t.Schema, Name: t.Name, Comment: t.Comment}
		line := t.Schema + "." + t.Name
		var rows float64
		var size int64
		sizing := a.SizingFor(t.Qual())
		if sizing != nil {
			rows = sizing.Reltuples
			size = sizing.TableSize
			rowsEst := int64(rows)
			structured.RowsEstimate = &rowsEst
			structured.SizeBytes = &sizing.TableSize
			line += fmt.Sprintf(" (~%d rows)", int64(rows))
		}
		if t.PartitionInfo != nil {
			structured.Partitioned = &tablePartitionSummary{
				Strategy: string(t.PartitionInfo.Strategy),
				Key:      t.PartitionInfo.Key,
				Children: len(t.PartitionInfo.Children),
			}
			line += fmt.Sprintf(" [partitioned: %s(%s), %d parts]",
				t.PartitionInfo.Strategy, t.PartitionInfo.Key,
				len(t.PartitionInfo.Children))
		}
		if t.Comment != nil {
			line += " - " + *t.Comment
		}
		entries = append(entries, tableEntry{line: line, name: t.Schema + "." + t.Name, rows: rows, size: size, entry: structured})
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
		return structuredTextResult(
			listTablesResult{Tables: []tableListEntry{}, Meta: s.newMeta("", nil)},
			s.wrapText("No tables found.", "")), nil
	}

	offset := int(getFloatArg(req, "offset", 0))
	limit := limitArg(req)

	if offset >= total {
		return structuredTextResult(
			listTablesResult{Tables: []tableListEntry{}, Count: total, Offset: offset, Meta: s.newMeta("", nil)},
			s.wrapText(fmt.Sprintf("%d table(s) total. Offset %d is beyond the end.", total, offset), "")), nil
	}
	end := pageEnd(offset, limit, total)
	entries = entries[offset:end]

	lines := make([]string, len(entries))
	page := make([]tableListEntry, len(entries))
	for i, e := range entries {
		lines[i] = e.line
		page[i] = e.entry
	}

	var body string
	if offset == 0 && end == total {
		body = fmt.Sprintf("%d table(s):\n%s", total, strings.Join(lines, "\n"))
	} else {
		body = fmt.Sprintf("Showing %d-%d of %d table(s):\n%s", offset+1, end, total, strings.Join(lines, "\n"))
	}
	return structuredTextResult(
		listTablesResult{Tables: page, Count: total, Offset: offset, Meta: s.newMeta("", nil)},
		s.wrapText(body, "")), nil
}

func (s *Server) handleDescribeTable(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := a.Schema

	ref, miss := resolveTable(snap, req)
	if miss != nil {
		return miss, nil
	}
	detail := argOr(req, "detail", "summary")

	fields := getStringSliceArg(req, "fields")
	if fields != nil {
		known := map[string]bool{}
		for _, k := range describeTableFields {
			known[k] = true
		}
		for _, f := range fields {
			if !known[f] {
				return errResult(fmt.Sprintf("unknown field '%s'; valid: %s",
					f, strings.Join(describeTableFields, ", "))), nil
			}
		}
		// these sections live on the raw Table; bump to full so the merge picks them up
		for _, f := range []string{"policies", "triggers", "reloptions", "rls_enabled"} {
			if wantsField(fields, f) {
				detail = "full"
				break
			}
		}
	}

	t := ref.Table
	qual := t.Qual()
	sizing := a.SizingFor(qual)
	var tableRows float64
	if sizing != nil {
		tableRows = sizing.Reltuples
	}

	var profiles []map[string]any
	for _, col := range t.Columns {
		cs := a.ColumnStats(qual, col.Name)
		if p := schema.ProfileColumn(col, cs, tableRows); p != nil {
			profiles = append(profiles, map[string]any{
				"column":  col.Name,
				"profile": p,
			})
		}
	}

	result := map[string]any{}

	bloat := a.TableBloatFor(qual)
	switch detail {
	case "full":
		raw, err := json.Marshal(t)
		if err != nil {
			return errResult(fmt.Sprintf("serialization error: %v", err)), nil
		}
		_ = json.Unmarshal(raw, &result)
		// a table with no indexes or constraints is ordinary, and these fields
		// carry no omitempty, so nil arrives as null and fails the array type
		for _, k := range []string{"columns", "indexes", "constraints"} {
			if v, ok := result[k]; ok && v == nil {
				result[k] = []any{}
			}
		}
		if sizing != nil {
			result["stats"] = sizing
		}
		if bloat != nil {
			result["bloat"] = bloat
		}
	case "relations":
		result["schema"] = t.Schema
		result["name"] = t.Name
	case "stats":
		result["schema"] = t.Schema
		result["name"] = t.Name
		if sizing != nil {
			result["stats"] = sizing
		}
		if bloat != nil {
			result["bloat"] = bloat
		}
		if act := a.PrimaryActivity(qual); act != nil {
			result["activity"] = act
		}
	default:
		raw, err := json.Marshal(toCompactTable(t, sizing))
		if err != nil {
			return errResult(fmt.Sprintf("serialization error: %v", err)), nil
		}
		_ = json.Unmarshal(raw, &result)
	}

	if len(profiles) > 0 && detail != "relations" {
		result["column_profiles"] = profiles
	}

	// not in the summary default: it is a derived view, and the default response
	// is already the widest in the product
	if detail == "stats" || detail == "full" || wantsField(fields, "vacuum") {
		// detect reports offenders only, so this is the only home for the
		// trigger points, effective knobs and last-vacuum times of a table with
		// no concern. Nil below the analyzer's 10k-row floor.
		if vh := vacuum.AnalyzeVacuumHealthFor(a, qual); vh != nil {
			result["vacuum"] = vh
		}
	}

	var relCascades []edgeTarget
	relHint := ""
	// build it only if it will survive the fields whitelist below, or the hint
	// and follow-ups would describe a section that is not in the response
	if (detail == "relations" || wantsField(fields, "relations")) &&
		(fields == nil || wantsField(fields, "relations")) {
		var rel findRelatedResult
		rel, relHint, relCascades = buildRelations(snap, ref, limitArgOr(req, defaultMaxItems))
		result["relations"] = rel
	}

	wantNodeBreakdown := detail != "relations" &&
		(fields == nil || wantsField(fields, "indexes") || wantsField(fields, "stats"))
	if wantNodeBreakdown && a.Merged != nil {
		var nodeBreakdown []map[string]any
		for _, n := range a.Merged.Nodes {
			for _, ts := range n.Tables {
				if ts.Table != qual {
					continue
				}
				nodeBreakdown = append(nodeBreakdown, map[string]any{
					"source":    n.Node.Source,
					"timestamp": stamp(n.Node.Timestamp),
					"activity":  ts.Activity,
				})
			}
		}
		if len(nodeBreakdown) > 0 {
			result["node_breakdown"] = nodeBreakdown
		}
	}
	if t.PartitionInfo != nil && detail != "relations" {
		result["partition_summary"] = fmt.Sprintf(
			"PARTITIONED BY %s (%s) - %d partitions. "+
				"Always include '%s' in WHERE clauses for partition pruning.",
			t.PartitionInfo.Strategy, t.PartitionInfo.Key,
			len(t.PartitionInfo.Children), t.PartitionInfo.Key)

		// per-partition child sizing — Rust 60ca7e3
		var childSizing []map[string]any
		for _, ch := range t.PartitionInfo.Children {
			csz := a.SizingFor(schema.QualifiedName{Schema: ch.Schema, Name: ch.Name})
			if csz != nil {
				childSizing = append(childSizing, map[string]any{
					"schema": ch.Schema, "name": ch.Name, "sizing": csz,
				})
			}
		}
		if len(childSizing) > 0 {
			result["partition_child_sizing"] = childSizing
		}
	}

	selected := len(getStringSliceArg(req, "columns")) > 0
	if selected {
		if err := selectColumns(result, t, getStringSliceArg(req, "columns")); err != nil {
			return errResult(err.Error()), nil
		}
	}

	if fields != nil {
		allowed := map[string]bool{"schema": true, "name": true}
		for _, f := range fields {
			allowed[f] = true
		}
		for k := range result {
			if !allowed[k] {
				delete(result, k)
			}
		}
	}

	caps := capTableSections(result, t, limitArgOr(req, defaultMaxItems), selected)

	hint := relHint
	var next []NextCall
	// the follow-up needs the unquoted catalog name, not SQL text
	if len(relCascades) > 0 {
		next = append(next, NextCall{Tool: "describe_table", Args: map[string]any{
			"table": relCascades[0].name, "schema": relCascades[0].schema, "detail": "relations",
		}})
	}
	if rel, shown := result["relations"].(findRelatedResult); shown &&
		rel.OutgoingOmitted+rel.IncomingOmitted > 0 {
		next = append(next, NextCall{Tool: "describe_table", Args: map[string]any{
			"table": ref.Name, "schema": ref.Schema, "detail": "relations", "limit": 0,
		}})
	}
	if _, shown := result["relations"]; !shown {
		for _, c := range t.Constraints {
			if c.Kind == schema.ConstraintForeignKey {
				hint = joinHints(hint, "This table has foreign keys — detail=relations lists both sides with a pasteable JOIN and each ON DELETE action.")
				next = append(next, NextCall{Tool: "describe_table", Args: map[string]any{
					"table": ref.Name, "schema": ref.Schema, "detail": "relations",
				}})
				break
			}
		}
	}
	// asked for by name, and never capped: ddl missing a column would not
	// create the table
	if wantsField(fields, "ddl") {
		rendered, err := ddl.RenderTable(snap, t)
		if err != nil {
			// the other sections were asked for too, and they are fine
			result["ddl_error"] = err.Error()
		} else {
			result["ddl"] = rendered.SQL
			if len(rendered.Omitted) > 0 {
				result["ddl_omitted"] = rendered.Omitted
			}
			if !rendered.ParseChecked {
				result["ddl_parse_checked"] = false
				hint = joinHints(hint, "The snapshot is from a newer PostgreSQL than the bundled parser, so the ddl was not syntax-checked.")
			}
			if selected {
				hint = joinHints(hint, "The ddl is the whole table; the columns argument narrows the other sections only.")
			}
		}
	}

	if kinds := tableFindings(a, t); len(kinds) > 0 {
		hint = joinHints(hint, findingsHint(kinds))
		next = append(next, NextCall{
			Tool: "detect",
			Args: map[string]any{
				"kind": findingsKind(kinds), "table": ref.Name, "schema": ref.Schema,
				"threshold": findingsBloatThreshold, // pinned to what the hint promised
			},
		})
	}

	if capped := capHint(caps); capped != "" {
		hint = joinHints(capped, hint)
		args := map[string]any{"table": ref.Name, "schema": ref.Schema, "limit": 0}
		if d := getArg(req, "detail"); d != "" {
			args["detail"] = d
		}
		if fields != nil {
			args["fields"] = fields
		}
		next = append(next, NextCall{Tool: "describe_table", Args: args})
	}
	s.injectMeta(result, joinHints(ref.Note, hint), next)
	return jsonResult(result), nil
}

func (s *Server) handleSearchSchema(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	q := strings.ToLower(getArg(req, "query"))
	var results []string
	var matches []searchMatch

	add := func(line string, m searchMatch) {
		results = append(results, line)
		matches = append(matches, m)
	}

	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		if strings.Contains(strings.ToLower(t.Name), q) {
			comment := ""
			detail := ""
			if t.Comment != nil {
				comment = " - " + *t.Comment
				detail = *t.Comment
			}
			add("TABLE "+qualified+comment, searchMatch{Kind: "table", Object: qualified, Detail: detail})
		}
		for _, col := range t.Columns {
			if strings.Contains(strings.ToLower(col.Name), q) {
				add(fmt.Sprintf("COLUMN %s.%s (%s)", qualified, col.Name, col.TypeName),
					searchMatch{Kind: "column", Object: qualified + "." + col.Name, Detail: col.TypeName})
			}
		}
		for _, idx := range t.Indexes {
			if strings.Contains(strings.ToLower(idx.Name), q) || strings.Contains(strings.ToLower(idx.Definition), q) {
				add(fmt.Sprintf("INDEX %s: %s", qualified, idx.Definition),
					searchMatch{Kind: "index", Object: qualified + "." + idx.Name, Detail: idx.Definition})
			}
		}
	}
	for _, v := range snap.Views {
		if strings.Contains(strings.ToLower(v.Name), q) {
			kind, label := "view", "VIEW"
			if v.IsMaterialized {
				kind, label = "materialized_view", "MATERIALIZED VIEW"
			}
			add(fmt.Sprintf("%s %s.%s", label, v.Schema, v.Name),
				searchMatch{Kind: kind, Object: v.Schema + "." + v.Name})
		}
	}
	for _, f := range snap.Functions {
		if strings.Contains(strings.ToLower(f.Name), q) {
			add(fmt.Sprintf("FUNCTION %s.%s(%s)", f.Schema, f.Name, f.IdentityArgs),
				searchMatch{Kind: "function", Object: f.Schema + "." + f.Name, Detail: f.IdentityArgs})
		}
	}
	for _, e := range snap.Enums {
		if strings.Contains(strings.ToLower(e.Name), q) {
			add(fmt.Sprintf("ENUM %s.%s: [%s]", e.Schema, e.Name, strings.Join(e.Labels, ", ")),
				searchMatch{Kind: "enum", Object: e.Schema + "." + e.Name, Detail: strings.Join(e.Labels, ", ")})
		}
	}

	query := getArg(req, "query")
	total := len(results)
	if total == 0 {
		return structuredTextResult(
			searchSchemaResult{Query: query, Matches: []searchMatch{}, Meta: s.newMeta("", nil)},
			s.wrapText(fmt.Sprintf("No matches for '%s'.", query), "")), nil
	}

	offset := int(getFloatArg(req, "offset", 0))
	limit := limitArgOr(req, 30)

	if offset >= total {
		return structuredTextResult(
			searchSchemaResult{Query: query, Matches: []searchMatch{}, Count: total, Offset: offset, Meta: s.newMeta("", nil)},
			s.wrapText(fmt.Sprintf("%d match(es) for '%s'. Offset %d is beyond the end.", total, query, offset), "")), nil
	}
	end := pageEnd(offset, limit, total)
	shown := results[offset:end]

	var body string
	if offset == 0 && end == total {
		body = fmt.Sprintf("%d match(es) for '%s':\n%s", total, query, strings.Join(shown, "\n"))
	} else {
		body = fmt.Sprintf("Showing %d-%d of %d match(es) for '%s':\n%s",
			offset+1, end, total, query, strings.Join(shown, "\n"))
	}
	return structuredTextResult(
		searchSchemaResult{Query: query, Matches: matches[offset:end], Count: total, Offset: offset, Meta: s.newMeta("", nil)},
		s.wrapText(body, "")), nil
}

// buildRelations is describe_table's relations section: the FK neighbourhood of
// one table, each edge with a pasteable JOIN and its ON DELETE action. Returns
// the section, its hint, and the cascade to chase next if there is one.
func buildRelations(snap *schema.SchemaSnapshot, ref tableRef, max int) (findRelatedResult, string, []edgeTarget) {
	qualified := ref.Schema + "." + ref.Name
	local := qualify(ref.Schema, ref.Name)

	outgoing := outgoingEdges(ref.Table, local)
	incoming := incomingEdges(snap, qualified, local)

	// keep delete-affecting keys past the cap; a hub table may exceed it
	keptOut, outOmitted, outKept := capRetainingBy(outgoing, destructive, max)
	keptIn, inOmitted, inKept := capRetainingBy(incoming, destructive, max)

	result := findRelatedResult{
		Table:           local,
		Outgoing:        keptOut,
		OutgoingCount:   len(outgoing),
		OutgoingOmitted: outOmitted,
		Incoming:        keptIn,
		IncomingCount:   len(incoming),
		IncomingOmitted: inOmitted,
	}
	if result.Outgoing == nil {
		result.Outgoing = []relatedEdge{}
	}
	if result.Incoming == nil {
		result.Incoming = []relatedEdge{}
	}

	hint, cascades := deleteHint(incoming)
	if outOmitted+inOmitted > 0 {
		note := fmt.Sprintf("Omitted %d of %d relations. Re-run with limit=0 for all of them.",
			outOmitted+inOmitted, len(outgoing)+len(incoming))
		if outKept+inKept > 0 {
			note += " Keys that cascade or clear on delete are kept past the cap."
		}
		hint = joinHints(hint, note)
	}
	if len(outgoing)+len(incoming) == 0 {
		hint = joinHints(hint, "No declared foreign keys. Relations enforced in application code do not appear here.")
	}
	return result, hint, cascades
}

func wantsField(fields []string, name string) bool {
	for _, f := range fields {
		if f == name {
			return true
		}
	}
	return false
}
