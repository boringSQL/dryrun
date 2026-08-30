package mcp

import (
	"context"
	"encoding/json"
	"fmt"
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
