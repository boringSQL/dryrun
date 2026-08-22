package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/vacuum"
)

func (s *Server) handleDetect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	kind := argOr(req, "kind", "all")

	switch kind {
	case "stale_stats":
		return s.handleDetectStaleStats(ctx, req)
	case "unused_indexes":
		return s.handleDetectUnusedIndexes(ctx, req)
	case "anomalies":
		return s.handleDetectAnomalies(ctx, req)
	case "bloated_indexes":
		return s.handleDetectBloatedIndexes(ctx, req)
	case "bloated_tables":
		return s.handleDetectBloatedTables(ctx, req)
	case "all":
		return s.handleDetectAll(ctx, req)
	default:
		return errResult(fmt.Sprintf("unknown detect kind: %q", kind)), nil
	}
}

// schema/table extractors for filterByQual
func staleKey(e schema.StaleStatsEntry) (string, string)        { return e.Schema, e.Table }
func unusedKey(e schema.UnusedIndexEntry) (string, string)      { return e.Schema, e.Table }
func bloatKey(e schema.BloatedIndexEntry) (string, string)      { return e.Schema, e.Table }
func bloatTableKey(e schema.BloatedTableEntry) (string, string) { return e.Schema, e.Table }
func vacuumKey(e vacuum.VacuumHealth) (string, string)          { return e.Schema, e.Table }
func anomalyKey(m map[string]any) (string, string) {
	s, _ := m["schema"].(string)
	t, _ := m["table"].(string)
	return s, t
}

// caps never-analyzed and stale independently so it can provide more targeted advice
func capStaleStats(entries []schema.StaleStatsEntry, max int) (kept []schema.StaleStatsEntry, omitted int) {
	var never, stale []schema.StaleStatsEntry
	for _, e := range entries {
		if e.LastAnalyzedDaysAgo == nil {
			never = append(never, e)
		} else {
			stale = append(stale, e)
		}
	}
	neverKept, neverOmitted := capItems(never, max)
	staleKept, staleOmitted := capItems(stale, max)
	return append(neverKept, staleKept...), neverOmitted + staleOmitted
}

// Pre-validated re-run of one kind, uncapped, keeping any active filter.
func narrowNext(kind, schemaF, tableF string) []NextCall {
	args := map[string]any{"kind": kind, "limit": 0}
	if schemaF != "" {
		args["schema"] = schemaF
	}
	if tableF != "" {
		args["table"] = tableF
	}
	return []NextCall{{Tool: "detect", Args: args}}
}

func (s *Server) handleDetectAll(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	max := limitArg(req)
	threshold := getFloatArg(req, "threshold", 4.0)

	staleEntries := filterByQual(schema.DetectStaleStats(a, int64(7)), schemaF, tableF, staleKey)
	unusedEntries := filterByQual(schema.DetectUnusedIndexes(a), schemaF, tableF, unusedKey)
	bloatEntries := filterByQual(schema.DetectBloatedIndexes(a, threshold), schemaF, tableF, bloatKey)
	bloatTableEntries := filterByQual(schema.DetectBloatedTables(a, threshold), schemaF, tableF, bloatTableKey)
	rawAnomalies, anomalyNote := buildAnomalies(a)
	anomalies := filterByQual(rawAnomalies, schemaF, tableF, anomalyKey)

	staleKept, staleOmitted := capStaleStats(staleEntries, max)
	// cap before the hint so unattributedScansHint covers only shown rows
	anomaliesKept, anomaliesOmitted := capItems(anomalies, max)
	wrapper := map[string]any{
		"stale_stats":     cappedBlock(staleKept, staleOmitted, len(staleEntries)),
		"unused_indexes":  entryBlock(unusedEntries, max),
		"anomalies":       cappedBlock(anomaliesKept, anomaliesOmitted, len(anomalies)),
		"bloated_indexes": entryBlock(bloatEntries, max),
		"bloated_tables":  entryBlock(bloatTableEntries, max),
	}

	hint := ""
	switch {
	case len(staleEntries) > 0 && len(unusedEntries) > 0:
		hint = "Stale stats may cause bad plans; run ANALYZE. Unused indexes add write overhead; verify per-node index scans before dropping."
	case len(staleEntries) > 0:
		hint = "Stale stats may cause bad query plans; consider running ANALYZE."
	case len(unusedEntries) > 0:
		hint = "Unused indexes add write overhead. Verify index scans across all replicas before dropping."
	}
	if len(staleEntries)+len(unusedEntries)+len(bloatEntries)+len(bloatTableEntries)+len(anomalies) > 0 {
		filterNote = ""
	}
	hint = joinHints(hint, filterNote, anomalyNote, unattributedScansHint(anomaliesKept))

	// point next at the truncated categories only
	var next []NextCall
	for _, k := range []string{"stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "bloated_tables"} {
		if block, ok := wrapper[k].(map[string]any); ok && block["truncated"] == true {
			next = append(next, narrowNext(k, schemaF, tableF)...)
		}
	}
	if len(next) > 0 {
		if hint != "" {
			hint += " "
		}
		hint += fmt.Sprintf("Some categories capped at %d; _meta.next re-runs them uncapped, or narrow with schema=/table=.", max)
	}

	s.injectMeta(wrapper, hint, next)
	return jsonResult(wrapper), nil
}

func (s *Server) handleDetectStaleStats(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	entries := filterByQual(schema.DetectStaleStats(a, int64(7)), schemaF, tableF, staleKey)
	if len(entries) == 0 {
		return s.emptyKindResult("stale_stats", "No stale statistics detected.", filterNote), nil
	}

	total := len(entries)
	kept, omitted := capStaleStats(entries, limitArg(req))
	var lines []string
	for _, e := range kept {
		if e.LastAnalyzedDaysAgo == nil {
			lines = append(lines, fmt.Sprintf("  %s: %s.%s - never analyzed", e.Node, e.Schema, e.Table))
		} else {
			lines = append(lines, fmt.Sprintf("  %s: %s.%s - last analyzed %d days ago", e.Node, e.Schema, e.Table, *e.LastAnalyzedDaysAgo))
		}
	}
	body := fmt.Sprintf("Stale statistics (%d entries):\n%s", total, strings.Join(lines, "\n"))
	wrapper := map[string]any{"stale_stats": kept, "count": total}
	if omitted > 0 {
		body += fmt.Sprintf("\n\n%d more not shown; narrow with schema=/table= or re-run with limit=0.", omitted)
		wrapper["truncated"] = true
		wrapper["omitted"] = omitted
		s.injectMeta(wrapper,
			fmt.Sprintf("Showing first %d of %d; %d not shown. Narrow with schema=/table= or re-run with limit=0.", len(kept), total, omitted),
			narrowNext("stale_stats", schemaF, tableF))
	}
	return structuredTextResult(wrapper, body), nil
}

// Structured zero-entry result so schema-aware clients see {<kind>: [], count: 0}
// while thin clients keep the friendly prose; notes also reach _meta.hint.
func (s *Server) emptyKindResult(kind, text string, notes ...string) *mcp.CallToolResult {
	wrapper := map[string]any{kind: []any{}, "count": 0}
	hint := joinHints(notes...)
	s.injectMeta(wrapper, hint, nil)
	if hint != "" {
		text = joinHints(text, hint)
	}
	return structuredTextResult(wrapper, text)
}

func joinHints(parts ...string) string {
	var kept []string
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, " ")
}

func (s *Server) handleDetectUnusedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	entries := filterByQual(schema.DetectUnusedIndexes(a), schemaF, tableF, unusedKey)
	if len(entries) == 0 {
		return s.emptyKindResult("unused_indexes", "No unused indexes detected. All indexes have at least one scan recorded.", filterNote), nil
	}
	return cappedKindResult(s, "unused_indexes", entries, limitArg(req), schemaF, tableF), nil
}

func (s *Server) handleDetectAnomalies(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)

	// no node stats is the bigger reason, but a bad filter is still worth saying
	if a.Merged == nil {
		return s.emptyKindResult("anomalies", "No node statistics available for anomaly detection.", filterNote), nil
	}

	rawAnomalies, anomalyNote := buildAnomalies(a)
	anomalies := filterByQual(rawAnomalies, schemaF, tableF, anomalyKey)
	if len(anomalies) == 0 {
		return s.emptyKindResult("anomalies", "No anomalies detected.", filterNote, anomalyNote), nil
	}
	max := limitArg(req)
	shown, _ := capItems(anomalies, max)
	return cappedKindResult(s, "anomalies", anomalies, max, schemaF, tableF, anomalyNote, unattributedScansHint(shown)), nil
}

func (s *Server) handleDetectBloatedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	threshold := getFloatArg(req, "threshold", 4.0)
	entries := filterByQual(schema.DetectBloatedIndexes(a, threshold), schemaF, tableF, bloatKey)
	if len(entries) == 0 {
		return s.emptyKindResult("bloated_indexes", "No bloated indexes detected.", filterNote), nil
	}
	return cappedKindResult(s, "bloated_indexes", entries, limitArg(req), schemaF, tableF), nil
}

func (s *Server) handleDetectBloatedTables(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	threshold := getFloatArg(req, "threshold", 4.0)
	entries := filterByQual(schema.DetectBloatedTables(a, threshold), schemaF, tableF, bloatTableKey)
	if len(entries) == 0 {
		return s.emptyKindResult("bloated_tables", "No bloated tables detected.", filterNote), nil
	}
	return cappedKindResult(s, "bloated_tables", entries, limitArg(req), schemaF, tableF), nil
}

func cappedKindResult[T any](s *Server, kind string, entries []T, max int, schemaF, tableF string, notes ...string) *mcp.CallToolResult {
	kept, omitted := capItems(entries, max)
	wrapper := map[string]any{
		kind:    kept,
		"count": len(entries),
	}
	var hint string
	var next []NextCall
	if omitted > 0 {
		wrapper["truncated"] = true
		wrapper["omitted"] = omitted
		hint = fmt.Sprintf("Showing first %d of %d; %d not shown. Narrow with schema=/table= or re-run with limit=0.", len(kept), len(entries), omitted)
		next = narrowNext(kind, schemaF, tableF)
	}
	hint = joinHints(append([]string{hint}, notes...)...)
	if hint != "" || len(next) > 0 {
		s.injectMeta(wrapper, hint, next)
	}
	return jsonResult(wrapper)
}

func (s *Server) handleVacuumHealth(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	results := filterByQual(vacuum.AnalyzeVacuumHealth(a), schemaF, tableF, vacuumKey)
	if len(results) == 0 {
		return structuredTextResult(
			vacuumHealthResult{Entries: []vacuum.VacuumHealth{}, Meta: s.newMeta(filterNote, nil)},
			s.wrapText("No vacuum health concerns found.", filterNote)), nil
	}

	kept, omitted := capItems(results, limitArg(req))
	out := vacuumHealthResult{Entries: kept, Count: len(results)}
	hint := ""
	if omitted > 0 {
		out.Truncated = true
		out.Omitted = omitted
		hint = fmt.Sprintf("Showing first %d of %d; %d not shown. Narrow with schema=/table= or re-run with limit=0.", len(kept), len(results), omitted)
	}
	out.Meta = s.newMeta(hint, nil)
	return jsonResult(out), nil
}
