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
	if kind == "all" && offsetArg(req) > 0 {
		return errResult("offset pages one kind at a time; pass kind= (the _meta.next calls already do)"), nil
	}

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
	case "vacuum_health":
		return s.handleDetectVacuumHealth(ctx, req)
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
func capStaleStats(entries []schema.StaleStatsEntry, max, offset int) (kept []schema.StaleStatsEntry, omitted int) {
	var never, stale []schema.StaleStatsEntry
	for _, e := range entries {
		if e.LastAnalyzedDaysAgo == nil {
			never = append(never, e)
		} else {
			stale = append(stale, e)
		}
	}
	neverKept, neverOmitted := pageItems(never, max, offset)
	staleKept, staleOmitted := pageItems(stale, max, offset)
	return append(append(make([]schema.StaleStatsEntry, 0, len(neverKept)+len(staleKept)), neverKept...), staleKept...), neverOmitted + staleOmitted
}

// Pre-validated next page of one kind, keeping any active filter. Every list in a kind
// pages by the same offset, so the page size is also the step.
func nextDetectPage(kind, schemaF, tableF string, max, offset int) []NextCall {
	args := map[string]any{"kind": kind, "limit": max, "offset": offset + max}
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
	vacuumEntries := withFindings(filterByQual(vacuum.AnalyzeVacuumHealth(a), schemaF, tableF, vacuumKey))
	rawAnomalies, anomalyNote := buildAnomalies(a)
	anomalies := filterByQual(rawAnomalies, schemaF, tableF, anomalyKey)

	staleKept, staleOmitted := capStaleStats(staleEntries, max, 0)
	// cap before the hint so unattributedScansHint covers only shown rows
	anomaliesKept, anomaliesOmitted := capItems(anomalies, max)
	wrapper := map[string]any{
		"stale_stats":     cappedBlock(staleKept, staleOmitted, len(staleEntries)),
		"unused_indexes":  unusedBlock(a, unusedEntries, max),
		"anomalies":       cappedBlock(anomaliesKept, anomaliesOmitted, len(anomalies)),
		"bloated_indexes": entryBlock(bloatEntries, max),
		"bloated_tables":  entryBlock(bloatTableEntries, max),
		"vacuum_health":   entryBlock(vacuumEntries, max),
	}

	hint := ""
	switch {
	case len(staleEntries) > 0 && len(unusedEntries) > 0:
		hint = "Stale stats may cause bad plans; run ANALYZE. Unused indexes add write overhead; verify per-node index scans before dropping."
	case len(staleEntries) > 0:
		hint = "Stale stats may cause bad query plans; consider running ANALYZE."
	case len(unusedEntries) > 0:
		hint = "Unused indexes add write overhead. Verify index scans across all replicas before dropping."
	case len(vacuumEntries) > 0:
		hint = "Autovacuum is not keeping up on some tables; dead tuples and last-vacuum times are on each vacuum_health entry."
	}
	if len(staleEntries)+len(unusedEntries)+len(bloatEntries)+len(bloatTableEntries)+len(anomalies)+len(vacuumEntries) > 0 {
		filterNote = ""
	}
	hint = joinHints(hint, filterNote, anomalyNote, unattributedScansHint(anomaliesKept))

	// point next at the truncated categories only
	var next []NextCall
	for _, k := range []string{"stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "bloated_tables", "vacuum_health"} {
		if block, ok := wrapper[k].(map[string]any); ok && block["truncated"] == true {
			next = append(next, nextDetectPage(k, schemaF, tableF, max, 0)...)
		}
	}
	if len(next) > 0 {
		if hint != "" {
			hint += " "
		}
		hint += fmt.Sprintf("Some categories capped at %d; _meta.next pages through them, or narrow with schema=/table=.", max)
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
	max, offset := limitArg(req), offsetArg(req)
	kept, omitted := capStaleStats(entries, max, offset)
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
	hint, next := "", []NextCall(nil)
	if omitted > 0 {
		body += fmt.Sprintf("\n\n%d more not shown; the next page is in _meta.next, or narrow with schema=/table=.", omitted)
		wrapper["truncated"] = true
		wrapper["omitted"] = omitted
		hint = fmt.Sprintf("Showing %d of %d; %d not shown. The next page is in _meta.next, or narrow with schema=/table=.", len(kept), total, omitted)
		next = nextDetectPage("stale_stats", schemaF, tableF, max, offset)
	}
	if offset > 0 {
		wrapper["offset"] = offset
		if len(kept) == 0 {
			hint = fmt.Sprintf("Offset %d is beyond the end (%d total).", offset, total)
		}
	}
	// same invariant as cappedKindResult: _meta carries the capture stamps
	s.injectMeta(wrapper, hint, next)
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
		if !a.HasIndexActivity() {
			return s.emptyKindResult("unused_indexes", notMeasuredIndexNote, filterNote), nil
		}
		return s.emptyKindResult("unused_indexes", "No unused indexes detected among indexes with recorded activity. Unique and primary key indexes are never listed.", filterNote), nil
	}
	return cappedKindResult(s, "unused_indexes", entries, limitArg(req), offsetArg(req), schemaF, tableF), nil
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
	max, offset := limitArg(req), offsetArg(req)
	shown, _ := pageItems(anomalies, max, offset)
	return cappedKindResult(s, "anomalies", anomalies, max, offset, schemaF, tableF, anomalyNote, unattributedScansHint(shown)), nil
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
	return cappedKindResult(s, "bloated_indexes", entries, limitArg(req), offsetArg(req), schemaF, tableF), nil
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
	return cappedKindResult(s, "bloated_tables", entries, limitArg(req), offsetArg(req), schemaF, tableF), nil
}

func cappedKindResult[T any](s *Server, kind string, entries []T, max, offset int, schemaF, tableF string, notes ...string) *mcp.CallToolResult {
	kept, omitted := pageItems(entries, max, offset)
	wrapper := map[string]any{
		kind:    kept,
		"count": len(entries),
	}
	var hint string
	var next []NextCall
	if omitted > 0 {
		wrapper["truncated"] = true
		wrapper["omitted"] = omitted
		hint = fmt.Sprintf("Showing %d of %d; %d not shown. The next page is in _meta.next, or narrow with schema=/table=.", len(kept), len(entries), omitted)
		next = nextDetectPage(kind, schemaF, tableF, max, offset)
	}
	if offset > 0 {
		wrapper["offset"] = offset
		if offset >= len(entries) {
			hint = fmt.Sprintf("Offset %d is beyond the end (%d total).", offset, len(entries))
		}
	}
	hint = joinHints(append([]string{hint}, notes...)...)
	// always: _meta carries the capture stamps the tool description promises,
	// which the answer that has findings needs at least as much as an empty one
	s.injectMeta(wrapper, hint, next)
	return jsonResult(wrapper)
}

// AnalyzeVacuumHealth returns a row per large table whether or not it found
// anything. detect reports offenders, so every path here filters to rows that
// carry a finding; the per-table picture (trigger points, effective knobs, last
// vacuum times) is describe_table's question, not a detection.
func withFindings(entries []vacuum.VacuumHealth) []vacuum.VacuumHealth {
	kept := make([]vacuum.VacuumHealth, 0, len(entries))
	for _, e := range entries {
		if len(e.Findings) > 0 {
			kept = append(kept, e)
		}
	}
	return kept
}

func (s *Server) handleDetectVacuumHealth(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaFilterArg(req)
	tableF := getArg(req, "table")
	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaF, tableF)
	entries := withFindings(filterByQual(vacuum.AnalyzeVacuumHealth(a), schemaF, tableF, vacuumKey))
	if len(entries) == 0 {
		return s.emptyKindResult("vacuum_health", "No vacuum health concerns found.", filterNote), nil
	}
	return cappedKindResult(s, "vacuum_health", entries, limitArg(req), offsetArg(req), schemaF, tableF), nil
}

const notMeasuredIndexNote = "not_measured: this snapshot has no per-index activity (idx_scan), so no index can be called unused. Take a snapshot from a live database to record it."

func unusedBlock(a *schema.AnnotatedSchema, entries []schema.UnusedIndexEntry, max int) map[string]any {
	block := entryBlock(entries, max)
	if !a.HasIndexActivity() {
		block["not_measured"] = true
		block["note"] = notMeasuredIndexNote
	}
	return block
}
