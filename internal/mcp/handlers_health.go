package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
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
	case "all":
		return s.handleDetectAll(ctx, req)
	default:
		return errResult(fmt.Sprintf("unknown detect kind: %q", kind)), nil
	}
}

// schema/table extractors for filterByQual
func staleKey(e schema.StaleStatsEntry) (string, string)   { return e.Schema, e.Table }
func unusedKey(e schema.UnusedIndexEntry) (string, string) { return e.Schema, e.Table }
func bloatKey(e schema.BloatedIndexEntry) (string, string) { return e.Schema, e.Table }
func vacuumKey(e schema.VacuumHealth) (string, string)     { return e.Schema, e.Table }
func anomalyKey(m map[string]any) (string, string) {
	s, _ := m["schema"].(string)
	t, _ := m["table"].(string)
	return s, t
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

	schemaF := schemaArg(req)
	tableF := getArg(req, "table")
	max := limitArg(req)
	threshold := getFloatArg(req, "threshold", 4.0)

	staleEntries := filterByQual(schema.DetectStaleStats(a, int64(7)), schemaF, tableF, staleKey)
	unusedEntries := filterByQual(schema.DetectUnusedIndexes(a), schemaF, tableF, unusedKey)
	bloatEntries := filterByQual(schema.DetectBloatedIndexes(a, threshold), schemaF, tableF, bloatKey)
	anomalies := filterByQual(buildAnomalies(a), schemaF, tableF, anomalyKey)

	wrapper := map[string]any{
		"stale_stats":     entryBlock(staleEntries, max),
		"unused_indexes":  entryBlock(unusedEntries, max),
		"anomalies":       entryBlock(anomalies, max),
		"bloated_indexes": entryBlock(bloatEntries, max),
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

	// point next at the truncated categories while trancating
	var next []NextCall
	for _, k := range []string{"stale_stats", "unused_indexes", "anomalies", "bloated_indexes"} {
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

	schemaF := schemaArg(req)
	tableF := getArg(req, "table")
	entries := filterByQual(schema.DetectStaleStats(a, int64(7)), schemaF, tableF, staleKey)
	if len(entries) == 0 {
		return textResult("No stale statistics detected."), nil
	}

	total := len(entries)
	kept, omitted := capItems(entries, limitArg(req))
	var lines []string
	for _, e := range kept {
		if e.LastAnalyzedDaysAgo == nil {
			lines = append(lines, fmt.Sprintf("  %s: %s.%s - never analyzed", e.Node, e.Schema, e.Table))
		} else {
			lines = append(lines, fmt.Sprintf("  %s: %s.%s - last analyzed %d days ago", e.Node, e.Schema, e.Table, *e.LastAnalyzedDaysAgo))
		}
	}
	body := fmt.Sprintf("Stale statistics (%d entries):\n%s", total, strings.Join(lines, "\n"))
	if omitted > 0 {
		body += fmt.Sprintf("\n\n%d more not shown; narrow with schema=/table= or re-run with limit=0.", omitted)
	}
	return textResult(body), nil
}

func (s *Server) handleDetectUnusedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaArg(req)
	tableF := getArg(req, "table")
	entries := filterByQual(schema.DetectUnusedIndexes(a), schemaF, tableF, unusedKey)
	if len(entries) == 0 {
		return textResult("No unused indexes detected. All indexes have at least one scan recorded."), nil
	}
	return cappedKindResult(s, "unused_indexes", entries, limitArg(req), schemaF, tableF), nil
}

func (s *Server) handleDetectAnomalies(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	if a.Merged == nil {
		return textResult("No node statistics available for anomaly detection."), nil
	}

	schemaF := schemaArg(req)
	tableF := getArg(req, "table")
	anomalies := filterByQual(buildAnomalies(a), schemaF, tableF, anomalyKey)
	if len(anomalies) == 0 {
		return textResult("No anomalies detected."), nil
	}
	return cappedKindResult(s, "anomalies", anomalies, limitArg(req), schemaF, tableF), nil
}

func (s *Server) handleDetectBloatedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaArg(req)
	tableF := getArg(req, "table")
	threshold := getFloatArg(req, "threshold", 4.0)
	entries := filterByQual(schema.DetectBloatedIndexes(a, threshold), schemaF, tableF, bloatKey)
	if len(entries) == 0 {
		return textResult("No bloated indexes detected."), nil
	}
	return cappedKindResult(s, "bloated_indexes", entries, limitArg(req), schemaF, tableF), nil
}

func cappedKindResult[T any](s *Server, kind string, entries []T, max int, schemaF, tableF string) *mcp.CallToolResult {
	kept, omitted := capItems(entries, max)
	wrapper := map[string]any{
		kind:    kept,
		"count": len(entries),
	}
	if omitted > 0 {
		wrapper["truncated"] = true
		wrapper["omitted"] = omitted
		hint := fmt.Sprintf("Showing first %d of %d; %d not shown. Narrow with schema=/table= or re-run with limit=0.", len(kept), len(entries), omitted)
		s.injectMeta(wrapper, hint, narrowNext(kind, schemaF, tableF))
	}
	return jsonResult(wrapper)
}

func (s *Server) handleVacuumHealth(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF := schemaArg(req)
	tableF := getArg(req, "table")
	results := filterByQual(schema.AnalyzeVacuumHealth(a), schemaF, tableF, vacuumKey)
	if len(results) == 0 {
		return textResult(s.wrapText("No vacuum health concerns found.", "")), nil
	}

	kept, omitted := capItems(results, limitArg(req))
	wrapper := map[string]any{
		"vacuum_health": kept,
		"count":         len(results),
	}
	hint := ""
	if omitted > 0 {
		wrapper["truncated"] = true
		wrapper["omitted"] = omitted
		hint = fmt.Sprintf("Showing first %d of %d; %d not shown. Narrow with schema=/table= or re-run with limit=0.", len(kept), len(results), omitted)
	}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
}
