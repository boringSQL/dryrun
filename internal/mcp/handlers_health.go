package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

func (s *Server) handleCompareNodes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	tableName := getArg(req, "table")
	schemaName := schemaArg(req)
	qual := schema.QualifiedName{Schema: schemaName, Name: tableName}

	if a.Merged == nil {
		return textResult("No node statistics available. Import stats from multiple nodes first."), nil
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("Node comparison for %s.%s:\n", schemaName, tableName))

	sz := a.SizingFor(qual)
	for _, n := range a.Merged.Nodes {
		for _, ts := range n.Tables {
			if ts.Table != qual {
				continue
			}
			rt := 0.0
			tableSize := int64(0)
			if sz != nil {
				rt = sz.Reltuples
				tableSize = sz.TableSize
			}
			lines = append(lines, fmt.Sprintf("  %s: %.0f rows, seq_scan=%d, idx_scan=%d, size=%d",
				n.Node.Source, rt, ts.Activity.SeqScan, ts.Activity.IdxScan, tableSize))
		}
	}

	if len(lines) == 1 {
		return textResult(s.wrapText(fmt.Sprintf("No stats found for %s.%s across nodes.", schemaName, tableName), "")), nil
	}
	return textResult(s.wrapText(strings.Join(lines, "\n"), "")), nil
}

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

func (s *Server) handleDetectAll(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	staleDays := int64(7)
	staleEntries := schema.DetectStaleStats(a, staleDays)
	unusedEntries := schema.DetectUnusedIndexes(a)

	threshold := getFloatArg(req, "threshold", 4.0)
	bloatEntries := schema.DetectBloatedIndexes(a, threshold)

	anomalies := buildAnomalies(a)

	wrapper := map[string]any{
		"stale_stats":     map[string]any{"entries": staleEntries, "count": len(staleEntries)},
		"unused_indexes":  map[string]any{"entries": unusedEntries, "count": len(unusedEntries)},
		"anomalies":       map[string]any{"entries": anomalies, "count": len(anomalies)},
		"bloated_indexes": map[string]any{"entries": bloatEntries, "count": len(bloatEntries)},
	}
	hint := ""
	switch {
	case len(staleEntries) > 0 && len(unusedEntries) > 0:
		hint = "Stale stats may cause bad plans — run ANALYZE. Unused indexes add write overhead — verify with compare_nodes before dropping."
	case len(staleEntries) > 0:
		hint = "Stale stats may cause bad query plans — consider running ANALYZE."
	case len(unusedEntries) > 0:
		hint = "Unused indexes add write overhead. Use compare_nodes to verify across all replicas before dropping."
	}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
}

func (s *Server) handleDetectStaleStats(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	entries := schema.DetectStaleStats(a, int64(7))
	if len(entries) == 0 {
		return textResult("No stale statistics detected."), nil
	}

	var lines []string
	for _, e := range entries {
		if e.LastAnalyzedDaysAgo == nil {
			lines = append(lines, fmt.Sprintf("  %s: %s.%s - never analyzed", e.Node, e.Schema, e.Table))
		} else {
			lines = append(lines, fmt.Sprintf("  %s: %s.%s - last analyzed %d days ago", e.Node, e.Schema, e.Table, *e.LastAnalyzedDaysAgo))
		}
	}
	return textResult(fmt.Sprintf("Stale statistics (%d entries):\n%s", len(entries), strings.Join(lines, "\n"))), nil
}

func (s *Server) handleDetectUnusedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	entries := schema.DetectUnusedIndexes(a)
	if len(entries) == 0 {
		return textResult("No unused indexes detected. All indexes have at least one scan recorded."), nil
	}
	return jsonResult(map[string]any{
		"unused_indexes": entries,
		"count":          len(entries),
	}), nil
}

func (s *Server) handleDetectAnomalies(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	if a.Merged == nil {
		return textResult("No node statistics available for anomaly detection."), nil
	}

	anomalies := buildAnomalies(a)
	if len(anomalies) == 0 {
		return textResult("No anomalies detected."), nil
	}
	return jsonResult(anomalies), nil
}

func (s *Server) handleDetectBloatedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	threshold := getFloatArg(req, "threshold", 4.0)
	entries := schema.DetectBloatedIndexes(a, threshold)
	if len(entries) == 0 {
		return textResult("No bloated indexes detected."), nil
	}
	return jsonResult(map[string]any{
		"bloated_indexes": entries,
		"count":           len(entries),
	}), nil
}

func (s *Server) handleVacuumHealth(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	results := schema.AnalyzeVacuumHealth(a)

	if len(results) == 0 {
		return textResult(s.wrapText("No vacuum health concerns found.", "")), nil
	}
	wrapper := map[string]any{
		"vacuum_health": results,
		"count":         len(results),
	}
	s.injectMeta(wrapper, "", nil)
	return jsonResult(wrapper), nil
}
