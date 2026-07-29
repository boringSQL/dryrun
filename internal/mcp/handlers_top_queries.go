package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

// canonicalTruncateLen caps the SQL text embedded per entry.
const canonicalTruncateLen = 300

// snapshot-to-store only; MCP has no live pg_stat_statements connection.
func (s *Server) handleListTopQueries(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	s.mu.RLock()
	hist, key := s.history, s.snapshotKey
	s.mu.RUnlock()
	if hist == nil || key.ProjectID == "" {
		return errResult("no snapshot history available; capture with `dryrun snapshot query-stats` first"), nil
	}
	if note := s.historyNote(); note != nil {
		return errResult(*note), nil
	}

	snaps, err := hist.LatestQueryStats(ctx, key)
	if err != nil {
		return errResult(fmt.Sprintf("load query stats: %v", err)), nil
	}
	if len(snaps) == 0 {
		return structuredTextResult(
			listTopQueriesResult{Queries: []queryStatsEntry{}, Meta: s.newMeta("", nil)},
			s.wrapText("No query stats captured yet.", "capture with `dryrun snapshot query-stats` (or `dryrun init`/`snapshot take`, which capture it best-effort)")), nil
	}

	nodeFilter := getArg(req, "node")
	if nodeFilter != "" {
		var matched []schema.QueryStatsSnapshot
		var have []string
		for _, snap := range snaps {
			have = append(have, snap.Node.Source)
			if snap.Node.Source == nodeFilter {
				matched = append(matched, snap)
			}
		}
		if len(matched) == 0 {
			return errResult(fmt.Sprintf("no query stats for node %q (have: %s)", nodeFilter, strings.Join(have, ", "))), nil
		}
		snaps = matched
	}

	minCalls := int64(getFloatArg(req, "min_calls", 2))

	var entries []queryStatsEntry
	nodeExecTime := map[string]float64{}
	for _, snap := range snaps {
		capturedAt := snap.Node.Timestamp.Format(time.RFC3339)
		for _, q := range snap.Queries {
			if q.Calls < minCalls {
				continue
			}
			canonical := q.Canonical
			truncated := false
			if len(canonical) >= canonicalTruncateLen {
				canonical = canonical[:canonicalTruncateLen]
				truncated = true
			}
			var rowsPerCall float64
			if q.Calls > 0 {
				rowsPerCall = float64(q.Rows) / float64(q.Calls)
			}
			entries = append(entries, queryStatsEntry{
				Node:               snap.Node.Source,
				CapturedAt:         capturedAt,
				SchemaRefHash:      snap.SchemaRefHash,
				Fingerprint:        q.Fingerprint,
				Canonical:          canonical,
				CanonicalTruncated: truncated,
				Calls:              q.Calls,
				TotalExecTimeMs:    q.TotalExecTimeMs,
				MeanExecTimeMs:     q.MeanExecTimeMs,
				Rows:               q.Rows,
				RowsPerCall:        rowsPerCall,
			})
			nodeExecTime[snap.Node.Source] += q.TotalExecTimeMs
		}
	}

	// denominator is per-node: entries are never merged/averaged across nodes,
	// so a shared cross-node total would misrepresent each node's own share.
	for i := range entries {
		if total := nodeExecTime[entries[i].Node]; total > 0 {
			entries[i].PctOfTotalExecTime = entries[i].TotalExecTimeMs / total * 100
		}
	}

	switch getArg(req, "sort") {
	case "calls":
		sort.Slice(entries, func(i, j int) bool { return entries[i].Calls > entries[j].Calls })
	case "mean_time":
		sort.Slice(entries, func(i, j int) bool { return entries[i].MeanExecTimeMs > entries[j].MeanExecTimeMs })
	default:
		sort.Slice(entries, func(i, j int) bool { return entries[i].TotalExecTimeMs > entries[j].TotalExecTimeMs })
	}

	total := len(entries)
	if total == 0 {
		return structuredTextResult(
			listTopQueriesResult{Queries: []queryStatsEntry{}, Meta: s.newMeta("", nil)},
			s.wrapText(fmt.Sprintf("No queries with >= %d calls.", minCalls), "")), nil
	}
	offset := int(getFloatArg(req, "offset", 0))
	limit := limitArg(req)
	if offset >= total {
		return structuredTextResult(
			listTopQueriesResult{Queries: []queryStatsEntry{}, Count: total, Offset: offset, Meta: s.newMeta("", nil)},
			s.wrapText(fmt.Sprintf("%d querie(s) total. Offset %d is beyond the end.", total, offset), "")), nil
	}
	end := pageEnd(offset, limit, total)
	page := entries[offset:end]

	lines := make([]string, len(page))
	for i, e := range page {
		lines[i] = fmt.Sprintf("[%s] %d calls, %.1fms total, %.1fms mean: %s",
			e.Node, e.Calls, e.TotalExecTimeMs, e.MeanExecTimeMs, e.Canonical)
	}
	var body string
	if offset == 0 && end == total {
		body = fmt.Sprintf("%d querie(s):\n%s", total, strings.Join(lines, "\n"))
	} else {
		body = fmt.Sprintf("Showing %d-%d of %d querie(s):\n%s", offset+1, end, total, strings.Join(lines, "\n"))
	}
	return structuredTextResult(
		listTopQueriesResult{Queries: page, Count: total, Offset: offset, Meta: s.newMeta("", nil)},
		s.wrapText(body, "")), nil
}
