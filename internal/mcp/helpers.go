package mcp

import (
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

func textResult(text string) *mcp.CallToolResult {
	return mcp.NewToolResultText(text)
}

func jsonResult(v any) *mcp.CallToolResult {
	data, _ := json.MarshalIndent(v, "", "  ")
	return mcp.NewToolResultText(string(data))
}

func errResult(msg string) *mcp.CallToolResult {
	return mcp.NewToolResultError(msg)
}

func (s *Server) wrapText(body, hint string) string {
	header := fmt.Sprintf("PostgreSQL %s | %s | %s\n", s.pgDisplay(), s.databaseName(), s.modeStr())
	if hint != "" {
		return header + body + "\n\n> " + hint
	}
	return header + body
}

func (s *Server) injectMeta(val map[string]any, hint string) {
	meta := map[string]any{
		"pg_version": s.pgDisplay(),
		"database":   s.databaseName(),
		"mode":       s.modeStr(),
	}
	if hint != "" {
		meta["hint"] = hint
	}
	val["_meta"] = meta
}

// Round-trips payload through map so we can attach _meta without struct churn.
func (s *Server) metaJSONResult(payload any, key, hint string) *mcp.CallToolResult {
	data, err := json.Marshal(payload)
	if err != nil {
		return errResult(fmt.Sprintf("serialization error: %v", err))
	}
	wrapper := map[string]any{}
	var asObj map[string]any
	if err := json.Unmarshal(data, &asObj); err == nil && asObj != nil {
		wrapper = asObj
	} else if key != "" {
		var raw any
		_ = json.Unmarshal(data, &raw)
		wrapper[key] = raw
	}
	s.injectMeta(wrapper, hint)
	out, err := json.MarshalIndent(wrapper, "", "  ")
	if err != nil {
		return errResult(fmt.Sprintf("serialization error: %v", err))
	}
	return mcp.NewToolResultText(string(out))
}

// Shallow-copy snap, retaining tables matching filters. Empty filter = no filtering on that axis.
func filterSnap(snap *schema.SchemaSnapshot, schemaFilter, tableFilter string) *schema.SchemaSnapshot {
	if schemaFilter == "" && tableFilter == "" {
		return snap
	}
	out := *snap
	tables := make([]schema.Table, 0, len(snap.Tables))
	for _, t := range snap.Tables {
		if schemaFilter != "" && t.Schema != schemaFilter {
			continue
		}
		if tableFilter != "" && t.Name != tableFilter {
			continue
		}
		tables = append(tables, t)
	}
	out.Tables = tables
	return &out
}

func buildAnomalies(a *schema.AnnotatedSchema) []map[string]any {
	if a == nil || a.Merged == nil {
		return nil
	}
	var anomalies []map[string]any
	for _, sm := range schema.SummarizeTableStats(a) {
		flags := schema.DetectTableFlags(&sm, a)
		if len(flags) == 0 {
			continue
		}
		flagStrs := make([]string, len(flags))
		for i, f := range flags {
			flagStrs[i] = string(f)
		}
		anomalies = append(anomalies, map[string]any{
			"schema": sm.Schema, "table": sm.Table,
			"flags":          flagStrs,
			"total_seq_scan": sm.TotalSeqScan, "total_idx_scan": sm.TotalIdxScan,
		})
	}
	return anomalies
}

type (
	compactColumn struct {
		Name             string  `json:"name"`
		Ordinal          int16   `json:"ordinal"`
		TypeName         string  `json:"type_name"`
		Nullable         bool    `json:"nullable"`
		Default          *string `json:"default,omitempty"`
		Identity         *string `json:"identity,omitempty"`
		Comment          *string `json:"comment,omitempty"`
		StatisticsTarget *int16  `json:"statistics_target,omitempty"`
		Generated        *string `json:"generated,omitempty"`
	}

	compactIndex struct {
		Name       string   `json:"name"`
		Columns    []string `json:"columns"`
		IndexType  string   `json:"index_type"`
		IsUnique   bool     `json:"is_unique"`
		IsPrimary  bool     `json:"is_primary"`
		Predicate  *string  `json:"predicate,omitempty"`
		Definition string   `json:"definition"`
		IsValid    bool     `json:"is_valid"`
	}

	compactTable struct {
		OID           uint32              `json:"oid"`
		Schema        string              `json:"schema"`
		Name          string              `json:"name"`
		Columns       []compactColumn     `json:"columns"`
		Constraints   []schema.Constraint `json:"constraints"`
		Indexes       []compactIndex      `json:"indexes"`
		RLSEnabled    bool                `json:"rls_enabled"`
		Comment       *string             `json:"comment,omitempty"`
		Sizing        *schema.TableSizing `json:"sizing,omitempty"`
		Policies      []schema.RlsPolicy  `json:"policies,omitempty"`
		Triggers      []schema.Trigger    `json:"triggers,omitempty"`
		Reloptions    []string            `json:"reloptions,omitempty"`
		PartitionInfo any                 `json:"partition_info,omitempty"`
	}

	compactPartitionInfo struct {
		Strategy       schema.PartitionStrategy `json:"strategy"`
		Key            string                   `json:"key"`
		ChildrenShown  []schema.PartitionChild  `json:"children_shown"`
		ChildrenTotal  int                      `json:"children_total"`
		ChildrenElided string                   `json:"children_elided"`
	}
)

func toCompactTable(t *schema.Table, sizing *schema.TableSizing) compactTable {
	out := compactTable{
		OID: t.OID, Schema: t.Schema, Name: t.Name,
		Constraints: t.Constraints, RLSEnabled: t.RLSEnabled,
		Comment: t.Comment, Sizing: sizing,
		Policies: t.Policies, Triggers: t.Triggers, Reloptions: t.Reloptions,
	}
	out.Columns = make([]compactColumn, len(t.Columns))
	for i, c := range t.Columns {
		out.Columns[i] = compactColumn{c.Name, c.Ordinal, c.TypeName, c.Nullable, c.Default, c.Identity, c.Comment, c.StatisticsTarget, c.Generated}
	}
	out.Indexes = make([]compactIndex, len(t.Indexes))
	for i, idx := range t.Indexes {
		out.Indexes[i] = compactIndex{idx.Name, idx.Columns, idx.IndexType, idx.IsUnique, idx.IsPrimary, idx.Predicate, idx.Definition, idx.IsValid}
	}
	if pi := t.PartitionInfo; pi != nil {
		if len(pi.Children) > 20 {
			truncated := append(append([]schema.PartitionChild{}, pi.Children[:5]...), pi.Children[len(pi.Children)-5:]...)
			out.PartitionInfo = compactPartitionInfo{
				Strategy: pi.Strategy, Key: pi.Key,
				ChildrenShown: truncated, ChildrenTotal: len(pi.Children),
				ChildrenElided: fmt.Sprintf("showing first 5 and last 5 of %d partitions", len(pi.Children)),
			}
		} else {
			out.PartitionInfo = pi
		}
	}
	return out
}
