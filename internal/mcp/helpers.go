package mcp

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

func textResult(text string) *mcp.CallToolResult {
	return mcp.NewToolResultText(text)
}

// JSON payloads ride as structuredContent for schema-aware clients; the
// pretty-printed text stays as the thin-client fallback.
func jsonResult(v any) *mcp.CallToolResult {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return errResult(fmt.Sprintf("serialization error: %v", err))
	}
	return mcp.NewToolResultStructured(v, string(data))
}

// Structured payload with a human-readable text fallback instead of raw JSON.
func structuredTextResult(v any, text string) *mcp.CallToolResult {
	return mcp.NewToolResultStructured(v, text)
}

func errResult(msg string) *mcp.CallToolResult {
	return mcp.NewToolResultError(msg)
}

func (s *Server) wrapText(body, hint string) string {
	header := fmt.Sprintf("PostgreSQL %s | %s | %s%s\n",
		s.pgDisplay(), s.databaseName(), s.modeStr(), s.captureTimes().suffix())
	if hint != "" {
		return header + body + "\n\n> " + hint
	}
	return header + body
}

type (
	// NextCall is a pre-validated follow-up tool call surfaced as _meta.next.
	// Hint stays as prose; next is for clients that can chain mechanically.
	NextCall struct {
		Tool string         `json:"tool"`
		Args map[string]any `json:"args"`
	}

	// toolMeta is the typed _meta envelope for tools with generated output
	// schemas; map-based payloads keep using injectMeta.
	toolMeta struct {
		PgVersion string `json:"pg_version"`
		Database  string `json:"database"`
		Mode      string `json:"mode"`
		// DDL, planner stats and activity are captured by separate commands on
		// separate schedules; one timestamp would misstate the other two
		SchemaCapturedAt   string `json:"schema_captured_at,omitempty"`
		PlannerCapturedAt  string `json:"planner_captured_at,omitempty"`
		ActivityCapturedAt string `json:"activity_captured_at,omitempty"`
		ActivityOldestNode string `json:"activity_oldest_node,omitempty"`
		// history.db holds a newer snapshot than the one served; call reload_schema
		NewerSnapshotAt string     `json:"newer_snapshot_at,omitempty"`
		Hint            string     `json:"hint,omitempty"`
		Next            []NextCall `json:"next,omitempty"`
	}
)

func (s *Server) newMeta(hint string, next []NextCall) *toolMeta {
	c := s.captureTimes()
	return &toolMeta{
		PgVersion:          s.pgDisplay(),
		Database:           s.databaseName(),
		Mode:               s.modeStr(),
		SchemaCapturedAt:   c.schema,
		PlannerCapturedAt:  c.planner,
		ActivityCapturedAt: c.activity,
		ActivityOldestNode: c.activityNode,
		NewerSnapshotAt:    s.newerSnapshotAt(),
		Hint:               hint,
		Next:               next,
	}
}

func (s *Server) injectMeta(val map[string]any, hint string, next []NextCall) {
	meta := map[string]any{
		"pg_version": s.pgDisplay(),
		"database":   s.databaseName(),
		"mode":       s.modeStr(),
	}
	for k, v := range s.captureTimes().fields() {
		meta[k] = v
	}
	if at := s.newerSnapshotAt(); at != "" {
		meta["newer_snapshot_at"] = at
	}
	if hint != "" {
		meta["hint"] = hint
	}
	if len(next) > 0 {
		meta["next"] = next
	}
	val["_meta"] = meta
}

// Round-trips payload through map so we can attach _meta without struct churn.
func (s *Server) metaJSONResult(payload any, key, hint string, next []NextCall) *mcp.CallToolResult {
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
	s.injectMeta(wrapper, hint, next)
	return jsonResult(wrapper)
}

const defaultMaxItems = 50

// max <= 0 disables the cap.
func capItems[T any](items []T, max int) (kept []T, omitted int) {
	if max <= 0 || len(items) <= max {
		return items, 0
	}
	return items[:max], len(items) - max
}

// count is the full total even when entries is capped, so callers see more exists.
func cappedBlock[T any](kept []T, omitted, total int) map[string]any {
	block := map[string]any{"entries": kept, "count": total}
	if omitted > 0 {
		block["truncated"] = true
		block["omitted"] = omitted
	}
	return block
}

func entryBlock[T any](entries []T, max int) map[string]any {
	kept, omitted := capItems(entries, max)
	return cappedBlock(kept, omitted, len(entries))
}

// Filters result entries (not the schema) since detectors draw from mixed sources. Empty filter = no-op on that axis
func filterByQual[T any](items []T, schemaFilter, tableFilter string, key func(T) (string, string)) []T {
	if schemaFilter == "" && tableFilter == "" {
		return items
	}
	out := make([]T, 0, len(items))
	for _, it := range items {
		s, t := key(it)
		if schemaFilter != "" && s != schemaFilter {
			continue
		}
		if tableFilter != "" && t != tableFilter {
			continue
		}
		out = append(out, it)
	}
	return out
}

// missing -> fallback; explicit 0 -> uncapped (getFloatArg treats 0 as absent,
// which would silently re-cap the "re-run with limit=0" flow _meta.next emits).
func limitArgOr(req mcp.CallToolRequest, fallback int) int {
	if v, ok := req.GetArguments()["limit"]; ok {
		if f, ok := v.(float64); ok && f >= 0 {
			return int(f)
		}
	}
	return fallback
}

func limitArg(req mcp.CallToolRequest) int {
	return limitArgOr(req, defaultMaxItems)
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

// Second return: why unattributed_scans was not evaluated, "" when it was.
func buildAnomalies(a *schema.AnnotatedSchema) ([]map[string]any, string) {
	if a == nil || a.Merged == nil {
		return nil, ""
	}
	var anomalies []map[string]any
	refIndex := schema.BuildQueryRefIndex(a)
	worthChecking := false
	for _, sm := range schema.SummarizeTableStats(a) {
		if schema.ScansWorthAttributing(sm.TotalSeqScan + sm.TotalIdxScan) {
			worthChecking = true
		}
		flags := schema.DetectTableFlags(&sm, a, refIndex)
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
	// worst cases first (by seq-scan volume) for cap to be actually helpful
	sort.SliceStable(anomalies, func(i, j int) bool {
		si, _ := anomalies[i]["total_seq_scan"].(int64)
		sj, _ := anomalies[j]["total_seq_scan"].(int64)
		return si > sj
	})

	// Silent when no table was busy enough to test; the reason would be noise.
	note := ""
	if worthChecking {
		if r := refIndex.SkipReason(); r != "" {
			note = "unattributed_scans not evaluated: " + r + "."
		}
	}
	return anomalies, note
}

// Interpretation of the unattributed_scans flag, emitted with the finding
// rather than in the tool description.
func unattributedScansHint(anomalies []map[string]any) string {
	for _, a := range anomalies {
		for _, f := range anomalyFlags(a) {
			if f == string(schema.FlagUnattributedScans) {
				return "unattributed_scans: the table sees heavy scan traffic that no captured statement references. " +
					"dryrun's capture holds top-level statements only (as does pg_stat_statements.track = 'top'), " +
					"so scans issued inside functions and triggers are invisible to query stats, not absent. " +
					"auto_explain with log_nested_statements, or reading pg_stat_statements directly under track = 'all', will show them."
			}
		}
	}
	return ""
}

// Anomalies carry []string in-process; a JSON round trip turns it into []any.
func anomalyFlags(a map[string]any) []string {
	switch v := a["flags"].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, f := range v {
			if s, ok := f.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
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
		GenerationExpr   *string `json:"generation_expr,omitempty"`
	}

	compactIndex struct {
		Name       string   `json:"name"`
		Columns    []string `json:"columns"`
		IndexType  string   `json:"index_type"`
		IsUnique   bool     `json:"is_unique"`
		IsPrimary  bool     `json:"is_primary"`
		Predicate  *string  `json:"predicate,omitempty"`
		Definition string   `json:"definition"`
		// only surfaced when problematic; a healthy index omits these
		IsValid *bool `json:"is_valid,omitempty"`
		IsReady *bool `json:"is_ready,omitempty"`
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
		Sizing        *schema.TableSizing `json:"stats,omitempty"`
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
		out.Columns[i] = compactColumn{
			Name: c.Name, Ordinal: c.Ordinal, TypeName: c.TypeName, Nullable: c.Nullable,
			Default: c.Default, Identity: c.Identity, Comment: c.Comment,
			StatisticsTarget: c.StatisticsTarget, Generated: c.Generated, GenerationExpr: c.GenerationExpr,
		}
	}
	out.Indexes = make([]compactIndex, len(t.Indexes))
	for i, idx := range t.Indexes {
		ci := compactIndex{
			Name: idx.Name, Columns: idx.Columns, IndexType: idx.IndexType,
			IsUnique: idx.IsUnique, IsPrimary: idx.IsPrimary,
			Predicate: idx.Predicate, Definition: idx.Definition,
		}
		if !idx.IsValid {
			ci.IsValid = &idx.IsValid
		}
		if !idx.IsReady {
			ci.IsReady = &idx.IsReady
		}
		out.Indexes[i] = ci
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
