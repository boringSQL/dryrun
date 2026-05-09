package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/internal/audit"
	"github.com/boringsql/dryrun/internal/diff"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/pgmustard"
	"github.com/boringsql/dryrun/internal/query"
	"github.com/boringsql/dryrun/internal/schema"
)

type (
	Server struct {
		pool             *pgxpool.Pool
		dbURL            string
		snap             *schema.SchemaSnapshot
		mu               sync.RWMutex
		history          *history.Store
		lintConfig       lint.Config
		pgmustardClient  *pgmustard.Client
		schemaCandidates []string
		uninitialized    bool
	}
)

func NewServer(pool *pgxpool.Pool, dbURL string, snap *schema.SchemaSnapshot, hist *history.Store, lintCfg lint.Config, pgMustardAPIKey string) *Server {
	return &Server{
		pool:            pool,
		dbURL:           dbURL,
		snap:            snap,
		history:         hist,
		lintConfig:      lintCfg,
		pgmustardClient: pgmustard.NewClient(pgMustardAPIKey),
	}
}

func NewOfflineServer(snap *schema.SchemaSnapshot, lintCfg lint.Config) *Server {
	slog.Info("loaded schema from file", "tables", len(snap.Tables), "database", snap.Database)
	return &Server{snap: snap, lintConfig: lintCfg, pgmustardClient: pgmustard.NewClient("")}
}

func (s *Server) SetSchemaCandidates(paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaCandidates = paths
}

func (s *Server) SetUninitialized(paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaCandidates = paths
	s.uninitialized = true
}

func (s *Server) getSchema() (*schema.SchemaSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snap == nil || s.uninitialized {
		return nil, fmt.Errorf("no schema loaded — initialize first:\n\n1. Run `dryrun dump-schema --db <DATABASE_URL>` in a terminal\n2. Call the `reload_schema` tool in this session\n\nThe schema will be picked up without restarting the server.")
	}
	return s.snap, nil
}

func (s *Server) modeStr() string {
	if s.pool != nil {
		return "live"
	}
	return "offline"
}

func (s *Server) pgDisplay() string {
	snap, err := s.getSchema()
	if err != nil || snap.PgVersion == "" {
		return ""
	}
	if v, err := dryrun.ParsePgVersion(snap.PgVersion); err == nil {
		return v.String()
	}
	return snap.PgVersion
}

func (s *Server) databaseName() string {
	snap, err := s.getSchema()
	if err != nil {
		return ""
	}
	return snap.Database
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
	// merge if payload is already an object; otherwise nest under `key`
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

func (s *Server) requirePool() (*pgxpool.Pool, error) {
	if s.pool == nil {
		return nil, fmt.Errorf("this tool requires a live database connection (--db)")
	}
	return s.pool, nil
}

func tool(name, description string) mcp.Tool {
	return mcp.Tool{Name: name, Description: description}
}

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

func getArg(req mcp.CallToolRequest, key string) string {
	args := req.GetArguments()
	if args == nil {
		return ""
	}
	v, ok := args[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func getFloatArg(req mcp.CallToolRequest, key string, fallback float64) float64 {
	args := req.GetArguments()
	if args == nil {
		return fallback
	}
	v, ok := args[key]
	if !ok {
		return fallback
	}
	f, _ := v.(float64)
	if f <= 0 {
		return fallback
	}
	return f
}

func getBoolArg(req mcp.CallToolRequest, key string) bool {
	args := req.GetArguments()
	if args == nil {
		return false
	}
	v, ok := args[key]
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

func schemaArg(req mcp.CallToolRequest) string {
	return argOr(req, "schema", "public")
}

func argOr(req mcp.CallToolRequest, key, fallback string) string {
	if v := getArg(req, key); v != "" {
		return v
	}
	return fallback
}

func pageEnd(offset, limit, total int) int {
	if limit > 0 && offset+limit < total {
		return offset + limit
	}
	return total
}

// Shallow-copy snap, retaining tables + per-node stats matching filters.
// empty filter means no filtering on that axis
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

	if len(snap.NodeStats) > 0 {
		nodes := make([]schema.NodeStats, len(snap.NodeStats))
		for i, ns := range snap.NodeStats {
			nodes[i] = ns
			if schemaFilter != "" || tableFilter != "" {
				ts := make([]schema.NodeTableStats, 0, len(ns.TableStats))
				for _, t := range ns.TableStats {
					if schemaFilter != "" && t.Schema != schemaFilter {
						continue
					}
					if tableFilter != "" && t.Table != tableFilter {
						continue
					}
					ts = append(ts, t)
				}
				is := make([]schema.NodeIndexStats, 0, len(ns.IndexStats))
				for _, x := range ns.IndexStats {
					if schemaFilter != "" && x.Schema != schemaFilter {
						continue
					}
					if tableFilter != "" && x.Table != tableFilter {
						continue
					}
					is = append(is, x)
				}
				nodes[i].TableStats = ts
				nodes[i].IndexStats = is
			}
		}
		out.NodeStats = nodes
	}
	return &out
}

func buildAnomalies(snap *schema.SchemaSnapshot) []map[string]any {
	if len(snap.NodeStats) == 0 {
		return nil
	}
	var anomalies []map[string]any
	for _, sm := range schema.SummarizeTableStats(snap.NodeStats) {
		flags := schema.DetectTableFlags(&sm, snap.NodeStats)
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

func (s *Server) Instructions() string {
	snap, err := s.getSchema()
	if err != nil || snap.PgVersion == "" {
		return "dryrun PostgreSQL schema advisor. No schema loaded yet."
	}

	ver, err := dryrun.ParsePgVersion(snap.PgVersion)
	if err != nil {
		return fmt.Sprintf("dryrun PostgreSQL schema advisor. Database: %s", snap.Database)
	}

	return fmt.Sprintf("dryrun PostgreSQL schema advisor. PostgreSQL %s; database: %s", ver, snap.Database)
}

// Online-only tools (explain_query, refresh_schema, check_drift) are
// Registered only with a live db connection.
func (s *Server) Register(srv *mcpserver.MCPServer) {
	// offline-capable
	srv.AddTool(
		mcp.NewTool("list_tables",
			mcp.WithDescription("List tables with row estimates, comments, and aggregated node statistics. Use limit/offset to paginate large schemas."),
			mcp.WithString("schema", mcp.Description("Filter by schema name")),
			mcp.WithString("sort",
				mcp.Enum("name", "rows", "size"),
				mcp.DefaultString("name"),
				mcp.Description("Sort order: name (alphabetical), rows (descending), size (descending)"),
			),
			mcp.WithNumber("limit", mcp.DefaultNumber(50), mcp.Description("Max results to return (default 50, 0 for all)")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results")),
		),
		s.handleListTables,
	)
	srv.AddTool(
		mcp.NewTool("describe_table",
			mcp.WithDescription("Describe a table: columns, constraints, indexes, stats. Default summary mode strips verbose raw statistis and returns interpreted column profiles to make it much more compact for LLM context."),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name")),
			mcp.WithString("schema", mcp.Description("Schema name (default: public)")),
			mcp.WithString("detail",
				mcp.Enum("summary", "full", "stats"),
				mcp.DefaultString("summary"),
				mcp.Description("summary=compact with interpreted profiles (default), full=raw stats included, stats=only profiles and table stats"),
			),
		),
		s.handleDescribeTable,
	)
	srv.AddTool(
		mcp.NewTool("search_schema",
			mcp.WithDescription("Search across table names, column names, comments, constraints. Use limit/offset for large result sets."),
			mcp.WithString("query", mcp.Required(), mcp.Description("Search term")),
			mcp.WithNumber("limit", mcp.DefaultNumber(30), mcp.Description("Max results to return (default 30, 0 for all)")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results")),
		),
		s.handleSearchSchema,
	)
	srv.AddTool(tool("find_related", "Find tables related via foreign keys"), s.handleFindRelated)
	srv.AddTool(tool("validate_query", "Parse and validate SQL against the schema"), s.handleValidateQuery)
	srv.AddTool(tool("check_migration", "Check DDL migration safety"), s.handleCheckMigration)
	srv.AddTool(tool("suggest_index", "Suggest indexes for a SQL query"), s.handleSuggestIndex)
	srv.AddTool(
		mcp.NewTool("lint_schema",
			mcp.WithDescription("Lint schema for convention violations and structural issues"),
			mcp.WithString("scope",
				mcp.Enum("conventions", "audit", "all"),
				mcp.DefaultString("all"),
				mcp.Description("conventions=naming/types/constraints, audit=indexes/FKs/docs, all=both"),
			),
			mcp.WithString("schema",
				mcp.Description("Filter to a specific schema (e.g. public)"),
			),
			mcp.WithString("table",
				mcp.Description("Filter to a single table"),
			),
		),
		s.handleLintSchema,
	)
	srv.AddTool(tool("compare_nodes", "Compare statistics across database nodes for a specific table"), s.handleCompareNodes)
	srv.AddTool(
		mcp.NewTool("detect",
			mcp.WithDescription("Run health checks: stale stats, unused indexes, seq-scan anomalies, index bloat. kind=all for combined report."),
			mcp.WithString("kind",
				mcp.Enum("stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "all"),
				mcp.DefaultString("all"),
				mcp.Description("Which detection to run. Defaults to all."),
			),
			mcp.WithNumber("threshold",
				mcp.DefaultNumber(4.0),
				mcp.Description("Bloat ratio threshold (only for bloated_indexes/all)."),
			),
			mcp.WithString("schema",
				mcp.Description("Filter to a specific schema (e.g. public)"),
			),
			mcp.WithString("table",
				mcp.Description("Filter to a single table"),
			),
		),
		s.handleDetect,
	)
	srv.AddTool(
		mcp.NewTool("vacuum_health",
			mcp.WithDescription("Analyze autovacuum health: effective settings, trigger thresholds, and recommendations per table"),
			mcp.WithString("schema",
				mcp.Description("Filter to a specific schema (e.g. public)"),
			),
			mcp.WithString("table",
				mcp.Description("Filter to a single table"),
			),
		),
		s.handleVacuumHealth,
	)
	srv.AddTool(
		tool("reload_schema", "Reload schema from disk. Use after running `dryrun dump-schema` to pick up the schema without restarting the server."),
		s.handleReloadSchema,
	)

	// require live db
	if s.pool != nil {
		slog.Debug("registering online-only tools", "tools", "explain_query,refresh_schema,check_drift")
		srv.AddTool(
			mcp.NewTool("explain_query",
				mcp.WithDescription("Run EXPLAIN on local database and return structured plan with warnings"),
				mcp.WithString("sql",
					mcp.Required(),
					mcp.Description("SQL query to explain"),
				),
				mcp.WithBoolean("analyze",
					mcp.Description("Run EXPLAIN ANALYZE (wrapped in rolled-back transaction)"),
				),
				mcp.WithBoolean("with_stats",
					mcp.Description("Inject production stats from schema snapshot before EXPLAIN"),
				),
				mcp.WithString("node",
					mcp.Description("Which node's stats to use (multi-node snapshots only)"),
				),
				mcp.WithBoolean("pgmustard",
					mcp.Description("Submit plan to pgMustard API for additional tips"),
				),
			),
			s.handleExplainQuery,
		)
		srv.AddTool(tool("refresh_schema", "Re-introspect the database schema"), s.handleRefreshSchema)
		srv.AddTool(tool("check_drift", "Compare live database schema against the saved snapshot to detect drift"), s.handleCheckDrift)
	} else {
		slog.Info("offline mode: explain_query, refresh_schema, check_drift not available")
	}
}

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
				// profiles + table stats only
				if stats := schema.EffectiveTableStats(t, snap); stats != nil {
					result["table_stats"] = stats
				}
			default:
				// compact, no raw column stats
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

type (
	compactColumn struct {
		Name     string  `json:"name"`
		Ordinal  int16   `json:"ordinal"`
		TypeName string  `json:"type_name"`
		Nullable bool    `json:"nullable"`
		Default  *string `json:"default,omitempty"`
		Identity *string `json:"identity,omitempty"`
		Comment  *string `json:"comment,omitempty"`
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
		Stats         *schema.TableStats  `json:"stats,omitempty"`
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

func toCompactTable(t *schema.Table) compactTable {
	out := compactTable{
		OID: t.OID, Schema: t.Schema, Name: t.Name,
		Constraints: t.Constraints, RLSEnabled: t.RLSEnabled,
		Comment: t.Comment, Stats: t.Stats,
		Policies: t.Policies, Triggers: t.Triggers, Reloptions: t.Reloptions,
	}
	out.Columns = make([]compactColumn, len(t.Columns))
	for i, c := range t.Columns {
		out.Columns[i] = compactColumn{c.Name, c.Ordinal, c.TypeName, c.Nullable, c.Default, c.Identity, c.Comment}
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

func (s *Server) handleValidateQuery(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	result, err := query.ValidateQuery(getArg(req, "sql"), snap)
	if err != nil {
		return errResult(fmt.Sprintf("SQL parse error: %v", err)), nil
	}

	hint := ""
	if result.Valid && len(result.Warnings) > 0 {
		hint = "Query is valid but has warnings. Use advise for index suggestions and plan analysis."
	} else if result.Valid {
		hint = "Query is valid. Use advise if you need optimization suggestions."
	}
	return s.metaJSONResult(result, "", hint), nil
}

func (s *Server) handleExplainQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap, _ := s.getSchema()

	withStats := getBoolArg(req, "with_stats")
	node := getArg(req, "node")

	var injectResult *schema.InjectResult

	if withStats {
		if snap == nil {
			return errResult("no schema snapshot available for stats injection"), nil
		}
		snap = snap.CloneForStats()
		if node != "" {
			if err := schema.ApplyNodeStats(snap, node); err != nil {
				return errResult(fmt.Sprintf("node stats: %v", err)), nil
			}
		}
		if err := schema.CanInjectStats(snap); err != nil {
			return errResult(fmt.Sprintf("cannot inject stats: %v", err)), nil
		}
		pgVer, err := dryrun.ParsePgVersion(snap.PgVersion)
		if err != nil {
			return errResult(fmt.Sprintf("cannot parse PG version: %v", err)), nil
		}
		injectResult, err = schema.InjectStats(ctx, pool, snap, pgVer.Major)
		if err != nil {
			return errResult(fmt.Sprintf("stats injection failed: %v", err)), nil
		}
	}

	result, err := query.ExplainQuery(ctx, pool, getArg(req, "sql"), getBoolArg(req, "analyze"), snap)
	if err != nil {
		return errResult(fmt.Sprintf("EXPLAIN failed: %v", err)), nil
	}

	result.StatsInjected = injectResult

	if getBoolArg(req, "pgmustard") {
		addPgmWarn := func(msg string) {
			result.Warnings = append(result.Warnings, query.PlanWarning{
				Severity: "warning", Message: msg, NodeType: "pgmustard",
			})
		}
		switch {
		case !getBoolArg(req, "analyze"):
			addPgmWarn("pgMustard requires EXPLAIN ANALYZE output with timings; re-run with analyze: true")
		case withStats:
			addPgmWarn("pgMustard tips are not useful with injected stats: ANALYZE timings reflect local data, not production")
		case !s.pgmustardClient.HasKey():
			addPgmWarn("pgMustard API key not configured; set pgmustard_api_key in dryrun.toml [services] or PGMUSTARD_API_KEY env var")
		default:
			tips, err := s.pgmustardClient.AnalyzePlan(result.RawPlanJSON)
			if err != nil {
				addPgmWarn(fmt.Sprintf("pgMustard analysis failed: %v", err))
			} else {
				result.PgMustardTips = tips.Tips
			}
		}
	}

	hint := ""
	if len(result.Warnings) > 0 {
		hint = "Warnings detected. Use advise for index suggestions and actionable recommendations."
	}
	return s.metaJSONResult(result, "", hint), nil
}

func (s *Server) handleCheckMigration(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	pgVersion, _ := dryrun.ParsePgVersion(snap.PgVersion)
	checks, err := query.CheckMigration(getArg(req, "ddl"), snap, &pgVersion)
	if err != nil {
		return errResult(fmt.Sprintf("DDL parse error: %v", err)), nil
	}
	if len(checks) == 0 {
		return textResult("Could not identify a specific DDL operation to check."), nil
	}

	hint := ""
	for _, c := range checks {
		if c.Safety == query.SafetyDangerous {
			hint = "DANGEROUS operations detected. Check the recommendation and rollback_ddl fields for safe alternatives."
			break
		}
	}
	wrapper := map[string]any{"checks": checks}
	s.injectMeta(wrapper, hint)
	return jsonResult(wrapper), nil
}

func (s *Server) handleSuggestIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	sql := getArg(req, "sql")
	pgVersion, _ := dryrun.ParsePgVersion(snap.PgVersion)

	var plan *query.PlanNode
	if s.pool != nil {
		result, err := query.ExplainQuery(ctx, s.pool, sql, false, snap)
		if err == nil {
			plan = &result.Plan
		}
	}

	suggestions, err := query.SuggestIndex(sql, snap, plan, &pgVersion)
	if err != nil {
		return errResult(fmt.Sprintf("analysis failed: %v", err)), nil
	}
	if len(suggestions) == 0 {
		return textResult("No index suggestions."), nil
	}
	hint := ""
	if len(suggestions) > 0 {
		hint = "Index suggestions contain DDL. Run each through check_migration before applying — it checks lock safety and duration."
	}
	wrapper := map[string]any{"index_suggestions": suggestions}
	s.injectMeta(wrapper, hint)
	return jsonResult(wrapper), nil
}

func (s *Server) handleLintSchema(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	target := filterSnap(snap, getArg(req, "schema"), getArg(req, "table"))

	scope := argOr(req, "scope", "all")
	result := map[string]any{}

	if scope == "all" || scope == "conventions" {
		findings := lint.RunRules(target, &s.lintConfig)
		report := lint.NewReport(findings, len(target.Tables), "conventions")
		result["conventions"] = lint.CompactReportFromReportN(report, 5)
	}
	hasDDLFixes := false
	if scope == "all" || scope == "audit" {
		auditCfg := audit.DefaultConfig()
		findings := audit.RunRules(target, &auditCfg)
		for _, f := range findings {
			if f.DDLFix != nil {
				hasDDLFixes = true
				break
			}
		}
		result["audit"] = lint.NewReport(findings, len(target.Tables), "audit")
	}

	hint := ""
	if hasDDLFixes {
		hint = "Some findings include ddl_fix fields. Run those through check_migration before applying to verify lock safety."
	}
	s.injectMeta(result, hint)

	data, err := json.Marshal(result)
	if err != nil {
		return errResult(fmt.Sprintf("serialization error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

func (s *Server) handleRefreshSchema(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}

	snap, err := schema.IntrospectSchema(ctx, pool)
	if err != nil {
		return errResult(fmt.Sprintf("introspection failed: %v", err)), nil
	}

	s.mu.Lock()
	s.snap = snap
	s.mu.Unlock()

	hash := snap.ContentHash
	if len(hash) > 16 {
		hash = hash[:16]
	}
	return textResult(fmt.Sprintf("Schema refreshed: %d tables, %d views, %d functions (hash: %s)",
		len(snap.Tables), len(snap.Views), len(snap.Functions), hash)), nil
}

func (s *Server) handleCompareNodes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	tableName := getArg(req, "table")
	schemaName := schemaArg(req)

	if len(snap.NodeStats) == 0 {
		return textResult("No node statistics available. Import stats from multiple nodes first."), nil
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("Node comparison for %s.%s:\n", schemaName, tableName))

	for _, ns := range snap.NodeStats {
		for _, ts := range ns.TableStats {
			if ts.Schema == schemaName && ts.Table == tableName {
				lines = append(lines, fmt.Sprintf("  %s: %.0f rows, seq_scan=%d, idx_scan=%d, size=%d",
					ns.Source, ts.Stats.Reltuples, ts.Stats.SeqScan, ts.Stats.IdxScan, ts.Stats.TableSize))
			}
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
	rawSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := filterSnap(rawSnap, getArg(req, "schema"), getArg(req, "table"))

	staleDays := int64(7)
	staleEntries := schema.DetectStaleStats(snap.NodeStats, staleDays)
	unusedEntries := schema.DetectUnusedIndexes(snap.NodeStats, snap.Tables)

	threshold := getFloatArg(req, "threshold", 4.0)
	bloatEntries := schema.DetectBloatedIndexes(snap.NodeStats, snap.Tables, threshold)

	anomalies := buildAnomalies(snap)

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
	s.injectMeta(wrapper, hint)
	return jsonResult(wrapper), nil
}

func (s *Server) handleDetectStaleStats(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	rawSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := filterSnap(rawSnap, getArg(req, "schema"), getArg(req, "table"))

	staleDays := int64(7)
	if len(snap.NodeStats) == 0 {
		// fall back to table-level stats
		var stale []string
		for _, t := range snap.Tables {
			if t.Stats == nil {
				continue
			}
			if t.Stats.LastAnalyze == nil && t.Stats.LastAutoanalyze == nil {
				stale = append(stale, fmt.Sprintf("  %s.%s: never analyzed", t.Schema, t.Name))
			}
		}
		if len(stale) == 0 {
			return textResult("No stale statistics detected."), nil
		}
		return textResult(fmt.Sprintf("Tables with stale/missing statistics:\n%s", strings.Join(stale, "\n"))), nil
	}

	entries := schema.DetectStaleStats(snap.NodeStats, staleDays)
	if len(entries) == 0 {
		return textResult("No stale statistics detected across nodes."), nil
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
	rawSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := filterSnap(rawSnap, getArg(req, "schema"), getArg(req, "table"))

	entries := schema.DetectUnusedIndexes(snap.NodeStats, snap.Tables)
	if len(entries) == 0 {
		return textResult("No unused indexes detected. All indexes have at least one scan recorded."), nil
	}
	return jsonResult(map[string]any{
		"unused_indexes": entries,
		"count":          len(entries),
	}), nil
}

func (s *Server) handleDetectAnomalies(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	rawSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := filterSnap(rawSnap, getArg(req, "schema"), getArg(req, "table"))

	if len(snap.NodeStats) == 0 {
		return textResult("No node statistics available for anomaly detection."), nil
	}

	anomalies := buildAnomalies(snap)
	if len(anomalies) == 0 {
		return textResult("No anomalies detected."), nil
	}
	return jsonResult(anomalies), nil
}

func (s *Server) handleDetectBloatedIndexes(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	rawSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := filterSnap(rawSnap, getArg(req, "schema"), getArg(req, "table"))

	threshold := getFloatArg(req, "threshold", 4.0)
	entries := schema.DetectBloatedIndexes(snap.NodeStats, snap.Tables, threshold)
	if len(entries) == 0 {
		return textResult("No bloated indexes detected."), nil
	}
	return jsonResult(map[string]any{
		"bloated_indexes": entries,
		"count":           len(entries),
	}), nil
}

func (s *Server) handleVacuumHealth(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	target := filterSnap(snap, getArg(req, "schema"), getArg(req, "table"))
	results := schema.AnalyzeVacuumHealth(target)

	if len(results) == 0 {
		return textResult(s.wrapText("No vacuum health concerns found.", "")), nil
	}
	wrapper := map[string]any{
		"vacuum_health": results,
		"count":         len(results),
	}
	s.injectMeta(wrapper, "")
	return jsonResult(wrapper), nil
}

func (s *Server) handleReloadSchema(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	s.mu.RLock()
	candidates := append([]string(nil), s.schemaCandidates...)
	s.mu.RUnlock()

	for _, path := range candidates {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		snap, err := schema.LoadSchemaFile(path)
		if err != nil {
			return errResult(fmt.Sprintf("failed to load %s: %v", path, err)), nil
		}
		s.mu.Lock()
		s.snap = snap
		s.uninitialized = false
		s.mu.Unlock()
		return textResult(fmt.Sprintf("Schema loaded from %s: %d tables, %d views, %d functions",
			path, len(snap.Tables), len(snap.Views), len(snap.Functions))), nil
	}

	var lines []string
	for _, p := range candidates {
		lines = append(lines, "  - "+p)
	}
	msg := "no schema file found at any expected location"
	if len(lines) > 0 {
		msg += ":\n" + strings.Join(lines, "\n")
	}
	msg += "\n\nRun `dryrun dump-schema --db <DATABASE_URL>` first."
	return errResult(msg), nil
}

func (s *Server) handleCheckDrift(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}
	savedSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	liveSnap, err := schema.IntrospectSchema(ctx, pool)
	if err != nil {
		return errResult(fmt.Sprintf("introspection failed: %v", err)), nil
	}

	report := diff.ClassifyDrift(savedSnap, liveSnap)

	if report.Direction == diff.DriftIdentical {
		return textResult(s.wrapText(fmt.Sprintf("No drift detected. Schema hash: %s", report.LiveHash), "")), nil
	}

	return s.metaJSONResult(report, "", ""), nil
}
