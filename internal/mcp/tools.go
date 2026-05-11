package mcp

import (
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// Online-only tools (explain_query, refresh_schema, check_drift) are
// registered only with a live db connection.
func (s *Server) Register(srv *mcpserver.MCPServer) {
	srv.AddTool(
		mcp.NewTool("list_tables",
			mcp.WithDescription("List tables with row estimates, comments, and aggregated node statistics. Use limit/offset to paginate large schemas."),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
			mcp.WithString("sort",
				mcp.Enum("name", "rows", "size"),
				mcp.DefaultString("name"),
				mcp.Description("Sort by: 'name' (default), 'rows', or 'size'."),
			),
			mcp.WithNumber("limit", mcp.DefaultNumber(50), mcp.Description("Max results (default 50, 0 for all).")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results.")),
		),
		s.handleListTables,
	)
	srv.AddTool(
		mcp.NewTool("describe_table",
			mcp.WithDescription("Table columns, types, constraints, indexes and stats. Per-node stats when present."),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name.")),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
			mcp.WithString("detail",
				mcp.Enum("summary", "full", "stats"),
				mcp.DefaultString("summary"),
				mcp.Description("Detail level: 'summary' (default), 'full' (raw stats), 'stats' (profiles and stats only)."),
			),
		),
		s.handleDescribeTable,
	)
	srv.AddTool(
		mcp.NewTool("search_schema",
			mcp.WithDescription("Substring search over tables, columns, views, functions, enums, indexes, comments."),
			mcp.WithString("query", mcp.Required(), mcp.Description("Case-insensitive substring.")),
			mcp.WithNumber("limit", mcp.DefaultNumber(30), mcp.Description("Max results (default 30, 0 for all).")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results.")),
		),
		s.handleSearchSchema,
	)
	srv.AddTool(
		mcp.NewTool("find_related",
			mcp.WithDescription("Incoming and outgoing foreign keys for a table, with sample JOINs."),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name.")),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
		),
		s.handleFindRelated,
	)
	srv.AddTool(
		mcp.NewTool("validate_query",
			mcp.WithDescription("Parse SQL and check it against the schema. Flags missing tables or columns and common anti-patterns. Offline."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
		),
		s.handleValidateQuery,
	)
	srv.AddTool(
		mcp.NewTool("check_migration",
			mcp.WithDescription("Check a DDL statement for lock level, duration, table-size impact, and suggest safer alternatives."),
			mcp.WithString("ddl", mcp.Required(), mcp.Description("DDL statement.")),
		),
		s.handleCheckMigration,
	)
	srv.AddTool(
		mcp.NewTool("suggest_index",
			mcp.WithDescription("Suggest indexes for a SQL query."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
		),
		s.handleSuggestIndex,
	)
	srv.AddTool(
		mcp.NewTool("lint_schema",
			mcp.WithDescription("Schema quality checks. scope=conventions, audit, or all (default). Offline."),
			mcp.WithString("scope",
				mcp.Enum("conventions", "audit", "all"),
				mcp.DefaultString("all"),
				mcp.Description("Scope: 'conventions', 'audit', or 'all' (default)."),
			),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
			mcp.WithString("table", mcp.Description("Table filter (default: all tables).")),
		),
		s.handleLintSchema,
	)
	srv.AddTool(
		mcp.NewTool("compare_nodes",
			mcp.WithDescription("Per-node stats for a table. Shows reltuples, relpages, scans, size and per-index numbers. Offline."),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name.")),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
		),
		s.handleCompareNodes,
	)
	srv.AddTool(
		mcp.NewTool("detect",
			mcp.WithDescription("Health checks. kind=stale_stats, unused_indexes, anomalies, bloated_indexes, or all (default). Offline."),
			mcp.WithString("kind",
				mcp.Enum("stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "all"),
				mcp.DefaultString("all"),
				mcp.Description("Which detection to run (default: all)."),
			),
			mcp.WithNumber("threshold",
				mcp.DefaultNumber(4.0),
				mcp.Description("Bloat ratio threshold (bloated_indexes/all only)."),
			),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
			mcp.WithString("table", mcp.Description("Table filter (default: all tables).")),
		),
		s.handleDetect,
	)
	srv.AddTool(
		mcp.NewTool("vacuum_health",
			mcp.WithDescription("Autovacuum status with thresholds, dead tuples and tuning hints. Offline."),
			mcp.WithString("schema", mcp.Description("Schema filter (default: all schemas).")),
			mcp.WithString("table", mcp.Description("Table filter (default: all tables).")),
		),
		s.handleVacuumHealth,
	)
	srv.AddTool(
		mcp.NewTool("reload_schema",
			mcp.WithDescription("Reload the on-disk schema without restarting. Run after `dryrun dump-schema`."),
		),
		s.handleReloadSchema,
	)

	if s.pool != nil {
		slog.Debug("registering online-only tools", "tools", "explain_query,refresh_schema,check_drift")
		srv.AddTool(
			mcp.NewTool("explain_query",
				mcp.WithDescription("Run EXPLAIN on a query. Pass analyze=true to run EXPLAIN ANALYZE. Needs live DB."),
				mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
				mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (executes the query).")),
				mcp.WithBoolean("with_stats", mcp.Description("Inject snapshot stats before EXPLAIN.")),
				mcp.WithString("node", mcp.Description("Which node's stats to use (multi-node only).")),
				mcp.WithBoolean("pgmustard", mcp.Description("Submit plan to pgMustard for extra tips.")),
			),
			s.handleExplainQuery,
		)
		srv.AddTool(
			mcp.NewTool("refresh_schema",
				mcp.WithDescription("Re-introspect the database schema."),
			),
			s.handleRefreshSchema,
		)
		srv.AddTool(
			mcp.NewTool("check_drift",
				mcp.WithDescription("Compare the live local DB against the loaded production snapshot. Each diff is tagged ahead, behind or diverged. Needs live DB."),
			),
			s.handleCheckDrift,
		)
	} else {
		slog.Info("offline mode: explain_query, refresh_schema, check_drift not available")
	}
}
