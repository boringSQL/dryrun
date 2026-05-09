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
