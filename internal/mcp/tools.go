package mcp

import (
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// Online-only tools (explain_query, check_drift) are
// registered only with a live db connection.
func (s *Server) Register(srv *mcpserver.MCPServer) {
	srv.AddTool(
		mcp.NewTool("list_tables",
			mcp.WithDescription("List tables with row estimates, comments, and aggregated node statistics. Use limit/offset to paginate large schemas."),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
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
			mcp.WithDescription("Table columns, types, constraints, indexes, stats"),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name.")),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
			mcp.WithString("detail",
				mcp.Enum("summary", "full", "stats"),
				mcp.DefaultString("summary"),
				mcp.Description("Detail level: 'summary' (default), 'full' (raw stats), 'stats' (profiles and stats only)."),
			),
			mcp.WithArray("fields",
				mcp.Items(map[string]any{"type": "string"}),
				mcp.Description("Whitelist of sections: columns, indexes, constraints, stats, partition_info, column_profiles, comment, policies, triggers, reloptions, rls_enabled."),
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
			mcp.WithString("schema", mcp.Description("Schema filter.")),
		),
		s.handleFindRelated,
	)
	srv.AddTool(
		mcp.NewTool("validate_query",
			mcp.WithDescription("Validate SQL against the schema; flags missing refs and anti-patterns"),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
		),
		s.handleValidateQuery,
	)
	srv.AddTool(
		mcp.NewTool("check_migration",
			mcp.WithDescription("Check DDL for lock level, duration, and safer alternatives"),
			mcp.WithString("ddl", mcp.Required(), mcp.Description("DDL statement.")),
		),
		s.handleCheckMigration,
	)
	srv.AddTool(
		mcp.NewTool("analyze_plan",
			mcp.WithDescription("Analyze an EXPLAIN JSON plan against the schema"),
			mcp.WithString("sql", mcp.Required(), mcp.Description("The original SQL query text.")),
			mcp.WithObject("plan_json", mcp.Required(), mcp.Description("EXPLAIN output in JSON format (EXPLAIN (FORMAT JSON)).")),
			mcp.WithBoolean("include_index_suggestions", mcp.DefaultBool(true), mcp.Description("Include index suggestions (default true).")),
		),
		s.handleAnalyzePlan,
	)
	srv.AddTool(
		mcp.NewTool("advise",
			mcp.WithDescription("Plan, anti-pattern, and index advice for a query"),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
			mcp.WithBoolean("include_index_suggestions", mcp.DefaultBool(true), mcp.Description("Include index suggestions (default true).")),
			mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (executes the query; live DB only).")),
		),
		s.handleAdvise,
	)
	srv.AddTool(
		mcp.NewTool("lint_schema",
			mcp.WithDescription("Schema quality checks (lint + audit)"),
			mcp.WithString("scope",
				mcp.Enum("conventions", "audit", "all"),
				mcp.DefaultString("all"),
				mcp.Description("Scope: 'conventions', 'audit', or 'all' (default)."),
			),
			mcp.WithString("verbosity",
				mcp.Enum("summary", "full"),
				mcp.DefaultString("summary"),
				mcp.Description("Verbosity: 'summary' (counts and rule names) or 'full' (findings, examples, ddl_fix)."),
			),
			mcp.WithArray("fields",
				mcp.Items(map[string]any{"type": "string"}),
				mcp.Description("Whitelist of sections: conventions, audit. Overrides 'scope' when set."),
			),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
			mcp.WithString("table", mcp.Description("Table filter.")),
		),
		s.handleLintSchema,
	)
	srv.AddTool(
		mcp.NewTool("detect",
			mcp.WithDescription("Health checks: stale stats, unused/bloated indexes, anomalies"),
			mcp.WithString("kind",
				mcp.Enum("stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "all"),
				mcp.DefaultString("all"),
				mcp.Description("Which detection to run (default: all)."),
			),
			mcp.WithNumber("threshold",
				mcp.DefaultNumber(4.0),
				mcp.Description("Bloat ratio threshold (bloated_indexes/all only)."),
			),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
			mcp.WithString("table", mcp.Description("Table filter.")),
			mcp.WithNumber("limit",
				mcp.DefaultNumber(50),
				mcp.Description("Max entries per category (default 50, 0=all)."),
			),
		),
		s.handleDetect,
	)
	srv.AddTool(
		mcp.NewTool("vacuum_health",
			mcp.WithDescription("Autovacuum status, dead tuples, tuning hints"),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
			mcp.WithString("table", mcp.Description("Table filter.")),
			mcp.WithNumber("limit",
				mcp.DefaultNumber(50),
				mcp.Description("Max entries (default 50, 0=all)."),
			),
		),
		s.handleVacuumHealth,
	)
	srv.AddTool(
		mcp.NewTool("schema_diff",
			mcp.WithDescription("Diff two snapshots, or the latest against live schema"),
			mcp.WithString("from", mcp.Description("Content hash of the base snapshot. Omit to use the latest saved snapshot.")),
			mcp.WithString("to", mcp.Description("Content hash of the target snapshot. Omit to compare against current live schema.")),
		),
		s.handleSchemaDiff,
	)
	srv.AddTool(
		mcp.NewTool("reload_schema",
			mcp.WithDescription("Reload schema from history.db or schema.json"),
		),
		s.handleReloadSchema,
	)

	if s.pool != nil {
		slog.Debug("registering online-only tools", "tools", "explain_query,check_drift")
		srv.AddTool(
			mcp.NewTool("explain_query",
				mcp.WithDescription("EXPLAIN a query (analyze=true runs EXPLAIN ANALYZE)"),
				mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
				mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (executes the query).")),
				mcp.WithBoolean("with_stats", mcp.Description("Inject snapshot stats before EXPLAIN.")),
				mcp.WithString("node", mcp.Description("Which node's stats to use (multi-node only).")),
				mcp.WithBoolean("pgmustard", mcp.Description("Submit plan to pgMustard for extra tips.")),
			),
			s.handleExplainQuery,
		)
		srv.AddTool(
			mcp.NewTool("check_drift",
				mcp.WithDescription("Diff live DB against loaded snapshot (ahead/behind/diverged)"),
			),
			s.handleCheckDrift,
		)
	} else {
		slog.Info("offline mode: explain_query, check_drift not available")
	}
}
