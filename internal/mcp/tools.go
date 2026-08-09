package mcp

import (
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// Register wires the full set: schema-only subset, history tools (snapshot_diff,
// reload_schema), and live-only tools when a pool is set.
func (s *Server) Register(srv *mcpserver.MCPServer) {
	s.registerSchemaTools(srv)
	s.registerHistoryTools(srv)
	if s.pool != nil {
		s.registerLiveTools(srv)
	} else {
		slog.Info("offline mode: explain_query, check_drift not available")
	}
}

// RegisterOffline wires only the schema-only subset (no history.db, no pool):
// the surface a hosted endpoint serves per request. Omits snapshot_diff,
// reload_schema, and the live tools.
func (s *Server) RegisterOffline(srv *mcpserver.MCPServer) {
	s.registerSchemaTools(srv)
}

// registerSchemaTools registers the schema-only subset (no history.db, no pool).
func (s *Server) registerSchemaTools(srv *mcpserver.MCPServer) {
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
			mcp.WithOutputSchema[listTablesResult](),
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
			mcp.WithRawOutputSchema(describeTableOutputSchema),
		),
		s.handleDescribeTable,
	)
	srv.AddTool(
		mcp.NewTool("search_schema",
			mcp.WithDescription("Substring search over tables, columns, views, functions, enums, indexes, comments."),
			mcp.WithString("query", mcp.Required(), mcp.Description("Case-insensitive substring.")),
			mcp.WithNumber("limit", mcp.DefaultNumber(30), mcp.Description("Max results (default 30, 0 for all).")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results.")),
			mcp.WithOutputSchema[searchSchemaResult](),
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
			// the handler accepts both the [{"Plan": ...}] array EXPLAIN (FORMAT JSON)
			// returns and the unwrapped {"Plan": ...} object; the schema must say so
			// or input validation rejects the array shape
			mcp.WithAny("plan_json", mcp.Required(),
				mcp.Description("EXPLAIN output in JSON format (EXPLAIN (FORMAT JSON)): the [{\"Plan\": ...}] array or the unwrapped {\"Plan\": ...} object."),
				func(schema map[string]any) { schema["type"] = []string{"object", "array"} },
			),
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
			mcp.WithRawOutputSchema(lintSchemaOutputSchema),
		),
		s.handleLintSchema,
	)
	srv.AddTool(
		mcp.NewTool("detect",
			mcp.WithDescription("Health checks: stale stats, unused/bloated indexes, anomalies. The anomalies flag 'unattributed_scans' means a table sees heavy scan traffic that no captured statement references while the capture holds top-level statements only (pg_stat_statements.track = 'top', or a toplevel-filtered capture) — statements inside functions and triggers are then invisible to query stats; auto_explain with log_nested_statements, or reading pg_stat_statements directly under track = 'all', will show it."),
			mcp.WithString("kind",
				mcp.Enum("stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "bloated_tables", "all"),
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
			mcp.WithRawOutputSchema(detectOutputSchema),
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
			mcp.WithOutputSchema[vacuumHealthResult](),
		),
		s.handleVacuumHealth,
	)
}

// registerHistoryTools registers the snapshot-to-snapshot and reload tools that
// read the local history.db.
func (s *Server) registerHistoryTools(srv *mcpserver.MCPServer) {
	srv.AddTool(
		mcp.NewTool("snapshot_diff",
			mcp.WithDescription("What changed between two snapshots: schema DDL plus the correlated planner sizing/stats and activity drift for the same capture window. Snapshot-to-snapshot, read from .dryrun/history.db."),
			mcp.WithString("from", mcp.Description("Base snapshot: 'latest~N' or a content-hash prefix. Default 'latest~1'.")),
			mcp.WithString("to", mcp.Description("Target snapshot: 'latest' or a content-hash prefix. Default 'latest'.")),
			mcp.WithString("kind",
				mcp.Enum("schema", "planner", "activity"),
				mcp.DefaultString("schema"),
				mcp.Description("Timeline the refs name and the headline delta; the other kinds are correlated by capture time. Use 'planner' when the schema is stable but stats/sizing moved."),
			),
			mcp.WithString("node", mcp.Description("Activity node label, when activity has multiple nodes.")),
			mcp.WithString("view",
				mcp.Enum("summary", "full"),
				mcp.DefaultString("summary"),
				mcp.Description("'summary' (ranked objects + counts) or 'full' (adds the raw per-row deltas)."),
			),
			mcp.WithString("schema", mcp.Description("Narrow to one schema.")),
			mcp.WithString("table", mcp.Description("Narrow to one table.")),
			mcp.WithNumber("limit", mcp.DefaultNumber(50), mcp.Description("Max objects (and raw rows in full view); 0 for all. Truncation sets _meta.next to re-run uncapped.")),
			mcp.WithNumber("window_minutes", mcp.DefaultNumber(30), mcp.Description("Correlation window for matching planner/activity captures to each anchor (default 30).")),
			mcp.WithRawOutputSchema(snapshotDiffOutputSchema),
		),
		s.handleSnapshotDiff,
	)
	srv.AddTool(
		mcp.NewTool("reload_schema",
			mcp.WithDescription("Reload schema from history.db or schema.json"),
		),
		s.handleReloadSchema,
	)
	srv.AddTool(
		mcp.NewTool("list_top_queries",
			mcp.WithDescription("Captured pg_stat_statements query shapes, ranked by exec time/calls. Read from .dryrun/history.db (dryrun snapshot query-stats or init/take's best-effort capture); each entry is tagged with its reporting node and never averaged across nodes — a primary and a replica are different workloads. Canonical SQL is qshape-normalized/parameterized text, truncated for long queries. Counters are cumulative since the last pg_stat_statements reset, not a recent-activity rate. Current captures exclude statements executed inside functions and triggers wherever pg_stat_statements exposes the toplevel flag (1.9, PG14+), so no time is double counted even under track = 'all'; nested_exec_time_ms appears only on captures predating that filter, where it reports the nested part excluded from the pct_of_total_exec_time denominator. Each entry carries capture_rule_version: which predicate decided whether a statement was captured at all. Entries with different values describe different populations and must not be compared or differenced; 0 means the capture predates the field, so unknown rather than matching."),
			mcp.WithString("node", mcp.Description("Filter to one node label. Omit to see all nodes' queries together (each entry still tagged with its own node).")),
			mcp.WithString("sort",
				mcp.Enum("total_time", "calls", "mean_time"),
				mcp.DefaultString("total_time"),
				mcp.Description("Sort by: 'total_time' (default, total_exec_time_ms), 'calls', or 'mean_time' (mean_exec_time_ms)."),
			),
			mcp.WithNumber("min_calls", mcp.DefaultNumber(2), mcp.Description("Skip entries with fewer calls than this (default 2) to filter one-shot noise.")),
			mcp.WithNumber("limit", mcp.DefaultNumber(50), mcp.Description("Max results (default 50, 0 for all).")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results.")),
			mcp.WithOutputSchema[listTopQueriesResult](),
		),
		s.handleListTopQueries,
	)
}

// registerLiveTools registers the tools that need a live db connection.
func (s *Server) registerLiveTools(srv *mcpserver.MCPServer) {
	slog.Debug("registering online-only tools", "tools", "explain_query,check_drift")
	srv.AddTool(
		mcp.NewTool("explain_query",
			mcp.WithDescription("EXPLAIN a query (analyze=true runs EXPLAIN ANALYZE). Snapshot planner GUCs are replayed via SET LOCAL for plan parity; with analyze=true the query also executes under those settings (e.g. prod work_mem)."),
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
	srv.AddTool(
		mcp.NewTool("columnar_report",
			mcp.WithDescription("AlloyDB only: columnar-engine state and findings (resident columns, empty store, stale blocks)"),
		),
		s.handleColumnarReport,
	)
}
