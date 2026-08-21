package mcp

import (
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// mcp-go's NewTool defaults every tool to readOnly=false, destructive=true,
// openWorld=true -- the opposite of what all but two of ours do. Each tool must
// state its own.
var (
	// reads the loaded snapshot only: no DB, no disk, same answer every call
	annSnapshot = mcp.WithToolAnnotation(mcp.ToolAnnotation{
		ReadOnlyHint:    mcp.ToBoolPtr(true),
		DestructiveHint: mcp.ToBoolPtr(false),
		IdempotentHint:  mcp.ToBoolPtr(true),
		OpenWorldHint:   mcp.ToBoolPtr(false),
	})
	// reads local history.db, whose newest row moves between calls
	annHistory = mcp.WithToolAnnotation(mcp.ToolAnnotation{
		ReadOnlyHint:    mcp.ToBoolPtr(true),
		DestructiveHint: mcp.ToBoolPtr(false),
		IdempotentHint:  mcp.ToBoolPtr(false),
		OpenWorldHint:   mcp.ToBoolPtr(false),
	})
	// reads a live database
	annLiveRead = mcp.WithToolAnnotation(mcp.ToolAnnotation{
		ReadOnlyHint:    mcp.ToBoolPtr(true),
		DestructiveHint: mcp.ToBoolPtr(false),
		IdempotentHint:  mcp.ToBoolPtr(false),
		OpenWorldHint:   mcp.ToBoolPtr(true),
	})
	// analyze=true runs the caller's SQL verbatim, which may be DML
	annLiveExec = mcp.WithToolAnnotation(mcp.ToolAnnotation{
		ReadOnlyHint:    mcp.ToBoolPtr(false),
		DestructiveHint: mcp.ToBoolPtr(true),
		IdempotentHint:  mcp.ToBoolPtr(false),
		OpenWorldHint:   mcp.ToBoolPtr(true),
	})
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
			annSnapshot,
		),
		s.handleListTables,
	)
	srv.AddTool(
		mcp.NewTool("describe_table",
			mcp.WithDescription("Full definition of one table from the snapshot: columns and types, constraints, indexes, and planner stats. Call before writing SQL against a table you have not seen."),
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
			annSnapshot,
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
			annSnapshot,
		),
		s.handleSearchSchema,
	)
	srv.AddTool(
		mcp.NewTool("find_related",
			mcp.WithDescription("Incoming and outgoing foreign keys for a table, with sample JOINs."),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name.")),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
			annSnapshot,
		),
		s.handleFindRelated,
	)
	srv.AddTool(
		mcp.NewTool("validate_query",
			mcp.WithDescription("Check SQL against the snapshot without executing it: reports unknown tables and columns, ambiguous references, and anti-patterns. Call before proposing a query."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
			annSnapshot,
		),
		s.handleValidateQuery,
	)
	srv.AddTool(
		mcp.NewTool("check_migration",
			mcp.WithDescription("Review DDL offline: reports the lock level each statement takes, whether it rewrites the table, and a safer rewrite where one exists. Call before applying a migration."),
			mcp.WithString("ddl", mcp.Required(), mcp.Description("DDL statement.")),
			annSnapshot,
		),
		s.handleCheckMigration,
	)
	srv.AddTool(
		mcp.NewTool("analyze_plan",
			mcp.WithDescription("Interpret EXPLAIN JSON you already have against the snapshot: flags row misestimates, poor scan choices, and missing indexes. Use advise or explain_query to obtain a plan first."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("The original SQL query text.")),
			// the handler accepts both the [{"Plan": ...}] array EXPLAIN (FORMAT JSON)
			// returns and the unwrapped {"Plan": ...} object; the schema must say so
			// or input validation rejects the array shape
			mcp.WithAny("plan_json", mcp.Required(),
				mcp.Description("EXPLAIN output in JSON format (EXPLAIN (FORMAT JSON)): the [{\"Plan\": ...}] array or the unwrapped {\"Plan\": ...} object."),
				func(schema map[string]any) { schema["type"] = []string{"object", "array"} },
			),
			mcp.WithBoolean("include_index_suggestions", mcp.DefaultBool(true), mcp.Description("Include index suggestions (default true).")),
			annSnapshot,
		),
		s.handleAnalyzePlan,
	)
	srv.AddTool(
		mcp.NewTool("advise",
			mcp.WithDescription("One-shot review of a single query: plan shape, anti-patterns, and index suggestions. Offline by default; analyze=true runs EXPLAIN ANALYZE, executing the SQL on a live DB."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
			mcp.WithBoolean("include_index_suggestions", mcp.DefaultBool(true), mcp.Description("Include index suggestions (default true).")),
			mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (executes the query; live DB only).")),
			annLiveExec,
		),
		s.handleAdvise,
	)
	srv.AddTool(
		mcp.NewTool("lint_schema",
			mcp.WithDescription("Schema quality report: naming and convention violations plus an audit of missing keys, indexes, and unsafe defaults, each with a ddl_fix. Call when reviewing a schema as a whole."),
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
			annSnapshot,
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
			annSnapshot,
		),
		s.handleDetect,
	)
	srv.AddTool(
		mcp.NewTool("vacuum_health",
			mcp.WithDescription("Per-table autovacuum state from the snapshot: dead tuples, last (auto)vacuum and analyze times, and tuning hints for tables the autovacuumer is not keeping up with."),
			mcp.WithString("schema", mcp.Description("Schema filter.")),
			mcp.WithString("table", mcp.Description("Table filter.")),
			mcp.WithNumber("limit",
				mcp.DefaultNumber(50),
				mcp.Description("Max entries (default 50, 0=all)."),
			),
			mcp.WithOutputSchema[vacuumHealthResult](),
			annSnapshot,
		),
		s.handleVacuumHealth,
	)
}

// registerHistoryTools registers the snapshot-to-snapshot and reload tools that
// read the local history.db.
func (s *Server) registerHistoryTools(srv *mcpserver.MCPServer) {
	srv.AddTool(
		mcp.NewTool("snapshot_diff",
			mcp.WithDescription("What changed between two snapshots: schema DDL plus the correlated planner sizing/stats, activity and query-shape drift for the same capture window. Query deltas are per shape over the window, so their mean is comparable across captures -- unlike pg_stat_statements' own mean, which averages since its last reset. Snapshot-to-snapshot, read from .dryrun/history.db."),
			mcp.WithString("from", mcp.Description("Base snapshot: 'latest~N' or a content-hash prefix. Default 'latest~1'.")),
			mcp.WithString("to", mcp.Description("Target snapshot: 'latest' or a content-hash prefix. Default 'latest'.")),
			mcp.WithString("kind",
				mcp.Enum("schema", "planner", "activity", "query"),
				mcp.DefaultString("schema"),
				mcp.Description("Timeline the refs name and the headline delta; the other kinds are correlated by capture time. Use 'planner' when the schema is stable but stats/sizing moved, 'query' for pg_stat_statements drift (per shape, per node)."),
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
			annHistory,
		),
		s.handleSnapshotDiff,
	)
	srv.AddTool(
		mcp.NewTool("reload_schema",
			mcp.WithDescription("Re-read the newest schema snapshot from .dryrun/history.db into the server. Call after `dryrun snapshot take` so the other tools stop answering from the stale schema."),
			annHistory,
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
			annHistory,
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
			annLiveExec,
		),
		s.handleExplainQuery,
	)
	srv.AddTool(
		mcp.NewTool("check_drift",
			mcp.WithDescription("Compare the live database against the loaded snapshot and report ahead, behind, or diverged, listing the objects that differ. Call when tool output looks out of date."),
			annLiveRead,
		),
		s.handleCheckDrift,
	)
	srv.AddTool(
		mcp.NewTool("columnar_report",
			mcp.WithDescription("AlloyDB only: columnar-engine state -- resident columns, empty column store, stale blocks -- with findings. Returns unsupported on stock PostgreSQL."),
			annLiveRead,
		),
		s.handleColumnarReport,
	)
}
