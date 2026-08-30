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
	// answers from the loaded snapshot, never the database; _meta may add a freshness note from the local history
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
// list_top_queries), and live-only tools when a pool is set.
func (s *Server) Register(srv *mcpserver.MCPServer) {
	s.registerSchemaTools(srv)
	s.registerHistoryTools(srv)
	if s.pool != nil {
		s.registerLiveTools(srv)
	} else {
		slog.Info("offline mode: explain_query, check_drift, columnar_report not available")
	}
}

// RegisterOffline wires only the schema-only subset (no history.db, no pool):
// the surface a hosted endpoint serves per request. Omits snapshot_diff,
// list_top_queries, and the live tools.
func (s *Server) RegisterOffline(srv *mcpserver.MCPServer) {
	s.registerSchemaTools(srv)
}

// registerSchemaTools registers the schema-only subset (no history.db, no pool).
func (s *Server) registerSchemaTools(srv *mcpserver.MCPServer) {
	srv.AddTool(
		mcp.NewTool("find_objects",
			mcp.WithDescription("First call against an unfamiliar database, and the way to find where a name lives. With no query it inventories what exists and how big it is in production -- row estimates and table size (the heap: indexes and TOAST are not counted), partition children folded into their parent -- which the repo cannot tell you; kind= inventories views, functions or enums instead. With a query it searches names, table and column comments, index and view definitions, and enum labels, so a value like 'cancelled' finds the enum that carries it, not just an object named after it. Every hit is tagged with the field it matched (matched_on) and exact name matches rank first. Needs no database connection. Case-insensitive substring only -- no regex, no fuzzy matching, and never row data. Function bodies are not in the snapshot, so a name that appears only inside one is not findable here. One line per object: describe_table is what reads a table in full."),
			mcp.WithString("query", mcp.Description("Case-insensitive substring. Omit to inventory rather than search.")),
			mcp.WithString("kind",
				mcp.Enum("table", "column", "index", "view", "materialized_view", "function", "enum"),
				mcp.Description("Restrict to one kind. Default: tables when there is no query, every kind when there is one."),
			),
			mcp.WithString("schema", mcp.Description("Schema filter; omit for all schemas.")),
			mcp.WithString("sort",
				mcp.Enum("name", "rows", "size"),
				mcp.Description("Order: 'name', or 'rows'/'size' biggest first (objects that have no table sort last). Default is name without a query, match quality with one."),
			),
			mcp.WithNumber("limit", mcp.DefaultNumber(50), mcp.Description("Max results (default 50, 0 for all). Truncation sets _meta.next to re-run uncapped.")),
			mcp.WithNumber("offset", mcp.DefaultNumber(0), mcp.Description("Skip N results.")),
			mcp.WithOutputSchema[findObjectsResult](),
			annSnapshot,
		),
		s.handleFindObjects,
	)
	srv.AddTool(
		mcp.NewTool("describe_table",
			mcp.WithDescription("Before writing SQL against a table you have not read in this session, before a JOIN, or before a DELETE that might cascade: one table in full -- its columns and types, constraints, indexes, and planner stats. Needs no database connection; answers from the snapshot, and points at detect when the table has findings. detail=relations gives the incoming and outgoing foreign keys, each with a JOIN clause you can paste and the ON DELETE / ON UPDATE action it carries (declared constraints only, one hop, so a table reached by a cascade may cascade further). detail=stats adds vacuum: dead tuples, trigger points, effective autovacuum knobs and last vacuum/analyze times, for any table over 10k rows whether or not detect flagged it. Ask for the ddl field to get the CREATE TABLE that would rebuild it, with whatever the snapshot cannot reproduce listed beside it. Structure as of the last capture: no row data, and nothing a later migration changed."),
			mcp.WithString("table", mcp.Required(), mcp.Description("Table name, bare or schema-qualified.")),
			mcp.WithString("schema", mcp.Description("Schema name. Without it a bare name resolves in public, or in the one schema that holds it.")),
			mcp.WithString("detail",
				mcp.Enum("summary", "full", "stats", "relations"),
				mcp.DefaultString("summary"),
				mcp.Description("Detail level: 'summary' (default), 'full' (raw stats), 'stats' (profiles, stats and vacuum only), 'relations' (foreign keys on both sides, no columns)."),
			),
			mcp.WithArray("fields",
				mcp.Items(map[string]any{"type": "string"}),
				mcp.Description("Whitelist of sections: columns, indexes, constraints, stats, partition_info, column_profiles, comment, policies, triggers, reloptions, rls_enabled, ddl, vacuum, relations. Ask for ddl to get the CREATE TABLE."),
			),
			mcp.WithArray("columns",
				mcp.Items(map[string]any{"type": "string"}),
				mcp.Description("Only these columns, by exact name. The way to read four columns of a wide table."),
			),
			mcp.WithNumber("limit",
				mcp.DefaultNumber(50),
				mcp.Description("Max columns, indexes, constraints, partition children and relations per direction (default 50, 0 for all). Key, unique and indexed columns are kept past it, as are relations that cascade or clear on delete."),
			),
			mcp.WithRawOutputSchema(describeTableOutputSchema),
			annSnapshot,
		),
		s.handleDescribeTable,
	)
	srv.AddTool(
		mcp.NewTool("validate_query",
			mcp.WithDescription("Before proposing or running any SQL: checks it against the snapshot and reports unknown tables, unknown columns on qualified references (t.col), and anti-patterns. When every unknown name has exactly one candidate it also returns corrected_sql -- the same query with those names replaced, re-validated -- so a misspelling comes back as a patch rather than prose. Needs no database connection and never executes the query. Names and shape only: not cost, not plan, not whether any rows match (advise, explain_query)."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
			annSnapshot,
		),
		s.handleValidateQuery,
	)
	srv.AddTool(
		mcp.NewTool("check_migration",
			mcp.WithDescription("Before any ALTER TABLE, CREATE INDEX, DROP, or other DDL reaches a database: the lock level each statement takes, whether it rewrites the table, and rollback_ddl where one applies. Where the safe form is mechanical it returns safer_sql -- the rewrite as statements to run in order, each on its own rather than wrapped in one transaction. Needs no database connection and applies nothing. It does not estimate wall-clock duration, and not every unsafe statement has a rewrite: a column type change or a backfill needs a batch size and a deploy order this tool cannot decide for you."),
			mcp.WithString("ddl", mcp.Required(), mcp.Description("DDL statement.")),
			annSnapshot,
		),
		s.handleCheckMigration,
	)
	srv.AddTool(
		mcp.NewTool("analyze_plan",
			mcp.WithDescription("When you already hold an EXPLAIN plan -- pasted by the user, captured in CI, or returned by explain_query: interprets it against the snapshot and flags row misestimates, poor scan choices, and missing indexes. Needs no database connection. It does not produce a plan, and without a connection dryrun cannot produce one either -- the plan must come from outside."),
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
			mcp.WithDescription("One-shot review of a single query. With no database connection it returns validation and index suggestions only -- plan-shape advice needs a connection. With one it adds the plan review; analyze=true runs EXPLAIN ANALYZE, which EXECUTES the SQL in a rolled-back transaction. It suggests; it never rewrites the query."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query.")),
			mcp.WithBoolean("include_index_suggestions", mcp.DefaultBool(true), mcp.Description("Include index suggestions (default true).")),
			mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (executes the query; live DB only).")),
			annLiveExec,
		),
		s.handleAdvise,
	)
	srv.AddTool(
		mcp.NewTool("lint_schema",
			mcp.WithDescription("When reviewing a schema as a whole -- a design review, an inherited database, or the state a migration left behind: naming and convention violations plus an audit of missing keys, unindexed foreign keys, and unsafe defaults. Needs no database connection. Defaults to verbosity='summary', which returns per-rule counts only: pass verbosity='full' for the findings and their ddl_fix (not every finding has one). Whole-schema, not per-query (validate_query, advise); it applies nothing."),
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
			mcp.WithString("schema", mcp.Description("Schema filter; omit for all schemas.")),
			mcp.WithString("table", mcp.Description("Table filter, bare or schema-qualified.")),
			mcp.WithRawOutputSchema(lintSchemaOutputSchema),
			annSnapshot,
		),
		s.handleLintSchema,
	)
	srv.AddTool(
		mcp.NewTool("detect",
			mcp.WithDescription("When a query got slow, before a tuning change, when autovacuum or dead tuples come up, or when asked what is wrong with this database: stale statistics, unused indexes, bloated indexes and tables, per-table scan anomalies, and vacuum health. kind=vacuum_health reports autovacuum_disabled, default_knobs_large_table, high_dead_tuple_ratio, vacuum_threshold_too_high, freeze_age_high and mxid_age_high, each with the dead-tuple counts and last (auto)vacuum and analyze times behind it. Every kind reports offenders only: a table with nothing wrong does not appear, here or in kind=all. For a table with no finding, describe_table detail=stats carries its raw counters and last vacuum/analyze times. Needs no database connection: findings are as fresh as the last capture, and quiet where it carries no node statistics. It reports only -- no VACUUM, no ANALYZE, no settings changed. Caveats for a finding arrive with it in _meta.hint."),
			mcp.WithString("kind",
				mcp.Enum("stale_stats", "unused_indexes", "anomalies", "bloated_indexes", "bloated_tables", "vacuum_health", "all"),
				mcp.DefaultString("all"),
				mcp.Description("Which detection to run (default: all)."),
			),
			mcp.WithNumber("threshold",
				mcp.DefaultNumber(4.0),
				mcp.Description("Bloat ratio threshold; applies to kind=bloated_indexes, kind=bloated_tables and kind=all."),
			),
			mcp.WithString("schema", mcp.Description("Schema filter; omit for all schemas.")),
			mcp.WithString("table", mcp.Description("Table filter, bare or schema-qualified.")),
			mcp.WithNumber("limit",
				mcp.DefaultNumber(50),
				mcp.Description("Max entries per category (default 50, 0=all)."),
			),
			mcp.WithRawOutputSchema(detectOutputSchema),
			annSnapshot,
		),
		s.handleDetect,
	)
}

// registerHistoryTools registers the tools that read the local history.db.
func (s *Server) registerHistoryTools(srv *mcpserver.MCPServer) {
	srv.AddTool(
		mcp.NewTool("snapshot_diff",
			mcp.WithDescription("When something changed and you need to know what: the schema DDL delta between two snapshots, plus planner sizing, activity, and query-shape drift correlated to the same capture window. Query deltas are per shape over that window, so their mean is comparable across captures -- unlike pg_stat_statements' own since-reset mean. Needs no database connection, but does need two snapshots in the local history. Snapshot-to-snapshot only, never against the live database (check_drift); it reports what moved, not why."),
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
			mcp.WithString("table", mcp.Description("Narrow to one table. Bare names only: unlike detect and lint_schema this filter runs against the snapshots being compared, not the loaded one.")),
			mcp.WithNumber("limit", mcp.DefaultNumber(50), mcp.Description("Max objects (and raw rows in full view); 0 for all. Truncation sets _meta.next to re-run uncapped.")),
			mcp.WithNumber("window_minutes", mcp.DefaultNumber(30), mcp.Description("Correlation window for matching planner/activity captures to each anchor (default 30).")),
			mcp.WithRawOutputSchema(snapshotDiffOutputSchema),
			annHistory,
		),
		s.handleSnapshotDiff,
	)
	srv.AddTool(
		mcp.NewTool("list_top_queries",
			mcp.WithDescription("When asked what this database spends its time on: captured pg_stat_statements query shapes ranked by exec time, calls, or mean. Needs no database connection; reads the local history (dryrun snapshot query-stats, or init/take's best-effort capture) and is empty when no capture carries query stats. Each entry is tagged with its reporting node and never averaged across nodes: a primary and a replica are different workloads. SQL is qshape-normalized and parameterized, never the literal text or parameter values a user ran. Counters are cumulative since the last reset, not a recent-activity rate. Before comparing or differencing any two numbers, read _meta.hint -- it carries the caveats that decide whether they are comparable at all."),
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
	slog.Debug("registering online-only tools", "tools", "explain_query,check_drift,columnar_report")
	srv.AddTool(
		mcp.NewTool("explain_query",
			mcp.WithDescription("When you need the real plan for one query: runs EXPLAIN against the connected database. analyze=true runs EXPLAIN ANALYZE, which EXECUTES the query inside a transaction that is always rolled back -- writes do not persist, but their work and any triggers still run. Planner GUCs are replayed via SET LOCAL for plan parity where the snapshot captured them, so the query runs under them (e.g. prod work_mem); older snapshots carry none and get the server's own settings. Requires a connection, so it is absent in offline mode. It returns the plan; analyze_plan interprets it."),
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
			mcp.WithDescription("When a migration may have run since the snapshot, or before acting on an offline answer that is expensive to get wrong: compares the live database against the newest snapshot in the local history and reports ahead, behind, or diverged with the objects that differ, naming the baseline it used. Requires a connection, so it is absent in offline mode. Read-only: it refreshes nothing (`dryrun snapshot take` does) and changes nothing."),
			annLiveRead,
		),
		s.handleCheckDrift,
	)
	srv.AddTool(
		mcp.NewTool("columnar_report",
			mcp.WithDescription("AlloyDB only, when a scan should be served by the columnar engine and is not: resident columns, empty column store, stale blocks, with findings. Needs a live AlloyDB connection: it errors on stock PostgreSQL and is absent in offline mode. Read-only; it populates and configures nothing."),
			annLiveRead,
		),
		s.handleColumnarReport,
	)
}
