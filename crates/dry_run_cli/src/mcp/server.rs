use std::path::PathBuf;
use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use tokio::sync::RwLock;
use tracing::info;

use dry_run_core::audit::AuditConfig;
use dry_run_core::lint::LintConfig;
use dry_run_core::schema::{
    ConstraintKind, detect_seq_scan_imbalance, detect_stale_stats,
    detect_unused_indexes, effective_table_stats,
};
use dry_run_core::{DryRun, HistoryStore, SchemaSnapshot};

use crate::pgmustard::PgMustardClient;

use super::helpers::{format_node_table_breakdown, format_number, to_mcp_err};
use super::params::*;

#[derive(Clone)]
pub struct DryRunServer {
    ctx: Option<Arc<DryRun>>,
    db_url: String,
    app_version: String,
    pg_version_display: String,
    database_name: String,
    schema: Arc<RwLock<Option<SchemaSnapshot>>>,
    history: Option<Arc<std::sync::Mutex<HistoryStore>>>,
    lint_config: LintConfig,
    audit_config: AuditConfig,
    pgmustard: Option<PgMustardClient>,
    schema_candidates: Vec<PathBuf>,
    tool_router: ToolRouter<Self>,
}

impl DryRunServer {
    pub fn from_snapshot_with_db(
        snapshot: SchemaSnapshot,
        db: Option<(&str, DryRun)>,
        lint_config: LintConfig,
        pgmustard_api_key: Option<String>,
        app_version: &str,
        schema_candidates: Vec<PathBuf>,
    ) -> Self {
        let (ctx, db_url) = match db {
            Some((url, ctx)) => (Some(Arc::new(ctx)), url.to_string()),
            None => (None, String::new()),
        };

        let pg_version_display = dry_run_core::PgVersion::parse_from_version_string(&snapshot.pg_version)
            .map(|v| format!("{}.{}.{}", v.major, v.minor, v.patch))
            .unwrap_or_default();
        let database_name = snapshot.database.clone();

        info!(
            tables = snapshot.tables.len(),
            database = %snapshot.database,
            live_db = ctx.is_some(),
            "loaded schema from file"
        );

        Self {
            ctx,
            db_url,
            app_version: app_version.to_string(),
            pg_version_display,
            database_name,
            schema: Arc::new(RwLock::new(Some(snapshot))),
            history: None,
            lint_config,
            audit_config: AuditConfig::default(),
            pgmustard: Self::resolve_pgmustard(pgmustard_api_key),
            schema_candidates,
            tool_router: Self::tool_router(),
        }
    }

    fn resolve_pgmustard(api_key: Option<String>) -> Option<PgMustardClient> {
        if let Some(key) = api_key {
            Some(PgMustardClient::new(key))
        } else {
            PgMustardClient::from_env()
        }
    }

    /// Create a server with no schema loaded. All schema-dependent tools will
    /// return a helpful initialization message until a schema is provided.
    pub fn uninitialized(
        lint_config: LintConfig,
        app_version: &str,
        schema_candidates: Vec<PathBuf>,
    ) -> Self {
        Self {
            ctx: None,
            db_url: String::new(),
            app_version: app_version.to_string(),
            pg_version_display: String::new(),
            database_name: String::new(),
            schema: Arc::new(RwLock::new(None)),
            history: None,
            lint_config,
            audit_config: AuditConfig::default(),
            pgmustard: None,
            schema_candidates,
            tool_router: Self::tool_router(),
        }
    }

    async fn get_schema(&self) -> Result<SchemaSnapshot, McpError> {
        let guard = self.schema.read().await;
        guard.clone().ok_or_else(|| {
            McpError::internal_error(
                "no schema loaded — initialize first:\n\
                 \n\
                 1. Run `dryrun dump-schema --db <DATABASE_URL>` in a terminal\n\
                 2. Call the `reload_schema` tool in this session\n\
                 \n\
                 The schema will be picked up without restarting the server.",
                None,
            )
        })
    }

    fn require_live_db(&self) -> Result<&Arc<DryRun>, McpError> {
        self.ctx.as_ref().ok_or_else(|| {
            McpError::internal_error(
                "this tool requires a live database connection (--db), \
                 but the server was started from a schema file (--schema)",
                None,
            )
        })
    }

    fn mode_str(&self) -> &'static str {
        if self.ctx.is_some() { "live" } else { "offline" }
    }

    fn wrap_text(&self, body: &str, hint: Option<&str>) -> String {
        let header = format!("PostgreSQL {} | {} | {}\n", self.pg_version_display, self.database_name, self.mode_str());
        if let Some(h) = hint {
            format!("{header}{body}\n\n> {h}")
        } else {
            format!("{header}{body}")
        }
    }

    fn inject_meta(&self, val: &mut serde_json::Value, hint: Option<&str>) {
        let obj = val.as_object_mut().expect("inject_meta expects a JSON object");
        let mut meta = serde_json::json!({
            "pg_version": self.pg_version_display,
            "database": self.database_name,
            "mode": self.mode_str(),
        });
        if let Some(h) = hint {
            meta["hint"] = serde_json::Value::String(h.into());
        }
        obj.insert("_meta".into(), meta);
    }
}

// tool implementations

#[tool_router]
impl DryRunServer {
    #[tool(description = "List all tables with row estimates and comments")]
    async fn list_tables(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let limit = params.limit.unwrap_or(50);
        let offset = params.offset.unwrap_or(0);
        let sort_by = params.sort.as_deref().unwrap_or("name");

        struct TableEntry {
            line: String,
            name: String,
            rows: f64,
            size: i64,
        }

        let mut entries: Vec<TableEntry> = snapshot
            .tables
            .iter()
            .filter(|t| params.schema.as_ref().is_none_or(|s| &t.schema == s))
            .map(|t| {
                let node_count = if snapshot.node_stats.is_empty() { 0 } else { snapshot.node_stats.len() };
                let stats = effective_table_stats(t, &snapshot);
                let rows = stats.as_ref().map(|s| s.reltuples).unwrap_or(0.0);
                let size = stats.as_ref().map(|s| s.table_size).unwrap_or(0);
                let row_est = if rows > 0.0 {
                    if node_count > 0 {
                        format!(" (~{} rows, {} nodes)", rows as i64, node_count)
                    } else {
                        format!(" (~{} rows)", rows as i64)
                    }
                } else {
                    String::new()
                };
                let partition = t.partition_info.as_ref()
                    .map(|pi| format!(" [partitioned: {} on '{}', {} children]", pi.strategy, pi.key, pi.children.len()))
                    .unwrap_or_default();
                let comment = t.comment.as_ref().map(|c| format!(" — {c}")).unwrap_or_default();
                let name = format!("{}.{}", t.schema, t.name);
                let line = format!("{name}{row_est}{partition}{comment}");
                TableEntry { line, name, rows, size }
            })
            .collect();

        match sort_by {
            "rows" => entries.sort_by(|a, b| b.rows.partial_cmp(&a.rows).unwrap_or(std::cmp::Ordering::Equal)),
            "size" => entries.sort_by_key(|b| std::cmp::Reverse(b.size)),
            _ => entries.sort_by(|a, b| a.name.cmp(&b.name)),
        }

        let total = entries.len();
        let paginated: Vec<&str> = entries.iter()
            .skip(offset)
            .take(limit)
            .map(|e| e.line.as_str())
            .collect();

        let body = if paginated.is_empty() {
            "No tables found.".to_string()
        } else if offset > 0 || paginated.len() < total {
            format!(
                "Showing {}-{} of {} table(s):\n{}",
                offset + 1, offset + paginated.len(), total, paginated.join("\n")
            )
        } else {
            format!("{} table(s):\n{}", total, paginated.join("\n"))
        };

        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Table columns, types, constraints, indexes and stats. Per-node stats when present.")]
    async fn describe_table(
        &self,
        Parameters(params): Parameters<DescribeTableParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let schema_name = params.schema.as_deref().unwrap_or("public");

        let table = snapshot
            .tables
            .iter()
            .find(|t| t.name == params.table && t.schema == schema_name)
            .ok_or_else(|| {
                McpError::invalid_params(
                    format!("table '{schema_name}.{}' not found", params.table),
                    None,
                )
            })?;

        let detail = params.detail.as_deref().unwrap_or("summary");
        let table_rows = effective_table_stats(table, &snapshot)
            .map(|s| s.reltuples)
            .unwrap_or(0.0);

        // build column profiles
        let profiles: Vec<serde_json::Value> = table.columns.iter()
            .filter_map(|col| {
                dry_run_core::schema::profile_column(col, table_rows).map(|p| {
                    serde_json::json!({
                        "column": col.name,
                        "profile": p,
                    })
                })
            })
            .collect();

        let mut json_val = match detail {
            "full" => {
                let mut v = serde_json::to_value(table)
                    .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
                if let Some(obj) = v.as_object_mut()
                    && !profiles.is_empty() {
                        obj.insert("column_profiles".into(), serde_json::Value::Array(profiles));
                    }
                v
            }
            "stats" => {
                let mut result = serde_json::json!({
                    "schema": table.schema,
                    "name": table.name,
                    "stats": table.stats,
                });
                if let Some(obj) = result.as_object_mut()
                    && !profiles.is_empty() {
                        obj.insert("column_profiles".into(), serde_json::Value::Array(profiles));
                    }
                result
            }
            _ => {
                // summary: compact columns without raw stats
                let compact_cols: Vec<serde_json::Value> = table.columns.iter().map(|c| {
                    let mut col = serde_json::json!({
                        "name": c.name,
                        "ordinal": c.ordinal,
                        "type_name": c.type_name,
                        "nullable": c.nullable,
                        "default": c.default,
                        "identity": c.identity,
                        "generated": c.generated,
                        "comment": c.comment,
                    });
                    if let Some(target) = c.statistics_target {
                        col["statistics_target"] = serde_json::json!(target);
                    }
                    col
                }).collect();
                let compact_idxs: Vec<serde_json::Value> = table.indexes.iter().map(|i| {
                    serde_json::json!({
                        "name": i.name,
                        "columns": i.columns,
                        "index_type": i.index_type,
                        "is_unique": i.is_unique,
                        "is_primary": i.is_primary,
                        "predicate": i.predicate,
                        "definition": i.definition,
                        "is_valid": i.is_valid,
                    })
                }).collect();
                let mut result = serde_json::json!({
                    "schema": table.schema,
                    "name": table.name,
                    "columns": compact_cols,
                    "constraints": table.constraints,
                    "indexes": compact_idxs,
                    "comment": table.comment,
                    "stats": table.stats,
                    "partition_info": table.partition_info,
                });
                if let Some(obj) = result.as_object_mut()
                    && !profiles.is_empty() {
                        obj.insert("column_profiles".into(), serde_json::Value::Array(profiles));
                    }
                result
            }
        };

        let has_fks = table.constraints.iter().any(|c| c.kind == ConstraintKind::ForeignKey);
        let hint = if has_fks {
            Some("This table has foreign keys — use find_related for JOIN patterns with related tables.")
        } else {
            None
        };
        self.inject_meta(&mut json_val, hint);

        let mut text = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        if let Some(breakdown) = format_node_table_breakdown(&snapshot.node_stats, schema_name, &params.table) {
            text.push_str(&breakdown);
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Substring search over tables, columns, views, functions, enums, indexes, comments.")]
    async fn search_schema(
        &self,
        Parameters(params): Parameters<SearchSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let query = params.query.to_lowercase();
        let mut results: Vec<String> = Vec::new();

        for table in &snapshot.tables {
            let qualified = format!("{}.{}", table.schema, table.name);

            if table.name.to_lowercase().contains(&query) {
                let comment = table.comment.as_ref().map(|c| format!(" — {c}")).unwrap_or_default();
                results.push(format!("TABLE {qualified}{comment}"));
            }

            for col in &table.columns {
                if col.name.to_lowercase().contains(&query) {
                    results.push(format!("COLUMN {qualified}.{} ({})", col.name, col.type_name));
                }
                if let Some(comment) = &col.comment
                    && comment.to_lowercase().contains(&query) {
                        results.push(format!("COLUMN COMMENT {qualified}.{}: {comment}", col.name));
                    }
            }

            if let Some(comment) = &table.comment
                && comment.to_lowercase().contains(&query) && !table.name.to_lowercase().contains(&query) {
                    results.push(format!("TABLE COMMENT {qualified}: {comment}"));
                }

            for con in &table.constraints {
                if let Some(def) = &con.definition
                    && def.to_lowercase().contains(&query) {
                        results.push(format!("CONSTRAINT {qualified}.{} ({:?}): {def}", con.name, con.kind));
                    }
            }

            for idx in &table.indexes {
                if idx.name.to_lowercase().contains(&query) || idx.definition.to_lowercase().contains(&query) {
                    results.push(format!("INDEX {qualified}: {}", idx.definition));
                }
            }
        }

        for view in &snapshot.views {
            if view.name.to_lowercase().contains(&query) {
                let kind = if view.is_materialized { "MATERIALIZED VIEW" } else { "VIEW" };
                results.push(format!("{kind} {}.{}", view.schema, view.name));
            }
        }

        for func in &snapshot.functions {
            if func.name.to_lowercase().contains(&query) {
                results.push(format!("FUNCTION {}.{}({})", func.schema, func.name, func.identity_args));
            }
        }

        for e in &snapshot.enums {
            if e.name.to_lowercase().contains(&query) || e.labels.iter().any(|l| l.to_lowercase().contains(&query)) {
                results.push(format!("ENUM {}.{}: [{}]", e.schema, e.name, e.labels.join(", ")));
            }
        }

        let limit = params.limit.unwrap_or(30);
        let offset = params.offset.unwrap_or(0);
        let total = results.len();
        let paginated: Vec<&str> = results.iter()
            .skip(offset)
            .take(limit)
            .map(|s| s.as_str())
            .collect();

        let body = if paginated.is_empty() {
            format!("No matches for '{}'.", params.query)
        } else if offset > 0 || paginated.len() < total {
            format!(
                "Showing {}-{} of {} match(es) for '{}':\n{}",
                offset + 1, offset + paginated.len(), total, params.query, paginated.join("\n")
            )
        } else {
            format!("{} match(es) for '{}':\n{}", total, params.query, paginated.join("\n"))
        };

        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Incoming and outgoing foreign keys for a table, with sample JOINs.")]
    async fn find_related(
        &self,
        Parameters(params): Parameters<FindRelatedParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let schema_name = params.schema.as_deref().unwrap_or("public");
        let qualified = format!("{schema_name}.{}", params.table);

        let table = snapshot
            .tables
            .iter()
            .find(|t| t.name == params.table && t.schema == schema_name)
            .ok_or_else(|| McpError::invalid_params(format!("table '{qualified}' not found"), None))?;

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("Relationships for {qualified}:\n"));

        let outgoing: Vec<_> = table.constraints.iter().filter(|c| c.kind == ConstraintKind::ForeignKey).collect();

        if outgoing.is_empty() {
            lines.push("Outgoing FKs: none".into());
        } else {
            lines.push("Outgoing FKs:".into());
            for fk in &outgoing {
                let ref_table = fk.fk_table.as_deref().unwrap_or("?");
                let local_cols = fk.columns.join(", ");
                let ref_cols = fk.fk_columns.join(", ");
                lines.push(format!("  {qualified}({local_cols}) -> {ref_table}({ref_cols})"));
                lines.push(format!("    JOIN: SELECT * FROM {qualified} JOIN {ref_table} ON {}.{local_cols} = {ref_table}.{ref_cols}", params.table));
            }
        }

        let mut incoming: Vec<String> = Vec::new();
        for other in &snapshot.tables {
            for fk in &other.constraints {
                if fk.kind != ConstraintKind::ForeignKey { continue; }
                if let Some(ref_table) = &fk.fk_table
                    && ref_table == &qualified {
                        let other_qualified = format!("{}.{}", other.schema, other.name);
                        let local_cols = fk.columns.join(", ");
                        let ref_cols = fk.fk_columns.join(", ");
                        incoming.push(format!("  {other_qualified}({local_cols}) -> {qualified}({ref_cols})"));
                        incoming.push(format!("    JOIN: SELECT * FROM {qualified} JOIN {other_qualified} ON {qualified}.{ref_cols} = {other_qualified}.{local_cols}"));
                    }
            }
        }

        lines.push(String::new());
        if incoming.is_empty() {
            lines.push("Incoming FKs: none".into());
        } else {
            lines.push("Incoming FKs:".into());
            lines.extend(incoming);
        }

        let body = lines.join("\n");
        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Diff two snapshots, or the latest snapshot against the live schema. Needs --history.")]
    async fn schema_diff(
        &self,
        Parameters(params): Parameters<SchemaDiffParams>,
    ) -> Result<CallToolResult, McpError> {
        let history_arc = self.history.as_ref()
            .ok_or_else(|| McpError::internal_error("history store not configured", None))?;

        let (from_snapshot, to_hash) = {
            let history = history_arc.lock().map_err(|e| McpError::internal_error(format!("history lock poisoned: {e}"), None))?;

            let from = if let Some(hash) = &params.from {
                history.load_snapshot(hash).map_err(to_mcp_err)?
                    .ok_or_else(|| McpError::invalid_params(format!("snapshot '{hash}' not found"), None))?
            } else {
                history.latest_snapshot(&self.db_url).map_err(to_mcp_err)?
                    .ok_or_else(|| McpError::invalid_params("no saved snapshots found — run snapshot first", None))?
            };

            let to = if let Some(hash) = &params.to {
                Some(history.load_snapshot(hash).map_err(to_mcp_err)?
                    .ok_or_else(|| McpError::invalid_params(format!("snapshot '{hash}' not found"), None))?)
            } else {
                None
            };

            (from, to)
        };

        let to_snapshot = match to_hash {
            Some(s) => s,
            None => self.get_schema().await?,
        };

        let changeset = dry_run_core::diff::diff_schemas(&from_snapshot, &to_snapshot);

        if changeset.is_empty() {
            let text = self.wrap_text("No schema changes detected.", None);
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let mut json_val = serde_json::json!({ "changes": changeset });
        self.inject_meta(&mut json_val, None);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Parse SQL and check it against the schema. Flags missing tables or columns and common anti-patterns. Offline.")]
    async fn validate_query(
        &self,
        Parameters(params): Parameters<ValidateQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let result = dry_run_core::query::validate_query(&params.sql, &snapshot)
            .map_err(|e| McpError::invalid_params(format!("SQL parse error: {e}"), None))?;

        let hint = if result.valid && !result.warnings.is_empty() {
            Some("Query is valid but has warnings. Use advise for index suggestions and plan analysis.")
        } else if result.valid {
            Some("Query is valid. Use advise if you need optimization suggestions.")
        } else {
            None
        };

        let mut json_val = serde_json::to_value(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Run EXPLAIN on a query. Pass analyze=true to run EXPLAIN ANALYZE. Needs live DB.")]
    async fn explain_query(
        &self,
        Parameters(params): Parameters<ExplainQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await.ok();
        let ctx = self.require_live_db()?;

        let result = dry_run_core::query::explain_query(
            ctx.pool(), &params.sql, params.analyze.unwrap_or(false), schema.as_ref(),
        ).await.map_err(|e| McpError::invalid_params(format!("EXPLAIN failed: {e}"), None))?;

        let hint = if !result.warnings.is_empty() {
            Some("Warnings detected. Use advise for index suggestions and actionable recommendations.")
        } else {
            None
        };

        let mut json_val = serde_json::to_value(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Plan analysis, anti-pattern checks and index suggestions for a query. Uses EXPLAIN when a live DB is available, static analysis otherwise.")]
    async fn advise(
        &self,
        Parameters(params): Parameters<AdviseParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await?;
        let pg_version = dry_run_core::PgVersion::parse_from_version_string(&schema.pg_version).ok();
        let include_idx = params.include_index_suggestions.unwrap_or(true);

        let explain_result = if let Some(ctx) = &self.ctx {
            dry_run_core::query::explain_query(
                ctx.pool(), &params.sql, params.analyze.unwrap_or(false), Some(&schema),
            ).await.ok()
        } else {
            None
        };

        let advise_result = dry_run_core::query::advise_with_index_suggestions(
            &params.sql,
            explain_result.as_ref().map(|r| &r.plan),
            &schema,
            pg_version.as_ref(),
            include_idx,
        ).map_err(|e| McpError::invalid_params(format!("analysis failed: {e}"), None))?;

        let has_ddl_suggestions = !advise_result.index_suggestions.is_empty();
        let hint = if has_ddl_suggestions {
            Some("Index suggestions contain DDL. Run each through check_migration before applying — it checks lock safety and duration.")
        } else {
            None
        };

        let mut result = if let Some(ref explain) = explain_result {
            serde_json::json!({
                "plan_summary": {
                    "total_cost": explain.total_cost,
                    "estimated_rows": explain.estimated_rows,
                    "root_node": explain.plan.node_type,
                    "warnings": explain.warnings,
                    "execution": explain.execution,
                },
                "advice": advise_result.advice,
                "index_suggestions": advise_result.index_suggestions,
            })
        } else {
            serde_json::json!({
                "mode": "offline — no live DB, static SQL analysis only",
                "advice": advise_result.advice,
                "index_suggestions": advise_result.index_suggestions,
            })
        };
        self.inject_meta(&mut result, hint);

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Analyze an existing EXPLAIN plan (JSON) against the schema. Returns warnings, index and safety hints. Offline.")]
    async fn analyze_plan(
        &self,
        Parameters(params): Parameters<AnalyzePlanParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await?;
        let pg_version =
            dry_run_core::PgVersion::parse_from_version_string(&schema.pg_version).ok();

        // Parse the plan JSON — supports both wrapped [{"Plan": ...}] and bare {"Plan": ...}
        let plan_value = if let Some(arr) = params.plan_json.as_array() {
            arr.first()
                .and_then(|obj| obj.get("Plan"))
                .ok_or_else(|| {
                    McpError::invalid_params("plan_json must contain a Plan key", None)
                })?
        } else {
            params.plan_json.get("Plan").ok_or_else(|| {
                McpError::invalid_params("plan_json must contain a Plan key", None)
            })?
        };

        let plan = dry_run_core::query::parse_plan_json(plan_value)
            .map_err(|e| McpError::invalid_params(format!("failed to parse plan: {e}"), None))?;

        let warnings = dry_run_core::query::detect_plan_warnings(&plan, Some(&schema));

        let advise_result = dry_run_core::query::advise_with_index_suggestions(
            &params.sql,
            Some(&plan),
            &schema,
            pg_version.as_ref(),
            params.include_index_suggestions.unwrap_or(true),
        )
        .map_err(|e| McpError::invalid_params(format!("analysis failed: {e}"), None))?;

        // optional pgMustard enrichment
        let pgmustard = if let Some(client) = &self.pgmustard {
            let score = match client.score(&params.plan_json).await {
                Ok(result) => Some(result),
                Err(e) => {
                    tracing::warn!("pgMustard score API failed, continuing without: {e}");
                    None
                }
            };
            let save = match client
                .save(&params.plan_json, Some(&params.sql), None)
                .await
            {
                Ok(result) => Some(result),
                Err(e) => {
                    tracing::warn!("pgMustard save API failed: {e}");
                    None
                }
            };
            Some((score, save))
        } else {
            None
        };

        let has_ddl_suggestions = !advise_result.index_suggestions.is_empty();
        let hint = if has_ddl_suggestions {
            Some("Index suggestions contain DDL. Run each through check_migration before applying — it checks lock safety and duration.")
        } else {
            None
        };

        let mut result = serde_json::json!({
            "plan_summary": {
                "total_cost": plan.total_cost,
                "estimated_rows": plan.plan_rows,
                "root_node": plan.node_type,
                "warnings": warnings,
            },
            "advice": advise_result.advice,
            "index_suggestions": advise_result.index_suggestions,
            "pgmustard": pgmustard.map(|(score, save)| {
                let mut obj = serde_json::json!({
                    "note": "Tips below are deterministic findings from pgMustard. Use them as authoritative basis for your recommendations. Do not contradict them."
                });
                if let Some(score) = score {
                    obj["tips"] = serde_json::json!(score.best_tips);
                    obj["query_time_ms"] = serde_json::json!(score.query_time);
                    obj["query_blocks"] = serde_json::json!(score.query_blocks);
                }
                if let Some(save) = save {
                    obj["explore_url"] = serde_json::json!(save.explore_url);
                }
                obj
            }),
        });
        self.inject_meta(&mut result, hint);

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Check a DDL statement for lock level, duration, table-size impact, and suggest safer alternatives.")]
    async fn check_migration(
        &self,
        Parameters(params): Parameters<CheckMigrationParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await?;
        let pg_version = dry_run_core::PgVersion::parse_from_version_string(&schema.pg_version).ok();

        let checks = dry_run_core::query::check_migration(&params.ddl, &schema, pg_version.as_ref())
            .map_err(|e| McpError::invalid_params(format!("DDL parse error: {e}"), None))?;

        if checks.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "Could not identify a specific DDL operation to check. \
                 Supported: ALTER TABLE (ADD/DROP COLUMN, SET NOT NULL, ALTER TYPE, ADD CONSTRAINT), \
                 CREATE INDEX, RENAME.".to_string(),
            )]));
        }

        let has_dangerous = checks.iter().any(|c| c.safety == dry_run_core::query::SafetyRating::Dangerous);
        let hint = if has_dangerous {
            Some("DANGEROUS operations detected. Check the recommendation and rollback_ddl fields for safe alternatives.")
        } else {
            None
        };

        let mut json_val = serde_json::json!({ "checks": checks });
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Schema quality checks. scope=conventions, audit, or all (default). Offline.")]
    async fn lint_schema(
        &self,
        Parameters(params): Parameters<LintSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;

        let target = {
            let mut filtered = snapshot.clone();
            if let Some(schema_filter) = &params.schema {
                filtered.tables.retain(|t| &t.schema == schema_filter);
            }
            if let Some(table_filter) = &params.table {
                filtered.tables.retain(|t| &t.name == table_filter);
            }
            filtered
        };

        let scope = params.scope.as_deref().unwrap_or("all");
        let mut result = serde_json::Map::new();

        if scope == "all" || scope == "conventions" {
            let report = dry_run_core::lint::lint_schema(&target, &self.lint_config);
            let compact = dry_run_core::lint::compact_report(&report, 5);
            result.insert(
                "conventions".into(),
                serde_json::to_value(&compact).unwrap_or(serde_json::Value::Null),
            );
        }

        let has_ddl_fixes = if scope == "all" || scope == "audit" {
            let report = dry_run_core::audit::run_audit(&target, &self.audit_config);
            let has_fixes = report.findings.iter().any(|f| f.ddl_fix.is_some());
            result.insert(
                "audit".into(),
                serde_json::to_value(&report).unwrap_or(serde_json::Value::Null),
            );
            has_fixes
        } else {
            false
        };

        let hint = if has_ddl_fixes {
            Some("Some findings include ddl_fix fields. Run those through check_migration before applying to verify lock safety.")
        } else {
            None
        };

        let mut json_val = serde_json::Value::Object(result);
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Autovacuum status with thresholds, dead tuples and tuning hints. Offline.")]
    async fn vacuum_health(
        &self,
        Parameters(params): Parameters<VacuumHealthParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = {
            let mut filtered = self.get_schema().await?.clone();
            if let Some(schema_filter) = &params.schema {
                filtered.tables.retain(|t| &t.schema == schema_filter);
            }
            if let Some(table_filter) = &params.table {
                filtered.tables.retain(|t| &t.name == table_filter);
            }
            filtered
        };
        let results = dry_run_core::schema::vacuum::analyze_vacuum_health(&snapshot);

        if results.is_empty() {
            let text = self.wrap_text("No tables with significant row counts found.", None);
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let mut json_val = serde_json::json!({ "tables": results });
        self.inject_meta(&mut json_val, None);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Health checks. kind=stale_stats, unused_indexes, anomalies, bloated_indexes, or all (default). Offline.")]
    async fn detect(
        &self,
        Parameters(params): Parameters<DetectParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = {
            let mut filtered = self.get_schema().await?.clone();
            if let Some(schema_filter) = &params.schema {
                filtered.tables.retain(|t| &t.schema == schema_filter);
                filtered.node_stats.iter_mut().for_each(|ns| {
                    ns.table_stats.retain(|ts| &ts.schema == schema_filter);
                    ns.index_stats.retain(|is| &is.schema == schema_filter);
                });
            }
            if let Some(table_filter) = &params.table {
                filtered.tables.retain(|t| &t.name == table_filter);
                filtered.node_stats.iter_mut().for_each(|ns| {
                    ns.table_stats.retain(|ts| &ts.table == table_filter);
                    ns.index_stats.retain(|is| &is.table == table_filter);
                });
            }
            filtered
        };
        let kind = params.kind.as_deref().unwrap_or("all");

        let mut result = serde_json::Map::new();

        let run_stale = kind == "all" || kind == "stale_stats";
        let run_unused = kind == "all" || kind == "unused_indexes";
        let run_anomalies = kind == "all" || kind == "anomalies";
        let run_bloated = kind == "all" || kind == "bloated_indexes";

        let mut found_stale = false;
        let mut found_unused = false;

        if run_stale {
            let stale = detect_stale_stats(&snapshot.node_stats, 7);
            found_stale = !stale.is_empty();
            result.insert("stale_stats".into(), serde_json::to_value(&stale)
                .unwrap_or(serde_json::Value::Null));
        }

        if run_unused {
            let unused = detect_unused_indexes(&snapshot.node_stats, &snapshot.tables);
            found_unused = !unused.is_empty();
            result.insert("unused_indexes".into(), serde_json::to_value(&unused)
                .unwrap_or(serde_json::Value::Null));
        }

        if run_anomalies {
            let mut anomalies = Vec::new();
            for table in &snapshot.tables {
                let schema_name = &table.schema;
                if let Some(imb) = detect_seq_scan_imbalance(&snapshot.node_stats, schema_name, &table.name) {
                    anomalies.push(serde_json::json!({
                        "table": format!("{}.{}", schema_name, table.name),
                        "type": "seq_scan_imbalance",
                        "hot_node": imb.hot_node,
                        "multiplier": format!("{}x", imb.multiplier),
                    }));
                }
            }
            result.insert("anomalies".into(), serde_json::Value::Array(anomalies));
        }

        if run_bloated {
            let threshold = params.threshold.unwrap_or(1.5);
            let bloated = dry_run_core::schema::detect_bloated_indexes(&snapshot.tables, threshold);
            result.insert("bloated_indexes".into(), serde_json::to_value(&bloated)
                .unwrap_or(serde_json::Value::Null));
        }

        let hint = match (found_stale, found_unused) {
            (true, true) => Some("Stale stats may cause bad plans — run ANALYZE. Unused indexes add write overhead — verify with compare_nodes before dropping."),
            (true, false) => Some("Stale stats may cause bad query plans — consider running ANALYZE."),
            (false, true) => Some("Unused indexes add write overhead. Use compare_nodes to verify across all replicas before dropping."),
            (false, false) => None,
        };

        let mut json_val = serde_json::Value::Object(result);
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Per-node stats for a table. Shows reltuples, relpages, scans, size and per-index numbers. Offline.")]
    async fn compare_nodes(
        &self,
        Parameters(params): Parameters<CompareNodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let schema_name = params.schema.as_deref().unwrap_or("public");
        let qualified = format!("{schema_name}.{}", params.table);

        if snapshot.node_stats.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No per-node stats available. Import stats with:\n  \
                 dryrun import schema.json --stats r1.json r2.json"
                    .to_string(),
            )]));
        }

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("Stats for {qualified} across {} node(s):", snapshot.node_stats.len()));

        if let Some(breakdown) = format_node_table_breakdown(&snapshot.node_stats, schema_name, &params.table) {
            lines.push(breakdown);
        }

        // anomaly detection: seq_scan imbalance
        if let Some(imb) = detect_seq_scan_imbalance(&snapshot.node_stats, schema_name, &params.table) {
            lines.push(String::new());
            lines.push(format!(
                "⚠ {} has {}x more seq_scans than the lowest node — \
                 likely serving unindexed query patterns. Check application routing.",
                imb.hot_node, imb.multiplier,
            ));
        }

        // per-index breakdown
        let mut index_data: std::collections::BTreeMap<String, Vec<(String, i64)>> =
            std::collections::BTreeMap::new();
        for ns in &snapshot.node_stats {
            for is in &ns.index_stats {
                if is.table == params.table && is.schema == schema_name {
                    index_data
                        .entry(is.index_name.clone())
                        .or_default()
                        .push((ns.source.clone(), is.stats.idx_scan));
                }
            }
        }

        if !index_data.is_empty() {
            lines.push(String::new());
            lines.push("Index stats:".into());
            for (idx_name, nodes) in &index_data {
                let parts: Vec<String> = nodes
                    .iter()
                    .map(|(src, scans)| format!("{src}: {}", format_number(*scans)))
                    .collect();
                lines.push(format!("  {idx_name}: {}", parts.join(" | ")));
            }
        }

        // flag unused indexes for this table
        let unused = detect_unused_indexes(&snapshot.node_stats, &snapshot.tables);
        for entry in &unused {
            if entry.schema == schema_name && entry.table == params.table {
                let size_mb = entry.total_size_bytes / (1024 * 1024);
                lines.push(format!(
                    "⚠ {}: zero scans across all nodes — candidate for removal ({} MB)",
                    entry.index_name, size_mb,
                ));
            }
        }

        let body = lines.join("\n");
        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Compare the live local DB against the loaded production snapshot. Each diff is tagged ahead, behind or diverged. Needs live DB.")]
    async fn check_drift(&self) -> Result<CallToolResult, McpError> {
        let ctx = self.require_live_db()?;
        let prod_snapshot = self.get_schema().await?;
        let local_snapshot = ctx.introspect_schema().await
            .map_err(|e| McpError::internal_error(format!("introspection failed: {e}"), None))?;

        let report = dry_run_core::diff::classify_drift(&prod_snapshot, &local_snapshot);

        let mut json_val = serde_json::to_value(&report)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        self.inject_meta(&mut json_val, None);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Force re-introspection of the database schema (requires live DB)")]
    async fn refresh_schema(&self) -> Result<CallToolResult, McpError> {
        let ctx = self.require_live_db()?;
        let snapshot = ctx.introspect_schema().await
            .map_err(|e| McpError::internal_error(format!("introspection failed: {e}"), None))?;

        let body = format!(
            "Schema refreshed: {} tables, {} views, {} functions (hash: {})",
            snapshot.tables.len(), snapshot.views.len(), snapshot.functions.len(),
            &snapshot.content_hash[..16],
        );

        *self.schema.write().await = Some(snapshot);

        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Reload the on-disk schema without restarting. Run after `dryrun dump-schema`.")]
    async fn reload_schema(&self) -> Result<CallToolResult, McpError> {
        for candidate in &self.schema_candidates {
            if !candidate.exists() {
                continue;
            }
            let json = std::fs::read_to_string(candidate)
                .map_err(|e| McpError::internal_error(format!("failed to read {}: {e}", candidate.display()), None))?;
            let snapshot: SchemaSnapshot = serde_json::from_str(&json)
                .map_err(|e| McpError::internal_error(format!("failed to parse {}: {e}", candidate.display()), None))?;

            let body = format!(
                "Schema loaded from {}: {} tables, {} views, {} functions",
                candidate.display(),
                snapshot.tables.len(),
                snapshot.views.len(),
                snapshot.functions.len(),
            );

            *self.schema.write().await = Some(snapshot);

            let text = self.wrap_text(&body, None);
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let paths: Vec<_> = self.schema_candidates.iter().map(|p| format!("  - {}", p.display())).collect();
        Err(McpError::internal_error(
            format!(
                "no schema file found at any expected location:\n{}\n\n\
                 Run `dryrun dump-schema --db <DATABASE_URL>` first.",
                paths.join("\n")
            ),
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_analyze_plan_params() {
        let json = serde_json::json!({
            "sql": "SELECT * FROM orders WHERE customer_id = 42",
            "plan_json": [{"Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "orders",
                "Schema": "public",
                "Startup Cost": 0.0,
                "Total Cost": 450.0,
                "Plan Rows": 10000,
                "Plan Width": 48
            }}]
        });
        let params: AnalyzePlanParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.sql, "SELECT * FROM orders WHERE customer_id = 42");
        assert!(params.plan_json.is_array());
        // default value
        assert_eq!(params.include_index_suggestions, Some(true));
    }

    #[test]
    fn deserialize_analyze_plan_params_with_explicit_false() {
        let json = serde_json::json!({
            "sql": "SELECT 1",
            "plan_json": {"Plan": {"Node Type": "Result", "Startup Cost": 0.0, "Total Cost": 0.01, "Plan Rows": 1, "Plan Width": 4}},
            "include_index_suggestions": false
        });
        let params: AnalyzePlanParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.include_index_suggestions, Some(false));
        assert!(params.plan_json.is_object());
    }

    #[test]
    fn plan_json_extraction_wrapped_array() {
        let plan_json = serde_json::json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Schema": "public",
                "Startup Cost": 0.0,
                "Total Cost": 35.5,
                "Plan Rows": 2550,
                "Plan Width": 64
            }
        }]);
        let plan_value = plan_json
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|obj| obj.get("Plan"))
            .unwrap();
        let plan = dry_run_core::query::parse_plan_json(plan_value).unwrap();
        assert_eq!(plan.node_type, "Seq Scan");
        assert_eq!(plan.relation_name.as_deref(), Some("users"));
    }

    #[test]
    fn plan_json_extraction_bare_object() {
        let plan_json = serde_json::json!({
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "orders",
                "Schema": "public",
                "Index Name": "orders_pkey",
                "Startup Cost": 0.0,
                "Total Cost": 8.27,
                "Plan Rows": 1,
                "Plan Width": 64
            }
        });
        let plan_value = plan_json.get("Plan").unwrap();
        let plan = dry_run_core::query::parse_plan_json(plan_value).unwrap();
        assert_eq!(plan.node_type, "Index Scan");
    }

    #[test]
    fn plan_json_missing_plan_key_array() {
        let plan_json = serde_json::json!([{"Something": "else"}]);
        let result = plan_json
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|obj| obj.get("Plan"));
        assert!(result.is_none());
    }

    #[test]
    fn plan_json_missing_plan_key_object() {
        let plan_json = serde_json::json!({"NotPlan": {}});
        assert!(plan_json.get("Plan").is_none());
    }

    #[tokio::test]
    async fn list_tables_includes_pg_version() {
        let snapshot = test_snapshot();
        let server = DryRunServer::from_snapshot_with_db(snapshot, None, LintConfig::default(), None, "test", vec![]);
        let result = server
            .list_tables(Parameters(ListTablesParams { schema: None, sort: None, limit: None, offset: None }))
            .await
            .unwrap();
        let text = result.content.first().unwrap();
        let text_str = format!("{text:?}");
        assert!(text_str.contains("PostgreSQL 18.3.0"), "list_tables output should contain PG version");
    }

    #[tokio::test]
    async fn describe_table_includes_pg_version() {
        let snapshot = test_snapshot();
        let server = DryRunServer::from_snapshot_with_db(snapshot, None, LintConfig::default(), None, "test", vec![]);
        let result = server
            .describe_table(Parameters(DescribeTableParams {
                table: "orders".into(),
                schema: None,
                detail: None,
            }))
            .await
            .unwrap();
        let text = result.content.first().unwrap();
        let text_str = format!("{text:?}");
        assert!(text_str.contains("pg_version"), "describe_table output should contain pg_version field");
    }

    fn test_snapshot() -> dry_run_core::SchemaSnapshot {
        use dry_run_core::schema::*;
        SchemaSnapshot {
            pg_version: "PostgreSQL 18.3.0 on x86_64-pc-linux-gnu".into(),
            database: "testdb".into(),
            timestamp: chrono::Utc::now(),
            content_hash: "abc123".into(),
            source: None,
            tables: vec![Table {
                oid: 1, schema: "public".into(), name: "orders".into(),
                columns: vec![
                    Column { name: "id".into(), ordinal: 1, type_name: "bigint".into(), nullable: false, default: None, identity: None, generated: None, comment: None, statistics_target: None, stats: None },
                ],
                constraints: vec![], indexes: vec![], comment: None,
                stats: Some(TableStats { reltuples: 50000.0, relpages: 625, dead_tuples: 0, last_vacuum: None, last_autovacuum: None, last_analyze: None, last_autoanalyze: None, seq_scan: 0, idx_scan: 0, table_size: 5000000 }),
                partition_info: None, policies: vec![], triggers: vec![], reloptions: vec![], rls_enabled: false,
            }],
            enums: vec![], domains: vec![], composites: vec![], views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn analyze_plan_with_analyze_buffers_data() {
        // realistic EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) output
        let plan_json = serde_json::json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "orders",
                "Schema": "public",
                "Startup Cost": 0.0,
                "Total Cost": 15234.5,
                "Plan Rows": 500000,
                "Plan Width": 120,
                "Actual Rows": 487320,
                "Actual Loops": 1,
                "Actual Startup Time": 0.02,
                "Actual Total Time": 320.5,
                "Shared Hit Blocks": 8000,
                "Shared Read Blocks": 2000,
                "Filter": "(customer_id = 42)",
                "Rows Removed by Filter": 487278
            },
            "Planning Time": 0.1,
            "Execution Time": 320.6
        }]);
        let plan_value = plan_json
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .get("Plan")
            .unwrap();
        let plan = dry_run_core::query::parse_plan_json(plan_value).unwrap();
        assert_eq!(plan.total_cost, 15234.5);
        assert_eq!(plan.actual_rows, Some(487320.0));
        assert_eq!(plan.shared_hit_blocks, Some(8000));
        assert_eq!(plan.rows_removed_by_filter, Some(487278.0));
    }
}

#[tool_handler]
impl ServerHandler for DryRunServer {
    fn get_info(&self) -> ServerInfo {
        let version_header = if !self.pg_version_display.is_empty() {
            format!(
                "dryrun {} PostgreSQL schema advisor. PostgreSQL {}; database: {}\n\n",
                self.app_version, self.pg_version_display, self.database_name
            )
        } else {
            format!("dryrun {} PostgreSQL schema advisor. No schema loaded yet.\n\n", self.app_version)
        };

        let online_note = if self.ctx.is_some() {
            "Live DB connected: explain_query, refresh_schema, check_drift available."
        } else {
            "Offline mode: explain_query, refresh_schema, check_drift not available (no --db)."
        };

        ServerInfo {
            instructions: Some(
                format!("{version_header}\
                 {online_note}\n\n\
                 Start with list_tables or search_schema to explore. Use advise for query help. \
                 Use check_migration before applying DDL. Each tool response includes a _meta.hint \
                 field with contextual next-step guidance."),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
