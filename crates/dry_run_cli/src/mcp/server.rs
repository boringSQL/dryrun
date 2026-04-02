use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::info;

use dry_run_core::audit::AuditConfig;
use dry_run_core::lint::LintConfig;
use dry_run_core::schema::{
    ConstraintKind, NodeStats, detect_seq_scan_imbalance, detect_stale_stats,
    detect_unused_indexes, effective_table_stats,
};
use dry_run_core::{DryRun, HistoryStore, SchemaSnapshot};

use crate::pgmustard::PgMustardClient;

#[derive(Clone)]
pub struct DryRunServer {
    ctx: Option<Arc<DryRun>>,
    db_url: String,
    pg_version_display: String,
    database_name: String,
    schema: Arc<RwLock<Option<SchemaSnapshot>>>,
    history: Option<Arc<std::sync::Mutex<HistoryStore>>>,
    lint_config: LintConfig,
    audit_config: AuditConfig,
    pgmustard: Option<PgMustardClient>,
    tool_router: ToolRouter<Self>,
}

impl DryRunServer {
    pub fn from_snapshot_with_db(
        snapshot: SchemaSnapshot,
        db: Option<(&str, DryRun)>,
        lint_config: LintConfig,
        pgmustard_api_key: Option<String>,
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
            pg_version_display,
            database_name,
            schema: Arc::new(RwLock::new(Some(snapshot))),
            history: None,
            lint_config,
            audit_config: AuditConfig::default(),
            pgmustard: Self::resolve_pgmustard(pgmustard_api_key),
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

    async fn get_schema(&self) -> Result<SchemaSnapshot, McpError> {
        let guard = self.schema.read().await;
        guard
            .clone()
            .ok_or_else(|| McpError::internal_error("schema not available", None))
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
}

// parameter types

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTablesParams {
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTableParams {
    pub table: String,
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchSchemaParams {
    #[schemars(description = "Case-insensitive substring to search for across all schema objects.")]
    pub query: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FindRelatedParams {
    pub table: String,
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SchemaDiffParams {
    #[serde(default)]
    #[schemars(description = "Content hash of the base snapshot. Omit to use the latest saved snapshot.")]
    pub from: Option<String>,
    #[serde(default)]
    #[schemars(description = "Content hash of the target snapshot. Omit to compare against current live schema.")]
    pub to: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ValidateQueryParams {
    pub sql: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExplainQueryParams {
    pub sql: String,
    #[serde(default)]
    #[schemars(description = "Run EXPLAIN ANALYZE (actually executes the query). Default: false.")]
    pub analyze: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AdviseParams {
    pub sql: String,
    #[serde(default)]
    #[schemars(description = "Run EXPLAIN ANALYZE (actually executes the query). Default: false.")]
    pub analyze: Option<bool>,
    #[serde(default = "default_true")]
    pub include_index_suggestions: Option<bool>,
}

fn default_true() -> Option<bool> {
    Some(true)
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CheckMigrationParams {
    pub ddl: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LintSchemaParams {
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
    #[serde(default)]
    #[schemars(description = "Scope: 'conventions' (lint only), 'audit' (audit only), or 'all' (default, both).")]
    pub scope: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DetectParams {
    #[serde(default)]
    #[schemars(description = "Detection kind: stale_stats, unused_indexes, bloated_indexes, or all (default).")]
    pub kind: Option<String>,
    #[serde(default)]
    #[schemars(description = "Bloat ratio threshold (default 1.5).")]
    pub threshold: Option<f64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CompareNodesParams {
    #[schemars(description = "Table name (without schema prefix).")]
    pub table: String,
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
}


#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AnalyzePlanParams {
    #[schemars(description = "The original SQL query text.")]
    pub sql: String,
    #[schemars(description = "EXPLAIN output in PostgreSQL JSON format (the output of EXPLAIN (FORMAT JSON) or EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)).")]
    pub plan_json: serde_json::Value,
    #[serde(default = "default_true")]
    pub include_index_suggestions: Option<bool>,
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

        let tables: Vec<_> = snapshot
            .tables
            .iter()
            .filter(|t| params.schema.as_ref().is_none_or(|s| &t.schema == s))
            .map(|t| {
                let node_count = if snapshot.node_stats.is_empty() {
                    0
                } else {
                    snapshot.node_stats.len()
                };
                let row_est = effective_table_stats(t, &snapshot)
                    .map(|s| {
                        if node_count > 0 {
                            format!(" (~{} rows, {} nodes)", s.reltuples as i64, node_count)
                        } else {
                            format!(" (~{} rows)", s.reltuples as i64)
                        }
                    })
                    .unwrap_or_default();
                let partition = t
                    .partition_info
                    .as_ref()
                    .map(|pi| {
                        format!(
                            " [partitioned: {} on '{}', {} children]",
                            pi.strategy, pi.key, pi.children.len()
                        )
                    })
                    .unwrap_or_default();
                let comment = t
                    .comment
                    .as_ref()
                    .map(|c| format!(" — {c}"))
                    .unwrap_or_default();
                format!("{}.{}{}{}{}", t.schema, t.name, row_est, partition, comment)
            })
            .collect();

        let pg_header = if !snapshot.pg_version.is_empty() {
            let ver = dry_run_core::PgVersion::parse_from_version_string(&snapshot.pg_version)
                .map(|v| format!("{}.{}.{}", v.major, v.minor, v.patch))
                .unwrap_or_else(|_| snapshot.pg_version.clone());
            format!("PostgreSQL {} | database: {}\n", ver, snapshot.database)
        } else {
            String::new()
        };

        let text = if tables.is_empty() {
            format!("{pg_header}No tables found.")
        } else {
            format!("{pg_header}{} table(s):\n{}", tables.len(), tables.join("\n"))
        };

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Describe a table in detail: columns, types, constraints, indexes, stats. Includes per-node breakdown when multi-node stats are available.")]
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

        let mut json_val = serde_json::to_value(table)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        if let Some(obj) = json_val.as_object_mut() {
            obj.insert("pg_version".into(), serde_json::Value::String(snapshot.pg_version.clone()));
        }

        let mut text = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        if let Some(breakdown) = format_node_table_breakdown(&snapshot.node_stats, schema_name, &params.table) {
            text.push_str(&breakdown);
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Search across tables, columns, views, functions, enums, indexes, and comments by substring")]
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
                if let Some(comment) = &col.comment {
                    if comment.to_lowercase().contains(&query) {
                        results.push(format!("COLUMN COMMENT {qualified}.{}: {comment}", col.name));
                    }
                }
            }

            if let Some(comment) = &table.comment {
                if comment.to_lowercase().contains(&query) && !table.name.to_lowercase().contains(&query) {
                    results.push(format!("TABLE COMMENT {qualified}: {comment}"));
                }
            }

            for con in &table.constraints {
                if let Some(def) = &con.definition {
                    if def.to_lowercase().contains(&query) {
                        results.push(format!("CONSTRAINT {qualified}.{} ({:?}): {def}", con.name, con.kind));
                    }
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

        let text = if results.is_empty() {
            format!("No matches for '{}'.", params.query)
        } else {
            format!("{} match(es) for '{}':\n{}", results.len(), params.query, results.join("\n"))
        };

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Find tables related via foreign keys — outgoing and incoming FKs with sample JOIN patterns")]
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
                if let Some(ref_table) = &fk.fk_table {
                    if ref_table == &qualified {
                        let other_qualified = format!("{}.{}", other.schema, other.name);
                        let local_cols = fk.columns.join(", ");
                        let ref_cols = fk.fk_columns.join(", ");
                        incoming.push(format!("  {other_qualified}({local_cols}) -> {qualified}({ref_cols})"));
                        incoming.push(format!("    JOIN: SELECT * FROM {qualified} JOIN {other_qualified} ON {qualified}.{ref_cols} = {other_qualified}.{local_cols}"));
                    }
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

        Ok(CallToolResult::success(vec![Content::text(lines.join("\n"))]))
    }

    #[tool(description = "Show schema changes between two saved snapshots, or between the latest saved snapshot and the current live schema. Requires history store (--history).")]
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
            return Ok(CallToolResult::success(vec![Content::text("No schema changes detected.".to_string())]));
        }

        let json = serde_json::to_string_pretty(&changeset)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Parse and validate a SQL query against the schema — checks table/column existence, detects anti-patterns. Uses aggregated multi-node stats when available.")]
    async fn validate_query(
        &self,
        Parameters(params): Parameters<ValidateQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let result = dry_run_core::query::validate_query(&params.sql, &snapshot)
            .map_err(|e| McpError::invalid_params(format!("SQL parse error: {e}"), None))?;

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Run EXPLAIN on a SQL query — returns structured plan with cost estimates and performance warnings. Requires live DB connection. Set analyze=true to execute the query (EXPLAIN ANALYZE).")]
    async fn explain_query(
        &self,
        Parameters(params): Parameters<ExplainQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await.ok();
        let ctx = self.require_live_db()?;

        let result = dry_run_core::query::explain_query(
            ctx.pool(), &params.sql, params.analyze.unwrap_or(false), schema.as_ref(),
        ).await.map_err(|e| McpError::invalid_params(format!("EXPLAIN failed: {e}"), None))?;

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Analyze a query: run EXPLAIN (when live DB available), detect plan anti-patterns, suggest indexes from query structure. Works offline with static SQL analysis.")]
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

        let result = if let Some(ref explain) = explain_result {
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

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Analyze a pre-existing EXPLAIN plan — accepts plan JSON (from autoexplain, monitoring tools, or manual EXPLAIN output) plus the original SQL. Returns schema-enriched warnings, index suggestions, and migration safety advice. No live DB required.")]
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

        let result = serde_json::json!({
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

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Check DDL migration safety — analyzes lock types, duration, table size impact, provides safe alternatives")]
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

        let json = serde_json::to_string_pretty(&checks)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Schema quality checks. Scope: 'conventions' (naming, types, timestamps), 'audit' (indexes, FKs, structure), or 'all' (default, both). Works offline.")]
    async fn lint_schema(
        &self,
        Parameters(params): Parameters<LintSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;

        let target = if let Some(schema_filter) = &params.schema {
            let mut filtered = snapshot.clone();
            filtered.tables.retain(|t| &t.schema == schema_filter);
            filtered
        } else {
            snapshot
        };

        let scope = params.scope.as_deref().unwrap_or("all");
        let mut result = serde_json::Map::new();

        if scope == "all" || scope == "conventions" {
            let report = dry_run_core::lint::lint_schema(&target, &self.lint_config);
            result.insert(
                "conventions".into(),
                serde_json::to_value(&report).unwrap_or(serde_json::Value::Null),
            );
        }

        if scope == "all" || scope == "audit" {
            let report = dry_run_core::audit::run_audit(&target, &self.audit_config);
            result.insert(
                "audit".into(),
                serde_json::to_value(&report).unwrap_or(serde_json::Value::Null),
            );
        }

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Analyze autovacuum health for all significant tables. Shows trigger thresholds, dead tuple progress, and recommendations for tuning. Works offline.")]
    async fn vacuum_health(&self) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let results = dry_run_core::schema::vacuum::analyze_vacuum_health(&snapshot);

        if results.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No tables with significant row counts found.",
            )]));
        }

        let json = serde_json::to_string_pretty(&results)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Run health detection checks. Kinds: stale_stats, unused_indexes, anomalies, bloated_indexes, all (default). Works offline from imported node_stats.")]
    async fn detect(
        &self,
        Parameters(params): Parameters<DetectParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;
        let kind = params.kind.as_deref().unwrap_or("all");

        let mut result = serde_json::Map::new();

        let run_stale = kind == "all" || kind == "stale_stats";
        let run_unused = kind == "all" || kind == "unused_indexes";
        let run_bloated = kind == "all" || kind == "bloated_indexes";

        if run_stale {
            let stale = detect_stale_stats(&snapshot.node_stats, 7);
            result.insert("stale_stats".into(), serde_json::to_value(&stale)
                .unwrap_or(serde_json::Value::Null));
        }

        if run_unused {
            let unused = detect_unused_indexes(&snapshot.node_stats, &snapshot.tables);
            result.insert("unused_indexes".into(), serde_json::to_value(&unused)
                .unwrap_or(serde_json::Value::Null));
        }

        if run_bloated {
            let threshold = params.threshold.unwrap_or(1.5);
            let bloated = dry_run_core::schema::detect_bloated_indexes(&snapshot.tables, threshold);
            result.insert("bloated_indexes".into(), serde_json::to_value(&bloated)
                .unwrap_or(serde_json::Value::Null));
        }

        let json = serde_json::to_string_pretty(&result)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Compare per-node stats for a table across all nodes — shows reltuples, relpages, seq/idx scans, table size, and per-index breakdowns. Works offline from imported node_stats.")]
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
                 dry-run import schema.json --stats r1.json r2.json"
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

        Ok(CallToolResult::success(vec![Content::text(lines.join("\n"))]))
    }

    #[tool(description = "Compare the live local database against the loaded production schema snapshot. Classifies each difference as ahead (local has extra — your pending migration), behind (prod has something local doesn't — you need to catch up), or diverged (both differ — potential conflict). Requires live DB connection.")]
    async fn check_drift(&self) -> Result<CallToolResult, McpError> {
        let ctx = self.require_live_db()?;
        let prod_snapshot = self.get_schema().await?;
        let local_snapshot = ctx.introspect_schema().await
            .map_err(|e| McpError::internal_error(format!("introspection failed: {e}"), None))?;

        let report = dry_run_core::diff::classify_drift(&prod_snapshot, &local_snapshot);

        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Force re-introspection of the database schema (requires live DB)")]
    async fn refresh_schema(&self) -> Result<CallToolResult, McpError> {
        let ctx = self.require_live_db()?;
        let snapshot = ctx.introspect_schema().await
            .map_err(|e| McpError::internal_error(format!("introspection failed: {e}"), None))?;

        let summary = format!(
            "Schema refreshed: {} tables, {} views, {} functions (hash: {})",
            snapshot.tables.len(), snapshot.views.len(), snapshot.functions.len(),
            &snapshot.content_hash[..16],
        );

        *self.schema.write().await = Some(snapshot);

        Ok(CallToolResult::success(vec![Content::text(summary)]))
    }
}

fn to_mcp_err(e: dry_run_core::Error) -> McpError {
    McpError::internal_error(e.to_string(), None)
}

fn format_number(n: i64) -> String {
    if n.abs() < 1_000 {
        return n.to_string();
    }
    let s = n.abs().to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    if n < 0 {
        result.push('-');
    }
    result.chars().rev().collect()
}

fn format_node_table_breakdown(node_stats: &[NodeStats], schema: &str, table: &str) -> Option<String> {
    if node_stats.is_empty() {
        return None;
    }

    let newest = node_stats.iter().map(|ns| ns.timestamp).max();
    let stale_threshold = newest.map(|t| t - chrono::TimeDelta::days(7));

    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "\nPer-node breakdown ({} node(s)):\n",
        node_stats.len()
    ));
    lines.push(format!(
        "{:<16} {:>12} {:>10} {:>10} {:>10} {:>12}  {}",
        "", "reltuples", "relpages", "seq_scan", "idx_scan", "table_size", "collected"
    ));

    for ns in node_stats {
        let ts = ns
            .table_stats
            .iter()
            .find(|t| t.table == table && t.schema == schema);

        if let Some(ts) = ts {
            let size_mb = ts.stats.table_size / (1024 * 1024);
            let collected = ns.timestamp.format("%Y-%m-%d %H:%M");
            let stale = stale_threshold
                .is_some_and(|threshold| ns.timestamp < threshold);
            lines.push(format!(
                "{:<16} {:>12} {:>10} {:>10} {:>10} {:>9} MB  {}{}",
                ns.source,
                format_number(ts.stats.reltuples as i64),
                format_number(ts.stats.relpages),
                format_number(ts.stats.seq_scan),
                format_number(ts.stats.idx_scan),
                format_number(size_mb),
                collected,
                if stale { " (stale)" } else { "" },
            ));
        } else {
            lines.push(format!("{:<16} (no data for this table)", ns.source));
        }
    }

    Some(lines.join("\n"))
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
                "dryrun PostgreSQL schema advisor. PostgreSQL {}; database: {}\n\n",
                self.pg_version_display, self.database_name
            )
        } else {
            "dryrun PostgreSQL schema advisor. No schema loaded yet.\n\n".to_string()
        };

        ServerInfo {
            instructions: Some(
                format!("{version_header}\
                 MODE REQUIREMENTS:\n\
                 - Most tools work offline from schema snapshots.\n\
                 - explain_query and refresh_schema require a live DB connection (--db). analyze=true actually executes the query.\n\
                 - schema_diff requires the history store (--history).\n\n\
                 Schema exploration: list_tables, describe_table, search_schema, find_related.\n\
                 Schema history: schema_diff (compare snapshots by content hash).\n\
                 Query analysis: validate_query (offline), explain_query (live DB), advise (both — prefer this for query help), analyze_plan (accepts pre-existing EXPLAIN JSON, offline).\n\
                 Migration safety: check_migration.\n\
                 Schema quality:\n\
                 - lint_schema: convention + audit checks. Use scope='conventions' for naming/types/timestamps, \
                   scope='audit' for indexes/FKs/structure, or scope='all' (default) for both.\n\
                 Cluster health: vacuum_health, compare_nodes (per-table drill-down)."),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
