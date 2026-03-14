use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::info;

use dry_run_core::lint::LintConfig;
use dry_run_core::schema::ConstraintKind;
use dry_run_core::{DryRun, HistoryStore, SchemaSnapshot};

#[derive(Clone)]
pub struct DryRunServer {
    ctx: Option<Arc<DryRun>>,
    db_url: String,
    schema: Arc<RwLock<Option<SchemaSnapshot>>>,
    history: Option<Arc<std::sync::Mutex<HistoryStore>>>,
    lint_config: LintConfig,
    tool_router: ToolRouter<Self>,
}

impl DryRunServer {
    pub async fn new(
        ctx: DryRun,
        db_url: String,
        history: Option<HistoryStore>,
        lint_config: LintConfig,
    ) -> Result<Self, dry_run_core::Error> {
        let snapshot = ctx.introspect_schema().await?;
        info!(tables = snapshot.tables.len(), "initial schema introspection complete");

        Ok(Self {
            ctx: Some(Arc::new(ctx)),
            db_url,
            schema: Arc::new(RwLock::new(Some(snapshot))),
            history: history.map(|h| Arc::new(std::sync::Mutex::new(h))),
            lint_config,
            tool_router: Self::tool_router(),
        })
    }

    pub fn from_snapshot_with_config(snapshot: SchemaSnapshot, lint_config: LintConfig) -> Self {
        info!(
            tables = snapshot.tables.len(),
            database = %snapshot.database,
            "loaded schema from file"
        );

        Self {
            ctx: None,
            db_url: String::new(),
            schema: Arc::new(RwLock::new(Some(snapshot))),
            history: None,
            lint_config,
            tool_router: Self::tool_router(),
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
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTableParams {
    pub table: String,
    #[serde(default)]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchSchemaParams {
    pub query: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FindRelatedParams {
    pub table: String,
    #[serde(default)]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SchemaDiffParams {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
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
    pub analyze: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AdviseParams {
    pub sql: String,
    #[serde(default)]
    pub analyze: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CheckMigrationParams {
    pub ddl: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SuggestIndexParams {
    pub sql: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LintSchemaParams {
    #[serde(default)]
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CompareNodesParams {
    pub table: String,
    #[serde(default)]
    pub schema: Option<String>,
}

// tool implementations

#[tool_router]
impl DryRunServer {
    #[tool(description = "List all tables in the database with row estimates and comments")]
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
                let row_est = t
                    .stats
                    .as_ref()
                    .map(|s| format!(" (~{} rows)", s.reltuples as i64))
                    .unwrap_or_default();
                let comment = t
                    .comment
                    .as_ref()
                    .map(|c| format!(" — {c}"))
                    .unwrap_or_default();
                format!("{}.{}{}{}", t.schema, t.name, row_est, comment)
            })
            .collect();

        let text = if tables.is_empty() {
            "No tables found.".to_string()
        } else {
            format!("{} table(s):\n{}", tables.len(), tables.join("\n"))
        };

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Describe a table in detail: columns, constraints, indexes, stats")]
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

        let json = serde_json::to_string_pretty(table)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Search across table names, column names, comments, and constraint definitions")]
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

    #[tool(description = "Show schema changes between two snapshots, or between the latest saved snapshot and the current live schema")]
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

    #[tool(description = "Run EXPLAIN on a SQL query — returns structured plan with cost estimates and performance warnings")]
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

    #[tool(description = "Analyze a query: run EXPLAIN, match plan anti-patterns against the knowledge base, return actionable recommendations")]
    async fn advise(
        &self,
        Parameters(params): Parameters<AdviseParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await?;
        let ctx = self.require_live_db()?;

        let explain_result = dry_run_core::query::explain_query(
            ctx.pool(), &params.sql, params.analyze.unwrap_or(false), Some(&schema),
        ).await.map_err(|e| McpError::invalid_params(format!("EXPLAIN failed: {e}"), None))?;

        let pg_version = dry_run_core::PgVersion::parse_from_version_string(&schema.pg_version).ok();
        let advice = dry_run_core::query::advise(&explain_result.plan, &schema, pg_version.as_ref());

        let result = serde_json::json!({
            "plan_summary": {
                "total_cost": explain_result.total_cost,
                "estimated_rows": explain_result.estimated_rows,
                "root_node": explain_result.plan.node_type,
                "warnings": explain_result.warnings,
                "execution": explain_result.execution,
            },
            "advice": advice,
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

    #[tool(description = "Suggest indexes for a SQL query based on WHERE, JOIN, ORDER BY columns and optional EXPLAIN plan. When multi-node stats are available, uses aggregated values across all nodes.")]
    async fn suggest_index(
        &self,
        Parameters(params): Parameters<SuggestIndexParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = self.get_schema().await?;
        let pg_version = dry_run_core::PgVersion::parse_from_version_string(&schema.pg_version).ok();

        let plan = if let Some(ctx) = &self.ctx {
            dry_run_core::query::explain_query(ctx.pool(), &params.sql, false, Some(&schema)).await.ok()
        } else {
            None
        };

        let suggestions = dry_run_core::query::suggest_index(
            &params.sql, &schema, plan.as_ref().map(|p| &p.plan), pg_version.as_ref(),
        ).map_err(|e| McpError::invalid_params(format!("analysis failed: {e}"), None))?;

        if suggestions.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No index suggestions — existing indexes appear sufficient, or tables are too small to benefit.".to_string(),
            )]));
        }

        let json = serde_json::to_string_pretty(&suggestions)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Lint the loaded schema against convention rules — naming, types, constraints, timestamps")]
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

        let report = dry_run_core::lint::lint_schema(&target, &self.lint_config);

        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Overview of stats health across all nodes — tables sorted by total seq_scans, highlighting missing indexes, node routing imbalances, and stale stats. Works offline.")]
    async fn stats_summary(&self) -> Result<CallToolResult, McpError> {
        let snapshot = self.get_schema().await?;

        if snapshot.node_stats.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No per-node stats available. Import stats with:\n  \
                 dry-run import schema.json --stats r1.json r2.json"
                    .to_string(),
            )]));
        }

        // aggregate per-table stats across nodes
        struct TableAgg {
            qualified: String,
            total_seq_scan: i64,
            total_idx_scan: i64,
            per_node_seq: Vec<(String, i64)>,
        }

        let mut agg: std::collections::BTreeMap<String, TableAgg> =
            std::collections::BTreeMap::new();

        for ns in &snapshot.node_stats {
            for ts in &ns.table_stats {
                let key = format!("{}.{}", ts.schema, ts.table);
                let entry = agg.entry(key.clone()).or_insert_with(|| TableAgg {
                    qualified: key,
                    total_seq_scan: 0,
                    total_idx_scan: 0,
                    per_node_seq: Vec::new(),
                });
                entry.total_seq_scan += ts.stats.seq_scan;
                entry.total_idx_scan += ts.stats.idx_scan;
                entry.per_node_seq.push((ns.source.clone(), ts.stats.seq_scan));
            }
        }

        let mut sorted: Vec<TableAgg> = agg.into_values().collect();
        sorted.sort_by(|a, b| b.total_seq_scan.cmp(&a.total_seq_scan));

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!(
            "Stats summary across {} node(s), {} table(s):\n",
            snapshot.node_stats.len(),
            sorted.len()
        ));

        lines.push(format!(
            "{:<40} {:>12} {:>12}  {}",
            "table", "seq_scan", "idx_scan", "flags"
        ));
        lines.push("-".repeat(90));

        for ta in sorted.iter().take(30) {
            let mut flags = Vec::new();

            // high seq_scan / low idx_scan ratio
            if ta.total_seq_scan > 100 && ta.total_idx_scan > 0 {
                let ratio = ta.total_seq_scan as f64 / ta.total_idx_scan as f64;
                if ratio > 0.5 {
                    flags.push("⚠ high seq/idx ratio".to_string());
                }
            } else if ta.total_seq_scan > 100 && ta.total_idx_scan == 0 {
                flags.push("⚠ seq_scan only (no idx_scan)".to_string());
            }

            // node imbalance: seq_scan differs >5x across nodes
            if ta.per_node_seq.len() >= 2 {
                let nonzero: Vec<i64> = ta.per_node_seq.iter().map(|(_, v)| *v).filter(|v| *v > 0).collect();
                if nonzero.len() >= 2 {
                    let min = *nonzero.iter().min().unwrap_or(&1);
                    let max = *nonzero.iter().max().unwrap_or(&1);
                    if min > 0 && max / min >= 5 {
                        flags.push("⚠ node imbalance".to_string());
                    }
                }
            }

            let flag_str = if flags.is_empty() {
                String::new()
            } else {
                flags.join(", ")
            };

            lines.push(format!(
                "{:<40} {:>12} {:>12}  {}",
                ta.qualified,
                format_number(ta.total_seq_scan),
                format_number(ta.total_idx_scan),
                flag_str,
            ));
        }

        if sorted.len() > 30 {
            lines.push(format!("... and {} more tables", sorted.len() - 30));
        }

        Ok(CallToolResult::success(vec![Content::text(lines.join("\n"))]))
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
        lines.push(format!(
            "Stats for {qualified} across {} node(s):\n",
            snapshot.node_stats.len()
        ));

        // header
        lines.push(format!(
            "{:<16} {:>12} {:>10} {:>10} {:>10} {:>12}",
            "", "reltuples", "relpages", "seq_scan", "idx_scan", "table_size"
        ));

        let mut seq_scans: Vec<(&str, i64)> = Vec::new();

        for ns in &snapshot.node_stats {
            let ts = ns
                .table_stats
                .iter()
                .find(|t| t.table == params.table && t.schema == schema_name);

            if let Some(ts) = ts {
                let size_mb = ts.stats.table_size / (1024 * 1024);
                lines.push(format!(
                    "{:<16} {:>12} {:>10} {:>10} {:>10} {:>9} MB",
                    ns.source,
                    format_number(ts.stats.reltuples as i64),
                    format_number(ts.stats.relpages),
                    format_number(ts.stats.seq_scan),
                    format_number(ts.stats.idx_scan),
                    format_number(size_mb),
                ));
                seq_scans.push((&ns.source, ts.stats.seq_scan));
            } else {
                lines.push(format!("{:<16} (no data for this table)", ns.source));
            }
        }

        // anomaly detection: seq_scan imbalance
        if seq_scans.len() >= 2 {
            let min_seq = seq_scans.iter().map(|(_, v)| *v).filter(|v| *v > 0).min();
            let max_entry = seq_scans.iter().max_by_key(|(_, v)| *v);
            if let (Some(min), Some((name, max))) = (min_seq, max_entry) {
                if min > 0 && *max / min >= 5 {
                    lines.push(String::new());
                    lines.push(format!(
                        "⚠ {name} has {}x more seq_scans than the lowest node — \
                         likely serving unindexed query patterns. Check application routing.",
                        max / min
                    ));
                }
            }
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

        Ok(CallToolResult::success(vec![Content::text(lines.join("\n"))]))
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

#[tool_handler]
impl ServerHandler for DryRunServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "PostgreSQL schema intelligence server. \
                 Tools: list_tables, describe_table, search_schema, find_related, \
                 schema_diff, validate_query, explain_query, advise, \
                 check_migration, suggest_index, lint_schema, \
                 stats_summary, compare_nodes, refresh_schema."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
