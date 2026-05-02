use std::path::PathBuf;
use std::sync::Arc;

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
};
use tokio::sync::RwLock;
use tracing::info;

use dry_run_core::audit::AuditConfig;
use dry_run_core::history::{SnapshotKey, SnapshotRef, SnapshotStore};
use dry_run_core::lint::LintConfig;
use dry_run_core::schema::{ConstraintKind, NodeSelector, QualifiedName};
use dry_run_core::{AnnotatedSnapshot, DryRun, HistoryStore, SchemaSnapshot};

use crate::pgmustard::PgMustardClient;

use super::helpers::{format_node_table_breakdown, format_number, to_mcp_err};
use super::params::*;

async fn persist_refresh(
    store: &HistoryStore,
    key: &SnapshotKey,
    schema: &SchemaSnapshot,
    planner: Option<&dry_run_core::PlannerStatsSnapshot>,
    activity_by_node: &std::collections::BTreeMap<String, dry_run_core::ActivityStatsSnapshot>,
) {
    if let Err(e) = store.put(key, schema).await {
        tracing::warn!(error = %e, "failed to persist schema");
    }
    if let Some(p) = planner
        && let Err(e) = store.put_planner_stats(key, p).await
    {
        tracing::warn!(error = %e, "failed to persist planner stats");
    }
    if let Some(a) = activity_by_node.get("primary")
        && let Err(e) = store.put_activity_stats(key, a).await
    {
        tracing::warn!(error = %e, "failed to persist activity stats");
    }
}

fn wrap_schema_only(schema: SchemaSnapshot) -> AnnotatedSnapshot {
    AnnotatedSnapshot {
        schema,
        planner: None,
        activity_by_node: std::collections::BTreeMap::new(),
    }
}

#[derive(Clone)]
pub struct DryRunServer {
    ctx: Option<Arc<DryRun>>,
    app_version: String,
    pg_version_display: String,
    database_name: String,
    schema: Arc<RwLock<Option<AnnotatedSnapshot>>>,
    history: Option<Arc<HistoryStore>>,
    snapshot_key: Option<SnapshotKey>,
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
        let ctx = db.map(|(_url, ctx)| Arc::new(ctx));

        let pg_version_display =
            dry_run_core::PgVersion::parse_from_version_string(&snapshot.pg_version)
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
            app_version: app_version.to_string(),
            pg_version_display,
            database_name,
            schema: Arc::new(RwLock::new(Some(wrap_schema_only(snapshot)))),
            history: None,
            snapshot_key: None,
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
            app_version: app_version.to_string(),
            pg_version_display: String::new(),
            database_name: String::new(),
            schema: Arc::new(RwLock::new(None)),
            history: None,
            snapshot_key: None,
            lint_config,
            audit_config: AuditConfig::default(),
            pgmustard: None,
            schema_candidates,
            tool_router: Self::tool_router(),
        }
    }

    #[allow(dead_code)]
    pub fn with_history(mut self, store: HistoryStore, key: Option<SnapshotKey>) -> Self {
        self.history = Some(Arc::new(store));
        self.snapshot_key = key;
        self
    }

    async fn get_schema(&self) -> Result<SchemaSnapshot, McpError> {
        Ok(self.get_annotated().await?.schema)
    }

    async fn get_annotated(&self) -> Result<AnnotatedSnapshot, McpError> {
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
        if self.ctx.is_some() {
            "live"
        } else {
            "offline"
        }
    }

    fn wrap_text(&self, body: &str, hint: Option<&str>) -> String {
        let header = format!(
            "PostgreSQL {} | {} | {}\n",
            self.pg_version_display,
            self.database_name,
            self.mode_str()
        );
        if let Some(h) = hint {
            format!("{header}{body}\n\n> {h}")
        } else {
            format!("{header}{body}")
        }
    }

    fn inject_meta(&self, val: &mut serde_json::Value, hint: Option<&str>) {
        let obj = val
            .as_object_mut()
            .expect("inject_meta expects a JSON object");
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
        let annotated = self.get_annotated().await?;
        let limit = params.limit.unwrap_or(50);
        let offset = params.offset.unwrap_or(0);
        let sort_by = params.sort.as_deref().unwrap_or("name");

        struct TableEntry {
            line: String,
            name: String,
            rows: f64,
            size: i64,
        }

        // Default node selector: "primary" — single-node planner data
        // is the right fit for a row-count summary. The "N nodes" suffix
        // counts how many distinct activity captures we have, which
        // signals "we have multi-node data for this cluster" but doesn't
        // change the headline number.
        let view = annotated.view(None);
        let node_count = annotated.activity_by_node.len();

        let mut entries: Vec<TableEntry> = annotated
            .schema
            .tables
            .iter()
            .filter(|t| params.schema.as_ref().is_none_or(|s| &t.schema == s))
            .map(|t| {
                let qn = QualifiedName::new(&t.schema, &t.name);
                let rows = view.reltuples(&qn).unwrap_or(0.0);
                let size = view.table_size(&qn).unwrap_or(0);
                let row_est = if rows > 0.0 {
                    if node_count > 0 {
                        format!(" (~{} rows, {} nodes)", rows as i64, node_count)
                    } else {
                        format!(" (~{} rows)", rows as i64)
                    }
                } else {
                    String::new()
                };
                let partition = t
                    .partition_info
                    .as_ref()
                    .map(|pi| {
                        format!(
                            " [partitioned: {} on '{}', {} children]",
                            pi.strategy,
                            pi.key,
                            pi.children.len()
                        )
                    })
                    .unwrap_or_default();
                let comment = t
                    .comment
                    .as_ref()
                    .map(|c| format!(" — {c}"))
                    .unwrap_or_default();
                let name = format!("{}.{}", t.schema, t.name);
                let line = format!("{name}{row_est}{partition}{comment}");
                TableEntry {
                    line,
                    name,
                    rows,
                    size,
                }
            })
            .collect();

        match sort_by {
            "rows" => entries.sort_by(|a, b| {
                b.rows
                    .partial_cmp(&a.rows)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            "size" => entries.sort_by_key(|b| std::cmp::Reverse(b.size)),
            _ => entries.sort_by(|a, b| a.name.cmp(&b.name)),
        }

        let total = entries.len();
        let paginated: Vec<&str> = entries
            .iter()
            .skip(offset)
            .take(limit)
            .map(|e| e.line.as_str())
            .collect();

        let body = if paginated.is_empty() {
            "No tables found.".to_string()
        } else if offset > 0 || paginated.len() < total {
            format!(
                "Showing {}-{} of {} table(s):\n{}",
                offset + 1,
                offset + paginated.len(),
                total,
                paginated.join("\n")
            )
        } else {
            format!("{} table(s):\n{}", total, paginated.join("\n"))
        };

        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        description = "Table columns, types, constraints, indexes and stats. Per-node stats when present."
    )]
    async fn describe_table(
        &self,
        Parameters(params): Parameters<DescribeTableParams>,
    ) -> Result<CallToolResult, McpError> {
        // Pull the annotated bundle — every stats field this tool surfaces
        // (reltuples, dead tuples, last vacuum, per-node breakdown, column
        // profiles) reads from planner / activity, not from the legacy
        // embedded fields.
        let annotated = self.get_annotated().await?;
        let schema_name = params.schema.as_deref().unwrap_or("public");

        let table = annotated
            .schema
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
        let qn = QualifiedName::new(schema_name, &params.table);
        let view = annotated.view(None);
        let table_rows = view.reltuples(&qn).unwrap_or(0.0);

        // Build column profiles — pull each column's stats out of the
        // planner snapshot via `column_stats(qn, name)`. Profile is None
        // when no stats are present, in which case the column is omitted
        // from the profiles array (matches legacy behavior).
        let profiles: Vec<serde_json::Value> = table
            .columns
            .iter()
            .filter_map(|col| {
                let stats = view.column_stats(&qn, &col.name);
                dry_run_core::schema::profile_column(&col.name, &col.type_name, stats, table_rows)
                    .map(|p| {
                        serde_json::json!({
                            "column": col.name,
                            "profile": p,
                        })
                    })
            })
            .collect();

        // Synthesize a "stats" JSON object that mirrors the legacy
        // TableStats shape, but built from planner sizing + (merged-or-single)
        // activity. Returns an empty object when no stats are captured —
        // intentionally distinct from `null` so consumers can tell the
        // difference between "no snapshot yet" (object missing) vs.
        // "snapshot exists, no rows for this table" (object empty).
        let synth_stats = serde_json::json!({
            "reltuples": view.reltuples(&qn),
            "relpages": view.relpages(&qn),
            "table_size": view.table_size(&qn),
            "dead_tuples": view.n_dead_tup_sum(&qn),
            "seq_scan": view.seq_scan_sum(&qn),
            "last_vacuum": view.last_vacuum_max(&qn),
            "last_analyze": view.last_analyze_max(&qn),
            "vacuum_count": view.vacuum_count_sum(&qn),
        });

        let mut json_val = match detail {
            "full" => {
                let mut v = serde_json::to_value(table).map_err(|e| {
                    McpError::internal_error(format!("serialization error: {e}"), None)
                })?;
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("stats".into(), synth_stats.clone());
                    if !profiles.is_empty() {
                        obj.insert("column_profiles".into(), serde_json::Value::Array(profiles));
                    }
                }
                v
            }
            "stats" => {
                let mut result = serde_json::json!({
                    "schema": table.schema,
                    "name": table.name,
                    "stats": synth_stats,
                });
                if let Some(obj) = result.as_object_mut()
                    && !profiles.is_empty()
                {
                    obj.insert("column_profiles".into(), serde_json::Value::Array(profiles));
                }
                result
            }
            _ => {
                // summary: compact columns without raw stats
                let compact_cols: Vec<serde_json::Value> = table
                    .columns
                    .iter()
                    .map(|c| {
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
                    })
                    .collect();
                let compact_idxs: Vec<serde_json::Value> = table
                    .indexes
                    .iter()
                    .map(|i| {
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
                    })
                    .collect();
                let mut result = serde_json::json!({
                    "schema": table.schema,
                    "name": table.name,
                    "columns": compact_cols,
                    "constraints": table.constraints,
                    "indexes": compact_idxs,
                    "comment": table.comment,
                    "stats": synth_stats,
                    "partition_info": table.partition_info,
                });
                if let Some(obj) = result.as_object_mut()
                    && !profiles.is_empty()
                {
                    obj.insert("column_profiles".into(), serde_json::Value::Array(profiles));
                }
                result
            }
        };

        let has_fks = table
            .constraints
            .iter()
            .any(|c| c.kind == ConstraintKind::ForeignKey);
        let hint = if has_fks {
            Some(
                "This table has foreign keys — use find_related for JOIN patterns with related tables.",
            )
        } else {
            None
        };
        self.inject_meta(&mut json_val, hint);

        let mut text = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        // Per-node breakdown trailer — only meaningful when we have ≥ 2
        // nodes' worth of activity. Single-node clusters skip the section.
        if let Some(breakdown) = format_node_table_breakdown(&annotated, schema_name, &params.table)
        {
            text.push_str(&breakdown);
        }

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        description = "Substring search over tables, columns, views, functions, enums, indexes, comments."
    )]
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
                let comment = table
                    .comment
                    .as_ref()
                    .map(|c| format!(" — {c}"))
                    .unwrap_or_default();
                results.push(format!("TABLE {qualified}{comment}"));
            }

            for col in &table.columns {
                if col.name.to_lowercase().contains(&query) {
                    results.push(format!(
                        "COLUMN {qualified}.{} ({})",
                        col.name, col.type_name
                    ));
                }
                if let Some(comment) = &col.comment
                    && comment.to_lowercase().contains(&query)
                {
                    results.push(format!(
                        "COLUMN COMMENT {qualified}.{}: {comment}",
                        col.name
                    ));
                }
            }

            if let Some(comment) = &table.comment
                && comment.to_lowercase().contains(&query)
                && !table.name.to_lowercase().contains(&query)
            {
                results.push(format!("TABLE COMMENT {qualified}: {comment}"));
            }

            for con in &table.constraints {
                if let Some(def) = &con.definition
                    && def.to_lowercase().contains(&query)
                {
                    results.push(format!(
                        "CONSTRAINT {qualified}.{} ({:?}): {def}",
                        con.name, con.kind
                    ));
                }
            }

            for idx in &table.indexes {
                if idx.name.to_lowercase().contains(&query)
                    || idx.definition.to_lowercase().contains(&query)
                {
                    results.push(format!("INDEX {qualified}: {}", idx.definition));
                }
            }
        }

        for view in &snapshot.views {
            if view.name.to_lowercase().contains(&query) {
                let kind = if view.is_materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };
                results.push(format!("{kind} {}.{}", view.schema, view.name));
            }
        }

        for func in &snapshot.functions {
            if func.name.to_lowercase().contains(&query) {
                results.push(format!(
                    "FUNCTION {}.{}({})",
                    func.schema, func.name, func.identity_args
                ));
            }
        }

        for e in &snapshot.enums {
            if e.name.to_lowercase().contains(&query)
                || e.labels.iter().any(|l| l.to_lowercase().contains(&query))
            {
                results.push(format!(
                    "ENUM {}.{}: [{}]",
                    e.schema,
                    e.name,
                    e.labels.join(", ")
                ));
            }
        }

        let limit = params.limit.unwrap_or(30);
        let offset = params.offset.unwrap_or(0);
        let total = results.len();
        let paginated: Vec<&str> = results
            .iter()
            .skip(offset)
            .take(limit)
            .map(|s| s.as_str())
            .collect();

        let body = if paginated.is_empty() {
            format!("No matches for '{}'.", params.query)
        } else if offset > 0 || paginated.len() < total {
            format!(
                "Showing {}-{} of {} match(es) for '{}':\n{}",
                offset + 1,
                offset + paginated.len(),
                total,
                params.query,
                paginated.join("\n")
            )
        } else {
            format!(
                "{} match(es) for '{}':\n{}",
                total,
                params.query,
                paginated.join("\n")
            )
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
            .ok_or_else(|| {
                McpError::invalid_params(format!("table '{qualified}' not found"), None)
            })?;

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("Relationships for {qualified}:\n"));

        let outgoing: Vec<_> = table
            .constraints
            .iter()
            .filter(|c| c.kind == ConstraintKind::ForeignKey)
            .collect();

        if outgoing.is_empty() {
            lines.push("Outgoing FKs: none".into());
        } else {
            lines.push("Outgoing FKs:".into());
            for fk in &outgoing {
                let ref_table = fk.fk_table.as_deref().unwrap_or("?");
                let local_cols = fk.columns.join(", ");
                let ref_cols = fk.fk_columns.join(", ");
                lines.push(format!(
                    "  {qualified}({local_cols}) -> {ref_table}({ref_cols})"
                ));
                lines.push(format!("    JOIN: SELECT * FROM {qualified} JOIN {ref_table} ON {}.{local_cols} = {ref_table}.{ref_cols}", params.table));
            }
        }

        let mut incoming: Vec<String> = Vec::new();
        for other in &snapshot.tables {
            for fk in &other.constraints {
                if fk.kind != ConstraintKind::ForeignKey {
                    continue;
                }
                if let Some(ref_table) = &fk.fk_table
                    && ref_table == &qualified
                {
                    let other_qualified = format!("{}.{}", other.schema, other.name);
                    let local_cols = fk.columns.join(", ");
                    let ref_cols = fk.fk_columns.join(", ");
                    incoming.push(format!(
                        "  {other_qualified}({local_cols}) -> {qualified}({ref_cols})"
                    ));
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

    #[tool(
        description = "Diff two snapshots, or the latest snapshot against the live schema. Needs --history."
    )]
    async fn schema_diff(
        &self,
        Parameters(params): Parameters<SchemaDiffParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self
            .history
            .as_ref()
            .ok_or_else(|| McpError::internal_error("history store not configured", None))?;
        let key = self.snapshot_key.as_ref().ok_or_else(|| {
            McpError::internal_error(
                "schema_diff needs a snapshot key — pass --db or set [default].profile",
                None,
            )
        })?;

        let from_snapshot = match &params.from {
            Some(hash) => store
                .get(key, SnapshotRef::Hash(hash.clone()))
                .await
                .map_err(to_mcp_err)?,
            None => store
                .get(key, SnapshotRef::Latest)
                .await
                .map_err(to_mcp_err)?,
        };

        let to_snapshot = match &params.to {
            Some(hash) => store
                .get(key, SnapshotRef::Hash(hash.clone()))
                .await
                .map_err(to_mcp_err)?,
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

    #[tool(
        description = "Parse SQL and check it against the schema. Flags missing tables or columns and common anti-patterns. Offline."
    )]
    async fn validate_query(
        &self,
        Parameters(params): Parameters<ValidateQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let annotated = self.get_annotated().await?;
        let view = annotated.view(None);
        let result = dry_run_core::query::validate_query(&params.sql, &view)
            .map_err(|e| McpError::invalid_params(format!("SQL parse error: {e}"), None))?;

        let hint = if result.valid && !result.warnings.is_empty() {
            Some(
                "Query is valid but has warnings. Use advise for index suggestions and plan analysis.",
            )
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

    #[tool(
        description = "Run EXPLAIN on a query. Pass analyze=true to run EXPLAIN ANALYZE. Needs live DB."
    )]
    async fn explain_query(
        &self,
        Parameters(params): Parameters<ExplainQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        // Pull annotated so plan-warning rules have planner reltuples
        // available as a fallback when the plan's own row estimate is zero.
        let annotated = self.get_annotated().await.ok();
        let view = annotated.as_ref().map(|a| a.view(None));
        let ctx = self.require_live_db()?;

        let result = dry_run_core::query::explain_query(
            ctx.pool(),
            &params.sql,
            params.analyze.unwrap_or(false),
            view.as_ref(),
        )
        .await
        .map_err(|e| McpError::invalid_params(format!("EXPLAIN failed: {e}"), None))?;

        let hint = if !result.warnings.is_empty() {
            Some(
                "Warnings detected. Use advise for index suggestions and actionable recommendations.",
            )
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

    #[tool(
        description = "Plan analysis, anti-pattern checks and index suggestions for a query. Uses EXPLAIN when a live DB is available, static analysis otherwise."
    )]
    async fn advise(
        &self,
        Parameters(params): Parameters<AdviseParams>,
    ) -> Result<CallToolResult, McpError> {
        // Pull the annotated bundle — advise's stats-aware refinements
        // (selectivity, partial-index suggestions, per-replica seq_scan
        // breakdown) all hang off planner/activity, not the raw schema.
        let annotated = self.get_annotated().await?;
        let pg_version =
            dry_run_core::PgVersion::parse_from_version_string(&annotated.schema.pg_version).ok();
        let include_idx = params.include_index_suggestions.unwrap_or(true);

        // Default node selector: "primary" for a single-node view —
        // advise is a planner-stats-driven tool and primary is where
        // those originate. Per-node breakdowns inside advise itself
        // still iterate every node via `seq_scan_per_node`.
        let view = annotated.view(None);

        let explain_result = if let Some(ctx) = &self.ctx {
            dry_run_core::query::explain_query(
                ctx.pool(),
                &params.sql,
                params.analyze.unwrap_or(false),
                Some(&view),
            )
            .await
            .ok()
        } else {
            None
        };

        let advise_result = dry_run_core::query::advise_with_index_suggestions(
            &params.sql,
            explain_result.as_ref().map(|r| &r.plan),
            &view,
            pg_version.as_ref(),
            include_idx,
        )
        .map_err(|e| McpError::invalid_params(format!("analysis failed: {e}"), None))?;

        let has_ddl_suggestions = !advise_result.index_suggestions.is_empty();
        let hint = if has_ddl_suggestions {
            Some(
                "Index suggestions contain DDL. Run each through check_migration before applying — it checks lock safety and duration.",
            )
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

    #[tool(
        description = "Analyze an existing EXPLAIN plan (JSON) against the schema. Returns warnings, index and safety hints. Offline."
    )]
    async fn analyze_plan(
        &self,
        Parameters(params): Parameters<AnalyzePlanParams>,
    ) -> Result<CallToolResult, McpError> {
        let annotated = self.get_annotated().await?;
        let pg_version =
            dry_run_core::PgVersion::parse_from_version_string(&annotated.schema.pg_version).ok();

        // Parse the plan JSON — supports both wrapped [{"Plan": ...}] and bare {"Plan": ...}
        let plan_value = if let Some(arr) = params.plan_json.as_array() {
            arr.first().and_then(|obj| obj.get("Plan")).ok_or_else(|| {
                McpError::invalid_params("plan_json must contain a Plan key", None)
            })?
        } else {
            params.plan_json.get("Plan").ok_or_else(|| {
                McpError::invalid_params("plan_json must contain a Plan key", None)
            })?
        };

        let plan = dry_run_core::query::parse_plan_json(plan_value)
            .map_err(|e| McpError::invalid_params(format!("failed to parse plan: {e}"), None))?;

        let view = annotated.view(None);
        let warnings = dry_run_core::query::detect_plan_warnings(&plan, Some(&view));

        let advise_result = dry_run_core::query::advise_with_index_suggestions(
            &params.sql,
            Some(&plan),
            &view,
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
            Some(
                "Index suggestions contain DDL. Run each through check_migration before applying — it checks lock safety and duration.",
            )
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

    #[tool(
        description = "Check a DDL statement for lock level, duration, table-size impact, and suggest safer alternatives."
    )]
    async fn check_migration(
        &self,
        Parameters(params): Parameters<CheckMigrationParams>,
    ) -> Result<CallToolResult, McpError> {
        let annotated = self.get_annotated().await?;
        let pg_version =
            dry_run_core::PgVersion::parse_from_version_string(&annotated.schema.pg_version).ok();
        let view = annotated.view(None);

        let checks = dry_run_core::query::check_migration(&params.ddl, &view, pg_version.as_ref())
            .map_err(|e| McpError::invalid_params(format!("DDL parse error: {e}"), None))?;

        if checks.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "Could not identify a specific DDL operation to check. \
                 Supported: ALTER TABLE (ADD/DROP COLUMN, SET NOT NULL, ALTER TYPE, ADD CONSTRAINT), \
                 CREATE INDEX, RENAME.".to_string(),
            )]));
        }

        let has_dangerous = checks
            .iter()
            .any(|c| c.safety == dry_run_core::query::SafetyRating::Dangerous);
        let hint = if has_dangerous {
            Some(
                "DANGEROUS operations detected. Check the recommendation and rollback_ddl fields for safe alternatives.",
            )
        } else {
            None
        };

        let mut json_val = serde_json::json!({ "checks": checks });
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Schema quality checks. scope=conventions, audit, or all (default). Offline."
    )]
    async fn lint_schema(
        &self,
        Parameters(params): Parameters<LintSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        // Pull the full annotated bundle — we need it for the audit pass,
        // which contains stats-aware rules. Lint itself is DDL-only and
        // just borrows `target.schema` below.
        let mut target = self.get_annotated().await?;
        if let Some(schema_filter) = &params.schema {
            target.schema.tables.retain(|t| &t.schema == schema_filter);
        }
        if let Some(table_filter) = &params.table {
            target.schema.tables.retain(|t| &t.name == table_filter);
        }

        let scope = params.scope.as_deref().unwrap_or("all");
        let mut result = serde_json::Map::new();

        if scope == "all" || scope == "conventions" {
            // Conventions/lint reads no stats — DDL only.
            let report = dry_run_core::lint::lint_schema(&target.schema, &self.lint_config);
            let compact = dry_run_core::lint::compact_report(&report, 5);
            result.insert(
                "conventions".into(),
                serde_json::to_value(&compact).unwrap_or(serde_json::Value::Null),
            );
        }

        let has_ddl_fixes = if scope == "all" || scope == "audit" {
            // Audit needs planner sizing for the bloat / vacuum-defaults rules
            // — pass the annotated view so those have a chance to fire.
            let report = dry_run_core::audit::run_audit(&target.view(None), &self.audit_config);
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
            Some(
                "Some findings include ddl_fix fields. Run those through check_migration before applying to verify lock safety.",
            )
        } else {
            None
        };

        let mut json_val = serde_json::Value::Object(result);
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Autovacuum status with thresholds, dead tuples and tuning hints. Offline."
    )]
    async fn vacuum_health(
        &self,
        Parameters(params): Parameters<VacuumHealthParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut annotated = self.get_annotated().await?;
        if let Some(schema_filter) = &params.schema {
            annotated
                .schema
                .tables
                .retain(|t| &t.schema == schema_filter);
        }
        if let Some(table_filter) = &params.table {
            annotated.schema.tables.retain(|t| &t.name == table_filter);
        }
        let results = dry_run_core::schema::vacuum::analyze_vacuum_health(&annotated.view(None));

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

    #[tool(
        description = "Health checks. kind=stale_stats, unused_indexes, anomalies, bloated_indexes, or all (default). Offline."
    )]
    async fn detect(
        &self,
        Parameters(params): Parameters<DetectParams>,
    ) -> Result<CallToolResult, McpError> {
        // Pull the cached annotated bundle and clone it — we filter
        // tables in-place to honor the schema/table query params, and we
        // don't want those mutations to leak back into the shared cache.
        //
        // Activity rows reference qualified-name keys, not table OIDs, so
        // they're naturally narrowed by the lookups in
        // `AnnotatedSnapshot::unused_indexes` / `seq_scan_imbalance` once
        // we've thinned out `schema.tables`. No need to scrub the
        // activity_by_node map by hand.
        let mut annotated = self.get_annotated().await?;
        if let Some(schema_filter) = &params.schema {
            annotated
                .schema
                .tables
                .retain(|t| &t.schema == schema_filter);
        }
        if let Some(table_filter) = &params.table {
            annotated.schema.tables.retain(|t| &t.name == table_filter);
        }

        let kind = params.kind.as_deref().unwrap_or("all");

        let mut result = serde_json::Map::new();

        let run_stale = kind == "all" || kind == "stale_stats";
        let run_unused = kind == "all" || kind == "unused_indexes";
        let run_anomalies = kind == "all" || kind == "anomalies";
        let run_bloated = kind == "all" || kind == "bloated_indexes";

        let mut found_stale = false;
        let mut found_unused = false;

        if run_stale {
            // 7-day staleness threshold — matches the legacy default.
            // `stale_stats` walks every node in the selector and emits one
            // entry per (node, table) that's stale or never analyzed.
            let stale = annotated.stale_stats(&NodeSelector::All, 7);
            found_stale = !stale.is_empty();
            result.insert(
                "stale_stats".into(),
                serde_json::to_value(&stale).unwrap_or(serde_json::Value::Null),
            );
        }

        if run_unused {
            // Cluster-wide question — sum scans across all known nodes.
            // An index that's unused on the primary may still be hot on
            // a read replica, so we deliberately don't restrict to one node.
            let unused = annotated.unused_indexes(&NodeSelector::All);
            found_unused = !unused.is_empty();
            result.insert(
                "unused_indexes".into(),
                serde_json::to_value(&unused).unwrap_or(serde_json::Value::Null),
            );
        }

        if run_anomalies {
            let mut anomalies = Vec::new();
            for table in &annotated.schema.tables {
                let qn = dry_run_core::schema::QualifiedName::new(&table.schema, &table.name);
                if let Some(imb) = annotated.seq_scan_imbalance(&qn) {
                    anomalies.push(serde_json::json!({
                        "table": format!("{}.{}", table.schema, table.name),
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
            // Bloat needs IndexSizing from the planner snapshot — pass the
            // annotated view so the rule can pull it via `index_sizing()`.
            let bloated =
                dry_run_core::schema::detect_bloated_indexes(&annotated.view(None), threshold);
            result.insert(
                "bloated_indexes".into(),
                serde_json::to_value(&bloated).unwrap_or(serde_json::Value::Null),
            );
        }

        let hint = match (found_stale, found_unused) {
            (true, true) => Some(
                "Stale stats may cause bad plans — run ANALYZE. Unused indexes add write overhead — verify with compare_nodes before dropping.",
            ),
            (true, false) => {
                Some("Stale stats may cause bad query plans — consider running ANALYZE.")
            }
            (false, true) => Some(
                "Unused indexes add write overhead. Use compare_nodes to verify across all replicas before dropping.",
            ),
            (false, false) => None,
        };

        let mut json_val = serde_json::Value::Object(result);
        self.inject_meta(&mut json_val, hint);

        let json = serde_json::to_string_pretty(&json_val)
            .map_err(|e| McpError::internal_error(format!("serialization error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Per-node stats for a table. Shows reltuples, relpages, scans, size and per-index numbers. Offline."
    )]
    async fn compare_nodes(
        &self,
        Parameters(params): Parameters<CompareNodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let annotated = self.get_annotated().await?;
        let schema_name = params.schema.as_deref().unwrap_or("public");
        let qualified = format!("{schema_name}.{}", params.table);
        let qn = QualifiedName::new(schema_name, &params.table);

        if annotated.activity_by_node.is_empty() {
            // No per-node activity captured — can't compare. Tell the user
            // exactly which command will populate it.
            return Ok(CallToolResult::success(vec![Content::text(
                "No per-node activity stats available. Capture from each replica with:\n  \
                 dryrun snapshot activity --from <replica-url> --label <name>"
                    .to_string(),
            )]));
        }

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!(
            "Stats for {qualified} across {} node(s):",
            annotated.activity_by_node.len()
        ));

        if let Some(breakdown) = format_node_table_breakdown(&annotated, schema_name, &params.table)
        {
            lines.push(breakdown);
        }

        // Anomaly detection — flag if one node is doing 5x+ the seq_scans
        // of the quietest non-zero node. Often points at a routing
        // misconfiguration or an unindexed query slipping past primary.
        if let Some(imb) = annotated.seq_scan_imbalance(&qn) {
            lines.push(String::new());
            lines.push(format!(
                "⚠ {} has {}x more seq_scans than the lowest node — \
                 likely serving unindexed query patterns. Check application routing.",
                imb.hot_node, imb.multiplier,
            ));
        }

        // Per-index breakdown — pull each index belonging to this table
        // out of the schema, then ask each node's activity what its
        // idx_scan counter is for that index.
        let mut index_data: std::collections::BTreeMap<String, Vec<(String, i64)>> =
            std::collections::BTreeMap::new();
        if let Some(table) = annotated
            .schema
            .tables
            .iter()
            .find(|t| t.name == params.table && t.schema == schema_name)
        {
            for idx in &table.indexes {
                let idx_qn = QualifiedName::new(schema_name, &idx.name);
                let per_node = annotated.view(None).idx_scan_per_node(&idx_qn);
                if !per_node.is_empty() {
                    index_data.insert(idx.name.clone(), per_node);
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

        // Flag unused indexes for this table — `unused_indexes` already
        // skips primary keys and aggregates across selected nodes.
        let unused = annotated.unused_indexes(&NodeSelector::All);
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

    #[tool(
        description = "Compare the live local DB against the loaded production snapshot. Each diff is tagged ahead, behind or diverged. Needs live DB."
    )]
    async fn check_drift(&self) -> Result<CallToolResult, McpError> {
        let ctx = self.require_live_db()?;
        let prod_snapshot = self.get_schema().await?;
        let local_snapshot = ctx
            .introspect_schema()
            .await
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
        let schema = ctx
            .introspect_schema()
            .await
            .map_err(|e| McpError::internal_error(format!("introspection failed: {e}"), None))?;
        let schema_hash = schema.content_hash.clone();

        let planner = match ctx.introspect_planner_stats(&schema_hash).await {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!(error = %e, "planner stats introspection failed; continuing without");
                None
            }
        };

        let mut activity_by_node = std::collections::BTreeMap::new();
        match ctx.introspect_activity_stats(&schema_hash, "primary").await {
            Ok(a) => {
                activity_by_node.insert("primary".to_string(), a);
            }
            Err(e) => {
                tracing::warn!(error = %e, "activity stats introspection failed; continuing without");
            }
        }

        if let (Some(store), Some(key)) = (self.history.as_ref(), self.snapshot_key.as_ref()) {
            persist_refresh(store, key, &schema, planner.as_ref(), &activity_by_node).await;
        }

        let body = format!(
            "Schema refreshed: {} tables, {} views, {} functions (hash: {})\n\
             Planner stats: {}\n\
             Activity stats: {} node(s)",
            schema.tables.len(),
            schema.views.len(),
            schema.functions.len(),
            &schema_hash[..16],
            if planner.is_some() {
                "captured"
            } else {
                "unavailable"
            },
            activity_by_node.len(),
        );

        *self.schema.write().await = Some(AnnotatedSnapshot {
            schema,
            planner,
            activity_by_node,
        });

        let text = self.wrap_text(&body, None);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        description = "Reload the on-disk schema without restarting. Run after `dryrun dump-schema`."
    )]
    async fn reload_schema(&self) -> Result<CallToolResult, McpError> {
        for candidate in &self.schema_candidates {
            if !candidate.exists() {
                continue;
            }
            let json = std::fs::read_to_string(candidate).map_err(|e| {
                McpError::internal_error(
                    format!("failed to read {}: {e}", candidate.display()),
                    None,
                )
            })?;
            let snapshot: SchemaSnapshot = serde_json::from_str(&json).map_err(|e| {
                McpError::internal_error(
                    format!("failed to parse {}: {e}", candidate.display()),
                    None,
                )
            })?;

            let body = format!(
                "Schema loaded from {}: {} tables, {} views, {} functions",
                candidate.display(),
                snapshot.tables.len(),
                snapshot.views.len(),
                snapshot.functions.len(),
            );

            *self.schema.write().await = Some(wrap_schema_only(snapshot));

            let text = self.wrap_text(&body, None);
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let paths: Vec<_> = self
            .schema_candidates
            .iter()
            .map(|p| format!("  - {}", p.display()))
            .collect();
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
#[path = "server_tests.rs"]
mod tests;

#[tool_handler]
impl ServerHandler for DryRunServer {
    fn get_info(&self) -> ServerInfo {
        let version_header = if !self.pg_version_display.is_empty() {
            format!(
                "dryrun {} PostgreSQL schema advisor. PostgreSQL {}; database: {}\n\n",
                self.app_version, self.pg_version_display, self.database_name
            )
        } else {
            format!(
                "dryrun {} PostgreSQL schema advisor. No schema loaded yet.\n\n",
                self.app_version
            )
        };

        let online_note = if self.ctx.is_some() {
            "Live DB connected: explain_query, refresh_schema, check_drift available."
        } else {
            "Offline mode: explain_query, refresh_schema, check_drift not available (no --db)."
        };

        ServerInfo {
            instructions: Some(format!(
                "{version_header}\
                 {online_note}\n\n\
                 Start with list_tables or search_schema to explore. Use advise for query help. \
                 Use check_migration before applying DDL. Each tool response includes a _meta.hint \
                 field with contextual next-step guidance."
            )),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
