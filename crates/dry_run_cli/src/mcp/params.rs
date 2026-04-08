use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTablesParams {
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
    #[serde(default)]
    #[schemars(description = "Sort by: 'name' (default), 'rows', or 'size'.")]
    pub sort: Option<String>,
    #[serde(default)]
    #[schemars(description = "Maximum number of results (default 50).")]
    pub limit: Option<usize>,
    #[serde(default)]
    #[schemars(description = "Skip N results (default 0).")]
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTableParams {
    pub table: String,
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
    #[serde(default)]
    #[schemars(description = "Detail level: 'summary' (default, compact with profiles), 'full' (all raw stats), 'stats' (only profiles and stats).")]
    pub detail: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchSchemaParams {
    #[schemars(description = "Case-insensitive substring to search for across all schema objects.")]
    pub query: String,
    #[serde(default)]
    #[schemars(description = "Maximum number of results (default 30).")]
    pub limit: Option<usize>,
    #[serde(default)]
    #[schemars(description = "Skip N results (default 0).")]
    pub offset: Option<usize>,
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
    #[schemars(description = "SQL query to validate against the schema.")]
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
    #[schemars(description = "DDL statement(s) to check for migration safety (e.g. ALTER TABLE, CREATE INDEX).")]
    pub ddl: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LintSchemaParams {
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
    #[serde(default)]
    #[schemars(description = "Table name to lint a single table. Omit to include all tables.")]
    pub table: Option<String>,
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
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
    #[serde(default)]
    #[schemars(description = "Table name to check a single table. Omit to include all tables.")]
    pub table: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct VacuumHealthParams {
    #[serde(default)]
    #[schemars(description = "PostgreSQL schema name to filter by. Omit to include all schemas.")]
    pub schema: Option<String>,
    #[serde(default)]
    #[schemars(description = "Table name to check a single table. Omit to include all tables.")]
    pub table: Option<String>,
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
