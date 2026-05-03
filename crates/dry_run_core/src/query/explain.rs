use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::plan::{PlanNode, parse_plan_json};
use super::plan_warnings::detect_plan_warnings;
use crate::error::{Error, Result};
use crate::schema::AnnotatedSchema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    pub plan: PlanNode,
    pub total_cost: f64,
    pub estimated_rows: f64,
    pub warnings: Vec<PlanWarning>,
    pub execution: Option<ExecutionStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_plan: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgmustard_tips: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanWarning {
    pub severity: String,
    pub message: String,
    pub node_type: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub execution_time_ms: f64,
    pub planning_time_ms: f64,
}

pub async fn explain_query(
    pool: &PgPool,
    sql: &str,
    analyze: bool,
    annotated: Option<&AnnotatedSchema<'_>>,
) -> Result<ExplainResult> {
    let explain_sql = if analyze {
        format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
    } else {
        format!("EXPLAIN (FORMAT JSON) {sql}")
    };

    let json_str: String = if analyze {
        let mut tx = pool.begin().await?;

        let result: String = sqlx::query_scalar(&explain_sql)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Introspection(format!("EXPLAIN ANALYZE failed: {e}")))?;

        tx.rollback().await.ok();
        result
    } else {
        sqlx::query_scalar(&explain_sql)
            .fetch_one(pool)
            .await
            .map_err(|e| Error::Introspection(format!("EXPLAIN failed: {e}")))?
    };

    let plan_json: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| Error::Introspection(format!("failed to parse EXPLAIN JSON: {e}")))?;

    let plan_obj = plan_json
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| Error::Introspection("empty EXPLAIN result".into()))?;

    let plan_node_json = plan_obj
        .get("Plan")
        .ok_or_else(|| Error::Introspection("no Plan in EXPLAIN output".into()))?;

    let plan = parse_plan_json(plan_node_json)?;

    let total_cost = plan.total_cost;
    let estimated_rows = plan.plan_rows;

    let execution = if analyze {
        let exec_time = plan_obj
            .get("Execution Time")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let plan_time = plan_obj
            .get("Planning Time")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        Some(ExecutionStats {
            execution_time_ms: exec_time,
            planning_time_ms: plan_time,
        })
    } else {
        None
    };

    let warnings = detect_plan_warnings(&plan, annotated);

    Ok(ExplainResult {
        plan,
        total_cost,
        estimated_rows,
        warnings,
        execution,
        raw_plan: Some(plan_json),
        pgmustard_tips: None,
    })
}
