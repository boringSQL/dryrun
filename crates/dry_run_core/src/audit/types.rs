use serde::{Deserialize, Serialize};

use crate::lint::Severity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditCategory {
    Indexes,
    ForeignKeys,
    PrimaryKeys,
    Naming,
    Documentation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditFinding {
    pub rule: String,
    pub category: AuditCategory,
    pub severity: Severity,
    pub tables: Vec<String>,
    pub message: String,
    pub recommendation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_fix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    pub disabled_rules: Vec<String>,
    pub max_indexes_per_table: usize,
    pub no_comment_min_columns: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            disabled_rules: vec![],
            max_indexes_per_table: 10,
            no_comment_min_columns: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditSummary {
    pub errors: usize,
    pub warnings: usize,
    pub info: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    pub findings: Vec<AuditFinding>,
    pub tables_analyzed: usize,
    pub summary: AuditSummary,
}

impl AuditReport {
    #[must_use]
    pub fn new(findings: Vec<AuditFinding>, tables_analyzed: usize) -> Self {
        let summary = AuditSummary {
            errors: findings
                .iter()
                .filter(|f| f.severity == Severity::Error)
                .count(),
            warnings: findings
                .iter()
                .filter(|f| f.severity == Severity::Warning)
                .count(),
            info: findings
                .iter()
                .filter(|f| f.severity == Severity::Info)
                .count(),
        };
        Self {
            findings,
            tables_analyzed,
            summary,
        }
    }
}
