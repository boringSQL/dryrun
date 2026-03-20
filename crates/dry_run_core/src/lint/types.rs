use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintViolation {
    pub rule: String,
    pub severity: Severity,
    pub table: String,
    pub column: Option<String>,
    pub message: String,
    pub recommendation: String,
    pub convention_doc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintSummary {
    pub errors: usize,
    pub warnings: usize,
    pub info: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintReport {
    pub violations: Vec<LintViolation>,
    pub tables_checked: usize,
    pub summary: LintSummary,
    pub config_source: String,
}

impl LintReport {
    pub fn new(
        violations: Vec<LintViolation>,
        tables_checked: usize,
        config_source: String,
    ) -> Self {
        let summary = LintSummary {
            errors: violations
                .iter()
                .filter(|v| v.severity == Severity::Error)
                .count(),
            warnings: violations
                .iter()
                .filter(|v| v.severity == Severity::Warning)
                .count(),
            info: violations
                .iter()
                .filter(|v| v.severity == Severity::Info)
                .count(),
        };
        Self {
            violations,
            tables_checked,
            summary,
            config_source,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct LintReportCompact {
    pub tables_checked: usize,
    pub total_violations: usize,
    pub summary: LintSummary,
    pub by_rule: Vec<RuleGroup>,
    pub config_source: String,
}

#[derive(Debug, Serialize)]
pub struct RuleGroup {
    pub rule: String,
    pub severity: Severity,
    pub count: usize,
    pub message: String,
    pub recommendation: String,
    pub examples: Vec<CompactViolation>,
    pub omitted: usize,
}

#[derive(Debug, Serialize)]
pub struct CompactViolation {
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LintConfig {
    pub table_name_style: String,
    pub column_name_style: String,
    pub pk_type: String,
    pub fk_pattern: String,
    pub index_pattern: String,
    pub require_timestamps: bool,
    pub timestamp_type: String,
    pub prefer_text_over_varchar: bool,
    pub disabled_rules: Vec<String>,
    pub min_severity: Severity,
    pub table_name_regex: Option<String>,
    pub column_name_regex: Option<String>,
}

impl Default for LintConfig {
    fn default() -> Self {
        Self {
            table_name_style: "auto".into(),
            column_name_style: "snake_case".into(),
            pk_type: "bigint_identity".into(),
            fk_pattern: "fk_{table}_{column}".into(),
            index_pattern: "idx_{table}_{columns}".into(),
            require_timestamps: true,
            timestamp_type: "timestamptz".into(),
            prefer_text_over_varchar: true,
            disabled_rules: vec![],
            min_severity: Severity::Warning,
            table_name_regex: None,
            column_name_regex: None,
        }
    }
}
