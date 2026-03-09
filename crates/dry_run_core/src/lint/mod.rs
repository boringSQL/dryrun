mod rules;
mod types;

pub use types::{LintConfig, LintReport, LintSummary, LintViolation, Severity};

use crate::schema::SchemaSnapshot;

pub fn lint_schema(schema: &SchemaSnapshot, config: &LintConfig) -> LintReport {
    let tables_checked = schema.tables.len();
    let violations = rules::run_all_rules(schema, config);
    let config_source = if config.disabled_rules.is_empty() {
        "default (boringsql)".into()
    } else {
        format!("custom ({} rules disabled)", config.disabled_rules.len())
    };
    LintReport::new(violations, tables_checked, config_source)
}
