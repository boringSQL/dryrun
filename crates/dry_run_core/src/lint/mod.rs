mod rules;
mod types;

pub use types::{
    CompactViolation, LintConfig, LintReport, LintReportCompact, LintSummary, LintViolation,
    RuleGroup, Severity,
};

use std::collections::HashMap;

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

pub fn compact_report(report: &LintReport, max_examples: usize) -> LintReportCompact {
    let mut groups_map: HashMap<String, (Severity, String, Vec<CompactViolation>)> =
        HashMap::new();

    for v in &report.violations {
        let entry = groups_map
            .entry(v.rule.clone())
            .or_insert_with(|| (v.severity.clone(), v.recommendation.clone(), Vec::new()));
        entry.2.push(CompactViolation {
            table: v.table.clone(),
            column: v.column.clone(),
            message: v.message.clone(),
        });
    }

    let mut by_rule: Vec<RuleGroup> = groups_map
        .into_iter()
        .map(|(rule, (severity, recommendation, examples))| {
            let count = examples.len();
            let omitted = count.saturating_sub(max_examples);
            let capped = examples.into_iter().take(max_examples).collect();
            RuleGroup {
                rule,
                severity,
                count,
                recommendation,
                examples: capped,
                omitted,
            }
        })
        .collect();

    // Sort: errors first, then warnings, then info; within same severity highest count first
    by_rule.sort_by(|a, b| {
        fn severity_ord(s: &Severity) -> u8 {
            match s {
                Severity::Error => 0,
                Severity::Warning => 1,
                Severity::Info => 2,
            }
        }
        severity_ord(&a.severity)
            .cmp(&severity_ord(&b.severity))
            .then(b.count.cmp(&a.count))
    });

    let total_violations =
        report.summary.errors + report.summary.warnings + report.summary.info;

    LintReportCompact {
        tables_checked: report.tables_checked,
        total_violations,
        summary: report.summary.clone(),
        by_rule,
        config_source: report.config_source.clone(),
    }
}
