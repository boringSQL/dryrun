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
    let mut groups_map: HashMap<String, (Severity, String, String, Vec<CompactViolation>)> =
        HashMap::new();

    for v in &report.violations {
        let entry = groups_map.entry(v.rule.clone()).or_insert_with(|| {
            (
                v.severity.clone(),
                v.message.clone(),
                v.recommendation.clone(),
                Vec::new(),
            )
        });
        entry.3.push(CompactViolation {
            table: v.table.clone(),
            column: v.column.clone(),
        });
    }

    let mut by_rule: Vec<RuleGroup> = groups_map
        .into_iter()
        .map(|(rule, (severity, message, recommendation, examples))| {
            let count = examples.len();
            let omitted = count.saturating_sub(max_examples);
            let capped = examples.into_iter().take(max_examples).collect();
            RuleGroup {
                rule,
                severity,
                count,
                message,
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

    let total_violations = report.summary.errors + report.summary.warnings + report.summary.info;

    LintReportCompact {
        tables_checked: report.tables_checked,
        total_violations,
        summary: report.summary.clone(),
        by_rule,
        config_source: report.config_source.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_violation(rule: &str, severity: Severity, table: &str) -> LintViolation {
        LintViolation {
            rule: rule.into(),
            severity,
            table: table.into(),
            column: None,
            message: format!("{table} violates {rule}"),
            recommendation: format!("fix {rule}"),
            ddl_fix: None,
            convention_doc: String::new(),
        }
    }

    fn make_report(violations: Vec<LintViolation>) -> LintReport {
        LintReport::new(violations, 5, "test".into())
    }

    #[test]
    fn test_compact_report_empty() {
        let report = make_report(vec![]);
        let compact = compact_report(&report, 3);

        assert_eq!(compact.total_violations, 0);
        assert_eq!(compact.tables_checked, 5);
        assert!(compact.by_rule.is_empty());
    }

    #[test]
    fn test_compact_report_groups_by_rule() {
        let report = make_report(vec![
            make_violation("naming", Severity::Warning, "users"),
            make_violation("naming", Severity::Warning, "orders"),
            make_violation("pk_type", Severity::Error, "users"),
        ]);
        let compact = compact_report(&report, 10);

        assert_eq!(compact.by_rule.len(), 2);
        // error rule should come first
        assert_eq!(compact.by_rule[0].rule, "pk_type");
        assert_eq!(compact.by_rule[0].count, 1);
        assert_eq!(compact.by_rule[1].rule, "naming");
        assert_eq!(compact.by_rule[1].count, 2);
    }

    #[test]
    fn test_compact_report_caps_examples() {
        let report = make_report(vec![
            make_violation("naming", Severity::Warning, "t1"),
            make_violation("naming", Severity::Warning, "t2"),
            make_violation("naming", Severity::Warning, "t3"),
            make_violation("naming", Severity::Warning, "t4"),
            make_violation("naming", Severity::Warning, "t5"),
        ]);
        let compact = compact_report(&report, 2);

        let group = &compact.by_rule[0];
        assert_eq!(group.count, 5);
        assert_eq!(group.examples.len(), 2);
        assert_eq!(group.omitted, 3);
    }

    #[test]
    fn test_compact_report_severity_ordering() {
        let report = make_report(vec![
            make_violation("info_rule", Severity::Info, "t1"),
            make_violation("warn_rule", Severity::Warning, "t1"),
            make_violation("err_rule", Severity::Error, "t1"),
        ]);
        let compact = compact_report(&report, 10);

        assert_eq!(compact.by_rule[0].severity, Severity::Error);
        assert_eq!(compact.by_rule[1].severity, Severity::Warning);
        assert_eq!(compact.by_rule[2].severity, Severity::Info);
    }

    #[test]
    fn test_compact_report_count_ordering_within_severity() {
        let report = make_report(vec![
            make_violation("few", Severity::Warning, "t1"),
            make_violation("many", Severity::Warning, "t1"),
            make_violation("many", Severity::Warning, "t2"),
            make_violation("many", Severity::Warning, "t3"),
        ]);
        let compact = compact_report(&report, 10);

        assert_eq!(compact.by_rule[0].rule, "many");
        assert_eq!(compact.by_rule[0].count, 3);
        assert_eq!(compact.by_rule[1].rule, "few");
        assert_eq!(compact.by_rule[1].count, 1);
    }

    #[test]
    fn test_compact_report_preserves_summary() {
        let report = make_report(vec![
            make_violation("r1", Severity::Error, "t1"),
            make_violation("r2", Severity::Warning, "t1"),
            make_violation("r3", Severity::Warning, "t2"),
            make_violation("r4", Severity::Info, "t1"),
        ]);
        let compact = compact_report(&report, 10);

        assert_eq!(compact.summary.errors, 1);
        assert_eq!(compact.summary.warnings, 2);
        assert_eq!(compact.summary.info, 1);
        assert_eq!(compact.total_violations, 4);
    }

    #[test]
    fn test_compact_report_no_omitted_when_under_cap() {
        let report = make_report(vec![
            make_violation("r1", Severity::Error, "t1"),
            make_violation("r1", Severity::Error, "t2"),
        ]);
        let compact = compact_report(&report, 5);

        assert_eq!(compact.by_rule[0].examples.len(), 2);
        assert_eq!(compact.by_rule[0].omitted, 0);
    }
}
