use regex::Regex;

use crate::jit;
use crate::schema::{SchemaSnapshot, Table};

use super::super::types::{LintViolation, Severity};

pub fn check_partition_too_many_children(
    table: &Table,
    qualified: &str,
    violations: &mut Vec<LintViolation>,
) {
    let pi = match &table.partition_info {
        Some(pi) => pi,
        None => return,
    };
    let n = pi.children.len();
    if n > 500 {
        let e = jit::partition_too_many_children(qualified, n);
        let rec = match &e.note {
            Some(note) => format!("{}\n{note}", e.reason),
            None => e.reason,
        };
        violations.push(LintViolation {
            rule: "partition/too_many_children".into(),
            severity: Severity::Warning,
            table: qualified.into(),
            column: None,
            message: format!(
                "table has {n} partitions; planning overhead may be significant"
            ),
            recommendation: rec,
            ddl_fix: None,
            convention_doc: "partitioning".into(),
        });
    }
}

pub fn check_partition_range_gaps(
    table: &Table,
    qualified: &str,
    violations: &mut Vec<LintViolation>,
) {
    let pi = match &table.partition_info {
        Some(pi) if pi.strategy == crate::schema::PartitionStrategy::Range => pi,
        _ => return,
    };

    let re = match Regex::new(r"FROM \('([^']+)'\) TO \('([^']+)'\)") {
        Ok(r) => r,
        Err(_) => return,
    };

    let mut bounds: Vec<(String, String)> = pi
        .children
        .iter()
        .filter_map(|c| {
            re.captures(&c.bound).map(|cap| {
                (cap[1].to_string(), cap[2].to_string())
            })
        })
        .collect();

    bounds.sort_by(|a, b| a.0.cmp(&b.0));

    for w in bounds.windows(2) {
        if w[0].1 != w[1].0 {
            let e = jit::partition_range_gap(&table.name, &w[0].1, &w[1].0);
            violations.push(LintViolation {
                rule: "partition/range_gaps".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: None,
                message: format!(
                    "gap in range partitions: '{}' ends at '{}' but next starts at '{}'",
                    qualified, w[0].1, w[1].0
                ),
                recommendation: e.reason,
                ddl_fix: Some(e.fix),
            convention_doc: "partitioning".into(),
            });
        }
    }
}

pub fn check_partition_no_default(
    table: &Table,
    qualified: &str,
    violations: &mut Vec<LintViolation>,
) {
    let pi = match &table.partition_info {
        Some(pi) => pi,
        None => return,
    };

    let has_default = pi.children.iter().any(|c| c.bound.contains("DEFAULT"));
    if !has_default {
        let e = jit::partition_no_default(&table.name);
        violations.push(LintViolation {
            rule: "partition/no_default".into(),
            severity: Severity::Info,
            table: qualified.into(),
            column: None,
            message: format!(
                "partitioned table '{qualified}' has no DEFAULT partition — \
                 rows not matching any partition will be rejected"
            ),
            recommendation: e.reason,
            ddl_fix: Some(e.fix),
            convention_doc: "partitioning".into(),
        });
    }
}

pub fn check_partition_gucs(schema: &SchemaSnapshot, violations: &mut Vec<LintViolation>) {
    let has_partitioned = schema
        .tables
        .iter()
        .any(|t| t.partition_info.is_some());

    if !has_partitioned {
        return;
    }

    let gucs_to_check = [
        ("enable_partition_pruning", "on"),
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
    ];

    for (name, expected) in &gucs_to_check {
        let current = schema.gucs.iter().find(|g| g.name == *name);
        let value = current.map(|g| g.setting.as_str()).unwrap_or("on");
        if value != *expected {
            violations.push(LintViolation {
                rule: "partition/gucs".into(),
                severity: Severity::Warning,
                table: String::new(),
                column: None,
                message: format!(
                    "{name} = '{value}' — should be '{expected}' when partitioned tables exist"
                ),
                recommendation: format!("ALTER SYSTEM SET {name} = '{expected}';"),
                ddl_fix: None,
            convention_doc: "partitioning".into(),
            });
        }
    }
}
