use super::parse::ParsedQuery;
use super::validate::{ValidationWarning, WarningSeverity};
use crate::schema::{SchemaSnapshot, effective_table_stats};

const LARGE_TABLE_THRESHOLD: f64 = 10_000.0;

pub fn detect_antipatterns(
    parsed: &ParsedQuery,
    schema: &SchemaSnapshot,
    warnings: &mut Vec<ValidationWarning>,
) {
    detect_select_star(parsed, warnings);
    detect_unbounded_query(parsed, schema, warnings);
    detect_cartesian_join(parsed, warnings);
    detect_dml_without_where(parsed, warnings);
    detect_partition_key_antipatterns(parsed, schema, warnings);
    detect_partition_key_update(parsed, schema, warnings);
}

fn detect_select_star(parsed: &ParsedQuery, warnings: &mut Vec<ValidationWarning>) {
    if parsed.info.has_select_star {
        warnings.push(ValidationWarning {
            severity: WarningSeverity::Warning,
            message: "SELECT * — consider listing columns explicitly to avoid extra I/O \
                      and breakage when columns change"
                .into(),
        });
    }
}

fn detect_unbounded_query(
    parsed: &ParsedQuery,
    schema: &SchemaSnapshot,
    warnings: &mut Vec<ValidationWarning>,
) {
    if parsed.info.statement_type != "SELECT" {
        return;
    }
    if parsed.info.has_where || parsed.info.has_limit {
        return;
    }

    for table_ref in &parsed.info.tables {
        let schema_name = table_ref.schema.as_deref().unwrap_or("public");
        if let Some(table) = schema
            .tables
            .iter()
            .find(|t| t.name == table_ref.name && t.schema == schema_name)
        {
            let reltuples = effective_table_stats(table, schema).map(|s| s.reltuples);

            if let Some(rows) = reltuples
                && rows > LARGE_TABLE_THRESHOLD {
                    warnings.push(ValidationWarning {
                        severity: WarningSeverity::Warning,
                        message: format!(
                            "unbounded query on {}.{} (~{} rows) with no WHERE or LIMIT — \
                             consider adding a filter or LIMIT clause",
                            table.schema, table.name, rows as i64
                        ),
                    });
                }
        }
    }
}

fn detect_cartesian_join(parsed: &ParsedQuery, warnings: &mut Vec<ValidationWarning>) {
    if parsed.info.statement_type != "SELECT" {
        return;
    }

    let select_tables: Vec<_> = parsed
        .info
        .tables
        .iter()
        .filter(|t| t.context == "select")
        .collect();

    if select_tables.len() > 1 && !parsed.info.has_join {
        let table_names: Vec<String> = select_tables.iter().map(|t| t.name.clone()).collect();
        warnings.push(ValidationWarning {
            severity: WarningSeverity::Warning,
            message: format!(
                "possible Cartesian join between {} — missing JOIN condition",
                table_names.join(", ")
            ),
        });
    }
}

fn detect_dml_without_where(parsed: &ParsedQuery, warnings: &mut Vec<ValidationWarning>) {
    let is_dml = parsed.info.statement_type == "UPDATE" || parsed.info.statement_type == "DELETE";
    if is_dml && !parsed.info.has_where {
        warnings.push(ValidationWarning {
            severity: WarningSeverity::Error,
            message: format!(
                "{} without WHERE clause — this will affect ALL rows",
                parsed.info.statement_type
            ),
        });
    }
}

fn detect_partition_key_antipatterns(
    parsed: &ParsedQuery,
    schema: &SchemaSnapshot,
    warnings: &mut Vec<ValidationWarning>,
) {
    for table_ref in &parsed.info.tables {
        let schema_name = table_ref.schema.as_deref().unwrap_or("public");

        let table = schema
            .tables
            .iter()
            .find(|t| t.name == table_ref.name && t.schema == schema_name);

        let table = match table {
            Some(t) => t,
            None => continue,
        };

        let pi = match &table.partition_info {
            Some(pi) => pi,
            None => continue,
        };

        let key_columns = parse_partition_key_columns(&pi.key);
        let found = key_columns.iter().any(|kc| {
            parsed
                .info
                .filter_columns
                .iter()
                .any(|(_, col)| col.eq_ignore_ascii_case(kc))
        });

        if !found {
            warnings.push(ValidationWarning {
                severity: WarningSeverity::Warning,
                message: format!(
                    "query on partitioned table '{}.{}' ({} on '{}', {} partitions) \
                     does not filter on partition key; all partitions will be scanned",
                    table.schema,
                    table.name,
                    pi.strategy,
                    pi.key,
                    pi.children.len()
                ),
            });
        }

        // check for function-wrapped partition key columns
        for kc in &key_columns {
            for fwc in &parsed.info.func_wrapped_columns {
                if fwc.column.eq_ignore_ascii_case(kc) {
                    warnings.push(ValidationWarning {
                        severity: WarningSeverity::Warning,
                        message: format!(
                            "partition key '{}' on '{}.{}' is wrapped in {} — this prevents \
                             partition pruning. {}",
                            kc,
                            table.schema,
                            table.name,
                            fwc.func_name,
                            func_wrap_rewrite_hint(&fwc.func_name, kc)
                        ),
                    });
                }
            }
        }
    }
}

fn detect_partition_key_update(
    parsed: &ParsedQuery,
    schema: &SchemaSnapshot,
    warnings: &mut Vec<ValidationWarning>,
) {
    if parsed.info.statement_type != "UPDATE" || parsed.info.update_targets.is_empty() {
        return;
    }

    for table_ref in &parsed.info.tables {
        let schema_name = table_ref.schema.as_deref().unwrap_or("public");

        let table = schema
            .tables
            .iter()
            .find(|t| t.name == table_ref.name && t.schema == schema_name);

        let table = match table {
            Some(t) => t,
            None => continue,
        };

        let pi = match &table.partition_info {
            Some(pi) => pi,
            None => continue,
        };

        let key_columns = parse_partition_key_columns(&pi.key);
        for kc in &key_columns {
            for ut in &parsed.info.update_targets {
                if ut.eq_ignore_ascii_case(kc) {
                    warnings.push(ValidationWarning {
                        severity: WarningSeverity::Warning,
                        message: format!(
                            "UPDATE changes partition key '{kc}' on partitioned table '{}.{}'. \
                             This causes cross-partition row movement (DELETE + INSERT)",
                            table.schema, table.name
                        ),
                    });
                }
            }
        }
    }
}

fn func_wrap_rewrite_hint(func_name: &str, col: &str) -> String {
    match func_name {
        "extract" | "::date" | "to_char" => format!(
            "Rewrite as: WHERE {col} >= '2025-01-01' AND {col} < '2026-01-01'"
        ),
        "date_trunc" => format!(
            "Rewrite as: WHERE {col} >= date_trunc('month', target) \
             AND {col} < date_trunc('month', target) + interval '1 month'"
        ),
        _ => format!("Rewrite using a direct range comparison on {col} instead."),
    }
}

fn parse_partition_key_columns(key: &str) -> Vec<String> {
    key.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        PartitionChild, PartitionInfo, PartitionStrategy, Table,
    };
    use crate::query::{QueryInfo, ReferencedTable};

    fn partitioned_snapshot() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "16.0".into(),
            database: "test".into(),
            timestamp: chrono::Utc::now(),
            content_hash: String::new(),
            source: None,
            tables: vec![Table {
                oid: 1,
                schema: "public".into(),
                name: "orders".into(),
                columns: vec![],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                stats: None,
                partition_info: Some(PartitionInfo {
                    strategy: PartitionStrategy::Range,
                    key: "created_at".into(),
                    children: vec![
                        PartitionChild {
                            schema: "public".into(),
                            name: "orders_2024_q1".into(),
                            bound: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')".into(),
                        },
                        PartitionChild {
                            schema: "public".into(),
                            name: "orders_2024_q2".into(),
                            bound: "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')".into(),
                        },
                    ],
                }),
                policies: vec![],
                triggers: vec![],
                reloptions: vec![],
                rls_enabled: false,
            }],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn partition_key_missing_warns() {
        let parsed = ParsedQuery {
            sql: "SELECT * FROM orders WHERE status = 'active'".into(),
            info: QueryInfo {
                tables: vec![ReferencedTable {
                    schema: Some("public".into()),
                    name: "orders".into(),
                    alias: None,
                    context: "select".into(),
                }],
                filter_columns: vec![(None, "status".into())],
                func_wrapped_columns: vec![],
                update_targets: vec![],
                has_select_star: true,
                has_limit: false,
                has_where: true,
                has_join: false,
                statement_type: "SELECT".into(),
            },
        };

        let snap = partitioned_snapshot();
        let mut warnings = Vec::new();
        detect_partition_key_antipatterns(&parsed, &snap, &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("does not filter on partition key"));
    }

    #[test]
    fn partition_key_func_wrapped_warns() {
        let parsed = ParsedQuery {
            sql: "SELECT * FROM orders WHERE EXTRACT(year FROM created_at) = 2024".into(),
            info: QueryInfo {
                tables: vec![ReferencedTable {
                    schema: Some("public".into()),
                    name: "orders".into(),
                    alias: None,
                    context: "select".into(),
                }],
                filter_columns: vec![(None, "created_at".into())],
                func_wrapped_columns: vec![crate::query::FuncWrappedColumn {
                    table: None,
                    column: "created_at".into(),
                    func_name: "extract".into(),
                }],
                update_targets: vec![],
                has_select_star: true,
                has_limit: false,
                has_where: true,
                has_join: false,
                statement_type: "SELECT".into(),
            },
        };

        let snap = partitioned_snapshot();
        let mut warnings = Vec::new();
        detect_partition_key_antipatterns(&parsed, &snap, &mut warnings);
        // should have a func-wrap warning (partition key is in filter_columns so no missing-key warning)
        assert!(warnings.iter().any(|w| w.message.contains("wrapped in extract")));
        assert!(warnings.iter().any(|w| w.message.contains("Rewrite as")));
    }

    #[test]
    fn partition_key_update_warns() {
        let parsed = ParsedQuery {
            sql: "UPDATE orders SET created_at = NOW() WHERE id = 1".into(),
            info: QueryInfo {
                tables: vec![ReferencedTable {
                    schema: Some("public".into()),
                    name: "orders".into(),
                    alias: None,
                    context: "dml".into(),
                }],
                filter_columns: vec![(None, "id".into())],
                func_wrapped_columns: vec![],
                update_targets: vec!["created_at".into()],
                has_select_star: false,
                has_limit: false,
                has_where: true,
                has_join: false,
                statement_type: "UPDATE".into(),
            },
        };

        let snap = partitioned_snapshot();
        let mut warnings = Vec::new();
        detect_partition_key_update(&parsed, &snap, &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("cross-partition row movement"));
    }

    #[test]
    fn partition_key_update_non_key_no_warn() {
        let parsed = ParsedQuery {
            sql: "UPDATE orders SET status = 'done' WHERE id = 1".into(),
            info: QueryInfo {
                tables: vec![ReferencedTable {
                    schema: Some("public".into()),
                    name: "orders".into(),
                    alias: None,
                    context: "dml".into(),
                }],
                filter_columns: vec![(None, "id".into())],
                func_wrapped_columns: vec![],
                update_targets: vec!["status".into()],
                has_select_star: false,
                has_limit: false,
                has_where: true,
                has_join: false,
                statement_type: "UPDATE".into(),
            },
        };

        let snap = partitioned_snapshot();
        let mut warnings = Vec::new();
        detect_partition_key_update(&parsed, &snap, &mut warnings);
        assert!(warnings.is_empty());
    }

    #[test]
    fn partition_key_present_no_warn() {
        let parsed = ParsedQuery {
            sql: "SELECT * FROM orders WHERE created_at >= '2024-01-01'".into(),
            info: QueryInfo {
                tables: vec![ReferencedTable {
                    schema: Some("public".into()),
                    name: "orders".into(),
                    alias: None,
                    context: "select".into(),
                }],
                filter_columns: vec![(None, "created_at".into())],
                func_wrapped_columns: vec![],
                update_targets: vec![],
                has_select_star: true,
                has_limit: false,
                has_where: true,
                has_join: false,
                statement_type: "SELECT".into(),
            },
        };

        let snap = partitioned_snapshot();
        let mut warnings = Vec::new();
        detect_partition_key_antipatterns(&parsed, &snap, &mut warnings);
        assert!(warnings.is_empty());
    }
}
