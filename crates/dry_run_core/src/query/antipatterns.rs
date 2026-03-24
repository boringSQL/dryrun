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

            if let Some(rows) = reltuples {
                if rows > LARGE_TABLE_THRESHOLD {
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
    }
}

fn parse_partition_key_columns(key: &str) -> Vec<String> {
    key.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}
