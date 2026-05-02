use pg_query::NodeRef;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::jit;
use crate::schema::{AnnotatedSchema, QualifiedName, SchemaSnapshot};
use crate::version::PgVersion;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheck {
    pub operation: String,
    pub table: Option<String>,
    pub safety: SafetyRating,
    pub lock_type: String,
    pub lock_duration: String,
    pub table_size: Option<String>,
    pub row_estimate: Option<f64>,
    pub recommendation: String,
    pub version_behavior: Option<String>,
    pub rollback_ddl: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyRating {
    Safe,
    Caution,
    Dangerous,
}

// Inspect a DDL string and emit safety / lock-impact checks for each
// statement. Takes the annotated view because two of the inner analyses
// reach for stats: `lookup_table_stats` synthesizes the "(2 GB, ~50M
// rows)" flavor text from planner sizing, and the SET NOT NULL path
// reads column null_frac to predict whether the constraint scan will
// actually find offending rows.
pub fn check_migration(
    ddl: &str,
    annotated: &AnnotatedSchema<'_>,
    pg_version: Option<&PgVersion>,
) -> Result<Vec<MigrationCheck>> {
    let result =
        pg_query::parse(ddl).map_err(|e| Error::Introspection(format!("DDL parse error: {e}")))?;

    let mut checks = Vec::new();

    for (node, _depth, _context, _) in result.protobuf.nodes() {
        match node {
            NodeRef::AlterTableStmt(stmt) => {
                for cmd_node in &stmt.cmds {
                    if let Some(pg_query::protobuf::node::Node::AlterTableCmd(cmd)) = &cmd_node.node
                        && let Some(check) =
                            analyze_alter_table_cmd(cmd, &result, annotated, pg_version)
                    {
                        checks.push(check);
                    }
                }
            }
            NodeRef::IndexStmt(idx) => {
                checks.push(analyze_create_index(idx, annotated, pg_version));
            }
            NodeRef::RenameStmt(ren) => {
                checks.push(analyze_rename(ren, annotated.schema));
            }
            _ => {}
        }
    }

    if checks.is_empty()
        && let Some(check) = fallback_keyword_check(ddl, annotated.schema, pg_version)
    {
        checks.push(check);
    }

    Ok(checks)
}

fn analyze_alter_table_cmd(
    cmd: &pg_query::protobuf::AlterTableCmd,
    parse_result: &pg_query::ParseResult,
    annotated: &AnnotatedSchema<'_>,
    pg_version: Option<&PgVersion>,
) -> Option<MigrationCheck> {
    let subtype = pg_query::protobuf::AlterTableType::try_from(cmd.subtype).ok()?;
    let table_name = parse_result
        .tables
        .iter()
        .find(|(_, ctx)| *ctx == pg_query::Context::DDL)
        .map(|(name, _)| name.clone())
        .unwrap_or_default();

    let (table_size, row_estimate) = lookup_table_stats(annotated, &table_name);

    match subtype {
        pg_query::protobuf::AlterTableType::AtAddColumn => {
            let has_default = cmd.def.as_ref().is_some_and(|def| {
                if let Some(pg_query::protobuf::node::Node::ColumnDef(col)) = &def.node {
                    col.raw_default.is_some()
                        || col.constraints.iter().any(|c| {
                            matches!(
                                &c.node,
                                Some(pg_query::protobuf::node::Node::Constraint(con))
                                    if pg_query::protobuf::ConstrType::try_from(con.contype).ok()
                                        == Some(pg_query::protobuf::ConstrType::ConstrDefault)
                            )
                        })
                } else {
                    false
                }
            });

            let (safety, recommendation, lock_duration) = if !has_default {
                (
                    SafetyRating::Safe,
                    "Nullable column without DEFAULT — metadata-only change.".into(),
                    "brief (milliseconds)".into(),
                )
            } else if pg_version.is_some_and(|v| v.major >= 11) {
                let e = jit::add_column_volatile_default(&table_name, &cmd.name, "unknown", "<default>");
                (
                    SafetyRating::Caution,
                    format!(
                        "Column with DEFAULT on PG 11+ — safe for immutable defaults (metadata-only). \
                         Volatile defaults (now(), random()) still trigger a full table rewrite.\n\n\
                         If the default IS volatile:\n{}", e.fix
                    ),
                    "brief for immutable default, long for volatile".into(),
                )
            } else {
                let e = jit::add_column_pre_pg11(&table_name, &cmd.name, "unknown", "<default>");
                (
                    SafetyRating::Dangerous,
                    e.to_string(),
                    "proportional to table size".into(),
                )
            };

            Some(MigrationCheck {
                operation: "ADD COLUMN".into(),
                table: Some(table_name),
                safety,
                lock_type: "ACCESS EXCLUSIVE".into(),
                lock_duration,
                table_size,
                row_estimate,
                recommendation,
                version_behavior: version_behavior_add_column(pg_version),
                rollback_ddl: if cmd.name.is_empty() {
                    None
                } else {
                    Some(format!("ALTER TABLE ... DROP COLUMN {};", cmd.name))
                },
            })
        }

        pg_query::protobuf::AlterTableType::AtDropColumn => {
            Some(MigrationCheck {
                operation: "DROP COLUMN".into(),
                table: Some(table_name),
                safety: SafetyRating::Safe,
                lock_type: "ACCESS EXCLUSIVE".into(),
                lock_duration: "brief (metadata-only)".into(),
                table_size,
                row_estimate,
                recommendation: "Metadata-only operation. Column space reclaimed by VACUUM.".into(),
                version_behavior: None,
                rollback_ddl: None,
            })
        }

        pg_query::protobuf::AlterTableType::AtSetNotNull => {
            let pg_major = pg_version.map(|v| v.major).unwrap_or(0);
            let col_name = if cmd.name.is_empty() { "<col>" } else { &cmd.name };
            let e = jit::set_not_null(&table_name, col_name, pg_major);
            let safety = if pg_major >= 12 {
                SafetyRating::Caution
            } else {
                SafetyRating::Dangerous
            };

            let mut rec = e.to_string();

            // Check column stats for null_frac context — pulls the
            // ColumnStats out of the planner snapshot so we can warn
            // the user about how many rows would currently fail the new
            // NOT NULL constraint. Skipped when there's no planner
            // snapshot — better to omit the data check than to bluff
            // a "0% NULLs" estimate we can't actually verify.
            let col_stats = if !cmd.name.is_empty() {
                let (schema_part, name_part) = if let Some((s, n)) = table_name.rsplit_once('.') {
                    (s, n)
                } else {
                    ("public", table_name.as_str())
                };
                annotated.column_stats(&QualifiedName::new(schema_part, name_part), &cmd.name)
            } else {
                None
            };
            if let Some(nf) = col_stats.and_then(|s| s.null_frac) {
                if nf == 0.0 {
                    rec.push_str("\n\nDATA CHECK: Column currently has 0% NULLs. The scan will pass, but ACCESS EXCLUSIVE lock is still held.");
                } else if let Some(rows) = row_estimate {
                    let null_rows = (nf * rows) as i64;
                    rec.push_str(&format!(
                        "\n\nDATA CHECK: Column has ~{:.0}% NULLs (~{} rows) that must be backfilled before this constraint can be applied.",
                        nf * 100.0, null_rows
                    ));
                }
            }

            Some(MigrationCheck {
                operation: "SET NOT NULL".into(),
                table: Some(table_name),
                safety,
                lock_type: "ACCESS EXCLUSIVE".into(),
                lock_duration: "scan duration (unless CHECK exists on PG 12+)".into(),
                table_size,
                row_estimate,
                recommendation: rec,
                version_behavior: Some(
                    "PG 12+: skips scan if a valid CHECK (col IS NOT NULL) exists.".into(),
                ),
                rollback_ddl: Some("ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL;".into()),
            })
        }

        pg_query::protobuf::AlterTableType::AtAlterColumnType => {
            let col_name = &cmd.name;
            let e = jit::alter_column_type(&table_name, col_name, "<new_type>");
            Some(MigrationCheck {
                operation: "ALTER COLUMN TYPE".into(),
                table: Some(table_name),
                safety: SafetyRating::Dangerous,
                lock_type: "ACCESS EXCLUSIVE".into(),
                lock_duration: "proportional to table size (full rewrite)".into(),
                table_size,
                row_estimate,
                recommendation: e.to_string(),
                version_behavior: None,
                rollback_ddl: None,
            })
        }

        pg_query::protobuf::AlterTableType::AtAddConstraint => analyze_add_constraint(
            cmd,
            &table_name,
            table_size,
            row_estimate,
            annotated.schema,
            pg_version,
        ),

        pg_query::protobuf::AlterTableType::AtValidateConstraint => Some(MigrationCheck {
            operation: "VALIDATE CONSTRAINT".into(),
            table: Some(table_name),
            safety: SafetyRating::Safe,
            lock_type: "SHARE UPDATE EXCLUSIVE".into(),
            lock_duration: "proportional to table size (but allows concurrent DML)".into(),
            table_size,
            row_estimate,
            recommendation:
                "Safe — validates existing rows with a weaker lock that allows concurrent reads and writes."
                    .into(),
            version_behavior: None,
            rollback_ddl: None,
        }),

        _ => None,
    }
}

fn analyze_add_constraint(
    cmd: &pg_query::protobuf::AlterTableCmd,
    table_name: &str,
    table_size: Option<String>,
    row_estimate: Option<f64>,
    _schema: &SchemaSnapshot,
    _pg_version: Option<&PgVersion>,
) -> Option<MigrationCheck> {
    let is_not_valid = cmd.def.as_ref().is_some_and(|def| {
        if let Some(pg_query::protobuf::node::Node::Constraint(con)) = &def.node {
            con.skip_validation
        } else {
            false
        }
    });

    let con_type = cmd.def.as_ref().and_then(|def| {
        if let Some(pg_query::protobuf::node::Node::Constraint(con)) = &def.node {
            pg_query::protobuf::ConstrType::try_from(con.contype).ok()
        } else {
            None
        }
    });

    let operation = match con_type {
        Some(pg_query::protobuf::ConstrType::ConstrForeign) => "ADD FOREIGN KEY",
        Some(pg_query::protobuf::ConstrType::ConstrCheck) => "ADD CHECK CONSTRAINT",
        _ => "ADD CONSTRAINT",
    };

    let (safety, recommendation, lock_duration) = if is_not_valid {
        (
            SafetyRating::Safe,
            format!("{operation} NOT VALID — metadata-only. Follow up with VALIDATE CONSTRAINT."),
            "brief (metadata-only)".into(),
        )
    } else {
        let e = match operation {
            "ADD FOREIGN KEY" => {
                jit::add_foreign_key_unsafe(table_name, "<col>", "<ref_table>", "<ref_col>")
            }
            "ADD CHECK CONSTRAINT" => jit::add_check_constraint_unsafe(table_name, "<expr>"),
            _ => jit::add_check_constraint_unsafe(table_name, "<expr>"),
        };
        (
            SafetyRating::Dangerous,
            e.to_string(),
            "proportional to table size".into(),
        )
    };

    Some(MigrationCheck {
        operation: operation.into(),
        table: Some(table_name.into()),
        safety,
        lock_type: if is_not_valid {
            "ACCESS EXCLUSIVE (brief)".into()
        } else {
            "ACCESS EXCLUSIVE".into()
        },
        lock_duration,
        table_size,
        row_estimate,
        recommendation,
        version_behavior: None,
        rollback_ddl: Some(format!("ALTER TABLE {table_name} DROP CONSTRAINT <name>;")),
    })
}

fn analyze_create_index(
    idx: &pg_query::protobuf::IndexStmt,
    annotated: &AnnotatedSchema<'_>,
    _pg_version: Option<&PgVersion>,
) -> MigrationCheck {
    let table_name = idx
        .relation
        .as_ref()
        .map(|r| {
            if r.schemaname.is_empty() {
                r.relname.clone()
            } else {
                format!("{}.{}", r.schemaname, r.relname)
            }
        })
        .unwrap_or_default();

    let (table_size, row_estimate) = lookup_table_stats(annotated, &table_name);

    let (safety, recommendation, lock_type) = if idx.concurrent {
        (
            SafetyRating::Safe,
            "CREATE INDEX CONCURRENTLY — does not block reads or writes. Takes ~2-3x longer. \
             Cannot run inside a transaction. If it fails, drop the INVALID index."
                .into(),
            "SHARE UPDATE EXCLUSIVE".to_string(),
        )
    } else {
        let idx_method = if idx.access_method.is_empty() {
            "btree"
        } else {
            &idx.access_method
        };
        let e = jit::create_index_blocking(&table_name, &idx.idxname, idx_method, "<columns>");
        (
            SafetyRating::Dangerous,
            e.to_string(),
            "SHARE (blocks writes)".to_string(),
        )
    };

    let idx_name = if idx.idxname.is_empty() {
        "<auto>".into()
    } else {
        idx.idxname.clone()
    };

    MigrationCheck {
        operation: format!(
            "CREATE {}INDEX",
            if idx.concurrent { "CONCURRENTLY " } else { "" }
        ),
        table: Some(table_name),
        safety,
        lock_type,
        lock_duration: if idx.concurrent {
            "~2-3x normal build time (non-blocking)".into()
        } else {
            "proportional to table size (blocking)".into()
        },
        table_size,
        row_estimate,
        recommendation,
        version_behavior: None,
        rollback_ddl: Some(format!("DROP INDEX CONCURRENTLY {idx_name};")),
    }
}

fn analyze_rename(
    _ren: &pg_query::protobuf::RenameStmt,
    _schema: &SchemaSnapshot,
) -> MigrationCheck {
    let e = jit::rename("<old_name>", "<new_name>");
    MigrationCheck {
        operation: "RENAME".into(),
        table: None,
        safety: SafetyRating::Dangerous,
        lock_type: "ACCESS EXCLUSIVE".into(),
        lock_duration: "brief (metadata-only)".into(),
        table_size: None,
        row_estimate: None,
        recommendation: e.to_string(),
        version_behavior: None,
        rollback_ddl: Some("ALTER TABLE/COLUMN ... RENAME TO <old_name>;".into()),
    }
}

fn fallback_keyword_check(
    ddl: &str,
    _schema: &SchemaSnapshot,
    _pg_version: Option<&PgVersion>,
) -> Option<MigrationCheck> {
    let upper = ddl.to_uppercase();

    if upper.contains("DROP TABLE") {
        return Some(MigrationCheck {
            operation: "DROP TABLE".into(),
            table: None,
            safety: SafetyRating::Dangerous,
            lock_type: "ACCESS EXCLUSIVE".into(),
            lock_duration: "brief".into(),
            table_size: None,
            row_estimate: None,
            recommendation: "Irreversible. Ensure no dependent objects or application code references this table.".into(),
            version_behavior: None,
            rollback_ddl: None,
        });
    }

    None
}

// Pull (formatted_size, row_estimate) for a table out of the planner
// snapshot. Both fields end up in MigrationCheck so the LLM consumer can
// say things like "ALTER COLUMN TYPE on a 12 GB table will hold ACCESS
// EXCLUSIVE for ~minutes". Returns (None, None) when there's no planner
// snapshot — caller's flavor text just omits the size context in that
// case rather than guessing.
fn lookup_table_stats(
    annotated: &AnnotatedSchema<'_>,
    table_name: &str,
) -> (Option<String>, Option<f64>) {
    let (schema_part, name_part) = if let Some((s, n)) = table_name.rsplit_once('.') {
        (s, n)
    } else {
        ("public", table_name)
    };
    let qn = QualifiedName::new(schema_part, name_part);
    let size = annotated.table_size(&qn).map(format_bytes);
    let rows = annotated.reltuples(&qn);
    (size, rows)
}

fn format_bytes(bytes: i64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} bytes")
    }
}

fn version_behavior_add_column(pg_version: Option<&PgVersion>) -> Option<String> {
    let ver = pg_version?;
    if ver.major >= 11 {
        Some("PG 11+: Immutable DEFAULT is metadata-only (no table rewrite).".into())
    } else {
        Some("PG <11: Any DEFAULT triggers a full table rewrite.".into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;

    use super::*;
    use crate::schema::*;
    use crate::schema::{AnnotatedSnapshot, PlannerStatsSnapshot, TableSizing, TableSizingEntry};

    // Build a stats-bearing AnnotatedSnapshot for the migration tests.
    // Most check_migration outputs reference table size / row count in
    // their flavor text — we hand-roll a 2 GB / 5M-row planner row so
    // the tests can exercise that path without spelunking.
    fn empty_annotated() -> AnnotatedSnapshot {
        let schema = empty_schema();
        let planner = PlannerStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "ph".into(),
            schema_ref_hash: schema.content_hash.clone(),
            tables: vec![TableSizingEntry {
                table: QualifiedName::new("public", "orders"),
                sizing: TableSizing {
                    reltuples: 5_000_000.0,
                    relpages: 262144,
                    table_size: 2_147_483_648,
                    total_size: None,
                    index_size: None,
                },
            }],
            columns: vec![],
            indexes: vec![],
        };
        AnnotatedSnapshot {
            schema,
            planner: Some(planner),
            activity_by_node: BTreeMap::new(),
        }
    }

    fn empty_schema() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "test".into(),
            source: None,
            tables: vec![Table {
                oid: 1,
                schema: "public".into(),
                name: "orders".into(),
                columns: vec![],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                // Stats now live in the PlannerStatsSnapshot built by
                // `empty_annotated`; the legacy embedded field stays None.
                stats: None,
                partition_info: None,
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

    fn pg17() -> PgVersion {
        PgVersion {
            major: 17,
            minor: 0,
            patch: 0,
        }
    }

    #[test]
    fn add_column_no_default_safe() {
        let checks = check_migration(
            "ALTER TABLE orders ADD COLUMN notes text",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].operation, "ADD COLUMN");
        assert_eq!(checks[0].safety, SafetyRating::Safe);
    }

    #[test]
    fn add_column_with_default() {
        let checks = check_migration(
            "ALTER TABLE orders ADD COLUMN status text DEFAULT 'pending'",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].safety, SafetyRating::Caution);
        assert!(checks[0].recommendation.contains("immutable"));
    }

    #[test]
    fn create_index_without_concurrently() {
        let checks = check_migration(
            "CREATE INDEX idx_orders_status ON orders(status)",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].safety, SafetyRating::Dangerous);
        assert!(checks[0].recommendation.contains("CONCURRENTLY"));
    }

    #[test]
    fn create_index_concurrently_safe() {
        let checks = check_migration(
            "CREATE INDEX CONCURRENTLY idx_orders_status ON orders(status)",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].safety, SafetyRating::Safe);
    }

    #[test]
    fn set_not_null_caution_pg12() {
        let pg12 = PgVersion {
            major: 12,
            minor: 0,
            patch: 0,
        };
        let checks = check_migration(
            "ALTER TABLE orders ALTER COLUMN status SET NOT NULL",
            &empty_annotated().view(None),
            Some(&pg12),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].operation, "SET NOT NULL");
        assert_eq!(checks[0].safety, SafetyRating::Caution);
        assert!(checks[0].recommendation.contains("CHECK"));
    }

    #[test]
    fn alter_column_type_dangerous() {
        let checks = check_migration(
            "ALTER TABLE orders ALTER COLUMN id TYPE bigint",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].safety, SafetyRating::Dangerous);
    }

    #[test]
    fn drop_column_safe() {
        let checks = check_migration(
            "ALTER TABLE orders DROP COLUMN legacy",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].safety, SafetyRating::Safe);
    }

    #[test]
    fn includes_table_size() {
        let checks = check_migration(
            "ALTER TABLE orders ADD COLUMN x text",
            &empty_annotated().view(None),
            Some(&pg17()),
        )
        .unwrap();
        assert!(checks[0].table_size.as_ref().unwrap().contains("GB"));
        assert_eq!(checks[0].row_estimate, Some(5_000_000.0));
    }
}
