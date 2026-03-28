use std::collections::{HashMap, HashSet};

use crate::audit::types::{AuditCategory, AuditFinding};
use crate::lint::Severity;
use crate::schema::{ConstraintKind, SchemaSnapshot};

#[derive(Debug)]
pub struct FkGraph {
    edges: HashMap<String, HashSet<String>>,
    nodes: HashSet<String>,
}

impl FkGraph {
    #[must_use]
    pub fn build(schema: &SchemaSnapshot) -> Self {
        let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
        let mut nodes = HashSet::new();

        for table in &schema.tables {
            let source = format!("{}.{}", table.schema, table.name);
            nodes.insert(source.clone());

            for constraint in &table.constraints {
                if constraint.kind == ConstraintKind::ForeignKey {
                    if let Some(ref target) = constraint.fk_table {
                        nodes.insert(target.clone());
                        edges.entry(source.clone()).or_default().insert(target.clone());
                    }
                }
            }
        }

        Self { edges, nodes }
    }

    fn in_degree(&self, node: &str) -> usize {
        self.edges
            .values()
            .filter(|targets| targets.contains(node))
            .count()
    }

    fn out_degree(&self, node: &str) -> usize {
        self.edges.get(node).map_or(0, |t| t.len())
    }
}

// Detect cycles using DFS with coloring (white/gray/black)
#[must_use]
pub fn check_circular_fks(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let graph = FkGraph::build(schema);
    let mut findings = Vec::new();
    let mut color: HashMap<&str, u8> = HashMap::new(); // 0=white, 1=gray, 2=black
    let mut path: Vec<&str> = Vec::new();
    let mut cycles: Vec<Vec<String>> = Vec::new();

    for node in &graph.nodes {
        if color.get(node.as_str()).copied().unwrap_or(0) == 0 {
            dfs_find_cycles(
                node.as_str(),
                &graph.edges,
                &mut color,
                &mut path,
                &mut cycles,
            );
        }
    }

    for cycle in cycles {
        findings.push(AuditFinding {
            rule: "fk/circular".into(),
            category: AuditCategory::ForeignKeys,
            severity: Severity::Warning,
            tables: cycle.clone(),
            message: format!("Circular FK dependency: {}", cycle.join(" → ")),
            recommendation: "Circular FKs complicate migrations and cascade deletes — consider breaking the cycle".into(),
            ddl_fix: None,
            min_pg_version: None,
        });
    }

    findings
}

fn dfs_find_cycles<'a>(
    node: &'a str,
    edges: &'a HashMap<String, HashSet<String>>,
    color: &mut HashMap<&'a str, u8>,
    path: &mut Vec<&'a str>,
    cycles: &mut Vec<Vec<String>>,
) {
    color.insert(node, 1); // gray
    path.push(node);

    if let Some(neighbors) = edges.get(node) {
        for neighbor in neighbors {
            match color.get(neighbor.as_str()).copied().unwrap_or(0) {
                0 => {
                    dfs_find_cycles(neighbor.as_str(), edges, color, path, cycles);
                }
                1 => {
                    // back edge found — extract cycle from path
                    if let Some(start) = path.iter().position(|&n| n == neighbor.as_str()) {
                        let mut cycle: Vec<String> =
                            path[start..].iter().map(|s| s.to_string()).collect();
                        cycle.push(neighbor.clone());
                        cycles.push(cycle);
                    }
                }
                _ => {} // black — already processed
            }
        }
    }

    path.pop();
    color.insert(node, 2); // black
}

#[must_use]
pub fn check_orphan_tables(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let graph = FkGraph::build(schema);
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);
        if graph.in_degree(&qualified) == 0 && graph.out_degree(&qualified) == 0 {
            findings.push(AuditFinding {
                rule: "fk/orphan".into(),
                category: AuditCategory::ForeignKeys,
                severity: Severity::Info,
                tables: vec![qualified],
                message: "Table has no FK relationships (no incoming, no outgoing) — data island"
                    .into(),
                recommendation:
                    "Verify this table is intentionally standalone or add FK relationships".into(),
                ddl_fix: None,
                min_pg_version: None,
            });
        }
    }

    findings
}

// Check that FK column type matches the referenced PK column type
#[must_use]
pub fn check_fk_type_mismatch(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    // build lookup: "schema.table" -> table ref
    let table_map: HashMap<String, &crate::schema::Table> = schema
        .tables
        .iter()
        .map(|t| (format!("{}.{}", t.schema, t.name), t))
        .collect();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);
        let col_type_map: HashMap<&str, &str> = table
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str()))
            .collect();

        for constraint in &table.constraints {
            if constraint.kind != ConstraintKind::ForeignKey {
                continue;
            }
            let Some(ref fk_table) = constraint.fk_table else {
                continue;
            };
            let Some(ref_table) = table_map.get(fk_table.as_str()) else {
                continue;
            };

            let ref_col_types: HashMap<&str, &str> = ref_table
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_name.as_str()))
                .collect();

            for (fk_col, ref_col) in constraint.columns.iter().zip(constraint.fk_columns.iter()) {
                let Some(fk_type) = col_type_map.get(fk_col.as_str()) else {
                    continue;
                };
                let Some(ref_type) = ref_col_types.get(ref_col.as_str()) else {
                    continue;
                };

                if !types_compatible(fk_type, ref_type) {
                    findings.push(AuditFinding {
                        rule: "fk/type_mismatch".into(),
                        category: AuditCategory::ForeignKeys,
                        severity: Severity::Error,
                        tables: vec![qualified.clone(), fk_table.clone()],
                        message: format!(
                            "FK column {}.{} ({}) references {}.{} ({}) — type mismatch kills index usage",
                            table.name, fk_col, fk_type,
                            ref_table.name, ref_col, ref_type,
                        ),
                        recommendation: format!(
                            "Alter {}.{} to match type '{}'",
                            table.name, fk_col, ref_type,
                        ),
                        ddl_fix: Some(format!(
                            "ALTER TABLE {qualified} ALTER COLUMN {fk_col} TYPE {ref_type};",
                        )),
                        min_pg_version: None,
                    });
                }
            }
        }
    }

    findings
}

// Normalize and compare types — treat int4/integer and int8/bigint as equivalent
fn types_compatible(a: &str, b: &str) -> bool {
    normalize_type(a) == normalize_type(b)
}

fn normalize_type(t: &str) -> &str {
    match t {
        "int4" | "integer" | "int" => "integer",
        "int8" | "bigint" => "bigint",
        "int2" | "smallint" => "smallint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "timestamp" | "timestamp without time zone" => "timestamp",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use chrono::Utc;

    fn make_col(name: &str, type_name: &str) -> Column {
        Column {
            name: name.into(), ordinal: 0, type_name: type_name.into(),
            nullable: false, default: None, identity: None, comment: None, stats: None,
        }
    }

    fn make_pk(name: &str, columns: &[&str]) -> Constraint {
        Constraint {
            name: name.into(), kind: ConstraintKind::PrimaryKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None, fk_table: None, fk_columns: vec![], comment: None,
        }
    }

    fn make_fk(name: &str, columns: &[&str], fk_table: &str, fk_columns: &[&str]) -> Constraint {
        Constraint {
            name: name.into(), kind: ConstraintKind::ForeignKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None, fk_table: Some(fk_table.into()),
            fk_columns: fk_columns.iter().map(|s| s.to_string()).collect(),
            comment: None,
        }
    }

    fn make_table(name: &str, columns: Vec<Column>, constraints: Vec<Constraint>) -> Table {
        Table {
            oid: 0, schema: "public".into(), name: name.into(),
            columns, constraints, indexes: vec![],
            comment: None, stats: None, partition_info: None,
            policies: vec![], triggers: vec![], reloptions: vec![], rls_enabled: false,
        }
    }

    fn schema_with(tables: Vec<Table>) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(), database: "test".into(),
            timestamp: Utc::now(), content_hash: "abc".into(), source: None,
            tables, enums: vec![], domains: vec![], composites: vec![],
            views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn detects_circular_fk() {
        let schema = schema_with(vec![
            make_table(
                "a",
                vec![make_col("id", "bigint"), make_col("b_id", "bigint")],
                vec![make_fk("fk_a_b", &["b_id"], "public.b", &["id"])],
            ),
            make_table(
                "b",
                vec![make_col("id", "bigint"), make_col("c_id", "bigint")],
                vec![make_fk("fk_b_c", &["c_id"], "public.c", &["id"])],
            ),
            make_table(
                "c",
                vec![make_col("id", "bigint"), make_col("a_id", "bigint")],
                vec![make_fk("fk_c_a", &["a_id"], "public.a", &["id"])],
            ),
        ]);
        let findings = check_circular_fks(&schema);
        assert!(!findings.is_empty(), "should detect cycle A→B→C→A");
        assert_eq!(findings[0].rule, "fk/circular");
    }

    #[test]
    fn no_cycle_in_linear_chain() {
        let schema = schema_with(vec![
            make_table(
                "a",
                vec![make_col("id", "bigint")],
                vec![],
            ),
            make_table(
                "b",
                vec![make_col("id", "bigint"), make_col("a_id", "bigint")],
                vec![make_fk("fk_b_a", &["a_id"], "public.a", &["id"])],
            ),
            make_table(
                "c",
                vec![make_col("id", "bigint"), make_col("b_id", "bigint")],
                vec![make_fk("fk_c_b", &["b_id"], "public.b", &["id"])],
            ),
        ]);
        let findings = check_circular_fks(&schema);
        assert!(findings.is_empty(), "linear chain has no cycles");
    }

    #[test]
    fn detects_orphan_table() {
        let schema = schema_with(vec![
            make_table(
                "users",
                vec![make_col("id", "bigint")],
                vec![],
            ),
            make_table(
                "orders",
                vec![make_col("id", "bigint"), make_col("user_id", "bigint")],
                vec![make_fk("fk_orders_users", &["user_id"], "public.users", &["id"])],
            ),
            make_table(
                "config",
                vec![make_col("id", "bigint"), make_col("key", "text")],
                vec![],
            ),
        ]);
        let findings = check_orphan_tables(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].tables[0], "public.config");
    }

    #[test]
    fn detects_fk_type_mismatch() {
        let schema = schema_with(vec![
            make_table(
                "users",
                vec![make_col("user_id", "bigint")],
                vec![make_pk("pk_users", &["user_id"])],
            ),
            make_table(
                "orders",
                vec![make_col("id", "bigint"), make_col("user_id", "integer")],
                vec![make_fk("fk_orders_user", &["user_id"], "public.users", &["user_id"])],
            ),
        ]);
        let findings = check_fk_type_mismatch(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "fk/type_mismatch");
    }

    #[test]
    fn no_mismatch_when_int4_matches_integer() {
        let schema = schema_with(vec![
            make_table(
                "users",
                vec![make_col("user_id", "int4")],
                vec![make_pk("pk_users", &["user_id"])],
            ),
            make_table(
                "orders",
                vec![make_col("id", "bigint"), make_col("user_id", "integer")],
                vec![make_fk("fk_orders_user", &["user_id"], "public.users", &["user_id"])],
            ),
        ]);
        let findings = check_fk_type_mismatch(&schema);
        assert!(findings.is_empty(), "int4 and integer should be treated as equivalent");
    }
}
