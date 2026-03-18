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
