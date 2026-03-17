use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub pg_version: String,
    pub database: String,
    pub timestamp: DateTime<Utc>,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub tables: Vec<Table>,
    pub enums: Vec<EnumType>,
    pub domains: Vec<DomainType>,
    pub composites: Vec<CompositeType>,
    pub views: Vec<View>,
    pub functions: Vec<Function>,
    pub extensions: Vec<Extension>,
    pub gucs: Vec<GucSetting>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_stats: Vec<NodeStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    pub comment: Option<String>,
    pub stats: Option<TableStats>,
    pub partition_info: Option<PartitionInfo>,
    pub policies: Vec<RlsPolicy>,
    pub triggers: Vec<Trigger>,
    pub rls_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ordinal: i16,
    pub type_name: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub identity: Option<String>,
    pub comment: Option<String>,
    pub stats: Option<ColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub kind: ConstraintKind,
    pub columns: Vec<String>,
    pub definition: Option<String>,
    pub fk_table: Option<String>,
    pub fk_columns: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintKind {
    PrimaryKey,
    ForeignKey,
    Unique,
    Check,
    Exclusion,
}

impl ConstraintKind {
    pub fn from_pg_contype(c: &str) -> Option<Self> {
        match c {
            "p" => Some(Self::PrimaryKey),
            "f" => Some(Self::ForeignKey),
            "u" => Some(Self::Unique),
            "c" => Some(Self::Check),
            "x" => Some(Self::Exclusion),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub index_type: String,
    pub is_unique: bool,
    pub is_primary: bool,
    pub predicate: Option<String>,
    pub definition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<IndexStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub idx_scan: i64,
    pub idx_tup_read: i64,
    pub idx_tup_fetch: i64,
    pub size: i64,
    #[serde(default)]
    pub relpages: i64,
    #[serde(default)]
    pub reltuples: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub reltuples: f64,
    #[serde(default)]
    pub relpages: i64,
    pub dead_tuples: i64,
    pub last_vacuum: Option<DateTime<Utc>>,
    pub last_autovacuum: Option<DateTime<Utc>>,
    pub last_analyze: Option<DateTime<Utc>>,
    pub last_autoanalyze: Option<DateTime<Utc>>,
    pub seq_scan: i64,
    pub idx_scan: i64,
    pub table_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub null_frac: Option<f64>,
    pub n_distinct: Option<f64>,
    pub most_common_vals: Option<String>,
    pub most_common_freqs: Option<String>,
    pub histogram_bounds: Option<String>,
    pub correlation: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub strategy: PartitionStrategy,
    pub key: String,
    pub children: Vec<PartitionChild>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    Range,
    List,
    Hash,
}

impl PartitionStrategy {
    pub fn from_pg_partstrat(c: &str) -> Option<Self> {
        match c {
            "r" => Some(Self::Range),
            "l" => Some(Self::List),
            "h" => Some(Self::Hash),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionChild {
    pub schema: String,
    pub name: String,
    pub bound: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    pub name: String,
    pub command: String,
    pub permissive: bool,
    pub roles: Vec<String>,
    pub using_expr: Option<String>,
    pub with_check_expr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumType {
    pub schema: String,
    pub name: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DomainType {
    pub schema: String,
    pub name: String,
    pub base_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub check_constraints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeType {
    pub schema: String,
    pub name: String,
    pub fields: Vec<CompositeField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeField {
    pub name: String,
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub schema: String,
    pub name: String,
    pub definition: String,
    pub is_materialized: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
    pub schema: String,
    pub name: String,
    pub identity_args: String,
    pub return_type: String,
    pub language: String,
    pub volatility: Volatility,
    pub security_definer: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl Volatility {
    pub fn from_pg_provolatile(c: &str) -> Option<Self> {
        match c {
            "i" => Some(Self::Immutable),
            "s" => Some(Self::Stable),
            "v" => Some(Self::Volatile),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Extension {
    pub name: String,
    pub version: String,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GucSetting {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
}

pub fn aggregate_table_stats(
    node_stats: &[NodeStats],
    schema: &str,
    table: &str,
) -> Option<TableStats> {
    let matching: Vec<&TableStats> = node_stats
        .iter()
        .flat_map(|ns| &ns.table_stats)
        .filter(|nts| nts.schema == schema && nts.table == table)
        .map(|nts| &nts.stats)
        .collect();

    if matching.is_empty() {
        return None;
    }

    // max reltuples (all replicas should be close, take max for safety)
    let reltuples = matching.iter().map(|s| s.reltuples).fold(0.0_f64, f64::max);
    let relpages = matching.iter().map(|s| s.relpages).max().unwrap_or(0);
    let dead_tuples = matching.iter().map(|s| s.dead_tuples).max().unwrap_or(0);
    let seq_scan: i64 = matching.iter().map(|s| s.seq_scan).sum();
    let idx_scan: i64 = matching.iter().map(|s| s.idx_scan).sum();
    let table_size = matching.iter().map(|s| s.table_size).max().unwrap_or(0);

    Some(TableStats {
        reltuples,
        relpages,
        dead_tuples,
        last_vacuum: None,
        last_autovacuum: None,
        last_analyze: None,
        last_autoanalyze: None,
        seq_scan,
        idx_scan,
        table_size,
    })
}

// Per-table summary aggregated across all nodes.
#[derive(Debug, Clone)]
pub struct TableSummary {
    pub schema: String,
    pub table: String,
    pub total_seq_scan: i64,
    pub total_idx_scan: i64,
    /// (node source, seq_scan) for each node that has stats for this table.
    pub per_node_seq: Vec<(String, i64)>,
}

// Detected seq_scan imbalance across nodes.
#[derive(Debug, Clone)]
pub struct NodeImbalance {
    pub hot_node: String,
    pub multiplier: i64,
}

// Aggregate per-table stats across all nodes, preserving per-node seq_scan breakdown.
pub fn summarize_table_stats(node_stats: &[NodeStats]) -> Vec<TableSummary> {
    use std::collections::BTreeMap;

    let mut agg: BTreeMap<String, TableSummary> = BTreeMap::new();

    for ns in node_stats {
        for ts in &ns.table_stats {
            let key = format!("{}.{}", ts.schema, ts.table);
            let entry = agg.entry(key).or_insert_with(|| TableSummary {
                schema: ts.schema.clone(),
                table: ts.table.clone(),
                total_seq_scan: 0,
                total_idx_scan: 0,
                per_node_seq: Vec::new(),
            });
            entry.total_seq_scan += ts.stats.seq_scan;
            entry.total_idx_scan += ts.stats.idx_scan;
            entry.per_node_seq.push((ns.source.clone(), ts.stats.seq_scan));
        }
    }

    agg.into_values().collect()
}

// Detect seq_scan imbalance for a single table across nodes.
/// Returns `Some` if max/min seq_scan >= 5x among nodes with nonzero scans.
pub fn detect_seq_scan_imbalance(
    node_stats: &[NodeStats],
    schema: &str,
    table: &str,
) -> Option<NodeImbalance> {
    let seq_scans: Vec<(&str, i64)> = node_stats
        .iter()
        .filter_map(|ns| {
            ns.table_stats
                .iter()
                .find(|t| t.table == table && t.schema == schema)
                .map(|t| (ns.source.as_str(), t.stats.seq_scan))
        })
        .collect();

    if seq_scans.len() < 2 {
        return None;
    }

    let nonzero: Vec<(&str, i64)> = seq_scans.into_iter().filter(|(_, v)| *v > 0).collect();
    if nonzero.len() < 2 {
        return None;
    }

    let min = nonzero.iter().map(|(_, v)| *v).min().unwrap_or(1);
    let (hot_node, max) = nonzero.iter().max_by_key(|(_, v)| *v).copied().unwrap_or(("", 1));

    if min > 0 && max / min >= 5 {
        Some(NodeImbalance {
            hot_node: hot_node.to_string(),
            multiplier: max / min,
        })
    } else {
        None
    }
}

// use aggregated multi-node stats over table-level stats
pub fn effective_table_stats(table: &Table, schema: &SchemaSnapshot) -> Option<TableStats> {
    if !schema.node_stats.is_empty() {
        if let Some(agg) = aggregate_table_stats(&schema.node_stats, &table.schema, &table.name) {
            return Some(agg);
        }
    }
    table.stats.clone()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub source: String,
    pub timestamp: DateTime<Utc>,
    pub table_stats: Vec<NodeTableStats>,
    pub index_stats: Vec<NodeIndexStats>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_stats: Vec<NodeColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTableStats {
    pub schema: String,
    pub table: String,
    pub stats: TableStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIndexStats {
    pub schema: String,
    pub table: String,
    pub index_name: String,
    pub stats: IndexStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeColumnStats {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub stats: ColumnStats,
}
