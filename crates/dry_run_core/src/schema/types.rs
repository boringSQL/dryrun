use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

fn null_as_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(deserializer).map(|v| v.unwrap_or_default())
}

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
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub columns: Vec<Column>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub constraints: Vec<Constraint>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub indexes: Vec<Index>,
    pub comment: Option<String>,
    pub stats: Option<TableStats>,
    pub partition_info: Option<PartitionInfo>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub policies: Vec<RlsPolicy>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub triggers: Vec<Trigger>,
    #[serde(
        default,
        deserialize_with = "null_as_empty_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub reloptions: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated: Option<String>,
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics_target: Option<i16>,
    pub stats: Option<ColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub kind: ConstraintKind,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub columns: Vec<String>,
    pub definition: Option<String>,
    pub fk_table: Option<String>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub fk_columns: Vec<String>,
    pub backing_index: Option<String>,
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
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub columns: Vec<String>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub include_columns: Vec<String>,
    pub index_type: String,
    pub is_unique: bool,
    pub is_primary: bool,
    pub predicate: Option<String>,
    pub definition: String,
    #[serde(default = "default_true")]
    pub is_valid: bool,
    #[serde(default)]
    pub backs_constraint: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<IndexStats>,
}

fn default_true() -> bool {
    true
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

impl std::fmt::Display for PartitionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Range => write!(f, "range"),
            Self::List => write!(f, "list"),
            Self::Hash => write!(f, "hash"),
        }
    }
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

    // Vacuum/analyze timestamps only make sense from primary nodes
    // (autovacuum doesn't run on standbys, so timestamps are always null there).
    let primary_stats: Vec<&TableStats> = node_stats
        .iter()
        .filter(|ns| !ns.is_standby)
        .flat_map(|ns| &ns.table_stats)
        .filter(|nts| nts.schema == schema && nts.table == table)
        .map(|nts| &nts.stats)
        .collect();

    let last_vacuum = primary_stats.iter().filter_map(|s| s.last_vacuum).max();
    let last_autovacuum = primary_stats.iter().filter_map(|s| s.last_autovacuum).max();
    let last_analyze = primary_stats.iter().filter_map(|s| s.last_analyze).max();
    let last_autoanalyze = primary_stats
        .iter()
        .filter_map(|s| s.last_autoanalyze)
        .max();

    Some(TableStats {
        reltuples,
        relpages,
        dead_tuples,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
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

// Anomaly flag for a table's stats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableFlag {
    // seq_scan / idx_scan ratio is suspiciously high.
    HighSeqIdxRatio,
    // Table has seq_scans but zero idx_scans.
    SeqScanOnly,
    // One node handles disproportionately more seq_scans.
    NodeImbalance,
}

// Detected seq_scan imbalance across nodes.
#[derive(Debug, Clone)]
pub struct NodeImbalanceInfo {
    pub hot_node: String,
    pub multiplier: i64,
}

// A single table with stale or missing analyze stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleStatsEntry {
    pub node: String,
    pub schema: String,
    pub table: String,
    pub last_analyzed_days_ago: Option<i64>,
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
            entry
                .per_node_seq
                .push((ns.source.clone(), ts.stats.seq_scan));
        }
    }

    agg.into_values().collect()
}

// Compute anomaly flags for a single table summary.
pub fn detect_table_flags(summary: &TableSummary, node_stats: &[NodeStats]) -> Vec<TableFlag> {
    let mut flags = Vec::new();

    if summary.total_seq_scan > 100 && summary.total_idx_scan > 0 {
        let ratio = summary.total_seq_scan as f64 / summary.total_idx_scan as f64;
        if ratio > 0.5 {
            flags.push(TableFlag::HighSeqIdxRatio);
        }
    } else if summary.total_seq_scan > 100 && summary.total_idx_scan == 0 {
        flags.push(TableFlag::SeqScanOnly);
    }

    if detect_seq_scan_imbalance(node_stats, &summary.schema, &summary.table).is_some() {
        flags.push(TableFlag::NodeImbalance);
    }

    flags
}

// Detect tables with stale or missing analyze stats across nodes.
pub fn detect_stale_stats(node_stats: &[NodeStats], stale_days: i64) -> Vec<StaleStatsEntry> {
    let now = chrono::Utc::now();
    let threshold = chrono::TimeDelta::days(stale_days);
    let mut entries = Vec::new();

    for ns in node_stats {
        for ts in &ns.table_stats {
            let last_analyzed = ts.stats.last_analyze.max(ts.stats.last_autoanalyze);

            match last_analyzed {
                Some(when) if now - when > threshold => {
                    entries.push(StaleStatsEntry {
                        node: ns.source.clone(),
                        schema: ts.schema.clone(),
                        table: ts.table.clone(),
                        last_analyzed_days_ago: Some((now - when).num_days()),
                    });
                }
                None => {
                    entries.push(StaleStatsEntry {
                        node: ns.source.clone(),
                        schema: ts.schema.clone(),
                        table: ts.table.clone(),
                        last_analyzed_days_ago: None,
                    });
                }
                _ => {}
            }
        }
    }

    entries
}

/// Detect seq_scan imbalance for a single table across nodes.
/// Returns `Some` if max/min seq_scan >= 5x among nodes with nonzero scans.
pub fn detect_seq_scan_imbalance(
    node_stats: &[NodeStats],
    schema: &str,
    table: &str,
) -> Option<NodeImbalanceInfo> {
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
    let (hot_node, max) = nonzero
        .iter()
        .max_by_key(|(_, v)| *v)
        .copied()
        .unwrap_or(("", 1));

    if min > 0 && max / min >= 5 {
        Some(NodeImbalanceInfo {
            hot_node: hot_node.to_string(),
            multiplier: max / min,
        })
    } else {
        None
    }
}

// A single unused index (idx_scan = 0 across all nodes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnusedIndexEntry {
    pub schema: String,
    pub table: String,
    pub index_name: String,
    pub total_idx_scan: i64,
    pub total_size_bytes: i64,
    pub is_unique: bool,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloatedIndexEntry {
    pub schema: String,
    pub table: String,
    pub index_name: String,
    pub bloat_ratio: f64,
    pub actual_pages: i64,
    pub expected_pages: i64,
    pub definition: String,
}

pub fn detect_bloated_indexes(tables: &[Table], threshold: f64) -> Vec<BloatedIndexEntry> {
    let mut entries = Vec::new();

    for table in tables {
        for idx in &table.indexes {
            if let Some(est) = super::bloat::estimate_index_bloat(idx, table)
                && est.bloat_ratio > threshold
            {
                entries.push(BloatedIndexEntry {
                    schema: table.schema.clone(),
                    table: table.name.clone(),
                    index_name: idx.name.clone(),
                    bloat_ratio: est.bloat_ratio,
                    actual_pages: est.actual_pages,
                    expected_pages: est.expected_pages,
                    definition: idx.definition.clone(),
                });
            }
        }
    }

    entries.sort_by(|a, b| {
        b.bloat_ratio
            .partial_cmp(&a.bloat_ratio)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    entries
}

/// Detect indexes with zero scans across all nodes.
/// Skips primary key indexes — those are never droppable.
/// When `node_stats` is empty, falls back to `Table.indexes[].stats`.
pub fn detect_unused_indexes(node_stats: &[NodeStats], tables: &[Table]) -> Vec<UnusedIndexEntry> {
    use std::collections::BTreeMap;

    let mut entries = Vec::new();

    if node_stats.is_empty() {
        // single-node fallback: use table-level index stats
        for t in tables {
            for idx in &t.indexes {
                if idx.is_primary {
                    continue;
                }
                if let Some(ref stats) = idx.stats
                    && stats.idx_scan == 0
                {
                    entries.push(UnusedIndexEntry {
                        schema: t.schema.clone(),
                        table: t.name.clone(),
                        index_name: idx.name.clone(),
                        total_idx_scan: 0,
                        total_size_bytes: stats.size,
                        is_unique: idx.is_unique,
                        definition: idx.definition.clone(),
                    });
                }
            }
        }
    } else {
        // multi-node: aggregate idx_scan and size by (schema, table, index_name)
        #[derive(Default)]
        struct Agg {
            total_idx_scan: i64,
            max_size: i64,
        }

        let mut agg: BTreeMap<(String, String, String), Agg> = BTreeMap::new();
        for ns in node_stats {
            for is in &ns.index_stats {
                let key = (is.schema.clone(), is.table.clone(), is.index_name.clone());
                let entry = agg.entry(key).or_default();
                entry.total_idx_scan += is.stats.idx_scan;
                if is.stats.size > entry.max_size {
                    entry.max_size = is.stats.size;
                }
            }
        }

        // build index lookup from tables
        let idx_lookup: BTreeMap<(&str, &str, &str), &Index> = tables
            .iter()
            .flat_map(|t| {
                t.indexes
                    .iter()
                    .map(move |idx| (t.schema.as_str(), t.name.as_str(), idx.name.as_str(), idx))
            })
            .map(|(s, t, n, idx)| ((s, t, n), idx))
            .collect();

        for ((schema, table, index_name), a) in &agg {
            if a.total_idx_scan != 0 {
                continue;
            }

            let idx_info = idx_lookup.get(&(schema.as_str(), table.as_str(), index_name.as_str()));

            // skip primary keys
            if idx_info.is_some_and(|idx| idx.is_primary) {
                continue;
            }

            entries.push(UnusedIndexEntry {
                schema: schema.clone(),
                table: table.clone(),
                index_name: index_name.clone(),
                total_idx_scan: 0,
                total_size_bytes: a.max_size,
                is_unique: idx_info.is_some_and(|idx| idx.is_unique),
                definition: idx_info
                    .map(|idx| idx.definition.clone())
                    .unwrap_or_default(),
            });
        }
    }

    // sort by size descending (biggest waste first)
    entries.sort_by_key(|b| std::cmp::Reverse(b.total_size_bytes));
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index_stats(idx_scan: i64, size: i64) -> IndexStats {
        IndexStats {
            idx_scan,
            idx_tup_read: 0,
            idx_tup_fetch: 0,
            size,
            relpages: 0,
            reltuples: 0.0,
        }
    }

    fn make_index(
        name: &str,
        is_primary: bool,
        is_unique: bool,
        stats: Option<IndexStats>,
    ) -> Index {
        Index {
            name: name.into(),
            columns: vec!["col".into()],
            include_columns: vec![],
            index_type: "btree".into(),
            is_unique,
            is_primary,
            predicate: None,
            definition: format!("CREATE INDEX {name} ON t (col)"),
            is_valid: true,
            backs_constraint: false,
            stats,
        }
    }

    fn make_table(name: &str, indexes: Vec<Index>) -> Table {
        Table {
            oid: 0,
            schema: "public".into(),
            name: name.into(),
            columns: vec![],
            constraints: vec![],
            indexes,
            comment: None,
            stats: None,
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }
    }

    fn make_node_stats(source: &str, index_stats: Vec<NodeIndexStats>) -> NodeStats {
        NodeStats {
            source: source.into(),
            timestamp: chrono::Utc::now(),
            is_standby: false,
            table_stats: vec![],
            index_stats,
            column_stats: vec![],
        }
    }

    // --- single-node (empty node_stats) tests ---

    #[test]
    fn test_single_node_unused_index_detected() {
        let tables = vec![make_table(
            "orders",
            vec![make_index(
                "idx_unused",
                false,
                false,
                Some(make_index_stats(0, 8192)),
            )],
        )];

        let result = detect_unused_indexes(&[], &tables);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_name, "idx_unused");
        assert_eq!(result[0].total_size_bytes, 8192);
    }

    #[test]
    fn test_single_node_used_index_not_reported() {
        let tables = vec![make_table(
            "orders",
            vec![make_index(
                "idx_used",
                false,
                false,
                Some(make_index_stats(42, 8192)),
            )],
        )];

        let result = detect_unused_indexes(&[], &tables);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_node_primary_key_skipped() {
        let tables = vec![make_table(
            "orders",
            vec![make_index(
                "orders_pkey",
                true,
                true,
                Some(make_index_stats(0, 8192)),
            )],
        )];

        let result = detect_unused_indexes(&[], &tables);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_node_no_stats_skipped() {
        let tables = vec![make_table(
            "orders",
            vec![make_index("idx_no_stats", false, false, None)],
        )];

        let result = detect_unused_indexes(&[], &tables);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_node_unique_flag_preserved() {
        let tables = vec![make_table(
            "orders",
            vec![make_index(
                "idx_unique_unused",
                false,
                true,
                Some(make_index_stats(0, 4096)),
            )],
        )];

        let result = detect_unused_indexes(&[], &tables);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_unique);
    }

    // --- multi-node tests ---

    #[test]
    fn test_multi_node_unused_across_all_nodes() {
        let tables = vec![make_table(
            "orders",
            vec![make_index("idx_unused", false, false, None)],
        )];

        let node_stats = vec![
            make_node_stats(
                "node1",
                vec![NodeIndexStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    index_name: "idx_unused".into(),
                    stats: make_index_stats(0, 8192),
                }],
            ),
            make_node_stats(
                "node2",
                vec![NodeIndexStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    index_name: "idx_unused".into(),
                    stats: make_index_stats(0, 16384),
                }],
            ),
        ];

        let result = detect_unused_indexes(&node_stats, &tables);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_name, "idx_unused");
        // should use max size across nodes
        assert_eq!(result[0].total_size_bytes, 16384);
    }

    #[test]
    fn test_multi_node_used_on_one_node_not_reported() {
        let tables = vec![make_table(
            "orders",
            vec![make_index("idx_partial_use", false, false, None)],
        )];

        let node_stats = vec![
            make_node_stats(
                "node1",
                vec![NodeIndexStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    index_name: "idx_partial_use".into(),
                    stats: make_index_stats(0, 8192),
                }],
            ),
            make_node_stats(
                "node2",
                vec![NodeIndexStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    index_name: "idx_partial_use".into(),
                    stats: make_index_stats(5, 8192),
                }],
            ),
        ];

        let result = detect_unused_indexes(&node_stats, &tables);
        assert!(result.is_empty());
    }

    #[test]
    fn test_multi_node_primary_key_skipped() {
        let tables = vec![make_table(
            "orders",
            vec![make_index("orders_pkey", true, true, None)],
        )];

        let node_stats = vec![make_node_stats(
            "node1",
            vec![NodeIndexStats {
                schema: "public".into(),
                table: "orders".into(),
                index_name: "orders_pkey".into(),
                stats: make_index_stats(0, 8192),
            }],
        )];

        let result = detect_unused_indexes(&node_stats, &tables);
        assert!(result.is_empty());
    }

    #[test]
    fn test_multi_node_sorted_by_size_desc() {
        let tables = vec![make_table(
            "orders",
            vec![
                make_index("idx_small", false, false, None),
                make_index("idx_big", false, false, None),
            ],
        )];

        let node_stats = vec![make_node_stats(
            "node1",
            vec![
                NodeIndexStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    index_name: "idx_small".into(),
                    stats: make_index_stats(0, 1024),
                },
                NodeIndexStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    index_name: "idx_big".into(),
                    stats: make_index_stats(0, 999_999),
                },
            ],
        )];

        let result = detect_unused_indexes(&node_stats, &tables);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].index_name, "idx_big");
        assert_eq!(result[1].index_name, "idx_small");
    }

    #[test]
    fn test_multi_node_unknown_index_still_reported() {
        // index in node_stats but not in tables — should still appear with defaults
        let tables: Vec<Table> = vec![];

        let node_stats = vec![make_node_stats(
            "node1",
            vec![NodeIndexStats {
                schema: "public".into(),
                table: "orders".into(),
                index_name: "idx_ghost".into(),
                stats: make_index_stats(0, 4096),
            }],
        )];

        let result = detect_unused_indexes(&node_stats, &tables);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_name, "idx_ghost");
        assert!(!result[0].is_unique);
        assert!(result[0].definition.is_empty());
    }

    #[test]
    fn test_empty_inputs_returns_empty() {
        let result = detect_unused_indexes(&[], &[]);
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // Snapshot-split types: serde round-trip + identity checks.
    // -----------------------------------------------------------------------

    #[test]
    fn qualified_name_displays_schema_dot_name() {
        let qn = QualifiedName::new("public", "orders");
        assert_eq!(qn.to_string(), "public.orders");
    }

    #[test]
    fn qualified_name_round_trips_through_serde() {
        let qn = QualifiedName::new("public", "orders");
        let json = serde_json::to_string(&qn).unwrap();
        let back: QualifiedName = serde_json::from_str(&json).unwrap();
        assert_eq!(back, qn);
    }

    fn sample_planner_stats() -> PlannerStatsSnapshot {
        PlannerStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "accounts".into(),
            timestamp: Utc::now(),
            content_hash: "abc123".into(),
            schema_ref_hash: "def456".into(),
            tables: vec![TableSizingEntry {
                table: QualifiedName::new("public", "orders"),
                sizing: TableSizing {
                    reltuples: 1234.0,
                    relpages: 42,
                    table_size: 1_000_000,
                    total_size: Some(2_000_000),
                    index_size: Some(1_000_000),
                },
            }],
            columns: vec![ColumnStatsEntry {
                table: QualifiedName::new("public", "orders"),
                column: "user_id".into(),
                stats: ColumnStats {
                    null_frac: Some(0.0),
                    n_distinct: Some(-0.5),
                    most_common_vals: None,
                    most_common_freqs: None,
                    histogram_bounds: None,
                    correlation: Some(0.1),
                },
            }],
            indexes: vec![IndexSizingEntry {
                index: QualifiedName::new("public", "orders_pkey"),
                sizing: IndexSizing {
                    size: 8192,
                    relpages: 1,
                    reltuples: 1234.0,
                },
            }],
        }
    }

    fn sample_activity_stats() -> ActivityStatsSnapshot {
        ActivityStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "accounts".into(),
            timestamp: Utc::now(),
            content_hash: "h1".into(),
            schema_ref_hash: "h2".into(),
            node: NodeIdentity {
                label: "primary".into(),
                host: "10.0.0.1".into(),
                is_standby: false,
                replication_lag_bytes: None,
                stats_reset: None,
            },
            tables: vec![TableActivityEntry {
                table: QualifiedName::new("public", "orders"),
                activity: TableActivity {
                    seq_scan: 7,
                    idx_scan: 100,
                    n_live_tup: 1000,
                    n_dead_tup: 5,
                    last_vacuum: None,
                    last_autovacuum: None,
                    last_analyze: None,
                    last_autoanalyze: None,
                    vacuum_count: 0,
                    autovacuum_count: 1,
                    analyze_count: 0,
                    autoanalyze_count: 1,
                },
            }],
            indexes: vec![IndexActivityEntry {
                index: QualifiedName::new("public", "orders_pkey"),
                activity: IndexActivity {
                    idx_scan: 100,
                    idx_tup_read: 200,
                    idx_tup_fetch: 150,
                },
            }],
        }
    }

    #[test]
    fn planner_stats_round_trips_through_json() {
        let snap = sample_planner_stats();
        let json = serde_json::to_string(&snap).unwrap();
        let back: PlannerStatsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.tables.len(), 1);
        assert_eq!(back.tables[0].table, snap.tables[0].table);
        assert_eq!(back.columns.len(), 1);
        assert_eq!(back.columns[0].column, "user_id");
        assert_eq!(back.indexes.len(), 1);
        assert_eq!(back.indexes[0].index.name, "orders_pkey");
        assert_eq!(back.schema_ref_hash, "def456");
    }

    #[test]
    fn activity_stats_round_trips_through_json() {
        let snap = sample_activity_stats();
        let json = serde_json::to_string(&snap).unwrap();
        let back: ActivityStatsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.node.label, "primary");
        assert!(!back.node.is_standby);
        assert_eq!(back.tables[0].activity.seq_scan, 7);
        assert_eq!(back.indexes[0].activity.idx_scan, 100);
    }

    #[test]
    fn activity_stats_accepts_missing_optional_fields() {
        // Older payloads without the *_count fields and without lag should still load.
        let json = r#"{
            "pg_version": "PostgreSQL 17.0",
            "database": "accounts",
            "timestamp": "2026-01-01T00:00:00Z",
            "content_hash": "h1",
            "schema_ref_hash": "h2",
            "node": {
                "label": "replica1",
                "host": "10.0.0.2",
                "is_standby": true
            },
            "tables": [{
                "table": {"schema": "public", "name": "orders"},
                "activity": {
                    "seq_scan": 1,
                    "idx_scan": 2,
                    "last_vacuum": null,
                    "last_autovacuum": null,
                    "last_analyze": null,
                    "last_autoanalyze": null
                }
            }],
            "indexes": []
        }"#;
        let back: ActivityStatsSnapshot = serde_json::from_str(json).unwrap();
        assert!(back.node.is_standby);
        assert!(back.node.replication_lag_bytes.is_none());
        assert_eq!(back.tables[0].activity.n_live_tup, 0);
        assert_eq!(back.tables[0].activity.vacuum_count, 0);
    }

    #[test]
    fn node_selector_variants_are_constructable() {
        let _ = NodeSelector::All;
        match NodeSelector::Some(vec!["primary".into(), "replica1".into()]) {
            NodeSelector::Some(v) => assert_eq!(v.len(), 2),
            NodeSelector::All => panic!("wrong variant"),
        }
    }

    fn activity_for(
        label: &str,
        idx_scan: i64,
        seq_scan: i64,
        n_dead_tup: i64,
        last_vacuum: Option<DateTime<Utc>>,
        last_autovacuum: Option<DateTime<Utc>>,
        stats_reset: Option<DateTime<Utc>>,
    ) -> ActivityStatsSnapshot {
        ActivityStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "accounts".into(),
            timestamp: Utc::now(),
            content_hash: format!("hash-{label}"),
            schema_ref_hash: "schema-h".into(),
            node: NodeIdentity {
                label: label.into(),
                host: format!("10.0.0.{label}"),
                is_standby: label != "primary",
                replication_lag_bytes: None,
                stats_reset,
            },
            tables: vec![TableActivityEntry {
                table: QualifiedName::new("public", "orders"),
                activity: TableActivity {
                    seq_scan,
                    idx_scan,
                    n_live_tup: 0,
                    n_dead_tup,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze: None,
                    last_autoanalyze: None,
                    vacuum_count: 0,
                    autovacuum_count: 0,
                    analyze_count: 0,
                    autoanalyze_count: 0,
                },
            }],
            indexes: vec![IndexActivityEntry {
                index: QualifiedName::new("public", "orders_pkey"),
                activity: IndexActivity {
                    idx_scan,
                    idx_tup_read: 0,
                    idx_tup_fetch: 0,
                },
            }],
        }
    }

    fn empty_schema_snap() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "accounts".into(),
            timestamp: Utc::now(),
            content_hash: "schema-h".into(),
            source: None,
            tables: vec![],
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

    fn snap_with_nodes(nodes: Vec<ActivityStatsSnapshot>) -> AnnotatedSnapshot {
        let mut activity_by_node = BTreeMap::new();
        for n in nodes {
            activity_by_node.insert(n.node.label.clone(), n);
        }
        AnnotatedSnapshot {
            schema: empty_schema_snap(),
            planner: None,
            activity_by_node,
        }
    }

    #[test]
    fn merged_activity_idx_scan_sum_across_nodes() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 10, 0, 0, None, None, None),
            activity_for("replica1", 20, 0, 0, None, None, None),
            activity_for("replica2", 5, 0, 0, None, None, None),
        ]);
        let merged = snap.merged(&NodeSelector::All).expect("3 nodes");
        let ix = QualifiedName::new("public", "orders_pkey");
        assert_eq!(merged.idx_scan_sum(&ix), 35);
    }

    #[test]
    fn merged_activity_idx_scan_per_node_returns_breakdown() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 10, 0, 0, None, None, None),
            activity_for("replica1", 20, 0, 0, None, None, None),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        let ix = QualifiedName::new("public", "orders_pkey");
        let per_node = merged.idx_scan_per_node(&ix);
        // BTreeMap ordering: primary < replica1
        assert_eq!(
            per_node,
            vec![("primary".into(), 10), ("replica1".into(), 20)]
        );
    }

    #[test]
    fn merged_activity_seq_scan_sum_across_nodes() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 0, 3, 0, None, None, None),
            activity_for("replica1", 0, 7, 0, None, None, None),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        let t = QualifiedName::new("public", "orders");
        assert_eq!(merged.seq_scan_sum(&t), 10);
    }

    #[test]
    fn merged_activity_n_dead_tup_sums_across_nodes() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 0, 0, 100, None, None, None),
            activity_for("replica1", 0, 0, 50, None, None, None),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        let t = QualifiedName::new("public", "orders");
        assert_eq!(merged.n_dead_tup_sum(&t), 150);
    }

    #[test]
    fn merged_activity_last_vacuum_max_picks_max_across_nodes_and_kinds() {
        let early = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let mid = "2026-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let late = "2026-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let snap = snap_with_nodes(vec![
            // primary: manual at early, autovacuum at mid → node max = mid
            activity_for("primary", 0, 0, 0, Some(early), Some(mid), None),
            // replica1: autovacuum at late → node max = late
            activity_for("replica1", 0, 0, 0, None, Some(late), None),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        let t = QualifiedName::new("public", "orders");
        assert_eq!(merged.last_vacuum_max(&t), Some(late));
    }

    #[test]
    fn merged_activity_last_vacuum_max_returns_none_when_never_vacuumed() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 0, 0, 0, None, None, None),
            activity_for("replica1", 0, 0, 0, None, None, None),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        let t = QualifiedName::new("public", "orders");
        assert_eq!(merged.last_vacuum_max(&t), None);
    }

    #[test]
    fn annotated_snapshot_view_defaults_to_primary() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 1, 0, 0, None, None, None),
            activity_for("replica1", 2, 0, 0, None, None, None),
        ]);
        let view = snap.view(None);
        let activity = view.activity.expect("primary should resolve by default");
        assert_eq!(activity.node.label, "primary");
    }

    #[test]
    fn annotated_snapshot_view_unknown_label_yields_no_activity() {
        let snap = snap_with_nodes(vec![activity_for("primary", 1, 0, 0, None, None, None)]);
        let view = snap.view(Some("ghost"));
        assert!(view.activity.is_none());
    }

    #[test]
    fn annotated_snapshot_view_single_node_has_no_merged() {
        let snap = snap_with_nodes(vec![activity_for("primary", 1, 0, 0, None, None, None)]);
        let view = snap.view(None);
        assert!(view.merged.is_none());
    }

    #[test]
    fn annotated_snapshot_view_multi_node_populates_merged() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 1, 0, 0, None, None, None),
            activity_for("replica1", 2, 0, 0, None, None, None),
        ]);
        let view = snap.view(None);
        let merged = view.merged.expect("multi-node should produce merged view");
        assert_eq!(merged.nodes.len(), 2);
    }

    #[test]
    fn annotated_snapshot_merged_partial_when_any_node_lacks_reset() {
        let reset = "2026-04-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let snap = snap_with_nodes(vec![
            activity_for("primary", 0, 0, 0, None, None, Some(reset)),
            activity_for("replica1", 0, 0, 0, None, None, None),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        assert!(
            merged.partial,
            "partial should be true when a node lacks stats_reset"
        );
    }

    #[test]
    fn annotated_snapshot_merged_window_start_is_min_reset() {
        let early = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let later = "2026-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let snap = snap_with_nodes(vec![
            activity_for("primary", 0, 0, 0, None, None, Some(later)),
            activity_for("replica1", 0, 0, 0, None, None, Some(early)),
        ]);
        let merged = snap.merged(&NodeSelector::All).unwrap();
        assert_eq!(merged.window_start, early);
        assert!(!merged.partial);
    }

    #[test]
    fn annotated_snapshot_merged_node_selector_some_filters() {
        let snap = snap_with_nodes(vec![
            activity_for("primary", 1, 0, 0, None, None, None),
            activity_for("replica1", 2, 0, 0, None, None, None),
            activity_for("replica2", 4, 0, 0, None, None, None),
        ]);
        let merged = snap
            .merged(&NodeSelector::Some(vec![
                "replica1".into(),
                "replica2".into(),
            ]))
            .unwrap();
        let ix = QualifiedName::new("public", "orders_pkey");
        assert_eq!(merged.idx_scan_sum(&ix), 6);
        assert_eq!(merged.nodes.len(), 2);
    }

    #[test]
    fn annotated_snapshot_merged_returns_none_for_empty_selector() {
        let snap = snap_with_nodes(vec![]);
        assert!(snap.merged(&NodeSelector::All).is_none());
    }
}

// use aggregated multi-node stats over table-level stats
pub fn effective_table_stats(table: &Table, schema: &SchemaSnapshot) -> Option<TableStats> {
    if !schema.node_stats.is_empty()
        && let Some(agg) = aggregate_table_stats(&schema.node_stats, &table.schema, &table.name)
    {
        return Some(agg);
    }
    table.stats.clone()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub source: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub is_standby: bool,
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

//
// Snapshot split: schema / planner_stats / activity_stats
//
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}

impl QualifiedName {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSizing {
    pub reltuples: f64,
    #[serde(default)]
    pub relpages: i64,
    pub table_size: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_size: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableActivity {
    pub seq_scan: i64,
    pub idx_scan: i64,
    #[serde(default)]
    pub n_live_tup: i64,
    #[serde(default)]
    pub n_dead_tup: i64,
    pub last_vacuum: Option<DateTime<Utc>>,
    pub last_autovacuum: Option<DateTime<Utc>>,
    pub last_analyze: Option<DateTime<Utc>>,
    pub last_autoanalyze: Option<DateTime<Utc>>,
    #[serde(default)]
    pub vacuum_count: i64,
    #[serde(default)]
    pub autovacuum_count: i64,
    #[serde(default)]
    pub analyze_count: i64,
    #[serde(default)]
    pub autoanalyze_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSizing {
    pub size: i64,
    #[serde(default)]
    pub relpages: i64,
    #[serde(default)]
    pub reltuples: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexActivity {
    pub idx_scan: i64,
    pub idx_tup_read: i64,
    pub idx_tup_fetch: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub label: String,
    pub host: String,
    pub is_standby: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_lag_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats_reset: Option<DateTime<Utc>>,
}

// Vec<...Entry> rather than HashMap<QualifiedName, _> in the persisted shape:
// JSON map keys must be strings, and a tuple key (table, column) does not
// round-trip through serde_json. Readers build a HashMap on load.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSizingEntry {
    pub table: QualifiedName,
    pub sizing: TableSizing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableActivityEntry {
    pub table: QualifiedName,
    pub activity: TableActivity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatsEntry {
    pub table: QualifiedName,
    pub column: String,
    pub stats: ColumnStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSizingEntry {
    pub index: QualifiedName,
    pub sizing: IndexSizing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexActivityEntry {
    pub index: QualifiedName,
    pub activity: IndexActivity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerStatsSnapshot {
    pub pg_version: String,
    pub database: String,
    pub timestamp: DateTime<Utc>,
    pub content_hash: String,
    pub schema_ref_hash: String,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub tables: Vec<TableSizingEntry>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub columns: Vec<ColumnStatsEntry>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub indexes: Vec<IndexSizingEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityStatsSnapshot {
    pub pg_version: String,
    pub database: String,
    pub timestamp: DateTime<Utc>,
    pub content_hash: String,
    pub schema_ref_hash: String,
    pub node: NodeIdentity,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub tables: Vec<TableActivityEntry>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub indexes: Vec<IndexActivityEntry>,
}

// In-memory views — never persisted, so no serde derive.

#[derive(Debug, Clone)]
pub enum NodeSelector {
    All,
    Some(Vec<String>),
}

#[derive(Debug)]
pub struct AnnotatedSchema<'a> {
    pub schema: &'a SchemaSnapshot,
    pub planner: Option<&'a PlannerStatsSnapshot>,
    pub activity: Option<&'a ActivityStatsSnapshot>,
    pub merged: Option<MergedActivity<'a>>,
}

#[derive(Debug)]
pub struct MergedActivity<'a> {
    pub schema_ref_hash: String,
    pub nodes: Vec<&'a ActivityStatsSnapshot>,
    pub window_start: DateTime<Utc>,
    pub partial: bool,
}

impl<'a> MergedActivity<'a> {
    pub fn idx_scan_sum(&self, ix: &QualifiedName) -> i64 {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.indexes
                    .iter()
                    .find(|e| &e.index == ix)
                    .map(|e| e.activity.idx_scan)
            })
            .sum()
    }

    pub fn idx_scan_per_node(&self, ix: &QualifiedName) -> Vec<(String, i64)> {
        self.nodes
            .iter()
            .map(|n| {
                let scan = n
                    .indexes
                    .iter()
                    .find(|e| &e.index == ix)
                    .map(|e| e.activity.idx_scan)
                    .unwrap_or(0);
                (n.node.label.clone(), scan)
            })
            .collect()
    }

    pub fn seq_scan_sum(&self, t: &QualifiedName) -> i64 {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.tables
                    .iter()
                    .find(|e| &e.table == t)
                    .map(|e| e.activity.seq_scan)
            })
            .sum()
    }

    // max across nodes of max(last_vacuum, last_autovacuum) — "did anything vacuum"
    pub fn last_vacuum_max(&self, t: &QualifiedName) -> Option<DateTime<Utc>> {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.tables.iter().find(|e| &e.table == t).and_then(|e| {
                    match (e.activity.last_vacuum, e.activity.last_autovacuum) {
                        (Some(a), Some(b)) => Some(a.max(b)),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    }
                })
            })
            .max()
    }

    pub fn n_dead_tup_sum(&self, t: &QualifiedName) -> i64 {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.tables
                    .iter()
                    .find(|e| &e.table == t)
                    .map(|e| e.activity.n_dead_tup)
            })
            .sum()
    }
}

#[derive(Debug, Clone)]
pub struct AnnotatedSnapshot {
    pub schema: SchemaSnapshot,
    pub planner: Option<PlannerStatsSnapshot>,
    pub activity_by_node: BTreeMap<String, ActivityStatsSnapshot>,
}

impl AnnotatedSnapshot {
    pub fn view(&self, node_label: Option<&str>) -> AnnotatedSchema<'_> {
        let label = node_label.unwrap_or("primary");
        let activity = self.activity_by_node.get(label);
        let merged = if self.activity_by_node.len() > 1 {
            self.merged(&NodeSelector::All)
        } else {
            None
        };
        AnnotatedSchema {
            schema: &self.schema,
            planner: self.planner.as_ref(),
            activity,
            merged,
        }
    }

    pub fn merged(&self, selector: &NodeSelector) -> Option<MergedActivity<'_>> {
        let nodes: Vec<&ActivityStatsSnapshot> = match selector {
            NodeSelector::All => self.activity_by_node.values().collect(),
            NodeSelector::Some(labels) => labels
                .iter()
                .filter_map(|l| self.activity_by_node.get(l))
                .collect(),
        };
        if nodes.is_empty() {
            return None;
        }
        let schema_ref_hash = nodes[0].schema_ref_hash.clone();
        let partial = nodes.iter().any(|n| n.node.stats_reset.is_none());
        let window_start = nodes
            .iter()
            .map(|n| n.node.stats_reset.unwrap_or(n.timestamp))
            .min()
            .unwrap_or(nodes[0].timestamp);
        Some(MergedActivity {
            schema_ref_hash,
            nodes,
            window_start,
            partial,
        })
    }

    pub fn node_labels(&self) -> impl Iterator<Item = &str> {
        self.activity_by_node.keys().map(|s| s.as_str())
    }
}
