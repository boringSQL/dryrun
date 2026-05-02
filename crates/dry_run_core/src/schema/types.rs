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
}

fn default_true() -> bool {
    true
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

#[derive(Debug, Clone)]
pub struct NodeImbalanceInfo {
    pub hot_node: String,
    pub multiplier: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleStatsEntry {
    pub node: String,
    pub schema: String,
    pub table: String,
    pub last_analyzed_days_ago: Option<i64>,
}

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

pub fn detect_bloated_indexes(
    annotated: &AnnotatedSchema<'_>,
    threshold: f64,
) -> Vec<BloatedIndexEntry> {
    let mut entries = Vec::new();

    for table in &annotated.schema.tables {
        for idx in &table.indexes {
            let qn = QualifiedName::new(&table.schema, &idx.name);
            let sizing = annotated.index_sizing(&qn);
            if let Some(est) = super::bloat::estimate_index_bloat(idx, sizing, table)
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

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    pub fn last_analyze_max(&self, t: &QualifiedName) -> Option<DateTime<Utc>> {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.tables.iter().find(|e| &e.table == t).and_then(|e| {
                    match (e.activity.last_analyze, e.activity.last_autoanalyze) {
                        (Some(a), Some(b)) => Some(a.max(b)),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    }
                })
            })
            .max()
    }

    pub fn vacuum_count_sum(&self, t: &QualifiedName) -> i64 {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.tables
                    .iter()
                    .find(|e| &e.table == t)
                    .map(|e| e.activity.vacuum_count + e.activity.autovacuum_count)
            })
            .sum()
    }
}

// Accessors that read from planner (sizing / column histograms) and from
// activity (counters), with a uniform fall-through: merged across nodes →
// single-node activity → empty. Consumers don't have to branch on
// "do I have one node or many".
impl<'a> AnnotatedSchema<'a> {
    pub fn reltuples(&self, t: &QualifiedName) -> Option<f64> {
        self.planner?
            .tables
            .iter()
            .find(|e| &e.table == t)
            .map(|e| e.sizing.reltuples)
    }

    pub fn table_size(&self, t: &QualifiedName) -> Option<i64> {
        self.planner?
            .tables
            .iter()
            .find(|e| &e.table == t)
            .map(|e| e.sizing.table_size)
    }

    pub fn relpages(&self, t: &QualifiedName) -> Option<i64> {
        self.planner?
            .tables
            .iter()
            .find(|e| &e.table == t)
            .map(|e| e.sizing.relpages)
    }

    pub fn column_stats(&self, t: &QualifiedName, col: &str) -> Option<&'a ColumnStats> {
        self.planner?
            .columns
            .iter()
            .find(|e| &e.table == t && e.column == col)
            .map(|e| &e.stats)
    }

    pub fn index_sizing(&self, ix: &QualifiedName) -> Option<&'a IndexSizing> {
        self.planner?
            .indexes
            .iter()
            .find(|e| &e.index == ix)
            .map(|e| &e.sizing)
    }

    pub fn idx_scan_sum(&self, ix: &QualifiedName) -> i64 {
        if let Some(m) = &self.merged {
            return m.idx_scan_sum(ix);
        }
        self.activity
            .and_then(|a| a.indexes.iter().find(|e| &e.index == ix))
            .map(|e| e.activity.idx_scan)
            .unwrap_or(0)
    }

    pub fn idx_scan_per_node(&self, ix: &QualifiedName) -> Vec<(String, i64)> {
        if let Some(m) = &self.merged {
            return m.idx_scan_per_node(ix);
        }
        match self.activity {
            Some(a) => {
                let scan = a
                    .indexes
                    .iter()
                    .find(|e| &e.index == ix)
                    .map(|e| e.activity.idx_scan)
                    .unwrap_or(0);
                vec![(a.node.label.clone(), scan)]
            }
            None => Vec::new(),
        }
    }

    // Per-node breakdown of seq_scan counters for a single table — used by
    // tools that want to surface "this replica is doing the unindexed work,
    // the others aren't" patterns. Ordering follows the BTreeMap key order
    // when more than one node is present, so output is stable across runs.
    pub fn seq_scan_per_node(&self, t: &QualifiedName) -> Vec<(String, i64)> {
        if let Some(m) = &self.merged {
            return m
                .nodes
                .iter()
                .map(|n| {
                    let scan = n
                        .tables
                        .iter()
                        .find(|e| &e.table == t)
                        .map(|e| e.activity.seq_scan)
                        .unwrap_or(0);
                    (n.node.label.clone(), scan)
                })
                .collect();
        }
        match self.activity {
            Some(a) => {
                let scan = a
                    .tables
                    .iter()
                    .find(|e| &e.table == t)
                    .map(|e| e.activity.seq_scan)
                    .unwrap_or(0);
                vec![(a.node.label.clone(), scan)]
            }
            None => Vec::new(),
        }
    }

    pub fn seq_scan_sum(&self, t: &QualifiedName) -> i64 {
        if let Some(m) = &self.merged {
            return m.seq_scan_sum(t);
        }
        self.activity
            .and_then(|a| a.tables.iter().find(|e| &e.table == t))
            .map(|e| e.activity.seq_scan)
            .unwrap_or(0)
    }

    pub fn n_dead_tup_sum(&self, t: &QualifiedName) -> i64 {
        if let Some(m) = &self.merged {
            return m.n_dead_tup_sum(t);
        }
        self.activity
            .and_then(|a| a.tables.iter().find(|e| &e.table == t))
            .map(|e| e.activity.n_dead_tup)
            .unwrap_or(0)
    }

    pub fn last_vacuum_max(&self, t: &QualifiedName) -> Option<DateTime<Utc>> {
        if let Some(m) = &self.merged {
            return m.last_vacuum_max(t);
        }
        let e = self
            .activity
            .and_then(|a| a.tables.iter().find(|e| &e.table == t))?;
        match (e.activity.last_vacuum, e.activity.last_autovacuum) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    pub fn last_analyze_max(&self, t: &QualifiedName) -> Option<DateTime<Utc>> {
        if let Some(m) = &self.merged {
            return m.last_analyze_max(t);
        }
        let e = self
            .activity
            .and_then(|a| a.tables.iter().find(|e| &e.table == t))?;
        match (e.activity.last_analyze, e.activity.last_autoanalyze) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    pub fn vacuum_count_sum(&self, t: &QualifiedName) -> i64 {
        if let Some(m) = &self.merged {
            return m.vacuum_count_sum(t);
        }
        self.activity
            .and_then(|a| a.tables.iter().find(|e| &e.table == t))
            .map(|e| e.activity.vacuum_count + e.activity.autovacuum_count)
            .unwrap_or(0)
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

    // Indexes with zero scans across the requested nodes. Mirrors
    // `detect_unused_indexes` (legacy NodeStats path), but reads from the
    // activity_by_node map. Skips primary keys.
    pub fn unused_indexes(&self, selector: &NodeSelector) -> Vec<UnusedIndexEntry> {
        use std::collections::BTreeMap;

        let nodes: Vec<&ActivityStatsSnapshot> = match selector {
            NodeSelector::All => self.activity_by_node.values().collect(),
            NodeSelector::Some(labels) => labels
                .iter()
                .filter_map(|l| self.activity_by_node.get(l))
                .collect(),
        };

        // Build (qualified_index, sum, max_size) by walking each node's index activity,
        // joined to the planner's index sizing for byte counts.
        #[derive(Default)]
        struct Agg {
            total_idx_scan: i64,
            max_size: i64,
        }
        let mut agg: BTreeMap<QualifiedName, Agg> = BTreeMap::new();
        for n in &nodes {
            for ie in &n.indexes {
                let entry = agg.entry(ie.index.clone()).or_default();
                entry.total_idx_scan += ie.activity.idx_scan;
            }
        }
        if let Some(p) = &self.planner {
            for ie in &p.indexes {
                if let Some(entry) = agg.get_mut(&ie.index)
                    && ie.sizing.size > entry.max_size
                {
                    entry.max_size = ie.sizing.size;
                }
            }
        }

        let idx_lookup: BTreeMap<(&str, &str), &Index> = self
            .schema
            .tables
            .iter()
            .flat_map(|t| {
                t.indexes
                    .iter()
                    .map(move |idx| (t.schema.as_str(), t.name.as_str(), idx))
            })
            .map(|(s, _t, idx)| ((s, idx.name.as_str()), idx))
            .collect();

        let mut entries = Vec::new();
        for (qn, a) in &agg {
            if a.total_idx_scan != 0 {
                continue;
            }
            let idx_info = idx_lookup.get(&(qn.schema.as_str(), qn.name.as_str()));
            if idx_info.is_some_and(|idx| idx.is_primary) {
                continue;
            }

            // table name comes from the schema's index → owning table mapping
            let owning_table = self
                .schema
                .tables
                .iter()
                .find(|t| t.schema == qn.schema && t.indexes.iter().any(|idx| idx.name == qn.name))
                .map(|t| t.name.clone())
                .unwrap_or_default();

            entries.push(UnusedIndexEntry {
                schema: qn.schema.clone(),
                table: owning_table,
                index_name: qn.name.clone(),
                total_idx_scan: 0,
                total_size_bytes: a.max_size,
                is_unique: idx_info.is_some_and(|idx| idx.is_unique),
                definition: idx_info
                    .map(|idx| idx.definition.clone())
                    .unwrap_or_default(),
            });
        }
        entries.sort_by_key(|b| std::cmp::Reverse(b.total_size_bytes));
        entries
    }

    // Tables whose last_analyze (or last_autoanalyze) is older than `days`,
    // or which have never been analyzed. One entry per (node, table).
    pub fn stale_stats(&self, selector: &NodeSelector, days: i64) -> Vec<StaleStatsEntry> {
        let nodes: Vec<&ActivityStatsSnapshot> = match selector {
            NodeSelector::All => self.activity_by_node.values().collect(),
            NodeSelector::Some(labels) => labels
                .iter()
                .filter_map(|l| self.activity_by_node.get(l))
                .collect(),
        };
        let now = chrono::Utc::now();
        let threshold = chrono::TimeDelta::days(days);
        let mut entries = Vec::new();
        for n in nodes {
            for te in &n.tables {
                let last = te.activity.last_analyze.max(te.activity.last_autoanalyze);
                match last {
                    Some(when) if now - when > threshold => {
                        entries.push(StaleStatsEntry {
                            node: n.node.label.clone(),
                            schema: te.table.schema.clone(),
                            table: te.table.name.clone(),
                            last_analyzed_days_ago: Some((now - when).num_days()),
                        });
                    }
                    None => {
                        entries.push(StaleStatsEntry {
                            node: n.node.label.clone(),
                            schema: te.table.schema.clone(),
                            table: te.table.name.clone(),
                            last_analyzed_days_ago: None,
                        });
                    }
                    _ => {}
                }
            }
        }
        entries
    }

    // 5x+ seq_scan imbalance between hottest and coldest non-zero node.
    pub fn seq_scan_imbalance(&self, t: &QualifiedName) -> Option<NodeImbalanceInfo> {
        let scans: Vec<(&str, i64)> = self
            .activity_by_node
            .values()
            .filter_map(|n| {
                n.tables
                    .iter()
                    .find(|e| &e.table == t)
                    .map(|e| (n.node.label.as_str(), e.activity.seq_scan))
            })
            .collect();
        if scans.len() < 2 {
            return None;
        }
        let nonzero: Vec<(&str, i64)> = scans.into_iter().filter(|(_, v)| *v > 0).collect();
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
}
