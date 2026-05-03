use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::{ColumnStats, Index, SchemaSnapshot, null_as_empty_vec};

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

    pub fn seq_scan_per_node(&self, t: &QualifiedName) -> Vec<(String, i64)> {
        self.nodes
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
            .collect()
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

// Planner reads serve sizing / column histograms; activity reads delegate
// to MergedActivity, which transparently aggregates across whatever nodes
// the snapshot has captured (one or many). When no activity is present
// the accessors return 0 / None / empty, so consumers never have to
// branch on "is there activity data".
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
        self.merged.as_ref().map_or(0, |m| m.idx_scan_sum(ix))
    }

    pub fn idx_scan_per_node(&self, ix: &QualifiedName) -> Vec<(String, i64)> {
        self.merged
            .as_ref()
            .map_or_else(Vec::new, |m| m.idx_scan_per_node(ix))
    }

    pub fn seq_scan_per_node(&self, t: &QualifiedName) -> Vec<(String, i64)> {
        self.merged
            .as_ref()
            .map_or_else(Vec::new, |m| m.seq_scan_per_node(t))
    }

    pub fn seq_scan_sum(&self, t: &QualifiedName) -> i64 {
        self.merged.as_ref().map_or(0, |m| m.seq_scan_sum(t))
    }

    pub fn n_dead_tup_sum(&self, t: &QualifiedName) -> i64 {
        self.merged.as_ref().map_or(0, |m| m.n_dead_tup_sum(t))
    }

    pub fn last_vacuum_max(&self, t: &QualifiedName) -> Option<DateTime<Utc>> {
        self.merged.as_ref().and_then(|m| m.last_vacuum_max(t))
    }

    pub fn last_analyze_max(&self, t: &QualifiedName) -> Option<DateTime<Utc>> {
        self.merged.as_ref().and_then(|m| m.last_analyze_max(t))
    }

    pub fn vacuum_count_sum(&self, t: &QualifiedName) -> i64 {
        self.merged.as_ref().map_or(0, |m| m.vacuum_count_sum(t))
    }
}

#[derive(Debug, Clone)]
pub struct AnnotatedSnapshot {
    pub schema: SchemaSnapshot,
    pub planner: Option<PlannerStatsSnapshot>,
    pub activity_by_node: BTreeMap<String, ActivityStatsSnapshot>,
}

impl AnnotatedSnapshot {
    pub fn view(&self) -> AnnotatedSchema<'_> {
        AnnotatedSchema {
            schema: &self.schema,
            planner: self.planner.as_ref(),
            merged: self.merged(&NodeSelector::All),
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

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
