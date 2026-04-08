use chrono::{DateTime, Utc};

pub(super) struct RawTable {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub rls_enabled: bool,
    pub reloptions: Vec<String>,
}

pub(super) struct RawColumn {
    pub table_oid: u32,
    pub name: String,
    pub ordinal: i16,
    pub type_name: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub identity: Option<String>,
    pub generated: Option<String>,
    pub statistics_target: Option<i16>,
}

pub(super) struct RawConstraint {
    pub table_oid: u32,
    pub name: String,
    pub contype: String,
    pub columns: Vec<String>,
    pub definition: Option<String>,
    pub fk_table: Option<String>,
    pub fk_columns: Vec<String>,
    pub backing_index: Option<String>,
    pub comment: Option<String>,
}

pub(super) struct RawTableComment {
    pub table_oid: u32,
    pub comment: String,
}

pub(super) struct RawColumnComment {
    pub table_oid: u32,
    pub column_name: String,
    pub comment: String,
}

pub(super) struct RawIndex {
    pub table_oid: u32,
    pub name: String,
    pub columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub index_type: String,
    pub is_unique: bool,
    pub is_primary: bool,
    pub predicate: Option<String>,
    pub definition: String,
    pub is_valid: bool,
    pub backs_constraint: bool,
}

pub(super) struct RawTableStats {
    pub table_oid: u32,
    pub reltuples: f64,
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

pub(super) struct RawColumnStats {
    pub table_oid: u32,
    pub column_name: String,
    pub null_frac: Option<f64>,
    pub n_distinct: Option<f64>,
    pub most_common_vals: Option<String>,
    pub most_common_freqs: Option<String>,
    pub histogram_bounds: Option<String>,
    pub correlation: Option<f64>,
}

pub(super) struct RawPartitionInfo {
    pub table_oid: u32,
    pub strategy: String,
    pub key: String,
}

pub(super) struct RawPartitionChild {
    pub parent_oid: u32,
    pub schema: String,
    pub name: String,
    pub bound: String,
}

pub(super) struct RawPolicy {
    pub table_oid: u32,
    pub name: String,
    pub command: String,
    pub permissive: bool,
    pub roles: Vec<String>,
    pub using_expr: Option<String>,
    pub with_check_expr: Option<String>,
}

pub(super) struct RawTrigger {
    pub table_oid: u32,
    pub name: String,
    pub definition: String,
}

pub(super) struct RawIndexStats {
    pub table_oid: u32,
    pub index_name: String,
    pub idx_scan: i64,
    pub idx_tup_read: i64,
    pub idx_tup_fetch: i64,
    pub size: i64,
    pub relpages: i64,
    pub reltuples: f64,
}
