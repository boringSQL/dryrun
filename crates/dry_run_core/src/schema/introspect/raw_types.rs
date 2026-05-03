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
