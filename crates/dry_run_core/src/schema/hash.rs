use sha2::{Digest, Sha256};

use super::types::{
    Column, CompositeType, DomainType, EnumType, Extension, Function, Index, Table, View,
};

// content for schema content hash.
pub struct HashInput<'a> {
    pub pg_version: &'a str,
    pub tables: &'a [Table],
    pub enums: &'a [EnumType],
    pub domains: &'a [DomainType],
    pub composites: &'a [CompositeType],
    pub views: &'a [View],
    pub functions: &'a [Function],
    pub extensions: &'a [Extension],
}

pub fn compute_content_hash(input: &HashInput<'_>) -> String {
    // Strip runtime stats from tables/columns before hashing.
    let tables_structural: Vec<serde_json::Value> =
        input.tables.iter().map(table_to_structural).collect();

    let canonical = serde_json::json!({
        "pg_version": input.pg_version,
        "tables": tables_structural,
        "enums": input.enums,
        "domains": input.domains,
        "composites": input.composites,
        "views": input.views,
        "functions": input.functions,
        "extensions": input.extensions,
    });

    let json_bytes = serde_json::to_vec(&canonical).expect("schema serialization cannot fail");
    let digest = Sha256::digest(&json_bytes);
    hex_encode(digest)
}

fn table_to_structural(t: &Table) -> serde_json::Value {
    let columns: Vec<serde_json::Value> = t.columns.iter().map(column_to_structural).collect();
    let indexes: Vec<serde_json::Value> = t.indexes.iter().map(index_to_structural).collect();

    serde_json::json!({
        "schema": t.schema,
        "name": t.name,
        "columns": columns,
        "constraints": t.constraints,
        "indexes": indexes,
        "comment": t.comment,
        "partition_info": t.partition_info,
        "policies": t.policies,
        "triggers": t.triggers,
        "rls_enabled": t.rls_enabled,
    })
}

fn index_to_structural(idx: &Index) -> serde_json::Value {
    serde_json::json!({
        "name": idx.name,
        "columns": idx.columns,
        "include_columns": idx.include_columns,
        "index_type": idx.index_type,
        "is_unique": idx.is_unique,
        "is_primary": idx.is_primary,
        "predicate": idx.predicate,
        "definition": idx.definition,
    })
}

fn column_to_structural(c: &Column) -> serde_json::Value {
    serde_json::json!({
        "name": c.name,
        "ordinal": c.ordinal,
        "type_name": c.type_name,
        "nullable": c.nullable,
        "default": c.default,
        "identity": c.identity,
        "comment": c.comment,
    })
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{b:02x}").expect("write to String cannot fail");
        s
    })
}
