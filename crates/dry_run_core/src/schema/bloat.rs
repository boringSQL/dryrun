use serde::{Deserialize, Serialize};

use super::types::{Index, IndexSizing, Table};

const PAGE_SIZE: f64 = 8192.0;
const BTREE_FILLFACTOR: f64 = 0.9;
const TUPLE_OVERHEAD: usize = 8;
const DEFAULT_WIDTH: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloatEstimate {
    pub bloat_ratio: f64,
    pub expected_pages: i64,
    pub actual_pages: i64,
    pub avg_key_width: usize,
}

pub fn estimate_index_bloat(
    index: &Index,
    sizing: Option<&IndexSizing>,
    table: &Table,
) -> Option<BloatEstimate> {
    let s = sizing?;
    estimate_index_bloat_from_stats(
        s.reltuples,
        s.relpages,
        &index.columns,
        table,
        &index.index_type,
    )
}

pub fn estimate_index_bloat_from_stats(
    reltuples: f64,
    relpages: i64,
    columns: &[String],
    table: &Table,
    index_type: &str,
) -> Option<BloatEstimate> {
    if index_type != "btree" {
        return None;
    }
    if reltuples <= 0.0 || relpages <= 0 {
        return None;
    }

    let col_types: std::collections::HashMap<&str, &str> = table
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.type_name.as_str()))
        .collect();

    let avg_key_width: usize = columns
        .iter()
        .map(|col| {
            col_types
                .get(col.as_str())
                .map(|tn| lookup_type_width(tn))
                .unwrap_or(DEFAULT_WIDTH) // expression column
        })
        .sum();

    if avg_key_width == 0 {
        return None;
    }

    let usable = PAGE_SIZE * BTREE_FILLFACTOR;
    let tuple_size = (TUPLE_OVERHEAD + avg_key_width) as f64;
    let tuples_per_page = usable / tuple_size;
    let expected_pages = (reltuples / tuples_per_page).ceil() as i64;
    let expected_pages = expected_pages.max(1);

    Some(BloatEstimate {
        bloat_ratio: relpages as f64 / expected_pages as f64,
        expected_pages,
        actual_pages: relpages,
        avg_key_width,
    })
}

fn lookup_type_width(type_name: &str) -> usize {
    let mut normalized = type_name.trim().to_lowercase();

    // strip parenthesized suffixes: varchar(255) -> varchar
    if let Some(idx) = normalized.find('(') {
        normalized.truncate(idx);
        normalized = normalized.trim_end().to_string();
    }

    // strip array suffix
    if normalized.ends_with("[]") {
        normalized.truncate(normalized.len() - 2);
    }

    match normalized.as_str() {
        "smallint" | "int2" => 2,
        "integer" | "int" | "int4" => 4,
        "bigint" | "int8" => 8,
        "real" | "float4" => 4,
        "double precision" | "float8" => 8,
        "boolean" | "bool" => 1,
        "date" => 4,
        "timestamp without time zone"
        | "timestamp"
        | "timestamp with time zone"
        | "timestamptz" => 8,
        "uuid" => 16,
        "inet" | "cidr" => 19,
        "macaddr" => 6,
        "oid" => 4,
        "numeric" => 16,
        "text" | "character varying" | "varchar" | "character" | "char" | "bpchar" | "bytea" => 32,
        "jsonb" | "json" | "xml" => 64,
        _ => DEFAULT_WIDTH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;

    fn make_table_with_cols(cols: Vec<(&str, &str)>) -> Table {
        Table {
            oid: 1,
            schema: "public".into(),
            name: "test".into(),
            columns: cols
                .into_iter()
                .enumerate()
                .map(|(i, (name, type_name))| Column {
                    ordinal: i as i16 + 1,
                    name: name.into(),
                    type_name: type_name.into(),
                    nullable: false,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    statistics_target: None,
                    stats: None,
                })
                .collect(),
            constraints: vec![],
            indexes: vec![],
            comment: None,
            stats: None,
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }
    }

    #[test]
    fn estimate_bloat_ratio() {
        let table = make_table_with_cols(vec![("id", "bigint"), ("name", "text")]);
        let est = estimate_index_bloat_from_stats(100_000.0, 1000, &["id".into()], &table, "btree");
        let est = est.unwrap();
        assert!(est.bloat_ratio > 0.0);
        assert_eq!(est.actual_pages, 1000);
        assert_eq!(est.avg_key_width, 8); // bigint
    }

    #[test]
    fn non_btree_returns_none() {
        let table = make_table_with_cols(vec![("data", "jsonb")]);
        let est = estimate_index_bloat_from_stats(100_000.0, 500, &["data".into()], &table, "gin");
        assert!(est.is_none());
    }

    #[test]
    fn type_width_lookup() {
        assert_eq!(lookup_type_width("bigint"), 8);
        assert_eq!(lookup_type_width("varchar(255)"), 32);
        assert_eq!(lookup_type_width("integer[]"), 4);
        assert_eq!(lookup_type_width("TIMESTAMP WITH TIME ZONE"), 8);
        assert_eq!(lookup_type_width("unknown_type"), DEFAULT_WIDTH);
    }

    fn bare_index(name: &str) -> Index {
        Index {
            name: name.into(),
            columns: vec!["id".into()],
            include_columns: vec![],
            index_type: "btree".into(),
            is_unique: true,
            is_primary: true,
            is_valid: true,
            backs_constraint: false,
            predicate: None,
            definition: String::new(),
            stats: None,
        }
    }

    #[test]
    fn bloat_estimated_when_index_sizing_present() {
        let table = make_table_with_cols(vec![("id", "bigint")]);
        let idx = bare_index("test_pkey");
        let sizing = IndexSizing {
            size: 8192 * 500,
            relpages: 500,
            reltuples: 100_000.0,
        };
        let est = estimate_index_bloat(&idx, Some(&sizing), &table);
        assert!(est.is_some());
    }

    #[test]
    fn bloat_returns_none_without_sizing() {
        let table = make_table_with_cols(vec![("id", "bigint")]);
        let idx = bare_index("test_pkey");
        assert!(estimate_index_bloat(&idx, None, &table).is_none());
    }
}
