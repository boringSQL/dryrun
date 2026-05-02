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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ColumnStats, IndexStats, TableStats};

    fn empty_table(schema: &str, name: &str) -> Table {
        Table {
            oid: 1,
            schema: schema.into(),
            name: name.into(),
            columns: vec![Column {
                name: "id".into(),
                ordinal: 1,
                type_name: "int4".into(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                statistics_target: None,
                stats: None,
            }],
            constraints: vec![],
            indexes: vec![Index {
                name: format!("{name}_pkey"),
                columns: vec!["id".into()],
                include_columns: vec![],
                index_type: "btree".into(),
                is_unique: true,
                is_primary: true,
                predicate: None,
                definition: format!("CREATE UNIQUE INDEX {name}_pkey ON {schema}.{name} (id)"),
                is_valid: true,
                backs_constraint: true,
                stats: None,
            }],
            comment: None,
            stats: None,
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }
    }

    fn populated_stats(t: &Table, reltuples: f64, idx_scan: i64) -> Table {
        let mut t = t.clone();
        t.stats = Some(TableStats {
            reltuples,
            relpages: (reltuples / 100.0) as i64,
            dead_tuples: 17,
            last_vacuum: None,
            last_autovacuum: None,
            last_analyze: None,
            last_autoanalyze: None,
            seq_scan: 9,
            idx_scan,
            table_size: 4096,
        });
        for col in &mut t.columns {
            col.stats = Some(ColumnStats {
                null_frac: Some(0.1),
                n_distinct: Some(reltuples),
                most_common_vals: None,
                most_common_freqs: None,
                histogram_bounds: None,
                correlation: Some(0.5),
            });
        }
        for idx in &mut t.indexes {
            idx.stats = Some(IndexStats {
                idx_scan,
                idx_tup_read: idx_scan * 2,
                idx_tup_fetch: idx_scan,
                size: 8192,
                relpages: 1,
                reltuples,
            });
        }
        t
    }

    fn input_for<'a>(tables: &'a [Table]) -> HashInput<'a> {
        HashInput {
            pg_version: "PostgreSQL 17.0",
            tables,
            enums: &[],
            domains: &[],
            composites: &[],
            views: &[],
            functions: &[],
            extensions: &[],
        }
    }

    // Drift hash must not change when only stats churn — same DDL, different
    // reltuples / idx_scan / dead_tuples / column histograms must produce the
    // same content_hash. Regression for the original snapshot-split bug.
    #[test]
    fn content_hash_stable_across_stats_churn() {
        let bare = empty_table("public", "orders");
        let cold = populated_stats(&bare, 100.0, 0);
        let hot = populated_stats(&bare, 1_000_000.0, 42_000);

        let h_bare = compute_content_hash(&input_for(&[bare]));
        let h_cold = compute_content_hash(&input_for(&[cold]));
        let h_hot = compute_content_hash(&input_for(&[hot]));

        assert_eq!(h_bare, h_cold, "stats absence vs. presence must not differ");
        assert_eq!(h_cold, h_hot, "stats values must not affect hash");
    }

    #[test]
    fn content_hash_changes_when_ddl_changes() {
        let a = empty_table("public", "orders");
        let b = empty_table("public", "orders_v2");
        assert_ne!(
            compute_content_hash(&input_for(&[a])),
            compute_content_hash(&input_for(&[b])),
        );
    }

    #[test]
    fn content_hash_changes_when_column_added() {
        let a = empty_table("public", "orders");
        let mut b = empty_table("public", "orders");
        b.columns.push(Column {
            name: "total".into(),
            ordinal: 2,
            type_name: "numeric".into(),
            nullable: true,
            default: None,
            identity: None,
            generated: None,
            comment: None,
            statistics_target: None,
            stats: None,
        });
        assert_ne!(
            compute_content_hash(&input_for(&[a])),
            compute_content_hash(&input_for(&[b])),
        );
    }

    #[test]
    fn content_hash_changes_when_column_type_changes() {
        let a = empty_table("public", "orders");
        let mut b = empty_table("public", "orders");
        b.columns[0].type_name = "int8".into();
        assert_ne!(
            compute_content_hash(&input_for(&[a])),
            compute_content_hash(&input_for(&[b])),
        );
    }

    #[test]
    fn content_hash_changes_when_column_nullability_changes() {
        let a = empty_table("public", "orders");
        let mut b = empty_table("public", "orders");
        b.columns[0].nullable = !b.columns[0].nullable;
        assert_ne!(
            compute_content_hash(&input_for(&[a])),
            compute_content_hash(&input_for(&[b])),
        );
    }

    #[test]
    fn content_hash_changes_when_index_added() {
        let a = empty_table("public", "orders");
        let mut b = empty_table("public", "orders");
        b.indexes.push(Index {
            name: "orders_id_idx".into(),
            columns: vec!["id".into()],
            include_columns: vec![],
            index_type: "btree".into(),
            is_unique: false,
            is_primary: false,
            predicate: None,
            definition: "CREATE INDEX orders_id_idx ON public.orders (id)".into(),
            is_valid: true,
            backs_constraint: false,
            stats: None,
        });
        assert_ne!(
            compute_content_hash(&input_for(&[a])),
            compute_content_hash(&input_for(&[b])),
        );
    }

    #[test]
    fn content_hash_changes_when_pg_version_changes() {
        let t = empty_table("public", "orders");
        let tables = vec![t];
        let mut a = input_for(&tables);
        let mut b = input_for(&tables);
        a.pg_version = "PostgreSQL 16.4";
        b.pg_version = "PostgreSQL 17.0";
        assert_ne!(compute_content_hash(&a), compute_content_hash(&b));
    }

    #[test]
    fn content_hash_changes_when_enum_added() {
        let tables: Vec<Table> = vec![];
        let no_enums = HashInput {
            pg_version: "PostgreSQL 17.0",
            tables: &tables,
            enums: &[],
            domains: &[],
            composites: &[],
            views: &[],
            functions: &[],
            extensions: &[],
        };
        let with_enum_vec = vec![EnumType {
            schema: "public".into(),
            name: "order_status".into(),
            labels: vec!["new".into(), "shipped".into()],
        }];
        let with_enum = HashInput {
            enums: &with_enum_vec,
            ..no_enums
        };
        assert_ne!(
            compute_content_hash(&no_enums),
            compute_content_hash(&with_enum),
        );
    }

    #[test]
    fn content_hash_unchanged_when_index_stats_only_differ() {
        let bare = empty_table("public", "orders");
        let mut hot = bare.clone();
        hot.indexes[0].stats = Some(IndexStats {
            idx_scan: 500_000,
            idx_tup_read: 1_000_000,
            idx_tup_fetch: 750_000,
            size: 1_048_576,
            relpages: 128,
            reltuples: 1_000_000.0,
        });
        assert_eq!(
            compute_content_hash(&input_for(&[bare])),
            compute_content_hash(&input_for(&[hot])),
        );
    }

    #[test]
    fn content_hash_unchanged_when_column_stats_only_differ() {
        let bare = empty_table("public", "orders");
        let mut analyzed = bare.clone();
        analyzed.columns[0].stats = Some(ColumnStats {
            null_frac: Some(0.42),
            n_distinct: Some(10_000.0),
            most_common_vals: Some("{1,2,3}".into()),
            most_common_freqs: Some("{0.1,0.05,0.02}".into()),
            histogram_bounds: Some("{0,500,1000}".into()),
            correlation: Some(-0.9),
        });
        assert_eq!(
            compute_content_hash(&input_for(&[bare])),
            compute_content_hash(&input_for(&[analyzed])),
        );
    }
}
