mod document;

pub use document::{KnowledgeCategory, KnowledgeDoc};

use crate::version::PgVersion;

static MIGRATION_SAFETY_DOCS: &[(&str, &str)] = &[
    (
        "add_column",
        include_str!("../../knowledge/migration_safety/add_column.md"),
    ),
    (
        "create_index",
        include_str!("../../knowledge/migration_safety/create_index.md"),
    ),
    (
        "add_foreign_key",
        include_str!("../../knowledge/migration_safety/add_foreign_key.md"),
    ),
    (
        "add_check_constraint",
        include_str!("../../knowledge/migration_safety/add_check_constraint.md"),
    ),
    (
        "alter_column_type",
        include_str!("../../knowledge/migration_safety/alter_column_type.md"),
    ),
    (
        "drop_column",
        include_str!("../../knowledge/migration_safety/drop_column.md"),
    ),
    (
        "rename",
        include_str!("../../knowledge/migration_safety/rename.md"),
    ),
    (
        "add_not_null",
        include_str!("../../knowledge/migration_safety/add_not_null.md"),
    ),
];

static INDEX_DECISION_DOCS: &[(&str, &str)] = &[
    (
        "btree",
        include_str!("../../knowledge/index_decisions/btree.md"),
    ),
    (
        "gin",
        include_str!("../../knowledge/index_decisions/gin.md"),
    ),
    (
        "gist",
        include_str!("../../knowledge/index_decisions/gist.md"),
    ),
    (
        "brin",
        include_str!("../../knowledge/index_decisions/brin.md"),
    ),
    (
        "partial",
        include_str!("../../knowledge/index_decisions/partial.md"),
    ),
    (
        "covering",
        include_str!("../../knowledge/index_decisions/covering.md"),
    ),
    (
        "composite",
        include_str!("../../knowledge/index_decisions/composite.md"),
    ),
];

static CONVENTIONS_DOCS: &[(&str, &str)] = &[
    (
        "naming",
        include_str!("../../knowledge/conventions/naming.md"),
    ),
    (
        "primary_keys",
        include_str!("../../knowledge/conventions/primary_keys.md"),
    ),
    (
        "types",
        include_str!("../../knowledge/conventions/types.md"),
    ),
    (
        "constraints",
        include_str!("../../knowledge/conventions/constraints.md"),
    ),
    (
        "timestamps",
        include_str!("../../knowledge/conventions/timestamps.md"),
    ),
    (
        "design_patterns",
        include_str!("../../knowledge/conventions/design_patterns.md"),
    ),
    (
        "anti_patterns",
        include_str!("../../knowledge/conventions/anti_patterns.md"),
    ),
];

pub fn migration_safety_docs() -> Vec<KnowledgeDoc> {
    MIGRATION_SAFETY_DOCS
        .iter()
        .filter_map(|(name, content)| {
            KnowledgeDoc::parse(name, KnowledgeCategory::MigrationSafety, content)
        })
        .collect()
}

pub fn index_decision_docs() -> Vec<KnowledgeDoc> {
    INDEX_DECISION_DOCS
        .iter()
        .filter_map(|(name, content)| {
            KnowledgeDoc::parse(name, KnowledgeCategory::IndexDecisions, content)
        })
        .collect()
}

pub fn conventions_docs() -> Vec<KnowledgeDoc> {
    CONVENTIONS_DOCS
        .iter()
        .filter_map(|(name, content)| {
            KnowledgeDoc::parse(name, KnowledgeCategory::SchemaConventions, content)
        })
        .collect()
}

pub fn all_docs() -> Vec<KnowledgeDoc> {
    let mut docs = migration_safety_docs();
    docs.extend(index_decision_docs());
    docs.extend(conventions_docs());
    docs
}

pub fn lookup_migration_safety(
    operation: &str,
    pg_version: Option<&PgVersion>,
) -> Vec<KnowledgeDoc> {
    lookup_by_keywords(&migration_safety_docs(), operation, pg_version)
}

pub fn lookup_index_decisions(query: &str, pg_version: Option<&PgVersion>) -> Vec<KnowledgeDoc> {
    lookup_by_keywords(&index_decision_docs(), query, pg_version)
}

pub fn lookup_conventions(query: &str, pg_version: Option<&PgVersion>) -> Vec<KnowledgeDoc> {
    lookup_by_keywords(&conventions_docs(), query, pg_version)
}

fn lookup_by_keywords(
    docs: &[KnowledgeDoc],
    query: &str,
    pg_version: Option<&PgVersion>,
) -> Vec<KnowledgeDoc> {
    let q_lower = query.to_lowercase();
    docs.iter()
        .filter(|doc| doc.keywords.iter().any(|k| q_lower.contains(k)))
        .filter(|doc| {
            pg_version
                .map(|ver| doc.applies_to_version(ver))
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_migration_docs_parse() {
        let docs = migration_safety_docs();
        assert_eq!(docs.len(), 8, "all 8 migration safety docs should parse");
        for doc in &docs {
            assert!(!doc.name.is_empty());
            assert!(!doc.body.is_empty());
            assert!(!doc.keywords.is_empty());
        }
    }

    #[test]
    fn all_index_docs_parse() {
        let docs = index_decision_docs();
        assert_eq!(docs.len(), 7, "all 7 index decision docs should parse");
        for doc in &docs {
            assert!(!doc.name.is_empty());
            assert!(!doc.body.is_empty());
            assert!(!doc.keywords.is_empty());
        }
    }

    #[test]
    fn all_conventions_docs_parse() {
        let docs = conventions_docs();
        assert_eq!(docs.len(), 7, "all 7 convention docs should parse");
        for doc in &docs {
            assert!(!doc.name.is_empty());
            assert!(!doc.body.is_empty());
            assert!(!doc.keywords.is_empty());
        }
    }

    #[test]
    fn total_doc_count() {
        assert_eq!(all_docs().len(), 22);
    }

    #[test]
    fn lookup_naming_convention() {
        let docs = lookup_conventions("snake_case naming", None);
        assert!(!docs.is_empty(), "should find naming doc");
        assert!(docs.iter().any(|d| d.name == "naming"));
    }

    #[test]
    fn lookup_primary_key_convention() {
        let docs = lookup_conventions("primary key identity", None);
        assert!(!docs.is_empty(), "should find primary_keys doc");
    }

    #[test]
    fn lookup_timestamp_convention() {
        let docs = lookup_conventions("created_at timestamp", None);
        assert!(!docs.is_empty(), "should find timestamps doc");
    }

    #[test]
    fn lookup_add_column() {
        let docs = lookup_migration_safety("ADD COLUMN", None);
        assert!(!docs.is_empty(), "should find add_column doc");
        assert!(docs.iter().any(|d| d.name == "add_column"));
    }

    #[test]
    fn lookup_create_index() {
        let docs = lookup_migration_safety("CREATE INDEX", None);
        assert!(!docs.is_empty(), "should find create_index doc");
    }

    #[test]
    fn lookup_with_version() {
        let pg14 = PgVersion {
            major: 14,
            minor: 0,
            patch: 0,
        };
        let docs = lookup_migration_safety("ADD COLUMN DEFAULT", Some(&pg14));
        assert!(!docs.is_empty());

        let pg11 = PgVersion {
            major: 11,
            minor: 0,
            patch: 0,
        };
        let docs11 = lookup_migration_safety("ADD COLUMN DEFAULT", Some(&pg11));
        assert!(!docs11.is_empty());
    }

    #[test]
    fn lookup_not_null() {
        let docs = lookup_migration_safety("ADD NOT NULL", None);
        assert!(!docs.is_empty());
    }

    #[test]
    fn lookup_btree() {
        let docs = lookup_index_decisions("btree index", None);
        assert!(!docs.is_empty());
        assert!(docs.iter().any(|d| d.name == "btree"));
    }

    #[test]
    fn lookup_gin_jsonb() {
        let docs = lookup_index_decisions("jsonb containment", None);
        assert!(!docs.is_empty());
        assert!(docs.iter().any(|d| d.name == "gin"));
    }

    #[test]
    fn lookup_partial_index() {
        let docs = lookup_index_decisions("partial index WHERE clause", None);
        assert!(!docs.is_empty());
        assert!(docs.iter().any(|d| d.name == "partial"));
    }

    #[test]
    fn lookup_covering_include() {
        let docs = lookup_index_decisions("INCLUDE covering index", None);
        assert!(!docs.is_empty());
        assert!(docs.iter().any(|d| d.name == "covering"));
    }

    #[test]
    fn lookup_brin_correlation() {
        let docs = lookup_index_decisions("BRIN time series correlation", None);
        assert!(!docs.is_empty());
    }

    #[test]
    fn lookup_composite() {
        let docs = lookup_index_decisions("composite multi-column index order", None);
        assert!(!docs.is_empty());
    }

    #[test]
    fn lookup_gist_spatial() {
        let docs = lookup_index_decisions("GiST geometry spatial", None);
        assert!(!docs.is_empty());
    }
}
