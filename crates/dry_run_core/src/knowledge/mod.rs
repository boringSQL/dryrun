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
