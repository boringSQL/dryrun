mod changeset;

pub use changeset::{
    Change, ChangeKind, ColumnChange, DriftDirection, DriftEntry, DriftReport, DriftSummary,
    SchemaChangeset,
};

use crate::schema::SchemaSnapshot;

pub fn diff_schemas(from: &SchemaSnapshot, to: &SchemaSnapshot) -> SchemaChangeset {
    changeset::compute_changeset(from, to)
}

pub fn classify_drift(
    prod_snapshot: &SchemaSnapshot,
    local_snapshot: &SchemaSnapshot,
) -> DriftReport {
    let changeset = diff_schemas(prod_snapshot, local_snapshot);

    let entries: Vec<DriftEntry> = changeset
        .changes
        .into_iter()
        .map(|change| {
            let direction = match change.kind {
                ChangeKind::Added => DriftDirection::Ahead,
                ChangeKind::Removed => DriftDirection::Behind,
                ChangeKind::Modified => DriftDirection::Diverged,
            };
            DriftEntry { direction, change }
        })
        .collect();

    let summary = DriftSummary {
        ahead: entries
            .iter()
            .filter(|e| e.direction == DriftDirection::Ahead)
            .count(),
        behind: entries
            .iter()
            .filter(|e| e.direction == DriftDirection::Behind)
            .count(),
        diverged: entries
            .iter()
            .filter(|e| e.direction == DriftDirection::Diverged)
            .count(),
    };

    DriftReport {
        local_hash: local_snapshot.content_hash.clone(),
        snapshot_hash: prod_snapshot.content_hash.clone(),
        entries,
        summary,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::schema::Table;

    fn empty_snapshot(hash: &str) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: hash.into(),
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

    fn make_table(name: &str) -> Table {
        Table {
            oid: 0,
            schema: "public".into(),
            name: name.into(),
            columns: vec![],
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
    fn test_classify_drift_identical_schemas() {
        let prod = empty_snapshot("aaa");
        let local = empty_snapshot("bbb");
        let report = classify_drift(&prod, &local);
        assert!(report.entries.is_empty());
        assert_eq!(report.summary.ahead, 0);
        assert_eq!(report.summary.behind, 0);
        assert_eq!(report.summary.diverged, 0);
    }

    #[test]
    fn test_classify_drift_local_ahead() {
        let prod = empty_snapshot("aaa");
        let mut local = empty_snapshot("bbb");
        local.tables.push(make_table("new_feature"));

        let report = classify_drift(&prod, &local);
        assert_eq!(report.summary.ahead, 1);
        assert_eq!(report.summary.behind, 0);
        assert_eq!(report.entries[0].direction, DriftDirection::Ahead);
        assert_eq!(report.entries[0].change.name, "new_feature");
    }

    #[test]
    fn test_classify_drift_local_behind() {
        let mut prod = empty_snapshot("aaa");
        prod.tables.push(make_table("prod_only"));
        let local = empty_snapshot("bbb");

        let report = classify_drift(&prod, &local);
        assert_eq!(report.summary.behind, 1);
        assert_eq!(report.summary.ahead, 0);
        assert_eq!(report.entries[0].direction, DriftDirection::Behind);
        assert_eq!(report.entries[0].change.name, "prod_only");
    }

    #[test]
    fn test_classify_drift_mixed() {
        let mut prod = empty_snapshot("aaa");
        prod.tables.push(make_table("prod_only"));
        prod.tables.push(make_table("shared"));

        let mut local = empty_snapshot("bbb");
        local.tables.push(make_table("local_only"));
        let mut shared = make_table("shared");
        shared.columns.push(crate::schema::Column {
            name: "extra_col".into(),
            ordinal: 1,
            type_name: "text".into(),
            nullable: true,
            default: None,
            identity: None,
            generated: None,
            comment: None,
            statistics_target: None,
            stats: None,
        });
        local.tables.push(shared);

        let report = classify_drift(&prod, &local);
        assert_eq!(report.summary.ahead, 1); // local_only
        assert_eq!(report.summary.behind, 1); // prod_only
        assert_eq!(report.summary.diverged, 1); // shared (modified)
        assert_eq!(report.entries.len(), 3);
    }
}
