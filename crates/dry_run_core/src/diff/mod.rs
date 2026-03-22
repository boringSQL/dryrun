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
