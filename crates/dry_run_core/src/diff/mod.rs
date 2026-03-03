mod changeset;

pub use changeset::{Change, ChangeKind, ColumnChange, SchemaChangeset};

use crate::schema::SchemaSnapshot;

pub fn diff_schemas(from: &SchemaSnapshot, to: &SchemaSnapshot) -> SchemaChangeset {
    changeset::compute_changeset(from, to)
}
