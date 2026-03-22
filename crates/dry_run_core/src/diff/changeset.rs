use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::schema::{Column, Function, SchemaSnapshot, Table, View};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeset {
    pub from_hash: String,
    pub to_hash: String,
    pub from_timestamp: String,
    pub to_timestamp: String,
    pub changes: Vec<Change>,
}

impl SchemaChangeset {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub kind: ChangeKind,
    pub object_type: String,
    pub schema: Option<String>,
    pub name: String,
    pub details: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Added,
    Removed,
    Modified,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChange {
    pub column: String,
    pub field: String,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftDirection {
    Ahead,
    Behind,
    Diverged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftEntry {
    pub direction: DriftDirection,
    #[serde(flatten)]
    pub change: Change,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftReport {
    pub local_hash: String,
    pub snapshot_hash: String,
    pub entries: Vec<DriftEntry>,
    pub summary: DriftSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftSummary {
    pub ahead: usize,
    pub behind: usize,
    pub diverged: usize,
}

pub fn compute_changeset(from: &SchemaSnapshot, to: &SchemaSnapshot) -> SchemaChangeset {
    let mut changes = Vec::new();

    diff_tables(&from.tables, &to.tables, &mut changes);
    diff_views(&from.views, &to.views, &mut changes);
    diff_functions(&from.functions, &to.functions, &mut changes);
    diff_named("enum", &from.enums, &to.enums, &mut changes, |e| {
        format!("{}.{}", e.schema, e.name)
    });
    diff_named("domain", &from.domains, &to.domains, &mut changes, |d| {
        format!("{}.{}", d.schema, d.name)
    });
    diff_named(
        "composite_type",
        &from.composites,
        &to.composites,
        &mut changes,
        |c| format!("{}.{}", c.schema, c.name),
    );
    diff_named(
        "extension",
        &from.extensions,
        &to.extensions,
        &mut changes,
        |e| e.name.clone(),
    );

    SchemaChangeset {
        from_hash: from.content_hash.clone(),
        to_hash: to.content_hash.clone(),
        from_timestamp: from.timestamp.to_rfc3339(),
        to_timestamp: to.timestamp.to_rfc3339(),
        changes,
    }
}

// table diffing

fn diff_tables(from: &[Table], to: &[Table], changes: &mut Vec<Change>) {
    let from_map: HashMap<(&str, &str), &Table> = from
        .iter()
        .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
        .collect();
    let to_map: HashMap<(&str, &str), &Table> = to
        .iter()
        .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
        .collect();

    for (key, table) in &to_map {
        if !from_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Added,
                object_type: "table".into(),
                schema: Some(table.schema.clone()),
                name: table.name.clone(),
                details: vec![format!("{} columns", table.columns.len())],
            });
        }
    }

    for (key, table) in &from_map {
        if !to_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Removed,
                object_type: "table".into(),
                schema: Some(table.schema.clone()),
                name: table.name.clone(),
                details: vec![],
            });
        }
    }

    for (key, old) in &from_map {
        if let Some(new) = to_map.get(key) {
            let details = diff_table_details(old, new);
            if !details.is_empty() {
                changes.push(Change {
                    kind: ChangeKind::Modified,
                    object_type: "table".into(),
                    schema: Some(old.schema.clone()),
                    name: old.name.clone(),
                    details,
                });
            }
        }
    }
}

fn diff_table_details(old: &Table, new: &Table) -> Vec<String> {
    let mut details = Vec::new();

    let old_cols: HashMap<&str, &Column> =
        old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_cols: HashMap<&str, &Column> =
        new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    for (name, col) in &new_cols {
        if !old_cols.contains_key(name) {
            details.push(format!("column added: {name} ({})", col.type_name));
        }
    }
    for name in old_cols.keys() {
        if !new_cols.contains_key(name) {
            details.push(format!("column removed: {name}"));
        }
    }
    for (name, old_col) in &old_cols {
        if let Some(new_col) = new_cols.get(name) {
            if old_col.type_name != new_col.type_name {
                details.push(format!(
                    "column {name}: type changed {} -> {}",
                    old_col.type_name, new_col.type_name
                ));
            }
            if old_col.nullable != new_col.nullable {
                let change = if new_col.nullable {
                    "NOT NULL removed"
                } else {
                    "NOT NULL added"
                };
                details.push(format!("column {name}: {change}"));
            }
            if old_col.default != new_col.default {
                details.push(format!(
                    "column {name}: default changed {:?} -> {:?}",
                    old_col.default, new_col.default
                ));
            }
            if old_col.comment != new_col.comment {
                details.push(format!(
                    "column {name}: comment changed {:?} -> {:?}",
                    old_col.comment, new_col.comment
                ));
            }
        }
    }

    diff_named_items(
        "constraint",
        &old.constraints,
        &new.constraints,
        &mut details,
        |c| c.name.as_str(),
    );

    diff_named_items("index", &old.indexes, &new.indexes, &mut details, |i| {
        i.name.as_str()
    });

    if old.comment != new.comment {
        details.push(format!(
            "comment changed: {:?} -> {:?}",
            old.comment, new.comment
        ));
    }

    if old.rls_enabled != new.rls_enabled {
        let state = if new.rls_enabled {
            "enabled"
        } else {
            "disabled"
        };
        details.push(format!("RLS {state}"));
    }

    details
}

fn diff_named_items<T>(
    label: &str,
    old: &[T],
    new: &[T],
    details: &mut Vec<String>,
    name_fn: fn(&T) -> &str,
) {
    let old_names: std::collections::HashSet<&str> = old.iter().map(name_fn).collect();
    let new_names: std::collections::HashSet<&str> = new.iter().map(name_fn).collect();

    for name in &new_names {
        if !old_names.contains(name) {
            details.push(format!("{label} added: {name}"));
        }
    }
    for name in &old_names {
        if !new_names.contains(name) {
            details.push(format!("{label} removed: {name}"));
        }
    }
}

// view diffing

fn diff_views(from: &[View], to: &[View], changes: &mut Vec<Change>) {
    let from_map: HashMap<(&str, &str), &View> = from
        .iter()
        .map(|v| ((v.schema.as_str(), v.name.as_str()), v))
        .collect();
    let to_map: HashMap<(&str, &str), &View> = to
        .iter()
        .map(|v| ((v.schema.as_str(), v.name.as_str()), v))
        .collect();

    for (key, view) in &to_map {
        if !from_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Added,
                object_type: "view".into(),
                schema: Some(view.schema.clone()),
                name: view.name.clone(),
                details: vec![],
            });
        }
    }
    for (key, view) in &from_map {
        if !to_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Removed,
                object_type: "view".into(),
                schema: Some(view.schema.clone()),
                name: view.name.clone(),
                details: vec![],
            });
        }
    }
    for (key, old) in &from_map {
        if let Some(new) = to_map.get(key) {
            if old.definition != new.definition {
                changes.push(Change {
                    kind: ChangeKind::Modified,
                    object_type: "view".into(),
                    schema: Some(old.schema.clone()),
                    name: old.name.clone(),
                    details: vec!["definition changed".into()],
                });
            }
        }
    }
}

// function diffing

fn diff_functions(from: &[Function], to: &[Function], changes: &mut Vec<Change>) {
    fn key_fn(f: &Function) -> (String, String, String) {
        (f.schema.clone(), f.name.clone(), f.identity_args.clone())
    }

    let from_map: HashMap<_, &Function> = from.iter().map(|f| (key_fn(f), f)).collect();
    let to_map: HashMap<_, &Function> = to.iter().map(|f| (key_fn(f), f)).collect();

    for (key, func) in &to_map {
        if !from_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Added,
                object_type: "function".into(),
                schema: Some(func.schema.clone()),
                name: format!("{}({})", func.name, func.identity_args),
                details: vec![],
            });
        }
    }
    for (key, func) in &from_map {
        if !to_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Removed,
                object_type: "function".into(),
                schema: Some(func.schema.clone()),
                name: format!("{}({})", func.name, func.identity_args),
                details: vec![],
            });
        }
    }
    for (key, old) in &from_map {
        if let Some(new) = to_map.get(key) {
            let mut details = Vec::new();
            if old.return_type != new.return_type {
                details.push(format!(
                    "return type: {} -> {}",
                    old.return_type, new.return_type
                ));
            }
            if old.volatility != new.volatility {
                details.push(format!(
                    "volatility: {:?} -> {:?}",
                    old.volatility, new.volatility
                ));
            }
            if old.security_definer != new.security_definer {
                let state = if new.security_definer {
                    "SECURITY DEFINER added"
                } else {
                    "SECURITY DEFINER removed"
                };
                details.push(state.into());
            }
            if !details.is_empty() {
                changes.push(Change {
                    kind: ChangeKind::Modified,
                    object_type: "function".into(),
                    schema: Some(old.schema.clone()),
                    name: format!("{}({})", old.name, old.identity_args),
                    details,
                });
            }
        }
    }
}

// generic named-object diffing (enums, domains, composites, extensions)

fn diff_named<T: Serialize + PartialEq>(
    object_type: &str,
    from: &[T],
    to: &[T],
    changes: &mut Vec<Change>,
    key_fn: fn(&T) -> String,
) {
    let from_map: HashMap<String, &T> = from.iter().map(|x| (key_fn(x), x)).collect();
    let to_map: HashMap<String, &T> = to.iter().map(|x| (key_fn(x), x)).collect();

    for key in to_map.keys() {
        if !from_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Added,
                object_type: object_type.into(),
                schema: None,
                name: key.clone(),
                details: vec![],
            });
        }
    }
    for key in from_map.keys() {
        if !to_map.contains_key(key) {
            changes.push(Change {
                kind: ChangeKind::Removed,
                object_type: object_type.into(),
                schema: None,
                name: key.clone(),
                details: vec![],
            });
        }
    }
    for (key, old) in &from_map {
        if let Some(new) = to_map.get(key) {
            if old != new {
                changes.push(Change {
                    kind: ChangeKind::Modified,
                    object_type: object_type.into(),
                    schema: None,
                    name: key.clone(),
                    details: vec!["definition changed".into()],
                });
            }
        }
    }
}
