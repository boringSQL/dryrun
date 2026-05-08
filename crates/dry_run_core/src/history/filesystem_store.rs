use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::{Error, Result};
use crate::history::{
    DatabaseId, ProjectId, PutOutcome, SnapshotKey, SnapshotKind, SnapshotRef, SnapshotStore,
    SnapshotSummary, StoredSnapshot, TimeRange, parse_snapshot_filename, snapshot_path,
};
use crate::schema::{
    ActivityStatsSnapshot, HashInput, PlannerStatsSnapshot, SchemaSnapshot, compute_content_hash,
};

pub struct FilesystemStore {
    root: Arc<PathBuf>,
}

impl FilesystemStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
        }
    }

    pub fn list_keys(&self) -> Result<Vec<SnapshotKey>> {
        list_keys_sync(&self.root)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Bundle {
    schema: SchemaSnapshot,
    #[serde(default)]
    planner: Option<PlannerStatsSnapshot>,
    #[serde(default)]
    activity: BTreeMap<String, ActivityStatsSnapshot>,
}

#[async_trait]
impl SnapshotStore for FilesystemStore {
    async fn put(&self, key: &SnapshotKey, snap: &StoredSnapshot) -> Result<PutOutcome> {
        let root = self.root.clone();
        let key = key.clone();
        let snap = snap.clone();
        run_blocking(move || match snap {
            StoredSnapshot::Schema(s) => put_schema(&root, &key, s),
            StoredSnapshot::Planner(p) => put_planner(&root, &key, p),
            StoredSnapshot::Activity(a) => put_activity(&root, &key, a),
        })
        .await
    }

    async fn get(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        at: SnapshotRef,
    ) -> Result<StoredSnapshot> {
        let root = self.root.clone();
        let key = key.clone();
        let kind = kind.clone();
        run_blocking(move || get_kind(&root, &key, &kind, at)).await
    }

    async fn list(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        range: TimeRange,
    ) -> Result<Vec<SnapshotSummary>> {
        let root = self.root.clone();
        let key = key.clone();
        let kind = kind.clone();
        run_blocking(move || list_kind(&root, &key, &kind, range)).await
    }

    async fn delete_before(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        cutoff: DateTime<Utc>,
    ) -> Result<usize> {
        let root = self.root.clone();
        let key = key.clone();
        let kind = kind.clone();
        run_blocking(move || delete_before(&root, &key, &kind, cutoff)).await
    }

    async fn list_kinds(&self, key: &SnapshotKey) -> Result<Vec<SnapshotKind>> {
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || list_kinds_sync(&root, &key)).await
    }
}

fn put_schema(root: &Path, key: &SnapshotKey, snap: SchemaSnapshot) -> Result<PutOutcome> {
    let dir = stream_dir(root, key);
    if let Some(latest) = read_latest_hash(&dir)?
        && latest == snap.content_hash
    {
        debug!(hash = %snap.content_hash, "schema unchanged, skipping put");
        return Ok(PutOutcome::Deduped);
    }

    let path = snapshot_path(root, key, snap.timestamp, &snap.content_hash);
    let bundle = Bundle {
        schema: snap.clone(),
        planner: None,
        activity: BTreeMap::new(),
    };
    write_bundle(&path, &bundle)?;

    info!(
        hash = %snap.content_hash,
        project = %key.project_id.0,
        database = %key.database_id.0,
        "snapshot put (fs)",
    );
    Ok(PutOutcome::Inserted)
}

fn put_planner(root: &Path, key: &SnapshotKey, snap: PlannerStatsSnapshot) -> Result<PutOutcome> {
    let dir = stream_dir(root, key);
    let (path, mut bundle) =
        find_bundle_by_schema_hash(&dir, &snap.schema_ref_hash)?.ok_or_else(|| {
            Error::History(format!(
                "FilesystemStore: planner orphan — no schema bundle for ref {}",
                snap.schema_ref_hash
            ))
        })?;

    if let Some(existing) = &bundle.planner
        && existing.content_hash == snap.content_hash
    {
        return Ok(PutOutcome::Deduped);
    }

    bundle.planner = Some(snap);
    write_bundle(&path, &bundle)?;
    Ok(PutOutcome::Inserted)
}

fn put_activity(root: &Path, key: &SnapshotKey, snap: ActivityStatsSnapshot) -> Result<PutOutcome> {
    let dir = stream_dir(root, key);
    let (path, mut bundle) =
        find_bundle_by_schema_hash(&dir, &snap.schema_ref_hash)?.ok_or_else(|| {
            Error::History(format!(
                "FilesystemStore: activity orphan — no schema bundle for ref {}",
                snap.schema_ref_hash
            ))
        })?;

    let label = snap.node.label.clone();
    if let Some(existing) = bundle.activity.get(&label)
        && existing.content_hash == snap.content_hash
    {
        return Ok(PutOutcome::Deduped);
    }

    bundle.activity.insert(label, snap);
    write_bundle(&path, &bundle)?;
    Ok(PutOutcome::Inserted)
}

fn get_kind(
    root: &Path,
    key: &SnapshotKey,
    kind: &SnapshotKind,
    at: SnapshotRef,
) -> Result<StoredSnapshot> {
    let dir = stream_dir(root, key);
    let entries = read_stream_entries(&dir)?;

    match kind {
        SnapshotKind::Schema => {
            let chosen = match &at {
                SnapshotRef::Latest => entries.into_iter().max_by_key(|(ts, _, _)| *ts),
                SnapshotRef::At(target) => entries
                    .into_iter()
                    .filter(|(ts, _, _)| *ts <= *target)
                    .max_by_key(|(ts, _, _)| *ts),
                SnapshotRef::Hash(h) => entries.into_iter().find(|(_, hash, _)| hash == h),
            };
            let (_, _, path) = chosen.ok_or_else(|| not_found_err("schema", &at))?;
            let bundle = read_bundle(&path)?;
            Ok(StoredSnapshot::Schema(bundle.schema))
        }
        SnapshotKind::Planner => {
            let mut bundles: Vec<(DateTime<Utc>, Bundle)> = entries
                .into_iter()
                .filter_map(|(ts, _, p)| read_bundle(&p).ok().map(|b| (ts, b)))
                .filter(|(_, b)| b.planner.is_some())
                .collect();
            bundles.sort_by_key(|(ts, _)| std::cmp::Reverse(*ts));
            let chosen = match &at {
                SnapshotRef::Latest => bundles.into_iter().next(),
                SnapshotRef::At(target) => bundles.into_iter().find(|(ts, _)| *ts <= *target),
                SnapshotRef::Hash(h) => bundles
                    .into_iter()
                    .find(|(_, b)| b.planner.as_ref().map(|p| &p.content_hash) == Some(h)),
            };
            let (_, bundle) = chosen.ok_or_else(|| not_found_err("planner", &at))?;
            Ok(StoredSnapshot::Planner(bundle.planner.expect("filtered")))
        }
        SnapshotKind::Activity { node_label } => {
            let mut bundles: Vec<(DateTime<Utc>, Bundle)> = entries
                .into_iter()
                .filter_map(|(ts, _, p)| read_bundle(&p).ok().map(|b| (ts, b)))
                .filter(|(_, b)| b.activity.contains_key(node_label))
                .collect();
            bundles.sort_by_key(|(ts, _)| std::cmp::Reverse(*ts));
            let chosen = match &at {
                SnapshotRef::Latest => bundles.into_iter().next(),
                SnapshotRef::At(target) => bundles.into_iter().find(|(ts, _)| *ts <= *target),
                SnapshotRef::Hash(h) => bundles
                    .into_iter()
                    .find(|(_, b)| b.activity.get(node_label).map(|a| &a.content_hash) == Some(h)),
            };
            let (_, mut bundle) = chosen.ok_or_else(|| not_found_err("activity", &at))?;
            let act = bundle.activity.remove(node_label).expect("filtered above");
            Ok(StoredSnapshot::Activity(act))
        }
    }
}

fn list_kind(
    root: &Path,
    key: &SnapshotKey,
    kind: &SnapshotKind,
    range: TimeRange,
) -> Result<Vec<SnapshotSummary>> {
    let dir = stream_dir(root, key);
    let entries = read_stream_entries(&dir)?;

    let mut out: Vec<SnapshotSummary> = Vec::new();
    for (_schema_ts, _schema_hash, path) in entries {
        let bundle = match read_bundle(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(s) = bundle_summary_for_kind(&bundle, key, kind) {
            if range.from.is_none_or(|f| s.timestamp >= f)
                && range.to.is_none_or(|t| s.timestamp < t)
            {
                out.push(s);
            }
        }
    }
    out.sort_by_key(|s| std::cmp::Reverse(s.timestamp));
    Ok(out)
}

fn delete_before(
    root: &Path,
    key: &SnapshotKey,
    kind: &SnapshotKind,
    cutoff: DateTime<Utc>,
) -> Result<usize> {
    let dir = stream_dir(root, key);
    let entries = read_stream_entries(&dir)?;
    let mut affected = 0usize;

    match kind {
        SnapshotKind::Schema => {
            for (_ts, _h, path) in entries {
                let bundle = match read_bundle(&path) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                if bundle.schema.timestamp < cutoff {
                    std::fs::remove_file(&path)
                        .map_err(|e| Error::History(format!("remove {}: {e}", path.display())))?;
                    affected += 1;
                }
            }
        }
        SnapshotKind::Planner => {
            for (_ts, _h, path) in entries {
                let mut bundle = match read_bundle(&path) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let drop = bundle
                    .planner
                    .as_ref()
                    .is_some_and(|p| p.timestamp < cutoff);
                if drop {
                    bundle.planner = None;
                    write_bundle(&path, &bundle)?;
                    affected += 1;
                }
            }
        }
        SnapshotKind::Activity { node_label } => {
            for (_ts, _h, path) in entries {
                let mut bundle = match read_bundle(&path) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let drop = bundle
                    .activity
                    .get(node_label)
                    .is_some_and(|a| a.timestamp < cutoff);
                if drop {
                    bundle.activity.remove(node_label);
                    write_bundle(&path, &bundle)?;
                    affected += 1;
                }
            }
        }
    }
    Ok(affected)
}

fn list_kinds_sync(root: &Path, key: &SnapshotKey) -> Result<Vec<SnapshotKind>> {
    let dir = stream_dir(root, key);
    let entries = read_stream_entries(&dir)?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut has_schema = false;
    let mut has_planner = false;
    let mut activity_labels: std::collections::BTreeSet<String> = Default::default();

    for (_ts, _h, path) in entries {
        let bundle = match read_bundle(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        has_schema = true;
        if bundle.planner.is_some() {
            has_planner = true;
        }
        for label in bundle.activity.keys() {
            activity_labels.insert(label.clone());
        }
    }

    let mut out = Vec::new();
    if has_schema {
        out.push(SnapshotKind::Schema);
    }
    if has_planner {
        out.push(SnapshotKind::Planner);
    }
    for label in activity_labels {
        out.push(SnapshotKind::Activity { node_label: label });
    }
    Ok(out)
}

fn bundle_summary_for_kind(
    bundle: &Bundle,
    key: &SnapshotKey,
    kind: &SnapshotKind,
) -> Option<SnapshotSummary> {
    let project = Some(key.project_id.0.clone());
    let database = Some(key.database_id.0.clone());
    let db_name = key.database_id.0.clone();
    match kind {
        SnapshotKind::Schema => Some(SnapshotSummary {
            id: 0,
            kind: SnapshotKind::Schema,
            timestamp: bundle.schema.timestamp,
            content_hash: bundle.schema.content_hash.clone(),
            schema_ref_hash: None,
            database: db_name,
            project_id: project,
            database_id: database,
        }),
        SnapshotKind::Planner => bundle.planner.as_ref().map(|p| SnapshotSummary {
            id: 0,
            kind: SnapshotKind::Planner,
            timestamp: p.timestamp,
            content_hash: p.content_hash.clone(),
            schema_ref_hash: Some(bundle.schema.content_hash.clone()),
            database: db_name,
            project_id: project,
            database_id: database,
        }),
        SnapshotKind::Activity { node_label } => {
            bundle.activity.get(node_label).map(|a| SnapshotSummary {
                id: 0,
                kind: SnapshotKind::Activity {
                    node_label: node_label.clone(),
                },
                timestamp: a.timestamp,
                content_hash: a.content_hash.clone(),
                schema_ref_hash: Some(bundle.schema.content_hash.clone()),
                database: db_name,
                project_id: project,
                database_id: database,
            })
        }
    }
}

fn find_bundle_by_schema_hash(dir: &Path, schema_hash: &str) -> Result<Option<(PathBuf, Bundle)>> {
    for (_, _, path) in read_stream_entries(dir)? {
        let bundle = match read_bundle(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bundle.schema.content_hash == schema_hash {
            return Ok(Some((path, bundle)));
        }
    }
    Ok(None)
}

fn read_bundle(path: &Path) -> Result<Bundle> {
    let bytes =
        std::fs::read(path).map_err(|e| Error::History(format!("read {}: {e}", path.display())))?;
    let json = zstd::decode_all(bytes.as_slice()).map_err(|e| {
        Error::History(format!(
            "corrupt snapshot {}: zstd decode: {e}",
            path.display()
        ))
    })?;
    let bundle = if let Ok(b) = serde_json::from_slice::<Bundle>(&json) {
        b
    } else {
        // handle original base snapshot
        // TODO: remove in about month time
        let schema: SchemaSnapshot = serde_json::from_slice(&json).map_err(|e| {
            Error::History(format!("corrupt snapshot {}: JSON: {e}", path.display()))
        })?;
        Bundle {
            schema,
            planner: None,
            activity: BTreeMap::new(),
        }
    };

    verify_bundle_hash(path, &bundle)?;
    Ok(bundle)
}

// filename hash must match recomputed schema content_hash
fn verify_bundle_hash(path: &Path, bundle: &Bundle) -> Result<()> {
    let fname = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| Error::History(format!("non-utf8 filename: {}", path.display())))?;
    let (_, expected) = parse_snapshot_filename(fname).ok_or_else(|| {
        Error::History(format!(
            "corrupt snapshot {}: filename does not match {{ts}}-{{hash}}.json.zst",
            path.display()
        ))
    })?;

    if bundle.schema.content_hash != expected {
        return Err(Error::History(format!(
            "corrupt snapshot {}: filename hash {} != stored schema.content_hash {}",
            path.display(),
            expected,
            bundle.schema.content_hash,
        )));
    }

    let recomputed = compute_content_hash(&HashInput {
        pg_version: &bundle.schema.pg_version,
        tables: &bundle.schema.tables,
        enums: &bundle.schema.enums,
        domains: &bundle.schema.domains,
        composites: &bundle.schema.composites,
        views: &bundle.schema.views,
        functions: &bundle.schema.functions,
        extensions: &bundle.schema.extensions,
    });
    if recomputed != expected {
        return Err(Error::History(format!(
            "corrupt snapshot {}: filename hash {} != recomputed schema hash {}",
            path.display(),
            expected,
            recomputed,
        )));
    }
    Ok(())
}

fn write_bundle(path: &Path, bundle: &Bundle) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::History(format!("create_dir_all {}: {e}", parent.display())))?;
    }
    // unique tmp path so concurrent same-hash writers don't collide
    let tmp = unique_tmp_path(path);
    let json = serde_json::to_vec(bundle)
        .map_err(|e| Error::History(format!("cannot serialize bundle: {e}")))?;
    let compressed = zstd::encode_all(json.as_slice(), 3)
        .map_err(|e| Error::History(format!("zstd encode: {e}")))?;
    std::fs::write(&tmp, compressed)
        .map_err(|e| Error::History(format!("write {}: {e}", tmp.display())))?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(Error::History(format!("rename to {}: {e}", path.display())));
    }
    Ok(())
}

fn unique_tmp_path(path: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let suffix = format!("zst.{pid}.{n}.tmp");
    path.with_extension(suffix)
}

fn not_found_err(kind: &str, at: &SnapshotRef) -> Error {
    let detail = match at {
        SnapshotRef::Latest => "latest".to_string(),
        SnapshotRef::At(ts) => format!("at-or-before {ts}"),
        SnapshotRef::Hash(h) => format!("hash {h}"),
    };
    Error::History(format!("{kind} snapshot not found ({detail})"))
}

fn stream_dir(root: &Path, key: &SnapshotKey) -> PathBuf {
    root.join(&key.project_id.0).join(&key.database_id.0)
}

fn read_stream_entries(dir: &Path) -> Result<Vec<(DateTime<Utc>, String, PathBuf)>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir)
        .map_err(|e| Error::History(format!("read_dir {}: {e}", dir.display())))?
    {
        let entry = entry.map_err(|e| Error::History(format!("dirent: {e}")))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some((ts, hash)) = parse_snapshot_filename(name) {
            entries.push((ts, hash, path));
        }
    }
    Ok(entries)
}

fn read_latest_hash(dir: &Path) -> Result<Option<String>> {
    Ok(read_stream_entries(dir)?
        .into_iter()
        .max_by_key(|(ts, _, _)| *ts)
        .map(|(_, hash, _)| hash))
}

fn list_keys_sync(root: &Path) -> Result<Vec<SnapshotKey>> {
    let mut keys = Vec::new();
    if !root.is_dir() {
        return Ok(keys);
    }
    for proj_entry in std::fs::read_dir(root)
        .map_err(|e| Error::History(format!("read_dir {}: {e}", root.display())))?
    {
        let proj_entry = proj_entry.map_err(|e| Error::History(format!("dirent: {e}")))?;
        let proj_path = proj_entry.path();
        if !proj_path.is_dir() {
            continue;
        }
        let Some(project_id) = proj_path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        for db_entry in std::fs::read_dir(&proj_path)
            .map_err(|e| Error::History(format!("read_dir {}: {e}", proj_path.display())))?
        {
            let db_entry = db_entry.map_err(|e| Error::History(format!("dirent: {e}")))?;
            let db_path = db_entry.path();
            if !db_path.is_dir() {
                continue;
            }
            let Some(database_id) = db_path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            keys.push(SnapshotKey {
                project_id: ProjectId(project_id.to_string()),
                database_id: DatabaseId(database_id.to_string()),
            });
        }
    }
    keys.sort_by(|a, b| {
        a.project_id
            .0
            .cmp(&b.project_id.0)
            .then_with(|| a.database_id.0.cmp(&b.database_id.0))
    });
    Ok(keys)
}

async fn run_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::History(format!("blocking task failed: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::test_fixtures;
    use tempfile::TempDir;

    fn make_schema(hash: &str) -> SchemaSnapshot {
        test_fixtures::make_snap(hash, "auth")
    }

    fn make_planner(schema_ref: &str, hash: &str) -> PlannerStatsSnapshot {
        test_fixtures::make_planner(schema_ref, "auth", hash)
    }

    fn make_activity(schema_ref: &str, label: &str, hash: &str) -> ActivityStatsSnapshot {
        test_fixtures::make_activity(schema_ref, "auth", label, hash)
    }

    fn key() -> SnapshotKey {
        test_fixtures::key("p", "auth")
    }

    fn temp_store() -> (TempDir, FilesystemStore) {
        let dir = TempDir::new().unwrap();
        let store = FilesystemStore::new(dir.path().to_path_buf());
        (dir, store)
    }

    #[tokio::test]
    async fn put_schema_round_trips_via_trait() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("h1")).await.unwrap();

        let got = store.get_schema(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(got.content_hash, "h1");
    }

    #[tokio::test]
    async fn put_schema_dedupes_on_same_content_hash() {
        let (_dir, store) = temp_store();
        let k = key();
        let s = make_schema("h1");
        assert_eq!(
            store.put_schema(&k, &s).await.unwrap(),
            PutOutcome::Inserted
        );
        assert_eq!(store.put_schema(&k, &s).await.unwrap(), PutOutcome::Deduped);
    }

    #[tokio::test]
    async fn bundle_round_trips_all_three_kinds() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("sh")).await.unwrap();
        store
            .put_planner_stats(&k, &make_planner("sh", "ph"))
            .await
            .unwrap();
        store
            .put_activity_stats(&k, &make_activity("sh", "primary", "ah"))
            .await
            .unwrap();

        let s = store.get_schema(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(s.content_hash, "sh");

        let p = store
            .get(&k, &SnapshotKind::Planner, SnapshotRef::Latest)
            .await
            .unwrap()
            .into_planner()
            .unwrap();
        assert_eq!(p.content_hash, "ph");

        let a = store
            .get(
                &k,
                &SnapshotKind::Activity {
                    node_label: "primary".into(),
                },
                SnapshotRef::Latest,
            )
            .await
            .unwrap()
            .into_activity()
            .unwrap();
        assert_eq!(a.content_hash, "ah");
        assert_eq!(a.node.label, "primary");
    }

    #[tokio::test]
    async fn put_planner_without_schema_errors() {
        let (_dir, store) = temp_store();
        let k = key();
        let err = store
            .put_planner_stats(&k, &make_planner("missing", "ph"))
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("orphan"), "expected orphan error, got: {msg}");
    }

    #[tokio::test]
    async fn put_planner_dedupes_on_same_content_hash() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("sh")).await.unwrap();
        let p = make_planner("sh", "ph");
        assert_eq!(
            store.put_planner_stats(&k, &p).await.unwrap(),
            PutOutcome::Inserted
        );
        assert_eq!(
            store.put_planner_stats(&k, &p).await.unwrap(),
            PutOutcome::Deduped
        );
    }

    #[tokio::test]
    async fn put_activity_upserts_per_node_label() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("sh")).await.unwrap();
        store
            .put_activity_stats(&k, &make_activity("sh", "primary", "a1"))
            .await
            .unwrap();
        store
            .put_activity_stats(&k, &make_activity("sh", "standby", "b1"))
            .await
            .unwrap();
        // overwrite primary
        store
            .put_activity_stats(&k, &make_activity("sh", "primary", "a2"))
            .await
            .unwrap();

        let primary = store
            .get(
                &k,
                &SnapshotKind::Activity {
                    node_label: "primary".into(),
                },
                SnapshotRef::Latest,
            )
            .await
            .unwrap()
            .into_activity()
            .unwrap();
        assert_eq!(primary.content_hash, "a2");

        let standby = store
            .get(
                &k,
                &SnapshotKind::Activity {
                    node_label: "standby".into(),
                },
                SnapshotRef::Latest,
            )
            .await
            .unwrap()
            .into_activity()
            .unwrap();
        assert_eq!(standby.content_hash, "b1");
    }

    #[tokio::test]
    async fn list_planner_returns_only_bundles_with_planner() {
        let (_dir, store) = temp_store();
        let k = key();
        // bundle #1: schema + planner
        store.put_schema(&k, &make_schema("sh1")).await.unwrap();
        store
            .put_planner_stats(&k, &make_planner("sh1", "ph1"))
            .await
            .unwrap();
        // bundle #2: schema only
        store.put_schema(&k, &make_schema("sh2")).await.unwrap();

        let schemas = store
            .list(&k, &SnapshotKind::Schema, TimeRange::default())
            .await
            .unwrap();
        assert_eq!(schemas.len(), 2);

        let planners = store
            .list(&k, &SnapshotKind::Planner, TimeRange::default())
            .await
            .unwrap();
        assert_eq!(planners.len(), 1);
        assert_eq!(planners[0].content_hash, "ph1");
        assert_eq!(planners[0].schema_ref_hash.as_deref(), Some("sh1"));
    }

    #[tokio::test]
    async fn list_kinds_reports_distinct_node_labels() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("sh")).await.unwrap();
        store
            .put_planner_stats(&k, &make_planner("sh", "ph"))
            .await
            .unwrap();
        store
            .put_activity_stats(&k, &make_activity("sh", "primary", "a1"))
            .await
            .unwrap();
        store
            .put_activity_stats(&k, &make_activity("sh", "standby", "b1"))
            .await
            .unwrap();

        let kinds = store.list_kinds(&k).await.unwrap();
        assert!(kinds.contains(&SnapshotKind::Schema));
        assert!(kinds.contains(&SnapshotKind::Planner));
        assert!(kinds.contains(&SnapshotKind::Activity {
            node_label: "primary".into()
        }));
        assert!(kinds.contains(&SnapshotKind::Activity {
            node_label: "standby".into()
        }));
        assert_eq!(kinds.len(), 4);
    }

    #[tokio::test]
    async fn delete_before_planner_clears_field_keeps_schema() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("sh")).await.unwrap();
        store
            .put_planner_stats(&k, &make_planner("sh", "ph"))
            .await
            .unwrap();

        let cutoff = Utc::now() + chrono::Duration::seconds(60);
        let removed = store
            .delete_before(&k, &SnapshotKind::Planner, cutoff)
            .await
            .unwrap();
        assert_eq!(removed, 1);

        // schema still there
        let s = store.get_schema(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(s.content_hash, "sh");

        // planner gone
        let planners = store
            .list(&k, &SnapshotKind::Planner, TimeRange::default())
            .await
            .unwrap();
        assert!(planners.is_empty());
    }

    #[tokio::test]
    async fn delete_before_schema_removes_whole_bundle() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("sh")).await.unwrap();
        store
            .put_planner_stats(&k, &make_planner("sh", "ph"))
            .await
            .unwrap();

        let cutoff = Utc::now() + chrono::Duration::seconds(60);
        let removed = store
            .delete_before(&k, &SnapshotKind::Schema, cutoff)
            .await
            .unwrap();
        assert_eq!(removed, 1);

        let schemas = store
            .list(&k, &SnapshotKind::Schema, TimeRange::default())
            .await
            .unwrap();
        assert!(schemas.is_empty());
        let planners = store
            .list(&k, &SnapshotKind::Planner, TimeRange::default())
            .await
            .unwrap();
        assert!(planners.is_empty());
    }
}
