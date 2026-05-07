use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, info};

use crate::error::{Error, Result};
use crate::history::{
    DatabaseId, ProjectId, PutOutcome, SnapshotKey, SnapshotKind, SnapshotRef, SnapshotStore,
    SnapshotSummary, StoredSnapshot, TimeRange, parse_snapshot_filename, snapshot_path,
};
use crate::schema::SchemaSnapshot;

// schema-only for now
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

fn unsupported(kind: &SnapshotKind) -> Error {
    Error::History(format!(
        "FilesystemStore: only schema snapshots supported (got {})",
        kind.db_kind()
    ))
}

#[async_trait]
impl SnapshotStore for FilesystemStore {
    async fn put(&self, key: &SnapshotKey, snap: &StoredSnapshot) -> Result<PutOutcome> {
        let schema = match snap {
            StoredSnapshot::Schema(s) => s.clone(),
            other => return Err(unsupported(&other.kind())),
        };
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || put_schema(&root, &key, schema)).await
    }

    async fn get(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        at: SnapshotRef,
    ) -> Result<StoredSnapshot> {
        if !matches!(kind, SnapshotKind::Schema) {
            return Err(unsupported(kind));
        }
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || get_schema(&root, &key, at).map(StoredSnapshot::Schema)).await
    }

    async fn list(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        range: TimeRange,
    ) -> Result<Vec<SnapshotSummary>> {
        if !matches!(kind, SnapshotKind::Schema) {
            return Ok(Vec::new());
        }
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || list_schema(&root, &key, range)).await
    }

    async fn latest(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
    ) -> Result<Option<SnapshotSummary>> {
        Ok(self
            .list(key, kind, TimeRange::default())
            .await?
            .into_iter()
            .next())
    }

    async fn delete_before(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        cutoff: DateTime<Utc>,
    ) -> Result<usize> {
        if !matches!(kind, SnapshotKind::Schema) {
            return Err(unsupported(kind));
        }
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || delete_schema_before(&root, &key, cutoff)).await
    }

    async fn list_kinds(&self, key: &SnapshotKey) -> Result<Vec<SnapshotKind>> {
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || {
            let entries = read_stream_entries(&stream_dir(&root, &key))?;
            Ok(if entries.is_empty() {
                Vec::new()
            } else {
                vec![SnapshotKind::Schema]
            })
        })
        .await
    }
}

fn put_schema(root: &Path, key: &SnapshotKey, snap: SchemaSnapshot) -> Result<PutOutcome> {
    let stream_dir = stream_dir(root, key);
    if let Some(latest) = read_latest_hash(&stream_dir)?
        && latest == snap.content_hash
    {
        debug!(hash = %snap.content_hash, "schema unchanged, skipping put");
        return Ok(PutOutcome::Deduped);
    }

    let path = snapshot_path(root, key, snap.timestamp, &snap.content_hash);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::History(format!("create_dir_all {}: {e}", parent.display())))?;
    }

    let tmp = path.with_extension("zst.tmp");
    let json = serde_json::to_vec(&snap)
        .map_err(|e| Error::History(format!("cannot serialize snapshot: {e}")))?;
    let compressed = zstd::encode_all(json.as_slice(), 3)
        .map_err(|e| Error::History(format!("zstd encode: {e}")))?;
    std::fs::write(&tmp, compressed)
        .map_err(|e| Error::History(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, &path)
        .map_err(|e| Error::History(format!("rename to {}: {e}", path.display())))?;

    info!(
        hash = %snap.content_hash,
        project = %key.project_id.0,
        database = %key.database_id.0,
        "snapshot put (fs)",
    );
    Ok(PutOutcome::Inserted)
}

fn get_schema(root: &Path, key: &SnapshotKey, at: SnapshotRef) -> Result<SchemaSnapshot> {
    let entries = read_stream_entries(&stream_dir(root, key))?;
    let chosen = match &at {
        SnapshotRef::Latest => entries.into_iter().max_by_key(|(ts, _, _)| *ts),
        SnapshotRef::At(target) => entries
            .into_iter()
            .filter(|(ts, _, _)| *ts <= *target)
            .max_by_key(|(ts, _, _)| *ts),
        SnapshotRef::Hash(h) => entries.into_iter().find(|(_, hash, _)| hash == h),
    };

    let (_, _, path) = chosen.ok_or_else(|| {
        let detail = match &at {
            SnapshotRef::Latest => "latest".to_string(),
            SnapshotRef::At(ts) => format!("at-or-before {ts}"),
            SnapshotRef::Hash(h) => format!("hash {h}"),
        };
        Error::History(format!("snapshot not found ({detail})"))
    })?;

    let bytes = std::fs::read(&path)
        .map_err(|e| Error::History(format!("read {}: {e}", path.display())))?;
    let json = zstd::decode_all(bytes.as_slice())
        .map_err(|e| Error::History(format!("zstd decode: {e}")))?;
    serde_json::from_slice(&json).map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))
}

fn list_schema(root: &Path, key: &SnapshotKey, range: TimeRange) -> Result<Vec<SnapshotSummary>> {
    let entries = read_stream_entries(&stream_dir(root, key))?;
    let mut summaries: Vec<SnapshotSummary> = entries
        .into_iter()
        .filter(|(ts, _, _)| {
            range.from.is_none_or(|from| *ts >= from) && range.to.is_none_or(|to| *ts < to)
        })
        .map(|(ts, hash, _)| SnapshotSummary {
            id: 0,
            kind: SnapshotKind::Schema,
            timestamp: ts,
            content_hash: hash,
            schema_ref_hash: None,
            database: key.database_id.0.clone(),
            project_id: Some(key.project_id.0.clone()),
            database_id: Some(key.database_id.0.clone()),
        })
        .collect();
    summaries.sort_by_key(|s| std::cmp::Reverse(s.timestamp));
    Ok(summaries)
}

fn delete_schema_before(root: &Path, key: &SnapshotKey, cutoff: DateTime<Utc>) -> Result<usize> {
    let entries = read_stream_entries(&stream_dir(root, key))?;
    let mut deleted = 0usize;
    for (ts, _, path) in entries {
        if ts < cutoff {
            std::fs::remove_file(&path)
                .map_err(|e| Error::History(format!("remove {}: {e}", path.display())))?;
            deleted += 1;
        }
    }
    Ok(deleted)
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
    use crate::schema::{
        IndexActivity, IndexActivityEntry, NodeIdentity, QualifiedName, TableActivity,
        TableActivityEntry,
    };
    use tempfile::TempDir;

    fn make_schema(hash: &str) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "auth".into(),
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
        }
    }

    fn make_planner(schema_ref: &str, hash: &str) -> crate::schema::PlannerStatsSnapshot {
        crate::schema::PlannerStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "auth".into(),
            timestamp: Utc::now(),
            content_hash: hash.into(),
            schema_ref_hash: schema_ref.into(),
            tables: vec![],
            columns: vec![],
            indexes: vec![],
        }
    }

    fn make_activity(
        schema_ref: &str,
        label: &str,
        hash: &str,
    ) -> crate::schema::ActivityStatsSnapshot {
        crate::schema::ActivityStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "auth".into(),
            timestamp: Utc::now(),
            content_hash: hash.into(),
            schema_ref_hash: schema_ref.into(),
            node: NodeIdentity {
                label: label.into(),
                host: format!("host-{label}"),
                is_standby: label != "primary",
                replication_lag_bytes: None,
                stats_reset: None,
            },
            tables: vec![TableActivityEntry {
                table: QualifiedName::new("public", "orders"),
                activity: TableActivity {
                    seq_scan: 1,
                    idx_scan: 2,
                    n_live_tup: 0,
                    n_dead_tup: 0,
                    last_vacuum: None,
                    last_autovacuum: None,
                    last_analyze: None,
                    last_autoanalyze: None,
                    vacuum_count: 0,
                    autovacuum_count: 0,
                    analyze_count: 0,
                    autoanalyze_count: 0,
                },
            }],
            indexes: vec![IndexActivityEntry {
                index: QualifiedName::new("public", "orders_pkey"),
                activity: IndexActivity {
                    idx_scan: 0,
                    idx_tup_read: 0,
                    idx_tup_fetch: 0,
                },
            }],
        }
    }

    fn key() -> SnapshotKey {
        SnapshotKey {
            project_id: ProjectId("p".into()),
            database_id: DatabaseId("auth".into()),
        }
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
    async fn put_planner_returns_unsupported_error() {
        let (_dir, store) = temp_store();
        let k = key();
        let err = store
            .put_planner_stats(&k, &make_planner("schema-h1", "p1"))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("only schema snapshots supported"));
    }

    #[tokio::test]
    async fn put_activity_returns_unsupported_error() {
        let (_dir, store) = temp_store();
        let k = key();
        let err = store
            .put_activity_stats(&k, &make_activity("schema-h1", "primary", "a1"))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("only schema snapshots supported"));
    }

    #[tokio::test]
    async fn get_planner_returns_unsupported_error() {
        let (_dir, store) = temp_store();
        let k = key();
        let err = store
            .get(&k, &SnapshotKind::Planner, SnapshotRef::Latest)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("only schema snapshots supported"));
    }

    #[tokio::test]
    async fn list_returns_empty_for_non_schema_kinds() {
        let (_dir, store) = temp_store();
        let k = key();
        store.put_schema(&k, &make_schema("h1")).await.unwrap();

        let planner = store
            .list(&k, &SnapshotKind::Planner, TimeRange::default())
            .await
            .unwrap();
        assert!(planner.is_empty());
    }

    #[tokio::test]
    async fn list_kinds_returns_schema_only_when_files_present() {
        let (_dir, store) = temp_store();
        let k = key();
        assert!(store.list_kinds(&k).await.unwrap().is_empty());

        store.put_schema(&k, &make_schema("h1")).await.unwrap();
        let kinds = store.list_kinds(&k).await.unwrap();
        assert_eq!(kinds, vec![SnapshotKind::Schema]);
    }
}
