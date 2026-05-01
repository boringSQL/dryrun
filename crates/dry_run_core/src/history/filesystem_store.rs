use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, info};

use crate::error::{Error, Result};
use crate::history::{
    DatabaseId, ProjectId, PutOutcome, SnapshotKey, SnapshotRef, SnapshotStore, SnapshotSummary,
    TimeRange, parse_snapshot_filename, snapshot_path,
};
use crate::schema::SchemaSnapshot;

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

#[async_trait]
impl SnapshotStore for FilesystemStore {
    async fn put(&self, key: &SnapshotKey, snap: &SchemaSnapshot) -> Result<PutOutcome> {
        let root = self.root.clone();
        let key = key.clone();
        let snap = snap.clone();
        run_blocking(move || {
            let stream_dir = stream_dir(&root, &key);
            if let Some(latest) = read_latest_hash(&stream_dir)?
                && latest == snap.content_hash
            {
                debug!(hash = %snap.content_hash, "schema unchanged, skipping put");
                return Ok(PutOutcome::Deduped);
            }

            let path = snapshot_path(&root, &key, snap.timestamp, &snap.content_hash);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::History(format!("create_dir_all {}: {e}", parent.display()))
                })?;
            }

            // atomic write: tmp + rename
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
        })
        .await
    }

    async fn get(&self, key: &SnapshotKey, at: SnapshotRef) -> Result<SchemaSnapshot> {
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || {
            let entries = read_stream_entries(&stream_dir(&root, &key))?;

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
            serde_json::from_slice(&json)
                .map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))
        })
        .await
    }

    async fn list(&self, key: &SnapshotKey, range: TimeRange) -> Result<Vec<SnapshotSummary>> {
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || {
            let entries = read_stream_entries(&stream_dir(&root, &key))?;
            let mut summaries: Vec<SnapshotSummary> = entries
                .into_iter()
                .filter(|(ts, _, _)| {
                    range.from.is_none_or(|from| *ts >= from) && range.to.is_none_or(|to| *ts < to)
                })
                .map(|(ts, hash, _)| SnapshotSummary {
                    id: 0,
                    timestamp: ts,
                    content_hash: hash,
                    database: key.database_id.0.clone(),
                    project_id: Some(key.project_id.0.clone()),
                    database_id: Some(key.database_id.0.clone()),
                })
                .collect();
            summaries.sort_by_key(|s| std::cmp::Reverse(s.timestamp));
            Ok(summaries)
        })
        .await
    }

    async fn latest(&self, key: &SnapshotKey) -> Result<Option<SnapshotSummary>> {
        Ok(self
            .list(key, TimeRange::default())
            .await?
            .into_iter()
            .next())
    }

    async fn delete_before(&self, key: &SnapshotKey, cutoff: DateTime<Utc>) -> Result<usize> {
        let root = self.root.clone();
        let key = key.clone();
        run_blocking(move || {
            let entries = read_stream_entries(&stream_dir(&root, &key))?;
            let mut deleted = 0usize;
            for (ts, _, path) in entries {
                if ts < cutoff {
                    std::fs::remove_file(&path)
                        .map_err(|e| Error::History(format!("remove {}: {e}", path.display())))?;
                    deleted += 1;
                }
            }
            Ok(deleted)
        })
        .await
    }
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
