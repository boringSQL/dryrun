use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use tracing::{debug, info, warn};

use crate::error::{Error, Result};
use crate::history::snapshot_store::{
    PutOutcome, SnapshotKey, SnapshotRef, SnapshotStore, TimeRange,
};
use crate::schema::{
    ActivityStatsSnapshot, AnnotatedSnapshot, PlannerStatsSnapshot, SchemaSnapshot,
};

pub struct HistoryStore {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct SnapshotSummary {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub content_hash: String,
    pub database: String,
    pub project_id: Option<String>,
    pub database_id: Option<String>,
}

impl HistoryStore {
    const SCHEMA_VERSION: i32 = 2;

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::History(format!("cannot create directory: {e}")))?;
        }

        let existed = path.exists();

        let conn = Connection::open(path)
            .map_err(|e| Error::History(format!("cannot open history db: {e}")))?;

        let conn = if existed {
            let version: i32 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .map_err(|e| Error::History(format!("cannot read user_version: {e}")))?;

            match version.cmp(&Self::SCHEMA_VERSION) {
                std::cmp::Ordering::Equal => conn,
                std::cmp::Ordering::Less => {
                    warn!(
                        path = %path.display(),
                        from = version,
                        to = Self::SCHEMA_VERSION,
                        "history db on stale schema version; resetting",
                    );
                    drop(conn);
                    std::fs::remove_file(path).map_err(|e| {
                        Error::History(format!("cannot remove stale history db: {e}"))
                    })?;
                    Connection::open(path)
                        .map_err(|e| Error::History(format!("cannot reopen history db: {e}")))?
                }
                std::cmp::Ordering::Greater => {
                    return Err(Error::History(
                        "history db is from a newer version of dryrun".into(),
                    ));
                }
            }
        } else {
            conn
        };

        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.migrate()?;
        store.set_user_version(Self::SCHEMA_VERSION)?;

        debug!(path = %path.display(), "history store opened");
        Ok(store)
    }

    fn set_user_version(&self, version: i32) -> Result<()> {
        let conn = lock_conn(&self.conn)?;
        conn.pragma_update(None, "user_version", version)
            .map_err(|e| Error::History(format!("cannot set user_version: {e}")))?;
        Ok(())
    }

    pub fn open_default() -> Result<Self> {
        let path = default_history_path()?;
        Self::open(&path)
    }

    pub async fn latest_schema_hash(&self, key: &SnapshotKey) -> Result<Option<String>> {
        let pid = key.project_id.0.clone();
        let did = key.database_id.0.clone();
        run_blocking(&self.conn, move |conn| {
            let row: rusqlite::Result<String> = conn.query_row(
                "SELECT content_hash FROM snapshots
                  WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'
                  ORDER BY timestamp DESC LIMIT 1",
                params![pid, did],
                |r| r.get(0),
            );
            match row {
                Ok(h) => Ok(Some(h)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }

    pub async fn put_planner_stats(
        &self,
        key: &SnapshotKey,
        snap: &PlannerStatsSnapshot,
    ) -> Result<PutOutcome> {
        let key = key.clone();
        let snap = snap.clone();
        run_blocking(&self.conn, move |conn| {
            let pid = &key.project_id.0;
            let did = &key.database_id.0;

            let latest: Option<String> = conn
                .query_row(
                    "SELECT content_hash FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND kind = 'planner_stats'
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did],
                    |row| row.get(0),
                )
                .ok();

            if latest.as_deref() == Some(snap.content_hash.as_str()) {
                debug!(hash = %snap.content_hash, "planner stats unchanged, skipping put");
                return Ok(PutOutcome::Deduped);
            }

            let json = serde_json::to_string(&snap)
                .map_err(|e| Error::History(format!("cannot serialize planner stats: {e}")))?;

            conn.execute(
                "INSERT INTO snapshots (kind, timestamp, content_hash, schema_ref_hash,
                                        database_name, snapshot_json, project_id, database_id)
                 VALUES ('planner_stats', ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    snap.timestamp.to_rfc3339(),
                    snap.content_hash,
                    snap.schema_ref_hash,
                    snap.database,
                    json,
                    pid,
                    did,
                ],
            )?;

            info!(hash = %snap.content_hash, schema_ref = %snap.schema_ref_hash,
                project = %pid, database = %did, "planner stats put");
            Ok(PutOutcome::Inserted)
        })
        .await
    }

    pub async fn put_activity_stats(
        &self,
        key: &SnapshotKey,
        snap: &ActivityStatsSnapshot,
    ) -> Result<PutOutcome> {
        let key = key.clone();
        let snap = snap.clone();
        run_blocking(&self.conn, move |conn| {
            let pid = &key.project_id.0;
            let did = &key.database_id.0;
            let label = &snap.node.label;

            let latest: Option<String> = conn
                .query_row(
                    "SELECT content_hash FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2
                        AND kind = 'activity_stats' AND node_label = ?3
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did, label],
                    |row| row.get(0),
                )
                .ok();

            if latest.as_deref() == Some(snap.content_hash.as_str()) {
                debug!(hash = %snap.content_hash, label = %label,
                    "activity stats unchanged, skipping put");
                return Ok(PutOutcome::Deduped);
            }

            let json = serde_json::to_string(&snap)
                .map_err(|e| Error::History(format!("cannot serialize activity stats: {e}")))?;

            conn.execute(
                "INSERT INTO snapshots (kind, timestamp, content_hash, schema_ref_hash,
                                        node_label, database_name, snapshot_json,
                                        project_id, database_id)
                 VALUES ('activity_stats', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    snap.timestamp.to_rfc3339(),
                    snap.content_hash,
                    snap.schema_ref_hash,
                    label,
                    snap.database,
                    json,
                    pid,
                    did,
                ],
            )?;

            info!(hash = %snap.content_hash, schema_ref = %snap.schema_ref_hash,
                label = %label, project = %pid, database = %did,
                "activity stats put");
            Ok(PutOutcome::Inserted)
        })
        .await
    }

    pub async fn get_annotated(
        &self,
        key: &SnapshotKey,
        at: SnapshotRef,
    ) -> Result<AnnotatedSnapshot> {
        let schema = SnapshotStore::get(self, key, at.clone()).await?;
        let schema_hash = schema.content_hash.clone();
        let pid = key.project_id.0.clone();
        let did = key.database_id.0.clone();

        let planner = {
            let pid = pid.clone();
            let did = did.clone();
            let h = schema_hash.clone();
            run_blocking(&self.conn, move |conn| {
                let row: rusqlite::Result<String> = conn.query_row(
                    "SELECT snapshot_json FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2
                        AND kind = 'planner_stats' AND schema_ref_hash = ?3
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did, h],
                    |r| r.get(0),
                );
                match row {
                    Ok(j) => Ok(Some(
                        serde_json::from_str::<PlannerStatsSnapshot>(&j).map_err(|e| {
                            Error::History(format!("corrupt planner stats JSON: {e}"))
                        })?,
                    )),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            })
            .await?
        };

        let activity_by_node: BTreeMap<String, ActivityStatsSnapshot> = {
            let h = schema_hash.clone();
            run_blocking(&self.conn, move |conn| {
                // For each node_label, pick the latest row at this schema ref.
                let mut stmt = conn.prepare(
                    "SELECT node_label, snapshot_json FROM snapshots a
                      WHERE project_id = ?1 AND database_id = ?2
                        AND kind = 'activity_stats' AND schema_ref_hash = ?3
                        AND node_label IS NOT NULL
                        AND timestamp = (
                            SELECT MAX(b.timestamp) FROM snapshots b
                              WHERE b.project_id = a.project_id
                                AND b.database_id = a.database_id
                                AND b.kind = 'activity_stats'
                                AND b.schema_ref_hash = a.schema_ref_hash
                                AND b.node_label = a.node_label
                        )",
                )?;
                let rows = stmt.query_map(params![pid, did, h], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?;
                let mut out: BTreeMap<String, ActivityStatsSnapshot> = BTreeMap::new();
                for row in rows {
                    let (label, json) = row?;
                    let snap: ActivityStatsSnapshot = serde_json::from_str(&json)
                        .map_err(|e| Error::History(format!("corrupt activity stats JSON: {e}")))?;
                    out.insert(label, snap);
                }
                Ok(out)
            })
            .await?
        };

        Ok(AnnotatedSnapshot {
            schema,
            planner,
            activity_by_node,
        })
    }

    pub fn list_keys(&self) -> Result<Vec<SnapshotKey>> {
        let conn = lock_conn(&self.conn)?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT project_id, database_id
               FROM snapshots
              WHERE project_id IS NOT NULL AND database_id IS NOT NULL
              ORDER BY project_id, database_id",
        )?;
        let rows = stmt.query_map([], |row| {
            let pid: String = row.get(0)?;
            let did: String = row.get(1)?;
            Ok(SnapshotKey {
                project_id: crate::history::ProjectId(pid),
                database_id: crate::history::DatabaseId(did),
            })
        })?;
        rows.map(|r| r.map_err(Error::from)).collect()
    }

    fn migrate(&self) -> Result<()> {
        let conn = lock_conn(&self.conn)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS snapshots (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind            TEXT NOT NULL DEFAULT 'schema'
                                        CHECK (kind IN ('schema','planner_stats','activity_stats')),
                    timestamp       TEXT NOT NULL,
                    content_hash    TEXT NOT NULL,
                    schema_ref_hash TEXT,
                    node_label      TEXT,
                    database_name   TEXT NOT NULL,
                    snapshot_json   TEXT NOT NULL,
                    project_id      TEXT,
                    database_id     TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_content_hash
                    ON snapshots(content_hash);
                CREATE INDEX IF NOT EXISTS idx_snapshots_kind_schema_ref
                    ON snapshots(kind, schema_ref_hash);
                CREATE INDEX IF NOT EXISTS idx_snapshots_kind_node_ts
                    ON snapshots(kind, node_label, timestamp DESC);",
        )
        .map_err(|e| Error::History(format!("migration failed: {e}")))?;
        Ok(())
    }
}

fn default_history_path() -> Result<PathBuf> {
    let dir = default_data_dir()?;
    Ok(dir.join("history.db"))
}

pub fn default_data_dir() -> Result<PathBuf> {
    let cwd = std::env::current_dir()
        .map_err(|e| Error::History(format!("cannot determine working directory: {e}")))?;
    Ok(cwd.join(".dryrun"))
}

fn lock_conn(conn: &Mutex<Connection>) -> Result<std::sync::MutexGuard<'_, Connection>> {
    conn.lock()
        .map_err(|e| Error::History(format!("lock poisoned: {e}")))
}

fn row_to_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<SnapshotSummary> {
    let ts_str: String = row.get(1)?;
    Ok(SnapshotSummary {
        id: row.get(0)?,
        timestamp: DateTime::parse_from_rfc3339(&ts_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_default(),
        content_hash: row.get(2)?,
        database: row.get(3)?,
        project_id: row.get(4)?,
        database_id: row.get(5)?,
    })
}

async fn run_blocking<F, T>(conn: &Arc<Mutex<Connection>>, f: F) -> Result<T>
where
    F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let conn = conn.clone();
    tokio::task::spawn_blocking(move || -> Result<T> {
        let conn = conn
            .lock()
            .map_err(|e| Error::History(format!("lock poisoned: {e}")))?;
        f(&conn)
    })
    .await
    .map_err(|e| Error::History(format!("blocking task failed: {e}")))?
}

#[async_trait]
impl SnapshotStore for HistoryStore {
    async fn put(&self, key: &SnapshotKey, snap: &SchemaSnapshot) -> Result<PutOutcome> {
        let key = key.clone();
        let snap = snap.clone();
        run_blocking(&self.conn, move |conn| {
            let pid = &key.project_id.0;
            let did = &key.database_id.0;

            let latest: Option<String> = conn
                .query_row(
                    "SELECT content_hash FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did],
                    |row| row.get(0),
                )
                .ok();

            if latest.as_deref() == Some(snap.content_hash.as_str()) {
                debug!(hash = %snap.content_hash, "schema unchanged, skipping put");
                return Ok(PutOutcome::Deduped);
            }

            let json = serde_json::to_string(&snap)
                .map_err(|e| Error::History(format!("cannot serialize snapshot: {e}")))?;

            conn.execute(
                "INSERT INTO snapshots (kind, timestamp, content_hash, database_name,
                                        snapshot_json, project_id, database_id)
                 VALUES ('schema', ?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    snap.timestamp.to_rfc3339(),
                    snap.content_hash,
                    snap.database,
                    json,
                    pid,
                    did,
                ],
            )?;

            info!(hash = %snap.content_hash, project = %pid, database = %did, "snapshot put");
            Ok(PutOutcome::Inserted)
        })
        .await
    }

    async fn get(&self, key: &SnapshotKey, at: SnapshotRef) -> Result<SchemaSnapshot> {
        let pid = key.project_id.0.clone();
        let did = key.database_id.0.clone();
        run_blocking(&self.conn, move |conn| {
            let row = match &at {
                SnapshotRef::Latest => conn.query_row(
                    "SELECT snapshot_json FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did],
                    |r| r.get::<_, String>(0),
                ),
                SnapshotRef::At(ts) => conn.query_row(
                    "SELECT snapshot_json FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'
                        AND timestamp <= ?3
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did, ts.to_rfc3339()],
                    |r| r.get::<_, String>(0),
                ),
                SnapshotRef::Hash(h) => conn.query_row(
                    "SELECT snapshot_json FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'
                        AND content_hash = ?3
                      LIMIT 1",
                    params![pid, did, h],
                    |r| r.get::<_, String>(0),
                ),
            };

            let json = match row {
                Ok(j) => j,
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    let detail = match at {
                        SnapshotRef::Latest => "latest".to_string(),
                        SnapshotRef::At(ts) => format!("at-or-before {ts}"),
                        SnapshotRef::Hash(h) => format!("hash {h}"),
                    };
                    return Err(Error::History(format!("snapshot not found ({detail})")));
                }
                Err(e) => return Err(e.into()),
            };

            serde_json::from_str(&json)
                .map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))
        })
        .await
    }

    async fn list(&self, key: &SnapshotKey, range: TimeRange) -> Result<Vec<SnapshotSummary>> {
        let pid = key.project_id.0.clone();
        let did = key.database_id.0.clone();
        run_blocking(&self.conn, move |conn| {
            let mut sql = String::from(
                "SELECT id, timestamp, content_hash, database_name,
                        project_id, database_id
                   FROM snapshots
                  WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'",
            );
            let mut bound: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(pid), Box::new(did)];
            if let Some(from) = range.from {
                sql += &format!(" AND timestamp >= ?{}", bound.len() + 1);
                bound.push(Box::new(from.to_rfc3339()));
            }
            if let Some(to) = range.to {
                sql += &format!(" AND timestamp < ?{}", bound.len() + 1);
                bound.push(Box::new(to.to_rfc3339()));
            }
            sql += " ORDER BY timestamp DESC";

            let mut stmt = conn.prepare(&sql)?;
            let params: Vec<&dyn rusqlite::ToSql> = bound.iter().map(|b| b.as_ref()).collect();
            stmt.query_map(params.as_slice(), row_to_summary)?
                .map(|r| r.map_err(Error::from))
                .collect()
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
        let pid = key.project_id.0.clone();
        let did = key.database_id.0.clone();
        run_blocking(&self.conn, move |conn| {
            Ok(conn.execute(
                "DELETE FROM snapshots
                  WHERE project_id = ?1 AND database_id = ?2 AND kind = 'schema'
                    AND timestamp < ?3",
                params![pid, did, cutoff.to_rfc3339()],
            )?)
        })
        .await
    }
}

#[cfg(test)]
mod trait_tests {
    use chrono::Duration;
    use tempfile::TempDir;

    use super::*;
    use crate::history::snapshot_store::{DatabaseId, ProjectId};

    fn make_snap(hash: &str, database: &str) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: database.into(),
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

    fn key(proj: &str, db: &str) -> SnapshotKey {
        SnapshotKey {
            project_id: ProjectId(proj.into()),
            database_id: DatabaseId(db.into()),
        }
    }

    fn temp_store() -> (TempDir, HistoryStore) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_history.db");
        let store = HistoryStore::open(&path).unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn put_inserts_then_dedupes() {
        let (_dir, store) = temp_store();
        let k = key("p", "auth");
        let snap = make_snap("h1", "auth");

        assert_eq!(store.put(&k, &snap).await.unwrap(), PutOutcome::Inserted);
        assert_eq!(store.put(&k, &snap).await.unwrap(), PutOutcome::Deduped);
    }

    #[tokio::test]
    async fn put_isolates_across_databases() {
        let (_dir, store) = temp_store();
        let auth = key("p", "auth");
        let billing = key("p", "billing");

        // same content_hash under different database_id should not dedupe
        assert_eq!(
            store.put(&auth, &make_snap("same", "auth")).await.unwrap(),
            PutOutcome::Inserted
        );
        assert_eq!(
            store
                .put(&billing, &make_snap("same", "billing"))
                .await
                .unwrap(),
            PutOutcome::Inserted
        );

        let auth_rows = store.list(&auth, TimeRange::default()).await.unwrap();
        let billing_rows = store.list(&billing, TimeRange::default()).await.unwrap();
        assert_eq!(auth_rows.len(), 1);
        assert_eq!(billing_rows.len(), 1);
        assert_eq!(auth_rows[0].database_id.as_deref(), Some("auth"));
        assert_eq!(billing_rows[0].database_id.as_deref(), Some("billing"));
    }

    #[tokio::test]
    async fn put_isolates_across_projects() {
        let (_dir, store) = temp_store();
        let a = key("a", "x");
        let b = key("b", "x");
        store.put(&a, &make_snap("h", "x")).await.unwrap();
        store.put(&b, &make_snap("h", "x")).await.unwrap();

        let a_rows = store.list(&a, TimeRange::default()).await.unwrap();
        let b_rows = store.list(&b, TimeRange::default()).await.unwrap();
        assert_eq!(a_rows.len(), 1);
        assert_eq!(b_rows.len(), 1);
        assert_eq!(a_rows[0].project_id.as_deref(), Some("a"));
        assert_eq!(b_rows[0].project_id.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn list_orders_newest_first() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        let mut s1 = make_snap("h1", "x");
        s1.timestamp = Utc::now() - Duration::hours(2);
        let mut s2 = make_snap("h2", "x");
        s2.timestamp = Utc::now() - Duration::hours(1);
        store.put(&k, &s1).await.unwrap();
        store.put(&k, &s2).await.unwrap();

        let rows = store.list(&k, TimeRange::default()).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].content_hash, "h2");
        assert_eq!(rows[1].content_hash, "h1");
    }

    #[tokio::test]
    async fn list_filters_by_time_range() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        let now = Utc::now();
        for (i, hash) in ["h0", "h1", "h2"].iter().enumerate() {
            let mut s = make_snap(hash, "x");
            s.timestamp = now - Duration::hours(2 - i as i64);
            store.put(&k, &s).await.unwrap();
        }

        // from = -90min: h0 at -2h is excluded, h1 at -1h and h2 at 0 included
        let rows = store
            .list(
                &k,
                TimeRange {
                    from: Some(now - Duration::minutes(90)),
                    to: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].content_hash, "h2");
        assert_eq!(rows[1].content_hash, "h1");

        // to = -30min (exclusive): h2 at 0 excluded, h0 and h1 included
        let rows = store
            .list(
                &k,
                TimeRange {
                    from: None,
                    to: Some(now - Duration::minutes(30)),
                },
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].content_hash, "h1");
        assert_eq!(rows[1].content_hash, "h0");
    }

    #[tokio::test]
    async fn latest_returns_most_recent_or_none() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        assert!(store.latest(&k).await.unwrap().is_none());

        let mut s1 = make_snap("old", "x");
        s1.timestamp = Utc::now() - Duration::hours(1);
        let s2 = make_snap("new", "x");
        store.put(&k, &s1).await.unwrap();
        store.put(&k, &s2).await.unwrap();

        let latest = store.latest(&k).await.unwrap().unwrap();
        assert_eq!(latest.content_hash, "new");
    }

    #[tokio::test]
    async fn get_latest_returns_most_recent() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        let mut s1 = make_snap("old", "x");
        s1.timestamp = Utc::now() - Duration::hours(1);
        let s2 = make_snap("new", "x");
        store.put(&k, &s1).await.unwrap();
        store.put(&k, &s2).await.unwrap();

        let got = store.get(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(got.content_hash, "new");
    }

    #[tokio::test]
    async fn get_at_returns_at_or_before() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        let now = Utc::now();
        let mut s1 = make_snap("h1", "x");
        s1.timestamp = now - Duration::hours(2);
        let mut s2 = make_snap("h2", "x");
        s2.timestamp = now;
        store.put(&k, &s1).await.unwrap();
        store.put(&k, &s2).await.unwrap();

        // at -1h: h2 is in the future, only h1 qualifies
        let got = store
            .get(&k, SnapshotRef::At(now - Duration::hours(1)))
            .await
            .unwrap();
        assert_eq!(got.content_hash, "h1");
    }

    #[tokio::test]
    async fn get_hash_returns_matching_scoped_to_key() {
        let (_dir, store) = temp_store();
        let a = key("p", "auth");
        let b = key("p", "billing");
        store.put(&a, &make_snap("shared", "auth")).await.unwrap();

        // direct lookup under correct key works
        let got = store
            .get(&a, SnapshotRef::Hash("shared".into()))
            .await
            .unwrap();
        assert_eq!(got.content_hash, "shared");

        // same hash under different key fails — content_hash lookup is key-scoped
        let result = store.get(&b, SnapshotRef::Hash("shared".into())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_missing_returns_error() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        assert!(store.get(&k, SnapshotRef::Latest).await.is_err());
        assert!(
            store
                .get(&k, SnapshotRef::Hash("nope".into()))
                .await
                .is_err()
        );
        assert!(store.get(&k, SnapshotRef::At(Utc::now())).await.is_err());
    }

    #[tokio::test]
    async fn delete_before_returns_count_and_removes_old() {
        let (_dir, store) = temp_store();
        let k = key("p", "x");
        let now = Utc::now();
        for (i, hash) in ["h0", "h1", "h2", "h3"].iter().enumerate() {
            let mut s = make_snap(hash, "x");
            s.timestamp = now - Duration::hours(3 - i as i64);
            store.put(&k, &s).await.unwrap();
        }

        let deleted = store
            .delete_before(&k, now - Duration::minutes(90))
            .await
            .unwrap();
        assert_eq!(deleted, 2); // h0 (-3h) and h1 (-2h)

        let remaining = store.list(&k, TimeRange::default()).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].content_hash, "h3");
        assert_eq!(remaining[1].content_hash, "h2");
    }

    #[tokio::test]
    async fn delete_before_scoped_to_key() {
        let (_dir, store) = temp_store();
        let a = key("p", "auth");
        let b = key("p", "billing");
        let mut s = make_snap("h", "auth");
        s.timestamp = Utc::now() - Duration::hours(2);
        store.put(&a, &s).await.unwrap();
        let mut s = make_snap("h", "billing");
        s.timestamp = Utc::now() - Duration::hours(2);
        store.put(&b, &s).await.unwrap();

        // delete in `a` should not touch `b`
        let deleted = store
            .delete_before(&a, Utc::now() - Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(store.list(&a, TimeRange::default()).await.unwrap().len(), 0);
        assert_eq!(store.list(&b, TimeRange::default()).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_keys_returns_distinct_streams_ordered() {
        let (_dir, store) = temp_store();
        // empty store
        assert!(store.list_keys().unwrap().is_empty());

        // put under three streams, with one stream getting two snapshots
        store
            .put(&key("p", "billing"), &make_snap("h1", "billing"))
            .await
            .unwrap();
        store
            .put(&key("p", "auth"), &make_snap("h2", "auth"))
            .await
            .unwrap();
        store
            .put(&key("p", "auth"), &make_snap("h3", "auth"))
            .await
            .unwrap();
        store
            .put(&key("other", "auth"), &make_snap("h4", "auth"))
            .await
            .unwrap();

        let keys = store.list_keys().unwrap();
        // three distinct (project, database) pairs, ordered by project then database
        assert_eq!(keys.len(), 3);
        assert_eq!(
            (
                keys[0].project_id.0.as_str(),
                keys[0].database_id.0.as_str()
            ),
            ("other", "auth")
        );
        assert_eq!(
            (
                keys[1].project_id.0.as_str(),
                keys[1].database_id.0.as_str()
            ),
            ("p", "auth")
        );
        assert_eq!(
            (
                keys[2].project_id.0.as_str(),
                keys[2].database_id.0.as_str()
            ),
            ("p", "billing")
        );
    }

    use crate::schema::{
        ActivityStatsSnapshot, IndexActivity, IndexActivityEntry, NodeIdentity,
        PlannerStatsSnapshot, QualifiedName, TableActivity, TableActivityEntry,
    };

    fn make_planner(schema_ref: &str, db: &str, hash: &str) -> PlannerStatsSnapshot {
        PlannerStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: db.into(),
            timestamp: Utc::now(),
            content_hash: hash.into(),
            schema_ref_hash: schema_ref.into(),
            tables: vec![],
            columns: vec![],
            indexes: vec![],
        }
    }

    fn make_activity(schema_ref: &str, db: &str, label: &str, hash: &str) -> ActivityStatsSnapshot {
        ActivityStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: db.into(),
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

    #[tokio::test]
    async fn snapshot_get_filters_to_kind_schema() {
        // Regression: planner_stats rows must not bleed into SnapshotStore::get(Latest).
        let (_dir, store) = temp_store();
        let k = key("p", "auth");

        let schema = make_snap("schema-h1", "auth");
        store.put(&k, &schema).await.unwrap();

        // Insert a newer planner_stats row referring to the schema.
        let planner = make_planner("schema-h1", "auth", "planner-h1");
        store.put_planner_stats(&k, &planner).await.unwrap();

        let got = store.get(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(got.content_hash, "schema-h1");
    }

    #[tokio::test]
    async fn get_annotated_joins_schema_planner_and_single_node_activity() {
        let (_dir, store) = temp_store();
        let k = key("p", "auth");

        let schema = make_snap("schema-h1", "auth");
        store.put(&k, &schema).await.unwrap();
        let planner = make_planner("schema-h1", "auth", "planner-h1");
        store.put_planner_stats(&k, &planner).await.unwrap();
        let primary = make_activity("schema-h1", "auth", "primary", "act-primary-1");
        store.put_activity_stats(&k, &primary).await.unwrap();

        let bundle = store.get_annotated(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(bundle.schema.content_hash, "schema-h1");
        assert!(bundle.planner.is_some());
        assert_eq!(bundle.activity_by_node.len(), 1);
        assert!(bundle.activity_by_node.contains_key("primary"));
    }

    #[tokio::test]
    async fn get_annotated_returns_multiple_activity_nodes() {
        let (_dir, store) = temp_store();
        let k = key("p", "auth");
        store
            .put(&k, &make_snap("schema-h1", "auth"))
            .await
            .unwrap();
        for label in ["primary", "replica1", "replica2"] {
            let a = make_activity("schema-h1", "auth", label, &format!("act-{label}"));
            store.put_activity_stats(&k, &a).await.unwrap();
        }

        let bundle = store.get_annotated(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(bundle.activity_by_node.len(), 3);
        let labels: Vec<&str> = bundle.node_labels().collect();
        assert_eq!(labels, vec!["primary", "replica1", "replica2"]);
    }

    #[tokio::test]
    async fn get_annotated_excludes_planner_with_stale_schema_ref() {
        // Planner attached to schema A. New schema B replaces A as latest.
        // get_annotated(Latest) must return planner=None — strict-match on schema_ref_hash.
        let (_dir, store) = temp_store();
        let k = key("p", "auth");

        store.put(&k, &make_snap("schema-A", "auth")).await.unwrap();
        let planner = make_planner("schema-A", "auth", "planner-A");
        store.put_planner_stats(&k, &planner).await.unwrap();

        // small sleep to ensure later timestamp ordering
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        store.put(&k, &make_snap("schema-B", "auth")).await.unwrap();

        let bundle = store.get_annotated(&k, SnapshotRef::Latest).await.unwrap();
        assert_eq!(bundle.schema.content_hash, "schema-B");
        assert!(
            bundle.planner.is_none(),
            "planner attached to old schema must not bleed onto new schema"
        );
    }

    #[tokio::test]
    async fn get_annotated_with_no_stats_returns_empty_bundle() {
        let (_dir, store) = temp_store();
        let k = key("p", "auth");
        store
            .put(&k, &make_snap("schema-h1", "auth"))
            .await
            .unwrap();

        let bundle = store.get_annotated(&k, SnapshotRef::Latest).await.unwrap();
        assert!(bundle.planner.is_none());
        assert!(bundle.activity_by_node.is_empty());
    }

    #[tokio::test]
    async fn get_annotated_picks_latest_per_node_label() {
        let (_dir, store) = temp_store();
        let k = key("p", "auth");
        store
            .put(&k, &make_snap("schema-h1", "auth"))
            .await
            .unwrap();

        // Two activity rows for the same label; only the latest should appear.
        let first = make_activity("schema-h1", "auth", "primary", "act-1");
        store.put_activity_stats(&k, &first).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let second = make_activity("schema-h1", "auth", "primary", "act-2");
        store.put_activity_stats(&k, &second).await.unwrap();

        let bundle = store.get_annotated(&k, SnapshotRef::Latest).await.unwrap();
        let primary = bundle.activity_by_node.get("primary").unwrap();
        assert_eq!(primary.content_hash, "act-2");
    }
}
