use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::error::{Error, Result};
use crate::history::snapshot_store::{
    PutOutcome, SnapshotKey, SnapshotRef, SnapshotStore, TimeRange,
};
use crate::schema::SchemaSnapshot;

pub struct HistoryStore {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct SnapshotSummary {
    pub id: i64,
    pub db_url_hash: String,
    pub timestamp: DateTime<Utc>,
    pub content_hash: String,
    pub database: String,
    pub project_id: Option<String>,
    pub database_id: Option<String>,
}

impl HistoryStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::History(format!("cannot create directory: {e}")))?;
        }

        let conn = Connection::open(path)
            .map_err(|e| Error::History(format!("cannot open history db: {e}")))?;

        let store = Self { conn: Arc::new(Mutex::new(conn)) };
        store.migrate()?;

        debug!(path = %path.display(), "history store opened");
        Ok(store)
    }

    pub fn open_default() -> Result<Self> {
        let path = default_history_path()?;
        Self::open(&path)
    }

    // saves snapshot, returns false if content_hash unchanged from latest
    pub fn save_snapshot(&self, db_url: &str, snapshot: &SchemaSnapshot) -> Result<bool> {
        let conn = lock_conn(&self.conn)?;
        let db_url_hash = hash_url(db_url);

        let latest_hash: Option<String> = conn
            .query_row(
                "SELECT content_hash FROM snapshots
                  WHERE db_url_hash = ?1
                  ORDER BY timestamp DESC LIMIT 1",
                params![db_url_hash],
                |row| row.get(0),
            )
            .ok();

        if latest_hash.as_deref() == Some(&snapshot.content_hash) {
            debug!(hash = %snapshot.content_hash, "schema unchanged, skipping save");
            return Ok(false);
        }

        let json = serde_json::to_string(snapshot)
            .map_err(|e| Error::History(format!("cannot serialize snapshot: {e}")))?;

        conn.execute(
            "INSERT INTO snapshots (db_url_hash, timestamp, content_hash, database_name, snapshot_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                db_url_hash,
                snapshot.timestamp.to_rfc3339(),
                snapshot.content_hash,
                snapshot.database,
                json,
            ],
        )
        .map_err(|e| Error::History(format!("cannot save snapshot: {e}")))?;

        info!(
            hash = %snapshot.content_hash,
            database = %snapshot.database,
            "snapshot saved"
        );
        Ok(true)
    }

    pub fn load_snapshot(&self, content_hash: &str) -> Result<Option<SchemaSnapshot>> {
        let conn = lock_conn(&self.conn)?;
        let json: Option<String> = conn
            .query_row(
                "SELECT snapshot_json FROM snapshots WHERE content_hash = ?1 LIMIT 1",
                params![content_hash],
                |row| row.get(0),
            )
            .ok();

        match json {
            Some(j) => {
                let snapshot: SchemaSnapshot = serde_json::from_str(&j)
                    .map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    pub fn list_snapshots(&self, db_url: &str) -> Result<Vec<SnapshotSummary>> {
        let conn = lock_conn(&self.conn)?;
        let db_url_hash = hash_url(db_url);

        let mut stmt = conn.prepare(
            "SELECT id, db_url_hash, timestamp, content_hash, database_name,
                    project_id, database_id
               FROM snapshots
              WHERE db_url_hash = ?1
              ORDER BY timestamp DESC",
        )?;

        stmt.query_map(params![db_url_hash], row_to_summary)?
            .map(|r| r.map_err(Error::from))
            .collect()
    }

    pub fn snapshots_since(
        &self,
        db_url: &str,
        since: DateTime<Utc>,
    ) -> Result<Vec<SchemaSnapshot>> {
        let conn = lock_conn(&self.conn)?;
        let db_url_hash = hash_url(db_url);

        let mut stmt = conn.prepare(
            "SELECT snapshot_json FROM snapshots
              WHERE db_url_hash = ?1 AND timestamp >= ?2
              ORDER BY timestamp ASC",
        )?;

        stmt.query_map(params![db_url_hash, since.to_rfc3339()], |row| {
            row.get::<_, String>(0)
        })?
        .map(|r| {
            let json = r?;
            serde_json::from_str(&json)
                .map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))
        })
        .collect()
    }

    pub fn latest_snapshot(&self, db_url: &str) -> Result<Option<SchemaSnapshot>> {
        let conn = lock_conn(&self.conn)?;
        let db_url_hash = hash_url(db_url);

        let json: Option<String> = conn
            .query_row(
                "SELECT snapshot_json FROM snapshots
                  WHERE db_url_hash = ?1
                  ORDER BY timestamp DESC LIMIT 1",
                params![db_url_hash],
                |row| row.get(0),
            )
            .ok();

        match json {
            Some(j) => {
                let snapshot: SchemaSnapshot = serde_json::from_str(&j)
                    .map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    fn migrate(&self) -> Result<()> {
        let conn = lock_conn(&self.conn)?;
        conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS snapshots (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_url_hash   TEXT NOT NULL,
                    timestamp     TEXT NOT NULL,
                    content_hash  TEXT NOT NULL,
                    database_name TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    project_id    TEXT,
                    database_id   TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_db_url_hash
                    ON snapshots(db_url_hash, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_snapshots_content_hash
                    ON snapshots(content_hash);",
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

fn hash_url(url: &str) -> String {
    let digest = Sha256::digest(url.as_bytes());
    let hex: String = digest.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{b:02x}").expect("write to String cannot fail");
        s
    });
    hex[..16].to_string()
}

fn synthetic_db_url_hash(key: &SnapshotKey) -> String {
    let input = format!("dryrun-key:{}:{}", key.project_id.0, key.database_id.0);
    let digest = Sha256::digest(input.as_bytes());
    let hex: String = digest.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{b:02x}").expect("write to String cannot fail");
        s
    });
    hex[..16].to_string()
}

fn lock_conn(conn: &Mutex<Connection>) -> Result<std::sync::MutexGuard<'_, Connection>> {
    conn.lock()
        .map_err(|e| Error::History(format!("lock poisoned: {e}")))
}

fn row_to_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<SnapshotSummary> {
    let ts_str: String = row.get(2)?;
    Ok(SnapshotSummary {
        id: row.get(0)?,
        db_url_hash: row.get(1)?,
        timestamp: DateTime::parse_from_rfc3339(&ts_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_default(),
        content_hash: row.get(3)?,
        database: row.get(4)?,
        project_id: row.get(5)?,
        database_id: row.get(6)?,
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
                      WHERE project_id = ?1 AND database_id = ?2
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
                "INSERT INTO snapshots (db_url_hash, timestamp, content_hash, database_name,
                                        snapshot_json, project_id, database_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    synthetic_db_url_hash(&key),
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
                      WHERE project_id = ?1 AND database_id = ?2
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did],
                    |r| r.get::<_, String>(0),
                ),
                SnapshotRef::At(ts) => conn.query_row(
                    "SELECT snapshot_json FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND timestamp <= ?3
                      ORDER BY timestamp DESC LIMIT 1",
                    params![pid, did, ts.to_rfc3339()],
                    |r| r.get::<_, String>(0),
                ),
                SnapshotRef::Hash(h) => conn.query_row(
                    "SELECT snapshot_json FROM snapshots
                      WHERE project_id = ?1 AND database_id = ?2 AND content_hash = ?3
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
                "SELECT id, db_url_hash, timestamp, content_hash, database_name,
                        project_id, database_id
                   FROM snapshots
                  WHERE project_id = ?1 AND database_id = ?2",
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
        Ok(self.list(key, TimeRange::default()).await?.into_iter().next())
    }

    async fn delete_before(&self, key: &SnapshotKey, cutoff: DateTime<Utc>) -> Result<usize> {
        let pid = key.project_id.0.clone();
        let did = key.database_id.0.clone();
        run_blocking(&self.conn, move |conn| {
            Ok(conn.execute(
                "DELETE FROM snapshots
                  WHERE project_id = ?1 AND database_id = ?2 AND timestamp < ?3",
                params![pid, did, cutoff.to_rfc3339()],
            )?)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::TempDir;

    use super::*;
    use crate::schema::SchemaSnapshot;

    fn make_snapshot(hash: &str, database: &str) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: database.into(),
            timestamp: Utc::now(),
            content_hash: hash.into(),
            source: None,
            tables: vec![], enums: vec![], domains: vec![], composites: vec![],
            views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    fn temp_store() -> (TempDir, HistoryStore) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_history.db");
        let store = HistoryStore::open(&path).unwrap();
        (dir, store)
    }

    #[test]
    fn save_and_load() {
        let (_dir, store) = temp_store();
        let snap = make_snapshot("abc123", "mydb");
        let url = "postgres://user@host/mydb";

        assert!(store.save_snapshot(url, &snap).unwrap());

        let loaded = store.load_snapshot("abc123").unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().content_hash, "abc123");
    }

    #[test]
    fn skip_duplicate_hash() {
        let (_dir, store) = temp_store();
        let url = "postgres://user@host/mydb";

        assert!(store.save_snapshot(url, &make_snapshot("same_hash", "mydb")).unwrap());
        assert!(!store.save_snapshot(url, &make_snapshot("same_hash", "mydb")).unwrap());
    }

    #[test]
    fn list_snapshots_order() {
        let (_dir, store) = temp_store();
        let url = "postgres://user@host/mydb";

        let mut s1 = make_snapshot("hash1", "mydb");
        s1.timestamp = Utc::now() - chrono::Duration::hours(2);
        store.save_snapshot(url, &s1).unwrap();

        let mut s2 = make_snapshot("hash2", "mydb");
        s2.timestamp = Utc::now() - chrono::Duration::hours(1);
        store.save_snapshot(url, &s2).unwrap();

        let list = store.list_snapshots(url).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].content_hash, "hash2"); // newest first
        assert_eq!(list[1].content_hash, "hash1");
    }

    #[test]
    fn latest_snapshot() {
        let (_dir, store) = temp_store();
        let url = "postgres://user@host/mydb";

        let mut s1 = make_snapshot("old", "mydb");
        s1.timestamp = Utc::now() - chrono::Duration::hours(1);
        store.save_snapshot(url, &s1).unwrap();

        let s2 = make_snapshot("new", "mydb");
        store.save_snapshot(url, &s2).unwrap();

        let latest = store.latest_snapshot(url).unwrap().unwrap();
        assert_eq!(latest.content_hash, "new");
    }

    #[test]
    fn different_urls_isolated() {
        let (_dir, store) = temp_store();
        let url1 = "postgres://user@host/db1";
        let url2 = "postgres://user@host/db2";

        store.save_snapshot(url1, &make_snapshot("h1", "db1")).unwrap();
        store.save_snapshot(url2, &make_snapshot("h2", "db2")).unwrap();

        let list1 = store.list_snapshots(url1).unwrap();
        assert_eq!(list1.len(), 1);
        assert_eq!(list1[0].content_hash, "h1");

        let list2 = store.list_snapshots(url2).unwrap();
        assert_eq!(list2.len(), 1);
        assert_eq!(list2[0].content_hash, "h2");
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
            tables: vec![], enums: vec![], domains: vec![], composites: vec![],
            views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
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
            store.put(&billing, &make_snap("same", "billing")).await.unwrap(),
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
        assert!(store.get(&k, SnapshotRef::Hash("nope".into())).await.is_err());
        assert!(store
            .get(&k, SnapshotRef::At(Utc::now()))
            .await
            .is_err());
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
    async fn legacy_save_and_trait_put_coexist() {
        let (_dir, store) = temp_store();
        let url = "postgres://user@host/mydb";
        let k = key("p", "mydb");

        store.save_snapshot(url, &make_snap("legacy", "mydb")).unwrap();
        store.put(&k, &make_snap("trait_row", "mydb")).await.unwrap();

        // legacy listing finds only the legacy row
        let legacy = store.list_snapshots(url).unwrap();
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].content_hash, "legacy");
        assert!(legacy[0].project_id.is_none());

        // trait listing finds only the trait row
        let trait_rows = store.list(&k, TimeRange::default()).await.unwrap();
        assert_eq!(trait_rows.len(), 1);
        assert_eq!(trait_rows[0].content_hash, "trait_row");
        assert_eq!(trait_rows[0].project_id.as_deref(), Some("p"));
        assert_eq!(trait_rows[0].database_id.as_deref(), Some("mydb"));
    }

    #[tokio::test]
    async fn synthetic_db_url_hash_is_deterministic_and_disjoint() {
        let k1 = key("p", "auth");
        let k2 = key("p", "billing");

        // deterministic
        assert_eq!(synthetic_db_url_hash(&k1), synthetic_db_url_hash(&k1));
        // distinct keys produce distinct hashes
        assert_ne!(synthetic_db_url_hash(&k1), synthetic_db_url_hash(&k2));
        // disjoint from real URL hashes
        assert_ne!(
            synthetic_db_url_hash(&k1),
            hash_url("postgres://user@host/auth")
        );
    }
}
