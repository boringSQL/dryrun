use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::error::{Error, Result};
use crate::schema::SchemaSnapshot;

pub struct HistoryStore {
    conn: Connection,
}

#[derive(Debug, Clone)]
pub struct SnapshotSummary {
    pub id: i64,
    pub db_url_hash: String,
    pub timestamp: DateTime<Utc>,
    pub content_hash: String,
    pub database: String,
}

impl HistoryStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::History(format!("cannot create directory: {e}")))?;
        }

        let conn = Connection::open(path)
            .map_err(|e| Error::History(format!("cannot open history db: {e}")))?;

        let store = Self { conn };
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
        let db_url_hash = hash_url(db_url);

        let latest_hash: Option<String> = self
            .conn
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

        self.conn
            .execute(
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
        let json: Option<String> = self
            .conn
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
        let db_url_hash = hash_url(db_url);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, db_url_hash, timestamp, content_hash, database_name
                   FROM snapshots
                  WHERE db_url_hash = ?1
                  ORDER BY timestamp DESC",
            )
            .map_err(|e| Error::History(e.to_string()))?;

        let rows = stmt
            .query_map(params![db_url_hash], |row| {
                let ts_str: String = row.get(2)?;
                Ok(SnapshotSummary {
                    id: row.get(0)?,
                    db_url_hash: row.get(1)?,
                    timestamp: DateTime::parse_from_rfc3339(&ts_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_default(),
                    content_hash: row.get(3)?,
                    database: row.get(4)?,
                })
            })
            .map_err(|e| Error::History(e.to_string()))?;

        let mut summaries = Vec::new();
        for row in rows {
            summaries.push(row.map_err(|e| Error::History(e.to_string()))?);
        }
        Ok(summaries)
    }

    pub fn snapshots_since(
        &self,
        db_url: &str,
        since: DateTime<Utc>,
    ) -> Result<Vec<SchemaSnapshot>> {
        let db_url_hash = hash_url(db_url);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT snapshot_json FROM snapshots
                  WHERE db_url_hash = ?1 AND timestamp >= ?2
                  ORDER BY timestamp ASC",
            )
            .map_err(|e| Error::History(e.to_string()))?;

        let rows = stmt
            .query_map(params![db_url_hash, since.to_rfc3339()], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|e| Error::History(e.to_string()))?;

        let mut snapshots = Vec::new();
        for row in rows {
            let json = row.map_err(|e| Error::History(e.to_string()))?;
            let snapshot: SchemaSnapshot = serde_json::from_str(&json)
                .map_err(|e| Error::History(format!("corrupt snapshot JSON: {e}")))?;
            snapshots.push(snapshot);
        }
        Ok(snapshots)
    }

    pub fn latest_snapshot(&self, db_url: &str) -> Result<Option<SchemaSnapshot>> {
        let db_url_hash = hash_url(db_url);

        let json: Option<String> = self
            .conn
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
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS snapshots (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_url_hash   TEXT NOT NULL,
                    timestamp     TEXT NOT NULL,
                    content_hash  TEXT NOT NULL,
                    database_name TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
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
    Ok(cwd.join(".dry_run"))
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
