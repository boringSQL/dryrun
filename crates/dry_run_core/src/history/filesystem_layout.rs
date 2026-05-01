use crate::history::SnapshotKey;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::path::{Path, PathBuf};

pub const SNAPSHOT_EXTENSION: &str = "json.zst";

const TS_FORMAT: &str = "%Y%m%dT%H%M%SZ";

#[must_use]
pub fn snapshot_path(
    root: &Path,
    key: &SnapshotKey,
    timestamp: DateTime<Utc>,
    content_hash: &str,
) -> PathBuf {
    root.join(&key.project_id.0)
        .join(&key.database_id.0)
        .join(format!(
            "{}-{}.{}",
            timestamp.format(TS_FORMAT),
            content_hash,
            SNAPSHOT_EXTENSION,
        ))
}

#[must_use]
pub fn parse_snapshot_filename(name: &str) -> Option<(DateTime<Utc>, String)> {
    let stem = name.strip_suffix(&format!(".{SNAPSHOT_EXTENSION}"))?;
    let (ts_str, hash) = stem.split_once('-')?;
    let naive = NaiveDateTime::parse_from_str(ts_str, TS_FORMAT).ok()?;
    let ts = Utc.from_utc_datetime(&naive);

    Some((ts, hash.to_string()))
}
