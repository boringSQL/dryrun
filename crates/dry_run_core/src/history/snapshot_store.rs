use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schema::{ActivityStatsSnapshot, PlannerStatsSnapshot, SchemaSnapshot};

pub use super::store::SnapshotSummary;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatabaseId(pub String);

#[derive(Debug, Clone)]
pub struct SnapshotKey {
    pub project_id: ProjectId,
    pub database_id: DatabaseId,
}

#[derive(Debug, Clone)]
pub enum SnapshotRef {
    Latest,
    At(DateTime<Utc>),
    Hash(String),
}

#[derive(Debug, Clone, Default)]
pub struct TimeRange {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Inserted,
    Deduped,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SnapshotKind {
    Schema,
    Planner,
    Activity { node_label: String },
}

impl SnapshotKind {
    /// String stored in the SQLite `kind` column.
    #[must_use]
    pub fn db_kind(&self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Planner => "planner_stats",
            Self::Activity { .. } => "activity_stats",
        }
    }

    #[must_use]
    pub fn node_label(&self) -> Option<&str> {
        match self {
            Self::Activity { node_label } => Some(node_label.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoredSnapshot {
    Schema(SchemaSnapshot),
    Planner(PlannerStatsSnapshot),
    Activity(ActivityStatsSnapshot),
}

impl StoredSnapshot {
    #[must_use]
    pub fn kind(&self) -> SnapshotKind {
        match self {
            Self::Schema(_) => SnapshotKind::Schema,
            Self::Planner(_) => SnapshotKind::Planner,
            Self::Activity(a) => SnapshotKind::Activity {
                node_label: a.node.label.clone(),
            },
        }
    }

    #[must_use]
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Schema(s) => s.timestamp,
            Self::Planner(p) => p.timestamp,
            Self::Activity(a) => a.timestamp,
        }
    }

    #[must_use]
    pub fn content_hash(&self) -> &str {
        match self {
            Self::Schema(s) => &s.content_hash,
            Self::Planner(p) => &p.content_hash,
            Self::Activity(a) => &a.content_hash,
        }
    }

    #[must_use]
    pub fn schema_ref_hash(&self) -> Option<&str> {
        match self {
            Self::Schema(_) => None,
            Self::Planner(p) => Some(&p.schema_ref_hash),
            Self::Activity(a) => Some(&a.schema_ref_hash),
        }
    }

    #[must_use]
    pub fn database(&self) -> &str {
        match self {
            Self::Schema(s) => &s.database,
            Self::Planner(p) => &p.database,
            Self::Activity(a) => &a.database,
        }
    }

    pub fn into_schema(self) -> Result<SchemaSnapshot> {
        match self {
            Self::Schema(s) => Ok(s),
            other => Err(Error::History(format!(
                "expected schema snapshot, got {}",
                other.kind().db_kind()
            ))),
        }
    }

    pub fn into_planner(self) -> Result<PlannerStatsSnapshot> {
        match self {
            Self::Planner(p) => Ok(p),
            other => Err(Error::History(format!(
                "expected planner snapshot, got {}",
                other.kind().db_kind()
            ))),
        }
    }

    pub fn into_activity(self) -> Result<ActivityStatsSnapshot> {
        match self {
            Self::Activity(a) => Ok(a),
            other => Err(Error::History(format!(
                "expected activity snapshot, got {}",
                other.kind().db_kind()
            ))),
        }
    }
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn put(&self, key: &SnapshotKey, snap: &StoredSnapshot) -> Result<PutOutcome>;
    async fn get(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        at: SnapshotRef,
    ) -> Result<StoredSnapshot>;
    async fn list(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        range: TimeRange,
    ) -> Result<Vec<SnapshotSummary>>;
    async fn latest(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
    ) -> Result<Option<SnapshotSummary>>;
    async fn delete_before(
        &self,
        key: &SnapshotKey,
        kind: &SnapshotKind,
        cutoff: DateTime<Utc>,
    ) -> Result<usize>;
    async fn list_kinds(&self, key: &SnapshotKey) -> Result<Vec<SnapshotKind>>;

    // wrapper per schema kind
    async fn put_schema(&self, key: &SnapshotKey, snap: &SchemaSnapshot) -> Result<PutOutcome> {
        self.put(key, &StoredSnapshot::Schema(snap.clone())).await
    }

    async fn put_planner_stats(
        &self,
        key: &SnapshotKey,
        snap: &PlannerStatsSnapshot,
    ) -> Result<PutOutcome> {
        self.put(key, &StoredSnapshot::Planner(snap.clone())).await
    }

    async fn put_activity_stats(
        &self,
        key: &SnapshotKey,
        snap: &ActivityStatsSnapshot,
    ) -> Result<PutOutcome> {
        self.put(key, &StoredSnapshot::Activity(snap.clone())).await
    }

    async fn get_schema(&self, key: &SnapshotKey, at: SnapshotRef) -> Result<SchemaSnapshot> {
        self.get(key, &SnapshotKind::Schema, at)
            .await?
            .into_schema()
    }

    async fn list_schema(
        &self,
        key: &SnapshotKey,
        range: TimeRange,
    ) -> Result<Vec<SnapshotSummary>> {
        self.list(key, &SnapshotKind::Schema, range).await
    }

    async fn latest_schema(&self, key: &SnapshotKey) -> Result<Option<SnapshotSummary>> {
        self.latest(key, &SnapshotKind::Schema).await
    }

    async fn delete_schema_before(
        &self,
        key: &SnapshotKey,
        cutoff: DateTime<Utc>,
    ) -> Result<usize> {
        self.delete_before(key, &SnapshotKind::Schema, cutoff).await
    }
}
