use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::schema::SchemaSnapshot;

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

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn put(&self, key: &SnapshotKey, snap: &SchemaSnapshot) -> Result<PutOutcome>;
    async fn get(&self, key: &SnapshotKey, at: SnapshotRef) -> Result<SchemaSnapshot>;
    async fn list(&self, key: &SnapshotKey, range: TimeRange) -> Result<Vec<SnapshotSummary>>;
    async fn latest(&self, key: &SnapshotKey) -> Result<Option<SnapshotSummary>>;
    async fn delete_before(&self, key: &SnapshotKey, cutoff: DateTime<Utc>) -> Result<usize>;
}
