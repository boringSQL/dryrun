use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tracing::{debug, info};

use crate::error::{Error, Result};
use crate::schema::{NodeStats, SchemaSnapshot};
use crate::version::PgVersion;

pub struct DryRun {
    pool: PgPool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub version: PgVersion,
    pub version_string: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivilegeReport {
    pub pg_catalog: bool,
    pub information_schema: bool,
    pub pg_stat_user_tables: bool,
}

impl DryRun {
    pub async fn connect(url: &str) -> Result<Self> {
        let opts: PgConnectOptions = url
            .parse()
            .map_err(|e: sqlx::Error| Error::Connection(e.to_string()))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await
            .map_err(|e| classify_sqlx_error(e, url))?;

        debug!("connected to PostgreSQL");
        Ok(Self { pool })
    }

    pub async fn probe(&self) -> Result<ProbeResult> {
        let version_string: String = sqlx::query_scalar("SELECT version()")
            .fetch_one(&self.pool)
            .await?;

        let version = PgVersion::parse_from_version_string(&version_string)?;
        info!(pg_version = %version, "probed PostgreSQL");

        Ok(ProbeResult {
            version,
            version_string,
        })
    }

    pub async fn check_privileges(&self) -> Result<PrivilegeReport> {
        let pg_catalog =
            check_access(&self.pool, "SELECT 1 FROM pg_catalog.pg_tables LIMIT 1").await;
        let information_schema = check_access(
            &self.pool,
            "SELECT 1 FROM information_schema.columns LIMIT 1",
        )
        .await;
        let pg_stat_user_tables =
            check_access(&self.pool, "SELECT 1 FROM pg_stat_user_tables LIMIT 1").await;

        let report = PrivilegeReport {
            pg_catalog,
            information_schema,
            pg_stat_user_tables,
        };
        info!(?report, "privilege check complete");
        Ok(report)
    }

    pub async fn introspect_schema(&self) -> Result<SchemaSnapshot> {
        crate::schema::introspect_schema(&self.pool).await
    }

    pub async fn introspect_stats_only(&self, source: &str) -> Result<NodeStats> {
        crate::schema::fetch_stats_only(&self.pool, source).await
    }

    pub async fn is_standby(&self) -> Result<bool> {
        crate::schema::fetch_is_standby(&self.pool).await
    }

    pub async fn current_database(&self) -> Result<String> {
        let dbname: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&self.pool)
            .await?;
        Ok(dbname)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

async fn check_access(pool: &PgPool, query: &str) -> bool {
    sqlx::query(query).fetch_optional(pool).await.is_ok()
}

fn classify_sqlx_error(err: sqlx::Error, url: &str) -> Error {
    match &err {
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().unwrap_or_default();
            match code.as_ref() {
                "28000" | "28P01" => Error::Auth(db_err.message().to_string()),
                "3D000" => Error::Connection(format!("database not found: {}", db_err.message())),
                _ => Error::Connection(format!("{db_err} (connecting to {url})")),
            }
        }
        _ => Error::Connection(err.to_string()),
    }
}
