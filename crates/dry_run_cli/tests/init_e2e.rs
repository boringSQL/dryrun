//! End-to-end test for `dryrun init --db <url>` against a real Postgres.
//!
//! Spins up Postgres via testcontainers, seeds a tiny schema, runs
//! ANALYZE so planner stats have something to capture, then invokes the
//! built `dryrun` binary in a tempdir. Verifies that the command writes
//! the config file, schema.json, and a history.db that contains schema,
//! planner, and activity rows.
//!
//! Requires Docker. Marked `#[ignore]` so it's skipped by default — run
//! explicitly with one of:
//!
//!   cargo test -p dry_run_cli --test init_e2e -- --ignored
//!   cargo test -p dry_run_cli -- --ignored             # all ignored tests
//!   cargo test --workspace -- --include-ignored        # everything

use std::path::PathBuf;
use std::process::Command;

use dry_run_core::history::{
    DatabaseId, HistoryStore, ProjectId, SnapshotKey, SnapshotRef, SnapshotStore, TimeRange,
};
use sqlx::Executor;
use sqlx::postgres::PgPoolOptions;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

const SEED_SQL: &str = r#"
CREATE TABLE widgets (
    widget_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX widgets_name_idx ON widgets(name);

INSERT INTO widgets (name)
SELECT 'w-' || g FROM generate_series(1, 50) g;

ANALYZE widgets;
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn init_full_capture_writes_schema_planner_and_activity() {
    // pin a modern PG: pg_introspect's catalog queries use views & columns
    // that don't exist in the testcontainers-modules default (postgres:11-alpine).
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres container (is Docker running?)");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");

    // testcontainers-modules postgres defaults: user=postgres, pw=postgres, db=postgres
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    seed(&url).await;

    let workdir = tempfile::tempdir().expect("tempdir");
    let dryrun_bin: PathBuf = env!("CARGO_BIN_EXE_dryrun").into();

    let output = Command::new(&dryrun_bin)
        .arg("init")
        .arg("--db")
        .arg(&url)
        .current_dir(workdir.path())
        // Don't let the developer's shell env (DATABASE_URL, HOME with a
        // global .dryrun/) leak into the subprocess and skew the test.
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", workdir.path())
        .output()
        .expect("spawn dryrun");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "dryrun init failed\nstatus: {}\nstdout: {stdout}\nstderr: {stderr}",
        output.status,
    );

    // stderr mentions all three captures
    assert!(
        stderr.contains("Captured schema:"),
        "missing schema capture line in stderr:\n{stderr}",
    );
    assert!(
        stderr.contains("Planner stats:"),
        "missing planner stats line in stderr:\n{stderr}",
    );
    assert!(
        stderr.contains("Activity stats:"),
        "missing activity stats line in stderr:\n{stderr}",
    );
    assert!(
        stderr.contains("label=primary"),
        "activity stats should be labelled primary:\n{stderr}",
    );

    // files on disk
    let config = workdir.path().join("dryrun.toml");
    assert!(config.exists(), "dryrun.toml not created");

    let data_dir = workdir.path().join(".dryrun");
    assert!(data_dir.exists(), ".dryrun/ not created");

    let schema_json = data_dir.join("schema.json");
    assert!(schema_json.exists(), "schema.json not written");
    let schema_text = std::fs::read_to_string(&schema_json).expect("read schema.json");
    let schema: dry_run_core::SchemaSnapshot =
        serde_json::from_str(&schema_text).expect("parse schema.json");
    assert!(
        schema.tables.iter().any(|t| t.name == "widgets"),
        "widgets table missing from schema.json (tables: {:?})",
        schema.tables.iter().map(|t| &t.name).collect::<Vec<_>>(),
    );

    let history_db = data_dir.join("history.db");
    assert!(history_db.exists(), "history.db not created");

    // round-trip the history db: schema + planner + activity should all be present
    let store = HistoryStore::open(&history_db).expect("open history.db");

    // project_id defaults to the cwd's basename
    let project_id = workdir
        .path()
        .file_name()
        .and_then(|n| n.to_str())
        .expect("tempdir name")
        .to_string();
    let key = SnapshotKey {
        project_id: ProjectId(project_id),
        database_id: DatabaseId("postgres".into()),
    };

    let summaries = store.list(&key, TimeRange::default()).await.expect("list");
    assert_eq!(
        summaries.len(),
        1,
        "expected exactly one schema snapshot, got {}",
        summaries.len(),
    );

    let annotated = store
        .get_annotated(&key, SnapshotRef::Latest)
        .await
        .expect("get_annotated");

    assert_eq!(annotated.schema.content_hash, schema.content_hash);
    assert!(
        annotated.planner.is_some(),
        "planner stats not stored in history.db",
    );
    let activity = annotated
        .activity_by_node
        .get("primary")
        .expect("primary activity row missing from history.db");
    assert_eq!(activity.node.label, "primary");
    assert!(
        !activity.node.is_standby,
        "init must capture against the primary",
    );
}

async fn seed(url: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect to seeded postgres");
    pool.execute(SEED_SQL).await.expect("seed schema");
    pool.close().await;
}
