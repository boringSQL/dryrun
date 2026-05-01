mod mcp;
mod pgmustard;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use dry_run_core::history::{
    DatabaseId, PutOutcome, SnapshotKey, SnapshotRef, SnapshotStore, TimeRange,
};
use dry_run_core::schema::{NodeColumnStats, NodeIndexStats, NodeStats, NodeTableStats};
use dry_run_core::{DryRun, HistoryStore, ProjectConfig};
use rmcp::ServiceExt;

fn get_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[derive(Parser)]
#[command(name = "dryrun", version = get_version(), about = "PostgreSQL schema intelligence")]
struct Cli {
    #[arg(long)]
    profile: Option<String>,

    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
    },
    Import {
        file: PathBuf,
        #[arg(long, num_args = 1..)]
        stats: Vec<PathBuf>,
    },
    Probe {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
    },
    Lint {
        #[arg(long)]
        schema_name: Option<String>,
        #[arg(long)]
        pretty: bool,
        #[arg(long)]
        json: bool,
    },
    DumpSchema {
        #[arg(long, env = "SOURCE_DATABASE_URL")]
        source: Option<String>,
        #[arg(long)]
        pretty: bool,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(long)]
        stats_only: bool,
        #[arg(long)]
        name: Option<String>,
    },
    Snapshot {
        #[command(subcommand)]
        action: SnapshotAction,
    },
    Profile {
        #[command(subcommand)]
        action: ProfileAction,
    },
    Stats {
        #[command(subcommand)]
        action: StatsAction,
    },
    Drift {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long)]
        against: Option<PathBuf>,
        #[arg(long)]
        pretty: bool,
        #[arg(long)]
        json: bool,
    },
    McpServe {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long, env = "SCHEMA_FILE")]
        schema_file: Option<PathBuf>,
        #[arg(long, default_value = "stdio")]
        transport: String,
        #[arg(long, default_value = "3000")]
        port: u16,
    },
}

#[derive(Subcommand)]
enum StatsAction {
    Apply {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long, short)]
        schema_file: Option<PathBuf>,
        #[arg(long, short)]
        node: Option<String>,
    },
}

#[derive(Subcommand)]
enum SnapshotAction {
    Take {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long)]
        history_db: Option<PathBuf>,
    },
    List {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long)]
        history_db: Option<PathBuf>,
    },
    Diff {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        latest: bool,
        #[arg(long)]
        history_db: Option<PathBuf>,
        #[arg(long)]
        pretty: bool,
    },
    Export {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        history_db: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ProfileAction {
    List,
    Show { name: String },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Probe { ref db } => cmd_probe(&cli, db.as_deref()).await,
        Command::DumpSchema {
            ref source,
            pretty,
            ref output,
            stats_only,
            ref name,
        } => {
            cmd_dump_schema(
                &cli,
                source.as_deref(),
                pretty,
                output.clone(),
                stats_only,
                name.clone(),
            )
            .await
        }
        Command::Init { ref db } => cmd_init(db.as_deref()).await,
        Command::Import {
            ref file,
            ref stats,
        } => cmd_import(&cli, file, stats).await,
        Command::Lint {
            ref schema_name,
            pretty,
            json,
        } => cmd_lint(&cli, schema_name.as_deref(), pretty, json).await,
        Command::Snapshot { ref action } => cmd_snapshot(&cli, action).await,
        Command::Profile { ref action } => cmd_profile(&cli, action),
        Command::Stats { ref action } => cmd_stats(&cli, action).await,
        Command::Drift {
            ref db,
            ref against,
            pretty,
            json,
        } => cmd_drift(&cli, db.as_deref(), against.as_deref(), pretty, json).await,
        Command::McpServe {
            ref db,
            ref schema_file,
            ref transport,
            port,
        } => cmd_mcp_serve(&cli, db.as_deref(), schema_file.as_deref(), transport, port).await,
    }
}

async fn cmd_probe(cli: &Cli, db: Option<&str>) -> anyhow::Result<()> {
    let resolved = active_resolved_profile(cli, db, None)?;
    let db_url = resolved
        .db_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--db or a profile with db_url is required"))?;
    let ctx = DryRun::connect(db_url).await?;

    let result = ctx.probe().await?;
    println!("PostgreSQL {}", result.version);
    println!("  {}", result.version_string);

    let report = ctx.check_privileges().await?;
    println!("Privileges:");
    println!(
        "  pg_catalog:           {}",
        if report.pg_catalog { "ok" } else { "DENIED" }
    );
    println!(
        "  information_schema:   {}",
        if report.information_schema {
            "ok"
        } else {
            "DENIED"
        }
    );
    println!(
        "  pg_stat_user_tables:  {}",
        if report.pg_stat_user_tables {
            "ok"
        } else {
            "DENIED"
        }
    );
    Ok(())
}

async fn cmd_dump_schema(
    cli: &Cli,
    source: Option<&str>,
    pretty: bool,
    output: Option<PathBuf>,
    stats_only: bool,
    name: Option<String>,
) -> anyhow::Result<()> {
    let resolved = active_resolved_profile(cli, source, None)?;
    let db_url = resolved
        .db_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--source or a profile with db_url is required"))?;
    let name = name.or_else(|| resolved.database_id.as_ref().map(|d| d.0.clone()));
    let ctx = DryRun::connect(db_url).await?;

    if stats_only {
        let source =
            name.ok_or_else(|| anyhow::anyhow!("--name is required when using --stats-only"))?;
        let node_stats = ctx.introspect_stats_only(&source).await?;

        let json = if pretty {
            serde_json::to_string_pretty(&node_stats)?
        } else {
            serde_json::to_string(&node_stats)?
        };

        if let Some(path) = &output {
            std::fs::write(path, &json)?;
            eprintln!(
                "Stats written to {} ({} tables, {} indexes)",
                path.display(),
                node_stats.table_stats.len(),
                node_stats.index_stats.len()
            );
        } else {
            println!("{json}");
        }
        return Ok(());
    }

    let mut snapshot = ctx.introspect_schema().await?;
    snapshot.source = name;

    if let Some(ref source) = snapshot.source {
        let mut table_stats = Vec::new();
        let mut index_stats = Vec::new();
        let mut column_stats = Vec::new();

        for table in &snapshot.tables {
            if let Some(ref ts) = table.stats {
                table_stats.push(NodeTableStats {
                    schema: table.schema.clone(),
                    table: table.name.clone(),
                    stats: ts.clone(),
                });
            }
            for idx in &table.indexes {
                if let Some(ref is) = idx.stats {
                    index_stats.push(NodeIndexStats {
                        schema: table.schema.clone(),
                        table: table.name.clone(),
                        index_name: idx.name.clone(),
                        stats: is.clone(),
                    });
                }
            }
            for col in &table.columns {
                if let Some(ref cs) = col.stats {
                    column_stats.push(NodeColumnStats {
                        schema: table.schema.clone(),
                        table: table.name.clone(),
                        column: col.name.clone(),
                        stats: cs.clone(),
                    });
                }
            }
        }

        let is_standby = ctx.is_standby().await?;

        snapshot.node_stats = vec![NodeStats {
            source: source.clone(),
            timestamp: snapshot.timestamp,
            is_standby,
            table_stats,
            index_stats,
            column_stats,
        }];
    }

    let json = if pretty {
        serde_json::to_string_pretty(&snapshot)?
    } else {
        serde_json::to_string(&snapshot)?
    };

    if let Some(path) = &output {
        std::fs::write(path, &json)?;
        eprintln!("Schema written to {}", path.display());
    } else {
        println!("{json}");
    }
    Ok(())
}

async fn cmd_init(db: Option<&str>) -> anyhow::Result<()> {
    let config_path = PathBuf::from("dryrun.toml");
    let cwd = std::env::current_dir().unwrap_or_default();

    // scaffold config file
    if !config_path.exists() {
        let project_id = cwd
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("default");
        let profile_name = project_id;
        let content = format!(
            r#"[project]
id = "{project_id}"

[default]
profile = "{profile_name}"

[profiles.{profile_name}]
schema_file = ".dryrun/schema.json"
# database_id = "{profile_name}"   # defaults to profile name; override to e.g. "auth", "billing"

# [profiles.dev]
# db_url = "${{DATABASE_URL}}"
# database_id = "dev"

# [conventions]
# See: https://boringsql.com/dryrun/docs/dryrun-toml
"#
        );
        std::fs::write(&config_path, &content)?;
        eprintln!(
            "Created {} (profile \"{profile_name}\")",
            config_path.display()
        );
    } else {
        eprintln!("{} already exists, skipping", config_path.display());
    }

    // create .dryrun/ directory
    let data_dir = dry_run_core::history::default_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;

    // if --db is provided, introspect and save schema
    if let Some(db_url) = db {
        let ctx = DryRun::connect(db_url).await?;
        let snapshot = ctx.introspect_schema().await?;

        let schema_path = data_dir.join("schema.json");
        let json = serde_json::to_string_pretty(&snapshot)?;
        std::fs::write(&schema_path, &json)?;

        let store = open_history_store(None)?;
        let config = ProjectConfig::discover(&cwd)
            .map(|(_, c)| Ok(c))
            .unwrap_or_else(|| ProjectConfig::parse(""))?;
        let resolved = config.resolve_profile(Some(db_url), None, None, &cwd)?;
        let key = complete_key(&resolved, &snapshot.database);
        store.put(&key, &snapshot).await?;

        eprintln!(
            "Captured schema: {} tables, {} views, {} functions",
            snapshot.tables.len(),
            snapshot.views.len(),
            snapshot.functions.len()
        );
        eprintln!("  Schema: {}", schema_path.display());
        eprintln!(
            "  project={} database={}",
            key.project_id.0, key.database_id.0
        );
    } else {
        eprintln!("Run 'dryrun init --db <url>' to capture a schema snapshot");
    }

    Ok(())
}

async fn cmd_lint(
    cli: &Cli,
    schema_filter: Option<&str>,
    pretty: bool,
    json: bool,
) -> anyhow::Result<()> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let project_config = load_project_config(cli, &cwd);

    let snapshot = resolve_schema(None, project_config.as_ref(), cli.profile.as_deref())?;

    let snapshot = if let Some(filter) = schema_filter {
        let mut filtered = snapshot.clone();
        filtered.tables.retain(|t| t.schema == filter);
        filtered
    } else {
        snapshot
    };

    let lint_config = project_config
        .as_ref()
        .map(|c| c.lint_config())
        .unwrap_or_default();

    let report = dry_run_core::lint::lint_schema(&snapshot, &lint_config);

    if json {
        let output = if pretty {
            serde_json::to_string_pretty(&report)?
        } else {
            serde_json::to_string(&report)?
        };
        println!("{output}");
    } else {
        if report.violations.is_empty() {
            println!(
                "No lint violations found ({} tables checked).",
                report.tables_checked
            );
        } else {
            for v in &report.violations {
                let location = if let Some(col) = &v.column {
                    format!("{}.{}", v.table, col)
                } else {
                    v.table.clone()
                };
                let severity = match v.severity {
                    dry_run_core::lint::Severity::Error => "ERROR",
                    dry_run_core::lint::Severity::Warning => "WARN ",
                    dry_run_core::lint::Severity::Info => "INFO ",
                };
                println!("[{severity}] {location}: {}", v.message);
                println!("       fix: {}", v.recommendation);
            }
            println!();
            println!(
                "{} violation(s): {} error, {} warning, {} info ({} tables checked)",
                report.violations.len(),
                report.summary.errors,
                report.summary.warnings,
                report.summary.info,
                report.tables_checked,
            );
        }

        if report.summary.errors > 0 {
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn cmd_snapshot(cli: &Cli, action: &SnapshotAction) -> anyhow::Result<()> {
    let profile = cli.profile.as_deref();
    match action {
        SnapshotAction::Take { db, history_db } => {
            let db_url = require_db_url(db.as_deref())?;
            let ctx = DryRun::connect(db_url).await?;
            let store = open_history_store(history_db.as_deref())?;
            let snapshot = ctx.introspect_schema().await?;

            let cwd = std::env::current_dir().unwrap_or_default();
            let config = ProjectConfig::discover(&cwd)
                .map(|(_, c)| Ok(c))
                .unwrap_or_else(|| ProjectConfig::parse(""))?;
            let resolved = config.resolve_profile(Some(db_url), None, profile, &cwd)?;
            let key = complete_key(&resolved, &snapshot.database);

            match store.put(&key, &snapshot).await? {
                PutOutcome::Inserted => {
                    println!("Snapshot saved: {}", snapshot.content_hash);
                    println!(
                        "  {} tables, {} views, {} functions",
                        snapshot.tables.len(),
                        snapshot.views.len(),
                        snapshot.functions.len()
                    );
                    println!(
                        "  project={} database={}",
                        key.project_id.0, key.database_id.0
                    );
                }
                PutOutcome::Deduped => {
                    println!("Schema unchanged (hash: {})", snapshot.content_hash);
                    println!(
                        "  project={} database={}",
                        key.project_id.0, key.database_id.0
                    );
                }
            }
            Ok(())
        }
        SnapshotAction::List { db, history_db } => {
            let store = open_history_store(history_db.as_deref())?;
            let key = resolve_read_key(db.as_deref(), profile).await?;
            let rows = store.list(&key, TimeRange::default()).await?;

            if rows.is_empty() {
                println!(
                    "No snapshots found (project={} database={})",
                    key.project_id.0, key.database_id.0
                );
            } else {
                for s in &rows {
                    println!(
                        "{}  {}  {}",
                        s.timestamp.format("%Y-%m-%d %H:%M:%S"),
                        &s.content_hash[..16.min(s.content_hash.len())],
                        s.database,
                    );
                }
                println!(
                    "\n{} snapshot(s) total (project={} database={})",
                    rows.len(),
                    key.project_id.0,
                    key.database_id.0
                );
            }
            Ok(())
        }
        SnapshotAction::Diff {
            db,
            from,
            to,
            latest,
            history_db,
            pretty,
        } => {
            let db_url = require_db_url(db.as_deref())?;
            let ctx = DryRun::connect(db_url).await?;
            let store = open_history_store(history_db.as_deref())?;
            let key = resolve_read_key(Some(db_url), profile).await?;

            let from_snapshot = if let Some(hash) = &from {
                store.get(&key, SnapshotRef::Hash(hash.clone())).await?
            } else if *latest {
                store.get(&key, SnapshotRef::Latest).await?
            } else {
                anyhow::bail!("specify --from <hash> or --latest");
            };

            let to_snapshot = if let Some(hash) = &to {
                store.get(&key, SnapshotRef::Hash(hash.clone())).await?
            } else {
                ctx.introspect_schema().await?
            };

            let changeset = dry_run_core::diff::diff_schemas(&from_snapshot, &to_snapshot);
            let json = if *pretty {
                serde_json::to_string_pretty(&changeset)?
            } else {
                serde_json::to_string(&changeset)?
            };
            println!("{json}");
            Ok(())
        }
        SnapshotAction::Export { out, history_db } => {
            let store = open_history_store(history_db.as_deref())?;
            let out_root = out.clone().unwrap_or_else(|| {
                dry_run_core::history::default_data_dir()
                    .map(|d| d.join("snapshots"))
                    .unwrap_or_else(|_| PathBuf::from(".dryrun/snapshots"))
            });

            let keys = store.list_keys()?;
            let mut written = 0usize;
            for key in &keys {
                let summaries = store.list(key, TimeRange::default()).await?;
                for s in &summaries {
                    let snap = store
                        .get(key, SnapshotRef::Hash(s.content_hash.clone()))
                        .await?;
                    write_snapshot_export(&out_root, key, &snap)?;
                    written += 1;
                }
            }
            println!(
                "Exported {written} snapshot(s) from {} stream(s) to {}",
                keys.len(),
                out_root.display(),
            );
            Ok(())
        }
    }
}

fn cmd_profile(cli: &Cli, action: &ProfileAction) -> anyhow::Result<()> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let (config_path, config) = if let Some(config_path) = &cli.config {
        let config = ProjectConfig::load(config_path)?;
        (config_path.clone(), config)
    } else {
        ProjectConfig::discover(&cwd).ok_or_else(|| anyhow::anyhow!("no dryrun.toml found"))?
    };

    match action {
        ProfileAction::List => {
            println!("Config: {}", config_path.display());
            if let Some(default) = &config.default
                && let Some(profile) = &default.profile
            {
                println!("Default profile: {profile}");
            }
            println!();

            if config.profiles.is_empty() {
                println!("No profiles defined.");
            } else {
                for (name, profile) in &config.profiles {
                    let source = if profile.db_url.is_some() {
                        "db_url"
                    } else if profile.schema_file.is_some() {
                        "schema_file"
                    } else {
                        "empty"
                    };
                    println!("  {name} ({source})");
                }
            }
        }
        ProfileAction::Show { name } => {
            let profile = config
                .profiles
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("profile '{name}' not found"))?;
            println!("Profile: {name}");
            if let Some(url) = &profile.db_url {
                println!("  db_url: {url}");
            }
            if let Some(file) = &profile.schema_file {
                println!("  schema_file: {file}");
            }
        }
    }
    Ok(())
}

async fn cmd_import(
    cli: &Cli,
    file: &std::path::Path,
    stats_files: &[PathBuf],
) -> anyhow::Result<()> {
    let json = std::fs::read_to_string(file)?;
    let mut snapshot: dry_run_core::SchemaSnapshot = serde_json::from_str(&json)
        .map_err(|e| anyhow::anyhow!("invalid schema JSON in '{}': {e}", file.display()))?;

    if !stats_files.is_empty() {
        for stats_path in stats_files {
            let stats_json = std::fs::read_to_string(stats_path)?;
            let node_stats: dry_run_core::NodeStats =
                serde_json::from_str(&stats_json).map_err(|e| {
                    anyhow::anyhow!("invalid stats JSON in '{}': {e}", stats_path.display())
                })?;
            eprintln!(
                "  merging stats from '{}' ({} tables, {} indexes)",
                node_stats.source,
                node_stats.table_stats.len(),
                node_stats.index_stats.len()
            );
            snapshot.node_stats.push(node_stats);
        }
    }

    let data_dir = dry_run_core::history::default_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;

    // route to the resolved profile's schema_file when one is configured;
    // fall back to .dryrun/schema.json
    let out_path = active_resolved_profile(cli, None, None)
        .ok()
        .and_then(|r| r.schema_file)
        .unwrap_or_else(|| data_dir.join("schema.json"));
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let out_json = serde_json::to_string_pretty(&snapshot)?;
    std::fs::write(&out_path, &out_json)?;

    eprintln!(
        "Imported {} tables to {}{}",
        snapshot.tables.len(),
        out_path.display(),
        if snapshot.node_stats.is_empty() {
            String::new()
        } else {
            format!(" (with {} node stats)", snapshot.node_stats.len())
        }
    );
    Ok(())
}

async fn cmd_stats(cli: &Cli, action: &StatsAction) -> anyhow::Result<()> {
    match action {
        StatsAction::Apply {
            db,
            schema_file,
            node,
        } => {
            let resolved = active_resolved_profile(cli, db.as_deref(), schema_file.as_deref())?;
            let db_url = resolved
                .db_url
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--db or a profile with db_url is required"))?;

            let snapshot = match resolved.schema_file.as_deref() {
                Some(path) => load_schema_file(path)?,
                None => resolve_schema(schema_file.as_deref(), None, None)?,
            };

            let ctx = DryRun::connect(db_url).await?;

            let result =
                dry_run_core::schema::apply_stats(ctx.pool(), &snapshot, node.as_deref()).await?;

            // pg_regresql warning
            if !result.regresql_loaded {
                eprintln!();
                eprintln!("  pg_regresql extension is not loaded.");
                eprintln!("  Without it, PostgreSQL ignores pg_class.reltuples/relpages and uses");
                eprintln!("  physical file sizes instead. Your injected row counts will have no");
                eprintln!("  effect on EXPLAIN cost estimates.");
                eprintln!();
                eprintln!("  Install: sudo pgxn install pg_regresql");
                eprintln!("  Then:    CREATE EXTENSION pg_regresql;");
                eprintln!("  See:     https://github.com/boringSQL/regresql");
                eprintln!();
            }

            eprintln!(
                "Applied: {} tables, {} indexes, {} columns",
                result.tables_updated, result.indexes_updated, result.columns_injected
            );

            if !result.skipped.is_empty() {
                eprintln!("Skipped ({}):", result.skipped.len());
                for s in &result.skipped {
                    eprintln!("  {s}");
                }
            }

            Ok(())
        }
    }
}

async fn cmd_drift(
    cli: &Cli,
    db: Option<&str>,
    against: Option<&std::path::Path>,
    pretty: bool,
    json: bool,
) -> anyhow::Result<()> {
    let resolved = active_resolved_profile(cli, db, against)?;
    let db_url = resolved
        .db_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--db or a profile with db_url is required"))?;

    let prod_snapshot = match resolved.schema_file.as_deref() {
        Some(path) => load_schema_file(path)?,
        None => resolve_schema(against, None, None)?,
    };

    let ctx = DryRun::connect(db_url).await?;
    let local_snapshot = ctx.introspect_schema().await?;

    let report = dry_run_core::diff::classify_drift(&prod_snapshot, &local_snapshot);

    if json {
        let output = if pretty {
            serde_json::to_string_pretty(&report)?
        } else {
            serde_json::to_string(&report)?
        };
        println!("{output}");
    } else {
        if report.entries.is_empty() {
            println!("No drift detected. Local DB matches the snapshot.");
        } else {
            for entry in &report.entries {
                let arrow = match entry.direction {
                    dry_run_core::diff::DriftDirection::Ahead => "AHEAD",
                    dry_run_core::diff::DriftDirection::Behind => "BEHIND",
                    dry_run_core::diff::DriftDirection::Diverged => "DIVERGED",
                };
                let location = entry
                    .change
                    .schema
                    .as_deref()
                    .map_or(entry.change.name.clone(), |s| {
                        format!("{s}.{}", entry.change.name)
                    });
                println!("[{arrow:>8}] {}: {location}", entry.change.object_type);
                for detail in &entry.change.details {
                    println!("           {detail}");
                }
            }
            println!();
            println!(
                "{} difference(s): {} ahead, {} behind, {} diverged",
                report.entries.len(),
                report.summary.ahead,
                report.summary.behind,
                report.summary.diverged,
            );
        }
    }
    Ok(())
}

// helpers

fn require_db_url(db: Option<&str>) -> anyhow::Result<&str> {
    db.ok_or_else(|| anyhow::anyhow!("--db or DATABASE_URL is required"))
}

fn active_resolved_profile(
    cli: &Cli,
    cli_db: Option<&str>,
    cli_schema: Option<&std::path::Path>,
) -> anyhow::Result<dry_run_core::ResolvedProfile> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let config = ProjectConfig::discover(&cwd)
        .map(|(_, c)| Ok(c))
        .unwrap_or_else(|| ProjectConfig::parse(""))?;
    Ok(config.resolve_profile(cli_db, cli_schema, cli.profile.as_deref(), &cwd)?)
}

fn load_project_config(cli: &Cli, cwd: &std::path::Path) -> Option<ProjectConfig> {
    if let Some(config_path) = &cli.config {
        ProjectConfig::load(config_path).ok()
    } else {
        ProjectConfig::discover(cwd).map(|(_, c)| c)
    }
}

/// Returns the ordered list of paths where a schema file might live,
/// without checking whether any of them actually exist.
fn schema_candidate_paths(
    schema_file: Option<&std::path::Path>,
    project_config: Option<&ProjectConfig>,
    profile: Option<&str>,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(path) = schema_file {
        candidates.push(path.to_path_buf());
    }

    let cwd = std::env::current_dir().unwrap_or_default();

    if let Some(config) = project_config
        && let Ok(resolved) = config.resolve_profile(None, None, profile, &cwd)
        && let Some(sf) = resolved.schema_file
    {
        candidates.push(sf);
    }

    if let Ok(data_dir) = dry_run_core::history::default_data_dir() {
        candidates.push(data_dir.join("schema.json"));
    }

    candidates
}

fn resolve_schema_path(
    schema_file: Option<&std::path::Path>,
    project_config: Option<&ProjectConfig>,
    profile: Option<&str>,
) -> anyhow::Result<PathBuf> {
    schema_candidate_paths(schema_file, project_config, profile)
        .into_iter()
        .find(|p| p.exists())
        .ok_or_else(|| {
            anyhow::anyhow!("no schema found — run dump-schema first or pass --schema-file")
        })
}

fn resolve_schema(
    schema_file: Option<&std::path::Path>,
    project_config: Option<&ProjectConfig>,
    profile: Option<&str>,
) -> anyhow::Result<dry_run_core::SchemaSnapshot> {
    let path = resolve_schema_path(schema_file, project_config, profile)?;
    load_schema_file(&path)
}

fn load_schema_file(path: &std::path::Path) -> anyhow::Result<dry_run_core::SchemaSnapshot> {
    let json = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&json)?)
}

fn open_history_store(path: Option<&std::path::Path>) -> anyhow::Result<HistoryStore> {
    let store = if let Some(p) = path {
        HistoryStore::open(p)?
    } else {
        HistoryStore::open_default()?
    };
    Ok(store)
}

// completes a SnapshotKey from a resolved profile; falls back to snapshot.database
// when the profile didn't declare a database_id (the <cli>/<auto> case).
fn write_snapshot_export(
    out_root: &std::path::Path,
    key: &SnapshotKey,
    snap: &dry_run_core::SchemaSnapshot,
) -> anyhow::Result<PathBuf> {
    let path = out_root
        .join(&key.project_id.0)
        .join(&key.database_id.0)
        .join(format!(
            "{}-{}.json.zst",
            snap.timestamp.format("%Y%m%dT%H%M%SZ"),
            snap.content_hash,
        ));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_vec(snap)?;
    let compressed = zstd::encode_all(json.as_slice(), 3)?;
    std::fs::write(&path, compressed)?;
    Ok(path)
}

fn complete_key(resolved: &dry_run_core::ResolvedProfile, snapshot_database: &str) -> SnapshotKey {
    SnapshotKey {
        project_id: resolved.project_id.clone(),
        database_id: resolved
            .database_id
            .clone()
            .unwrap_or_else(|| DatabaseId(snapshot_database.to_string())),
    }
}

async fn resolve_read_key(
    db_url: Option<&str>,
    profile: Option<&str>,
) -> anyhow::Result<SnapshotKey> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let config = ProjectConfig::discover(&cwd)
        .map(|(_, c)| Ok(c))
        .unwrap_or_else(|| ProjectConfig::parse(""))?;
    let resolved = config.resolve_profile(db_url, None, profile, &cwd)?;

    if let Some(database_id) = resolved.database_id {
        return Ok(SnapshotKey {
            project_id: resolved.project_id,
            database_id,
        });
    }

    let url = resolved
        .db_url
        .ok_or_else(|| anyhow::anyhow!("no profile and no --db; cannot determine snapshot key"))?;
    let ctx = DryRun::connect(&url).await?;
    let dbname = ctx.current_database().await?;
    Ok(SnapshotKey {
        project_id: resolved.project_id,
        database_id: DatabaseId(dbname),
    })
}

async fn cmd_mcp_serve(
    cli: &Cli,
    db: Option<&str>,
    schema_path: Option<&std::path::Path>,
    transport: &str,
    port: u16,
) -> anyhow::Result<()> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let project_config = load_project_config(cli, &cwd);

    let lint_config = project_config
        .as_ref()
        .map(|c| c.lint_config())
        .unwrap_or_default();

    let pgmustard_api_key = project_config.as_ref().and_then(|c| c.pgmustard_api_key());

    let candidates =
        schema_candidate_paths(schema_path, project_config.as_ref(), cli.profile.as_deref());

    // try to load schema — if missing, start in uninitialized mode;
    // if file exists but is broken, propagate the error
    let schema_path_result =
        resolve_schema_path(schema_path, project_config.as_ref(), cli.profile.as_deref());

    let server = match schema_path_result {
        Ok(schema_file) => {
            let json = std::fs::read_to_string(&schema_file)?;
            let snapshot: dry_run_core::SchemaSnapshot = serde_json::from_str(&json)?;
            eprintln!(
                "dryrun: loaded schema from {} ({} tables)",
                schema_file.display(),
                snapshot.tables.len()
            );

            // optional --db enables live tools (explain_query, refresh_schema)
            let effective_db = db.map(|s| s.to_string()).or_else(|| {
                if let Some(ref config) = project_config
                    && let Ok(resolved) =
                        config.resolve_profile(None, None, cli.profile.as_deref(), &cwd)
                {
                    return resolved.db_url;
                }
                None
            });

            let db_connection = if let Some(ref db_url) = effective_db {
                let ctx = DryRun::connect(db_url).await?;
                eprintln!("dryrun: connected to local db (live tools enabled)");
                Some((db_url.as_str(), ctx))
            } else {
                eprintln!("dryrun: offline mode (explain_query, refresh_schema disabled)");
                None
            };

            mcp::DryRunServer::from_snapshot_with_db(
                snapshot,
                db_connection,
                lint_config,
                pgmustard_api_key,
                get_version(),
                candidates,
            )
        }
        Err(_) => {
            eprintln!(
                "dryrun: no schema found — starting in uninitialized mode\n\
                 dryrun: use the reload_schema tool after running dump-schema"
            );
            mcp::DryRunServer::uninitialized(lint_config, get_version(), candidates)
        }
    };

    match transport {
        "stdio" => {
            eprintln!("dryrun: starting MCP server on stdio");
            let service = server.serve(rmcp::transport::stdio()).await?;
            service.waiting().await?;
        }
        "sse" => {
            let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse()?;
            let sse_server = rmcp::transport::sse_server::SseServer::serve(bind_addr).await?;
            eprintln!("dryrun: SSE server listening on http://{bind_addr}/sse");
            let ct = sse_server.config.ct.clone();
            sse_server.with_service(move || server.clone());
            ct.cancelled().await;
        }
        other => {
            anyhow::bail!("unknown transport '{other}' (expected: stdio, sse)");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use dry_run_core::history::{DatabaseId, ProjectId};
    use dry_run_core::{ResolvedProfile, SchemaSnapshot};
    use tempfile::TempDir;

    fn make_snap(hash: &str, database: &str) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: database.into(),
            timestamp: Utc.with_ymd_and_hms(2026, 4, 30, 14, 22, 11).unwrap(),
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
            node_stats: vec![],
        }
    }

    fn key(proj: &str, db: &str) -> SnapshotKey {
        SnapshotKey {
            project_id: ProjectId(proj.into()),
            database_id: DatabaseId(db.into()),
        }
    }

    #[test]
    fn complete_key_uses_resolved_database_id_when_set() {
        let resolved = ResolvedProfile {
            name: "prod".into(),
            db_url: None,
            schema_file: None,
            project_id: ProjectId("clusterity".into()),
            database_id: Some(DatabaseId("auth".into())),
        };
        let key = complete_key(&resolved, "fallback_db");
        assert_eq!(key.project_id.0, "clusterity");
        assert_eq!(key.database_id.0, "auth");
    }

    #[test]
    fn complete_key_falls_back_to_snapshot_database() {
        let resolved = ResolvedProfile {
            name: "<cli>".into(),
            db_url: None,
            schema_file: None,
            project_id: ProjectId("myproj".into()),
            database_id: None,
        };
        let key = complete_key(&resolved, "actual_db");
        assert_eq!(key.project_id.0, "myproj");
        assert_eq!(key.database_id.0, "actual_db");
    }

    #[test]
    fn write_snapshot_export_roundtrips() {
        let dir = TempDir::new().unwrap();
        let k = key("myproj", "auth");
        let snap = make_snap("abc123def456", "auth");

        let path = write_snapshot_export(dir.path(), &k, &snap).unwrap();

        // path layout
        let expected = dir
            .path()
            .join("myproj")
            .join("auth")
            .join("20260430T142211Z-abc123def456.json.zst");
        assert_eq!(path, expected);
        assert!(path.exists());

        // round-trip: decompress and parse
        let bytes = std::fs::read(&path).unwrap();
        let json = zstd::decode_all(bytes.as_slice()).unwrap();
        let restored: SchemaSnapshot = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.content_hash, "abc123def456");
        assert_eq!(restored.database, "auth");
    }

    #[test]
    fn schema_candidate_paths_explicit_first_then_profile_then_default() {
        // explicit --schema-file path goes first; then resolved profile's path;
        // the default-data-dir fallback is appended last
        let toml = r#"
[profiles.dev]
schema_file = "from-profile.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let explicit = PathBuf::from("/tmp/explicit.json");
        let candidates = schema_candidate_paths(Some(&explicit), Some(&config), Some("dev"));
        assert!(candidates.len() >= 2);
        assert_eq!(candidates[0], explicit);
        // second candidate is the resolved profile path (relative to cwd)
        let cwd = std::env::current_dir().unwrap_or_default();
        assert_eq!(candidates[1], cwd.join("from-profile.json"));
    }

    #[test]
    fn schema_candidate_paths_no_inputs_still_includes_default_dir() {
        let candidates = schema_candidate_paths(None, None, None);
        // expect at least the default data-dir fallback
        assert!(!candidates.is_empty());
        assert!(candidates.last().unwrap().ends_with(".dryrun/schema.json"));
    }

    #[test]
    fn resolve_schema_path_picks_first_existing() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing.json");
        let present = dir.path().join("present.json");
        std::fs::write(&present, "{}").unwrap();

        // explicit path that doesn't exist; profile-resolved path that does
        let toml = format!("[profiles.dev]\nschema_file = \"{}\"\n", present.display());
        let config = ProjectConfig::parse(&toml).unwrap();
        let resolved = resolve_schema_path(Some(&missing), Some(&config), Some("dev")).unwrap();
        assert_eq!(resolved, present);
    }

    #[test]
    fn resolve_schema_path_errors_when_nothing_exists() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("nope.json");
        let result = resolve_schema_path(Some(&missing), None, None);
        assert!(result.is_err());
    }

    #[test]
    fn load_schema_file_round_trips() {
        let dir = TempDir::new().unwrap();
        let snap = make_snap("h1", "auth");
        let path = dir.path().join("schema.json");
        std::fs::write(&path, serde_json::to_string(&snap).unwrap()).unwrap();
        let restored = load_schema_file(&path).unwrap();
        assert_eq!(restored.content_hash, "h1");
        assert_eq!(restored.database, "auth");
    }

    #[test]
    fn load_schema_file_errors_on_invalid_json() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("broken.json");
        std::fs::write(&path, "{not json").unwrap();
        assert!(load_schema_file(&path).is_err());
    }

    #[test]
    fn write_snapshot_export_isolates_streams() {
        let dir = TempDir::new().unwrap();
        let auth = key("p", "auth");
        let billing = key("p", "billing");

        write_snapshot_export(dir.path(), &auth, &make_snap("h1", "auth")).unwrap();
        write_snapshot_export(dir.path(), &billing, &make_snap("h2", "billing")).unwrap();

        assert!(dir.path().join("p/auth").is_dir());
        assert!(dir.path().join("p/billing").is_dir());
        let auth_files: Vec<_> = std::fs::read_dir(dir.path().join("p/auth"))
            .unwrap()
            .collect();
        let billing_files: Vec<_> = std::fs::read_dir(dir.path().join("p/billing"))
            .unwrap()
            .collect();
        assert_eq!(auth_files.len(), 1);
        assert_eq!(billing_files.len(), 1);
    }
}
