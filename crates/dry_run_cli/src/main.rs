mod mcp;
mod pgmustard;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use dry_run_core::schema::{NodeColumnStats, NodeIndexStats, NodeStats, NodeTableStats};
use dry_run_core::{DryRun, HistoryStore, ProjectConfig};
use rmcp::ServiceExt;

fn get_version() -> &'static str {
    option_env!("DRY_RUN_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))
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
        Command::Probe { ref db } => cmd_probe(db.as_deref()).await,
        Command::DumpSchema { ref source, pretty, ref output, stats_only, ref name } => {
            cmd_dump_schema(source.as_deref(), pretty, output.clone(), stats_only, name.clone()).await
        }
        Command::Init { ref db } => cmd_init(db.as_deref()).await,
        Command::Import { ref file, ref stats } => cmd_import(file, stats).await,
        Command::Lint {
            ref schema_name,
            pretty,
            json,
        } => cmd_lint(&cli, schema_name.as_deref(), pretty, json).await,
        Command::Snapshot { ref action } => cmd_snapshot(action).await,
        Command::Profile { ref action } => cmd_profile(&cli, action),
        Command::Stats { ref action } => cmd_stats(action).await,
        Command::Drift { ref db, ref against, pretty, json } => {
            cmd_drift(db.as_deref(), against.as_deref(), pretty, json).await
        }
        Command::McpServe { ref db, ref schema_file, ref transport, port } => {
            cmd_mcp_serve(&cli, db.as_deref(), schema_file.as_deref(), transport, port).await
        }
    }
}

async fn cmd_probe(db: Option<&str>) -> anyhow::Result<()> {
    let db_url = require_db_url(db)?;
    let ctx = DryRun::connect(&db_url).await?;

    let result = ctx.probe().await?;
    println!("PostgreSQL {}", result.version);
    println!("  {}", result.version_string);

    let report = ctx.check_privileges().await?;
    println!("Privileges:");
    println!("  pg_catalog:           {}", if report.pg_catalog { "ok" } else { "DENIED" });
    println!("  information_schema:   {}", if report.information_schema { "ok" } else { "DENIED" });
    println!("  pg_stat_user_tables:  {}", if report.pg_stat_user_tables { "ok" } else { "DENIED" });
    Ok(())
}

async fn cmd_dump_schema(
    source: Option<&str>,
    pretty: bool,
    output: Option<PathBuf>,
    stats_only: bool,
    name: Option<String>,
) -> anyhow::Result<()> {
    let db_url = require_db_url(source)?;
    let ctx = DryRun::connect(&db_url).await?;

    if stats_only {
        let source = name.ok_or_else(|| {
            anyhow::anyhow!("--name is required when using --stats-only")
        })?;
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

    // scaffold config file
    if !config_path.exists() {
        let cwd = std::env::current_dir().unwrap_or_default();
        let profile_name = cwd
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("default");
        let content = format!(
            r#"[default]
profile = "{profile_name}"

[profiles.{profile_name}]
schema_file = ".dryrun/schema.json"

# [profiles.dev]
# db_url = "${{DATABASE_URL}}"

# [conventions]
# See: https://boringsql.com/dryrun/docs/dryrun-toml
"#
        );
        std::fs::write(&config_path, &content)?;
        eprintln!("Created {} (profile \"{profile_name}\")", config_path.display());
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
        if let Err(e) = store.save_snapshot(db_url, &snapshot) {
            eprintln!("warning: could not save snapshot: {e}");
        }

        eprintln!(
            "Captured schema: {} tables, {} views, {} functions",
            snapshot.tables.len(),
            snapshot.views.len(),
            snapshot.functions.len()
        );
        eprintln!("  Schema: {}", schema_path.display());
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
            println!("No lint violations found ({} tables checked).", report.tables_checked);
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

async fn cmd_snapshot(action: &SnapshotAction) -> anyhow::Result<()> {
    match action {
        SnapshotAction::Take { db, history_db } => {
            let db_url = require_db_url(db.as_deref())?;
            let ctx = DryRun::connect(&db_url).await?;
            let store = open_history_store(history_db.as_deref())?;
            let snapshot = ctx.introspect_schema().await?;

            match store.save_snapshot(&db_url, &snapshot)? {
                true => {
                    println!("Snapshot saved: {}", snapshot.content_hash);
                    println!(
                        "  {} tables, {} views, {} functions",
                        snapshot.tables.len(), snapshot.views.len(), snapshot.functions.len()
                    );
                }
                false => {
                    println!("Schema unchanged (hash: {})", snapshot.content_hash);
                }
            }
            Ok(())
        }
        SnapshotAction::List { db, history_db } => {
            let db_url = require_db_url(db.as_deref())?;
            let store = open_history_store(history_db.as_deref())?;
            let snapshots = store.list_snapshots(&db_url)?;

            if snapshots.is_empty() {
                println!("No snapshots found for this database.");
            } else {
                for s in &snapshots {
                    println!(
                        "{}  {}  {}",
                        s.timestamp.format("%Y-%m-%d %H:%M:%S"),
                        &s.content_hash[..16.min(s.content_hash.len())],
                        s.database,
                    );
                }
                println!("\n{} snapshot(s) total", snapshots.len());
            }
            Ok(())
        }
        SnapshotAction::Diff {
            db, from, to, latest, history_db, pretty,
        } => {
            let db_url = require_db_url(db.as_deref())?;
            let ctx = DryRun::connect(&db_url).await?;
            let store = open_history_store(history_db.as_deref())?;

            let from_snapshot = if let Some(hash) = &from {
                store.load_snapshot(hash)?
                    .ok_or_else(|| anyhow::anyhow!("snapshot with hash '{hash}' not found"))?
            } else if *latest {
                store.latest_snapshot(&db_url)?
                    .ok_or_else(|| anyhow::anyhow!("no saved snapshots found for this database"))?
            } else {
                anyhow::bail!("specify --from <hash> or --latest");
            };

            let to_snapshot = if let Some(hash) = &to {
                store.load_snapshot(hash)?
                    .ok_or_else(|| anyhow::anyhow!("snapshot with hash '{hash}' not found"))?
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
    }
}

fn cmd_profile(cli: &Cli, action: &ProfileAction) -> anyhow::Result<()> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let (config_path, config) = if let Some(config_path) = &cli.config {
        let config = ProjectConfig::load(config_path)?;
        (config_path.clone(), config)
    } else {
        ProjectConfig::discover(&cwd)
            .ok_or_else(|| anyhow::anyhow!("no dryrun.toml found"))?
    };

    match action {
        ProfileAction::List => {
            println!("Config: {}", config_path.display());
            if let Some(default) = &config.default {
                if let Some(profile) = &default.profile {
                    println!("Default profile: {profile}");
                }
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
            let profile = config.profiles.get(name)
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

async fn cmd_import(file: &std::path::Path, stats_files: &[PathBuf]) -> anyhow::Result<()> {
    let json = std::fs::read_to_string(file)?;
    let mut snapshot: dry_run_core::SchemaSnapshot = serde_json::from_str(&json)
        .map_err(|e| anyhow::anyhow!("invalid schema JSON in '{}': {e}", file.display()))?;

    if !stats_files.is_empty() {
        for stats_path in stats_files {
            let stats_json = std::fs::read_to_string(stats_path)?;
            let node_stats: dry_run_core::NodeStats = serde_json::from_str(&stats_json)
                .map_err(|e| anyhow::anyhow!(
                    "invalid stats JSON in '{}': {e}",
                    stats_path.display()
                ))?;
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

    let out_path = data_dir.join("schema.json");
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

async fn cmd_stats(action: &StatsAction) -> anyhow::Result<()> {
    match action {
        StatsAction::Apply { db, schema_file, node } => {
            let db_url = require_db_url(db.as_deref())?;

            let snapshot = resolve_schema(schema_file.as_deref(), None, None)?;

            let ctx = DryRun::connect(&db_url).await?;

            let result = dry_run_core::schema::apply_stats(
                ctx.pool(),
                &snapshot,
                node.as_deref(),
            )
            .await?;

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
    db: Option<&str>,
    against: Option<&std::path::Path>,
    pretty: bool,
    json: bool,
) -> anyhow::Result<()> {
    let db_url = require_db_url(db)?;
    let prod_snapshot = resolve_schema(against, None, None)?;

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
                let location = entry.change.schema.as_deref().map_or(
                    entry.change.name.clone(),
                    |s| format!("{s}.{}", entry.change.name),
                );
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

fn load_project_config(cli: &Cli, cwd: &std::path::Path) -> Option<ProjectConfig> {
    if let Some(config_path) = &cli.config {
        ProjectConfig::load(config_path).ok()
    } else {
        ProjectConfig::discover(cwd).map(|(_, c)| c)
    }
}

fn resolve_schema_path(
    schema_file: Option<&std::path::Path>,
    project_config: Option<&ProjectConfig>,
    profile: Option<&str>,
) -> anyhow::Result<PathBuf> {
    // 1. explicit --schema-file
    if let Some(path) = schema_file {
        return Ok(path.to_path_buf());
    }

    let cwd = std::env::current_dir().unwrap_or_default();

    // 2. profile config in dryrun.toml
    if let Some(config) = project_config {
        if let Ok(resolved) = config.resolve_profile(None, None, profile, &cwd) {
            if let Some(sf) = resolved.schema_file {
                if sf.exists() {
                    return Ok(sf);
                }
            }
        }
    }

    // 3. auto-discovered .dryrun/schema.json
    if let Ok(data_dir) = dry_run_core::history::default_data_dir() {
        let candidate = data_dir.join("schema.json");
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    anyhow::bail!("no schema found — run dump-schema first or pass --schema-file");
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

    let pgmustard_api_key = project_config
        .as_ref()
        .and_then(|c| c.pgmustard_api_key());

    // schema file is always required
    let schema_file = resolve_schema_path(
        schema_path, project_config.as_ref(), cli.profile.as_deref(),
    )?;
    let json = std::fs::read_to_string(&schema_file)?;
    let snapshot: dry_run_core::SchemaSnapshot = serde_json::from_str(&json)?;
    eprintln!(
        "dryrun: loaded schema from {} ({} tables)",
        schema_file.display(), snapshot.tables.len()
    );

    // optional --db enables live tools (explain_query, refresh_schema)
    let effective_db = db.map(|s| s.to_string()).or_else(|| {
        if let Some(ref config) = project_config {
            if let Ok(resolved) = config.resolve_profile(None, None, cli.profile.as_deref(), &cwd) {
                return resolved.db_url;
            }
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

    let server = mcp::DryRunServer::from_snapshot_with_db(
        snapshot, db_connection, lint_config, pgmustard_api_key, get_version(),
    );

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
