mod mcp;
mod pgmustard;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use dry_run_core::schema::{NodeColumnStats, NodeIndexStats, NodeStats, NodeTableStats};
use dry_run_core::{DryRun, HistoryStore, ProjectConfig};
use rmcp::ServiceExt;

#[derive(Parser)]
#[command(name = "dry-run", version, about = "PostgreSQL schema intelligence")]
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
    McpServe {
        #[arg(long, env = "DATABASE_URL")]
        db: Option<String>,
        #[arg(long, env = "DRY_RUN_SCHEMA_FILE")]
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

        snapshot.node_stats = vec![NodeStats {
            source: source.clone(),
            timestamp: snapshot.timestamp,
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
    let db_url = require_db_url(db)?;
    let ctx = DryRun::connect(&db_url).await?;
    let snapshot = ctx.introspect_schema().await?;

    let data_dir = dry_run_core::history::default_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;

    let schema_path = data_dir.join("schema.json");
    let json = serde_json::to_string_pretty(&snapshot)?;
    std::fs::write(&schema_path, &json)?;

    let store = open_history_store(None)?;
    if let Err(e) = store.save_snapshot(&db_url, &snapshot) {
        eprintln!("warning: could not save snapshot: {e}");
    }

    eprintln!(
        "Initialized .dry_run/ with {} tables, {} views, {} functions",
        snapshot.tables.len(),
        snapshot.views.len(),
        snapshot.functions.len()
    );
    eprintln!("  Schema: {}", schema_path.display());
    eprintln!("  History: {}", data_dir.join("history.db").display());
    Ok(())
}

async fn cmd_lint(
    cli: &Cli,
    schema_filter: Option<&str>,
    pretty: bool,
    json: bool,
) -> anyhow::Result<()> {
    let snapshot = load_schema_for_lint(cli).await?;

    let snapshot = if let Some(filter) = schema_filter {
        let mut filtered = snapshot.clone();
        filtered.tables.retain(|t| t.schema == filter);
        filtered
    } else {
        snapshot
    };

    let cwd = std::env::current_dir().unwrap_or_default();
    let project_config = load_project_config(cli, &cwd);

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
            .ok_or_else(|| anyhow::anyhow!("no dry_run.toml found"))?
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

            // load schema.json
            let snapshot = if let Some(path) = schema_file {
                load_schema_file(path)?
            } else if let Ok(data_dir) = dry_run_core::history::default_data_dir() {
                let candidate = data_dir.join("schema.json");
                if candidate.exists() {
                    load_schema_file(&candidate)?
                } else {
                    anyhow::bail!(
                        "no schema.json found at {}. Use --schema-file to specify path",
                        candidate.display()
                    );
                }
            } else {
                anyhow::bail!("no schema source found. Use --schema-file to specify path to schema.json");
            };

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

async fn load_schema_for_lint(cli: &Cli) -> anyhow::Result<dry_run_core::SchemaSnapshot> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let project_config = load_project_config(cli, &cwd);

    // try profile-based schema file
    if let Some(ref config) = project_config {
        if let Ok(resolved) =
            config.resolve_profile(None, None, cli.profile.as_deref(), &cwd)
        {
            if let Some(schema_file) = resolved.schema_file {
                if schema_file.exists() {
                    return load_schema_file(&schema_file);
                }
            }
        }
    }

    // try auto-discovered schema.json
    if let Ok(data_dir) = dry_run_core::history::default_data_dir() {
        let candidate = data_dir.join("schema.json");
        if candidate.exists() {
            return load_schema_file(&candidate);
        }
    }

    anyhow::bail!(
        "no schema found — run dump-schema first or pass --schema-file"
    );
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

    // resolve schema source
    let auto_schema = schema_path.map(|p| p.to_path_buf()).or_else(|| {
        if let Some(ref config) = project_config {
            if let Ok(resolved) = config.resolve_profile(
                db, None, cli.profile.as_deref(), &cwd,
            ) {
                if let Some(sf) = resolved.schema_file {
                    if sf.exists() { return Some(sf); }
                }
            }
        }
        let candidate = dry_run_core::history::default_data_dir().ok()?.join("schema.json");
        candidate.exists().then_some(candidate)
    });

    // resolve db_url from profile if not set via CLI
    let effective_db = db.map(|s| s.to_string()).or_else(|| {
        if let Some(ref config) = project_config {
            if let Ok(resolved) = config.resolve_profile(None, None, cli.profile.as_deref(), &cwd) {
                return resolved.db_url;
            }
        }
        None
    });

    let server = if let Some(schema_file) = &auto_schema {
        let json = std::fs::read_to_string(schema_file)?;
        let snapshot: dry_run_core::SchemaSnapshot = serde_json::from_str(&json)?;
        eprintln!(
            "dry-run: loaded schema from {} ({} tables, offline mode)",
            schema_file.display(), snapshot.tables.len()
        );
        mcp::DryRunServer::from_snapshot_with_config(snapshot, lint_config, pgmustard_api_key)
    } else if let Some(db_url) = &effective_db {
        let ctx = DryRun::connect(db_url).await?;
        let history = HistoryStore::open_default().ok();
        mcp::DryRunServer::new(ctx, db_url.clone(), history, lint_config, pgmustard_api_key).await?
    } else {
        anyhow::bail!(
            "no schema source found. Either:\n\
             1. Run 'dry-run --db <url> init' to create .dry_run/schema.json\n\
             2. Pass --schema-file <path> to a schema JSON file\n\
             3. Pass --db <url> for live database mode\n\
             4. Configure a profile in dry_run.toml"
        );
    };

    match transport {
        "stdio" => {
            eprintln!("dry-run: starting MCP server on stdio");
            let service = server.serve(rmcp::transport::stdio()).await?;
            service.waiting().await?;
        }
        "sse" => {
            let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse()?;
            let sse_server = rmcp::transport::sse_server::SseServer::serve(bind_addr).await?;
            eprintln!("dry-run: SSE server listening on http://{bind_addr}/sse");
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
