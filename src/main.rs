mod agent;
mod config;
mod ingestion;
mod readiness;
mod scheduler;
mod store;

use agent::{run_executable_version, run_read_only_smoke_test};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::{default_config_path, Config};
use readiness::check;
use scheduler::{run as run_scheduler, run_from_path as run_scheduler_from_path};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use store::Store;

slint::include_modules!();

#[derive(Debug, Parser)]
#[command(
    name = "hoodrat",
    version,
    about = "Agentic Robinhood trading supervisor"
)]
struct Cli {
    #[arg(long, global = true, default_value_os_t = default_config_path())]
    config: PathBuf,
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    /// Create the fail-closed config and initialize its SQLite database.
    Init,
    /// Inspect configuration and local Cline/MCP readiness.
    Doctor,
    /// Run a read-only Cline/Robinhood MCP connectivity test.
    SmokeTest,
    /// Run scheduled agent evaluations.
    Run {
        /// Evaluate each enabled lane once instead of running continuously.
        #[arg(long)]
        once: bool,
    },
    /// Open the local Slint monitoring dashboard.
    Dashboard,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CommandKind::Init => initialize(&cli.config),
        CommandKind::Doctor => doctor(&cli.config),
        CommandKind::SmokeTest => smoke_test(&cli.config),
        CommandKind::Run { once } => run(cli.config, once),
        CommandKind::Dashboard => dashboard(cli.config),
    }
}

fn initialize(config_path: &Path) -> Result<()> {
    Config::write_default(config_path)?;
    let mut config = Config::load(config_path)?;
    config.resolve_paths(config_path);
    config.ensure_parent_directories()?;
    let _store = Store::open(&config.storage.database_path)?;
    println!("created fail-closed config at {}", config_path.display());
    println!(
        "initialized database at {}",
        config.storage.database_path.display()
    );
    println!("run `cargo run -- doctor` for setup guidance");
    Ok(())
}

fn load_runtime(config_path: &Path) -> Result<(Config, Store)> {
    let mut config = Config::load(config_path).with_context(|| {
        format!(
            "could not load {}; run `cargo run -- init` first",
            config_path.display()
        )
    })?;
    config.resolve_paths(config_path);
    config.ensure_parent_directories()?;
    let store = Store::open(&config.storage.database_path)?;
    Ok((config, store))
}

fn doctor(config_path: &Path) -> Result<()> {
    let (config, _store) = load_runtime(config_path)?;
    let report = check(&config);
    println!("config: {}", config_path.display());
    println!("execution mode: {:?}", config.execution.mode);
    println!(
        "kill switch engaged: {}",
        config.execution.kill_switch_engaged
    );
    println!("Cline executable: {}", config.agent.executable);
    println!(
        "Cline config directory: {}",
        config.agent.config_dir.display()
    );
    println!("Cline data directory: {}", config.agent.data_dir.display());
    println!(
        "Cline resolved executable: {}",
        agent::resolve_executable(&config.agent.executable)
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "not found".to_owned())
    );
    println!(
        "Cline provider settings: {}",
        settings_file_status(&config.agent.data_dir, "providers.json")
    );
    println!(
        "Cline MCP settings: {}",
        settings_file_status(&config.agent.data_dir, "cline_mcp_settings.json")
    );
    println!("Cline model: {}", config.agent.model);
    println!("Cline provider: {}", config.agent.provider);
    println!("session mode: {:?}", config.agent.session_mode);
    println!("Robinhood MCP: {}", config.robinhood.trading_mcp_url);
    println!("MCP server name: {}", config.robinhood.mcp_server_name);
    println!("database schema version: {}", _store.schema_version()?);
    println!("readiness: {}", report.status());
    for blocker in report.blockers {
        println!("  blocker: {blocker}");
    }
    for note in report.notes {
        println!("  note: {note}");
    }

    match run_executable_version(&config.agent.executable) {
        Ok(output) if output.status.success() => {
            println!("Cline executable check: available");
        }
        Ok(output) => {
            println!("Cline executable check: returned {}", output.status);
        }
        Err(error) => {
            println!("Cline executable check: unavailable ({error})");
        }
    }
    println!();
    println!("Robinhood setup command documented by Robinhood:");
    println!(
        "  cline --config {} --data-dir {} mcp add --yes --json --transport http robinhood-trading {}",
        config.agent.config_dir.display(),
        config.agent.data_dir.display(),
        config.robinhood.trading_mcp_url
    );
    println!(
        "Use the same --config and --data-dir for Cline authentication and every Hoodrat run."
    );
    println!("Then authenticate the server in Cline and mark connection_ready=true only after verification.");
    Ok(())
}

fn smoke_test(config_path: &Path) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;

    if config.execution.mode != config::ExecutionMode::Disabled {
        anyhow::bail!("smoke-test requires execution.mode=disabled");
    }
    if !config.execution.kill_switch_engaged {
        anyhow::bail!("smoke-test requires the kill switch to remain engaged");
    }
    if config.risk.confirmed {
        anyhow::bail!("smoke-test requires risk.confirmed=false");
    }
    if !config.robinhood.agentic_account_only {
        anyhow::bail!("smoke-test requires robinhood.agentic_account_only=true");
    }

    println!("starting read-only MCP smoke test");
    println!("Cline executable: {}", config.agent.executable);
    println!(
        "Cline config directory: {}",
        config.agent.config_dir.display()
    );
    println!("Cline data directory: {}", config.agent.data_dir.display());
    println!("Robinhood MCP: {}", config.robinhood.trading_mcp_url);
    println!("safety mode: plan=true, auto_approve=false");

    let result = run_read_only_smoke_test(&config.agent, &store, &agent::ProcessAgentExecutor)?;
    println!(
        "smoke test run {} finished with exit={:?}, events={}, tool_events={}",
        result.run_id, result.exit_code, result.event_count, result.tool_event_count
    );
    println!(
        "portfolio snapshots ingested: {}",
        store.portfolio_snapshot_count()?
    );
    println!("executions ingested: {}", store.execution_count()?);
    println!("No configuration or execution flags were changed.");
    if result.exit_code != Some(0) {
        anyhow::bail!(
            "Cline smoke-test process exited with {:?}",
            result.exit_code
        );
    }
    Ok(())
}

fn run(config_path: PathBuf, once: bool) -> Result<()> {
    run_scheduler_from_path(&config_path, once)
}

fn dashboard(config_path: PathBuf) -> Result<()> {
    let (config, store) = load_runtime(&config_path)?;
    let report = check(&config);
    let snapshot = store.dashboard_snapshot(&config.storage.database_path)?;
    let window = MainWindow::new()?;
    window.set_bot_status(if report.ready { "LIVE READY" } else { "LOCKED" }.into());
    window.set_execution_mode(format!("mode: {:?}", config.execution.mode).into());
    window.set_portfolio_value(format_money(snapshot.portfolio_value).into());
    window.set_buying_power(format_money(snapshot.buying_power).into());
    window.set_realized_pnl(format_money(snapshot.realized_pnl).into());
    window.set_last_run(snapshot.last_run.into());
    window.set_last_run_status(snapshot.last_run_status.into());
    window.set_database_path(snapshot.database_path.into());
    window.set_risk_status(if report.ready {
        "Confirmed".into()
    } else {
        "Not confirmed / locked".into()
    });
    window.set_recent_events(snapshot.recent_events.into());

    let shared_store = Arc::new(Mutex::new(store));
    let refresh_window = window.as_weak();
    let refresh_store = Arc::clone(&shared_store);
    let refresh_config_path = config_path.clone();
    window.on_refresh(move || {
        let Ok((config, store)) = load_runtime(&refresh_config_path) else {
            return;
        };
        let Ok(snapshot) = store.dashboard_snapshot(&config.storage.database_path) else {
            return;
        };
        if let Some(window) = refresh_window.upgrade() {
            window.set_last_run(snapshot.last_run.into());
            window.set_last_run_status(snapshot.last_run_status.into());
            window.set_database_path(snapshot.database_path.into());
            window.set_portfolio_value(format_money(snapshot.portfolio_value).into());
            window.set_buying_power(format_money(snapshot.buying_power).into());
            window.set_realized_pnl(format_money(snapshot.realized_pnl).into());
            window.set_recent_events(snapshot.recent_events.into());
            window.set_bot_status(
                if check(&config).ready {
                    "LIVE READY"
                } else {
                    "LOCKED"
                }
                .into(),
            );
            window.set_execution_mode(format!("mode: {:?}", config.execution.mode).into());
            window.set_risk_status(if check(&config).ready {
                "Confirmed".into()
            } else {
                "Not confirmed / locked".into()
            });
        }
        if let Ok(mut guard) = refresh_store.lock() {
            if let Ok(new_store) = Store::open(&config.storage.database_path) {
                *guard = new_store;
            }
        }
    });

    let run_window = window.as_weak();
    let run_store = Arc::clone(&shared_store);
    let run_config_path = config_path.clone();
    window.on_run_now(move || {
        let Ok((config, store)) = load_runtime(&run_config_path) else {
            if let Some(window) = run_window.upgrade() {
                window.set_last_run_status("unable to reload configuration".into());
            }
            return;
        };
        if !check(&config).ready {
            if let Some(window) = run_window.upgrade() {
                window.set_last_run_status("blocked by readiness gate".into());
            }
            return;
        }
        if let Ok(mut guard) = run_store.lock() {
            *guard = store;
            let _ = run_scheduler(&config, &guard, true);
        }
    });

    let kill_window = window.as_weak();
    let kill_config_path = config_path;
    window.on_toggle_kill_switch(move || {
        let Ok(mut config) = Config::load(&kill_config_path) else {
            return;
        };
        config.execution.kill_switch_engaged = !config.execution.kill_switch_engaged;
        if config.save(&kill_config_path).is_ok() {
            if let Some(window) = kill_window.upgrade() {
                window.set_execution_mode(if config.execution.kill_switch_engaged {
                    "KILL SWITCH ENGAGED".into()
                } else {
                    format!("mode: {:?}", config.execution.mode).into()
                });
            }
        }
    });

    window.run()?;
    Ok(())
}

fn format_money(value: Option<f64>) -> String {
    value
        .map(|value| format!("${value:.2}"))
        .unwrap_or_else(|| "—".to_owned())
}

fn settings_file_status(data_dir: &Path, file_name: &str) -> &'static str {
    if data_dir.join("settings").join(file_name).is_file() {
        "present"
    } else {
        "missing"
    }
}
