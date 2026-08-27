mod agent;
mod config;
mod readiness;
mod scheduler;
mod store;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::{default_config_path, Config};
use readiness::check;
use scheduler::run as run_scheduler;
use std::path::{Path, PathBuf};
use std::process::Command;
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
    println!("Cline model: {}", config.agent.model);
    println!("Cline provider: {}", config.agent.provider);
    println!("session mode: {:?}", config.agent.session_mode);
    println!("Robinhood MCP: {}", config.robinhood.trading_mcp_url);
    println!("MCP server name: {}", config.robinhood.mcp_server_name);
    println!("readiness: {}", report.status());
    for blocker in report.blockers {
        println!("  blocker: {blocker}");
    }
    for note in report.notes {
        println!("  note: {note}");
    }

    match Command::new(&config.agent.executable)
        .arg("--version")
        .output()
    {
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
        "  cline mcp add robinhood-trading --transport http {}",
        config.robinhood.trading_mcp_url
    );
    println!("Then authenticate the server in Cline and mark connection_ready=true only after verification.");
    Ok(())
}

fn run(config_path: PathBuf, once: bool) -> Result<()> {
    let (config, store) = load_runtime(&config_path)?;
    run_scheduler(&config, &store, once)
}

fn dashboard(config_path: PathBuf) -> Result<()> {
    let (config, store) = load_runtime(&config_path)?;
    let report = check(&config);
    let snapshot = store.dashboard_snapshot(&config.storage.database_path)?;
    let window = MainWindow::new()?;
    window.set_bot_status(if report.ready { "LIVE READY" } else { "LOCKED" }.into());
    window.set_execution_mode(format!("mode: {:?}", config.execution.mode).into());
    window.set_portfolio_value("—".into());
    window.set_buying_power("—".into());
    window.set_realized_pnl("—".into());
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
    let run_config = config.clone();
    let run_store = Arc::clone(&shared_store);
    window.on_run_now(move || {
        if !check(&run_config).ready {
            if let Some(window) = run_window.upgrade() {
                window.set_last_run_status("blocked by readiness gate".into());
            }
            return;
        }
        if let Ok(guard) = run_store.lock() {
            let _ = run_scheduler(&run_config, &guard, true);
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
