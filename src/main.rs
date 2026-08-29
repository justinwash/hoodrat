mod agent;
mod config;
mod firewall;
mod ingestion;
mod readiness;
mod scheduler;
mod simulator;
mod store;

use agent::{
    run_executable_version, run_read_only_market_data, run_read_only_market_probe,
    run_read_only_reconciliation, run_read_only_smoke_test,
};
use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use config::{default_config_path, Config, SizingMode};
use readiness::{check, ReadinessReport};
use scheduler::{
    run as run_scheduler, run_from_path as run_scheduler_from_path,
    run_from_path_until as run_scheduler_from_path_until,
};
use simulator::{simulate as run_paper_simulation, MarketPlan};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use store::{fmt_money, short_ts, DashboardSnapshot, Store};

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
    /// Run the four-tool read-only account/portfolio reconciliation.
    Reconcile,
    /// Fetch read-only live market data and run the local paper simulator.
    Simulate,
    /// Probe configured read-only MCP market-data tools and print redacted schemas.
    MarketProbe,
    /// Accept the latest detected reconciliation drift as a new operator-approved baseline.
    AcceptBaseline {
        /// Required acknowledgement that the latest drift was reviewed.
        #[arg(long, required = true)]
        confirm: bool,
        /// Operator identity recorded in the append-only acceptance log.
        #[arg(long, required = true)]
        operator: String,
        /// Human-readable reason recorded with the acceptance.
        #[arg(long, required = true)]
        reason: String,
    },
    /// Run scheduled agent evaluations.
    Run {
        /// Evaluate each enabled lane once instead of running continuously.
        #[arg(long)]
        once: bool,
    },
    /// Run the scheduler and the monitoring dashboard together in one process.
    App,
    /// Open the local Slint monitoring dashboard.
    Dashboard,
    /// Inspect the pre-trade firewall: show recent order proposals/verdicts.
    Gate,
    /// Evaluate and record an order proposal through the firewall.
    Propose {
        /// Asset class (equity|option).
        #[arg(long, default_value = "equity")]
        asset_class: String,
        /// Symbol (e.g. SPY).
        #[arg(long, required = true)]
        symbol: String,
        /// Side (buy|sell|buy_to_open|...).
        #[arg(long, required = true)]
        side: String,
        /// Order notional in USD.
        #[arg(long, required = true)]
        notional: f64,
        /// Quantity (optional).
        #[arg(long)]
        quantity: Option<f64>,
        /// Limit price (optional).
        #[arg(long)]
        limit_price: Option<f64>,
        /// Do not prompt for confirmation when recording.
        #[arg(long)]
        yes: bool,
    },
    /// List proposals awaiting operator approval.
    Pending,
    /// Record explicit operator approval of an approved proposal.
    Approve {
        /// Proposal id from `gate`/`pending`.
        #[arg(long, required = true)]
        id: i64,
        /// Operator identity.
        #[arg(long, required = true)]
        operator: String,
        /// Human-readable reason.
        #[arg(long, required = true)]
        reason: String,
        /// Required acknowledgement.
        #[arg(long, required = true)]
        confirm: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CommandKind::Init => initialize(&cli.config),
        CommandKind::Doctor => doctor(&cli.config),
        CommandKind::SmokeTest => smoke_test(&cli.config),
        CommandKind::Reconcile => reconcile(&cli.config),
        CommandKind::Simulate => simulate_market(&cli.config),
        CommandKind::MarketProbe => market_probe(&cli.config),
        CommandKind::AcceptBaseline {
            confirm,
            operator,
            reason,
        } => accept_baseline(&cli.config, confirm, &operator, &reason),
        CommandKind::Run { once } => run(cli.config, once),
        CommandKind::App => app(cli.config),
        CommandKind::Dashboard => dashboard(cli.config),
        CommandKind::Gate => gate(&cli.config),
        CommandKind::Propose {
            asset_class,
            symbol,
            side,
            notional,
            quantity,
            limit_price,
            yes,
        } => propose(
            &cli.config,
            &asset_class,
            &symbol,
            &side,
            notional,
            quantity,
            limit_price,
            yes,
        ),
        CommandKind::Pending => pending(&cli.config),
        CommandKind::Approve {
            id,
            operator,
            reason,
            confirm,
        } => approve(&cli.config, id, &operator, &reason, confirm),
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
    println!(
        "Robinhood MCP authorization: {}",
        robinhood_mcp_authorization_status(
            &config.agent.data_dir,
            &config.robinhood.mcp_server_name
        )
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
    println!(
        "OpenRouter auth: cline --config {} --data-dir {} auth --provider openrouter --apikey \"<KEY>\" --modelid \"{}\"",
        config.agent.config_dir.display(),
        config.agent.data_dir.display(),
        config.agent.model
    );
    println!("Then complete Robinhood MCP OAuth in Cline and mark connection_ready=true only after a successful read-only smoke test.");
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
    println!(
        "safety mode: plan=true, auto_approve=true, retries=1, read probe restricted by prompt"
    );

    let result = run_read_only_smoke_test(
        &config.agent,
        &store,
        &config.robinhood.mcp_server_name,
        &agent::ProcessAgentExecutor,
    )?;
    println!(
        "smoke test run {} finished with exit={:?}, events={}, tool_events={}, robinhood_reads={}, mcp_errors={}, unexpected_tools={}",
        result.run_id,
        result.exit_code,
        result.event_count,
        result.tool_event_count,
        result.robinhood_read_count,
        result.mcp_error_count,
        result.unexpected_tool_count
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
    if result.unexpected_tool_count > 0 {
        anyhow::bail!(
            "Robinhood MCP smoke test policy violation: {} unexpected tool call(s) were observed",
            result.unexpected_tool_count
        );
    }
    if result.robinhood_read_count == 0 {
        let authorization_status = robinhood_mcp_authorization_status(
            &config.agent.data_dir,
            &config.robinhood.mcp_server_name,
        );
        if authorization_status == "required" {
            anyhow::bail!(
                "Robinhood MCP was not verified: OAuth authorization is required for the configured server"
            );
        }
        if result.mcp_error_count > 0 {
            anyhow::bail!(
                "Robinhood MCP was not verified: {} MCP call(s) reported errors; check Robinhood OAuth authorization",
                result.mcp_error_count
            );
        }
        anyhow::bail!(
            "Robinhood MCP was not verified: no successful read tool call targeted the configured server"
        );
    }
    Ok(())
}

fn reconcile(config_path: &Path) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;

    if config.execution.mode != config::ExecutionMode::Disabled {
        anyhow::bail!("reconcile requires execution.mode=disabled");
    }
    if !config.execution.kill_switch_engaged {
        anyhow::bail!("reconcile requires the kill switch to remain engaged");
    }
    if config.risk.confirmed {
        anyhow::bail!("reconcile requires risk.confirmed=false");
    }
    if !config.robinhood.agentic_account_only {
        anyhow::bail!("reconcile requires robinhood.agentic_account_only=true");
    }

    println!("starting read-only account reconciliation");
    println!("safety mode: plan=true, auto_approve=true, retries=1, thinking=none");
    let result = run_read_only_reconciliation(
        &config.agent,
        &store,
        &config.robinhood.mcp_server_name,
        &agent::ProcessAgentExecutor,
    )?;
    println!(
        "reconciliation run {} finished with exit={:?}, events={}, tool_events={}, robinhood_reads={}, mcp_errors={}, unexpected_tools={}",
        result.run_id,
        result.exit_code,
        result.event_count,
        result.tool_event_count,
        result.robinhood_read_count,
        result.mcp_error_count,
        result.unexpected_tool_count
    );
    if let Some(report) = result.reconciliation.as_ref() {
        println!(
            "reconciliation status={}, accounts={}, balances={}, positions={}, pnl_trades={}, drift_items={}",
            report.status,
            report.account_count,
            report.balance_count,
            report.position_count,
            report.pnl_trade_count,
            report.drift.len()
        );
    }
    println!("No configuration or execution flags were changed.");
    if result.exit_code != Some(0) {
        anyhow::bail!(
            "Cline reconciliation process exited with {:?}",
            result.exit_code
        );
    }
    if result.unexpected_tool_count > 0 {
        anyhow::bail!(
            "Robinhood MCP reconciliation policy violation: {} unexpected tool call(s) were observed",
            result.unexpected_tool_count
        );
    }
    if result.mcp_error_count > 0 || result.reconciliation.is_none() {
        anyhow::bail!("Robinhood MCP reconciliation did not complete successfully");
    }
    if result
        .reconciliation
        .as_ref()
        .is_some_and(|report| !matches!(report.status.as_str(), "baseline" | "reconciled"))
    {
        anyhow::bail!("Robinhood MCP reconciliation did not establish a usable baseline");
    }
    Ok(())
}

fn simulate_market(config_path: &Path) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;
    config.simulation.validate()?;
    if config.execution.mode != config::ExecutionMode::Disabled {
        anyhow::bail!("simulate requires execution.mode=disabled");
    }
    if !config.execution.kill_switch_engaged {
        anyhow::bail!("simulate requires the kill switch to remain engaged");
    }
    if config.risk.confirmed {
        anyhow::bail!("simulate requires risk.confirmed=false");
    }
    if !config.robinhood.agentic_account_only {
        anyhow::bail!("simulate requires robinhood.agentic_account_only=true");
    }

    println!("starting read-only live market-data paper simulation");
    println!("simulation profile: {}", config.simulation.profile.name);
    println!(
        "market-data tools: {}",
        config.simulation.market_data_tools.join(", ")
    );
    println!("symbols: {}", config.simulation.symbols.join(", "));
    println!("safety mode: plan=true, auto_approve=true, no broker writes");

    let result = run_read_only_market_data(
        &config.agent,
        &store,
        &config.robinhood.mcp_server_name,
        &config.simulation.market_data_tools,
        &config.simulation.symbols,
        &agent::ProcessAgentExecutor,
    )?;
    if result.exit_code != Some(0) {
        anyhow::bail!(
            "read-only market-data task exited with {:?}",
            result.exit_code
        );
    }
    if result.mcp_error_count > 0
        || result.unexpected_tool_count > 0
        || !result.expected_reads_complete
    {
        anyhow::bail!(
            "read-only market-data policy failed: complete={}, mcp_errors={}, unexpected_tools={}",
            result.expected_reads_complete,
            result.mcp_error_count,
            result.unexpected_tool_count
        );
    }
    if result.robinhood_read_count == 0 {
        anyhow::bail!("read-only market-data task produced no successful MCP reads");
    }

    let proposals = MarketPlan::paper_proposals_from_agent_output(&result.raw_output)?;
    let plan = MarketPlan::from_mcp_outputs(
        &result.mcp_outputs,
        proposals,
        &config.simulation,
        Utc::now(),
    )?;
    let simulation = run_paper_simulation(plan, &config.simulation, Utc::now())?;
    store.record_paper_simulation(&simulation)?;
    println!(
        "paper simulation {} status={}, events={}, positions={}, final_equity=${:.2}, realized_pnl=${:.2}, unrealized_pnl=${:.2}",
        simulation.id,
        simulation.status,
        simulation.events.len(),
        simulation.positions.len(),
        simulation.final_equity_usd,
        simulation.realized_pnl_usd,
        simulation.unrealized_pnl_usd
    );
    for reason in &simulation.no_op_reasons {
        println!("  no-op: {reason}");
    }
    println!("No configuration or broker execution flags were changed.");
    Ok(())
}

fn market_probe(config_path: &Path) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;
    config.simulation.validate_market_data()?;
    if config.execution.mode != config::ExecutionMode::Disabled {
        anyhow::bail!("market-probe requires execution.mode=disabled");
    }
    if !config.execution.kill_switch_engaged {
        anyhow::bail!("market-probe requires the kill switch to remain engaged");
    }
    if config.risk.confirmed {
        anyhow::bail!("market-probe requires risk.confirmed=false");
    }
    if !config.robinhood.agentic_account_only {
        anyhow::bail!("market-probe requires robinhood.agentic_account_only=true");
    }

    println!("starting read-only MCP market-data schema probe");
    println!("tools: {}", config.simulation.market_data_tools.join(", "));
    println!("symbols: {}", config.simulation.symbols.join(", "));
    println!("safety mode: plan=true, auto_approve=true, no broker writes");
    let result = run_read_only_market_probe(
        &config.agent,
        &store,
        &config.robinhood.mcp_server_name,
        &config.simulation.market_data_tools,
        &config.simulation.symbols,
        &agent::ProcessAgentExecutor,
    )?;
    println!(
        "market probe run {} finished with exit={:?}, reads={}, errors={}, unexpected_tools={}, complete={}",
        result.run_id,
        result.exit_code,
        result.robinhood_read_count,
        result.mcp_error_count,
        result.unexpected_tool_count,
        result.expected_reads_complete
    );
    for summary in simulator::summarize_mcp_outputs(&result.mcp_outputs) {
        println!("  {summary}");
    }
    if result.exit_code != Some(0) {
        anyhow::bail!(
            "read-only market-data probe exited with {:?}",
            result.exit_code
        );
    }
    if result.unexpected_tool_count > 0 || result.mcp_error_count > 0 {
        anyhow::bail!("market-data probe observed a policy violation or MCP error");
    }
    if !result.expected_reads_complete {
        anyhow::bail!("market-data probe did not complete every configured read exactly once");
    }
    println!("Raw MCP responses were persisted in the configured SQLite run/tool-event store.");
    println!("No configuration or broker execution flags were changed.");
    Ok(())
}

fn accept_baseline(config_path: &Path, confirm: bool, operator: &str, reason: &str) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;

    if config.execution.mode != config::ExecutionMode::Disabled {
        anyhow::bail!("accept-baseline requires execution.mode=disabled");
    }
    if !config.execution.kill_switch_engaged {
        anyhow::bail!("accept-baseline requires the kill switch to remain engaged");
    }
    if config.risk.confirmed {
        anyhow::bail!("accept-baseline requires risk.confirmed=false");
    }
    if !config.robinhood.agentic_account_only {
        anyhow::bail!("accept-baseline requires robinhood.agentic_account_only=true");
    }

    store.accept_latest_drift(operator, reason, confirm)?;
    println!(
        "accepted the latest reconciliation drift as a new baseline for operator '{}'",
        operator.trim()
    );
    println!("No configuration or execution flags were changed.");
    Ok(())
}

fn run(config_path: PathBuf, once: bool) -> Result<()> {
    run_scheduler_from_path(&config_path, once)
}

fn gate(config_path: &Path) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;
    firewall::connectivity_check(&store)?;
    println!("Hoodrat order firewall");
    println!("  execution:       {:?}", config.execution.mode);
    println!(
        "  kill switch:     {}",
        if config.execution.kill_switch_engaged {
            "engaged"
        } else {
            "released"
        }
    );
    println!(
        "  gateway submit:  {}",
        if config.gateway.submit {
            "ENABLED"
        } else {
            "disabled (safe)"
        }
    );
    println!(
        "  operator approval: {}",
        if config.gateway.require_operator_approval {
            "required"
        } else {
            "not required"
        }
    );
    println!("\nRecent proposals:");
    let proposals = store.pending_proposals(20)?;
    if proposals.is_empty() {
        println!("  (none recorded yet)\n");
    } else {
        for row in proposals {
            println!(
                "  #{:<4} {:<19} {:<7} {:<10} {:<9} {}",
                row.id,
                short_ts(&row.proposed_at),
                row.symbol,
                row.side,
                fmt_money(Some(row.notional_usd)),
                row.verdict
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn propose(
    config_path: &Path,
    asset_class: &str,
    symbol: &str,
    side: &str,
    notional: f64,
    quantity: Option<f64>,
    limit_price: Option<f64>,
    yes: bool,
) -> Result<()> {
    let (config, store) = load_runtime(config_path)?;
    let proposal = firewall::OrderProposal {
        account_number: None,
        asset_class: asset_class.to_owned(),
        symbol: symbol.to_uppercase(),
        side: side.to_owned(),
        order_type: "limit".to_owned(),
        quantity,
        notional_usd: notional,
        limit_price,
        quote_price: None,
        quote_captured_at: None,
        source: "cli".to_owned(),
    };
    if !yes {
        println!(
            "Proposed order: {} {} {} @ ~${:.2}",
            side,
            symbol.to_uppercase(),
            asset_class,
            notional
        );
    }
    let verdict = firewall::evaluate(&config, &store, &proposal)?;
    let id = store.record_order_proposal(&proposal, None, &verdict)?;
    store.record_audit(
        None,
        "firewall",
        if verdict.approved {
            "approved"
        } else {
            "blocked"
        },
        &serde_json::json!({
            "symbol": proposal.symbol,
            "side": proposal.side,
            "notional_usd": proposal.notional_usd,
            "verdict": if verdict.approved { "approved" } else { "blocked" },
            "reasons": verdict.reasons,
        }),
    )?;
    if verdict.approved {
        println!(
            "  verdict: APPROVED (proposal #{id}) {}",
            if verdict.submitted {
                "— submit enabled"
            } else {
                "— not submitted (gateway.submit=false)"
            }
        );
        println!("  review with `cargo run -- gate` and approve with `cargo run -- approve --id {id} --operator <you> --reason <note> --confirm`");
    } else {
        println!("  verdict: BLOCKED (proposal #{id})");
        for reason in &verdict.reasons {
            println!("    - {reason}");
        }
    }
    Ok(())
}

fn pending(config_path: &Path) -> Result<()> {
    let (_config, store) = load_runtime(config_path)?;
    let rows = store.pending_approvals(50)?;
    if rows.is_empty() {
        println!("No proposals awaiting operator approval.");
        return Ok(());
    }
    println!("Proposals awaiting operator approval:");
    for row in rows {
        println!(
            "  #{:<4} {:<19} {:<7} {:<10} {:<9} {}",
            row.id,
            store::short_ts(&row.proposed_at),
            row.symbol,
            row.side,
            store::fmt_money(Some(row.notional_usd)),
            row.verdict
        );
    }
    Ok(())
}

fn approve(config_path: &Path, id: i64, operator: &str, reason: &str, confirm: bool) -> Result<()> {
    if !confirm {
        anyhow::bail!("--confirm is required to record approval");
    }
    let (config, store) = load_runtime(config_path)?;
    if !config.gateway.require_operator_approval {
        println!("note: gateway.require_operator_approval is false; recording approval anyway");
    }
    store.record_order_approval(id, operator, reason)?;
    store.record_audit(
        None,
        "firewall",
        "operator_approved",
        &serde_json::json!({"proposal_id": id, "operator": operator, "reason": reason}),
    )?;
    let blockers = firewall::submission_blockers(&config, &store, id)?;
    println!("Recorded operator approval for proposal #{id} by '{operator}'.");
    if blockers.is_empty() {
        println!("  proposal is clear for submission (gateway.submit enabled + approved).");
    } else {
        println!("  proposal still blocked from submission:");
        for blocker in blockers {
            println!("    - {blocker}");
        }
    }
    Ok(())
}

fn app(config_path: PathBuf) -> Result<()> {
    let stop = Arc::new(AtomicBool::new(false));
    let scheduler_path = config_path.clone();
    let scheduler_stop = Arc::clone(&stop);
    let scheduler = thread::spawn(move || {
        if let Err(error) = run_scheduler_from_path_until(&scheduler_path, scheduler_stop) {
            eprintln!("scheduler thread exited with error: {error:#}");
        }
    });

    let dashboard_result = dashboard(config_path);

    // Request the scheduler loop to stop, then wait for it to wind down so the
    // process exits cleanly after the dashboard window closes.
    stop.store(true, Ordering::Relaxed);
    let _ = scheduler.join();
    dashboard_result
}

fn dashboard(config_path: PathBuf) -> Result<()> {
    let (config, store) = load_runtime(&config_path)?;
    let report = check(&config);
    let snapshot = store.dashboard_snapshot(&config.storage.database_path)?;
    let window = MainWindow::new()?;
    apply_snapshot(&window, &report, &config, &snapshot);

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
            let report = check(&config);
            apply_snapshot(&window, &report, &config, &snapshot);
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

fn apply_snapshot(
    window: &MainWindow,
    report: &ReadinessReport,
    config: &Config,
    snapshot: &DashboardSnapshot,
) {
    window.set_bot_status(if report.ready { "LIVE READY" } else { "LOCKED" }.into());
    window.set_execution_mode(format!("mode: {:?}", config.execution.mode).into());
    window.set_risk_status(if report.ready {
        "Confirmed".into()
    } else {
        "Not confirmed / locked".into()
    });
    window.set_portfolio_value(format_money(snapshot.portfolio_value).into());
    window.set_buying_power(format_money(snapshot.buying_power).into());
    window.set_cash(format_money(snapshot.cash).into());
    window.set_equity(format_money(snapshot.equity).into());
    window.set_realized_pnl(format_money(snapshot.realized_pnl).into());
    window.set_unrealized_pnl(format_money(snapshot.unrealized_pnl).into());
    window.set_reconciliation_status(snapshot.reconciliation_status.clone().into());
    window.set_reconciliation_details(snapshot.reconciliation_details.clone().into());
    window.set_last_run(snapshot.last_run.clone().into());
    window.set_last_run_status(snapshot.last_run_status.clone().into());
    window.set_database_path(snapshot.database_path.clone().into());
    window.set_market_session(market_session_label(config).into());
    window.set_last_refresh(Utc::now().format("%H:%M:%S UTC").to_string().into());
    window.set_max_order_notional(format_money(Some(config.risk.max_order_notional_usd)).into());
    window.set_daily_loss_cap(format_money(Some(config.risk.daily_loss_limit_usd)).into());
    window.set_max_exposure(format_money(Some(config.risk.max_total_exposure_usd)).into());
    window.set_sizing_mode(sizing_label(config).into());
    window.set_leverage_status(
        if config.strategy.allow_leverage {
            "enabled"
        } else {
            "disabled"
        }
        .into(),
    );
    window.set_symbol_scope(symbol_scope(config).into());
    window.set_recent_events(snapshot.recent_events.clone().into());
    window.set_equity_chart_path(snapshot.equity_chart_path.clone().into());
    window.set_equity_chart_labels(snapshot.equity_chart_labels.clone().into());
    window.set_pnl_chart_path(snapshot.pnl_chart_path.clone().into());
    window.set_overview_stats(snapshot.overview_stats.clone().into());
    window.set_accounts_table(snapshot.accounts_table.clone().into());
    window.set_balances_table(snapshot.balances_table.clone().into());
    window.set_positions_table(snapshot.positions_table.clone().into());
    window.set_pnl_snapshots_table(snapshot.pnl_snapshots_table.clone().into());
    window.set_pnl_trades_table(snapshot.pnl_trades_table.clone().into());
    window.set_runs_table(snapshot.runs_table.clone().into());
    window.set_tool_events_table(snapshot.tool_events_table.clone().into());
    window.set_audit_table(snapshot.audit_table.clone().into());
    window.set_reconciliations_table(snapshot.reconciliations_table.clone().into());
    window.set_baseline_acceptances_table(snapshot.baseline_acceptances_table.clone().into());
    window.set_strategy_table(config.strategy.summary().into());
    window.set_simulation_table(snapshot.simulation_table.clone().into());
    window.set_proposals_table(snapshot.proposals_table.clone().into());
}

fn sizing_label(config: &Config) -> String {
    match config.strategy.sizing_mode {
        SizingMode::FixedNotional => "fixed notional".to_owned(),
        SizingMode::AvailableBalance => "available balance".to_owned(),
    }
}

fn symbol_scope(config: &Config) -> String {
    if config.strategy.allows_any_symbol() {
        "*  unrestricted".to_owned()
    } else if config.strategy.approved_symbols.is_empty() {
        "none approved".to_owned()
    } else {
        config.strategy.approved_symbols.join(", ")
    }
}

fn market_session_label(config: &Config) -> String {
    let timezone = config
        .schedule
        .equity_options
        .timezone
        .parse::<chrono_tz::Tz>()
        .ok();
    let start =
        chrono::NaiveTime::parse_from_str(&config.schedule.equity_options.start_local, "%H:%M")
            .ok();
    let end =
        chrono::NaiveTime::parse_from_str(&config.schedule.equity_options.end_local, "%H:%M").ok();
    let is_open = match (timezone, start, end) {
        (Some(timezone), Some(start), Some(end)) => {
            let local_time = Utc::now().with_timezone(&timezone).time();
            config.schedule.equity_options.enabled && local_time >= start && local_time <= end
        }
        _ => false,
    };
    if is_open {
        "● MARKET OPEN · 09:35—15:55 ET".to_owned()
    } else {
        "○ MARKET CLOSED · 09:35—15:55 ET".to_owned()
    }
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

fn robinhood_mcp_authorization_status(data_dir: &Path, server_name: &str) -> &'static str {
    let path = data_dir.join("settings").join("cline_mcp_settings.json");
    let Ok(contents) = std::fs::read_to_string(path) else {
        return "unknown";
    };
    let Ok(settings) = serde_json::from_str::<serde_json::Value>(&contents) else {
        return "unknown";
    };
    let Some(server) = settings
        .get("mcpServers")
        .and_then(|servers| servers.get(server_name))
    else {
        return "not configured";
    };
    if server
        .get("oauth")
        .and_then(|oauth| oauth.get("authorizationRequired"))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        "required"
    } else {
        "not marked required"
    }
}
