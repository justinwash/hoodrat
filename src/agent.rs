use crate::config::{AgentConfig, RiskConfig, StrategyContract};
use crate::ingestion::{
    parse_json_text, single_agentic_account, BrokerDataSink, BrokerPayload, ExecutionRecord,
    PortfolioSnapshot,
};
use crate::store::{AgentEventRecord, AgentToolEventRecord, ReconciliationReport, Store};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Instant;

const READ_ONLY_DISABLED_CLINE_TOOLS: &[&str] = &[
    "read_files",
    "search_codebase",
    "run_commands",
    "editor",
    "fetch_web_content",
    "skills",
    "ask_question",
    "spawn_agent",
    "teams",
    "apply_patch",
    "submit_and_exit",
];

#[derive(Debug, Clone, Copy)]
pub enum Lane {
    EquityOptions,
}

impl Lane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EquityOptions => "equity_options",
        }
    }

    pub fn capabilities(self) -> &'static str {
        match self {
            Self::EquityOptions => "equities and options",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentCommand {
    pub executable: String,
    pub args: Vec<String>,
    pub working_directory: Option<std::path::PathBuf>,
    environment: Vec<(String, String)>,
    cline_data_dir: PathBuf,
    restrict_local_tools: bool,
}

impl AgentCommand {
    #[allow(dead_code)]
    pub fn from_config(config: &AgentConfig, prompt: String) -> Self {
        Self::from_config_with_options(config, prompt, false, config.auto_approve, None)
    }

    pub fn from_config_with_options(
        config: &AgentConfig,
        prompt: String,
        plan_mode: bool,
        auto_approve: bool,
        system_prompt: Option<String>,
    ) -> Self {
        let mut args = Vec::new();
        if plan_mode {
            args.push("--plan".to_owned());
        }
        args.extend([
            "--json".to_owned(),
            "--model".to_owned(),
            config.model.clone(),
            "--provider".to_owned(),
            config.provider.clone(),
            "--timeout".to_owned(),
            config.timeout_secs.to_string(),
            "--auto-approve".to_owned(),
            auto_approve.to_string(),
            "--data-dir".to_owned(),
            config.data_dir.display().to_string(),
            "--config".to_owned(),
            config.config_dir.display().to_string(),
        ]);
        if system_prompt.is_some() {
            args.extend([
                "--retries".to_owned(),
                "1".to_owned(),
                "--thinking".to_owned(),
                "none".to_owned(),
            ]);
        }
        if let Some(system_prompt) = system_prompt {
            args.extend(["--system".to_owned(), system_prompt]);
        }
        args.push(prompt);
        Self {
            executable: config.executable.clone(),
            args,
            working_directory: config.working_directory.clone(),
            environment: Vec::new(),
            cline_data_dir: config.data_dir.clone(),
            restrict_local_tools: false,
        }
    }

    fn restrict_local_commands(mut self) -> Self {
        self.environment.push((
            "CLINE_DATA_DIR".to_owned(),
            self.cline_data_dir.display().to_string(),
        ));
        self.environment.push((
            "CLINE_COMMAND_PERMISSIONS".to_owned(),
            r#"{"allow":[],"deny":["*"]}"#.to_owned(),
        ));
        self.restrict_local_tools = true;
        self
    }

    fn spawn(&self) -> Result<Output> {
        let executable =
            resolve_executable(&self.executable).unwrap_or_else(|| PathBuf::from(&self.executable));
        let mut command = process_command(&executable, &self.args);
        #[cfg(debug_assertions)]
        {
            let mut line = executable.display().to_string();
            for a in &self.args {
                line.push(' ');
                line.push_str(a);
            }
            eprintln!("[spawn] exec='{}' argv_len={}", executable.display(), line.len());
        }
        let settings_guard = if self.restrict_local_tools {
            Some(prepare_read_only_cline_tools(&self.cline_data_dir)?)
        } else {
            None
        };
        if let Some(directory) = &self.working_directory {
            command.current_dir(directory);
        }
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        let result = command
            .output()
            .with_context(|| format!("failed to launch Cline executable '{}'", self.executable));
        if let Some(settings_guard) = settings_guard {
            settings_guard.restore()?;
        }
        result
    }
}

struct ReadOnlyClineSettingsGuard {
    path: PathBuf,
    original: Option<Vec<u8>>,
}

impl ReadOnlyClineSettingsGuard {
    fn restore(self) -> Result<()> {
        match self.original {
            Some(contents) => fs::write(&self.path, contents),
            None => match fs::remove_file(&self.path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            },
        }
        .with_context(|| {
            format!(
                "failed to restore isolated Cline settings at {}",
                self.path.display()
            )
        })
    }
}

fn prepare_read_only_cline_tools(data_dir: &Path) -> Result<ReadOnlyClineSettingsGuard> {
    let settings_path = data_dir.join("settings").join("global-settings.json");
    let original = if settings_path.is_file() {
        Some(fs::read(&settings_path).with_context(|| {
            format!(
                "failed to read isolated Cline settings at {}",
                settings_path.display()
            )
        })?)
    } else {
        None
    };
    let mut settings = if let Some(contents) = original.as_deref() {
        serde_json::from_slice::<Value>(contents).with_context(|| {
            format!(
                "isolated Cline settings at {} are not valid JSON",
                settings_path.display()
            )
        })?
    } else {
        serde_json::json!({})
    };
    let object = settings
        .as_object_mut()
        .context("isolated Cline global settings must be a JSON object")?;
    let disabled_tools = object
        .entry("disabledTools")
        .or_insert_with(|| serde_json::json!([]));
    let disabled_tools = disabled_tools
        .as_array_mut()
        .context("isolated Cline disabledTools must be a JSON array")?;
    for tool_name in READ_ONLY_DISABLED_CLINE_TOOLS {
        if !disabled_tools
            .iter()
            .any(|value| value.as_str() == Some(tool_name))
        {
            disabled_tools.push(Value::String((*tool_name).to_owned()));
        }
    }
    fs::create_dir_all(
        settings_path
            .parent()
            .context("isolated Cline settings path has no parent directory")?,
    )?;
    fs::write(
        &settings_path,
        format!("{}\n", serde_json::to_string_pretty(&settings)?),
    )?;
    Ok(ReadOnlyClineSettingsGuard {
        path: settings_path,
        original,
    })
}

pub fn resolve_executable(executable: &str) -> Option<PathBuf> {
    let configured_path = Path::new(executable);
    let has_directory = configured_path
        .parent()
        .is_some_and(|parent| !parent.as_os_str().is_empty() && parent != Path::new("."));
    if configured_path.is_absolute() || has_directory {
        return configured_path
            .is_file()
            .then(|| configured_path.to_path_buf());
    }

    let candidates = if cfg!(windows) && configured_path.extension().is_none() {
        vec![
            format!("{executable}.cmd"),
            format!("{executable}.exe"),
            format!("{executable}.bat"),
            executable.to_owned(),
        ]
    } else {
        vec![executable.to_owned()]
    };

    if let Some(path_variable) = env::var_os("PATH") {
        for directory in env::split_paths(&path_variable) {
            if let Some(path) = find_candidate(&directory, &candidates) {
                return Some(path);
            }
        }
    }

    if cfg!(windows) {
        if let Some(app_data) = env::var_os("APPDATA") {
            if let Some(path) = find_candidate(&PathBuf::from(app_data).join("npm"), &candidates) {
                return Some(path);
            }
        }
    }

    None
}

pub fn run_executable_version(executable: &str) -> Result<Output> {
    let resolved = resolve_executable(executable).unwrap_or_else(|| PathBuf::from(executable));
    process_command(&resolved, &["--version".to_owned()])
        .output()
        .with_context(|| format!("failed to launch executable '{executable}'"))
}

fn process_command(executable: &Path, args: &[String]) -> Command {
    #[cfg(windows)]
    if matches!(
        executable.extension().and_then(|value| value.to_str()),
        Some("cmd" | "bat")
    ) {
        if let Some(script) = npm_package_script(executable) {
            let mut command = Command::new("node");
            command.arg(script);
            command.args(args);
            return command;
        }
        let mut command = Command::new("cmd.exe");
        command.arg("/D").arg("/S").arg("/C").arg(executable);
        command.args(args);
        return command;
    }

    let mut command = Command::new(executable);
    command.args(args);
    command
}

#[cfg(windows)]
fn npm_package_script(executable: &Path) -> Option<PathBuf> {
    let package_name = executable.file_stem()?.to_str()?;
    let script = executable
        .parent()?
        .join("node_modules")
        .join(package_name)
        .join("bin")
        .join(package_name);
    script.is_file().then_some(script)
}

fn find_candidate(directory: &Path, candidates: &[String]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(|candidate| directory.join(candidate))
        .find(|path| path.is_file())
}

pub trait AgentExecutor {
    fn execute(&self, command: &AgentCommand) -> Result<Output>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessAgentExecutor;

impl AgentExecutor for ProcessAgentExecutor {
    fn execute(&self, command: &AgentCommand) -> Result<Output> {
        command.spawn()
    }
}

#[derive(Debug)]
pub struct AgentRunResult {
    pub run_id: String,
    pub exit_code: Option<i32>,
    pub event_count: u32,
    pub tool_event_count: u32,
    pub robinhood_read_count: u32,
    pub mcp_error_count: u32,
    pub unexpected_tool_count: u32,
    pub reconciliation: Option<ReconciliationReport>,
    pub raw_output: String,
    pub expected_reads_complete: bool,
    pub mcp_outputs: BTreeMap<String, Vec<McpOutput>>,
}

#[derive(Debug, Clone)]
pub struct McpOutput {
    pub is_error: bool,
    pub value: Value,
}

#[derive(Debug, Clone)]
struct AgentTaskOptions {
    plan_mode: bool,
    auto_approve: bool,
    system_prompt: Option<String>,
    expected_mcp_server: Option<String>,
    expected_mcp_tools: Option<HashSet<String>>,
    strict_typed_ingestion: bool,
    strategy_contract: Option<StrategyContract>,
    restrict_local_commands: bool,
}

#[allow(dead_code)]
pub fn build_prompt(lane: Lane, context: &str, config_summary: &str) -> String {
    build_prompt_with_strategy(lane, context, config_summary, &StrategyContract::default())
}

fn build_prompt_with_strategy(
    lane: Lane,
    context: &str,
    config_summary: &str,
    strategy: &StrategyContract,
) -> String {
    format!(
        "You are the Hoodrat trading analyst for the Robinhood Agentic account.\n\n\
Execution lane: {}. You may consider only {} in this run.\n\
Use the Robinhood Trading MCP tools already configured in this Cline profile.\n\
Retrieve current account, portfolio, market, watchlist, and open-order information\n\
before making any decision. Never rely on stale context.\n\n\
The user is ultimately responsible for all trades. Do not claim certainty or\n\
a guaranteed return. Do not trade any non-Agentic Robinhood account.\n\n\
Return a concise explanation and, if action is appropriate, clearly state the\n\
tool action you took and its result. If no trade is appropriate, say so.\n\
Preserve a machine-readable final summary using this shape when possible:\n\
{{\"decision\":\"hold|buy|sell|reduce|close\",\"symbol\":\"...\",\"reason\":\"...\",\"risk_notes\":\"...\"}}\n\n\
Current persisted context:\n{}\n\n\
 |Configured monitoring policy (not a pre-trade firewall in direct MCP mode):\n{}\n\n\
 |Strategy contract (version {}):\n{}\n\
 |If any strategy contract condition is not satisfied, take no action and state that the run is a no-op.",
        lane.as_str(),
        lane.capabilities(),
        context,
        config_summary,
        strategy.contract_version,
        strategy.summary()
    )
}

#[allow(dead_code)]
pub fn run_fresh_task(
    config: &AgentConfig,
    store: &Store,
    lane: Lane,
    context: &str,
    config_summary: &str,
) -> Result<AgentRunResult> {
    run_fresh_task_with_strategy_with_executor(
        config,
        store,
        lane,
        context,
        config_summary,
        &RiskConfig::default(),
        &StrategyContract::default(),
        &ProcessAgentExecutor,
    )
}

#[allow(dead_code)]
pub fn run_fresh_task_with_executor<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    lane: Lane,
    context: &str,
    config_summary: &str,
    executor: &E,
) -> Result<AgentRunResult> {
    run_fresh_task_with_strategy_with_executor(
        config,
        store,
        lane,
        context,
        config_summary,
        &RiskConfig::default(),
        &StrategyContract::default(),
        executor,
    )
}

pub fn run_fresh_task_with_strategy(
    config: &AgentConfig,
    store: &Store,
    lane: Lane,
    context: &str,
    config_summary: &str,
    risk: &RiskConfig,
    strategy: &StrategyContract,
) -> Result<AgentRunResult> {
    run_fresh_task_with_strategy_with_executor(
        config,
        store,
        lane,
        context,
        config_summary,
        risk,
        strategy,
        &ProcessAgentExecutor,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_fresh_task_with_strategy_with_executor<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    lane: Lane,
    context: &str,
    config_summary: &str,
    risk: &RiskConfig,
    strategy: &StrategyContract,
    executor: &E,
) -> Result<AgentRunResult> {
    strategy.validate_against_risk(risk)?;
    let prompt = build_prompt_with_strategy(lane, context, config_summary, strategy);
    run_task_with_executor(
        config,
        store,
        lane.as_str(),
        prompt,
        AgentTaskOptions {
            plan_mode: false,
            auto_approve: config.auto_approve,
            system_prompt: None,
            expected_mcp_server: None,
            expected_mcp_tools: None,
            strict_typed_ingestion: false,
            strategy_contract: Some(strategy.clone()),
            restrict_local_commands: false,
        },
        executor,
    )
}

pub fn run_read_only_smoke_test<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    robinhood_server_name: &str,
    executor: &E,
) -> Result<AgentRunResult> {
    run_task_with_executor(
        config,
        store,
        "smoke_test",
        build_smoke_test_prompt(robinhood_server_name),
        AgentTaskOptions {
            plan_mode: true,
            auto_approve: true,
            system_prompt: Some(build_smoke_test_system_prompt(robinhood_server_name)),
            expected_mcp_server: Some(robinhood_server_name.to_owned()),
            expected_mcp_tools: Some(HashSet::from(["get_accounts".to_owned()])),
            strict_typed_ingestion: false,
            strategy_contract: None,
            restrict_local_commands: true,
        },
        executor,
    )
}

pub fn run_read_only_reconciliation<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    robinhood_server_name: &str,
    executor: &E,
) -> Result<AgentRunResult> {
    run_task_with_executor(
        config,
        store,
        "reconciliation",
        build_reconciliation_prompt(robinhood_server_name),
        AgentTaskOptions {
            plan_mode: true,
            auto_approve: true,
            system_prompt: Some(build_reconciliation_system_prompt(robinhood_server_name)),
            expected_mcp_server: Some(robinhood_server_name.to_owned()),
            expected_mcp_tools: Some(
                [
                    "get_accounts",
                    "get_portfolio",
                    "get_realized_pnl",
                    "get_pnl_trade_history",
                ]
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            ),
            strict_typed_ingestion: true,
            strategy_contract: None,
            restrict_local_commands: true,
        },
        executor,
    )
}

pub fn run_read_only_market_data<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    robinhood_server_name: &str,
    market_data_tools: &[String],
    symbols: &[String],
    executor: &E,
) -> Result<AgentRunResult> {
    if market_data_tools.is_empty() {
        anyhow::bail!("market-data simulation requires at least one configured tool");
    }
    let expected_tools = market_data_tools
        .iter()
        .map(|tool| tool.rsplit("__").next().unwrap_or(tool).to_owned())
        .collect::<HashSet<_>>();
    run_task_with_executor(
        config,
        store,
        "market_data",
        build_market_data_prompt(robinhood_server_name, market_data_tools, symbols),
        AgentTaskOptions {
            plan_mode: true,
            auto_approve: true,
            system_prompt: Some(build_market_data_system_prompt(robinhood_server_name)),
            expected_mcp_server: Some(robinhood_server_name.to_owned()),
            expected_mcp_tools: Some(expected_tools),
            strict_typed_ingestion: false,
            strategy_contract: None,
            restrict_local_commands: true,
        },
        executor,
    )
}

pub fn run_read_only_market_probe<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    robinhood_server_name: &str,
    market_data_tools: &[String],
    symbols: &[String],
    executor: &E,
) -> Result<AgentRunResult> {
    if market_data_tools.is_empty() {
        anyhow::bail!("market-data probe requires at least one configured tool");
    }
    let expected_tools = market_data_tools
        .iter()
        .map(|tool| tool.rsplit("__").next().unwrap_or(tool).to_owned())
        .collect::<HashSet<_>>();
    run_task_with_executor(
        config,
        store,
        "market_probe",
        build_market_probe_prompt(robinhood_server_name, market_data_tools, symbols),
        AgentTaskOptions {
            plan_mode: true,
            auto_approve: true,
            system_prompt: Some(build_market_data_system_prompt(robinhood_server_name)),
            expected_mcp_server: Some(robinhood_server_name.to_owned()),
            expected_mcp_tools: Some(expected_tools),
            strict_typed_ingestion: false,
            strategy_contract: None,
            restrict_local_commands: true,
        },
        executor,
    )
}

fn build_smoke_test_system_prompt(server_name: &str) -> String {
    format!(
        "You are a data-connectivity checker, not a coding agent. The only permitted external service is the Robinhood Trading MCP server named '{server_name}'. Do not inspect or modify the local workspace. Do not use filesystem, shell, browser, delegation, coding, checkpoint, or any built-in workspace tool. If a tool is unavailable or requires approval, report that fact and stop. Never place, cancel, replace, or preview an order. Never modify a watchlist or account setting. Never expose credentials or tokens."
    )
}

fn build_smoke_test_prompt(server_name: &str) -> String {
    format!(
        "Perform exactly one READ-ONLY Robinhood Trading MCP connectivity probe using only the configured MCP server '{server_name}'. Call only the read-only MCP tool 'get_accounts' on that server, then stop. Do not call any other tool, including skills, shell, filesystem, browser, delegation, coding, or any other MCP tool. Do not place, cancel, replace, or preview an order. Do not modify Robinhood state. Return only whether the get_accounts read succeeded and non-sensitive metadata."
    )
}

fn build_reconciliation_system_prompt(server_name: &str) -> String {
    format!(
        "You are a read-only account reconciliation checker, not a coding agent. The only permitted external service is the Robinhood Trading MCP server named '{server_name}'. Do not inspect or modify the local workspace. Do not use filesystem, shell, browser, delegation, coding, checkpoint, or any built-in workspace tool. If a tool is unavailable or requires approval, report that fact and stop. Never place, cancel, replace, or preview an order. Never modify a watchlist, account setting, or any Robinhood state. Never expose credentials or tokens. You may call exactly these four MCP tools, once each, and no other tools: get_accounts, get_portfolio, get_realized_pnl, get_pnl_trade_history. Use the agent-accessible account returned by get_accounts for all subsequent account-scoped reads."
    )
}

fn build_reconciliation_prompt(server_name: &str) -> String {
    format!(
        "Perform a startup READ-ONLY Robinhood account reconciliation using only the configured MCP server '{server_name}'. Call get_accounts exactly once with {{}}. From its returned data.accounts array, select the single object whose agentic_allowed field is true and copy that object's complete account_number string internally. The account_number must be passed unchanged in the MCP input; the instruction not to repeat account numbers applies only to your final response and not to tool arguments. Then make these three calls exactly once each, in this order: get_portfolio with {{account_number:<selected account_number>}}; get_realized_pnl with {{account_number:<selected account_number>,span:day,start_date:\"\",end_date:\"\",asset_classes:null,display_currency:USD,timezone:America/New_York}}; get_pnl_trade_history with {{account_number:<selected account_number>,span:week,symbol:\"\",cursor:\"\"}}. Do not call any other tool, including skills, shell, filesystem, browser, delegation, coding, or any other MCP tool. Do not place, cancel, replace, or preview an order. Do not modify Robinhood state. Do not stop after get_accounts or after an error: complete the remaining permitted read calls exactly once each. Return only success/failure and non-sensitive counts or schema metadata; never repeat account numbers, balances, positions, symbols, or tokens."
    )
}

fn build_market_data_system_prompt(server_name: &str) -> String {
    format!(
        "You are a read-only market-data collector for the Robinhood Trading MCP server named '{server_name}', not a trading agent. Do not place, cancel, replace, preview, or submit orders. Do not modify watchlists, accounts, settings, or any Robinhood state. Do not use filesystem, shell, browser, delegation, coding, skills, or any non-Robinhood tool. If a configured endpoint is unavailable, do not substitute another endpoint and do not retry with a different tool; return failure and stop. Return only an optional paper-proposals envelope. Never invent, restate, summarize, or transform prices or timestamps; Hoodrat reads those from the raw MCP responses."
    )
}

fn build_market_data_prompt(
    server_name: &str,
    market_data_tools: &[String],
    symbols: &[String],
) -> String {
    format!(
        "Collect current READ-ONLY equity and options market data using only the configured Robinhood Trading MCP server '{server_name}'. You may call only these configured read tools, and no other tools: {}. Call only the tools listed above, exactly once each, in dependency order. Do not return a paper_proposals envelope or terminate until every configured tool has been called exactly once. Call get_equity_quotes once with every configured equity symbol in its symbols array. If get_option_chains is configured, call it once for the first configured equity underlying using {{ids:\"\",underlying_symbol:\"<one symbol>\"}}. If get_option_instruments is configured, call it only after get_option_chains returns a non-empty chain id, passing that id unchanged as chain_id. If get_option_quotes is configured and get_option_instruments returns any instrument ids, calling get_option_quotes once with those returned ids is mandatory; do not treat an empty proposal envelope as a substitute for that quote read. If a dependency is empty or unavailable, do not invent identifiers and do not make an empty-ID quote call; report failure and stop. Never request crypto data. Never use watchlist, order, review, preview, place, cancel, replace, submit, get_accounts, or any other state-changing or fallback tool. Inspect only these configured symbols or their option contracts: {}. Never report or transform bid, ask, last price, timestamps, account data, or any other market value in your response; those values must be read from the raw MCP tool outputs by Hoodrat. Return only an optional machine-readable paper-proposals object with this shape: {{\\\"paper_proposals\\\":[{{\\\"action\\\":\\\"buy|sell|short|cover|reduce|close|hold\\\",\\\"symbol\\\":\\\"<quoted symbol>\\\",\\\"asset_class\\\":\\\"equity|option\\\",\\\"quantity\\\":<number>,\\\"limit_price\\\":<number or null>,\\\"underlying\\\":\\\"<underlying or null>\\\",\\\"option_type\\\":\\\"call|put or null\\\",\\\"strike\\\":<number or null>,\\\"expiration\\\":\\\"<RFC3339 UTC/date or null>\\\",\\\"multiplier\\\":<number or null>,\\\"reason\\\":\\\"paper-only rationale\\\"}}]}}. If data is missing, stale, ambiguous, unsupported, or unavailable after all configured reads have completed, return an empty paper_proposals array.",
        market_data_tools.join(", "),
        symbols.join(", ")
    )
}

fn build_market_probe_prompt(
    server_name: &str,
    market_data_tools: &[String],
    symbols: &[String],
) -> String {
    format!(
        "Perform a READ-ONLY equity/options market-data schema probe using only the configured Robinhood Trading MCP server '{server_name}'. Call only the configured tools exactly once, in this dependency order: {}. Call get_equity_quotes once with all configured equity symbols. If get_option_chains is configured, call it once for one configured underlying with {{ids:\"\",underlying_symbol:\"<one symbol>\"}}. If get_option_instruments is configured, call it only after a non-empty chain id is returned, passing that id unchanged as chain_id. If get_option_quotes is configured, call it only after non-empty instrument ids are returned, passing them unchanged as instrument_ids. If a dependency is empty or unavailable, do not invent identifiers, do not call a fallback tool, and report failure. Never request crypto data. Never place, cancel, replace, preview, or submit an order. Never modify Robinhood state. Do not use skills or any fallback tool such as get_accounts. Return only a short success/failure statement. Do not repeat prices, timestamps, account numbers, credentials, or raw market data; Hoodrat persists and analyzes the raw tool responses locally. Configured symbols: {}.",
        market_data_tools.join(", "),
        symbols.join(", ")
    )
}

fn run_task_with_executor<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    lane_label: &str,
    prompt: String,
    options: AgentTaskOptions,
    executor: &E,
) -> Result<AgentRunResult> {
    let run_id = format!("{}-{}", lane_label, Utc::now().format("%Y%m%dT%H%M%S%.3fZ"));
    let strategy_contract = options.strategy_contract.clone();
    store.begin_run_with_strategy(&run_id, lane_label, &prompt, strategy_contract.as_ref())?;
    if let Some(contract) = strategy_contract.as_ref() {
        store.record_audit(
            Some(&run_id),
            "strategy",
            "contract_bound",
            &serde_json::json!({
                "contract_version": contract.contract_version,
                "contract_fingerprint": contract.fingerprint(),
            }),
        )?;
    }

    let command = AgentCommand::from_config_with_options(
        config,
        prompt,
        options.plan_mode,
        options.auto_approve,
        options.system_prompt,
    );
    let command = if options.restrict_local_commands {
        command.restrict_local_commands()
    } else {
        command
    };
    let started = Instant::now();
    let output = match executor.execute(&command) {
        Ok(output) => output,
        Err(error) => {
            let summary = format!("Cline launch failed: {error:#}");
            store.finish_run(&run_id, "failed", "", &summary)?;
            store.record_audit(
                Some(&run_id),
                "agent",
                "launch_failed",
                &serde_json::json!({"error": error.to_string()}),
            )?;
            return Err(error);
        }
    };
    let raw_output = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut summary_parts = Vec::new();
    let mut event_count = 0;
    let mut tool_event_count = 0;
    let mut robinhood_read_count = 0;
    let mut mcp_error_count = 0;
    let mut unexpected_tool_count = 0;
    let mut pending_tools = HashMap::new();
    let mut typed_payloads = BTreeMap::new();
    let mut typed_ingestion_error = None;
    let mut expected_tool_counts = BTreeMap::new();
    let mut mcp_outputs = BTreeMap::new();
    let mut selected_account_number = None;

    for (index, line) in raw_output.lines().enumerate() {
        let sequence_number = index as u32;
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(_) => {
                summary_parts.push(line.to_owned());
                store.record_agent_event(&AgentEventRecord {
                    run_id: run_id.clone(),
                    sequence_number,
                    event_type: "unstructured".to_owned(),
                    text: Some(line.to_owned()),
                    raw_json: line.to_owned(),
                    recorded_at: Utc::now().to_rfc3339(),
                })?;
                event_count += 1;
                continue;
            }
        };

        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let text = event_text(&value);
        if let Some(text) = &text {
            summary_parts.push(text.clone());
        }
        store.record_agent_event(&AgentEventRecord {
            run_id: run_id.clone(),
            sequence_number,
            event_type,
            text,
            raw_json: line.to_owned(),
            recorded_at: Utc::now().to_rfc3339(),
        })?;
        event_count += 1;

        for tool in collect_tool_events(&value, &mut pending_tools) {
            let input = tool.input.as_ref().map(serde_json::to_string).transpose()?;
            let output_value = tool.output.as_ref().map(parse_json_text);
            let output_is_error = output_value.as_ref().is_some_and(|output| {
                output
                    .get("isError")
                    .or_else(|| output.get("is_error"))
                    .and_then(Value::as_bool)
                    == Some(true)
            });
            let tool_is_error = tool.is_error || output_is_error;
            let output_json = output_value
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            store.record_tool_event(&AgentToolEventRecord {
                run_id: run_id.clone(),
                sequence_number,
                tool_name: tool.storage_name(),
                input_json: input,
                output_json,
                is_error: tool_is_error,
                recorded_at: Utc::now().to_rfc3339(),
            })?;
            store.record_audit(
                Some(&run_id),
                "tool",
                "call_observed",
                &serde_json::json!({
                    "tool_name": tool.name,
                    "mcp_server": tool.server_name,
                    "mcp_tool": tool.mcp_tool_name,
                    "is_error": tool_is_error,
                    "sequence_number": sequence_number,
                }),
            )?;
            tool_event_count += 1;
            if tool_is_error {
                mcp_error_count += u32::from(tool.server_name.is_some());
            }
            if let Some(expected_server) = options.expected_mcp_server.as_deref() {
                let is_expected_read =
                    tool.is_expected_read(expected_server, options.expected_mcp_tools.as_ref());
                if is_expected_read {
                    *expected_tool_counts
                        .entry(tool.target_name().to_ascii_lowercase())
                        .or_insert(0) += 1;
                }
                if is_expected_read {
                    if let Some(output) = output_value.as_ref() {
                        mcp_outputs
                            .entry(tool.target_name().to_ascii_lowercase())
                            .or_insert_with(Vec::new)
                            .push(McpOutput {
                                is_error: tool_is_error,
                                value: output.clone(),
                            });
                    }
                }
                if is_expected_read && !tool_is_error {
                    robinhood_read_count += 1;
                }
                if !is_expected_read {
                    unexpected_tool_count += 1;
                }
            }
            if options.expected_mcp_server.is_none()
                || options
                    .expected_mcp_server
                    .as_deref()
                    .is_some_and(|server| {
                        tool.is_expected_read(server, options.expected_mcp_tools.as_ref())
                            && !tool_is_error
                    })
            {
                if options.strict_typed_ingestion {
                    let Some(output) = output_value.as_ref() else {
                        typed_ingestion_error = Some(format!(
                            "read-only reconciliation tool '{}' returned no output",
                            tool.target_name()
                        ));
                        continue;
                    };
                    if tool.target_name().eq_ignore_ascii_case("get_accounts") {
                        match store.ingest_typed_broker_payload(tool.target_name(), output) {
                            Ok(BrokerPayload::Accounts(accounts)) => {
                                match single_agentic_account(&accounts) {
                                    Ok(account_number) => {
                                        selected_account_number = Some(account_number)
                                    }
                                    Err(error) => typed_ingestion_error = Some(error.to_string()),
                                }
                                typed_payloads
                                    .insert(tool.target_name().to_owned(), output.clone());
                            }
                            Ok(_) => {
                                typed_ingestion_error = Some(
                                    "get_accounts returned the wrong typed payload".to_owned(),
                                );
                            }
                            Err(error) => {
                                typed_ingestion_error = Some(format!(
                                    "typed ingestion for '{}' failed: {error:#}",
                                    tool.target_name()
                                ));
                            }
                        }
                    } else if let Some(expected_account) = selected_account_number.as_deref() {
                        if tool_account_number(tool.input.as_ref()) != Some(expected_account) {
                            typed_ingestion_error = Some(format!(
                                "{} did not pass the selected account identifier unchanged",
                                tool.target_name()
                            ));
                        } else {
                            match store.ingest_typed_broker_payload(tool.target_name(), output) {
                                Ok(_) => {
                                    typed_payloads
                                        .insert(tool.target_name().to_owned(), output.clone());
                                }
                                Err(error) => {
                                    typed_ingestion_error = Some(format!(
                                        "typed ingestion for '{}' failed: {error:#}",
                                        tool.target_name()
                                    ));
                                }
                            }
                        }
                    } else {
                        typed_ingestion_error = Some(format!(
                            "{} was called before get_accounts selected an agent-accessible account",
                            tool.target_name()
                        ));
                    }
                } else {
                    ingest_tool_output(store, &run_id, tool.target_name(), output_value.as_ref())?;
                }
            }
        }
    }

    let mut seen_summary_parts = HashSet::new();
    let mut summary = summary_parts
        .into_iter()
        .filter(|part| seen_summary_parts.insert(part.clone()))
        .collect::<Vec<_>>()
        .join("\n");
    if !stderr.trim().is_empty() {
        summary.push_str("\nCline stderr: ");
        summary.push_str(stderr.trim());
    }
    if summary.is_empty() {
        summary = "Cline produced no structured output.".to_owned();
    }
    let expected_reads_complete = options.expected_mcp_tools.as_ref().is_some_and(|tools| {
        tools.len() == expected_tool_counts.len()
            && tools.iter().all(|tool| {
                expected_tool_counts
                    .get(&tool.to_ascii_lowercase())
                    .copied()
                    == Some(1)
            })
    }) && mcp_error_count == 0
        && unexpected_tool_count == 0;
    let mut status = if output.status.success() {
        "completed"
    } else {
        "failed"
    };
    if output.status.success() && options.expected_mcp_server.is_some() && robinhood_read_count == 0
    {
        status = "mcp_not_verified";
    }
    if output.status.success() && options.expected_mcp_server.is_some() && unexpected_tool_count > 0
    {
        status = "policy_violation";
    }
    if output.status.success()
        && options.expected_mcp_server.is_some()
        && robinhood_read_count > 0
        && !expected_reads_complete
        && unexpected_tool_count == 0
        && mcp_error_count == 0
    {
        status = "market_data_incomplete";
    }
    let reconciliation = if options.strict_typed_ingestion && output.status.success() {
        let expected_count = options.expected_mcp_tools.as_ref().map_or(0, HashSet::len);
        let has_exact_tool_cardinality = options.expected_mcp_tools.as_ref().is_some_and(|tools| {
            tools.iter().all(|tool| {
                expected_tool_counts
                    .get(&tool.to_ascii_lowercase())
                    .copied()
                    == Some(1)
            })
        });
        if unexpected_tool_count > 0 {
            status = "policy_violation";
            None
        } else if let Some(error) = typed_ingestion_error.as_deref() {
            status = "reconciliation_failed";
            summary.push_str("\nTyped ingestion error: ");
            summary.push_str(error);
            None
        } else if mcp_error_count > 0 {
            status = "reconciliation_failed";
            None
        } else if !has_exact_tool_cardinality
            || robinhood_read_count as usize != expected_count
            || typed_payloads.len() != expected_count
        {
            status = "reconciliation_incomplete";
            None
        } else {
            let report = store.finalize_reconciliation(&typed_payloads)?;
            if report.status == "drift_detected" {
                status = "drift_detected";
            }
            Some(report)
        }
    } else {
        None
    };
    if let Some(report) = reconciliation.as_ref() {
        status = match report.status.as_str() {
            "baseline" | "reconciled" | "drift_detected" => report.status.as_str(),
            _ => "reconciliation_incomplete",
        };
    }
    store.finish_run(&run_id, status, &raw_output, &summary)?;
    store.record_audit(
        Some(&run_id),
        "agent",
        "task_finished",
        &serde_json::json!({
            "lane": lane_label,
            "exit_code": output.status.code(),
            "event_count": event_count,
            "tool_event_count": tool_event_count,
            "robinhood_read_count": robinhood_read_count,
            "mcp_error_count": mcp_error_count,
            "unexpected_tool_count": unexpected_tool_count,
            "typed_ingestion_error": typed_ingestion_error,
            "elapsed_ms": started.elapsed().as_millis(),
        }),
    )?;

    Ok(AgentRunResult {
        run_id,
        exit_code: output.status.code(),
        event_count,
        tool_event_count,
        robinhood_read_count,
        mcp_error_count,
        unexpected_tool_count,
        reconciliation,
        raw_output,
        expected_reads_complete,
        mcp_outputs,
    })
}

#[derive(Debug)]
struct ToolEvent {
    name: String,
    input: Option<Value>,
    output: Option<Value>,
    is_error: bool,
    server_name: Option<String>,
    mcp_tool_name: Option<String>,
}

impl ToolEvent {
    fn storage_name(&self) -> String {
        match (&self.server_name, &self.mcp_tool_name) {
            (Some(server), Some(tool)) => format!("{server}::{tool}"),
            _ => self.name.clone(),
        }
    }

    fn target_name(&self) -> &str {
        self.mcp_tool_name.as_deref().unwrap_or(&self.name)
    }

    #[allow(dead_code)]
    fn is_allowed_read(&self) -> bool {
        matches!(
            self.target_name().to_ascii_lowercase().as_str(),
            "get_accounts"
                | "get_portfolio"
                | "get_realized_pnl"
                | "get_pnl_trade_history"
                | "get_watchlists"
                | "get_watchlist_items"
                | "get_option_watchlist"
                | "get_popular_watchlists"
                | "get_equity_historicals"
                | "get_equity_fundamentals"
                | "get_financials"
                | "get_equity_price_book"
                | "get_equity_quotes"
                | "get_option_chains"
                | "get_option_instruments"
                | "get_option_quotes"
                | "get_index_quotes"
        )
    }

    fn is_expected_read(
        &self,
        expected_server: &str,
        expected_tools: Option<&HashSet<String>>,
    ) -> bool {
        self.server_name.as_deref() == Some(expected_server)
            && expected_tools.is_some_and(|tools| {
                tools
                    .iter()
                    .any(|tool| tool.eq_ignore_ascii_case(self.target_name()))
            })
    }
}

#[derive(Debug)]
struct PendingTool {
    name: String,
    input: Option<Value>,
    server_name: Option<String>,
    mcp_tool_name: Option<String>,
}

fn tool_event(value: &Value) -> Option<ToolEvent> {
    let object = value.as_object()?;
    let name = ["tool_name", "toolName", "name", "tool"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))?;
    let has_tool_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.contains("tool") || value.contains("mcp"))
        .unwrap_or(false);
    if !has_tool_type
        && !object.contains_key("tool_name")
        && !object.contains_key("toolName")
        && !object.contains_key("tool_input")
        && !object.contains_key("tool_output")
    {
        return None;
    }
    let input = first_value(object, &["input", "arguments", "tool_input", "toolInput"]);
    let server_name = first_string(object, &["server_name", "serverName"]).or_else(|| {
        input
            .as_ref()
            .and_then(|value| value.get("server_name"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    });
    let (composite_server_name, composite_tool_name) = split_mcp_tool_name(name);
    let mcp_tool_name = first_string(object, &["mcp_tool_name", "mcpToolName"]).or_else(|| {
        if name == "use_mcp_tool" {
            input
                .as_ref()
                .and_then(|value| value.get("tool_name"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        } else {
            None
        }
    });
    Some(ToolEvent {
        name: name.to_owned(),
        input,
        output: first_value(object, &["output", "result", "tool_output", "toolOutput"]),
        is_error: object
            .get("is_error")
            .or_else(|| object.get("isError"))
            .and_then(Value::as_bool)
            .unwrap_or_else(|| {
                object
                    .get("type")
                    .and_then(Value::as_str)
                    .map(|value| value.contains("error"))
                    .unwrap_or(false)
            }),
        server_name: server_name.or(composite_server_name),
        mcp_tool_name: mcp_tool_name.or(composite_tool_name),
    })
}

fn split_mcp_tool_name(name: &str) -> (Option<String>, Option<String>) {
    name.split_once("__")
        .map(|(server, tool)| (Some(server.to_owned()), Some(tool.to_owned())))
        .unwrap_or((None, None))
}

fn first_value(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<Value> {
    keys.iter().find_map(|key| object.get(*key).cloned())
}

fn first_string(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}

fn tool_account_number(input: Option<&Value>) -> Option<&str> {
    let object = input.and_then(Value::as_object)?;
    object
        .get("account_number")
        .or_else(|| {
            object
                .get("arguments")
                .and_then(|value| value.get("account_number"))
        })
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn collect_tool_events(
    value: &Value,
    pending_tools: &mut HashMap<String, PendingTool>,
) -> Vec<ToolEvent> {
    let Some(event) = value.get("event").and_then(Value::as_object) else {
        return tool_event(value).into_iter().collect();
    };
    if event.get("contentType").and_then(Value::as_str) != Some("tool") {
        return tool_event(value).into_iter().collect();
    }

    let Some(name) = first_string(event, &["toolName", "tool_name", "name"]) else {
        return Vec::new();
    };
    let (composite_server_name, composite_tool_name) = split_mcp_tool_name(&name);
    let call_id = first_string(event, &["toolCallId", "tool_call_id"]);
    let input = first_value(event, &["input", "arguments", "tool_input", "toolInput"]);
    let server_name = input.as_ref().and_then(|value| {
        value
            .get("server_name")
            .or_else(|| value.get("serverName"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    });
    let mcp_tool_name = if name == "use_mcp_tool" {
        input.as_ref().and_then(|value| {
            value
                .get("tool_name")
                .or_else(|| value.get("toolName"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
    } else {
        None
    };
    let server_name = server_name.or(composite_server_name);
    let mcp_tool_name = mcp_tool_name.or(composite_tool_name);
    let content_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if content_type == "content_start" {
        if let Some(call_id) = call_id {
            pending_tools.insert(
                call_id,
                PendingTool {
                    name,
                    input,
                    server_name,
                    mcp_tool_name,
                },
            );
        }
        return Vec::new();
    }

    if content_type != "content_end" {
        return Vec::new();
    }
    let output = first_value(event, &["output", "result", "tool_output", "toolOutput"]);
    let error = event.get("error");
    let is_error = error.is_some()
        || output
            .as_ref()
            .and_then(Value::as_object)
            .is_some_and(|object| object.contains_key("error"));
    let pending = call_id.and_then(|call_id| pending_tools.remove(&call_id));
    let Some(pending) = pending else {
        return vec![ToolEvent {
            name,
            input,
            output,
            is_error,
            server_name,
            mcp_tool_name,
        }];
    };
    vec![ToolEvent {
        name: pending.name,
        input: pending.input,
        output,
        is_error,
        server_name: pending.server_name,
        mcp_tool_name: pending.mcp_tool_name,
    }]
}

fn ingest_tool_output(
    store: &Store,
    run_id: &str,
    tool_name: &str,
    output: Option<&Value>,
) -> Result<()> {
    let Some(output) = output else {
        return Ok(());
    };
    let lower_name = tool_name.to_ascii_lowercase();
    if lower_name.contains("portfolio") {
        if let Ok(snapshot) = PortfolioSnapshot::from_value(output) {
            store.ingest_portfolio_snapshot(&snapshot)?;
            store.record_audit(
                Some(run_id),
                "broker_data",
                "portfolio_ingested",
                &serde_json::json!({"tool_name": tool_name}),
            )?;
        }
    }
    if lower_name.contains("order")
        || lower_name.contains("execution")
        || lower_name.contains("trade")
    {
        if let Ok(execution) = ExecutionRecord::from_value(output) {
            let symbol = execution.symbol.clone();
            store.ingest_execution(&execution, Some(run_id))?;
            store.record_audit(
                Some(run_id),
                "broker_data",
                "execution_ingested",
                &serde_json::json!({"tool_name": tool_name, "symbol": symbol}),
            )?;
        }
    }
    Ok(())
}

fn event_text(value: &Value) -> Option<String> {
    if let Some(text) = ["text", "say", "ask", "reasoning"].iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }) {
        return Some(text);
    }
    if let Some(message) = value
        .get("error")
        .and_then(Value::as_object)
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
    {
        return Some(message.to_owned());
    }
    for key in ["event", "result"] {
        if let Some(nested) = value.get(key) {
            if let Some(text) = event_text(nested) {
                return Some(text);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::ExitStatus;

    #[derive(Debug)]
    struct FakeExecutor {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        status: ExitStatus,
    }

    impl AgentExecutor for FakeExecutor {
        fn execute(&self, _command: &AgentCommand) -> Result<Output> {
            Ok(Output {
                status: self.status,
                stdout: self.stdout.clone(),
                stderr: self.stderr.clone(),
            })
        }
    }

    fn success_status() -> ExitStatus {
        if cfg!(windows) {
            Command::new("cmd")
                .args(["/C", "exit", "0"])
                .status()
                .unwrap()
        } else {
            Command::new("sh").args(["-c", "exit 0"]).status().unwrap()
        }
    }

    #[test]
    fn prompt_identifies_lane_and_safety_boundary() {
        let prompt = build_prompt(Lane::EquityOptions, "state", "policy");
        assert!(prompt.contains("equity_options"));
        assert!(prompt.contains("not a pre-trade firewall"));
        assert!(prompt.contains("state"));
    }

    #[test]
    fn command_contains_isolated_fresh_task_flags() {
        let config = AgentConfig::default();
        let command = AgentCommand::from_config(&config, "prompt".to_owned());
        assert_eq!(command.executable, "cline");
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--json", "--model"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--provider", "openrouter"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--timeout", "300"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--auto-approve", "true"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--data-dir", "data/cline/data"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--config", "data/cline"]));
        assert_eq!(command.args.last(), Some(&"prompt".to_owned()));
    }

    #[test]
    fn resolves_installed_cline_entrypoint_when_available() {
        if let Some(path) = resolve_executable("cline") {
            assert!(path.is_file());
            assert!(matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("cmd") | Some("exe") | Some("bat") | Some("")
            ));
        }
    }

    #[test]
    fn smoke_test_command_forces_safe_probe_flags() {
        let config = AgentConfig::default();
        let command = AgentCommand::from_config_with_options(
            &config,
            "smoke prompt".to_owned(),
            true,
            true,
            Some("single-line system prompt".to_owned()),
        );
        assert_eq!(command.args.first(), Some(&"--plan".to_owned()));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--auto-approve", "true"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--retries", "1"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--system", "single-line system prompt"]));
        assert_eq!(command.args.last(), Some(&"smoke prompt".to_owned()));
    }

    #[test]
    fn read_only_tasks_deny_local_shell_commands() {
        let config = AgentConfig::default();
        let command = AgentCommand::from_config_with_options(
            &config,
            "probe prompt".to_owned(),
            true,
            true,
            Some("read-only system prompt".to_owned()),
        )
        .restrict_local_commands();
        assert_eq!(
            command.environment,
            vec![
                ("CLINE_DATA_DIR".to_owned(), "data/cline/data".to_owned()),
                (
                    "CLINE_COMMAND_PERMISSIONS".to_owned(),
                    r#"{"allow":[],"deny":["*"]}"#.to_owned()
                )
            ]
        );
    }

    #[test]
    fn read_only_profile_disables_all_builtin_workspace_tools() {
        let directory = std::env::temp_dir().join(format!(
            "hoodrat-cline-settings-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let guard = prepare_read_only_cline_tools(&directory).unwrap();
        let settings = serde_json::from_str::<Value>(
            &fs::read_to_string(directory.join("settings/global-settings.json")).unwrap(),
        )
        .unwrap();
        let disabled = settings["disabledTools"].as_array().unwrap();
        assert!(READ_ONLY_DISABLED_CLINE_TOOLS
            .iter()
            .all(|tool| disabled.iter().any(|value| value.as_str() == Some(tool))));
        guard.restore().unwrap();
        assert!(!directory.join("settings/global-settings.json").exists());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn parses_current_composite_mcp_tool_name() {
        let value = serde_json::json!({
            "type": "mcp_tool_result",
            "toolName": "robinhood-trading__get_accounts",
            "result": {"accounts": []}
        });
        let event = tool_event(&value).unwrap();
        assert_eq!(event.server_name.as_deref(), Some("robinhood-trading"));
        assert_eq!(event.mcp_tool_name.as_deref(), Some("get_accounts"));
        assert!(event.is_allowed_read());
    }

    #[test]
    fn event_text_handles_cline_fields() {
        assert_eq!(
            event_text(&serde_json::json!({"type":"say","text":"ok"})),
            Some("ok".to_owned())
        );
        assert_eq!(
            event_text(&serde_json::json!({"type":"say","say":"ok"})),
            Some("ok".to_owned())
        );
        assert_eq!(event_text(&serde_json::json!({"type":"say"})), None);
    }

    #[test]
    fn extracts_nested_cline_error_messages() {
        let value = serde_json::json!({
            "type": "agent_event",
            "event": {
                "type": "error",
                "error": {"message": "provider unavailable"}
            }
        });
        assert_eq!(event_text(&value), Some("provider unavailable".to_owned()));
    }

    #[test]
    fn parses_tool_event_shapes_without_assuming_one_schema() {
        let value = serde_json::json!({
            "type": "mcp_tool_result",
            "toolName": "get_portfolio",
            "result": {"portfolio_value": 100}
        });
        let event = tool_event(&value).unwrap();
        assert_eq!(event.name, "get_portfolio");
        assert_eq!(event.output.unwrap()["portfolio_value"], 100);
    }

    #[test]
    fn pairs_nested_mcp_tool_start_and_result() {
        let mut pending = HashMap::new();
        let start = serde_json::json!({
            "type": "agent_event",
            "event": {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "use_mcp_tool",
                "toolCallId": "call-1",
                "input": {
                    "server_name": "robinhood-trading",
                    "tool_name": "get_accounts",
                    "arguments": {}
                }
            }
        });
        assert!(collect_tool_events(&start, &mut pending).is_empty());
        let end = serde_json::json!({
            "type": "agent_event",
            "event": {
                "type": "content_end",
                "contentType": "tool",
                "toolName": "use_mcp_tool",
                "toolCallId": "call-1",
                "output": {"accounts": []}
            }
        });
        let events = collect_tool_events(&end, &mut pending);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].server_name.as_deref(), Some("robinhood-trading"));
        assert_eq!(events[0].mcp_tool_name.as_deref(), Some("get_accounts"));
        assert!(events[0].is_allowed_read());
    }

    #[test]
    fn identifies_tool_errors() {
        let value = serde_json::json!({
            "type": "mcp_tool_error",
            "tool_name": "place_order",
            "is_error": true,
            "output": "rejected"
        });
        let event = tool_event(&value).unwrap();
        assert_eq!(event.name, "place_order");
        assert!(event.is_error);
    }

    #[test]
    fn fake_executor_records_tool_and_broker_data() {
        let output = concat!(
            "{\"type\":\"say\",\"text\":\"checking\"}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"use_mcp_tool\",\"toolCallId\":\"call-1\",\"input\":{\"server_name\":\"robinhood-trading\",\"tool_name\":\"get_portfolio\",\"arguments\":{}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"use_mcp_tool\",\"toolCallId\":\"call-1\",\"output\":{\"portfolio_value\":1200,\"buying_power\":400}}}\n",
            "not-json\n"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let config = AgentConfig {
            executable: "fake".to_owned(),
            ..AgentConfig::default()
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_fresh_task_with_executor(
            &config,
            &store,
            Lane::EquityOptions,
            "context",
            "policy",
            &executor,
        )
        .unwrap();
        assert_eq!(result.event_count, 4);
        assert_eq!(result.tool_event_count, 1);
        assert_eq!(result.robinhood_read_count, 0);
        assert_eq!(result.unexpected_tool_count, 0);
        assert_eq!(store.tool_event_count(&result.run_id).unwrap(), 1);
        assert_eq!(store.portfolio_snapshot_count().unwrap(), 1);
        assert!(store
            .recent_events(10)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "unstructured"));
    }

    #[test]
    fn failed_process_exit_is_persisted_as_failed_run() {
        let executor = FakeExecutor {
            stdout: br#"{"type":"say","text":"failed"}"#.to_vec(),
            stderr: b"provider unavailable".to_vec(),
            status: if cfg!(windows) {
                Command::new("cmd")
                    .args(["/C", "exit", "7"])
                    .status()
                    .unwrap()
            } else {
                Command::new("sh").args(["-c", "exit 7"]).status().unwrap()
            },
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_smoke_test(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert_eq!(result.exit_code, Some(7));
        assert_eq!(store.latest_run().unwrap().unwrap().status, "failed");
    }

    #[test]
    fn smoke_test_uses_dedicated_run_label() {
        let executor = FakeExecutor {
            stdout: br#"{"type":"say","text":"read only"}"#.to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_smoke_test(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert!(result.run_id.starts_with("smoke_test-"));
        assert_eq!(result.tool_event_count, 0);
        assert_eq!(
            store.latest_run().unwrap().unwrap().status,
            "mcp_not_verified"
        );
    }

    #[test]
    fn smoke_test_rejects_unexpected_tool_calls() {
        let output = concat!(
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"skills\",\"toolCallId\":\"call-skill\",\"input\":{\"skill\":\"example\"}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"skills\",\"toolCallId\":\"call-skill\",\"output\":{\"ok\":true}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"call-rh\",\"input\":{}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"call-rh\",\"output\":{\"accounts\":[]}}}\n"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_smoke_test(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert_eq!(result.robinhood_read_count, 1);
        assert_eq!(result.unexpected_tool_count, 1);
        assert_eq!(
            store.latest_run().unwrap().unwrap().status,
            "policy_violation"
        );
    }

    #[test]
    fn market_data_task_requires_configured_reads_and_preserves_raw_outputs() {
        let output = concat!(
            "{\"type\":\"tool_call\",\"tool_name\":\"robinhood-trading__get_equity_price_book\",\"input\":{\"symbol\":\"SPY\"},\"output\":{\"ok\":true}}\n",
            "{\"type\":\"tool_call\",\"tool_name\":\"robinhood-trading__get_equity_historicals\",\"input\":{\"symbol\":\"SPY\"},\"output\":{\"ok\":true}}\n",
            "{\"type\":\"say\",\"text\":\"{\\\"paper_proposals\\\":[]}\"}"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_market_data(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &[
                "get_equity_price_book".to_owned(),
                "get_equity_historicals".to_owned(),
            ],
            &["SPY".to_owned()],
            &executor,
        )
        .unwrap();
        assert_eq!(result.robinhood_read_count, 2);
        assert_eq!(result.unexpected_tool_count, 0);
        assert!(result.expected_reads_complete);
        assert_eq!(result.mcp_outputs.len(), 2);
        assert!(result.raw_output.contains("paper_proposals"));
    }

    #[test]
    fn reconciliation_requires_exactly_four_typed_robinhood_reads() {
        let output = concat!(
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts\",\"input\":{}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts\",\"output\":{\"structuredContent\":{\"data\":{\"accounts\":[{\"account_number\":\"account-1\",\"agentic_allowed\":true}]}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_portfolio\",\"toolCallId\":\"portfolio\",\"input\":{\"account_number\":\"account-1\"}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_portfolio\",\"toolCallId\":\"portfolio\",\"output\":{\"structuredContent\":{\"data\":{\"total_value\":\"100\",\"buying_power\":\"50\"}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_realized_pnl\",\"toolCallId\":\"pnl\",\"input\":{\"account_number\":\"account-1\"}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_realized_pnl\",\"toolCallId\":\"pnl\",\"output\":{\"structuredContent\":{\"data\":{\"realized_gain\":\"1.25\",\"window\":\"day\"}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_pnl_trade_history\",\"toolCallId\":\"history\",\"input\":{\"account_number\":\"account-1\"}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_pnl_trade_history\",\"toolCallId\":\"history\",\"output\":{\"structuredContent\":{\"data\":{\"trades\":[{\"trade_id\":\"trade-1\",\"symbol\":\"TEST\",\"realized_pnl\":\"1.25\"}]}}}}}\n"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_reconciliation(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert_eq!(result.robinhood_read_count, 4);
        assert_eq!(result.unexpected_tool_count, 0);
        assert_eq!(result.reconciliation.as_ref().unwrap().status, "baseline");
        assert_eq!(store.latest_run().unwrap().unwrap().status, "baseline");
    }

    #[test]
    fn reconciliation_rejects_non_robinhood_tools_even_when_process_succeeds() {
        let output = concat!(
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"run_commands\",\"toolCallId\":\"call-shell\",\"input\":{\"commands\":[\"echo blocked\"]}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"run_commands\",\"toolCallId\":\"call-shell\",\"output\":{\"ok\":true}}}\n"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_reconciliation(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert_eq!(result.unexpected_tool_count, 1);
        assert!(result.reconciliation.is_none());
        assert_eq!(
            store.latest_run().unwrap().unwrap().status,
            "policy_violation"
        );
    }

    #[test]
    fn reconciliation_rejects_duplicate_required_reads() {
        let output = concat!(
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts\",\"input\":{}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts\",\"output\":{\"structuredContent\":{\"data\":{\"accounts\":[{\"account_number\":\"account-1\",\"agentic_allowed\":true}]}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts-duplicate\",\"input\":{}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts-duplicate\",\"output\":{\"structuredContent\":{\"data\":{\"accounts\":[{\"account_number\":\"account-1\",\"agentic_allowed\":true}]}}}}}\n"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_reconciliation(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert_eq!(result.robinhood_read_count, 2);
        assert_eq!(result.unexpected_tool_count, 0);
        assert!(result.reconciliation.is_none());
        assert_eq!(
            store.latest_run().unwrap().unwrap().status,
            "reconciliation_incomplete"
        );
    }

    #[test]
    fn reconciliation_rejects_dependent_read_for_wrong_account() {
        let output = concat!(
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts\",\"input\":{}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_accounts\",\"toolCallId\":\"accounts\",\"output\":{\"structuredContent\":{\"data\":{\"accounts\":[{\"account_number\":\"account-1\",\"agentic_allowed\":true}]}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"use_mcp_tool\",\"toolCallId\":\"portfolio\",\"input\":{\"server_name\":\"robinhood-trading\",\"tool_name\":\"get_portfolio\",\"arguments\":{\"account_number\":\"account-2\"}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"use_mcp_tool\",\"toolCallId\":\"portfolio\",\"output\":{\"structuredContent\":{\"data\":{\"total_value\":\"100\",\"buying_power\":\"50\"}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_realized_pnl\",\"toolCallId\":\"pnl\",\"input\":{\"account_number\":\"account-1\"}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_realized_pnl\",\"toolCallId\":\"pnl\",\"output\":{\"structuredContent\":{\"data\":{\"realized_gain\":\"1.25\"}}}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_start\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_pnl_trade_history\",\"toolCallId\":\"history\",\"input\":{\"account_number\":\"account-1\"}}}\n",
            "{\"type\":\"agent_event\",\"event\":{\"type\":\"content_end\",\"contentType\":\"tool\",\"toolName\":\"robinhood-trading__get_pnl_trade_history\",\"toolCallId\":\"history\",\"output\":{\"structuredContent\":{\"data\":{\"trades\":[]}}}}}\n"
        );
        let executor = FakeExecutor {
            stdout: output.as_bytes().to_vec(),
            stderr: Vec::new(),
            status: success_status(),
        };
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let result = run_read_only_reconciliation(
            &AgentConfig::default(),
            &store,
            "robinhood-trading",
            &executor,
        )
        .unwrap();
        assert_eq!(result.robinhood_read_count, 4);
        assert!(result.reconciliation.is_none());
        assert_eq!(
            store.latest_run().unwrap().unwrap().status,
            "reconciliation_failed"
        );
    }
}
