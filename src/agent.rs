use crate::config::AgentConfig;
use crate::ingestion::{parse_json_text, BrokerDataSink, ExecutionRecord, PortfolioSnapshot};
use crate::store::{AgentEventRecord, AgentToolEventRecord, Store};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub enum Lane {
    EquityOptions,
    Crypto,
}

impl Lane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EquityOptions => "equity_options",
            Self::Crypto => "crypto",
        }
    }

    pub fn capabilities(self) -> &'static str {
        match self {
            Self::EquityOptions => "equities and options",
            Self::Crypto => "crypto",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentCommand {
    pub executable: String,
    pub args: Vec<String>,
    pub working_directory: Option<std::path::PathBuf>,
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
        if let Some(system_prompt) = system_prompt {
            args.extend(["--system".to_owned(), system_prompt]);
        }
        args.push(prompt);
        Self {
            executable: config.executable.clone(),
            args,
            working_directory: config.working_directory.clone(),
        }
    }

    fn spawn(&self) -> Result<Output> {
        let executable =
            resolve_executable(&self.executable).unwrap_or_else(|| PathBuf::from(&self.executable));
        let mut command = process_command(&executable, &self.args);
        if let Some(directory) = &self.working_directory {
            command.current_dir(directory);
        }
        command
            .output()
            .with_context(|| format!("failed to launch Cline executable '{}'", self.executable))
    }
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
        let mut command = Command::new("cmd.exe");
        command.arg("/D").arg("/S").arg("/C").arg(executable);
        command.args(args);
        return command;
    }

    let mut command = Command::new(executable);
    command.args(args);
    command
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
}

#[derive(Debug, Clone)]
struct AgentTaskOptions {
    plan_mode: bool,
    auto_approve: bool,
    system_prompt: Option<String>,
    expected_mcp_server: Option<String>,
}

pub fn build_prompt(lane: Lane, context: &str, config_summary: &str) -> String {
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
Configured monitoring policy (not a pre-trade firewall in direct MCP mode):\n{}",
        lane.as_str(),
        lane.capabilities(),
        context,
        config_summary
    )
}

pub fn run_fresh_task(
    config: &AgentConfig,
    store: &Store,
    lane: Lane,
    context: &str,
    config_summary: &str,
) -> Result<AgentRunResult> {
    run_fresh_task_with_executor(
        config,
        store,
        lane,
        context,
        config_summary,
        &ProcessAgentExecutor,
    )
}

pub fn run_fresh_task_with_executor<E: AgentExecutor>(
    config: &AgentConfig,
    store: &Store,
    lane: Lane,
    context: &str,
    config_summary: &str,
    executor: &E,
) -> Result<AgentRunResult> {
    let prompt = build_prompt(lane, context, config_summary);
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
            auto_approve: false,
            system_prompt: Some(build_smoke_test_system_prompt(robinhood_server_name)),
            expected_mcp_server: Some(robinhood_server_name.to_owned()),
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
        "Perform a READ-ONLY Robinhood Trading MCP connectivity test using only\n\
the configured MCP server '{server_name}'. Your first action must be a read\n\
call to that server. Use the MCP tool named 'get_accounts' if available, then\n\
read-only calls for 'get_portfolio', 'get_realized_pnl', and\n\
'get_pnl_trade_history'. Do not call any write-capable tool.\n\n\
For every requested read, report success or the exact unavailable/auth error.\n\
Return a concise connectivity report with non-sensitive fields only. This is\n\
not an investment recommendation and must not change Robinhood state."
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
    store.begin_run(&run_id, lane_label, &prompt)?;

    let command = AgentCommand::from_config_with_options(
        config,
        prompt,
        options.plan_mode,
        options.auto_approve,
        options.system_prompt,
    );
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
    let mut pending_tools = HashMap::new();

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
                is_error: tool.is_error,
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
                    "is_error": tool.is_error,
                    "sequence_number": sequence_number,
                }),
            )?;
            tool_event_count += 1;
            if tool.is_error {
                mcp_error_count += u32::from(tool.server_name.is_some());
            }
            if options.expected_mcp_server.as_deref() == tool.server_name.as_deref()
                && !tool.is_error
                && tool.is_allowed_read()
            {
                robinhood_read_count += 1;
            }
            ingest_tool_output(store, &run_id, tool.target_name(), output_value.as_ref())?;
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
    let mut status = if output.status.success() {
        "completed"
    } else {
        "failed"
    };
    if output.status.success() && options.expected_mcp_server.is_some() && robinhood_read_count == 0
    {
        status = "mcp_not_verified";
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
        )
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
        server_name,
        mcp_tool_name,
    })
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
        let prompt = build_prompt(Lane::Crypto, "state", "policy");
        assert!(prompt.contains("crypto"));
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
    fn smoke_test_command_forces_plan_and_no_auto_approval() {
        let config = AgentConfig::default();
        let command = AgentCommand::from_config_with_options(
            &config,
            "smoke prompt".to_owned(),
            true,
            false,
            Some("single-line system prompt".to_owned()),
        );
        assert_eq!(command.args.first(), Some(&"--plan".to_owned()));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--auto-approve", "false"]));
        assert!(command
            .args
            .windows(2)
            .any(|pair| pair == ["--system", "single-line system prompt"]));
        assert_eq!(command.args.last(), Some(&"smoke prompt".to_owned()));
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
        let result =
            run_read_only_smoke_test(&config, &store, "robinhood-trading", &executor).unwrap();
        assert_eq!(result.event_count, 4);
        assert_eq!(result.tool_event_count, 1);
        assert_eq!(result.robinhood_read_count, 1);
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
}
