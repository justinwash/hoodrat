use crate::config::AgentConfig;
use crate::ingestion::{parse_json_text, BrokerDataSink, ExecutionRecord, PortfolioSnapshot};
use crate::store::{AgentEventRecord, AgentToolEventRecord, Store};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
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
    pub fn from_config(config: &AgentConfig, prompt: String) -> Self {
        Self {
            executable: config.executable.clone(),
            args: vec![
                "--json".to_owned(),
                "--model".to_owned(),
                config.model.clone(),
                "--provider".to_owned(),
                config.provider.clone(),
                "--timeout".to_owned(),
                config.timeout_secs.to_string(),
                "--auto-approve".to_owned(),
                config.auto_approve.to_string(),
                "--data-dir".to_owned(),
                config.data_dir.display().to_string(),
                prompt,
            ],
            working_directory: config.working_directory.clone(),
        }
    }

    fn spawn(&self) -> Result<Output> {
        let mut command = Command::new(&self.executable);
        command.args(&self.args);
        if let Some(directory) = &self.working_directory {
            command.current_dir(directory);
        }
        command
            .output()
            .with_context(|| format!("failed to launch Cline executable '{}'", self.executable))
    }
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
    let run_id = format!(
        "{}-{}",
        lane.as_str(),
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
    );
    store.begin_run(&run_id, lane.as_str(), &prompt)?;

    let command = AgentCommand::from_config(config, prompt);
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

        if let Some(tool) = tool_event(&value) {
            let input = tool.input.as_ref().map(serde_json::to_string).transpose()?;
            let output_value = tool.output.as_ref().map(parse_json_text);
            let output_json = output_value
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            store.record_tool_event(&AgentToolEventRecord {
                run_id: run_id.clone(),
                sequence_number,
                tool_name: tool.name.clone(),
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
                    "is_error": tool.is_error,
                    "sequence_number": sequence_number,
                }),
            )?;
            tool_event_count += 1;
            ingest_tool_output(store, &run_id, &tool.name, output_value.as_ref())?;
        }
    }

    let mut summary = summary_parts.join("\n");
    if !stderr.trim().is_empty() {
        summary.push_str("\nCline stderr: ");
        summary.push_str(stderr.trim());
    }
    if summary.is_empty() {
        summary = "Cline produced no structured output.".to_owned();
    }
    let status = if output.status.success() {
        "completed"
    } else {
        "failed"
    };
    store.finish_run(&run_id, status, &raw_output, &summary)?;
    store.record_audit(
        Some(&run_id),
        "agent",
        "task_finished",
        &serde_json::json!({
            "lane": lane.as_str(),
            "exit_code": output.status.code(),
            "event_count": event_count,
            "tool_event_count": tool_event_count,
            "elapsed_ms": started.elapsed().as_millis(),
        }),
    )?;

    Ok(AgentRunResult {
        run_id,
        exit_code: output.status.code(),
        event_count,
        tool_event_count,
    })
}

#[derive(Debug)]
struct ToolEvent {
    name: String,
    input: Option<Value>,
    output: Option<Value>,
    is_error: bool,
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
    Some(ToolEvent {
        name: name.to_owned(),
        input: first_value(object, &["input", "arguments", "tool_input", "toolInput"]),
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
    })
}

fn first_value(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<Value> {
    keys.iter().find_map(|key| object.get(*key).cloned())
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
    ["text", "say", "ask", "reasoning"].iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
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
            .any(|pair| pair == ["--provider", "openai-compatible"]));
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
            .any(|pair| pair == ["--data-dir", "data/cline"]));
        assert_eq!(command.args.last(), Some(&"prompt".to_owned()));
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
            "{\"type\":\"mcp_tool_result\",\"toolName\":\"get_portfolio\",\"result\":{\"portfolio_value\":1200,\"buying_power\":400}}\n",
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
        assert_eq!(result.event_count, 3);
        assert_eq!(result.tool_event_count, 1);
        assert_eq!(store.tool_event_count(&result.run_id).unwrap(), 1);
        assert_eq!(store.portfolio_snapshot_count().unwrap(), 1);
        assert!(store
            .recent_events(10)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "unstructured"));
    }
}
