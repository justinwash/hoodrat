use crate::config::AgentConfig;
use crate::store::{AgentEventRecord, Store};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use std::process::Command;
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

#[derive(Debug)]
pub struct AgentRunResult {
    pub run_id: String,
    pub exit_code: Option<i32>,
    pub event_count: u32,
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
    let prompt = build_prompt(lane, context, config_summary);
    let run_id = format!(
        "{}-{}",
        lane.as_str(),
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
    );
    store.begin_run(&run_id, lane.as_str(), &prompt)?;

    let mut command = Command::new(&config.executable);
    command
        .arg("--json")
        .arg("--model")
        .arg(&config.model)
        .arg("--provider")
        .arg(&config.provider)
        .arg("--timeout")
        .arg(config.timeout_secs.to_string())
        .arg("--auto-approve")
        .arg(config.auto_approve.to_string())
        .arg("--data-dir")
        .arg(&config.data_dir)
        .arg(prompt.clone());

    if let Some(directory) = &config.working_directory {
        command.current_dir(directory);
    }

    let started = Instant::now();
    let output = command
        .output()
        .with_context(|| format!("failed to launch Cline executable '{}'", config.executable))?;
    let raw_output = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut summary_parts = Vec::new();
    let mut event_count = 0;

    for (index, line) in raw_output.lines().enumerate() {
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => {
                summary_parts.push(line.to_owned());
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
            sequence_number: index as u32,
            event_type,
            text,
            raw_json: line.to_owned(),
            recorded_at: Utc::now().to_rfc3339(),
        })?;
        event_count += 1;
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
            "elapsed_ms": started.elapsed().as_millis(),
        }),
    )?;

    Ok(AgentRunResult {
        run_id,
        exit_code: output.status.code(),
        event_count,
    })
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

    #[test]
    fn prompt_identifies_lane_and_safety_boundary() {
        let prompt = build_prompt(Lane::Crypto, "state", "policy");
        assert!(prompt.contains("crypto"));
        assert!(prompt.contains("not a pre-trade firewall"));
        assert!(prompt.contains("state"));
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
}
