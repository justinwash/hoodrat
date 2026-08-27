use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::path::Path;

const MIGRATION: &str = include_str!("../migrations/001_initial.sql");

#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub id: String,
    pub lane: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: String,
    pub prompt: String,
    pub raw_output: Option<String>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentEventRecord {
    pub run_id: String,
    pub sequence_number: u32,
    pub event_type: String,
    pub text: Option<String>,
    pub raw_json: String,
    pub recorded_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardSnapshot {
    pub bot_status: String,
    pub last_run: String,
    pub last_run_status: String,
    pub recent_events: String,
    pub database_path: String,
}

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let connection = Connection::open(path)
            .with_context(|| format!("failed to open database at {}", path.display()))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(MIGRATION)?;
        Ok(Self { connection })
    }

    pub fn begin_run(&self, id: &str, lane: &str, prompt: &str) -> Result<()> {
        self.connection.execute(
            "INSERT INTO agent_runs (id, lane, started_at, status, prompt) VALUES (?1, ?2, ?3, 'running', ?4)",
            params![id, lane, now(), prompt],
        )?;
        Ok(())
    }

    pub fn finish_run(
        &self,
        id: &str,
        status: &str,
        raw_output: &str,
        summary: &str,
    ) -> Result<()> {
        self.connection.execute(
            "UPDATE agent_runs SET finished_at = ?1, status = ?2, raw_output = ?3, summary = ?4 WHERE id = ?5",
            params![now(), status, raw_output, summary, id],
        )?;
        Ok(())
    }

    pub fn record_agent_event(&self, event: &AgentEventRecord) -> Result<()> {
        self.connection.execute(
            "INSERT INTO agent_events (run_id, sequence_number, event_type, text, raw_json, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event.run_id,
                event.sequence_number,
                event.event_type,
                event.text,
                event.raw_json,
                event.recorded_at,
            ],
        )?;
        Ok(())
    }

    pub fn record_audit(
        &self,
        run_id: Option<&str>,
        category: &str,
        action: &str,
        detail: &Value,
    ) -> Result<()> {
        self.connection.execute(
            "INSERT INTO audit_events (run_id, category, action, detail_json, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run_id, category, action, serde_json::to_string(detail)?, now()],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn record_portfolio_snapshot(&self, raw: &Value) -> Result<()> {
        let object = raw.as_object();
        self.connection.execute(
            "INSERT INTO portfolio_snapshots (captured_at, total_value_usd, buying_power_usd, realized_pnl_usd, unrealized_pnl_usd, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                now(),
                object.and_then(|o| number(o, "total_value_usd")),
                object.and_then(|o| number(o, "buying_power_usd")),
                object.and_then(|o| number(o, "realized_pnl_usd")),
                object.and_then(|o| number(o, "unrealized_pnl_usd")),
                serde_json::to_string(raw)?,
            ],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn record_execution(&self, raw: &Value, run_id: Option<&str>) -> Result<()> {
        let object = raw
            .as_object()
            .context("execution payload must be a JSON object")?;
        let external_id = object.get("external_id").and_then(Value::as_str);
        self.connection.execute(
            "INSERT OR REPLACE INTO executions (external_id, run_id, asset_class, symbol, side, quantity, notional_usd, status, submitted_at, filled_at, average_fill_price, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                external_id,
                run_id,
                object.get("asset_class").and_then(Value::as_str).unwrap_or("unknown"),
                object.get("symbol").and_then(Value::as_str).unwrap_or("unknown"),
                object.get("side").and_then(Value::as_str).unwrap_or("unknown"),
                object.get("quantity").and_then(Value::as_f64),
                object.get("notional_usd").and_then(Value::as_f64),
                object.get("status").and_then(Value::as_str).unwrap_or("unknown"),
                object.get("submitted_at").and_then(Value::as_str),
                object.get("filled_at").and_then(Value::as_str),
                object.get("average_fill_price").and_then(Value::as_f64),
                serde_json::to_string(raw)?,
            ],
        )?;
        Ok(())
    }

    pub fn latest_run(&self) -> Result<Option<RunRecord>> {
        self.connection
            .query_row(
                "SELECT id, lane, started_at, finished_at, status, prompt, raw_output, summary FROM agent_runs ORDER BY started_at DESC LIMIT 1",
                [],
                |row| {
                    Ok(RunRecord {
                        id: row.get(0)?,
                        lane: row.get(1)?,
                        started_at: row.get(2)?,
                        finished_at: row.get(3)?,
                        status: row.get(4)?,
                        prompt: row.get(5)?,
                        raw_output: row.get(6)?,
                        summary: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn recent_events(&self, limit: u32) -> Result<Vec<AgentEventRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT run_id, sequence_number, event_type, text, raw_json, recorded_at FROM agent_events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit], |row| {
            Ok(AgentEventRecord {
                run_id: row.get(0)?,
                sequence_number: row.get(1)?,
                event_type: row.get(2)?,
                text: row.get(3)?,
                raw_json: row.get(4)?,
                recorded_at: row.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn dashboard_snapshot(&self, database_path: &Path) -> Result<DashboardSnapshot> {
        let latest = self.latest_run()?;
        let events = self.recent_events(12)?;
        let recent_events = events
            .iter()
            .rev()
            .map(|event| {
                format!(
                    "[{}] {}: {}",
                    event.recorded_at,
                    event.event_type,
                    event.text.as_deref().unwrap_or("(no text)")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(DashboardSnapshot {
            bot_status: "READY FOR CONFIGURATION".to_owned(),
            last_run: latest
                .as_ref()
                .map(|run| run.started_at.clone())
                .unwrap_or_else(|| "No runs recorded".to_owned()),
            last_run_status: latest
                .as_ref()
                .map(|run| run.status.clone())
                .unwrap_or_else(|| "—".to_owned()),
            recent_events: if recent_events.is_empty() {
                "No agent events recorded.".to_owned()
            } else {
                recent_events
            },
            database_path: database_path.display().to_string(),
        })
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn number(object: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    object.get(key).and_then(Value::as_f64)
}

#[allow(dead_code)]
fn _parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_schema_and_records_audit() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store
            .record_audit(None, "test", "created", &serde_json::json!({"ok": true}))
            .unwrap();
        assert!(store.latest_run().unwrap().is_none());
    }

    #[test]
    fn records_and_reads_run_events() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store.begin_run("run-1", "crypto", "prompt").unwrap();
        store
            .record_agent_event(&AgentEventRecord {
                run_id: "run-1".to_owned(),
                sequence_number: 1,
                event_type: "say".to_owned(),
                text: Some("hello".to_owned()),
                raw_json: "{\"type\":\"say\"}".to_owned(),
                recorded_at: now(),
            })
            .unwrap();
        assert_eq!(store.recent_events(5).unwrap().len(), 1);
        assert_eq!(store.latest_run().unwrap().unwrap().lane, "crypto");
    }
}
