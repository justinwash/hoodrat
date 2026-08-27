use crate::ingestion::{BrokerDataSink, ExecutionRecord, PortfolioSnapshot};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::path::Path;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const TOOL_EVENTS_MIGRATION: &str = include_str!("../migrations/002_tool_events.sql");
const SCHEMA_METADATA_MIGRATION: &str = include_str!("../migrations/003_schema_metadata.sql");

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
pub struct AgentToolEventRecord {
    pub run_id: String,
    pub sequence_number: u32,
    pub tool_name: String,
    pub input_json: Option<String>,
    pub output_json: Option<String>,
    pub is_error: bool,
    pub recorded_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardSnapshot {
    pub bot_status: String,
    pub last_run: String,
    pub last_run_status: String,
    pub recent_events: String,
    pub database_path: String,
    pub portfolio_value: Option<f64>,
    pub buying_power: Option<f64>,
    pub realized_pnl: Option<f64>,
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
        connection.execute_batch(INITIAL_MIGRATION)?;
        connection.execute_batch(TOOL_EVENTS_MIGRATION)?;
        connection.execute_batch(SCHEMA_METADATA_MIGRATION)?;
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
            "INSERT OR REPLACE INTO agent_events (run_id, sequence_number, event_type, text, raw_json, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
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

    pub fn record_tool_event(&self, event: &AgentToolEventRecord) -> Result<()> {
        self.connection.execute(
            "INSERT OR REPLACE INTO agent_tool_events (run_id, sequence_number, tool_name, input_json, output_json, is_error, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                event.run_id,
                event.sequence_number,
                event.tool_name,
                event.input_json,
                event.output_json,
                event.is_error,
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
        let snapshot = PortfolioSnapshot::from_value(raw)?;
        self.ingest_portfolio_snapshot(&snapshot)
    }

    fn insert_portfolio_snapshot(&self, snapshot: &PortfolioSnapshot) -> Result<()> {
        self.connection.execute(
            "INSERT INTO portfolio_snapshots (captured_at, total_value_usd, buying_power_usd, realized_pnl_usd, unrealized_pnl_usd, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                snapshot.captured_at,
                snapshot.total_value_usd,
                snapshot.buying_power_usd,
                snapshot.realized_pnl_usd,
                snapshot.unrealized_pnl_usd,
                serde_json::to_string(&snapshot.raw)?,
            ],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn record_execution(&self, raw: &Value, run_id: Option<&str>) -> Result<()> {
        let execution = ExecutionRecord::from_value(raw)?;
        self.ingest_execution(&execution, run_id)
    }

    fn insert_execution(&self, execution: &ExecutionRecord, run_id: Option<&str>) -> Result<()> {
        self.connection.execute(
            "INSERT OR REPLACE INTO executions (external_id, run_id, asset_class, symbol, side, quantity, notional_usd, status, submitted_at, filled_at, average_fill_price, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                execution.external_id,
                run_id,
                execution.asset_class,
                execution.symbol,
                execution.side,
                execution.quantity,
                execution.notional_usd,
                execution.status,
                execution.submitted_at,
                execution.filled_at,
                execution.average_fill_price,
                serde_json::to_string(&execution.raw)?,
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

    #[allow(dead_code)]
    pub fn tool_event_count(&self, run_id: &str) -> Result<u32> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM agent_tool_events WHERE run_id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn portfolio_snapshot_count(&self) -> Result<u32> {
        self.connection
            .query_row("SELECT COUNT(*) FROM portfolio_snapshots", [], |row| {
                row.get(0)
            })
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn execution_count(&self) -> Result<u32> {
        self.connection
            .query_row("SELECT COUNT(*) FROM executions", [], |row| row.get(0))
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn tool_event_error_count(&self, run_id: &str) -> Result<u32> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM agent_tool_events WHERE run_id = ?1 AND is_error = 1",
                params![run_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn schema_version(&self) -> Result<u32> {
        self.connection
            .query_row(
                "SELECT value FROM schema_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .context("schema metadata is missing")?
            .parse::<u32>()
            .context("schema version is not an unsigned integer")
    }

    pub fn dashboard_snapshot(&self, database_path: &Path) -> Result<DashboardSnapshot> {
        let latest = self.latest_run()?;
        let events = self.recent_events(12)?;
        let metrics = self.latest_portfolio_metrics()?;
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
            portfolio_value: metrics.0,
            buying_power: metrics.1,
            realized_pnl: metrics.2,
        })
    }

    fn latest_portfolio_metrics(&self) -> Result<(Option<f64>, Option<f64>, Option<f64>)> {
        self.connection
            .query_row(
                "SELECT total_value_usd, buying_power_usd, realized_pnl_usd FROM portfolio_snapshots ORDER BY id DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map(|value| value.unwrap_or((None, None, None)))
            .map_err(Into::into)
    }
}

impl BrokerDataSink for Store {
    fn ingest_portfolio_snapshot(&self, snapshot: &PortfolioSnapshot) -> Result<()> {
        self.insert_portfolio_snapshot(snapshot)
    }

    fn ingest_execution(&self, execution: &ExecutionRecord, run_id: Option<&str>) -> Result<()> {
        self.insert_execution(execution, run_id)
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
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
        assert_eq!(store.schema_version().unwrap(), 3);
    }

    #[test]
    fn records_and_reads_run_events_and_tool_events() {
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
        store
            .record_tool_event(&AgentToolEventRecord {
                run_id: "run-1".to_owned(),
                sequence_number: 2,
                tool_name: "get_portfolio".to_owned(),
                input_json: Some("{}".to_owned()),
                output_json: None,
                is_error: false,
                recorded_at: now(),
            })
            .unwrap();
        assert_eq!(store.recent_events(5).unwrap().len(), 1);
        assert_eq!(store.tool_event_count("run-1").unwrap(), 1);
        assert_eq!(store.tool_event_error_count("run-1").unwrap(), 0);
        assert_eq!(store.latest_run().unwrap().unwrap().lane, "crypto");
    }

    #[test]
    fn ingests_normalized_broker_records() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store
            .begin_run("run-1", "equity_options", "prompt")
            .unwrap();
        store
            .record_portfolio_snapshot(&serde_json::json!({
                "portfolio_value": "1200.50",
                "buying_power": 400
            }))
            .unwrap();
        store
            .record_execution(
                &serde_json::json!({
                    "id": "order-1",
                    "symbol": "ABC",
                    "side": "buy",
                    "quantity": 2,
                    "status": "filled"
                }),
                Some("run-1"),
            )
            .unwrap();
        let snapshot = store
            .dashboard_snapshot(Path::new("data/hoodrat.db"))
            .unwrap();
        assert_eq!(snapshot.portfolio_value, Some(1200.50));
        assert_eq!(snapshot.buying_power, Some(400.0));
        assert_eq!(store.portfolio_snapshot_count().unwrap(), 1);
        assert_eq!(store.execution_count().unwrap(), 1);
    }
}
