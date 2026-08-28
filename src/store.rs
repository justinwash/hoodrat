use crate::config::StrategyContract;
use crate::ingestion::{
    field_coverage, parse_broker_payload, AccountRecord, BalanceRecord, BrokerDataSink,
    BrokerPayload, ExecutionRecord, PnlSnapshot, PnlTradeRecord, PortfolioSnapshot, PositionRecord,
};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
const TOOL_EVENTS_MIGRATION: &str = include_str!("../migrations/002_tool_events.sql");
const SCHEMA_METADATA_MIGRATION: &str = include_str!("../migrations/003_schema_metadata.sql");
const TYPED_BROKER_MIGRATION: &str = include_str!("../migrations/004_typed_broker_ingestion.sql");
const STRATEGY_BASELINE_MIGRATION: &str =
    include_str!("../migrations/005_strategy_and_baseline_acceptance.sql");

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
    pub strategy_contract_version: Option<String>,
    pub strategy_contract_fingerprint: Option<String>,
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
    pub reconciliation_status: String,
    pub reconciliation_details: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReconciliationReport {
    pub captured_at: String,
    pub status: String,
    pub account_count: u32,
    pub balance_count: u32,
    pub position_count: u32,
    pub pnl_trade_count: u32,
    pub drift: Vec<String>,
    pub coverage: BTreeMap<String, String>,
    pub order_history_status: String,
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
        connection.execute_batch(TYPED_BROKER_MIGRATION)?;
        connection.execute_batch(STRATEGY_BASELINE_MIGRATION)?;
        ensure_column(
            &connection,
            "agent_runs",
            "strategy_contract_version",
            "TEXT",
        )?;
        ensure_column(
            &connection,
            "agent_runs",
            "strategy_contract_fingerprint",
            "TEXT",
        )?;
        if !has_column(&connection, "reconciliation_runs", "coverage_json")?
            || !has_column(&connection, "reconciliation_runs", "order_history_status")?
        {
            if !has_column(&connection, "reconciliation_runs", "coverage_json")? {
                ensure_column(
                    &connection,
                    "reconciliation_runs",
                    "coverage_json",
                    "TEXT NOT NULL DEFAULT '{}'",
                )?;
            }
            if !has_column(&connection, "reconciliation_runs", "order_history_status")? {
                ensure_column(
                    &connection,
                    "reconciliation_runs",
                    "order_history_status",
                    "TEXT NOT NULL DEFAULT 'not_documented'",
                )?;
            }
        }
        ensure_column(
            &connection,
            "broker_pnl_snapshots",
            "total_returns_usd",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "broker_pnl_snapshots",
            "rate_of_realized_gain",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "broker_pnl_snapshots",
            "total_rate_of_return",
            "REAL",
        )?;
        ensure_column(
            &connection,
            "broker_pnl_snapshots",
            "number_of_trades",
            "INTEGER",
        )?;
        ensure_column(&connection, "broker_accounts", "rhc_account_number", "TEXT")?;
        ensure_column(
            &connection,
            "broker_balances",
            "unleveraged_buying_power_usd",
            "REAL",
        )?;
        Ok(Self { connection })
    }

    #[allow(dead_code)]
    pub fn begin_run(&self, id: &str, lane: &str, prompt: &str) -> Result<()> {
        self.begin_run_with_strategy(id, lane, prompt, None)
    }

    pub fn begin_run_with_strategy(
        &self,
        id: &str,
        lane: &str,
        prompt: &str,
        strategy: Option<&StrategyContract>,
    ) -> Result<()> {
        self.connection.execute(
            "INSERT INTO agent_runs (id, lane, started_at, status, prompt, strategy_contract_version, strategy_contract_fingerprint) VALUES (?1, ?2, ?3, 'running', ?4, ?5, ?6)",
            params![
                id,
                lane,
                now(),
                prompt,
                strategy.map(|value| value.contract_version.as_str()),
                strategy.map(StrategyContract::fingerprint),
            ],
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

    pub fn ingest_typed_broker_payload(
        &self,
        tool_name: &str,
        raw: &Value,
    ) -> Result<BrokerPayload> {
        let payload = parse_broker_payload(tool_name, raw)?;
        match &payload {
            BrokerPayload::Accounts(accounts) => {
                for account in accounts {
                    self.insert_account(account)?;
                }
            }
            BrokerPayload::Portfolio {
                snapshot,
                balances,
                positions,
            } => {
                self.insert_portfolio_snapshot(snapshot)?;
                for balance in balances {
                    self.insert_balance(balance)?;
                }
                for position in positions {
                    self.insert_position(position)?;
                }
            }
            BrokerPayload::Pnl(snapshot) => self.insert_pnl_snapshot(snapshot)?,
            BrokerPayload::PnlTradeHistory(trades) => {
                for trade in trades {
                    self.insert_pnl_trade(trade)?;
                }
            }
        }
        Ok(payload)
    }

    pub fn finalize_reconciliation(
        &self,
        raw_payloads: &BTreeMap<String, Value>,
    ) -> Result<ReconciliationReport> {
        let mut accounts = Vec::new();
        let mut balances = Vec::new();
        let mut positions = Vec::new();
        let mut pnl_snapshots = Vec::new();
        let mut pnl_trades = Vec::new();
        for (tool_name, raw) in raw_payloads {
            match parse_broker_payload(tool_name, raw)? {
                BrokerPayload::Accounts(values) => accounts.extend(values),
                BrokerPayload::Portfolio {
                    balances: values,
                    positions: position_values,
                    ..
                } => {
                    balances.extend(values);
                    positions.extend(position_values);
                }
                BrokerPayload::Pnl(value) => pnl_snapshots.push(value),
                BrokerPayload::PnlTradeHistory(values) => pnl_trades.extend(values),
            }
        }
        let coverage = reconciliation_coverage(raw_payloads)?;
        let fingerprint = fingerprint_json(
            &accounts,
            &balances,
            &positions,
            &pnl_snapshots,
            &pnl_trades,
            &coverage,
        );
        let previous = self
            .connection
            .query_row(
                "SELECT accepted_fingerprint_json FROM baseline_acceptances ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let previous = match previous {
            Some(value) => Some(value),
            None => self
                .connection
                .query_row(
                    "SELECT fingerprint_json FROM reconciliation_runs WHERE status IN ('baseline', 'reconciled') ORDER BY id DESC LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()?,
        };
        let drift = previous
            .as_deref()
            .map(|old| {
                let old_value = serde_json::from_str(old).unwrap_or(Value::Null);
                let new_value = serde_json::from_str(&fingerprint).unwrap_or(Value::Null);
                drift_categories(&old_value, &new_value)
            })
            .unwrap_or_default();
        let status = if coverage_has_missing_required_data(&coverage) {
            "coverage_incomplete"
        } else if previous.is_none() {
            "baseline"
        } else if drift.is_empty() {
            "reconciled"
        } else {
            "drift_detected"
        };
        let report = ReconciliationReport {
            captured_at: now(),
            status: status.to_owned(),
            account_count: accounts.len() as u32,
            balance_count: balances.len() as u32,
            position_count: positions.len() as u32,
            pnl_trade_count: pnl_trades.len() as u32,
            drift,
            coverage,
            order_history_status: "not_documented".to_owned(),
        };
        self.connection.execute(
            "INSERT INTO reconciliation_runs (captured_at, status, account_count, balance_count, position_count, pnl_trade_count, drift_json, raw_json, fingerprint_json, coverage_json, order_history_status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                report.captured_at,
                report.status,
                report.account_count,
                report.balance_count,
                report.position_count,
                report.pnl_trade_count,
                serde_json::to_string(&report.drift)?,
                serde_json::to_string(raw_payloads)?,
                fingerprint,
                serde_json::to_string(&report.coverage)?,
                report.order_history_status,
            ],
        )?;
        Ok(report)
    }

    #[allow(dead_code)]
    pub fn latest_reconciliation(&self) -> Result<Option<ReconciliationReport>> {
        self.connection
            .query_row(
                "SELECT captured_at, status, account_count, balance_count, position_count, pnl_trade_count, drift_json, coverage_json, order_history_status FROM reconciliation_runs ORDER BY id DESC LIMIT 1",
                [],
                |row| {
                    let drift_json: String = row.get(6)?;
                    let coverage_json: String = row.get(7)?;
                    Ok(ReconciliationReport {
                        captured_at: row.get(0)?,
                        status: row.get(1)?,
                        account_count: row.get(2)?,
                        balance_count: row.get(3)?,
                        position_count: row.get(4)?,
                        pnl_trade_count: row.get(5)?,
                        drift: serde_json::from_str(&drift_json).unwrap_or_default(),
                        coverage: serde_json::from_str(&coverage_json).unwrap_or_default(),
                        order_history_status: row.get(8)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
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

    fn insert_account(&self, account: &AccountRecord) -> Result<()> {
        self.connection.execute(
            "INSERT OR IGNORE INTO broker_accounts (captured_at, account_number, rhs_account_number, rhc_account_number, account_type, brokerage_account_type, nickname, is_default, agentic_allowed, option_level, management_type, affiliate, state, deactivated, permanently_deactivated, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                account.captured_at,
                account.account_number,
                account.rhs_account_number,
                account.rhc_account_number,
                account.account_type,
                account.brokerage_account_type,
                account.nickname,
                account.is_default,
                account.agentic_allowed,
                account.option_level,
                account.management_type,
                account.affiliate,
                account.state,
                account.deactivated,
                account.permanently_deactivated,
                serde_json::to_string(&account.raw)?,
            ],
        )?;
        Ok(())
    }

    fn insert_balance(&self, balance: &BalanceRecord) -> Result<()> {
        self.connection.execute(
            "INSERT INTO broker_balances (captured_at, account_number, cash_usd, buying_power_usd, unleveraged_buying_power_usd, equity_usd, margin_used_usd, unsettled_funds_usd, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                balance.captured_at,
                balance.account_number,
                balance.cash_usd,
                balance.buying_power_usd,
                balance.unleveraged_buying_power_usd,
                balance.equity_usd,
                balance.margin_used_usd,
                balance.unsettled_funds_usd,
                serde_json::to_string(&balance.raw)?,
            ],
        )?;
        Ok(())
    }

    fn insert_position(&self, position: &PositionRecord) -> Result<()> {
        self.connection.execute(
            "INSERT INTO broker_positions (captured_at, account_number, symbol, instrument_id, asset_class, quantity, average_cost_usd, market_value_usd, current_price_usd, unrealized_pnl_usd, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                position.captured_at,
                position.account_number,
                position.symbol,
                position.instrument_id,
                position.asset_class,
                position.quantity,
                position.average_cost_usd,
                position.market_value_usd,
                position.current_price_usd,
                position.unrealized_pnl_usd,
                serde_json::to_string(&position.raw)?,
            ],
        )?;
        Ok(())
    }

    fn insert_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()> {
        self.connection.execute(
            "INSERT INTO broker_pnl_snapshots (captured_at, account_number, span, start_date, end_date, realized_pnl_usd, total_returns_usd, rate_of_realized_gain, total_rate_of_return, number_of_trades, by_asset_class_json, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                snapshot.captured_at,
                snapshot.account_number,
                snapshot.span,
                snapshot.start_date,
                snapshot.end_date,
                snapshot.realized_pnl_usd,
                snapshot.total_returns_usd,
                snapshot.rate_of_realized_gain,
                snapshot.total_rate_of_return,
                snapshot.number_of_trades,
                snapshot
                    .by_asset_class
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                serde_json::to_string(&snapshot.raw)?,
            ],
        )?;
        Ok(())
    }

    fn insert_pnl_trade(&self, trade: &PnlTradeRecord) -> Result<()> {
        self.connection.execute(
            "INSERT INTO broker_pnl_trades (captured_at, account_number, external_id, symbol, asset_class, side, quantity, realized_pnl_usd, opened_at, closed_at, raw_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                trade.captured_at,
                trade.account_number,
                trade.external_id,
                trade.symbol,
                trade.asset_class,
                trade.side,
                trade.quantity,
                trade.realized_pnl_usd,
                trade.opened_at,
                trade.closed_at,
                serde_json::to_string(&trade.raw)?,
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
                "SELECT id, lane, started_at, finished_at, status, prompt, raw_output, summary, strategy_contract_version, strategy_contract_fingerprint FROM agent_runs ORDER BY started_at DESC LIMIT 1",
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
                        strategy_contract_version: row.get(8)?,
                        strategy_contract_fingerprint: row.get(9)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn accept_latest_drift(&self, operator: &str, reason: &str, confirmed: bool) -> Result<()> {
        if !confirmed {
            anyhow::bail!("baseline acceptance requires --confirm");
        }
        if operator.trim().is_empty() {
            anyhow::bail!("baseline acceptance requires a non-empty operator");
        }
        if reason.trim().is_empty() {
            anyhow::bail!("baseline acceptance requires a non-empty reason");
        }
        let latest = self
            .connection
            .query_row(
                "SELECT id, status, fingerprint_json FROM reconciliation_runs ORDER BY id DESC LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .context("no reconciliation run is available for baseline acceptance")?;
        if latest.1 != "drift_detected" {
            anyhow::bail!(
                "baseline acceptance requires the latest reconciliation to be drift_detected"
            );
        }
        let already_accepted = self
            .connection
            .query_row(
                "SELECT 1 FROM baseline_acceptances WHERE reconciliation_run_id = ?1 LIMIT 1",
                params![latest.0],
                |_row| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if already_accepted {
            anyhow::bail!("the latest reconciliation drift has already been accepted");
        }
        let prior = self
            .connection
            .query_row(
                "SELECT accepted_fingerprint_json FROM baseline_acceptances ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let prior = match prior {
            Some(value) => value,
            None => self
                .connection
                .query_row(
                    "SELECT fingerprint_json FROM reconciliation_runs WHERE id < ?1 AND status IN ('baseline', 'reconciled') ORDER BY id DESC LIMIT 1",
                    params![latest.0],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .context("cannot determine the prior reconciliation fingerprint")?,
        };
        self.connection.execute(
            "INSERT INTO baseline_acceptances (accepted_at, operator, reason, reconciliation_run_id, prior_fingerprint_json, accepted_fingerprint_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![now(), operator.trim(), reason.trim(), latest.0, prior, latest.2],
        )?;
        self.record_audit(
            None,
            "reconciliation",
            "baseline_accepted",
            &serde_json::json!({
                "operator": operator.trim(),
                "reason": reason.trim(),
                "reconciliation_run_id": latest.0,
                "prior_fingerprint": prior,
                "accepted_fingerprint": latest.2,
            }),
        )?;
        Ok(())
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
        let reconciliation = self.latest_reconciliation()?;
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
            reconciliation_status: reconciliation
                .as_ref()
                .map(|report| report.status.clone())
                .unwrap_or_else(|| "not_run".to_owned()),
            reconciliation_details: reconciliation
                .as_ref()
                .map(reconciliation_details)
                .unwrap_or_else(|| "No reconciliation recorded.".to_owned()),
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

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    column_type: &str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let present = columns
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == column);
    if !present {
        connection.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {column_type}"),
            [],
        )?;
    }
    Ok(())
}

fn has_column(connection: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    Ok(columns
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == column))
}

fn reconciliation_coverage(
    raw_payloads: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, String>> {
    let mut coverage = BTreeMap::new();
    coverage.insert(
        "accounts".to_owned(),
        coverage_for(raw_payloads, "get_accounts", &["accounts"])?,
    );
    coverage.insert(
        "portfolio_total_value".to_owned(),
        coverage_for(
            raw_payloads,
            "get_portfolio",
            &["total_value", "total_value_usd", "portfolio_value"],
        )?,
    );
    coverage.insert(
        "portfolio_buying_power".to_owned(),
        coverage_for(
            raw_payloads,
            "get_portfolio",
            &["buying_power", "buying_power_usd"],
        )?,
    );
    coverage.insert(
        "portfolio_positions".to_owned(),
        coverage_for(raw_payloads, "get_portfolio", &["positions", "holdings"])?,
    );
    coverage.insert(
        "realized_pnl".to_owned(),
        coverage_for(
            raw_payloads,
            "get_realized_pnl",
            &["realized_gain", "realized_pnl", "total_returns"],
        )?,
    );
    coverage.insert(
        "pnl_trade_history".to_owned(),
        coverage_for(raw_payloads, "get_pnl_trade_history", &["trades"])?,
    );
    coverage.insert("order_history".to_owned(), "not_documented".to_owned());
    Ok(coverage)
}

fn coverage_has_missing_required_data(coverage: &BTreeMap<String, String>) -> bool {
    [
        "accounts",
        "portfolio_total_value",
        "portfolio_buying_power",
        "realized_pnl",
        "pnl_trade_history",
    ]
    .iter()
    .any(|key| coverage.get(*key).map(String::as_str) == Some("missing"))
}

fn coverage_for(
    raw_payloads: &BTreeMap<String, Value>,
    tool_name: &str,
    keys: &[&str],
) -> Result<String> {
    raw_payloads
        .get(tool_name)
        .map(|raw| field_coverage(raw, keys))
        .unwrap_or_else(|| Ok("missing".to_owned()))
}

fn drift_categories(previous: &Value, current: &Value) -> Vec<String> {
    let mut categories = [
        ("accounts", "accounts_changed"),
        ("balances", "balances_changed"),
        ("positions", "positions_changed"),
        ("pnl_trades", "trade_history_changed"),
    ]
    .into_iter()
    .filter_map(|(key, category)| {
        (previous.get(key) != current.get(key)).then_some(category.to_owned())
    })
    .collect::<Vec<_>>();
    if !pnl_snapshots_match(previous.get("pnl_snapshots"), current.get("pnl_snapshots")) {
        categories.push("pnl_changed".to_owned());
    }
    if previous.get("coverage").is_some() && previous.get("coverage") != current.get("coverage") {
        categories.push("coverage_changed".to_owned());
    }
    categories
}

fn pnl_snapshots_match(previous: Option<&Value>, current: Option<&Value>) -> bool {
    fn normalize(value: Option<&Value>) -> Value {
        serde_json::Value::Array(
            value
                .and_then(Value::as_array)
                .map(|snapshots| {
                    snapshots
                        .iter()
                        .map(|snapshot| {
                            serde_json::json!({
                                "account_number": snapshot.get("account_number"),
                                "span": snapshot.get("span"),
                                "realized_pnl_usd": snapshot.get("realized_pnl_usd"),
                                "total_returns_usd": snapshot.get("total_returns_usd"),
                                "rate_of_realized_gain": snapshot.get("rate_of_realized_gain"),
                                "total_rate_of_return": snapshot.get("total_rate_of_return"),
                                "number_of_trades": snapshot.get("number_of_trades"),
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        )
    }

    normalize(previous) == normalize(current)
}

fn reconciliation_details(report: &ReconciliationReport) -> String {
    let mut details = format!(
        "accounts={}, balances={}, positions={}, pnl_trades={}, order_history={}",
        report.account_count,
        report.balance_count,
        report.position_count,
        report.pnl_trade_count,
        report.order_history_status
    );
    if !report.drift.is_empty() {
        details.push_str("; drift=");
        details.push_str(&report.drift.join(","));
    }
    if !report.coverage.is_empty() {
        details.push_str("; coverage=");
        details.push_str(
            &report
                .coverage
                .iter()
                .map(|(name, state)| format!("{name}:{state}"))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    details
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn creates_schema_and_records_audit() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store
            .record_audit(None, "test", "created", &serde_json::json!({"ok": true}))
            .unwrap();
        assert!(store.latest_run().unwrap().is_none());
        assert_eq!(store.schema_version().unwrap(), 6);
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

    #[test]
    fn ingests_typed_payloads_and_establishes_then_checks_baseline() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let mut payloads = BTreeMap::new();
        for (tool, fixture) in [
            (
                "get_accounts",
                include_str!("../tests/fixtures/get_accounts.json"),
            ),
            (
                "get_portfolio",
                include_str!("../tests/fixtures/get_portfolio.json"),
            ),
            (
                "get_realized_pnl",
                include_str!("../tests/fixtures/get_realized_pnl.json"),
            ),
            (
                "get_pnl_trade_history",
                include_str!("../tests/fixtures/get_pnl_trade_history.json"),
            ),
        ] {
            let raw: Value = serde_json::from_str(fixture).unwrap();
            store.ingest_typed_broker_payload(tool, &raw).unwrap();
            payloads.insert(tool.to_owned(), raw);
        }
        let first = store.finalize_reconciliation(&payloads).unwrap();
        assert_eq!(first.status, "baseline");
        assert_eq!(first.account_count, 2);
        assert_eq!(first.position_count, 1);
        assert_eq!(first.pnl_trade_count, 1);
        assert_eq!(first.order_history_status, "not_documented");
        assert_eq!(first.coverage["pnl_trade_history"], "present");
        let second = store.finalize_reconciliation(&payloads).unwrap();
        assert_eq!(second.status, "reconciled");
        let latest = store.latest_reconciliation().unwrap().unwrap();
        assert_eq!(latest.status, "reconciled");
        assert_eq!(latest.coverage["order_history"], "not_documented");
        assert!(store
            .dashboard_snapshot(Path::new(":memory:"))
            .unwrap()
            .reconciliation_details
            .contains("order_history=not_documented"));

        let mut changed = payloads.clone();
        changed.insert(
            "get_portfolio".to_owned(),
            serde_json::json!({
                "structuredContent": {"data": {"total_value": "1300.50", "buying_power": "50"}}
            }),
        );
        let drift = store.finalize_reconciliation(&changed).unwrap();
        assert_eq!(drift.status, "drift_detected");
        assert!(drift.drift.contains(&"balances_changed".to_owned()));

        let mut missing = payloads.clone();
        missing.remove("get_realized_pnl");
        let incomplete = store.finalize_reconciliation(&missing).unwrap();
        assert_eq!(incomplete.status, "coverage_incomplete");
        assert_eq!(incomplete.coverage["realized_pnl"], "missing");
    }

    #[test]
    fn accepts_drift_once_and_uses_accepted_fingerprint_as_the_new_baseline() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let mut payloads = BTreeMap::new();
        for (tool, fixture) in [
            (
                "get_accounts",
                include_str!("../tests/fixtures/get_accounts.json"),
            ),
            (
                "get_portfolio",
                include_str!("../tests/fixtures/get_portfolio.json"),
            ),
            (
                "get_realized_pnl",
                include_str!("../tests/fixtures/get_realized_pnl.json"),
            ),
            (
                "get_pnl_trade_history",
                include_str!("../tests/fixtures/get_pnl_trade_history.json"),
            ),
        ] {
            let raw: Value = serde_json::from_str(fixture).unwrap();
            store.ingest_typed_broker_payload(tool, &raw).unwrap();
            payloads.insert(tool.to_owned(), raw);
        }
        store.finalize_reconciliation(&payloads).unwrap();
        let mut changed = payloads.clone();
        changed.insert(
            "get_portfolio".to_owned(),
            serde_json::json!({
                "structuredContent": {"data": {"total_value": "1300.50", "buying_power": "50"}}
            }),
        );
        let drift = store.finalize_reconciliation(&changed).unwrap();
        assert_eq!(drift.status, "drift_detected");

        assert!(store
            .accept_latest_drift("operator-1", "reviewed balance change", true)
            .is_ok());
        assert!(store
            .accept_latest_drift("operator-1", "duplicate acceptance", true)
            .is_err());
        let reconciled = store.finalize_reconciliation(&changed).unwrap();
        assert_eq!(reconciled.status, "reconciled");

        let acceptance_count: u32 = store
            .connection
            .query_row("SELECT COUNT(*) FROM baseline_acceptances", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(acceptance_count, 1);
        let audit_count: u32 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM audit_events WHERE action = 'baseline_accepted'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_count, 1);
    }

    #[test]
    fn upgrades_schema_five_database_to_schema_six() {
        let path = std::env::temp_dir().join(format!(
            "hoodrat-schema-upgrade-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let connection = Connection::open(&path).unwrap();
            connection.execute_batch(INITIAL_MIGRATION).unwrap();
            connection.execute_batch(TOOL_EVENTS_MIGRATION).unwrap();
            connection.execute_batch(SCHEMA_METADATA_MIGRATION).unwrap();
            connection.execute_batch(TYPED_BROKER_MIGRATION).unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT value FROM schema_metadata WHERE key = 'schema_version'",
                        [],
                        |row| row.get::<_, String>(0)
                    )
                    .unwrap(),
                "5"
            );
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.schema_version().unwrap(), 6);
            let baseline_table_exists: u32 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'baseline_acceptances'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(baseline_table_exists, 1);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ignores_volatile_pnl_window_fields_when_comparing_fingerprints() {
        let legacy = serde_json::json!({
            "pnl_snapshots": [{
                "account_number": "account-1",
                "span": "day",
                "start_date": "2026-08-27T04:00:00Z",
                "end_date": "2026-08-27T19:54:41Z",
                "realized_pnl_usd": 0.0,
                "total_returns_usd": 0.0,
                "rate_of_realized_gain": null,
                "total_rate_of_return": 0.0,
                "number_of_trades": 0,
                "by_asset_class": [{"realized_gain": null}]
            }]
        });
        let current = serde_json::json!({
            "pnl_snapshots": [{
                "account_number": "account-1",
                "span": "day",
                "realized_pnl_usd": 0.0,
                "total_returns_usd": 0.0,
                "rate_of_realized_gain": null,
                "total_rate_of_return": 0.0,
                "number_of_trades": 0
            }]
        });
        assert!(pnl_snapshots_match(
            legacy.get("pnl_snapshots"),
            current.get("pnl_snapshots")
        ));
        assert!(drift_categories(&legacy, &current).is_empty());
    }
}

fn fingerprint_json(
    accounts: &[AccountRecord],
    balances: &[BalanceRecord],
    positions: &[PositionRecord],
    pnl_snapshots: &[PnlSnapshot],
    pnl_trades: &[PnlTradeRecord],
    coverage: &BTreeMap<String, String>,
) -> String {
    let mut account_values = accounts
        .iter()
        .map(|account| {
            serde_json::json!({
                "account_number": account.account_number,
                "rhs_account_number": account.rhs_account_number,
                "rhc_account_number": account.rhc_account_number,
                "account_type": account.account_type,
                "brokerage_account_type": account.brokerage_account_type,
                "is_default": account.is_default,
                "agentic_allowed": account.agentic_allowed,
                "option_level": account.option_level,
                "management_type": account.management_type,
                "affiliate": account.affiliate,
                "state": account.state,
                "deactivated": account.deactivated,
                "permanently_deactivated": account.permanently_deactivated,
            })
        })
        .collect::<Vec<_>>();
    account_values.sort_by_key(|value| value.to_string());
    let mut balance_values = balances
        .iter()
        .map(|balance| {
            serde_json::json!({
                "account_number": balance.account_number,
                "cash_usd": balance.cash_usd,
                "buying_power_usd": balance.buying_power_usd,
                "unleveraged_buying_power_usd": balance.unleveraged_buying_power_usd,
                "equity_usd": balance.equity_usd,
                "margin_used_usd": balance.margin_used_usd,
                "unsettled_funds_usd": balance.unsettled_funds_usd,
            })
        })
        .collect::<Vec<_>>();
    balance_values.sort_by_key(|value| value.to_string());
    let mut position_values = positions
        .iter()
        .map(|position| {
            serde_json::json!({
                "account_number": position.account_number,
                "symbol": position.symbol,
                "instrument_id": position.instrument_id,
                "asset_class": position.asset_class,
                "quantity": position.quantity,
                "average_cost_usd": position.average_cost_usd,
                "market_value_usd": position.market_value_usd,
                "current_price_usd": position.current_price_usd,
                "unrealized_pnl_usd": position.unrealized_pnl_usd,
            })
        })
        .collect::<Vec<_>>();
    position_values.sort_by_key(|value| value.to_string());
    let mut pnl_snapshot_values = pnl_snapshots
        .iter()
        .map(|snapshot| {
            serde_json::json!({
                "account_number": snapshot.account_number,
                "span": snapshot.span,
                "realized_pnl_usd": snapshot.realized_pnl_usd,
                "total_returns_usd": snapshot.total_returns_usd,
                "rate_of_realized_gain": snapshot.rate_of_realized_gain,
                "total_rate_of_return": snapshot.total_rate_of_return,
                "number_of_trades": snapshot.number_of_trades,
            })
        })
        .collect::<Vec<_>>();
    pnl_snapshot_values.sort_by_key(|value| value.to_string());
    let mut pnl_values = pnl_trades
        .iter()
        .map(|trade| {
            serde_json::json!({
                "account_number": trade.account_number,
                "external_id": trade.external_id,
                "symbol": trade.symbol,
                "asset_class": trade.asset_class,
                "side": trade.side,
                "quantity": trade.quantity,
                "realized_pnl_usd": trade.realized_pnl_usd,
                "opened_at": trade.opened_at,
                "closed_at": trade.closed_at,
            })
        })
        .collect::<Vec<_>>();
    pnl_values.sort_by_key(|value| value.to_string());
    serde_json::json!({
        "accounts": account_values,
        "balances": balance_values,
        "positions": position_values,
        "pnl_snapshots": pnl_snapshot_values,
        "pnl_trades": pnl_values,
        "coverage": coverage,
    })
    .to_string()
}
