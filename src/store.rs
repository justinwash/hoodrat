use crate::config::StrategyContract;
use crate::ingestion::{
    field_coverage, parse_broker_payload, AccountRecord, BalanceRecord, BrokerDataSink,
    BrokerPayload, ExecutionRecord, PnlSnapshot, PnlTradeRecord, PortfolioSnapshot, PositionRecord,
};
use crate::simulator::PaperSimulation;
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
const PAPER_SIMULATIONS_MIGRATION: &str = include_str!("../migrations/006_paper_simulations.sql");
const ORDER_PROPOSALS_MIGRATION: &str = include_str!("../migrations/007_order_proposals.sql");

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
    // Status header
    pub bot_status: String,
    pub execution_mode: String,
    pub risk_status: String,
    pub database_path: String,
    pub last_run: String,
    pub last_run_status: String,
    // KPI numeric values (for the header cards / coloring)
    pub portfolio_value: Option<f64>,
    pub buying_power: Option<f64>,
    pub cash: Option<f64>,
    pub equity: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    // Reconciliation status card
    pub reconciliation_status: String,
    pub reconciliation_details: String,
    // Charts: SVG path-command strings computed by Rust
    pub equity_chart_path: String,
    pub equity_chart_labels: String,
    pub pnl_chart_path: String,
    // Tables (pre-formatted monospace text rendered in ScrollView/ListView)
    pub overview_stats: String,
    pub accounts_table: String,
    pub balances_table: String,
    pub positions_table: String,
    pub pnl_snapshots_table: String,
    pub pnl_trades_table: String,
    pub runs_table: String,
    pub tool_events_table: String,
    pub audit_table: String,
    pub reconciliations_table: String,
    pub baseline_acceptances_table: String,
    pub strategy_table: String,
    pub recent_events: String,
    pub simulation_table: String,
    pub proposals_table: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProposalRow {
    pub id: i64,
    pub proposed_at: String,
    pub symbol: String,
    pub side: String,
    pub notional_usd: f64,
    pub verdict: String,
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
        // Allow concurrent readers/writers (scheduler thread + dashboard in the
        // same process, or a separate dashboard process) to wait briefly for a
        // lock instead of failing immediately with "database is locked".
        connection.pragma_update(None, "busy_timeout", 5_000)?;
        connection.execute_batch(INITIAL_MIGRATION)?;
        connection.execute_batch(TOOL_EVENTS_MIGRATION)?;
        connection.execute_batch(SCHEMA_METADATA_MIGRATION)?;
        connection.execute_batch(TYPED_BROKER_MIGRATION)?;
        connection.execute_batch(STRATEGY_BASELINE_MIGRATION)?;
        connection.execute_batch(PAPER_SIMULATIONS_MIGRATION)?;
        connection.execute_batch(ORDER_PROPOSALS_MIGRATION)?;
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
            BrokerPayload::Positions(positions) => {
                for position in positions {
                    self.insert_position(position)?;
                }
            }
            BrokerPayload::Orders(orders) => {
                for order in orders {
                    self.insert_execution(order, None)?;
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
                BrokerPayload::Positions(values) => positions.extend(values),
                BrokerPayload::Orders(orders) => {
                    for order in orders {
                        self.insert_execution(&order, None)?;
                    }
                }
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

    /// Return the distinct MCP tool names invoked by a run (from the recorded
    /// agent_tool_events table). Used by the live-lane policy enforcement to
    /// detect any direct write/order tool calls that bypass the firewall.
    pub fn run_tool_names(&self, run_id: &str) -> Result<Vec<String>> {
        let mut stmt = self
            .connection
            .prepare("SELECT DISTINCT tool_name FROM agent_tool_events WHERE run_id = ?1")?;
        let rows = stmt.query_map(params![run_id], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
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

    pub fn record_paper_simulation(&self, simulation: &PaperSimulation) -> Result<()> {
        self.connection.execute(
            "INSERT INTO paper_simulations (id, started_at, finished_at, profile, status, starting_cash_usd, final_cash_usd, final_equity_usd, realized_pnl_usd, unrealized_pnl_usd, market_snapshot_json, result_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                simulation.id,
                simulation.started_at,
                simulation.finished_at,
                simulation.profile,
                simulation.status,
                simulation.starting_cash_usd,
                simulation.final_cash_usd,
                simulation.final_equity_usd,
                simulation.realized_pnl_usd,
                simulation.unrealized_pnl_usd,
                serde_json::to_string(&simulation.market_plan)?,
                serde_json::to_string(simulation)?,
            ],
        )?;
        for event in &simulation.events {
            self.connection.execute(
                "INSERT INTO paper_simulation_events (simulation_id, event_at, event_type, symbol, asset_class, side, quantity, price, notional_usd, fee_usd, realized_pnl_usd, details_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    simulation.id,
                    event.event_at,
                    event.event_type,
                    event.symbol,
                    event.asset_class,
                    event.side,
                    event.quantity,
                    event.price,
                    event.notional_usd,
                    event.fee_usd,
                    event.realized_pnl_usd,
                    serde_json::to_string(&serde_json::json!({"details": event.details}))?,
                ],
            )?;
        }
        for position in &simulation.positions {
            self.connection.execute(
                "INSERT INTO paper_simulation_positions (simulation_id, position_key, symbol, asset_class, quantity, average_entry_price, mark_price, market_value_usd, unrealized_pnl_usd, opened_at, underlying, option_type, strike, expiration, multiplier) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    simulation.id,
                    position.position_key,
                    position.symbol,
                    position.asset_class,
                    position.quantity,
                    position.average_entry_price,
                    position.mark_price,
                    position.market_value_usd,
                    position.unrealized_pnl_usd,
                    position.opened_at,
                    position.underlying,
                    position.option_type.as_ref().map(|value| match value {
                        crate::simulator::OptionType::Call => "call",
                        crate::simulator::OptionType::Put => "put",
                    }),
                    position.strike,
                    position.expiration,
                    position.multiplier,
                ],
            )?;
        }
        self.record_audit(
            None,
            "paper_simulation",
            "completed",
            &serde_json::json!({
                "simulation_id": simulation.id,
                "profile": simulation.profile,
                "status": simulation.status,
                "event_count": simulation.events.len(),
                "position_count": simulation.positions.len(),
                "final_equity_usd": simulation.final_equity_usd,
            }),
        )?;
        Ok(())
    }

    pub fn dashboard_snapshot(&self, database_path: &Path) -> Result<DashboardSnapshot> {
        let latest = self.latest_run()?;
        let events = self.recent_events(50)?;
        let metrics = self.latest_portfolio_metrics()?;
        let reconciliation = self.latest_reconciliation()?;

        let recent_events = events
            .iter()
            .rev()
            .take(20)
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

        let (equity_chart_path, equity_chart_labels) = self.equity_chart()?;
        let pnl_chart_path = self.pnl_chart()?;
        let balances = self.balance_history(30)?;
        let latest_balance = balances.first();
        let cash = latest_balance.and_then(|b| b.cash_usd);
        let equity = latest_balance.and_then(|b| b.equity_usd);
        let buying_power = metrics
            .1
            .or_else(|| latest_balance.and_then(|b| b.buying_power_usd))
            .or_else(|| latest_balance.and_then(|b| b.unleveraged_buying_power_usd));
        let effective_metrics = (metrics.0, buying_power, metrics.2);

        Ok(DashboardSnapshot {
            bot_status: "—".to_owned(),
            execution_mode: "—".to_owned(),
            risk_status: "—".to_owned(),
            database_path: database_path.display().to_string(),
            last_run: latest
                .as_ref()
                .map(|run| run.started_at.clone())
                .unwrap_or_else(|| "No runs recorded".to_owned()),
            last_run_status: latest
                .as_ref()
                .map(|run| run.status.clone())
                .unwrap_or_else(|| "—".to_owned()),
            portfolio_value: metrics.0,
            buying_power,
            cash,
            equity,
            realized_pnl: metrics.2,
            unrealized_pnl: self.latest_unrealized_pnl()?,
            reconciliation_status: reconciliation
                .as_ref()
                .map(|report| report.status.clone())
                .unwrap_or_else(|| "not_run".to_owned()),
            reconciliation_details: reconciliation
                .as_ref()
                .map(reconciliation_details)
                .unwrap_or_else(|| "No reconciliation recorded.".to_owned()),
            equity_chart_path,
            equity_chart_labels,
            pnl_chart_path,
            overview_stats: self.overview_stats(effective_metrics, cash, equity)?,
            accounts_table: self.accounts_table()?,
            balances_table: self.balances_table()?,
            positions_table: self.positions_table()?,
            pnl_snapshots_table: self.pnl_snapshots_table()?,
            pnl_trades_table: self.pnl_trades_table()?,
            runs_table: self.runs_table()?,
            tool_events_table: self.tool_events_table()?,
            audit_table: self.audit_table()?,
            reconciliations_table: self.reconciliations_table()?,
            baseline_acceptances_table: self.baseline_acceptances_table()?,
            strategy_table: String::new(),
            recent_events: if recent_events.is_empty() {
                "No agent events recorded.".to_owned()
            } else {
                recent_events
            },
            simulation_table: self.simulation_table()?,
            proposals_table: self.proposals_table()?,
        })
    }

    fn latest_unrealized_pnl(&self) -> Result<Option<f64>> {
        self.connection
            .query_row(
                "SELECT unrealized_pnl_usd FROM portfolio_snapshots ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, Option<f64>>(0),
            )
            .optional()
            .map(|v| v.flatten())
            .map_err(Into::into)
    }

    /// Recent account balance history (cash/equity), most-recent first.
    fn balance_history(&self, limit: u32) -> Result<Vec<BalanceRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT captured_at, account_number, cash_usd, buying_power_usd, unleveraged_buying_power_usd, equity_usd, margin_used_usd, unsettled_funds_usd, raw_json FROM broker_balances ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit], |row| {
            Ok(BalanceRecord {
                captured_at: row.get(0)?,
                account_number: row.get(1)?,
                cash_usd: row.get(2)?,
                buying_power_usd: row.get(3)?,
                unleveraged_buying_power_usd: row.get(4)?,
                equity_usd: row.get(5)?,
                margin_used_usd: row.get(6)?,
                unsettled_funds_usd: row.get(7)?,
                raw: parse_json(row.get::<_, String>(8)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Builds an SVG area-chart path for equity over time, oldest -> newest,
    /// normalized to a 0..100 box so Slint can scale it via viewbox. Returns
    /// the path commands and a short axis label string.
    fn equity_chart(&self) -> Result<(String, String)> {
        let balances = self.balance_history(120)?;
        let values: Vec<f64> = balances.iter().filter_map(|b| b.equity_usd).rev().collect();
        if values.len() < 2 {
            return Ok((String::new(), String::new()));
        }
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let span = (max - min).max(1.0);
        let n = values.len() as f64;
        let mut parts = Vec::with_capacity(values.len() * 2 + 3);
        for (i, v) in values.iter().enumerate() {
            let x = i as f64 / (n - 1.0);
            let y = 1.0 - ((v - min) / span);
            let cmd = if i == 0 { "M" } else { "L" };
            parts.push(format!("{cmd}{:.4} {:.4}", x * 100.0, y * 100.0));
        }
        // Close the line against the bottom edge so the UI can render a
        // readable filled-area chart without a second data series.
        let path = format!("{} L100 100 L0 100 Z", parts.join(" "));
        let labels = format!(
            "low {:.2}  ·  high {:.2}  ·  points {}",
            min,
            max,
            values.len()
        );
        Ok((path, labels))
    }

    /// Grouped bar chart path showing total returns by span (per-last snapshot).
    fn pnl_chart(&self) -> Result<String> {
        let mut statement = self
            .connection
            .prepare("SELECT span, total_returns_usd FROM broker_pnl_snapshots")?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut seen = std::collections::HashSet::new();
        let buckets: Vec<(String, f64)> = rows
            .into_iter()
            .filter(|(s, _)| seen.insert(s.clone()))
            .map(|(s, v)| (s, v.unwrap_or(0.0)))
            .collect();
        if buckets.is_empty() {
            return Ok(String::new());
        }
        let max = buckets
            .iter()
            .map(|(_, v)| v.abs())
            .fold(0.0, f64::max)
            .max(1.0);
        let n = buckets.len() as f64;
        let bar_w = 12.0 / n.max(1.0);
        let mut body = String::new();
        for (i, (_, v)) in buckets.iter().enumerate() {
            let cx = (i as f64 + 0.5) * (100.0 / n);
            let h = (v.abs() / max) * 80.0;
            let y0 = 100.0 - if *v >= 0.0 { h } else { 0.0 };
            let y1 = if *v >= 0.0 { 100.0 } else { 100.0 + h };
            body.push_str(&format!(
                "M{:.2} {:.2} L{:.2} {:.2} L{:.2} {:.2} L{:.2} {:.2} ",
                cx - bar_w / 2.0,
                y0,
                cx - bar_w / 2.0,
                y1,
                cx + bar_w / 2.0,
                y1,
                cx + bar_w / 2.0,
                y0
            ));
        }
        Ok(body.trim().to_owned())
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

    fn overview_stats(
        &self,
        metrics: (Option<f64>, Option<f64>, Option<f64>),
        cash: Option<f64>,
        equity: Option<f64>,
    ) -> Result<String> {
        let summary = self.latest_summary()?;
        let mut s = String::from("── ACCOUNT OVERVIEW ───────────────────────\n");
        s.push_str(&format!(
            "Total equity     {:>12}\n",
            fmt_money(equity.or(metrics.0))
        ));
        s.push_str(&format!("Cash             {:>12}\n", fmt_money(cash)));
        s.push_str(&format!("Buying power     {:>12}\n", fmt_money(metrics.1)));
        s.push_str(&format!("Realized PnL     {:>12}\n", fmt_money(metrics.2)));
        s.push_str(&format!(
            "Unrealized PnL   {:>12}\n",
            fmt_money(self.latest_unrealized_pnl()?)
        ));
        s.push_str("\n── LAST RUN ────────────────────────────────\n");
        s.push_str(&summary);
        Ok(s)
    }

    fn latest_summary(&self) -> Result<String> {
        let run = self.latest_run()?;
        Ok(match run {
            Some(run) => format!(
                "lane: {}\nstatus: {}\nstarted: {}\n{}",
                run.lane,
                run.status,
                run.started_at,
                run.summary.as_deref().unwrap_or("(no summary)")
            ),
            None => "No runs recorded.".to_owned(),
        })
    }

    fn accounts_table(&self) -> Result<String> {
        let mut s = String::from("ACCOUNT          TYPE          AGENTIC  OPTLVL   STATE\n");
        let mut stmt = self.connection.prepare(
            "SELECT account_number, account_type, agentic_allowed, option_level, state, nickname FROM broker_accounts ORDER BY rowid DESC LIMIT 30",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<bool>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        for r in rows {
            let (acct, ty, agentic, opt, state, nick) = r?;
            s.push_str(&format!(
                "{:<15} {:<12} {:<8} {:<8} {:<10} {}\n",
                acct,
                ty.as_deref().unwrap_or("—"),
                agentic.map(|b| if b { "YES" } else { "no" }).unwrap_or("—"),
                opt.as_deref().unwrap_or("—"),
                state.as_deref().unwrap_or("—"),
                nick.as_deref().unwrap_or("")
            ));
        }
        Ok(s)
    }

    fn balances_table(&self) -> Result<String> {
        let mut s = String::from("TIME                    CASH      EQUITY\n");
        for b in self.balance_history(40)? {
            s.push_str(&format!(
                "{:<24} {:<10} {:<10}\n",
                short_ts(&b.captured_at),
                fmt_money(b.cash_usd),
                fmt_money(b.equity_usd)
            ));
        }
        Ok(s)
    }

    fn positions_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT captured_at, symbol, asset_class, quantity, average_cost_usd, market_value_usd, current_price_usd, unrealized_pnl_usd FROM broker_positions ORDER BY rowid DESC LIMIT 100",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<f64>>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<f64>>(5)?,
                row.get::<_, Option<f64>>(6)?,
                row.get::<_, Option<f64>>(7)?,
            ))
        })?;
        let mut s = String::from("SYMBOL   CLASS   QTY      COST      MV        UAV      UNREAL\n");
        let mut count = 0;
        for r in rows {
            let (_, sym, class, qty, cost, mv, price, unreal) = r?;
            count += 1;
            s.push_str(&format!(
                "{:<8} {:<7} {:<8} {:<10} {:<10} {:<10} {:<10}\n",
                sym.as_deref().unwrap_or("—"),
                class.as_deref().unwrap_or("—"),
                fmt_opt(qty),
                fmt_money(cost),
                fmt_money(mv),
                fmt_money(price),
                fmt_money(unreal)
            ));
        }
        if count == 0 {
            s.push_str(
                "No line-item positions captured.\nThe reconciliation get_portfolio read returns only aggregate totals,\nnot per-symbol holdings.",
            );
        }
        Ok(s)
    }

    fn pnl_snapshots_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT span, start_date, end_date, realized_pnl_usd, total_returns_usd, number_of_trades FROM broker_pnl_snapshots ORDER BY rowid DESC LIMIT 40",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<f64>>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<f64>>(5)?,
            ))
        })?;
        let mut s =
            String::from("SPAN   FROM           TO             REALIZED  TOTAL     TRADES\n");
        for r in rows {
            let (span, from, to, rp, tr, n) = r?;
            s.push_str(&format!(
                "{:<6} {:<15} {:<15} {:<10} {:<10} {:<6}\n",
                span,
                from.as_deref().unwrap_or("—"),
                to.as_deref().unwrap_or("—"),
                fmt_money(rp),
                fmt_money(tr),
                n.map(|v: f64| format!("{v:.0}"))
                    .unwrap_or_else(|| "—".into())
            ));
        }
        Ok(s)
    }

    fn pnl_trades_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT captured_at, symbol, asset_class, side, quantity, realized_pnl_usd FROM broker_pnl_trades ORDER BY rowid DESC LIMIT 100",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<f64>>(5)?,
            ))
        })?;
        let mut s = String::from("TIME                SYMBOL  CLASS  SIDE   QTY    REALIZED\n");
        let mut n = 0;
        for r in rows {
            let (at, sym, class, side, qty, rp) = r?;
            n += 1;
            s.push_str(&format!(
                "{:<19} {:<7} {:<6} {:<6} {:<6} {:<10}\n",
                short_ts(&at),
                sym.as_deref().unwrap_or("—"),
                class.as_deref().unwrap_or("—"),
                side.as_deref().unwrap_or("—"),
                fmt_opt(qty),
                fmt_money(rp)
            ));
        }
        if n == 0 {
            s.push_str("No realized PnL trades recorded yet.\n");
        }
        Ok(s)
    }

    fn runs_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT id, lane, started_at, status FROM agent_runs ORDER BY rowid DESC LIMIT 60",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut s = String::from("RUN ID                          LANE             STATUS\n");
        for r in rows {
            let (id, lane, _at, status) = r?;
            s.push_str(&format!("{:<32} {:<16} {}\n", short_id(&id), lane, status));
        }
        Ok(s)
    }

    fn tool_events_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT run_id, tool_name, is_error FROM agent_tool_events ORDER BY rowid DESC LIMIT 60",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?;
        let mut s = String::from("RUN ID                          TOOL                     ERR\n");
        for r in rows {
            let (id, tool, err) = r?;
            s.push_str(&format!(
                "{:<32} {:<24} {}\n",
                short_id(&id),
                tool,
                if err { "✗" } else { "·" }
            ));
        }
        Ok(s)
    }

    fn audit_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT recorded_at, category, action, detail_json FROM audit_events ORDER BY rowid DESC LIMIT 100",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        let mut s = String::from("TIME                CATEGORY        ACTION\n");
        for r in rows {
            let (at, cat, action, detail) = r?;
            s.push_str(&format!("{:<19} {:<15} {}\n", short_ts(&at), cat, action));
            if let Some(d) = detail {
                let trimmed = d.trim();
                if !trimmed.is_empty() && trimmed != "null" {
                    s.push_str(&format!("    {}\n", compact_detail(trimmed)));
                }
            }
        }
        Ok(s)
    }

    fn reconciliations_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT captured_at, status, account_count, balance_count, position_count, order_history_status FROM reconciliation_runs ORDER BY rowid DESC LIMIT 40",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        let mut s = String::from("TIME                STATUS    ACCS  BAL  POS  ORDHIST\n");
        for r in rows {
            let (at, status, accs, bal, pos, ord) = r?;
            s.push_str(&format!(
                "{:<19} {:<9} {:<5} {:<4} {:<4} {}\n",
                short_ts(&at),
                status,
                accs,
                bal,
                pos,
                ord
            ));
        }
        Ok(s)
    }

    fn baseline_acceptances_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT accepted_at, operator, reason, reconciliation_run_id FROM baseline_acceptances ORDER BY rowid DESC LIMIT 30",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?;
        let mut s = String::from("ACCEPTED            OPERATOR  RUN  REASON\n");
        for r in rows {
            let (at, op, reason, run) = r?;
            s.push_str(&format!(
                "{:<19} {:<9} {:<4} {}\n",
                short_ts(&at),
                op,
                run.map(|v| format!("{v}")).unwrap_or_else(|| "—".into()),
                reason
            ));
        }
        Ok(s)
    }

    fn simulation_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT id, profile, status, starting_cash_usd, final_equity_usd, realized_pnl_usd FROM paper_simulations ORDER BY rowid DESC LIMIT 20",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<f64>>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<f64>>(5)?,
            ))
        })?;
        let mut s = String::from(
            "SIMULATION                       PROFILE                     STATUS    START      FINAL      REALIZED\n",
        );
        let mut n = 0;
        for r in rows {
            let (id, profile, status, start, final_, realized) = r?;
            n += 1;
            s.push_str(&format!(
                "{:<30} {:<28} {:<9} {:<10} {:<10} {:<10}\n",
                short_id(&id),
                profile,
                status,
                fmt_money(start),
                fmt_money(final_),
                fmt_money(realized)
            ));
        }
        if n == 0 {
            s.push_str("No paper simulations recorded yet.\n");
        }
        Ok(s)
    }
    pub fn record_order_proposal(
        &self,
        proposal: &crate::firewall::OrderProposal,
        run_id: Option<&str>,
        verdict: &crate::firewall::FirewallVerdict,
    ) -> Result<i64> {
        self.connection.execute(
            "INSERT INTO order_proposals (proposed_at, run_id, account_number, asset_class, symbol, side, order_type, quantity, notional_usd, limit_price, quote_age_secs, source, verdict, reasons_json, proposal_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                now(),
                run_id,
                proposal.account_number,
                proposal.asset_class,
                proposal.symbol,
                proposal.side,
                proposal.order_type,
                proposal.quantity,
                proposal.notional_usd,
                proposal.limit_price,
                Option::<i64>::None,
                proposal.source,
                if verdict.approved { "approved" } else { "blocked" },
                serde_json::to_string(&verdict.reasons)?,
                serde_json::to_string(proposal)?,
            ],
        )?;
        Ok(self.connection.last_insert_rowid())
    }

    pub fn pending_proposals(&self, limit: u32) -> Result<Vec<ProposalRow>> {
        let mut stmt = self.connection.prepare(
            "SELECT id, proposed_at, symbol, side, notional_usd, verdict FROM order_proposals ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(ProposalRow {
                id: row.get(0)?,
                proposed_at: row.get(1)?,
                symbol: row.get(2)?,
                side: row.get(3)?,
                notional_usd: row.get(4)?,
                verdict: row.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn has_recent_proposal(
        &self,
        symbol: &str,
        side: &str,
        cooldown_secs: u64,
    ) -> Result<bool> {
        let cutoff = chrono::Utc::now()
            .checked_sub_signed(chrono::Duration::seconds(cooldown_secs as i64))
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned());
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM order_proposals WHERE symbol = ?1 AND side = ?2 AND proposed_at >= ?3 AND verdict = 'approved'",
                params![symbol, side, cutoff],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(Into::into)
    }

    pub fn latest_realized_pnl_usd(&self) -> Result<Option<f64>> {
        self.connection
            .query_row(
                "SELECT realized_pnl_usd FROM broker_pnl_snapshots WHERE realized_pnl_usd IS NOT NULL ORDER BY rowid DESC LIMIT 1",
                [],
                |row| row.get::<_, Option<f64>>(0),
            )
            .optional()
            .map(|v| v.flatten())
            .map_err(Into::into)
    }

    pub fn latest_equity_usd(&self) -> Result<Option<f64>> {
        Ok(self.balance_history(1)?.first().and_then(|b| b.equity_usd))
    }

    pub fn latest_cash_usd(&self) -> Result<Option<f64>> {
        Ok(self.balance_history(1)?.first().and_then(|b| b.cash_usd))
    }

    pub fn latest_buying_power_usd(&self) -> Result<Option<f64>> {
        Ok(self
            .balance_history(1)?
            .first()
            .and_then(|b| b.buying_power_usd))
    }

    pub fn proposals_table(&self) -> Result<String> {
        let mut stmt = self.connection.prepare(
            "SELECT id, proposed_at, symbol, side, notional_usd, verdict, substr(reasons_json,1,80) FROM order_proposals ORDER BY id DESC LIMIT 40",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;
        let mut s =
            String::from("ID   TIME                SYMBOL  SIDE   NOTIONAL  VERDICT   DETAIL\n");
        for r in rows {
            let (id, at, symbol, side, notional, verdict, reasons) = r?;
            s.push_str(&format!(
                "{:<4} {:<19} {:<7} {:<6} {:<9} {:<9} {}\n",
                id,
                short_ts(&at),
                symbol,
                side,
                fmt_money(Some(notional)),
                verdict,
                if reasons.trim().is_empty() || reasons.trim() == "[]" {
                    "—"
                } else {
                    &reasons
                }
            ));
        }
        Ok(s)
    }

    pub fn ping_proposals(&self) -> rusqlite::Result<i64> {
        self.connection
            .query_row("SELECT COUNT(*) FROM order_proposals", [], |row| {
                row.get::<_, i64>(0)
            })
    }

    /// Record an explicit operator approval of an approved order proposal.
    pub fn record_order_approval(
        &self,
        proposal_id: i64,
        operator: &str,
        reason: &str,
    ) -> Result<i64> {
        self.connection.execute(
            "INSERT INTO order_approvals (approved_at, operator, reason, proposal_id) VALUES (?1, ?2, ?3, ?4)",
            params![
                now(),
                operator,
                reason,
                proposal_id,
            ],
        )?;
        Ok(self.connection.last_insert_rowid())
    }

    /// Whether an approved proposal already has an operator approval record.
    pub fn has_operator_approval(&self, proposal_id: i64) -> Result<bool> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM order_approvals WHERE proposal_id = ?1",
                params![proposal_id],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(Into::into)
    }

    /// Pending (approved-but-not-operator-approved) proposals.
    pub fn pending_approvals(&self, limit: u32) -> Result<Vec<ProposalRow>> {
        let mut stmt = self.connection.prepare(
            "SELECT id, proposed_at, symbol, side, notional_usd, verdict FROM order_proposals WHERE verdict = 'approved' AND id NOT IN (SELECT proposal_id FROM order_approvals) ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(ProposalRow {
                id: row.get(0)?,
                proposed_at: row.get(1)?,
                symbol: row.get(2)?,
                side: row.get(3)?,
                notional_usd: row.get(4)?,
                verdict: row.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
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

fn parse_json(text: String) -> Value {
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

/// Truncate an ISO timestamp to seconds-without-timezone for compact display.
pub(crate) fn short_ts(ts: &str) -> String {
    // e.g. 2026-08-28T13:31:48.634662100+00:00 -> 2026-08-28 13:31:48
    let body = ts.split('.').next().unwrap_or(ts);
    body.replace('T', " ")
}

/// Shorten long IDs (like reconciliation-<timestamp>) to keep tables readable.
fn short_id(id: &str) -> String {
    if id.len() <= 32 {
        id.to_owned()
    } else {
        format!("{}…{}", &id[..24], &id[id.len() - 6..])
    }
}

pub(crate) fn fmt_money(value: Option<f64>) -> String {
    value
        .map(|v| format!("${v:.2}"))
        .unwrap_or_else(|| "—".to_owned())
}

fn fmt_opt(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.4}"))
        .unwrap_or_else(|| "—".to_owned())
}

/// Collapse embedded JSON / control characters so detail lines stay on one row.
fn compact_detail(detail: &str) -> String {
    let collapsed = detail
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>();
    if collapsed.len() > 160 {
        format!("{}…", &collapsed[..160])
    } else {
        collapsed
    }
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
        assert_eq!(store.schema_version().unwrap(), 8);
    }

    #[test]
    fn records_and_reads_run_events_and_tool_events() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store
            .begin_run("run-1", "equity_options", "prompt")
            .unwrap();
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
        assert_eq!(store.latest_run().unwrap().unwrap().lane, "equity_options");
    }

    #[test]
    fn persists_paper_simulation_events_and_positions() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let config = crate::config::SimulationConfig {
            enabled: true,
            max_quote_age_secs: 120,
            ..crate::config::SimulationConfig::default()
        };
        let now = "2026-08-27T12:00:00Z".parse().unwrap();
        let simulation = crate::simulator::simulate(
            crate::simulator::MarketPlan {
                captured_at: Some("2026-08-27T11:59:30Z".to_owned()),
                quotes: vec![crate::simulator::MarketQuote {
                    symbol: "SPY".to_owned(),
                    asset_class: crate::simulator::AssetClass::Equity,
                    bid: Some(499.9),
                    ask: Some(500.1),
                    last: Some(500.0),
                    as_of: Some("2026-08-27T11:59:30Z".to_owned()),
                    underlying: None,
                    option_type: None,
                    strike: None,
                    expiration: None,
                    multiplier: None,
                }],
                proposals: vec![crate::simulator::TradeProposal {
                    action: "buy".to_owned(),
                    symbol: "SPY".to_owned(),
                    asset_class: crate::simulator::AssetClass::Equity,
                    quantity: 1.0,
                    limit_price: None,
                    underlying: None,
                    option_type: None,
                    strike: None,
                    expiration: None,
                    multiplier: None,
                    reason: Some("persistence test".to_owned()),
                }],
            },
            &config,
            now,
        )
        .unwrap();
        store.record_paper_simulation(&simulation).unwrap();
        let simulation_count: u32 = store
            .connection
            .query_row("SELECT COUNT(*) FROM paper_simulations", [], |row| {
                row.get(0)
            })
            .unwrap();
        let event_count: u32 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_simulation_events WHERE simulation_id = ?1",
                params![simulation.id],
                |row| row.get(0),
            )
            .unwrap();
        let position_count: u32 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_simulation_positions WHERE simulation_id = ?1",
                params![simulation.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(simulation_count, 1);
        assert_eq!(event_count, 1);
        assert_eq!(position_count, 1);
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
    fn upgrades_schema_five_database_to_schema_seven() {
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
            assert_eq!(store.schema_version().unwrap(), 8);
            let baseline_table_exists: u32 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'baseline_acceptances'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(baseline_table_exists, 1);
            let paper_table_exists: u32 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'paper_simulations'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(paper_table_exists, 1);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ignores_volatile_equity_marks_when_comparing_balance_fingerprints() {
        use crate::ingestion::BalanceRecord;
        use std::collections::BTreeMap;

        let record = |equity: Option<f64>| BalanceRecord {
            captured_at: "2026-08-28T12:00:00Z".to_owned(),
            account_number: Some("account-1".to_owned()),
            cash_usd: Some(35.15),
            buying_power_usd: Some(35.15),
            unleveraged_buying_power_usd: Some(35.15),
            equity_usd: equity,
            margin_used_usd: None,
            unsettled_funds_usd: Some(0.0),
            raw: serde_json::Value::Null,
        };
        let coverage = BTreeMap::new();
        let baseline =
            fingerprint_json(&[], &[record(Some(441.6712511))], &[], &[], &[], &coverage);
        let current = fingerprint_json(&[], &[record(Some(441.436735))], &[], &[], &[], &coverage);
        // Pure mark-to-market equity movement must not read as account drift.
        let baseline_value: Value = serde_json::from_str(&baseline).unwrap();
        let current_value: Value = serde_json::from_str(&current).unwrap();
        assert!(drift_categories(&baseline_value, &current_value).is_empty());

        // A real balance movement (cash settling) must still flag as drift.
        let cash_record = BalanceRecord {
            cash_usd: Some(35.0),
            ..record(Some(441.436735))
        };
        let settled = fingerprint_json(&[], &[cash_record], &[], &[], &[], &coverage);
        let settled_value: Value = serde_json::from_str(&settled).unwrap();
        assert!(drift_categories(&current_value, &settled_value)
            .contains(&"balances_changed".to_owned()));
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
            // Note: equity_usd is intentionally excluded from the fingerprint.
            // It is a pure mark-to-market figure (cash + live position value at
            // snapshot time) that ticks with every price move, so including it
            // makes intraday re-start reconciliations read as "balances_changed"
            // on a few-cent mark wobble. Real account movement (cash, buying
            // power, unsettled funds, margin used) is still fingerprinted. This
            // mirrors the existing volatile-field normalization applied to
            // pnl_snapshots (operator-approved for live standing-bot runs).
            serde_json::json!({
                "account_number": balance.account_number,
                "cash_usd": balance.cash_usd,
                "buying_power_usd": balance.buying_power_usd,
                "unleveraged_buying_power_usd": balance.unleveraged_buying_power_usd,
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
