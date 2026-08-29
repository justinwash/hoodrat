//! Hoodrat order firewall.
//!
//! This is the deterministic pre-trade gate that runs entirely in Rust between
//! a proposed order and any broker submission. The direct Cline-to-MCP design
//! has no guaranteed interception point, so the safest posture is: the agent
//! proposes, Hoodrat validates against real config + persisted state, and the
//! proposal is recorded with a verdict. Submission is only ever allowed when
//! `gateway.submit` is explicitly enabled AND the proposal passes every check.
//!
//! Defaults are fail-closed: no config == rejected.

use crate::config::{Config, ExecutionMode};
use crate::store::Store;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderProposal {
    pub account_number: Option<String>,
    pub asset_class: String,
    pub symbol: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub order_type: String,
    pub quantity: Option<f64>,
    pub notional_usd: f64,
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub quote_price: Option<f64>,
    #[serde(default)]
    pub quote_captured_at: Option<String>,
    #[serde(default)]
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirewallVerdict {
    pub approved: bool,
    pub submitted: bool,
    pub reasons: Vec<String>,
}

impl FirewallVerdict {
    fn reject(mut reasons: Vec<String>) -> Self {
        reasons.push("rejected by pre-trade firewall".to_owned());
        Self {
            approved: false,
            submitted: false,
            reasons,
        }
    }
}

impl OrderProposal {
    pub fn validate_basic(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.symbol.trim().is_empty() {
            problems.push("empty symbol".to_owned());
        }
        if self.notional_usd <= 0.0 || !self.notional_usd.is_finite() {
            problems.push(format!(
                "invalid notional {} (must be finite and > 0)",
                self.notional_usd
            ));
        }
        if let Some(qty) = self.quantity {
            if qty <= 0.0 || !qty.is_finite() {
                problems.push(format!("invalid quantity {qty}"));
            }
        }
        if !matches!(
            self.side.to_ascii_lowercase().as_str(),
            "buy" | "sell" | "buy_to_open" | "buy_to_close" | "sell_to_open" | "sell_to_close"
        ) {
            problems.push(format!("unsupported side '{}'", self.side));
        }
        problems
    }
}
/// Evaluate a proposed order against the full configured + persisted state.
pub fn evaluate(
    config: &Config,
    store: &Store,
    proposal: &OrderProposal,
) -> Result<FirewallVerdict> {
    let mut reasons = Vec::new();

    // ── Kill switch / mode / risk confirmation ────────────────────
    if config.execution.mode != ExecutionMode::Live {
        reasons.push("execution.mode is not live".to_owned());
    }
    if config.execution.kill_switch_engaged {
        reasons.push("kill switch is engaged".to_owned());
    }
    if !config.risk.confirmed {
        reasons.push("risk policy is not confirmed".to_owned());
    }
    if !config.robinhood.connection_ready {
        reasons.push("Robinhood connection is not marked ready".to_owned());
    }
    if !config.robinhood.agentic_account_only {
        reasons.push("agentic_account_only must be true".to_owned());
    }

    // ── Basic shape ───────────────────────────────────────────────
    reasons.extend(proposal.validate_basic());

    // ── Asset class / lane / symbol scope ─────────────────────────
    let asset_class = proposal.asset_class.to_ascii_lowercase();
    if !config
        .strategy
        .allowed_asset_classes
        .iter()
        .any(|class| class.eq_ignore_ascii_case(&asset_class))
    {
        reasons.push(format!(
            "asset class '{}' is not allowed by the strategy contract",
            proposal.asset_class
        ));
    }
    if asset_class == "option" && !config.strategy.allow_options {
        reasons.push("options are disabled by the strategy contract".to_owned());
    }
    if !config.strategy.allows_any_symbol() {
        let approved: HashSet<&str> = config
            .strategy
            .approved_symbols
            .iter()
            .map(String::as_str)
            .collect();
        if !approved
            .iter()
            .any(|s| s.eq_ignore_ascii_case(&proposal.symbol))
        {
            reasons.push(format!(
                "symbol '{}' is not on the approved list",
                proposal.symbol
            ));
        }
    }

    // ── Notional / loss / exposure caps ───────────────────────────
    if proposal.notional_usd > config.strategy.max_order_notional_usd {
        reasons.push(format!(
            "notional {} exceeds max order notional {}",
            proposal.notional_usd, config.strategy.max_order_notional_usd
        ));
    }
    let today_realized = store.latest_realized_pnl_usd()?.unwrap_or(0.0);
    if today_realized.is_sign_negative()
        && today_realized.abs() + proposal.notional_usd > config.strategy.daily_loss_limit_usd
    {
        reasons.push(format!(
            "daily loss limit would be reached (realized {:.2} + notional {:.2})",
            today_realized, proposal.notional_usd
        ));
    }
    if let Some(equity) = store.latest_equity_usd()? {
        let projected = equity + proposal.notional_usd;
        if projected > config.strategy.max_total_exposure_usd {
            reasons.push(format!(
                "projected exposure {projected:.2} exceeds max total exposure {}",
                config.strategy.max_total_exposure_usd
            ));
        }
    }

    // ── Leverage guard ────────────────────────────────────────────
    if !config.strategy.allow_leverage {
        if let Some(buying_power) = store.latest_buying_power_usd()? {
            if proposal.notional_usd > buying_power {
                reasons.push(format!(
                    "notional {} exceeds available buying power {:.2} and leverage is disabled",
                    proposal.notional_usd, buying_power
                ));
            }
        } else if let Some(cash) = store.latest_cash_usd()? {
            if proposal.notional_usd > cash {
                reasons.push(format!(
                    "notional {} exceeds available cash {:.2} and leverage is disabled",
                    proposal.notional_usd, cash
                ));
            }
        }
    }

    // ── Duplicate-order cooldown ──────────────────────────────────
    if store.has_recent_proposal(
        &proposal.symbol,
        &proposal.side,
        config.strategy.duplicate_order_cooldown_secs,
    )? {
        reasons.push("duplicate order within cooldown window".to_owned());
    }

    if !reasons.is_empty() {
        return Ok(FirewallVerdict::reject(reasons));
    }

    // ── Submission decision ───────────────────────────────────────
    // The verdict approves the proposal; whether it is submitted is decided
    // separately (see can_submit), which checks the recorded proposal id and
    // any operator-approval requirement.
    let submitted = config.gateway.submit;
    Ok(FirewallVerdict {
        approved: true,
        submitted,
        reasons: Vec::new(),
    })
}

/// Decide whether a *recorded* approved proposal may actually be submitted.
/// This is the final gate that runs after the proposal has an id. Returns the
/// reasons blocking submission (empty when it may proceed).
pub fn submission_blockers(
    config: &Config,
    store: &Store,
    proposal_id: i64,
) -> Result<Vec<String>> {
    let mut blockers = Vec::new();
    if !config.gateway.submit {
        blockers.push("gateway.submit is disabled".to_owned());
    }
    if config.gateway.require_operator_approval && !store.has_operator_approval(proposal_id)? {
        blockers.push("operator approval is required but not recorded".to_owned());
    }
    Ok(blockers)
}

/// Parse an agent's machine-readable decision block into an order proposal.
/// This is intentionally lenient: the firewall rejects incomplete proposals,
/// so we only need to extract whatever is present and let `validate_basic`
/// fail closed. Returns None when no trade decision block is present.
pub fn proposal_from_decision(
    text: &str,
    asset_class: &str,
    source: &str,
    default_notional: f64,
) -> Option<OrderProposal> {
    let start = text.find('{')?;
    let end = text[start..].rfind('}')? + start;
    let block = &text[start..=end];
    let value: serde_json::Value = serde_json::from_str(block).ok()?;
    let object = value.as_object()?;
    let decision = object
        .get("decision")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("hold")
        .to_ascii_lowercase();
    if !matches!(
        decision.as_str(),
        "buy" | "sell" | "buy_to_open" | "buy_to_close" | "sell_to_open" | "sell_to_close"
    ) {
        return None;
    }
    let symbol = object
        .get("symbol")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_uppercase();
    if symbol.is_empty() {
        return None;
    }
    Some(OrderProposal {
        account_number: object
            .get("account_number")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        asset_class: asset_class.to_owned(),
        symbol,
        side: decision,
        order_type: "limit".to_owned(),
        quantity: object.get("quantity").and_then(serde_json::Value::as_f64),
        notional_usd: object
            .get("notional_usd")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(default_notional),
        limit_price: object
            .get("limit_price")
            .and_then(serde_json::Value::as_f64),
        quote_price: None,
        quote_captured_at: None,
        source: source.to_owned(),
    })
}

/// Deterministic classification of a Robinhood Trading MCP tool name. Returns
/// true for any tool that writes broker state (places, cancels, replaces,
/// previews, submits, modifies, deletes, watchlist/account mutations). Used by
/// the live-lane post-run enforcement so a run that bypassed the firewall and
/// called a write tool directly is flagged and force-blocked.
pub fn is_broker_write_tool(tool_name: &str) -> bool {
    let name = tool_name
        .rsplit("__")
        .next()
        .unwrap_or(tool_name)
        .to_ascii_lowercase();
    if name.starts_with("get_") {
        return false;
    }
    matches!(
        name.as_str(),
        "place_order"
            | "preview_order"
            | "cancel_order"
            | "replace_order"
            | "modify_order"
            | "submit_order"
            | "update_order"
            | "delete_order"
            | "create_watchlist"
            | "add_to_watchlist"
            | "remove_from_watchlist"
            | "delete_watchlist"
            | "update_watchlist"
            | "add"
            | "remove"
            | "modify"
            | "update"
            | "create"
            | "delete"
            | "enable"
            | "disable"
    ) || name.contains("order")
        || name.contains("watchlist")
}

/// Verify no live-lane run called a broker write tool directly. Returns the
/// offending tool names, or an empty vec when the run was read/propose-only.
pub fn check_lane_policy_violations(store: &Store, run_id: &str) -> Result<Vec<String>> {
    Ok(store
        .run_tool_names(run_id)?
        .into_iter()
        .filter(|name| is_broker_write_tool(name))
        .collect())
}

/// Verify the proposal store is reachable (used by the `gate` CLI).
pub fn connectivity_check(store: &Store) -> Result<()> {
    store
        .ping_proposals()
        .map(|_| ())
        .map_err(|err| anyhow::anyhow!("order_proposals table is unavailable: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ExecutionConfig, ExecutionMode, RobinhoodConfig};
    use std::path::Path;

    fn approved_proposal() -> OrderProposal {
        OrderProposal {
            account_number: Some("888248572".to_owned()),
            asset_class: "equity".to_owned(),
            symbol: "SPY".to_owned(),
            side: "buy".to_owned(),
            order_type: "limit".to_owned(),
            quantity: Some(1.0),
            notional_usd: 25.0,
            limit_price: Some(500.0),
            quote_price: None,
            quote_captured_at: None,
            source: "test".to_owned(),
        }
    }

    fn live_config() -> Config {
        let base = Config::default();
        Config {
            execution: ExecutionConfig {
                mode: ExecutionMode::Live,
                kill_switch_engaged: false,
            },
            risk: crate::config::RiskConfig {
                confirmed: true,
                ..base.risk.clone()
            },
            robinhood: RobinhoodConfig {
                connection_ready: true,
                ..base.robinhood.clone()
            },
            strategy: crate::config::StrategyContract {
                canary_enabled: true,
                approved_symbols: vec!["SPY".to_owned()],
                max_order_notional_usd: 100.0,
                ..base.strategy.clone()
            },
            ..base
        }
    }

    #[test]
    fn firewall_rejects_when_config_is_fail_closed() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let verdict = evaluate(&Config::default(), &store, &approved_proposal()).unwrap();
        assert!(!verdict.approved);
        assert!(!verdict.submitted);
        assert!(verdict.reasons.iter().any(|r| r.contains("kill switch")));
    }

    #[test]
    fn firewall_approves_a_small_approved_symbol_proposal() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let config = live_config();
        let verdict = evaluate(&config, &store, &approved_proposal()).unwrap();
        assert!(verdict.approved);
        assert!(!verdict.submitted);
    }

    #[test]
    fn firewall_blocks_unapproved_symbol() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let config = live_config();
        let mut proposal = approved_proposal();
        proposal.symbol = "MU".to_owned();
        let verdict = evaluate(&config, &store, &proposal).unwrap();
        assert!(!verdict.approved);
        assert!(verdict.reasons.iter().any(|r| r.contains("approved list")));
    }

    #[test]
    fn firewall_blocks_oversized_notional() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let config = live_config();
        let mut proposal = approved_proposal();
        proposal.notional_usd = 10_000.0;
        let verdict = evaluate(&config, &store, &proposal).unwrap();
        assert!(!verdict.approved);
        assert!(verdict
            .reasons
            .iter()
            .any(|r| r.contains("max order notional")));
    }

    #[test]
    fn write_tool_classifier_flags_broker_writes_only() {
        assert!(is_broker_write_tool("robinhood-trading__place_order"));
        assert!(is_broker_write_tool("robinhood-trading__cancel_order"));
        assert!(is_broker_write_tool("robinhood-trading__preview_order"));
        assert!(is_broker_write_tool("robinhood-trading__add_to_watchlist"));
        assert!(!is_broker_write_tool("robinhood-trading__get_accounts"));
        assert!(!is_broker_write_tool(
            "robinhood-trading__get_equity_orders"
        ));
        assert!(!is_broker_write_tool(
            "robinhood-trading__get_equity_quotes"
        ));
        assert!(!is_broker_write_tool("read_files"));
    }

    #[test]
    fn proposal_parser_extracts_only_trade_decisions() {
        let text = r#"{"decision":"buy","symbol":"SPY","notional_usd":25,"quantity":1,"reason":"trend"} trailing"#;
        let proposal = proposal_from_decision(text, "equity", "test", 25.0).unwrap();
        assert_eq!(proposal.symbol, "SPY");
        assert_eq!(proposal.notional_usd, 25.0);

        let hold = proposal_from_decision(
            r#"{"decision":"hold","symbol":"SPY"}"#,
            "equity",
            "test",
            25.0,
        );
        assert!(hold.is_none());
    }
}
