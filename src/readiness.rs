use crate::config::{Config, ExecutionMode, SessionMode};

#[derive(Debug, Clone)]
pub struct ReadinessReport {
    pub ready: bool,
    pub blockers: Vec<String>,
    pub notes: Vec<String>,
}

impl ReadinessReport {
    pub fn status(&self) -> String {
        if self.ready {
            "LIVE READY (DIRECT MCP / SOFT RUST CONTROLS)".to_owned()
        } else {
            "LOCKED: ".to_owned() + &self.blockers.join("; ")
        }
    }
}

pub fn check(config: &Config) -> ReadinessReport {
    let mut blockers = Vec::new();
    let mut notes = vec![
        "Rust monitoring cannot pre-block direct Cline-to-Robinhood MCP writes.".to_owned(),
        "Robinhood agent controls and confirmations are the primary execution boundary.".to_owned(),
    ];

    if config.execution.mode != ExecutionMode::Live {
        blockers.push("execution.mode must be live".to_owned());
    }
    if config.execution.kill_switch_engaged {
        blockers.push("kill switch is engaged".to_owned());
    }
    if !config.risk.confirmed {
        blockers.push("risk policy has not been explicitly confirmed".to_owned());
    }
    if !config.robinhood.connection_ready {
        blockers.push("Robinhood MCP connection is not marked ready".to_owned());
    }
    if !config.robinhood.agentic_account_only {
        blockers.push("agentic_account_only must remain true".to_owned());
    }
    if config.agent.session_mode != SessionMode::Fresh {
        blockers.push("live execution requires fresh Cline tasks".to_owned());
    }
    if config.agent.timeout_secs == 0 {
        blockers.push("agent timeout must be greater than zero".to_owned());
    }
    if !config.robinhood.trading_mcp_url.starts_with("https://") {
        blockers.push("Robinhood Trading MCP URL must use HTTPS".to_owned());
    }
    if config.schedule.equity_options.interval_secs == 0 {
        blockers.push("equity/options interval must be greater than zero".to_owned());
    }
    if config.schedule.crypto.interval_secs == 0 {
        blockers.push("crypto interval must be greater than zero".to_owned());
    }
    if config.risk.max_order_notional_usd <= 0.0 {
        blockers.push("max order notional must be greater than zero".to_owned());
    }
    if config.risk.daily_loss_limit_usd <= 0.0 {
        blockers.push("daily loss limit must be greater than zero".to_owned());
    }
    if config.risk.max_total_exposure_usd <= 0.0 {
        blockers.push("max total exposure must be greater than zero".to_owned());
    }
    if config.risk.max_concurrent_positions == 0 {
        blockers.push("max concurrent positions must be greater than zero".to_owned());
    }
    if let Err(error) = config.strategy.validate_against_risk(&config.risk) {
        blockers.push(format!("strategy contract is invalid: {error}"));
    }
    if config.execution.mode == ExecutionMode::Live && !config.strategy.canary_enabled {
        blockers.push(
            "live execution requires the strategy canary to be explicitly enabled".to_owned(),
        );
    }

    if !config.schedule.equity_options.enabled && !config.schedule.crypto.enabled {
        notes.push("both schedule lanes are disabled".to_owned());
    }

    ReadinessReport {
        ready: blockers.is_empty(),
        blockers,
        notes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn default_config_is_locked() {
        let report = check(&Config::default());
        assert!(!report.ready);
        assert!(report.blockers.iter().any(|item| item.contains("live")));
        assert!(report
            .blockers
            .iter()
            .any(|item| item.contains("kill switch")));
    }

    #[test]
    fn configured_live_mode_can_be_ready() {
        let mut config = Config::default();
        config.execution.mode = ExecutionMode::Live;
        config.execution.kill_switch_engaged = false;
        config.risk.confirmed = true;
        config.robinhood.connection_ready = true;
        config.strategy.canary_enabled = true;
        config.strategy.approved_symbols = vec!["BTC".to_owned()];
        assert!(check(&config).ready);
    }

    #[test]
    fn live_mode_requires_an_explicit_strategy_canary() {
        let mut config = Config::default();
        config.execution.mode = ExecutionMode::Live;
        config.execution.kill_switch_engaged = false;
        config.risk.confirmed = true;
        config.robinhood.connection_ready = true;
        let report = check(&config);
        assert!(report
            .blockers
            .iter()
            .any(|item| item.contains("strategy canary")));
    }
}
