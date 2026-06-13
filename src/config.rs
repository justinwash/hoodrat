use anyhow::Result;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradingMode {
    Paper,
    Live,
}

/// Which Robinhood account the bot may trade in.
/// The individual investing account is never listed here by design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountTarget {
    /// The dedicated agentic trading account at agent.robinhood.com.
    Agentic,
    /// The "Machine" brokerage account, approved for options trading.
    Machine,
}

#[derive(Clone)]
pub struct Config {
    pub poll_interval_secs: u64,
    pub mode: TradingMode,
    pub account: AccountTarget,
    pub system_prompt: String,
    pub paper_starting_cash: f64,
    pub paper_portfolio_path: PathBuf,
    /// If set, enables "runs remaining" prediction in the UI (CLAUDE_BUDGET_USD env var).
    pub claude_budget_usd: Option<f64>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mode = match std::env::var("TRADING_MODE")
            .unwrap_or_else(|_| "paper".to_string())
            .to_lowercase()
            .as_str()
        {
            "live" => TradingMode::Live,
            _ => TradingMode::Paper,
        };

        let system_prompt = std::env::var("SYSTEM_PROMPT").unwrap_or_else(|_| {
            "You are an expert equity and options trader with deep knowledge of technical analysis, \
             fundamentals, options pricing (Greeks, IV, skew), and risk management. You have access \
             to a Robinhood trading account via MCP tools. Your job is to scan broadly across the \
             market — any sector, any market cap — and find the best opportunities available right now, \
             whether that means buying equities outright or expressing a view through options.\n\n\
             Trading approach:\n\
             - Consider both direct equity trades (long/short stocks and ETFs) AND options strategies \
               depending on which gives better risk/reward for the setup\n\
             - Do not restrict yourself to well-known large-cap tech names — look across sectors, \
               including small/mid caps, sector ETFs, commodities, and international ETFs\n\
             - Use market screening tools to surface momentum, volume anomalies, or technical setups \
               across the broader market before deciding where to focus\n\
             - For options: prefer defined-risk strategies (spreads, iron condors) over naked exposure; \
               do not leg into spreads — enter as a spread order\n\
             - Check existing positions before opening new ones to avoid overconcentration\n\n\
             Execution rules:\n\
             - When you identify a good trade, execute it — do not stop to seek confirmation\n\
             - State your reasoning concisely as part of the same response in which you act\n\
             - Inaction is a deliberate choice; if you choose not to trade, say why briefly"
                .to_string()
        });

        let account = match std::env::var("ACCOUNT_TARGET")
            .unwrap_or_else(|_| "agentic".to_string())
            .to_lowercase()
            .as_str()
        {
            "machine" => AccountTarget::Machine,
            _ => AccountTarget::Agentic,
        };

        Ok(Config {
            poll_interval_secs: std::env::var("POLL_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            mode,
            account,
            system_prompt,
            paper_starting_cash: std::env::var("PAPER_STARTING_CASH")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(25_000.0),
            paper_portfolio_path: std::env::var("PAPER_PORTFOLIO_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("paper_portfolio.json")),
            claude_budget_usd: std::env::var("CLAUDE_BUDGET_USD")
                .ok()
                .and_then(|v| v.parse::<f64>().ok()),
        })
    }
}
