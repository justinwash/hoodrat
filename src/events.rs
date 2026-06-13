use std::sync::mpsc;

#[derive(Debug, Clone)]
pub struct PositionSummary {
    pub symbol: String,
    pub detail: String,   // options descriptor ("530C 7/18"), empty for equity
    pub qty: String,
    pub avg: String,
    pub current: String,
    pub pnl: String,
    pub is_gain: bool,
}

#[derive(Debug, Clone)]
pub struct PortfolioSnapshot {
    pub cash: f64,
    pub equity_value: f64,
    pub options_value: f64,
    pub total_value: f64,
    pub pnl: f64,
    pub pnl_pct: f64,
}

#[derive(Debug, Clone)]
pub struct TradeSnapshot {
    pub side: String,
    pub symbol: String,
    pub quantity: f64,
    pub price: f64,
    pub total: f64,
    pub is_buy: bool,
}

#[derive(Debug, Clone)]
pub enum BotEvent {
    /// Short status message for the header bar
    Status(String),
    /// A new analysis cycle started
    CycleStart,
    /// The current cycle finished
    CycleEnd,
    /// Claude called an MCP tool
    ToolCall { name: String, preview: String },
    /// MCP tool returned a result
    ToolResult { preview: String },
    /// Claude produced a final text analysis
    Analysis(String),
    /// Updated portfolio state (paper or live)
    Portfolio(PortfolioSnapshot),
    /// An order was filled (paper or live)
    Trade(TradeSnapshot),
    /// A non-fatal error occurred
    Error(String),
    /// Open position list updated (paper mode)
    Positions(Vec<PositionSummary>),
    /// Backtest finished — show result then revert
    BacktestComplete { pnl: String, pnl_pct: String, total: String },
    /// Token/cost usage after a completed Claude invocation
    Usage(UsageSnapshot),
}

#[derive(Debug, Clone)]
pub struct UsageSnapshot {
    #[allow(dead_code)]
    pub cost_this_run: f64,
    pub cost_session:  f64,
    pub run_count:     u32,
    pub input_tokens:  u64,
    pub output_tokens: u64,
    /// Set only when CLAUDE_BUDGET_USD env var is configured
    pub budget_usd: Option<f64>,
}

pub type EventTx = mpsc::SyncSender<BotEvent>;
pub type EventRx = mpsc::Receiver<BotEvent>;

pub fn channel() -> (EventTx, EventRx) {
    mpsc::sync_channel(512)
}
