use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::config::{AccountTarget, Config, TradingMode};
use crate::events::{BotEvent, EventTx};
use crate::paper::{PaperExchange, PaperPortfolio};

pub struct Bot {
    config: Config,
    events: Option<EventTx>,
    /// Steering message queued from the UI, consumed at the start of each cycle.
    pub steer: Arc<Mutex<Option<String>>>,
    /// Selected tickers from the UI — read (not consumed) each cycle.
    pub tickers: Arc<Mutex<Vec<String>>>,
    /// Active only in Paper mode — simulates fills and tracks virtual portfolio.
    paper: Option<PaperExchange>,
    /// Extracted from Claude's output during a Paper cycle; applied after the loop.
    paper_trades_pending: Option<String>,
    /// Accumulated spend across all runs this session (from Claude result events).
    session_cost: f64,
    /// Number of completed Claude invocations this session.
    run_count: u32,
    /// Live-mode portfolio state accumulated from tool results during the current cycle.
    live_portfolio: Option<crate::events::PortfolioSnapshot>,
    /// Live-mode position list, merged from tool results during the current cycle.
    live_positions: Vec<crate::events::PositionSummary>,
}

impl Bot {
    pub fn new(config: Config) -> Self {
        Self::new_with_sender(config, None)
    }

    pub fn new_with_sender(config: Config, events: Option<EventTx>) -> Self {
        let paper = if config.mode == TradingMode::Paper {
            let portfolio = PaperPortfolio::load_or_new(
                &config.paper_portfolio_path,
                config.paper_starting_cash,
            );
            Some(PaperExchange::new(portfolio, config.paper_portfolio_path.clone()))
        } else {
            None
        };
        Self {
            config,
            events,
            steer: Arc::new(Mutex::new(None)),
            tickers: Arc::new(Mutex::new(Vec::new())),
            paper,
            paper_trades_pending: None,
            session_cost: 0.0,
            run_count: 0,
            live_portfolio: None,
            live_positions: Vec::new(),
        }
    }

    fn emit(&self, event: BotEvent) {
        if let Some(tx) = &self.events {
            let _ = tx.send(event);
        }
    }

    fn ticker_section(&self) -> String {
        let t = self.tickers.lock().unwrap();
        if t.is_empty() {
            "\n\nNo tickers are pinned — scan the broader market. Use any available screening \
             or news tools to surface candidates across sectors and market caps: don't limit \
             yourself to large-cap tech. Look at sector ETFs, small/mid caps, commodities ETFs, \
             and anything showing unusual volume, momentum, or a clear technical setup. \
             Evaluate each candidate for both a direct equity trade and an options structure, \
             then pick whichever gives the best risk/reward."
                .to_string()
        } else {
            format!(
                "\n\nFocus your analysis on these tickers: {}. For each, evaluate both a \
                 direct equity trade and an options structure and pick whichever gives the \
                 better risk/reward.",
                t.join(", ")
            )
        }
    }

    fn build_prompt(&self) -> String {
        let account_instruction = account_prompt_instruction(&self.config.account);

        let mode_instruction = match self.config.mode {
            TradingMode::Paper => {
                "This is PAPER TRADING mode. Use your Robinhood tools to gather real market \
                 data and inspect the target account. DO NOT call any order-placement tools — \
                 this is a simulation only.\n\n\
                 After your analysis, output your reasoning and then IMMEDIATELY output the \
                 PAPER_TRADES_JSON line. If you identified a trade worth taking, it MUST appear \
                 in the trades array — do not second-guess yourself into an empty array. \
                 Only use an empty trades array if you genuinely found no setup worth acting on \
                 and you explain why.\n\n\
                 The line must be the last thing you output, in this exact format:\n\
                 PAPER_TRADES_JSON: {\"real_cash\":150.00,\"real_equity_value\":50.00,\"real_options_value\":0.00,\
                 \"trades\":[{\"symbol\":\"AAPL\",\"side\":\"buy\",\"quantity\":10,\"price\":195.50}]}\n\n\
                 real_cash: available cash balance in the target account.\n\
                 real_equity_value: total market value of equity positions in the target account.\n\
                 real_options_value: total market value of options positions in the target account.\n\
                 trades: orders to simulate. Each trade: symbol (uppercase), side (\"buy\"/\"sell\"), \
                 quantity, price (per share; for options also add expiration_date YYYY-MM-DD, \
                 strike, option_type \"call\"/\"put\"). \
                 Use prices from the market data you queried. Report 0.0 for any value you could not determine."
            }
            TradingMode::Live => {
                "This is LIVE trading mode. Real orders will be placed on the target account. \
                 These are for-fun accounts — risk tolerance is relaxed. \
                 Size positions based on your conviction and available capital.\n\n\
                 Workflow: state your reasoning in one short paragraph, then IMMEDIATELY call \
                 the order placement tool. Do not stop between the reasoning and the tool call — \
                 there is no confirmation step. If you decide to act, act."
            }
        };

        let steer = self.steer.lock().unwrap().take();
        let steer_section = steer
            .as_deref()
            .map(|s| format!("\n\nAdditional instructions from the user: {s}"))
            .unwrap_or_default();

        // In paper mode, inject the current simulated portfolio so Claude knows
        // what positions are already open and doesn't re-enter them.
        let paper_portfolio_section = match &self.paper {
            Some(px) if px.portfolio.real_synced => format!(
                "\n\n=== Current Simulated Portfolio (paper) ===\n{}\n\
                 Do NOT open new positions in symbols already held above unless you are \
                 intentionally adding to or closing the position.",
                px.portfolio.summary()
            ),
            _ => String::new(),
        };

        format!(
            "Use your Robinhood trading tools to review the current portfolio, then scan the \
             market for the best available trade — equity or options, any sector or market cap.\n\n\
             {account_instruction}\n\n\
             {mode_instruction}{}{steer_section}{paper_portfolio_section}",
            self.ticker_section()
        )
    }


    // ── Public entry points ───────────────────────────────────────────────────

    pub async fn run_once(&mut self) -> Result<()> {
        self.run_cycle(Arc::new(AtomicBool::new(false))).await
    }

    pub async fn run_loop(&mut self) -> Result<()> {
        self.run_loop_cancellable(Arc::new(AtomicBool::new(false))).await
    }

    pub async fn run_loop_cancellable(&mut self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::Relaxed) {
            if let Err(e) = self.run_cycle(stop.clone()).await {
                error!("Cycle failed: {e:#}");
                self.emit(BotEvent::Error(e.to_string()));
                self.emit(BotEvent::CycleEnd);
                self.emit(BotEvent::Status("Idle (error)".to_string()));
            }

            if stop.load(Ordering::Relaxed) { break; }

            let secs = self.config.poll_interval_secs;
            self.emit(BotEvent::Status(format!("Next cycle in {secs}s")));
            info!("Sleeping {secs}s until next cycle");

            for _ in 0..(secs * 10) {
                if stop.load(Ordering::Relaxed) { return Ok(()); }
                sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }

    pub async fn run_backtest(&mut self, period: &str, stop: Arc<AtomicBool>) -> Result<()> {
        self.emit(BotEvent::CycleStart);

        // ── 1. Fetch historical data for all selected tickers ────────────────
        let ticker_list: Vec<String> = {
            let t = self.tickers.lock().unwrap();
            if t.is_empty() {
                vec![
                    "SPY".into(), "QQQ".into(), "IWM".into(),   // broad market
                    "XLF".into(), "XLE".into(), "XLV".into(),   // financials, energy, health
                    "GLD".into(), "TLT".into(),                  // gold, bonds
                    "AAPL".into(), "TSLA".into(), "NVDA".into(), // individual names
                ]
            } else {
                t.clone()
            }
        };

        let mut all_bars: std::collections::HashMap<String, Vec<crate::historical::OhlcvBar>> =
            std::collections::HashMap::new();

        for symbol in &ticker_list {
            self.emit(BotEvent::Status(format!("Fetching {symbol} ({period})…")));
            match crate::historical::fetch_history(symbol, period).await {
                Ok(bars) => {
                    info!("[backtest] {} bars for {symbol}", bars.len());
                    all_bars.insert(symbol.clone(), bars);
                }
                Err(e) => {
                    warn!("[backtest] fetch failed for {symbol}: {e}");
                    self.emit(BotEvent::Error(format!("Data unavailable for {symbol}: {e}")));
                }
            }
        }

        let max_len = all_bars.values().map(|v| v.len()).max().unwrap_or(0);
        if max_len == 0 {
            self.emit(BotEvent::Error("No historical data could be fetched.".into()));
            self.emit(BotEvent::CycleEnd);
            self.emit(BotEvent::Status("Idle".into()));
            return Ok(());
        }

        // ── 3. Swap in a fresh paper exchange for the backtest ───────────────
        // The real paper portfolio is saved and restored afterwards so backtests
        // never pollute the persistent paper-trading state.
        let saved_paper = self.paper.take();
        {
            let fresh = crate::paper::PaperPortfolio::new(self.config.paper_starting_cash);
            self.paper = Some(crate::paper::PaperExchange::new(
                fresh,
                std::path::PathBuf::from("backtest_portfolio.json"),
            ));
        }

        // ── 4. Sync real account state (cash + positions) into the backtest ──
        self.emit(BotEvent::Status("Syncing account state for backtest…".into()));
        let sync_prompt = self.build_account_sync_prompt();
        let tools = self.allowed_tools();
        self.run_subprocess(sync_prompt, stop.clone(), true, &tools).await?;

        if let Some(ref px) = self.paper {
            info!("[backtest] Starting state: {}", px.portfolio.summary());
            self.emit(BotEvent::Portfolio(px.portfolio.snapshot()));
        }

        // Consume the steer once — applies to the whole backtest session.
        let steer = self.steer.lock().unwrap().take();
        let steer_section = steer.as_deref()
            .map(|s| format!("\n\nAdditional focus for this backtest: {s}"))
            .unwrap_or_default();

        // ── 2. Step through time, giving Claude only data up to each point ───
        // Each step is one Claude call. Claude never sees bars beyond `cursor`,
        // making lookahead bias structurally impossible.
        let step_size   = backtest_step_size(period, max_len);
        let total_steps = max_len.div_ceil(step_size);

        info!("--- Backtest ({period}): {total_steps} steps, step_size={step_size} ---");

        if let Some(ref px) = self.paper {
            self.emit(BotEvent::Portfolio(px.portfolio.snapshot()));
        }

        for step_num in 1..=total_steps {
            if stop.load(Ordering::Relaxed) { break; }

            let cursor = (step_num * step_size).min(max_len); // bars visible this step

            // Build sliced data sections for each ticker.
            let mut sections: Vec<String> = Vec::new();
            let mut last_ts: i64 = 0;
            for symbol in &ticker_list {
                if let Some(bars) = all_bars.get(symbol) {
                    let visible = &bars[..cursor.min(bars.len())];
                    if !visible.is_empty() {
                        if let Some(b) = visible.last() { last_ts = b.timestamp; }
                        sections.push(crate::historical::format_for_prompt(symbol, visible, period));
                    }
                }
            }

            let sim_time = crate::historical::unix_to_datetime(last_ts);
            self.emit(BotEvent::Status(
                format!("Backtest {step_num}/{total_steps} — {sim_time}")
            ));

            let portfolio_state = self.portfolio_state_for_prompt();
            let market_data     = sections.join("\n");

            let prompt = build_backtest_step_prompt(
                step_num, total_steps, &sim_time, period,
                &market_data, &portfolio_state, &steer_section,
            );

            // No MCP tools — all data is inline. Blocking tools prevents Claude
            // from accidentally querying live prices (which would be "future" data
            // relative to the simulated timestamp).
            self.run_subprocess(prompt, stop.clone(), true, "").await?;

            let step_snap = if let Some(ref px) = self.paper {
                Some((px.portfolio.snapshot(), px.portfolio.position_summaries()))
            } else { None };
            if let Some((s, p)) = step_snap {
                self.emit(BotEvent::Portfolio(s));
                self.emit(BotEvent::Positions(p));
            }
        }

        // Capture final backtest result before restoring.
        let backtest_result = if let Some(ref px) = self.paper {
            let s = px.portfolio.snapshot();
            let sign = if s.pnl >= 0.0 { "+" } else { "" };
            Some((
                format!("{sign}${:.2}", s.pnl),
                format!("{sign}{:.2}%", s.pnl_pct),
                format!("${:.2}", s.total_value),
            ))
        } else { None };

        // Restore the real paper portfolio — backtest is purely ephemeral.
        self.paper = saved_paper;

        if let Some((pnl, pnl_pct, total)) = backtest_result {
            info!("--- Backtest complete ({total_steps} steps) — {total} P&L {pnl} ({pnl_pct}) ---");
            self.emit(BotEvent::BacktestComplete { pnl, pnl_pct, total });
        } else {
            info!("--- Backtest complete ({total_steps} steps) ---");
        }

        // Revert UI to real portfolio state.
        let restored = if let Some(ref px) = self.paper {
            Some((px.portfolio.snapshot(), px.portfolio.position_summaries()))
        } else { None };
        if let Some((s, p)) = restored {
            self.emit(BotEvent::Portfolio(s));
            self.emit(BotEvent::Positions(p));
        }

        self.emit(BotEvent::CycleEnd);
        self.emit(BotEvent::Status("Idle".into()));
        Ok(())
    }

    // ── Core cycle ────────────────────────────────────────────────────────────

    async fn run_cycle(&mut self, stop: Arc<AtomicBool>) -> Result<()> {
        self.emit(BotEvent::CycleStart);
        self.emit(BotEvent::Status("Starting...".to_string()));
        info!("--- Starting cycle ({}) ---", mode_label(&self.config.mode));
        // Reset live-mode accumulator so stale state from prior cycles doesn't bleed in.
        self.live_portfolio = None;
        self.live_positions.clear();

        let initial = if let Some(ref px) = self.paper {
            if px.portfolio.real_synced {
                Some((px.portfolio.snapshot(), px.portfolio.position_summaries()))
            } else { None }
        } else { None };
        if let Some((s, p)) = initial {
            self.emit(BotEvent::Portfolio(s));
            self.emit(BotEvent::Positions(p));
        }

        let prompt = self.build_prompt();
        let tools  = self.allowed_tools();
        self.run_subprocess(prompt, stop, true, &tools).await?;

        self.emit(BotEvent::CycleEnd);
        self.emit(BotEvent::Status("Idle".to_string()));
        info!("--- Cycle complete ---");
        Ok(())
    }

    fn build_account_sync_prompt(&self) -> String {
        let account_instruction = account_prompt_instruction(&self.config.account);
        format!(
            "Retrieve the complete current state of the target account: \
             cash balance, all open stock positions, and all open options positions.\n\n\
             {account_instruction}\n\n\
             Output the account state on a SINGLE LINE in this exact format \
             (no other text before or after):\n\
             PAPER_TRADES_JSON: {{\"real_cash\":200.00,\"real_equity_value\":0.00,\
             \"real_options_value\":0.00,\
             \"real_equity_positions\":[{{\"symbol\":\"AAPL\",\"quantity\":1.0,\"avg_cost\":195.00}}],\
             \"real_options_positions\":[{{\"symbol\":\"SPY\",\"expiration\":\"2026-07-18\",\
             \"strike\":530.0,\"option_type\":\"call\",\"contracts\":1.0,\"avg_cost\":2.50}}],\
             \"trades\":[]}}\n\n\
             real_equity_positions: every open stock position — empty array [] if none.\n\
             real_options_positions: every open options position — empty array [] if none.\n\
             Use 0.0 for any numeric value you could not determine."
        )
    }

    fn allowed_tools(&self) -> String {
        match self.config.account {
            AccountTarget::Agentic =>
                "mcp__robinhood-trading__*".to_string(),
            AccountTarget::Machine =>
                // Include both MCPs: machine handles account/orders, trading for market data
                "mcp__robinhood-trading__*,mcp__robinhood-machine__*".to_string(),
        }
    }

    fn portfolio_state_for_prompt(&self) -> String {
        match &self.paper {
            Some(px) => px.portfolio.summary(),
            None     => "(live mode — portfolio state tracked by brokerage)".to_string(),
        }
    }

    // ── Live-mode portfolio ingestion ─────────────────────────────────────────
    //
    // Called once per tool result in live mode. Tries to parse portfolio totals
    // and/or individual positions from the raw JSON response text, accumulating
    // state across multiple tool calls within a single cycle.

    fn ingest_live_tool_result(
        &mut self,
        text: &str,
    ) -> Option<(crate::events::PortfolioSnapshot, Vec<crate::events::PositionSummary>)> {
        use serde_json::Value;
        let Ok(v) = serde_json::from_str::<Value>(text) else { return None; };

        let mut changed = false;

        // ── Portfolio totals ──────────────────────────────────────────────────
        if let Some(obj) = v.as_object() {
            let cash   = lv_f64(obj, &["cash", "buying_power", "available_funds"]);
            let equity = lv_f64(obj, &["equity_value", "market_value", "equity"]);
            let opts   = lv_f64(obj, &["options_value", "options_market_value"]);
            let total  = lv_f64(obj, &["total_value", "portfolio_value", "account_value"]);

            if total.is_some() || cash.is_some() || equity.is_some() {
                let c = cash.unwrap_or(0.0);
                let e = equity.unwrap_or(0.0);
                let o = opts.unwrap_or(0.0);
                let t = total.unwrap_or(c + e + o);
                let pnl = lv_f64(obj, &["unrealized_pnl", "pnl", "profit_loss"]).unwrap_or(0.0);
                let cost = t - pnl;
                self.live_portfolio = Some(crate::events::PortfolioSnapshot {
                    cash: c, equity_value: e, options_value: o, total_value: t,
                    pnl, pnl_pct: if cost > 0.01 { pnl / cost * 100.0 } else { 0.0 },
                });
                changed = true;
            }
        }

        // ── Positions ─────────────────────────────────────────────────────────
        // Check top-level array and nested arrays under common keys.
        let mut pos_sources: Vec<&Value> = Vec::new();
        if v.is_array() { pos_sources.push(&v); }
        if let Some(obj) = v.as_object() {
            for key in &["results", "positions", "equity_positions", "stock_positions",
                         "options_positions", "option_positions"] {
                if let Some(arr) = obj.get(*key) { pos_sources.push(arr); }
            }
        }

        for src in pos_sources {
            let parsed = lv_parse_positions(src);
            if !parsed.is_empty() {
                // Merge: update existing entry if same symbol+detail, else append.
                for np in parsed {
                    let key = format!("{}{}", np.symbol, np.detail);
                    match self.live_positions.iter_mut()
                        .find(|p| format!("{}{}", p.symbol, p.detail) == key)
                    {
                        Some(slot) => *slot = np,
                        None       => self.live_positions.push(np),
                    }
                }
                changed = true;
            }
        }

        if !changed { return None; }

        let snap = self.live_portfolio.clone().unwrap_or(crate::events::PortfolioSnapshot {
            cash: 0.0, equity_value: 0.0, options_value: 0.0,
            total_value: 0.0, pnl: 0.0, pnl_pct: 0.0,
        });
        Some((snap, self.live_positions.clone()))
    }

    // ── Subprocess runner (shared by trading cycles and backtests) ────────────

    async fn run_subprocess(
        &mut self,
        prompt: String,
        stop: Arc<AtomicBool>,
        apply_paper: bool,
        tools: &str,
    ) -> Result<()> {
        let system_prompt = self.config.system_prompt.clone();

        let mut cmd = tokio::process::Command::new("claude");
        cmd.args(["--print", "--output-format", "stream-json", "--verbose"]);
        if !tools.is_empty() {
            cmd.args(["--allowedTools", tools]);
        }
        if !system_prompt.is_empty() {
            cmd.args(["--system-prompt", &system_prompt]);
        }
        cmd.arg(&prompt);
        cmd.stdin(std::process::Stdio::null())
           .stdout(std::process::Stdio::piped())
           .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn()
            .context("Failed to spawn `claude` — is Claude Code installed and in PATH?")?;

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let stderr_tx = self.events.clone();
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if !line.is_empty() {
                    error!("[claude stderr] {line}");
                    if let Some(tx) = &stderr_tx {
                        let _ = tx.send(BotEvent::Error(format!("[stderr] {line}")));
                    }
                }
            }
        });

        let mut lines = BufReader::new(stdout).lines();

        loop {
            if stop.load(Ordering::Relaxed) {
                child.kill().await.ok();
                self.emit(BotEvent::Status("Stopped".to_string()));
                break;
            }

            let line = match tokio::time::timeout(
                Duration::from_millis(100),
                lines.next_line(),
            ).await {
                Ok(Ok(Some(l))) => l,
                Ok(Ok(None))    => break,
                Ok(Err(e))      => { error!("stdout read error: {e}"); break; }
                Err(_)          => continue,
            };

            if let Some(url) = extract_auth_url(&line) {
                info!("[auth] Opening Robinhood auth URL in browser");
                self.emit(BotEvent::Status(
                    "Robinhood auth required — complete login in your browser".to_string(),
                ));
                if let Err(e) = webbrowser::open(&url) {
                    warn!("Could not open browser: {e}");
                    self.emit(BotEvent::Error(format!("Open this URL to authenticate: {url}")));
                }
            }

            let is_final = line.contains(r#""type":"result""#);
            self.handle_line(&line);
            if is_final { break; }
        }

        stderr_task.await.ok();
        child.wait().await.ok();

        if apply_paper {
            if let Some(raw) = self.paper_trades_pending.take() {
                self.apply_paper_trades(&raw);
            }
            let post = if let Some(ref px) = self.paper {
                Some((px.portfolio.snapshot(), px.portfolio.position_summaries()))
            } else { None };
            if let Some((s, p)) = post {
                self.emit(BotEvent::Portfolio(s));
                self.emit(BotEvent::Positions(p));
            }
        } else {
            self.paper_trades_pending = None;
        }

        Ok(())
    }

    // ── Apply simulated paper trades ──────────────────────────────────────────

    fn apply_paper_trades(&mut self, raw: &str) {
        let Ok(batch) = serde_json::from_str::<serde_json::Value>(raw) else {
            warn!("[paper] Failed to parse PAPER_TRADES_JSON: {raw}");
            return;
        };

        // Sync the real account baseline before simulating any trades.
        if let Some(ref mut px) = self.paper {
            let real_cash          = batch["real_cash"].as_f64().unwrap_or(0.0);
            let real_equity_value  = batch["real_equity_value"].as_f64().unwrap_or(0.0);
            let real_options_value = batch["real_options_value"].as_f64().unwrap_or(0.0);

            let has_position_detail =
                batch.get("real_equity_positions").is_some()
                || batch.get("real_options_positions").is_some();

            if has_position_detail {
                // Full account snapshot — seed with real positions.
                let equity  = parse_equity_positions(&batch);
                let options = parse_options_positions(&batch);
                px.portfolio.seed_from_real_state(real_cash, equity, options);
            } else if real_cash > 0.0 || real_equity_value > 0.0 || real_options_value > 0.0 {
                // Value-only sync (normal paper trading cycles).
                px.portfolio.sync_from_real(real_cash, real_equity_value, real_options_value);
            }
        }

        let Some(trades) = batch["trades"].as_array() else { return };
        if trades.is_empty() {
            info!("[paper] No trades this cycle");
            return;
        }

        type Outcome = Result<(String, Option<crate::events::TradeSnapshot>), String>;
        let mut outcomes: Vec<Outcome> = Vec::new();

        if let Some(ref mut px) = self.paper {
            for trade in trades {
                let tool_name = if trade.get("expiration_date").is_some()
                    || trade.get("strike").is_some()
                    || trade.get("option_type").is_some()
                {
                    "options_order"
                } else {
                    "equity_order"
                };
                match px.simulate_order(tool_name, trade) {
                    Ok(msg)  => outcomes.push(Ok((msg, px.last_trade_snapshot()))),
                    Err(e)   => outcomes.push(Err(e.to_string())),
                }
            }
        }

        for outcome in outcomes {
            match outcome {
                Ok((msg, snap)) => {
                    info!("[paper] {}", truncate(msg.clone(), 300));
                    self.emit(BotEvent::Analysis(format!("[paper] {}", truncate(msg, 200))));
                    if let Some(snap) = snap {
                        self.emit(BotEvent::Trade(snap));
                    }
                }
                Err(e) => {
                    warn!("[paper] Trade failed: {e}");
                    self.emit(BotEvent::Error(format!("Paper trade failed: {e}")));
                }
            }
        }
    }

    // ── Parse one stream-json line ────────────────────────────────────────────

    fn handle_line(&mut self, line: &str) {
        let Ok(json) = serde_json::from_str::<serde_json::Value>(line) else { return };

        match json["type"].as_str() {
            Some("assistant") => {
                let Some(content) = json["message"]["content"].as_array() else { return };
                for block in content {
                    match block["type"].as_str() {
                        Some("text") => {
                            let text = block["text"].as_str().unwrap_or("").trim();
                            if text.is_empty() { continue; }

                            // Capture the paper trades marker for post-cycle processing.
                            // Use serde_json's streaming deserializer so trailing text
                            // (Claude's follow-up commentary) doesn't break the parse.
                            // Check paper.is_some() rather than mode so backtests also
                            // capture this in Live mode.
                            if self.paper.is_some() {
                                if let Some(pos) = text.find("PAPER_TRADES_JSON:") {
                                    let after = text[pos + "PAPER_TRADES_JSON:".len()..].trim_start();
                                    if after.starts_with('{') {
                                        use serde::de::Deserialize as _;
                                        let mut de = serde_json::Deserializer::from_str(after);
                                        if let Ok(val) = serde_json::Value::deserialize(&mut de) {
                                            info!("[paper] captured PAPER_TRADES_JSON");
                                            self.paper_trades_pending = Some(val.to_string());
                                        } else {
                                            warn!("[paper] PAPER_TRADES_JSON found but failed to parse");
                                        }
                                    }
                                }
                            }

                            info!("[claude] {}", truncate(text.to_string(), 200));
                            self.emit(BotEvent::Analysis(text.to_string()));
                        }
                        Some("tool_use") => {
                            let name = block["name"].as_str().unwrap_or("?");
                            let input_str = block["input"].to_string();
                            info!("[tool→] {name}\n{}", indent_json(&input_str));
                            let human = humanize_json(&input_str);
                            self.emit(BotEvent::Status(format!("Calling {name}...")));
                            self.emit(BotEvent::ToolCall { name: name.to_string(), preview: truncate(human, 800) });
                        }
                        _ => {}
                    }
                }
            }

            Some("user") => {
                let Some(content) = json["message"]["content"].as_array() else { return };
                for block in content {
                    if block["type"] == "tool_result" {
                        let text = extract_tool_result_text(block);

                        let price_update = if let Some(ref mut px) = self.paper {
                            px.ingest_prices(&text);
                            if !px.portfolio.equity.is_empty() || !px.portfolio.options.is_empty() {
                                Some((px.portfolio.snapshot(), px.portfolio.position_summaries()))
                            } else { None }
                        } else {
                            self.ingest_live_tool_result(&text)
                        };
                        if let Some((s, p)) = price_update {
                            self.emit(BotEvent::Portfolio(s));
                            self.emit(BotEvent::Positions(p));
                        }

                        let raw = if text.len() > 3000 {
                            format!("{}…", safe_truncate(&text, 3000))
                        } else { text };
                        info!("[tool←]\n{}", indent_json(&raw));
                        let preview = humanize_json(&raw);
                        self.emit(BotEvent::ToolResult { preview });
                    }
                }
            }

            Some("result") => {
                if json["subtype"] == "error" {
                    let msg = json["error"].as_str().unwrap_or("unknown error");
                    error!("[claude result error] {msg}");
                    self.emit(BotEvent::Error(msg.to_string()));
                }
                // Always count the run; cost_usd may not be present in all CLI versions.
                let cost          = json["cost_usd"].as_f64().unwrap_or(0.0);
                let input_tokens  = json["usage"]["input_tokens"].as_u64().unwrap_or(0);
                let output_tokens = json["usage"]["output_tokens"].as_u64().unwrap_or(0);
                self.session_cost += cost;
                self.run_count    += 1;
                info!(
                    "[usage] run ${cost:.4} | session ${:.4} (run #{}) | tokens {}in {}out",
                    self.session_cost, self.run_count, input_tokens, output_tokens
                );
                self.emit(BotEvent::Usage(crate::events::UsageSnapshot {
                    cost_this_run: cost,
                    cost_session:  self.session_cost,
                    run_count:     self.run_count,
                    input_tokens,
                    output_tokens,
                    budget_usd:    self.config.claude_budget_usd,
                }));
            }

            Some("system") => {}
            _ => tracing::debug!("[claude unknown] {line}"),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn extract_auth_url(line: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(line).ok()?;

    if json["type"] == "user" {
        if let Some(content) = json["message"]["content"].as_array() {
            for block in content {
                if block["type"] == "tool_result" {
                    let text = extract_tool_result_text(block);
                    if let Some(url) = pick_oauth_url(&text) {
                        return Some(url);
                    }
                }
            }
        }
    }

    if json["type"] == "assistant" {
        if let Some(content) = json["message"]["content"].as_array() {
            for block in content {
                if block["type"] == "text" {
                    let text = block["text"].as_str().unwrap_or("");
                    if let Some(url) = pick_oauth_url(text) {
                        return Some(url);
                    }
                }
            }
        }
    }

    None
}

fn pick_oauth_url(text: &str) -> Option<String> {
    let start = text.find("https://robinhood.com/oauth")?;
    let rest = &text[start..];
    let end = rest.find(|c: char| c.is_ascii_whitespace() || c == '"' || c == '\'')
        .unwrap_or(rest.len());
    let url = rest[..end].to_string();
    if url.contains("code_challenge") { Some(url) } else { None }
}

fn extract_tool_result_text(block: &serde_json::Value) -> String {
    if let Some(s) = block["content"].as_str() {
        return s.to_string();
    }
    if let Some(arr) = block["content"].as_array() {
        return arr.iter().filter_map(|b| b["text"].as_str()).collect::<Vec<_>>().join("\n");
    }
    String::new()
}

fn mode_label(mode: &TradingMode) -> &'static str {
    match mode { TradingMode::Paper => "paper", TradingMode::Live => "LIVE" }
}

fn parse_equity_positions(batch: &serde_json::Value) -> Vec<crate::paper::EquityPosition> {
    batch.get("real_equity_positions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().filter_map(|p| {
                let symbol   = p["symbol"].as_str()?.to_uppercase();
                let quantity = p["quantity"].as_f64()?;
                let avg_cost = p["avg_cost"].as_f64().unwrap_or(0.0);
                Some(crate::paper::EquityPosition { symbol, quantity, avg_cost })
            }).collect()
        })
        .unwrap_or_default()
}

fn parse_options_positions(batch: &serde_json::Value) -> Vec<crate::paper::OptionsPosition> {
    batch.get("real_options_positions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().filter_map(|p| {
                let symbol      = p["symbol"].as_str()?.to_uppercase();
                let expiration  = p["expiration"].as_str()?.to_string();
                let strike      = p["strike"].as_f64()?;
                let option_type = p["option_type"].as_str()?.to_lowercase();
                let contracts   = p["contracts"].as_f64()?;
                let avg_cost    = p["avg_cost"].as_f64().unwrap_or(0.0);
                Some(crate::paper::OptionsPosition { symbol, expiration, strike, option_type, contracts, avg_cost })
            }).collect()
        })
        .unwrap_or_default()
}

/// How many bars to advance per backtest step.
/// Fewer steps → faster run; more steps → finer-grained simulation.
/// Targets ~6–13 decision points per period.
fn backtest_step_size(period: &str, total_bars: usize) -> usize {
    let target_steps: usize = match period {
        "1 day"   => 6,
        "1 week"  => 8,
        "1 month" => 7,
        "1 year"  => 12,
        _         => 8,
    };
    (total_bars / target_steps).max(1)
}

/// Build the prompt for one time-sliced backtest step.
/// `market_data` contains ONLY bars up to `sim_time` — Claude cannot see future bars.
fn build_backtest_step_prompt(
    step: usize,
    total: usize,
    sim_time: &str,
    period: &str,
    market_data: &str,
    portfolio: &str,
    steer_section: &str,
) -> String {
    format!(
        "BACKTEST STEP {step}/{total}  ({period} simulation)\n\
         Simulated time: {sim_time}\n\n\
         RULES:\n\
         • You are a live trader at exactly {sim_time}. You have NO knowledge of \
           what happens after this moment.\n\
         • ONLY use the price data provided below — do not infer future prices, \
           do not recall any data beyond the last row shown.\n\
         • Do NOT call any external tools or MCP services — all data is inline.\n\n\
         === Current Simulated Portfolio ===\n\
         {portfolio}\n\n\
         === Market Data up to {sim_time} ===\n\
         {market_data}\n\
         (End of visible data — no rows exist after this point.)\n\n\
         Apply your trading strategy to the data above. Briefly state:\n\
         1. What signals you observe in the most recent bars\n\
         2. Your decision (trade or hold) and reasoning\n\n\
         Then output on a SINGLE line:\n\
         PAPER_TRADES_JSON: {{\"real_cash\":0.0,\"real_equity_value\":0.0,\
         \"real_options_value\":0.0,\
         \"trades\":[{{\"symbol\":\"SPY\",\"side\":\"buy\",\"quantity\":1,\"price\":530.00}}]}}\n\n\
         Use the last available close price as the fill price. \
         Empty trades array if holding.{steer_section}"
    )
}

// ---------------------------------------------------------------------------
// Live-mode JSON parsing helpers
// ---------------------------------------------------------------------------

fn lv_f64(obj: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| {
        let v = obj.get(*k)?;
        v.as_f64().or_else(|| v.as_str()?.parse().ok())
    })
}

fn lv_parse_positions(arr: &serde_json::Value) -> Vec<crate::events::PositionSummary> {
    let Some(items) = arr.as_array() else { return vec![] };
    items.iter().filter_map(lv_parse_one_position).collect()
}

fn lv_parse_one_position(p: &serde_json::Value) -> Option<crate::events::PositionSummary> {
    let obj = p.as_object()?;

    let symbol = obj.get("symbol").or_else(|| obj.get("ticker"))
        .or_else(|| obj.get("underlying_symbol"))
        .and_then(|s| s.as_str())
        .map(str::to_uppercase)?;

    let qty = lv_f64(obj, &["quantity", "contracts"])?;
    if qty == 0.0 { return None; }

    let is_option = obj.contains_key("strike_price") || obj.contains_key("option_type")
        || obj.contains_key("expiration_date");

    if is_option {
        let avg = lv_f64(obj, &["average_open_price", "avg_cost", "average_price"]).unwrap_or(0.0);
        let cur = lv_f64(obj, &["mark_price", "price", "last_trade_price"]).unwrap_or(avg);
        let pnl = (cur - avg) * qty * 100.0;
        let is_gain = pnl >= 0.0;
        let strike = lv_f64(obj, &["strike_price", "strike"]);
        let opt_type = obj.get("option_type").or_else(|| obj.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| if s.to_lowercase().starts_with('c') { "C" } else { "P" })
            .unwrap_or("?");
        let exp = obj.get("expiration_date").or_else(|| obj.get("expiration"))
            .and_then(|v| v.as_str())
            .map(|s| {
                let p: Vec<&str> = s.split('-').collect();
                if p.len() == 3 { format!("{}/{}", p[1].trim_start_matches('0'), p[2].trim_start_matches('0')) }
                else { s.to_string() }
            });
        let detail = match (strike, exp) {
            (Some(k), Some(e)) => format!("{k:.0}{opt_type} {e}"),
            _ => String::new(),
        };
        Some(crate::events::PositionSummary {
            symbol, detail,
            qty: format!("{qty:.0}"),
            avg: format!("${avg:.2}"),
            current: format!("${cur:.2}"),
            pnl: format!("{}{:.2}", if is_gain { "+" } else { "-" }, pnl.abs()),
            is_gain,
        })
    } else {
        let avg = lv_f64(obj, &["average_buy_price", "avg_cost", "average_price"]).unwrap_or(0.0);
        let cur = lv_f64(obj, &["last_trade_price", "price", "mark_price", "last_price"]).unwrap_or(avg);
        let pnl = (cur - avg) * qty;
        let is_gain = pnl >= 0.0;
        Some(crate::events::PositionSummary {
            symbol, detail: String::new(),
            qty: format!("{qty:.0}"),
            avg: format!("${avg:.2}"),
            current: format!("${cur:.2}"),
            pnl: format!("{}{:.2}", if is_gain { "+" } else { "-" }, pnl.abs()),
            is_gain,
        })
    }
}

fn account_prompt_instruction(account: &AccountTarget) -> &'static str {
    match account {
        AccountTarget::Agentic =>
            "ACCOUNT: Use the agentic trading account for all portfolio queries and order \
             placement. Do NOT access or mention the individual investing account — it must \
             never be used for trading.",
        AccountTarget::Machine =>
            "ACCOUNT: Use the Robinhood account named 'Machine' for all portfolio queries \
             and order placement. Call get_accounts to locate it by name and retrieve its \
             account ID, then use that ID exclusively. This account is approved for options \
             trading — spreads, puts, calls, and defined-risk strategies are all permitted. \
             Do NOT access the individual investing account or the agentic account.",
    }
}

/// Pretty-print JSON for terminal logs.
fn indent_json(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                return pretty;
            }
        }
    }
    s.to_string()
}

/// Convert JSON to human-readable plain text for the UI log.
/// Strips all braces/brackets and renders key: value pairs.
fn humanize_json(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            return render_value(&v, 0);
        }
    }
    s.to_string()
}

fn render_value(v: &serde_json::Value, depth: usize) -> String {
    let pad = "  ".repeat(depth);
    match v {
        serde_json::Value::Object(map) => {
            if map.is_empty() { return format!("{pad}(empty)"); }
            map.iter().map(|(k, val)| {
                let label = k.replace('_', " ");
                match val {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        format!("{pad}{label}:\n{}", render_value(val, depth + 1))
                    }
                    _ => format!("{pad}{label}: {}", render_leaf(val)),
                }
            }).collect::<Vec<_>>().join("\n")
        }
        serde_json::Value::Array(arr) => {
            if arr.is_empty() { return format!("{pad}(none)"); }
            arr.iter().map(|item| match item {
                serde_json::Value::Object(_) => {
                    format!("{pad}▸\n{}", render_value(item, depth + 1))
                }
                _ => format!("{pad}• {}", render_leaf(item)),
            }).collect::<Vec<_>>().join("\n")
        }
        _ => format!("{pad}{}", render_leaf(v)),
    }
}

fn render_leaf(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null      => "—".to_string(),
        serde_json::Value::Bool(b)   => if *b { "yes" } else { "no" }.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other                        => other.to_string(),
    }
}

fn truncate(s: String, max: usize) -> String {
    if s.len() <= max { s } else { format!("{}…", safe_truncate(&s, max)) }
}

/// Find the last valid UTF-8 char boundary at or before `max_bytes`.
fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes { return s; }
    let mut b = max_bytes;
    while b > 0 && !s.is_char_boundary(b) { b -= 1; }
    &s[..b]
}
