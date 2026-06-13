use std::rc::Rc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use anyhow::Result;
use slint::{Model, ModelRc, VecModel};

use crate::bot::Bot;
use crate::config::{AccountTarget, Config, TradingMode};
use crate::events::{self, BotEvent};

slint::include_modules!();

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn launch(config: Config) -> Result<()> {
    let window = MainWindow::new()?;

    window.set_mode_idx(match config.mode {
        TradingMode::Paper => 0,
        TradingMode::Live => 1,
    });
    window.set_account_idx(match config.account {
        AccountTarget::Agentic => 0,
        AccountTarget::Machine => 1,
    });

    let (tx, rx) = events::channel();

    // Background portfolio poller — direct MCP calls, no token spend.
    // Runs independently of bot cycles; live mode only (paper tracks state locally).
    let poller_stop = Arc::new(AtomicBool::new(false));
    crate::poller::launch(config.clone(), tx.clone(), poller_stop.clone());

    // Steer message: set from UI, consumed at the start of each bot cycle
    let steer: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let steer_cb = steer.clone();
    window.on_steer(move |msg: slint::SharedString| {
        let msg = msg.to_string();
        if !msg.is_empty() {
            *steer_cb.lock().unwrap() = Some(msg);
        }
    });

    // Tickers: shared set of selected symbols passed into each bot cycle
    let tickers_arc: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // Ticker model displayed in the UI
    let tickers_model: Rc<VecModel<TickerEntry>> = Rc::new(VecModel::default());
    for sym in ["SPY", "XSP", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN"] {
        tickers_model.push(TickerEntry { symbol: sym.into(), selected: false });
    }
    window.set_tickers(ModelRc::from(tickers_model.clone()));

    // Toggle an existing ticker chip
    let model_tog = tickers_model.clone();
    let tickers_tog = tickers_arc.clone();
    window.on_toggle_ticker(move |symbol: slint::SharedString| {
        let sym = symbol.to_string();
        for i in 0..model_tog.row_count() {
            let entry = model_tog.row_data(i).unwrap();
            if entry.symbol.as_str() == sym.as_str() {
                let now_selected = !entry.selected;
                model_tog.set_row_data(i, TickerEntry { symbol: entry.symbol, selected: now_selected });
                let mut t = tickers_tog.lock().unwrap();
                if now_selected { if !t.contains(&sym) { t.push(sym.clone()); } }
                else            { t.retain(|s| s != &sym); }
                break;
            }
        }
    });

    // Add a custom ticker (always selected when added)
    let model_add = tickers_model.clone();
    let tickers_add = tickers_arc.clone();
    window.on_add_ticker(move |symbol: slint::SharedString| {
        let sym = symbol.to_string().trim().to_uppercase();
        if sym.is_empty() { return; }
        for i in 0..model_add.row_count() {
            let entry = model_add.row_data(i).unwrap();
            if entry.symbol.as_str() == sym.as_str() {
                // Already present — just select it
                model_add.set_row_data(i, TickerEntry { symbol: entry.symbol, selected: true });
                let mut t = tickers_add.lock().unwrap();
                if !t.contains(&sym) { t.push(sym); }
                return;
            }
        }
        model_add.push(TickerEntry { symbol: sym.as_str().into(), selected: true });
        tickers_add.lock().unwrap().push(sym);
    });

    // Data models
    let trades_model: Rc<VecModel<TradeEntry>> = Rc::new(VecModel::default());
    let log_model: Rc<VecModel<LogEntry>> = Rc::new(VecModel::default());
    let positions_model: Rc<VecModel<PositionEntry>> = Rc::new(VecModel::default());
    window.set_trades(ModelRc::from(trades_model.clone()));
    window.set_log_entries(ModelRc::from(log_model.clone()));
    window.set_positions(ModelRc::from(positions_model.clone()));

    let stop_flag: Rc<std::cell::Cell<Option<Arc<AtomicBool>>>> = Rc::new(std::cell::Cell::new(None));

    // ── Timer: drain the event channel 10× per second ─────────────────────
    let win_weak = window.as_weak();
    let trades_timer = trades_model.clone();
    let log_timer = log_model.clone();
    let positions_timer = positions_model.clone();
    let timer = slint::Timer::default();

    // Chart history accumulates for the lifetime of this UI session.
    let mut chart_values: Vec<f64> = Vec::new();

    timer.start(
        slint::TimerMode::Repeated,
        Duration::from_millis(100),
        move || {
            let Some(win) = win_weak.upgrade() else { return };
            while let Ok(event) = rx.try_recv() {
                match event {
                    BotEvent::Status(s)   => win.set_status_text(s.into()),
                    BotEvent::CycleStart  => {
                        win.set_running(true);
                        chart_values.clear();
                        win.set_backtest_result("".into());
                    }
                    BotEvent::CycleEnd    => win.set_running(false),
                    BotEvent::Portfolio(snap) => {
                        apply_portfolio(&win, &snap);
                        chart_values.push(snap.total_value);
                        update_chart(&win, &chart_values, snap.pnl, snap.pnl_pct);
                    }

                    BotEvent::Trade(t) => {
                        win.set_trade_is_buy(t.is_buy);
                        win.set_trade_flash(true);
                        trades_timer.insert(0, TradeEntry {
                            side:     t.side.to_uppercase().as_str().into(),
                            symbol:   t.symbol.as_str().into(),
                            quantity: format!("{:.0}", t.quantity).into(),
                            price:    format!("${:.2}", t.price).into(),
                            total:    format!("${:.2}", t.total).into(),
                            is_buy:   t.is_buy,
                        });
                    }

                    BotEvent::ToolCall { name, preview } => {
                        log_timer.insert(0, LogEntry {
                            kind: "tool".into(),
                            text: format!("→ {name}  {preview}").into(),
                        });
                        win.set_tool_active(true);
                    }

                    BotEvent::ToolResult { preview } => {
                        log_timer.insert(0, LogEntry {
                            kind: "result".into(),
                            text: preview.as_str().into(),
                        });
                        win.set_tool_active(false);
                    }

                    BotEvent::Analysis(text) => {
                        log_timer.insert(0, LogEntry {
                            kind: "analysis".into(),
                            text: text.as_str().into(),
                        });
                    }

                    BotEvent::Error(e) => {
                        log_timer.insert(0, LogEntry {
                            kind: "error".into(),
                            text: format!("Error: {e}").into(),
                        });
                    }

                    BotEvent::Positions(new_positions) => {
                        // Replace the positions list in-place.
                        let n = positions_timer.row_count();
                        for _ in 0..n { positions_timer.remove(0); }
                        for p in new_positions {
                            positions_timer.push(PositionEntry {
                                symbol:  p.symbol.into(),
                                detail:  p.detail.into(),
                                qty:     p.qty.into(),
                                avg:     p.avg.into(),
                                current: p.current.into(),
                                pnl:     p.pnl.into(),
                                is_gain: p.is_gain,
                            });
                        }
                    }

                    BotEvent::BacktestComplete { pnl, pnl_pct, total } => {
                        win.set_backtest_result(
                            format!("Final  {total}\nP&L  {pnl}  ({pnl_pct})").into()
                        );
                    }

                    BotEvent::Usage(u) => {
                        win.set_usage_runs(format!("{}", u.run_count).into());
                        win.set_usage_session_cost(format!("${:.4}", u.cost_session).into());
                        let per_run = if u.run_count > 0 { u.cost_session / u.run_count as f64 } else { 0.0 };
                        win.set_usage_per_run(format!("${:.4}", per_run).into());
                        let runs_left = if let Some(budget) = u.budget_usd {
                            let remaining = (budget - u.cost_session).max(0.0);
                            if per_run > 0.0 {
                                format!("~{:.0}", remaining / per_run)
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        };
                        win.set_usage_runs_left(runs_left.into());
                        win.set_usage_tokens(
                            format!("{}k in / {}k out",
                                (u.input_tokens + 500) / 1000,
                                (u.output_tokens + 500) / 1000,
                            ).into()
                        );
                    }
                }
            }
        },
    );

    // ── "Run Once" button ──────────────────────────────────────────────────
    let tx_once   = tx.clone();
    let cfg_once  = Arc::new(config.clone());
    let win_once  = window.as_weak();
    let steer_once   = steer.clone();
    let tickers_once = tickers_arc.clone();
    window.on_run_once(move || {
        let tx = tx_once.clone();
        let mut config = (*cfg_once).clone();
        if let Some(win) = win_once.upgrade() {
            config.mode    = idx_to_mode(win.get_mode_idx());
            config.account = idx_to_account(win.get_account_idx());
        }
        let steer   = steer_once.clone();
        let tickers = tickers_once.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            rt.block_on(async move {
                let mut bot = Bot::new_with_sender(config, Some(tx.clone()));
                bot.steer   = steer;
                bot.tickers = tickers;
                if let Err(e) = bot.run_once().await {
                    let _ = tx.send(BotEvent::Error(e.to_string()));
                    let _ = tx.send(BotEvent::CycleEnd);
                    let _ = tx.send(BotEvent::Status("Idle".to_string()));
                }
            });
        });
    });

    // ── "Start Loop / Stop" button ─────────────────────────────────────────
    let tx_loop      = tx.clone();
    let cfg_loop     = Arc::new(config.clone());
    let stop_flag_cb = stop_flag.clone();
    let win_loop     = window.as_weak();
    let steer_loop   = steer.clone();
    let tickers_loop = tickers_arc.clone();

    window.on_toggle_running(move || {
        if let Some(flag) = stop_flag_cb.take() {
            flag.store(true, Ordering::Relaxed);
            return;
        }

        let stop = Arc::new(AtomicBool::new(false));
        stop_flag_cb.set(Some(stop.clone()));

        let tx = tx_loop.clone();
        let mut config = (*cfg_loop).clone();
        if let Some(win) = win_loop.upgrade() {
            config.mode    = idx_to_mode(win.get_mode_idx());
            config.account = idx_to_account(win.get_account_idx());
        }
        let steer   = steer_loop.clone();
        let tickers = tickers_loop.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            rt.block_on(async move {
                let mut bot = Bot::new_with_sender(config, Some(tx.clone()));
                bot.steer   = steer;
                bot.tickers = tickers;
                if let Err(e) = bot.run_loop_cancellable(stop).await {
                    let _ = tx.send(BotEvent::Error(e.to_string()));
                }
                let _ = tx.send(BotEvent::CycleEnd);
                let _ = tx.send(BotEvent::Status("Idle".to_string()));
            });
        });
    });

    // ── "Backtest" button ──────────────────────────────────────────────────
    let tx_bt      = tx.clone();
    let cfg_bt     = Arc::new(config.clone());
    let win_bt     = window.as_weak();
    let steer_bt   = steer.clone();
    let tickers_bt = tickers_arc.clone();

    window.on_run_backtest(move || {
        let period = match win_bt.upgrade().map(|w| w.get_backtest_period_idx()) {
            Some(0) => "1 day",
            Some(2) => "1 month",
            Some(3) => "1 year",
            _       => "1 week",
        };

        let tx     = tx_bt.clone();
        let mut config = (*cfg_bt).clone();
        if let Some(win) = win_bt.upgrade() {
            config.account = idx_to_account(win.get_account_idx());
        }
        let steer   = steer_bt.clone();
        let tickers = tickers_bt.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            rt.block_on(async move {
                let mut bot = Bot::new_with_sender(config, Some(tx.clone()));
                bot.steer   = steer;
                bot.tickers = tickers;
                let stop = Arc::new(AtomicBool::new(false));
                if let Err(e) = bot.run_backtest(period, stop).await {
                    let _ = tx.send(BotEvent::Error(e.to_string()));
                }
                let _ = tx.send(BotEvent::CycleEnd);
                let _ = tx.send(BotEvent::Status("Idle".to_string()));
            });
        });
    });

    window.run()?;
    poller_stop.store(true, Ordering::Relaxed);
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn idx_to_mode(idx: i32) -> TradingMode {
    match idx { 1 => TradingMode::Live, _ => TradingMode::Paper }
}

fn idx_to_account(idx: i32) -> AccountTarget {
    match idx { 1 => AccountTarget::Machine, _ => AccountTarget::Agentic }
}

fn update_chart(win: &MainWindow, values: &[f64], pnl: f64, pnl_pct: f64) {
    if values.is_empty() { return; }

    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let current = *values.last().unwrap();

    // Build SVG paths in a 1000×100 viewbox with 2px top/bottom margin.
    let (line, area) = chart_paths(values, min, max);

    win.set_chart_line_path(line.into());
    win.set_chart_area_path(area.into());
    win.set_chart_positive(pnl >= 0.0);

    let sign = if pnl >= 0.0 { "+" } else { "" };
    win.set_chart_label(
        format!("${current:.2}\n{sign}${pnl:.2}\n({sign}{pnl_pct:.2}%)").into(),
    );
    if values.len() >= 2 {
        win.set_chart_high_label(format!("${max:.2}").into());
        win.set_chart_low_label(format!("${min:.2}").into());
    }
}

fn chart_paths(values: &[f64], min: f64, max: f64) -> (String, String) {
    if values.len() < 2 { return (String::new(), String::new()); }

    let range = if (max - min).abs() < 1e-9 { 1.0 } else { max - min };
    let n = values.len();

    // Map each value to (x, y) in the 1000×100 viewbox.
    // Leave 4% margin top/bottom so the line is never clipped.
    let pts: Vec<(f64, f64)> = values.iter().enumerate().map(|(i, &v)| {
        let x = i as f64 / (n - 1) as f64 * 1000.0;
        let y = 96.0 - (v - min) / range * 92.0;  // y=4 at max, y=96 at min
        (x, y)
    }).collect();

    let line: String = pts.iter().enumerate().map(|(i, (x, y))| {
        if i == 0 { format!("M {x:.1} {y:.1}") }
        else       { format!(" L {x:.1} {y:.1}") }
    }).collect();

    // Area path: trace the line, then close along the bottom edge.
    let area = {
        let mut s = format!("M {:.1} 100", pts[0].0);
        for (x, y) in &pts { s.push_str(&format!(" L {x:.1} {y:.1}")); }
        s.push_str(&format!(" L {:.1} 100 Z", pts.last().unwrap().0));
        s
    };

    (line, area)
}

fn apply_portfolio(win: &MainWindow, snap: &crate::events::PortfolioSnapshot) {
    win.set_cash(format!("${:.2}", snap.cash).into());
    win.set_equity_value(format!("${:.2}", snap.equity_value).into());
    win.set_options_value(format!("${:.2}", snap.options_value).into());
    win.set_total_value(format!("${:.2}", snap.total_value).into());

    let sign = if snap.pnl >= 0.0 { "+" } else { "" };
    win.set_pnl_text(format!("{sign}${:.2} ({sign}{:.2}%)", snap.pnl, snap.pnl_pct).into());
    win.set_pnl_positive(snap.pnl >= 0.0);
}
