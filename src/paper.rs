use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::events::{PortfolioSnapshot, TradeSnapshot};

// ---------------------------------------------------------------------------
// Portfolio state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPosition {
    pub symbol: String,
    /// Positive = long, negative = short
    pub quantity: f64,
    pub avg_cost: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionsPosition {
    pub symbol: String,
    pub expiration: String,
    pub strike: f64,
    /// "call" or "put"
    pub option_type: String,
    /// Positive = long, negative = short (written)
    pub contracts: f64,
    /// Premium per share (contract = 100 shares)
    pub avg_cost: f64,
}

impl OptionsPosition {
    pub fn key(symbol: &str, expiration: &str, strike: f64, option_type: &str) -> String {
        format!("{symbol}_{expiration}_{strike:.2}_{option_type}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilledOrder {
    pub timestamp_unix: u64,
    pub asset_type: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub fill_price: f64,
    pub total_value: f64,
    pub expiration: Option<String>,
    pub strike: Option<f64>,
    pub option_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPortfolio {
    pub starting_cash: f64,
    pub cash: f64,
    pub equity: HashMap<String, EquityPosition>,
    pub options: HashMap<String, OptionsPosition>,
    pub orders: Vec<FilledOrder>,
    /// Best-effort price cache — populated from read tool responses.
    pub price_cache: HashMap<String, f64>,
    /// True once we've synced starting_cash from the real agentic account.
    /// Prevents overwriting with a stale env-var default on subsequent cycles.
    #[serde(default)]
    pub real_synced: bool,
}

impl PaperPortfolio {
    pub fn new(starting_cash: f64) -> Self {
        Self {
            starting_cash,
            cash: starting_cash,
            equity: HashMap::new(),
            options: HashMap::new(),
            orders: Vec::new(),
            price_cache: HashMap::new(),
            real_synced: false,
        }
    }

    /// Seed baseline from the real account — total value only, no position detail.
    /// Used during regular paper-trading cycles. Only runs once per portfolio file.
    pub fn sync_from_real(&mut self, cash: f64, equity_value: f64, options_value: f64) {
        if self.real_synced { return; }
        let total = cash + equity_value + options_value;
        info!(
            "Syncing paper portfolio baseline from real account: \
             cash ${cash:.2}, equity ${equity_value:.2}, options ${options_value:.2} → total ${total:.2}"
        );
        self.starting_cash = total;
        self.cash = total;
        self.real_synced = true;
    }

    /// Fully seed from a real account snapshot, preserving individual positions.
    /// Used for backtest initialisation. Always overwrites — no idempotency guard.
    pub fn seed_from_real_state(
        &mut self,
        cash: f64,
        equity_positions: Vec<EquityPosition>,
        options_positions: Vec<OptionsPosition>,
    ) {
        let equity_value: f64 = equity_positions.iter()
            .map(|p| p.quantity * p.avg_cost)
            .sum();
        let options_value: f64 = options_positions.iter()
            .map(|p| p.contracts * p.avg_cost * 100.0)
            .sum();
        let total = cash + equity_value + options_value;
        info!(
            "Seeding backtest portfolio from real account: \
             cash ${cash:.2}, {} equity pos, {} options pos → total ${total:.2}",
            equity_positions.len(), options_positions.len()
        );
        self.starting_cash = total;
        self.cash = cash;
        self.equity = equity_positions.into_iter()
            .map(|p| (p.symbol.clone(), p))
            .collect();
        self.options = options_positions.into_iter()
            .map(|p| {
                let key = OptionsPosition::key(&p.symbol, &p.expiration, p.strike, &p.option_type);
                (key, p)
            })
            .collect();
        self.real_synced = true;
    }

    pub fn load_or_new(path: &Path, starting_cash: f64) -> Self {
        if path.exists() {
            if let Ok(raw) = std::fs::read_to_string(path) {
                if let Ok(p) = serde_json::from_str::<PaperPortfolio>(&raw) {
                    info!(
                        "Loaded paper portfolio from {} (cash ${:.2}, {} equity positions, {} options positions)",
                        path.display(),
                        p.cash,
                        p.equity.len(),
                        p.options.len()
                    );
                    return p;
                }
            }
            warn!("Could not parse paper portfolio at {} — starting fresh", path.display());
        }
        info!("New paper portfolio: ${starting_cash:.2} starting cash");
        Self::new(starting_cash)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn market_value_of_equity(&self) -> f64 {
        self.equity.values().map(|p| {
            let price = self.price_cache.get(&p.symbol).copied().unwrap_or(p.avg_cost);
            p.quantity * price
        }).sum()
    }

    fn market_value_of_options(&self) -> f64 {
        self.options.values().map(|p| {
            let key = OptionsPosition::key(&p.symbol, &p.expiration, p.strike, &p.option_type);
            let price = self.price_cache.get(&key).copied().unwrap_or(p.avg_cost);
            p.contracts * price * 100.0
        }).sum()
    }

    pub fn total_value(&self) -> f64 {
        self.cash + self.market_value_of_equity() + self.market_value_of_options()
    }

    pub fn pnl(&self) -> f64 {
        self.total_value() - self.starting_cash
    }

    pub fn snapshot(&self) -> PortfolioSnapshot {
        let equity_value = self.market_value_of_equity();
        let options_value = self.market_value_of_options();
        let total_value = self.cash + equity_value + options_value;
        let pnl = total_value - self.starting_cash;
        let pnl_pct = if self.starting_cash != 0.0 {
            pnl / self.starting_cash * 100.0
        } else {
            0.0
        };
        PortfolioSnapshot { cash: self.cash, equity_value, options_value, total_value, pnl, pnl_pct }
    }

    pub fn position_summaries(&self) -> Vec<crate::events::PositionSummary> {
        use crate::events::PositionSummary;
        let mut out = Vec::new();

        let mut equity: Vec<_> = self.equity.values().collect();
        equity.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        for pos in equity {
            let price = self.price_cache.get(&pos.symbol).copied().unwrap_or(pos.avg_cost);
            let pnl = pos.quantity * (price - pos.avg_cost);
            let sign = if pnl >= 0.0 { "+" } else { "" };
            out.push(PositionSummary {
                symbol: pos.symbol.clone(),
                detail: String::new(),
                qty: format!("{:.0}", pos.quantity.abs()),
                avg: format!("${:.2}", pos.avg_cost),
                current: format!("${:.2}", price),
                pnl: format!("{sign}${:.2}", pnl),
                is_gain: pnl >= 0.0,
            });
        }

        let mut options: Vec<_> = self.options.values().collect();
        options.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        for pos in options {
            let key = OptionsPosition::key(&pos.symbol, &pos.expiration, pos.strike, &pos.option_type);
            let price = self.price_cache.get(&key).copied().unwrap_or(pos.avg_cost);
            let pnl = pos.contracts * (price - pos.avg_cost) * 100.0;
            let sign = if pnl >= 0.0 { "+" } else { "" };
            let parts: Vec<&str> = pos.expiration.splitn(3, '-').collect();
            let exp_short = if parts.len() == 3 {
                format!("{}/{}", parts[1].trim_start_matches('0'), parts[2].trim_start_matches('0'))
            } else {
                pos.expiration.clone()
            };
            let c = pos.option_type.chars().next().unwrap_or('?').to_uppercase().to_string();
            out.push(PositionSummary {
                symbol: pos.symbol.clone(),
                detail: format!("{:.0}{c} {exp_short}", pos.strike),
                qty: format!("{:.0}c", pos.contracts.abs()),
                avg: format!("${:.2}", pos.avg_cost),
                current: format!("${:.2}", price),
                pnl: format!("{sign}${:.2}", pnl),
                is_gain: pnl >= 0.0,
            });
        }
        out
    }

    pub fn summary(&self) -> String {
        let total = self.total_value();
        let pnl = self.pnl();
        let pnl_pct = pnl / self.starting_cash * 100.0;
        let pnl_sign = if pnl >= 0.0 { "+" } else { "" };

        let mut lines = vec![
            "=== Paper Portfolio ===".to_string(),
            format!("Cash:         ${:.2}", self.cash),
            format!("Equity mkt:   ${:.2}", self.market_value_of_equity()),
            format!("Options mkt:  ${:.2}", self.market_value_of_options()),
            format!("Total:        ${:.2}", total),
            format!("P&L:          {pnl_sign}${:.2} ({pnl_sign}{:.2}%)", pnl, pnl_pct),
        ];

        if !self.equity.is_empty() {
            lines.push(String::new());
            lines.push("Equities:".to_string());
            for (sym, pos) in &self.equity {
                let mkt = self.price_cache.get(sym).copied().unwrap_or(pos.avg_cost);
                let unreal = pos.quantity * (mkt - pos.avg_cost);
                let sign = if unreal >= 0.0 { "+" } else { "" };
                let direction = if pos.quantity >= 0.0 { "long" } else { "short" };
                lines.push(format!(
                    "  {sym}: {direction} {:.0} @ ${:.2} avg | mkt ${:.2} | unrealized {sign}${:.2}",
                    pos.quantity.abs(), pos.avg_cost, mkt, unreal
                ));
            }
        }

        if !self.options.is_empty() {
            lines.push(String::new());
            lines.push("Options:".to_string());
            for pos in self.options.values() {
                let key = OptionsPosition::key(&pos.symbol, &pos.expiration, pos.strike, &pos.option_type);
                let mkt = self.price_cache.get(&key).copied().unwrap_or(pos.avg_cost);
                let unreal = pos.contracts * (mkt - pos.avg_cost) * 100.0;
                let sign = if unreal >= 0.0 { "+" } else { "" };
                let direction = if pos.contracts >= 0.0 { "long" } else { "short" };
                lines.push(format!(
                    "  {} {direction} {:.0}c  ${:.2}{} exp {} | avg ${:.2} | mkt ${:.2} | {sign}${:.2}",
                    pos.symbol,
                    pos.contracts.abs(),
                    pos.strike,
                    pos.option_type.chars().next().unwrap_or('?').to_uppercase(),
                    pos.expiration,
                    pos.avg_cost,
                    mkt,
                    unreal
                ));
            }
        }

        if !self.orders.is_empty() {
            lines.push(String::new());
            lines.push(format!("Orders filled this session: {}", self.orders.len()));
        }

        lines.join("\n")
    }
}

// ---------------------------------------------------------------------------
// PaperExchange — intercepts write tool calls and simulates fills
// ---------------------------------------------------------------------------

pub struct PaperExchange {
    pub portfolio: PaperPortfolio,
    path: PathBuf,
}

impl PaperExchange {
    pub fn new(portfolio: PaperPortfolio, path: PathBuf) -> Self {
        Self { portfolio, path }
    }

    // -----------------------------------------------------------------------
    // Price ingestion
    // -----------------------------------------------------------------------

    /// Walk a JSON blob returned by any read tool and cache any prices found.
    pub fn ingest_prices(&mut self, raw: &str) {
        if let Ok(v) = serde_json::from_str::<Value>(raw) {
            self.walk_json_for_prices(&v);
        }
    }

    fn walk_json_for_prices(&mut self, v: &Value) {
        match v {
            Value::Object(map) => {
                let symbol = map.get("symbol")
                    .or_else(|| map.get("ticker"))
                    .or_else(|| map.get("underlying_symbol"))
                    .and_then(|s| s.as_str())
                    .map(str::to_uppercase);

                if let Some(sym) = symbol {
                    // Equity price
                    let price = find_f64(map, &["price", "last_price", "mark_price", "last", "close", "ask_price", "bid_price"]);
                    if let Some(p) = price {
                        self.portfolio.price_cache.insert(sym.clone(), p);
                    }

                    // Options mark price — key by contract identifiers when present
                    if let (Some(exp), Some(strike), Some(opt_type)) = (
                        map.get("expiration_date").and_then(|v| v.as_str()),
                        map.get("strike_price").and_then(|v| v.as_f64()),
                        map.get("option_type")
                            .or_else(|| map.get("type"))
                            .and_then(|v| v.as_str()),
                    ) {
                        let mark = find_f64(map, &["mark_price", "price", "last_price", "last"]);
                        if let Some(m) = mark {
                            let key = OptionsPosition::key(&sym, exp, strike, &opt_type.to_lowercase());
                            self.portfolio.price_cache.insert(key, m);
                        }
                    }
                }

                for val in map.values() {
                    self.walk_json_for_prices(val);
                }
            }
            Value::Array(arr) => {
                for val in arr {
                    self.walk_json_for_prices(val);
                }
            }
            _ => {}
        }
    }

    // -----------------------------------------------------------------------
    // Order simulation
    // -----------------------------------------------------------------------

    /// Dispatch to equity or options simulator based on tool name / args.
    pub fn simulate_order(&mut self, tool_name: &str, args: &Value) -> Result<String> {
        let is_options = tool_name.to_lowercase().contains("option")
            || args.get("expiration_date").is_some()
            || args.get("expiration").is_some()
            || args.get("strike_price").is_some()
            || args.get("strike").is_some();

        let result = if is_options {
            self.fill_options(args)?
        } else {
            self.fill_equity(args)?
        };

        self.save();
        Ok(result)
    }

    fn fill_equity(&mut self, args: &Value) -> Result<String> {
        let symbol = str_field(args, &["symbol", "ticker"])
            .ok_or_else(|| anyhow::anyhow!("missing symbol"))?
            .to_uppercase();

        let side = str_field(args, &["side"])
            .ok_or_else(|| anyhow::anyhow!("missing side"))?
            .to_lowercase();

        let qty = num_field(args, &["quantity", "qty", "shares"])
            .ok_or_else(|| anyhow::anyhow!("missing quantity"))?;

        let fill = num_field(args, &["price", "limit_price"])
            .or_else(|| self.portfolio.price_cache.get(&symbol).copied())
            .ok_or_else(|| anyhow::anyhow!(
                "No price cached for {symbol}. Have Claude call a quote tool first, \
                 or provide a limit_price in the order."
            ))?;

        let total = qty * fill;

        match side.as_str() {
            "buy" => {
                if self.portfolio.cash < total {
                    bail!("Insufficient cash: need ${total:.2}, have ${:.2}", self.portfolio.cash);
                }
                self.portfolio.cash -= total;
                let pos = self.portfolio.equity
                    .entry(symbol.clone())
                    .or_insert(EquityPosition { symbol: symbol.clone(), quantity: 0.0, avg_cost: fill });
                let prev_basis = pos.quantity * pos.avg_cost;
                pos.quantity += qty;
                if pos.quantity != 0.0 {
                    pos.avg_cost = (prev_basis + qty * fill) / pos.quantity;
                }
            }
            "sell" => {
                let pos = self.portfolio.equity
                    .entry(symbol.clone())
                    .or_insert(EquityPosition { symbol: symbol.clone(), quantity: 0.0, avg_cost: fill });
                let prev_basis = pos.quantity * pos.avg_cost;
                pos.quantity -= qty;
                // Allow going short; don't require an existing position
                if pos.quantity != 0.0 {
                    pos.avg_cost = (prev_basis - qty * fill) / pos.quantity;
                }
                self.portfolio.cash += total;
                if pos.quantity == 0.0 {
                    self.portfolio.equity.remove(&symbol);
                }
            }
            other => bail!("Unknown side '{other}' — expected 'buy' or 'sell'"),
        }

        self.portfolio.price_cache.insert(symbol.clone(), fill);
        self.record_order("equity", &symbol, &side, qty, fill, total, None, None, None);

        Ok(format!(
            "PAPER FILL — {side} {qty:.0} {symbol} @ ${fill:.2} (${total:.2} total)\n\
             Cash remaining: ${:.2}\n\n{}",
            self.portfolio.cash,
            self.portfolio.summary()
        ))
    }

    fn fill_options(&mut self, args: &Value) -> Result<String> {
        let symbol = str_field(args, &["symbol", "ticker", "underlying_symbol"])
            .ok_or_else(|| anyhow::anyhow!("missing symbol"))?
            .to_uppercase();

        let expiration = str_field(args, &["expiration_date", "expiration", "expiry"])
            .ok_or_else(|| anyhow::anyhow!("missing expiration_date"))?
            .to_string();

        let strike = num_field(args, &["strike_price", "strike"])
            .ok_or_else(|| anyhow::anyhow!("missing strike_price"))?;

        let option_type = str_field(args, &["option_type", "type", "call_or_put", "put_call"])
            .ok_or_else(|| anyhow::anyhow!("missing option_type (call/put)"))?
            .to_lowercase();

        let contracts = num_field(args, &["quantity", "contracts", "qty"])
            .ok_or_else(|| anyhow::anyhow!("missing quantity (number of contracts)"))?;

        let side = str_field(args, &["side"])
            .ok_or_else(|| anyhow::anyhow!("missing side (buy/sell)"))?
            .to_lowercase();

        let key = OptionsPosition::key(&symbol, &expiration, strike, &option_type);

        let premium = num_field(args, &["price", "premium", "limit_price"])
            .or_else(|| self.portfolio.price_cache.get(&key).copied())
            .ok_or_else(|| anyhow::anyhow!(
                "No premium cached for {symbol} {strike} {option_type} {expiration}. \
                 Have Claude get an options quote first."
            ))?;

        // Each contract = 100 shares
        let total = contracts * premium * 100.0;

        match side.as_str() {
            "buy" => {
                if self.portfolio.cash < total {
                    bail!(
                        "Insufficient cash: need ${total:.2} ({contracts:.0}c × ${premium:.2} × 100), \
                         have ${:.2}",
                        self.portfolio.cash
                    );
                }
                self.portfolio.cash -= total;
                let pos = self.portfolio.options
                    .entry(key.clone())
                    .or_insert(OptionsPosition {
                        symbol: symbol.clone(), expiration: expiration.clone(),
                        strike, option_type: option_type.clone(), contracts: 0.0, avg_cost: premium,
                    });
                let prev = pos.contracts * pos.avg_cost;
                pos.contracts += contracts;
                if pos.contracts != 0.0 {
                    pos.avg_cost = (prev + contracts * premium) / pos.contracts;
                }
            }
            "sell" => {
                // Selling to open (writing) increases cash; no margin modeled
                self.portfolio.cash += total;
                let pos = self.portfolio.options
                    .entry(key.clone())
                    .or_insert(OptionsPosition {
                        symbol: symbol.clone(), expiration: expiration.clone(),
                        strike, option_type: option_type.clone(), contracts: 0.0, avg_cost: premium,
                    });
                let prev = pos.contracts * pos.avg_cost;
                pos.contracts -= contracts;
                if pos.contracts != 0.0 {
                    pos.avg_cost = (prev - contracts * premium) / pos.contracts;
                }
                if pos.contracts == 0.0 {
                    self.portfolio.options.remove(&key);
                }
            }
            other => bail!("Unknown side '{other}' — expected 'buy' or 'sell'"),
        }

        self.portfolio.price_cache.insert(key, premium);
        self.record_order(
            "options", &symbol, &side, contracts, premium, total,
            Some(expiration.clone()), Some(strike), Some(option_type.clone()),
        );

        let opt_label = format!("{strike:.2}{}", option_type.chars().next().unwrap_or('?').to_uppercase());
        Ok(format!(
            "PAPER FILL — {side} {contracts:.0}c {symbol} {opt_label} exp {expiration} \
             @ ${premium:.2}/share (${total:.2} total)\n\
             Cash remaining: ${:.2}\n\n{}",
            self.portfolio.cash,
            self.portfolio.summary()
        ))
    }

    fn record_order(
        &mut self,
        asset_type: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        fill_price: f64,
        total_value: f64,
        expiration: Option<String>,
        strike: Option<f64>,
        option_type: Option<String>,
    ) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.portfolio.orders.push(FilledOrder {
            timestamp_unix: ts,
            asset_type: asset_type.to_string(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            quantity,
            fill_price,
            total_value,
            expiration,
            strike,
            option_type,
        });
    }

    /// Return a `TradeSnapshot` for the most recently filled order, if any.
    pub fn last_trade_snapshot(&self) -> Option<TradeSnapshot> {
        let o = self.portfolio.orders.last()?;
        Some(TradeSnapshot {
            side: o.side.clone(),
            symbol: o.symbol.clone(),
            quantity: o.quantity,
            price: o.fill_price,
            total: o.total_value,
            is_buy: o.side == "buy",
        })
    }

    fn save(&self) {
        if let Err(e) = self.portfolio.save(&self.path) {
            warn!("Failed to persist paper portfolio: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// JSON extraction helpers
// ---------------------------------------------------------------------------

fn str_field<'a>(v: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| v.get(k)?.as_str())
}

fn num_field(v: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| {
        let field = v.get(k)?;
        field.as_f64().or_else(|| field.as_str()?.parse().ok())
    })
}

fn find_f64(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| {
        let v = map.get(*k)?;
        v.as_f64().or_else(|| v.as_str()?.parse().ok())
    })
}
