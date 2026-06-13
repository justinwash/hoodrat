//! Background portfolio poller — calls the Robinhood MCP server directly
//! (no Claude, no token spend) to keep the UI position list live between cycles.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::config::{AccountTarget, Config};
use crate::events::{BotEvent, EventTx, PortfolioSnapshot, PositionSummary};

/// Spawn a dedicated OS thread with its own Tokio runtime for the poller.
/// No-op in Paper mode (paper exchange tracks state locally) or without a token.
pub fn launch(config: Config, tx: EventTx, stop: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("poller tokio runtime");
        rt.block_on(run(config, tx, stop));
    });
}

async fn run(config: Config, tx: EventTx, stop: Arc<AtomicBool>) {
    let token = std::env::var("ROBINHOOD_ACCESS_TOKEN").unwrap_or_default();
    if token.is_empty() {
        info!("[poller] No ROBINHOOD_ACCESS_TOKEN — portfolio polling disabled");
        return;
    }
    info!("[poller] Starting (account={:?})", config.account);

    let mcp_url = match config.account {
        AccountTarget::Agentic => std::env::var("ROBINHOOD_MCP_URL")
            .unwrap_or_else(|_| "https://agent.robinhood.com/mcp/trading".to_string()),
        AccountTarget::Machine => std::env::var("ROBINHOOD_MACHINE_MCP_URL")
            .unwrap_or_else(|_| "https://machine.robinhood.com/mcp/trading".to_string()),
    };

    let interval = Duration::from_secs(
        std::env::var("POLL_PORTFOLIO_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64),
    );

    let Ok(client) = Client::builder().timeout(Duration::from_secs(30)).build() else {
        warn!("[poller] Failed to build HTTP client");
        return;
    };

    // Brief delay so the UI finishes initialising before we push events.
    tokio::time::sleep(Duration::from_secs(5)).await;

    loop {
        if stop.load(Ordering::Relaxed) { break; }

        match poll_once(&client, &mcp_url, &token).await {
            Ok(Some((snap, positions))) => {
                info!("[poller] Portfolio: ${:.2} total, {} position(s)",
                    snap.total_value, positions.len());
                let _ = tx.send(BotEvent::Portfolio(snap));
                let _ = tx.send(BotEvent::Positions(positions));
            }
            Ok(None) => {}
            Err(e)   => warn!("[poller] {e:#}"),
        }

        tokio::time::sleep(interval).await;
    }
}

// ---------------------------------------------------------------------------
// Minimal MCP HTTP client (Streamable HTTP transport, MCP 2024-11-05)
// ---------------------------------------------------------------------------

struct McpSession {
    client: Client,
    url: String,
    token: String,
    session_id: Option<String>,
    next_id: u64,
}

impl McpSession {
    fn new(client: Client, url: String, token: String) -> Self {
        Self { client, url, token, session_id: None, next_id: 1 }
    }

    async fn initialize(&mut self) -> Result<()> {
        self.rpc("initialize", json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "hoodrat-poller", "version": "0.1.0" }
        })).await?;
        // Notification — no response expected; ignore errors
        let _ = self.post(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        })).await;
        Ok(())
    }

    async fn tools_list(&mut self) -> Result<Vec<String>> {
        let result = self.rpc("tools/list", json!({})).await?;
        Ok(result["tools"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|t| t["name"].as_str().map(str::to_string))
            .collect())
    }

    async fn tools_call(&mut self, name: &str) -> Result<String> {
        let result = self.rpc("tools/call", json!({
            "name": name,
            "arguments": {}
        })).await?;
        Ok(result["content"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|b| b["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"))
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let body = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let resp = self.post(body).await?;
        if let Some(err) = resp.get("error") {
            anyhow::bail!("MCP {method}: {err}");
        }
        Ok(resp["result"].clone())
    }

    async fn post(&mut self, body: Value) -> Result<Value> {
        let mut req = self.client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream");

        if let Some(sid) = &self.session_id {
            req = req.header("Mcp-Session-Id", sid.clone());
        }

        let resp = req.json(&body).send().await.context("HTTP POST")?;

        // Capture session ID if the server provides one
        if let Some(sid) = resp.headers().get("Mcp-Session-Id") {
            self.session_id = sid.to_str().ok().map(str::to_string);
        }

        if resp.status().as_u16() == 202 {
            return Ok(json!(null)); // Notification acknowledged
        }

        let ct = resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let text = resp.text().await.context("read response body")?;

        if ct.contains("text/event-stream") {
            extract_sse_result(&text).context("parse SSE")
        } else {
            serde_json::from_str(&text).context("parse JSON response")
        }
    }
}

fn extract_sse_result(text: &str) -> Result<Value> {
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if data.trim() == "[DONE]" { continue; }
            if let Ok(v) = serde_json::from_str::<Value>(data) {
                if v.get("result").is_some() || v.get("error").is_some() {
                    return Ok(v);
                }
            }
        }
    }
    anyhow::bail!("No JSON-RPC result in SSE stream")
}

// ---------------------------------------------------------------------------
// Portfolio polling
// ---------------------------------------------------------------------------

async fn poll_once(
    client: &Client,
    mcp_url: &str,
    token: &str,
) -> Result<Option<(PortfolioSnapshot, Vec<PositionSummary>)>> {
    info!("[poller] Polling {mcp_url}");
    let mut mcp = McpSession::new(client.clone(), mcp_url.to_string(), token.to_string());
    mcp.initialize().await.context("MCP initialize")?;
    info!("[poller] Initialized (session={:?})", mcp.session_id);

    let tools = mcp.tools_list().await.context("tools/list")?;
    if tools.is_empty() {
        anyhow::bail!("MCP returned no tools");
    }
    info!("[poller] Available tools: {}", tools.join(", "));

    // Match by keyword priority — more specific patterns first
    let port_tool = pick(&tools, &["get_portfolio", "portfolio", "account_summary", "account"]);
    let pos_tool  = pick(&tools, &["get_positions", "equity_positions", "stock_positions", "positions"]);
    let opt_tool  = pick(&tools, &["get_options_positions", "options_positions", "option_positions"]);

    let mut combined = String::new();
    for tool in [port_tool, pos_tool, opt_tool].into_iter().flatten() {
        match mcp.tools_call(tool).await {
            Ok(text) => { combined.push('\n'); combined.push_str(&text); }
            Err(e)   => warn!("[poller] {tool}: {e}"),
        }
    }

    if combined.trim().is_empty() {
        return Ok(None);
    }

    let snap      = parse_portfolio(&combined);
    let positions = parse_positions(&combined);

    if snap.total_value == 0.0 && positions.is_empty() {
        return Ok(None);
    }

    Ok(Some((snap, positions)))
}

fn pick<'a>(tools: &'a [String], patterns: &[&str]) -> Option<&'a str> {
    for pat in patterns {
        if let Some(t) = tools.iter().find(|t| t.to_lowercase().contains(pat)) {
            return Some(t.as_str());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Flexible response parsing
// ---------------------------------------------------------------------------

fn parse_portfolio(text: &str) -> PortfolioSnapshot {
    // The combined text may contain multiple JSON blobs — scan for the first
    // object that has portfolio-shaped keys.
    let mut best = PortfolioSnapshot { cash: 0.0, equity_value: 0.0, options_value: 0.0,
        total_value: 0.0, pnl: 0.0, pnl_pct: 0.0 };

    scan_json_objects(text, &mut |obj| {
        let cash    = pluck(obj, &["cash", "buying_power", "available_funds", "cash_balance"]);
        let equity  = pluck(obj, &["equity_value", "market_value", "equity", "stock_value"]);
        let options = pluck(obj, &["options_value", "options_market_value"]);
        let total   = pluck(obj, &["total_value", "total", "portfolio_value", "account_value"]);
        let pnl     = pluck(obj, &["unrealized_pnl", "pnl", "profit_loss", "total_return"]);

        if total.is_some() || (cash.is_some() && equity.is_some()) {
            let c = cash.unwrap_or(0.0);
            let e = equity.unwrap_or(0.0);
            let o = options.unwrap_or(0.0);
            let t = total.unwrap_or(c + e + o);
            let p = pnl.unwrap_or(0.0);
            let cost = t - p;
            best = PortfolioSnapshot {
                cash: c, equity_value: e, options_value: o, total_value: t,
                pnl: p, pnl_pct: if cost > 0.01 { p / cost * 100.0 } else { 0.0 },
            };
        }
    });

    best
}

fn parse_positions(text: &str) -> Vec<PositionSummary> {
    let mut out = Vec::new();

    scan_json_objects(text, &mut |obj| {
        let has_sym = obj.contains_key("symbol") || obj.contains_key("ticker")
            || obj.contains_key("underlying_symbol");
        let has_qty = obj.contains_key("quantity");
        if !has_sym || !has_qty { return; }

        if obj.contains_key("strike_price") || obj.contains_key("option_type")
            || obj.contains_key("expiration_date") {
            if let Some(p) = build_option_pos(obj) { out.push(p); }
        } else if obj.contains_key("average_buy_price") || obj.contains_key("avg_cost") {
            if let Some(p) = build_equity_pos(obj) { out.push(p); }
        }
    });

    out.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    out.dedup_by(|a, b| a.symbol == b.symbol && a.detail == b.detail);
    out
}

fn scan_json_objects(text: &str, f: &mut dyn FnMut(&serde_json::Map<String, Value>)) {
    // Find every JSON object (or array of objects) in arbitrary text.
    let mut i = 0;
    let bytes = text.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'{' || bytes[i] == b'[' {
            if let Ok(v) = serde_json::from_str::<Value>(&text[i..]) {
                visit_value(&v, f);
                // Advance past this blob (approximate — find matching bracket depth)
                i += 1;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
}

fn visit_value(v: &Value, f: &mut dyn FnMut(&serde_json::Map<String, Value>)) {
    match v {
        Value::Object(map) => {
            f(map);
            for val in map.values() { visit_value(val, f); }
        }
        Value::Array(arr) => {
            for val in arr { visit_value(val, f); }
        }
        _ => {}
    }
}

fn build_equity_pos(obj: &serde_json::Map<String, Value>) -> Option<PositionSummary> {
    let symbol = get_str(obj, &["symbol", "ticker"])?.to_uppercase();
    let qty    = get_f64(obj, &["quantity"])?;
    if qty == 0.0 { return None; }
    let avg    = get_f64(obj, &["average_buy_price", "avg_cost", "average_price"]).unwrap_or(0.0);
    let cur    = get_f64(obj, &["last_trade_price", "price", "mark_price", "last_price"]).unwrap_or(avg);
    let pnl    = (cur - avg) * qty;
    let is_gain = pnl >= 0.0;
    Some(PositionSummary {
        symbol, detail: String::new(),
        qty:     format!("{qty:.0}"),
        avg:     format!("${avg:.2}"),
        current: format!("${cur:.2}"),
        pnl:     format!("{}{:.2}", if is_gain { "+" } else { "-" }, pnl.abs()),
        is_gain,
    })
}

fn build_option_pos(obj: &serde_json::Map<String, Value>) -> Option<PositionSummary> {
    let symbol    = get_str(obj, &["symbol", "underlying_symbol", "ticker"])?.to_uppercase();
    let contracts = get_f64(obj, &["quantity", "contracts"])?;
    if contracts == 0.0 { return None; }
    let avg = get_f64(obj, &["average_open_price", "avg_cost", "average_price"]).unwrap_or(0.0);
    let cur = get_f64(obj, &["mark_price", "price", "last_trade_price"]).unwrap_or(avg);
    let pnl = (cur - avg) * contracts * 100.0;
    let is_gain = pnl >= 0.0;

    let strike   = get_f64(obj, &["strike_price", "strike"]);
    let opt_type = get_str(obj, &["option_type", "type"])
        .map(|s| if s.to_lowercase().starts_with('c') { "C" } else { "P" })
        .unwrap_or("?");
    let exp = get_str(obj, &["expiration_date", "expiration"]).map(short_date);

    let detail = match (strike, exp) {
        (Some(k), Some(e)) => format!("{k:.0}{opt_type} {e}"),
        _ => String::new(),
    };

    Some(PositionSummary {
        symbol, detail,
        qty:     format!("{contracts:.0}"),
        avg:     format!("${avg:.2}"),
        current: format!("${cur:.2}"),
        pnl:     format!("{}{:.2}", if is_gain { "+" } else { "-" }, pnl.abs()),
        is_gain,
    })
}

fn short_date(s: &str) -> String {
    let p: Vec<&str> = s.split('-').collect();
    if p.len() == 3 {
        format!("{}/{}", p[1].trim_start_matches('0'), p[2].trim_start_matches('0'))
    } else {
        s.to_string()
    }
}

fn get_str<'a>(obj: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| obj.get(*k)?.as_str())
}

fn get_f64(obj: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| {
        let v = obj.get(*k)?;
        v.as_f64().or_else(|| v.as_str()?.parse().ok())
    })
}

fn pluck(obj: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| {
        let v = obj.get(*k)?;
        v.as_f64().or_else(|| v.as_str()?.parse().ok())
    })
}
