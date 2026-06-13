use anyhow::{Context, Result};
use serde_json::Value;
use tracing::warn;

pub struct OhlcvBar {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

// Inline Python script for fetching from Robinhood's unofficial API.
// Uses env vars (HR_SYMBOL / HR_PERIOD) to avoid Rust format-string escaping.
// Reads credentials from the OS keychain (same service as the Machine MCP server).
const ROBINHOOD_FETCH_SCRIPT: &str = r#"
import os, sys, io, contextlib

symbol = os.environ.get("HR_SYMBOL", "")
period = os.environ.get("HR_PERIOD", "1 month")

try:
    import keyring
    import robin_stocks.robinhood as r
    from datetime import datetime, timezone
except ImportError as e:
    print(f"missing Python package: {e} -- pip install robin-stocks keyring", file=sys.stderr)
    sys.exit(1)

INTERVAL = {"1 day": "5minute", "1 week": "hour", "1 month": "day", "1 year": "week"}
SPAN     = {"1 day": "day",     "1 week": "week",  "1 month": "month", "1 year": "year"}

u  = keyring.get_password("hoodrat-robinhood-machine", "username")
pw = keyring.get_password("hoodrat-robinhood-machine", "password")
if not u or not pw:
    print("no Robinhood credentials in OS keychain -- start the Machine MCP server once to store them", file=sys.stderr)
    sys.exit(1)

# Suppress robin_stocks login chatter (it prints to stdout and would corrupt the CSV)
with contextlib.redirect_stdout(io.StringIO()):
    r.login(u, pw, store_session=True, pickle_name="machine")

data = r.get_stock_historicals(
    symbol,
    interval=INTERVAL.get(period, "day"),
    span=SPAN.get(period, "month"),
    bounds="regular",
) or []

for bar in data:
    if not bar or not bar.get("close_price"):
        continue
    ts = int(datetime.fromisoformat(bar["begins_at"].replace("Z", "+00:00")).timestamp())
    print(f"{ts},{bar['open_price']},{bar['high_price']},{bar['low_price']},{bar['close_price']},{bar.get('volume', 0)}")
"#;

/// Fetch OHLCV from Yahoo Finance, falling back to Robinhood's unofficial API
/// if Yahoo returns no data. The Robinhood path covers symbols Yahoo doesn't
/// carry, such as XSP (CBOE Mini-SPX), and reuses the Machine account credentials
/// already stored in the OS keychain.
pub async fn fetch_history(symbol: &str, period: &str) -> Result<Vec<OhlcvBar>> {
    match fetch_history_yahoo(symbol, period).await {
        Ok(bars) if !bars.is_empty() => return Ok(bars),
        Ok(_) => {
            warn!("[historical] Yahoo Finance returned no data for {symbol} — trying Robinhood");
        }
        Err(e) => {
            warn!("[historical] Yahoo Finance failed for {symbol}: {e} — trying Robinhood");
        }
    }
    fetch_history_robinhood(symbol, period).await
}

async fn fetch_history_yahoo(symbol: &str, period: &str) -> Result<Vec<OhlcvBar>> {
    let (interval, range) = period_params(period);
    let url = format!(
        "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?\
         interval={interval}&range={range}&includePrePost=false"
    );

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let resp: Value = client
        .get(&url)
        .send()
        .await
        .context("HTTP request failed")?
        .json()
        .await
        .context("JSON parse failed")?;

    let result = resp["chart"]["result"]
        .as_array()
        .and_then(|a| a.first())
        .context("no results in Yahoo Finance response")?;

    let timestamps = result["timestamp"]
        .as_array()
        .context("no timestamps")?;

    let quote = result["indicators"]["quote"]
        .as_array()
        .and_then(|a| a.first())
        .context("no quote data")?;

    let opens   = quote["open"].as_array().context("no open")?;
    let highs   = quote["high"].as_array().context("no high")?;
    let lows    = quote["low"].as_array().context("no low")?;
    let closes  = quote["close"].as_array().context("no close")?;
    let volumes = quote["volume"].as_array().context("no volume")?;

    let bars = timestamps
        .iter()
        .enumerate()
        .filter_map(|(i, ts)| {
            Some(OhlcvBar {
                timestamp: ts.as_i64()?,
                open:   opens.get(i)?.as_f64()?,
                high:   highs.get(i)?.as_f64()?,
                low:    lows.get(i)?.as_f64()?,
                close:  closes.get(i)?.as_f64()?,
                volume: volumes.get(i)?.as_f64().unwrap_or(0.0),
            })
        })
        .collect();

    Ok(bars)
}

async fn fetch_history_robinhood(symbol: &str, period: &str) -> Result<Vec<OhlcvBar>> {
    let output = tokio::process::Command::new("python")
        .args(["-c", ROBINHOOD_FETCH_SCRIPT])
        .env("HR_SYMBOL", symbol)
        .env("HR_PERIOD", period)
        .output()
        .await
        .context("failed to spawn Python — is Python 3 in PATH?")?;

    if !output.status.success() {
        let e = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Robinhood historical fetch failed: {}", e.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let bars: Vec<OhlcvBar> = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let mut p = line.split(',');
            let ts    = p.next()?.parse::<i64>().ok()?;
            let open  = p.next()?.parse::<f64>().ok()?;
            let high  = p.next()?.parse::<f64>().ok()?;
            let low   = p.next()?.parse::<f64>().ok()?;
            let close = p.next()?.parse::<f64>().ok()?;
            let vol   = p.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            Some(OhlcvBar { timestamp: ts, open, high, low, close, volume: vol })
        })
        .collect();

    Ok(bars)
}

fn period_params(period: &str) -> (&'static str, &'static str) {
    match period {
        "1 day"   => ("5m",  "1d"),
        "1 week"  => ("1h",  "5d"),
        "1 month" => ("1d",  "1mo"),
        "1 year"  => ("1wk", "1y"),
        _         => ("1d",  "1mo"),
    }
}

pub fn format_for_prompt(symbol: &str, bars: &[OhlcvBar], period: &str) -> String {
    let label = match period {
        "1 day"   => "1-day, 5-minute bars",
        "1 week"  => "1-week, hourly bars",
        "1 month" => "1-month, daily bars",
        "1 year"  => "1-year, weekly bars",
        _         => period,
    };
    let mut out = format!(
        "=== {symbol} ({label}, {} bars) ===\nDatetime (UTC),Open,High,Low,Close,Volume\n",
        bars.len()
    );
    for bar in bars {
        let dt = unix_to_datetime(bar.timestamp);
        out.push_str(&format!(
            "{dt},{:.2},{:.2},{:.2},{:.2},{:.0}\n",
            bar.open, bar.high, bar.low, bar.close, bar.volume
        ));
    }
    out
}

pub fn unix_to_datetime(ts: i64) -> String {
    let time_of_day = (ts % 86400) as u32;
    let hour = time_of_day / 3600;
    let min  = (time_of_day % 3600) / 60;
    format!("{} {:02}:{:02}", unix_to_ymd(ts), hour, min)
}

fn unix_to_ymd(ts: i64) -> String {
    let mut days = (ts / 86400) as u32;
    let mut year = 1970u32;
    loop {
        let in_year = if is_leap(year) { 366 } else { 365 };
        if days < in_year { break; }
        days -= in_year;
        year += 1;
    }
    let month_days: [u32; 12] = [
        31, if is_leap(year) { 29 } else { 28 },
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ];
    let mut month = 1u32;
    for &d in &month_days {
        if days < d { break; }
        days -= d;
        month += 1;
    }
    format!("{year}-{month:02}-{:02}", days + 1)
}

fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}
