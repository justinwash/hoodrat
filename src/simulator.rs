use crate::agent::McpOutput;
use crate::config::SimulationConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone)]
struct OptionInstrumentMetadata {
    underlying: String,
    option_type: OptionType,
    strike: f64,
    expiration: String,
    multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    Equity,
    Option,
}

impl AssetClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Equity => "equity",
            Self::Option => "option",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketQuote {
    #[serde(alias = "ticker")]
    pub symbol: String,
    pub asset_class: AssetClass,
    #[serde(default)]
    pub bid: Option<f64>,
    #[serde(default)]
    pub ask: Option<f64>,
    #[serde(default, alias = "last_price", alias = "mark")]
    pub last: Option<f64>,
    #[serde(default, alias = "quote_time", alias = "timestamp")]
    pub as_of: Option<String>,
    #[serde(default)]
    pub underlying: Option<String>,
    #[serde(default, alias = "right", alias = "contract_type")]
    pub option_type: Option<OptionType>,
    #[serde(default)]
    pub strike: Option<f64>,
    #[serde(default, alias = "expiration_date", alias = "expiry")]
    pub expiration: Option<String>,
    #[serde(default)]
    pub multiplier: Option<f64>,
}

impl MarketQuote {
    fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            anyhow::bail!("market quote symbol must not be empty");
        }
        if self.asset_class == AssetClass::Option
            && (self.underlying.as_deref().unwrap_or_default().is_empty()
                || self.option_type.is_none()
                || self.strike.is_none()
                || self.expiration.is_none())
        {
            anyhow::bail!("option quote requires underlying, option_type, strike, and expiration");
        }
        for value in [self.bid, self.ask, self.last, self.strike, self.multiplier]
            .into_iter()
            .flatten()
        {
            if !value.is_finite() || value < 0.0 {
                anyhow::bail!("market quote contains an invalid numeric value");
            }
        }
        if let (Some(bid), Some(ask)) = (self.bid, self.ask) {
            if ask < bid {
                anyhow::bail!("market quote ask is below bid for {}", self.symbol);
            }
        }
        if self.bid.is_none() && self.ask.is_none() && self.last.is_none() {
            anyhow::bail!("market quote has no bid, ask, or last price");
        }
        Ok(())
    }

    fn mark(&self) -> f64 {
        match (self.bid, self.ask, self.last) {
            (Some(bid), Some(ask), _) => (bid + ask) / 2.0,
            (_, _, Some(last)) => last,
            (Some(bid), None, None) => bid,
            (None, Some(ask), None) => ask,
            (None, None, None) => 0.0,
        }
    }

    fn executable_price(&self, buy: bool) -> f64 {
        if buy {
            self.ask.unwrap_or_else(|| self.mark())
        } else {
            self.bid.unwrap_or_else(|| self.mark())
        }
    }

    fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.asset_class.as_str(),
            self.symbol,
            self.expiration.as_deref().unwrap_or_default(),
            self.strike.unwrap_or_default(),
            self.option_type
                .as_ref()
                .map(|value| match value {
                    OptionType::Call => "call",
                    OptionType::Put => "put",
                })
                .unwrap_or_default()
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeProposal {
    pub action: String,
    pub symbol: String,
    pub asset_class: AssetClass,
    pub quantity: f64,
    #[serde(default)]
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub underlying: Option<String>,
    #[serde(default)]
    pub option_type: Option<OptionType>,
    #[serde(default)]
    pub strike: Option<f64>,
    #[serde(default)]
    pub expiration: Option<String>,
    #[serde(default)]
    pub multiplier: Option<f64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketPlan {
    #[serde(default)]
    pub captured_at: Option<String>,
    pub quotes: Vec<MarketQuote>,
    #[serde(default)]
    pub proposals: Vec<TradeProposal>,
}

impl MarketPlan {
    pub fn from_mcp_outputs(
        outputs: &BTreeMap<String, Vec<McpOutput>>,
        proposals: Vec<TradeProposal>,
        config: &SimulationConfig,
        now: DateTime<Utc>,
    ) -> Result<Self> {
        let mut option_instruments = HashMap::new();
        for (tool_name, tool_outputs) in outputs {
            if !tool_name
                .to_ascii_lowercase()
                .contains("option_instruments")
            {
                continue;
            }
            for output in tool_outputs {
                if !output.is_error {
                    let normalized = crate::ingestion::normalize_mcp_payload(&output.value)
                        .unwrap_or_else(|_| crate::ingestion::parse_json_text(&output.value));
                    collect_option_instruments(&normalized, &mut option_instruments);
                }
            }
        }
        let mut quotes = Vec::new();
        for (tool_name, tool_outputs) in outputs {
            for output in tool_outputs {
                if output.is_error {
                    continue;
                }
                quotes.extend(extract_quotes(
                    tool_name,
                    &output.value,
                    &option_instruments,
                )?);
            }
        }
        if quotes.is_empty() {
            anyhow::bail!(
                "successful MCP market-data outputs contained no timestamped current quotes"
            );
        }
        let captured_at = quotes
            .iter()
            .filter_map(|quote| quote.as_of.as_deref())
            .filter_map(|value| value.parse::<DateTime<Utc>>().ok())
            .max()
            .context("normalized MCP quotes have no timestamp")?
            .to_rfc3339();
        let plan = Self {
            captured_at: Some(captured_at),
            quotes,
            proposals,
        };
        plan.validate(config, now)?;
        Ok(plan)
    }

    pub fn paper_proposals_from_agent_output(raw_output: &str) -> Result<Vec<TradeProposal>> {
        for line in raw_output.lines() {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if let Some(proposals) = find_paper_proposals(&value, 0) {
                return proposals;
            }
        }
        Ok(Vec::new())
    }

    pub fn validate(&self, config: &SimulationConfig, now: DateTime<Utc>) -> Result<()> {
        if self.quotes.is_empty() {
            anyhow::bail!("market snapshot contains no quotes");
        }
        let captured_at = self
            .captured_at
            .as_deref()
            .context("market snapshot is missing captured_at")?
            .parse::<DateTime<Utc>>()?;
        validate_timestamp(captured_at, now, config.max_quote_age_secs, "snapshot")?;
        let configured_symbols = config
            .symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let mut quote_keys = HashSet::new();
        for quote in &self.quotes {
            quote.validate()?;
            let configured_symbol = quote
                .underlying
                .as_deref()
                .unwrap_or(&quote.symbol)
                .to_ascii_uppercase();
            if !configured_symbols.contains(&configured_symbol) {
                anyhow::bail!(
                    "market quote {} is outside the configured simulation symbols",
                    quote.symbol
                );
            }
            if !quote_keys.insert(quote.key()) {
                anyhow::bail!(
                    "market snapshot contains a duplicate quote for {}",
                    quote.symbol
                );
            }
            if let Some(as_of) = quote.as_of.as_deref() {
                validate_timestamp(
                    as_of.parse::<DateTime<Utc>>()?,
                    now,
                    config.max_quote_age_secs,
                    &format!("quote {}", quote.symbol),
                )?;
            }
        }
        Ok(())
    }
}

pub fn summarize_mcp_outputs(outputs: &BTreeMap<String, Vec<McpOutput>>) -> Vec<String> {
    outputs
        .iter()
        .map(|(tool, values)| {
            let mut shapes = BTreeSet::new();
            let mut paths = BTreeSet::new();
            for output in values {
                let normalized = crate::ingestion::normalize_mcp_payload(&output.value)
                    .unwrap_or_else(|_| output.value.clone());
                shapes.insert(json_shape(&normalized, 0));
                collect_schema_paths(&normalized, "", 0, &mut paths);
            }
            format!(
                "{tool}: responses={}, shapes={}, fields={}",
                values.len(),
                shapes.into_iter().collect::<Vec<_>>().join(" | "),
                paths.into_iter().take(40).collect::<Vec<_>>().join(", ")
            )
        })
        .collect()
}

fn extract_quotes(
    tool_name: &str,
    raw: &Value,
    option_instruments: &HashMap<String, OptionInstrumentMetadata>,
) -> Result<Vec<MarketQuote>> {
    let lower_tool = tool_name.to_ascii_lowercase();
    if lower_tool.contains("crypto") {
        return Ok(Vec::new());
    }
    if ["historical", "history", "candle"]
        .iter()
        .any(|term| lower_tool.contains(term))
    {
        return Ok(Vec::new());
    }
    if lower_tool.contains("option_instruments") || lower_tool.contains("option_chains") {
        return Ok(Vec::new());
    }
    let normalized = crate::ingestion::normalize_mcp_payload(raw)
        .unwrap_or_else(|_| crate::ingestion::parse_json_text(raw));
    if lower_tool.contains("option_quotes") {
        return extract_option_quotes(&normalized, option_instruments);
    }
    let mut quotes = Vec::new();
    if lower_tool.contains("price_book") {
        collect_price_book_quotes(&normalized, &mut quotes);
        return Ok(quotes);
    }
    collect_quotes(&normalized, tool_name, &mut quotes);
    Ok(quotes)
}

fn collect_option_instruments(
    value: &Value,
    instruments: &mut HashMap<String, OptionInstrumentMetadata>,
) {
    if let Some(object) = value.as_object() {
        let option_type =
            direct_string(object, &["type", "option_type", "optionType"]).and_then(|value| {
                match value.to_ascii_lowercase().as_str() {
                    "call" | "c" => Some(OptionType::Call),
                    "put" | "p" => Some(OptionType::Put),
                    _ => None,
                }
            });
        let state = direct_string(object, &["state"]);
        let tradability = direct_string(object, &["tradability"]);
        if let (
            Some(id),
            Some(underlying),
            Some(option_type),
            Some(strike),
            Some(expiration),
            Some(multiplier),
        ) = (
            direct_string(object, &["id", "instrument_id", "instrumentId"]),
            direct_string(object, &["chain_symbol", "underlying", "underlying_symbol"]),
            option_type,
            direct_number(object, &["strike_price", "strikePrice", "strike"]),
            direct_string(object, &["expiration_date", "expirationDate", "expiration"]),
            direct_number(object, &["trade_value_multiplier", "multiplier"]),
        ) {
            if state.as_deref() != Some("active")
                || tradability.as_deref() != Some("tradable")
                || strike <= 0.0
                || multiplier <= 0.0
            {
                return;
            }
            instruments.insert(
                id,
                OptionInstrumentMetadata {
                    underlying,
                    option_type,
                    strike,
                    expiration,
                    multiplier,
                },
            );
        }
        for child in object.values() {
            collect_option_instruments(child, instruments);
        }
    } else if let Some(array) = value.as_array() {
        for child in array {
            collect_option_instruments(child, instruments);
        }
    }
}

fn extract_option_quotes(
    value: &Value,
    instruments: &HashMap<String, OptionInstrumentMetadata>,
) -> Result<Vec<MarketQuote>> {
    let mut quotes = Vec::new();
    collect_option_quotes(value, instruments, &mut quotes)?;
    Ok(quotes)
}

fn collect_option_quotes(
    value: &Value,
    instruments: &HashMap<String, OptionInstrumentMetadata>,
    quotes: &mut Vec<MarketQuote>,
) -> Result<()> {
    if let Some(object) = value.as_object() {
        let instrument_id = direct_string(object, &["instrument_id", "instrumentId"]);
        let bid = direct_number(object, &["bid_price", "bidPrice", "bid"]);
        let mark = direct_number(
            object,
            &[
                "mark_price",
                "markPrice",
                "adjusted_mark_price",
                "adjustedMarkPrice",
            ],
        );
        let ask = direct_number(object, &["ask_price", "askPrice", "ask"]);
        if let Some(instrument_id) = instrument_id {
            if bid.is_some() || ask.is_some() || mark.is_some() {
                let metadata = instruments.get(&instrument_id).with_context(|| {
                    format!(
                        "option quote {instrument_id} has no active/tradable instrument metadata"
                    )
                })?;
                let as_of = direct_timestamp(object, &["updated_at", "updatedAt"])
                    .context("option quote is missing updated_at")?;
                quotes.push(MarketQuote {
                    symbol: instrument_id,
                    asset_class: AssetClass::Option,
                    bid: bid.filter(|value| *value > 0.0),
                    ask: ask.filter(|value| *value > 0.0),
                    last: mark.filter(|value| *value > 0.0),
                    as_of: Some(as_of),
                    underlying: Some(metadata.underlying.clone()),
                    option_type: Some(metadata.option_type.clone()),
                    strike: Some(metadata.strike),
                    expiration: Some(metadata.expiration.clone()),
                    multiplier: Some(metadata.multiplier),
                });
            }
        }
        for child in object.values() {
            collect_option_quotes(child, instruments, quotes)?;
        }
    } else if let Some(array) = value.as_array() {
        for child in array {
            collect_option_quotes(child, instruments, quotes)?;
        }
    }
    Ok(())
}

fn collect_price_book_quotes(value: &Value, quotes: &mut Vec<MarketQuote>) {
    if let Some(object) = value.as_object() {
        if let Some(books) = object.get("books").and_then(Value::as_array) {
            for book in books {
                let Some(book_object) = book.as_object() else {
                    continue;
                };
                let Some(symbol) = direct_string(book_object, &["symbol", "ticker"]) else {
                    continue;
                };
                let Some(as_of) = direct_timestamp(book_object, &["updated_at", "updatedAt"])
                else {
                    continue;
                };
                let bid = best_book_level(book_object.get("bids"));
                let ask = best_book_level(book_object.get("asks"));
                if bid.is_none() && ask.is_none() {
                    continue;
                }
                quotes.push(MarketQuote {
                    symbol,
                    asset_class: AssetClass::Equity,
                    bid,
                    ask,
                    last: None,
                    as_of: Some(as_of),
                    underlying: None,
                    option_type: None,
                    strike: None,
                    expiration: None,
                    multiplier: None,
                });
            }
        }
        for child in object.values() {
            collect_price_book_quotes(child, quotes);
        }
    } else if let Some(array) = value.as_array() {
        for child in array {
            collect_price_book_quotes(child, quotes);
        }
    }
}

fn best_book_level(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(Value::as_object)
        .and_then(|level| direct_number(level, &["price", "price_value", "priceValue"]))
        .filter(|price| *price > 0.0)
}

fn collect_quotes(value: &Value, tool_name: &str, quotes: &mut Vec<MarketQuote>) {
    if let Some(object) = value.as_object() {
        if let Some(quote) = parse_current_quote(object, tool_name) {
            quotes.push(quote);
        }
        for child in object.values() {
            collect_quotes(child, tool_name, quotes);
        }
    } else if let Some(array) = value.as_array() {
        for child in array {
            collect_quotes(child, tool_name, quotes);
        }
    }
}

fn parse_current_quote(
    object: &serde_json::Map<String, Value>,
    tool_name: &str,
) -> Option<MarketQuote> {
    if object
        .get("has_traded")
        .and_then(Value::as_bool)
        .is_some_and(|has_traded| !has_traded)
    {
        return None;
    }
    if direct_string(object, &["asset_class", "assetClass"])
        .is_some_and(|asset_class| asset_class.eq_ignore_ascii_case("crypto"))
    {
        return None;
    }
    if direct_string(object, &["state"]).is_some_and(|state| !state.eq_ignore_ascii_case("active"))
    {
        return None;
    }
    let symbol = direct_string(
        object,
        &["symbol", "ticker", "instrument_symbol", "instrumentSymbol"],
    )?;
    let lower_tool = tool_name.to_ascii_lowercase();
    let is_equity_quotes = lower_tool.contains("equity_quotes");
    let bid = direct_number(object, &["bid_price", "bidPrice", "bid"]).filter(|value| *value > 0.0);
    let ask = direct_number(object, &["ask_price", "askPrice", "ask"]).filter(|value| *value > 0.0);
    let bid_time = direct_timestamp(object, &["venue_bid_time", "venue_bidTime"]);
    let ask_time = direct_timestamp(object, &["venue_ask_time", "venue_askTime"]);
    let (last, last_time) = if is_equity_quotes {
        let regular = direct_number(object, &["last_trade_price", "lastTradePrice"]).zip(
            direct_timestamp(object, &["venue_last_trade_time", "venue_lastTradeTime"]),
        );
        let non_regular = direct_number(
            object,
            &["last_non_reg_trade_price", "lastNonRegTradePrice"],
        )
        .zip(direct_timestamp(
            object,
            &["venue_last_non_reg_trade_time", "venue_lastNonRegTradeTime"],
        ));
        match (regular, non_regular) {
            (Some((regular_price, regular_time)), Some((non_regular_price, non_regular_time))) => {
                if parse_timestamp(&non_regular_time) > parse_timestamp(&regular_time) {
                    (Some(non_regular_price), Some(non_regular_time))
                } else {
                    (Some(regular_price), Some(regular_time))
                }
            }
            (Some((price, time)), None) | (None, Some((price, time))) => (Some(price), Some(time)),
            (None, None) => (None, None),
        }
    } else {
        let last = direct_number(
            object,
            &[
                "mark_price",
                "markPrice",
                "mark",
                "last_price",
                "lastPrice",
                "last",
                "price",
            ],
        );
        let time = direct_timestamp(
            object,
            &[
                "timestamp",
                "updated_at",
                "updatedAt",
                "quote_time",
                "quoteTime",
                "as_of",
                "asOf",
            ],
        );
        (last, time)
    };
    let as_of = [bid_time, ask_time, last_time]
        .into_iter()
        .flatten()
        .max_by_key(|value| parse_timestamp(value));
    let as_of =
        as_of.or_else(|| direct_timestamp(object, &["timestamp", "updated_at", "updatedAt"]));
    if bid.is_none() && ask.is_none() && last.is_none() {
        return None;
    }
    let as_of = as_of?;
    let asset_class = direct_string(
        object,
        &["asset_class", "assetClass", "asset_type", "assetType"],
    )
    .map(|value| parse_asset_class(&value))
    .unwrap_or_else(|| infer_asset_class(tool_name, object));
    let option_type = direct_string(
        object,
        &[
            "option_type",
            "optionType",
            "right",
            "contract_type",
            "contractType",
        ],
    )
    .and_then(|value| match value.to_ascii_lowercase().as_str() {
        "call" | "c" => Some(OptionType::Call),
        "put" | "p" => Some(OptionType::Put),
        _ => None,
    });
    Some(MarketQuote {
        symbol,
        asset_class,
        bid,
        ask,
        last,
        as_of: Some(as_of),
        underlying: direct_string(
            object,
            &["underlying", "underlying_symbol", "underlyingSymbol"],
        ),
        option_type,
        strike: direct_number(object, &["strike", "strike_price", "strikePrice"]),
        expiration: direct_string(
            object,
            &["expiration", "expiration_date", "expirationDate", "expiry"],
        ),
        multiplier: direct_number(object, &["multiplier", "contract_multiplier"]),
    })
}

fn direct_string(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| match object.get(*key) {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        _ => None,
    })
}

fn direct_number(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        object.get(*key).and_then(|value| match value {
            Value::Number(number) => number.as_f64(),
            Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        })
    })
}

fn direct_timestamp(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(timestamp_value))
}

fn infer_asset_class(tool_name: &str, object: &serde_json::Map<String, Value>) -> AssetClass {
    if direct_string(
        object,
        &["underlying", "underlying_symbol", "underlyingSymbol"],
    )
    .is_some()
        || direct_number(object, &["strike", "strike_price", "strikePrice"]).is_some()
    {
        AssetClass::Option
    } else {
        parse_asset_class(tool_name)
    }
}

fn timestamp_value(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        if let Ok(timestamp) = value.parse::<DateTime<Utc>>() {
            return Some(timestamp.to_rfc3339());
        }
        if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            return Some(
                DateTime::<Utc>::from_naive_utc_and_offset(date.and_hms_opt(0, 0, 0)?, Utc)
                    .to_rfc3339(),
            );
        }
    }
    let number = value.as_f64()?;
    let seconds = if number > 100_000_000_000.0 {
        (number / 1_000.0) as i64
    } else {
        number as i64
    };
    Utc.timestamp_opt(seconds, 0)
        .single()
        .map(|timestamp| timestamp.to_rfc3339())
}

fn parse_timestamp(value: &str) -> DateTime<Utc> {
    value.parse().unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn parse_asset_class(value: &str) -> AssetClass {
    let value = value.to_ascii_lowercase();
    if value.contains("option") || value == "contract" {
        AssetClass::Option
    } else {
        AssetClass::Equity
    }
}

fn json_shape(value: &Value, depth: u8) -> String {
    if depth > 4 {
        return "<depth>".to_owned();
    }
    match value {
        Value::Object(object) => format!(
            "{{{}}}",
            object
                .iter()
                .take(30)
                .map(|(key, value)| format!("{key}:{}", json_shape(value, depth + 1)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Array(values) => format!(
            "[{}]",
            values
                .first()
                .map(|value| json_shape(value, depth + 1))
                .unwrap_or_default()
        ),
        Value::Null => "null".to_owned(),
        Value::Bool(_) => "bool".to_owned(),
        Value::Number(_) => "number".to_owned(),
        Value::String(_) => "string".to_owned(),
    }
}

fn collect_schema_paths(value: &Value, prefix: &str, depth: u8, paths: &mut BTreeSet<String>) {
    if depth > 5 {
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                paths.insert(path.clone());
                collect_schema_paths(child, &path, depth + 1, paths);
            }
        }
        Value::Array(values) => {
            for child in values.iter().take(3) {
                collect_schema_paths(child, &format!("{prefix}[]"), depth + 1, paths);
            }
        }
        _ => {}
    }
}

fn find_paper_proposals(value: &Value, depth: u8) -> Option<Result<Vec<TradeProposal>>> {
    if depth > 8 {
        return None;
    }
    if let Some(object) = value.as_object() {
        if let Some(proposals) = object.get("paper_proposals") {
            return Some(
                serde_json::from_value(proposals.clone())
                    .context("paper_proposals envelope is invalid"),
            );
        }
        for child in object.values() {
            if let Some(found) = find_paper_proposals(child, depth + 1) {
                return Some(found);
            }
        }
    } else if let Some(array) = value.as_array() {
        for child in array {
            if let Some(found) = find_paper_proposals(child, depth + 1) {
                return Some(found);
            }
        }
    } else if let Some(text) = value.as_str() {
        if let Ok(parsed) = serde_json::from_str::<Value>(text) {
            return find_paper_proposals(&parsed, depth + 1);
        }
    }
    None
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperEvent {
    pub event_type: String,
    pub event_at: String,
    pub symbol: String,
    pub asset_class: String,
    pub side: String,
    pub quantity: f64,
    pub price: f64,
    pub notional_usd: f64,
    pub fee_usd: f64,
    pub realized_pnl_usd: f64,
    pub details: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperPosition {
    pub position_key: String,
    pub symbol: String,
    pub asset_class: String,
    pub quantity: f64,
    pub average_entry_price: f64,
    pub mark_price: f64,
    pub market_value_usd: f64,
    pub unrealized_pnl_usd: f64,
    pub opened_at: String,
    pub underlying: Option<String>,
    pub option_type: Option<OptionType>,
    pub strike: Option<f64>,
    pub expiration: Option<String>,
    pub multiplier: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperSimulation {
    pub id: String,
    pub profile: String,
    pub started_at: String,
    pub finished_at: String,
    pub status: String,
    pub starting_cash_usd: f64,
    pub final_cash_usd: f64,
    pub final_equity_usd: f64,
    pub realized_pnl_usd: f64,
    pub unrealized_pnl_usd: f64,
    pub events: Vec<PaperEvent>,
    pub positions: Vec<PaperPosition>,
    pub no_op_reasons: Vec<String>,
    pub market_plan: MarketPlan,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    position_key: String,
    symbol: String,
    asset_class: AssetClass,
    quantity: f64,
    average_entry_price: f64,
    opened_at: DateTime<Utc>,
    underlying: Option<String>,
    option_type: Option<OptionType>,
    strike: Option<f64>,
    expiration: Option<String>,
    multiplier: f64,
}

pub fn simulate(
    plan: MarketPlan,
    config: &SimulationConfig,
    now: DateTime<Utc>,
) -> Result<PaperSimulation> {
    config.validate()?;
    plan.validate(config, now)?;
    let started_at = now.to_rfc3339();
    let mut engine = PaperEngine {
        config,
        now,
        cash: config.starting_cash_usd,
        realized_pnl: 0.0,
        positions: HashMap::new(),
        events: Vec::new(),
        no_op_reasons: Vec::new(),
    };
    let quotes = plan
        .quotes
        .iter()
        .map(|quote| (quote.key(), quote))
        .collect::<HashMap<_, _>>();
    let symbols = plan
        .quotes
        .iter()
        .map(|quote| (quote.symbol.to_ascii_uppercase(), quote))
        .collect::<HashMap<_, _>>();

    for proposal in &plan.proposals {
        if proposal.action.eq_ignore_ascii_case("hold") {
            engine
                .no_op_reasons
                .push(format!("{} proposal requested hold", proposal.symbol));
            continue;
        }
        let Some(quote) = find_quote(proposal, &quotes, &symbols) else {
            engine.no_op_reasons.push(format!(
                "no matching quote for {} {} proposal",
                proposal.asset_class.as_str(),
                proposal.symbol
            ));
            continue;
        };
        if let Err(error) = engine.apply_proposal(proposal, quote) {
            engine
                .no_op_reasons
                .push(format!("{} proposal skipped: {error}", proposal.symbol));
        }
    }
    engine.expire_options(&symbols);

    let positions = engine
        .positions
        .values()
        .filter_map(|position| engine.mark_position(position, &quotes, &symbols))
        .collect::<Vec<_>>();
    let market_value = positions
        .iter()
        .map(|position| position.market_value_usd)
        .sum::<f64>();
    let unrealized_pnl = positions
        .iter()
        .map(|position| position.unrealized_pnl_usd)
        .sum::<f64>();
    let status = if engine.events.is_empty() {
        "no_op"
    } else {
        "completed"
    };
    Ok(PaperSimulation {
        id: format!("paper-{}", now.format("%Y%m%dT%H%M%S%.3fZ")),
        profile: config.profile.name.clone(),
        started_at,
        finished_at: now.to_rfc3339(),
        status: status.to_owned(),
        starting_cash_usd: config.starting_cash_usd,
        final_cash_usd: engine.cash,
        final_equity_usd: engine.cash + market_value,
        realized_pnl_usd: engine.realized_pnl,
        unrealized_pnl_usd: unrealized_pnl,
        events: engine.events,
        positions,
        no_op_reasons: engine.no_op_reasons,
        market_plan: plan,
    })
}

struct PaperEngine<'a> {
    config: &'a SimulationConfig,
    now: DateTime<Utc>,
    cash: f64,
    realized_pnl: f64,
    positions: HashMap<String, OpenPosition>,
    events: Vec<PaperEvent>,
    no_op_reasons: Vec<String>,
}

impl PaperEngine<'_> {
    fn apply_proposal(&mut self, proposal: &TradeProposal, quote: &MarketQuote) -> Result<()> {
        self.validate_asset_class(&proposal.asset_class)?;
        if proposal.quantity <= 0.0 || !proposal.quantity.is_finite() {
            anyhow::bail!("quantity must be finite and greater than zero");
        }
        if let Some(limit) = proposal.limit_price {
            if limit <= 0.0 || !limit.is_finite() {
                anyhow::bail!("limit price must be finite and greater than zero");
            }
        }
        if proposal.asset_class == AssetClass::Option {
            self.validate_option(proposal, quote)?;
        }
        let existing_quantity = self
            .positions
            .get(&quote.key())
            .map(|position| position.quantity);
        let action = proposal.action.to_ascii_lowercase();
        let is_closing_short = matches!(action.as_str(), "reduce" | "close")
            && existing_quantity.is_some_and(|quantity| quantity < 0.0);
        let buy = matches!(action.as_str(), "buy" | "cover") || is_closing_short;
        let sell = matches!(action.as_str(), "sell" | "short")
            || (matches!(action.as_str(), "reduce" | "close")
                && existing_quantity.is_some_and(|quantity| quantity > 0.0));
        if !buy && !sell {
            anyhow::bail!("unsupported proposal action '{}'", proposal.action);
        }
        if action == "cover" && !existing_quantity.is_some_and(|quantity| quantity < 0.0) {
            anyhow::bail!("cover proposal has no open short position");
        }
        if action == "cover" && proposal.quantity > existing_quantity.unwrap_or_default().abs() {
            anyhow::bail!("cover proposal quantity exceeds the open short position");
        }
        if matches!(action.as_str(), "reduce" | "close") && existing_quantity.is_none() {
            anyhow::bail!("{} proposal has no open position", proposal.action);
        }
        if matches!(action.as_str(), "reduce" | "close") {
            let existing_quantity = existing_quantity.expect("checked above");
            if proposal.quantity > existing_quantity.abs() {
                anyhow::bail!(
                    "{} proposal quantity exceeds the open position",
                    proposal.action
                );
            }
        }
        if sell
            && !self.config.profile.allow_short
            && !self.has_long_position(proposal, quote)
            && !existing_quantity.is_some_and(|quantity| quantity < 0.0)
        {
            anyhow::bail!("short simulation is disabled by the profile");
        }
        let reference = quote.executable_price(buy);
        let slippage = reference * self.config.profile.slippage_bps as f64 / 10_000.0;
        let mut fill_price = if buy {
            reference + slippage
        } else {
            (reference - slippage).max(0.0)
        };
        if let Some(limit) = proposal.limit_price {
            if (buy && fill_price > limit) || (!buy && fill_price < limit) {
                anyhow::bail!("limit price was not marketable under the simulated quote");
            }
            fill_price = if buy {
                fill_price.min(limit)
            } else {
                fill_price.max(limit)
            };
        }
        let multiplier = proposal.multiplier.or(quote.multiplier).unwrap_or(
            if proposal.asset_class == AssetClass::Option {
                100.0
            } else {
                1.0
            },
        );
        let delta = if matches!(action.as_str(), "reduce" | "close") {
            -existing_quantity.expect("checked above").signum() * proposal.quantity
        } else if buy {
            proposal.quantity
        } else {
            -proposal.quantity
        };
        let notional = fill_price * proposal.quantity * multiplier;
        self.ensure_exposure(notional, delta, fill_price, quote, multiplier)?;
        let fee = notional * self.config.profile.fee_bps as f64 / 10_000.0;
        self.cash -= delta * fill_price * multiplier + fee;
        let position_key = quote.key();
        let realized = self.update_position(
            &position_key,
            proposal,
            quote,
            delta,
            fill_price,
            multiplier,
        );
        self.realized_pnl += realized - fee;
        self.events.push(PaperEvent {
            event_type: "fill".to_owned(),
            event_at: self.now.to_rfc3339(),
            symbol: proposal.symbol.clone(),
            asset_class: proposal.asset_class.as_str().to_owned(),
            side: if buy { "buy" } else { "sell" }.to_owned(),
            quantity: proposal.quantity,
            price: fill_price,
            notional_usd: notional,
            fee_usd: fee,
            realized_pnl_usd: realized - fee,
            details: proposal.reason.clone().unwrap_or_default(),
        });
        Ok(())
    }

    fn validate_asset_class(&self, asset_class: &AssetClass) -> Result<()> {
        let allowed = match asset_class {
            AssetClass::Equity => self.config.profile.allow_equities,
            AssetClass::Option => self.config.profile.allow_options,
        };
        if !allowed {
            anyhow::bail!(
                "asset class {} is disabled by the simulation profile",
                asset_class.as_str()
            );
        }
        Ok(())
    }

    fn validate_option(&self, proposal: &TradeProposal, quote: &MarketQuote) -> Result<()> {
        let expiration = proposal
            .expiration
            .as_deref()
            .or(quote.expiration.as_deref())
            .context("option proposal is missing expiration")?;
        let expiry = parse_expiration(expiration)?;
        let dte = (expiry.date_naive() - self.now.date_naive()).num_days();
        if !(0..=self.config.profile.max_option_dte as i64).contains(&dte) {
            anyhow::bail!("option expiration is outside the configured 0-1 DTE simulation window");
        }
        if dte == 0 && !self.config.profile.allow_zero_dte {
            anyhow::bail!("zero-DTE options are disabled by the simulation profile");
        }
        if proposal.strike.or(quote.strike).unwrap_or(0.0) <= 0.0 {
            anyhow::bail!("option proposal is missing a positive strike");
        }
        if proposal
            .option_type
            .as_ref()
            .or(quote.option_type.as_ref())
            .is_none()
        {
            anyhow::bail!("option proposal is missing call/put type");
        }
        Ok(())
    }

    fn ensure_exposure(
        &self,
        notional: f64,
        delta: f64,
        fill_price: f64,
        quote: &MarketQuote,
        multiplier: f64,
    ) -> Result<()> {
        let current = self
            .positions
            .values()
            .map(|position| {
                position.quantity.abs() * position.average_entry_price * position.multiplier
            })
            .sum::<f64>();
        let post_trade = if let Some(existing) = self.positions.get(&quote.key()) {
            let existing_notional =
                existing.quantity.abs() * existing.average_entry_price * existing.multiplier;
            let new_quantity = existing.quantity + delta;
            let new_average = if new_quantity.abs() <= f64::EPSILON {
                0.0
            } else if existing.quantity == 0.0 || existing.quantity.signum() == delta.signum() {
                ((existing.quantity.abs() * existing.average_entry_price)
                    + (delta.abs() * fill_price))
                    / new_quantity.abs()
            } else if existing.quantity.signum() == new_quantity.signum() {
                existing.average_entry_price
            } else {
                fill_price
            };
            current - existing_notional + new_quantity.abs() * new_average * existing.multiplier
        } else {
            current + notional
        };
        let leverage_limit = self.config.starting_cash_usd * self.config.profile.max_leverage;
        if post_trade > self.config.profile.max_gross_exposure_usd || post_trade > leverage_limit {
            anyhow::bail!(
                "simulated gross exposure would exceed the aggressive profile limit (quote {}, multiplier {})",
                quote.symbol,
                multiplier
            );
        }
        if self.positions.len() >= self.config.profile.max_positions
            && !self.positions.contains_key(&quote.key())
        {
            anyhow::bail!("simulated position count limit reached");
        }
        let fee = notional * self.config.profile.fee_bps as f64 / 10_000.0;
        if !self.config.profile.allow_leverage && delta > 0.0 && self.cash - notional - fee < 0.0 {
            anyhow::bail!("simulated cash would become negative");
        }
        Ok(())
    }

    fn has_long_position(&self, proposal: &TradeProposal, quote: &MarketQuote) -> bool {
        self.positions.get(&quote.key()).is_some_and(|position| {
            position.quantity > 0.0 && proposal.quantity <= position.quantity
        })
    }

    fn update_position(
        &mut self,
        position_key: &str,
        proposal: &TradeProposal,
        quote: &MarketQuote,
        delta: f64,
        fill_price: f64,
        multiplier: f64,
    ) -> f64 {
        let existing = self.positions.remove(position_key);
        let (old_quantity, old_average) = existing
            .as_ref()
            .map(|position| (position.quantity, position.average_entry_price))
            .unwrap_or((0.0, 0.0));
        let mut realized = 0.0;
        let same_direction = old_quantity == 0.0 || old_quantity.signum() == delta.signum();
        let new_quantity = old_quantity + delta;
        if !same_direction {
            let closed = old_quantity.abs().min(delta.abs());
            realized = if old_quantity > 0.0 {
                (fill_price - old_average) * closed * multiplier
            } else {
                (old_average - fill_price) * closed * multiplier
            };
        }
        if new_quantity.abs() > f64::EPSILON {
            let new_average = if same_direction {
                if old_quantity.abs() < f64::EPSILON {
                    fill_price
                } else {
                    ((old_quantity.abs() * old_average) + (delta.abs() * fill_price))
                        / new_quantity.abs()
                }
            } else if old_quantity.signum() == new_quantity.signum() {
                old_average
            } else {
                fill_price
            };
            self.positions.insert(
                position_key.to_owned(),
                OpenPosition {
                    position_key: position_key.to_owned(),
                    symbol: proposal.symbol.clone(),
                    asset_class: proposal.asset_class.clone(),
                    quantity: new_quantity,
                    average_entry_price: new_average,
                    opened_at: self.now,
                    underlying: proposal.underlying.clone().or(quote.underlying.clone()),
                    option_type: proposal.option_type.clone().or(quote.option_type.clone()),
                    strike: proposal.strike.or(quote.strike),
                    expiration: proposal.expiration.clone().or(quote.expiration.clone()),
                    multiplier,
                },
            );
        }
        realized
    }

    fn expire_options(&mut self, symbols: &HashMap<String, &MarketQuote>) {
        let expirations = self
            .positions
            .values()
            .filter(|position| position.asset_class == AssetClass::Option)
            .filter_map(|position| {
                position
                    .expiration
                    .as_deref()
                    .and_then(|value| parse_expiration(value).ok())
                    .filter(|expiry| *expiry <= self.now)
                    .map(|_| position.position_key.clone())
            })
            .collect::<Vec<_>>();
        for position_key in expirations {
            let Some(position) = self.positions.get(&position_key).cloned() else {
                continue;
            };
            let Some(underlying) = position
                .underlying
                .as_deref()
                .and_then(|symbol| symbols.get(&symbol.to_ascii_uppercase()).copied())
            else {
                self.no_op_reasons.push(format!(
                    "could not settle expired option {} without an underlying quote",
                    position.symbol
                ));
                continue;
            };
            let Some(strike) = position.strike else {
                continue;
            };
            let underlying_mark = underlying.last.unwrap_or_else(|| underlying.mark());
            let intrinsic = match position.option_type {
                Some(OptionType::Call) => (underlying_mark - strike).max(0.0),
                Some(OptionType::Put) => (strike - underlying_mark).max(0.0),
                None => 0.0,
            };
            let delta = -position.quantity;
            self.cash -= delta * intrinsic * position.multiplier;
            let realized = self.close_position(&position_key, delta, intrinsic);
            self.realized_pnl += realized;
            self.events.push(PaperEvent {
                event_type: "option_expiry".to_owned(),
                event_at: self.now.to_rfc3339(),
                symbol: position.symbol,
                asset_class: "option".to_owned(),
                side: if delta > 0.0 { "buy" } else { "sell" }.to_owned(),
                quantity: delta.abs(),
                price: intrinsic,
                notional_usd: intrinsic * delta.abs() * position.multiplier,
                fee_usd: 0.0,
                realized_pnl_usd: realized,
                details: "settled at intrinsic value".to_owned(),
            });
        }
    }

    fn close_position(&mut self, position_key: &str, delta: f64, price: f64) -> f64 {
        let Some(mut position) = self.positions.remove(position_key) else {
            return 0.0;
        };
        let closed = position.quantity.abs().min(delta.abs());
        let realized = if position.quantity > 0.0 {
            (price - position.average_entry_price) * closed * position.multiplier
        } else {
            (position.average_entry_price - price) * closed * position.multiplier
        };
        position.quantity += delta;
        if position.quantity.abs() > f64::EPSILON {
            self.positions.insert(position_key.to_owned(), position);
        }
        realized
    }

    fn mark_position(
        &self,
        position: &OpenPosition,
        quotes: &HashMap<String, &MarketQuote>,
        symbols: &HashMap<String, &MarketQuote>,
    ) -> Option<PaperPosition> {
        let quote = quotes
            .values()
            .find(|quote| quote.key() == position.position_key)?;
        let mark = quote.mark();
        let market_value = position.quantity * mark * position.multiplier;
        let unrealized = if position.quantity >= 0.0 {
            (mark - position.average_entry_price) * position.quantity * position.multiplier
        } else {
            (position.average_entry_price - mark) * position.quantity.abs() * position.multiplier
        };
        let _ = symbols;
        Some(PaperPosition {
            position_key: position.position_key.clone(),
            symbol: position.symbol.clone(),
            asset_class: position.asset_class.as_str().to_owned(),
            quantity: position.quantity,
            average_entry_price: position.average_entry_price,
            mark_price: mark,
            market_value_usd: market_value,
            unrealized_pnl_usd: unrealized,
            opened_at: position.opened_at.to_rfc3339(),
            underlying: position.underlying.clone(),
            option_type: position.option_type.clone(),
            strike: position.strike,
            expiration: position.expiration.clone(),
            multiplier: position.multiplier,
        })
    }
}

fn find_quote<'a>(
    proposal: &TradeProposal,
    quotes: &HashMap<String, &'a MarketQuote>,
    symbols: &HashMap<String, &'a MarketQuote>,
) -> Option<&'a MarketQuote> {
    let key = format!(
        "{}:{}:{}:{}:{}",
        proposal.asset_class.as_str(),
        proposal.symbol,
        proposal.expiration.as_deref().unwrap_or_default(),
        proposal.strike.unwrap_or_default(),
        proposal
            .option_type
            .as_ref()
            .map(|value| match value {
                OptionType::Call => "call",
                OptionType::Put => "put",
            })
            .unwrap_or_default()
    );
    quotes
        .get(&key)
        .copied()
        .or_else(|| symbols.get(&proposal.symbol.to_ascii_uppercase()).copied())
}

fn parse_expiration(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(timestamp) = value.parse::<DateTime<Utc>>() {
        return Ok(timestamp);
    }
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        date.and_hms_opt(23, 59, 59)
            .context("invalid expiration date")?,
        Utc,
    ))
}

fn validate_timestamp(
    timestamp: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: u64,
    label: &str,
) -> Result<()> {
    if timestamp > now + Duration::seconds(60) {
        anyhow::bail!("{label} timestamp is in the future");
    }
    if now.signed_duration_since(timestamp).num_seconds() > max_age_secs as i64 {
        anyhow::bail!("{label} data is stale");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SimulationConfig, SimulationProfile};

    fn now() -> DateTime<Utc> {
        "2026-08-27T12:00:00Z".parse().unwrap()
    }

    fn quote(symbol: &str, price: f64) -> MarketQuote {
        MarketQuote {
            symbol: symbol.to_owned(),
            asset_class: AssetClass::Equity,
            bid: Some(price - 0.05),
            ask: Some(price + 0.05),
            last: Some(price),
            as_of: Some("2026-08-27T11:59:30Z".to_owned()),
            underlying: None,
            option_type: None,
            strike: None,
            expiration: None,
            multiplier: None,
        }
    }

    fn plan(proposals: Vec<TradeProposal>) -> MarketPlan {
        MarketPlan {
            captured_at: Some("2026-08-27T11:59:30Z".to_owned()),
            quotes: vec![quote("SPY", 500.0)],
            proposals,
        }
    }

    #[test]
    fn normalizes_equity_quote_from_raw_mcp_response() {
        let config = SimulationConfig {
            enabled: true,
            max_quote_age_secs: 120,
            ..SimulationConfig::default()
        };
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "get_equity_quotes".to_owned(),
            vec![McpOutput {
                is_error: false,
                value: serde_json::json!({
                    "structuredContent": {"data": {"results": [{
                        "quote": {
                            "symbol": "SPY",
                            "bid_price": "499.90",
                            "ask_price": "500.10",
                            "last_trade_price": "500.00",
                            "venue_last_trade_time": "2026-08-27T11:59:20Z",
                            "last_non_reg_trade_price": "500.25",
                            "venue_last_non_reg_trade_time": "2026-08-27T11:59:40Z",
                            "venue_bid_time": "2026-08-27T11:59:30Z",
                            "venue_ask_time": "2026-08-27T11:59:30Z",
                            "has_traded": true,
                            "state": "active"
                        },
                        "close": {"symbol": "SPY", "price": "499.00"}
                    }]}}
                }),
            }],
        );
        let plan = MarketPlan::from_mcp_outputs(&outputs, Vec::new(), &config, now()).unwrap();
        assert_eq!(plan.quotes.len(), 1);
        assert_eq!(plan.quotes[0].symbol, "SPY");
        assert_eq!(plan.quotes[0].bid, Some(499.9));
        assert_eq!(plan.quotes[0].ask, Some(500.1));
        assert_eq!(plan.quotes[0].last, Some(500.25));
        assert_eq!(
            plan.quotes[0].as_of.as_deref(),
            Some("2026-08-27T11:59:40+00:00")
        );
    }

    #[test]
    fn normalizes_best_levels_from_equity_price_book() {
        let config = SimulationConfig {
            enabled: true,
            max_quote_age_secs: 120,
            ..SimulationConfig::default()
        };
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "get_equity_price_book".to_owned(),
            vec![McpOutput {
                is_error: false,
                value: serde_json::json!({
                    "structuredContent": {"data": {"books": [{
                        "symbol": "SPY",
                        "updated_at": "2026-08-27T11:59:30Z",
                        "bids": [{"price": "499.90", "quantity": "100"}],
                        "asks": [{"price": 500.10, "quantity": 50}]
                    }]}}
                }),
            }],
        );
        let plan = MarketPlan::from_mcp_outputs(&outputs, Vec::new(), &config, now()).unwrap();
        assert_eq!(plan.quotes.len(), 1);
        assert_eq!(plan.quotes[0].bid, Some(499.9));
        assert_eq!(plan.quotes[0].ask, Some(500.1));
    }

    #[test]
    fn normalizes_option_fields_from_raw_mcp_response() {
        let config = SimulationConfig {
            enabled: true,
            symbols: vec!["SPY".to_owned()],
            max_quote_age_secs: 120,
            ..SimulationConfig::default()
        };
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "get_option_instruments".to_owned(),
            vec![McpOutput {
                is_error: false,
                value: serde_json::json!({
                    "data": {"instruments": [{
                        "id": "instrument-1",
                        "chain_id": "chain-1",
                        "chain_symbol": "SPY",
                        "underlying_type": "equity",
                        "expiration_date": "2026-08-28",
                        "strike_price": "500.0000",
                        "type": "call",
                        "state": "active",
                        "tradability": "tradable",
                        "trade_value_multiplier": "100.0000"
                    }]}
                }),
            }],
        );
        outputs.insert(
            "get_option_quotes".to_owned(),
            vec![McpOutput {
                is_error: false,
                value: serde_json::json!({
                    "data": {"results": [{
                        "quote": {
                            "instrument_id": "instrument-1",
                            "bid_price": "2.00",
                            "ask_price": "2.20",
                            "mark_price": "2.10",
                            "updated_at": "2026-08-27T11:59:30Z"
                        },
                        "close": {
                            "instrument_id": "instrument-1",
                            "symbol": "SPY",
                            "date": "2026-08-26",
                            "price": "1.95",
                            "interpolated": false,
                            "source": "ddb-market-snapshot"
                        }
                    }]}
                }),
            }],
        );
        let plan = MarketPlan::from_mcp_outputs(&outputs, Vec::new(), &config, now()).unwrap();
        assert_eq!(plan.quotes.len(), 1);
        let option = plan
            .quotes
            .iter()
            .find(|quote| quote.asset_class == AssetClass::Option)
            .unwrap();
        assert_eq!(option.option_type, Some(OptionType::Call));
        assert_eq!(option.symbol, "instrument-1");
        assert_eq!(option.underlying.as_deref(), Some("SPY"));
        assert_eq!(option.strike, Some(500.0));
        assert_eq!(option.expiration.as_deref(), Some("2026-08-28"));
        assert_eq!(option.bid, Some(2.0));
        assert_eq!(option.ask, Some(2.2));
        assert_eq!(option.last, Some(2.1));
        assert_eq!(option.multiplier, Some(100.0));
    }

    #[test]
    fn rejects_crypto_quotes_in_equity_options_simulation() {
        let config = SimulationConfig {
            enabled: true,
            symbols: vec!["SPY".to_owned()],
            max_quote_age_secs: 120,
            ..SimulationConfig::default()
        };
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "get_equity_quotes".to_owned(),
            vec![McpOutput {
                is_error: false,
                value: serde_json::json!({
                    "data": {"results": [{"quote": {
                        "symbol": "SPY",
                        "asset_class": "crypto",
                        "bid_price": "499.90",
                        "ask_price": "500.10",
                        "last_trade_price": "500.00",
                        "venue_last_trade_time": "2026-08-27T11:59:30Z",
                        "state": "active",
                        "has_traded": true
                    }}]}
                }),
            }],
        );
        assert!(MarketPlan::from_mcp_outputs(&outputs, Vec::new(), &config, now()).is_err());
    }

    #[test]
    fn historical_outputs_do_not_become_current_quotes() {
        let config = SimulationConfig {
            enabled: true,
            max_quote_age_secs: 120,
            ..SimulationConfig::default()
        };
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "get_equity_historicals".to_owned(),
            vec![McpOutput {
                is_error: false,
                value: serde_json::json!({
                    "data": [{"symbol": "SPY", "close": 500.0, "timestamp": "2026-08-27T11:59:30Z"}]
                }),
            }],
        );
        assert!(MarketPlan::from_mcp_outputs(&outputs, Vec::new(), &config, now()).is_err());
    }

    #[test]
    fn agent_output_can_supply_proposals_but_not_quotes() {
        let raw = serde_json::json!({
            "paper_proposals": [{
                "action": "buy",
                "symbol": "SPY",
                "asset_class": "equity",
                "quantity": 1.0,
                "reason": "paper test"
            }],
            "market_snapshot": {"quotes": [{"symbol": "SPY", "last": 1.0}]}
        })
        .to_string();
        let proposals = MarketPlan::paper_proposals_from_agent_output(&raw).unwrap();
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].symbol, "SPY");
    }

    #[test]
    fn aggressive_profile_simulates_equity_fill_and_pnl() {
        let config = SimulationConfig {
            enabled: true,
            starting_cash_usd: 10_000.0,
            max_quote_age_secs: 120,
            profile: SimulationProfile::aggressive_default(),
            ..SimulationConfig::default()
        };
        let result = simulate(
            plan(vec![TradeProposal {
                action: "buy".to_owned(),
                symbol: "SPY".to_owned(),
                asset_class: AssetClass::Equity,
                quantity: 2.0,
                limit_price: None,
                underlying: None,
                option_type: None,
                strike: None,
                expiration: None,
                multiplier: None,
                reason: Some("momentum".to_owned()),
            }]),
            &config,
            now(),
        )
        .unwrap();
        assert_eq!(result.status, "completed");
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.positions.len(), 1);
        assert!(result.final_cash_usd < config.starting_cash_usd);
    }

    #[test]
    fn aggressive_profile_supports_short_positions() {
        let config = SimulationConfig {
            enabled: true,
            starting_cash_usd: 10_000.0,
            max_quote_age_secs: 120,
            profile: SimulationProfile::aggressive_default(),
            ..SimulationConfig::default()
        };
        let result = simulate(
            plan(vec![TradeProposal {
                action: "short".to_owned(),
                symbol: "SPY".to_owned(),
                asset_class: AssetClass::Equity,
                quantity: 2.0,
                limit_price: None,
                underlying: None,
                option_type: None,
                strike: None,
                expiration: None,
                multiplier: None,
                reason: Some("short momentum".to_owned()),
            }]),
            &config,
            now(),
        )
        .unwrap();
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].side, "sell");
        assert_eq!(result.positions.len(), 1);
        assert!(result.positions[0].quantity < 0.0);
        assert!(result.final_cash_usd > config.starting_cash_usd);
    }

    #[test]
    fn rejects_quotes_outside_configured_symbols() {
        let config = SimulationConfig {
            enabled: true,
            max_quote_age_secs: 120,
            ..SimulationConfig::default()
        };
        let mut market_plan = plan(Vec::new());
        market_plan.quotes[0] = quote("AAPL", 200.0);
        assert!(simulate(market_plan, &config, now()).is_err());
    }

    #[test]
    fn reduce_closes_a_short_position_instead_of_adding_to_it() {
        let config = SimulationConfig {
            enabled: true,
            starting_cash_usd: 10_000.0,
            max_quote_age_secs: 120,
            profile: SimulationProfile::aggressive_default(),
            ..SimulationConfig::default()
        };
        let market_plan = plan(vec![
            TradeProposal {
                action: "short".to_owned(),
                symbol: "SPY".to_owned(),
                asset_class: AssetClass::Equity,
                quantity: 2.0,
                limit_price: None,
                underlying: None,
                option_type: None,
                strike: None,
                expiration: None,
                multiplier: None,
                reason: None,
            },
            TradeProposal {
                action: "reduce".to_owned(),
                symbol: "SPY".to_owned(),
                asset_class: AssetClass::Equity,
                quantity: 1.0,
                limit_price: None,
                underlying: None,
                option_type: None,
                strike: None,
                expiration: None,
                multiplier: None,
                reason: None,
            },
        ]);
        let result = simulate(market_plan, &config, now()).unwrap();
        assert_eq!(result.positions.len(), 1);
        assert_eq!(result.positions[0].quantity, -1.0);
        assert_eq!(result.events[1].side, "buy");
    }

    #[test]
    fn zero_dte_call_settles_at_intrinsic_value() {
        let config = SimulationConfig {
            enabled: true,
            starting_cash_usd: 10_000.0,
            max_quote_age_secs: 120,
            profile: SimulationProfile::aggressive_default(),
            ..SimulationConfig::default()
        };
        let option = MarketQuote {
            symbol: "SPY-2026-08-27-500-C".to_owned(),
            asset_class: AssetClass::Option,
            bid: Some(2.0),
            ask: Some(2.2),
            last: Some(2.1),
            as_of: Some("2026-08-27T11:59:30Z".to_owned()),
            underlying: Some("SPY".to_owned()),
            option_type: Some(OptionType::Call),
            strike: Some(500.0),
            expiration: Some("2026-08-27T11:00:00Z".to_owned()),
            multiplier: Some(100.0),
        };
        let mut market_plan = plan(Vec::new());
        market_plan.quotes.push(option);
        market_plan.proposals.push(TradeProposal {
            action: "buy".to_owned(),
            symbol: "SPY-2026-08-27-500-C".to_owned(),
            asset_class: AssetClass::Option,
            quantity: 1.0,
            limit_price: None,
            underlying: Some("SPY".to_owned()),
            option_type: Some(OptionType::Call),
            strike: Some(500.0),
            expiration: Some("2026-08-27T11:00:00Z".to_owned()),
            multiplier: Some(100.0),
            reason: None,
        });
        let result = simulate(market_plan, &config, now()).unwrap();
        assert!(result
            .events
            .iter()
            .any(|event| event.event_type == "option_expiry"));
        assert!(result.positions.is_empty());
    }

    #[test]
    fn stale_market_snapshot_is_rejected() {
        let config = SimulationConfig {
            enabled: true,
            max_quote_age_secs: 10,
            ..SimulationConfig::default()
        };
        assert!(simulate(plan(Vec::new()), &config, now()).is_err());
    }
}
