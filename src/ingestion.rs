use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize)]
pub struct AccountRecord {
    pub captured_at: String,
    pub account_number: String,
    pub rhs_account_number: Option<String>,
    pub rhc_account_number: Option<String>,
    pub account_type: Option<String>,
    pub brokerage_account_type: Option<String>,
    pub nickname: Option<String>,
    pub is_default: Option<bool>,
    pub agentic_allowed: Option<bool>,
    pub option_level: Option<String>,
    pub management_type: Option<String>,
    pub affiliate: Option<String>,
    pub state: Option<String>,
    pub deactivated: Option<bool>,
    pub permanently_deactivated: Option<bool>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct BalanceRecord {
    pub captured_at: String,
    pub account_number: Option<String>,
    pub cash_usd: Option<f64>,
    pub buying_power_usd: Option<f64>,
    pub unleveraged_buying_power_usd: Option<f64>,
    pub equity_usd: Option<f64>,
    pub margin_used_usd: Option<f64>,
    pub unsettled_funds_usd: Option<f64>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionRecord {
    pub captured_at: String,
    pub account_number: Option<String>,
    pub symbol: Option<String>,
    pub instrument_id: Option<String>,
    pub asset_class: Option<String>,
    pub quantity: Option<f64>,
    pub average_cost_usd: Option<f64>,
    pub market_value_usd: Option<f64>,
    pub current_price_usd: Option<f64>,
    pub unrealized_pnl_usd: Option<f64>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSnapshot {
    pub captured_at: String,
    pub account_number: Option<String>,
    pub total_value_usd: Option<f64>,
    pub buying_power_usd: Option<f64>,
    pub cash_usd: Option<f64>,
    pub positions_value_usd: Option<f64>,
    pub unsettled_funds_usd: Option<f64>,
    pub realized_pnl_usd: Option<f64>,
    pub unrealized_pnl_usd: Option<f64>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct PnlSnapshot {
    pub captured_at: String,
    pub account_number: Option<String>,
    pub span: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub realized_pnl_usd: Option<f64>,
    pub total_returns_usd: Option<f64>,
    pub rate_of_realized_gain: Option<f64>,
    pub total_rate_of_return: Option<f64>,
    pub number_of_trades: Option<u32>,
    pub by_asset_class: Option<Value>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct PnlTradeRecord {
    pub captured_at: String,
    pub account_number: Option<String>,
    pub external_id: Option<String>,
    pub symbol: Option<String>,
    pub asset_class: Option<String>,
    pub side: Option<String>,
    pub quantity: Option<f64>,
    pub realized_pnl_usd: Option<f64>,
    pub opened_at: Option<String>,
    pub closed_at: Option<String>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionRecord {
    pub external_id: Option<String>,
    pub asset_class: String,
    pub symbol: String,
    pub side: String,
    pub quantity: Option<f64>,
    pub notional_usd: Option<f64>,
    pub status: String,
    pub submitted_at: Option<String>,
    pub filled_at: Option<String>,
    pub average_fill_price: Option<f64>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub enum BrokerPayload {
    Accounts(Vec<AccountRecord>),
    Portfolio {
        snapshot: PortfolioSnapshot,
        balances: Vec<BalanceRecord>,
        positions: Vec<PositionRecord>,
    },
    Pnl(PnlSnapshot),
    PnlTradeHistory(Vec<PnlTradeRecord>),
}

pub trait BrokerDataSink {
    fn ingest_portfolio_snapshot(&self, snapshot: &PortfolioSnapshot) -> Result<()>;
    fn ingest_execution(&self, execution: &ExecutionRecord, run_id: Option<&str>) -> Result<()>;
}

impl AccountRecord {
    pub fn from_value(raw: &Value) -> Result<Vec<Self>> {
        let payload = normalize_mcp_payload(raw)?;
        let accounts = find_value(&payload, "accounts")
            .and_then(Value::as_array)
            .context("accounts payload has no accounts array")?;
        let captured_at = Utc::now().to_rfc3339();
        let records = accounts
            .iter()
            .filter_map(Value::as_object)
            .filter_map(|object| {
                find_string(object, &["account_number", "accountNumber"]).map(|account_number| {
                    Self {
                        captured_at: captured_at.clone(),
                        account_number,
                        rhs_account_number: find_string(
                            object,
                            &["rhs_account_number", "rhsAccountNumber"],
                        ),
                        rhc_account_number: find_string(
                            object,
                            &["rhc_account_number", "rhcAccountNumber"],
                        ),
                        account_type: find_string(object, &["type", "account_type", "accountType"]),
                        brokerage_account_type: find_string(
                            object,
                            &["brokerage_account_type", "brokerageAccountType"],
                        ),
                        nickname: find_string(object, &["nickname", "name"]),
                        is_default: find_bool(object, &["is_default", "isDefault"]),
                        agentic_allowed: find_bool(object, &["agentic_allowed", "agenticAllowed"]),
                        option_level: find_string(object, &["option_level", "optionLevel"]),
                        management_type: find_string(
                            object,
                            &["management_type", "managementType"],
                        ),
                        affiliate: find_string(object, &["affiliate"]),
                        state: find_string(object, &["state", "status"]),
                        deactivated: find_bool(object, &["deactivated"]),
                        permanently_deactivated: find_bool(
                            object,
                            &["permanently_deactivated", "permanentlyDeactivated"],
                        ),
                        raw: Value::Object(object.clone()),
                    }
                })
            })
            .collect::<Vec<_>>();
        Ok(records)
    }
}

pub fn single_agentic_account(accounts: &[AccountRecord]) -> Result<String> {
    let eligible = accounts
        .iter()
        .filter(|account| account.agentic_allowed == Some(true))
        .map(|account| account.account_number.clone())
        .collect::<Vec<_>>();
    match eligible.as_slice() {
        [account_number] => Ok(account_number.clone()),
        [] => anyhow::bail!("accounts payload contains no agent-accessible account"),
        _ => anyhow::bail!("accounts payload contains multiple agent-accessible accounts"),
    }
}

impl PortfolioSnapshot {
    pub fn from_value(raw: &Value) -> Result<Self> {
        Self::from_value_for_account(raw, None).map(|(snapshot, _, _)| snapshot)
    }

    pub fn from_value_for_account(
        raw: &Value,
        account_number: Option<&str>,
    ) -> Result<(Self, Vec<BalanceRecord>, Vec<PositionRecord>)> {
        let payload = normalize_mcp_payload(raw)?;
        let captured_at = Utc::now().to_rfc3339();
        let embedded_account = find_string_any(
            &payload,
            &["account_number", "accountNumber", "account_id", "accountId"],
        );
        let account_number = embedded_account.or_else(|| account_number.map(ToOwned::to_owned));
        let total_value_usd = find_number_any(
            &payload,
            &[
                "total_value_usd",
                "total_value",
                "portfolio_value",
                "totalPortfolioValue",
            ],
        );
        let buying_power_usd = find_number_any(
            &payload,
            &["buying_power_usd", "buying_power", "buyingPower"],
        );
        let unleveraged_buying_power_usd = find_number_any(
            &payload,
            &[
                "unleveraged_buying_power_usd",
                "unleveraged_buying_power",
                "unleveragedBuyingPower",
            ],
        );
        let cash_usd = find_number_any(
            &payload,
            &["cash_usd", "cash", "cash_balance", "cashBalance"],
        );
        let positions_value_usd = find_number_any(
            &payload,
            &[
                "positions_value_usd",
                "positions_value",
                "holdings_value",
                "holdingsValue",
            ],
        );
        let unsettled_funds_usd = find_number_any(
            &payload,
            &["unsettled_funds_usd", "unsettled_funds", "unsettledFunds"],
        );
        let realized_pnl_usd = find_number_any(
            &payload,
            &["realized_pnl_usd", "realized_pnl", "realizedPnl"],
        );
        let unrealized_pnl_usd = find_number_any(
            &payload,
            &["unrealized_pnl_usd", "unrealized_pnl", "unrealizedPnl"],
        );

        if [
            total_value_usd,
            buying_power_usd,
            unleveraged_buying_power_usd,
            cash_usd,
            positions_value_usd,
            unsettled_funds_usd,
            realized_pnl_usd,
            unrealized_pnl_usd,
        ]
        .iter()
        .all(Option::is_none)
        {
            anyhow::bail!("portfolio payload contains no recognized numeric fields");
        }

        let snapshot = Self {
            captured_at: captured_at.clone(),
            account_number: account_number.clone(),
            total_value_usd,
            buying_power_usd,
            cash_usd,
            positions_value_usd,
            unsettled_funds_usd,
            realized_pnl_usd,
            unrealized_pnl_usd,
            raw: raw.clone(),
        };

        let balances = parse_balances(&payload, &captured_at, account_number.as_deref());
        let positions = parse_positions(&payload, &captured_at, account_number.as_deref());
        Ok((snapshot, balances, positions))
    }
}

impl PnlSnapshot {
    pub fn from_value(raw: &Value, account_number: Option<&str>) -> Result<Self> {
        let payload = normalize_mcp_payload(raw)?;
        let realized_pnl_usd = find_number_any(
            &payload,
            &[
                "realized_pnl_usd",
                "realized_pnl",
                "realizedPnl",
                "realized_gain",
                "realizedGain",
                "total_realized_pnl",
                "totalRealizedPnl",
                "pnl",
                "profit_loss",
                "profitLoss",
            ],
        );
        let total_returns_usd = find_number_any(
            &payload,
            &["total_returns", "total_returns_usd", "totalReturns"],
        );
        let rate_of_realized_gain =
            find_number_any(&payload, &["rate_of_realized_gain", "rateOfRealizedGain"]);
        let total_rate_of_return =
            find_number_any(&payload, &["total_rate_of_return", "totalRateOfReturn"]);
        let number_of_trades = find_value(&payload, "number_of_trades")
            .or_else(|| find_value(&payload, "numberOfTrades"))
            .and_then(number_value)
            .and_then(|value| u32::try_from(value as u64).ok());
        let by_asset_class = find_value(&payload, "by_asset_class")
            .or_else(|| find_value(&payload, "asset_class_breakdown"))
            .or_else(|| find_value(&payload, "data_points"))
            .cloned();
        if realized_pnl_usd.is_none() && total_returns_usd.is_none() && by_asset_class.is_none() {
            anyhow::bail!("realized PnL payload contains no recognized PnL fields");
        }
        Ok(Self {
            captured_at: Utc::now().to_rfc3339(),
            account_number: find_string_any(
                &payload,
                &["account_number", "accountNumber", "account_id", "accountId"],
            )
            .or_else(|| account_number.map(ToOwned::to_owned)),
            span: find_string_any(&payload, &["span", "period", "window"]),
            start_date: find_string_any(
                &payload,
                &["start_date", "startDate", "start_time", "startTime"],
            ),
            end_date: find_string_any(&payload, &["end_date", "endDate", "end_time", "endTime"]),
            realized_pnl_usd,
            total_returns_usd,
            rate_of_realized_gain,
            total_rate_of_return,
            number_of_trades,
            by_asset_class,
            raw: raw.clone(),
        })
    }
}

impl PnlTradeRecord {
    fn from_object(
        object: &Map<String, Value>,
        captured_at: &str,
        account_number: Option<&str>,
    ) -> Option<Self> {
        let external_id = find_string(
            object,
            &["id", "external_id", "externalId", "trade_id", "tradeId"],
        );
        let symbol = find_string(
            object,
            &["symbol", "ticker", "instrument_symbol", "instrumentSymbol"],
        );
        let realized_pnl_usd = find_number(
            object,
            &[
                "realized_pnl_usd",
                "realized_pnl",
                "realizedPnl",
                "pnl",
                "profit_loss",
                "profitLoss",
            ],
        );
        if external_id.is_none() && symbol.is_none() && realized_pnl_usd.is_none() {
            return None;
        }
        Some(Self {
            captured_at: captured_at.to_owned(),
            account_number: find_string(
                object,
                &["account_number", "accountNumber", "account_id", "accountId"],
            )
            .or_else(|| account_number.map(ToOwned::to_owned)),
            external_id,
            symbol,
            asset_class: find_string(object, &["asset_class", "assetClass", "type"]),
            side: find_string(object, &["side", "direction"]),
            quantity: find_number(object, &["quantity", "filled_quantity", "filledQuantity"]),
            realized_pnl_usd,
            opened_at: find_string(object, &["opened_at", "openedAt", "entry_at", "entryAt"]),
            closed_at: find_string(object, &["closed_at", "closedAt", "exit_at", "exitAt"]),
            raw: Value::Object(object.clone()),
        })
    }

    pub fn from_value(raw: &Value, account_number: Option<&str>) -> Result<Vec<Self>> {
        let payload = normalize_mcp_payload(raw)?;
        let captured_at = Utc::now().to_rfc3339();
        let records = if let Some(array) = payload.as_array() {
            array
                .iter()
                .filter_map(Value::as_object)
                .filter_map(|object| Self::from_object(object, &captured_at, account_number))
                .collect::<Vec<_>>()
        } else {
            let array = find_array(
                &payload,
                &["trades", "trade_history", "tradeHistory", "history"],
            )
            .or_else(|| find_value(&payload, "data").and_then(Value::as_array))
            .context("trade history payload has no recognizable trade collection")?;
            array
                .iter()
                .filter_map(Value::as_object)
                .filter_map(|object| Self::from_object(object, &captured_at, account_number))
                .collect::<Vec<_>>()
        };
        Ok(records)
    }
}

impl ExecutionRecord {
    pub fn from_value(raw: &Value) -> Result<Self> {
        let payload = normalize_mcp_payload(raw)?;
        let object = payload_object(&payload).context("execution payload is not a JSON object")?;
        let symbol = find_string(
            object,
            &["symbol", "ticker", "instrument_symbol", "instrumentSymbol"],
        )
        .context("execution payload has no recognized symbol")?;
        let side = find_string(object, &["side", "direction", "order_side", "orderSide"])
            .context("execution payload has no recognized side")?;

        Ok(Self {
            external_id: find_string(object, &["external_id", "id", "order_id", "orderId"]),
            asset_class: find_string(object, &["asset_class", "assetClass", "type"])
                .unwrap_or_else(|| "unknown".to_owned()),
            symbol,
            side,
            quantity: find_number(object, &["quantity", "filled_quantity", "filledQuantity"]),
            notional_usd: find_number(object, &["notional_usd", "notional", "amount"]),
            status: find_string(object, &["status", "state"])
                .unwrap_or_else(|| "unknown".to_owned()),
            submitted_at: find_string(
                object,
                &["submitted_at", "submittedAt", "created_at", "createdAt"],
            ),
            filled_at: find_string(
                object,
                &["filled_at", "filledAt", "executed_at", "executedAt"],
            ),
            average_fill_price: find_number(
                object,
                &[
                    "average_fill_price",
                    "averageFillPrice",
                    "fill_price",
                    "fillPrice",
                ],
            ),
            raw: raw.clone(),
        })
    }
}

pub fn parse_broker_payload(tool_name: &str, raw: &Value) -> Result<BrokerPayload> {
    match tool_name.to_ascii_lowercase().as_str() {
        "get_accounts" => AccountRecord::from_value(raw).map(BrokerPayload::Accounts),
        "get_portfolio" => PortfolioSnapshot::from_value_for_account(raw, None).map(
            |(snapshot, balances, positions)| BrokerPayload::Portfolio {
                snapshot,
                balances,
                positions,
            },
        ),
        "get_realized_pnl" => PnlSnapshot::from_value(raw, None).map(BrokerPayload::Pnl),
        "get_pnl_trade_history" => {
            PnlTradeRecord::from_value(raw, None).map(BrokerPayload::PnlTradeHistory)
        }
        _ => anyhow::bail!("unsupported typed broker tool: {tool_name}"),
    }
}

pub fn normalize_mcp_payload(raw: &Value) -> Result<Value> {
    let mut value = parse_json_text(raw);
    for _ in 0..6 {
        let Some(object) = value.as_object() else {
            break;
        };
        if object
            .get("isError")
            .or_else(|| object.get("is_error"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            anyhow::bail!("Robinhood MCP returned an error envelope");
        }
        if let Some(structured) = object.get("structuredContent") {
            let next = parse_json_text(structured);
            if next != value {
                value = next;
                continue;
            }
        }
        if let Some(content) = object.get("content").and_then(Value::as_array) {
            if let Some(text) = content
                .iter()
                .find_map(|item| item.get("text").and_then(Value::as_str))
            {
                let next = parse_json_text(&Value::String(text.to_owned()));
                if next != value {
                    value = next;
                    continue;
                }
            }
        }
        if let Some(result) = object.get("result") {
            let next = parse_json_text(result);
            if next != value {
                value = next;
                continue;
            }
        }
        break;
    }
    if value.is_string() {
        anyhow::bail!("Robinhood MCP payload is not structured JSON");
    }
    Ok(value)
}

pub fn field_coverage(raw: &Value, keys: &[&str]) -> Result<String> {
    let payload = normalize_mcp_payload(raw)?;
    let mut found = false;
    for key in keys {
        let Some(value) = find_value(&payload, key) else {
            continue;
        };
        found = true;
        let is_empty = match value {
            Value::Array(values) => values.is_empty(),
            Value::Object(object) => object.is_empty(),
            Value::String(text) => text.trim().is_empty(),
            Value::Null => true,
            _ => false,
        };
        if !is_empty {
            return Ok("present".to_owned());
        }
    }
    Ok(if found { "empty" } else { "missing" }.to_owned())
}

fn parse_balances(
    payload: &Value,
    captured_at: &str,
    account_number: Option<&str>,
) -> Vec<BalanceRecord> {
    let object = find_value(payload, "balances")
        .or_else(|| find_value(payload, "balance"))
        .and_then(Value::as_object)
        .or_else(|| payload.as_object());
    let Some(object) = object else {
        return Vec::new();
    };
    let nested_value = Value::Object(object.clone());
    let record = BalanceRecord {
        captured_at: captured_at.to_owned(),
        account_number: find_string(object, &["account_number", "accountNumber"])
            .or_else(|| find_string_any(payload, &["account_number", "accountNumber"]))
            .or_else(|| account_number.map(ToOwned::to_owned)),
        cash_usd: find_number_any(
            payload,
            &["cash_usd", "cash", "cash_balance", "cashBalance"],
        )
        .or_else(|| {
            find_number_any(
                &nested_value,
                &["cash_usd", "cash", "cash_balance", "cashBalance"],
            )
        }),
        buying_power_usd: find_number_any(
            payload,
            &["buying_power_usd", "buying_power", "buyingPower"],
        ),
        unleveraged_buying_power_usd: find_number_any(
            payload,
            &[
                "unleveraged_buying_power_usd",
                "unleveraged_buying_power",
                "unleveragedBuyingPower",
            ],
        ),
        equity_usd: find_number_any(
            payload,
            &[
                "equity_usd",
                "equity",
                "equity_value",
                "account_equity",
                "accountEquity",
            ],
        ),
        margin_used_usd: find_number_any(
            payload,
            &["margin_used_usd", "margin_used", "marginUsed"],
        ),
        unsettled_funds_usd: find_number_any(
            payload,
            &["unsettled_funds_usd", "unsettled_funds", "unsettledFunds"],
        ),
        raw: nested_value,
    };
    if [
        record.cash_usd,
        record.buying_power_usd,
        record.equity_usd,
        record.margin_used_usd,
        record.unsettled_funds_usd,
    ]
    .iter()
    .all(Option::is_none)
    {
        Vec::new()
    } else {
        vec![record]
    }
}

fn parse_positions(
    payload: &Value,
    captured_at: &str,
    account_number: Option<&str>,
) -> Vec<PositionRecord> {
    let Some(array) = find_array(
        payload,
        &[
            "positions",
            "holdings",
            "portfolio_positions",
            "portfolioPositions",
        ],
    ) else {
        return Vec::new();
    };
    array
        .iter()
        .filter_map(Value::as_object)
        .map(|object| {
            let value = Value::Object(object.clone());
            PositionRecord {
                captured_at: captured_at.to_owned(),
                account_number: find_string(object, &["account_number", "accountNumber"])
                    .or_else(|| account_number.map(ToOwned::to_owned)),
                symbol: find_string(
                    object,
                    &["symbol", "ticker", "instrument_symbol", "instrumentSymbol"],
                ),
                instrument_id: find_string(
                    object,
                    &["instrument_id", "instrumentId", "id", "instrument"],
                ),
                asset_class: find_string(object, &["asset_class", "assetClass", "type"]),
                quantity: find_number_any(&value, &["quantity", "shares", "units"]),
                average_cost_usd: find_number_any(
                    &value,
                    &[
                        "average_cost_usd",
                        "average_cost",
                        "averageCost",
                        "cost_basis",
                    ],
                ),
                market_value_usd: find_number_any(
                    &value,
                    &["market_value_usd", "market_value", "marketValue", "value"],
                ),
                current_price_usd: find_number_any(
                    &value,
                    &[
                        "current_price_usd",
                        "current_price",
                        "currentPrice",
                        "price",
                    ],
                ),
                unrealized_pnl_usd: find_number_any(
                    &value,
                    &["unrealized_pnl_usd", "unrealized_pnl", "unrealizedPnl"],
                ),
                raw: value,
            }
        })
        .collect()
}

fn payload_object(value: &Value) -> Option<&Map<String, Value>> {
    let object = value.as_object()?;
    for key in ["data", "result", "portfolio", "order", "execution"] {
        if let Some(nested) = object.get(key).and_then(Value::as_object) {
            return Some(nested);
        }
    }
    Some(object)
}

fn find_value<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(object) => {
            if let Some(found) = object.get(key) {
                return Some(found);
            }
            object.values().find_map(|child| find_value(child, key))
        }
        Value::Array(values) => values.iter().find_map(|child| find_value(child, key)),
        _ => None,
    }
}

fn find_array<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Vec<Value>> {
    keys.iter()
        .find_map(|key| find_value(value, key).and_then(Value::as_array))
}

fn find_number_any(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| find_value(value, key).and_then(number_value))
}

fn find_string_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| find_value(value, key).and_then(string_value))
}

fn find_number(object: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(number_value))
}

fn find_string(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(string_value))
}

fn find_bool(object: &Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| {
        object.get(*key).and_then(|value| match value {
            Value::Bool(value) => Some(*value),
            Value::String(value) => match value.to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        })
    })
}

fn string_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn number_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
}

pub fn parse_json_text(value: &Value) -> Value {
    value
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or_else(|| value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Value {
        serde_json::from_str(match name {
            "accounts" => include_str!("../tests/fixtures/get_accounts.json"),
            "portfolio" => include_str!("../tests/fixtures/get_portfolio.json"),
            "pnl" => include_str!("../tests/fixtures/get_realized_pnl.json"),
            "history" => include_str!("../tests/fixtures/get_pnl_trade_history.json"),
            _ => panic!("unknown fixture"),
        })
        .unwrap()
    }

    #[test]
    fn parses_accounts_fixture() {
        let accounts = AccountRecord::from_value(&fixture("accounts")).unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].account_number, "account-1");
        assert_eq!(accounts[0].rhc_account_number.as_deref(), Some("crypto-1"));
        assert_eq!(accounts[1].agentic_allowed, Some(true));
    }

    #[test]
    fn requires_exactly_one_agentic_account() {
        let accounts = AccountRecord::from_value(&fixture("accounts")).unwrap();
        assert_eq!(single_agentic_account(&accounts).unwrap(), "account-2");
        assert!(single_agentic_account(&[]).is_err());
        let mut multiple = accounts.clone();
        multiple[0].agentic_allowed = Some(true);
        assert!(single_agentic_account(&multiple).is_err());
    }

    #[test]
    fn parses_portfolio_fixture_into_balances_and_positions() {
        let (snapshot, balances, positions) =
            PortfolioSnapshot::from_value_for_account(&fixture("portfolio"), Some("account-1"))
                .unwrap();
        assert_eq!(snapshot.total_value_usd, Some(1200.5));
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].unleveraged_buying_power_usd, Some(350.0));
        assert_eq!(balances[0].equity_usd, Some(1200.5));
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].symbol.as_deref(), Some("TEST"));
    }

    #[test]
    fn parses_pnl_fixtures() {
        let pnl = PnlSnapshot::from_value(&fixture("pnl"), Some("account-1")).unwrap();
        assert_eq!(pnl.realized_pnl_usd, Some(12.34));
        assert_eq!(pnl.total_rate_of_return, Some(0.12));
        assert_eq!(pnl.number_of_trades, Some(1));
        let trades = PnlTradeRecord::from_value(&fixture("history"), Some("account-1")).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol.as_deref(), Some("TEST"));
    }

    #[test]
    fn rejects_error_envelopes() {
        assert!(normalize_mcp_payload(&serde_json::json!({"isError": true})).is_err());
    }

    #[test]
    fn distinguishes_missing_empty_and_present_collections() {
        assert_eq!(
            field_coverage(&serde_json::json!({"data": {"trades": []}}), &["trades"]).unwrap(),
            "empty"
        );
        assert_eq!(
            field_coverage(&serde_json::json!({"data": {}}), &["trades"]).unwrap(),
            "missing"
        );
        assert_eq!(
            field_coverage(
                &serde_json::json!({"data": {"trades": [{"id": "trade-1"}]}}),
                &["trades"]
            )
            .unwrap(),
            "present"
        );
        assert_eq!(
            field_coverage(
                &serde_json::json!({
                    "data": {
                        "data_points": [{"realized_gain": null}],
                        "total_returns": "0"
                    }
                }),
                &["realized_gain", "realized_pnl", "total_returns"]
            )
            .unwrap(),
            "present"
        );
    }

    #[test]
    fn accepts_empty_trade_history_as_a_valid_zero_state() {
        let trades = PnlTradeRecord::from_value(
            &serde_json::json!({"data": {"trades": []}}),
            Some("account-1"),
        )
        .unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn preserves_legacy_alias_parsing() {
        let snapshot = PortfolioSnapshot::from_value(&serde_json::json!({
            "data": {"portfolio_value": "1200.50", "buyingPower": 400}
        }))
        .unwrap();
        assert_eq!(snapshot.total_value_usd, Some(1200.50));
        assert_eq!(snapshot.buying_power_usd, Some(400.0));
    }

    #[test]
    fn requires_symbol_and_side_for_execution() {
        let execution = ExecutionRecord::from_value(&serde_json::json!({
            "order": {"symbol": "ABC", "side": "buy", "quantity": "2"}
        }))
        .unwrap();
        assert_eq!(execution.symbol, "ABC");
        assert_eq!(execution.quantity, Some(2.0));
        assert!(ExecutionRecord::from_value(&serde_json::json!({"symbol": "ABC"})).is_err());
    }

    #[test]
    fn parses_json_encoded_tool_output() {
        let parsed = parse_json_text(&Value::String("{\"symbol\":\"ABC\"}".to_owned()));
        assert_eq!(parsed["symbol"], "ABC");
    }
}
