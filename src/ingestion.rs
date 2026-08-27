use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSnapshot {
    pub captured_at: String,
    pub total_value_usd: Option<f64>,
    pub buying_power_usd: Option<f64>,
    pub realized_pnl_usd: Option<f64>,
    pub unrealized_pnl_usd: Option<f64>,
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

pub trait BrokerDataSink {
    fn ingest_portfolio_snapshot(&self, snapshot: &PortfolioSnapshot) -> Result<()>;
    fn ingest_execution(&self, execution: &ExecutionRecord, run_id: Option<&str>) -> Result<()>;
}

impl PortfolioSnapshot {
    pub fn from_value(raw: &Value) -> Result<Self> {
        let object = payload_object(raw).context("portfolio payload is not a JSON object")?;
        let total_value_usd = find_number(
            object,
            &[
                "total_value_usd",
                "total_value",
                "portfolio_value",
                "totalPortfolioValue",
            ],
        );
        let buying_power_usd =
            find_number(object, &["buying_power_usd", "buying_power", "buyingPower"]);
        let realized_pnl_usd =
            find_number(object, &["realized_pnl_usd", "realized_pnl", "realizedPnl"]);
        let unrealized_pnl_usd = find_number(
            object,
            &["unrealized_pnl_usd", "unrealized_pnl", "unrealizedPnl"],
        );

        if total_value_usd.is_none()
            && buying_power_usd.is_none()
            && realized_pnl_usd.is_none()
            && unrealized_pnl_usd.is_none()
        {
            anyhow::bail!("portfolio payload contains no recognized numeric fields");
        }

        Ok(Self {
            captured_at: Utc::now().to_rfc3339(),
            total_value_usd,
            buying_power_usd,
            realized_pnl_usd,
            unrealized_pnl_usd,
            raw: raw.clone(),
        })
    }
}

impl ExecutionRecord {
    pub fn from_value(raw: &Value) -> Result<Self> {
        let object = payload_object(raw).context("execution payload is not a JSON object")?;
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

fn payload_object(raw: &Value) -> Option<&Map<String, Value>> {
    let object = raw.as_object()?;
    for key in ["data", "result", "portfolio", "order", "execution"] {
        if let Some(nested) = object.get(key).and_then(Value::as_object) {
            return Some(nested);
        }
    }
    Some(object)
}

fn find_number(object: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(number_value))
}

fn find_string(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        object.get(*key).and_then(|value| match value {
            Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
    })
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

    #[test]
    fn normalizes_common_portfolio_aliases() {
        let snapshot = PortfolioSnapshot::from_value(&serde_json::json!({
            "data": {"portfolio_value": "1200.50", "buyingPower": 400}
        }))
        .unwrap();
        assert_eq!(snapshot.total_value_usd, Some(1200.50));
        assert_eq!(snapshot.buying_power_usd, Some(400.0));
    }

    #[test]
    fn rejects_unrecognized_portfolio_payloads() {
        assert!(PortfolioSnapshot::from_value(&serde_json::json!({"message": "ok"})).is_err());
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
