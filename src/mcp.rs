use anyhow::{bail, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 wire types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<Value>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A tool advertised by the Robinhood MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// Result returned by a tools/call invocation.
#[derive(Debug, Deserialize)]
pub struct ToolCallResult {
    /// Whether the tool reported an error.
    #[serde(rename = "isError", default)]
    pub is_error: bool,
    /// The content blocks returned by the tool.
    pub content: Vec<ToolContent>,
}

#[derive(Debug, Deserialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub _kind: String,
    pub text: Option<String>,
}

impl ToolCallResult {
    /// Flatten all text content blocks into a single string.
    pub fn as_text(&self) -> String {
        self.content
            .iter()
            .filter_map(|c| c.text.as_deref())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// ---------------------------------------------------------------------------
// MCP HTTP client
// ---------------------------------------------------------------------------

pub struct McpClient {
    http: Client,
    url: String,
    session_id: Option<String>,
    next_id: AtomicU64,
}

impl McpClient {
    pub fn new(url: String, access_token: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", access_token))?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        // Tell the server we speak the 2025-03-26 MCP spec
        headers.insert(
            "MCP-Protocol-Version",
            HeaderValue::from_static("2025-03-26"),
        );

        let http = Client::builder().default_headers(headers).build()?;

        Ok(Self {
            http,
            url,
            session_id: None,
            next_id: AtomicU64::new(1),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Send a JSON-RPC request and return the `result` field.
    async fn rpc(&mut self, method: &str, params: Option<Value>) -> Result<Value> {
        let id = self.next_id();
        let body = RpcRequest {
            jsonrpc: "2.0",
            id: Some(id),
            method,
            params,
        };

        let mut builder = self.http.post(&self.url).json(&body);
        if let Some(sid) = &self.session_id {
            builder = builder.header("Mcp-Session-Id", sid.as_str());
        }

        let response = builder.send().await?;

        // Capture session ID from the server if provided
        if let Some(val) = response.headers().get("Mcp-Session-Id") {
            self.session_id = Some(val.to_str()?.to_string());
        }

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Robinhood MCP HTTP {status} on method={method}: {body}");
        }

        let rpc: RpcResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("MCP response parse error on method={method}: {e}"))?;
        if let Some(err) = rpc.error {
            bail!("MCP error {}: {}", err.code, err.message);
        }
        rpc.result
            .ok_or_else(|| anyhow::anyhow!("MCP response missing result for method={method}"))
    }

    /// Send a JSON-RPC notification (no response expected).
    async fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        let body = RpcRequest {
            jsonrpc: "2.0",
            id: None,
            method,
            params,
        };
        let mut builder = self.http.post(&self.url).json(&body);
        if let Some(sid) = &self.session_id {
            builder = builder.header("Mcp-Session-Id", sid.as_str());
        }
        builder.send().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MCP lifecycle
    // -----------------------------------------------------------------------

    pub async fn initialize(&mut self) -> Result<()> {
        let params = serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "hoodrat",
                "version": env!("CARGO_PKG_VERSION")
            }
        });
        let result = self.rpc("initialize", Some(params)).await?;
        debug!("MCP initialized: {result}");
        self.notify("notifications/initialized", None).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tool operations
    // -----------------------------------------------------------------------

    pub async fn list_tools(&mut self) -> Result<Vec<McpTool>> {
        let result = self.rpc("tools/list", None).await?;
        let tools: Vec<McpTool> = serde_json::from_value(result["tools"].clone())?;
        Ok(tools)
    }

    pub async fn call_tool(&mut self, name: &str, arguments: Value) -> Result<ToolCallResult> {
        let params = serde_json::json!({
            "name": name,
            "arguments": arguments
        });
        let result = self.rpc("tools/call", Some(params)).await?;
        let call_result: ToolCallResult = serde_json::from_value(result)?;
        Ok(call_result)
    }
}
