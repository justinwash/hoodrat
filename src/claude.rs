use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use crate::mcp::McpTool;

// ---------------------------------------------------------------------------
// Anthropic messages API types
// ---------------------------------------------------------------------------

/// A single content block within a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
        content: String,
    },
    /// Catch-all for block types we don't handle (e.g. `thinking`, `image`)
    #[serde(other)]
    Unknown,
}

impl ContentBlock {
    pub fn text(s: impl Into<String>) -> Self {
        ContentBlock::Text { text: s.into() }
    }

    pub fn tool_result(tool_use_id: impl Into<String>, content: impl Into<String>) -> Self {
        ContentBlock::ToolResult {
            tool_use_id: tool_use_id.into(),
            is_error: None,
            content: content.into(),
        }
    }

    pub fn tool_result_error(tool_use_id: impl Into<String>, content: impl Into<String>) -> Self {
        ContentBlock::ToolResult {
            tool_use_id: tool_use_id.into(),
            is_error: Some(true),
            content: content.into(),
        }
    }
}

/// A message in the conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: Vec<ContentBlock>,
}

impl Message {
    pub fn user(content: Vec<ContentBlock>) -> Self {
        Message {
            role: "user".to_string(),
            content,
        }
    }

    pub fn assistant(content: Vec<ContentBlock>) -> Self {
        Message {
            role: "assistant".to_string(),
            content,
        }
    }
}

/// Tool definition forwarded to Claude in the format the API expects.
#[derive(Serialize)]
struct ClaudeTool {
    name: String,
    description: String,
    input_schema: Value,
}

/// Request body for POST /v1/messages.
#[derive(Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    tools: Vec<ClaudeTool>,
    messages: &'a [Message],
}

/// The stop reason returned by the API.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    #[serde(other)]
    Other,
}

/// Response body from POST /v1/messages.
#[derive(Debug, Deserialize)]
pub struct MessagesResponse {
    pub content: Vec<ContentBlock>,
    pub stop_reason: StopReason,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct ClaudeClient {
    http: Client,
    api_key: String,
    pub model: String,
    pub max_tokens: u32,
    pub system: String,
}

impl ClaudeClient {
    pub fn new(api_key: String, model: String, max_tokens: u32, system: String) -> Self {
        Self {
            http: Client::new(),
            api_key,
            model,
            max_tokens,
            system,
        }
    }

    pub async fn complete(
        &self,
        messages: &[Message],
        tools: &[McpTool],
    ) -> Result<MessagesResponse> {
        let claude_tools: Vec<ClaudeTool> = tools
            .iter()
            .map(|t| ClaudeTool {
                name: t.name.clone(),
                description: t.description.clone().unwrap_or_default(),
                input_schema: t.input_schema.clone(),
            })
            .collect();

        let body = MessagesRequest {
            model: &self.model,
            max_tokens: self.max_tokens,
            system: &self.system,
            tools: claude_tools,
            messages,
        };

        debug!("Sending {} messages to Claude", messages.len());

        let response = self
            .http
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Anthropic API {status}: {text}");
        }

        let resp: MessagesResponse = response.json().await?;
        debug!("Claude stop_reason: {:?}", resp.stop_reason);
        Ok(resp)
    }
}
