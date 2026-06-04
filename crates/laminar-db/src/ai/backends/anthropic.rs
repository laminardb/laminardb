//! Anthropic Messages API provider.
//!
//! Drives the discriminative and generative tasks (one bounded-concurrent call
//! per input row). Anthropic exposes no embeddings endpoint, so `ai_embed` is
//! rejected — use the OpenAI-compatible provider for embeddings.

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::ai::backends::remote::{add_usage, chat_prompt, post_json};
use crate::ai::provider::{
    InferenceOutputs, InferenceProvider, InferenceRequest, InferenceResponse, ProviderError, Usage,
};
use crate::ai::registry::Task;

const REQUEST_TIMEOUT_MS: u64 = 60_000;
const MAX_RETRIES: u32 = 2;
const ANTHROPIC_VERSION: &str = "2023-06-01";
/// Messages API requires `max_tokens`; this caps a single reply.
const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Anthropic Messages provider.
pub struct AnthropicProvider {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    max_concurrency: usize,
}

impl AnthropicProvider {
    /// Build a provider for `base_url` (e.g. `https://api.anthropic.com`),
    /// authenticating with `api_key` and issuing at most `max_concurrency`
    /// concurrent requests per batch.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Transport`] if the HTTP client cannot be built.
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        max_concurrency: usize,
    ) -> Result<Self, ProviderError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(REQUEST_TIMEOUT_MS))
            .build()
            .map_err(|e| ProviderError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            max_concurrency: max_concurrency.max(1),
        })
    }
}

#[async_trait]
impl InferenceProvider for AnthropicProvider {
    async fn infer_batch(
        &self,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ProviderError> {
        if request.task == Task::Embed {
            return Err(ProviderError::UnsupportedTask(Task::Embed));
        }

        let url = format!("{}/v1/messages", self.base_url);
        let bodies: Vec<MessageBody> = request
            .inputs
            .iter()
            .map(|input| {
                let (system, user) =
                    chat_prompt(request.task, input, request.params.labels.as_deref());
                MessageBody {
                    model: request.model.clone(),
                    max_tokens: DEFAULT_MAX_TOKENS,
                    system,
                    messages: vec![Message::user(user)],
                }
            })
            .collect();

        let url = &url;
        let calls = bodies.into_iter().map(|body| async move {
            let builder = self
                .client
                .post(url)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .json(&body);
            let response: MessageResponse =
                post_json(builder, MAX_RETRIES, REQUEST_TIMEOUT_MS).await?;
            parse_message(response)
        });

        let results: Vec<(String, Usage)> = futures::stream::iter(calls)
            .buffered(self.max_concurrency)
            .try_collect()
            .await?;

        let mut texts = Vec::with_capacity(results.len());
        let mut usage = Usage::ZERO;
        for (text, call_usage) in results {
            texts.push(text);
            usage = add_usage(usage, call_usage);
        }
        Ok(InferenceResponse {
            outputs: InferenceOutputs::Text(texts),
            usage,
        })
    }

    fn name(&self) -> &'static str {
        "anthropic"
    }
}

// --- wire shapes ---

#[derive(Serialize)]
struct MessageBody {
    model: String,
    max_tokens: u32,
    system: String,
    messages: Vec<Message>,
}

#[derive(Serialize)]
struct Message {
    role: &'static str,
    content: String,
}

impl Message {
    fn user(content: String) -> Self {
        Self {
            role: "user",
            content,
        }
    }
}

#[derive(Deserialize)]
struct MessageResponse {
    content: Vec<ContentBlock>,
    usage: Option<AnthropicUsage>,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
}

/// Take the first text content block and token usage from a messages response.
fn parse_message(response: MessageResponse) -> Result<(String, Usage), ProviderError> {
    let text = response
        .content
        .into_iter()
        .find(|b| b.kind == "text")
        .map(|b| b.text)
        .ok_or_else(|| {
            ProviderError::BadResponse("messages response had no text block".to_string())
        })?;
    let usage = response.usage.map_or(Usage::ZERO, |u| Usage {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        cost_micros: 0,
    });
    Ok((text, usage))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_body_has_system_and_max_tokens() {
        let body = MessageBody {
            model: "claude-x".to_string(),
            max_tokens: 256,
            system: "be terse".to_string(),
            messages: vec![Message::user("hello".to_string())],
        };
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["model"], "claude-x");
        assert_eq!(value["max_tokens"], 256);
        assert_eq!(value["system"], "be terse");
        assert_eq!(value["messages"][0]["role"], "user");
    }

    #[test]
    fn parse_message_takes_first_text_block() {
        let json = r#"{
            "content": [{"type": "text", "text": "positive"}],
            "usage": {"input_tokens": 9, "output_tokens": 1}
        }"#;
        let response: MessageResponse = serde_json::from_str(json).unwrap();
        let (text, usage) = parse_message(response).unwrap();
        assert_eq!(text, "positive");
        assert_eq!(usage.input_tokens, 9);
        assert_eq!(usage.output_tokens, 1);
    }

    #[test]
    fn parse_message_errors_without_text() {
        let response: MessageResponse = serde_json::from_str(r#"{"content": []}"#).unwrap();
        assert!(parse_message(response).is_err());
    }
}
