//! OpenAI-compatible remote provider (OpenAI, Azure, vLLM, local servers).
//! Chat completions for discriminative/generative tasks; the embeddings endpoint
//! for `ai_embed` (the only provider that supports it).

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

/// OpenAI-compatible HTTP provider.
pub struct OpenAiProvider {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    max_concurrency: usize,
}

impl OpenAiProvider {
    /// Build an OpenAI-compatible provider.
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

    async fn chat(&self, request: &InferenceRequest) -> Result<InferenceResponse, ProviderError> {
        let url = format!("{}/chat/completions", self.base_url);

        // Build owned bodies first so the async closure captures by value, not by reference.
        let bodies: Vec<ChatBody> = request
            .inputs
            .iter()
            .map(|input| {
                let (system, user) =
                    chat_prompt(request.task, input, request.params.labels.as_deref());
                ChatBody {
                    model: request.model.clone(),
                    messages: vec![ChatMessage::system(system), ChatMessage::user(user)],
                }
            })
            .collect();

        let url = &url;
        let calls = bodies.into_iter().map(|body| async move {
            let builder = self.client.post(url).bearer_auth(&self.api_key).json(&body);
            let response: ChatResponse =
                post_json(builder, MAX_RETRIES, REQUEST_TIMEOUT_MS).await?;
            parse_chat(response)
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

    async fn embed(&self, request: &InferenceRequest) -> Result<InferenceResponse, ProviderError> {
        let url = format!("{}/embeddings", self.base_url);
        let body = EmbedBody {
            model: request.model.clone(),
            input: request.inputs.clone(),
        };
        let builder = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body);
        let response: EmbedResponse = post_json(builder, MAX_RETRIES, REQUEST_TIMEOUT_MS).await?;
        let (vectors, usage) = parse_embed(response);
        Ok(InferenceResponse {
            outputs: InferenceOutputs::Vectors(vectors),
            usage,
        })
    }
}

#[async_trait]
impl InferenceProvider for OpenAiProvider {
    async fn infer_batch(
        &self,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ProviderError> {
        match request.task {
            Task::Embed => self.embed(&request).await,
            _ => self.chat(&request).await,
        }
    }

    fn name(&self) -> &'static str {
        "openai"
    }
}

// --- wire shapes ---

#[derive(Serialize)]
struct ChatBody {
    model: String,
    messages: Vec<ChatMessage>,
}

#[derive(Serialize)]
struct ChatMessage {
    role: &'static str,
    content: String,
}

impl ChatMessage {
    fn system(content: String) -> Self {
        Self {
            role: "system",
            content,
        }
    }
    fn user(content: String) -> Self {
        Self {
            role: "user",
            content,
        }
    }
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
    usage: Option<TokenUsage>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

#[derive(Serialize)]
struct EmbedBody {
    model: String,
    input: Vec<String>,
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
    usage: Option<TokenUsage>,
}

#[derive(Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Deserialize)]
struct TokenUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
}

/// Extract the first choice's text and token usage. Cost is left at zero;
/// token-to-dollar conversion needs a per-model price table.
fn parse_chat(response: ChatResponse) -> Result<(String, Usage), ProviderError> {
    let content = response
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| ProviderError::BadResponse("chat response had no choices".to_string()))?;
    Ok((content, token_usage(response.usage)))
}

/// Sort embeddings by `index` (API may return out of order) and collect usage.
fn parse_embed(mut response: EmbedResponse) -> (Vec<Vec<f32>>, Usage) {
    response.data.sort_by_key(|d| d.index);
    let usage = token_usage(response.usage);
    let vectors = response.data.into_iter().map(|d| d.embedding).collect();
    (vectors, usage)
}

fn token_usage(usage: Option<TokenUsage>) -> Usage {
    usage.map_or(Usage::ZERO, |u| Usage {
        input_tokens: u.prompt_tokens,
        output_tokens: u.completion_tokens,
        cost_micros: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_body_serializes_to_messages() {
        let body = ChatBody {
            model: "gpt-x".to_string(),
            messages: vec![
                ChatMessage::system("be terse".to_string()),
                ChatMessage::user("hello".to_string()),
            ],
        };
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["model"], "gpt-x");
        assert_eq!(value["messages"][0]["role"], "system");
        assert_eq!(value["messages"][1]["content"], "hello");
    }

    #[test]
    fn parse_chat_extracts_content_and_tokens() {
        let json = r#"{
            "choices": [{"message": {"role": "assistant", "content": "positive"}}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 1}
        }"#;
        let response: ChatResponse = serde_json::from_str(json).unwrap();
        let (text, usage) = parse_chat(response).unwrap();
        assert_eq!(text, "positive");
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 1);
    }

    #[test]
    fn parse_chat_errors_without_choices() {
        let response: ChatResponse = serde_json::from_str(r#"{"choices": []}"#).unwrap();
        assert!(parse_chat(response).is_err());
    }

    #[test]
    fn parse_embed_orders_by_index() {
        let json = r#"{
            "data": [
                {"embedding": [0.3, 0.4], "index": 1},
                {"embedding": [0.1, 0.2], "index": 0}
            ],
            "usage": {"prompt_tokens": 5}
        }"#;
        let response: EmbedResponse = serde_json::from_str(json).unwrap();
        let (vectors, usage) = parse_embed(response);
        assert_eq!(vectors, vec![vec![0.1, 0.2], vec![0.3, 0.4]]);
        assert_eq!(usage.input_tokens, 5);
    }
}
