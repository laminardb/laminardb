//! Shared helpers for HTTP providers: chat prompts and retry/backoff.
//! Provider-specific bodies and auth live in each provider module.

use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::ai::provider::{ProviderError, Usage};
use crate::ai::registry::Task;

/// Sum two usage records, saturating.
pub(crate) fn add_usage(a: Usage, b: Usage) -> Usage {
    Usage {
        input_tokens: a.input_tokens.saturating_add(b.input_tokens),
        output_tokens: a.output_tokens.saturating_add(b.output_tokens),
        cost_micros: a.cost_micros.saturating_add(b.cost_micros),
    }
}

/// Build the `(system, user)` chat messages for a task.
/// `Embed` never reaches here — it uses the embeddings endpoint.
pub(crate) fn chat_prompt(task: Task, input: &str, labels: Option<&[String]>) -> (String, String) {
    let system = match task {
        Task::Classify => {
            let options = labels.map(|l| l.join(", ")).unwrap_or_default();
            format!(
                "You are a text classifier. Reply with exactly one of these labels and \
                 nothing else: {options}."
            )
        }
        Task::Sentiment => "You are a sentiment scorer. Reply with only a number from -1 \
             (most negative) through 0 (neutral) to 1 (most positive), and nothing else."
            .to_string(),
        Task::Summarize => "Summarize the text concisely. Reply with the summary only.".to_string(),
        Task::Translate => "Translate the text. Reply with the translation only.".to_string(),
        Task::Extract => {
            "Extract the key fields from the text as compact JSON. Reply with JSON only."
                .to_string()
        }
        Task::Complete | Task::Gen => "Respond to the input.".to_string(),
        Task::Embed => String::new(),
    };
    (system, input.to_string())
}

/// Whether an error warrants a retry (transport failures, timeout, rate-limited).
pub(crate) fn retryable(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::Timeout(_) | ProviderError::RateLimited | ProviderError::Transport(_)
    )
}

/// Exponential backoff for retry `attempt` (1-based), capped at ~3.2 s.
pub(crate) fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1u64 << attempt.min(5)))
}

/// POST a request and decode the response, retrying transient failures with
/// backoff. `timeout_ms` labels timeout errors; the client enforces the deadline.
pub(crate) async fn post_json<R: DeserializeOwned>(
    builder: reqwest::RequestBuilder,
    max_retries: u32,
    timeout_ms: u64,
) -> Result<R, ProviderError> {
    let mut attempt = 0u32;
    loop {
        let Some(attempt_builder) = builder.try_clone() else {
            return Err(ProviderError::Transport(
                "request body is not cloneable".to_string(),
            ));
        };
        match send_once::<R>(attempt_builder, timeout_ms).await {
            Ok(response) => return Ok(response),
            Err(e) if attempt < max_retries && retryable(&e) => {
                attempt += 1;
                tokio::time::sleep(backoff(attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn send_once<R: DeserializeOwned>(
    builder: reqwest::RequestBuilder,
    timeout_ms: u64,
) -> Result<R, ProviderError> {
    let response = builder.send().await.map_err(|e| {
        if e.is_timeout() {
            ProviderError::Timeout(timeout_ms)
        } else {
            ProviderError::Transport(e.to_string())
        }
    })?;
    let status = response.status();
    if status.as_u16() == 429 {
        return Err(ProviderError::RateLimited);
    }
    if status.is_server_error() {
        return Err(ProviderError::Transport(format!("HTTP {status}")));
    }
    if !status.is_success() {
        return Err(ProviderError::BadResponse(format!("HTTP {status}")));
    }
    response
        .json::<R>()
        .await
        .map_err(|e| ProviderError::BadResponse(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_prompt_lists_labels() {
        let labels = vec!["positive".to_string(), "negative".to_string()];
        let (system, user) = chat_prompt(Task::Classify, "great quarter", Some(&labels));
        assert!(system.contains("positive, negative"));
        assert_eq!(user, "great quarter");
    }

    #[test]
    fn retry_policy() {
        assert!(retryable(&ProviderError::RateLimited));
        assert!(retryable(&ProviderError::Timeout(1000)));
        assert!(!retryable(&ProviderError::BadResponse("nope".to_string())));
        assert!(backoff(1) < backoff(3));
    }
}
