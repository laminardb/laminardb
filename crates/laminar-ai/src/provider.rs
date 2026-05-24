//! The single transport abstraction over a model backend.
//!
//! An [`InferenceProvider`] does I/O only: a homogeneous batch of inputs in, a
//! homogeneous batch of outputs plus usage out. It knows nothing about SQL tasks
//! or output columns — framing a request and turning the response into a task's
//! output column is the adapter's job. Implementors: Anthropic, an
//! OpenAI-compatible provider (OpenAI / Azure / vLLM via `base_url`), and a
//! local ONNX Runtime provider.

use async_trait::async_trait;
use thiserror::Error;

use crate::registry::Task;

/// One batch of inputs to run through a model. A request is homogeneous: a
/// single task, a single model, and one input string per row in order.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceRequest {
    /// The task being performed.
    pub task: Task,
    /// Runtime model identifier — the vendor model id for remote backends, or
    /// the weight source for local backends.
    pub model: String,
    /// One input string per row, in row order.
    pub inputs: Vec<String>,
    /// Task-shaping parameters that also version the result cache.
    pub params: InferenceParams,
}

/// Knobs that shape a request and contribute to the cache's `params_version`,
/// so the same text under different parameters never collides. Generation knobs
/// (`max_tokens`, `temperature`, …) are added here as backends consume them.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InferenceParams {
    /// Candidate label set for classification (required for remote classify;
    /// for local classifiers it must match or be a subset of the model's
    /// intrinsic labels, validated at plan time).
    pub labels: Option<Vec<String>>,
}

/// Per-row outputs of a batch. Homogeneous for a given request: a classify or
/// generate batch yields text; an embed batch — or a local classifier's raw
/// logits awaiting softmax in the adapter — yields numeric vectors.
#[derive(Debug, Clone, PartialEq)]
pub enum InferenceOutputs {
    /// One text output per input row.
    Text(Vec<String>),
    /// One numeric vector per input row (embeddings, or classifier logits).
    Vectors(Vec<Vec<f32>>),
}

impl InferenceOutputs {
    /// Number of rows produced. Must equal the request's input count.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            InferenceOutputs::Text(v) => v.len(),
            InferenceOutputs::Vectors(v) => v.len(),
        }
    }

    /// Whether no rows were produced.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Token and cost accounting for a single batch call. Local backends report
/// [`Usage::ZERO`]; remote backends report what the provider charged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    /// Prompt/input tokens billed.
    pub input_tokens: u64,
    /// Completion/output tokens billed.
    pub output_tokens: u64,
    /// Metered cost in micro-USD (millionths of a dollar); 0 for local.
    pub cost_micros: u64,
}

impl Usage {
    /// Zero usage — what local, deterministic backends report.
    pub const ZERO: Usage = Usage {
        input_tokens: 0,
        output_tokens: 0,
        cost_micros: 0,
    };
}

/// The result of a batch inference call: outputs aligned 1:1 with the request's
/// inputs, plus usage.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceResponse {
    /// Per-row outputs, in input order.
    pub outputs: InferenceOutputs,
    /// Token/cost accounting for the call.
    pub usage: Usage,
}

/// Transport over a model backend. Implementors perform I/O only — no task
/// framing, no result parsing. Shared as `Arc<dyn InferenceProvider>` and
/// driven from the Ring 1 inference worker, never from Ring 0.
#[async_trait]
pub trait InferenceProvider: Send + Sync {
    /// Run one batch of inputs through the model.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError`] on transport failure, timeout, rate limiting,
    /// a malformed response, or an unsupported task.
    async fn infer_batch(
        &self,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ProviderError>;

    /// Stable backend-kind identity for logging and the `laminar.ai_calls`
    /// log (e.g. `anthropic`, `openai`, `local`). Constant per implementor.
    fn name(&self) -> &'static str;
}

/// Errors a provider can return for a batch call.
#[derive(Debug, Error)]
pub enum ProviderError {
    /// Network or connection failure.
    #[error("transport error: {0}")]
    Transport(String),

    /// The call exceeded its deadline.
    #[error("request timed out after {0} ms")]
    Timeout(u64),

    /// The provider signalled rate limiting.
    #[error("rate limited by provider")]
    RateLimited,

    /// The response could not be parsed into the expected shape.
    #[error("malformed response: {0}")]
    BadResponse(String),

    /// The provider cannot perform the requested task.
    #[error("provider does not support task '{0}'")]
    UnsupportedTask(Task),
}
