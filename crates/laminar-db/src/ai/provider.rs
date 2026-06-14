//! [`InferenceProvider`] trait and request/response types.
//!
//! Providers are I/O only: inputs in, outputs + usage out. Task framing and
//! response parsing belong to the adapter.

use async_trait::async_trait;
use thiserror::Error;

use crate::ai::registry::Task;

/// One homogeneous batch of inputs for a single task and model.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceRequest {
    /// Task to perform.
    pub task: Task,
    /// Vendor model id for remote backends; weight source for local.
    pub model: String,
    /// One input string per row, in row order.
    pub inputs: Vec<String>,
    /// Task-shaping parameters; also versions the result cache.
    pub params: InferenceParams,
}

/// Parameters that shape a request and version the result cache.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InferenceParams {
    /// Candidate labels for classification. Required for remote classify;
    /// must match or be a subset of a local classifier's intrinsic labels.
    pub labels: Option<Vec<String>>,
}

/// Per-row outputs of a batch. Shape is homogeneous within a request.
#[derive(Debug, Clone, PartialEq)]
pub enum InferenceOutputs {
    /// Text outputs (classify, complete, …).
    Text(Vec<String>),
    /// Numeric vectors (embeddings, or classifier logits before adapter post-processing).
    Vectors(Vec<Vec<f32>>),
    /// Scalar sentiment scores in `[-1, 1]`. Produced by the adapter; providers
    /// never return this shape directly.
    Scores(Vec<f64>),
}

impl InferenceOutputs {
    /// Number of output rows.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            InferenceOutputs::Text(v) => v.len(),
            InferenceOutputs::Vectors(v) => v.len(),
            InferenceOutputs::Scores(v) => v.len(),
        }
    }

    /// Whether no rows are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Token and cost accounting for a batch call. Local backends report [`Usage::ZERO`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    /// Prompt/input tokens billed.
    pub input_tokens: u64,
    /// Completion/output tokens billed.
    pub output_tokens: u64,
    /// Micro-USD (millionths); 0 for local.
    pub cost_micros: u64,
}

impl Usage {
    /// Zero usage for local backends.
    pub const ZERO: Usage = Usage {
        input_tokens: 0,
        output_tokens: 0,
        cost_micros: 0,
    };
}

/// Result of a batch inference call.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceResponse {
    /// Per-row outputs, in input order.
    pub outputs: InferenceOutputs,
    /// Token/cost accounting.
    pub usage: Usage,
}

/// I/O transport over a model backend. Shared as `Arc<dyn InferenceProvider>`;
/// driven from Ring 1, never Ring 0.
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

    /// Backend name for logging (e.g. `anthropic`, `openai`, `local`).
    fn name(&self) -> &'static str;

    /// Labels intrinsic to a local classifier (`config.json` `id2label`).
    /// Returns `None` for remote providers and embedding models. Lets a lazily
    /// downloaded classifier return labels without them being known at startup.
    fn intrinsic_labels(&self, _model: &str) -> Option<Vec<String>> {
        None
    }
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
