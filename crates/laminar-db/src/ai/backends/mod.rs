//! Concrete inference backends.
//!
//! Each backend is feature-gated so the default build carries zero HTTP or ML
//! weight. Backends are transport only: they turn an [`InferenceRequest`] into
//! provider calls and the responses back into [`InferenceOutputs`]. Task
//! framing (the chat prompt) and any numeric post-processing live in the shared
//! helpers and the adapter, not in the wire layer.
//!
//! [`InferenceRequest`]: crate::ai::provider::InferenceRequest
//! [`InferenceOutputs`]: crate::ai::provider::InferenceOutputs

#[cfg(feature = "remote")]
pub mod anthropic;
#[cfg(feature = "local")]
pub mod local;
#[cfg(feature = "remote")]
pub mod openai;
#[cfg(feature = "remote")]
pub mod rate_limited;
#[cfg(feature = "remote")]
mod remote;

#[cfg(feature = "remote")]
pub use anthropic::AnthropicProvider;
#[cfg(feature = "local")]
pub use local::LocalProvider;
#[cfg(feature = "remote")]
pub use openai::OpenAiProvider;
#[cfg(feature = "remote")]
pub use rate_limited::RateLimitedProvider;
