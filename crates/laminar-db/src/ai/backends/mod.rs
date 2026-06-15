//! Concrete inference backends, feature-gated so the default build has no HTTP
//! or ML weight. Transport only: task framing lives in the adapter, not here.

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
