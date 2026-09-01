//! Backend-agnostic AI inference types: registry, provider trait, cache, call
//! log, adapters, and backends. Inference runs on a Ring 1 worker, never Ring 0.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::duration_suboptimal_units)] // MSRV 1.85; from_mins/from_hours are 1.91+
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::disallowed_types)] // cold path: registry/planning + background inference only
#![allow(clippy::doc_markdown)] // product/provider names (ONNX, OpenAI, …) read better unticked

pub mod adapter;
#[cfg(any(feature = "remote", feature = "local"))]
pub mod backends;
pub mod cache;
pub mod call_log;
pub mod provider;
pub mod registry;
pub mod runtime;

pub use adapter::{parse_response, AdapterError};
pub use cache::{
    content_hash, params_version, AiCacheKey, AiResultCache, AiResultCacheConfig, CachedOutput,
};
pub use call_log::{AiCallLog, AiCallRecord, CallOutcome};
pub use provider::{
    InferenceOutputs, InferenceParams, InferenceProvider, InferenceRequest, InferenceResponse,
    ProviderError, Usage,
};
pub use registry::{BackendKind, ModelBackend, ModelEntry, ModelRegistry, RegistryError, Task};
pub use runtime::{AiRuntime, AiRuntimeError, ResolvedModel};
