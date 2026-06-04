//! Backend-agnostic AI inference for `LaminarDB`.
//!
//! AI SQL functions (`ai_classify`, `ai_embed`, `ai_complete`, …) are task
//! contracts with fixed input/output. A named model from the [`ModelRegistry`](crate::ai::registry::ModelRegistry)
//! satisfies a contract; the model's backend — local ONNX or a remote LLM — is
//! hidden behind a single [`InferenceProvider`](crate::ai::provider::InferenceProvider). This crate holds the pieces
//! that know nothing about Ring 0: the registry, the provider trait, request
//! and response types, and (in later batches) the result cache, call log,
//! adapters, and concrete backends.
//!
//! Nothing here runs on the hot path. Inference is driven from a Ring 1 worker
//! off the compute thread; see the operator in `laminar-db`.

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
