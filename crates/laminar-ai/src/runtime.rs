//! The assembled AI subsystem: the model registry, the provider clients that
//! back it, the shared result cache, and the call log.
//!
//! Built once from server configuration and threaded into the engine. Given a
//! model name referenced in SQL, [`AiRuntime::resolve`] returns everything the
//! inference operator needs to run it: the backend kind, a stable cache id, the
//! provider client, the provider-side model id, and any labels. The registry is
//! kept whole (it also backs the `laminar.models` catalog view), even for models
//! whose backend has no provider wired yet.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::cache::AiResultCache;
use crate::call_log::AiCallLog;
use crate::provider::InferenceProvider;
use crate::registry::{BackendKind, ModelBackend, ModelRegistry, RegistryError};

/// Everything the inference operator needs to run one model.
#[derive(Clone)]
pub struct ResolvedModel {
    /// Backend kind (selects the adapter path).
    pub kind: BackendKind,
    /// Stable per-run integer id for the result-cache key.
    pub model_id: u32,
    /// The transport client.
    pub provider: Arc<dyn InferenceProvider>,
    /// The provider-side model identifier passed in the request.
    pub provider_model: String,
    /// Intrinsic labels (local classifiers), if known.
    pub labels: Option<Vec<String>>,
}

/// Errors from resolving a model to a runnable backend.
#[derive(Debug, Error)]
pub enum AiRuntimeError {
    /// The model is not registered, or it cannot perform the requested task.
    #[error(transparent)]
    Registry(#[from] RegistryError),

    /// A remote model names a provider that was not configured.
    #[error("model '{model}' references provider '{provider}', which is not configured")]
    UnknownProvider {
        /// The model name.
        model: String,
        /// The missing provider name.
        provider: String,
    },

    /// A local model was referenced but the local backend is not available.
    #[error("model '{0}' is local, but the local backend is not enabled in this build")]
    LocalBackendUnavailable(String),
}

/// The assembled AI subsystem.
pub struct AiRuntime {
    registry: ModelRegistry,
    providers: HashMap<String, Arc<dyn InferenceProvider>>,
    local_provider: Option<Arc<dyn InferenceProvider>>,
    cache: Arc<AiResultCache>,
    call_log: Arc<AiCallLog>,
    model_ids: HashMap<String, u32>,
}

impl AiRuntime {
    /// Assemble a runtime. `providers` is keyed by provider name (matching a
    /// model's `provider`); `local_provider`, when present, serves every local
    /// model. Each registered model is assigned a stable cache id.
    #[must_use]
    pub fn new(
        registry: ModelRegistry,
        providers: impl IntoIterator<Item = (String, Arc<dyn InferenceProvider>)>,
        local_provider: Option<Arc<dyn InferenceProvider>>,
        cache: Arc<AiResultCache>,
        call_log: Arc<AiCallLog>,
    ) -> Self {
        let model_ids = registry
            .iter()
            .enumerate()
            .map(|(i, entry)| (entry.id.clone(), u32::try_from(i).unwrap_or(u32::MAX)))
            .collect();
        Self {
            registry,
            providers: providers.into_iter().collect(),
            local_provider,
            cache,
            call_log,
            model_ids,
        }
    }

    /// The model registry (backs `laminar.models` and plan-time validation).
    #[must_use]
    pub fn registry(&self) -> &ModelRegistry {
        &self.registry
    }

    /// The shared result cache.
    #[must_use]
    pub fn cache(&self) -> &Arc<AiResultCache> {
        &self.cache
    }

    /// The call log (backs `laminar.ai_calls`).
    #[must_use]
    pub fn call_log(&self) -> &Arc<AiCallLog> {
        &self.call_log
    }

    /// Resolve a model name to a runnable backend.
    ///
    /// # Errors
    ///
    /// Returns [`AiRuntimeError`] if the model is unknown, names an unconfigured
    /// provider, or is local while the local backend is unavailable.
    pub fn resolve(&self, model_name: &str) -> Result<ResolvedModel, AiRuntimeError> {
        let entry = self.registry.resolve(model_name)?;
        let model_id = self.model_ids.get(model_name).copied().unwrap_or(u32::MAX);
        match &entry.backend {
            ModelBackend::Remote { provider, model } => {
                let client = self.providers.get(provider).ok_or_else(|| {
                    AiRuntimeError::UnknownProvider {
                        model: model_name.to_string(),
                        provider: provider.clone(),
                    }
                })?;
                Ok(ResolvedModel {
                    kind: BackendKind::Remote,
                    model_id,
                    provider: Arc::clone(client),
                    provider_model: model.clone(),
                    labels: None,
                })
            }
            ModelBackend::Local { labels, source } => {
                let client = self.local_provider.as_ref().ok_or_else(|| {
                    AiRuntimeError::LocalBackendUnavailable(model_name.to_string())
                })?;
                Ok(ResolvedModel {
                    kind: BackendKind::Local,
                    model_id,
                    provider: Arc::clone(client),
                    provider_model: source.clone(),
                    labels: labels.clone(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{
        InferenceOutputs, InferenceRequest, InferenceResponse, ProviderError, Usage,
    };
    use crate::registry::{ModelEntry, Task};
    use async_trait::async_trait;

    struct Stub;

    #[async_trait]
    impl InferenceProvider for Stub {
        async fn infer_batch(
            &self,
            request: InferenceRequest,
        ) -> Result<InferenceResponse, ProviderError> {
            Ok(InferenceResponse {
                outputs: InferenceOutputs::Text(vec![String::new(); request.inputs.len()]),
                usage: Usage::ZERO,
            })
        }
        fn name(&self) -> &'static str {
            "stub"
        }
    }

    fn runtime() -> AiRuntime {
        let mut registry = ModelRegistry::new();
        registry
            .register(ModelEntry {
                id: "haiku".into(),
                tasks: vec![Task::Classify],
                backend: ModelBackend::Remote {
                    provider: "anthropic".into(),
                    model: "claude-haiku-4-5-20251001".into(),
                },
            })
            .unwrap();
        registry
            .register(ModelEntry {
                id: "finbert".into(),
                tasks: vec![Task::Classify],
                backend: ModelBackend::Local {
                    source: "hf:onnx-community/finbert".into(),
                    labels: Some(vec!["positive".into(), "negative".into()]),
                },
            })
            .unwrap();
        let mut providers: HashMap<String, Arc<dyn InferenceProvider>> = HashMap::new();
        providers.insert("anthropic".into(), Arc::new(Stub));
        AiRuntime::new(
            registry,
            providers,
            None,
            Arc::new(AiResultCache::with_defaults()),
            Arc::new(AiCallLog::with_defaults()),
        )
    }

    #[test]
    fn resolves_remote_model_to_its_provider() {
        let rt = runtime();
        let resolved = rt.resolve("haiku").unwrap();
        assert_eq!(resolved.kind, BackendKind::Remote);
        assert_eq!(resolved.provider.name(), "stub");
        assert_eq!(resolved.provider_model, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn local_model_without_backend_errors() {
        let rt = runtime();
        assert!(matches!(
            rt.resolve("finbert"),
            Err(AiRuntimeError::LocalBackendUnavailable(_))
        ));
    }

    #[test]
    fn unknown_model_errors() {
        let rt = runtime();
        assert!(matches!(
            rt.resolve("ghost"),
            Err(AiRuntimeError::Registry(RegistryError::UnknownModel(_)))
        ));
    }
}
