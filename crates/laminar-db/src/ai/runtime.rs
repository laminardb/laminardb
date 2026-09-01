//! Assembled AI subsystem: registry, provider clients, result cache, and call log.
//! Built once at startup; [`AiRuntime::resolve`] returns everything the inference
//! operator needs to run a named model.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::ai::cache::AiResultCache;
use crate::ai::call_log::AiCallLog;
use crate::ai::provider::InferenceProvider;
use crate::ai::registry::{BackendKind, ModelBackend, ModelRegistry, RegistryError};

/// Everything the inference operator needs to run one model.
#[derive(Clone)]
pub struct ResolvedModel {
    /// Backend kind (selects the adapter path).
    pub kind: BackendKind,
    /// Stable per-run integer for the result-cache key.
    pub model_id: u32,
    /// Transport client.
    pub provider: Arc<dyn InferenceProvider>,
    /// Provider-side model identifier.
    pub provider_model: String,
    /// Intrinsic labels for local classifiers.
    pub labels: Option<Vec<String>>,
}

/// Errors from resolving a model to a runnable backend.
#[derive(Debug, Error)]
pub enum AiRuntimeError {
    /// The model is not registered or cannot perform the requested task.
    #[error(transparent)]
    Registry(#[from] RegistryError),

    /// A remote model references an unconfigured provider.
    #[error("model '{model}' references provider '{provider}', which is not configured")]
    UnknownProvider {
        /// The model name.
        model: String,
        /// The missing provider name.
        provider: String,
    },

    /// Local model referenced but the local backend is not enabled.
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
    /// Assemble a runtime. `providers` is keyed by provider name; `local_provider`
    /// serves all local models. Each model gets a stable cache id.
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

    /// Model registry (backs `laminar.models`).
    #[must_use]
    pub fn registry(&self) -> &ModelRegistry {
        &self.registry
    }

    /// Shared result cache.
    #[must_use]
    pub fn cache(&self) -> &Arc<AiResultCache> {
        &self.cache
    }

    /// Call log (backs `laminar.ai_calls`).
    #[must_use]
    pub fn call_log(&self) -> &Arc<AiCallLog> {
        &self.call_log
    }

    /// Resolve a model name to a runnable backend.
    ///
    /// # Errors
    ///
    /// Returns [`AiRuntimeError`] if the model is unknown, the provider is not
    /// configured, or the local backend is unavailable.
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
    use crate::ai::provider::{
        InferenceOutputs, InferenceRequest, InferenceResponse, ProviderError, Usage,
    };
    use crate::ai::registry::{ModelEntry, Task};
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
