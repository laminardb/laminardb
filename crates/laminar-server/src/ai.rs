//! Build the AI subsystem (model registry + provider clients + cache + call
//! log) from `[ai]` / `[models]` server configuration.
//!
//! Secrets are resolved here, at startup: `api_key_env` names an environment
//! variable, never the key itself. A provider's transport is its `kind` (or,
//! when omitted, inferred from the provider name): `anthropic`, `local`, or
//! otherwise an OpenAI-compatible endpoint (`openai`, Azure, vLLM, …). A single
//! `local` provider (its `cache_dir`) backs every local model; remote providers
//! are keyed by name and matched to each remote model's `provider`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use std::num::NonZeroU32;

use laminar_ai::backends::{
    local, AnthropicProvider, LocalProvider, OpenAiProvider, RateLimitedProvider,
};
use laminar_ai::{
    AiCallLog, AiResultCache, AiRuntime, InferenceProvider, ModelBackend, ModelEntry,
    ModelRegistry, Task,
};

use crate::config::{ProviderConfig, ServerConfig};
use crate::server::ServerError;

/// Retained `laminar.ai_calls` records.
const CALL_LOG_CAPACITY: usize = 10_000;

/// Build the AI runtime from configuration, or `None` if no models are
/// configured.
///
/// # Errors
///
/// Returns [`ServerError::Build`] for an unset `api_key_env`, an unknown task
/// name, a malformed model entry, or a provider client that fails to construct.
#[allow(clippy::result_large_err)] // matches the crate's ServerError convention
pub(crate) fn build_ai_runtime(
    config: &ServerConfig,
) -> Result<Option<Arc<AiRuntime>>, ServerError> {
    if config.models.is_empty() {
        return Ok(None);
    }

    // One local provider serves every local model. More than one is ambiguous
    // (which cache_dir wins?), and picking via HashMap iteration order would be
    // nondeterministic — so require exactly zero or one.
    let mut locals = config
        .ai
        .providers
        .iter()
        .filter(|(name, cfg)| provider_kind(name, cfg) == "local");
    let local = locals.next();
    if locals.next().is_some() {
        return Err(build_err(
            "more than one local AI provider configured; only one is supported".to_string(),
        ));
    }
    let local_cache_dir: Option<PathBuf> = local
        .and_then(|(_, cfg)| cfg.cache_dir.clone())
        .map(PathBuf::from);
    let local_provider: Option<Arc<dyn InferenceProvider>> = local_cache_dir
        .clone()
        .map(|dir| Arc::new(LocalProvider::new(dir)) as Arc<dyn InferenceProvider>);

    let mut providers: HashMap<String, Arc<dyn InferenceProvider>> = HashMap::new();
    for (name, provider) in &config.ai.providers {
        if let Some(client) = build_provider(name, provider)? {
            providers.insert(name.clone(), client);
        }
    }

    let mut registry = ModelRegistry::new();
    for (name, model) in &config.models {
        let tasks = model
            .task
            .tasks()
            .iter()
            .map(|t| {
                t.parse::<Task>()
                    .map_err(|e| build_err(format!("model '{name}': {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let backend = match model.kind.as_str() {
            "remote" => ModelBackend::Remote {
                provider: model.provider.clone().ok_or_else(|| {
                    build_err(format!("model '{name}': remote model requires a provider"))
                })?,
                model: model.model.clone().ok_or_else(|| {
                    build_err(format!("model '{name}': remote model requires a model id"))
                })?,
            },
            "local" => {
                let source = model.source.clone().ok_or_else(|| {
                    build_err(format!("model '{name}': local model requires a source"))
                })?;
                // Auto-derive classifier labels from the model's config.json.
                let labels = local_cache_dir
                    .as_ref()
                    .map(|dir| local::load_labels(dir, &source))
                    .filter(|l| !l.is_empty());
                ModelBackend::Local { source, labels }
            }
            other => {
                return Err(build_err(format!(
                    "model '{name}': kind must be 'local' or 'remote', got '{other}'"
                )))
            }
        };
        registry
            .register(ModelEntry {
                id: name.clone(),
                tasks,
                backend,
            })
            .map_err(|e| build_err(e.to_string()))?;
    }

    for (task_name, model_name) in &config.ai.defaults {
        let task = task_name
            .parse::<Task>()
            .map_err(|e| build_err(format!("ai.defaults: {e}")))?;
        registry.set_default(task, model_name.clone());
    }

    let cache = Arc::new(AiResultCache::with_defaults());
    let call_log = Arc::new(AiCallLog::new(CALL_LOG_CAPACITY));
    Ok(Some(Arc::new(AiRuntime::new(
        registry,
        providers,
        local_provider,
        cache,
        call_log,
    ))))
}

/// A provider's transport kind: its explicit `kind`, else inferred from the name.
fn provider_kind<'a>(name: &'a str, cfg: &'a ProviderConfig) -> &'a str {
    cfg.kind.as_deref().unwrap_or(name)
}

/// Build one remote provider client. Returns `None` for the local backend (it is
/// the runtime's separate `local_provider`, not an entry in the providers map).
#[allow(clippy::result_large_err)]
fn build_provider(
    name: &str,
    cfg: &ProviderConfig,
) -> Result<Option<Arc<dyn InferenceProvider>>, ServerError> {
    let base: Arc<dyn InferenceProvider> = match provider_kind(name, cfg) {
        "local" => return Ok(None),
        "anthropic" => {
            let key = resolve_key(name, cfg)?;
            let base_url = cfg
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.anthropic.com".to_string());
            Arc::new(
                AnthropicProvider::new(base_url, key, cfg.max_concurrency)
                    .map_err(|e| build_err(e.to_string()))?,
            )
        }
        // OpenAI-compatible (openai, Azure, vLLM, local servers via base_url).
        _ => {
            let key = resolve_key(name, cfg)?;
            let base_url = cfg
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
            Arc::new(
                OpenAiProvider::new(base_url, key, cfg.max_concurrency)
                    .map_err(|e| build_err(e.to_string()))?,
            )
        }
    };
    Ok(Some(maybe_rate_limit(base, cfg)))
}

/// Pace a provider to `requests_per_second` when configured, else leave it as-is.
fn maybe_rate_limit(
    provider: Arc<dyn InferenceProvider>,
    cfg: &ProviderConfig,
) -> Arc<dyn InferenceProvider> {
    match cfg.requests_per_second.and_then(NonZeroU32::new) {
        Some(rps) => Arc::new(RateLimitedProvider::new(provider, rps)),
        None => provider,
    }
}

/// Resolve a provider's API key from its `api_key_env` environment variable.
#[allow(clippy::result_large_err)]
fn resolve_key(name: &str, cfg: &ProviderConfig) -> Result<String, ServerError> {
    let var = cfg.api_key_env.as_deref().ok_or_else(|| {
        build_err(format!(
            "provider '{name}': remote provider requires api_key_env"
        ))
    })?;
    std::env::var(var).map_err(|_| {
        build_err(format!(
            "provider '{name}': environment variable '{var}' (api_key_env) is not set"
        ))
    })
}

fn build_err(msg: String) -> ServerError {
    ServerError::Build(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> ServerConfig {
        toml::from_str(toml).unwrap()
    }

    #[test]
    fn no_models_yields_none() {
        assert!(build_ai_runtime(&parse("[server]\n")).unwrap().is_none());
    }

    #[test]
    fn local_model_builds_without_a_key() {
        let config = parse(
            r#"
[server]
[ai.providers.local]
cache_dir = "/tmp/models"
[models.finbert]
kind = "local"
source = "hf:onnx-community/finbert"
task = "classify"
"#,
        );
        assert!(build_ai_runtime(&config).unwrap().is_some());
    }

    /// The shipped crypto-sentiment demo config parses, validates, and builds a
    /// runtime whose `sentiment` default resolves to the local backend — the demo
    /// is wired to a local ONNX model and must stay that way.
    #[test]
    fn crypto_sentiment_demo_builds_a_local_runtime() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/demos/crypto_sentiment/pipeline.toml");
        let config = crate::config::load_config(&path).expect("demo config parses and validates");
        let runtime = build_ai_runtime(&config)
            .expect("AI runtime builds")
            .expect("the demo configures a model");
        assert_eq!(
            runtime.registry().default_for(laminar_ai::Task::Sentiment),
            Some("sentiment"),
            "ai_sentiment resolves to the configured default"
        );
        assert_eq!(
            runtime.resolve("sentiment").unwrap().kind,
            laminar_ai::BackendKind::Local,
            "the demo scores sentiment on a local model"
        );
    }

    #[test]
    fn remote_model_without_env_key_fails_fast() {
        let config = parse(
            r#"
[server]
[ai.providers.openai]
api_key_env = "LAMINAR_TEST_DEFINITELY_UNSET_KEY_XYZ"
base_url = "http://localhost:1234/v1"
[models.embed]
kind = "remote"
provider = "openai"
model = "text-embedding-3-small"
task = "embed"
"#,
        );
        let Err(err) = build_ai_runtime(&config) else {
            panic!("expected an error for the unset api_key_env");
        };
        assert!(format!("{err}").contains("not set"), "{err}");
    }
}
