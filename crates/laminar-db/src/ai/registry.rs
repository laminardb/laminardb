//! The model registry: the curated catalog that maps a SQL-referenced model
//! name to the backend that runs it and the tasks it can serve.
//!
//! The registry is built once at startup from server configuration and is
//! immutable thereafter. AI functions resolve `model => '<name>'` against it at
//! plan time and fail the query if the model is unknown or cannot perform the
//! requested task — never per event.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// A task contract an AI SQL function fulfils. Each `ai_*` function maps to
/// exactly one task; a model declares the subset of tasks it supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Task {
    /// Assign one label from a candidate set (`ai_classify`).
    Classify,
    /// Classify over a fixed sentiment label set (`ai_sentiment`).
    Sentiment,
    /// Produce a dense embedding vector (`ai_embed`).
    Embed,
    /// Pull structured fields out of text (`ai_extract`).
    Extract,
    /// Free-form completion (`ai_complete`).
    Complete,
    /// Summarize text (`ai_summarize`).
    Summarize,
    /// Translate text (`ai_translate`).
    Translate,
    /// Open-ended generation (`ai_gen`).
    Gen,
}

impl Task {
    /// Canonical lower-case name, matching the `task = "…"` config spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Task::Classify => "classify",
            Task::Sentiment => "sentiment",
            Task::Embed => "embed",
            Task::Extract => "extract",
            Task::Complete => "complete",
            Task::Summarize => "summarize",
            Task::Translate => "translate",
            Task::Gen => "gen",
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Task {
    type Err = RegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "classify" => Ok(Task::Classify),
            "sentiment" => Ok(Task::Sentiment),
            "embed" => Ok(Task::Embed),
            "extract" => Ok(Task::Extract),
            "complete" => Ok(Task::Complete),
            "summarize" => Ok(Task::Summarize),
            "translate" => Ok(Task::Translate),
            "gen" => Ok(Task::Gen),
            other => Err(RegistryError::UnknownTask(other.to_string())),
        }
    }
}

/// Which runtime executes a model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackendKind {
    /// Local ONNX model run in-process via tract.
    Local,
    /// Model served by a configured remote provider.
    Remote,
}

/// Backend-specific wiring for a registered model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelBackend {
    /// A local ONNX model.
    Local {
        /// Weight source — e.g. `hf:onnx-community/finbert`, or a file /
        /// object_store URI. Resolved and mmapped by the local backend.
        source: String,
        /// Intrinsic classifier labels (`id2label`), when known at
        /// registration. `None` until derived from the model's `config.json`.
        labels: Option<Vec<String>>,
    },
    /// A model served by a remote provider.
    Remote {
        /// Key into the configured providers map (e.g. `anthropic`).
        provider: String,
        /// Provider-specific model id (e.g. `claude-haiku-4-5-20251001`).
        model: String,
    },
}

/// One registered model: its name, the tasks it serves, and its backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEntry {
    /// Registry key — the name referenced as `model => '<id>'` in SQL.
    pub id: String,
    /// Tasks this model can perform.
    pub tasks: Vec<Task>,
    /// The backend that runs it.
    pub backend: ModelBackend,
}

impl ModelEntry {
    /// The backend kind (local vs remote).
    #[must_use]
    pub fn kind(&self) -> BackendKind {
        match self.backend {
            ModelBackend::Local { .. } => BackendKind::Local,
            ModelBackend::Remote { .. } => BackendKind::Remote,
        }
    }

    /// Whether this model can perform `task`.
    #[must_use]
    pub fn supports(&self, task: Task) -> bool {
        self.tasks.contains(&task)
    }

    /// Deterministic results are cacheable permanently for correctness; only
    /// local backends are deterministic.
    #[must_use]
    pub fn is_deterministic(&self) -> bool {
        matches!(self.kind(), BackendKind::Local)
    }

    /// Whether calls incur metered cost (remote) versus free (local). Costed
    /// calls are logged to `laminar.ai_calls` with tokens and cost.
    #[must_use]
    pub fn is_costed(&self) -> bool {
        matches!(self.kind(), BackendKind::Remote)
    }

    /// Intrinsic labels of a local classifier, if any.
    #[must_use]
    pub fn labels(&self) -> Option<&[String]> {
        match &self.backend {
            ModelBackend::Local { labels, .. } => labels.as_deref(),
            ModelBackend::Remote { .. } => None,
        }
    }
}

/// Curated map of model name → entry, plus a default model per task.
#[derive(Debug, Default)]
pub struct ModelRegistry {
    models: HashMap<String, ModelEntry>,
    defaults: HashMap<Task, String>,
}

impl ModelRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a model.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::DuplicateModel`] if a model with the same id is
    /// already registered.
    pub fn register(&mut self, entry: ModelEntry) -> Result<(), RegistryError> {
        if self.models.contains_key(&entry.id) {
            return Err(RegistryError::DuplicateModel(entry.id.clone()));
        }
        self.models.insert(entry.id.clone(), entry);
        Ok(())
    }

    /// Set the default model for a task (the `[ai.defaults]` config block).
    pub fn set_default(&mut self, task: Task, model: impl Into<String>) {
        self.defaults.insert(task, model.into());
    }

    /// Default model name for a task, if one is configured.
    #[must_use]
    pub fn default_for(&self, task: Task) -> Option<&str> {
        self.defaults.get(&task).map(String::as_str)
    }

    /// Resolve a model by name.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownModel`] if no model with that name is
    /// registered.
    pub fn resolve(&self, name: &str) -> Result<&ModelEntry, RegistryError> {
        self.models
            .get(name)
            .ok_or_else(|| RegistryError::UnknownModel(name.to_string()))
    }

    /// Resolve a model and confirm it supports `task`. This is the plan-time
    /// gate behind every AI function call.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownModel`] if the model is not registered,
    /// or [`RegistryError::TaskUnsupported`] if it cannot perform `task`.
    pub fn validate(&self, name: &str, task: Task) -> Result<&ModelEntry, RegistryError> {
        let entry = self.resolve(name)?;
        if entry.supports(task) {
            Ok(entry)
        } else {
            Err(RegistryError::TaskUnsupported {
                model: name.to_string(),
                task,
                supported: entry.tasks.clone(),
            })
        }
    }

    /// Number of registered models.
    #[must_use]
    pub fn len(&self) -> usize {
        self.models.len()
    }

    /// Whether no models are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    /// Iterate registered entries in unspecified order. Backs `laminar.models`.
    pub fn iter(&self) -> impl Iterator<Item = &ModelEntry> {
        self.models.values()
    }
}

/// Errors from resolving or validating a model. Surfaced at plan time.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// No model with the given name is registered.
    #[error("unknown model '{0}'")]
    UnknownModel(String),

    /// The model exists but does not support the requested task.
    #[error("model '{model}' does not support task '{task}' (supports: {supported:?})")]
    TaskUnsupported {
        /// The referenced model name.
        model: String,
        /// The task that was requested.
        task: Task,
        /// The tasks the model actually supports.
        supported: Vec<Task>,
    },

    /// A model with this id is already registered.
    #[error("model '{0}' is already registered")]
    DuplicateModel(String),

    /// A `task = "…"` string did not name a known task.
    #[error("unknown task '{0}'")]
    UnknownTask(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_classifier() -> ModelEntry {
        ModelEntry {
            id: "finbert".to_string(),
            tasks: vec![Task::Classify, Task::Sentiment],
            backend: ModelBackend::Local {
                source: "hf:onnx-community/finbert".to_string(),
                labels: Some(vec![
                    "positive".to_string(),
                    "negative".to_string(),
                    "neutral".to_string(),
                ]),
            },
        }
    }

    fn remote_llm() -> ModelEntry {
        ModelEntry {
            id: "haiku".to_string(),
            tasks: vec![Task::Classify, Task::Extract, Task::Complete],
            backend: ModelBackend::Remote {
                provider: "anthropic".to_string(),
                model: "claude-haiku-4-5-20251001".to_string(),
            },
        }
    }

    #[test]
    fn resolve_and_validate() {
        let mut reg = ModelRegistry::new();
        assert!(reg.is_empty());
        reg.register(local_classifier()).unwrap();
        reg.register(remote_llm()).unwrap();
        reg.set_default(Task::Classify, "finbert");
        assert_eq!(reg.len(), 2);

        // Unknown model.
        assert_eq!(
            reg.resolve("missing").unwrap_err(),
            RegistryError::UnknownModel("missing".to_string())
        );

        // Supported task resolves.
        assert_eq!(
            reg.validate("finbert", Task::Sentiment).unwrap().id,
            "finbert"
        );

        // Unsupported task is rejected with the supported set.
        match reg.validate("finbert", Task::Complete).unwrap_err() {
            RegistryError::TaskUnsupported {
                model,
                task,
                supported,
            } => {
                assert_eq!(model, "finbert");
                assert_eq!(task, Task::Complete);
                assert_eq!(supported, vec![Task::Classify, Task::Sentiment]);
            }
            other => panic!("unexpected error: {other}"),
        }

        assert_eq!(reg.default_for(Task::Classify), Some("finbert"));
        assert_eq!(reg.default_for(Task::Embed), None);
    }

    #[test]
    fn duplicate_registration_rejected() {
        let mut reg = ModelRegistry::new();
        reg.register(local_classifier()).unwrap();
        assert_eq!(
            reg.register(local_classifier()).unwrap_err(),
            RegistryError::DuplicateModel("finbert".to_string())
        );
    }
}
