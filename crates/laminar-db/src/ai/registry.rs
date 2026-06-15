//! Model registry: maps SQL-referenced model names to backends and tasks.
//! Built once at startup; AI functions resolve against it at plan time.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// Task contract for an AI SQL function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Task {
    /// `ai_classify`
    Classify,
    /// `ai_sentiment`
    Sentiment,
    /// `ai_embed`
    Embed,
    /// `ai_extract`
    Extract,
    /// `ai_complete`
    Complete,
    /// `ai_summarize`
    Summarize,
    /// `ai_translate`
    Translate,
    /// `ai_gen`
    Gen,
}

impl Task {
    /// Canonical name matching the `task = "…"` config spelling.
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
    /// In-process ONNX Runtime.
    Local,
    /// Configured remote provider.
    Remote,
}

/// Backend-specific wiring for a registered model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelBackend {
    /// In-process ONNX model.
    Local {
        /// `hf:org/repo`, `file://<path>`, or a bare path.
        source: String,
        /// `id2label` from config.json; `None` until derived at load time.
        labels: Option<Vec<String>>,
    },
    /// Remote provider model.
    Remote {
        /// Key into the configured providers map.
        provider: String,
        /// Provider-specific model id.
        model: String,
    },
}

/// One registered model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEntry {
    /// Name referenced as `model => '<id>'` in SQL.
    pub id: String,
    /// Tasks this model can perform.
    pub tasks: Vec<Task>,
    /// Backend that runs it.
    pub backend: ModelBackend,
}

impl ModelEntry {
    /// Backend kind for this entry.
    #[must_use]
    pub fn kind(&self) -> BackendKind {
        match self.backend {
            ModelBackend::Local { .. } => BackendKind::Local,
            ModelBackend::Remote { .. } => BackendKind::Remote,
        }
    }

    /// Whether this model supports `task`.
    #[must_use]
    pub fn supports(&self, task: Task) -> bool {
        self.tasks.contains(&task)
    }

    /// Whether results are deterministic (local only) and permanently cacheable.
    #[must_use]
    pub fn is_deterministic(&self) -> bool {
        matches!(self.kind(), BackendKind::Local)
    }

    /// Whether calls incur metered cost (remote providers).
    #[must_use]
    pub fn is_costed(&self) -> bool {
        matches!(self.kind(), BackendKind::Remote)
    }

    /// Intrinsic labels of a local classifier, if known.
    #[must_use]
    pub fn labels(&self) -> Option<&[String]> {
        match &self.backend {
            ModelBackend::Local { labels, .. } => labels.as_deref(),
            ModelBackend::Remote { .. } => None,
        }
    }
}

/// Map of model name to entry, with optional per-task defaults.
#[derive(Debug, Default)]
pub struct ModelRegistry {
    models: HashMap<String, ModelEntry>,
    defaults: HashMap<Task, String>,
}

impl ModelRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a model.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::DuplicateModel`] if the id is already registered.
    pub fn register(&mut self, entry: ModelEntry) -> Result<(), RegistryError> {
        if self.models.contains_key(&entry.id) {
            return Err(RegistryError::DuplicateModel(entry.id.clone()));
        }
        self.models.insert(entry.id.clone(), entry);
        Ok(())
    }

    /// Set the default model for a task (`[ai.defaults]` config block).
    pub fn set_default(&mut self, task: Task, model: impl Into<String>) {
        self.defaults.insert(task, model.into());
    }

    /// Default model for a task, if configured.
    #[must_use]
    pub fn default_for(&self, task: Task) -> Option<&str> {
        self.defaults.get(&task).map(String::as_str)
    }

    /// Resolve a model by name.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownModel`] if not registered.
    pub fn resolve(&self, name: &str) -> Result<&ModelEntry, RegistryError> {
        self.models
            .get(name)
            .ok_or_else(|| RegistryError::UnknownModel(name.to_string()))
    }

    /// Resolve a model and confirm it supports `task`.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::UnknownModel`] or [`RegistryError::TaskUnsupported`].
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

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    /// Iterate entries in unspecified order. Backs `laminar.models`.
    pub fn iter(&self) -> impl Iterator<Item = &ModelEntry> {
        self.models.values()
    }
}

/// Errors from resolving or validating a model.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// No model with the given name is registered.
    #[error("unknown model '{0}'")]
    UnknownModel(String),

    /// The model exists but does not support the requested task.
    #[error("model '{model}' does not support task '{task}' (supports: {supported:?})")]
    TaskUnsupported {
        /// The model name.
        model: String,
        /// The requested task.
        task: Task,
        /// Tasks the model actually supports.
        supported: Vec<Task>,
    },

    /// A model with this id is already registered.
    #[error("model '{0}' is already registered")]
    DuplicateModel(String),

    /// A `task = "…"` config string named an unknown task.
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
