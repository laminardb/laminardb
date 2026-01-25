//! Error types for materialized view operations.

use thiserror::Error;

/// Errors that can occur during materialized view operations.
#[derive(Debug, Error)]
pub enum MvError {
    /// Attempted to register a view with a name that already exists.
    #[error("materialized view already exists: {0}")]
    DuplicateName(String),

    /// A source referenced by the view does not exist.
    #[error("source not found: {0}")]
    SourceNotFound(String),

    /// Registration would create a dependency cycle.
    #[error("dependency cycle detected involving: {0}")]
    CycleDetected(String),

    /// The materialized view was not found.
    #[error("materialized view not found: {0}")]
    ViewNotFound(String),

    /// The operator for a view was not found.
    #[error("operator not found for view: {0}")]
    OperatorNotFound(String),

    /// Cannot drop view because other views depend on it.
    #[error("cannot drop view '{0}': dependent views exist: {1:?}")]
    HasDependents(String, Vec<String>),

    /// Operator error during processing.
    #[error("operator error: {0}")]
    OperatorError(#[from] crate::operator::OperatorError),

    /// State serialization/deserialization error.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// View is in an invalid state for the requested operation.
    #[error("view '{0}' is in invalid state: {1:?}")]
    InvalidState(String, MvState),

    /// Watermark propagation error.
    #[error("watermark propagation error: {0}")]
    WatermarkError(String),
}

/// Materialized view execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MvState {
    /// View is actively processing events.
    #[default]
    Running,
    /// View is paused (e.g., waiting for dependency).
    Paused,
    /// View encountered an error.
    Error,
    /// View is being dropped.
    Dropping,
}

impl MvState {
    /// Returns true if the view is in a state that can process events.
    #[must_use]
    pub fn can_process(&self) -> bool {
        matches!(self, Self::Running)
    }

    /// Returns true if the view is in an error state.
    #[must_use]
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error)
    }
}
