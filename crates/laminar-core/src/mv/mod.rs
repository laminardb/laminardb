//! Cascading materialized view registry with dependency tracking and cycle detection.

mod error;
mod registry;

pub use error::{MvError, MvState};
pub use registry::{MaterializedView, MvRegistry};

/// Suffix of the graph operator that owns a changelog view's result slices, beside the
/// view's own stream operator.
pub const VIEW_RESULT_OPERATOR_SUFFIX: &str = "::view";

/// The graph operator that owns `view`'s result slices.
#[must_use]
pub fn view_result_operator(view: &str) -> String {
    format!("{view}{VIEW_RESULT_OPERATOR_SUFFIX}")
}

#[cfg(test)]
mod tests;
