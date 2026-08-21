//! Cascading materialized view registry with dependency tracking and cycle detection.

mod error;
mod registry;

pub use error::{MvError, MvState};
pub use registry::{MaterializedView, MvRegistry};

#[cfg(test)]
mod tests;
