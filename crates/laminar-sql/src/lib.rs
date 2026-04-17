//! LaminarDB SQL parser, planner, and DataFusion integration.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::duration_suboptimal_units)] // MSRV 1.85; from_mins/from_hours are 1.91+
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::disallowed_types)] // cold path: SQL parsing and query planning only
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]

pub mod datafusion;
pub mod error;
pub mod parser;
pub mod planner;
pub mod translator;

// Re-export key types
pub use parser::{parse_streaming_sql, StreamingStatement};
pub use planner::streaming_optimizer::{StreamingPhysicalValidator, StreamingValidatorMode};
pub use planner::StreamingPlanner;
pub use translator::{OrderOperatorConfig, WindowOperatorConfig, WindowType};

// Re-export types
pub use datafusion::execute::execute_streaming_sql;
pub use datafusion::{
    base_session_config, create_session_context, create_streaming_context_with_validator,
    register_streaming_functions, register_streaming_functions_with_watermark, DdlResult,
    QueryResult, StreamingSqlResult,
};

/// Result type for SQL operations
pub type Result<T> = std::result::Result<T, Error>;

/// SQL-specific errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SQL parsing error
    ParseError(#[from] parser::ParseError),

    /// Planning error
    PlanningError(#[from] planner::PlanningError),

    /// `DataFusion` error (translated to user-friendly messages on display)
    DataFusionError(#[from] datafusion_common::DataFusionError),

    /// Unsupported SQL feature
    UnsupportedFeature(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError(e) => write!(f, "SQL parse error: {e}"),
            Self::PlanningError(e) => write!(f, "Planning error: {e}"),
            Self::DataFusionError(e) => {
                let translated = error::translate_datafusion_error(&e.to_string());
                write!(f, "{translated}")
            }
            Self::UnsupportedFeature(msg) => {
                write!(f, "Unsupported feature: {msg}")
            }
        }
    }
}
