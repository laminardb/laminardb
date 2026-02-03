//! # `LaminarDB` SQL
//!
//! SQL interface for `LaminarDB` with streaming extensions.
//!
//! This crate provides:
//! - SQL parsing with streaming extensions (windows, watermarks, EMIT)
//! - Query planning and optimization via `DataFusion`
//! - Streaming-aware physical operators
//! - SQL-to-operator translation
//!
//! ## Streaming SQL Extensions
//!
//! ```sql
//! -- Tumbling window with EMIT
//! SELECT
//!   window_start,
//!   COUNT(*) as event_count
//! FROM events
//! GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
//! EMIT AFTER WATERMARK;
//!
//! -- Stream-to-stream join
//! SELECT *
//! FROM orders o
//! JOIN order_items i
//!   ON o.order_id = i.order_id
//!   AND i.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '1' HOUR;
//! ```

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]

pub mod datafusion;
pub mod parser;
pub mod planner;
pub mod translator;

// Re-export key types
pub use parser::{parse_streaming_sql, StreamingStatement};
pub use planner::StreamingPlanner;
pub use translator::{OrderOperatorConfig, WindowOperatorConfig, WindowType};

// Re-export F005B types
pub use datafusion::execute::execute_streaming_sql;
pub use datafusion::{
    register_streaming_functions, register_streaming_functions_with_watermark, DdlResult,
    QueryResult, StreamingSqlResult,
};

/// Result type for SQL operations
pub type Result<T> = std::result::Result<T, Error>;

/// SQL-specific errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SQL parsing error
    #[error("SQL parse error: {0}")]
    ParseError(#[from] parser::ParseError),

    /// Planning error
    #[error("Planning error: {0}")]
    PlanningError(#[from] planner::PlanningError),

    /// `DataFusion` error
    #[error("DataFusion error: {0}")]
    DataFusionError(#[from] datafusion_common::DataFusionError),

    /// Unsupported SQL feature
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),
}
