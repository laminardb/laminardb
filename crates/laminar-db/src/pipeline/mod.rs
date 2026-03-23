//! Streaming connector pipeline.
//!
//! Each source connector runs as a tokio task, pushing batches via mpsc
//! channel to the `StreamingCoordinator`. The coordinator runs SQL
//! execution cycles, routes results to sinks, and manages checkpoint
//! barriers.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────┐     ┌──────────┐     ┌──────────┐
//! │ Source 0  │     │ Source 1  │     │ Source N  │   tokio tasks
//! │ (tokio)   │     │ (tokio)   │     │ (tokio)   │
//! └────┬─────┘     └────┬─────┘     └────┬─────┘
//!      │ mpsc           │ mpsc           │ mpsc
//!      └───────┬────────┘────────────────┘
//!              ▼
//!     ┌────────────────────────┐
//!     │ StreamingCoordinator   │  tokio task: SQL exec, sinks, checkpoints
//!     └────────────────────────┘
//! ```

pub mod callback;
pub mod config;
pub mod streaming_coordinator;

pub use callback::{PipelineCallback, SourceRegistration};
pub use config::PipelineConfig;
pub use streaming_coordinator::StreamingCoordinator;

use arrow::datatypes::SchemaRef;
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{OrderOperatorConfig, WindowOperatorConfig};

/// Control message sent from `LaminarDB` DDL handlers to the running
/// `StreamingCoordinator` for live schema changes (add/drop streams).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ControlMsg {
    /// Add a new streaming query to the running pipeline.
    AddStream {
        /// Stream name.
        name: String,
        /// SQL query text.
        sql: String,
        /// EMIT clause (e.g., `OnWindowClose`).
        emit_clause: Option<EmitClause>,
        /// Window configuration (tumbling, hopping, session).
        window_config: Option<WindowOperatorConfig>,
        /// ORDER BY configuration.
        order_config: Option<OrderOperatorConfig>,
    },
    /// Remove a streaming query from the running pipeline.
    DropStream {
        /// Stream name to remove.
        name: String,
    },
    /// Register a new source schema (for queries that depend on a source
    /// created after `start()`).
    AddSourceSchema {
        /// Source name.
        name: String,
        /// Arrow schema.
        schema: SchemaRef,
    },
}
