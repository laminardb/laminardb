//! Streaming connector pipeline. Each source connector runs as a tokio task
//! pushing batches via crossfire mpsc to the `StreamingCoordinator`, which
//! drives SQL execution cycles, routes results to sinks, and manages
//! checkpoint barriers. See the `streaming_coordinator` submodule for the
//! runtime topology.

pub mod callback;
pub mod config;
pub mod streaming_coordinator;

pub use callback::{BarrierOutcome, PipelineCallback, SkipReason, SourceRegistration};
pub use config::PipelineConfig;
pub use streaming_coordinator::StreamingCoordinator;

use arrow::datatypes::SchemaRef;
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{JoinOperatorConfig, OrderOperatorConfig, WindowOperatorConfig};

/// Live DDL message from `LaminarDB` to the running `StreamingCoordinator`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ControlMsg {
    /// Add a new streaming query to the running pipeline.
    AddStream {
        /// Stream name.
        name: String,
        /// SQL query text.
        sql: String,
        /// EMIT clause.
        emit_clause: Option<EmitClause>,
        /// Window configuration.
        window_config: Option<WindowOperatorConfig>,
        /// ORDER BY configuration.
        order_config: Option<OrderOperatorConfig>,
        /// Per-step join configs (left-deep).
        join_config: Option<Vec<JoinOperatorConfig>>,
    },
    /// Remove a streaming query from the running pipeline.
    DropStream {
        /// Stream name to remove.
        name: String,
    },
    /// Register a source schema for queries added after `start()`.
    AddSourceSchema {
        /// Source name.
        name: String,
        /// Arrow schema.
        schema: SchemaRef,
    },
}
