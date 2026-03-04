//! Event-driven connector pipeline.
//!
//! Replaces the monolithic polling loop in `db.rs` with a fan-out/fan-in
//! architecture where each source connector runs in its own tokio task
//! with exclusive ownership (no `Arc<Mutex>`).
//!
//! # Architecture
//!
//! ```text
//! ┌──────────┐     ┌──────────┐     ┌──────────┐
//! │ Source 0  │     │ Source 1  │     │ Source N  │   Fan-out: per-source tasks
//! │  (task)   │     │  (task)   │     │  (task)   │
//! └────┬─────┘     └────┬─────┘     └────┬─────┘
//!      │                │                │
//!      └───────┬────────┘────────────────┘
//!              ▼
//!     ┌─────────────────┐
//!     │   mpsc channel   │  Fan-in: bounded channel
//!     └───────┬─────────┘
//!             ▼
//!     ┌─────────────────┐
//!     │  Coordinator    │  Single task: SQL exec, sink routing, checkpoints
//!     └─────────────────┘
//! ```

pub mod config;
pub mod coordinator;
pub mod metrics;
pub mod source_adapter;
pub mod source_event;
pub mod source_task;
pub mod tpc_coordinator;
pub mod tpc_runtime;

pub use config::PipelineConfig;
pub use coordinator::{PipelineCallback, PipelineCoordinator, SourceRegistration};
pub use metrics::{MetricsSnapshot, SourceTaskMetrics};
pub use source_event::SourceEvent;
pub use source_task::SourceTaskHandle;
pub use tpc_coordinator::TpcPipelineCoordinator;
pub use tpc_runtime::TpcRuntime;
