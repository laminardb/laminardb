//! Thread-per-core connector pipeline.
//!
//! Each source connector runs on a dedicated I/O thread with a
//! single-threaded tokio runtime, pushing events through SPSC queues
//! to CPU-pinned core threads. The `TpcPipelineCoordinator` drains
//! core outboxes, runs SQL execution cycles, routes results to sinks,
//! and manages checkpoint barriers.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────┐     ┌──────────┐     ┌──────────┐
//! │ Source 0  │     │ Source 1  │     │ Source N  │   I/O threads
//! │ (thread)  │     │ (thread)  │     │ (thread)  │
//! └────┬─────┘     └────┬─────┘     └────┬─────┘
//!      │ SPSC           │ SPSC           │ SPSC
//!      ▼                ▼                ▼
//! ┌──────────┐     ┌──────────┐     ┌──────────┐
//! │  Core 0   │     │  Core 1   │     │  Core M   │   Pinned threads
//! └────┬─────┘     └────┬─────┘     └────┬─────┘
//!      │ SPSC           │ SPSC           │ SPSC
//!      └───────┬────────┘────────────────┘
//!              ▼
//!     ┌─────────────────────┐
//!     │ TpcPipelineCoordinator │  tokio task: SQL exec, sinks, checkpoints
//!     └─────────────────────┘
//! ```

pub mod callback;
pub mod config;
pub mod source_adapter;
#[allow(dead_code)] // Infrastructure for Phase E integration
pub mod streaming_coordinator;
pub mod tpc_coordinator;
pub mod tpc_runtime;

pub use callback::{PipelineCallback, SourceRegistration};
pub use config::PipelineConfig;
pub use streaming_coordinator::StreamingCoordinator;
pub use tpc_coordinator::TpcPipelineCoordinator;
pub use tpc_runtime::TpcRuntime;
