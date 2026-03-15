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
