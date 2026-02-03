//! Connector bridge — connects DAG source/sink nodes to external connectors.
//!
//! The bridge layer provides a thin orchestration layer between the DAG
//! execution engine ([`laminar_core::dag`]) and the connector SDK
//! ([`crate::connector`]). Since both sides work with Arrow
//! [`RecordBatch`](arrow_array::RecordBatch) data, no serialization
//! is needed.
//!
//! # Architecture
//!
//! ```text
//! Ring 1 (Async)              Ring 0 (Sync)              Ring 1 (Async)
//! ┌──────────────┐    ┌────────────────────────┐    ┌──────────────┐
//! │SourceConnector│    │     DagExecutor        │    │SinkConnector │
//! │  poll_batch() │───▶│  process_event()       │───▶│ write_batch()│
//! └──────────────┘    │  take_sink_outputs()   │    └──────────────┘
//!       ▲              └────────────────────────┘          ▲
//!  DagSourceBridge                                   DagSinkBridge
//!       └─────────── ConnectorBridgeRuntime ──────────────┘
//! ```
//!
//! # Modules
//!
//! - [`source_bridge`] — Polls a source connector and feeds events into the DAG.
//! - [`sink_bridge`] — Drains DAG sink outputs and writes them to a sink connector.
//! - [`runtime`] — Orchestrates the full source → DAG → sink pipeline.
//! - [`metrics`] — Atomic counters for bridge monitoring.
//! - [`config`] — Runtime configuration.

pub mod config;
pub mod metrics;
pub mod runtime;
pub mod sink_bridge;
pub mod source_bridge;

pub use config::BridgeRuntimeConfig;
pub use metrics::{BridgeRuntimeMetrics, SinkBridgeMetrics, SourceBridgeMetrics};
pub use runtime::{ConnectorBridgeRuntime, CycleResult, RuntimeState};
pub use sink_bridge::{DagSinkBridge, FlushResult, SinkBridgeState};
pub use source_bridge::{DagSourceBridge, PollResult, SourceBridgeState};
