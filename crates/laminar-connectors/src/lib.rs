//! # `LaminarDB` Connectors
//!
//! External system connectors and the Connector SDK for streaming data
//! in and out of `LaminarDB`.
//!
//! ## Connector SDK
//!
//! The SDK provides traits and utilities for building connectors:
//!
//! - [`connector`] - Core traits (`SourceConnector`, `SinkConnector`)
//! - [`serde`] - Serialization framework (JSON, CSV, Debezium)
//! - [`registry`] - Factory pattern for connector instantiation
//! - [`runtime`] - Lifecycle management
//! - [`testing`] - Mock connectors and test utilities
//!
//! ## Architecture
//!
//! ```text
//! Ring 0: Hot Path
//!   Source<T>::push_arrow() <-- deserialized RecordBatch
//!   Subscription::poll()   --> RecordBatch for serialization
//!
//! Ring 1: Connectors
//!   SourceConnector(poll) -> Serde(deser) -> push_arrow
//!   SinkConnector(write)  <- Serde(ser)   <- subscription(poll)
//! ```

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Common test patterns that are acceptable
#![cfg_attr(
    test,
    allow(
        clippy::field_reassign_with_default,
        clippy::float_cmp,
        clippy::manual_let_else,
        clippy::needless_return,
        clippy::unreadable_literal,
        clippy::approx_constant,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        clippy::no_effect_underscore_binding,
        unused_mut
    )
)]

// ── Connector SDK ──

/// Connector error types.
pub mod error;

/// Connector configuration types.
pub mod config;

/// Core connector traits (`SourceConnector`, `SinkConnector`).
pub mod connector;

/// Connector checkpoint types.
pub mod checkpoint;

/// Typed checkpoint extension for source connectors (F-E2E-001).
pub mod checkpoint_typed;

/// Connector health status types.
pub mod health;

/// Connector metrics types.
pub mod metrics;

/// Record serialization and deserialization framework.
pub mod serde;

/// Connector registry with factory pattern.
pub mod registry;

/// Connector runtime for lifecycle management.
pub mod runtime;

/// Testing utilities (mock connectors, helpers).
pub mod testing;

// ── Existing Modules ──

/// Kafka source and sink connectors.
#[cfg(feature = "kafka")]
pub mod kafka;

/// Change Data Capture connectors for databases.
pub mod cdc;

/// PostgreSQL sink connector.
#[cfg(feature = "postgres-sink")]
pub mod postgres;

/// Lookup table support for enrichment joins.
pub mod lookup;

/// Connector bridge for DAG source/sink integration.
pub mod bridge;

/// Lakehouse connectors (Delta Lake, Iceberg).
pub mod lakehouse;

/// Cloud storage infrastructure (credential resolution, validation, secret masking).
pub mod storage;

/// Connector SDK - developer tooling and operational resilience.
pub mod sdk;

/// Reference table source trait and refresh modes.
pub mod reference;

/// WebSocket source and sink connectors.
#[cfg(feature = "websocket")]
pub mod websocket;
