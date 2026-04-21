//! `LaminarDB` connector framework.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::duration_suboptimal_units)] // MSRV 1.85; from_mins/from_hours are 1.91+
#![allow(clippy::module_name_repetitions)]
// Connectors are Ring 1 (cold path): std HashMap/HashSet are acceptable
// throughout config, registry, schema, checkpoint, and CDC modules.
#![allow(clippy::disallowed_types)]
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

#[macro_use]
mod macros;

/// Connector configuration types.
pub mod config;

/// Core connector traits (`SourceConnector`, `SinkConnector`).
pub mod connector;

/// Connector checkpoint types.
pub mod checkpoint;

/// Connector health status types.
pub mod health;

/// Connector metrics types.
pub mod metrics;

/// Record serialization and deserialization framework.
pub mod serde;

/// Schema inference, resolution, and evolution framework.
pub mod schema;

/// Connector registry with factory pattern.
pub mod registry;

/// Testing utilities (mock connectors, helpers).
#[cfg(any(test, feature = "testing"))]
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

/// Lakehouse connectors (Delta Lake, Iceberg).
pub mod lakehouse;

/// Cloud storage infrastructure (credential resolution, validation, secret masking).
pub mod storage;

/// Reference table source trait and refresh modes.
pub mod reference;

/// WebSocket source and sink connectors.
#[cfg(feature = "websocket")]
pub mod websocket;

/// MongoDB CDC source and sink connectors.
#[cfg(feature = "mongodb-cdc")]
pub mod mongodb;

/// OpenTelemetry OTLP/gRPC source connector.
#[cfg(feature = "otel")]
pub mod otel;

/// NATS core and JetStream source and sink connectors.
#[cfg(feature = "nats")]
pub mod nats;

/// AutoLoader-style file source and sink connectors.
#[cfg(feature = "files")]
#[allow(
    clippy::similar_names,
    clippy::cast_possible_truncation,
    clippy::must_use_candidate,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::missing_fields_in_debug,
    clippy::unnecessary_wraps,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::map_unwrap_or,
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines
)]
pub mod files;
