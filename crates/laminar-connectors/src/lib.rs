//! # `LaminarDB` Connectors
//!
//! External system connectors for streaming data in and out of `LaminarDB`.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Kafka source and sink connectors
#[cfg(feature = "kafka")]
pub mod kafka;

/// Change Data Capture connectors - CDC connectors for databases
pub mod cdc;

/// Lookup table support - External lookup tables for enrichment joins
pub mod lookup;