//! # LaminarDB Connectors
//!
//! External system connectors for streaming data in and out of LaminarDB.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Kafka source and sink connectors
#[cfg(feature = "kafka")]
pub mod kafka;

/// Change Data Capture connectors
pub mod cdc {
    //! CDC connectors for databases
}

/// Lookup table support
pub mod lookup {
    //! External lookup tables for enrichment joins
}