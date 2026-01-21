//! # LaminarDB Storage
//!
//! Durability layer for LaminarDB - WAL, checkpointing, and lakehouse integration.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Write-ahead log implementation
pub mod wal {
    //! WAL for durability and exactly-once semantics
}

/// Checkpointing for state persistence
pub mod checkpoint {
    //! Incremental and full checkpointing
}

/// Lakehouse format integration
pub mod lakehouse {
    //! Delta Lake and Iceberg sink support
}