//! # `LaminarDB` Storage
//!
//! Checkpoint persistence and object store integration for `LaminarDB`.
//!
//! ## Module Overview
//!
//! - [`checkpoint_manifest`]: Unified checkpoint manifest types
//! - [`checkpoint_store`]: Checkpoint persistence trait and implementations
//! - [`object_store_builder`]: Factory for cloud/local object store backends
//!
//! **Note:** Lakehouse sinks (Delta Lake, Iceberg) are in `laminar-connectors` crate,
//! not here. This crate handles `LaminarDB`'s internal durability, not external storage formats.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::disallowed_types)] // cold path: all storage modules are off hot path

/// Unified checkpoint manifest types
pub mod checkpoint_manifest;

/// Checkpoint persistence trait and filesystem/object store implementations
pub mod checkpoint_store;

/// Object store factory — builds S3, GCS, Azure, or local backends from URL schemes.
pub mod object_store_builder;

// Re-export key types
pub use checkpoint_manifest::{CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint};
pub use checkpoint_store::{
    CheckpointStore, CheckpointStoreError, FileSystemCheckpointStore, ObjectStoreCheckpointStore,
    RecoveryReport, ValidationResult,
};
