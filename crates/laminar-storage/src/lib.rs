//! Checkpoint persistence and object store integration.

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
