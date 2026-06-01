//! Checkpoint barrier protocol and storage.
//!
//! Coordinator-triggered barriers flow through sources to trigger consistent
//! state snapshots. The fast path is a single `AtomicU64` load (~10ns).

/// Checkpoint barrier types and cross-thread injection.
pub mod barrier;

/// Unified checkpoint manifest types
pub mod checkpoint_manifest;

/// Checkpoint persistence trait and filesystem/object store implementations
pub mod checkpoint_store;

/// Object store factory — builds S3, GCS, Azure, or local backends from URL schemes.
pub mod object_store_builder;

pub use barrier::{
    flags, BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};

pub use checkpoint_manifest::{CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint};
pub use checkpoint_store::{
    CheckpointStore, CheckpointStoreError, FileSystemCheckpointStore, ObjectStoreCheckpointStore,
    RecoveryReport, ValidationIssue, ValidationResult,
};
