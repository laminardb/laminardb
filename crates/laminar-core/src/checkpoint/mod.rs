//! Checkpoint barrier protocol and storage.
//!
//! Coordinator-triggered barriers flow through sources to trigger consistent
//! state snapshots. The fast path is a single `AtomicU64` load (~10ns).

/// Checkpoint barrier types and cross-thread injection.
pub mod barrier;

/// Feature-neutral assignment certificate retained by exact checkpoint attempts.
pub mod assignment;

/// Feature-neutral leader authority retained by durable protocol records.
pub mod authority;

/// Unified checkpoint manifest types
pub mod checkpoint_manifest;

/// Checkpoint persistence trait and filesystem/object store implementations
pub mod checkpoint_store;

/// Object store factory — builds S3, GCS, Azure, or local backends from URL schemes.
pub mod object_store_builder;

/// Canonical recovery image selected by a committed cluster checkpoint.
pub mod recovery_capsule;

pub use assignment::{
    source_drain_plan_digest, source_drain_source_id, source_drain_vnode_digest, AssignmentDrainId,
    AssignmentDrainTransition, CheckpointAssignmentAdoption, CheckpointAssignmentFence,
    CheckpointParticipant, NodeDrainReceiptAggregate, SourceDrainReceipt,
    MAX_CHECKPOINT_PARTICIPANTS, SOURCE_DRAIN_RECEIPT_VERSION,
};
pub use authority::{LeaderProof, LeaderProofOwner};
pub use barrier::{
    flags, BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};

pub use checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint, PipelineIdentity,
    PIPELINE_IDENTITY_VERSION,
};
pub use checkpoint_store::{
    CheckpointStore, CheckpointStoreError, FileSystemCheckpointStore, ObjectStoreCheckpointStore,
    RecoveryReport, ValidationIssue, ValidationResult,
};
pub use recovery_capsule::{
    canonical_json_bytes, canonical_json_sha256, CheckpointWatermark, ClusterRecoveryCapsule,
    CommittedSourceHandoff, ParticipantRecoveryRef, RecoveryCapsuleRef, SourceHandoffState,
    CLUSTER_RECOVERY_CAPSULE_VERSION, MAX_RECOVERY_CAPSULE_BYTES,
};
