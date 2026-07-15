//! Checkpoint-artifact backend abstraction. Two concrete backends:
//! `InProcessBackend` for embedded single-process runs, and
//! `ObjectStoreBackend` for anything durable (local filesystem via
//! `file://`, or S3/GCS/Azure).

pub mod backend;
pub mod config;
pub mod in_process;
pub mod object_store;
pub mod vnode;

pub use backend::{
    CheckpointAttempt, CheckpointAttemptRelation, CheckpointSealInventory, SealedCommitDescriptor,
    SealedCommitDescriptorWriter, SealedVnodePartial, SealedVnodeWriter, StateBackend,
    StateBackendDurability, StateBackendError,
};
pub use config::{
    StateBackendBuildError, StateBackendConfig, DEFAULT_VNODE_CAPACITY, MAX_VNODE_CAPACITY,
};
pub use in_process::InProcessBackend;
pub use object_store::ObjectStoreBackend;
pub use vnode::{
    key_hash, owned_vnodes, owners_per_domain, peer_owners, rendezvous_assignment, Locality,
    NodeId, VnodeAssignmentReadGuard, VnodeAssignmentSnapshot, VnodeLifecycleState, VnodeRegistry,
};
