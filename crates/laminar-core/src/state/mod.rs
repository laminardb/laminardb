//! Checkpoint-artifact backend abstraction. Two concrete backends:
//! `InProcessBackend` for embedded single-process runs, and
//! `ObjectStoreBackend` for anything durable (local filesystem via
//! `file://`, or S3/GCS/Azure).

pub mod backend;
pub mod config;
pub mod in_process;
pub mod object_store;
pub mod partition_key;
pub mod schema_descriptor;
pub mod vnode;

pub use backend::{
    CheckpointAttempt, CheckpointAttemptRelation, CheckpointSealInventory, SealedCommitDescriptor,
    SealedCommitDescriptorWriter, SealedVnodePartial, SealedVnodeWriter, StateBackend,
    StateBackendDurability, StateBackendError,
};
pub use config::{StateBackendBuildError, StateBackendConfig};
pub use in_process::InProcessBackend;
pub use object_store::ObjectStoreBackend;
pub use partition_key::{PartitionKeyCodecError, PartitionKeyCodecV1, PartitionKeySchemaV1};
pub use schema_descriptor::{SchemaDescriptorV1, SCHEMA_DESCRIPTOR_VERSION};
pub use vnode::{
    key_hash, owned_vnodes, owners_per_domain, peer_owners, rendezvous_assignment,
    InvalidKeyGroupCount, KeyGroupCount, Locality, NodeId, VnodeAssignmentReadGuard,
    VnodeAssignmentSnapshot, VnodeLifecycleState, VnodeRegistry, DEFAULT_CLUSTER_KEY_GROUP_COUNT,
    LOCAL_KEY_GROUP_COUNT, MAX_KEY_GROUP_COUNT, PARTITIONING_ABI_VERSION,
};
