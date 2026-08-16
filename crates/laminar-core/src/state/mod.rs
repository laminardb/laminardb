//! Partition-key encoding and virtual-node routing.

pub mod partition_key;
mod partition_key_schema;
pub mod vnode;

pub use partition_key::{PartitionKeyCodecError, PartitionKeyCodecV1, PartitionKeySchemaV1};
pub use vnode::{
    key_hash, owned_vnodes, owners_per_domain, peer_owners, rendezvous_assignment,
    InvalidKeyGroupCount, KeyGroupCount, Locality, NodeId, VnodeAssignmentReadGuard,
    VnodeAssignmentSnapshot, VnodeRegistry, DEFAULT_KEY_GROUP_COUNT, LOCAL_NODE_ID,
    MAX_KEY_GROUP_COUNT, PARTITIONING_ABI_VERSION,
};
