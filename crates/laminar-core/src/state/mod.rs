//! State backend abstraction. Two concrete backends:
//! `InProcessBackend` for embedded single-process runs, and
//! `ObjectStoreBackend` for anything durable (local filesystem via
//! `file://`, or S3/GCS/Azure).

pub mod backend;
pub mod config;
pub mod in_process;
pub mod object_store;
pub mod vnode;

pub use backend::{StateBackend, StateBackendError};
pub use config::{
    DiscoveryMode, StateBackendBuildError, StateBackendConfig, DEFAULT_VNODE_CAPACITY,
};
pub use in_process::InProcessBackend;
pub use object_store::ObjectStoreBackend;
pub use vnode::{key_hash, owned_vnodes, round_robin_assignment, NodeId, VnodeRegistry};
