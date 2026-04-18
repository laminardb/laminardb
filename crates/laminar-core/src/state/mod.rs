//! State backend abstraction.
//!
//! A single [`StateBackend`] trait lets operators write partial
//! aggregates and publish watermarks without knowing whether they are
//! running embedded in-process, on a single node's local disk, or
//! against a shared object store.
//!
//! Four concrete backends are planned, each selected by
//! [`config::StateBackendConfig`]:
//!
//! | Backend           | Mode                          | Storage                 |
//! |-------------------|-------------------------------|-------------------------|
//! | `InProcessBackend`| Embedded, single process      | In-memory               |
//! | `LocalBackend`    | Standalone, single node       | mmap + local FS         |
//! | `ObjectStoreBackend` | Distributed-embedded       | S3/GCS/Azure            |
//! | `DistributedEmbeddedBackend` | Static or dynamic  | Object store + gossip    |
//!
//! The engine only ever holds `Arc<dyn StateBackend>`. Migrating
//! between modes is a config edit; application code is unchanged.

pub mod backend;
pub mod config;
pub mod in_process;
pub mod local;
pub mod object_store;
pub mod vnode;

pub use backend::{StateBackend, StateBackendError};
pub use config::{
    DiscoveryMode, StateBackendBuildError, StateBackendConfig, DEFAULT_VNODE_CAPACITY,
};
pub use in_process::InProcessBackend;
pub use local::LocalBackend;
pub use object_store::ObjectStoreBackend;
pub use vnode::{key_hash, owned_vnodes, round_robin_assignment, NodeId, VnodeRegistry};
