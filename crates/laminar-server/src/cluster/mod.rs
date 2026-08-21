//! Cluster (multi-node) mode: public facade over the startup, shutdown,
//! discovery, lease, assignment, control-KV, and service-wiring owners.
//!
//! `start_cluster`, `ClusterHandle`, and `ClusterStartupError` are the stable
//! crate-facing surface; every domain implementation lives in a child module:
//!
//! - `startup` — the ordered startup transaction and its rollback eras;
//! - `shutdown` (via `ClusterHandle`) — ordered graceful and runtime-failure
//!   shutdown;
//! - `discovery` — strategy dispatch, membership observation, announcements;
//! - `leases` — process-identity and leader lease runtimes and fencing;
//! - `assignment` — roster verification, assignment CAS, certification;
//! - `control_kv` — static and durable object-store cluster KV;
//! - `services` — control store, TLS, controller, shuffle, HTTP binding.

mod activation;
mod assignment;
mod bootstrap;
mod control_kv;
mod discovery;
mod leases;
mod services;
mod serving;
mod shutdown;
mod startup;

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::discovery::NodeInfo;
use laminar_db::LaminarDB;

use discovery::DiscoveryImpl;
use leases::{LeaderLeaseRuntime, ProcessLeaseRuntime};

pub use startup::start_cluster;

use shutdown::{
    abort_and_join_cluster_task, stop_bootstrap_rebalance_tasks, stop_rebalance_tasks,
    wait_for_cluster_task_exit, wait_for_rebalance_task_exit,
};

const PROCESS_INCARNATION_TAG: &str = "laminardb.process-incarnation";
const CLUSTER_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, thiserror::Error)]
pub enum ClusterStartupError {
    #[error("discovery failed: {0}")]
    Discovery(String),
    #[error("formation timeout: only {found} of {needed} peers discovered")]
    FormationTimeout { found: usize, needed: usize },
    #[error("engine construction failed: {0}")]
    EngineConstruction(String),
    #[error("HTTP startup failed: {0}")]
    HttpStartup(String),
    #[error("engine shutdown failed: {0}")]
    EngineShutdown(String),
    #[error("cluster authority lost: {0}")]
    AuthorityLost(String),
}

pub struct ClusterHandle {
    db: Arc<LaminarDB>,
    db_shutdown_complete: bool,
    discovery: DiscoveryImpl,
    serving_gate: Arc<crate::http::ServingGate>,
    api_handle: tokio::task::JoinHandle<()>,
    watcher_handle: Option<tokio::task::JoinHandle<()>>,
    membership_handle: tokio::task::JoinHandle<()>,
    /// This node's own membership record. Cloned and re-announced with
    /// [`laminar_core::cluster::discovery::NodeState::Draining`] on shutdown so peers stop routing
    /// to us.
    local_node: NodeInfo,
    /// Cluster control plane. `begin_drain` is called on shutdown so the leader excludes us from
    /// vnode assignment.
    cluster_controller: Arc<laminar_core::cluster::control::ClusterController>,
    /// Durable vnode assignment snapshot. Polled on shutdown to block
    /// until the leader has reassigned every vnode we own.
    snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    /// Fixed vnode cardinality used to validate the durable drain head before shutdown.
    vnode_count: u32,
    /// Cancels the leader-lease renewal loop on shutdown so a draining
    /// node stops renewing and its lease expires promptly.
    leader_lease: LeaderLeaseRuntime,
    /// Keeps the stable-node process lease renewed for the lifetime of this runtime.
    process_lease: ProcessLeaseRuntime,
    /// Snapshot watcher + leader rebalance controller tasks.
    rebalance_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Persistent shutdown signal shared with [`Self::rebalance_tasks`].
    rebalance_shutdown: tokio_util::sync::CancellationToken,
}

/// Stable numeric identity shared by cluster runtime and offline checkpoint validation.
pub(crate) fn numeric_node_id(node_id: &str) -> u64 {
    // xxhash3 is deterministic across Rust/compiler versions. Avoid the UNASSIGNED sentinel.
    let hash = xxhash_rust::xxh3::xxh3_64(node_id.as_bytes());
    if hash == 0 {
        1
    } else {
        hash
    }
}

#[cfg(test)]
mod tests;
