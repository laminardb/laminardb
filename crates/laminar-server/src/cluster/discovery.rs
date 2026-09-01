//! Discovery ownership: strategy dispatch, membership observation, and bounded
//! node-state announcements.
//!
//! Responsibility: wrap the two concrete discovery implementations behind one
//! enum (`Discovery` uses `async fn` and is not dyn-compatible), observe
//! membership changes for peer lifecycle logging, and bound every discovery
//! announcement so startup and shutdown cannot wedge on an unresponsive peer.
//!
//! Invariants:
//! - announcements are bounded by `DISCOVERY_ANNOUNCEMENT_TIMEOUT`; a timeout
//!   or transport failure is logged and reported, never unwrapped;
//! - `stop_discovery_with_bound` is deliberately longer than discovery's own
//!   internal graceful join plus abort settle so the outer bound cannot cancel
//!   discovery's forced cleanup at its own boundary.

use std::collections::HashMap;

use tokio::sync::watch;
use tracing::{info, warn};

use laminar_core::cluster::discovery::{
    Discovery, DiscoveryError, GossipDiscovery, NodeInfo, NodeState, StaticDiscovery,
};

use super::CLUSTER_TASK_SHUTDOWN_TIMEOUT;

const DISCOVERY_ANNOUNCEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Enum dispatch — `Discovery` trait uses `async fn` (not dyn-compatible).
pub(super) enum DiscoveryImpl {
    Static(StaticDiscovery),
    Gossip(GossipDiscovery),
}

impl DiscoveryImpl {
    pub(super) async fn start(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.start().await,
            Self::Gossip(d) => d.start().await,
        }
    }

    pub(super) async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        match self {
            Self::Static(d) => d.peers().await,
            Self::Gossip(d) => d.peers().await,
        }
    }

    pub(super) async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.announce(info).await,
            Self::Gossip(d) => d.announce(info).await,
        }
    }

    pub(super) fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        match self {
            Self::Static(d) => d.membership_watch(),
            Self::Gossip(d) => d.membership_watch(),
        }
    }

    pub(super) async fn stop(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.stop().await,
            Self::Gossip(d) => d.stop().await,
        }
    }
}

/// Watches membership changes and logs peer join/leave/crash events.
pub(super) fn spawn_membership_watcher(
    local_node_id: &str,
    mut rx: watch::Receiver<Vec<NodeInfo>>,
) -> tokio::task::JoinHandle<()> {
    let local_name = local_node_id.to_string();
    tokio::spawn(async move {
        let mut known: HashMap<u64, (String, NodeState)> = HashMap::new();
        for node in rx.borrow_and_update().iter() {
            known.insert(node.id.0, (node.name.clone(), node.state));
        }

        loop {
            if rx.changed().await.is_err() {
                // Sender dropped — discovery shut down
                info!("[{local_name}] Membership watcher stopping (discovery shut down)");
                break;
            }

            let current_peers = rx.borrow_and_update().clone();

            let mut current: HashMap<u64, (String, NodeState)> = HashMap::new();
            for node in &current_peers {
                current.insert(node.id.0, (node.name.clone(), node.state));
            }

            for (id, (name, state)) in &current {
                if !known.contains_key(id) {
                    info!(
                        "[{local_name}] Peer joined: '{}' (id={}, state={})",
                        name, id, state
                    );
                }
            }

            for (id, (name, old_state)) in &known {
                if !current.contains_key(id) {
                    if *old_state == NodeState::Suspected {
                        warn!(
                            "[{local_name}] Peer crashed: '{}' (id={}, was suspected)",
                            name, id
                        );
                    } else {
                        warn!(
                            "[{local_name}] Peer left: '{}' (id={}, was {})",
                            name, id, old_state
                        );
                    }
                }
            }

            for (id, (name, new_state)) in &current {
                if let Some((_, old_state)) = known.get(id) {
                    if old_state != new_state {
                        let level = match new_state {
                            NodeState::Suspected => "WARN",
                            NodeState::Left | NodeState::Draining => "WARN",
                            _ => "INFO",
                        };
                        if level == "WARN" {
                            warn!(
                                "[{local_name}] Peer state changed: '{}' (id={}) {} -> {}",
                                name, id, old_state, new_state
                            );
                        } else {
                            info!(
                                "[{local_name}] Peer state changed: '{}' (id={}) {} -> {}",
                                name, id, old_state, new_state
                            );
                        }
                    }
                }
            }

            known = current;
        }
    })
}

/// Announce a node state while authority may still be lost mid-flight.
/// Returns whether the process terminal token fired during the announcement.
pub(super) async fn announce_node_state_with_bound(
    discovery: &DiscoveryImpl,
    info: NodeInfo,
    terminal: &tokio_util::sync::CancellationToken,
    operation: &'static str,
) -> bool {
    let announcement = tokio::select! {
        biased;
        () = terminal.cancelled() => return true,
        result = tokio::time::timeout(
            DISCOVERY_ANNOUNCEMENT_TIMEOUT,
            discovery.announce(info),
        ) => result,
    };

    match announcement {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, operation, "Discovery announcement failed"),
        Err(_) => warn!(
            operation,
            timeout = ?DISCOVERY_ANNOUNCEMENT_TIMEOUT,
            "Discovery announcement timed out"
        ),
    }
    terminal.is_cancelled()
}

/// Announce a node state after every local authority was fenced synchronously.
/// Returns whether the withdrawal reached discovery.
pub(super) async fn announce_node_state_after_fence_with_bound(
    discovery: &DiscoveryImpl,
    info: NodeInfo,
    operation: &'static str,
) -> bool {
    match tokio::time::timeout(DISCOVERY_ANNOUNCEMENT_TIMEOUT, discovery.announce(info)).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(%error, operation, "Discovery announcement failed after authority fencing");
            false
        }
        Err(_) => {
            warn!(
                operation,
                timeout = ?DISCOVERY_ANNOUNCEMENT_TIMEOUT,
                "Discovery announcement timed out after authority fencing"
            );
            false
        }
    }
}

pub(super) async fn announce_left_after_fence_with_bound(
    discovery: &DiscoveryImpl,
    active: &NodeInfo,
    operation: &'static str,
) -> bool {
    let mut left = active.clone();
    left.state = NodeState::Left;
    announce_node_state_after_fence_with_bound(discovery, left, operation).await
}

pub(super) async fn stop_discovery_with_bound(discovery: &mut DiscoveryImpl) -> bool {
    // Discovery owns a five-second graceful join plus a one-second abort settle. The outer bound
    // is deliberately longer so it cannot cancel that forced cleanup at its own boundary.
    let timeout = CLUSTER_TASK_SHUTDOWN_TIMEOUT + std::time::Duration::from_secs(2);
    match tokio::time::timeout(timeout, discovery.stop()).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(%error, "Discovery stop error");
            false
        }
        Err(_) => {
            warn!(?timeout, "Discovery did not stop within the shutdown bound");
            false
        }
    }
}
