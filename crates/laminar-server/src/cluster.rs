//! Cluster (multi-node) mode startup orchestrator.

use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStoreExt;
use tokio::sync::watch;
use tracing::{info, warn};

use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::cluster::discovery::{
    Discovery, DiscoveryError, GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo,
    NodeMetadata, NodeState, StaticDiscovery, StaticDiscoveryConfig,
};

const PROCESS_INCARNATION_TAG: &str = "laminardb.process-incarnation";
const DISCOVERY_ANNOUNCEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const CLUSTER_TASK_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Enum dispatch — `Discovery` trait uses `async fn` (not dyn-compatible).
enum DiscoveryImpl {
    Static(StaticDiscovery),
    Gossip(GossipDiscovery),
}

impl DiscoveryImpl {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.start().await,
            Self::Gossip(d) => d.start().await,
        }
    }

    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        match self {
            Self::Static(d) => d.peers().await,
            Self::Gossip(d) => d.peers().await,
        }
    }

    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.announce(info).await,
            Self::Gossip(d) => d.announce(info).await,
        }
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        match self {
            Self::Static(d) => d.membership_watch(),
            Self::Gossip(d) => d.membership_watch(),
        }
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.stop().await,
            Self::Gossip(d) => d.stop().await,
        }
    }
}

/// Watches membership changes and logs peer join/leave/crash events.
fn spawn_membership_watcher(
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

use laminar_db::{ClusterStartupDisposition, LaminarDB, Profile};

use crate::cluster_config::ClusterConfig;
use crate::config::{DiscoverySection, ServerConfig};
use crate::server;

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

struct ProcessLeaseRuntime {
    acquired: laminar_core::cluster::control::ProcessLease,
    deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    live_rx: watch::Receiver<bool>,
    shutdown: tokio_util::sync::CancellationToken,
    terminal: tokio_util::sync::CancellationToken,
    renewal_task: tokio::task::JoinHandle<()>,
    terminal_task: tokio::task::JoinHandle<()>,
    fence_task: Option<tokio::task::JoinHandle<()>>,
}

fn spawn_process_lease_terminal_monitor(
    mut live_rx: watch::Receiver<bool>,
    deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    terminal: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {
            biased;
            () = deadline.wait_until_expired() => {}
            () = async {
                loop {
                    if !*live_rx.borrow_and_update() {
                        break;
                    }
                    if live_rx.changed().await.is_err() {
                        break;
                    }
                }
            } => {}
        }
        terminal.cancel();
    })
}

struct LeaderLeaseRuntime {
    shutdown: tokio_util::sync::CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LeaderLeaseRuntime {
    fn new(
        shutdown: tokio_util::sync::CancellationToken,
        task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            shutdown,
            task: Some(task),
        }
    }

    fn cancel(&self) {
        self.shutdown.cancel();
    }

    fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.clone()
    }

    async fn stop(&mut self) {
        self.cancel();
        let Some(mut task) = self.task.take() else {
            return;
        };
        match tokio::time::timeout(PROCESS_LEASE_IO_TIMEOUT, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Err(error)) => warn!(%error, "Leader lease task failed during shutdown"),
            Err(_) => {
                task.abort();
                match tokio::time::timeout(PROCESS_LEASE_IO_TIMEOUT, &mut task).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if error.is_cancelled() => {}
                    Ok(Err(error)) => {
                        warn!(%error, "Leader lease task failed after shutdown abort")
                    }
                    Err(_) => warn!(
                        timeout = ?PROCESS_LEASE_IO_TIMEOUT,
                        "Leader lease task did not stop after abort"
                    ),
                }
            }
        }
    }
}

impl Drop for LeaderLeaseRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn revoke_process_authority(
    db: &LaminarDB,
    serving_gate: &crate::http::ServingGate,
    leader_lease_shutdown: &tokio_util::sync::CancellationToken,
    terminal: &tokio_util::sync::CancellationToken,
) {
    serving_gate.fence();
    db.revoke_cluster_authority();
    leader_lease_shutdown.cancel();
    terminal.cancel();
}

async fn cleanup_cluster_startup(
    discovery: &mut DiscoveryImpl,
    db: &LaminarDB,
    leader_lease: &mut LeaderLeaseRuntime,
    terminal: &tokio_util::sync::CancellationToken,
    force_terminal: bool,
) {
    let mut authority_lost = force_terminal || terminal.is_cancelled();
    let shutdown = db.shutdown();
    tokio::pin!(shutdown);
    let mut shutdown_result = None;
    if !authority_lost {
        tokio::select! {
            biased;
            () = terminal.cancelled() => authority_lost = true,
            result = &mut shutdown => shutdown_result = Some(result),
        }
    }

    if authority_lost {
        db.revoke_cluster_authority();
        let _ = stop_discovery_with_bound(discovery).await;
        leader_lease.stop().await;
    }

    if shutdown_result.is_none() {
        let _ = shutdown.await;
    }
    if !authority_lost {
        leader_lease.stop().await;
        let _ = stop_discovery_with_bound(discovery).await;
    }
}

impl ProcessLeaseRuntime {
    fn is_live(&self) -> bool {
        self.deadline.is_live() && *self.live_rx.borrow() && !self.renewal_task.is_finished()
    }

    fn terminal_token(&self) -> tokio_util::sync::CancellationToken {
        self.terminal.clone()
    }

    fn disarm_for_shutdown(&mut self) -> bool {
        if let Some(task) = self.fence_task.take() {
            task.abort();
        }
        self.terminal_task.abort();

        let was_live = self.deadline.is_live()
            && *self.live_rx.borrow()
            && !self.terminal.is_cancelled()
            && !self.renewal_task.is_finished();

        self.shutdown.cancel();
        self.renewal_task.abort();
        was_live
    }

    fn install_fence(
        &mut self,
        db: Arc<LaminarDB>,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        serving_gate: Arc<crate::http::ServingGate>,
        leader_lease_shutdown: tokio_util::sync::CancellationToken,
    ) {
        let terminal = self.terminal.clone();
        self.fence_task = Some(tokio::spawn(async move {
            terminal.cancelled().await;
            revoke_process_authority(&db, &serving_gate, &leader_lease_shutdown, &terminal);
            tracing::error!(
                node = controller.instance_id().0,
                "stable node identity lease lost; database intake and cluster control fenced"
            );
        }));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogStartupAuthority {
    DurableLease,
    ActivePeer,
}

async fn wait_for_catalog_startup_authority(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    timeout: std::time::Duration,
) -> Result<CatalogStartupAuthority, String> {
    let mut grants = controller
        .leader_grant_watch()
        .ok_or_else(|| "durable leader lease fencing is not installed".to_string())?;
    let mut members = controller.members_watch();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if controller.capture_catalog_bootstrap_proof().is_some() {
            return Ok(CatalogStartupAuthority::DurableLease);
        }
        if members.borrow().iter().any(|member| {
            member.id != controller.instance_id() && matches!(member.state, NodeState::Active)
        }) {
            return Ok(CatalogStartupAuthority::ActivePeer);
        }

        tokio::select! {
            () = &mut deadline => {
                return Err(format!(
                    "timed out after {timeout:?} waiting for the durable catalog bootstrap lease or an active peer"
                ));
            }
            changed = grants.changed() => {
                if changed.is_err() {
                    return Err("durable leader lease manager stopped during catalog bootstrap".into());
                }
            }
            changed = members.changed() => {
                if changed.is_err() {
                    return Err("membership discovery stopped during catalog bootstrap".into());
                }
            }
        }
    }
}

impl Drop for ProcessLeaseRuntime {
    fn drop(&mut self) {
        if let Some(task) = self.fence_task.take() {
            task.abort();
        }
        self.terminal_task.abort();
        self.shutdown.cancel();
        self.renewal_task.abort();
        self.deadline.fence();
        self.terminal.cancel();
    }
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
    /// [`NodeState::Draining`] on shutdown so peers stop routing to us.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterShutdownTrigger {
    Signal,
    ProcessLeaseLost,
}

async fn wait_for_cluster_shutdown_trigger(
    terminal: &tokio_util::sync::CancellationToken,
) -> Result<ClusterShutdownTrigger, ClusterStartupError> {
    tokio::select! {
        biased;
        () = terminal.cancelled() => Ok(ClusterShutdownTrigger::ProcessLeaseLost),
        signal = server::wait_for_termination_signal() => {
            signal.map_err(|e| {
                ClusterStartupError::Discovery(format!("signal handler: {e}"))
            })?;
            Ok(ClusterShutdownTrigger::Signal)
        }
    }
}

async fn abort_and_join_cluster_task(
    task: &mut tokio::task::JoinHandle<()>,
    task_name: &'static str,
) -> bool {
    task.abort();
    match tokio::time::timeout(CLUSTER_TASK_SHUTDOWN_TIMEOUT, task).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) if error.is_cancelled() => true,
        Ok(Err(error)) => {
            warn!(task = task_name, %error, "Cluster task failed during shutdown");
            false
        }
        Err(_) => {
            warn!(
                task = task_name,
                timeout = ?CLUSTER_TASK_SHUTDOWN_TIMEOUT,
                "Cluster task did not stop within the shutdown bound"
            );
            false
        }
    }
}

async fn announce_node_state_with_bound(
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

async fn stop_discovery_with_bound(discovery: &mut DiscoveryImpl) -> bool {
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

fn log_rebalance_task_result(result: Result<(), tokio::task::JoinError>) -> bool {
    if let Err(error) = result {
        if !error.is_cancelled() {
            warn!(%error, "Rebalance task failed during shutdown");
            return false;
        }
    }
    true
}

async fn stop_rebalance_tasks(
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    shutdown: &tokio_util::sync::CancellationToken,
) -> bool {
    shutdown.cancel();
    let mut stopped_cleanly = true;
    let graceful_deadline = tokio::time::Instant::now() + CLUSTER_TASK_SHUTDOWN_TIMEOUT;
    while !tasks.is_empty() {
        match tokio::time::timeout_at(graceful_deadline, &mut tasks[0]).await {
            Ok(result) => {
                stopped_cleanly &= log_rebalance_task_result(result);
                tasks.swap_remove(0);
            }
            Err(_) => break,
        }
    }

    if tasks.is_empty() {
        return stopped_cleanly;
    }

    for task in tasks.iter() {
        task.abort();
    }
    let abort_deadline = tokio::time::Instant::now() + CLUSTER_TASK_SHUTDOWN_TIMEOUT;
    while !tasks.is_empty() {
        match tokio::time::timeout_at(abort_deadline, &mut tasks[0]).await {
            Ok(result) => {
                stopped_cleanly &= log_rebalance_task_result(result);
                tasks.swap_remove(0);
            }
            Err(_) => {
                warn!(
                    remaining = tasks.len(),
                    timeout = ?CLUSTER_TASK_SHUTDOWN_TIMEOUT,
                    "Rebalance tasks did not stop within the shutdown bound"
                );
                stopped_cleanly = false;
                break;
            }
        }
    }
    stopped_cleanly && tasks.is_empty()
}

async fn stop_cluster_advertisement_and_admission(
    discovery: &mut DiscoveryImpl,
    membership_handle: &mut tokio::task::JoinHandle<()>,
    mut watcher_handle: Option<&mut tokio::task::JoinHandle<()>>,
    api_handle: &mut tokio::task::JoinHandle<()>,
) -> bool {
    membership_handle.abort();
    if let Some(watcher) = watcher_handle.as_ref() {
        watcher.abort();
    }
    api_handle.abort();

    let (discovery_stopped, membership_stopped, watcher_stopped, api_stopped) = tokio::join!(
        stop_discovery_with_bound(discovery),
        abort_and_join_cluster_task(membership_handle, "membership watcher"),
        async {
            if let Some(watcher) = watcher_handle.take() {
                abort_and_join_cluster_task(watcher, "configuration watcher").await
            } else {
                true
            }
        },
        abort_and_join_cluster_task(api_handle, "HTTP API server"),
    );
    discovery_stopped && membership_stopped && watcher_stopped && api_stopped
}

impl ClusterHandle {
    pub async fn wait_for_shutdown(mut self) -> Result<(), ClusterStartupError> {
        let terminal = self.process_lease.terminal_token();
        let mut authority_lost = wait_for_cluster_shutdown_trigger(&terminal).await?
            == ClusterShutdownTrigger::ProcessLeaseLost;
        self.serving_gate.fence();

        if authority_lost {
            warn!("Stable node identity lease lost; stopping cluster node");
        } else {
            info!("Received shutdown signal, shutting down cluster node...");
        }

        // Graceful drain. Discovery and the rebalance control plane must
        // stay alive here: peers need to observe our Draining state and
        // the leader needs to rotate our vnodes away before we tear down.
        //
        // 1. Announce Draining so peers stop routing to us and the
        //    leader's `assignable_instances` drops us from assignment.
        if !authority_lost {
            let mut draining = self.local_node.clone();
            draining.state = NodeState::Draining;
            authority_lost = announce_node_state_with_bound(
                &self.discovery,
                draining,
                &terminal,
                "announce draining state",
            )
            .await;

            if !authority_lost {
                // 2. Flip the local draining flag so that if we are the leader,
                //    our own rebalance controller excludes us from assignment.
                let retains_drain_leadership = self.cluster_controller.begin_drain();
                // A non-leader yields immediately. The certified current leader must keep
                // renewing until it checkpoints its own predecessor cut; the target roster
                // excludes it and transfers candidacy after the committed assignment is adopted.
                if !retains_drain_leadership {
                    self.leader_lease.cancel();
                }

                // 3. Block until the leader has reassigned every vnode we own,
                //    bounded so a stuck cluster can't wedge shutdown forever.
                let me = laminar_core::state::NodeId(self.local_node.id.0);
                let drain = async {
                    match self.cluster_controller.checkpoint_authority() {
                        Ok(authority) => {
                            laminar_db::rebalance::wait_until_drained(
                                &self.snapshot_store,
                                Some(&authority),
                                me,
                                self.vnode_count,
                                std::time::Duration::from_secs(1),
                                std::time::Duration::from_secs(30),
                            )
                            .await
                        }
                        Err(error) => {
                            warn!(%error, "Drain cannot certify assignment authority");
                            false
                        }
                    }
                };
                let drained = tokio::select! {
                    biased;
                    () = terminal.cancelled() => {
                        authority_lost = true;
                        false
                    }
                    drained = drain => drained,
                };
                if authority_lost {
                    warn!("Process lease lost while draining; switching to terminal shutdown");
                } else if drained {
                    info!("Drain complete: all owned vnodes reassigned");
                } else {
                    warn!("Drain timed out after 30s; proceeding with shutdown");
                }
            }
        }
        authority_lost |= terminal.is_cancelled();
        self.leader_lease.cancel();

        let mut external_runtime_stopped = false;
        let mut runtime_tasks_clean = true;
        if authority_lost {
            runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
                &mut self.discovery,
                &mut self.membership_handle,
                self.watcher_handle.as_mut(),
                &mut self.api_handle,
            )
            .await;
            external_runtime_stopped = true;
        }

        runtime_tasks_clean &=
            stop_rebalance_tasks(&mut self.rebalance_tasks, &self.rebalance_shutdown).await;

        authority_lost |= terminal.is_cancelled();
        if authority_lost && !external_runtime_stopped {
            runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
                &mut self.discovery,
                &mut self.membership_handle,
                self.watcher_handle.as_mut(),
                &mut self.api_handle,
            )
            .await;
            external_runtime_stopped = true;
        }

        let stop = self.leader_lease.stop();
        tokio::pin!(stop);
        if authority_lost {
            stop.await;
        } else {
            let mut leader_stopped = false;
            tokio::select! {
                biased;
                () = terminal.cancelled() => authority_lost = true,
                () = &mut stop => leader_stopped = true,
            }
            if authority_lost {
                runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
                    &mut self.discovery,
                    &mut self.membership_handle,
                    self.watcher_handle.as_mut(),
                    &mut self.api_handle,
                )
                .await;
                external_runtime_stopped = true;
            }
            if !leader_stopped {
                stop.await;
            }
        }

        authority_lost |= terminal.is_cancelled();

        if authority_lost && !external_runtime_stopped {
            runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
                &mut self.discovery,
                &mut self.membership_handle,
                self.watcher_handle.as_mut(),
                &mut self.api_handle,
            )
            .await;
            external_runtime_stopped = true;
        }

        let shutdown = self.db.shutdown();
        tokio::pin!(shutdown);
        let mut shutdown_result = None;
        if !authority_lost {
            tokio::select! {
                biased;
                () = terminal.cancelled() => authority_lost = true,
                result = &mut shutdown => shutdown_result = Some(result),
            }
        }

        if authority_lost && !external_runtime_stopped {
            runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
                &mut self.discovery,
                &mut self.membership_handle,
                self.watcher_handle.as_mut(),
                &mut self.api_handle,
            )
            .await;
            external_runtime_stopped = true;
        }

        let shutdown_result = match shutdown_result {
            Some(result) => result,
            None => shutdown.await,
        };
        self.db_shutdown_complete = shutdown_result.is_ok();

        // A graceful stop lets checkpoint tails settle before withdrawing discovery. Terminal
        // lease loss does the reverse: stop all external admission and advertisement first.
        if !external_runtime_stopped {
            runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
                &mut self.discovery,
                &mut self.membership_handle,
                self.watcher_handle.as_mut(),
                &mut self.api_handle,
            )
            .await;
        }

        authority_lost |= terminal.is_cancelled();
        authority_lost |= !self.process_lease.disarm_for_shutdown();
        if authority_lost {
            self.db.revoke_cluster_authority();
        }

        if authority_lost {
            if let Err(error) = shutdown_result {
                warn!(%error, "Database shutdown after authority loss failed");
            }
            if !runtime_tasks_clean {
                warn!("Cluster runtime cleanup was incomplete after authority loss");
            }
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease expired or was superseded".into(),
            ))
        } else {
            shutdown_result
                .map_err(|error| ClusterStartupError::EngineShutdown(error.to_string()))?;
            if !runtime_tasks_clean {
                return Err(ClusterStartupError::EngineShutdown(
                    "one or more cluster runtime tasks did not terminate cleanly".into(),
                ));
            }
            info!("Cluster node shutdown complete");
            Ok(())
        }
    }
}

impl Drop for ClusterHandle {
    fn drop(&mut self) {
        let _ = self.process_lease.disarm_for_shutdown();
        self.serving_gate.fence();
        self.db.revoke_cluster_authority();
        if !self.db_shutdown_complete {
            self.db.close();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let db = Arc::clone(&self.db);
                std::mem::drop(runtime.spawn(async move {
                    if let Err(error) = db.shutdown().await {
                        warn!(%error, "Database cleanup after cluster handle drop failed");
                    }
                }));
            }
        }
        self.leader_lease.cancel();
        for task in &self.rebalance_tasks {
            task.abort();
        }
        self.membership_handle.abort();
        if let Some(watcher) = &self.watcher_handle {
            watcher.abort();
        }
        self.api_handle.abort();
    }
}

const PROCESS_LEASE_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const PROCESS_LEASE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

fn unix_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

fn start_process_lease_runtime(
    store: Arc<laminar_core::cluster::control::ProcessLeaseStore>,
    owner: uuid::Uuid,
    config: laminar_core::cluster::control::ProcessLeaseConfig,
    acquisition_started_at: std::time::Instant,
    acquired: laminar_core::cluster::control::ProcessLease,
) -> Result<ProcessLeaseRuntime, ClusterStartupError> {
    let manager = laminar_core::cluster::control::ProcessLeaseManager::new(
        store,
        owner,
        config,
        acquisition_started_at,
        &acquired,
    )
    .map_err(|error| {
        ClusterStartupError::EngineConstruction(format!(
            "start stable node identity lease renewal: {error}"
        ))
    })?;
    let live_rx = manager.live_watch();
    let deadline = manager.deadline();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let terminal = tokio_util::sync::CancellationToken::new();
    let renewal_task = manager.spawn(shutdown.clone());
    let terminal_task = spawn_process_lease_terminal_monitor(
        live_rx.clone(),
        Arc::clone(&deadline),
        terminal.clone(),
    );
    Ok(ProcessLeaseRuntime {
        acquired,
        deadline,
        live_rx,
        shutdown,
        terminal,
        renewal_task,
        terminal_task,
        fence_task: None,
    })
}

async fn acquire_process_lease(
    store: Arc<laminar_core::cluster::control::ProcessLeaseStore>,
    owner: uuid::Uuid,
    config: laminar_core::cluster::control::ProcessLeaseConfig,
) -> Result<ProcessLeaseRuntime, ClusterStartupError> {
    use laminar_core::cluster::control::ProcessLeaseOutcome;

    let deadline = std::time::Instant::now() + PROCESS_LEASE_ACQUIRE_TIMEOUT;
    let mut last_failure = "no acquisition attempt completed".to_string();
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(ClusterStartupError::EngineConstruction(format!(
                "stable node identity lease was not acquired within {PROCESS_LEASE_ACQUIRE_TIMEOUT:?}: {last_failure}"
            )));
        }
        let attempt_timeout = PROCESS_LEASE_IO_TIMEOUT.min(remaining);
        let acquisition_started_at = std::time::Instant::now();
        match tokio::time::timeout(
            attempt_timeout,
            store.try_acquire(owner, unix_time_millis()),
        )
        .await
        {
            Ok(Ok(ProcessLeaseOutcome::Acquired(acquired))) => {
                return start_process_lease_runtime(
                    store,
                    owner,
                    config,
                    acquisition_started_at,
                    acquired,
                );
            }
            Ok(Ok(ProcessLeaseOutcome::Held(incumbent))) => {
                last_failure = format!(
                    "live boot {} owns term {} until {}",
                    incumbent.owner, incumbent.term, incumbent.expires_at_ms
                );
                let observation = store.observe_rival(&incumbent).map_err(|error| {
                    ClusterStartupError::EngineConstruction(format!(
                        "observe stable node identity lease: {error}"
                    ))
                })?;
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let observation_time = config.ttl.min(remaining);
                tokio::time::sleep(observation_time).await;
                if observation_time < config.ttl {
                    continue;
                }
                let takeover_started_at = std::time::Instant::now();
                match tokio::time::timeout(
                    PROCESS_LEASE_IO_TIMEOUT
                        .min(deadline.saturating_duration_since(std::time::Instant::now())),
                    store.try_takeover(owner, &observation, unix_time_millis()),
                )
                .await
                {
                    Ok(Ok(ProcessLeaseOutcome::Acquired(acquired))) => {
                        return start_process_lease_runtime(
                            store,
                            owner,
                            config,
                            takeover_started_at,
                            acquired,
                        );
                    }
                    Ok(Ok(ProcessLeaseOutcome::Held(current))) => {
                        last_failure = format!(
                            "boot {} renewed or won term {} during takeover observation",
                            current.owner, current.term
                        );
                    }
                    Ok(Err(error)) => last_failure = error.to_string(),
                    Err(_) => {
                        last_failure =
                            "takeover verification exceeded the object-store I/O timeout".into();
                    }
                }
            }
            Ok(Err(error)) => {
                last_failure = error.to_string();
                tokio::time::sleep(
                    std::time::Duration::from_millis(250)
                        .min(deadline.saturating_duration_since(std::time::Instant::now())),
                )
                .await;
            }
            Err(_) => {
                last_failure = format!("object-store operation exceeded {attempt_timeout:?}");
            }
        }
    }
}

async fn start_cluster_http_api_before_activation(
    db: Arc<LaminarDB>,
    registry: Arc<prometheus::Registry>,
    config_path: PathBuf,
    config: ServerConfig,
    serving_gate: Arc<crate::http::ServingGate>,
    cluster: crate::http::ClusterComponents,
) -> Result<(Arc<crate::http::AppState>, tokio::task::JoinHandle<()>), ClusterStartupError> {
    let controller = Arc::clone(&cluster.controller);
    let local = controller.instance_id();
    if controller.live_instances().contains(&local) {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster HTTP listener must bind before local activation".into(),
        ));
    }

    let prepared = server::prepare_http_api(
        db,
        registry,
        config_path,
        config,
        serving_gate,
        Some(cluster),
    )
    .await
    .map_err(|error| ClusterStartupError::HttpStartup(error.to_string()))?;
    let (app_state, mut api_handle) = prepared
        .start()
        .await
        .map_err(|error| ClusterStartupError::HttpStartup(error.to_string()))?;
    if controller.live_instances().contains(&local) {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        return Err(ClusterStartupError::EngineConstruction(
            "local activation raced cluster HTTP listener startup".into(),
        ));
    }
    Ok((app_state, api_handle))
}

/// Start a LaminarDB server in cluster (multi-node) mode.
pub async fn start_cluster(
    config: ServerConfig,
    cluster_cfg: ClusterConfig,
    config_path: PathBuf,
) -> Result<ClusterHandle, ClusterStartupError> {
    let temporal_join_idle_history_retention = config
        .server
        .validated_temporal_join_idle_history_retention()
        .map_err(|error| ClusterStartupError::EngineConstruction(format!("server.{error}")))?;
    let source_idle_timeout = config
        .server
        .validated_source_idle_timeout()
        .map_err(|error| ClusterStartupError::EngineConstruction(format!("server.{error}")))?;
    let event_time_max_future_skew = config
        .server
        .validated_event_time_max_future_skew()
        .map_err(|error| ClusterStartupError::EngineConstruction(format!("server.{error}")))?;
    let node_id_str = cluster_cfg.node_id.as_str().to_string();
    let node_id_num = numeric_node_id(&node_id_str);
    let node_id = NodeId(node_id_num);

    let bind_addr = &config.server.bind;
    let http_port = if let Some(colon) = bind_addr.rfind(':') {
        bind_addr[colon + 1..].parse::<u16>().unwrap_or(8080)
    } else {
        8080
    };

    // Extract the host part from bind address, handling IPv6.
    // Examples: "127.0.0.1:8080" → "127.0.0.1", "[::1]:8080" → "[::1]"
    let bind_host = if let Some(bracket_end) = bind_addr.rfind(']') {
        // IPv6: "[::1]:8080" — take up to and including ']'
        &bind_addr[..=bracket_end]
    } else if let Some(colon) = bind_addr.rfind(':') {
        // IPv4: "127.0.0.1:8080" — take before the last ':'
        &bind_addr[..colon]
    } else {
        bind_addr.as_str()
    };

    let host_trimmed = bind_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host_trimmed == "0.0.0.0" || host_trimmed == "::" || host_trimmed.is_empty();
    let advertise_host = if let Some(ref host) = cluster_cfg.discovery.advertise_host {
        host.clone()
    } else if ip_wildcard {
        let hostname = gethostname::gethostname();
        let hostname = hostname.to_string_lossy().into_owned();
        if hostname.is_empty() {
            "127.0.0.1".to_string()
        } else {
            hostname
        }
    } else {
        bind_host.to_string()
    };

    if !matches!(cluster_cfg.discovery.strategy.as_str(), "gossip" | "static") {
        return Err(ClusterStartupError::Discovery(format!(
            "unsupported discovery strategy {:?}; expected \"gossip\" or \"static\"",
            cluster_cfg.discovery.strategy
        )));
    }
    if cluster_cfg.discovery.seeds.is_empty() {
        return Err(ClusterStartupError::Discovery(
            "cluster mode requires [discovery].seeds — list every node's \
             gossip address including this one (e.g. [\"node-0:7946\", \
             \"node-1:7946\"]); expected membership is derived from it"
                .into(),
        ));
    }
    let key_groups = config.server.resolved_key_groups();

    // Claim the stable node identity before discovery can publish a duplicate member. The
    // durable recovery authority is deliberately not published until the database runtime exists.
    if CheckpointStorageScope::for_url(&config.checkpoint.url)
        != CheckpointStorageScope::ClusterShared
    {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster mode requires ClusterShared checkpoint storage".into(),
        ));
    }
    let control_store = build_control_store(&config)?;
    let lease_cfg = laminar_core::cluster::control::LeaderLeaseConfig::default();
    let lease_ttl_ms = i64::try_from(lease_cfg.ttl.as_millis()).map_err(|_| {
        ClusterStartupError::EngineConstruction(
            "leader lease TTL exceeds the durable diagnostic range".into(),
        )
    })?;
    let lease_store = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
        Arc::clone(&control_store),
        lease_ttl_ms,
    ));
    lease_store
        .verify_store_contract(OBJECT_STORE_CONTROL_IO_TIMEOUT)
        .await
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "cluster control store does not provide required create/update fencing: {error}"
            ))
        })?;
    install_cluster_tls(&cluster_cfg.discovery)?;
    let process_incarnation = uuid::Uuid::new_v4();
    let process_lease_config = laminar_core::cluster::control::ProcessLeaseConfig::default();
    let process_lease_ttl_ms =
        i64::try_from(process_lease_config.ttl.as_millis()).map_err(|_| {
            ClusterStartupError::EngineConstruction(
                "process lease TTL exceeds i64 milliseconds".into(),
            )
        })?;
    let process_lease_authority = Arc::new(
        laminar_core::cluster::control::process_lease::ProcessLeaseAuthority::new(
            Arc::clone(&control_store),
            process_lease_config.ttl,
        )
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "construct shared process lease authority: {error}"
            ))
        })?,
    );
    let process_lease_store = process_lease_authority.store_for(node_id);
    let mut process_lease = acquire_process_lease(
        process_lease_store,
        process_incarnation,
        process_lease_config,
    )
    .await?;
    let process_lease_terminal = process_lease.terminal_token();
    info!(
        term = process_lease.acquired.term,
        ttl_seconds = process_lease_config.ttl.as_secs(),
        "Stable node identity lease acquired"
    );

    // Bind ShuffleReceiver first to discover port and publish it in metadata tags.
    let bind_addr: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid shuffle bind host: {e}"))
    })?;
    let shuffle_receiver =
        laminar_core::shuffle::ShuffleReceiver::bind(node_id.0, bind_addr, process_incarnation)
            .await
            .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?;
    shuffle_receiver
        .install_process_lease_deadline(Arc::clone(&process_lease.deadline))
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "install inbound shuffle process lease: {error}"
            ))
        })?;
    let shuffle_receiver = Arc::new(shuffle_receiver);
    let shuffle_advertise = shuffle_advertise_addr(shuffle_receiver.local_addr(), &advertise_host);

    let mut local_node = NodeInfo {
        id: node_id,
        name: node_id_str.clone(),
        rpc_address: format!("{advertise_host}:{http_port}"),
        // `NodeInfo` retains this legacy wire field, but LaminarDB has no Raft
        // transport and must not advertise a service that is not bound.
        raft_address: String::new(),
        state: NodeState::Joining,
        metadata: NodeMetadata {
            cores: num_cpus(),
            memory_bytes: 0,
            failure_domain: cluster_cfg.discovery.failure_domain.clone(),
            tags: std::collections::HashMap::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        last_heartbeat_ms: 0,
    };
    local_node.metadata.tags.insert(
        laminar_core::shuffle::SHUFFLE_ADDR_KEY.to_string(),
        shuffle_advertise.clone(),
    );
    local_node.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.to_string(),
        process_incarnation.to_string(),
    );

    // 1. Start discovery layer
    info!(
        "Starting cluster discovery (strategy: {})",
        cluster_cfg.discovery.strategy
    );

    let mut discovery: DiscoveryImpl = match cluster_cfg.discovery.strategy.as_str() {
        "gossip" => {
            let gossip_config = GossipDiscoveryConfig {
                gossip_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                seed_nodes: cluster_cfg.discovery.seeds.clone(),
                gossip_interval: std::time::Duration::from_secs(1),
                phi_threshold: 8.0,
                dead_node_grace_period: std::time::Duration::from_secs(60),
                cluster_id: "laminardb".to_string(),
                node_id,
                process_generation: process_lease.acquired.term,
                local_node: local_node.clone(),
                advertise_host: cluster_cfg.discovery.advertise_host.clone(),
            };
            DiscoveryImpl::Gossip(GossipDiscovery::new(gossip_config))
        }
        "static" => {
            let static_config = StaticDiscoveryConfig {
                local_node: local_node.clone(),
                seeds: cluster_cfg.discovery.seeds.clone(),
                heartbeat_interval: std::time::Duration::from_secs(1),
                suspect_threshold: 3,
                dead_threshold: 10,
                listen_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                process_generation: process_lease.acquired.term,
                process_incarnation,
            };
            DiscoveryImpl::Static(StaticDiscovery::new(static_config))
        }
        strategy => {
            return Err(ClusterStartupError::Discovery(format!(
                "unsupported discovery strategy {strategy:?}; expected \"gossip\" or \"static\""
            )));
        }
    };

    let discovery_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = discovery.start() => Some(result),
    };
    match discovery_start {
        Some(Ok(())) => {}
        Some(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::Discovery(error.to_string()));
        }
        None => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while starting discovery".into(),
            ));
        }
    }
    info!("Discovery layer started");

    // 2. Wait for expected membership. Seeds include self by
    // convention (every node lists the full cluster), so the target
    // is `seeds.len() - 1`.
    let expected_peers = cluster_cfg.discovery.seeds.len().saturating_sub(1);
    let deadline = std::time::Instant::now() + cluster_cfg.formation_timeout;
    let mut last_seen = 0usize;
    let peers: Vec<NodeInfo> = loop {
        let discovered = tokio::select! {
            biased;
            () = process_lease_terminal.cancelled() => None,
            result = discovery.peers() => Some(result),
        };
        let Some(discovered) = discovered else {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during cluster formation".into(),
            ));
        };
        if let Ok(discovered) = discovered {
            let eligible: Vec<NodeInfo> = discovered
                .into_iter()
                .filter(|peer| matches!(peer.state, NodeState::Joining | NodeState::Active))
                .collect();
            last_seen = eligible.len();
            if eligible.len() >= expected_peers {
                break eligible;
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::FormationTimeout {
                found: last_seen,
                needed: expected_peers,
            });
        }
        tokio::select! {
            biased;
            () = process_lease_terminal.cancelled() => {
                let _ = discovery.stop().await;
                return Err(ClusterStartupError::AuthorityLost(
                    "stable node identity lease was lost during cluster formation".into(),
                ));
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    };
    info!(
        "Discovered {} peer(s) (expected {})",
        peers.len(),
        expected_peers
    );
    let roster_timeout = cluster_cfg
        .formation_timeout
        .min(laminar_core::cluster::control::MAX_SHARED_NAMESPACE_PROOF_TIMEOUT);
    let roster = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = tokio::time::timeout(
            roster_timeout,
            assignment_seed_participants(
                laminar_core::state::NodeId(node_id.0),
                process_incarnation,
                &peers,
                &control_store,
                process_lease_ttl_ms,
            ),
        ) => Some(result),
    };
    let Some(roster) = roster else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost during startup roster validation".into(),
        ));
    };
    let startup_participants = match roster {
        Ok(Ok(participants)) => participants,
        Ok(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
        Err(_) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "exact startup roster lease validation exceeded {roster_timeout:?}"
            )));
        }
    };
    let expected_participants = expected_peers + 1;
    if startup_participants.len() != expected_participants {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::Discovery(format!(
            "startup roster contains {} participants but the configured exact roster requires {expected_participants}",
            startup_participants.len()
        )));
    }

    // Build LaminarDB with Profile::Cluster
    let mut builder = LaminarDB::builder();
    builder = builder
        .profile(Profile::Cluster)
        .delivery_guarantee(config.server.delivery);
    if let Some(ref token) = config.server.console_token {
        builder = builder.http_auth_token(token.expose());
    }
    // Build the vnode registry. If a shared `AssignmentSnapshot` already exists,
    // every node adopts it; otherwise the first peer CAS-creates it and losers
    // re-load and adopt the winner.
    let assignment = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = resolve_vnode_assignment(
            node_id,
            &peers,
            u32::from(key_groups),
            Arc::clone(&control_store),
            &startup_participants,
            process_lease.deadline.as_ref(),
        ) => Some(result),
    };
    let Some(assignment) = assignment else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while resolving the initial assignment".into(),
        ));
    };
    let (vnode_registry, snapshot_store) = match assignment {
        Ok(assignment) => assignment,
        Err(error) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
    };

    // Discovery/barrier traffic stays on the low-latency KV. Coordinated recovery always uses
    // the shared object store so generation, phases, and acknowledgements survive process loss.
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    let recovery_kv = Arc::new(ObjectStoreClusterKv::new(
        process_lease.acquired.clone(),
        Arc::clone(&process_lease.deadline),
        process_lease_ttl_ms,
        Arc::clone(&control_store),
        discovery.membership_watch(),
    )) as Arc<dyn ClusterKv>;
    let controller_kv = match &discovery {
        DiscoveryImpl::Gossip(gossip) => gossip
            .chitchat_handle()
            .map(|handle| Arc::new(ChitchatKv::from_handle(handle)) as Arc<dyn ClusterKv>),
        DiscoveryImpl::Static(_) => Some(Arc::clone(&recovery_kv)),
    };
    let Some(controller_kv) = controller_kv else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "gossip discovery started without its control channel".into(),
        ));
    };
    let namespace_control = Arc::clone(&controller_kv);
    let local_participant = laminar_core::checkpoint::CheckpointParticipant {
        node_id: node_id.0,
        boot_incarnation: process_incarnation,
    };
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before shared checkpoint namespace proof".into(),
        ));
    }
    let namespace_proof = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = laminar_core::cluster::control::prove_shared_object_store_namespaces(
            local_participant,
            &startup_participants,
            namespace_control,
            Arc::clone(&control_store),
            cluster_cfg.formation_timeout,
        ) => Some(result),
    };
    let verified_namespaces = match namespace_proof {
        Some(Ok(namespaces)) => namespaces,
        Some(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(error.to_string()));
        }
        None => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during shared checkpoint namespace proof"
                    .into(),
            ));
        }
    };
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during shared checkpoint namespace proof".into(),
        ));
    }
    info!(
        participants = startup_participants.len(),
        "Shared checkpoint namespace verified"
    );

    let controller = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = install_cluster_controller(
            controller_kv,
            recovery_kv,
            Arc::clone(&snapshot_store),
            discovery.membership_watch(),
            bind_host,
            &advertise_host,
            &process_lease,
        ) => Some(result),
    };
    let Some(controller) = controller else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while installing cluster control".into(),
        ));
    };
    let cluster_controller = match controller {
        Ok(controller) => controller,
        Err(error) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
    };
    // Hand the controller this node's own locality so the topology-aware rebalancer can place
    // self correctly (peers' localities arrive via discovery; self is folded in by id only).
    cluster_controller.set_self_locality(laminar_core::state::Locality::parse(
        local_node.metadata.failure_domain.as_deref().unwrap_or(""),
    ));
    if let Err(error) =
        cluster_controller.set_process_lease_authority(Arc::clone(&process_lease_authority))
    {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error));
    }
    builder = builder.cluster_controller(Arc::clone(&cluster_controller));

    // LaminarDB derives the participant namespace from the installed controller.
    builder = builder.incremental_emit(config.server.incremental_emit);
    if let Some(retention) = temporal_join_idle_history_retention {
        builder = builder.temporal_join_idle_history_retention(retention);
    }
    if let Some(timeout) = source_idle_timeout {
        builder = builder.source_idle_timeout(timeout);
    }
    builder = builder.event_time_max_future_skew(event_time_max_future_skew);
    builder = server::apply_verified_cluster_checkpoint_config(
        builder,
        &config.checkpoint,
        verified_namespaces,
    )
    .map_err(|error| {
        ClusterStartupError::EngineConstruction(format!("checkpoint storage: {error}"))
    })?;

    builder = builder.vnode_registry(Arc::clone(&vnode_registry));

    // Durable cluster 2PC decisions use a cluster-wide prefix in the shared checkpoint store.
    // Without this the leader's `Commit` announcement is the only commit
    // signal — ephemeral, so a mid-2PC leader crash produces split state.
    let decision_store = Arc::new(
        laminar_core::cluster::control::CheckpointDecisionStore::new(Arc::clone(&control_store)),
    );
    builder = builder.decision_store(decision_store);

    // Hand the builder the same snapshot store resolved in `resolve_vnode_assignment`
    // so the snapshot watcher and rebalance controller share one backing object.
    builder = builder.assignment_snapshot_store(Arc::clone(&snapshot_store));

    // Catalog sealing and leader fencing share one append-only CAS sequence.
    let catalog_store = Arc::new(laminar_core::cluster::control::CatalogManifestStore::new(
        Arc::clone(&lease_store),
    ));
    builder = builder.catalog_manifest_store(Arc::clone(&catalog_store));

    // Shuffle fabric. ShuffleReceiver was bound at startup.
    let shuffle_sender = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        sender = build_shuffle_sender(
            node_id.0,
            &discovery,
            shuffle_advertise.clone(),
            discovery.membership_watch(),
            process_incarnation,
        ) => Some(sender),
    };
    let Some(shuffle_sender) = shuffle_sender else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while building the shuffle fabric".into(),
        ));
    };
    if let Err(error) =
        shuffle_sender.install_process_lease_deadline(Arc::clone(&process_lease.deadline))
    {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "install outbound shuffle process lease: {error}"
        )));
    }

    // Streaming aggregates go through the row-shuffle bridge driven by
    // `IncrementalAggState`; the DataFusion-native aggregate-rewrite path was removed.
    builder = builder
        .shuffle_sender(Arc::clone(&shuffle_sender))
        .shuffle_receiver(Arc::clone(&shuffle_receiver))
        .target_partitions(1);

    let db = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = builder.build() => Some(result),
    };
    let Some(db) = db else {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while constructing the database runtime".into(),
        ));
    };
    let db = match db {
        Ok(db) => db,
        Err(error) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(error.to_string()));
        }
    };

    let startup_controller = &cluster_controller;
    if !process_lease.is_live() {
        db.revoke_cluster_authority();
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost while constructing the database runtime".into(),
        ));
    }
    let recovery_identity = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = startup_controller.publish_leased_recovery_incarnation(&process_lease.acquired) => {
            Some(result)
        }
    };
    let Some(recovery_identity) = recovery_identity else {
        db.revoke_cluster_authority();
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost during recovery identity publication".into(),
        ));
    };
    if let Err(error) = recovery_identity {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "publish leased recovery process incarnation: {error}"
        )));
    }
    if !process_lease.is_live() {
        db.revoke_cluster_authority();
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during recovery identity publication".into(),
        ));
    }

    // Build prometheus registry before start() so connectors register on it.
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let pipeline_name = config
        .pipelines
        .first()
        .map_or("default", |p| p.name.as_str())
        .to_string();
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), hostname),
        ("pipeline".into(), pipeline_name),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    if let Err(error) = db.set_prometheus_registry(Arc::clone(&registry)) {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error.to_string()));
    }

    // Fenced leader lease. Wiring the watch into the controller makes
    // `is_leader()` lease-aware, so every leader-gated path (checkpoint, 2PC,
    // rebalance, committer) inherits fencing: a stale candidate whose lease
    // expired stops being the leader. Renewal is gated on `is_gossip_leader` so
    // the lease owner converges to the gossip candidate. Wired before `start()`.
    cluster_controller.set_leader_lease_store(Arc::clone(&lease_store));
    let manager = match laminar_core::cluster::control::LeaderLeaseManager::new(
        Arc::clone(&lease_store),
        &process_lease.acquired,
        lease_cfg,
    ) {
        Ok(manager) => manager,
        Err(error) => {
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "leader lease manager: {error}"
            )));
        }
    };
    if let Err(error) = cluster_controller.set_leader_lease_watch(
        manager.lease_watch(),
        manager.owner().clone(),
        manager.deadline(),
    ) {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error));
    }
    let token = tokio_util::sync::CancellationToken::new();
    let candidacy = cluster_controller.leader_candidacy_watch();
    let task = manager.spawn(token.clone(), candidacy);
    info!(
        "Leader lease manager started (ttl={}s)",
        lease_cfg.ttl.as_secs()
    );
    let mut leader_lease = LeaderLeaseRuntime::new(token, task);

    let serving_gate = Arc::new(crate::http::ServingGate::starting());
    if let Err(error) =
        serving_gate.install_process_lease_deadline(Arc::clone(&process_lease.deadline))
    {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::AuthorityLost(error.into()));
    }
    process_lease.install_fence(
        Arc::clone(&db),
        Arc::clone(startup_controller),
        Arc::clone(&serving_gate),
        leader_lease.shutdown_token(),
    );
    if !process_lease.is_live() {
        revoke_process_authority(
            &db,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease_terminal,
        );
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before pipeline startup".into(),
        ));
    }

    let catalog_startup = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err("stable node identity lease was lost during catalog bootstrap".to_string())
        }
        result = async {
            let authority = wait_for_catalog_startup_authority(
                startup_controller,
                cluster_cfg.formation_timeout,
            )
            .await?;
            server::execute_config_ddl(&db, &config, true)
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(authority)
        } => result,
    };
    match catalog_startup {
        Ok(CatalogStartupAuthority::DurableLease) => {
            info!("Cluster catalog sealed under the durable leader lease");
        }
        Ok(CatalogStartupAuthority::ActivePeer) => {
            info!("Cluster catalog replayed after observing an active peer");
        }
        Err(error) => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster catalog startup: {error}"
            )));
        }
    }
    if !process_lease.is_live() {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during catalog bootstrap".into(),
        ));
    }

    // Coordinated recovery is the only cluster fault path. Before start() so an early
    // fault is observed.
    db.fence_cluster_startup();
    let recovery_generation = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = db.prepare_cluster_startup_recovery_generation(
            tokio::time::Instant::now() + PROCESS_LEASE_IO_TIMEOUT,
        ) => Some(result),
    };
    match recovery_generation {
        Some(Ok(_)) => {}
        Some(Err(error)) => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster recovery generation bootstrap: {error}"
            )));
        }
        None => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                true,
            )
            .await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while restoring recovery generation".into(),
            ));
        }
    }
    if let Err(error) = db.enable_coordinated_recovery() {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            false,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "cluster recovery monitor initialization: {error}"
        )));
    }

    let pipeline_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = db.start() => Some(result),
    };
    match pipeline_start {
        Some(Ok(())) => {}
        Some(Err(error)) => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "pipeline start: {error}"
            )));
        }
        None => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                true,
            )
            .await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while starting the pipeline".into(),
            ));
        }
    }
    info!("Pipeline started");

    let rebalance_config = laminar_db::rebalance::RebalanceConfig {
        placement_isolation_tier: cluster_cfg.discovery.placement_isolation_tier,
        ..laminar_db::rebalance::RebalanceConfig::default()
    };

    // Existing clusters deliberately start replacement processes unassigned. The retained
    // assignment names predecessor process incarnations and must not be installed in a new graph.
    // Keep intake and the HTTP serving gate closed until the watcher/rebalancer, started after the
    // process is announced Active, materializes and certifies an authority-sequenced successor.

    let catalog_verification = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = tokio::time::timeout(
            rebalance_config.checkpoint_timeout,
            catalog_store.load(),
        ) => Some(result),
    };
    let Some(catalog_verification) = catalog_verification else {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while verifying the cluster catalog".into(),
        ));
    };
    let catalog_verification = match catalog_verification {
        Ok(result) => result,
        Err(_) => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster catalog verification exceeded {:?}",
                rebalance_config.checkpoint_timeout
            )));
        }
    };
    match catalog_verification {
        Ok(Some(_)) => {}
        Ok(None) => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(ClusterStartupError::EngineConstruction(
                "cluster catalog is not sealed before readiness announcement".into(),
            ));
        }
        Err(error) => {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "verify sealed cluster catalog before readiness announcement: {error}"
            )));
        }
    }
    if !process_lease.is_live() {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before announcing cluster readiness".into(),
        ));
    }

    let Some(leader_authority_timeout) =
        startup_leader_authority_timeout(lease_cfg, OBJECT_STORE_CONTROL_IO_TIMEOUT)
    else {
        if !process_lease_terminal.is_cancelled() {
            let mut left = local_node.clone();
            left.state = NodeState::Left;
            let _ = announce_node_state_with_bound(
                &discovery,
                left,
                &process_lease_terminal,
                "withdraw node after invalid leader authority timeout",
            )
            .await;
        }
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            false,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "leader authority convergence timeout exceeds the monotonic timer range".into(),
        ));
    };

    // Bind and begin serving before this process can become an assignment owner. The startup
    // gate answers data/control requests with 503 until authority and recovery are established;
    // accepting now avoids holding requests in the kernel listen backlog for later replay.
    let cluster_components = crate::http::ClusterComponents {
        controller: Arc::clone(&cluster_controller),
        snapshot_store: Arc::clone(&snapshot_store),
        membership_rx: discovery.membership_watch(),
    };
    let http_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = start_cluster_http_api_before_activation(
            Arc::clone(&db),
            registry,
            config_path.clone(),
            config,
            Arc::clone(&serving_gate),
            cluster_components,
        ) => Some(result),
    };
    let Some(http_start) = http_start else {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while binding the HTTP listener".into(),
        ));
    };
    let (app_state, mut api_handle) = match http_start {
        Ok(prepared) => prepared,
        Err(error) => {
            db.fence_cluster_startup();
            startup_controller.set_active(false);
            if !process_lease_terminal.is_cancelled() {
                let mut left = local_node.clone();
                left.state = NodeState::Left;
                let _ = announce_node_state_with_bound(
                    &discovery,
                    left,
                    &process_lease_terminal,
                    "withdraw node after HTTP startup failure",
                )
                .await;
            }
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(error);
        }
    };

    if !process_lease.is_live() {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        revoke_process_authority(
            &db,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease_terminal,
        );
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost while binding the HTTP listener".into(),
        ));
    }

    // The pipeline, shuffle receiver, and gated HTTP listener are live. Only now publish
    // assignment eligibility so an occupied API port can never leave an Active ghost node.
    let mut active = local_node.clone();
    active.state = NodeState::Active;
    let active_announcement = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = tokio::time::timeout(
            PROCESS_LEASE_IO_TIMEOUT,
            discovery.announce(active.clone()),
        ) => Some(result),
    };
    let Some(active_announcement) = active_announcement else {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost while announcing cluster readiness".into(),
        ));
    };
    let active_announcement = match active_announcement {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(_) => Err(format!(
            "readiness announcement exceeded {PROCESS_LEASE_IO_TIMEOUT:?}"
        )),
    };
    if let Err(error) = active_announcement {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        startup_controller.set_active(false);
        if !process_lease_terminal.is_cancelled() {
            let mut left = local_node.clone();
            left.state = NodeState::Left;
            let _ = announce_node_state_with_bound(
                &discovery,
                left,
                &process_lease_terminal,
                "withdraw node after readiness announcement failure",
            )
            .await;
        }
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            false,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "announce cluster runtime readiness: {error}"
        )));
    }
    startup_controller.set_active(true);

    // Rebalance and assignment certification use the same durable snapshot in every discovery
    // mode.
    let rebalance_shutdown = tokio_util::sync::CancellationToken::new();
    let mut rebalance_tasks = vec![
        laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(&db),
            Arc::clone(&snapshot_store),
            Arc::clone(&vnode_registry),
            rebalance_shutdown.clone(),
            rebalance_config,
            Some(Arc::clone(startup_controller)),
        ),
        laminar_db::rebalance::spawn_rebalance_controller(
            Arc::clone(&db),
            Arc::clone(startup_controller),
            Arc::clone(&snapshot_store),
            Arc::clone(&vnode_registry),
            rebalance_shutdown.clone(),
            rebalance_config,
        ),
    ];
    info!("Rebalance control plane started");

    let startup_gate = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while certifying startup authority".into(),
            ))
        }
        result = async {
            wait_for_startup_assignment_fence(startup_controller, &vnode_registry).await?;
            wait_for_startup_leader_authority(
                startup_controller,
                &vnode_registry,
                leader_authority_timeout,
            )
            .await?;
            info!(
                timeout = ?leader_authority_timeout,
                "Durable leader authority converged with the certified assignment"
            );
            let authority_deadline =
                tokio::time::Instant::now() + rebalance_config.checkpoint_timeout;
            db.finish_cluster_startup(authority_deadline)
                .await
                .map_err(|error| {
                    ClusterStartupError::EngineConstruction(format!(
                        "cluster startup recovery fence: {error}"
                    ))
                })
        } => result,
    };
    let startup_disposition = match startup_gate {
        Ok(disposition) => disposition,
        Err(error) => {
            let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
            db.fence_cluster_startup();
            startup_controller.set_active(false);
            if !process_lease_terminal.is_cancelled() {
                let mut left = active.clone();
                left.state = NodeState::Left;
                let _ = announce_node_state_with_bound(
                    &discovery,
                    left,
                    &process_lease_terminal,
                    "withdraw node after startup authority failure",
                )
                .await;
            }
            let _ = stop_rebalance_tasks(&mut rebalance_tasks, &rebalance_shutdown).await;
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                false,
            )
            .await;
            return Err(error);
        }
    };
    match startup_disposition {
        ClusterStartupDisposition::Serving => {
            info!("Cluster assignment certified; source intake opened");
        }
        ClusterStartupDisposition::Idle => {
            info!("Cluster worker owns no vnodes; data plane remains fenced pending assignment");
        }
        ClusterStartupDisposition::RecoveryFenced => {
            info!("Cluster source intake remains fenced for coordinated recovery");
            let recovery_wait = tokio::select! {
                biased;
                () = process_lease_terminal.cancelled() => {
                    Err(ClusterStartupError::AuthorityLost(
                        "stable node identity lease was lost during coordinated startup recovery"
                            .into(),
                    ))
                }
                result = tokio::time::timeout(STARTUP_RECOVERY_TIMEOUT, async {
                    while db.cluster_intake_fenced() || startup_controller.is_recovering() {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }) => result.map_err(|_| {
                    ClusterStartupError::EngineConstruction(format!(
                        "coordinated startup recovery did not release intake within {STARTUP_RECOVERY_TIMEOUT:?}"
                    ))
                }),
            };
            if let Err(error) = recovery_wait {
                let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
                db.fence_cluster_startup();
                startup_controller.set_active(false);
                if !process_lease_terminal.is_cancelled() {
                    let mut left = active.clone();
                    left.state = NodeState::Left;
                    let _ = announce_node_state_with_bound(
                        &discovery,
                        left,
                        &process_lease_terminal,
                        "withdraw node after startup recovery failure",
                    )
                    .await;
                }
                let _ = stop_rebalance_tasks(&mut rebalance_tasks, &rebalance_shutdown).await;
                cleanup_cluster_startup(
                    &mut discovery,
                    &db,
                    &mut leader_lease,
                    &process_lease_terminal,
                    false,
                )
                .await;
                return Err(error);
            }
        }
    }

    // An idle worker serves control-plane readiness while its data plane remains fenced until the
    // watcher grants ownership.
    if !app_state.open_startup_gate() || !process_lease.is_live() {
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        db.revoke_cluster_authority();
        startup_controller.set_active(false);
        if !process_lease_terminal.is_cancelled() {
            let mut left = active.clone();
            left.state = NodeState::Left;
            let _ = announce_node_state_with_bound(
                &discovery,
                left,
                &process_lease_terminal,
                "withdraw node after serving gate failure",
            )
            .await;
        }
        let _ = stop_rebalance_tasks(&mut rebalance_tasks, &rebalance_shutdown).await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before HTTP serving authority opened".into(),
        ));
    }
    let watcher_handle = server::spawn_config_watcher(&app_state, config_path);
    let membership_rx = discovery.membership_watch();
    let membership_handle = spawn_membership_watcher(&node_id_str, membership_rx);
    info!("Membership watcher started");

    info!("Cluster node '{node_id_str}' started");

    Ok(ClusterHandle {
        db,
        db_shutdown_complete: false,
        discovery,
        serving_gate,
        api_handle,
        watcher_handle,
        membership_handle,
        local_node: active,
        cluster_controller,
        snapshot_store,
        vnode_count: vnode_registry.vnode_count(),
        leader_lease,
        process_lease,
        rebalance_tasks,
        rebalance_shutdown,
    })
}

const STARTUP_ASSIGNMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const STARTUP_RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const STARTUP_LEADER_AUTHORITY_MIN_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(25);
const STARTUP_LEADER_AUTHORITY_MAX_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(250);
const STARTUP_LEADER_AUTHORITY_MAX_SLEEP: std::time::Duration =
    std::time::Duration::from_millis(375);

fn exact_startup_assignment_fence(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
) -> Option<laminar_core::checkpoint::CheckpointAssignmentFence> {
    let assignment = registry.versioned_snapshot();
    let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
    let fence = controller.checkpoint_assignment_fence(assignment.version())?;
    fence.matches_owner_map(&owners).then_some(fence)
}

fn startup_leader_audit_deadline(overall: tokio::time::Instant) -> tokio::time::Instant {
    // The exact controller audit serializes five bounded control operations under one deadline:
    // durable head, live proof, process term, live proof, and final durable head.
    let audit_budget = OBJECT_STORE_CONTROL_IO_TIMEOUT
        .checked_mul(5)
        .expect("the fixed startup authority audit budget fits Duration");
    tokio::time::Instant::now()
        .checked_add(audit_budget)
        .map_or(overall, |deadline| deadline.min(overall))
}

fn startup_leader_authority_timeout(
    config: laminar_core::cluster::control::LeaderLeaseConfig,
    control_io: std::time::Duration,
) -> Option<std::time::Duration> {
    // Initial attempt, full rival observation, and takeover can each consume one TTL. The
    // successful remote audit then serializes five bounded operations: durable head, live RPC,
    // process-term verification, live RPC, and final durable head.
    config
        .ttl
        .checked_mul(3)?
        .checked_add(config.renew_interval)?
        .checked_add(control_io.checked_mul(5)?)?
        .checked_add(STARTUP_LEADER_AUTHORITY_MAX_SLEEP)
}

async fn wait_for_startup_leader_authority(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
    timeout: std::time::Duration,
) -> Result<(), ClusterStartupError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| {
            ClusterStartupError::EngineConstruction(
                "leader authority convergence deadline exceeds the monotonic timer range".into(),
            )
        })?;
    let mut last_expected = None;
    let mut last_audit_error = None;
    let mut backoff = STARTUP_LEADER_AUTHORITY_MIN_BACKOFF;
    let mut previous_candidate = None;
    let wait = async {
        loop {
            let fence = exact_startup_assignment_fence(controller, registry);
            let candidate = fence.as_ref().and_then(|fence| {
                let leader = controller.current_leader()?;
                let participant = fence
                    .participants
                    .iter()
                    .find(|participant| participant.node_id == leader.0)?;
                Some((
                    fence.assignment_version,
                    fence.assignment_digest,
                    *participant,
                ))
            });
            if candidate != previous_candidate {
                backoff = STARTUP_LEADER_AUTHORITY_MIN_BACKOFF;
                previous_candidate = candidate;
            }
            if let Some((version, _, participant)) = candidate {
                last_expected = Some(format!(
                    "assignment {version} candidate {} boot {}",
                    participant.node_id, participant.boot_incarnation
                ));
            } else if let Some(fence) = fence.as_ref() {
                last_expected = Some(format!(
                    "assignment {} has no certified current leader participant",
                    fence.assignment_version
                ));
            } else {
                last_expected = Some("no exact assignment fence is currently installed".into());
            }

            if let Some(fence) = fence {
                match controller
                    .audit_assignment_leader_authority(
                        &fence,
                        None,
                        startup_leader_audit_deadline(deadline),
                    )
                    .await
                {
                    Ok(_) => return,
                    Err(error) => last_audit_error = Some(error),
                }
            } else {
                last_audit_error = Some("no exact assignment fence is currently installed".into());
            }
            use rand::RngExt as _;
            let base_ms = u64::try_from(backoff.as_millis()).unwrap_or(250);
            let jitter_ms = rand::rng().random_range(0..=base_ms / 2);
            tokio::time::sleep(backoff + std::time::Duration::from_millis(jitter_ms)).await;
            backoff = backoff
                .checked_mul(2)
                .unwrap_or(STARTUP_LEADER_AUTHORITY_MAX_BACKOFF)
                .min(STARTUP_LEADER_AUTHORITY_MAX_BACKOFF);
        }
    };
    if tokio::time::timeout_at(deadline, wait).await.is_ok() {
        return Ok(());
    }

    let expected =
        last_expected.unwrap_or_else(|| "no assignment-certified leader candidate".to_string());
    let audit_error = last_audit_error
        .map(|error| format!("; last authority audit failed: {error}"))
        .unwrap_or_default();
    Err(ClusterStartupError::EngineConstruction(format!(
        "durable leader authority did not converge with a live certified grant within {timeout:?}: {expected}{audit_error}"
    )))
}

async fn wait_for_startup_assignment_fence(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
) -> Result<(), ClusterStartupError> {
    let mut fence_rx = controller.checkpoint_assignment_watch();
    let mut members_rx = controller.members_watch();
    let wait = async {
        loop {
            let assignment = registry.versioned_snapshot();
            let version = assignment.version();
            let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
            if controller
                .checkpoint_assignment_fence(version)
                .is_some_and(|fence| fence.matches_owner_map(&owners))
            {
                return Ok(());
            }
            tokio::select! {
                result = fence_rx.changed() => result.map_err(|_| {
                    ClusterStartupError::EngineConstruction(
                        "cluster assignment certification channel closed during startup".into(),
                    )
                })?,
                result = members_rx.changed() => result.map_err(|_| {
                    ClusterStartupError::EngineConstruction(
                        "cluster membership channel closed during assignment certification".into(),
                    )
                })?,
            }
        }
    };
    tokio::time::timeout(STARTUP_ASSIGNMENT_TIMEOUT, wait)
        .await
        .map_err(|_| {
            ClusterStartupError::EngineConstruction(format!(
                "cluster assignment {} was not certified within {STARTUP_ASSIGNMENT_TIMEOUT:?}",
                registry.assignment_version()
            ))
        })?
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

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Verify advertised startup process incarnations against their durable stable-node leases.
async fn assignment_seed_participants(
    self_id: laminar_core::state::NodeId,
    self_incarnation: uuid::Uuid,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    store: &Arc<dyn object_store::ObjectStore>,
    process_lease_ttl_ms: i64,
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    use laminar_core::cluster::control::ProcessLeaseStore;

    let advertised = advertised_startup_participants(self_id, self_incarnation, peers)?;
    let mut participants = Vec::with_capacity(advertised.len());
    for participant in advertised {
        let node = NodeId(participant.node_id);
        let boot_incarnation = participant.boot_incarnation;
        let lease = ProcessLeaseStore::new(Arc::clone(store), node, process_lease_ttl_ms)
            .load()
            .await
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "load process lease for node {}: {error}",
                    node.0
                ))
            })?
            .ok_or_else(|| {
                ClusterStartupError::EngineConstruction(format!(
                    "node {} has no durable process lease",
                    node.0
                ))
            })?;
        if lease.owner != boot_incarnation {
            return Err(ClusterStartupError::EngineConstruction(format!(
                "node {} advertised process {} but durable lease belongs to {}",
                node.0, boot_incarnation, lease.owner
            )));
        }
        participants.push(participant);
    }
    Ok(participants)
}

fn advertised_startup_participants(
    self_id: laminar_core::state::NodeId,
    self_incarnation: uuid::Uuid,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    let mut advertised = Vec::with_capacity(peers.len() + 1);
    advertised.push((self_id, self_incarnation));
    for peer in peers {
        let incarnation = peer
            .metadata
            .tags
            .get(PROCESS_INCARNATION_TAG)
            .ok_or_else(|| {
                ClusterStartupError::EngineConstruction(format!(
                    "peer {} did not advertise its process incarnation",
                    peer.id.0
                ))
            })?
            .parse()
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "peer {} advertised an invalid process incarnation: {error}",
                    peer.id.0
                ))
            })?;
        advertised.push((peer.id, incarnation));
    }
    advertised.sort_unstable_by_key(|(node, _)| node.0);
    if advertised.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(ClusterStartupError::EngineConstruction(
            "initial assignment participant roster contains duplicate node ids".into(),
        ));
    }
    if advertised.len() > laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS {
        return Err(ClusterStartupError::EngineConstruction(format!(
            "startup roster has {} participants; maximum is {}",
            advertised.len(),
            laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
        )));
    }
    Ok(advertised
        .into_iter()
        .map(
            |(node, boot_incarnation)| laminar_core::checkpoint::CheckpointParticipant {
                node_id: node.0,
                boot_incarnation,
            },
        )
        .collect())
}

/// Resolve the boot-time vnode registry and shared assignment store.
/// Existing clusters boot unassigned and adopt the audited committed head after `db.start()`.
/// A new cluster CAS-creates its rendezvous assignment and CAS losers install the winner.
async fn resolve_vnode_assignment(
    self_id: laminar_core::cluster::discovery::NodeId,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    vnode_count: u32,
    control_store: Arc<dyn object_store::ObjectStore>,
    startup_participants: &[laminar_core::checkpoint::CheckpointParticipant],
    process_deadline: &laminar_core::cluster::control::LeaseDeadline,
) -> Result<
    (
        Arc<laminar_core::state::VnodeRegistry>,
        Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    ),
    ClusterStartupError,
> {
    use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};
    use laminar_core::state::{rendezvous_assignment, NodeId, VnodeRegistry};

    if !process_deadline.is_live() {
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease expired before assignment resolution".into(),
        ));
    }

    let peer_ids: Vec<NodeId> = peers
        .iter()
        .map(|p| NodeId(p.id.0))
        .chain(std::iter::once(NodeId(self_id.0)))
        .collect();
    let assignment: Arc<[NodeId]> = rendezvous_assignment(vnode_count, &peer_ids);

    let snapshot_store = Arc::new(AssignmentSnapshotStore::new(control_store));

    // Snapshot exists → restart or joiner. Boot owning nothing: the stored snapshot may be
    // stale (a shed can race the restart), and acting on assumed ownership bypasses the adopt
    // protocol. `start_cluster` explicitly adopts the stored snapshot after `db.start()`.
    if let Some(existing) = snapshot_store
        .load()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot load: {e}")))?
    {
        if !process_deadline.is_live() {
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease expired while loading the assignment".into(),
            ));
        }
        existing.to_vnode_vec(vnode_count).map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("stored assignment: {error}"))
        })?;
        let registry = VnodeRegistry::new_unassigned(vnode_count);
        info!(
            stored_version = existing.version,
            "found stored assignment snapshot; booting unassigned — adopt runs after start"
        );
        return Ok((Arc::new(registry), snapshot_store));
    }

    // Nothing stored yet — propose ours and CAS-create. A racing peer
    // may win; if so, re-load and adopt the winner.
    let owner_ids: std::collections::BTreeSet<u64> =
        assignment.iter().map(|owner| owner.0).collect();
    let owner_participants: Vec<_> = startup_participants
        .iter()
        .filter(|participant| owner_ids.contains(&participant.node_id))
        .cloned()
        .collect();
    if owner_participants.len() != owner_ids.len() {
        return Err(ClusterStartupError::EngineConstruction(
            "initial assignment has an owner without a certified process incarnation".into(),
        ));
    }
    let proposal = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&assignment),
            owner_participants,
        )
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("initial assignment snapshot: {error}"))
        })?;
    if !process_deadline.is_live() {
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease expired before the initial assignment CAS".into(),
        ));
    }
    let winner = match snapshot_store
        .save_if_absent(&proposal)
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot save: {e}")))?
    {
        Some(w) => {
            info!("Created assignment snapshot v{}", w.version);
            w
        }
        None => {
            let w = snapshot_store
                .load()
                .await
                .map_err(|e| {
                    ClusterStartupError::EngineConstruction(format!("snapshot re-load: {e}"))
                })?
                .ok_or_else(|| {
                    ClusterStartupError::EngineConstruction(
                        "snapshot CAS lost but re-load returned None".into(),
                    )
                })?;
            info!("Adopted snapshot v{} after CAS race", w.version);
            w
        }
    };
    if !process_deadline.is_live() {
        return Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease expired while resolving the assignment winner".into(),
        ));
    }
    let registry = VnodeRegistry::new_unassigned(vnode_count);
    let winning_assignment = winner.to_vnode_vec(vnode_count).map_err(|error| {
        ClusterStartupError::EngineConstruction(format!("winning assignment: {error}"))
    })?;
    registry.set_assignment_and_version(winning_assignment.into(), winner.version);
    Ok((Arc::new(registry), snapshot_store))
}

/// Build the shared, cluster-wide control-plane object store (assignment snapshot plus
/// `ObjectStoreClusterKv`) from the cluster-shared checkpoint namespace.
fn build_control_store(
    config: &ServerConfig,
) -> Result<Arc<dyn object_store::ObjectStore>, ClusterStartupError> {
    laminar_core::checkpoint::object_store_builder::build_object_store(
        &config.checkpoint.url,
        &config.checkpoint.storage,
    )
    .map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("control-plane object store: {e}"))
    })
}

/// Build a `ClusterController` from a ready KV handle and start its barrier sync
/// server. Shared by the gossip and static discovery paths.
async fn install_cluster_controller(
    kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    recovery_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
    bind_host: &str,
    advertise_host: &str,
    process_lease: &ProcessLeaseRuntime,
) -> Result<Arc<laminar_core::cluster::control::ClusterController>, ClusterStartupError> {
    use laminar_core::cluster::control::ClusterController;

    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        process_lease.acquired.node,
        kv,
        recovery_kv,
        Some(snapshot_store),
        members_rx,
        process_lease.acquired.owner,
    ));
    controller
        .set_process_lease_deadline(Arc::clone(&process_lease.deadline))
        .map_err(ClusterStartupError::EngineConstruction)?;
    controller.set_active(false);
    controller.install_local_leader_proof_provider();

    let bind: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid barrier sync bind host: {e}"))
    })?;
    let bound = controller
        .start_leased_barrier_server(
            bind,
            Some(advertise_host.to_string()),
            &process_lease.acquired,
        )
        .await
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("cluster control endpoint start: {e}"))
        })?;
    info!("Barrier sync gRPC server listening on {bound}");
    info!(
        "ClusterController installed (leader={})",
        controller.is_leader()
    );
    Ok(controller)
}

struct StaticClusterKv {
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
}

impl StaticClusterKv {
    fn new(membership_rx: watch::Receiver<Vec<NodeInfo>>) -> Self {
        Self { membership_rx }
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for StaticClusterKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        let peers = self.membership_rx.borrow();
        let peer = peers.iter().find(|p| p.id == who)?;
        peer.metadata.tags.get(key).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let peers = self.membership_rx.borrow();
        peers
            .iter()
            .filter_map(|p| p.metadata.tags.get(key).map(|v| (p.id, v.clone())))
            .collect()
    }
}

/// Resolve the process-wide control-plane transport before any server/client
/// binds. Install mTLS when configured; otherwise atomically claim plaintext.
fn install_cluster_tls(d: &DiscoverySection) -> Result<(), ClusterStartupError> {
    let configured = (
        &d.cluster_tls_cert,
        &d.cluster_tls_key,
        &d.cluster_tls_client_ca,
        &d.cluster_tls_server_name,
    );
    let (cert, key, ca, name) = match configured {
        (Some(cert), Some(key), Some(ca), Some(name)) => (cert, key, ca, name),
        (None, None, None, None) => {
            laminar_core::cluster::control::claim_cluster_plaintext().map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "select plaintext cluster transport before startup: {error}"
                ))
            })?;
            return Ok(());
        }
        _ => {
            return Err(ClusterStartupError::EngineConstruction(
                "cluster control-plane TLS requires cert, key, client CA, and server name".into(),
            ));
        }
    };
    let read = |p: &std::path::Path| {
        std::fs::read(p).map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("read {}: {e}", p.display()))
        })
    };
    let tls = laminar_core::cluster::control::ClusterTls::from_pem(
        &read(cert)?,
        &read(key)?,
        &read(ca)?,
        name,
    );
    laminar_core::cluster::control::set_cluster_tls(tls).map_err(|error| {
        ClusterStartupError::EngineConstruction(format!(
            "install cluster control-plane TLS before transport startup: {error}"
        ))
    })?;
    info!("cluster control-plane mTLS enabled (server_name={name})");
    Ok(())
}

/// Build an outbound shuffle sender. When gossip discovery is active,
/// publish `advertise_addr` under `SHUFFLE_ADDR_KEY` so peers find us, and
/// give the sender a KV handle for reverse lookup. Static discovery
/// uses `StaticClusterKv` to query peer shuffle addresses from TCP heartbeat metadata.
async fn build_shuffle_sender(
    node_id: u64,
    discovery: &DiscoveryImpl,
    advertise_addr: String,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    process_incarnation: uuid::Uuid,
) -> Arc<laminar_core::shuffle::ShuffleSender> {
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    use laminar_core::shuffle::{ShuffleSender, SHUFFLE_ADDR_KEY};

    let sender = match discovery {
        DiscoveryImpl::Gossip(gossip) => {
            if let Some(handle) = gossip.chitchat_handle() {
                let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
                kv.write(SHUFFLE_ADDR_KEY, advertise_addr).await;
                ShuffleSender::with_kv(node_id, kv, process_incarnation)
            } else {
                ShuffleSender::new(node_id, process_incarnation)
            }
        }
        DiscoveryImpl::Static(_) => {
            let kv: Arc<dyn ClusterKv> = Arc::new(StaticClusterKv::new(membership_rx.clone()));
            ShuffleSender::with_kv(node_id, kv, process_incarnation)
        }
    };
    let sender = Arc::new(sender);

    // Static advertises shuffle addrs in heartbeat metadata; register peers as they appear.
    if matches!(discovery, DiscoveryImpl::Static(_)) {
        let sender_clone = Arc::clone(&sender);
        let mut rx = membership_rx;
        tokio::spawn(async move {
            loop {
                let members = rx.borrow().clone();
                for node in members {
                    if node.id.0 != node_id {
                        if let Some(addr) = node
                            .metadata
                            .tags
                            .get(SHUFFLE_ADDR_KEY)
                            .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
                        {
                            sender_clone.register_peer(node.id.0, addr);
                        }
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    sender
}

/// Compute the address peers should use to reach our `ShuffleReceiver`.
///
/// The receiver binds to `0.0.0.0:0` (any interface, ephemeral port), so
/// `local_addr.ip()` is the wildcard — publishing it unchanged leaves remote
/// senders unable to connect. Use the configured advertise host (matching the
/// HTTP/barrier endpoints for NAT/container deployments), falling back to
/// `gethostname` when it is itself a wildcard, keeping the actual bound port.
fn shuffle_advertise_addr(local_addr: std::net::SocketAddr, advertise_host: &str) -> String {
    let port = local_addr.port();
    let host = advertise_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host == "0.0.0.0" || host == "::" || host.is_empty();
    if !ip_wildcard {
        return format!("{advertise_host}:{port}");
    }
    let hostname = gethostname::gethostname();
    let hostname = hostname.to_string_lossy();
    if hostname.is_empty() {
        local_addr.to_string()
    } else {
        format!("{hostname}:{port}")
    }
}

const OBJECT_STORE_CONTROL_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const OBJECT_STORE_CONTROL_MAX_VALUE_BYTES: u64 = 1024 * 1024;
const OBJECT_STORE_CONTROL_MAX_KEY_BYTES: usize = 1024;
const OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES: u64 =
    OBJECT_STORE_CONTROL_MAX_VALUE_BYTES * 6 + 16 * 1024;
const OBJECT_STORE_CONTROL_VERSION: u8 = 2;
const OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN: usize = 2;
const OBJECT_STORE_CONTROL_MAX_LIST_RECORDS: usize = 4096;
const OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS: usize = 4;
const OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS: usize = 256;
const OBJECT_STORE_CONTROL_MAX_PRUNE_BATCHES: usize = 4;
const OBJECT_STORE_CONTROL_SCAN_CONCURRENCY: usize = 32;
const RECOVERY_GENERATION_KEY: &str = "control:recovery-gen";

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectStoreControlRecord {
    version: u8,
    node: u64,
    owner: uuid::Uuid,
    term: u64,
    sequence: u64,
    key: String,
    value: String,
}

impl ObjectStoreControlRecord {
    fn validate(
        &self,
        lease: &laminar_core::cluster::control::ProcessLease,
        key: &str,
        sequence: u64,
    ) -> Result<(), String> {
        if self.version != OBJECT_STORE_CONTROL_VERSION
            || self.node != lease.node.0
            || self.owner != lease.owner
            || self.term != lease.term
            || sequence == 0
            || self.sequence != sequence
            || self.key != key
            || u64::try_from(self.value.len()).unwrap_or(u64::MAX)
                > OBJECT_STORE_CONTROL_MAX_VALUE_BYTES
        {
            return Err("control record does not match its durable path and process lease".into());
        }
        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RecoveryGenerationRecord {
    version: u8,
    generation: u64,
    writer_node: u64,
    writer_owner: uuid::Uuid,
    writer_term: u64,
}

fn object_store_control_key_digest(key: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(key.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn object_store_control_key_prefix(
    lease: &laminar_core::cluster::control::ProcessLease,
    key: &str,
) -> String {
    format!(
        "cluster-control-kv/v{OBJECT_STORE_CONTROL_VERSION}/node={}/term={:020}/owner={}/key={}/",
        lease.node.0,
        lease.term,
        lease.owner,
        object_store_control_key_digest(key)
    )
}

fn object_store_control_record_path(
    lease: &laminar_core::cluster::control::ProcessLease,
    key: &str,
    sequence: u64,
) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{}v{sequence:020}.json",
        object_store_control_key_prefix(lease, key)
    ))
}

const RECOVERY_GENERATION_PREFIX: &str = "cluster-control-kv/v2/recovery-generation/";

fn recovery_generation_path(generation: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{RECOVERY_GENERATION_PREFIX}v{generation:020}.json"
    ))
}

fn sequence_from_path(prefix: &str, path: &object_store::path::Path) -> Result<u64, String> {
    let raw = path
        .as_ref()
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('v'))
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .ok_or_else(|| format!("invalid control record path {path}"))?;
    if raw.len() != 20 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid control record sequence in {path}"));
    }
    raw.parse::<u64>()
        .map_err(|error| format!("invalid control record sequence in {path}: {error}"))
}

fn retain_oldest_control_record(
    oldest: &mut BinaryHeap<(u64, String)>,
    sequence: u64,
    path: &object_store::path::Path,
) {
    let candidate = (sequence, path.to_string());
    if oldest.len() < OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS {
        oldest.push(candidate);
    } else if oldest.peek().is_some_and(|largest| &candidate < largest) {
        oldest.pop();
        oldest.push(candidate);
    }
}

async fn list_control_sequences(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<u64>, String> {
    use futures::StreamExt;

    let prefix_path = object_store::path::Path::from(prefix);
    let mut entries = store.list(Some(&prefix_path));
    let mut sequences = Vec::new();
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| error.to_string())?;
        if sequences.len() == OBJECT_STORE_CONTROL_MAX_LIST_RECORDS {
            return Err(format!(
                "control history exceeds the fixed {OBJECT_STORE_CONTROL_MAX_LIST_RECORDS}-record scan bound"
            ));
        }
        sequences.push(sequence_from_path(prefix, &entry.location)?);
    }
    sequences.sort_unstable();
    sequences.dedup();
    Ok(sequences)
}

async fn prune_control_history_batch(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<bool, String> {
    use futures::StreamExt;

    let prefix_path = object_store::path::Path::from(prefix);
    let mut entries = store.list(Some(&prefix_path));
    let mut oldest = BinaryHeap::with_capacity(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS);
    let mut total = 0usize;
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| error.to_string())?;
        let sequence = sequence_from_path(prefix, &entry.location)?;
        total = total.saturating_add(1);
        retain_oldest_control_record(&mut oldest, sequence, &entry.location);
    }
    let delete_count = total
        .saturating_sub(OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN)
        .min(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS);
    let deletions = futures::stream::iter(
        oldest
            .into_sorted_vec()
            .into_iter()
            .take(delete_count)
            .map(|(_, path)| Ok::<_, object_store::Error>(object_store::path::Path::from(path))),
    )
    .boxed();
    let mut results = store.delete_stream(deletions);
    while let Some(result) = results.next().await {
        if let Err(error) = result {
            if !matches!(error, object_store::Error::NotFound { .. }) {
                return Err(error.to_string());
            }
        }
    }
    Ok(total.saturating_sub(delete_count) <= OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN)
}

struct ObjectStoreClusterKv {
    local_id: NodeId,
    local_lease: laminar_core::cluster::control::ProcessLease,
    local_lease_deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    process_lease_ttl_ms: i64,
    store: Arc<dyn object_store::ObjectStore>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    sequence_states:
        std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<Option<u64>>>>>,
    prune_states: Arc<std::sync::Mutex<std::collections::HashMap<String, bool>>>,
}

impl ObjectStoreClusterKv {
    fn new(
        local_lease: laminar_core::cluster::control::ProcessLease,
        local_lease_deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
        process_lease_ttl_ms: i64,
        store: Arc<dyn object_store::ObjectStore>,
        membership_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        debug_assert!(!local_lease.node.is_unassigned());
        debug_assert!(!local_lease.owner.is_nil());
        debug_assert!(local_lease.term > 0);
        debug_assert!(process_lease_ttl_ms > 0);
        Self {
            local_id: local_lease.node,
            local_lease,
            local_lease_deadline,
            process_lease_ttl_ms,
            store,
            membership_rx,
            sequence_states: std::sync::Mutex::new(std::collections::HashMap::new()),
            prune_states: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn visible_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = {
            let members = self.membership_rx.borrow();
            members.iter().map(|member| member.id).collect()
        };
        ids.push(self.local_id);
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn validate_key_and_value(key: &str, value: &str) -> Result<(), String> {
        if key.is_empty() || key.len() > OBJECT_STORE_CONTROL_MAX_KEY_BYTES {
            return Err(format!(
                "control key is {} bytes; expected 1..={OBJECT_STORE_CONTROL_MAX_KEY_BYTES}",
                key.len()
            ));
        }
        // An empty value is intentional: the controller uses it as the durable clear/tombstone
        // signal for a completed recovery round.
        if u64::try_from(value.len()).unwrap_or(u64::MAX) > OBJECT_STORE_CONTROL_MAX_VALUE_BYTES {
            return Err(format!(
                "control value is {} bytes; maximum is {OBJECT_STORE_CONTROL_MAX_VALUE_BYTES}",
                value.len()
            ));
        }
        Ok(())
    }

    async fn load_process_lease(
        &self,
        node: NodeId,
    ) -> Result<Option<laminar_core::cluster::control::ProcessLease>, String> {
        laminar_core::cluster::control::ProcessLeaseStore::new(
            Arc::clone(&self.store),
            node,
            self.process_lease_ttl_ms,
        )
        .load()
        .await
        .map_err(|error| error.to_string())
    }

    fn same_process_term(
        left: &laminar_core::cluster::control::ProcessLease,
        right: &laminar_core::cluster::control::ProcessLease,
    ) -> bool {
        left.node == right.node && left.owner == right.owner && left.term == right.term
    }

    fn require_live_local_deadline(&self) -> Result<(), String> {
        if self.local_lease_deadline.is_live() {
            Ok(())
        } else {
            Err("local process lease deadline expired".into())
        }
    }

    async fn require_local_process_term(&self) -> Result<(), String> {
        self.require_live_local_deadline()?;
        let current = self
            .load_process_lease(self.local_id)
            .await?
            .ok_or_else(|| "local process lease is absent".to_string())?;
        if !Self::same_process_term(&current, &self.local_lease) {
            return Err("local process lease owner or term changed".into());
        }
        self.require_live_local_deadline()
    }

    fn sequence_state(&self, prefix: &str) -> Arc<tokio::sync::Mutex<Option<u64>>> {
        let mut states = self
            .sequence_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            states
                .entry(prefix.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None))),
        )
    }

    async fn put_create(
        &self,
        path: &object_store::path::Path,
        bytes: Vec<u8>,
    ) -> Result<bool, String> {
        let options = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..object_store::PutOptions::default()
        };
        match self
            .store
            .put_opts(
                path,
                object_store::PutPayload::from(bytes::Bytes::from(bytes)),
                options,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    fn schedule_prune(&self, prefix: String) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        {
            let mut states = self
                .prune_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = states.get_mut(&prefix) {
                *pending = true;
                return;
            }
            states.insert(prefix.clone(), false);
        }
        let store = Arc::clone(&self.store);
        let prune_states = Arc::clone(&self.prune_states);
        runtime.spawn(async move {
            loop {
                let prune = async {
                    for _ in 0..OBJECT_STORE_CONTROL_MAX_PRUNE_BATCHES {
                        match tokio::time::timeout(
                            OBJECT_STORE_CONTROL_IO_TIMEOUT,
                            prune_control_history_batch(&store, &prefix),
                        )
                        .await
                        {
                            Ok(Ok(true)) => return Ok(()),
                            Ok(Ok(false)) => tokio::task::yield_now().await,
                            Ok(Err(error)) => return Err(error),
                            Err(_) => {
                                return Err(format!(
                                    "prune timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
                                ));
                            }
                        }
                    }
                    Err("history still exceeds the bounded prune budget".to_string())
                }
                .await;
                if let Err(error) = prune {
                    warn!(%prefix, %error, "object-store control history prune failed");
                }

                let rerun = {
                    let mut states = prune_states
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match states.get_mut(&prefix) {
                        Some(pending) if *pending => {
                            *pending = false;
                            true
                        }
                        Some(_) => {
                            states.remove(&prefix);
                            false
                        }
                        None => false,
                    }
                };
                if !rerun {
                    return;
                }
                tokio::task::yield_now().await;
            }
        });
    }

    async fn read_control_record(
        &self,
        lease: &laminar_core::cluster::control::ProcessLease,
        key: &str,
        sequence: u64,
    ) -> Result<ObjectStoreControlRecord, String> {
        let path = object_store_control_record_path(lease, key, sequence);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| error.to_string())?;
        if result.meta.size > OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES {
            return Err(format!(
                "control envelope is {} bytes; maximum is {OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES}",
                result.meta.size
            ));
        }
        let bytes = result.bytes().await.map_err(|error| error.to_string())?;
        let record: ObjectStoreControlRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        record.validate(lease, key, sequence)?;
        let canonical = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err("control record body is not canonically encoded".into());
        }
        Ok(record)
    }

    async fn write_control_value(&self, key: &str, value: String) -> Result<(), String> {
        Self::validate_key_and_value(key, &value)?;
        self.require_local_process_term().await?;
        let prefix = object_store_control_key_prefix(&self.local_lease, key);
        let state = self.sequence_state(&prefix);
        let mut durable_sequence = state.lock().await;
        if durable_sequence.is_none() {
            let sequences = match list_control_sequences(&self.store, &prefix).await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_prune(prefix.clone());
                    return Err(error);
                }
            };
            *durable_sequence = Some(sequences.last().copied().unwrap_or(0));
        }

        for _ in 0..OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS {
            let sequence = durable_sequence
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| "control record sequence exhausted".to_string())?;
            *durable_sequence = Some(sequence);
            let record = ObjectStoreControlRecord {
                version: OBJECT_STORE_CONTROL_VERSION,
                node: self.local_id.0,
                owner: self.local_lease.owner,
                term: self.local_lease.term,
                sequence,
                key: key.to_string(),
                value: value.clone(),
            };
            let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            if u64::try_from(encoded.len()).unwrap_or(u64::MAX)
                > OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES
            {
                return Err("control envelope exceeds its canonical size bound".into());
            }
            let path = object_store_control_record_path(&self.local_lease, key, sequence);
            if self.put_create(&path, encoded).await? {
                drop(durable_sequence);
                self.schedule_prune(prefix);
                return self.require_local_process_term().await;
            }

            let existing = self
                .read_control_record(&self.local_lease, key, sequence)
                .await?;
            if existing.value == value {
                drop(durable_sequence);
                self.schedule_prune(prefix);
                return self.require_local_process_term().await;
            }
            let sequences = match list_control_sequences(&self.store, &prefix).await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_prune(prefix.clone());
                    return Err(error);
                }
            };
            let head = sequences.last().copied().unwrap_or(sequence);
            *durable_sequence = Some((*durable_sequence).unwrap_or(0).max(head));
        }
        self.schedule_prune(prefix);
        Err(format!(
            "control record create conflicted {OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS} times"
        ))
    }

    async fn read_control_value(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        Self::validate_key_and_value(key, "")?;
        let Some(before) = self.load_process_lease(who).await? else {
            return Ok(None);
        };
        let prefix = object_store_control_key_prefix(&before, key);
        let sequences = match list_control_sequences(&self.store, &prefix).await {
            Ok(sequences) => sequences,
            Err(error) => {
                self.schedule_prune(prefix);
                return Err(error);
            }
        };
        let value = if let Some(sequence) = sequences.last().copied() {
            Some(
                self.read_control_record(&before, key, sequence)
                    .await?
                    .value,
            )
        } else {
            None
        };
        let after = self
            .load_process_lease(who)
            .await?
            .ok_or_else(|| "process lease vanished during control read".to_string())?;
        if !Self::same_process_term(&before, &after) {
            return Err("process lease owner or term changed during control read".into());
        }
        if !sequences.is_empty() {
            self.schedule_prune(prefix);
        }
        Ok(value)
    }

    async fn read_target_value(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        if key == RECOVERY_GENERATION_KEY {
            // Recovery generation is the one cluster-global exception: keeping it outside a
            // process term prevents a full-cluster restart from resetting the recovery epoch.
            self.load_recovery_generation()
                .await
                .map(|generation| generation.map(|value| value.to_string()))
        } else {
            self.read_control_value(who, key).await
        }
    }

    async fn load_recovery_generation(&self) -> Result<Option<u64>, String> {
        let sequences = match list_control_sequences(&self.store, RECOVERY_GENERATION_PREFIX).await
        {
            Ok(sequences) => sequences,
            Err(error) => {
                self.schedule_prune(RECOVERY_GENERATION_PREFIX.to_string());
                return Err(error);
            }
        };
        let Some(generation) = sequences.last().copied() else {
            return Ok(None);
        };
        if generation == 0 {
            return Err("recovery generation record cannot be zero".into());
        }
        let path = recovery_generation_path(generation);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| error.to_string())?;
        if result.meta.size > 512 {
            return Err(format!(
                "recovery generation envelope is {} bytes; maximum is 512",
                result.meta.size
            ));
        }
        let bytes = result.bytes().await.map_err(|error| error.to_string())?;
        let record: RecoveryGenerationRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if record.version != OBJECT_STORE_CONTROL_VERSION
            || record.generation != generation
            || record.writer_node == 0
            || record.writer_owner.is_nil()
            || record.writer_term == 0
        {
            return Err("recovery generation record does not match its durable path".into());
        }
        let canonical = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err("recovery generation body is not canonically encoded".into());
        }
        Ok(Some(generation))
    }

    async fn write_recovery_generation(&self, value: String) -> Result<(), String> {
        let generation = value
            .parse::<u64>()
            .map_err(|error| format!("invalid recovery generation: {error}"))?;
        if generation == 0 || value != generation.to_string() {
            return Err("recovery generation must be a canonical nonzero u64".into());
        }
        self.require_local_process_term().await?;
        if let Some(current) = self.load_recovery_generation().await? {
            if current > generation {
                return Err(format!(
                    "recovery generation {generation} regresses durable generation {current}"
                ));
            }
            if current == generation {
                return Ok(());
            }
        }
        let record = RecoveryGenerationRecord {
            version: OBJECT_STORE_CONTROL_VERSION,
            generation,
            writer_node: self.local_id.0,
            writer_owner: self.local_lease.owner,
            writer_term: self.local_lease.term,
        };
        let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        let path = recovery_generation_path(generation);
        let created = self.put_create(&path, encoded).await?;
        if !created {
            let observed = self.load_recovery_generation().await?;
            if observed != Some(generation) {
                return Err("recovery generation create conflicted with a newer marker".into());
            }
        }
        self.schedule_prune(RECOVERY_GENERATION_PREFIX.to_string());
        let observed = self.load_recovery_generation().await?;
        if observed != Some(generation) {
            return Err(format!(
                "recovery generation {generation} was superseded during publication"
            ));
        }
        self.require_local_process_term().await
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for ObjectStoreClusterKv {
    async fn write(&self, key: &str, value: String) {
        if let Err(error) = self.write_checked(key, value).await {
            warn!(%error, %key, "object-store control write failed");
        }
    }

    async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
        let write = async {
            if key == RECOVERY_GENERATION_KEY {
                self.write_recovery_generation(value).await
            } else {
                self.write_control_value(key, value).await
            }
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, write).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "write timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        match self.read_from_checked(who, key).await {
            Ok(value) => value,
            Err(error) => {
                warn!(node = who.0, %key, %error, "object-store control read failed");
                None
            }
        }
    }

    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        let read = async {
            self.require_local_process_term().await?;
            let value = self.read_target_value(who, key).await?;
            self.require_local_process_term().await?;
            Ok(value)
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, read).await {
            Ok(value) => value,
            Err(_) => Err(format!(
                "read timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        match self.scan_checked(key).await {
            Ok(values) => values,
            Err(error) => {
                warn!(%key, %error, "object-store control scan failed");
                Vec::new()
            }
        }
    }

    async fn scan_checked(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
        use futures::StreamExt;

        let scan = async {
            self.require_local_process_term().await?;
            if key == RECOVERY_GENERATION_KEY {
                let value = self.read_target_value(self.local_id, key).await?;
                self.require_local_process_term().await?;
                return Ok(value.map_or_else(Vec::new, |value| vec![(self.local_id, value)]));
            }
            let mut reads = futures::stream::iter(self.visible_ids())
                .map(|id| async move {
                    let value = self.read_target_value(id, key).await?;
                    Ok::<_, String>((id, value))
                })
                .buffer_unordered(OBJECT_STORE_CONTROL_SCAN_CONCURRENCY);
            let mut results = Vec::new();
            while let Some(result) = reads.next().await {
                let (id, value) = result?;
                if let Some(value) = value {
                    results.push((id, value));
                }
            }
            results.sort_unstable_by_key(|(id, _)| *id);
            self.require_local_process_term().await?;
            Ok(results)
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, scan).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "scan timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }
}

#[cfg(test)]
mod tests;
