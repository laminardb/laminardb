//! Cluster (multi-node) mode startup orchestrator.

use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStoreExt;
use tokio::sync::watch;
use tracing::{info, warn};

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
const NAMESPACE_PROOF_KEY: &str = "control:shared-namespace-proof-v1";
const NAMESPACE_PROOF_VERSION: u8 = 1;
const NAMESPACE_PROOF_MAX_RECORD_BYTES: usize = 512;
const NAMESPACE_PROOF_MAX_SENTINEL_BYTES: u64 = 512;
const NAMESPACE_PROOF_MAX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const NAMESPACE_PROOF_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const NAMESPACE_PROOF_READ_CONCURRENCY: usize = 16;

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct NamespaceProofRecord {
    version: u8,
    node_id: u64,
    boot_incarnation: uuid::Uuid,
    nonce: uuid::Uuid,
    roster_sha256: String,
}

impl NamespaceProofRecord {
    fn validate_identity(
        &self,
        participant: laminar_core::checkpoint::CheckpointParticipant,
    ) -> Result<(), String> {
        if self.version != NAMESPACE_PROOF_VERSION
            || self.node_id != participant.node_id
            || self.boot_incarnation != participant.boot_incarnation
            || self.nonce.is_nil()
            || self.roster_sha256.len() != 64
            || !self
                .roster_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "node {} published a stale or mismatched shared-namespace proof",
                participant.node_id
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum NamespaceProofStore {
    Checkpoint,
    State,
}

impl NamespaceProofStore {
    const fn name(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::State => "state",
        }
    }
}

fn namespace_proof_roster_sha256(
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
) -> String {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    hash.update(b"LAMINAR_SHARED_NAMESPACE_ROSTER_V1\0");
    hash.update(
        u64::try_from(participants.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for participant in participants {
        hash.update(participant.node_id.to_be_bytes());
        hash.update(participant.boot_incarnation.as_bytes());
    }
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn namespace_proof_path(store: NamespaceProofStore, node_id: u64) -> object_store::path::Path {
    // Object-store namespace equality is symmetric. Fixed per-node keys retained for the process
    // lifetime let a rolling joiner test both local clients against every active peer without
    // making peers rerun startup; the stable-node lease authorizes the next boot's overwrite.
    object_store::path::Path::from(format!(
        "cluster-namespace-proof/v1/{}/node={node_id}/sentinel",
        store.name()
    ))
}

fn namespace_proof_sentinel(
    store: NamespaceProofStore,
    record: &NamespaceProofRecord,
) -> bytes::Bytes {
    bytes::Bytes::from(format!(
        "LAMINAR_SHARED_NAMESPACE_V1\n{}\n{}\n{}\n{}\n{}\n",
        store.name(),
        record.node_id,
        record.boot_incarnation,
        record.nonce,
        record.roster_sha256
    ))
}

async fn write_namespace_proof_sentinel(
    object_store: &Arc<dyn object_store::ObjectStore>,
    store: NamespaceProofStore,
    record: &NamespaceProofRecord,
) -> Result<(), String> {
    let payload = namespace_proof_sentinel(store, record);
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) > NAMESPACE_PROOF_MAX_SENTINEL_BYTES {
        return Err("shared-namespace sentinel exceeds its fixed size bound".into());
    }
    object_store
        .put(
            &namespace_proof_path(store, record.node_id),
            object_store::PutPayload::from(payload),
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("write {} namespace sentinel: {error}", store.name()))
}

async fn read_namespace_proof_sentinel(
    object_store: &Arc<dyn object_store::ObjectStore>,
    store: NamespaceProofStore,
    record: &NamespaceProofRecord,
) -> Result<(), String> {
    let result = object_store
        .get(&namespace_proof_path(store, record.node_id))
        .await
        .map_err(|error| {
            format!(
                "read node {} {} namespace sentinel: {error}",
                record.node_id,
                store.name()
            )
        })?;
    if result.meta.size == 0 || result.meta.size > NAMESPACE_PROOF_MAX_SENTINEL_BYTES {
        return Err(format!(
            "node {} {} namespace sentinel is {} bytes; maximum is {}",
            record.node_id,
            store.name(),
            result.meta.size,
            NAMESPACE_PROOF_MAX_SENTINEL_BYTES
        ));
    }
    let bytes = result.bytes().await.map_err(|error| {
        format!(
            "read node {} {} namespace sentinel body: {error}",
            record.node_id,
            store.name()
        )
    })?;
    if bytes != namespace_proof_sentinel(store, record) {
        return Err(format!(
            "node {} {} namespace sentinel does not match its boot proof",
            record.node_id,
            store.name()
        ));
    }
    Ok(())
}

async fn verify_namespace_proof_visibility(
    control: &Arc<dyn laminar_core::cluster::control::ClusterKv>,
    checkpoint_store: &Arc<dyn object_store::ObjectStore>,
    state_store: &Arc<dyn object_store::ObjectStore>,
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
    local: laminar_core::checkpoint::CheckpointParticipant,
    roster_sha256: &str,
) -> Result<(), String> {
    use futures::StreamExt;

    let checks = futures::stream::iter(participants.iter().copied())
        .map(|participant| async move {
            let encoded = control
                .read_from_checked(NodeId(participant.node_id), NAMESPACE_PROOF_KEY)
                .await?
                .ok_or_else(|| {
                    format!(
                        "node {} has not published its shared-namespace proof",
                        participant.node_id
                    )
                })?;
            if encoded.len() > NAMESPACE_PROOF_MAX_RECORD_BYTES {
                return Err(format!(
                    "node {} shared-namespace proof exceeds {} bytes",
                    participant.node_id, NAMESPACE_PROOF_MAX_RECORD_BYTES
                ));
            }
            let record: NamespaceProofRecord = serde_json::from_str(&encoded).map_err(|error| {
                format!(
                    "decode node {} shared-namespace proof: {error}",
                    participant.node_id
                )
            })?;
            record.validate_identity(participant)?;
            if participant == local && record.roster_sha256 != roster_sha256 {
                return Err("local shared-namespace proof has the wrong startup roster".into());
            }
            tokio::try_join!(
                read_namespace_proof_sentinel(
                    checkpoint_store,
                    NamespaceProofStore::Checkpoint,
                    &record,
                ),
                read_namespace_proof_sentinel(state_store, NamespaceProofStore::State, &record),
            )?;
            Ok::<_, String>(())
        })
        .buffer_unordered(NAMESPACE_PROOF_READ_CONCURRENCY);
    let results: Vec<Result<(), String>> = checks.collect().await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn wait_for_namespace_proof_visibility(
    control: &Arc<dyn laminar_core::cluster::control::ClusterKv>,
    checkpoint_store: &Arc<dyn object_store::ObjectStore>,
    state_store: &Arc<dyn object_store::ObjectStore>,
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
    local: laminar_core::checkpoint::CheckpointParticipant,
    roster_sha256: &str,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let mut last_failure = "no verification attempt completed".to_string();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "shared-namespace peer visibility timed out: {last_failure}"
            ));
        }
        match tokio::time::timeout(
            remaining,
            verify_namespace_proof_visibility(
                control,
                checkpoint_store,
                state_store,
                participants,
                local,
                roster_sha256,
            ),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_failure = error,
            Err(_) => {
                return Err(format!(
                    "shared-namespace peer visibility timed out: {last_failure}"
                ));
            }
        }
        tokio::time::sleep(
            NAMESPACE_PROOF_RETRY_INTERVAL
                .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        )
        .await;
    }
}

async fn prove_shared_object_store_namespaces(
    local: laminar_core::checkpoint::CheckpointParticipant,
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
    control: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    checkpoint_store: Arc<dyn object_store::ObjectStore>,
    state_store: Arc<dyn object_store::ObjectStore>,
    timeout: std::time::Duration,
) -> Result<(), ClusterStartupError> {
    if participants.is_empty()
        || participants.len() > laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
        || participants
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        || participants
            .iter()
            .any(|participant| participant.node_id == 0 || participant.boot_incarnation.is_nil())
        || participants
            .iter()
            .filter(|participant| **participant == local)
            .count()
            != 1
    {
        return Err(ClusterStartupError::EngineConstruction(
            "shared-namespace proof requires one canonical exact startup roster".into(),
        ));
    }
    let timeout = timeout.min(NAMESPACE_PROOF_MAX_TIMEOUT);
    if timeout.is_zero() {
        return Err(ClusterStartupError::EngineConstruction(
            "shared-namespace proof timeout is zero".into(),
        ));
    }
    let roster_sha256 = namespace_proof_roster_sha256(participants);
    let record = NamespaceProofRecord {
        version: NAMESPACE_PROOF_VERSION,
        node_id: local.node_id,
        boot_incarnation: local.boot_incarnation,
        nonce: uuid::Uuid::new_v4(),
        roster_sha256: roster_sha256.clone(),
    };
    let deadline = tokio::time::Instant::now() + timeout;
    let proof = async {
        tokio::try_join!(
            write_namespace_proof_sentinel(
                &checkpoint_store,
                NamespaceProofStore::Checkpoint,
                &record,
            ),
            write_namespace_proof_sentinel(&state_store, NamespaceProofStore::State, &record),
        )?;
        let encoded = serde_json::to_string(&record).map_err(|error| error.to_string())?;
        if encoded.len() > NAMESPACE_PROOF_MAX_RECORD_BYTES {
            return Err("local shared-namespace proof exceeds its size bound".to_string());
        }
        control.write_checked(NAMESPACE_PROOF_KEY, encoded).await?;
        wait_for_namespace_proof_visibility(
            &control,
            &checkpoint_store,
            &state_store,
            participants,
            local,
            &roster_sha256,
            deadline,
        )
        .await
    };
    match tokio::time::timeout(timeout, proof).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ClusterStartupError::EngineConstruction(format!(
            "shared checkpoint/state namespace proof failed: {error}"
        ))),
        Err(_) => Err(ClusterStartupError::EngineConstruction(format!(
            "shared checkpoint/state namespace proof exceeded {timeout:?}"
        ))),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
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
    if !config
        .state
        .durability_scope()
        .satisfies(laminar_core::state::StateBackendDurability::ClusterShared)
    {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster mode requires ClusterShared state storage".into(),
        ));
    }
    let state_proof_store = config
        .state
        .build_object_store()
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "state namespace proof object store: {error}"
            ))
        })?
        .ok_or_else(|| {
            ClusterStartupError::EngineConstruction(
                "cluster mode requires an object-store-backed state namespace".into(),
            )
        })?;
    let key_groups = config.server.resolved_key_groups();
    let state_backend = cluster_state_backend(Arc::clone(&state_proof_store), node_id, key_groups);

    // Claim the stable node identity before discovery can publish a duplicate member. The
    // durable recovery authority is deliberately not published until the database runtime exists.
    if !laminar_core::state::StateBackendDurability::for_storage_url(&config.checkpoint.url)
        .satisfies(laminar_core::state::StateBackendDurability::ClusterShared)
    {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster mode requires ClusterShared checkpoint storage".into(),
        ));
    }
    let control_store = build_control_store(&config)?;
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
        .min(NAMESPACE_PROOF_MAX_TIMEOUT);
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
    if let Some(path) = config.state.local_storage_dir() {
        builder = builder.storage_dir(path);
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
            "stable node identity lease was lost before shared-namespace proof".into(),
        ));
    }
    let namespace_proof = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => None,
        result = prove_shared_object_store_namespaces(
            local_participant,
            &startup_participants,
            namespace_control,
            Arc::clone(&control_store),
            state_proof_store,
            cluster_cfg.formation_timeout,
        ) => Some(result),
    };
    match namespace_proof {
        Some(Ok(())) => {}
        Some(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
        None => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during shared-namespace proof".into(),
            ));
        }
    }
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during shared-namespace proof".into(),
        ));
    }
    info!(
        participants = startup_participants.len(),
        "Shared checkpoint and state namespaces verified"
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

    // LaminarDB derives the participant namespace from the installed controller. Keep the base
    // URL shared so durable commit decisions remain visible to every node.
    builder = builder.incremental_emit(config.server.incremental_emit);
    builder =
        server::apply_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint, true);

    builder = builder
        .state_backend(Arc::clone(&state_backend))
        .vnode_registry(Arc::clone(&vnode_registry));

    // Durable cluster 2PC decision store, on the shared control-plane bucket
    // (not the per-node state path) so commit decisions are cluster-wide.
    // Without this the leader's `Commit` announcement is the only commit
    // signal — ephemeral, so a mid-2PC leader crash produces split state.
    let decision_store = Arc::new(
        laminar_core::cluster::control::CheckpointDecisionStore::new(Arc::clone(&control_store)),
    );
    builder = builder.decision_store(decision_store);

    // Hand the builder the same snapshot store resolved in `resolve_vnode_assignment`
    // so the snapshot watcher and rebalance controller share one backing object.
    builder = builder.assignment_snapshot_store(Arc::clone(&snapshot_store));

    // Catalog sealing and leader fencing share one append-only CAS sequence. A catalog write can
    // therefore linearize before a takeover or be rejected by it; there is no check-then-write
    // gap across independent objects.
    let lease_cfg = laminar_core::cluster::control::LeaderLeaseConfig::default();
    let ttl_ms = match i64::try_from(lease_cfg.ttl.as_millis()) {
        Ok(ttl_ms) => ttl_ms,
        Err(_) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(
                "leader lease TTL exceeds the durable diagnostic range".into(),
            ));
        }
    };
    let lease_store = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
        Arc::clone(&control_store),
        ttl_ms,
    ));
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

    // Re-acquire the stored assignment through the standard adopt path (a restart boots
    // unassigned). Re-load each attempt so a shed that raced the boot wins; bounded retry so a
    // transient object-store failure cannot strand the node before its watcher starts.
    let snap_store = Arc::clone(&snapshot_store);
    let mut assignment_ready = false;
    for attempt in 0u32..5 {
        let adoption_deadline = tokio::time::Instant::now() + rebalance_config.checkpoint_timeout;
        let snapshot_load = tokio::select! {
            biased;
            () = process_lease_terminal.cancelled() => None,
            result = tokio::time::timeout_at(adoption_deadline, snap_store.load()) => {
                Some(result)
            }
        };
        let Some(snapshot_load) = snapshot_load else {
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                true,
            )
            .await;
            return Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost during startup assignment adoption".into(),
            ));
        };
        match snapshot_load {
            Ok(Ok(Some(durable_head))) => {
                let recovering_drain = durable_head.draining;
                let authority_audit = tokio::select! {
                    biased;
                    () = process_lease_terminal.cancelled() => None,
                    result = tokio::time::timeout_at(adoption_deadline, async {
                        laminar_db::rebalance::startup_committed_assignment(
                            snap_store.as_ref(),
                            Some(startup_controller),
                            durable_head,
                        )
                        .await
                        .map_err(ClusterStartupError::EngineConstruction)
                    }) => Some(result),
                };
                let Some(authority_audit) = authority_audit else {
                    cleanup_cluster_startup(
                        &mut discovery,
                        &db,
                        &mut leader_lease,
                        &process_lease_terminal,
                        true,
                    )
                    .await;
                    return Err(ClusterStartupError::AuthorityLost(
                        "stable node identity lease was lost during startup assignment audit"
                            .into(),
                    ));
                };
                let snapshot = match authority_audit {
                    Ok(Ok(snapshot)) => snapshot,
                    Ok(Err(error)) => {
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
                    Err(_) => {
                        cleanup_cluster_startup(
                            &mut discovery,
                            &db,
                            &mut leader_lease,
                            &process_lease_terminal,
                            false,
                        )
                        .await;
                        return Err(ClusterStartupError::EngineConstruction(
                            "startup assignment authority audit timed out".into(),
                        ));
                    }
                };
                if vnode_registry.assignment_version() >= snapshot.version {
                    assignment_ready = true;
                    break; // already adopted (watcher raced us)
                }
                let committed_version = snapshot.version;
                let adoption = tokio::select! {
                    biased;
                    () = process_lease_terminal.cancelled() => None,
                    result = db.adopt_assignment_snapshot(snapshot, adoption_deadline) => {
                        Some(result)
                    }
                };
                let Some(adoption) = adoption else {
                    cleanup_cluster_startup(
                        &mut discovery,
                        &db,
                        &mut leader_lease,
                        &process_lease_terminal,
                        true,
                    )
                    .await;
                    return Err(ClusterStartupError::AuthorityLost(
                        "stable node identity lease was lost while adopting startup state".into(),
                    ));
                };
                let adoption = match adoption {
                    Ok(adoption) => adoption,
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
                            "assignment state recovery: {error}"
                        )));
                    }
                };
                info!(
                    version = adoption.version,
                    adopted = adoption.adopted,
                    newly_acquired = adoption.newly_acquired.len(),
                    rehydrated = adoption.rehydrated,
                    "startup assignment adoption"
                );
                if adoption.adopted {
                    assignment_ready = true;
                    if recovering_drain {
                        info!(
                                committed_version,
                                "startup adopted the retained committed assignment; durable drain abort remains fenced"
                            );
                    }
                    break;
                }
            }
            Ok(Ok(None)) => {
                cleanup_cluster_startup(
                    &mut discovery,
                    &db,
                    &mut leader_lease,
                    &process_lease_terminal,
                    false,
                )
                .await;
                return Err(ClusterStartupError::EngineConstruction(
                    "durable assignment snapshot disappeared during startup".into(),
                ));
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, attempt, "startup snapshot load failed");
            }
            Err(_) => tracing::warn!(
                attempt,
                timeout = ?rebalance_config.checkpoint_timeout,
                "startup snapshot load timed out"
            ),
        }
        if attempt < 4 {
            tokio::select! {
                biased;
                () = process_lease_terminal.cancelled() => {
                    cleanup_cluster_startup(
                        &mut discovery,
                        &db,
                        &mut leader_lease,
                        &process_lease_terminal,
                        true,
                    )
                    .await;
                    return Err(ClusterStartupError::AuthorityLost(
                        "stable node identity lease was lost during startup assignment retry"
                            .into(),
                    ));
                }
                () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
            }
        }
    }
    if !assignment_ready {
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            false,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "durable startup assignment could not be adopted after five attempts".into(),
        ));
    }

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

fn cluster_state_backend(
    store: Arc<dyn object_store::ObjectStore>,
    node_id: NodeId,
    key_groups: laminar_core::state::KeyGroupCount,
) -> Arc<dyn laminar_core::state::StateBackend> {
    Arc::new(laminar_core::state::ObjectStoreBackend::cluster_shared(
        store,
        node_id.to_string(),
        u32::from(key_groups),
    ))
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
    laminar_core::storage::object_store_builder::build_object_store(
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
        .start_barrier_server(bind, Some(advertise_host.to_string()))
        .await
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("barrier sync server bind: {e}"))
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
                            sender_clone.register_peer(node.id.0, addr).await;
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
mod tests {
    use super::*;

    struct DelayedControlPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
        blocked_path: parking_lot::Mutex<Option<object_store::path::Path>>,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        completed: Arc<tokio::sync::Semaphore>,
        blocked_get_path: parking_lot::Mutex<Option<object_store::path::Path>>,
        get_entered: tokio::sync::Semaphore,
        get_release: tokio::sync::Semaphore,
        track_get_concurrency: std::sync::atomic::AtomicBool,
        active_gets: std::sync::atomic::AtomicUsize,
        max_gets: std::sync::atomic::AtomicUsize,
    }

    impl DelayedControlPutStore {
        fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
            Self {
                inner,
                blocked_path: parking_lot::Mutex::new(None),
                entered: Arc::new(tokio::sync::Semaphore::new(0)),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                completed: Arc::new(tokio::sync::Semaphore::new(0)),
                blocked_get_path: parking_lot::Mutex::new(None),
                get_entered: tokio::sync::Semaphore::new(0),
                get_release: tokio::sync::Semaphore::new(0),
                track_get_concurrency: std::sync::atomic::AtomicBool::new(false),
                active_gets: std::sync::atomic::AtomicUsize::new(0),
                max_gets: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn block_once(&self, path: object_store::path::Path) {
            *self.blocked_path.lock() = Some(path);
        }

        async fn wait_until_blocked(&self) {
            self.entered.acquire().await.unwrap().forget();
        }

        fn release(&self) {
            self.release.add_permits(1);
        }

        async fn wait_until_completed(&self) {
            self.completed.acquire().await.unwrap().forget();
        }

        fn block_get_once(&self, path: object_store::path::Path) {
            *self.blocked_get_path.lock() = Some(path);
        }

        async fn wait_until_get_blocked(&self) {
            self.get_entered.acquire().await.unwrap().forget();
        }

        fn release_get(&self) {
            self.get_release.add_permits(1);
        }

        fn begin_get_concurrency_probe(&self) {
            self.active_gets
                .store(0, std::sync::atomic::Ordering::Release);
            self.max_gets.store(0, std::sync::atomic::Ordering::Release);
            self.track_get_concurrency
                .store(true, std::sync::atomic::Ordering::Release);
        }

        fn finish_get_concurrency_probe(&self) -> usize {
            self.track_get_concurrency
                .store(false, std::sync::atomic::Ordering::Release);
            self.max_gets.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    impl std::fmt::Debug for DelayedControlPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("DelayedControlPutStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for DelayedControlPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("DelayedControlPutStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for DelayedControlPutStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let should_block = {
                let mut blocked_path = self.blocked_path.lock();
                if blocked_path.as_ref() == Some(location) {
                    blocked_path.take();
                    true
                } else {
                    false
                }
            };
            if should_block {
                self.entered.add_permits(1);
                let inner = Arc::clone(&self.inner);
                let release = Arc::clone(&self.release);
                let completed = Arc::clone(&self.completed);
                let location = location.clone();
                let (sender, receiver) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    let result = match release.acquire().await {
                        Ok(permit) => {
                            permit.forget();
                            inner.put_opts(&location, payload, options).await
                        }
                        Err(error) => Err(object_store::Error::Generic {
                            store: "DelayedControlPutStore",
                            source: Box::new(error),
                        }),
                    };
                    completed.add_permits(1);
                    let _ = sender.send(result);
                });
                return receiver
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "DelayedControlPutStore",
                        source: Box::new(error),
                    })?;
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let track_concurrency = self
                .track_get_concurrency
                .load(std::sync::atomic::Ordering::Acquire);
            if track_concurrency {
                let active = self
                    .active_gets
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                    + 1;
                self.max_gets
                    .fetch_max(active, std::sync::atomic::Ordering::AcqRel);
                tokio::task::yield_now().await;
            }
            let should_block = {
                let mut blocked_path = self.blocked_get_path.lock();
                if blocked_path.as_ref() == Some(location) {
                    blocked_path.take();
                    true
                } else {
                    false
                }
            };
            if should_block {
                self.get_entered.add_permits(1);
                self.get_release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "DelayedControlPutStore",
                        source: Box::new(error),
                    })?
                    .forget();
            }
            let result = self.inner.get_opts(location, options).await;
            if track_concurrency {
                self.active_gets
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
            result
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn acquire_test_process_lease(
        store: Arc<dyn object_store::ObjectStore>,
        node: NodeId,
        owner: uuid::Uuid,
        ttl_ms: i64,
    ) -> laminar_core::cluster::control::ProcessLease {
        use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

        let authority = ProcessLeaseStore::new(store, node, ttl_ms);
        let ProcessLeaseOutcome::Acquired(lease) = authority.try_acquire(owner, 1).await.unwrap()
        else {
            panic!("test process lease was not acquired");
        };
        lease
    }

    async fn take_over_test_process_lease(
        store: Arc<dyn object_store::ObjectStore>,
        incumbent: &laminar_core::cluster::control::ProcessLease,
        replacement: uuid::Uuid,
        ttl_ms: i64,
    ) -> laminar_core::cluster::control::ProcessLease {
        use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

        let authority = ProcessLeaseStore::new(store, incumbent.node, ttl_ms);
        let observation = authority.observe_rival(incumbent).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(
            u64::try_from(ttl_ms).unwrap() + 2,
        ))
        .await;
        let ProcessLeaseOutcome::Acquired(lease) = authority
            .try_takeover(replacement, &observation, 100)
            .await
            .unwrap()
        else {
            panic!("test process lease was not taken over");
        };
        lease
    }

    fn live_test_process_deadline() -> Arc<laminar_core::cluster::control::LeaseDeadline> {
        Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        ))
    }

    #[derive(Clone)]
    struct NamespaceProofTestKv {
        local_id: NodeId,
        values: Arc<parking_lot::Mutex<HashMap<(NodeId, String), String>>>,
    }

    #[async_trait::async_trait]
    impl laminar_core::cluster::control::ClusterKv for NamespaceProofTestKv {
        async fn write(&self, key: &str, value: String) {
            self.values
                .lock()
                .insert((self.local_id, key.to_string()), value);
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.values.lock().get(&(who, key.to_string())).cloned()
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.values
                .lock()
                .iter()
                .filter(|((_, stored_key), _)| stored_key == key)
                .map(|((node, _), value)| (*node, value.clone()))
                .collect()
        }
    }

    fn namespace_proof_participants() -> [laminar_core::checkpoint::CheckpointParticipant; 2] {
        [
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(11),
            },
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
            },
        ]
    }

    fn namespace_proof_test_kvs() -> [Arc<dyn laminar_core::cluster::control::ClusterKv>; 2] {
        let values = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        [
            Arc::new(NamespaceProofTestKv {
                local_id: NodeId(1),
                values: Arc::clone(&values),
            }),
            Arc::new(NamespaceProofTestKv {
                local_id: NodeId(2),
                values,
            }),
        ]
    }

    async fn run_two_node_namespace_proof(
        participants: &[laminar_core::checkpoint::CheckpointParticipant; 2],
        controls: &[Arc<dyn laminar_core::cluster::control::ClusterKv>; 2],
        checkpoint_stores: [Arc<dyn object_store::ObjectStore>; 2],
        state_stores: [Arc<dyn object_store::ObjectStore>; 2],
        timeout: std::time::Duration,
    ) -> [Result<(), ClusterStartupError>; 2] {
        let first = prove_shared_object_store_namespaces(
            participants[0],
            participants,
            Arc::clone(&controls[0]),
            Arc::clone(&checkpoint_stores[0]),
            Arc::clone(&state_stores[0]),
            timeout,
        );
        let second = prove_shared_object_store_namespaces(
            participants[1],
            participants,
            Arc::clone(&controls[1]),
            Arc::clone(&checkpoint_stores[1]),
            Arc::clone(&state_stores[1]),
            timeout,
        );
        let (first, second) = tokio::join!(first, second);
        [first, second]
    }

    async fn namespace_marker_count(
        store: &Arc<dyn object_store::ObjectStore>,
        role: NamespaceProofStore,
    ) -> usize {
        use futures::StreamExt;

        let prefix =
            object_store::path::Path::from(format!("cluster-namespace-proof/v1/{}/", role.name()));
        let mut entries = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(entry) = entries.next().await {
            entry.unwrap();
            count += 1;
        }
        count
    }

    #[test]
    fn test_cluster_startup_error_display() {
        let errors: Vec<ClusterStartupError> = vec![
            ClusterStartupError::Discovery("connection refused".into()),
            ClusterStartupError::FormationTimeout {
                found: 1,
                needed: 3,
            },
            ClusterStartupError::EngineConstruction("build failed".into()),
            ClusterStartupError::HttpStartup("port in use".into()),
            ClusterStartupError::AuthorityLost("process lease expired".into()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn test_formation_timeout_includes_counts() {
        let err = ClusterStartupError::FormationTimeout {
            found: 1,
            needed: 3,
        };
        let msg = err.to_string();
        assert!(msg.contains('1'));
        assert!(msg.contains('3'));
    }

    #[tokio::test]
    async fn terminal_process_signal_preempts_the_os_shutdown_wait() {
        let terminal = tokio_util::sync::CancellationToken::new();
        terminal.cancel();

        let trigger = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            wait_for_cluster_shutdown_trigger(&terminal),
        )
        .await
        .expect("terminal process signal must wake shutdown promptly")
        .unwrap();

        assert_eq!(trigger, ClusterShutdownTrigger::ProcessLeaseLost);
    }

    #[tokio::test]
    async fn process_lease_terminal_monitor_starts_before_resource_fencing() {
        let (live_tx, live_rx) = watch::channel(true);
        let terminal = tokio_util::sync::CancellationToken::new();
        let monitor = spawn_process_lease_terminal_monitor(
            live_rx,
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_secs(10),
            )),
            terminal.clone(),
        );

        live_tx.send_replace(false);
        tokio::time::timeout(std::time::Duration::from_millis(100), terminal.cancelled())
            .await
            .expect("terminal monitor must publish loss without installed resources");
        monitor.await.unwrap();
    }

    #[tokio::test]
    async fn process_lease_terminal_monitor_observes_the_monotonic_deadline() {
        let (_live_tx, live_rx) = watch::channel(true);
        let terminal = tokio_util::sync::CancellationToken::new();
        let monitor = spawn_process_lease_terminal_monitor(
            live_rx,
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_millis(20),
            )),
            terminal.clone(),
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), terminal.cancelled())
            .await
            .expect("monotonic expiry must publish terminal lease loss");
        monitor.await.unwrap();
    }

    #[tokio::test]
    async fn intentional_process_lease_disarm_cannot_run_the_loss_fence() {
        let node = NodeId(49);
        let owner = uuid::Uuid::from_u128(499);
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let acquired = acquire_test_process_lease(store, node, owner, 10_000).await;
        let deadline = live_test_process_deadline();
        let deadline_observer = Arc::clone(&deadline);
        let (_live_tx, live_rx) = watch::channel(true);
        let terminal = tokio_util::sync::CancellationToken::new();
        let terminal_observer = terminal.clone();
        let terminal_task = spawn_process_lease_terminal_monitor(
            live_rx.clone(),
            Arc::clone(&deadline),
            terminal.clone(),
        );
        let terminal_abort = terminal_task.abort_handle();
        let fence_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fence_terminal = terminal.clone();
        let fence_ran_task = Arc::clone(&fence_ran);
        let fence_task = tokio::spawn(async move {
            fence_terminal.cancelled().await;
            fence_ran_task.store(true, std::sync::atomic::Ordering::Release);
        });
        let fence_abort = fence_task.abort_handle();
        let renewal_task = tokio::spawn(std::future::pending::<()>());
        let renewal_abort = renewal_task.abort_handle();
        let mut process_lease = ProcessLeaseRuntime {
            acquired,
            deadline,
            live_rx,
            shutdown: tokio_util::sync::CancellationToken::new(),
            terminal,
            renewal_task,
            terminal_task,
            fence_task: Some(fence_task),
        };

        assert!(process_lease.disarm_for_shutdown());
        deadline_observer.fence();
        terminal_observer.cancel();

        let tasks = [terminal_abort, fence_abort, renewal_abort];
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while tasks.iter().any(|task| !task.is_finished()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disarmed process lease tasks must terminate");
        assert!(!fence_ran.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn rebalance_tasks_receive_a_graceful_shutdown_before_abort() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        shutdown.cancel();
        let task_shutdown = shutdown.clone();
        let task_stopped = Arc::clone(&stopped);
        let task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            task_shutdown.cancelled().await;
            task_stopped.store(true, std::sync::atomic::Ordering::Release);
        });
        let mut tasks = vec![task];

        assert!(stop_rebalance_tasks(&mut tasks, &shutdown).await);

        assert!(tasks.is_empty());
        assert!(stopped.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn leader_lease_runtime_cancels_and_joins_its_task() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_shutdown = shutdown.clone();
        let task_stopped = Arc::clone(&stopped);
        let task = tokio::spawn(async move {
            task_shutdown.cancelled().await;
            task_stopped.store(true, std::sync::atomic::Ordering::Release);
        });
        let mut runtime = LeaderLeaseRuntime::new(shutdown, task);

        runtime.stop().await;

        assert!(stopped.load(std::sync::atomic::Ordering::Acquire));
        assert!(runtime.task.is_none());
    }

    #[tokio::test]
    async fn dropping_cluster_handle_fences_authority_and_aborts_owned_tasks() {
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

        let node = NodeId(47);
        let boot = uuid::Uuid::from_u128(477);
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let snapshot_store = Arc::new(
            laminar_core::cluster::control::AssignmentSnapshotStore::new(Arc::clone(&store)),
        );
        let acquired = acquire_test_process_lease(Arc::clone(&store), node, boot, 10_000).await;
        let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&control),
            control,
            Some(Arc::clone(&snapshot_store)),
            members_rx,
            boot,
        ));
        let deadline = live_test_process_deadline();
        controller
            .set_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let serving_gate = Arc::new(crate::http::ServingGate::starting());
        assert!(serving_gate.open());

        let local_node = NodeInfo {
            id: node,
            name: "drop-test".into(),
            rpc_address: "127.0.0.1:0".into(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let discovery = DiscoveryImpl::Static(StaticDiscovery::new(StaticDiscoveryConfig {
            local_node: local_node.clone(),
            seeds: vec!["127.0.0.1:1".into()],
            heartbeat_interval: std::time::Duration::from_secs(1),
            suspect_threshold: 3,
            dead_threshold: 10,
            listen_address: "127.0.0.1:0".into(),
            process_generation: acquired.term,
            process_incarnation: boot,
        }));

        let pending_task = || tokio::spawn(std::future::pending::<()>());
        let api_handle = pending_task();
        let api_abort = api_handle.abort_handle();
        let watcher_handle = pending_task();
        let watcher_abort = watcher_handle.abort_handle();
        let membership_handle = pending_task();
        let membership_abort = membership_handle.abort_handle();
        let rebalance_task = pending_task();
        let rebalance_abort = rebalance_task.abort_handle();

        let leader_shutdown = tokio_util::sync::CancellationToken::new();
        let leader_task_shutdown = leader_shutdown.clone();
        let leader_task = tokio::spawn(async move {
            leader_task_shutdown.cancelled().await;
        });
        let leader_abort = leader_task.abort_handle();
        let leader_lease = LeaderLeaseRuntime::new(leader_shutdown.clone(), leader_task);

        let (live_tx, live_rx) = watch::channel(true);
        let process_terminal = tokio_util::sync::CancellationToken::new();
        let process_terminal_observer = process_terminal.clone();
        let process_deadline_observer = Arc::clone(&deadline);
        let terminal_task = spawn_process_lease_terminal_monitor(
            live_rx.clone(),
            Arc::clone(&deadline),
            process_terminal.clone(),
        );
        let terminal_abort = terminal_task.abort_handle();
        let renewal_task = pending_task();
        let renewal_abort = renewal_task.abort_handle();
        let process_lease = ProcessLeaseRuntime {
            acquired,
            deadline,
            live_rx,
            shutdown: tokio_util::sync::CancellationToken::new(),
            terminal: process_terminal,
            renewal_task,
            terminal_task,
            fence_task: None,
        };

        let handle = ClusterHandle {
            db: Arc::clone(&db),
            db_shutdown_complete: false,
            discovery,
            serving_gate: Arc::clone(&serving_gate),
            api_handle,
            watcher_handle: Some(watcher_handle),
            membership_handle,
            local_node,
            cluster_controller: Arc::clone(&controller),
            snapshot_store,
            vnode_count: 1,
            leader_lease,
            process_lease,
            rebalance_tasks: vec![rebalance_task],
            rebalance_shutdown: tokio_util::sync::CancellationToken::new(),
        };
        assert!(controller.process_lease_is_live());

        drop(handle);
        assert!(!process_deadline_observer.is_live());
        assert!(process_terminal_observer.is_cancelled());
        drop(live_tx);

        assert!(!serving_gate.open());
        assert!(!controller.process_lease_is_live());
        assert!(db.cluster_intake_fenced());
        assert!(leader_shutdown.is_cancelled());
        let tasks = [
            api_abort,
            watcher_abort,
            membership_abort,
            rebalance_abort,
            leader_abort,
            terminal_abort,
            renewal_abort,
        ];
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while tasks.iter().any(|task| !task.is_finished()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned runtime tasks must terminate");
    }

    #[tokio::test]
    async fn process_lease_loss_revokes_http_controller_and_database_authority() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

        let node = NodeId(41);
        let boot = uuid::Uuid::from_u128(411);
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let acquired = acquire_test_process_lease(Arc::clone(&store), node, boot, 10_000).await;
        let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&control),
            control,
            None,
            members_rx,
            boot,
        ));
        let deadline = live_test_process_deadline();
        controller
            .set_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[node.0],
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: boot,
            }],
        )
        .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::clone(&store))
            .build()
            .await
            .unwrap();
        assert!(controller.process_lease_is_live());
        let serving_gate = Arc::new(crate::http::ServingGate::starting());
        assert!(serving_gate.open());
        let (live_tx, live_rx) = watch::channel(true);
        let terminal = tokio_util::sync::CancellationToken::new();
        let terminal_task = spawn_process_lease_terminal_monitor(
            live_rx.clone(),
            Arc::clone(&deadline),
            terminal.clone(),
        );
        let mut process_lease = ProcessLeaseRuntime {
            acquired,
            deadline,
            live_rx,
            shutdown: tokio_util::sync::CancellationToken::new(),
            terminal,
            renewal_task: tokio::spawn(std::future::pending()),
            terminal_task,
            fence_task: None,
        };
        let terminal = process_lease.terminal_token();
        let leader_shutdown = tokio_util::sync::CancellationToken::new();
        process_lease.install_fence(
            Arc::clone(&db),
            Arc::clone(&controller),
            Arc::clone(&serving_gate),
            leader_shutdown.clone(),
        );

        live_tx.send_replace(false);
        let fence_task = process_lease.fence_task.take().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), fence_task)
            .await
            .expect("process fence must run promptly")
            .unwrap();

        assert!(!controller.process_lease_is_live());
        assert_eq!(controller.checkpoint_assignment_fence(1), None);
        assert!(db.cluster_intake_fenced());
        assert!(!serving_gate.open());
        assert!(leader_shutdown.is_cancelled());
        assert!(terminal.is_cancelled());
    }

    #[tokio::test]
    async fn occupied_http_port_fails_before_local_cluster_activation() {
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = occupied.local_addr().unwrap().to_string();
        let node = NodeId(41);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let assignment_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let snapshot_store = Arc::new(
            laminar_core::cluster::control::AssignmentSnapshotStore::new(assignment_store),
        );
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(
            node,
            kv,
            Some(Arc::clone(&snapshot_store)),
            members_rx,
        ));
        controller.set_active(false);

        let server_config = crate::config::ServerSection {
            bind,
            ..Default::default()
        };
        let config = ServerConfig {
            server: server_config,
            state: laminar_core::state::StateBackendConfig::default(),
            checkpoint: crate::config::CheckpointSection::default(),
            supervision: Default::default(),
            sources: Vec::new(),
            lookups: Vec::new(),
            pipelines: Vec::new(),
            sinks: Vec::new(),
            sql: None,
            discovery: None,
            node_id: None,
            ai: Default::default(),
            models: Default::default(),
        };
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let (_cluster_members_tx, cluster_members_rx) = watch::channel(Vec::new());
        let cluster = crate::http::ClusterComponents {
            controller: Arc::clone(&controller),
            snapshot_store,
            membership_rx: cluster_members_rx,
        };

        let result = start_cluster_http_api_before_activation(
            LaminarDB::open().unwrap(),
            registry,
            PathBuf::from("unused.toml"),
            config,
            Arc::new(crate::http::ServingGate::starting()),
            cluster,
        )
        .await;
        let error = match result {
            Ok(_) => panic!("occupied HTTP port unexpectedly bound"),
            Err(error) => error,
        };
        assert!(matches!(error, ClusterStartupError::HttpStartup(_)));
        assert!(!controller.live_instances().contains(&node));
    }

    #[tokio::test]
    async fn cluster_state_seal_records_runtime_node_id() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let node_id = NodeId(73);
        let backend = cluster_state_backend(
            Arc::clone(&store),
            node_id,
            laminar_core::state::LOCAL_KEY_GROUP_COUNT,
        );
        let attempt = laminar_core::state::CheckpointAttempt::new(9, 17);

        backend
            .write_partial(attempt, 0, 0, bytes::Bytes::from_static(b"state"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, None, &[0], &[])
            .await
            .unwrap());

        let path = object_store::path::Path::from(format!(
            "state-v2/epoch={}/checkpoint={}/_SEAL",
            attempt.epoch, attempt.checkpoint_id
        ));
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        let seal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let expected = node_id.to_string();
        assert_eq!(seal["instance_id"].as_str(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn object_store_control_kv_survives_reconstruction() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let first = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx.clone(),
        );
        first
            .write_checked("control:recover", "release-13".into())
            .await
            .unwrap();
        drop(first);

        let replacement = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        assert_eq!(
            replacement.read_from(NodeId(7), "control:recover").await,
            Some("release-13".into())
        );
        replacement
            .write_checked("control:recover", "release-14".into())
            .await
            .unwrap();
        assert_eq!(
            replacement.read_from(NodeId(7), "control:recover").await,
            Some("release-14".into())
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_rejects_oversized_values_before_body_read() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let oversized = usize::try_from(OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES + 1).unwrap();
        store
            .put(
                &object_store_control_record_path(&lease, "control:oversized", 1),
                object_store::PutPayload::from(bytes::Bytes::from(vec![0; oversized])),
            )
            .await
            .unwrap();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let kv = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        let error = kv
            .read_from_checked(NodeId(7), "control:oversized")
            .await
            .unwrap_err();
        assert!(error.contains("maximum"), "{error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_rejects_write_after_local_lease_deadline_expires() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = ObjectStoreClusterKv::new(
            lease.clone(),
            Arc::new(laminar_core::cluster::control::LeaseDeadline::fenced()),
            1_000,
            Arc::clone(&store),
            members_rx,
        );
        let error = kv
            .write_checked("control:recover", "must-not-publish".into())
            .await
            .unwrap_err();
        assert!(error.contains("deadline expired"), "{error}");
        assert!(list_control_sequences(
            &store,
            &object_store_control_key_prefix(&lease, "control:recover")
        )
        .await
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn object_store_control_kv_ignores_delayed_previous_term_write() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = Arc::new(ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        let stale_path = object_store_control_record_path(&first_lease, "control:recover", 1);
        delayed.block_once(stale_path.clone());
        let stale_writer = {
            let first = Arc::clone(&first);
            tokio::spawn(
                async move { first.write_checked("control:recover", "stale".into()).await },
            )
        };
        delayed.wait_until_blocked().await;
        stale_writer.abort();
        let _ = stale_writer.await;

        let replacement_lease = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;
        let replacement = ObjectStoreClusterKv::new(
            replacement_lease,
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx,
        );
        replacement
            .write_checked("control:recover", "current".into())
            .await
            .unwrap();

        delayed.release();
        delayed.wait_until_completed().await;
        assert!(inner.get(&stale_path).await.is_ok());
        assert_eq!(
            replacement.read_from(NodeId(7), "control:recover").await,
            Some("current".into())
        );
    }

    #[tokio::test]
    async fn superseded_process_phase_cannot_clobber_replacement_release() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome,
            ProcessLeaseAuthority, RecoverPhase, RecoveryAnnouncement, RecoveryFault,
            RecoveryRound,
        };

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let node = NodeId(7);
        let ttl_ms = 1;
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(
                Arc::clone(&store),
                std::time::Duration::from_millis(u64::try_from(ttl_ms).unwrap()),
            )
            .unwrap(),
        );
        let old_boot = uuid::Uuid::from_u128(71);
        let old_process =
            acquire_test_process_lease(Arc::clone(&store), node, old_boot, ttl_ms).await;
        let old_deadline = live_test_process_deadline();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let old_kv_impl = Arc::new(ObjectStoreClusterKv::new(
            old_process.clone(),
            Arc::clone(&old_deadline),
            ttl_ms,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        let old_kv: Arc<dyn ClusterKv> = old_kv_impl;
        let old_controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&old_kv),
            old_kv,
            None,
            members_rx.clone(),
            old_boot,
        ));
        old_controller
            .set_process_lease_deadline(Arc::clone(&old_deadline))
            .unwrap();
        old_controller
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        old_controller
            .publish_leased_recovery_incarnation(&old_process)
            .await
            .unwrap();

        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&store), ttl_ms));
        let old_owner = LeaderLeaseOwner {
            node,
            boot: old_process.owner,
            process_term: old_process.term,
        };
        let LeaseOutcome::Acquired(old_leader) =
            authority.begin_new_term(&old_owner, 0).await.unwrap()
        else {
            panic!("old process must acquire empty leader authority");
        };
        let (_old_leader_tx, old_leader_rx) = watch::channel(Some(old_leader.clone()));
        old_controller
            .set_leader_lease_watch(old_leader_rx, old_owner, old_deadline)
            .unwrap();
        old_controller.set_leader_lease_store(Arc::clone(&authority));

        let old_participant = CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: old_boot,
        };
        let old_round = RecoveryRound::new(
            61,
            old_leader.proof(),
            CheckpointAssignmentFence::from_owner_map(7, &[node.0], vec![old_participant]).unwrap(),
            Vec::new(),
            61,
            vec![RecoveryFault {
                reporter: node,
                sequence: 1,
            }],
        )
        .unwrap();
        old_controller
            .publish_checkpoint_assignment_fence(Some(old_round.assignment_fence.clone()));
        old_controller
            .announce_recover_prepare(&old_round)
            .await
            .unwrap();
        old_controller
            .announce_recover_start(&old_round, 4)
            .await
            .unwrap();

        let stale_path = object_store_control_record_path(&old_process, "control:recover", 3);
        delayed.block_once(stale_path.clone());
        let stale_release = {
            let controller = Arc::clone(&old_controller);
            let round = old_round.clone();
            tokio::spawn(async move { controller.announce_recover_release(&round, 4).await })
        };
        delayed.wait_until_blocked().await;

        let replacement_boot = uuid::Uuid::from_u128(72);
        let replacement_process = take_over_test_process_lease(
            Arc::clone(&store),
            &old_process,
            replacement_boot,
            ttl_ms,
        )
        .await;
        let replacement_owner = LeaderLeaseOwner {
            node,
            boot: replacement_process.owner,
            process_term: replacement_process.term,
        };
        let leader_observation = authority
            .observe_rival(&replacement_owner, &old_leader)
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        let LeaseOutcome::Acquired(replacement_leader) = authority
            .try_takeover(&replacement_owner, &leader_observation, 10)
            .await
            .unwrap()
        else {
            panic!("replacement process must take over leader authority");
        };

        let replacement_deadline = live_test_process_deadline();
        let replacement_kv_impl = Arc::new(ObjectStoreClusterKv::new(
            replacement_process.clone(),
            Arc::clone(&replacement_deadline),
            ttl_ms,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        let replacement_kv: Arc<dyn ClusterKv> = replacement_kv_impl;
        let replacement_controller = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&replacement_kv),
            replacement_kv,
            None,
            members_rx,
            replacement_boot,
        );
        replacement_controller
            .set_process_lease_deadline(Arc::clone(&replacement_deadline))
            .unwrap();
        replacement_controller
            .set_process_lease_authority(process_authority)
            .unwrap();
        replacement_controller
            .publish_leased_recovery_incarnation(&replacement_process)
            .await
            .unwrap();
        let (_replacement_leader_tx, replacement_leader_rx) =
            watch::channel(Some(replacement_leader.clone()));
        replacement_controller
            .set_leader_lease_watch(
                replacement_leader_rx,
                replacement_owner,
                replacement_deadline,
            )
            .unwrap();
        replacement_controller.set_leader_lease_store(authority);

        let replacement_participant = CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: replacement_boot,
        };
        let replacement_round = RecoveryRound::new(
            62,
            replacement_leader.proof(),
            CheckpointAssignmentFence::from_owner_map(8, &[node.0], vec![replacement_participant])
                .unwrap(),
            Vec::new(),
            62,
            vec![RecoveryFault {
                reporter: node,
                sequence: 2,
            }],
        )
        .unwrap();
        replacement_controller
            .publish_checkpoint_assignment_fence(Some(replacement_round.assignment_fence.clone()));
        replacement_controller
            .announce_recover_prepare(&replacement_round)
            .await
            .unwrap();
        replacement_controller
            .announce_recover_start(&replacement_round, 5)
            .await
            .unwrap();
        replacement_controller
            .announce_recover_release(&replacement_round, 5)
            .await
            .unwrap();
        let replacement_release = RecoveryAnnouncement {
            round: replacement_round,
            phase: RecoverPhase::Release { epoch: 5 },
        };
        assert_eq!(
            replacement_controller.observe_recover().await.unwrap(),
            Some(replacement_release.clone())
        );

        delayed.release();
        delayed.wait_until_completed().await;
        let stale_error = stale_release.await.unwrap().unwrap_err();
        assert!(
            stale_error.contains("local process lease owner or term changed"),
            "{stale_error}"
        );
        assert!(inner.get(&stale_path).await.is_ok());
        assert_eq!(
            replacement_controller.observe_recover().await.unwrap(),
            Some(replacement_release)
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_delayed_lower_sequence_cannot_regress_same_term() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = Arc::new(ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        let delayed_path = object_store_control_record_path(&lease, "control:recover", 1);
        delayed.block_once(delayed_path.clone());
        let delayed_writer = {
            let kv = Arc::clone(&kv);
            tokio::spawn(async move {
                kv.write_checked("control:recover", "sequence-1".into())
                    .await
            })
        };
        delayed.wait_until_blocked().await;
        delayed_writer.abort();
        let _ = delayed_writer.await;

        kv.write_checked("control:recover", "sequence-2".into())
            .await
            .unwrap();
        delayed.release();
        delayed.wait_until_completed().await;
        assert!(inner.get(&delayed_path).await.is_ok());
        assert_eq!(
            kv.read_from(NodeId(7), "control:recover").await,
            Some("sequence-2".into())
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_revalidates_lease_after_record_body_read() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = Arc::new(ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx,
        ));
        first
            .write_checked("control:recover", "old-term".into())
            .await
            .unwrap();
        delayed.block_get_once(object_store_control_record_path(
            &first_lease,
            "control:recover",
            1,
        ));
        let reader = {
            let first = Arc::clone(&first);
            tokio::spawn(async move { first.read_from_checked(NodeId(7), "control:recover").await })
        };
        delayed.wait_until_get_blocked().await;
        let _replacement = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;
        delayed.release_get();
        let error = reader.await.unwrap().unwrap_err();
        assert!(error.contains("changed during control read"), "{error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_stale_local_term_cannot_read_or_scan() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx,
        );
        first
            .write_checked("control:recover", "old-term".into())
            .await
            .unwrap();
        let _replacement = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;

        let read_error = first
            .read_from_checked(NodeId(7), "control:recover")
            .await
            .unwrap_err();
        assert!(read_error.contains("owner or term changed"), "{read_error}");
        let scan_error = first.scan_checked("control:recover").await.unwrap_err();
        assert!(scan_error.contains("owner or term changed"), "{scan_error}");
    }

    #[tokio::test]
    async fn object_store_control_scan_bounds_concurrency_and_preserves_order() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(inner));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_empty_tx, empty_rx) = watch::channel(Vec::new());
        let node_count = OBJECT_STORE_CONTROL_SCAN_CONCURRENCY + 5;
        let mut leases = Vec::with_capacity(node_count);

        for raw_id in 1..=node_count {
            let id = NodeId(u64::try_from(raw_id).unwrap());
            let lease = acquire_test_process_lease(
                Arc::clone(&store),
                id,
                uuid::Uuid::from_u128(raw_id as u128),
                60_000,
            )
            .await;
            let writer = ObjectStoreClusterKv::new(
                lease.clone(),
                live_test_process_deadline(),
                60_000,
                Arc::clone(&store),
                empty_rx.clone(),
            );
            writer
                .write_checked("control:test-scan", format!("node-{raw_id}"))
                .await
                .unwrap();
            leases.push(lease);
        }

        let members = (1..=node_count)
            .map(|raw_id| NodeInfo {
                id: NodeId(u64::try_from(raw_id).unwrap()),
                name: format!("node-{raw_id}"),
                rpc_address: String::new(),
                raft_address: String::new(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            })
            .collect();
        let (_members_tx, members_rx) = watch::channel(members);
        let scanner = ObjectStoreClusterKv::new(
            leases[0].clone(),
            live_test_process_deadline(),
            60_000,
            store,
            members_rx,
        );

        delayed.begin_get_concurrency_probe();
        let results = scanner.scan_checked("control:test-scan").await.unwrap();
        let max_gets = delayed.finish_get_concurrency_probe();

        assert_eq!(results.len(), node_count);
        assert_eq!(
            results.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
            (1..=u64::try_from(node_count).unwrap()).collect::<Vec<_>>()
        );
        assert!(max_gets > 1, "the probe must observe concurrent reads");
        assert!(
            max_gets <= OBJECT_STORE_CONTROL_SCAN_CONCURRENCY,
            "observed {max_gets} concurrent GETs"
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_rejects_noncanonical_record_body() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let record = ObjectStoreControlRecord {
            version: OBJECT_STORE_CONTROL_VERSION,
            node: lease.node.0,
            owner: lease.owner,
            term: lease.term,
            sequence: 1,
            key: "control:recover".into(),
            value: "release".into(),
        };
        store
            .put(
                &object_store_control_record_path(&lease, &record.key, record.sequence),
                object_store::PutPayload::from(bytes::Bytes::from(
                    serde_json::to_vec_pretty(&record).unwrap(),
                )),
            )
            .await
            .unwrap();
        let kv = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        let error = kv
            .read_from_checked(NodeId(7), "control:recover")
            .await
            .unwrap_err();
        assert!(error.contains("canonically encoded"), "{error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_prunes_history_but_retains_highest_two() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx.clone(),
        );
        for sequence in 1..=5 {
            kv.write_checked("control:recover", format!("value-{sequence}"))
                .await
                .unwrap();
        }
        let prefix = object_store_control_key_prefix(&lease, "control:recover");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if list_control_sequences(&store, &prefix).await.unwrap() == [4, 5] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            kv.read_from(NodeId(7), "control:recover").await,
            Some("value-5".into())
        );
        let reconstructed = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx,
        );
        reconstructed
            .write_checked("control:recover", "value-6".into())
            .await
            .unwrap();
        assert_eq!(
            reconstructed.read_from(NodeId(7), "control:recover").await,
            Some("value-6".into())
        );
    }

    #[test]
    fn object_store_control_kv_prune_selection_is_order_independent() {
        let prefix = "cluster-control-kv/v2/test/";
        let mut oldest = BinaryHeap::new();
        for index in 0..300u64 {
            let sequence = (index * 73) % 300 + 1;
            let path = object_store::path::Path::from(format!("{prefix}v{sequence:020}.json"));
            retain_oldest_control_record(&mut oldest, sequence, &path);
        }
        assert_eq!(
            oldest
                .into_sorted_vec()
                .into_iter()
                .map(|(sequence, _)| sequence)
                .collect::<Vec<_>>(),
            (1..=u64::try_from(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS).unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_pruning_coalesces_per_prefix() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        let key_prefix = object_store_control_key_prefix(&lease, "control:recover");
        let generation_prefix = RECOVERY_GENERATION_PREFIX.to_string();

        kv.schedule_prune(key_prefix.clone());
        kv.schedule_prune(key_prefix.clone());
        kv.schedule_prune(generation_prefix.clone());
        {
            let states = kv
                .prune_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(states.len(), 2);
            assert_eq!(states.get(&key_prefix), Some(&true));
            assert_eq!(states.get(&generation_prefix), Some(&false));
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if kv
                    .prune_states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn recovery_generation_ignores_delayed_lower_marker_and_survives_new_term() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(inner));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = Arc::new(ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        delayed.block_once(recovery_generation_path(1));
        let delayed_writer = {
            let first = Arc::clone(&first);
            tokio::spawn(async move {
                first
                    .write_checked(RECOVERY_GENERATION_KEY, "1".into())
                    .await
            })
        };
        delayed.wait_until_blocked().await;
        delayed_writer.abort();
        let _ = delayed_writer.await;
        first
            .write_checked(RECOVERY_GENERATION_KEY, "2".into())
            .await
            .unwrap();
        delayed.release();
        delayed.wait_until_completed().await;
        assert_eq!(
            first.read_from(NodeId(7), RECOVERY_GENERATION_KEY).await,
            Some("2".into())
        );

        let replacement_lease = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;
        let replacement = ObjectStoreClusterKv::new(
            replacement_lease,
            live_test_process_deadline(),
            ttl_ms,
            store,
            members_rx,
        );
        assert_eq!(
            replacement
                .read_from(NodeId(7), RECOVERY_GENERATION_KEY)
                .await,
            Some("2".into())
        );
        replacement
            .write_checked(RECOVERY_GENERATION_KEY, "3".into())
            .await
            .unwrap();
        assert_eq!(
            replacement
                .read_from(NodeId(7), RECOVERY_GENERATION_KEY)
                .await,
            Some("3".into())
        );
    }

    #[tokio::test]
    async fn shared_namespace_proof_retains_bounded_boot_markers_in_both_stores() {
        let checkpoint: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participants = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &participants,
            &controls,
            [Arc::clone(&checkpoint), Arc::clone(&checkpoint)],
            [Arc::clone(&state), Arc::clone(&state)],
            std::time::Duration::from_secs(2),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));

        for node_id in [1, 2] {
            for (store, role) in [
                (&checkpoint, NamespaceProofStore::Checkpoint),
                (&state, NamespaceProofStore::State),
            ] {
                let marker = store
                    .get(&namespace_proof_path(role, node_id))
                    .await
                    .expect("boot marker must remain available to rolling joiners");
                assert!(marker.meta.size <= NAMESPACE_PROOF_MAX_SENTINEL_BYTES);
            }
        }
    }

    #[tokio::test]
    async fn rolling_restart_uses_active_peers_retained_markers() {
        let checkpoint: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let initial = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &initial,
            &controls,
            [Arc::clone(&checkpoint), Arc::clone(&checkpoint)],
            [Arc::clone(&state), Arc::clone(&state)],
            std::time::Duration::from_secs(2),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        let active_peer_record = controls[1]
            .read_from(NodeId(2), NAMESPACE_PROOF_KEY)
            .await
            .unwrap();

        let restarted = [
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(111),
            },
            initial[1],
        ];
        prove_shared_object_store_namespaces(
            restarted[0],
            &restarted,
            Arc::clone(&controls[0]),
            Arc::clone(&checkpoint),
            Arc::clone(&state),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(
            controls[1].read_from(NodeId(2), NAMESPACE_PROOF_KEY).await,
            Some(active_peer_record),
            "the active peer must not rerun startup for a rolling joiner"
        );
        assert_eq!(
            namespace_marker_count(&checkpoint, NamespaceProofStore::Checkpoint).await,
            2
        );
        assert_eq!(
            namespace_marker_count(&state, NamespaceProofStore::State).await,
            2
        );
    }

    #[tokio::test]
    async fn shared_namespace_proof_rejects_split_state_namespaces() {
        let checkpoint: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state_a: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state_b: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participants = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &participants,
            &controls,
            [Arc::clone(&checkpoint), checkpoint],
            [state_a, state_b],
            std::time::Duration::from_millis(250),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_err()));
    }

    #[tokio::test]
    async fn shared_namespace_proof_rejects_split_checkpoint_namespaces() {
        let checkpoint_a: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let checkpoint_b: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participants = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &participants,
            &controls,
            [checkpoint_a, checkpoint_b],
            [Arc::clone(&state), state],
            std::time::Duration::from_millis(250),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_err()));
    }

    #[tokio::test]
    async fn failed_process_lease_candidate_cannot_overwrite_active_incarnation() {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, LeaseDeadline, ProcessLeaseAuthority,
            ProcessLeaseOutcome,
        };

        let node = NodeId(7);
        let recovery_impl = Arc::new(InMemoryKv::new(node));
        let recovery: Arc<dyn ClusterKv> = recovery_impl.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let active_owner = uuid::Uuid::from_u128(1);
        let active = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&recovery),
            Arc::clone(&recovery),
            None,
            members_rx.clone(),
            active_owner,
        );
        let process_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(process_store, std::time::Duration::from_millis(1_000))
                .unwrap(),
        );
        let lease_store = process_authority.store_for(node);
        let ProcessLeaseOutcome::Acquired(active_lease) =
            lease_store.try_acquire(active_owner, 0).await.unwrap()
        else {
            panic!("first process must acquire its stable identity");
        };
        active
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        active
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();
        active
            .publish_leased_recovery_incarnation(&active_lease)
            .await
            .unwrap();

        let candidate_owner = uuid::Uuid::from_u128(2);
        let candidate = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&recovery),
            Arc::clone(&recovery),
            None,
            members_rx,
            candidate_owner,
        );
        let ProcessLeaseOutcome::Held(incumbent) = lease_store
            .try_acquire(candidate_owner, 10_000)
            .await
            .unwrap()
        else {
            panic!("client wall time must not let a candidate steal the lease");
        };
        candidate
            .set_process_lease_authority(process_authority)
            .unwrap();
        candidate
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();
        assert!(candidate
            .publish_leased_recovery_incarnation(&incumbent)
            .await
            .is_err());
        assert_eq!(
            recovery_impl
                .read_from(node, "control:recovery-incarnation")
                .await,
            Some(active_owner.to_string())
        );
    }

    #[tokio::test]
    async fn startup_uses_retained_committed_assignment_when_head_is_draining() {
        use std::collections::BTreeMap;

        use laminar_core::checkpoint::{CheckpointParticipant, LeaderProof, LeaderProofOwner};
        use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};

        let object_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let store = AssignmentSnapshotStore::new(object_store);
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant])
            .unwrap();
        store.save_if_absent(&committed).await.unwrap();
        let draining = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                }],
                LeaderProof {
                    owner: LeaderProofOwner {
                        node_id: participant.node_id,
                        boot_id: participant.boot_incarnation,
                        process_term: 1,
                    },
                    fencing_token: 1,
                },
            )
            .unwrap();
        store
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();

        let selected = laminar_db::rebalance::startup_committed_assignment(&store, None, draining)
            .await
            .unwrap();
        assert_eq!(selected, committed);
        assert!(!selected.draining);
    }

    #[tokio::test]
    async fn assignment_seed_rejects_peer_tag_that_is_not_durable_lease_owner() {
        use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let self_id = NodeId(1);
        let peer_id = NodeId(2);
        let self_boot = uuid::Uuid::from_u128(11);
        let durable_peer_boot = uuid::Uuid::from_u128(22);
        for (node, boot) in [(self_id, self_boot), (peer_id, durable_peer_boot)] {
            let lease = ProcessLeaseStore::new(Arc::clone(&store), node, 1_000);
            assert!(matches!(
                lease.try_acquire(boot, 0).await.unwrap(),
                ProcessLeaseOutcome::Acquired(_)
            ));
        }

        let mut peer = NodeInfo {
            id: peer_id,
            name: "peer".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Joining,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        peer.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            durable_peer_boot.to_string(),
        );
        let participants =
            assignment_seed_participants(self_id, self_boot, &[peer.clone()], &store, 1_000)
                .await
                .unwrap();
        assert_eq!(
            participants,
            vec![
                laminar_core::checkpoint::CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: self_boot,
                },
                laminar_core::checkpoint::CheckpointParticipant {
                    node_id: peer_id.0,
                    boot_incarnation: durable_peer_boot,
                },
            ]
        );

        peer.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(222).to_string(),
        );
        let error = assignment_seed_participants(self_id, self_boot, &[peer], &store, 1_000)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("durable lease belongs"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn initial_assignment_roster_excludes_zero_vnode_workers() {
        use laminar_core::checkpoint::CheckpointParticipant;

        let nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let peers: Vec<NodeInfo> = nodes[1..]
            .iter()
            .map(|node| NodeInfo {
                id: *node,
                name: format!("node-{}", node.0),
                rpc_address: String::new(),
                raft_address: String::new(),
                state: NodeState::Joining,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            })
            .collect();
        let participants: Vec<_> = nodes
            .iter()
            .map(|node| CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(node.0)),
            })
            .collect();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let deadline = laminar_core::cluster::control::LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        );

        let (registry, snapshot_store) =
            resolve_vnode_assignment(nodes[0], &peers, 1, store, &participants, &deadline)
                .await
                .unwrap();
        let snapshot = snapshot_store.load().await.unwrap().unwrap();
        let owner = registry.snapshot()[0];

        assert_eq!(snapshot.participants.len(), 1);
        assert_eq!(snapshot.participants[0].node_id, owner.0);
        assert_eq!(
            nodes.iter().filter(|node| **node != owner).count(),
            2,
            "one vnode across three live workers must leave two workers idle"
        );
    }

    #[tokio::test]
    async fn fenced_process_cannot_create_the_initial_assignment() {
        use laminar_core::checkpoint::CheckpointParticipant;
        use laminar_core::cluster::control::{AssignmentSnapshotStore, LeaseDeadline};

        let node = NodeId(7);
        let participant = CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: uuid::Uuid::from_u128(77),
        };
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store));
        let deadline = LeaseDeadline::fenced();

        let result = resolve_vnode_assignment(node, &[], 1, store, &[participant], &deadline).await;
        let error = match result {
            Ok(_) => panic!("a fenced process created an assignment"),
            Err(error) => error,
        };

        assert!(matches!(error, ClusterStartupError::AuthorityLost(_)));
        assert!(snapshots.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn startup_waits_for_exact_local_assignment_certificate() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::cluster::control::{
            CheckpointParticipant, ClusterController, ClusterKv, InMemoryKv,
        };
        use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

        let node = NodeId(7);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node.0)));

        let publish = Arc::clone(&controller);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            publish.publish_checkpoint_assignment_fence(Some(
                CheckpointAssignmentFence::from_owner_map(
                    1,
                    &[node.0],
                    vec![CheckpointParticipant {
                        node_id: node.0,
                        boot_incarnation: publish.recovery_incarnation(),
                    }],
                )
                .unwrap(),
            ));
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            wait_for_startup_assignment_fence(&controller, &registry),
        )
        .await
        .expect("startup wait did not observe the assignment certificate")
        .unwrap();
    }

    #[tokio::test]
    async fn startup_rechecks_assignment_certificate_when_membership_becomes_active() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::cluster::control::{
            CheckpointParticipant, ClusterController, ClusterKv, InMemoryKv,
        };
        use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

        let node = NodeId(7);
        let peer = NodeId(8);
        let peer_boot = uuid::Uuid::from_u128(88);
        let joining_peer = NodeInfo {
            id: peer,
            name: "peer".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Joining,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (members_tx, members_rx) = watch::channel(vec![joining_peer.clone()]);
        let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
        controller.set_active(true);
        let registry = VnodeRegistry::new_unassigned(2);
        registry
            .set_assignment_and_version(vec![StateNodeId(node.0), StateNodeId(peer.0)].into(), 1);
        controller.publish_checkpoint_assignment_fence(Some(
            CheckpointAssignmentFence::from_owner_map(
                1,
                &[node.0, peer.0],
                vec![
                    CheckpointParticipant {
                        node_id: node.0,
                        boot_incarnation: controller.recovery_incarnation(),
                    },
                    CheckpointParticipant {
                        node_id: peer.0,
                        boot_incarnation: peer_boot,
                    },
                ],
            )
            .unwrap(),
        ));

        let wait = wait_for_startup_assignment_fence(&controller, &registry);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut wait)
                .await
                .is_err(),
            "a Joining assignment participant must keep startup fenced"
        );

        let mut active_peer = joining_peer;
        active_peer.state = NodeState::Active;
        members_tx.send_replace(vec![active_peer]);

        tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("membership-only activation did not wake assignment certification")
            .unwrap();
    }

    #[test]
    fn startup_leader_timeout_covers_manager_and_remote_audit_phases() {
        let timeout = startup_leader_authority_timeout(
            laminar_core::cluster::control::LeaderLeaseConfig {
                ttl: std::time::Duration::from_secs(5),
                renew_interval: std::time::Duration::from_secs(2),
            },
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(timeout, std::time::Duration::from_millis(42_375));
    }

    #[tokio::test]
    async fn startup_leader_authority_requires_a_live_full_owner_and_keeps_intake_fenced() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaderLeaseStore,
            LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome,
        };
        use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

        let node = NodeId(7);
        let stale_boot = uuid::Uuid::from_u128(71);
        let certified_boot = uuid::Uuid::from_u128(72);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&kv),
            kv,
            None,
            members_rx,
            certified_boot,
        );
        controller.publish_checkpoint_assignment_fence(Some(
            CheckpointAssignmentFence::from_owner_map(
                1,
                &[node.0],
                vec![CheckpointParticipant {
                    node_id: node.0,
                    boot_incarnation: certified_boot,
                }],
            )
            .unwrap(),
        ));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(30),
            )))
            .unwrap();
        let registry = VnodeRegistry::single_owner(1, StateNodeId(node.0));

        let delayed = Arc::new(DelayedControlPutStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));
        let backing: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&backing), std::time::Duration::from_millis(1))
                .unwrap(),
        );
        let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
            .store_for(node)
            .try_acquire(certified_boot, 0)
            .await
            .unwrap()
        else {
            panic!("empty process authority must be acquired");
        };
        controller
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
        let stale_owner = LeaderLeaseOwner {
            node,
            boot: stale_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(stale_lease) =
            authority.begin_new_term(&stale_owner, 0).await.unwrap()
        else {
            panic!("empty test authority must be acquired");
        };
        let certified_owner = LeaderLeaseOwner {
            node,
            boot: certified_boot,
            process_term: process_lease.term,
        };
        let (leader_tx, leader_rx) = watch::channel(None);
        controller
            .set_leader_lease_watch(
                leader_rx,
                certified_owner.clone(),
                Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
            )
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));
        let observation = authority
            .observe_rival(&certified_owner, &stale_lease)
            .unwrap();
        let error = wait_for_startup_leader_authority(
            &controller,
            &registry,
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains(&certified_boot.to_string()),
            "{error}"
        );

        let LeaseOutcome::Acquired(takeover) = authority
            .try_takeover(&certified_owner, &observation, 10)
            .await
            .unwrap()
        else {
            panic!("certified process must take over stale leader authority");
        };

        let no_grant = wait_for_startup_leader_authority(
            &controller,
            &registry,
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(
            no_grant.to_string().contains("live certified grant"),
            "{no_grant}"
        );

        leader_tx.send_replace(Some(takeover));

        wait_for_startup_leader_authority(
            &controller,
            &registry,
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn remote_startup_requires_a_live_exact_process_proof_and_resets_with_the_candidate() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::{
            CatalogManifest, CatalogManifestStore, ClusterController, LeaderLeaseOwner,
            LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority,
            ProcessLeaseOutcome,
        };
        use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

        fn active_node(node: NodeId) -> NodeInfo {
            NodeInfo {
                id: node,
                name: format!("node-{}", node.0),
                rpc_address: String::new(),
                raft_address: String::new(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            }
        }

        let leader_node = NodeId(1);
        let observer_node = NodeId(2);
        let replacement_node = NodeId(3);
        let leader_boot = uuid::Uuid::from_u128(11);
        let observer_boot = uuid::Uuid::from_u128(22);
        let replacement_boot = uuid::Uuid::from_u128(33);
        let controls = namespace_proof_test_kvs();
        let (observer_members_tx, observer_members_rx) =
            watch::channel(vec![active_node(leader_node)]);
        let (_leader_members_tx, leader_members_rx) =
            watch::channel(vec![active_node(observer_node)]);
        let observer = Arc::new(ClusterController::new_with_recovery_incarnation(
            observer_node,
            Arc::clone(&controls[1]),
            Arc::clone(&controls[1]),
            None,
            observer_members_rx,
            observer_boot,
        ));
        let leader = Arc::new(ClusterController::new_with_recovery_incarnation(
            leader_node,
            Arc::clone(&controls[0]),
            Arc::clone(&controls[0]),
            None,
            leader_members_rx,
            leader_boot,
        ));
        for controller in [&observer, &leader] {
            controller.install_local_leader_proof_provider();
            controller
                .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            controller
                .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                    std::time::Duration::from_secs(30),
                )))
                .unwrap();
            controller.set_active(true);
        }

        let registry = VnodeRegistry::single_owner(1, StateNodeId(leader_node.0));
        let leader_participant = CheckpointParticipant {
            node_id: leader_node.0,
            boot_incarnation: leader_boot,
        };
        let leader_fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[leader_node.0],
            vec![leader_participant],
        )
        .unwrap();
        observer.publish_checkpoint_assignment_fence(Some(leader_fence.clone()));
        leader.publish_checkpoint_assignment_fence(Some(leader_fence));

        let delayed = Arc::new(DelayedControlPutStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));
        let backing: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&backing), std::time::Duration::from_secs(1))
                .unwrap(),
        );
        let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
            .store_for(leader_node)
            .try_acquire(leader_boot, 0)
            .await
            .unwrap()
        else {
            panic!("remote process lease must be acquired");
        };
        observer
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1_000));
        let owner = LeaderLeaseOwner {
            node: leader_node,
            boot: leader_boot,
            process_term: process_lease.term,
        };
        let LeaseOutcome::Acquired(initial_lease) =
            authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("remote leader lease must be acquired");
        };
        observer.set_leader_lease_store(Arc::clone(&authority));
        leader.set_leader_lease_store(Arc::clone(&authority));
        let (leader_grant_tx, leader_grant_rx) = watch::channel(None);
        leader
            .set_leader_lease_watch(
                leader_grant_rx,
                owner,
                Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
            )
            .unwrap();
        let no_grant = wait_for_startup_leader_authority(
            &observer,
            &registry,
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(no_grant.to_string().contains("live certified grant"));

        delayed.block_get_once(object_store::path::Path::from(
            "control/leader-lease/v0000000000000001.json",
        ));
        let unrelated_wait = wait_for_startup_leader_authority(
            &observer,
            &registry,
            std::time::Duration::from_millis(250),
        );
        tokio::pin!(unrelated_wait);
        tokio::select! {
            biased;
            result = &mut unrelated_wait => panic!("startup audit completed before its first authority read was blocked: {result:?}"),
            () = delayed.wait_until_get_blocked() => {}
        }
        CatalogManifestStore::new(Arc::clone(&authority))
            .seal(&CatalogManifest::default(), &initial_lease.proof())
            .await
            .unwrap();
        delayed.release_get();
        let after_catalog = authority.load().await.unwrap().unwrap();
        assert!(after_catalog.seq > initial_lease.seq);
        assert_eq!(after_catalog.expires_at_ms, initial_lease.expires_at_ms);
        let unrelated_mutation = unrelated_wait.await.unwrap_err();
        assert!(
            unrelated_mutation
                .to_string()
                .contains("live certified grant"),
            "{unrelated_mutation}"
        );

        leader_grant_tx.send_replace(Some(after_catalog));
        wait_for_startup_leader_authority(&observer, &registry, std::time::Duration::from_secs(1))
            .await
            .unwrap();

        leader_grant_tx.send_replace(None);
        let dead_process = wait_for_startup_leader_authority(
            &observer,
            &registry,
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(dead_process.to_string().contains("live certified grant"));
        leader_grant_tx.send_replace(authority.load().await.unwrap());

        let replacement = CheckpointParticipant {
            node_id: replacement_node.0,
            boot_incarnation: replacement_boot,
        };
        registry.set_assignment_and_version(vec![StateNodeId(replacement_node.0)].into(), 2);
        observer.publish_checkpoint_assignment_fence(Some(
            CheckpointAssignmentFence::from_owner_map(2, &[replacement_node.0], vec![replacement])
                .unwrap(),
        ));
        observer_members_tx
            .send(vec![
                active_node(leader_node),
                active_node(replacement_node),
            ])
            .unwrap();
        let reset = wait_for_startup_leader_authority(
            &observer,
            &registry,
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(reset.to_string().contains(&replacement_boot.to_string()));
    }
}
