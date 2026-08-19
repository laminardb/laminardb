//! Cluster (multi-node) mode startup orchestrator.

mod assignment;
mod control_kv;
mod discovery;
mod leases;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::watch;
use tracing::{info, warn};

use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::cluster::discovery::{
    GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo, NodeMetadata, NodeState,
    StaticDiscovery, StaticDiscoveryConfig,
};

use assignment::{
    assignment_seed_participants, audit_stable_startup_assignment, resolve_vnode_assignment,
    startup_leader_authority_timeout, wait_for_catalog_startup_authority,
    wait_for_startup_assignment_fence, wait_for_startup_leader_authority, CatalogStartupAuthority,
};
use control_kv::{ObjectStoreClusterKv, StaticClusterKv, OBJECT_STORE_CONTROL_IO_TIMEOUT};
use discovery::{
    announce_left_after_fence_with_bound, announce_node_state_with_bound, spawn_membership_watcher,
    stop_discovery_with_bound, DiscoveryImpl,
};
use leases::{
    acquire_process_lease, revoke_process_authority, LeaderLeaseRuntime, ProcessLeaseRuntime,
    PROCESS_LEASE_IO_TIMEOUT,
};

const PROCESS_INCARNATION_TAG: &str = "laminardb.process-incarnation";
const CLUSTER_TASK_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const STARTUP_RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

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

fn fence_post_active_startup_failure(
    db: &LaminarDB,
    controller: &laminar_core::cluster::control::ClusterController,
    serving_gate: &crate::http::ServingGate,
    leader_lease_shutdown: &tokio_util::sync::CancellationToken,
    process_lease: &ProcessLeaseRuntime,
    rebalance_shutdown: Option<&tokio_util::sync::CancellationToken>,
    api_handle: &tokio::task::JoinHandle<()>,
) {
    leader_lease_shutdown.cancel();
    if let Some(shutdown) = rebalance_shutdown {
        shutdown.cancel();
    }
    controller.set_active(false);
    db.fence_cluster_startup();
    serving_gate.fence();
    db.revoke_cluster_authority();
    process_lease.fence_authority();
    api_handle.abort();
}

async fn cleanup_cluster_startup(
    discovery: &mut DiscoveryImpl,
    db: &LaminarDB,
    leader_lease: &mut LeaderLeaseRuntime,
    terminal: &tokio_util::sync::CancellationToken,
    force_terminal: bool,
) {
    leader_lease.cancel();
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
    LeaderLeaseExited,
    HttpApiExited,
    RebalanceTaskExited,
}

async fn wait_for_cluster_shutdown_trigger(
    terminal: &tokio_util::sync::CancellationToken,
    leader_lease: &LeaderLeaseRuntime,
    api_handle: &tokio::task::JoinHandle<()>,
    rebalance_tasks: &[tokio::task::JoinHandle<()>],
) -> Result<ClusterShutdownTrigger, ClusterStartupError> {
    tokio::select! {
        biased;
        () = terminal.cancelled() => Ok(ClusterShutdownTrigger::ProcessLeaseLost),
        () = leader_lease.wait_for_exit() => {
            Ok(ClusterShutdownTrigger::LeaderLeaseExited)
        }
        () = wait_for_cluster_task_exit(api_handle) => {
            Ok(ClusterShutdownTrigger::HttpApiExited)
        }
        () = wait_for_rebalance_task_exit(rebalance_tasks), if !rebalance_tasks.is_empty() => {
            Ok(ClusterShutdownTrigger::RebalanceTaskExited)
        }
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

async fn stop_bootstrap_rebalance_tasks(
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    shutdown: &tokio_util::sync::CancellationToken,
    operation_timeout: std::time::Duration,
) -> bool {
    shutdown.cancel();
    let graceful_timeout = operation_timeout
        .checked_add(CLUSTER_TASK_SHUTDOWN_TIMEOUT)
        .unwrap_or(operation_timeout);
    let graceful_deadline = tokio::time::Instant::now() + graceful_timeout;
    while !tasks.is_empty() {
        match tokio::time::timeout_at(graceful_deadline, &mut tasks[0]).await {
            Ok(Ok(())) => {
                tasks.swap_remove(0);
            }
            Ok(Err(error)) => {
                warn!(%error, "Bootstrap rebalance task failed before quiescence");
                tasks.swap_remove(0);
                for task in tasks.iter() {
                    task.abort();
                }
                while let Some(task) = tasks.pop() {
                    let _ = log_rebalance_task_result(task.await);
                }
                return false;
            }
            Err(_) => {
                warn!(
                    remaining = tasks.len(),
                    timeout = ?graceful_timeout,
                    "Bootstrap rebalance tasks did not quiesce before pipeline initialization"
                );
                for task in tasks.iter() {
                    task.abort();
                }
                while let Some(task) = tasks.pop() {
                    let _ = log_rebalance_task_result(task.await);
                }
                return false;
            }
        }
    }
    true
}

async fn wait_for_rebalance_task_exit(tasks: &[tokio::task::JoinHandle<()>]) {
    loop {
        if tasks.iter().any(tokio::task::JoinHandle::is_finished) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn wait_for_cluster_task_exit(task: &tokio::task::JoinHandle<()>) {
    while !task.is_finished() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
        let shutdown_trigger = wait_for_cluster_shutdown_trigger(
            &terminal,
            &self.leader_lease,
            &self.api_handle,
            &self.rebalance_tasks,
        )
        .await?;
        let runtime_failure = match shutdown_trigger {
            ClusterShutdownTrigger::LeaderLeaseExited => {
                Some("leader lease manager exited unexpectedly")
            }
            ClusterShutdownTrigger::HttpApiExited => Some("HTTP API server exited unexpectedly"),
            ClusterShutdownTrigger::RebalanceTaskExited => {
                Some("rebalance control task exited unexpectedly")
            }
            ClusterShutdownTrigger::Signal | ClusterShutdownTrigger::ProcessLeaseLost => None,
        };
        let mut authority_lost = shutdown_trigger != ClusterShutdownTrigger::Signal;
        self.serving_gate.fence();

        if let Some(error) = runtime_failure {
            self.rebalance_shutdown.cancel();
            self.cluster_controller.set_active(false);
            self.db.fence_cluster_startup();
            self.db.revoke_cluster_authority();
            self.leader_lease.cancel();
            self.process_lease.fence_authority();
            self.api_handle.abort();
            warn!(
                reason = error,
                "Cluster runtime task stopped; fencing and shutting down node"
            );
        } else if authority_lost {
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

        if let Some(runtime_failure) = runtime_failure {
            if let Err(error) = &shutdown_result {
                warn!(%error, "Database shutdown after runtime task failure failed");
            }
            if !runtime_tasks_clean {
                warn!("Cluster runtime cleanup was incomplete after a runtime task failure");
            }
            Err(ClusterStartupError::EngineShutdown(runtime_failure.into()))
        } else if authority_lost {
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
        self.leader_lease.cancel();
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
    // Build the vnode registry. Exact same-formation genesis peers may preinstall v1; every
    // existing or racing different formation boots unassigned for the audited bootstrap path.
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
    if let Err(error) = cluster_controller.set_leader_lease_runtime_watches(
        manager.lease_watch(),
        manager.owner().clone(),
        manager.deadline_watch(),
    ) {
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(error));
    }
    let token = tokio_util::sync::CancellationToken::new();
    let candidacy = cluster_controller.leader_candidacy_watch();
    let task = manager.spawn_supervised(token.clone(), candidacy, process_lease_terminal.clone());
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

    // Fence startup and audit this process generation's checkpoint artifacts before assignment
    // certification. The recovery monitor is installed only after the initial pipeline start.
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
    let rebalance_config = laminar_db::rebalance::RebalanceConfig {
        placement_isolation_tier: cluster_cfg.discovery.placement_isolation_tier,
        ..laminar_db::rebalance::RebalanceConfig::default()
    };

    // Existing clusters deliberately boot replacement processes unassigned. Certify and install
    // the current-process assignment while the DB and HTTP serving gate are still closed, then
    // perform normal checkpoint/source recovery against that exact fence before runtime launch.

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

    // The shuffle receiver and gated HTTP listener are live. Only now publish assignment
    // eligibility so an occupied API port can never leave an Active ghost node. The pipeline
    // remains Created and source intake remains fenced until assignment certification and restore.
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
        // A timeout or transport error may follow a remotely committed Active announcement.
        // Fence every local authority synchronously before any rollback I/O.
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            None,
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after readiness announcement failure",
        )
        .await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "announce cluster runtime readiness: {error}"
        )));
    }
    startup_controller.set_active(true);

    // Bootstrap assignment certification against the current process incarnation before building
    // the checkpoint coordinator. These tasks are stopped and joined after certification so no
    // watcher can advance the registry while startup snapshots its assignment.
    let bootstrap_rebalance_shutdown = tokio_util::sync::CancellationToken::new();
    let mut bootstrap_rebalance_tasks = vec![
        laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(&db),
            Arc::clone(&snapshot_store),
            Arc::clone(&vnode_registry),
            bootstrap_rebalance_shutdown.clone(),
            rebalance_config,
            Some(Arc::clone(startup_controller)),
        ),
        laminar_db::rebalance::spawn_rebalance_controller(
            Arc::clone(&db),
            Arc::clone(startup_controller),
            Arc::clone(&snapshot_store),
            Arc::clone(&vnode_registry),
            bootstrap_rebalance_shutdown.clone(),
            rebalance_config,
        ),
    ];
    info!("Bootstrap assignment control plane started");

    let assignment_gate = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while certifying the startup assignment"
                    .into(),
            ))
        }
        () = wait_for_cluster_task_exit(&api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while certifying the startup assignment".into(),
            ))
        }
        result = wait_for_startup_assignment_fence(
            startup_controller,
            &vnode_registry,
            &bootstrap_rebalance_tasks,
        ) => result,
    };
    if let Err(error) = assignment_gate {
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            Some(&bootstrap_rebalance_shutdown),
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after startup assignment failure",
        )
        .await;
        let _ = stop_bootstrap_rebalance_tasks(
            &mut bootstrap_rebalance_tasks,
            &bootstrap_rebalance_shutdown,
            rebalance_config.checkpoint_timeout,
        )
        .await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(error);
    }
    let bootstrap_tasks_stop = stop_bootstrap_rebalance_tasks(
        &mut bootstrap_rebalance_tasks,
        &bootstrap_rebalance_shutdown,
        rebalance_config.checkpoint_timeout,
    );
    tokio::pin!(bootstrap_tasks_stop);
    let bootstrap_stop_result = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            fence_post_active_startup_failure(
                &db,
                startup_controller,
                &serving_gate,
                &leader_lease.shutdown,
                &process_lease,
                Some(&bootstrap_rebalance_shutdown),
                &api_handle,
            );
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while stopping bootstrap assignment tasks"
                    .into(),
            ))
        }
        () = wait_for_cluster_task_exit(&api_handle) => {
            fence_post_active_startup_failure(
                &db,
                startup_controller,
                &serving_gate,
                &leader_lease.shutdown,
                &process_lease,
                Some(&bootstrap_rebalance_shutdown),
                &api_handle,
            );
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while stopping bootstrap assignment tasks".into(),
            ))
        }
        stopped = &mut bootstrap_tasks_stop => Ok(stopped),
    };
    let bootstrap_tasks_stopped = match bootstrap_stop_result {
        Ok(stopped) => stopped,
        Err(error) => {
            let _ = announce_left_after_fence_with_bound(
                &discovery,
                &active,
                "withdraw node after bootstrap assignment task interruption",
            )
            .await;
            let _ = bootstrap_tasks_stop.await;
            let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                true,
            )
            .await;
            return Err(error);
        }
    };
    if !bootstrap_tasks_stopped {
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            Some(&bootstrap_rebalance_shutdown),
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after bootstrap assignment task shutdown failure",
        )
        .await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(
            "bootstrap assignment tasks did not stop before pipeline initialization".into(),
        ));
    }
    let stable_assignment = if !process_lease.is_live() {
        Err(ClusterStartupError::AuthorityLost(
            "stable node identity lease was lost before pipeline initialization".into(),
        ))
    } else {
        tokio::select! {
            biased;
            () = process_lease_terminal.cancelled() => {
                Err(ClusterStartupError::AuthorityLost(
                    "stable node identity lease was lost while auditing the startup assignment"
                        .into(),
                ))
            }
            () = wait_for_cluster_task_exit(&api_handle) => {
                Err(ClusterStartupError::EngineConstruction(
                    "HTTP API server exited while auditing the startup assignment".into(),
                ))
            }
            result = audit_stable_startup_assignment(
                startup_controller,
                &snapshot_store,
                &vnode_registry,
                rebalance_config.checkpoint_timeout,
            ) => result,
        }
    };
    if let Err(error) = stable_assignment {
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            None,
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after bootstrap assignment changed during shutdown",
        )
        .await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(error);
    }
    info!(
        assignment_version = vnode_registry.assignment_version(),
        "Bootstrap assignment certified; starting checkpoint recovery"
    );

    let pipeline_start = tokio::select! {
        biased;
        () = process_lease_terminal.cancelled() => {
            Err(ClusterStartupError::AuthorityLost(
                "stable node identity lease was lost while starting the pipeline".into(),
            ))
        }
        () = wait_for_cluster_task_exit(&api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while starting the pipeline".into(),
            ))
        }
        result = db.start() => result.map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("pipeline start: {error}"))
        }),
    };
    if let Err(error) = pipeline_start {
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            None,
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after pipeline startup failure",
        )
        .await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(error);
    }
    info!("Pipeline started from the certified assignment");

    if let Err(error) = db.enable_coordinated_recovery() {
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            None,
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after recovery monitor initialization failure",
        )
        .await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "cluster recovery monitor initialization: {error}"
        )));
    }

    // Runtime launch is complete. Start the fresh long-lived watcher/rebalancer immediately: a
    // second process failure during startup recovery must still be able to authorize and follow a
    // successor assignment. Recovery fencing keeps source/checkpoint work closed meanwhile.
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
        () = wait_for_rebalance_task_exit(&rebalance_tasks) => {
            Err(ClusterStartupError::EngineConstruction(
                "rebalance control task exited while certifying startup authority".into(),
            ))
        }
        () = wait_for_cluster_task_exit(&api_handle) => {
            Err(ClusterStartupError::EngineConstruction(
                "HTTP API server exited while certifying startup authority".into(),
            ))
        }
        result = async {
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
            fence_post_active_startup_failure(
                &db,
                startup_controller,
                &serving_gate,
                &leader_lease.shutdown,
                &process_lease,
                Some(&rebalance_shutdown),
                &api_handle,
            );
            let _ = announce_left_after_fence_with_bound(
                &discovery,
                &active,
                "withdraw node after startup authority failure",
            )
            .await;
            let _ = stop_rebalance_tasks(&mut rebalance_tasks, &rebalance_shutdown).await;
            let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
            cleanup_cluster_startup(
                &mut discovery,
                &db,
                &mut leader_lease,
                &process_lease_terminal,
                true,
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
                () = wait_for_rebalance_task_exit(&rebalance_tasks) => {
                    Err(ClusterStartupError::EngineConstruction(
                        "rebalance control task exited during coordinated startup recovery".into(),
                    ))
                }
                () = wait_for_cluster_task_exit(&api_handle) => {
                    Err(ClusterStartupError::EngineConstruction(
                        "HTTP API server exited during coordinated startup recovery".into(),
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
                fence_post_active_startup_failure(
                    &db,
                    startup_controller,
                    &serving_gate,
                    &leader_lease.shutdown,
                    &process_lease,
                    Some(&rebalance_shutdown),
                    &api_handle,
                );
                let _ = announce_left_after_fence_with_bound(
                    &discovery,
                    &active,
                    "withdraw node after startup recovery failure",
                )
                .await;
                let _ = stop_rebalance_tasks(&mut rebalance_tasks, &rebalance_shutdown).await;
                let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
                cleanup_cluster_startup(
                    &mut discovery,
                    &db,
                    &mut leader_lease,
                    &process_lease_terminal,
                    true,
                )
                .await;
                return Err(error);
            }
        }
    }

    // An idle worker serves control-plane readiness while its data plane remains fenced until the
    // watcher grants ownership.
    let api_exited_before_serving = api_handle.is_finished();
    let process_lease_lost_before_serving = !process_lease.is_live();
    if api_exited_before_serving
        || process_lease_lost_before_serving
        || !app_state.open_startup_gate()
    {
        fence_post_active_startup_failure(
            &db,
            startup_controller,
            &serving_gate,
            &leader_lease.shutdown,
            &process_lease,
            Some(&rebalance_shutdown),
            &api_handle,
        );
        let _ = announce_left_after_fence_with_bound(
            &discovery,
            &active,
            "withdraw node after serving gate failure",
        )
        .await;
        let _ = stop_rebalance_tasks(&mut rebalance_tasks, &rebalance_shutdown).await;
        let _ = abort_and_join_cluster_task(&mut api_handle, "HTTP API server").await;
        cleanup_cluster_startup(
            &mut discovery,
            &db,
            &mut leader_lease,
            &process_lease_terminal,
            true,
        )
        .await;
        let reason = if api_exited_before_serving {
            "HTTP API server exited before serving authority opened"
        } else if process_lease_lost_before_serving {
            "stable node identity lease was lost before HTTP serving authority opened"
        } else {
            "HTTP serving gate could not be opened"
        };
        return Err(ClusterStartupError::EngineConstruction(reason.into()));
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

#[cfg(test)]
mod tests;
