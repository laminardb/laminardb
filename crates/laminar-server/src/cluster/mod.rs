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
mod startup;

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use laminar_core::cluster::discovery::{NodeInfo, NodeState};
use laminar_db::LaminarDB;

use discovery::{announce_node_state_with_bound, stop_discovery_with_bound, DiscoveryImpl};
use leases::{LeaderLeaseRuntime, ProcessLeaseRuntime};

pub use startup::start_cluster;

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
        signal = crate::server::wait_for_termination_signal() => {
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
    operation_timeout: Duration,
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_cluster_task_exit(task: &tokio::task::JoinHandle<()>) {
    while !task.is_finished() {
        tokio::time::sleep(Duration::from_millis(50)).await;
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
                                Duration::from_secs(1),
                                Duration::from_secs(30),
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
            if let Err(error) = &shutdown_result {
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
