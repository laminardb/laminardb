//! Ordered cluster shutdown: trigger selection, graceful drain, and the
//! terminal-biased teardown.
//!
//! Responsibility: select the shutdown trigger with a fixed biased priority
//! (process lease loss first, operating-system signal last), fence serving on
//! entry, drain owned vnodes when authority is retained, and tear tasks and
//! the database down in the fixed order - advertisement, rebalance, leader
//! lease, database, discovery - with the process terminal token re-checked
//! between every stage. A runtime-task failure fences everything
//! synchronously before any cleanup I/O.
//!
//! INVARIANT: the biased `tokio::select!` in trigger selection and in every
//! shutdown race must not be reordered - the priority is the contract.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use laminar_core::cluster::discovery::NodeState;

use super::discovery::{announce_node_state_with_bound, stop_discovery_with_bound, DiscoveryImpl};

use super::leases::LeaderLeaseRuntime;
use super::{ClusterHandle, ClusterStartupError, CLUSTER_TASK_SHUTDOWN_TIMEOUT};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClusterShutdownTrigger {
    Signal,
    ProcessLeaseLost,
    LeaderLeaseExited,
    HttpApiExited,
    RebalanceTaskExited,
}

pub(super) async fn wait_for_cluster_shutdown_trigger(
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

pub(super) async fn abort_and_join_cluster_task(
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

pub(super) fn log_rebalance_task_result(result: Result<(), tokio::task::JoinError>) -> bool {
    if let Err(error) = result {
        if !error.is_cancelled() {
            warn!(%error, "Rebalance task failed during shutdown");
            return false;
        }
    }
    true
}

pub(super) async fn stop_rebalance_tasks(
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

pub(super) async fn stop_bootstrap_rebalance_tasks(
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

pub(super) async fn wait_for_rebalance_task_exit(tasks: &[tokio::task::JoinHandle<()>]) {
    loop {
        if tasks.iter().any(tokio::task::JoinHandle::is_finished) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn wait_for_cluster_task_exit(task: &tokio::task::JoinHandle<()>) {
    while !task.is_finished() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn stop_cluster_advertisement_and_admission(
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

        if !authority_lost {
            authority_lost = drain_local_vnodes(&mut self, &terminal).await;
        }
        authority_lost |= terminal.is_cancelled();
        self.leader_lease.cancel();

        let (shutdown_result, runtime_tasks_clean, mut authority_lost) =
            teardown_runtime_and_database(&mut self, &terminal, authority_lost).await;

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

/// Announce Draining, flip the local drain flag, and block until the leader
/// has reassigned every vnode we own (bounded so a stuck cluster cannot wedge
/// shutdown). Returns whether process authority was lost during the drain.
async fn drain_local_vnodes(
    handle: &mut ClusterHandle,
    terminal: &tokio_util::sync::CancellationToken,
) -> bool {
    let mut authority_lost = false;
    // Graceful drain. Discovery and the rebalance control plane must
    // stay alive here: peers need to observe our Draining state and
    // the leader needs to rotate our vnodes away before we tear down.
    //
    // 1. Announce Draining so peers stop routing to us and the
    //    leader's `assignable_instances` drops us from assignment.
    if !authority_lost {
        let mut draining = handle.local_node.clone();
        draining.state = NodeState::Draining;
        authority_lost = announce_node_state_with_bound(
            &handle.discovery,
            draining,
            terminal,
            "announce draining state",
        )
        .await;

        if !authority_lost {
            // 2. Flip the local draining flag so that if we are the leader,
            //    our own rebalance controller excludes us from assignment.
            let retains_drain_leadership = handle.cluster_controller.begin_drain();
            // A non-leader yields immediately. The certified current leader must keep
            // renewing until it checkpoints its own predecessor cut; the target roster
            // excludes it and transfers candidacy after the committed assignment is adopted.
            if !retains_drain_leadership {
                handle.leader_lease.cancel();
            }

            // 3. Block until the leader has reassigned every vnode we own,
            //    bounded so a stuck cluster can't wedge shutdown forever.
            let me = laminar_core::state::NodeId(handle.local_node.id.0);
            let drain = async {
                match handle.cluster_controller.checkpoint_authority() {
                    Ok(authority) => {
                        laminar_db::rebalance::wait_until_drained(
                            &handle.snapshot_store,
                            Some(&authority),
                            me,
                            handle.vnode_count,
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
    authority_lost
}

/// Tear down advertisement, rebalance, leader lease, and the database in the
/// fixed order, re-checking the process terminal token between every stage.
/// A graceful stop lets checkpoint tails settle before withdrawing discovery;
/// terminal lease loss stops external admission and advertisement first.
/// Returns the database shutdown result, whether every runtime task stopped
/// cleanly, and whether authority was lost along the way.
async fn teardown_runtime_and_database(
    handle: &mut ClusterHandle,
    terminal: &tokio_util::sync::CancellationToken,
    mut authority_lost: bool,
) -> (Result<(), laminar_db::DbError>, bool, bool) {
    let mut external_runtime_stopped = false;
    let mut runtime_tasks_clean = true;
    if authority_lost {
        runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
            &mut handle.discovery,
            &mut handle.membership_handle,
            handle.watcher_handle.as_mut(),
            &mut handle.api_handle,
        )
        .await;
        external_runtime_stopped = true;
    }

    runtime_tasks_clean &=
        stop_rebalance_tasks(&mut handle.rebalance_tasks, &handle.rebalance_shutdown).await;

    authority_lost |= terminal.is_cancelled();
    if authority_lost && !external_runtime_stopped {
        runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
            &mut handle.discovery,
            &mut handle.membership_handle,
            handle.watcher_handle.as_mut(),
            &mut handle.api_handle,
        )
        .await;
        external_runtime_stopped = true;
    }

    let stop = handle.leader_lease.stop();
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
                &mut handle.discovery,
                &mut handle.membership_handle,
                handle.watcher_handle.as_mut(),
                &mut handle.api_handle,
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
            &mut handle.discovery,
            &mut handle.membership_handle,
            handle.watcher_handle.as_mut(),
            &mut handle.api_handle,
        )
        .await;
        external_runtime_stopped = true;
    }

    let shutdown = handle.db.shutdown();
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
            &mut handle.discovery,
            &mut handle.membership_handle,
            handle.watcher_handle.as_mut(),
            &mut handle.api_handle,
        )
        .await;
        external_runtime_stopped = true;
    }

    let shutdown_result = match shutdown_result {
        Some(result) => result,
        None => shutdown.await,
    };

    // A graceful stop lets checkpoint tails settle before withdrawing discovery. Terminal
    // lease loss does the reverse: stop all external admission and advertisement first.
    if !external_runtime_stopped {
        runtime_tasks_clean &= stop_cluster_advertisement_and_admission(
            &mut handle.discovery,
            &mut handle.membership_handle,
            handle.watcher_handle.as_mut(),
            &mut handle.api_handle,
        )
        .await;
    }
    handle.db_shutdown_complete = shutdown_result.is_ok();
    (shutdown_result, runtime_tasks_clean, authority_lost)
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
