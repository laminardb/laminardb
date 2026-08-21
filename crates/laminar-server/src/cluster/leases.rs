//! Process-identity and leader lease runtimes: durable acquisition, renewal
//! task ownership, terminal monitoring, and synchronous authority fencing.
//!
//! Responsibility: own the stable-node process lease (acquire/takeover with
//! bounded object-store I/O, renewal task, terminal monitor) and the leader
//! lease supervised task, and fence every local authority synchronously when
//! either lease is lost.
//!
//! Invariants:
//! - the process terminal token fires on monotonic deadline expiry or loss of
//!   the live watch, and every resource installed after acquisition is fenced
//!   through it;
//! - `revoke_process_authority` and `fence_authority` perform only synchronous
//!   fail-closed actions; asynchronous cleanup stays with their callers;
//! - `Drop` aborts owned tasks and fences the deadline but never performs I/O;
//! - takeover waits out the incumbent's observed TTL before stealing a term, so
//!   a live predecessor can never be superseded by a racing boot.

use std::sync::Arc;

use tokio::sync::watch;
use tracing::warn;

use laminar_db::LaminarDB;

use super::ClusterStartupError;

const PROCESS_LEASE_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
pub(super) const PROCESS_LEASE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub(super) struct ProcessLeaseRuntime {
    pub(super) acquired: laminar_core::cluster::control::ProcessLease,
    pub(super) deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    pub(super) live_rx: watch::Receiver<bool>,
    pub(super) shutdown: tokio_util::sync::CancellationToken,
    pub(super) terminal: tokio_util::sync::CancellationToken,
    pub(super) renewal_task: tokio::task::JoinHandle<()>,
    pub(super) terminal_task: tokio::task::JoinHandle<()>,
    pub(super) fence_task: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn spawn_process_lease_terminal_monitor(
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

pub(super) struct LeaderLeaseRuntime {
    pub(super) shutdown: tokio_util::sync::CancellationToken,
    pub(super) task: Option<tokio::task::JoinHandle<()>>,
}

impl LeaderLeaseRuntime {
    pub(super) fn new(
        shutdown: tokio_util::sync::CancellationToken,
        task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            shutdown,
            task: Some(task),
        }
    }

    pub(super) fn cancel(&self) {
        self.shutdown.cancel();
    }

    pub(super) fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.clone()
    }

    pub(super) async fn wait_for_exit(&self) {
        let Some(task) = self.task.as_ref() else {
            return;
        };
        super::wait_for_cluster_task_exit(task).await;
    }

    pub(super) async fn stop(&mut self) {
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

/// Synchronously fence every local authority the process lease guards. Cleanup
/// I/O (discovery withdrawal, database shutdown) belongs to the caller.
pub(super) fn revoke_process_authority(
    db: &LaminarDB,
    serving_gate: &crate::http::ServingGate,
    leader_lease_shutdown: &tokio_util::sync::CancellationToken,
    terminal: &tokio_util::sync::CancellationToken,
) {
    leader_lease_shutdown.cancel();
    serving_gate.fence();
    db.revoke_cluster_authority();
    terminal.cancel();
}

impl ProcessLeaseRuntime {
    pub(super) fn is_live(&self) -> bool {
        self.deadline.is_live()
            && *self.live_rx.borrow()
            && !self.terminal.is_cancelled()
            && !self.renewal_task.is_finished()
    }

    pub(super) fn terminal_token(&self) -> tokio_util::sync::CancellationToken {
        self.terminal.clone()
    }

    pub(super) fn fence_authority(&self) {
        self.deadline.fence();
        self.shutdown.cancel();
        self.renewal_task.abort();
        self.terminal.cancel();
    }

    pub(super) fn disarm_for_shutdown(&mut self) -> bool {
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
        self.deadline.fence();
        self.terminal.cancel();
        was_live
    }

    pub(super) fn install_fence(
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

pub(super) async fn acquire_process_lease(
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
