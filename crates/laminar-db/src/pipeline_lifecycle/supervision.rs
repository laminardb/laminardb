use super::{Arc, DbError, DbState, LaminarDB, StartupAttempt};

/// Prune timestamps outside `window`; if under `max_restarts`, record `now` and return
/// the 0-based attempt index within the window. `None` once the budget is exhausted.
pub(super) fn claim_restart_slot(
    history: &mut Vec<std::time::Instant>,
    now: std::time::Instant,
    max_restarts: usize,
    window: std::time::Duration,
) -> Option<usize> {
    history.retain(|t| now.duration_since(*t) < window);
    if history.len() >= max_restarts {
        None
    } else {
        let attempt = history.len();
        history.push(now);
        Some(attempt)
    }
}

/// Exponential backoff `initial * 2^attempt`, saturating and capped at `max`.
pub(super) fn backoff_for_attempt(
    initial: std::time::Duration,
    max: std::time::Duration,
    attempt: usize,
) -> std::time::Duration {
    let shift = u32::try_from(attempt).unwrap_or(u32::MAX).min(20);
    initial.saturating_mul(1u32 << shift).min(max)
}

/// Drive supervised restart independently of the watcher that observed the fault.
///
/// The database control runtime remains stable across caller-runtime teardown and is also where
/// the replacement watcher, connector tasks, and committer are spawned.
pub(super) fn spawn_supervised_restart(
    db: Arc<LaminarDB>,
    history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
) -> Result<tokio::task::JoinHandle<()>, DbError> {
    let handle = db.control_runtime.handle()?;
    Ok(handle.spawn(attempt_supervised_restart(db, history, metrics)))
}

/// One recover-from-checkpoint restart, honoring the restart budget.
pub(super) async fn attempt_supervised_restart(
    db: Arc<LaminarDB>,
    history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
) {
    if let Err(error) = db.ensure_catalog_cleanup_unfenced("supervised restart") {
        tracing::error!(%error, "supervisor rejected restart of terminally fenced database");
        return;
    }
    let policy = db.config.restart_policy.clone();
    let slot = {
        let mut hist = history.lock();
        claim_restart_slot(
            &mut hist,
            std::time::Instant::now(),
            policy.max_restarts,
            policy.window,
        )
    };
    let Some(attempt) = slot else {
        tracing::error!(
            max = policy.max_restarts,
            "pipeline faulted too many times within the restart window; \
             staying faulted for manual recovery"
        );
        return;
    };
    let backoff = backoff_for_attempt(policy.initial_backoff, policy.max_backoff, attempt);
    tokio::time::sleep(backoff).await;
    // A concurrent stop/shutdown moves the state out of Faulted; don't fight it — and don't count a
    // restart that won't happen, so a benign stop during backoff doesn't inflate the metric.
    if !matches!(DbState::load(&db.state), DbState::Faulted) {
        return;
    }
    if let Some(ref m) = metrics {
        m.pipeline_restarts_total.inc();
    }
    // Capture the reason before start() clears `last_fault`, so it survives in the log.
    let fault = db.last_fault().unwrap_or_else(|| "unknown".to_string());
    tracing::warn!(
        fault = %fault, ?backoff,
        "auto-restarting faulted pipeline from last checkpoint"
    );
    if let Err(e) = db.start().await {
        tracing::error!(error = %e, "auto-restart failed; pipeline left non-running");
    }
}

pub(super) struct StartupDriverGuard {
    attempt: Arc<StartupAttempt>,
    state: Arc<std::sync::atomic::AtomicU8>,
    last_fault: Arc<parking_lot::Mutex<Option<String>>>,
    armed: bool,
}

impl StartupDriverGuard {
    pub(super) fn new(db: &LaminarDB, attempt: Arc<StartupAttempt>) -> Self {
        Self {
            attempt,
            state: Arc::clone(&db.state),
            last_fault: Arc::clone(&db.last_fault),
            armed: true,
        }
    }

    pub(super) fn finish(mut self, mut result: Result<(), DbError>) {
        let observed = DbState::load(&self.state);
        if result.is_ok() && observed != DbState::Running {
            result = Err(DbError::Pipeline(format!(
                "startup driver completed in {observed:?} instead of Running"
            )));
            DbState::Faulted.store(&self.state);
        } else if result.is_err() && observed == DbState::Starting {
            self.last_fault
                .lock()
                .get_or_insert_with(|| "startup failed before publishing a terminal state".into());
            DbState::Faulted.store(&self.state);
        }
        self.attempt.complete(result);
        self.armed = false;
    }
}

impl Drop for StartupDriverGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let message =
            "startup driver was cancelled before terminal cleanup; pipeline remains fenced";
        self.last_fault.lock().get_or_insert(message.into());
        DbState::Faulted.store(&self.state);
        self.attempt
            .complete(Err(DbError::Pipeline(message.into())));
    }
}

/// Retire only the transition handed to a graph generation that never launches. A later
/// generation must reload the durable cut before source intake can open.
#[cfg(feature = "cluster")]
pub(super) struct PendingVnodeTransitionLaunchGuard {
    handle: crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    expected: Option<Arc<crate::vnode_transition_staging::PendingVnodeTransition>>,
    armed: bool,
}

#[cfg(feature = "cluster")]
impl PendingVnodeTransitionLaunchGuard {
    pub(super) fn capture(db: &LaminarDB) -> Self {
        Self {
            handle: Arc::clone(&db.pending_vnode_transition),
            expected: db.pending_vnode_transition.lock().clone(),
            armed: true,
        }
    }

    pub(super) fn complete(&mut self) {
        self.expected.take();
        self.armed = false;
    }
}

#[cfg(feature = "cluster")]
impl Drop for PendingVnodeTransitionLaunchGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(expected) = self.expected.as_ref() else {
            return;
        };
        crate::vnode_transition_staging::retire_exact_pending_vnode_transition(
            &self.handle,
            expected,
        );
    }
}

pub(super) fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic")
}

pub(super) fn publish_runtime_fault_state(state: &std::sync::atomic::AtomicU8) -> bool {
    loop {
        let observed = DbState::load(state);
        match observed {
            DbState::Starting | DbState::Running => {
                if DbState::compare_exchange(observed, DbState::Faulted, state).is_ok() {
                    return true;
                }
            }
            DbState::Faulted | DbState::Created | DbState::ShuttingDown | DbState::Stopped => {
                return false;
            }
        }
    }
}

pub(super) fn runtime_exit_is_covered_by_terminal_stop(
    owns_fault_state: bool,
    state: &std::sync::atomic::AtomicU8,
    runtime_shutdown: &tokio_util::sync::CancellationToken,
) -> bool {
    !owns_fault_state
        && runtime_shutdown.is_cancelled()
        && DbState::load(state) == DbState::ShuttingDown
}

/// Retire every vnode-state claim owned by a terminal cluster compute generation before making
/// its fault observable. The compute future and its private runtime must already be dropped, so
/// taking the write side proves no callback from that generation can still publish the staged
/// transition. Assignment adoption takes the same fence and therefore cannot split retirement
/// from the `Faulted` publication.
#[cfg(feature = "cluster")]
pub(super) fn retire_cluster_compute_generation(
    pending_vnode_transition: &crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    installed_vnode_state: &crate::vnode_transition_staging::InstalledVnodeStateHandle,
) {
    pending_vnode_transition.lock().take();
    installed_vnode_state.lock().take();
}

/// Serialize retirement of the two vnode-state claims against every graph callback and
/// assignment adoption. The returned guard lets lifecycle callers retain the serialization
/// through their terminal state publication.
#[cfg(feature = "cluster")]
pub(super) async fn retire_cluster_compute_generation_until(
    rotation_execution_fence: &Arc<tokio::sync::RwLock<()>>,
    pending_vnode_transition: &crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    installed_vnode_state: &crate::vnode_transition_staging::InstalledVnodeStateHandle,
    deadline: tokio::time::Instant,
) -> Result<tokio::sync::OwnedRwLockWriteGuard<()>, tokio::time::error::Elapsed> {
    let generation =
        tokio::time::timeout_at(deadline, Arc::clone(rotation_execution_fence).write_owned())
            .await?;
    retire_cluster_compute_generation(pending_vnode_transition, installed_vnode_state);
    Ok(generation)
}

#[cfg(feature = "cluster")]
pub(super) fn publish_cluster_compute_fault_state(
    state: &std::sync::atomic::AtomicU8,
    rotation_execution_fence: &Arc<tokio::sync::RwLock<()>>,
    pending_vnode_transition: &crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    installed_vnode_state: &crate::vnode_transition_staging::InstalledVnodeStateHandle,
) -> bool {
    let _generation = rotation_execution_fence.blocking_write();
    retire_cluster_compute_generation(pending_vnode_transition, installed_vnode_state);
    publish_runtime_fault_state(state)
}

#[cfg(feature = "cluster")]
pub(super) fn publish_cluster_terminal_compute_halt_state(
    state: &std::sync::atomic::AtomicU8,
    authority_transition: &parking_lot::Mutex<()>,
    terminal_pipeline_halt: &std::sync::atomic::AtomicBool,
    source_gate: &std::sync::atomic::AtomicBool,
    recovery_fence: &std::sync::atomic::AtomicBool,
    rotation_execution_fence: &Arc<tokio::sync::RwLock<()>>,
    pending_vnode_transition: &crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    installed_vnode_state: &crate::vnode_transition_staging::InstalledVnodeStateHandle,
) -> bool {
    latch_cluster_terminal_data_plane(
        authority_transition,
        terminal_pipeline_halt,
        source_gate,
        recovery_fence,
    );
    publish_cluster_compute_fault_state(
        state,
        rotation_execution_fence,
        pending_vnode_transition,
        installed_vnode_state,
    )
}

/// Linearize a terminal latch and its data-plane close against every source/shuffle authority
/// grant. Vnode-generation retirement deliberately happens after this guard is released: the
/// assignment activation path takes the rotation fence before this transition lock.
#[cfg(feature = "cluster")]
pub(super) fn latch_cluster_terminal_data_plane(
    authority_transition: &parking_lot::Mutex<()>,
    terminal_latch: &std::sync::atomic::AtomicBool,
    source_gate: &std::sync::atomic::AtomicBool,
    recovery_fence: &std::sync::atomic::AtomicBool,
) {
    let _transition = authority_transition.lock();
    terminal_latch.store(true, std::sync::atomic::Ordering::SeqCst);
    recovery_fence.store(true, std::sync::atomic::Ordering::Release);
    source_gate.store(true, std::sync::atomic::Ordering::SeqCst);
}
