//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode; always on).
//!
//! The leader freezes assignment owners plus available fault-evidence reporters, announces
//! `Prepare`, and requires each exact process to stop and inventory unresolved prepared
//! checkpoints. It terminally settles that inventory before selecting a recovery cut and
//! announcing `Start`. No committed epoch means target 0 — a fresh start from initial offsets.
//! Source intake remains closed until every owner restores and the release is durably committed.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::FutureExt as _;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::runtime::Handle;

use laminar_core::checkpoint::{
    CheckpointAssignmentAdoption, CheckpointAssignmentFence, CommittedCheckpointIndex,
};
use laminar_core::checkpoint_decision::{CheckpointOutcome, CheckpointScope};
use laminar_core::cluster::control::controller::{
    RecoveryAnnouncement, RecoveryFault, RecoveryFaultInventory, RecoveryRound,
    RecoveryStoppedReport,
};
use laminar_core::cluster::control::{
    ClusterController, RecoverPhase, RecoveryControlError, RecoveryFaultDisposition,
    RecoveryFaultReportOutcome, ReleaseCommitStatus,
};
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

/// Healthy-state monitor cadence. Only the leader polls the shared fault inventory; followers use
/// the replicated recovery intent unless they have a local settlement latch.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
const RELEASE_PROTOCOL_TIMEOUT: Duration = Duration::from_secs(60);
const DECISION_IO_TIMEOUT: Duration = Duration::from_secs(15);
const RECOVERY_START_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_QUORUM_INITIAL_POLL: Duration = Duration::from_millis(100);
const STOP_QUORUM_MAX_POLL: Duration = Duration::from_secs(1);
const STOP_ROSTER_AUDIT_INTERVAL: Duration = Duration::from_secs(1);
/// How long a stopped/restored node tolerates the exact round disappearing before faulting again.
const ORPHAN_STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery target meaning "no committed cut exists": start fresh.
const GENESIS: u64 = 0;

fn recovery_stop_lifecycle_ceiling(stop_timeout: Duration) -> Duration {
    // The owner-thread wait must outlive the stop's own deadline so a clean stop result, purge,
    // and active-guard release can reach the monitor instead of becoming an orphaned operation.
    stop_timeout.saturating_add(DECISION_IO_TIMEOUT)
}

fn stopped_quorum_ceiling(stop_lifecycle_timeout: Duration) -> Duration {
    // A peer may observe Prepare one healthy poll late, durably adopt the generation, consume its
    // full lifecycle ceiling, publish the stopped report, and then wait one quorum poll before the
    // driver sees it. The decision/report terms use the same conservative I/O bound.
    stop_lifecycle_timeout
        .saturating_add(POLL_INTERVAL)
        .saturating_add(DECISION_IO_TIMEOUT)
        .saturating_add(DECISION_IO_TIMEOUT)
        .saturating_add(STOP_QUORUM_MAX_POLL)
}

fn recovery_stop_lifecycle_timeout(db: &LaminarDB) -> Duration {
    recovery_stop_lifecycle_ceiling(db.coordinated_recovery_stop_timeout())
}

fn recovery_stopped_quorum_timeout(db: &LaminarDB) -> Duration {
    stopped_quorum_ceiling(recovery_stop_lifecycle_timeout(db))
}

fn checked_recovery_deadline(timeout: Duration) -> Option<tokio::time::Instant> {
    tokio::time::Instant::now().checked_add(timeout)
}

fn local_fault_disposition(db: &LaminarDB) -> RecoveryFaultDisposition {
    if db.terminal_pipeline_halt.load(Ordering::Acquire) {
        RecoveryFaultDisposition::Terminal
    } else {
        RecoveryFaultDisposition::Recoverable
    }
}

fn install_new_local_fault_request(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    let request = controller.next_recovery_fault_request()?;
    pending.fetch_max(request.sequence(), Ordering::AcqRel);
    Ok(pending.load(Ordering::Acquire))
}

/// Queue one new local fault event, atomically superseding an older outstanding request. The
/// request remains latched until an authorized committed Release consumes it.
pub(crate) fn queue_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<(), String> {
    install_new_local_fault_request(controller, pending).map(|_| ())
}

fn retain_local_fault_request(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    loop {
        let observed = pending.load(Ordering::Acquire);
        if observed != 0 {
            return Ok(observed);
        }
        let request = controller.next_recovery_fault_request()?.sequence();
        match pending.compare_exchange(0, request, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(request),
            Err(concurrent) if concurrent != 0 => return Ok(concurrent),
            Err(_) => {}
        }
    }
}

async fn persist_local_fault(
    controller: &ClusterController,
    raw_request: u64,
    disposition: RecoveryFaultDisposition,
) -> Result<RecoveryFaultReportOutcome, String> {
    let request = controller.recovery_fault_request(raw_request)?;
    let report = async {
        match disposition {
            RecoveryFaultDisposition::Recoverable => controller.report_fault(request).await,
            RecoveryFaultDisposition::Terminal => controller.report_terminal_fault(request).await,
        }
    };
    match tokio::time::timeout(DECISION_IO_TIMEOUT, report).await {
        Ok(Ok(outcome)) => {
            if outcome == RecoveryFaultReportOutcome::Active {
                tracing::warn!(
                    request_ordinal = raw_request,
                    "reported local fault for coordinated cluster recovery"
                );
            }
            Ok(outcome)
        }
        Ok(Err(error)) => {
            tracing::error!(request_ordinal = raw_request, %error, "could not persist local recovery fault");
            Err(error)
        }
        Err(_) => Err("local recovery fault publication timed out".into()),
    }
}

/// Publish the exact queued request without clearing its terminal-discovery latch.
async fn flush_pending_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
    disposition: RecoveryFaultDisposition,
) -> Result<RecoveryFaultReportOutcome, String> {
    let raw_request = pending.load(Ordering::Acquire);
    if raw_request == 0 {
        return Ok(RecoveryFaultReportOutcome::AlreadyCleared);
    }
    persist_local_fault(controller, raw_request, disposition).await
}

/// Coalesce a duplicate notification into the outstanding request and make one bounded durable
/// publication attempt.
pub(crate) async fn request_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    let raw_request = retain_local_fault_request(controller, pending)?;
    match persist_local_fault(
        controller,
        raw_request,
        RecoveryFaultDisposition::Recoverable,
    )
    .await?
    {
        RecoveryFaultReportOutcome::Active => Ok(raw_request),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => {
            let concurrent = pending.load(Ordering::Acquire);
            let fresh_request = if concurrent != 0 && concurrent != raw_request {
                concurrent
            } else {
                install_new_local_fault_request(controller, pending)?
            };
            match persist_local_fault(
                controller,
                fresh_request,
                RecoveryFaultDisposition::Recoverable,
            )
            .await?
            {
                RecoveryFaultReportOutcome::Active => Ok(fresh_request),
                RecoveryFaultReportOutcome::AlreadyCleared
                | RecoveryFaultReportOutcome::CoveredByNewerRequest => Err(format!(
                    "fresh recovery fault request {fresh_request} was settled before it became active"
                )),
                RecoveryFaultReportOutcome::TerminalFenceActive => Err(
                    "durable terminal pipeline fault already fences automatic recovery".into(),
                ),
            }
        }
        RecoveryFaultReportOutcome::TerminalFenceActive => {
            Err("durable terminal pipeline fault already fences automatic recovery".into())
        }
    }
}

async fn request_fresh_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
    disposition: RecoveryFaultDisposition,
) -> Result<u64, String> {
    let raw_request = install_new_local_fault_request(controller, pending)?;
    match persist_local_fault(controller, raw_request, disposition).await? {
        RecoveryFaultReportOutcome::Active => Ok(raw_request),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => Err(format!(
            "fresh recovery fault request {raw_request} was settled before it became active"
        )),
        RecoveryFaultReportOutcome::TerminalFenceActive
            if disposition == RecoveryFaultDisposition::Terminal =>
        {
            Ok(raw_request)
        }
        RecoveryFaultReportOutcome::TerminalFenceActive => {
            Err("durable terminal pipeline fault already fences automatic recovery".into())
        }
    }
}

/// Publish (or confirm) the permanent terminal disposition for this process's retained request.
pub(crate) async fn request_local_terminal_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<(u64, RecoveryFaultReportOutcome), String> {
    let raw_request = retain_local_fault_request(controller, pending)?;
    match persist_local_fault(controller, raw_request, RecoveryFaultDisposition::Terminal).await? {
        active @ (RecoveryFaultReportOutcome::Active
        | RecoveryFaultReportOutcome::TerminalFenceActive) => Ok((raw_request, active)),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => {
            let fresh_request = install_new_local_fault_request(controller, pending)?;
            let outcome = persist_local_fault(
                controller,
                fresh_request,
                RecoveryFaultDisposition::Terminal,
            )
            .await?;
            Ok((fresh_request, outcome))
        }
    }
}

/// Spawn the long-lived per-node recovery supervisor. It drives stop/start, so it must outlive
/// those cycles. An unexpected monitor panic closes intake, publishes a fresh durable fault, and
/// reconstructs the monitor from the durable control plane instead of silently disabling cluster
/// recovery.
pub(crate) fn spawn_monitor(db: &Arc<LaminarDB>, runtime: &Handle) -> tokio::task::JoinHandle<()> {
    let weak = Arc::downgrade(db);
    runtime.spawn(async move {
        loop {
            let outcome =
                std::panic::AssertUnwindSafe(RecoveryMonitor::default().run(Weak::clone(&weak)))
                    .catch_unwind()
                    .await;
            if outcome.is_ok() {
                return;
            }
            let Some(db) = weak.upgrade() else {
                return;
            };
            if db.is_closed() {
                return;
            }
            db.set_source_gate(true);
            let controller = { db.cluster_controller.lock().clone() };
            if let Some(controller) = controller {
                controller.set_recovering(true);
                if let Err(error) = request_fresh_local_fault(
                    &controller,
                    &db.pending_recovery_fault,
                    local_fault_disposition(&db),
                )
                .await
                {
                    tracing::error!(%error, "could not persist monitor restart fault");
                }
            }
            tracing::error!(
                "coordinated recovery monitor panicked; intake fenced and monitor restarting"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
}

#[derive(Default)]
struct RecoveryMonitor {
    applied_gen: u64,
    handled_faults: FxHashMap<NodeId, u64>,
    /// Leader term for which this node has resumed any durable artifact cleanup.
    retention_leader: Option<laminar_core::checkpoint::LeaderProof>,
    /// Whether a visible, unhandled durable fault has already suspended local assignment
    /// authority. The report remains level-triggered, but the suspension revision advances only
    /// once per continuously held fault period.
    fault_fenced: bool,
    /// A failed durable fault audit has closed authority but has not yet been converted into a
    /// durable local retry trigger. It must never be cleared merely because a later read is empty.
    fault_audit_unknown: bool,
    /// Round this node stopped for at `Prepare`, until its `Start` restores it.
    stopped_for: Option<(RecoveryRound, tokio::time::Instant)>,
    /// Exact Start this process restored and acknowledged. Sources remain gated until the
    /// identical Release arrives.
    restored_for: Option<(RecoveryAnnouncement, tokio::time::Instant)>,
    /// Deduplicates a persistent malformed/conflicting control-plane report.
    last_protocol_error: Option<String>,
    /// Exact in-process request whose authority publication succeeded. It suppresses redundant
    /// writes while the durable outstanding-request latch remains unchanged.
    published_local_request: Option<(u64, RecoveryFaultDisposition)>,
    /// Deterministic unit-test seam between idempotent drain settlement and the local authority
    /// revision recheck. Production has no hook or additional synchronization here.
    #[cfg(test)]
    release_drain_settlement_hook: Option<ReleaseDrainSettlementHook>,
}

#[cfg(test)]
struct ReleaseDrainSettlementHook {
    observed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryQuorum {
    Reached,
    Superseded,
    Conflicted,
    ParticipantsChanged,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestoreAckOutcome {
    Restored,
    Retry,
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StoppedQuorum {
    Reached(Vec<RecoveryStoppedReport>),
    LeadershipLost,
    Superseded,
    Conflicted,
    ParticipantsChanged,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundDriverAuthority {
    Current,
    Superseded,
    Uncertain,
    Conflicted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrepareOwnership {
    Owned,
    LeadershipLost,
    Invalid,
}

impl RecoveryMonitor {
    async fn run(mut self, weak: Weak<LaminarDB>) {
        let mut poll = tokio::time::interval(POLL_INTERVAL);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            // Tokio intervals tick immediately first, closing the startup window in which a
            // durable fault existed before this supervisor began polling.
            poll.tick().await;
            let Some(db) = weak.upgrade() else {
                return;
            };
            if db.is_closed() {
                return;
            }
            let Some(controller) = db.cluster_controller.lock().clone() else {
                continue;
            };

            match controller.capture_leader_proof() {
                None => self.retention_leader = None,
                Some(proof) if self.retention_leader.as_ref() != Some(&proof) => {
                    let coordinator = db.coordinator.lock().await;
                    if let Some(coordinator) = coordinator.as_ref() {
                        match coordinator.schedule_cluster_retention_resume(proof.clone()) {
                            Ok(()) => self.retention_leader = Some(proof),
                            Err(error) => {
                                tracing::warn!(%error, "could not resume cluster checkpoint retention after leadership acquisition");
                            }
                        }
                    }
                }
                Some(_) => {}
            }

            let pending_published = self.publish_pending_local_fault(&db, &controller).await;
            if db.pending_recovery_fault.load(Ordering::Acquire) != 0 && !pending_published {
                continue;
            }
            if self.fault_audit_unknown {
                self.hold_fault_fence(&db, &controller);
                if pending_published
                    || (db.pending_recovery_fault.load(Ordering::Acquire) == 0
                        && self.request_fresh_fault(&db, &controller).await)
                {
                    self.fault_audit_unknown = false;
                }
                continue;
            }
            let local_fault = match self.pending_local_fault_if_queued(&db, &controller).await {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::error!(%error, "could not read the local recovery fault report");
                    self.hold_for_unknown_fault_audit(&db, &controller).await;
                    continue;
                }
            };
            if local_fault.is_some_and(RecoveryFault::is_terminal) {
                self.latch_durable_terminal_fault(&db, &controller, Some(controller.instance_id()));
            }
            let local_pending = local_fault.into_iter().collect::<Vec<_>>();
            self.hold_for_visible_or_queued_fault(&db, &controller, &local_pending);
            self.observe(&db, &controller, local_fault).await;
            if !controller.is_leader() {
                continue;
            }
            let inventory = match self.fault_inventory(&controller).await {
                Ok(inventory) => inventory,
                Err(error) => {
                    tracing::error!(%error, "could not read cluster recovery fault reports");
                    self.hold_for_unknown_fault_audit(&db, &controller).await;
                    continue;
                }
            };
            let reported = inventory.faults().to_vec();
            if inventory.has_terminal_fault() {
                let reporter = reported
                    .iter()
                    .copied()
                    .find(|fault| fault.is_terminal())
                    .map(|fault| fault.reporter);
                self.latch_durable_terminal_fault(&db, &controller, reporter);
            }
            let pending = self.unhandled_faults(&reported);
            self.hold_for_visible_or_queued_fault(&db, &controller, &pending);

            // `drive_round` owns every nonterminal local Prepare/Start synchronously. Seeing one
            // here means that owner disappeared or returned early. A stopped Prepare is never
            // cleared: once its topology is safe to supersede, the fresh generation's durable
            // Prepare overwrites it directly. A pending Release is retained for its prepare/commit
            // retry, but must not mask a later fault set.
            let mut required_prepare_fence = None;
            let active =
                tokio::time::timeout(DECISION_IO_TIMEOUT, controller.observe_recover_control())
                    .await;
            match active {
                Err(_) => continue,
                Ok(result) => match result {
                    Ok(Some(active)) if active.round.has_terminal_fault() => {
                        self.latch_durable_terminal_fault(
                            &db,
                            &controller,
                            active
                                .round
                                .faults
                                .iter()
                                .copied()
                                .find(|fault| fault.is_terminal())
                                .map(|fault| fault.reporter),
                        );
                        tracing::error!(
                            gen = active.round.id.generation,
                            "retaining terminal recovery Prepare; automatic recovery is disabled"
                        );
                        continue;
                    }
                    Ok(Some(active)) if matches!(active.phase, RecoverPhase::Prepare) => {
                        if pending.is_empty() {
                            hold_intake_for_retry(
                                &db,
                                &controller,
                                active.round.id.generation,
                                false,
                            );
                            if !self.ensure_fault_request(&db, &controller).await {
                                self.fault_audit_unknown = true;
                            }
                            continue;
                        }
                        match recovery_prepare_supersession_fence_after_assignment_settlement(
                            &db,
                            &controller,
                            &active.round,
                        )
                        .await
                        {
                            Ok(Some(fence)) => {
                                required_prepare_fence = Some(fence);
                                tracing::warn!(
                                    gen = active.round.id.generation,
                                    old_driver = active.round.id.driver.0,
                                    "retrying stopped recovery with a direct Prepare-to-Prepare handoff"
                                );
                            }
                            Ok(None) => {
                                hold_intake_for_retry(
                                    &db,
                                    &controller,
                                    active.round.id.generation,
                                    false,
                                );
                                continue;
                            }
                            Err(error) => {
                                tracing::warn!(
                                    gen = active.round.id.generation,
                                    old_driver = active.round.id.driver.0,
                                    %error,
                                    "stopped recovery supersession audit failed; retaining Prepare"
                                );
                                hold_intake_for_retry(
                                    &db,
                                    &controller,
                                    active.round.id.generation,
                                    false,
                                );
                                continue;
                            }
                        }
                    }
                    Ok(Some(active)) if controller.recovery_driver_is_current(&active.round) => {
                        match active.phase {
                            RecoverPhase::Release { .. }
                                if active.round.fault_revision() == inventory.revision()
                                    && active.round.faults == reported =>
                            {
                                // `observe` owns retrying the retained prepare/commit barrier. Do
                                // not overwrite the exact still-active pre-commit fault inventory.
                                continue;
                            }
                            RecoverPhase::Release { .. }
                            | RecoverPhase::ReleaseCommitted { .. } => {}
                            RecoverPhase::Start { .. }
                                if active.round.id.driver == controller.instance_id() =>
                            {
                                let _ = controller.clear_recover(&active.round).await;
                                if !self.ensure_fault_request(&db, &controller).await {
                                    self.fault_audit_unknown = true;
                                }
                            }
                            RecoverPhase::Prepare | RecoverPhase::Start { .. } => continue,
                        }
                    }
                    Err(
                        RecoveryControlError::Uncertain(_) | RecoveryControlError::Superseded(_),
                    ) => continue,
                    Ok(_) => {}
                    Err(RecoveryControlError::Conflict(error)) => {
                        tracing::error!(%error, "invalid active recovery intent");
                        self.hold_for_unknown_fault_audit(&db, &controller).await;
                        continue;
                    }
                },
            }
            if !pending.is_empty() {
                self.drive_round(
                    &db,
                    &controller,
                    inventory.revision(),
                    reported,
                    required_prepare_fence.as_ref(),
                )
                .await;
            }
        }
    }

    async fn publish_pending_local_fault(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) -> bool {
        let raw_request = db.pending_recovery_fault.load(Ordering::Acquire);
        if raw_request == 0 {
            self.published_local_request = None;
            return false;
        }
        self.hold_fault_fence(db, controller);
        let disposition = local_fault_disposition(db);
        if self.published_local_request == Some((raw_request, disposition)) {
            return true;
        }
        match flush_pending_local_fault(controller, &db.pending_recovery_fault, disposition).await {
            Ok(RecoveryFaultReportOutcome::Active) => {
                self.cache_published_local_request(db, raw_request, disposition)
            }
            Ok(RecoveryFaultReportOutcome::TerminalFenceActive) => {
                self.latch_durable_terminal_fault(db, controller, Some(controller.instance_id()));
                true
            }
            Ok(
                RecoveryFaultReportOutcome::AlreadyCleared
                | RecoveryFaultReportOutcome::CoveredByNewerRequest,
            ) => false,
            Err(error) => {
                tracing::warn!(%error, "queued local fault remains pending durable publication");
                false
            }
        }
    }

    async fn request_fresh_fault(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) -> bool {
        self.published_local_request = None;
        let disposition = local_fault_disposition(db);
        match request_fresh_local_fault(controller, &db.pending_recovery_fault, disposition).await {
            Ok(raw_request) => self.cache_published_local_request(db, raw_request, disposition),
            Err(error) => {
                tracing::warn!(%error, "fresh recovery fault remains queued for publication retry");
                false
            }
        }
    }

    async fn ensure_fault_request(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) -> bool {
        let pending = db.pending_recovery_fault.load(Ordering::Acquire);
        let disposition = local_fault_disposition(db);
        if pending != 0 && self.published_local_request == Some((pending, disposition)) {
            return true;
        }
        let publication = match disposition {
            RecoveryFaultDisposition::Recoverable => {
                request_local_fault(controller, &db.pending_recovery_fault)
                    .await
                    .map(|raw_request| (raw_request, RecoveryFaultReportOutcome::Active))
            }
            RecoveryFaultDisposition::Terminal => {
                request_local_terminal_fault(controller, &db.pending_recovery_fault).await
            }
        };
        match publication {
            Ok((_, RecoveryFaultReportOutcome::TerminalFenceActive)) => {
                self.latch_durable_terminal_fault(db, controller, Some(controller.instance_id()));
                true
            }
            Ok((raw_request, RecoveryFaultReportOutcome::Active)) => {
                self.cache_published_local_request(db, raw_request, disposition)
            }
            Ok(_) => false,
            Err(error) => {
                tracing::warn!(%error, "recovery fault remains queued for publication retry");
                false
            }
        }
    }

    fn cache_published_local_request(
        &mut self,
        db: &LaminarDB,
        raw_request: u64,
        disposition: RecoveryFaultDisposition,
    ) -> bool {
        if raw_request != 0 && db.pending_recovery_fault.load(Ordering::Acquire) == raw_request {
            self.published_local_request = Some((raw_request, disposition));
            true
        } else {
            self.published_local_request = None;
            false
        }
    }

    /// Act on the leader's announcement: stop on `Prepare`, restore on `Start`.
    async fn observe(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        local_fault: Option<RecoveryFault>,
    ) {
        let mut rejected_committed_gen = None;
        if let Some(start) = self.restored_for.as_ref().map(|(start, _)| start.clone()) {
            if let RecoverPhase::Start { epoch } = start.phase {
                let terminal = tokio::time::timeout(
                    DECISION_IO_TIMEOUT,
                    controller.observe_committed_recover_release(&start.round, epoch),
                )
                .await;
                match terminal {
                    Err(_) => {
                        controller.set_recovering(true);
                        db.set_source_gate(true);
                        return;
                    }
                    Ok(result) => match result {
                        Ok(Some(terminal)) => {
                            self.last_protocol_error = None;
                            if self.observe_release(db, controller, terminal, epoch).await {
                                return;
                            }
                            rejected_committed_gen = Some(start.round.id.generation);
                        }
                        Ok(None) | Err(RecoveryControlError::Superseded(_)) => {}
                        Err(RecoveryControlError::Uncertain(error)) => {
                            controller.set_recovering(true);
                            db.set_source_gate(true);
                            tracing::warn!(%error, "committed recovery release is temporarily unreadable");
                            return;
                        }
                        Err(RecoveryControlError::Conflict(error)) => {
                            self.hold_for_protocol_error(db, controller, error).await;
                            return;
                        }
                    },
                }
            }
        } else {
            let pending_request = db.pending_recovery_fault.load(Ordering::Acquire);
            let stopped_round = self.stopped_for.as_ref().map(|(round, _)| round);
            let needs_terminal_settlement =
                local_fault.is_some() || pending_request != 0 || stopped_round.is_some();
            if needs_terminal_settlement {
                let terminal = match tokio::time::timeout(
                    DECISION_IO_TIMEOUT,
                    controller.latest_committed_recover_release(),
                )
                .await
                {
                    Err(_) => {
                        controller.set_recovering(true);
                        db.set_source_gate(true);
                        return;
                    }
                    Ok(Ok(terminal)) => terminal,
                    Ok(Err(RecoveryControlError::Superseded(_))) => None,
                    Ok(Err(RecoveryControlError::Uncertain(error))) => {
                        controller.set_recovering(true);
                        db.set_source_gate(true);
                        tracing::warn!(%error, "committed recovery release is temporarily unreadable");
                        return;
                    }
                    Ok(Err(RecoveryControlError::Conflict(error))) => {
                        self.hold_for_protocol_error(db, controller, error).await;
                        return;
                    }
                };

                if let Some(terminal) = terminal {
                    let covered_fault = terminal.round.fault_sequence(controller.instance_id());
                    let active_fault_matches =
                        local_fault.is_some_and(|fault| covered_fault == Some(fault.sequence));
                    let published_tombstone_matches = local_fault.is_none()
                        && pending_request != 0
                        && self.published_local_request
                            == Some((pending_request, RecoveryFaultDisposition::Recoverable))
                        && covered_fault.is_some();
                    let stopped_round_matches = local_fault.is_none()
                        && stopped_round.is_some_and(|round| *round == terminal.round);
                    let covers_local_settlement = active_fault_matches
                        || published_tombstone_matches
                        || stopped_round_matches;
                    if terminal.round.id.generation > self.applied_gen
                        && covers_local_settlement
                        && !terminal.round.contains_owner(controller.instance_id())
                    {
                        if !controller.recovery_round_requires_current_process_stop(&terminal.round)
                        {
                            self.applied_gen = terminal.round.id.generation;
                            self.stopped_for = None;
                            self.hold_fault_fence(db, controller);
                            tracing::error!(
                                gen = terminal.round.id.generation,
                                "committed recovery Release omitted or superseded this process; requesting a fresh round"
                            );
                            if !self.request_fresh_fault(db, controller).await {
                                self.fault_audit_unknown = true;
                            }
                            return;
                        }
                        self.last_protocol_error = None;
                        if self
                            .observe_nonparticipant_release(db, controller, &terminal)
                            .await
                        {
                            return;
                        }
                        rejected_committed_gen = Some(terminal.round.id.generation);
                    }
                }
            }
        }

        let observed = match tokio::time::timeout(
            DECISION_IO_TIMEOUT,
            controller.observe_recover_control(),
        )
        .await
        {
            Err(_) => {
                controller.set_recovering(true);
                db.set_source_gate(true);
                return;
            }
            Ok(result) => match result {
                Ok(observed) => {
                    self.last_protocol_error = None;
                    observed
                }
                Err(RecoveryControlError::Uncertain(error)) => {
                    controller.set_recovering(true);
                    db.set_source_gate(true);
                    tracing::warn!(%error, "coordinated-recovery intent is temporarily unreadable");
                    return;
                }
                Err(RecoveryControlError::Superseded(_)) => {
                    self.observe_orphans(db, controller).await;
                    return;
                }
                Err(RecoveryControlError::Conflict(error)) => {
                    self.hold_for_protocol_error(db, controller, error).await;
                    return;
                }
            },
        };
        let current = match observed
            .filter(|announcement| controller.recovery_driver_is_current(&announcement.round))
        {
            Some(announcement)
                if round_assignment_is_current(db, controller, &announcement.round) =>
            {
                Some(announcement)
            }
            Some(announcement)
                if matches!(announcement.phase, RecoverPhase::ReleaseCommitted { .. }) =>
            {
                Some(announcement)
            }
            Some(announcement) => {
                match recovery_round_assignment_is_restorable(
                    db,
                    controller,
                    &announcement.round,
                    tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
                )
                .await
                {
                    Ok(true) => Some(announcement),
                    Ok(false) => None,
                    Err(error) => {
                        tracing::warn!(
                            gen = announcement.round.id.generation,
                            %error,
                            "could not audit suspended assignment authority for recovery control"
                        );
                        None
                    }
                }
            }
            None => None,
        };
        if let Some(terminal) = current
            .as_ref()
            .filter(|active| active.round.has_terminal_fault())
        {
            self.latch_durable_terminal_fault(
                db,
                controller,
                terminal
                    .round
                    .faults
                    .iter()
                    .copied()
                    .find(|fault| fault.is_terminal())
                    .map(|fault| fault.reporter),
            );
        }
        if let Some(rejected_gen) = rejected_committed_gen {
            let successor_is_current = current.as_ref().is_some_and(|announcement| {
                announcement.round.id.generation > rejected_gen
                    && controller.recovery_round_requires_current_process_stop(&announcement.round)
            });
            if !successor_is_current {
                self.applied_gen = self.applied_gen.max(rejected_gen);
                self.restored_for = None;
                self.stopped_for = None;
                self.hold_fault_fence(db, controller);
                tracing::error!(
                    gen = rejected_gen,
                    "committed recovery Release cannot settle locally; requesting a successor round"
                );
                if !self.request_fresh_fault(db, controller).await {
                    self.fault_audit_unknown = true;
                }
                return;
            }
        }
        match current {
            Some(RecoveryAnnouncement {
                round,
                phase: RecoverPhase::Prepare,
            }) if round.id.generation > self.applied_gen
                && controller.recovery_round_requires_current_process_stop(&round)
                && self.stopped_for.as_ref().map(|(stopped, _)| stopped) != Some(&round) =>
            {
                self.observe_prepare(db, controller, round).await;
            }
            Some(
                start @ RecoveryAnnouncement {
                    phase: RecoverPhase::Start { epoch },
                    ..
                },
            ) if start.round.id.generation > self.applied_gen
                && controller.recovery_round_contains_current_process(&start.round) =>
            {
                self.observe_start(db, controller, start, epoch).await;
            }
            Some(
                release @ RecoveryAnnouncement {
                    phase: RecoverPhase::Release { epoch },
                    ..
                },
            ) if controller.recovery_round_contains_current_process(&release.round) => {
                let _ = self.observe_release(db, controller, release, epoch).await;
            }
            Some(RecoveryAnnouncement {
                phase: RecoverPhase::ReleaseCommitted { .. },
                ..
            }) => {
                self.hold_for_protocol_error(
                    db,
                    controller,
                    "committed recovery release appeared in the mutable intent slot".into(),
                )
                .await;
            }
            Some(active)
                if self
                    .stopped_for
                    .as_ref()
                    .is_some_and(|(stopped, _)| *stopped == active.round)
                    || self
                        .restored_for
                        .as_ref()
                        .is_some_and(|(start, _)| start.round == active.round) =>
            {
                // The exact driver is still making progress. Evidence-only participants remain
                // stopped through Start, and restored owners may legitimately await Release
                // longer than the local orphan timer. Only disappearance or supersession makes
                // either state orphaned.
                controller.set_recovering(true);
                db.set_source_gate(true);
            }
            _ => self.observe_orphans(db, controller).await,
        }
    }

    async fn hold_for_protocol_error(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        error: String,
    ) {
        controller.set_recovering(true);
        db.set_source_gate(true);
        if self.last_protocol_error.as_deref() != Some(error.as_str()) {
            tracing::error!(%error, "invalid coordinated-recovery control state");
            self.last_protocol_error = Some(error);
            if !self.request_fresh_fault(db, controller).await {
                self.fault_audit_unknown = true;
            }
        }
    }

    async fn observe_prepare(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        round: RecoveryRound,
    ) {
        let gen = round.id.generation;
        if db.terminal_pipeline_halt.load(Ordering::Acquire) && !round.has_terminal_fault() {
            controller.set_recovering(true);
            db.set_source_gate(true);
            tracing::error!(
                gen,
                "permanently halted pipeline refuses recovery Prepare and remains Faulted"
            );
            return;
        }
        if let Err(error) = replicate_recovery_gen(controller, gen).await {
            controller.set_recovering(true);
            db.set_source_gate(true);
            tracing::error!(
                gen,
                %error,
                "could not durably adopt recovery generation; withholding stopped acknowledgement"
            );
            return;
        }
        controller.set_recovering(true);
        db.set_source_gate(true);
        if !stop_for_recovery(db).await {
            tracing::error!(
                gen,
                "recovery prepare could not quiesce this node; withholding stopped acknowledgement"
            );
            return;
        }
        if let Err(error) = announce_stopped_bounded(controller, &round).await {
            tracing::error!(gen, %error, "could not acknowledge recovery Prepare");
            return;
        }
        self.restored_for = None;
        self.stopped_for = Some((round, tokio::time::Instant::now()));
        tracing::warn!(gen, "stopped for recovery round; awaiting target");
    }

    async fn observe_start(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        start: RecoveryAnnouncement,
        epoch: u64,
    ) {
        let round = start.round.clone();
        let gen = round.id.generation;
        if round.has_terminal_fault()
            || db.terminal_pipeline_halt.load(Ordering::Acquire)
            || db.durable_terminal_recovery_fence.load(Ordering::Acquire)
        {
            controller.set_recovering(true);
            db.set_source_gate(true);
            tracing::error!(
                gen,
                "permanently halted pipeline refuses coordinated recovery Start"
            );
            return;
        }
        if let Err(error) = replicate_recovery_gen(controller, gen).await {
            controller.set_recovering(true);
            db.set_source_gate(true);
            tracing::error!(
                gen,
                %error,
                "could not durably adopt recovery generation; refusing restore"
            );
            return;
        }
        controller.set_recovering(true);
        db.set_source_gate(true);
        if self.stopped_for.as_ref().map(|(stopped, _)| stopped) == Some(&round) {
            // Peers may have kept sending between our stop and theirs; now that the
            // round has quiesced, drop those stragglers too.
            db.purge_shuffle_receiver_buffers();
        } else if !stop_and_purge(db).await {
            // Missed the prepare (joined late / slow poll) — stop now.
            tracing::error!(
                gen,
                "recovery start could not quiesce this node; refusing restore"
            );
            return;
        }
        match self.restore_and_ack(db, controller, &start, epoch).await {
            RestoreAckOutcome::Restored => {
                tracing::warn!(
                    target_epoch = epoch,
                    gen,
                    "node restored to recovery epoch; source gate awaits Release"
                );
            }
            RestoreAckOutcome::Retry => {}
            RestoreAckOutcome::Terminal => {
                tracing::error!(gen, "permanent restore failure disabled automatic retry");
            }
        }
        // Failure: state untouched, `Start` still visible → retried next tick.
    }

    async fn observe_release(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: RecoveryAnnouncement,
        epoch: u64,
    ) -> bool {
        if release.round.has_terminal_fault()
            || db.terminal_pipeline_halt.load(Ordering::Acquire)
            || db.durable_terminal_recovery_fence.load(Ordering::Acquire)
        {
            controller.set_recovering(true);
            db.set_source_gate(true);
            tracing::error!(
                gen = release.round.id.generation,
                "permanently halted pipeline refuses coordinated recovery Release"
            );
            return false;
        }
        let exact_start = self.restored_for.as_ref().is_some_and(|(start, _)| {
            start.round == release.round && start.phase == (RecoverPhase::Start { epoch })
        });
        if !exact_start
            && release.phase == (RecoverPhase::Release { epoch })
            && release.round.id.driver == controller.instance_id()
            && release.round.id.generation <= self.applied_gen
        {
            let _ = tokio::time::timeout(
                DECISION_IO_TIMEOUT,
                controller.retire_committed_recover_release_hint(&release.round, epoch),
            )
            .await;
            return true;
        }
        if exact_start {
            let consumed = self
                .release_after_readiness_quorum(db, controller, &release, epoch)
                .await;
            if consumed {
                tracing::warn!(
                    target_epoch = epoch,
                    gen = release.round.id.generation,
                    "recovery Release consumed; source gate opened"
                );
            }
            return consumed;
        } else if release.round.id.generation > self.applied_gen {
            // A process that missed Start must never interpret a persisted Release as an
            // instruction to rewind alone. Fence it and request a fresh complete round.
            self.applied_gen = release.round.id.generation;
            controller.set_recovering(true);
            db.set_source_gate(true);
            tracing::error!(
                gen = release.round.id.generation,
                "Release observed without restoring its exact Start; holding intake"
            );
            if !self.request_fresh_fault(db, controller).await {
                self.fault_audit_unknown = true;
            }
            return true;
        }
        false
    }

    /// An ownerless worker consumes only a leader-fenced terminal that names this exact boot in
    /// its stopped roster and was committed after every data owner prepared while fenced. The
    /// terminal retains stable-node fault evidence after authority tombstones the covered slot.
    /// It never joins the frozen restore quorum, acknowledges recovery, or opens its
    /// assignment-closed source gate. It may rebuild a gated local runtime to settle connector
    /// evidence before consuming the terminal.
    async fn observe_nonparticipant_release(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &RecoveryAnnouncement,
    ) -> bool {
        let RecoverPhase::ReleaseCommitted { epoch } = release.phase else {
            return false;
        };
        controller.set_recovering(true);
        db.set_source_gate(true);
        let deadline = tokio::time::Instant::now() + RELEASE_PROTOCOL_TIMEOUT;
        let assignment_restorable =
            match recovery_round_assignment_is_restorable(db, controller, &release.round, deadline)
                .await
            {
                Ok(restorable) => restorable,
                Err(error) => {
                    tracing::warn!(
                        gen = release.round.id.generation,
                        %error,
                        "could not audit ownerless recovery assignment"
                    );
                    false
                }
            };
        if release.round.contains_owner(controller.instance_id())
            || !controller.recovery_round_requires_current_process_stop(&release.round)
            || !controller.process_lease_is_live()
            || !assignment_restorable
            || !matches!(
                tokio::time::timeout_at(
                    deadline,
                    controller.observe_committed_recover_release(&release.round, epoch),
                )
                .await,
                Ok(Ok(Some(active))) if active == *release
            )
        {
            return false;
        }

        // A non-owner is outside the restore quorum, but it can still hold a connector generation
        // stopped for this round. Quiesce it before accepting the terminal that tombstoned its
        // covered fault.
        if !stop_for_recovery(db).await {
            return false;
        }

        db.set_shuffle_recovery_gen(release.round.id.generation);
        db.purge_shuffle_receiver_buffers();
        let assignment_deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
        let authority_revision = match install_recovery_start_assignment(
            db,
            controller,
            &release.round,
            assignment_deadline,
        )
        .await
        {
            Ok(revision) => revision,
            Err(error) => {
                tracing::error!(
                    gen = release.round.id.generation,
                    %error,
                    "ownerless recovery assignment repair failed"
                );
                return false;
            }
        };
        // This process is outside the frozen owner quorum. By the time it observes the committed
        // terminal, released owners may already have committed a newer checkpoint. Rebuilding
        // the passive, assignment-closed runtime at the old release epoch would then violate the
        // no-rewind guard and request successor rounds forever. Recover the current durable head;
        // any later vnode acquisition still stages its exact committed handoff before intake opens.
        if let Err(error) = start_pipeline(db, None).await {
            tracing::error!(
                gen = release.round.id.generation,
                release_epoch = epoch,
                terminal = error.requires_pipeline_halt(),
                %error,
                "ownerless process could not rebuild its assignment-closed connector runtime"
            );
            return false;
        }
        let assignment_still_exact = matches!(
            recovery_round_assignment_is_restorable(
                db,
                controller,
                &release.round,
                tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
            )
            .await,
            Ok(true)
        );
        if !assignment_still_exact
            || !controller.process_lease_is_live()
            || db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
            || !round_assignment_is_current(db, controller, &release.round)
            || !matches!(
                controller
                    .observe_committed_recover_release(&release.round, epoch)
                    .await,
                Ok(Some(active)) if active == *release
            )
        {
            return false;
        }

        let deadline = tokio::time::Instant::now() + RELEASE_PROTOCOL_TIMEOUT;
        let mut backoff = Duration::from_millis(25);
        let release_guard = loop {
            match tokio::time::timeout_at(deadline, controller.begin_recovery_release(release))
                .await
            {
                Ok(Ok(Some(guard))) => break guard,
                Ok(Ok(None)) if controller.process_lease_is_live() => {
                    self.hold_fault_fence(db, controller);
                    self.applied_gen = self.applied_gen.max(release.round.id.generation);
                    self.stopped_for = None;
                    if !self.ensure_fault_request(db, controller).await {
                        self.fault_audit_unknown = true;
                    }
                    return false;
                }
                Ok(Err(RecoveryControlError::Uncertain(_)))
                    if tokio::time::Instant::now() < deadline =>
                {
                    release_retry_delay(deadline, &mut backoff).await;
                }
                Ok(
                    Ok(None)
                    | Err(RecoveryControlError::Superseded(_) | RecoveryControlError::Uncertain(_)),
                )
                | Err(_) => return false,
                Ok(Err(RecoveryControlError::Conflict(error))) => {
                    tracing::error!(
                        gen = release.round.id.generation,
                        %error,
                        "ownerless recovery authority state conflicts with the committed Release"
                    );
                    if !self.request_fresh_fault(db, controller).await {
                        self.fault_audit_unknown = true;
                    }
                    return false;
                }
            }
        };
        if !self.clear_authorized_pending_request(db) {
            tracing::warn!(
                gen = release.round.id.generation,
                "a replacement recovery request prevented ownerless Release consumption"
            );
            drop(release_guard);
            return false;
        }
        self.record_released_faults(&release.round);
        self.applied_gen = release.round.id.generation;
        self.stopped_for = None;
        controller.set_recovering(false);
        db.set_source_gate(true);
        drop(release_guard);
        db.release_coordinated_recovery_lifecycle();
        true
    }

    async fn observe_orphans(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
        if let Some((round, at)) = self.stopped_for.as_ref() {
            if at.elapsed() > ORPHAN_STOP_TIMEOUT {
                // Resuming here would emit at the pre-round generation from an unknown cut:
                // rewound peers discard those frames, un-rewound ones double-count them.
                if let Some(m) = db.engine_metrics.lock().clone() {
                    m.coordinated_recovery_failures_total.inc();
                }
                tracing::error!(
                    gen = round.id.generation,
                    "recovery round orphaned (no Start); holding intake"
                );
                self.stopped_for = None;
                if !self.request_fresh_fault(db, controller).await {
                    self.fault_audit_unknown = true;
                }
            }
        }
        let orphaned_start = self
            .restored_for
            .as_ref()
            .filter(|(_, at)| at.elapsed() > ORPHAN_STOP_TIMEOUT)
            .map(|(start, _)| start.clone());
        if let Some(start) = orphaned_start {
            let gen = start.round.id.generation;
            tracing::error!(
                gen,
                "recovery Start orphaned without Release; holding intake"
            );
            if start.round.id.driver == controller.instance_id() {
                let _ = controller.clear_recover(&start.round).await;
            }
            self.restored_for = None;
            hold_intake_and_request_retry(db, controller, gen, false).await;
        }
    }

    /// Fault reports not yet handled. Faults become handled only when an exact terminal `Release`
    /// covers their node and authority-assigned sequence; retaining that sequence also suppresses
    /// a stale post-clear observation without hiding a distinct later report.
    async fn pending_faults(
        &mut self,
        controller: &ClusterController,
    ) -> Result<Vec<RecoveryFault>, String> {
        let reported = self.reported_faults(controller).await?;
        Ok(self.unhandled_faults(&reported))
    }

    async fn reported_faults(
        &self,
        controller: &ClusterController,
    ) -> Result<Vec<RecoveryFault>, String> {
        Ok(self.fault_inventory(controller).await?.faults().to_vec())
    }

    async fn fault_inventory(
        &self,
        controller: &ClusterController,
    ) -> Result<RecoveryFaultInventory, String> {
        tokio::time::timeout(
            DECISION_IO_TIMEOUT,
            controller.read_recovery_fault_inventory(),
        )
        .await
        .map_err(|_| "cluster recovery fault inventory read timed out".to_string())?
    }

    fn unhandled_faults(&self, reported: &[RecoveryFault]) -> Vec<RecoveryFault> {
        reported
            .iter()
            .filter(|fault| self.handled_faults.get(&fault.reporter) != Some(&fault.sequence))
            .copied()
            .collect()
    }

    async fn pending_local_fault(
        &self,
        controller: &ClusterController,
    ) -> Result<Option<RecoveryFault>, String> {
        Ok(tokio::time::timeout(
            DECISION_IO_TIMEOUT,
            controller.read_local_recovery_fault_control(),
        )
        .await
        .map_err(|_| "local recovery fault report read timed out".to_string())?
        .map_err(|error| error.to_string())?
        .filter(|fault| {
            self.handled_faults.get(&controller.instance_id()) != Some(&fault.sequence)
        }))
    }

    async fn pending_local_fault_if_queued(
        &self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) -> Result<Option<RecoveryFault>, String> {
        // Before cluster startup installs durable checkpoint authority, a healthy process has no
        // local fault to audit. Keep that pre-wiring path side-effect free, while still requiring
        // authority (and failing closed) for an in-memory request. Once authority is installed we
        // always inspect it so a terminal fault survives process restart even though the new
        // process has no queued request ordinal.
        if db.pending_recovery_fault.load(Ordering::Acquire) == 0
            && controller.checkpoint_authority().is_err()
        {
            return Ok(None);
        }
        self.pending_local_fault(controller).await
    }

    fn latch_durable_terminal_fault(
        &self,
        db: &LaminarDB,
        controller: &ClusterController,
        reporter: Option<NodeId>,
    ) {
        db.latch_durable_terminal_recovery_fence();
        controller.set_recovering(true);
        let reason = reporter.map_or_else(
            || "cluster recovery is fenced by a durable terminal pipeline fault".to_string(),
            |reporter| {
                format!(
                    "cluster recovery is fenced by a durable terminal pipeline fault from {reporter}"
                )
            },
        );
        db.last_fault.lock().get_or_insert(reason);
    }

    async fn hold_for_unknown_fault_audit(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) {
        self.fault_audit_unknown = true;
        self.hold_fault_fence(db, controller);
        if self.request_fresh_fault(db, controller).await {
            self.fault_audit_unknown = false;
        }
    }

    /// Fence each data participant as soon as any durable fault is visible. Leadership controls
    /// who drives the round; ownerless workers retain their already-closed data plane without
    /// joining its quorum.
    fn hold_for_pending_fault(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
        pending: &[RecoveryFault],
    ) {
        if pending.is_empty() {
            // The leader monitor audits its local slot before the global inventory on every poll. An
            // empty local view must not unlatch a suspension held by a remote fault that the
            // global read will return immediately afterward. Successful Release clears both
            // lifecycle gates; only that boundary starts a new held-fault period.
            if !controller.is_recovering() && !db.coordinated_recovery_in_progress() {
                self.fault_fenced = false;
            }
            return;
        }
        self.hold_fault_fence(db, controller);
    }

    fn hold_for_visible_or_queued_fault(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
        pending: &[RecoveryFault],
    ) {
        if pending.is_empty() && db.pending_recovery_fault.load(Ordering::Acquire) != 0 {
            self.hold_fault_fence(db, controller);
        } else {
            self.hold_for_pending_fault(db, controller, pending);
        }
    }

    fn hold_fault_fence(&mut self, db: &LaminarDB, controller: &ClusterController) {
        // Assignment publication holds this same claim while replacing the owner map and
        // recovery flag. Read ownership under it so an ownerless observation cannot race a
        // topology-only adoption and overwrite its `recovering=true` publication afterward.
        let _lifecycle_claim = db.startup_attempt.lock();
        db.set_source_gate(true);
        db.coordinated_recovery_fenced
            .store(true, Ordering::Release);
        let owns_vnodes = db.vnode_registry.lock().as_ref().is_none_or(|registry| {
            registry
                .snapshot()
                .iter()
                .any(|owner| owner.0 == controller.instance_id().0)
        });
        if !owns_vnodes {
            // An ownerless worker has no data-plane authority to rewind or acknowledge. Keep its
            // source gate and public lifecycle closed; a round may still need its checkpoint
            // evidence even though it remains outside the restore quorum.
            controller.set_recovering(false);
            self.fault_fenced = false;
            return;
        }
        controller.set_recovering(true);
        if !self.fault_fenced {
            db.suspend_shuffle_assignment_fence();
            self.fault_fenced = true;
        }
    }

    /// Leader: stop the world, fix the target against the quiesced decision store, then restart
    /// the world. A protocol failure retains the intake fence, bumps
    /// `coordinated_recovery_failures_total`, and leaves its fault pending for a complete retry.
    /// Expected leadership transfer retains the same safety state without fabricating a failure or
    /// a second fault; the original unhandled fault drives the successor's fresh, driver-bound
    /// generation.
    async fn drive_round(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        fault_revision: u64,
        faults: Vec<RecoveryFault>,
        required_prepare_fence: Option<&CheckpointAssignmentFence>,
    ) {
        // Precheck readability before stopping anything — a transient decision-store error
        // must defer the round (faults stay pending), not flap a stop-the-world cycle.
        if read_committed_target_bounded(db).await.is_err() {
            tracing::warn!("coordinated recovery: decision store unreadable; deferring round");
            return;
        }
        let assignment_deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
        let assignment_fence = match current_recovery_assignment_fence(
            db,
            controller,
            assignment_deadline,
        )
        .await
        {
            Ok(Some(fence)) => fence,
            Ok(None) => {
                tracing::warn!(
                    "coordinated recovery: no current owner-complete assignment certificate; deferring round"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "coordinated recovery: owner-complete assignment audit failed; deferring round"
                );
                return;
            }
        };
        if required_prepare_fence.is_some_and(|required| required != &assignment_fence) {
            tracing::warn!(
                "recovery assignment no longer matches the exact stopped-Prepare supersession fence; deferring round"
            );
            return;
        }
        // Every participant replicates an accepted generation before quiescing. Allocating from
        // that replicated maximum remains monotonic when the previous driver's slot disappears
        // and does not make wall-clock synchronisation part of recovery correctness.
        let max_generation = match read_recovery_gen(controller).await {
            Ok(generation) => generation,
            Err(error) => {
                tracing::error!(%error, "could not read the replicated recovery generation");
                return;
            }
        };
        let Some(gen_id) = max_generation.checked_add(1) else {
            tracing::error!("recovery generation space exhausted");
            return;
        };
        let Some(leader_proof) = controller.capture_leader_proof() else {
            tracing::warn!(
                gen = gen_id,
                "coordinated recovery has no live durable leader proof"
            );
            return;
        };
        let evidence_candidates = faults
            .iter()
            .filter(|fault| !assignment_fence.contains(fault.reporter.0))
            .map(|fault| fault.reporter.0)
            .collect::<Vec<_>>();
        let evidence_participants = if evidence_candidates.is_empty() {
            Vec::new()
        } else {
            match controller
                .available_recovery_participant_incarnations(&evidence_candidates)
                .await
            {
                Ok(participants) => participants,
                Err(error) => {
                    tracing::error!(
                        gen = gen_id,
                        %error,
                        "could not freeze non-owner recovery evidence roster"
                    );
                    return;
                }
            }
        };
        let round = match RecoveryRound::new(
            gen_id,
            leader_proof,
            assignment_fence,
            evidence_participants,
            fault_revision,
            faults,
        ) {
            Ok(round) => round,
            Err(error) => {
                tracing::error!(gen = gen_id, %error, "could not construct recovery round");
                return;
            }
        };
        if let Err(error) = replicate_recovery_gen(controller, gen_id).await {
            tracing::error!(gen = gen_id, %error, "could not publish recovery generation");
            return;
        }

        let prepare_fence = current_recovery_assignment_fence(
            db,
            controller,
            tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
        )
        .await;
        let prepare_fence_matches = matches!(
            prepare_fence.as_ref(),
            Ok(Some(fence))
                if fence == &round.assignment_fence
                    && required_prepare_fence.is_none_or(|required| required == fence)
        );
        if !prepare_fence_matches {
            match prepare_fence {
                Err(error) => tracing::warn!(
                    gen = gen_id,
                    %error,
                    "recovery assignment audit failed before Prepare; deferring round"
                ),
                _ => tracing::warn!(
                    gen = gen_id,
                    "recovery assignment changed before Prepare; deferring round"
                ),
            }
            return;
        }

        controller.set_recovering(true);
        db.set_source_gate(true);
        if let Err(error) = announce_recover_prepare_bounded(controller, &round).await {
            tracing::warn!(gen = gen_id, %error, "could not publish recovery Prepare");
            return;
        }
        tracing::warn!(gen = gen_id, "leader announced recovery prepare");
        if !stop_for_recovery(db).await {
            tracing::error!(
                gen = gen_id,
                "leader could not quiesce after publishing recovery Prepare; retaining the fence"
            );
            retain_stopped_prepare_and_request_retry(db, controller, &round).await;
            return;
        }
        if let Err(error) = announce_stopped_bounded(controller, &round).await {
            tracing::error!(gen = gen_id, %error, "could not acknowledge recovery Prepare");
            retain_stopped_prepare_and_request_retry(db, controller, &round).await;
            return;
        }
        let stopped_reports = match wait_stopped_quorum(
            controller,
            &round,
            recovery_stopped_quorum_timeout(db),
        )
        .await
        {
            StoppedQuorum::Reached(reports) => reports,
            StoppedQuorum::LeadershipLost => {
                retain_recovery_control_after_leadership_loss(db, controller, &round);
                return;
            }
            StoppedQuorum::Superseded | StoppedQuorum::Conflicted => {
                tracing::warn!(
                    gen = gen_id,
                    "recovery Prepare superseded; yielding old driver"
                );
                retain_stopped_prepare_and_request_retry(db, controller, &round).await;
                return;
            }
            StoppedQuorum::ParticipantsChanged | StoppedQuorum::TimedOut => {
                // A straggler can still publish an ambiguous decision/state write, so selecting a
                // recovery cut without every round participant stopped would violate the exact-cut
                // premise and could resurrect a live timeline.
                tracing::error!(
                    gen = gen_id,
                    "stop quorum timed out; retaining the stopped recovery fence"
                );
                retain_stopped_prepare_and_request_retry(db, controller, &round).await;
                return;
            }
        };
        clear_stopped_assignment_quarantine(controller, &round, &stopped_reports);

        if !continue_if_driver_owns_prepare(db, controller, &round, "before target selection").await
        {
            return;
        }
        if round.has_terminal_fault() {
            self.latch_durable_terminal_fault(
                db,
                controller,
                round
                    .faults
                    .iter()
                    .copied()
                    .find(|fault| fault.is_terminal())
                    .map(|fault| fault.reporter),
            );
            tracing::error!(
                gen = gen_id,
                "terminal fault retained after the frozen stopped quorum; no Start will be published"
            );
            return;
        }

        let artifact_cleanup = {
            let deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
            let mut coordinator = db.coordinator.lock().await;
            match coordinator.as_mut() {
                Some(coordinator) => coordinator
                    .settle_cluster_checkpoint_artifacts_until(&round.leader_proof, deadline)
                    .await
                    .map_err(|error| error.to_string()),
                None => Err("checkpoint coordinator is not configured".to_string()),
            }
        };
        match artifact_cleanup {
            Ok(true) => tracing::warn!(
                gen = gen_id,
                "cleaned unresolved checkpoint artifacts after stop quorum"
            ),
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    gen = gen_id,
                    %error,
                    "could not reconcile unresolved checkpoint artifacts"
                );
                // Every frozen participant is already stopped. Artifact settlement can have
                // terminalized the assignment despite this ambiguous error, so retain Prepare as
                // the topology-adoption witness and retry without restarting predecessor V.
                handle_failed_recovery_boundary(
                    db,
                    controller,
                    &round,
                    "checkpoint artifact cleanup",
                )
                .await;
                return;
            }
        }
        if !continue_if_driver_owns_prepare(
            db,
            controller,
            &round,
            "during checkpoint artifact cleanup",
        )
        .await
        {
            return;
        }

        // This is the recovery/rotation linearization boundary. A live drain finalizer owns the
        // same mutex from its authority decision through terminal snapshot materialization. Hold
        // it from the post-cleanup drain inspection through Start publication so recovery observes
        // every earlier terminal, while every later terminal is ordered after this exact Start.
        let selection_deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
        let selection_guard =
            match tokio::time::timeout_at(selection_deadline, db.assignment_adoption_lock.lock())
                .await
            {
                Ok(guard) => guard,
                Err(_) => {
                    tracing::error!(
                        gen = gen_id,
                        "recovery target selection timed out waiting for assignment serialization"
                    );
                    handle_failed_recovery_boundary(
                        db,
                        controller,
                        &round,
                        "assignment serialization timeout",
                    )
                    .await;
                    return;
                }
            };
        let ownership = driver_owns_prepare(db, controller, &round).await;
        if ownership != PrepareOwnership::Owned {
            drop(selection_guard);
            let _ = handle_prepare_ownership(
                db,
                controller,
                &round,
                ownership,
                "at the assignment linearization boundary",
            )
            .await;
            return;
        }

        match crate::rebalance::settle_stopped_source_drain_after_recovery_quorum(
            db,
            controller,
            &round.assignment_fence,
            crate::rebalance::RebalanceConfig::default(),
        )
        .await
        {
            Ok(Some(version)) => {
                drop(selection_guard);
                tracing::warn!(
                    gen = gen_id,
                    version,
                    "settled an in-progress source drain after exact recovery stop quorum"
                );
                // The terminal changes the assignment generation. This Prepare can no longer
                // authorize Start, but it must remain durable until every stopped predecessor
                // owner publishes V+1 topology and every target owner reports exact adoption.
                hold_intake_and_request_retry(db, controller, gen_id, false).await;
                return;
            }
            Ok(None) => {}
            Err(error) => {
                drop(selection_guard);
                tracing::error!(
                    gen = gen_id,
                    %error,
                    "could not settle source drain after recovery stop quorum"
                );
                // Do not restart the stopped predecessor or publish Start while drain authority is
                // ambiguous. Keep this exact Prepare available for follower topology adoption and
                // let the monitor retry settlement.
                handle_failed_recovery_boundary(db, controller, &round, "source-drain settlement")
                    .await;
                return;
            }
        }

        // The world is stopped, so the greatest Commit and its immutable global index form the
        // exact recovery cut. No committed checkpoint means a fresh start.
        let selected = match read_committed_target_bounded(db).await {
            Ok(target) => target,
            Err(e) => {
                drop(selection_guard);
                tracing::error!(error = %e, gen = gen_id, "target read failed after stop quorum");
                handle_failed_recovery_boundary(db, controller, &round, "recovery target read")
                    .await;
                return;
            }
        };
        let target = selected
            .as_ref()
            .map_or(GENESIS, |(outcome, _)| outcome.epoch);
        let ownership = driver_owns_prepare(db, controller, &round).await;
        if ownership != PrepareOwnership::Owned {
            drop(selection_guard);
            let _ = handle_prepare_ownership(
                db,
                controller,
                &round,
                ownership,
                "during target selection",
            )
            .await;
            return;
        }
        db.purge_shuffle_receiver_buffers();
        if let Err(error) = controller.announce_recover_start(&round, target).await {
            drop(selection_guard);
            tracing::error!(gen = gen_id, %error, "could not transition recovery to Start");
            // The write may have landed even though its acknowledgement failed. Keep every
            // participant stopped and let the monitor re-observe the exact durable phase.
            handle_failed_recovery_boundary(db, controller, &round, "recovery Start publication")
                .await;
            return;
        }
        drop(selection_guard);
        let start = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch: target },
        };
        tracing::warn!(
            target_epoch = target,
            checkpoint_id = selected
                .as_ref()
                .map_or(GENESIS, |(outcome, _)| outcome.checkpoint_id),
            participants = selected
                .as_ref()
                .map_or(0, |(_, index)| index.participants.len()),
            gen = gen_id,
            "leader announced recovery start"
        );

        // Retry self-restore inline so the round and its cleanup stay in the leader's control.
        let mut restored = false;
        for attempt in 1..=SELF_RESTORE_ATTEMPTS {
            match self.restore_and_ack(db, controller, &start, target).await {
                RestoreAckOutcome::Restored => {
                    restored = true;
                    break;
                }
                RestoreAckOutcome::Terminal => {
                    tracing::error!(
                        gen = gen_id,
                        "leader self-restore halted permanently; retaining the cluster fence"
                    );
                    return;
                }
                RestoreAckOutcome::Retry => {}
            }
            tracing::warn!(
                gen = gen_id,
                attempt,
                "leader self-restore failed; retrying"
            );
        }
        let quorum = if restored {
            wait_restored_quorum(controller, &start, RESTORE_QUORUM_TIMEOUT).await
        } else {
            tracing::error!(gen = gen_id, "leader self-restore failed; abandoning round");
            RecoveryQuorum::TimedOut
        };
        if quorum != RecoveryQuorum::Reached {
            tracing::error!(
                gen = gen_id,
                ?quorum,
                "exact recovery restore quorum was not reached; Start will not be released"
            );
            let _ = controller.clear_recover(&round).await;
            hold_intake_and_request_retry(db, controller, gen_id, false).await;
            return;
        }
        if !round_is_releasable(db, controller, &round).await {
            tracing::error!(
                gen = gen_id,
                "recovery certificate changed after restore quorum; refusing Release"
            );
            let _ = controller.clear_recover(&round).await;
            hold_intake_and_request_retry(db, controller, gen_id, true).await;
            return;
        }
        if let Err(error) = controller.announce_recover_release(&round, target).await {
            tracing::error!(gen = gen_id, %error, "could not publish recovery Release");
            let _ = controller.clear_recover(&round).await;
            hold_intake_and_request_retry(db, controller, gen_id, true).await;
            return;
        }
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: target },
        };
        if !self
            .release_after_readiness_quorum(db, controller, &release, target)
            .await
        {
            return;
        }
        let pending = match self.pending_faults(controller).await {
            Ok(pending) => pending,
            Err(error) => {
                tracing::error!(%error, "could not audit faults after recovery Release quorum");
                self.hold_fault_fence(db, controller);
                return;
            }
        };
        self.hold_for_pending_fault(db, controller, &pending);
        if !pending.is_empty() {
            return;
        }
        let participants = round.assignment_fence.participants.len();
        tracing::warn!(
            gen = gen_id,
            participants,
            "coordinated recovery complete; Release retained"
        );
    }

    /// Restart this node to `target`, then ack the generation. The fault report remains live until
    /// the exact restore quorum and assignment fence both complete, so a crash or supersession
    /// can only cause a safe redundant round rather than losing the retry trigger.
    async fn restore_and_ack(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        start: &RecoveryAnnouncement,
        target: u64,
    ) -> RestoreAckOutcome {
        let gen_id = start.round.id.generation;
        // A durable Start can only be published after the driver has validated the complete,
        // exact stopped quorum. Quarantine is controller-local, so a follower that retained an
        // entry from an earlier leadership term must consume that same proof before the ordinary
        // assignment-authority audit below. The conditional clear cannot rehabilitate an
        // unbound entry or a different boot incarnation.
        clear_started_assignment_quarantine(controller, &start.round);
        // Sources come up paused: the restart re-reads and re-shuffles the replay window, and a
        // node that restarts first would shuffle into peers whose receivers haven't rebound.
        db.set_source_gate(true);
        // Fence delayed pre-rewind frames before the restored compute loop can drain the receiver.
        // Advancing this after `start_pipeline` leaves a window in which old data can be folded
        // onto restored state. The monotonic generation remains safe if startup fails and retries.
        db.set_shuffle_recovery_gen(gen_id);
        let assignment_deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
        let authority_revision = match install_recovery_start_assignment(
            db,
            controller,
            &start.round,
            assignment_deadline,
        )
        .await
        {
            Ok(revision) => revision,
            Err(error) => {
                tracing::error!(gen = gen_id, %error, "recovery Start assignment repair failed");
                return RestoreAckOutcome::Retry;
            }
        };
        if let Err(error) = start_pipeline(db, Some(target)).await {
            if error.requires_pipeline_halt() {
                return RestoreAckOutcome::Terminal;
            }
            // Starting from the selected cut failed. Keep intake fenced while `Start` remains
            // visible so the next monitor tick can retry without exposing pre-recovery state.
            // The lifecycle deadline may have expired while the owned start thread continued;
            // forget the old stop marker so the retry first quiesces any late successful start.
            self.stopped_for = None;
            return RestoreAckOutcome::Retry;
        }
        let assignment_still_exact = matches!(
            recovery_round_assignment_is_restorable(
                db,
                controller,
                &start.round,
                tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
            )
            .await,
            Ok(true)
        );
        if !assignment_still_exact
            || db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
            || !local_release_round_is_current(db, controller, &start.round)
        {
            tracing::error!(
                gen = gen_id,
                "recovery assignment changed while rebuilding the data plane; withholding restore acknowledgement"
            );
            return RestoreAckOutcome::Retry;
        }
        match controller.announce_recovered(start).await {
            Ok(()) => {
                if let Some(m) = db.engine_metrics.lock().clone() {
                    m.coordinated_recoveries_total.inc();
                }
                self.applied_gen = gen_id;
                self.stopped_for = None;
                self.restored_for = Some((start.clone(), tokio::time::Instant::now()));
                RestoreAckOutcome::Restored
            }
            Err(error) => {
                tracing::error!(gen = gen_id, %error, "could not acknowledge recovery restore");
                RestoreAckOutcome::Retry
            }
        }
    }

    fn record_released_faults(&mut self, round: &RecoveryRound) {
        for fault in &round.faults {
            self.handled_faults.insert(fault.reporter, fault.sequence);
        }
    }

    /// Clear only the exact request already published by this monitor. Callers hold an authorized
    /// committed-Release guard; a concurrently installed replacement remains latched and fenced.
    fn clear_authorized_pending_request(&mut self, db: &LaminarDB) -> bool {
        if db.terminal_pipeline_halt.load(Ordering::Acquire)
            || db.durable_terminal_recovery_fence.load(Ordering::Acquire)
        {
            self.published_local_request = None;
            return false;
        }
        let pending = db.pending_recovery_fault.load(Ordering::Acquire);
        if pending == 0 {
            self.published_local_request = None;
            return true;
        }
        if self.published_local_request != Some((pending, RecoveryFaultDisposition::Recoverable)) {
            return false;
        }
        if db
            .pending_recovery_fault
            .compare_exchange(pending, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.published_local_request = None;
            true
        } else {
            self.published_local_request = None;
            false
        }
    }

    fn defer_release_retry(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        gen_id: u64,
        quorum: bool,
    ) {
        hold_intake_for_retry(db, controller, gen_id, quorum);
        self.fault_audit_unknown = true;
    }

    async fn await_pending_release_commit(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        pending: &RecoveryAnnouncement,
        release_deadline: tokio::time::Instant,
        authority_revision: u64,
    ) -> Option<RecoveryAnnouncement> {
        let mut ready = false;
        let mut backoff = Duration::from_millis(25);
        let committed = loop {
            if tokio::time::Instant::now() >= release_deadline {
                tracing::error!(
                    gen = pending.round.id.generation,
                    "committed recovery Release was not observable before its deadline"
                );
                return None;
            }
            if db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
                || !local_release_round_is_current(db, controller, &pending.round)
            {
                self.defer_release_retry(db, controller, pending.round.id.generation, false);
                return None;
            }

            match exact_committed_release(controller, pending, release_deadline).await {
                Ok(Some(terminal)) => break terminal,
                Ok(None) => {}
                Err(RecoveryControlError::Uncertain(_)) => {
                    release_retry_delay(release_deadline, &mut backoff).await;
                    continue;
                }
                Err(RecoveryControlError::Conflict(error)) => {
                    tracing::error!(
                        gen = pending.round.id.generation,
                        %error,
                        "committed recovery Release conflicts with the pending intent"
                    );
                    self.defer_release_retry(db, controller, pending.round.id.generation, false);
                    return None;
                }
                Err(RecoveryControlError::Superseded(_)) => return None,
            }

            // Compact readiness is only a promise for the exact mutable Release intent. A
            // successor Prepare can replace that intent after this participant has published
            // Ready (for example, when another participant reports a fresh fault). Keep
            // auditing the intent after Ready so this waiter promptly drops the assignment and
            // execution guards instead of holding them until the full Release deadline and
            // preventing the successor round from stopping this process. The immutable
            // committed terminal is checked first above, so retiring or replacing the mutable
            // hint after a successful commit cannot hide an authoritative Release.
            match tokio::time::timeout_at(release_deadline, controller.observe_recover_control())
                .await
            {
                Ok(Ok(Some(active))) if active == *pending => {}
                Ok(Err(RecoveryControlError::Uncertain(_))) => {
                    release_retry_delay(release_deadline, &mut backoff).await;
                    continue;
                }
                Ok(Err(RecoveryControlError::Conflict(error))) => {
                    tracing::error!(
                        gen = pending.round.id.generation,
                        %error,
                        "recovery Release intent is invalid"
                    );
                    self.defer_release_retry(db, controller, pending.round.id.generation, false);
                    return None;
                }
                Ok(Err(RecoveryControlError::Superseded(_)) | Ok(Some(_) | None)) | Err(_) => {
                    return None
                }
            }

            if !ready {
                match tokio::time::timeout_at(
                    release_deadline,
                    controller.read_local_fault_report_control(),
                )
                .await
                {
                    Ok(Ok(observed))
                        if observed == pending.round.fault_sequence(controller.instance_id()) => {}
                    Ok(Ok(_) | Err(RecoveryControlError::Superseded(_))) | Err(_) => {
                        return None;
                    }
                    Ok(Err(RecoveryControlError::Uncertain(_))) => {
                        release_retry_delay(release_deadline, &mut backoff).await;
                        continue;
                    }
                    Ok(Err(RecoveryControlError::Conflict(error))) => {
                        tracing::error!(
                            gen = pending.round.id.generation,
                            %error,
                            "local recovery fault conflicts with the pending Release"
                        );
                        self.defer_release_retry(
                            db,
                            controller,
                            pending.round.id.generation,
                            false,
                        );
                        return None;
                    }
                }
                match tokio::time::timeout_at(
                    release_deadline,
                    controller.announce_release_ready(pending),
                )
                .await
                {
                    Ok(Ok(())) => ready = true,
                    Ok(Err(RecoveryControlError::Uncertain(_))) => {
                        release_retry_delay(release_deadline, &mut backoff).await;
                        continue;
                    }
                    Ok(Err(RecoveryControlError::Conflict(error))) => {
                        tracing::error!(
                            gen = pending.round.id.generation,
                            %error,
                            "recovery Release readiness conflicts with durable state"
                        );
                        self.defer_release_retry(
                            db,
                            controller,
                            pending.round.id.generation,
                            false,
                        );
                        return None;
                    }
                    Ok(Err(RecoveryControlError::Superseded(_))) | Err(_) => return None,
                }
            }

            if pending.round.id.driver == controller.instance_id() {
                if !controller.is_leader()
                    || controller.capture_leader_proof().as_ref()
                        != Some(&pending.round.leader_proof)
                {
                    return None;
                }
                match tokio::time::timeout_at(
                    release_deadline,
                    controller.try_commit_recover_release(pending),
                )
                .await
                {
                    Ok(
                        Ok(ReleaseCommitStatus::Pending { .. })
                        | Err(RecoveryControlError::Uncertain(_)),
                    ) => {}
                    Ok(Ok(ReleaseCommitStatus::Committed { terminal })) => break terminal,
                    Ok(Err(RecoveryControlError::Conflict(error))) => {
                        tracing::error!(
                            gen = pending.round.id.generation,
                            %error,
                            "recovery Release commit conflicts with durable state"
                        );
                        self.defer_release_retry(
                            db,
                            controller,
                            pending.round.id.generation,
                            false,
                        );
                        return None;
                    }
                    Ok(Err(RecoveryControlError::Superseded(_))) | Err(_) => return None,
                }
            }
            release_retry_delay(release_deadline, &mut backoff).await;
        };
        Some(committed)
    }

    /// Prepare for the exact release intent while intake is closed, then consume only the
    /// leader-fenced committed terminal. Compact readiness is the durable prepare promise; the
    /// driver alone reads the exact roster and fault set before committing. Assignment/execution
    /// fences are retained across the whole prepare/commit barrier.
    async fn release_after_readiness_quorum(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &RecoveryAnnouncement,
        target: u64,
    ) -> bool {
        let pending = match release.phase {
            RecoverPhase::Release { epoch } if epoch == target => Some(release.clone()),
            RecoverPhase::ReleaseCommitted { epoch } if epoch == target => None,
            _ => return false,
        };
        controller.set_recovering(true);
        db.set_source_gate(true);
        let release_deadline = tokio::time::Instant::now() + RELEASE_PROTOCOL_TIMEOUT;
        let authority_revision = db.assignment_authority_revision.load(Ordering::Acquire);
        let assignment_restorable = match recovery_round_assignment_is_restorable(
            db,
            controller,
            &release.round,
            release_deadline,
        )
        .await
        {
            Ok(restorable) => restorable,
            Err(error) => {
                tracing::error!(
                    gen = release.round.id.generation,
                    %error,
                    "could not audit recovery Release assignment"
                );
                false
            }
        };
        if !assignment_restorable
            || db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
        {
            return false;
        }
        if !db.complete_shuffle_recovery(release.round.id.generation) {
            tracing::error!(
                gen = release.round.id.generation,
                "shuffle loss cutoff did not match the released recovery generation"
            );
            return false;
        }
        match crate::rebalance::settle_source_drain_before_recovery_release(
            db,
            controller,
            &release.round.assignment_fence,
            release_deadline,
        )
        .await
        {
            Ok(Some(version)) => {
                tracing::info!(
                    version,
                    gen = release.round.id.generation,
                    "reapplied assignment drain terminal before recovery Release"
                );
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(
                    gen = release.round.id.generation,
                    %error,
                    "recovery Release could not settle assignment drain terminal"
                );
                return false;
            }
        }
        #[cfg(test)]
        if let Some(ReleaseDrainSettlementHook { observed, resume }) =
            self.release_drain_settlement_hook.take()
        {
            let _ = observed.send(());
            let _ = resume.await;
        }
        let settled_authority_revision = db.assignment_authority_revision.load(Ordering::Acquire);
        if settled_authority_revision != authority_revision {
            tracing::debug!(
                gen = release.round.id.generation,
                authority_revision,
                settled_authority_revision,
                "local assignment authority changed during drain settlement; retrying the same recovery Release"
            );
            return false;
        }
        if let Some(transition) = controller.checkpoint_drain_transition() {
            tracing::error!(
                gen = release.round.id.generation,
                drain = ?transition.id(),
                "recovery Release cannot reuse a process-local source drain cut"
            );
            return false;
        }
        let activation = db
            .activate_assignment_authority(
                &release.round.assignment_fence,
                None,
                authority_revision,
                release_deadline,
            )
            .await;
        match activation {
            Ok(activation)
                if activation.installed
                    && !activation.intake_open
                    && local_release_round_is_current(db, controller, &release.round) => {}
            Ok(_) => return false,
            Err(error) => {
                tracing::error!(
                    gen = release.round.id.generation,
                    %error,
                    "could not install release-ready assignment authority"
                );
                return false;
            }
        }

        let Ok(_adoption) =
            tokio::time::timeout_at(release_deadline, db.assignment_adoption_lock.lock()).await
        else {
            tracing::error!(
                gen = release.round.id.generation,
                "release readiness could not retain assignment authority"
            );
            return false;
        };
        let Ok(_execution) = tokio::time::timeout_at(
            release_deadline,
            Arc::clone(&db.rotation_execution_fence).write_owned(),
        )
        .await
        else {
            tracing::error!(
                gen = release.round.id.generation,
                "release readiness could not retain the execution fence"
            );
            return false;
        };
        if db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
            || !local_release_round_is_current(db, controller, &release.round)
        {
            self.defer_release_retry(db, controller, release.round.id.generation, false);
            return false;
        }
        if let Err(error) = db.install_shuffle_assignment_fence(&release.round.assignment_fence) {
            tracing::error!(
                gen = release.round.id.generation,
                %error,
                "release readiness lost its installed shuffle authority"
            );
            return false;
        }
        let committed = if let Some(pending) = pending.as_ref() {
            let Some(committed) = self
                .await_pending_release_commit(
                    db,
                    controller,
                    pending,
                    release_deadline,
                    authority_revision,
                )
                .await
            else {
                return false;
            };
            committed
        } else {
            release.clone()
        };

        if !committed_release_matches_intent(&release.round, target, &committed)
            || db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
            || !local_release_round_is_current(db, controller, &release.round)
        {
            self.defer_release_retry(db, controller, release.round.id.generation, false);
            return false;
        }
        let mut terminal_backoff = Duration::from_millis(25);
        loop {
            match exact_committed_release(controller, release, release_deadline).await {
                Ok(Some(active)) if active == committed => break,
                Err(RecoveryControlError::Uncertain(_))
                    if tokio::time::Instant::now() < release_deadline =>
                {
                    release_retry_delay(release_deadline, &mut terminal_backoff).await;
                }
                Ok(Some(_) | None)
                | Err(
                    RecoveryControlError::Conflict(_)
                    | RecoveryControlError::Superseded(_)
                    | RecoveryControlError::Uncertain(_),
                ) => {
                    self.defer_release_retry(db, controller, release.round.id.generation, false);
                    return false;
                }
            }
        }
        let mut clear_backoff = Duration::from_millis(25);
        let release_guard = loop {
            match tokio::time::timeout_at(
                release_deadline,
                controller.begin_recovery_release(&committed),
            )
            .await
            {
                Ok(Ok(Some(guard))) => break guard,
                Ok(Ok(None)) if controller.process_lease_is_live() => {
                    tracing::error!(
                        gen = release.round.id.generation,
                        "committed recovery Release cannot settle this live process; requesting a successor round"
                    );
                    self.applied_gen = self.applied_gen.max(release.round.id.generation);
                    self.restored_for = None;
                    if !self.ensure_fault_request(db, controller).await {
                        self.fault_audit_unknown = true;
                    }
                    return false;
                }
                Ok(Ok(None) | Err(RecoveryControlError::Superseded(_))) => {
                    return false;
                }
                Ok(Err(RecoveryControlError::Uncertain(_))) => {
                    if tokio::time::Instant::now() >= release_deadline {
                        return false;
                    }
                    release_retry_delay(release_deadline, &mut clear_backoff).await;
                }
                Ok(Err(RecoveryControlError::Conflict(error))) => {
                    tracing::error!(
                        gen = release.round.id.generation,
                        %error,
                        "local recovery fault state conflicts with the committed Release"
                    );
                    self.defer_release_retry(db, controller, release.round.id.generation, false);
                    return false;
                }
                Err(_) => return false,
            }
        };
        let release_still_authorized = db.assignment_authority_revision.load(Ordering::Acquire)
            == authority_revision
            && !db.terminal_pipeline_halt.load(Ordering::Acquire)
            && !db.durable_terminal_recovery_fence.load(Ordering::Acquire)
            && controller.is_recovering()
            && db.cluster_intake_fenced()
            && controller.process_lease_is_live()
            && controller
                .checkpoint_assignment_fence(release.round.assignment_fence.assignment_version)
                .as_ref()
                == Some(&release.round.assignment_fence)
            && controller.checkpoint_drain_transition().is_none();
        if !release_still_authorized {
            drop(release_guard);
            self.defer_release_retry(db, controller, release.round.id.generation, false);
            return false;
        }
        if !self.clear_authorized_pending_request(db) {
            tracing::warn!(
                gen = release.round.id.generation,
                "a replacement recovery request prevented Release consumption"
            );
            drop(release_guard);
            return false;
        }
        controller.set_recovering(false);
        db.set_source_gate(false);
        if db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
            || db.terminal_pipeline_halt.load(Ordering::Acquire)
            || db.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || controller.is_recovering()
            || db.cluster_intake_fenced()
            || !controller.process_lease_is_live()
            || controller
                .checkpoint_assignment_fence(release.round.assignment_fence.assignment_version)
                .as_ref()
                != Some(&release.round.assignment_fence)
            || controller.checkpoint_drain_transition().is_some()
        {
            controller.set_recovering(true);
            db.set_source_gate(true);
            drop(release_guard);
            self.defer_release_retry(db, controller, release.round.id.generation, false);
            return false;
        }
        self.record_released_faults(&release.round);
        drop(release_guard);
        if release.round.id.driver == controller.instance_id() {
            let _ = tokio::time::timeout(
                Duration::from_millis(250),
                controller.retire_committed_recover_release_hint(&release.round, target),
            )
            .await;
        }
        log_release_diagnostic(db, controller, release.round.id.generation, target);
        self.restored_for = None;
        db.release_coordinated_recovery_lifecycle();
        // Release is the synchronous boundary between continuously held fault periods. Reset
        // before returning so a replacement fault observed immediately afterward suspends the
        // newly installed authority instead of inheriting the old report's latch.
        self.fault_fenced = false;
        true
    }
}

/// Stop the pipeline and drop buffered shuffle input, on a dedicated thread (lifecycle
/// futures are `!Send`). `true` means the runtime and every owned decision writer are quiescent.
async fn stop_and_purge(db: &Arc<LaminarDB>) -> bool {
    let lifecycle_timeout = recovery_stop_lifecycle_timeout(db);
    run_lifecycle(db, lifecycle_timeout, |db| async move {
        db.stop_pipeline_for_coordinated_recovery().await?;
        // Pre-rewind shuffle slices are stale: their senders rewind and replay them, so
        // folding a buffered copy after the rewind double-counts.
        db.purge_shuffle_receiver_buffers();
        Ok(())
    })
    .await
}

/// Quiesce every local writer while public restart remains recovery-fenced.
async fn stop_for_recovery(db: &Arc<LaminarDB>) -> bool {
    db.fence_coordinated_recovery_lifecycle();
    stop_and_purge(db).await
}

async fn install_recovery_start_assignment(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    deadline: tokio::time::Instant,
) -> Result<u64, String> {
    if !controller.is_recovering() || !db.cluster_intake_fenced() {
        return Err("recovery Start assignment repair requires closed intake".into());
    }
    let revision = db.assignment_authority_revision.load(Ordering::Acquire);
    if !recovery_round_assignment_is_restorable(db, controller, round, deadline).await?
        || db.assignment_authority_revision.load(Ordering::Acquire) != revision
    {
        return Err("recovery Start assignment authority changed during audit".into());
    }
    let activation = db
        .activate_assignment_authority(&round.assignment_fence, None, revision, deadline)
        .await
        .map_err(|error| error.to_string())?;
    if !activation.installed
        || activation.intake_open
        || db.assignment_authority_revision.load(Ordering::Acquire) != revision
        || !round_assignment_is_current(db, controller, round)
    {
        return Err("recovery Start could not install its exact assignment authority".into());
    }
    Ok(revision)
}

/// Start the pipeline, rewinding to `target` when given while preserving a typed terminal error.
async fn start_pipeline(db: &Arc<LaminarDB>, target: Option<u64>) -> Result<(), crate::DbError> {
    if !coordinated_restart_assignment_ready(db).await {
        return Err(crate::DbError::Checkpoint(
            "coordinated recovery restart assignment is not ready".into(),
        ));
    }
    run_lifecycle_result(db, RECOVERY_START_LIFECYCLE_TIMEOUT, move |db| async move {
        db.set_recover_target_epoch(target);
        db.start_for_coordinated_recovery().await
    })
    .await
}

/// A coordinated restart must not publish or consume against unresolved assignment authority.
/// Ordinary process startup has its own assignment bootstrap path and does not call this helper.
async fn coordinated_restart_assignment_ready(db: &LaminarDB) -> bool {
    let Some(store) = db.assignment_snapshot_store.lock().clone() else {
        tracing::error!("coordinated recovery restart has no assignment snapshot store");
        return false;
    };
    match tokio::time::timeout(DECISION_IO_TIMEOUT, store.load()).await {
        Ok(Ok(Some(snapshot))) if !snapshot.draining => true,
        Ok(Ok(Some(snapshot))) => {
            tracing::error!(
                assignment_version = snapshot.version,
                "coordinated recovery restart blocked by unresolved assignment drain"
            );
            false
        }
        Ok(Ok(None)) => {
            tracing::error!("coordinated recovery restart has no durable assignment head");
            false
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "could not read assignment authority before recovery restart");
            false
        }
        Err(_) => {
            tracing::error!("assignment authority read timed out before recovery restart");
            false
        }
    }
}

struct LifecycleActiveGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for LifecycleActiveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Run a recovery lifecycle future on an independent owner thread. The stable DB runtime drives
/// it, so loss of the monitor's original caller runtime cannot cancel a stop/start round.
async fn run_lifecycle<F, Fut>(db: &Arc<LaminarDB>, timeout: Duration, f: F) -> bool
where
    F: FnOnce(Arc<LaminarDB>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), crate::DbError>>,
{
    match run_lifecycle_result(db, timeout, f).await {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(%error, "coordinated recovery lifecycle step failed");
            false
        }
    }
}

async fn run_lifecycle_result<F, Fut>(
    db: &Arc<LaminarDB>,
    timeout: Duration,
    f: F,
) -> Result<(), crate::DbError>
where
    F: FnOnce(Arc<LaminarDB>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), crate::DbError>>,
{
    let Some(deadline) = checked_recovery_deadline(timeout) else {
        return Err(crate::DbError::Pipeline(format!(
            "coordinated recovery lifecycle deadline {timeout:?} exceeds the platform clock range"
        )));
    };
    if db
        .coordinated_lifecycle_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(crate::DbError::InvalidOperation(
            "coordinated recovery lifecycle operation is still active; refusing overlap".into(),
        ));
    }

    let active = LifecycleActiveGuard(Arc::clone(&db.coordinated_lifecycle_active));
    let handle = match db.control_runtime.handle() {
        Ok(handle) => handle,
        Err(error) => return Err(error),
    };
    let db = Arc::clone(db);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let spawned = std::thread::Builder::new()
        .name("laminar-coord-recover".into())
        .spawn(move || {
            let _active = active;
            let res = handle.block_on(f(db));
            let _ = tx.send(res);
        });
    if let Err(e) = spawned {
        return Err(crate::DbError::Pipeline(format!(
            "coordinated recovery failed to spawn lifecycle thread: {e}"
        )));
    }
    match tokio::time::timeout_at(deadline, rx).await {
        Err(_) => Err(crate::DbError::Pipeline(format!(
            "coordinated recovery lifecycle step timed out after {timeout:?}; owner remains fenced"
        ))),
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(crate::DbError::Pipeline(
            "coordinated recovery lifecycle thread dropped".into(),
        )),
    }
}

/// Current owner-complete assignment certificate. Recovery cannot invent a quorum from a
/// transient membership view; it freezes this already-proven cut instead.
fn current_assignment_fence(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
) -> Option<laminar_core::checkpoint::CheckpointAssignmentFence> {
    let guard = db.vnode_registry.lock();
    let reg = guard.as_ref()?;
    let assignment = reg.versioned_snapshot();
    let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
    let available: FxHashSet<u64> = controller
        .checkpoint_instances()
        .into_iter()
        .filter(|node| !controller.is_unresponsive(*node))
        .map(|node| node.0)
        .collect();
    controller
        .checkpoint_assignment_fence(assignment.version())
        .filter(|fence| {
            fence.matches_owner_map(&owners)
                && fence
                    .participants
                    .iter()
                    .all(|participant| available.contains(&participant.node_id))
        })
}

/// Recover the exact current assignment cut after fault fencing has deliberately withdrawn the
/// active checkpoint certificate. This is a read-only recovery proof: it never republishes the
/// checkpoint fence, reopens intake, or grants shuffle authority.
///
/// The normal fast path retains the already-published certificate. The fallback is admitted only
/// while coordinated recovery owns the closed data plane, and reconstructs the same certificate
/// from the authority-audited durable head, exact current process incarnations, and durable
/// assignment-adoption reports. `vnode_state_ready` is deliberately not required: a faulted owner
/// must be recoverable after publishing a false readiness report.
async fn current_recovery_assignment_fence(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    deadline: tokio::time::Instant,
) -> Result<Option<laminar_core::checkpoint::CheckpointAssignmentFence>, String> {
    if tokio::time::Instant::now() >= deadline
        || !controller.process_lease_is_live()
        || !controller.is_recovering()
        || !db.cluster_intake_fenced()
        || !db.coordinated_recovery_fenced.load(Ordering::Acquire)
    {
        return Ok(None);
    }
    if let Some(fence) = current_assignment_fence(db, controller) {
        return Ok(Some(fence));
    }

    let authority_revision = db.assignment_authority_revision.load(Ordering::Acquire);
    let registry = db
        .vnode_registry
        .lock()
        .clone()
        .ok_or_else(|| "recovery assignment audit has no vnode registry".to_string())?;
    let local_assignment = registry.versioned_snapshot();
    let store = db
        .assignment_snapshot_store
        .lock()
        .clone()
        .ok_or_else(|| "recovery assignment audit has no assignment store".to_string())?;

    let head = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery assignment head read timed out".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "recovery assignment audit has no durable head".to_string())?;
    tokio::time::timeout_at(
        deadline,
        crate::rebalance::audit_assignment_snapshot_authority(&store, Some(controller), &head),
    )
    .await
    .map_err(|_| "recovery assignment authority audit timed out".to_string())??;
    let expected_drain_transition = head.drain_transition.clone();
    let committed = if head.draining {
        let Some(expected) = expected_drain_transition.as_ref() else {
            return Err("recovery assignment observed a draining head without a transition".into());
        };
        if controller
            .checkpoint_drain_transition()
            .as_ref()
            .is_some_and(|local| local != expected)
        {
            return Ok(None);
        }
        tokio::time::timeout_at(
            deadline,
            crate::rebalance::audit_drain_predecessor(&store, &registry, &head, Some(controller)),
        )
        .await
        .map_err(|_| "recovery drain predecessor audit timed out".to_string())??
    } else {
        if expected_drain_transition.is_some() || controller.checkpoint_drain_transition().is_some()
        {
            return Ok(None);
        }
        head.clone()
    };
    let fence = committed
        .assignment_fence()
        .map_err(|error| format!("recovery assignment fence is invalid: {error}"))?;
    let local_owners: Vec<u64> = local_assignment
        .owners()
        .iter()
        .map(|owner| owner.0)
        .collect();
    let local_id = controller.instance_id().0;
    if fence.assignment_version != local_assignment.version()
        || fence.vnode_count != registry.vnode_count()
        || !fence.matches_owner_map(&local_owners)
        || fence
            .participant_incarnation(local_id)
            .is_some_and(|incarnation| incarnation != controller.recovery_incarnation())
        || (fence.participant_incarnation(local_id).is_none() && local_owners.contains(&local_id))
    {
        return Ok(None);
    }

    let participant_ids = fence.participant_ids();
    let incarnations = tokio::time::timeout_at(
        deadline,
        controller.recovery_participant_incarnations(&participant_ids),
    )
    .await
    .map_err(|_| "recovery assignment incarnation audit timed out".to_string())??;
    if incarnations != fence.participants
        || !recovery_fence_participants_present(controller, &fence)
    {
        return Ok(None);
    }
    let adopted = tokio::time::timeout_at(deadline, controller.read_adopted_assignments())
        .await
        .map_err(|_| "recovery assignment adoption audit timed out".to_string())??
        .into_iter()
        .map(|(node, adoption)| (node.0, adoption))
        .collect::<FxHashMap<_, _>>();
    if fence.participants.iter().any(|participant| {
        adopted.get(&participant.node_id).is_none_or(|adoption| {
            adoption.participant != *participant || !adoption.matches_fence(&fence)
        })
    }) {
        return Ok(None);
    }

    let confirmed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery assignment head recheck timed out".to_string())?
        .map_err(|error| error.to_string())?;
    if confirmed.as_ref() != Some(&head) {
        return Ok(None);
    }
    tokio::time::timeout_at(
        deadline,
        crate::rebalance::audit_assignment_snapshot_authority(&store, Some(controller), &head),
    )
    .await
    .map_err(|_| "recovery assignment authority recheck timed out".to_string())??;
    let confirmed_committed = if head.draining {
        tokio::time::timeout_at(
            deadline,
            crate::rebalance::audit_drain_predecessor(&store, &registry, &head, Some(controller)),
        )
        .await
        .map_err(|_| "recovery drain predecessor recheck timed out".to_string())??
    } else {
        head.clone()
    };
    let confirmed_incarnations = tokio::time::timeout_at(
        deadline,
        controller.recovery_participant_incarnations(&participant_ids),
    )
    .await
    .map_err(|_| "recovery assignment incarnation recheck timed out".to_string())??;
    let confirmed_assignment = registry.versioned_snapshot();
    let confirmed_owners: Vec<u64> = confirmed_assignment
        .owners()
        .iter()
        .map(|owner| owner.0)
        .collect();
    let published = controller.checkpoint_assignment_watch().borrow().clone();
    if confirmed_committed != committed
        || controller
            .checkpoint_drain_transition()
            .as_ref()
            .is_some_and(|local| expected_drain_transition.as_ref() != Some(local))
        || confirmed_incarnations != fence.participants
        || confirmed_assignment.version() != fence.assignment_version
        || confirmed_assignment.owners() != local_assignment.owners()
        || !fence.matches_owner_map(&confirmed_owners)
        || !recovery_fence_participants_present(controller, &fence)
        || published
            .as_ref()
            .is_some_and(|published| published != &fence)
        || db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
        || !controller.process_lease_is_live()
        || !controller.is_recovering()
        || !db.cluster_intake_fenced()
        || !db.coordinated_recovery_fenced.load(Ordering::Acquire)
    {
        return Ok(None);
    }
    Ok(Some(fence))
}

fn recovery_fence_participants_present(
    controller: &ClusterController,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) -> bool {
    // A capture-quorum miss quarantines a process from ordinary checkpoint/placement authority,
    // but it cannot make that exact live boot unrecoverable. This fallback is reachable only with
    // recovery + intake fenced and separately audits durable incarnations and adoption reports;
    // the Recovery Prepare/stop/readiness quorums must then re-prove every participant before
    // Release. Keep requiring current checkpoint-capable membership here, but deliberately do not
    // apply the ordinary `unresponsive` filter.
    let available: FxHashSet<u64> = controller
        .checkpoint_instances()
        .into_iter()
        .map(|node| node.0)
        .collect();
    fence
        .participants
        .iter()
        .all(|participant| available.contains(&participant.node_id))
}

fn clear_stopped_assignment_quarantine(
    controller: &ClusterController,
    round: &RecoveryRound,
    reports: &[RecoveryStoppedReport],
) {
    let exact_assignment_publishers = reports
        .iter()
        .map(RecoveryStoppedReport::publisher)
        .filter(|publisher| {
            round
                .assignment_fence
                .participant_incarnation(publisher.node_id)
                == Some(publisher.boot_incarnation)
        })
        .collect::<Vec<_>>();
    controller.note_recovery_responsive(&exact_assignment_publishers);
}

fn clear_started_assignment_quarantine(controller: &ClusterController, round: &RecoveryRound) {
    controller.note_recovery_responsive(&round.assignment_fence.participants);
}

/// Whether the local registry and locally published owner-complete certificate are still the
/// exact cut frozen into this round. A merely newer converged assignment cannot release this
/// round: it needs a new stop/restore proof with its own roster.
fn round_assignment_is_current(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> bool {
    current_assignment_fence(db, controller).as_ref() == Some(&round.assignment_fence)
}

fn local_assignment_matches_recovery_round(
    db: &LaminarDB,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> bool {
    if !controller.process_lease_is_live() {
        return false;
    }
    let Some(registry) = db.vnode_registry.lock().clone() else {
        return false;
    };
    let assignment = registry.versioned_snapshot();
    let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
    let fence = &round.assignment_fence;
    let local_id = controller.instance_id().0;
    if !fence.is_canonical()
        || fence.assignment_version != assignment.version()
        || fence.vnode_count != registry.vnode_count()
        || !fence.matches_owner_map(&owners)
        || fence
            .participant_incarnation(local_id)
            .is_some_and(|incarnation| incarnation != controller.recovery_incarnation())
        || (fence.participant_incarnation(local_id).is_none() && owners.contains(&local_id))
    {
        return false;
    }
    let published = controller.checkpoint_assignment_watch();
    if published
        .borrow()
        .as_ref()
        .is_some_and(|published| published != fence)
    {
        return false;
    }
    controller
        .checkpoint_drain_transition()
        .is_none_or(|transition| transition.predecessor == *fence || transition.target == *fence)
}

/// Admit a recovery control phase from durable assignment authority when a local safety fence is
/// intentionally suspended. This may stop or rebuild a gated data plane; it never authorizes
/// Release without reinstalling the exact local certificate.
async fn recovery_round_assignment_is_restorable(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    deadline: tokio::time::Instant,
) -> Result<bool, String> {
    if !db.cluster_intake_fenced()
        || !local_assignment_matches_recovery_round(db, controller, round)
    {
        return Ok(false);
    }
    let authority_revision = db.assignment_authority_revision.load(Ordering::Acquire);
    let store = db
        .assignment_snapshot_store
        .lock()
        .clone()
        .ok_or_else(|| "recovery assignment admission has no assignment store".to_string())?;
    let registry = db
        .vnode_registry
        .lock()
        .clone()
        .ok_or_else(|| "recovery assignment admission has no vnode registry".to_string())?;
    let snapshot = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery assignment admission head read timed out".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "recovery assignment admission has no durable head".to_string())?;
    let draining_transition = snapshot.drain_transition.clone();
    let assignment_matches = if snapshot.draining {
        let Some(transition) = draining_transition.as_ref() else {
            return Err("recovery assignment observed a draining head without a transition".into());
        };
        if transition.predecessor != round.assignment_fence
            || controller
                .checkpoint_drain_transition()
                .as_ref()
                .is_some_and(|local| local != transition)
        {
            false
        } else {
            tokio::time::timeout_at(
                deadline,
                crate::rebalance::audit_drain_predecessor(
                    &store,
                    &registry,
                    &snapshot,
                    Some(controller),
                ),
            )
            .await
            .map_err(|_| "recovery drain predecessor admission timed out".to_string())??
            .assignment_fence()
            .map_err(|error| error.to_string())?
                == round.assignment_fence
        }
    } else {
        snapshot
            .assignment_fence()
            .map_err(|error| error.to_string())?
            == round.assignment_fence
    };
    if !assignment_matches {
        return Ok(false);
    }
    tokio::time::timeout_at(
        deadline,
        crate::rebalance::audit_assignment_snapshot_authority(&store, Some(controller), &snapshot),
    )
    .await
    .map_err(|_| "recovery assignment authority audit timed out".to_string())??;
    let durable_transition =
        tokio::time::timeout_at(deadline, store.load_drain_transition(snapshot.version))
            .await
            .map_err(|_| "recovery assignment transition read timed out".to_string())?
            .map_err(|error| error.to_string())?;
    let confirmed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery assignment admission head recheck timed out".to_string())?
        .map_err(|error| error.to_string())?;
    if confirmed.as_ref() != Some(&snapshot) {
        return Ok(false);
    }
    tokio::time::timeout_at(
        deadline,
        crate::rebalance::audit_assignment_snapshot_authority(&store, Some(controller), &snapshot),
    )
    .await
    .map_err(|_| "recovery assignment authority recheck timed out".to_string())??;
    if snapshot.draining {
        let confirmed_predecessor = tokio::time::timeout_at(
            deadline,
            crate::rebalance::audit_drain_predecessor(
                &store,
                &registry,
                &snapshot,
                Some(controller),
            ),
        )
        .await
        .map_err(|_| "recovery drain predecessor admission recheck timed out".to_string())??;
        if confirmed_predecessor
            .assignment_fence()
            .map_err(|error| error.to_string())?
            != round.assignment_fence
        {
            return Ok(false);
        }
    }
    let confirmed_transition =
        tokio::time::timeout_at(deadline, store.load_drain_transition(snapshot.version))
            .await
            .map_err(|_| "recovery assignment transition recheck timed out".to_string())?
            .map_err(|error| error.to_string())?;
    if confirmed_transition != durable_transition {
        return Ok(false);
    }
    let local_transition = controller.checkpoint_drain_transition();
    let transition_matches = if snapshot.draining {
        local_transition
            .as_ref()
            .is_none_or(|local| Some(local) == draining_transition.as_ref())
            && durable_transition.as_ref() == draining_transition.as_ref()
    } else {
        local_transition
            .as_ref()
            .is_none_or(|local| durable_transition.as_ref() == Some(local))
    };
    Ok(transition_matches
        && db.assignment_authority_revision.load(Ordering::Acquire) == authority_revision
        && db.cluster_intake_fenced()
        && local_assignment_matches_recovery_round(db, controller, round))
}

/// Both the restorable assignment cut and every assignment-owner process must still match the
/// frozen round.
async fn round_is_releasable(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> bool {
    matches!(
        recovery_round_assignment_is_restorable(
            db,
            controller,
            round,
            tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
        )
        .await,
        Ok(true)
    ) && matches!(
        controller.recovery_incarnations_match(round).await,
        Ok(true)
    )
}

fn local_release_round_is_current(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> bool {
    controller.process_lease_is_live()
        && controller.recovery_round_contains_current_process(round)
        && round_assignment_is_current(db, controller, round)
}

async fn exact_committed_release(
    controller: &ClusterController,
    intent: &RecoveryAnnouncement,
    deadline: tokio::time::Instant,
) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
    let epoch = match intent.phase {
        RecoverPhase::Release { epoch } | RecoverPhase::ReleaseCommitted { epoch } => epoch,
        RecoverPhase::Prepare | RecoverPhase::Start { .. } => {
            return Err(RecoveryControlError::Conflict(
                "exact release observation requires a Release intent or terminal".into(),
            ));
        }
    };
    tokio::time::timeout_at(
        deadline,
        controller.observe_committed_recover_release(&intent.round, epoch),
    )
    .await
    .map_err(|_| {
        RecoveryControlError::Uncertain(
            "committed recovery Release read exceeded the release deadline".into(),
        )
    })?
}

fn committed_release_matches_intent(
    round: &RecoveryRound,
    epoch: u64,
    committed: &RecoveryAnnouncement,
) -> bool {
    committed.round == *round && committed.phase == (RecoverPhase::ReleaseCommitted { epoch })
}

async fn release_retry_delay(deadline: tokio::time::Instant, backoff: &mut Duration) {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return;
    }
    tokio::time::sleep_until((now + *backoff).min(deadline)).await;
    *backoff = backoff.saturating_mul(2).min(Duration::from_millis(250));
}

/// Fail closed and make the hold live: resuming without the whole round restored, or against a
/// stale assignment, either drops this node's frames at rewound peers or double-counts them. A
/// fresh fault forces a new stop/restore generation; otherwise a fully restored round whose
/// assignment never settled could retire its announcement and leave every source gated forever.
async fn hold_intake_and_request_retry(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    gen_id: u64,
    quorum: bool,
) {
    hold_intake_for_retry(db, controller, gen_id, quorum);
    if let Err(error) = request_fresh_local_fault(
        controller,
        &db.pending_recovery_fault,
        local_fault_disposition(db),
    )
    .await
    {
        tracing::error!(%error, "could not persist recovery retry request");
    }
}

fn hold_intake_for_retry(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    gen_id: u64,
    quorum: bool,
) {
    controller.set_recovering(true);
    db.set_source_gate(true);
    if let Some(m) = db.engine_metrics.lock().clone() {
        m.coordinated_recovery_failures_total.inc();
    }
    tracing::error!(
        gen = gen_id,
        restore_quorum = quorum,
        "holding intake shut and requesting a fresh recovery round"
    );
}

/// Per-node snapshot at gate release; a cross-node diff at the same `gen` shows whether every node
/// resumed against the same, settled assignment.
fn log_release_diagnostic(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    gen_id: u64,
    target: u64,
) {
    let (owned, version) = {
        let guard = db.vnode_registry.lock();
        match guard.as_ref() {
            Some(reg) => {
                let self_id = laminar_core::state::NodeId(controller.instance_id().0);
                (
                    laminar_core::state::owned_vnodes(reg, self_id).len(),
                    reg.assignment_version(),
                )
            }
            None => (0, 0),
        }
    };
    tracing::info!(
        gen = gen_id,
        target,
        owned_vnodes = owned,
        assignment_version = version,
        "coordinated recovery: releasing source gate"
    );
}

/// Greatest cluster Commit together with its exact immutable global recovery index.
/// `Ok(None)` means nothing ever committed; `Err` means authority is unreadable or inconsistent.
async fn read_committed_target(
    db: &LaminarDB,
) -> Result<Option<(CheckpointOutcome, CommittedCheckpointIndex)>, String> {
    // Bind the clone before awaiting — an if-let scrutinee would hold the lock guard across it.
    let controller = db
        .cluster_controller
        .lock()
        .clone()
        .ok_or_else(|| "cluster controller is not configured".to_owned())?;
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    let Some(head) = authority
        .highest_cluster_committed_outcome()
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let Some((outcome, committed)) = authority
        .cluster_outcome_with_committed_checkpoint(head.epoch)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err(format!(
            "selected cluster Commit epoch {} disappeared during exact lookup",
            head.epoch
        ));
    };
    if outcome != head {
        return Err(format!(
            "selected cluster Commit epoch {} changed during exact lookup",
            head.epoch
        ));
    }
    let committed = committed.ok_or_else(|| {
        format!(
            "selected cluster Commit epoch {} has no committed checkpoint index",
            outcome.epoch
        )
    })?;
    if outcome.scope != CheckpointScope::Cluster || committed.scope != CheckpointScope::Cluster {
        return Err(format!(
            "selected checkpoint epoch {} is not cluster-scoped",
            outcome.epoch
        ));
    }
    Ok(Some((outcome, committed)))
}

async fn read_committed_target_bounded(
    db: &LaminarDB,
) -> Result<Option<(CheckpointOutcome, CommittedCheckpointIndex)>, String> {
    tokio::time::timeout(DECISION_IO_TIMEOUT, read_committed_target(db))
        .await
        .map_err(|_| {
            format!("decision-store recovery read timed out after {DECISION_IO_TIMEOUT:?}")
        })?
}

async fn read_recovery_gen(controller: &ClusterController) -> Result<u64, String> {
    controller.max_recovery_generation().await
}

/// Monotonically replicate `gen_id` into this participant's own slot and read it back before the
/// node quiesces or restores. `ClusterKv::write` is intentionally transport-neutral and cannot
/// report a backend error, so verification is the acknowledgement boundary.
async fn replicate_recovery_gen(controller: &ClusterController, gen_id: u64) -> Result<(), String> {
    tokio::time::timeout(
        DECISION_IO_TIMEOUT,
        controller.adopt_recovery_generation(gen_id),
    )
    .await
    .map_err(|_| {
        format!("recovery generation {gen_id} replication timed out after {DECISION_IO_TIMEOUT:?}")
    })?
}

async fn announce_stopped_bounded(
    controller: &ClusterController,
    round: &RecoveryRound,
) -> Result<(), String> {
    tokio::time::timeout(DECISION_IO_TIMEOUT, controller.announce_stopped(round))
        .await
        .map_err(|_| {
            format!(
                "recovery stopped acknowledgement for generation {} timed out after {DECISION_IO_TIMEOUT:?}",
                round.id.generation
            )
        })?
}

async fn announce_recover_prepare_bounded(
    controller: &ClusterController,
    round: &RecoveryRound,
) -> Result<(), String> {
    tokio::time::timeout(
        DECISION_IO_TIMEOUT,
        controller.announce_recover_prepare(round),
    )
    .await
    .map_err(|_| {
        format!(
            "recovery Prepare publication for generation {} timed out after {DECISION_IO_TIMEOUT:?}",
            round.id.generation
        )
    })?
}

async fn driver_controls_prepare(controller: &ClusterController, round: &RecoveryRound) -> bool {
    if !controller.is_leader()
        || !matches!(
            controller.recovery_incarnations_match(round).await,
            Ok(true)
        )
    {
        return false;
    }
    matches!(
        controller.observe_recover_control().await,
        Ok(Some(RecoveryAnnouncement {
            round: active,
            phase: RecoverPhase::Prepare,
        })) if active == *round
    )
}

async fn current_stopped_roster_has_adopted_target(
    controller: &ClusterController,
    round: &RecoveryRound,
    target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    reported: &FxHashMap<u64, CheckpointAssignmentAdoption>,
    deadline: tokio::time::Instant,
) -> Result<bool, String> {
    let mut required = Vec::with_capacity(
        round.assignment_fence.participants.len() + round.evidence_participants.len(),
    );
    required.extend(round.assignment_fence.participants.iter().copied());
    required.extend(round.evidence_participants.iter().copied());
    required.sort_unstable_by_key(|participant| participant.node_id);
    if required.is_empty() {
        return Ok(true);
    }
    for participant in required {
        if !controller
            .verify_current_process_incarnation(participant, deadline)
            .await?
        {
            continue;
        }
        if reported.get(&participant.node_id).is_none_or(|adoption| {
            adoption.participant != participant || !adoption.matches_fence(target)
        }) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Return the only assignment fence a fresh generation may use to overwrite this stopped Prepare.
/// The control slot is never cleared between rounds. Binding the audit to the returned fence keeps
/// a concurrent assignment advance from turning a safe predecessor retry into an unsafe successor
/// overwrite. An unchanged predecessor can be retried immediately. Once an adjacent target is
/// durable, every still-current boot from the old stopped/evidence roster must first publish exact
/// target topology. Durable process authority, not soft discovery membership, decides whether an
/// old boot is gone. This keeps the predecessor Prepare available to lagging stopped non-owners as
/// well as target owners.
pub(crate) async fn recovery_prepare_supersession_fence_after_assignment_settlement(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> Result<Option<CheckpointAssignmentFence>, String> {
    let deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
    let store = db
        .assignment_snapshot_store
        .lock()
        .clone()
        .ok_or_else(|| "recovery Prepare retirement has no assignment store".to_string())?;
    let head = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery Prepare retirement head read timed out".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "recovery Prepare retirement has no assignment head".to_string())?;
    let predecessor_version = round.assignment_fence.assignment_version;
    let successor_version = predecessor_version.checked_add(1).unwrap_or(0);
    if !head.draining && head.version == predecessor_version {
        if head.assignment_fence().map_err(|error| error.to_string())? != round.assignment_fence {
            return Err(
                "recovery Prepare retirement predecessor no longer matches the frozen assignment"
                    .into(),
            );
        }
        return Ok(Some(round.assignment_fence.clone()));
    }
    if head.draining {
        let exact_transition = head.drain_transition.as_ref().is_some_and(|transition| {
            head.version == successor_version
                && transition.predecessor == round.assignment_fence
                && transition.target.assignment_version == head.version
        });
        if exact_transition {
            return Ok(Some(round.assignment_fence.clone()));
        }
        return Ok(None);
    }
    if head.version != successor_version {
        return Ok(None);
    }
    let audited = tokio::time::timeout_at(
        deadline,
        crate::rebalance::audit_assignment_snapshot_authority_outcome(
            &store,
            Some(controller),
            &head,
        ),
    )
    .await
    .map_err(|_| "recovery Prepare retirement authority audit timed out".to_string())??;
    if audited.predecessor() != Some(&round.assignment_fence) {
        return Ok(None);
    }
    let target = head.assignment_fence().map_err(|error| error.to_string())?;
    let target_handoff = if audited.is_recovery() {
        if !audited.recovery_pin_is_active() {
            return Ok(None);
        }
        let handoff = audited.handoff_checkpoint().cloned().ok_or_else(|| {
            "materialized recovery successor has no exact handoff checkpoint".to_string()
        })?;
        let authority = controller
            .checkpoint_authority()
            .map_err(|error| error.to_string())?;
        let pinned =
            tokio::time::timeout_at(deadline, authority.assignment_handoff_checkpoint(&target))
                .await
                .map_err(|_| "recovery Prepare retirement handoff-pin read timed out".to_string())?
                .map_err(|error| error.to_string())?;
        if pinned.as_ref() != Some(&handoff) {
            return Err(
                "materialized recovery successor no longer retains its exact active handoff checkpoint pin"
                    .into(),
            );
        }
        Some((authority, handoff))
    } else {
        let Some(terminal) = audited.into_terminal() else {
            return Ok(None);
        };
        if terminal.transition().predecessor != round.assignment_fence
            || terminal.transition().target.assignment_version != target.assignment_version
        {
            return Err(
                "materialized drain terminal does not bind the frozen recovery edge".into(),
            );
        }
        if terminal.is_abort() {
            if target.participants != round.assignment_fence.participants
                || target.assignment_digest != round.assignment_fence.assignment_digest
                || target.vnode_count != round.assignment_fence.vnode_count
                || target.partitioning_abi_version
                    != round.assignment_fence.partitioning_abi_version
            {
                return Err(
                    "materialized Abort rollback does not retain the frozen owner map".into(),
                );
            }
            None
        } else if terminal.is_commit() {
            if terminal.transition().target != target {
                return Err("materialized Commit does not bind its exact target fence".into());
            }
            let handoff = terminal
                .committed_handoff_checkpoint()
                .cloned()
                .ok_or_else(|| "materialized Commit has no exact handoff checkpoint".to_string())?;
            let authority = controller
                .checkpoint_authority()
                .map_err(|error| error.to_string())?;
            let pinned =
                tokio::time::timeout_at(deadline, authority.assignment_handoff_checkpoint(&target))
                    .await
                    .map_err(|_| {
                        "recovery Prepare retirement handoff-pin read timed out".to_string()
                    })?
                    .map_err(|error| error.to_string())?;
            if pinned.as_ref() != Some(&handoff) {
                return Err(
                    "materialized Commit no longer retains its exact active handoff checkpoint pin"
                        .into(),
                );
            }
            Some((authority, handoff))
        } else {
            return Err("materialized drain terminal has no supported verdict".into());
        }
    };
    let target_ids = target.participant_ids();
    let live_target_participants = tokio::time::timeout_at(
        deadline,
        controller.recovery_participant_incarnations(&target_ids),
    )
    .await
    .map_err(|_| "recovery Prepare retirement target-roster read timed out".to_string())?
    .map_err(|error| error.to_string())?;
    if live_target_participants != target.participants {
        return Err("materialized assignment target process roster is no longer exact".into());
    }
    let reports = tokio::time::timeout_at(deadline, controller.read_adopted_assignments())
        .await
        .map_err(|_| "recovery Prepare retirement adoption-report read timed out".to_string())??;
    let reported: FxHashMap<u64, _> = reports
        .into_iter()
        .map(|(node, adoption)| (node.0, adoption))
        .collect();
    let owner_complete = target.participants.iter().all(|participant| {
        reported.get(&participant.node_id).is_some_and(|adoption| {
            adoption.participant == *participant && adoption.matches_fence(&target)
        })
    });
    if !owner_complete {
        return Ok(None);
    }
    if !current_stopped_roster_has_adopted_target(controller, round, &target, &reported, deadline)
        .await?
    {
        return Ok(None);
    }
    let confirmed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery Prepare retirement head recheck timed out".to_string())?
        .map_err(|error| error.to_string())?;
    if confirmed.as_ref() != Some(&head) {
        return Ok(None);
    }
    let confirmed_target_participants = tokio::time::timeout_at(
        deadline,
        controller.recovery_participant_incarnations(&target_ids),
    )
    .await
    .map_err(|_| "recovery Prepare retirement target-roster recheck timed out".to_string())?
    .map_err(|error| error.to_string())?;
    if confirmed_target_participants != target.participants {
        return Err(
            "materialized assignment target process roster changed during retirement".into(),
        );
    }
    let confirmed_reports =
        tokio::time::timeout_at(deadline, controller.read_adopted_assignments())
            .await
            .map_err(|_| {
                "recovery Prepare retirement adoption-report recheck timed out".to_string()
            })??
            .into_iter()
            .map(|(node, adoption)| (node.0, adoption))
            .collect::<FxHashMap<_, _>>();
    if target.participants.iter().any(|participant| {
        confirmed_reports
            .get(&participant.node_id)
            .is_none_or(|adoption| {
                adoption.participant != *participant || !adoption.matches_fence(&target)
            })
    }) || !current_stopped_roster_has_adopted_target(
        controller,
        round,
        &target,
        &confirmed_reports,
        deadline,
    )
    .await?
    {
        return Ok(None);
    }
    if let Some((authority, handoff)) = target_handoff {
        let confirmed_pin =
            tokio::time::timeout_at(deadline, authority.assignment_handoff_checkpoint(&target))
                .await
                .map_err(|_| {
                    "recovery Prepare retirement handoff-pin recheck timed out".to_string()
                })?
                .map_err(|error| error.to_string())?;
        if confirmed_pin.as_ref() != Some(&handoff) {
            return Err("materialized target handoff pin changed during retirement".into());
        }
    }
    Ok(Some(target))
}

/// Yield a round after local stop has begun without erasing its durable quiescence witness. The
/// monitor either resumes it or replaces it with a newer Prepare in one durable slot write.
async fn retain_stopped_prepare_and_request_retry(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) {
    let gen_id = round.id.generation;
    tracing::warn!(
        gen = gen_id,
        "retaining stopped recovery Prepare as the topology-adoption witness"
    );
    hold_intake_and_request_retry(db, controller, gen_id, false).await;
}

/// A recovery round is bound to its driver and exact durable leader proof, so a successor leader
/// must allocate a fresh generation. The original fault remains durable and unhandled until an
/// exact Release; retaining the current mutable control (Prepare or an ambiguously published
/// Start) and intake fence is therefore already live and must not be counted as a failed recovery
/// or manufacture another local fault.
fn retain_recovery_control_after_leadership_loss(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) {
    controller.set_recovering(true);
    db.set_source_gate(true);
    tracing::warn!(
        gen = round.id.generation,
        old_driver = round.id.driver.0,
        "recovery driver lost leadership; retaining current control and fence for successor generation"
    );
}

async fn exact_round_driver_authority(
    controller: &ClusterController,
    round: &RecoveryRound,
    boundary: &str,
) -> RoundDriverAuthority {
    match tokio::time::timeout(
        DECISION_IO_TIMEOUT,
        controller.audit_recovery_driver_proof(round, boundary),
    )
    .await
    {
        Ok(Ok(())) => RoundDriverAuthority::Current,
        Ok(Err(RecoveryControlError::Superseded(_))) => RoundDriverAuthority::Superseded,
        Ok(Err(RecoveryControlError::Uncertain(_))) | Err(_) => RoundDriverAuthority::Uncertain,
        Ok(Err(RecoveryControlError::Conflict(_))) => RoundDriverAuthority::Conflicted,
    }
}

/// Classify a failed stopped-round boundary by the exact round proof, independent of whether its
/// current mutable phase is Prepare or Start. Only proven authority supersession is a clean driver
/// handoff; an intact or unreadable/conflicting proof preserves the existing fail-closed retry.
async fn handle_failed_recovery_boundary(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    boundary: &'static str,
) {
    match exact_round_driver_authority(controller, round, boundary).await {
        RoundDriverAuthority::Superseded => {
            retain_recovery_control_after_leadership_loss(db, controller, round);
        }
        RoundDriverAuthority::Current
        | RoundDriverAuthority::Uncertain
        | RoundDriverAuthority::Conflicted => {
            hold_intake_and_request_retry(db, controller, round.id.generation, false).await;
        }
    }
}

async fn driver_owns_prepare(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> PrepareOwnership {
    driver_owns_prepare_with_final_authority_barrier(db, controller, round, std::future::ready(()))
        .await
}

async fn driver_owns_prepare_with_final_authority_barrier(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    before_final_authority_audit: impl std::future::Future<Output = ()>,
) -> PrepareOwnership {
    let deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
    let audit = async {
        if !matches!(
            recovery_prepare_assignment_is_current(db, controller, round, deadline).await,
            Ok(true)
        ) {
            return false;
        }
        driver_controls_prepare(controller, round).await
    };
    let control_is_owned = matches!(tokio::time::timeout_at(deadline, audit).await, Ok(true));
    before_final_authority_audit.await;
    match exact_round_driver_authority(controller, round, "Prepare ownership classification").await
    {
        RoundDriverAuthority::Current if control_is_owned => PrepareOwnership::Owned,
        RoundDriverAuthority::Superseded => PrepareOwnership::LeadershipLost,
        RoundDriverAuthority::Current
        | RoundDriverAuthority::Uncertain
        | RoundDriverAuthority::Conflicted => PrepareOwnership::Invalid,
    }
}

async fn handle_prepare_ownership(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    ownership: PrepareOwnership,
    boundary: &'static str,
) -> bool {
    match ownership {
        PrepareOwnership::Owned => true,
        PrepareOwnership::LeadershipLost => {
            retain_recovery_control_after_leadership_loss(db, controller, round);
            false
        }
        PrepareOwnership::Invalid => {
            tracing::warn!(
                gen = round.id.generation,
                boundary,
                "recovery driver lost Prepare ownership; yielding"
            );
            retain_stopped_prepare_and_request_retry(db, controller, round).await;
            false
        }
    }
}

async fn continue_if_driver_owns_prepare(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    boundary: &'static str,
) -> bool {
    let ownership = driver_owns_prepare(db, controller, round).await;
    handle_prepare_ownership(db, controller, round, ownership, boundary).await
}

/// Audit assignment authority while the round is still in Prepare. Unlike Start/Release
/// admission, this boundary deliberately accepts the exact draining successor of the frozen
/// predecessor: the stopped quorum must first clean checkpoint artifacts before that drain may be
/// authority-sequenced to Abort.
async fn recovery_prepare_assignment_is_current(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    deadline: tokio::time::Instant,
) -> Result<bool, String> {
    if !db.cluster_intake_fenced()
        || !controller.is_recovering()
        || !local_assignment_matches_recovery_round(db, controller, round)
    {
        return Ok(false);
    }
    let revision = db.assignment_authority_revision.load(Ordering::Acquire);
    let store =
        db.assignment_snapshot_store.lock().clone().ok_or_else(|| {
            "recovery Prepare assignment audit has no assignment store".to_string()
        })?;
    let head = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery Prepare assignment head read timed out".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "recovery Prepare assignment audit has no durable head".to_string())?;
    let matches_round = if head.draining {
        let Some(transition) = head.drain_transition.as_ref() else {
            return Err("recovery Prepare observed a draining head without a transition".into());
        };
        transition.predecessor == round.assignment_fence
            && transition.target.assignment_version
                == round
                    .assignment_fence
                    .assignment_version
                    .checked_add(1)
                    .unwrap_or(0)
            && head.version == transition.target.assignment_version
            && controller
                .checkpoint_drain_transition()
                .as_ref()
                .is_none_or(|local| local == transition)
    } else {
        head.assignment_fence().map_err(|error| error.to_string())? == round.assignment_fence
    };
    if !matches_round {
        return Ok(false);
    }
    tokio::time::timeout_at(
        deadline,
        crate::rebalance::audit_assignment_snapshot_authority(&store, Some(controller), &head),
    )
    .await
    .map_err(|_| "recovery Prepare assignment authority audit timed out".to_string())??;
    let confirmed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery Prepare assignment head recheck timed out".to_string())?
        .map_err(|error| error.to_string())?;
    let local_transition_matches = if head.draining {
        controller
            .checkpoint_drain_transition()
            .as_ref()
            .is_none_or(|local| Some(local) == head.drain_transition.as_ref())
    } else {
        controller.checkpoint_drain_transition().is_none()
    };
    Ok(confirmed.as_ref() == Some(&head)
        && local_transition_matches
        && db.assignment_authority_revision.load(Ordering::Acquire) == revision
        && db.cluster_intake_fenced()
        && controller.is_recovering()
        && local_assignment_matches_recovery_round(db, controller, round))
}

fn frozen_pending<T>(
    required: &[NodeId],
    reports: impl IntoIterator<Item = (NodeId, T)>,
    matches: impl Fn(&T) -> bool,
) -> Vec<NodeId> {
    let required_set: FxHashSet<NodeId> = required.iter().copied().collect();
    let acked: FxHashSet<NodeId> = reports
        .into_iter()
        .filter(|(node, report)| required_set.contains(node) && matches(report))
        .map(|(node, _)| node)
        .collect();
    required
        .iter()
        .copied()
        .filter(|node| !acked.contains(node))
        .collect()
}

/// Wait for every member of the frozen stopped/evidence roster. Membership is checked only for
/// invalidating the driver's roster; it never grows or shrinks this set.
async fn wait_stopped_quorum(
    controller: &ClusterController,
    round: &RecoveryRound,
    timeout: Duration,
) -> StoppedQuorum {
    wait_stopped_quorum_with_timeout_barrier(controller, round, timeout, std::future::ready(()))
        .await
}

/// Production supplies an immediately-ready barrier. Tests may pause only after the outer timeout
/// fires and before its exact-proof classification, without perturbing the quorum wait itself.
async fn wait_stopped_quorum_with_timeout_barrier(
    controller: &ClusterController,
    round: &RecoveryRound,
    timeout: Duration,
    after_timeout: impl std::future::Future<Output = ()>,
) -> StoppedQuorum {
    let Some(deadline) = checked_recovery_deadline(timeout) else {
        tracing::error!(
            gen = round.id.generation,
            ?timeout,
            "recovery stop-quorum deadline exceeds the platform clock range"
        );
        return StoppedQuorum::TimedOut;
    };
    if let Ok(outcome) =
        tokio::time::timeout_at(deadline, wait_stopped_quorum_until(controller, round)).await
    {
        outcome
    } else {
        after_timeout.await;
        match exact_round_driver_authority(controller, round, "stopped-quorum timeout").await {
            RoundDriverAuthority::Superseded => StoppedQuorum::LeadershipLost,
            RoundDriverAuthority::Current
            | RoundDriverAuthority::Uncertain
            | RoundDriverAuthority::Conflicted => {
                tracing::error!(gen = round.id.generation, "recovery stop quorum timed out");
                StoppedQuorum::TimedOut
            }
        }
    }
}

async fn classify_stopped_quorum_supersession(
    controller: &ClusterController,
    round: &RecoveryRound,
) -> StoppedQuorum {
    match exact_round_driver_authority(controller, round, "stopped quorum supersession").await {
        RoundDriverAuthority::Superseded => StoppedQuorum::LeadershipLost,
        RoundDriverAuthority::Conflicted => StoppedQuorum::Conflicted,
        RoundDriverAuthority::Current | RoundDriverAuthority::Uncertain => {
            StoppedQuorum::Superseded
        }
    }
}

async fn wait_stopped_quorum_until(
    controller: &ClusterController,
    round: &RecoveryRound,
) -> StoppedQuorum {
    let required = round.stopped_participants();
    let mut accepted = FxHashMap::<NodeId, RecoveryStoppedReport>::default();
    let mut poll = STOP_QUORUM_INITIAL_POLL;
    let mut next_roster_audit = tokio::time::Instant::now();
    loop {
        if round.id.driver != controller.instance_id()
            || controller.capture_leader_proof().as_ref() != Some(&round.leader_proof)
        {
            return StoppedQuorum::LeadershipLost;
        }
        let local_assignment_is_exact = controller
            .checkpoint_assignment_fence(round.assignment_fence.assignment_version)
            .as_ref()
            == Some(&round.assignment_fence);
        let published_assignment = controller.checkpoint_assignment_watch();
        // Absence is a deliberate safety suspension repaired by the post-quorum durable audit.
        // A present certificate that is no longer admissible means its roster changed.
        if !local_assignment_is_exact && published_assignment.borrow().is_some() {
            return StoppedQuorum::ParticipantsChanged;
        }
        if tokio::time::Instant::now() >= next_roster_audit {
            match controller.recovery_stopped_incarnations_match(round).await {
                Ok(true) => {
                    next_roster_audit = tokio::time::Instant::now() + STOP_ROSTER_AUDIT_INTERVAL;
                }
                Ok(false) => return StoppedQuorum::ParticipantsChanged,
                Err(RecoveryControlError::Uncertain(error)) => {
                    tracing::warn!(%error, "recovery stopped-roster audit is temporarily unavailable");
                    tokio::time::sleep(poll).await;
                    poll = std::cmp::min(poll.saturating_mul(2), STOP_QUORUM_MAX_POLL);
                    continue;
                }
                Err(RecoveryControlError::Conflict(_)) => return StoppedQuorum::Conflicted,
                Err(RecoveryControlError::Superseded(_)) => {
                    return classify_stopped_quorum_supersession(controller, round).await;
                }
            }
        }
        match controller.observe_recover_control().await {
            Err(RecoveryControlError::Uncertain(_)) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(RecoveryControlError::Conflict(_)) => return StoppedQuorum::Conflicted,
            Err(RecoveryControlError::Superseded(_)) | Ok(None) => {
                return classify_stopped_quorum_supersession(controller, round).await;
            }
            Ok(Some(active)) => {
                if active.round.id.generation > round.id.generation {
                    return StoppedQuorum::Superseded;
                }
                if active.round != *round || active.phase != RecoverPhase::Prepare {
                    return StoppedQuorum::Conflicted;
                }
            }
        }
        let missing = required
            .iter()
            .copied()
            .filter(|participant| !accepted.contains_key(participant))
            .collect::<Vec<_>>();
        let reports = match controller.read_stopped(round, &missing).await {
            Ok(reports) => reports,
            Err(RecoveryControlError::Uncertain(error)) => {
                tracing::warn!(%error, "recovery stopped reports are temporarily unavailable");
                tokio::time::sleep(poll).await;
                poll = std::cmp::min(poll.saturating_mul(2), STOP_QUORUM_MAX_POLL);
                continue;
            }
            Err(RecoveryControlError::Conflict(_)) => return StoppedQuorum::Conflicted,
            Err(RecoveryControlError::Superseded(_)) => {
                return classify_stopped_quorum_supersession(controller, round).await;
            }
        };
        if reports.iter().any(|report| {
            round.contains_stopped_participant(NodeId(report.publisher().node_id))
                && report.round_id().generation > round.id.generation
        }) {
            return StoppedQuorum::Superseded;
        }
        let made_progress = !reports.is_empty();
        for report in reports
            .into_iter()
            .filter(|report| report.round_id() == round.id)
        {
            accepted.insert(NodeId(report.publisher().node_id), report);
        }
        if accepted.len() == required.len() {
            match controller.recovery_stopped_incarnations_match(round).await {
                Ok(true) => match exact_round_driver_authority(
                    controller,
                    round,
                    "final stopped-quorum admission",
                )
                .await
                {
                    RoundDriverAuthority::Current => {
                        let exact = required
                            .iter()
                            .map(|participant| {
                                accepted
                                    .get(participant)
                                    .expect("complete stopped roster")
                                    .clone()
                            })
                            .collect();
                        return StoppedQuorum::Reached(exact);
                    }
                    RoundDriverAuthority::Superseded => {
                        return StoppedQuorum::LeadershipLost;
                    }
                    RoundDriverAuthority::Conflicted => return StoppedQuorum::Conflicted,
                    RoundDriverAuthority::Uncertain => {
                        tracing::warn!(
                            gen = round.id.generation,
                            "final recovery driver-proof audit is temporarily unavailable"
                        );
                    }
                },
                Ok(false) => return StoppedQuorum::ParticipantsChanged,
                Err(RecoveryControlError::Uncertain(error)) => {
                    tracing::warn!(%error, "final recovery stopped-roster audit is temporarily unavailable");
                }
                Err(RecoveryControlError::Conflict(_)) => return StoppedQuorum::Conflicted,
                Err(RecoveryControlError::Superseded(_)) => {
                    return classify_stopped_quorum_supersession(controller, round).await;
                }
            }
        }
        if made_progress {
            poll = STOP_QUORUM_INITIAL_POLL;
        }
        tokio::time::sleep(poll).await;
        poll = std::cmp::min(poll.saturating_mul(2), STOP_QUORUM_MAX_POLL);
    }
}

/// Wait for the frozen roster to acknowledge the identical round and `Start` target. A cleared
/// announcement is acceptable after acknowledgements exist; their payload is itself the durable
/// target-bound proof.
async fn wait_restored_quorum(
    controller: &ClusterController,
    start: &RecoveryAnnouncement,
    timeout: Duration,
) -> RecoveryQuorum {
    let round = &start.round;
    let Some(deadline) = checked_recovery_deadline(timeout) else {
        tracing::error!(
            gen = round.id.generation,
            ?timeout,
            "recovery restore-quorum deadline exceeds the platform clock range"
        );
        return RecoveryQuorum::TimedOut;
    };
    if let Ok(outcome) =
        tokio::time::timeout_at(deadline, wait_restored_quorum_until(controller, start)).await
    {
        outcome
    } else {
        tracing::error!(
            gen = round.id.generation,
            "recovery restore quorum timed out"
        );
        RecoveryQuorum::TimedOut
    }
}

async fn wait_restored_quorum_until(
    controller: &ClusterController,
    start: &RecoveryAnnouncement,
) -> RecoveryQuorum {
    let round = &start.round;
    loop {
        if !controller.is_leader()
            || controller
                .checkpoint_assignment_fence(round.assignment_fence.assignment_version)
                .as_ref()
                != Some(&round.assignment_fence)
            || !matches!(
                controller.recovery_incarnations_match(round).await,
                Ok(true)
            )
        {
            return RecoveryQuorum::ParticipantsChanged;
        }
        match controller.observe_recover_control().await {
            Err(RecoveryControlError::Uncertain(_)) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(RecoveryControlError::Conflict(_)) => return RecoveryQuorum::Conflicted,
            Err(RecoveryControlError::Superseded(_)) | Ok(None) => {
                return RecoveryQuorum::Superseded;
            }
            Ok(Some(active)) => {
                if active.round.id.generation > round.id.generation {
                    return RecoveryQuorum::Superseded;
                }
                if active.round.id.generation == round.id.generation && active != *start {
                    return RecoveryQuorum::Conflicted;
                }
            }
        }
        let Ok(reports) = controller.read_recovered().await else {
            return RecoveryQuorum::Conflicted;
        };
        if reports.iter().any(|(node, ack)| {
            round.contains_owner(*node) && ack.round.id.generation > round.id.generation
        }) {
            return RecoveryQuorum::Superseded;
        }
        if reports.iter().any(|(node, ack)| {
            round.contains_owner(*node)
                && ack.round.id.generation == round.id.generation
                && ack != start
        }) {
            return RecoveryQuorum::Conflicted;
        }
        let owners = round.owners();
        let pending = frozen_pending(&owners, reports, |ack| ack == start);
        if pending.is_empty() {
            return RecoveryQuorum::Reached;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests;
