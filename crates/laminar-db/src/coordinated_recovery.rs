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

use laminar_core::checkpoint::PreparedCheckpointWitness;
use laminar_core::checkpoint_decision::{
    CheckpointOutcome, CheckpointScope, CheckpointVerdict, RecordOutcomeResult,
};
use laminar_core::cluster::control::controller::{
    RecoveryAnnouncement, RecoveryFault, RecoveryFaultInventory, RecoveryRound,
    RecoveryStoppedReport,
};
use laminar_core::cluster::control::{
    ClusterController, RecoverPhase, RecoveryControlError, RecoveryFaultReportOutcome,
    ReleaseCommitStatus,
};
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

/// Healthy-state monitor cadence. Only the leader polls the shared fault inventory; followers use
/// the replicated recovery intent unless they have a local settlement latch.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
// A peer may legitimately consume the full lifecycle, inventory, and durable stopped-report
// budgets after observing Prepare. This is a failure-path ceiling; healthy quorums return at once.
const STOP_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
const RELEASE_PROTOCOL_TIMEOUT: Duration = Duration::from_secs(60);
const DECISION_IO_TIMEOUT: Duration = Duration::from_secs(15);
const DECISION_AMBIGUITY_AUDIT_RESERVE: Duration = Duration::from_secs(3);
const DECISION_AUDIT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
const DECISION_AUDIT_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const RECOVERY_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_QUORUM_INITIAL_POLL: Duration = Duration::from_millis(100);
const STOP_QUORUM_MAX_POLL: Duration = Duration::from_secs(1);
const STOP_ROSTER_AUDIT_INTERVAL: Duration = Duration::from_secs(1);
/// How long a stopped/restored node tolerates the exact round disappearing before faulting again.
const ORPHAN_STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery target meaning "no committed cut exists": start fresh.
const GENESIS: u64 = 0;

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
) -> Result<RecoveryFaultReportOutcome, String> {
    let request = controller.recovery_fault_request(raw_request)?;
    match tokio::time::timeout(DECISION_IO_TIMEOUT, controller.report_fault(request)).await {
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
) -> Result<(), String> {
    let raw_request = pending.load(Ordering::Acquire);
    if raw_request == 0 {
        return Ok(());
    }
    persist_local_fault(controller, raw_request)
        .await
        .map(|_| ())
}

/// Coalesce a duplicate notification into the outstanding request and make one bounded durable
/// publication attempt.
pub(crate) async fn request_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    let raw_request = retain_local_fault_request(controller, pending)?;
    match persist_local_fault(controller, raw_request).await? {
        RecoveryFaultReportOutcome::Active => Ok(raw_request),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => {
            let concurrent = pending.load(Ordering::Acquire);
            let fresh_request = if concurrent != 0 && concurrent != raw_request {
                concurrent
            } else {
                install_new_local_fault_request(controller, pending)?
            };
            match persist_local_fault(controller, fresh_request).await? {
                RecoveryFaultReportOutcome::Active => Ok(fresh_request),
                RecoveryFaultReportOutcome::AlreadyCleared
                | RecoveryFaultReportOutcome::CoveredByNewerRequest => Err(format!(
                    "fresh recovery fault request {fresh_request} was settled before it became active"
                )),
            }
        }
    }
}

async fn request_fresh_local_fault(
    controller: &ClusterController,
    pending: &std::sync::atomic::AtomicU64,
) -> Result<u64, String> {
    let raw_request = install_new_local_fault_request(controller, pending)?;
    match persist_local_fault(controller, raw_request).await? {
        RecoveryFaultReportOutcome::Active => Ok(raw_request),
        RecoveryFaultReportOutcome::AlreadyCleared
        | RecoveryFaultReportOutcome::CoveredByNewerRequest => Err(format!(
            "fresh recovery fault request {raw_request} was settled before it became active"
        )),
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
                if let Err(error) =
                    request_fresh_local_fault(&controller, &db.pending_recovery_fault).await
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
    published_local_request: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryQuorum {
    Reached,
    Superseded,
    Conflicted,
    ParticipantsChanged,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StoppedQuorum {
    Reached(Vec<RecoveryStoppedReport>),
    Superseded,
    Conflicted,
    ParticipantsChanged,
    TimedOut,
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
            let pending = self.unhandled_faults(&reported);
            self.hold_for_visible_or_queued_fault(&db, &controller, &pending);

            // `drive_round` owns every nonterminal local Prepare/Start synchronously. Seeing
            // one here means that owner disappeared or returned early. Retire only the exact
            // orphan before requesting a fresh generation. A pending Release is retained for its
            // prepare/commit retry, but must not mask a later fault set.
            let active =
                tokio::time::timeout(DECISION_IO_TIMEOUT, controller.observe_recover_control())
                    .await;
            match active {
                Err(_) => continue,
                Ok(result) => match result {
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
                            RecoverPhase::Prepare | RecoverPhase::Start { .. }
                                if active.round.id.driver == controller.instance_id() =>
                            {
                                let _ = controller.clear_recover(&active.round).await;
                                if !self.request_fresh_fault(&db, &controller).await {
                                    self.fault_audit_unknown = true;
                                }
                            }
                            RecoverPhase::Prepare | RecoverPhase::Start { .. } => continue,
                        }
                    }
                    Err(RecoveryControlError::Uncertain(_)) => continue,
                    Ok(_) => {}
                    Err(RecoveryControlError::Conflict(error)) => {
                        tracing::error!(%error, "invalid active recovery intent");
                        self.hold_for_unknown_fault_audit(&db, &controller).await;
                        continue;
                    }
                    Err(RecoveryControlError::Superseded(_)) => continue,
                },
            }
            if !pending.is_empty() {
                self.drive_round(&db, &controller, inventory.revision(), reported)
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
        if self.published_local_request == Some(raw_request) {
            return true;
        }
        if let Err(error) = flush_pending_local_fault(controller, &db.pending_recovery_fault).await
        {
            tracing::warn!(%error, "queued local fault remains pending durable publication");
            return false;
        }
        self.cache_published_local_request(db, raw_request)
    }

    async fn request_fresh_fault(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) -> bool {
        self.published_local_request = None;
        match request_fresh_local_fault(controller, &db.pending_recovery_fault).await {
            Ok(raw_request) => self.cache_published_local_request(db, raw_request),
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
        if pending != 0 && self.published_local_request == Some(pending) {
            return true;
        }
        match request_local_fault(controller, &db.pending_recovery_fault).await {
            Ok(raw_request) => self.cache_published_local_request(db, raw_request),
            Err(error) => {
                tracing::warn!(%error, "recovery fault remains queued for publication retry");
                false
            }
        }
    }

    fn cache_published_local_request(&mut self, db: &LaminarDB, raw_request: u64) -> bool {
        if raw_request != 0 && db.pending_recovery_fault.load(Ordering::Acquire) == raw_request {
            self.published_local_request = Some(raw_request);
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
                        Ok(Some(terminal))
                            if round_assignment_is_current(db, controller, &terminal.round) =>
                        {
                            self.last_protocol_error = None;
                            self.observe_release(db, controller, terminal, epoch).await;
                            return;
                        }
                        Ok(Some(_)) | Ok(None) | Err(RecoveryControlError::Superseded(_)) => {}
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
                        && self.published_local_request == Some(pending_request)
                        && covered_fault.is_some();
                    let stopped_round_matches = local_fault.is_none()
                        && stopped_round.is_some_and(|round| *round == terminal.round);
                    let covers_local_settlement = active_fault_matches
                        || published_tombstone_matches
                        || stopped_round_matches;
                    if terminal.round.id.generation > self.applied_gen
                        && covers_local_settlement
                        && !terminal.round.contains_owner(controller.instance_id())
                        && round_assignment_is_current(db, controller, &terminal.round)
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
                        self.observe_nonparticipant_release(db, controller, &terminal)
                            .await;
                        return;
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
        let current = observed.filter(|announcement| {
            controller.recovery_driver_is_current(&announcement.round)
                && round_assignment_is_current(db, controller, &announcement.round)
        });
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
                self.observe_release(db, controller, release, epoch).await;
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
        let Some(prepared_witnesses) = stop_and_collect_prepared(db).await else {
            tracing::error!(
                gen,
                "recovery prepare could not quiesce and inventory this node; withholding stopped acknowledgement"
            );
            return;
        };
        if let Err(error) = controller
            .announce_stopped(&round, prepared_witnesses)
            .await
        {
            tracing::error!(gen, %error, "could not acknowledge recovery Prepare");
            return;
        }
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
        if self.restore_and_ack(db, controller, &start, epoch).await {
            tracing::warn!(
                target_epoch = epoch,
                gen,
                "node restored to recovery epoch; source gate awaits Release"
            );
        }
        // Failure: state untouched, `Start` still visible → retried next tick.
    }

    async fn observe_release(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: RecoveryAnnouncement,
        epoch: u64,
    ) {
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
            return;
        }
        if exact_start {
            if self
                .release_after_readiness_quorum(db, controller, &release, epoch)
                .await
            {
                tracing::warn!(
                    target_epoch = epoch,
                    gen = release.round.id.generation,
                    "recovery Release consumed; source gate opened"
                );
            }
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
        }
    }

    /// An ownerless worker consumes only a leader-fenced terminal that names this exact boot in
    /// its stopped roster and was committed after every data owner prepared while fenced. The
    /// terminal retains stable-node fault evidence after authority tombstones the covered slot.
    /// It never joins the frozen data quorum, restarts state, acknowledges recovery, or opens its
    /// assignment-closed source gate.
    async fn observe_nonparticipant_release(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &RecoveryAnnouncement,
    ) {
        let RecoverPhase::ReleaseCommitted { epoch } = release.phase else {
            return;
        };
        let deadline = tokio::time::Instant::now() + RELEASE_PROTOCOL_TIMEOUT;
        if release.round.contains_owner(controller.instance_id())
            || !controller.recovery_round_requires_current_process_stop(&release.round)
            || !controller.process_lease_is_live()
            || !round_assignment_is_current(db, controller, &release.round)
            || !matches!(
                tokio::time::timeout_at(
                    deadline,
                    controller.observe_committed_recover_release(&release.round, epoch),
                )
                .await,
                Ok(Ok(Some(active))) if active == *release
            )
        {
            return;
        }

        // A non-owner is outside the restore quorum, but it can still hold participant-local
        // Prepared evidence or a connector generation stopped for this round. Quiesce it before
        // accepting the terminal that tombstoned its covered fault. Settlement makes a reported
        // Prepared inventory empty; a remaining witness means this boot was omitted from the
        // round and must request another evidence-bearing round instead of silently accepting the
        // Release.
        let Some(prepared_witnesses) = stop_and_collect_prepared(db).await else {
            return;
        };
        if !prepared_witnesses.is_empty() {
            tracing::error!(
                gen = release.round.id.generation,
                prepared_attempts = prepared_witnesses.len(),
                "ownerless process retains unresolved Prepared evidence after recovery Release"
            );
            if !self.request_fresh_fault(db, controller).await {
                self.fault_audit_unknown = true;
            }
            return;
        }

        db.set_shuffle_recovery_gen(release.round.id.generation);
        db.purge_shuffle_receiver_buffers();
        if !start_pipeline(db, Some(epoch)).await {
            tracing::error!(
                gen = release.round.id.generation,
                target_epoch = epoch,
                "ownerless process could not rebuild its assignment-closed connector runtime"
            );
            return;
        }
        if !controller.process_lease_is_live()
            || !round_assignment_is_current(db, controller, &release.round)
            || !matches!(
                controller
                    .observe_committed_recover_release(&release.round, epoch)
                    .await,
                Ok(Some(active)) if active == *release
            )
        {
            return;
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
                    return;
                }
                Ok(Ok(None)) | Ok(Err(RecoveryControlError::Superseded(_))) => return,
                Ok(Err(RecoveryControlError::Uncertain(_)))
                    if tokio::time::Instant::now() < deadline =>
                {
                    release_retry_delay(deadline, &mut backoff).await;
                }
                Ok(Err(RecoveryControlError::Conflict(error))) => {
                    tracing::error!(
                        gen = release.round.id.generation,
                        %error,
                        "ownerless recovery authority state conflicts with the committed Release"
                    );
                    if !self.request_fresh_fault(db, controller).await {
                        self.fault_audit_unknown = true;
                    }
                    return;
                }
                Ok(Err(RecoveryControlError::Uncertain(_))) | Err(_) => return,
            }
        };
        if !self.clear_authorized_pending_request(db) {
            tracing::warn!(
                gen = release.round.id.generation,
                "a replacement recovery request prevented ownerless Release consumption"
            );
            drop(release_guard);
            return;
        }
        self.record_released_faults(&release.round);
        self.applied_gen = release.round.id.generation;
        self.stopped_for = None;
        controller.set_recovering(false);
        db.set_source_gate(true);
        drop(release_guard);
        db.release_coordinated_recovery_lifecycle();
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
        Ok(
            tokio::time::timeout(DECISION_IO_TIMEOUT, controller.read_local_fault_report())
                .await
                .map_err(|_| "local recovery fault report read timed out".to_string())??
                .filter(|sequence| {
                    self.handled_faults.get(&controller.instance_id()) != Some(sequence)
                })
                .map(|sequence| RecoveryFault {
                    reporter: controller.instance_id(),
                    sequence,
                }),
        )
    }

    async fn pending_local_fault_if_queued(
        &self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) -> Result<Option<RecoveryFault>, String> {
        if db.pending_recovery_fault.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        self.pending_local_fault(controller).await
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
            self.fault_fenced = false;
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
            db.fence_coordinated_recovery_lifecycle();
            controller.set_recovering(false);
            self.fault_fenced = false;
            return;
        }
        db.fence_coordinated_recovery_lifecycle();
        controller.set_recovering(true);
        db.set_source_gate(true);
        if !self.fault_fenced {
            db.suspend_shuffle_assignment_fence();
            self.fault_fenced = true;
        }
    }

    /// Leader: stop the world, fix the target against the quiesced decision store, then restart
    /// the world. An incomplete round retains the intake fence, bumps
    /// `coordinated_recovery_failures_total`, and leaves its fault pending for a complete retry.
    #[allow(clippy::too_many_lines)]
    async fn drive_round(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        fault_revision: u64,
        faults: Vec<RecoveryFault>,
    ) {
        // Precheck readability before stopping anything — a transient decision-store error
        // must defer the round (faults stay pending), not flap a stop-the-world cycle.
        if read_committed_cut_bounded(db).await.is_err() {
            tracing::warn!("coordinated recovery: decision store unreadable; deferring round");
            return;
        }
        match crate::rebalance::settle_source_drain_before_recovery(
            db,
            controller,
            crate::rebalance::RebalanceConfig::default(),
        )
        .await
        {
            Ok(Some(version)) => {
                tracing::warn!(
                    version,
                    "settled an in-progress source drain before coordinated recovery"
                );
                return;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(%error, "could not settle source drain before recovery");
                return;
            }
        }
        let Some(assignment_fence) = current_assignment_fence(db, controller) else {
            tracing::warn!(
                "coordinated recovery: no current owner-complete assignment certificate; deferring round"
            );
            return;
        };
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

        controller.set_recovering(true);
        db.set_source_gate(true);
        if let Err(error) = controller.announce_recover_prepare(&round).await {
            tracing::warn!(gen = gen_id, %error, "could not publish recovery Prepare");
            return;
        }
        tracing::warn!(gen = gen_id, "leader announced recovery prepare");
        let Some(prepared_witnesses) = stop_and_collect_prepared(db).await else {
            tracing::error!(
                gen = gen_id,
                "leader could not quiesce and inventory its decision writer; abandoning recovery round"
            );
            self.abandon_round(db, controller, &round).await;
            return;
        };
        if let Err(error) = controller
            .announce_stopped(&round, prepared_witnesses)
            .await
        {
            tracing::error!(gen = gen_id, %error, "could not acknowledge recovery Prepare");
            self.abandon_round(db, controller, &round).await;
            return;
        }
        let stopped_reports =
            match wait_stopped_quorum(controller, &round, STOP_QUORUM_TIMEOUT).await {
                StoppedQuorum::Reached(reports) => reports,
                StoppedQuorum::Superseded | StoppedQuorum::Conflicted => {
                    tracing::warn!(
                        gen = gen_id,
                        "recovery Prepare superseded; yielding old driver"
                    );
                    let _ = controller.clear_recover(&round).await;
                    hold_intake_and_request_retry(db, controller, gen_id, false).await;
                    return;
                }
                StoppedQuorum::ParticipantsChanged | StoppedQuorum::TimedOut => {
                    // A straggler can still publish an ambiguous decision/state write, so selecting a
                    // recovery cut without every round participant stopped would violate the exact-cut
                    // premise and could resurrect a live timeline.
                    tracing::error!(
                        gen = gen_id,
                        "stop quorum timed out; abandoning recovery round"
                    );
                    self.abandon_round(db, controller, &round).await;
                    return;
                }
            };

        if !driver_owns_prepare(controller, &round).await {
            tracing::warn!(
                gen = gen_id,
                "recovery driver lost ownership before target selection; yielding"
            );
            let _ = controller.clear_recover(&round).await;
            hold_intake_and_request_retry(db, controller, gen_id, false).await;
            return;
        }

        if let Err(error) =
            settle_stopped_prepared_witnesses(db, controller, &round, &stopped_reports).await
        {
            tracing::error!(
                gen = gen_id,
                %error,
                "prepared checkpoint settlement failed; abandoning recovery round"
            );
            self.abandon_round(db, controller, &round).await;
            return;
        }

        // The world is stopped and every reported prepare is terminally resolved: the decision
        // store is quiescent, so this read IS the cut — no
        // seal fallback and no probe. No committed epoch means a fresh start.
        let target = match read_committed_cut_bounded(db).await {
            Ok(cut) => cut.unwrap_or(GENESIS),
            Err(e) => {
                tracing::error!(error = %e, gen = gen_id, "target read failed; abandoning round");
                self.abandon_round(db, controller, &round).await;
                return;
            }
        };
        if !driver_owns_prepare(controller, &round).await {
            tracing::warn!(
                gen = gen_id,
                "recovery driver was superseded during target selection; yielding"
            );
            let _ = controller.clear_recover(&round).await;
            hold_intake_and_request_retry(db, controller, gen_id, false).await;
            return;
        }
        // Every advancing Prepared attempt now has an immutable outcome. Namespaced artifacts can
        // be retained until normal decision-bound cleanup without affecting the selected cut.
        db.purge_shuffle_receiver_buffers();
        if let Err(error) = controller.announce_recover_start(&round, target).await {
            tracing::error!(gen = gen_id, %error, "could not transition recovery to Start");
            self.abandon_round(db, controller, &round).await;
            return;
        }
        let start = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch: target },
        };
        tracing::warn!(
            target_epoch = target,
            gen = gen_id,
            "leader announced recovery start"
        );

        // Retry self-restore inline so the round and its cleanup stay in the leader's control.
        let mut restored = false;
        for attempt in 1..=SELF_RESTORE_ATTEMPTS {
            if self.restore_and_ack(db, controller, &start, target).await {
                restored = true;
                break;
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
        if !round_is_current(db, controller, &round).await {
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

    /// Retire a round that failed before `Start` and restart only the leader's control loop.
    /// Intake remains fenced because peers may already be stopped. Faults are not marked handled,
    /// so the next poll retries the complete round with a fresh generation.
    async fn abandon_round(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        round: &RecoveryRound,
    ) {
        let gen_id = round.id.generation;
        if let Some(m) = db.engine_metrics.lock().clone() {
            m.coordinated_recovery_failures_total.inc();
        }
        let _ = controller.clear_recover(round).await;
        db.set_source_gate(true);
        start_pipeline(db, None).await;
        tracing::error!(
            gen = gen_id,
            "coordinated recovery round abandoned before start; intake remains fenced"
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
    ) -> bool {
        let gen_id = start.round.id.generation;
        // Sources come up paused: the restart re-reads and re-shuffles the replay window, and a
        // node that restarts first would shuffle into peers whose receivers haven't rebound.
        db.set_source_gate(true);
        // Fence delayed pre-rewind frames before the restored compute loop can drain the receiver.
        // Advancing this after `start_pipeline` leaves a window in which old data can be folded
        // onto restored state. The monotonic generation remains safe if startup fails and retries.
        db.set_shuffle_recovery_gen(gen_id);
        if !start_pipeline(db, Some(target)).await {
            // Starting from the selected cut failed. Keep intake fenced while `Start` remains
            // visible so the next monitor tick can retry without exposing pre-recovery state.
            // The lifecycle deadline may have expired while the owned start thread continued;
            // forget the old stop marker so the retry first quiesces any late successful start.
            self.stopped_for = None;
            return false;
        }
        match controller.announce_recovered(start).await {
            Ok(()) => {
                if let Some(m) = db.engine_metrics.lock().clone() {
                    m.coordinated_recoveries_total.inc();
                }
                self.applied_gen = gen_id;
                self.stopped_for = None;
                self.restored_for = Some((start.clone(), tokio::time::Instant::now()));
                true
            }
            Err(error) => {
                tracing::error!(gen = gen_id, %error, "could not acknowledge recovery restore");
                false
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
        let pending = db.pending_recovery_fault.load(Ordering::Acquire);
        if pending == 0 {
            self.published_local_request = None;
            return true;
        }
        if self.published_local_request != Some(pending) {
            return false;
        }
        match db.pending_recovery_fault.compare_exchange(
            pending,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.published_local_request = None;
                true
            }
            Err(_) => {
                self.published_local_request = None;
                false
            }
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
        if !local_release_round_is_current(db, controller, &release.round) {
            return false;
        }
        if !db.complete_shuffle_recovery(release.round.id.generation) {
            tracing::error!(
                gen = release.round.id.generation,
                "shuffle loss cutoff did not match the released recovery generation"
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
            Ok(activation) if activation.installed && !activation.intake_open => {}
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

        let _adoption =
            match tokio::time::timeout_at(release_deadline, db.assignment_adoption_lock.lock())
                .await
            {
                Ok(guard) => guard,
                Err(_) => {
                    tracing::error!(
                        gen = release.round.id.generation,
                        "release readiness could not retain assignment authority"
                    );
                    return false;
                }
            };
        let _execution = match tokio::time::timeout_at(
            release_deadline,
            Arc::clone(&db.rotation_execution_fence).write_owned(),
        )
        .await
        {
            Ok(guard) => guard,
            Err(_) => {
                tracing::error!(
                    gen = release.round.id.generation,
                    "release readiness could not retain the execution fence"
                );
                return false;
            }
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
            let mut ready = false;
            let mut backoff = Duration::from_millis(25);
            loop {
                if tokio::time::Instant::now() >= release_deadline {
                    tracing::error!(
                        gen = pending.round.id.generation,
                        "committed recovery Release was not observable before its deadline"
                    );
                    return false;
                }
                if db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
                    || !local_release_round_is_current(db, controller, &pending.round)
                {
                    self.defer_release_retry(db, controller, pending.round.id.generation, false);
                    return false;
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
                        self.defer_release_retry(
                            db,
                            controller,
                            pending.round.id.generation,
                            false,
                        );
                        return false;
                    }
                    Err(RecoveryControlError::Superseded(_)) => return false,
                }

                if !ready {
                    match tokio::time::timeout_at(
                        release_deadline,
                        controller.read_local_fault_report_control(),
                    )
                    .await
                    {
                        Ok(Ok(observed))
                            if observed
                                == pending.round.fault_sequence(controller.instance_id()) => {}
                        Ok(Ok(_)) | Ok(Err(RecoveryControlError::Superseded(_))) => return false,
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
                            return false;
                        }
                        Err(_) => return false,
                    }
                    match tokio::time::timeout_at(
                        release_deadline,
                        controller.observe_recover_control(),
                    )
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
                            self.defer_release_retry(
                                db,
                                controller,
                                pending.round.id.generation,
                                false,
                            );
                            return false;
                        }
                        Ok(Err(RecoveryControlError::Superseded(_)))
                        | Ok(Ok(Some(_)))
                        | Ok(Ok(None))
                        | Err(_) => return false,
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
                            return false;
                        }
                        Ok(Err(RecoveryControlError::Superseded(_))) | Err(_) => return false,
                    }
                }

                if pending.round.id.driver == controller.instance_id() {
                    if !controller.is_leader()
                        || controller.capture_leader_proof().as_ref()
                            != Some(&pending.round.leader_proof)
                    {
                        return false;
                    }
                    match tokio::time::timeout_at(
                        release_deadline,
                        controller.try_commit_recover_release(pending),
                    )
                    .await
                    {
                        Ok(Ok(ReleaseCommitStatus::Pending { .. })) => {}
                        Ok(Ok(ReleaseCommitStatus::Committed { terminal })) => break terminal,
                        Ok(Err(RecoveryControlError::Uncertain(_))) => {}
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
                            return false;
                        }
                        Ok(Err(RecoveryControlError::Superseded(_))) | Err(_) => return false,
                    }
                }
                release_retry_delay(release_deadline, &mut backoff).await;
            }
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
                Ok(Some(_))
                | Ok(None)
                | Err(RecoveryControlError::Conflict(_))
                | Err(RecoveryControlError::Superseded(_))
                | Err(RecoveryControlError::Uncertain(_)) => {
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
                Ok(Ok(None)) | Ok(Err(RecoveryControlError::Superseded(_))) => {
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
        true
    }
}

/// Stop the pipeline and drop buffered shuffle input, on a dedicated thread (lifecycle
/// futures are `!Send`). `true` means the runtime and every owned decision writer are quiescent.
async fn stop_and_purge(db: &Arc<LaminarDB>) -> bool {
    run_lifecycle(db, |db| async move {
        db.stop_pipeline_for_coordinated_recovery().await?;
        // Pre-rewind shuffle slices are stale: their senders rewind and replay them, so
        // folding a buffered copy after the rewind double-counts.
        db.purge_shuffle_receiver_buffers();
        Ok(())
    })
    .await
}

/// Quiesce every local writer, then capture the exact participant-local Prepared inventory while
/// public restart remains recovery-fenced. Failure withholds the stopped report.
async fn stop_and_collect_prepared(db: &Arc<LaminarDB>) -> Option<Vec<PreparedCheckpointWitness>> {
    db.fence_coordinated_recovery_lifecycle();
    if !stop_and_purge(db).await {
        return None;
    }
    let inventory = async {
        let coordinator = db.coordinator.lock().await;
        match coordinator.as_ref() {
            Some(coordinator) => coordinator.prepared_checkpoint_witnesses().await,
            None => Ok(Vec::new()),
        }
    };
    match tokio::time::timeout(DECISION_IO_TIMEOUT, inventory).await {
        Ok(Ok(witnesses)) => Some(witnesses),
        Ok(Err(error)) => {
            tracing::error!(%error, "could not validate local Prepared checkpoint inventory");
            None
        }
        Err(_) => {
            tracing::error!(
                timeout = ?DECISION_IO_TIMEOUT,
                "local Prepared checkpoint inventory timed out"
            );
            None
        }
    }
}

/// Start the pipeline, rewinding to `target` when given. `true` on a clean start.
async fn start_pipeline(db: &Arc<LaminarDB>, target: Option<u64>) -> bool {
    if !coordinated_restart_assignment_ready(db).await {
        return false;
    }
    run_lifecycle(db, move |db| async move {
        if let Some(t) = target {
            db.set_recover_target_epoch(t);
        }
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
async fn run_lifecycle<F, Fut>(db: &Arc<LaminarDB>, f: F) -> bool
where
    F: FnOnce(Arc<LaminarDB>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), crate::DbError>>,
{
    if db
        .coordinated_lifecycle_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        tracing::warn!(
            "coordinated recovery lifecycle operation is still active; refusing overlap"
        );
        return false;
    }

    let active = LifecycleActiveGuard(Arc::clone(&db.coordinated_lifecycle_active));
    let handle = match db.control_runtime.handle() {
        Ok(handle) => handle,
        Err(error) => {
            tracing::error!(%error, "coordinated recovery: DB control runtime is unavailable");
            return false;
        }
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
        tracing::error!(error = %e, "coordinated recovery: failed to spawn lifecycle thread");
        return false;
    }
    match tokio::time::timeout(RECOVERY_LIFECYCLE_TIMEOUT, rx).await {
        Err(_) => {
            tracing::error!(
                timeout = ?RECOVERY_LIFECYCLE_TIMEOUT,
                "coordinated recovery lifecycle step timed out; owner remains fenced"
            );
            false
        }
        Ok(Ok(Ok(()))) => true,
        Ok(Ok(Err(e))) => {
            tracing::error!(error = %e, "coordinated recovery lifecycle step failed");
            false
        }
        Ok(Err(_)) => {
            tracing::error!("coordinated recovery lifecycle thread dropped");
            false
        }
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

/// Both the assignment cut and every assignment-owner process must still match the frozen round.
async fn round_is_current(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
) -> bool {
    round_assignment_is_current(db, controller, round)
        && matches!(
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
    if let Err(error) = request_fresh_local_fault(controller, &db.pending_recovery_fault).await {
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

/// Highest epoch committed cluster-wide per the immutable outcome store — the only sound rewind
/// target (the durable seal is node-local durability, not a cluster commit). `Ok(None)`
/// means nothing ever committed; `Err` means the store is unreadable right now.
async fn read_committed_cut(db: &LaminarDB) -> Result<Option<u64>, String> {
    // Bind the clone before awaiting — an if-let scrutinee would hold the lock guard across it.
    let controller = db
        .cluster_controller
        .lock()
        .clone()
        .ok_or_else(|| "cluster controller is not configured".to_owned())?;
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    authority
        .highest_cluster_committed_outcome()
        .await
        .map(|outcome| outcome.map(|outcome| outcome.epoch))
        .map_err(|error| error.to_string())
}

async fn read_committed_cut_bounded(db: &LaminarDB) -> Result<Option<u64>, String> {
    tokio::time::timeout(DECISION_IO_TIMEOUT, read_committed_cut(db))
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
    controller.adopt_recovery_generation(gen_id).await
}

async fn driver_owns_prepare_until(
    controller: &ClusterController,
    round: &RecoveryRound,
    deadline: tokio::time::Instant,
) -> bool {
    let audit = async {
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
            return false;
        }
        matches!(
            controller.observe_recover_control().await,
            Ok(Some(RecoveryAnnouncement {
                round: active,
                phase: RecoverPhase::Prepare,
            })) if active == *round
        )
    };
    matches!(tokio::time::timeout_at(deadline, audit).await, Ok(true))
}

async fn driver_owns_prepare(controller: &ClusterController, round: &RecoveryRound) -> bool {
    driver_owns_prepare_until(
        controller,
        round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
}

async fn audit_cluster_outcome_until(
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    epoch: u64,
    deadline: tokio::time::Instant,
) -> Result<CheckpointOutcome, String> {
    let mut last_failure = None;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let attempt_deadline = std::cmp::min(now + DECISION_AUDIT_ATTEMPT_TIMEOUT, deadline);
        match tokio::time::timeout_at(attempt_deadline, authority.cluster_outcome(epoch)).await {
            Ok(Ok(Some(outcome))) => return Ok(outcome),
            Ok(Ok(None)) => last_failure = None,
            Ok(Err(error)) => last_failure = Some(error.to_string()),
            Err(_) => last_failure = Some("read attempt timed out".into()),
        }
        let wake = std::cmp::min(
            tokio::time::Instant::now() + DECISION_AUDIT_RETRY_INTERVAL,
            deadline,
        );
        tokio::time::sleep_until(wake).await;
    }
    match last_failure {
        Some(error) => Err(format!(
            "checkpoint settlement ambiguity audit for epoch {epoch} exhausted: {error}"
        )),
        None => Err(format!(
            "checkpoint settlement ambiguity audit found no immutable outcome for epoch {epoch}"
        )),
    }
}

/// Resolve the complete stopped-quorum union before selecting a recovery cut. Existing immutable
/// outcomes always win; genuinely unresolved advancing attempts receive a create-once Abort under
/// the exact recovery-round leader proof. Older gaps dominated by retained terminal authority do
/// not need an impossible non-advancing backfill.
async fn settle_stopped_prepared_witnesses(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    round: &RecoveryRound,
    reports: &[RecoveryStoppedReport],
) -> Result<(), String> {
    let mut witnesses = Vec::new();
    for report in reports {
        report.validate(round)?;
        if !round.contains_stopped_participant(NodeId(report.publisher().node_id)) {
            return Err("stopped report does not belong to the exact recovery round".into());
        }
        witnesses.extend(report.prepared_witnesses().iter().cloned());
    }
    if witnesses.is_empty() {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
    let write_deadline = deadline
        .checked_sub(DECISION_AMBIGUITY_AUDIT_RESERVE)
        .expect("ambiguity reserve is shorter than the settlement budget");
    {
        let coordinator = tokio::time::timeout_at(write_deadline, db.coordinator.lock())
            .await
            .map_err(|_| "checkpoint settlement coordinator lock timed out".to_string())?;
        let coordinator = coordinator.as_ref().ok_or_else(|| {
            "recovery stopped reports contain checkpoint evidence but no coordinator is installed"
                .to_string()
        })?;
        for report in reports {
            coordinator
                .validate_prepared_checkpoint_witnesses(report.prepared_witnesses())
                .map_err(|error| error.to_string())?;
        }
    }
    witnesses.sort_unstable_by_key(|witness| {
        (
            witness.attempt.epoch,
            witness.attempt.checkpoint_id,
            witness.participant_id,
        )
    });
    let mut attempts = witnesses
        .into_iter()
        .map(|witness| {
            (
                witness.attempt,
                witness.deployment_id,
                witness.pipeline_identity,
            )
        })
        .collect::<Vec<_>>();
    attempts.dedup();
    if attempts.windows(2).any(|pair| {
        pair[0].0.epoch >= pair[1].0.epoch || pair[0].0.checkpoint_id >= pair[1].0.checkpoint_id
    }) {
        return Err(
            "stopped Prepared witnesses contain conflicting epoch/checkpoint dimensions".into(),
        );
    }
    if !driver_owns_prepare_until(controller, round, write_deadline).await {
        return Err("recovery driver lost Prepare authority before checkpoint settlement".into());
    }

    let authority = controller
        .checkpoint_authority()
        .map_err(|error| format!("checkpoint settlement authority is unavailable: {error}"))?;
    let boundary = tokio::time::timeout_at(
        write_deadline,
        authority.cluster_outcome_retention_boundary(),
    )
    .await
    .map_err(|_| "checkpoint settlement retention-boundary read timed out".to_string())?
    .map_err(|error| format!("checkpoint settlement retention boundary is invalid: {error}"))?;
    let mut outcomes = tokio::time::timeout_at(write_deadline, authority.cluster_outcomes())
        .await
        .map_err(|_| "checkpoint settlement outcome inventory timed out".to_string())?
        .map_err(|error| format!("checkpoint settlement outcome inventory is invalid: {error}"))?;

    for (attempt, deployment_id, _) in attempts {
        if let Some(outcome) = outcomes
            .iter()
            .find(|outcome| outcome.epoch == attempt.epoch)
        {
            if outcome.checkpoint_id != attempt.checkpoint_id
                || outcome.deployment_id != deployment_id
                || outcome.scope != CheckpointScope::Cluster
            {
                return Err(format!(
                    "Prepared checkpoint {} epoch {} conflicts with its immutable outcome",
                    attempt.checkpoint_id, attempt.epoch
                ));
            }
            continue;
        }
        if outcomes
            .iter()
            .any(|outcome| outcome.checkpoint_id == attempt.checkpoint_id)
        {
            return Err(format!(
                "Prepared checkpoint {} is bound to another durable epoch",
                attempt.checkpoint_id
            ));
        }
        if let Some(outcome) = outcomes.last().filter(|outcome| {
            outcome.epoch > attempt.epoch && outcome.checkpoint_id > attempt.checkpoint_id
        }) {
            if outcome.deployment_id != deployment_id || outcome.scope != CheckpointScope::Cluster {
                return Err("dominant checkpoint outcome has foreign provenance".into());
            }
            continue;
        }
        if attempt.epoch < boundary.before_epoch {
            let anchor = boundary.terminal_anchor.as_ref().ok_or_else(|| {
                "checkpoint outcome floor has no terminal continuity anchor".to_string()
            })?;
            if anchor.deployment_id != deployment_id || anchor.scope != CheckpointScope::Cluster {
                return Err(format!(
                    "Prepared checkpoint {} epoch {} is below an incompatible outcome floor",
                    attempt.checkpoint_id, attempt.epoch
                ));
            }
            if anchor.epoch > attempt.epoch && anchor.checkpoint_id > attempt.checkpoint_id {
                continue;
            }
            return Err(format!(
                "Prepared checkpoint {} epoch {} is below the outcome floor but is not strictly dominated by its terminal anchor",
                attempt.checkpoint_id, attempt.epoch
            ));
        }

        let write = tokio::time::timeout_at(
            write_deadline,
            authority.record_cluster_outcome(
                &round.leader_proof,
                attempt.epoch,
                attempt.checkpoint_id,
                round.assignment_fence.clone(),
                CheckpointVerdict::Abort,
                None,
            ),
        )
        .await;
        let outcome = match write {
            Ok(Ok(
                RecordOutcomeResult::Created(outcome)
                | RecordOutcomeResult::Unchanged(outcome),
            )) => outcome,
            Ok(Ok(RecordOutcomeResult::Conflict { winner })) => winner,
            Ok(Err(write_error)) => {
                tracing::warn!(
                    epoch = attempt.epoch,
                    checkpoint_id = attempt.checkpoint_id,
                    %write_error,
                    "recovery Abort write failed; auditing the create-once winner"
                );
                audit_cluster_outcome_until(authority.as_ref(), attempt.epoch, deadline)
                    .await
                    .map_err(|audit_error| {
                        format!(
                            "Prepared checkpoint {} epoch {} remains unresolved after write failure ({write_error}): {audit_error}",
                            attempt.checkpoint_id, attempt.epoch
                        )
                    })?
            }
            Err(_) => audit_cluster_outcome_until(authority.as_ref(), attempt.epoch, deadline)
                .await
                .map_err(|audit_error| {
                    format!(
                        "Prepared checkpoint {} epoch {} remains unresolved after a timed-out write: {audit_error}",
                        attempt.checkpoint_id, attempt.epoch
                    )
                })?,
        };
        if outcome.epoch != attempt.epoch
            || outcome.checkpoint_id != attempt.checkpoint_id
            || outcome.deployment_id != deployment_id
            || outcome.scope != CheckpointScope::Cluster
        {
            return Err(format!(
                "Prepared checkpoint {} epoch {} was not settled by its exact immutable outcome",
                attempt.checkpoint_id, attempt.epoch
            ));
        }
        outcomes.push(outcome);
        outcomes.sort_unstable_by_key(|outcome| (outcome.epoch, outcome.checkpoint_id));
    }

    if !driver_owns_prepare_until(controller, round, deadline).await {
        return Err("recovery driver lost Prepare authority during checkpoint settlement".into());
    }
    Ok(())
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
    let deadline = tokio::time::Instant::now() + timeout;
    match tokio::time::timeout_at(deadline, wait_stopped_quorum_until(controller, round)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::error!(gen = round.id.generation, "recovery stop quorum timed out");
            StoppedQuorum::TimedOut
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
        if !controller.is_leader() {
            return StoppedQuorum::Superseded;
        }
        if controller
            .checkpoint_assignment_fence(round.assignment_fence.assignment_version)
            .as_ref()
            != Some(&round.assignment_fence)
        {
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
                Err(RecoveryControlError::Superseded(_)) => return StoppedQuorum::Superseded,
            }
        }
        match controller.observe_recover_control().await {
            Err(RecoveryControlError::Uncertain(_)) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(RecoveryControlError::Conflict(_)) => return StoppedQuorum::Conflicted,
            Err(RecoveryControlError::Superseded(_)) => return StoppedQuorum::Superseded,
            Ok(Some(active)) if active.round.id.generation > round.id.generation => {
                return StoppedQuorum::Superseded;
            }
            Ok(Some(active)) if active.round != *round || active.phase != RecoverPhase::Prepare => {
                return StoppedQuorum::Conflicted;
            }
            Ok(None) => return StoppedQuorum::Superseded,
            Ok(Some(_)) => {}
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
            Err(RecoveryControlError::Superseded(_)) => return StoppedQuorum::Superseded,
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
                Ok(true) => {
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
                Ok(false) => return StoppedQuorum::ParticipantsChanged,
                Err(RecoveryControlError::Uncertain(error)) => {
                    tracing::warn!(%error, "final recovery stopped-roster audit is temporarily unavailable");
                }
                Err(RecoveryControlError::Conflict(_)) => return StoppedQuorum::Conflicted,
                Err(RecoveryControlError::Superseded(_)) => return StoppedQuorum::Superseded,
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
    let deadline = tokio::time::Instant::now() + timeout;
    match tokio::time::timeout_at(deadline, wait_restored_quorum_until(controller, start)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::error!(
                gen = round.id.generation,
                "recovery restore quorum timed out"
            );
            RecoveryQuorum::TimedOut
        }
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
            Err(RecoveryControlError::Superseded(_)) => return RecoveryQuorum::Superseded,
            Ok(Some(active)) if active.round.id.generation > round.id.generation => {
                return RecoveryQuorum::Superseded;
            }
            Ok(Some(active))
                if active.round.id.generation == round.id.generation && active != *start =>
            {
                return RecoveryQuorum::Conflicted;
            }
            Ok(None) => return RecoveryQuorum::Superseded,
            Ok(Some(_)) => {}
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
mod tests {
    use super::*;
    use laminar_core::checkpoint::{CheckpointAssignmentFence, LeaderProof, LeaderProofOwner};
    use laminar_core::cluster::control::{
        AssignmentSnapshot, AssignmentSnapshotStore, CheckpointParticipant, ClusterKv, InMemoryKv,
        LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority,
        ProcessLeaseOutcome,
    };
    use laminar_core::cluster::discovery::{NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    fn info(id: u64) -> NodeInfo {
        NodeInfo {
            id: NodeId(id),
            name: format!("n{id}"),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        }
    }

    fn install_test_process_deadline(controller: &ClusterController) {
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
            .unwrap();
    }

    async fn install_test_process_authority(
        controller: &ClusterController,
        authority_store: Arc<dyn object_store::ObjectStore>,
    ) -> u64 {
        let lease_duration = Duration::from_secs(60);
        let authority = Arc::new(
            ProcessLeaseAuthority::new(authority_store, lease_duration)
                .expect("test process authority must accept its lease duration"),
        );
        let ProcessLeaseOutcome::Acquired(lease) = authority
            .store_for(controller.instance_id())
            .try_acquire(controller.recovery_incarnation(), 0)
            .await
            .unwrap()
        else {
            panic!("empty process authority must grant the test process");
        };
        if controller.process_lease_deadline().is_none() {
            install_test_process_deadline(controller);
        }
        controller.set_process_lease_authority(authority).unwrap();
        controller
            .publish_leased_recovery_incarnation(&lease)
            .await
            .unwrap();
        lease.term
    }

    async fn install_test_leader_authority(
        controller: &ClusterController,
        authority_store: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<LeaderLeaseStore> {
        let process_term =
            install_test_process_authority(controller, Arc::clone(&authority_store)).await;
        let authority = Arc::new(LeaderLeaseStore::new(authority_store, 10_000));
        let owner = LeaderLeaseOwner {
            node: controller.instance_id(),
            boot: controller.recovery_incarnation(),
            process_term,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty recovery test authority must grant leadership");
        };
        let (_lease_tx, lease_rx) = watch::channel(Some(lease));
        controller
            .set_leader_lease_watch(
                lease_rx,
                owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));
        controller.set_active(true);
        authority
    }

    async fn controller(
        peers: Vec<NodeInfo>,
    ) -> (
        ClusterController,
        watch::Sender<Vec<NodeInfo>>,
        Arc<InMemoryKv>,
    ) {
        controller_on(peers, Arc::new(object_store::memory::InMemory::new())).await
    }

    async fn controller_on(
        peers: Vec<NodeInfo>,
        authority_store: Arc<dyn object_store::ObjectStore>,
    ) -> (
        ClusterController,
        watch::Sender<Vec<NodeInfo>>,
        Arc<InMemoryKv>,
    ) {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (members_tx, members_rx) = watch::channel(peers);
        let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
        install_test_process_deadline(&controller);
        install_test_leader_authority(&controller, authority_store).await;
        (controller, members_tx, kv)
    }

    async fn driver_and_follower() -> (
        Arc<ClusterController>,
        Arc<ClusterController>,
        Arc<InMemoryKv>,
    ) {
        let driver_id = NodeId(1);
        let driver_kv = Arc::new(InMemoryKv::new(driver_id));
        let (_driver_members_tx, driver_members_rx) = watch::channel(vec![info(2)]);
        let driver = Arc::new(ClusterController::new(
            driver_id,
            driver_kv.clone(),
            None,
            driver_members_rx,
        ));
        install_test_process_deadline(&driver);
        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let authority = install_test_leader_authority(&driver, Arc::clone(&backing)).await;

        let follower_id = NodeId(2);
        let follower_kv = Arc::new(InMemoryKv::new(follower_id));
        let (_follower_members_tx, follower_members_rx) = watch::channel(vec![info(1)]);
        let follower = Arc::new(ClusterController::new(
            follower_id,
            follower_kv,
            None,
            follower_members_rx,
        ));
        install_test_process_deadline(&follower);
        install_test_process_authority(&follower, backing).await;
        follower.set_leader_lease_store(authority);

        (driver, follower, driver_kv)
    }

    async fn report_test_fault(controller: &ClusterController) -> RecoveryFault {
        let request = controller.next_recovery_fault_request().unwrap();
        controller.report_fault(request).await.unwrap();
        controller
            .read_recovery_fault_inventory()
            .await
            .unwrap()
            .faults()
            .iter()
            .find(|fault| fault.reporter == controller.instance_id())
            .copied()
            .expect("reported fault must appear in the shared authority inventory")
    }

    async fn round_for_current_faults(
        controller: &ClusterController,
        generation: u64,
        participants: &[u64],
    ) -> RecoveryRound {
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        let checkpoint_participants = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: controller.recovery_incarnation(),
            })
            .collect();
        let owners = participants.to_vec();
        RecoveryRound::new(
            generation,
            controller
                .capture_leader_proof()
                .expect("recovery test controller must hold durable leadership"),
            CheckpointAssignmentFence::from_owner_map(7, &owners, checkpoint_participants).unwrap(),
            Vec::new(),
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap()
    }

    async fn publish_round_roster(
        controller: &ClusterController,
        kv: &InMemoryKv,
        round: &RecoveryRound,
    ) {
        controller.publish_recovery_incarnation().await.unwrap();
        for participant in &round.assignment_fence.participants {
            if participant.node_id != controller.instance_id().0 {
                kv.seed(
                    NodeId(participant.node_id),
                    "control:recovery-incarnation",
                    participant.boot_incarnation.to_string(),
                );
            }
        }
        controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    }

    async fn activate_start(
        controller: &ClusterController,
        kv: &InMemoryKv,
        round: &RecoveryRound,
        epoch: u64,
    ) {
        publish_round_roster(controller, kv, round).await;
        controller.announce_recover_prepare(round).await.unwrap();
        controller
            .announce_recover_start(round, epoch)
            .await
            .unwrap();
    }

    async fn commit_release(
        controller: &ClusterController,
        kv: &InMemoryKv,
        round: &RecoveryRound,
        epoch: u64,
    ) -> RecoveryAnnouncement {
        activate_start(controller, kv, round, epoch).await;
        controller
            .announce_recover_release(round, epoch)
            .await
            .unwrap();
        let pending = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch },
        };
        controller.announce_release_ready(&pending).await.unwrap();
        let ReleaseCommitStatus::Committed { terminal } = controller
            .try_commit_recover_release(&pending)
            .await
            .unwrap()
        else {
            panic!("single-owner recovery Release must commit");
        };
        terminal
    }

    fn start(round: RecoveryRound, epoch: u64) -> RecoveryAnnouncement {
        RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Start { epoch },
        }
    }

    #[tokio::test]
    async fn pending_fault_retries_and_remains_latched_until_release() {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(
            self_id,
            kv.clone(),
            None,
            members_rx,
        ));
        install_test_process_deadline(&controller);
        let authority_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::clone(&authority_store))
            .build()
            .await
            .unwrap();
        queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
        let raw_request = db.pending_recovery_fault.load(Ordering::Acquire);
        let mut monitor = RecoveryMonitor::default();

        assert!(!monitor.publish_pending_local_fault(&db, &controller).await);
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            raw_request,
            "a failed first publication must retain the retry identity"
        );
        assert!(controller.checkpoint_authority().is_err());
        assert!(db.coordinated_recovery_in_progress());

        install_test_leader_authority(&controller, authority_store).await;
        assert!(monitor.publish_pending_local_fault(&db, &controller).await);
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            raw_request,
            "successful publication must retain terminal discovery"
        );
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        assert_ne!(inventory.revision(), 0);
        let [fault] = inventory.faults() else {
            panic!("one retried request must create one authority fault");
        };
        assert_eq!(fault.reporter, self_id);
        assert_ne!(fault.sequence, 0);
        assert_eq!(
            controller.read_local_fault_report().await.unwrap(),
            Some(fault.sequence)
        );

        let round = round_for_current_faults(&controller, 1, &[self_id.0]).await;
        let terminal = commit_release(&controller, &kv, &round, 1).await;
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            raw_request
        );
        let release_guard = controller
            .begin_recovery_release(&terminal)
            .await
            .unwrap()
            .expect("the exact committed Release must authorize latch settlement");
        assert!(monitor.clear_authorized_pending_request(&db));
        assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
        drop(release_guard);
    }

    #[tokio::test]
    async fn new_event_supersedes_a_settled_latch_while_monitor_flush_does_not() {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(
            self_id,
            kv.clone(),
            None,
            members_rx,
        ));
        install_test_process_deadline(&controller);
        let authority_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::clone(&authority_store))
            .build()
            .await
            .unwrap();
        install_test_leader_authority(&controller, authority_store).await;

        let original = request_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .unwrap();
        assert_eq!(
            request_local_fault(&controller, &db.pending_recovery_fault)
                .await
                .unwrap(),
            original,
            "a duplicate active notification must coalesce"
        );
        let round = round_for_current_faults(&controller, 1, &[self_id.0]).await;
        let terminal = commit_release(&controller, &kv, &round, 1).await;
        assert!(controller
            .read_recovery_fault_inventory()
            .await
            .unwrap()
            .faults()
            .is_empty());

        flush_pending_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .unwrap();
        assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), original);
        assert!(controller
            .read_recovery_fault_inventory()
            .await
            .unwrap()
            .faults()
            .is_empty());

        let successor = request_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .unwrap();
        assert!(successor > original);
        assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), successor);
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        assert_eq!(inventory.faults().len(), 1);
        assert_eq!(inventory.faults()[0].reporter, self_id);
        assert!(controller
            .begin_recovery_release(&terminal)
            .await
            .unwrap()
            .is_none());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn published_request_cache_rejects_a_concurrent_replacement() {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        install_test_process_deadline(&controller);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        let mut monitor = RecoveryMonitor::default();

        queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
        let reported = db.pending_recovery_fault.load(Ordering::Acquire);
        queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
        let replacement = db.pending_recovery_fault.load(Ordering::Acquire);

        assert!(replacement > reported);
        assert!(!monitor.cache_published_local_request(&db, reported));
        assert!(monitor.published_local_request.is_none());
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            replacement
        );
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn healthy_monitor_does_not_require_terminal_authority() {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        install_test_process_deadline(&controller);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        controller.set_recovering(false);
        let mut monitor = RecoveryMonitor::default();

        let local_fault = monitor
            .pending_local_fault_if_queued(&db, &controller)
            .await
            .unwrap();
        monitor.observe(&db, &controller, local_fault).await;

        assert!(controller.checkpoint_authority().is_err());
        assert!(!controller.is_recovering());
        assert!(!db.cluster_intake_fenced());
        assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
        assert!(monitor.last_protocol_error.is_none());
    }

    #[tokio::test]
    async fn replacement_fault_blocks_release_latch_clear_and_gate_open() {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        install_test_process_deadline(&controller);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        assert!(!db.cluster_intake_fenced());
        queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
        let published = db.pending_recovery_fault.load(Ordering::Acquire);
        assert!(request_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .is_err());
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            published,
            "a duplicate reporter must retain the compute event's request"
        );
        let mut monitor = RecoveryMonitor {
            published_local_request: Some(published),
            ..RecoveryMonitor::default()
        };

        queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
        let replacement = db.pending_recovery_fault.load(Ordering::Acquire);

        assert_ne!(replacement, published);
        assert!(!monitor.clear_authorized_pending_request(&db));
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            replacement
        );
        db.release_coordinated_recovery_lifecycle();
        assert!(db.coordinated_recovery_in_progress());
        db.set_source_gate(false);
        assert!(db.cluster_intake_fenced());
    }

    #[tokio::test]
    async fn follower_replica_keeps_takeover_generation_monotonic_without_a_clock() {
        // Model the old driver slot disappearing after this participant accepted generation 41.
        // The surviving participant becomes leader and must allocate from its replica, not from
        // wall time or the vanished driver's slot.
        let (controller, _members_tx, _kv) = controller(Vec::new()).await;
        replicate_recovery_gen(&controller, 41).await.unwrap();

        let replicated_max = read_recovery_gen(&controller).await.unwrap();

        assert_eq!(replicated_max.checked_add(1), Some(42));
        assert!(replicate_recovery_gen(&controller, 40).await.is_err());
        assert_eq!(read_recovery_gen(&controller).await.unwrap(), 41);
    }

    #[tokio::test]
    async fn follower_fences_each_continuous_durable_fault_once() {
        let (driver, controller, driver_kv) = driver_and_follower().await;
        assert!(!controller.is_leader());

        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        controller.set_recovering(false);
        let initial_revision = db.assignment_authority_revision.load(Ordering::Acquire);
        let mut monitor = RecoveryMonitor::default();

        let first_fault = report_test_fault(&driver).await;
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        let held_revision = db.assignment_authority_revision.load(Ordering::Acquire);

        assert_eq!(pending, vec![first_fault]);
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert_eq!(held_revision, initial_revision + 1);

        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        assert_eq!(
            db.assignment_authority_revision.load(Ordering::Acquire),
            held_revision,
            "a level-triggered report must not churn assignment authority"
        );

        let round = round_for_current_faults(&driver, 17, &[1]).await;
        let terminal = commit_release(&driver, &driver_kv, &round, 4).await;
        drop(
            driver
                .begin_recovery_release(&terminal)
                .await
                .unwrap()
                .unwrap(),
        );
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        assert!(pending.is_empty());
        assert!(!monitor.fault_fenced);
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());

        let second_fault = report_test_fault(&driver).await;
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        assert_eq!(pending, vec![second_fault]);
        assert!(second_fault.sequence > first_fault.sequence);
        assert_eq!(
            db.assignment_authority_revision.load(Ordering::Acquire),
            held_revision + 1,
            "a new held-fault period must suspend the replacement authority"
        );
    }

    #[tokio::test]
    async fn release_fault_guard_orders_a_new_report_after_local_gate_transition() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1]).await;
        let terminal = commit_release(&controller, &kv, &round, 4).await;
        let release_guard = controller
            .begin_recovery_release(&terminal)
            .await
            .unwrap()
            .unwrap();
        let report = {
            let controller = Arc::clone(&controller);
            tokio::spawn(async move { report_test_fault(&controller).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !report.is_finished(),
            "a new fault must not cross the guarded source-gate transition"
        );

        drop(release_guard);
        let next_fault = report.await.unwrap();
        assert_eq!(
            controller.read_local_fault_report().await.unwrap(),
            Some(next_fault.sequence)
        );
    }

    #[tokio::test]
    async fn fault_after_release_commit_is_preserved_for_the_next_round() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1]).await;
        let terminal = commit_release(&controller, &kv, &round, 4).await;

        // The committed terminal linearizes before this new failure. It may release peers that
        // already consumed it, but this reporting owner must stay fenced and the new authority
        // fault must remain the level-trigger for the immediately following global round.
        let next_fault = report_test_fault(&controller).await;
        let mut monitor = RecoveryMonitor::default();
        assert!(controller
            .begin_recovery_release(&terminal)
            .await
            .unwrap()
            .is_none());
        assert_eq!(monitor.handled_faults.get(&NodeId(1)), None);
        assert_eq!(
            monitor.pending_faults(&controller).await.unwrap(),
            vec![next_fault]
        );
    }

    #[tokio::test]
    async fn evidence_only_worker_consumes_tombstoned_release_after_stopped_quorum() {
        use laminar_core::state::{NodeId as StateNodeId, ObjectStoreBackend, VnodeRegistry};

        let self_id = NodeId(2);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
        let controller = Arc::new(ClusterController::new(
            self_id,
            kv.clone(),
            None,
            members_rx,
        ));
        install_test_process_deadline(&controller);
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: controller.recovery_incarnation(),
            }],
        )
        .unwrap();
        let driver_id = NodeId(1);
        let driver_boot = fence.participant_incarnation(1).unwrap();
        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        install_test_process_authority(&controller, Arc::clone(&backing)).await;
        let driver_kv = Arc::new(InMemoryKv::new(driver_id));
        let (_driver_members_tx, driver_members_rx) = watch::channel(vec![info(self_id.0)]);
        let driver = Arc::new(ClusterController::new_with_recovery_incarnation(
            driver_id,
            driver_kv.clone(),
            driver_kv.clone(),
            None,
            driver_members_rx,
            driver_boot,
        ));
        install_test_process_deadline(&driver);
        let driver_process_term =
            install_test_process_authority(&driver, Arc::clone(&backing)).await;
        let authority = Arc::new(LeaderLeaseStore::new(backing, 10_000));
        let driver_owner = LeaderLeaseOwner {
            node: driver_id,
            boot: driver_boot,
            process_term: driver_process_term,
        };
        let LeaseOutcome::Acquired(driver_lease) =
            authority.begin_new_term(&driver_owner, 0).await.unwrap()
        else {
            panic!("empty recovery test authority must grant the remote leader");
        };
        controller.set_leader_lease_store(Arc::clone(&authority));
        let (_driver_lease_tx, driver_lease_rx) = watch::channel(Some(driver_lease.clone()));
        driver
            .set_leader_lease_watch(
                driver_lease_rx,
                driver_owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        driver.set_leader_lease_store(authority);
        driver.set_active(true);

        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
        registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 7);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .state_backend(Arc::new(ObjectStoreBackend::cluster_shared(
                Arc::new(object_store::memory::InMemory::new()),
                "idle-worker",
                1,
            )))
            .vnode_registry(registry)
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        request_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .unwrap();
        let raw_request = db.pending_recovery_fault.load(Ordering::Acquire);
        let inventory = driver.read_recovery_fault_inventory().await.unwrap();
        let [idle_fault] = inventory.faults() else {
            panic!("one latched request must publish one evidence fault");
        };
        let idle_fault = *idle_fault;
        let evidence_participant = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: controller.recovery_incarnation(),
        };
        let round = RecoveryRound::new(
            5,
            driver_lease.proof(),
            fence.clone(),
            vec![evidence_participant],
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        assert!(controller.recovery_round_requires_current_process_stop(&round));
        controller.publish_checkpoint_assignment_fence(Some(fence));
        driver.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));
        db.set_assignment_snapshot_store(Arc::clone(&assignments));
        let vnodes = AssignmentSnapshot::vnodes_from_vec(&[NodeId(1)]);
        let participants = round.assignment_fence.participants.clone();
        let mut snapshot = AssignmentSnapshot::empty()
            .next_for_participants(vnodes.clone(), participants.clone())
            .unwrap();
        assignments.save_if_absent(&snapshot).await.unwrap();
        while snapshot.version < round.assignment_fence.assignment_version {
            let next = snapshot
                .next_for_participants(vnodes.clone(), participants.clone())
                .unwrap();
            assignments
                .save_if_version(&next, snapshot.version)
                .await
                .unwrap();
            snapshot = next;
        }
        assert!(coordinated_restart_assignment_ready(&db).await);

        driver_kv.seed(
            self_id,
            "control:recovery-incarnation",
            controller.recovery_incarnation().to_string(),
        );
        driver.announce_recover_prepare(&round).await.unwrap();
        driver.announce_stopped(&round, Vec::new()).await.unwrap();
        let prepare = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        };
        kv.seed(
            NodeId(1),
            "control:recover",
            serde_json::to_string(&prepare).unwrap(),
        );
        let mut monitor = RecoveryMonitor::default();
        assert!(monitor.publish_pending_local_fault(&db, &controller).await);
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        let local_fault = monitor.pending_local_fault(&controller).await.unwrap();
        monitor.observe(&db, &controller, local_fault).await;

        assert!(controller.is_recovering());
        assert!(db.cluster_intake_fenced());
        assert_eq!(
            monitor.stopped_for.as_ref().map(|(stopped, _)| stopped),
            Some(&round)
        );
        assert!(monitor.restored_for.is_none());
        assert_eq!(
            controller
                .read_recovery_fault_inventory()
                .await
                .unwrap()
                .faults(),
            &[idle_fault]
        );

        let evidence_stopped =
            RecoveryStoppedReport::new(&round, evidence_participant, Vec::new()).unwrap();
        driver_kv.seed(
            self_id,
            "control:recovery-stopped",
            serde_json::to_string(&evidence_stopped).unwrap(),
        );
        assert!(matches!(
            wait_stopped_quorum(&driver, &round, Duration::from_secs(1)).await,
            StoppedQuorum::Reached(_)
        ));
        driver.announce_recover_start(&round, 3).await.unwrap();
        driver.announce_recover_release(&round, 3).await.unwrap();
        let pending_release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: 3 },
        };
        driver
            .announce_release_ready(&pending_release)
            .await
            .unwrap();
        assert!(matches!(
            driver
                .try_commit_recover_release(&pending_release)
                .await
                .unwrap(),
            ReleaseCommitStatus::Committed { .. }
        ));
        assert_eq!(controller.read_local_fault_report().await.unwrap(), None);
        assert!(controller
            .read_recovery_fault_inventory()
            .await
            .unwrap()
            .faults()
            .is_empty());
        assert_eq!(
            db.pending_recovery_fault.load(Ordering::Acquire),
            raw_request,
            "atomic tombstoning must not clear terminal discovery"
        );
        kv.seed(
            NodeId(1),
            "control:recovery-incarnation",
            controller.recovery_incarnation().to_string(),
        );
        let local_fault = monitor.pending_local_fault(&controller).await.unwrap();
        monitor.observe(&db, &controller, local_fault).await;
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);

        assert!(pending.is_empty());
        assert!(!controller.is_recovering());
        assert!(db.cluster_intake_fenced());
        assert!(monitor.stopped_for.is_none());
        assert!(monitor.restored_for.is_none());
        assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
        assert_eq!(monitor.applied_gen, round.id.generation);
        assert_eq!(
            monitor.handled_faults.get(&self_id),
            Some(&idle_fault.sequence)
        );
    }

    #[tokio::test]
    async fn retry_request_restores_recovery_fence_before_reporting() {
        let (controller, _members_tx, _kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        controller.set_recovering(false);

        hold_intake_and_request_retry(&db, &controller, 9, false).await;

        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert_ne!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        assert_ne!(inventory.revision(), 0);
        assert!(inventory.faults().iter().any(|fault| fault.sequence != 0));
    }

    #[tokio::test]
    async fn transient_fault_authority_unavailability_becomes_a_durable_recovery_trigger() {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        install_test_process_deadline(&controller);
        let authority_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::clone(&authority_store))
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        let mut monitor = RecoveryMonitor::default();

        assert!(monitor.pending_faults(&controller).await.is_err());
        install_test_leader_authority(&controller, authority_store).await;
        monitor.hold_for_unknown_fault_audit(&db, &controller).await;

        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert!(!monitor.fault_audit_unknown);
        let pending = monitor.pending_faults(&controller).await.unwrap();
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        assert_ne!(inventory.revision(), 0);
        assert_eq!(pending.as_slice(), inventory.faults());
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].reporter, self_id);
        assert_ne!(pending[0].sequence, 0);
    }

    #[tokio::test]
    async fn coordinated_restart_requires_a_committed_assignment_head() {
        let (controller, _members_tx, _kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();

        assert!(!coordinated_restart_assignment_ready(&db).await);

        let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));
        db.set_assignment_snapshot_store(Arc::clone(&assignments));
        assert!(!coordinated_restart_assignment_ready(&db).await);

        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        };
        let owners = [NodeId(1)];
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&owners),
                vec![participant],
            )
            .unwrap();
        assignments.save_if_absent(&committed).await.unwrap();
        assert!(coordinated_restart_assignment_ready(&db).await);

        let draining = committed
            .next_draining(
                AssignmentSnapshot::vnodes_from_vec(&owners),
                vec![participant],
                LeaderProof {
                    owner: LeaderProofOwner {
                        node_id: 1,
                        boot_id: participant.boot_incarnation,
                        process_term: 1,
                    },
                    fencing_token: 1,
                },
            )
            .unwrap();
        assignments
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();
        assert!(!coordinated_restart_assignment_ready(&db).await);
    }

    #[tokio::test]
    async fn recovery_quorum_requires_the_exact_round_and_target() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let exact_round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &exact_round, 4).await;
        let start = start(exact_round, 4);
        controller.announce_recovered(&start).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &start, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Reached);
    }

    #[tokio::test]
    async fn newer_recovery_ack_supersedes_instead_of_satisfying_old_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let expected_round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &expected_round, 4).await;
        let expected = start(expected_round, 4);
        report_test_fault(&controller).await;
        let newer = start(round_for_current_faults(&controller, 8, &[1]).await, 4);
        controller.announce_recovered(&newer).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Superseded);
    }

    #[tokio::test]
    async fn same_generation_nonce_conflict_never_satisfies_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let expected_round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &expected_round, 4).await;
        let expected = start(expected_round, 4);
        let conflicting = start(round_for_current_faults(&controller, 7, &[1]).await, 4);
        controller.announce_recovered(&conflicting).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Conflicted);
    }

    #[tokio::test]
    async fn different_start_target_never_satisfies_restore_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let exact_round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &exact_round, 4).await;
        let expected = start(exact_round.clone(), 4);
        controller
            .announce_recovered(&start(exact_round, 5))
            .await
            .unwrap();

        let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Conflicted);
    }

    #[tokio::test]
    async fn an_exact_active_start_is_not_misclassified_as_an_orphan() {
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        let original_fault = report_test_fault(&controller).await;
        let exact_round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &exact_round, 4).await;
        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
        registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 7);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .build()
            .await
            .unwrap();
        let mut monitor = RecoveryMonitor {
            applied_gen: exact_round.id.generation,
            stopped_for: Some((
                exact_round.clone(),
                tokio::time::Instant::now() - ORPHAN_STOP_TIMEOUT - Duration::from_secs(1),
            )),
            ..RecoveryMonitor::default()
        };

        let local_fault = monitor.pending_local_fault(&controller).await.unwrap();
        monitor.observe(&db, &controller, local_fault).await;

        assert_eq!(
            monitor.stopped_for.as_ref().map(|(round, _)| round),
            Some(&exact_round)
        );
        assert_eq!(
            controller.read_local_fault_report().await.unwrap(),
            Some(original_fault.sequence),
            "the active round's original fault must remain unchanged until Release"
        );
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restore_quorum_does_not_shrink_when_membership_changes() {
        let (controller, members_tx, kv) = controller(vec![info(2)]).await;
        report_test_fault(&controller).await;
        let exact_round = round_for_current_faults(&controller, 7, &[1, 2]).await;
        activate_start(&controller, &kv, &exact_round, 4).await;
        let start = start(exact_round, 4);
        controller.announce_recovered(&start).await.unwrap();
        members_tx.send(Vec::new()).unwrap();

        let outcome = wait_restored_quorum(&controller, &start, Duration::from_millis(20)).await;

        assert_eq!(outcome, RecoveryQuorum::ParticipantsChanged);
    }

    #[tokio::test]
    async fn prepare_quorum_fails_when_its_assignment_certificate_changes() {
        let (controller, members_tx, kv) = controller(vec![info(2)]).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1, 2]).await;
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller
            .announce_stopped(&round, Vec::new())
            .await
            .unwrap();
        members_tx.send(Vec::new()).unwrap();

        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;

        assert_eq!(outcome, StoppedQuorum::ParticipantsChanged);
    }

    #[tokio::test]
    async fn missing_prepare_participant_obeys_the_hard_quorum_deadline() {
        let (controller, _members_tx, kv) = controller(vec![info(2)]).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1, 2]).await;
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller
            .announce_stopped(&round, Vec::new())
            .await
            .unwrap();

        let started = std::time::Instant::now();
        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_millis(25)).await;

        assert_eq!(outcome, StoppedQuorum::TimedOut);
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "quorum wait exceeded its single hard deadline: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn stopped_quorum_includes_non_owner_evidence_reporters() {
        let (controller, evidence_controller, kv) = driver_and_follower().await;
        let owner = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        };
        let evidence = CheckpointParticipant {
            node_id: 2,
            boot_incarnation: evidence_controller.recovery_incarnation(),
        };
        report_test_fault(&controller).await;
        report_test_fault(&evidence_controller).await;
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        let fence = CheckpointAssignmentFence::from_owner_map(7, &[1], vec![owner]).unwrap();
        let round = RecoveryRound::new(
            8,
            controller.capture_leader_proof().unwrap(),
            fence,
            vec![evidence],
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        publish_round_roster(&controller, &kv, &round).await;
        kv.seed(
            NodeId(2),
            "control:recovery-incarnation",
            evidence.boot_incarnation.to_string(),
        );
        controller.announce_recover_prepare(&round).await.unwrap();
        controller
            .announce_stopped(&round, Vec::new())
            .await
            .unwrap();
        let peer = RecoveryStoppedReport::new(&round, evidence, Vec::new()).unwrap();
        kv.seed(
            NodeId(2),
            "control:recovery-stopped",
            serde_json::to_string(&peer).unwrap(),
        );

        let StoppedQuorum::Reached(reports) =
            wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await
        else {
            panic!("owner and evidence reports must complete the stopped quorum");
        };
        assert_eq!(
            reports
                .iter()
                .map(|report| report.publisher().node_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn prepare_rejects_an_omitted_available_evidence_reporter() {
        let (controller, evidence_controller, kv) = driver_and_follower().await;
        let owner = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        };
        let evidence_boot = evidence_controller.recovery_incarnation();
        report_test_fault(&controller).await;
        report_test_fault(&evidence_controller).await;
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        let fence = CheckpointAssignmentFence::from_owner_map(7, &[1], vec![owner]).unwrap();
        let round = RecoveryRound::new(
            8,
            controller.capture_leader_proof().unwrap(),
            fence,
            Vec::new(),
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        publish_round_roster(&controller, &kv, &round).await;
        kv.seed(
            NodeId(2),
            "control:recovery-incarnation",
            evidence_boot.to_string(),
        );

        let error = controller
            .announce_recover_prepare(&round)
            .await
            .unwrap_err();
        assert!(error.contains("evidence roster changed"), "{error}");
    }

    #[tokio::test]
    async fn checkpoint_disabled_empty_stopped_inventory_needs_no_coordinator() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 8, &[1]).await;
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        let report = RecoveryStoppedReport::new(
            &round,
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: controller.recovery_incarnation(),
            },
            Vec::new(),
        )
        .unwrap();
        let db = Arc::new(LaminarDB::open().unwrap());
        assert!(db.coordinator.lock().await.is_none());

        settle_stopped_prepared_witnesses(&db, &controller, &round, &[report])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn outcome_floor_cannot_settle_an_incomparable_prepared_witness() {
        use crate::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
        use laminar_core::checkpoint::{
            CheckpointWatermark, ClusterRecoveryCapsule, ParticipantRecoveryRef, PipelineIdentity,
            PreparedCheckpointWitness, CLUSTER_RECOVERY_CAPSULE_VERSION,
        };
        use laminar_core::checkpoint_decision::{CheckpointDecisionStore, CheckpointVerdict};
        use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let decisions = CheckpointDecisionStore::new(Arc::clone(&backing));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let (controller, _members_tx, kv) = controller_on(Vec::new(), backing).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 8, &[1]).await;
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        let authority = controller.checkpoint_authority().unwrap();
        authority
            .record_cluster_outcome(
                &round.leader_proof,
                5,
                1,
                round.assignment_fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        let digest = "11".repeat(32);
        let capsule = decisions
            .create_recovery_capsule(&ClusterRecoveryCapsule {
                version: CLUSTER_RECOVERY_CAPSULE_VERSION,
                attempt: laminar_core::state::CheckpointAttempt::new(7, 3),
                deployment_id: deployment_id.clone(),
                pipeline_identity: PipelineIdentity::empty(),
                assignment_fence: round.assignment_fence.clone(),
                seal_inventory_sha256: digest.clone(),
                participants: vec![ParticipantRecoveryRef {
                    participant_id: 1,
                    readiness_sha256: digest.clone(),
                    manifest_sha256: digest.clone(),
                    portable_state_sha256: digest.clone(),
                }],
                source_offsets: std::collections::BTreeMap::new(),
                source_metadata: std::collections::BTreeMap::new(),
                source_assignment_versions: std::collections::BTreeMap::new(),
                source_watermarks: std::collections::BTreeMap::new(),
                cluster_watermark: CheckpointWatermark::Uninitialized,
                recovery_watermark_frontier: None,
                portable_state_sha256: digest,
            })
            .await
            .unwrap();
        authority
            .record_cluster_outcome(
                &round.leader_proof,
                7,
                3,
                round.assignment_fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule),
            )
            .await
            .unwrap();
        authority
            .prune_cluster_outcomes_before(&round.leader_proof, 7, |_| async {
                Ok::<(), String>(())
            })
            .await
            .unwrap();

        let checkpoint_dir = tempfile::tempdir().unwrap();
        let store =
            Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
        let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        coordinator
            .bind_deployment_id(deployment_id.clone())
            .unwrap();
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        let db = Arc::new(LaminarDB::open().unwrap());
        *db.coordinator.lock().await = Some(coordinator);

        let witness = PreparedCheckpointWitness::new(
            laminar_core::state::CheckpointAttempt::new(6, 99),
            1,
            deployment_id,
            PipelineIdentity::empty(),
        )
        .unwrap();
        let report = RecoveryStoppedReport::new(
            &round,
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: controller.recovery_incarnation(),
            },
            vec![witness],
        )
        .unwrap();

        let error = settle_stopped_prepared_witnesses(&db, &controller, &round, &[report])
            .await
            .expect_err("the outcome floor cannot settle an incomparable Prepared witness");
        assert!(error.contains("not strictly dominated"), "{error}");
        assert!(authority.cluster_outcome(6).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ambiguity_audit_finds_an_outcome_that_becomes_visible_after_the_write_returns() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 8, &[1]).await;
        publish_round_roster(&controller, &kv, &round).await;
        let authority = controller.checkpoint_authority().unwrap();
        let writer = {
            let authority = Arc::clone(&authority);
            let proof = round.leader_proof.clone();
            let fence = round.assignment_fence.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                authority
                    .record_cluster_outcome(&proof, 6, 60, fence, CheckpointVerdict::Abort, None)
                    .await
                    .unwrap()
            })
        };

        let outcome = audit_cluster_outcome_until(
            authority.as_ref(),
            6,
            tokio::time::Instant::now() + Duration::from_millis(250),
        )
        .await
        .unwrap();
        assert_eq!(outcome.checkpoint_id, 60);
        assert_eq!(outcome.verdict, CheckpointVerdict::Abort);
        assert!(matches!(
            writer.await.unwrap(),
            RecordOutcomeResult::Created(_) | RecordOutcomeResult::Unchanged(_)
        ));
    }

    #[tokio::test]
    async fn ambiguity_audit_fails_when_no_immutable_winner_appears() {
        let (controller, _members_tx, _kv) = controller(Vec::new()).await;
        let authority = controller.checkpoint_authority().unwrap();

        let error = audit_cluster_outcome_until(
            authority.as_ref(),
            99,
            tokio::time::Instant::now() + Duration::from_millis(25),
        )
        .await
        .unwrap_err();
        assert!(error.contains("found no immutable outcome"), "{error}");
    }

    #[tokio::test]
    async fn restarted_same_id_process_invalidates_persisted_stop_ack() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 9, &[1]).await;
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller
            .announce_stopped(&round, Vec::new())
            .await
            .unwrap();

        let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
        let replacement = ClusterController::new(NodeId(1), kv, None, replacement_rx);
        replacement.publish_recovery_incarnation().await.unwrap();

        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;
        assert_eq!(outcome, StoppedQuorum::ParticipantsChanged);
    }

    #[tokio::test]
    async fn takeover_settles_peer_only_prepare_and_fences_predecessor_commit() {
        use crate::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
        use laminar_core::checkpoint::{PipelineIdentity, PreparedCheckpointWitness};
        use laminar_core::checkpoint_decision::{
            CheckpointDecisionStore, CheckpointVerdict, RecordOutcomeResult,
        };
        use laminar_core::cluster::control::ClusterCheckpointAuthorityError;
        use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&backing)));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));

        let predecessor_boot = uuid::Uuid::from_u128(20);
        let process_authority =
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_secs(60)).unwrap();
        let ProcessLeaseOutcome::Acquired(predecessor_process_lease) = process_authority
            .store_for(NodeId(2))
            .try_acquire(predecessor_boot, 0)
            .await
            .unwrap()
        else {
            panic!("predecessor process must acquire its stable-node term");
        };
        let predecessor_owner = LeaderLeaseOwner {
            node: NodeId(2),
            boot: predecessor_boot,
            process_term: predecessor_process_lease.term,
        };
        let LeaseOutcome::Acquired(predecessor_lease) = authority
            .begin_new_term(&predecessor_owner, 0)
            .await
            .unwrap()
        else {
            panic!("predecessor must acquire the first term");
        };

        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(vec![info(2)]);
        let controller = Arc::new(ClusterController::new(
            self_id,
            kv.clone(),
            None,
            members_rx,
        ));
        install_test_process_deadline(&controller);
        let successor_process_term =
            install_test_process_authority(&controller, Arc::clone(&backing)).await;
        let successor_owner = LeaderLeaseOwner {
            node: self_id,
            boot: controller.recovery_incarnation(),
            process_term: successor_process_term,
        };
        let observation = authority
            .observe_rival(&successor_owner, &predecessor_lease)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let LeaseOutcome::Acquired(successor_lease) = authority
            .try_takeover(
                &successor_owner,
                &observation,
                predecessor_lease.expires_at_ms + 1,
            )
            .await
            .unwrap()
        else {
            panic!("successor must acquire the expired predecessor term");
        };
        let (_lease_tx, lease_rx) = watch::channel(Some(successor_lease.clone()));
        controller
            .set_leader_lease_watch(
                lease_rx,
                successor_owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));
        controller.set_active(true);

        let participants = vec![
            CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: controller.recovery_incarnation(),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: predecessor_boot,
            },
        ];
        let fence = CheckpointAssignmentFence::from_owner_map(7, &[1, 2], participants).unwrap();
        report_test_fault(&controller).await;
        let inventory = controller.read_recovery_fault_inventory().await.unwrap();
        let round = RecoveryRound::new(
            9,
            successor_lease.proof(),
            fence.clone(),
            Vec::new(),
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        controller.publish_recovery_incarnation().await.unwrap();
        kv.seed(
            NodeId(2),
            "control:recovery-incarnation",
            predecessor_boot.to_string(),
        );
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
        controller.announce_recover_prepare(&round).await.unwrap();

        let checkpoint_dir = tempfile::tempdir().unwrap();
        let store =
            Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
        let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        coordinator
            .set_decision_store(Arc::clone(&decisions))
            .unwrap();
        coordinator
            .bind_deployment_id(deployment_id.clone())
            .unwrap();
        coordinator.set_cluster_controller(Arc::clone(&controller));

        let db = Arc::new(LaminarDB::open().unwrap());
        *db.coordinator.lock().await = Some(coordinator);
        assert!(
            db.coordinator
                .lock()
                .await
                .as_ref()
                .unwrap()
                .prepared_checkpoint_witnesses()
                .await
                .unwrap()
                .is_empty(),
            "the promoted driver must not rely on leader-local Prepared state"
        );
        let local = RecoveryStoppedReport::new(
            &round,
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: controller.recovery_incarnation(),
            },
            Vec::new(),
        )
        .unwrap();
        let witness = PreparedCheckpointWitness::new(
            laminar_core::state::CheckpointAttempt::new(6, 60),
            2,
            deployment_id,
            PipelineIdentity::empty(),
        )
        .unwrap();
        let peer = RecoveryStoppedReport::new(
            &round,
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: predecessor_boot,
            },
            vec![witness],
        )
        .unwrap();

        settle_stopped_prepared_witnesses(&db, controller.as_ref(), &round, &[local, peer])
            .await
            .unwrap();

        let outcome = authority.cluster_outcome(6).await.unwrap().unwrap();
        assert_eq!(outcome.checkpoint_id, 60);
        assert_eq!(outcome.verdict, CheckpointVerdict::Abort);
        assert_eq!(
            outcome.leader_proof.as_ref(),
            Some(&successor_lease.proof())
        );

        let delayed = authority
            .record_cluster_outcome(
                &predecessor_lease.proof(),
                6,
                60,
                fence,
                CheckpointVerdict::Commit,
                None,
            )
            .await;
        assert!(matches!(
            delayed,
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert!(matches!(
            authority
                .record_cluster_outcome(
                    &successor_lease.proof(),
                    6,
                    60,
                    round.assignment_fence,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap(),
            RecordOutcomeResult::Unchanged(_)
        ));
    }

    #[tokio::test]
    async fn release_commit_rejects_a_post_ready_fault() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 11, &[1]).await;
        activate_start(&controller, &kv, &round, 6).await;
        let start = start(round.clone(), 6);
        controller.announce_recovered(&start).await.unwrap();
        controller
            .announce_recover_release(&round, 6)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Release { epoch: 6 },
        };
        controller.announce_release_ready(&release).await.unwrap();
        let post_ready_fault = report_test_fault(&controller).await;

        let RecoveryControlError::Superseded(reason) = controller
            .try_commit_recover_release(&release)
            .await
            .unwrap_err()
        else {
            panic!("a newer fault must block the release terminal");
        };
        assert!(reason.contains("fault set changed"));
        assert_eq!(
            controller
                .read_recovery_fault_inventory()
                .await
                .unwrap()
                .faults(),
            &[post_ready_fault]
        );
        assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
    }

    #[tokio::test]
    async fn shuffle_cutoff_failure_never_publishes_release_readiness() {
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &round, 4).await;
        let start = start(round.clone(), 4);
        controller.announce_recovered(&start).await.unwrap();
        controller
            .announce_recover_release(&round, 4)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: 4 },
        };
        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
        registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 7);
        let receiver = Arc::new(
            ShuffleReceiver::bind(
                1,
                "127.0.0.1:0".parse().unwrap(),
                controller.recovery_incarnation(),
            )
            .await
            .unwrap(),
        );
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .shuffle_sender(Arc::new(ShuffleSender::new(
                controller.instance_id().0,
                controller.recovery_incarnation(),
            )))
            .shuffle_receiver(receiver)
            .build()
            .await
            .unwrap();
        db.set_source_gate(true);
        controller.set_recovering(true);
        let mut monitor = RecoveryMonitor {
            restored_for: Some((start, tokio::time::Instant::now())),
            ..RecoveryMonitor::default()
        };

        assert!(
            !monitor
                .release_after_readiness_quorum(&db, &controller, &release, 4)
                .await
        );
        assert!(kv.scan("control:recovery-release-ready").await.is_empty());
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert!(monitor.restored_for.is_some());
    }

    #[tokio::test]
    async fn active_assignment_drain_blocks_recovery_release_readiness() {
        use laminar_core::checkpoint::AssignmentDrainTransition;
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &round, 4).await;
        let start = start(round.clone(), 4);
        controller.announce_recovered(&start).await.unwrap();
        controller
            .announce_recover_release(&round, 4)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: 4 },
        };
        let target = CheckpointAssignmentFence::from_owner_map(
            8,
            &[1],
            round.assignment_fence.participants.clone(),
        )
        .unwrap();
        let transition = AssignmentDrainTransition::new(
            round.assignment_fence.clone(),
            target,
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
        controller.publish_checkpoint_drain_transition(Some(transition.clone()));

        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
        registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 7);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .build()
            .await
            .unwrap();
        db.set_source_gate(true);
        controller.set_recovering(true);
        let mut monitor = RecoveryMonitor {
            restored_for: Some((start, tokio::time::Instant::now())),
            ..RecoveryMonitor::default()
        };

        assert!(
            !monitor
                .release_after_readiness_quorum(&db, &controller, &release, 4)
                .await
        );
        assert_eq!(controller.checkpoint_drain_transition(), Some(transition));
        assert!(kv.scan("control:recovery-release-ready").await.is_empty());
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert!(monitor.restored_for.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_closure_wins_while_recovery_release_waits_to_open_intake() {
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        report_test_fault(&controller).await;
        let round = round_for_current_faults(&controller, 7, &[1]).await;
        activate_start(&controller, &kv, &round, 4).await;
        let start = start(round.clone(), 4);
        controller.announce_recovered(&start).await.unwrap();
        controller
            .announce_recover_release(&round, 4)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: 4 },
        };

        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
        registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 7);
        let boot = controller.recovery_incarnation();
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), boot)
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, boot));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .shuffle_sender(sender)
            .shuffle_receiver(receiver)
            .build()
            .await
            .unwrap();
        db.set_source_gate(true);
        db.set_shuffle_recovery_gen(7);
        controller.set_recovering(true);
        let execution = Arc::clone(&db.rotation_execution_fence).read_owned().await;
        let releasing = {
            let db = Arc::clone(&db);
            let controller = Arc::clone(&controller);
            let release = release.clone();
            tokio::spawn(async move {
                let mut monitor = RecoveryMonitor {
                    restored_for: Some((start, tokio::time::Instant::now())),
                    ..RecoveryMonitor::default()
                };
                let opened = monitor
                    .release_after_readiness_quorum(&db, &controller, &release, 4)
                    .await;
                (opened, monitor.restored_for)
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if db.assignment_adoption_lock.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("release must reach the serialized activation boundary");

        db.set_source_gate(true);
        controller.publish_checkpoint_assignment_fence(None);
        db.suspend_shuffle_assignment_fence();
        drop(execution);

        let (opened, restored_for) = releasing.await.unwrap();
        assert!(!opened);
        assert!(restored_for.is_some());
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert_eq!(controller.checkpoint_assignment_fence(7), None);
    }
}
