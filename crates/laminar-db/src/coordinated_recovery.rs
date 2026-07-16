//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode; always on).
//!
//! Two-phase, stop-the-world: the leader announces `Prepare` (every data participant stops and
//! acks), reads
//! the recovery target from the now-quiesced decision store, then announces `Start`. Prepared
//! artifacts above that decision are unusable without a matching decision and normal retention
//! collects them. No committed epoch means target 0 — a fresh start from initial offsets. No node
//! resumes intake until the whole round has restored and the assignment has settled.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::FutureExt as _;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::runtime::Handle;

use laminar_core::cluster::control::controller::{
    RecoveryAnnouncement, RecoveryFault, RecoveryRound,
};
use laminar_core::cluster::control::{
    ClusterController, RecoverPhase, RecoveryControlError, ReleaseCommitStatus,
};
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

/// Healthy-state monitor cadence. Every worker point-reads only its own durable fault slot and the
/// current recovery driver; the leader alone scans the cluster fault set.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const STOP_QUORUM_TIMEOUT: Duration = Duration::from_secs(30);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
const RELEASE_PROTOCOL_TIMEOUT: Duration = Duration::from_secs(60);
const DECISION_IO_TIMEOUT: Duration = Duration::from_secs(15);
const RECOVERY_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a node stopped by `Prepare` waits for a `Start` before giving up on the round.
const ORPHAN_STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery target meaning "no committed cut exists": start fresh.
const GENESIS: u64 = 0;

/// Boot-bound, process-local fault sequence. Wall-clock rollback cannot repeat it, and a same-id
/// process restart changes the UUID-derived prefix.
fn next_fault_sequence(controller: &ClusterController) -> u64 {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let boot = controller.recovery_incarnation().as_u128();
    let prefix = u64::try_from(boot & u128::from(u64::MAX)).unwrap_or_default()
        ^ u64::try_from(boot >> 64).unwrap_or_default();
    let sequence = SEQUENCE.fetch_add(1, Ordering::AcqRel);
    prefix
        .rotate_left(17)
        .wrapping_add(sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .max(1)
}

/// Publish a fault so the leader drives a global restart; this node's monitor then
/// restores it on observing the round.
pub(crate) async fn report_local_fault(controller: &ClusterController) -> Result<(), String> {
    let seq = next_fault_sequence(controller);
    match tokio::time::timeout(DECISION_IO_TIMEOUT, controller.report_fault(seq)).await {
        Ok(Ok(())) => {
            tracing::warn!(seq, "reported local fault for coordinated cluster recovery");
            Ok(())
        }
        Ok(Err(error)) => {
            tracing::error!(seq, %error, "could not persist local recovery fault");
            Err(error)
        }
        Err(_) => Err("local recovery fault publication timed out".into()),
    }
}

/// A fresh identity, not the boot nonce: the leader already handled that one, so only a new value
/// drives the round an orphaned node needs.
async fn report_fresh_fault(controller: &ClusterController) -> bool {
    let seq = next_fault_sequence(controller);
    match tokio::time::timeout(DECISION_IO_TIMEOUT, controller.report_fault(seq)).await {
        Ok(Ok(())) => {
            tracing::warn!(seq, "reported fresh fault after an orphaned recovery round");
            true
        }
        Ok(Err(error)) => {
            tracing::error!(seq, %error, "could not persist fresh recovery fault");
            false
        }
        Err(_) => {
            tracing::error!(seq, "fresh recovery fault publication timed out");
            false
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
                report_fresh_fault(&controller).await;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryQuorum {
    Reached,
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

            if self.fault_audit_unknown {
                self.hold_fault_fence(&db, &controller);
                if report_fresh_fault(&controller).await {
                    self.fault_audit_unknown = false;
                }
                continue;
            }
            let local_pending = match self.pending_local_fault(&controller).await {
                Ok(pending) => pending.into_iter().collect::<Vec<_>>(),
                Err(error) => {
                    tracing::error!(%error, "could not read the local recovery fault report");
                    self.hold_for_unknown_fault_audit(&db, &controller).await;
                    continue;
                }
            };
            self.hold_for_pending_fault(&db, &controller, &local_pending);
            self.observe(&db, &controller).await;
            if !controller.is_leader() {
                continue;
            }
            let reported = match self.reported_faults(&controller).await {
                Ok(reported) => reported,
                Err(error) => {
                    tracing::error!(%error, "could not read cluster recovery fault reports");
                    self.hold_for_unknown_fault_audit(&db, &controller).await;
                    continue;
                }
            };
            let pending = self.unhandled_faults(&reported);
            self.hold_for_pending_fault(&db, &controller, &pending);
            // A committed release is immutable authority state, not a mutable driver-slot phase.
            // Do not overwrite it while any covered fault report is still waiting to consume it.
            let terminal = if reported.is_empty() {
                Ok(None)
            } else {
                match tokio::time::timeout(
                    DECISION_IO_TIMEOUT,
                    controller.latest_committed_recover_release(),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(RecoveryControlError::Uncertain(
                        "recovery release authority read timed out".into(),
                    )),
                }
            };
            match terminal {
                Ok(Some(terminal))
                    if reported
                        .iter()
                        .any(|fault| terminal.round.faults.contains(fault))
                        && round_assignment_is_current(&db, &controller, &terminal.round) =>
                {
                    continue;
                }
                Err(RecoveryControlError::Uncertain(error)) => {
                    tracing::warn!(%error, "recovery release authority is temporarily unreadable");
                    continue;
                }
                Err(RecoveryControlError::Conflict(error)) => {
                    tracing::error!(%error, "recovery release authority is invalid");
                    self.hold_for_unknown_fault_audit(&db, &controller).await;
                    continue;
                }
                Err(RecoveryControlError::Superseded(_)) | Ok(_) => {}
            }

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
                            RecoverPhase::Release { .. } if active.round.faults == pending => {
                                // `observe` owns retrying the retained prepare/commit barrier. Do not
                                // overwrite it merely because its covered reports remain intentionally
                                // nonzero until the terminal is consumed locally.
                                continue;
                            }
                            RecoverPhase::Release { .. }
                            | RecoverPhase::ReleaseCommitted { .. } => {}
                            RecoverPhase::Prepare | RecoverPhase::Start { .. }
                                if active.round.id.driver == controller.instance_id() =>
                            {
                                let _ = controller.clear_recover(&active.round).await;
                                if !report_fresh_fault(&controller).await {
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
                self.drive_round(&db, &controller, pending).await;
            }
        }
    }

    /// Act on the leader's announcement: stop on `Prepare`, restore on `Start`.
    async fn observe(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
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
            let local_fault = tokio::time::timeout(
                DECISION_IO_TIMEOUT,
                controller.read_local_fault_report_control(),
            )
            .await;
            match local_fault {
                Err(_) => {
                    controller.set_recovering(true);
                    db.set_source_gate(true);
                    return;
                }
                Ok(result) => match result {
                    Ok(Some(sequence)) => {
                        let terminal = tokio::time::timeout(
                            DECISION_IO_TIMEOUT,
                            controller.latest_committed_recover_release(),
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
                                    if terminal.round.fault_sequence(controller.instance_id())
                                        == Some(sequence)
                                        && !controller.recovery_round_contains_current_process(
                                            &terminal.round,
                                        )
                                        && round_assignment_is_current(
                                            db,
                                            controller,
                                            &terminal.round,
                                        ) =>
                                {
                                    self.last_protocol_error = None;
                                    self.observe_nonparticipant_release(db, controller, &terminal)
                                        .await;
                                    return;
                                }
                                Ok(_) | Err(RecoveryControlError::Superseded(_)) => {}
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
                    Ok(None) => {}
                    Err(error) => {
                        match error {
                            RecoveryControlError::Uncertain(error) => {
                                controller.set_recovering(true);
                                db.set_source_gate(true);
                                tracing::warn!(%error, "local recovery fault is temporarily unreadable");
                            }
                            RecoveryControlError::Conflict(error)
                            | RecoveryControlError::Superseded(error) => {
                                self.hold_for_protocol_error(db, controller, error).await;
                            }
                        }
                        return;
                    }
                },
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
        let current = current.filter(|announcement| {
            controller.recovery_round_contains_current_process(&announcement.round)
        });
        match current {
            Some(RecoveryAnnouncement {
                round,
                phase: RecoverPhase::Prepare,
            }) if round.id.generation > self.applied_gen
                && self.stopped_for.as_ref().map(|(stopped, _)| stopped) != Some(&round) =>
            {
                self.observe_prepare(db, controller, round).await;
            }
            Some(
                start @ RecoveryAnnouncement {
                    phase: RecoverPhase::Start { epoch },
                    ..
                },
            ) if start.round.id.generation > self.applied_gen => {
                self.observe_start(db, controller, start, epoch).await;
            }
            Some(
                release @ RecoveryAnnouncement {
                    phase: RecoverPhase::Release { epoch },
                    ..
                },
            ) => {
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
            if !report_fresh_fault(controller).await {
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
        if !stop_and_purge(db).await {
            tracing::error!(
                gen,
                "recovery prepare could not quiesce this node; withholding stopped acknowledgement"
            );
            return;
        }
        if let Err(error) = controller.announce_stopped(&round).await {
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
            if !report_fresh_fault(controller).await {
                self.fault_audit_unknown = true;
            }
        }
    }

    /// An ownerless worker consumes only a leader-fenced terminal committed after every data
    /// owner prepared while fenced.
    /// It never joins the frozen data quorum, restarts state, acknowledges recovery, or opens its
    /// assignment-closed source gate.
    async fn observe_nonparticipant_release(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &RecoveryAnnouncement,
    ) {
        let deadline = tokio::time::Instant::now() + RELEASE_PROTOCOL_TIMEOUT;
        let RecoverPhase::ReleaseCommitted { epoch } = release.phase else {
            return;
        };
        if !controller.process_lease_is_live()
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
        let mut backoff = Duration::from_millis(25);
        let release_guard = loop {
            match tokio::time::timeout_at(
                deadline,
                controller
                    .begin_recovery_release(release.round.fault_sequence(controller.instance_id())),
            )
            .await
            {
                Ok(Ok(Some(guard))) => break guard,
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
                        "ownerless recovery fault conflicts with the committed Release"
                    );
                    if !report_fresh_fault(controller).await {
                        self.fault_audit_unknown = true;
                    }
                    return;
                }
                Ok(Err(RecoveryControlError::Uncertain(_))) | Err(_) => return,
            }
        };
        self.record_released_faults(&release.round);
        self.applied_gen = release.round.id.generation;
        controller.set_recovering(false);
        db.set_source_gate(true);
        drop(release_guard);
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
                if !report_fresh_fault(controller).await {
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

    /// Fault reports not yet handled. `0` means no fault. Faults become handled only when an exact
    /// terminal `Release` covers their node and boot-bound sequence; retaining that sequence also
    /// suppresses a stale post-clear observation without hiding a distinct later report.
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
        let mut reported = Vec::new();
        let reports = tokio::time::timeout(DECISION_IO_TIMEOUT, controller.read_fault_reports())
            .await
            .map_err(|_| "cluster recovery fault report read timed out".to_string())??;
        for (node, seq) in reports {
            if seq != 0 {
                reported.push(RecoveryFault {
                    reporter: node,
                    sequence: seq,
                });
            }
        }
        reported.sort_unstable_by_key(|fault| fault.reporter);
        if reported
            .windows(2)
            .any(|pair| pair[0].reporter == pair[1].reporter)
        {
            return Err("duplicate recovery fault-report publisher".into());
        }
        Ok(reported)
    }

    fn unhandled_faults(&self, reported: &[RecoveryFault]) -> Vec<RecoveryFault> {
        reported
            .iter()
            .filter(|fault| self.handled_faults.get(&fault.reporter) != Some(&fault.sequence))
            .cloned()
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

    async fn hold_for_unknown_fault_audit(
        &mut self,
        db: &LaminarDB,
        controller: &ClusterController,
    ) {
        self.fault_audit_unknown = true;
        self.hold_fault_fence(db, controller);
        if report_fresh_fault(controller).await {
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

    fn hold_fault_fence(&mut self, db: &LaminarDB, controller: &ClusterController) {
        let owns_vnodes = db.vnode_registry.lock().as_ref().is_none_or(|registry| {
            registry
                .snapshot()
                .iter()
                .any(|owner| owner.0 == controller.instance_id().0)
        });
        if !owns_vnodes {
            // An ownerless worker has no data-plane authority to rewind or acknowledge. Keep its
            // source gate closed, but do not make it a recovery participant or churn the retained
            // assignment certificate.
            db.set_source_gate(true);
            controller.set_recovering(false);
            self.fault_fenced = false;
            return;
        }
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
        pending: Vec<RecoveryFault>,
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
        let round = match RecoveryRound::new(gen_id, leader_proof, assignment_fence, pending) {
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
        if !stop_and_purge(db).await {
            tracing::error!(
                gen = gen_id,
                "leader could not quiesce its decision writer; abandoning recovery round"
            );
            self.abandon_round(db, controller, &round).await;
            return;
        }
        if let Err(error) = controller.announce_stopped(&round).await {
            tracing::error!(gen = gen_id, %error, "could not acknowledge recovery Prepare");
            self.abandon_round(db, controller, &round).await;
            return;
        }
        match wait_stopped_quorum(controller, &round, STOP_QUORUM_TIMEOUT).await {
            RecoveryQuorum::Reached => {}
            RecoveryQuorum::Superseded | RecoveryQuorum::Conflicted => {
                tracing::warn!(
                    gen = gen_id,
                    "recovery Prepare superseded; yielding old driver"
                );
                let _ = controller.clear_recover(&round).await;
                hold_intake_and_request_retry(db, controller, gen_id, false).await;
                return;
            }
            RecoveryQuorum::ParticipantsChanged | RecoveryQuorum::TimedOut => {
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
        }

        if !driver_owns_prepare(controller, &round).await {
            tracing::warn!(
                gen = gen_id,
                "recovery driver lost ownership before target selection; yielding"
            );
            let _ = controller.clear_recover(&round).await;
            hold_intake_and_request_retry(db, controller, gen_id, false).await;
            return;
        }

        // The world is stopped: the decision store is quiescent, so this read IS the cut — no
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
        // Prepared state above the decision is harmless: every object is namespaced by its
        // globally unique checkpoint ID and every recovery/adoption read is decision-bound.
        // Background retention collects abandoned attempts without an O(store) rewind scan.
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
                controller
                    .begin_recovery_release(release.round.fault_sequence(controller.instance_id())),
            )
            .await
            {
                Ok(Ok(Some(guard))) => break guard,
                Ok(Ok(None)) | Ok(Err(RecoveryControlError::Superseded(_))) => {
                    tracing::error!(
                        gen = release.round.id.generation,
                        "a newer local fault replaced the released recovery fault"
                    );
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
        true
    }
}

/// Stop the pipeline and drop buffered shuffle input, on a dedicated thread (lifecycle
/// futures are `!Send`). `true` means the runtime and every owned decision writer are quiescent.
async fn stop_and_purge(db: &Arc<LaminarDB>) -> bool {
    run_lifecycle(db, |db| async move {
        db.stop_pipeline().await?;
        // Pre-rewind shuffle slices are stale: their senders rewind and replay them, so
        // folding a buffered copy after the rewind double-counts.
        db.purge_shuffle_receiver_buffers();
        Ok(())
    })
    .await
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
        db.start().await
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

/// Both the assignment cut and every participant process must still be the exact frozen round.
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
    report_fresh_fault(controller).await;
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

async fn driver_owns_prepare(controller: &ClusterController, round: &RecoveryRound) -> bool {
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
}

fn frozen_pending<T>(
    round: &RecoveryRound,
    reports: impl IntoIterator<Item = (NodeId, T)>,
    matches: impl Fn(&T) -> bool,
) -> Vec<NodeId> {
    let acked: FxHashSet<NodeId> = reports
        .into_iter()
        .filter(|(node, report)| round.contains(*node) && matches(report))
        .map(|(node, _)| node)
        .collect();
    round
        .participants()
        .into_iter()
        .filter(|node| !acked.contains(node))
        .collect()
}

/// Wait for every member of the immutable certified roster to stop. Membership is checked only
/// for invalidating the driver's certificate; it never grows or shrinks this quorum.
async fn wait_stopped_quorum(
    controller: &ClusterController,
    round: &RecoveryRound,
    timeout: Duration,
) -> RecoveryQuorum {
    let deadline = tokio::time::Instant::now() + timeout;
    match tokio::time::timeout_at(deadline, wait_stopped_quorum_until(controller, round)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::error!(gen = round.id.generation, "recovery stop quorum timed out");
            RecoveryQuorum::TimedOut
        }
    }
}

async fn wait_stopped_quorum_until(
    controller: &ClusterController,
    round: &RecoveryRound,
) -> RecoveryQuorum {
    loop {
        if !controller.is_leader() {
            return RecoveryQuorum::Superseded;
        }
        if controller
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
            Ok(Some(active)) if active.round != *round || active.phase != RecoverPhase::Prepare => {
                return RecoveryQuorum::Conflicted;
            }
            Ok(None) => return RecoveryQuorum::Superseded,
            Ok(Some(_)) => {}
        }
        let Ok(reports) = controller.read_stopped().await else {
            return RecoveryQuorum::Conflicted;
        };
        if reports
            .iter()
            .any(|(node, ack)| round.contains(*node) && ack.id.generation > round.id.generation)
        {
            return RecoveryQuorum::Superseded;
        }
        if reports.iter().any(|(node, ack)| {
            round.contains(*node) && ack.id.generation == round.id.generation && ack != round
        }) {
            return RecoveryQuorum::Conflicted;
        }
        let pending = frozen_pending(round, reports, |ack| ack == round);
        if pending.is_empty() {
            return RecoveryQuorum::Reached;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
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
            round.contains(*node) && ack.round.id.generation > round.id.generation
        }) {
            return RecoveryQuorum::Superseded;
        }
        if reports.iter().any(|(node, ack)| {
            round.contains(*node) && ack.round.id.generation == round.id.generation && ack != start
        }) {
            return RecoveryQuorum::Conflicted;
        }
        let pending = frozen_pending(round, reports, |ack| ack == start);
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
        LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
    };
    use laminar_core::cluster::discovery::{NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    struct FailOnceFaultScanKv {
        inner: InMemoryKv,
        fail: std::sync::atomic::AtomicBool,
    }

    /// Exposes a newer nonzero report to the conditional clear, then models that slot becoming
    /// absent before any accidental verification read. Once the clear observed the newer value,
    /// the Release must remain fenced regardless of what a later read would return.
    struct NewerThenMissingFaultKv {
        inner: InMemoryKv,
        newer_sequence: u64,
        fault_reads: std::sync::atomic::AtomicUsize,
    }

    impl NewerThenMissingFaultKv {
        fn new(local_id: NodeId, newer_sequence: u64) -> Self {
            Self {
                inner: InMemoryKv::new(local_id),
                newer_sequence,
                fault_reads: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn fault_reads(&self) -> usize {
            self.fault_reads.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    impl FailOnceFaultScanKv {
        fn new(local_id: NodeId) -> Self {
            Self {
                inner: InMemoryKv::new(local_id),
                fail: std::sync::atomic::AtomicBool::new(true),
            }
        }
    }

    #[async_trait::async_trait]
    impl ClusterKv for FailOnceFaultScanKv {
        async fn write(&self, key: &str, value: String) {
            self.inner.write(key, value).await;
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.inner.read_from(who, key).await
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.inner.scan(key).await
        }

        async fn scan_checked(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
            if key == "control:fault-report"
                && self.fail.swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return Err("injected recovery fault scan failure".into());
            }
            Ok(self.inner.scan(key).await)
        }
    }

    #[async_trait::async_trait]
    impl ClusterKv for NewerThenMissingFaultKv {
        async fn write(&self, key: &str, value: String) {
            self.inner.write(key, value).await;
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            if key == "control:fault-report" {
                let read = self
                    .fault_reads
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                return (read == 0).then(|| self.newer_sequence.to_string());
            }
            self.inner.read_from(who, key).await
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.inner.scan(key).await
        }
    }

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

    async fn controller(
        peers: Vec<NodeInfo>,
    ) -> (
        ClusterController,
        watch::Sender<Vec<NodeInfo>>,
        Arc<InMemoryKv>,
    ) {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (members_tx, members_rx) = watch::channel(peers);
        let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
        let authority = Arc::new(LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            10_000,
        ));
        let owner = LeaderLeaseOwner {
            node: self_id,
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            panic!("empty recovery test authority must grant leadership");
        };
        let (_lease_tx, lease_rx) = watch::channel(Some(lease));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))));
        controller
            .set_leader_lease_watch(
                lease_rx,
                owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        controller.set_leader_lease_store(authority);
        controller.set_active(true);
        (controller, members_tx, kv)
    }

    fn round(
        controller: &ClusterController,
        generation: u64,
        participants: &[u64],
    ) -> RecoveryRound {
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
            vec![RecoveryFault {
                reporter: NodeId(1),
                sequence: generation,
            }],
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

    fn start(round: RecoveryRound, epoch: u64) -> RecoveryAnnouncement {
        RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Start { epoch },
        }
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
        let self_id = NodeId(2);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
        let controller = Arc::new(ClusterController::new(
            self_id,
            kv.clone(),
            None,
            members_rx,
        ));
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

        kv.seed(NodeId(1), "control:fault-report", "17".into());
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        let held_revision = db.assignment_authority_revision.load(Ordering::Acquire);

        assert_eq!(
            pending,
            vec![RecoveryFault {
                reporter: NodeId(1),
                sequence: 17,
            }]
        );
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

        kv.seed(NodeId(1), "control:fault-report", "0".into());
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        assert!(pending.is_empty());
        assert!(!monitor.fault_fenced);
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());

        kv.seed(NodeId(1), "control:fault-report", "18".into());
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        assert_eq!(
            db.assignment_authority_revision.load(Ordering::Acquire),
            held_revision + 1,
            "a new held-fault period must suspend the replacement authority"
        );
    }

    #[tokio::test]
    async fn released_fault_set_does_not_hide_a_newer_local_report() {
        let (controller, _members_tx, _kv) = controller(Vec::new()).await;
        let round = round(&controller, 7, &[1]);
        controller.report_fault(8).await.unwrap();
        let mut monitor = RecoveryMonitor::default();

        assert!(controller
            .begin_recovery_release(round.fault_sequence(controller.instance_id()))
            .await
            .unwrap()
            .is_none());
        assert_eq!(monitor.handled_faults.get(&NodeId(1)), None);
        assert_eq!(
            monitor.pending_faults(&controller).await.unwrap(),
            vec![RecoveryFault {
                reporter: NodeId(1),
                sequence: 8,
            }]
        );
    }

    #[tokio::test]
    async fn observed_newer_fault_cannot_be_reinterpreted_as_an_idempotent_clear() {
        let (round_controller, _members_tx, _kv) = controller(Vec::new()).await;
        let round = round(&round_controller, 7, &[1]);
        let self_id = NodeId(1);
        let fast = Arc::new(InMemoryKv::new(self_id));
        let durable = Arc::new(NewerThenMissingFaultKv::new(self_id, 8));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = ClusterController::new_with_recovery_incarnation(
            self_id,
            fast,
            durable.clone(),
            None,
            members_rx,
            round_controller.recovery_incarnation(),
        );
        let monitor = RecoveryMonitor::default();

        assert!(controller
            .begin_recovery_release(round.fault_sequence(controller.instance_id()))
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            durable.fault_reads(),
            1,
            "a definitive newer-fault observation must not be weakened by a second read"
        );
        assert_eq!(monitor.handled_faults.get(&self_id), None);
    }

    #[tokio::test]
    async fn release_fault_guard_orders_a_new_report_after_local_gate_transition() {
        let (controller, _members_tx, _kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        controller.report_fault(7).await.unwrap();
        let release_guard = controller
            .begin_recovery_release(Some(7))
            .await
            .unwrap()
            .unwrap();
        let report = {
            let controller = Arc::clone(&controller);
            tokio::spawn(async move { controller.report_fault(8).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !report.is_finished(),
            "a new fault must not cross the guarded source-gate transition"
        );

        drop(release_guard);
        report.await.unwrap().unwrap();
        assert_eq!(controller.read_local_fault_report().await.unwrap(), Some(8));
    }

    #[tokio::test]
    async fn fault_after_release_commit_is_preserved_for_the_next_round() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let round = round(&controller, 7, &[1]);
        controller.report_fault(7).await.unwrap();
        activate_start(&controller, &kv, &round, 4).await;
        controller
            .announce_recover_release(&round, 4)
            .await
            .unwrap();
        let pending = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: 4 },
        };
        controller.announce_release_ready(&pending).await.unwrap();
        assert!(matches!(
            controller
                .try_commit_recover_release(&pending)
                .await
                .unwrap(),
            ReleaseCommitStatus::Committed { .. }
        ));

        // The committed terminal linearizes before this new failure. It may release peers that
        // already consumed it, but this reporting owner must stay fenced and the new sequence must
        // remain the level-trigger for the immediately following global round.
        controller.report_fault(8).await.unwrap();
        let mut monitor = RecoveryMonitor::default();
        assert!(controller
            .begin_recovery_release(round.fault_sequence(controller.instance_id()))
            .await
            .unwrap()
            .is_none());
        assert_eq!(monitor.handled_faults.get(&NodeId(1)), None);
        assert_eq!(
            monitor.pending_faults(&controller).await.unwrap(),
            vec![RecoveryFault {
                reporter: NodeId(1),
                sequence: 8,
            }]
        );
    }

    #[tokio::test]
    async fn idle_worker_consumes_only_a_committed_release_fault_set() {
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let self_id = NodeId(2);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
        let controller = Arc::new(ClusterController::new(
            self_id,
            kv.clone(),
            None,
            members_rx,
        ));
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
        let authority = Arc::new(LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            10_000,
        ));
        let driver_owner = LeaderLeaseOwner {
            node: driver_id,
            boot: driver_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(driver_lease) =
            authority.try_acquire(&driver_owner, 0).await.unwrap()
        else {
            panic!("empty recovery test authority must grant the remote leader");
        };
        controller.set_leader_lease_store(Arc::clone(&authority));
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
        driver.publish_recovery_incarnation().await.unwrap();
        let (_driver_lease_tx, driver_lease_rx) = watch::channel(Some(driver_lease.clone()));
        driver
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))));
        driver
            .set_leader_lease_watch(
                driver_lease_rx,
                driver_owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        driver.set_leader_lease_store(authority);
        driver.set_active(true);
        let round = RecoveryRound::new(
            5,
            driver_lease.proof(),
            fence.clone(),
            vec![RecoveryFault {
                reporter: self_id,
                sequence: 17,
            }],
        )
        .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(fence));
        driver.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        driver_kv.seed(self_id, "control:fault-report", "17".into());

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
        db.set_source_gate(false);
        kv.seed(self_id, "control:fault-report", "17".into());

        let start = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch: 3 },
        };
        kv.seed(
            NodeId(1),
            "control:recover",
            serde_json::to_string(&start).unwrap(),
        );
        let mut monitor = RecoveryMonitor::default();
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);
        monitor.observe(&db, &controller).await;

        assert!(!controller.is_recovering());
        assert!(db.cluster_intake_fenced());
        assert!(monitor.stopped_for.is_none());
        assert!(monitor.restored_for.is_none());
        assert_eq!(
            controller.read_fault_reports().await.unwrap(),
            vec![(self_id, 17)]
        );

        driver.announce_recover_prepare(&round).await.unwrap();
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
        kv.seed(
            NodeId(1),
            "control:recovery-incarnation",
            controller.recovery_incarnation().to_string(),
        );
        monitor.observe(&db, &controller).await;
        let pending = monitor.pending_faults(&controller).await.unwrap();
        monitor.hold_for_pending_fault(&db, &controller, &pending);

        assert!(pending.is_empty());
        assert!(!controller.is_recovering());
        assert!(db.cluster_intake_fenced());
        assert!(monitor.stopped_for.is_none());
        assert!(monitor.restored_for.is_none());
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
        assert!(controller
            .read_fault_reports()
            .await
            .unwrap()
            .iter()
            .any(|(_, sequence)| *sequence != 0));
    }

    #[tokio::test]
    async fn transient_fault_scan_failure_becomes_a_durable_recovery_trigger() {
        let self_id = NodeId(1);
        let fast: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
        let durable: Arc<dyn ClusterKv> = Arc::new(FailOnceFaultScanKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new_with_recovery_kv(
            self_id, fast, durable, None, members_rx,
        ));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        db.set_source_gate(false);
        let mut monitor = RecoveryMonitor::default();

        assert!(monitor.pending_faults(&controller).await.is_err());
        monitor.hold_for_unknown_fault_audit(&db, &controller).await;

        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert!(!monitor.fault_audit_unknown);
        let pending = monitor.pending_faults(&controller).await.unwrap();
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
        let exact_round = round(&controller, 7, &[1]);
        activate_start(&controller, &kv, &exact_round, 4).await;
        let start = start(exact_round, 4);
        controller.announce_recovered(&start).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &start, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Reached);
    }

    #[tokio::test]
    async fn newer_recovery_ack_supersedes_instead_of_satisfying_old_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let expected_round = round(&controller, 7, &[1]);
        activate_start(&controller, &kv, &expected_round, 4).await;
        let expected = start(expected_round, 4);
        let newer = start(round(&controller, 8, &[1]), 4);
        controller.announce_recovered(&newer).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Superseded);
    }

    #[tokio::test]
    async fn same_generation_nonce_conflict_never_satisfies_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let expected_round = round(&controller, 7, &[1]);
        activate_start(&controller, &kv, &expected_round, 4).await;
        let expected = start(expected_round, 4);
        let conflicting = start(round(&controller, 7, &[1]), 4);
        controller.announce_recovered(&conflicting).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Conflicted);
    }

    #[tokio::test]
    async fn different_start_target_never_satisfies_restore_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let exact_round = round(&controller, 7, &[1]);
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
    async fn restore_quorum_does_not_shrink_when_membership_changes() {
        let (controller, members_tx, kv) = controller(vec![info(2)]).await;
        let exact_round = round(&controller, 7, &[1, 2]);
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
        let round = round(&controller, 7, &[1, 2]);
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_stopped(&round).await.unwrap();
        members_tx.send(Vec::new()).unwrap();

        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::ParticipantsChanged);
    }

    #[tokio::test]
    async fn missing_prepare_participant_obeys_the_hard_quorum_deadline() {
        let (controller, _members_tx, kv) = controller(vec![info(2)]).await;
        let round = round(&controller, 7, &[1, 2]);
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_stopped(&round).await.unwrap();

        let started = std::time::Instant::now();
        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_millis(25)).await;

        assert_eq!(outcome, RecoveryQuorum::TimedOut);
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "quorum wait exceeded its single hard deadline: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn restarted_same_id_process_invalidates_persisted_stop_ack() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let round = round(&controller, 9, &[1]);
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_stopped(&round).await.unwrap();

        let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
        let replacement = ClusterController::new(NodeId(1), kv, None, replacement_rx);
        replacement.publish_recovery_incarnation().await.unwrap();

        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;
        assert_eq!(outcome, RecoveryQuorum::ParticipantsChanged);
    }

    #[tokio::test]
    async fn release_commit_rejects_a_post_ready_fault_sequence() {
        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let round = round(&controller, 11, &[1]);
        controller.report_fault(11).await.unwrap();
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
        controller.report_fault(12).await.unwrap();

        let RecoveryControlError::Superseded(reason) = controller
            .try_commit_recover_release(&release)
            .await
            .unwrap_err()
        else {
            panic!("a newer fault must block the release terminal");
        };
        assert!(reason.contains("fault set changed"));
        assert_eq!(
            controller.read_fault_reports().await.unwrap(),
            vec![(NodeId(1), 12)]
        );
        assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
    }

    #[tokio::test]
    async fn shuffle_cutoff_failure_never_publishes_release_readiness() {
        use laminar_core::shuffle::ShuffleReceiver;
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let (controller, _members_tx, kv) = controller(Vec::new()).await;
        let controller = Arc::new(controller);
        let round = round(&controller, 7, &[1]);
        controller.report_fault(7).await.unwrap();
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
        let round = round(&controller, 7, &[1]);
        controller.report_fault(7).await.unwrap();
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
        let round = round(&controller, 7, &[1]);
        controller.report_fault(7).await.unwrap();
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
