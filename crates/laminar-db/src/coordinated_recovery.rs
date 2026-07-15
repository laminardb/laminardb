//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode; always on).
//!
//! Two-phase, stop-the-world: the leader announces `Prepare` (every node stops and acks), reads
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

use laminar_core::cluster::control::controller::{RecoveryAnnouncement, RecoveryRound};
use laminar_core::cluster::control::{ClusterController, RecoverPhase};
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

/// Healthy-state monitor cadence. Observation is one direct current-driver read, so static
/// `ObjectStore` discovery performs about one GET per node per second rather than an N-way scan.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const STOP_QUORUM_TIMEOUT: Duration = Duration::from_secs(30);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
const RELEASE_QUORUM_TIMEOUT: Duration = Duration::from_secs(30);
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
    match controller.report_fault(seq).await {
        Ok(()) => {
            tracing::warn!(seq, "reported local fault for coordinated cluster recovery");
            Ok(())
        }
        Err(error) => {
            tracing::error!(seq, %error, "could not persist local recovery fault");
            Err(error)
        }
    }
}

/// A fresh identity, not the boot nonce: the leader already handled that one, so only a new value
/// drives the round an orphaned node needs.
async fn report_fresh_fault(controller: &ClusterController) {
    let seq = next_fault_sequence(controller);
    match controller.report_fault(seq).await {
        Ok(()) => tracing::warn!(seq, "reported fresh fault after an orphaned recovery round"),
        Err(error) => tracing::error!(seq, %error, "could not persist fresh recovery fault"),
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
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            let Some(db) = weak.upgrade() else {
                return;
            };
            if db.is_closed() {
                return;
            }
            let Some(controller) = db.cluster_controller.lock().clone() else {
                continue;
            };

            self.observe(&db, &controller).await;
            if controller.is_leader() {
                // `drive_round` owns every nonterminal local Prepare/Start synchronously. Seeing
                // one here means that owner disappeared or returned early. Retire only the exact
                // orphan before requesting a fresh generation. Release is a terminal
                // record: leave it in place, but do not let it mask later faults.
                match controller.observe_recover().await {
                    Ok(Some(active)) if controller.recovery_driver_is_current(&active.round) => {
                        match active.phase {
                            RecoverPhase::Release { .. } => {}
                            RecoverPhase::Prepare | RecoverPhase::Start { .. }
                                if active.round.id.driver == controller.instance_id() =>
                            {
                                let _ = controller.clear_recover(&active.round).await;
                                report_fresh_fault(&controller).await;
                            }
                            RecoverPhase::Prepare | RecoverPhase::Start { .. } => continue,
                        }
                    }
                    Ok(_) | Err(_) => {}
                }
                let pending = self.pending_faults(&controller).await;
                if !pending.is_empty() {
                    self.drive_round(&db, &controller, pending).await;
                }
            }
        }
    }

    /// Act on the leader's announcement: stop on `Prepare`, restore on `Start`.
    async fn observe(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
        let observed = match controller.observe_recover().await {
            Ok(observed) => {
                self.last_protocol_error = None;
                observed
            }
            Err(error) => {
                controller.set_recovering(true);
                db.set_source_gate(true);
                if self.last_protocol_error.as_deref() != Some(error.as_str()) {
                    tracing::error!(%error, "invalid coordinated-recovery control state");
                    self.last_protocol_error = Some(error);
                    report_fresh_fault(controller).await;
                }
                return;
            }
        };
        let current = observed.filter(|announcement| {
            controller.recovery_driver_is_current(&announcement.round)
                && controller.recovery_round_contains_current_process(&announcement.round)
                && round_assignment_is_current(db, controller, &announcement.round)
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
            _ => self.observe_orphans(db, controller).await,
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
        if exact_start {
            if self.release_and_ack(db, controller, &release, epoch).await {
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
            report_fresh_fault(controller).await;
        }
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
                report_fresh_fault(controller).await;
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

    /// Fault reports not yet handled. A `0` report is "no fault" and forgets the node, so a
    /// re-fault reusing a sequence still triggers. Pending faults are recorded as handled only
    /// once a round actually runs (`drive_round`), so a deferred round retries next poll.
    async fn pending_faults(&mut self, controller: &ClusterController) -> Vec<(NodeId, u64)> {
        let mut pending = Vec::new();
        let reports = match controller.read_fault_reports().await {
            Ok(reports) => reports,
            Err(error) => {
                tracing::error!(%error, "could not read recovery fault reports");
                return pending;
            }
        };
        for (node, seq) in reports {
            if seq == 0 {
                self.handled_faults.remove(&node);
            } else if self.handled_faults.get(&node) != Some(&seq) {
                pending.push((node, seq));
            }
        }
        pending
    }

    /// Leader: stop the world, fix the target against the quiesced decision store, then restart
    /// the world. An incomplete round retains the intake fence, bumps
    /// `coordinated_recovery_failures_total`, and leaves its fault pending for a complete retry.
    #[allow(clippy::too_many_lines)]
    async fn drive_round(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        pending: Vec<(NodeId, u64)>,
    ) {
        // Precheck readability before stopping anything — a transient decision-store error
        // must defer the round (faults stay pending), not flap a stop-the-world cycle.
        if read_committed_cut_bounded(db).await.is_err() {
            tracing::warn!("coordinated recovery: decision store unreadable; deferring round");
            return;
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
        let round = match RecoveryRound::new(gen_id, controller.instance_id(), assignment_fence) {
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
            controller.set_recovering(false);
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
            controller.set_recovering(false);
            hold_intake_and_request_retry(db, controller, gen_id, false).await;
            return;
        }
        if !round_is_current(db, controller, &round).await {
            tracing::error!(
                gen = gen_id,
                "recovery certificate changed after restore quorum; refusing Release"
            );
            let _ = controller.clear_recover(&round).await;
            controller.set_recovering(false);
            hold_intake_and_request_retry(db, controller, gen_id, true).await;
            return;
        }
        if let Err(error) = controller.announce_recover_release(&round, target).await {
            tracing::error!(gen = gen_id, %error, "could not publish recovery Release");
            let _ = controller.clear_recover(&round).await;
            controller.set_recovering(false);
            hold_intake_and_request_retry(db, controller, gen_id, true).await;
            return;
        }
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: target },
        };
        if !self.release_and_ack(db, controller, &release, target).await {
            controller.set_recovering(false);
            hold_intake_and_request_retry(db, controller, gen_id, true).await;
            return;
        }
        let release_quorum =
            wait_released_quorum(controller, &release, RELEASE_QUORUM_TIMEOUT).await;
        if release_quorum != RecoveryQuorum::Reached {
            controller.set_recovering(false);
            hold_intake_and_request_retry(db, controller, gen_id, true).await;
            return;
        }
        // Suppress a briefly stale gossip report only after this generation is complete. Marking
        // faults handled before `Start`/restore quorum would strand the cluster if a newer driver
        // superseded this round and then disappeared.
        for (node, seq) in pending {
            self.handled_faults.insert(node, seq);
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
        controller.set_recovering(false);
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

    /// Consume the exact terminal Release this process's restored Start was waiting for. The
    /// release acknowledgement is persisted before source intake opens; a failed acknowledgement
    /// leaves the gate closed and is retried from the Release on the next monitor tick.
    async fn release_and_ack(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &RecoveryAnnouncement,
        target: u64,
    ) -> bool {
        let authority_deadline = tokio::time::Instant::now() + DECISION_IO_TIMEOUT;
        let authority_revision = db.assignment_authority_revision.load(Ordering::Acquire);
        if !matches!(
            tokio::time::timeout_at(
                authority_deadline,
                round_is_current(db, controller, &release.round),
            )
            .await,
            Ok(true)
        ) {
            return false;
        }
        let released =
            tokio::time::timeout_at(authority_deadline, controller.announce_released(release))
                .await;
        if let Err(error) = released.unwrap_or_else(|_| {
            Err("recovery Release acknowledgement exceeded its authority deadline".into())
        }) {
            tracing::error!(
                gen = release.round.id.generation,
                %error,
                "could not acknowledge recovery Release"
            );
            return false;
        }
        let cleared =
            tokio::time::timeout_at(authority_deadline, controller.clear_fault_report()).await;
        if let Err(error) = cleared.unwrap_or_else(|_| {
            Err("released recovery fault clear exceeded its authority deadline".into())
        }) {
            tracing::error!(
                gen = release.round.id.generation,
                %error,
                "could not durably clear released recovery fault"
            );
            return false;
        }
        // The release acknowledgement and fault clear both awaited remote authority. Re-read the
        // complete round before mutating local delivery state or attempting to open intake.
        if !matches!(
            tokio::time::timeout_at(
                authority_deadline,
                round_is_current(db, controller, &release.round),
            )
            .await,
            Ok(true)
        ) {
            report_fresh_fault(controller).await;
            return false;
        }
        if !db.complete_shuffle_recovery(release.round.id.generation) {
            tracing::error!(
                gen = release.round.id.generation,
                "shuffle loss cutoff did not match the released recovery generation"
            );
            return false;
        }
        log_release_diagnostic(db, controller, release.round.id.generation, target);
        controller.set_recovering(false);
        let drain_transition = controller
            .checkpoint_drain_transition()
            .filter(|transition| transition.predecessor == release.round.assignment_fence);
        let activation = db
            .activate_assignment_authority(
                &release.round.assignment_fence,
                drain_transition,
                authority_revision,
                authority_deadline,
            )
            .await;
        match activation {
            Ok(activation) if activation.installed && activation.intake_open => {}
            Ok(_) => {
                controller.set_recovering(true);
                db.set_source_gate(true);
                report_fresh_fault(controller).await;
                return false;
            }
            Err(error) => {
                controller.set_recovering(true);
                db.set_source_gate(true);
                tracing::error!(
                    gen = release.round.id.generation,
                    %error,
                    "could not serialize released assignment authority"
                );
                report_fresh_fault(controller).await;
                return false;
            }
        }
        if !round_assignment_is_current(db, controller, &release.round) {
            controller.set_recovering(true);
            db.set_source_gate(true);
            report_fresh_fault(controller).await;
            return false;
        }
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
    run_lifecycle(db, move |db| async move {
        if let Some(t) = target {
            db.set_recover_target_epoch(t);
        }
        db.start().await
    })
    .await
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
    controller
        .checkpoint_assignment_fence(assignment.version())
        .filter(|fence| fence.matches_owner_map(&owners))
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
    db.set_source_gate(true);
    if let Some(m) = db.engine_metrics.lock().clone() {
        m.coordinated_recovery_failures_total.inc();
    }
    tracing::error!(
        gen = gen_id,
        restore_quorum = quorum,
        "holding intake shut and requesting a fresh recovery round"
    );
    report_fresh_fault(controller).await;
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
        controller.observe_recover().await,
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
        match controller.observe_recover().await {
            Err(_) => return RecoveryQuorum::Conflicted,
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
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(gen = round.id.generation, missing = ?pending, "recovery stop quorum timed out");
            return RecoveryQuorum::TimedOut;
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
        match controller.observe_recover().await {
            Err(_) => return RecoveryQuorum::Conflicted,
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
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(gen = round.id.generation, missing = ?pending, "recovery restore quorum timed out");
            return RecoveryQuorum::TimedOut;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until every frozen process has consumed the identical Release terminal record.
async fn wait_released_quorum(
    controller: &ClusterController,
    release: &RecoveryAnnouncement,
    timeout: Duration,
) -> RecoveryQuorum {
    let round = &release.round;
    let deadline = tokio::time::Instant::now() + timeout;
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
        match controller.observe_recover().await {
            Ok(Some(active)) if active == *release => {}
            Ok(Some(active)) if active.round.id.generation > round.id.generation => {
                return RecoveryQuorum::Superseded;
            }
            Err(_) | Ok(_) => return RecoveryQuorum::Conflicted,
        }
        let Ok(reports) = controller.read_released().await else {
            return RecoveryQuorum::Conflicted;
        };
        let pending = frozen_pending(round, reports, |ack| ack == release);
        if pending.is_empty() {
            return RecoveryQuorum::Reached;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(gen = round.id.generation, missing = ?pending, "recovery Release quorum timed out");
            return RecoveryQuorum::TimedOut;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::cluster::control::{CheckpointParticipant, InMemoryKv};
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

    fn controller(
        peers: Vec<NodeInfo>,
    ) -> (
        ClusterController,
        watch::Sender<Vec<NodeInfo>>,
        Arc<InMemoryKv>,
    ) {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (members_tx, members_rx) = watch::channel(peers);
        (
            ClusterController::new(self_id, kv.clone(), None, members_rx),
            members_tx,
            kv,
        )
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
            NodeId(1),
            CheckpointAssignmentFence::from_owner_map(7, &owners, checkpoint_participants).unwrap(),
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
        let (controller, _members_tx, _kv) = controller(Vec::new());
        replicate_recovery_gen(&controller, 41).await.unwrap();

        let replicated_max = read_recovery_gen(&controller).await.unwrap();

        assert_eq!(replicated_max.checked_add(1), Some(42));
        assert!(replicate_recovery_gen(&controller, 40).await.is_err());
        assert_eq!(read_recovery_gen(&controller).await.unwrap(), 41);
    }

    #[tokio::test]
    async fn recovery_quorum_requires_the_exact_round_and_target() {
        let (controller, _members_tx, kv) = controller(Vec::new());
        let exact_round = round(&controller, 7, &[1]);
        activate_start(&controller, &kv, &exact_round, 4).await;
        let start = start(exact_round, 4);
        controller.announce_recovered(&start).await.unwrap();

        let outcome = wait_restored_quorum(&controller, &start, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::Reached);
    }

    #[tokio::test]
    async fn newer_recovery_ack_supersedes_instead_of_satisfying_old_quorum() {
        let (controller, _members_tx, kv) = controller(Vec::new());
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
        let (controller, _members_tx, kv) = controller(Vec::new());
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
        let (controller, _members_tx, kv) = controller(Vec::new());
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
        let (controller, members_tx, kv) = controller(vec![info(2)]);
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
        let (controller, members_tx, kv) = controller(vec![info(2)]);
        let round = round(&controller, 7, &[1, 2]);
        publish_round_roster(&controller, &kv, &round).await;
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_stopped(&round).await.unwrap();
        members_tx.send(Vec::new()).unwrap();

        let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;

        assert_eq!(outcome, RecoveryQuorum::ParticipantsChanged);
    }

    #[tokio::test]
    async fn restarted_same_id_process_invalidates_persisted_stop_ack() {
        let (controller, _members_tx, kv) = controller(Vec::new());
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
    async fn release_quorum_requires_the_exact_durable_terminal() {
        let (controller, _members_tx, kv) = controller(Vec::new());
        let round = round(&controller, 11, &[1]);
        activate_start(&controller, &kv, &round, 6).await;
        let start = start(round.clone(), 6);
        controller.announce_recovered(&start).await.unwrap();
        assert_eq!(
            wait_restored_quorum(&controller, &start, Duration::from_secs(1)).await,
            RecoveryQuorum::Reached
        );

        controller
            .announce_recover_release(&round, 6)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Release { epoch: 6 },
        };
        controller.announce_released(&release).await.unwrap();

        assert_eq!(
            wait_released_quorum(&controller, &release, Duration::from_secs(1)).await,
            RecoveryQuorum::Reached
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_closure_wins_while_recovery_release_waits_to_open_intake() {
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

        let (controller, _members_tx, kv) = controller(Vec::new());
        let controller = Arc::new(controller);
        let round = round(&controller, 7, &[1]);
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
                let opened = monitor.release_and_ack(&db, &controller, &release, 4).await;
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
