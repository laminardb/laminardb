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

use rustc_hash::{FxHashMap, FxHashSet};
use tokio::runtime::Handle;

use laminar_core::cluster::control::{ClusterController, RecoverPhase};
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const STOP_QUORUM_TIMEOUT: Duration = Duration::from_secs(30);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
/// How long a node stopped by `Prepare` waits for a `Start` before giving up on the round.
const ORPHAN_STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// Must exceed `rebalance_debounce` (5s) + `watcher_poll` (2s) so a pending rotation lands inside
/// the round.
const ASSIGNMENT_SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Must exceed a peer's observe cycle plus the gossip round trip for its `stopped` ack, or a fast
/// round retires `Start` before its peers ever see it.
const PREPARE_MIN_DWELL: Duration = Duration::from_secs(2);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery generation, max-wins across leader change so a round keeps a stable id.
const RECOVERY_GEN_KEY: &str = "control:recovery-gen";

/// Recovery target meaning "no committed cut exists": start fresh.
const GENESIS: u64 = 0;

/// Boot-unique so a second kill of a node never reports the value the leader already handled
/// (a per-process counter resets to 1 and collides, and no round fires). Stable within a boot,
/// so repeated reports dedup to one round.
fn boot_fault_nonce() -> u64 {
    static NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *NONCE.get_or_init(unix_nanos)
}

/// Wall-clock nanos, clamped into `u64`. Monotonic enough to order rounds across leader changes.
fn unix_nanos() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(u64::MAX).max(1)
}

/// Publish a fault so the leader drives a global restart; this node's monitor then
/// restores it on observing the round.
pub(crate) async fn report_local_fault(controller: &ClusterController) {
    let seq = boot_fault_nonce();
    controller.report_fault(seq).await;
    tracing::warn!(seq, "reported local fault for coordinated cluster recovery");
}

/// A fresh identity, not the boot nonce: the leader already handled that one, so only a new value
/// drives the round an orphaned node needs.
async fn report_fresh_fault(controller: &ClusterController) {
    let seq = unix_nanos();
    controller.report_fault(seq).await;
    tracing::warn!(seq, "reported fresh fault after an orphaned recovery round");
}

/// Spawn the long-lived per-node monitor. It drives stop/start, so it must outlive those
/// cycles — not spawned from `start_inner`.
pub(crate) fn spawn_monitor(db: &Arc<LaminarDB>) {
    let weak = Arc::downgrade(db);
    tokio::spawn(async move {
        RecoveryMonitor::default().run(weak).await;
    });
}

#[derive(Default)]
struct RecoveryMonitor {
    applied_gen: u64,
    handled_faults: FxHashMap<NodeId, u64>,
    /// Round this node stopped for at `Prepare`, until its `Start` restores it.
    stopped_for: Option<(u64, tokio::time::Instant)>,
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
                let pending = self.pending_faults(&controller).await;
                if !pending.is_empty() {
                    self.drive_round(&db, &controller, pending).await;
                }
            }
        }
    }

    /// Act on the leader's announcement: stop on `Prepare`, restore on `Start`.
    async fn observe(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
        match controller.observe_recover().await {
            Some((RecoverPhase::Prepare, gen))
                if gen > self.applied_gen && self.stopped_for.map(|(g, _)| g) != Some(gen) =>
            {
                controller.set_recovering(true);
                if !stop_and_purge(db).await {
                    tracing::error!(
                        gen,
                        "recovery prepare could not quiesce this node; withholding stopped acknowledgement"
                    );
                    return;
                }
                controller.announce_stopped(gen).await;
                self.stopped_for = Some((gen, tokio::time::Instant::now()));
                tracing::warn!(gen, "stopped for recovery round; awaiting target");
            }
            Some((RecoverPhase::Start { epoch }, gen)) if gen > self.applied_gen => {
                controller.set_recovering(true);
                if self.stopped_for.map(|(g, _)| g) == Some(gen) {
                    // Peers may have kept sending between our stop and theirs; now that the
                    // round has quiesced, drop those stragglers too.
                    db.purge_shuffle_receiver_buffers();
                } else {
                    // Missed the prepare (joined late / slow poll) — stop now.
                    if !stop_and_purge(db).await {
                        tracing::error!(
                            gen,
                            "recovery start could not quiesce this node; refusing restore"
                        );
                        return;
                    }
                }
                if self.restore_and_ack(db, controller, epoch, gen).await {
                    // Nobody emits at the new generation until everyone is at it, so a peer's
                    // older-generation frame is provably pre-rewind and safe to discard.
                    let quorum = wait_gen_quorum(
                        || controller.read_recovered(),
                        controller,
                        gen,
                        RESTORE_QUORUM_TIMEOUT,
                    )
                    .await;
                    // Unfence before the settle wait: the rotation it waits on must checkpoint.
                    controller.set_recovering(false);
                    if !quorum || !await_assignment_settled(db, controller).await {
                        hold_intake_and_request_retry(db, controller, gen, quorum).await;
                        return;
                    }
                    log_release_diagnostic(db, controller, gen, epoch);
                    db.set_source_gate(false);
                    tracing::warn!(target_epoch = epoch, gen, "node restored to recovery epoch");
                }
                // Failure: state untouched, `Start` still visible → retried next tick.
            }
            _ => {
                if let Some((gen, at)) = self.stopped_for {
                    if at.elapsed() > ORPHAN_STOP_TIMEOUT {
                        // Resuming here would emit at the pre-round generation from an unknown cut:
                        // rewound peers discard those frames, un-rewound ones double-count them.
                        if let Some(m) = db.engine_metrics.lock().clone() {
                            m.coordinated_recovery_failures_total.inc();
                        }
                        tracing::error!(gen, "recovery round orphaned (no Start); holding intake");
                        self.stopped_for = None;
                        report_fresh_fault(controller).await;
                    }
                }
            }
        }
    }

    /// Fault reports not yet handled. A `0` report is "no fault" and forgets the node, so a
    /// re-fault reusing a sequence still triggers. Pending faults are recorded as handled only
    /// once a round actually runs (`drive_round`), so a deferred round retries next poll.
    async fn pending_faults(&mut self, controller: &ClusterController) -> Vec<(NodeId, u64)> {
        let mut pending = Vec::new();
        for (node, seq) in controller.read_fault_reports().await {
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
        if read_committed_cut(db).await.is_err() {
            tracing::warn!("coordinated recovery: decision store unreadable; deferring round");
            return;
        }
        // A gossip-KV `max + 1` collides when the leader that recorded the previous gen dies and
        // its slot vanishes; clock-derived gens stay monotonic across leader changes, and the KV
        // max still wins if a new leader's clock lags.
        let gen_id = read_recovery_gen(controller)
            .await
            .saturating_add(1)
            .max(unix_nanos());
        write_recovery_gen(controller, gen_id).await;

        controller.set_recovering(true);
        controller.announce_recover_prepare(gen_id).await;
        tracing::warn!(gen = gen_id, "leader announced recovery prepare");
        if !stop_and_purge(db).await {
            tracing::error!(
                gen = gen_id,
                "leader could not quiesce its decision writer; abandoning recovery round"
            );
            self.abandon_round(db, controller, gen_id).await;
            return;
        }
        controller.announce_stopped(gen_id).await;
        // Peers must observe `Prepare` and ack `stopped` before `Start`, or a driver with a stale
        // membership view races the whole round through in under a poll interval.
        tokio::time::sleep(PREPARE_MIN_DWELL).await;
        if !wait_gen_quorum(
            || controller.read_stopped(),
            controller,
            gen_id,
            STOP_QUORUM_TIMEOUT,
        )
        .await
        {
            // A straggler can still publish an ambiguous decision/state write, so selecting a
            // recovery cut without every round participant stopped would violate the exact-cut
            // premise and could resurrect a live timeline.
            tracing::error!(
                gen = gen_id,
                "stop quorum timed out; abandoning recovery round"
            );
            self.abandon_round(db, controller, gen_id).await;
            return;
        }

        // The world is stopped: the decision store is quiescent, so this read IS the cut — no
        // seal fallback and no probe. No committed epoch means a fresh start.
        let target = match read_committed_cut(db).await {
            Ok(cut) => cut.unwrap_or(GENESIS),
            Err(e) => {
                tracing::error!(error = %e, gen = gen_id, "target read failed; abandoning round");
                self.abandon_round(db, controller, gen_id).await;
                return;
            }
        };
        // Prepared state above the decision is harmless: every object is namespaced by its
        // globally unique checkpoint ID and every recovery/adoption read is decision-bound.
        // Background retention collects abandoned attempts without an O(store) rewind scan.
        for (node, seq) in pending {
            self.handled_faults.insert(node, seq);
        }
        db.purge_shuffle_receiver_buffers();
        controller.announce_recover_start(target, gen_id).await;
        tracing::warn!(
            target_epoch = target,
            gen = gen_id,
            "leader announced recovery start"
        );

        // Retry self-restore inline so the round and its cleanup stay in the leader's control.
        let mut restored = false;
        for attempt in 1..=SELF_RESTORE_ATTEMPTS {
            if self.restore_and_ack(db, controller, target, gen_id).await {
                restored = true;
                break;
            }
            tracing::warn!(
                gen = gen_id,
                attempt,
                "leader self-restore failed; retrying"
            );
        }
        let quorum_met = if restored {
            wait_gen_quorum(
                || controller.read_recovered(),
                controller,
                gen_id,
                RESTORE_QUORUM_TIMEOUT,
            )
            .await
        } else {
            tracing::error!(gen = gen_id, "leader self-restore failed; abandoning round");
            false
        };
        // Unfence before the settle wait: the rotation it waits on must checkpoint.
        controller.set_recovering(false);
        // Sources stay gated across the rotation so vnodes move with no data in flight. Releasing
        // without a full restore would emit at a generation the stragglers haven't reached.
        if !quorum_met || !await_assignment_settled(db, controller).await {
            hold_intake_and_request_retry(db, controller, gen_id, quorum_met).await;
            controller.clear_recover().await;
            return;
        }
        log_release_diagnostic(db, controller, gen_id, target);
        db.set_source_gate(false);
        // Always retire the announcement — a lingering target would be replayed by a later
        // fresh restart (applied_gen resets to 0); a still-faulted straggler re-triggers.
        controller.clear_recover().await;
        // Counts keep this honest: a round that restored only its driver must not read as complete.
        let participants = round_participants(controller, gen_id).await.len();
        let restored_count = controller
            .read_recovered()
            .await
            .into_iter()
            .filter(|(_, g)| *g >= gen_id)
            .count();
        tracing::warn!(
            gen = gen_id,
            participants,
            restored = restored_count,
            "coordinated recovery complete; fence released"
        );
    }

    /// Retire a round that failed before `Start` and restart only the leader's control loop.
    /// Intake remains fenced because peers may already be stopped. Faults are not marked handled,
    /// so the next poll retries the complete round with a fresh generation.
    async fn abandon_round(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        gen_id: u64,
    ) {
        if let Some(m) = db.engine_metrics.lock().clone() {
            m.coordinated_recovery_failures_total.inc();
        }
        controller.clear_recover().await;
        db.set_source_gate(true);
        start_pipeline(db, None).await;
        controller.set_recovering(false);
        tracing::error!(
            gen = gen_id,
            "coordinated recovery round abandoned before start; intake remains fenced"
        );
    }

    /// Restart this node to `target`, then ack the generation and clear its fault report.
    /// Fence-neutral. `false` on failure (state untouched so the next tick retries).
    async fn restore_and_ack(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        target: u64,
        gen_id: u64,
    ) -> bool {
        // Sources come up paused: the restart re-reads and re-shuffles the replay window, and a
        // node that restarts first would shuffle into peers whose receivers haven't rebound.
        db.set_source_gate(true);
        if !start_pipeline(db, Some(target)).await {
            // Starting from the selected cut failed. Keep intake fenced while `Start` remains
            // visible so the next monitor tick can retry without exposing pre-recovery state.
            return false;
        }
        // Bump before the gate opens so a peer's pre-rewind frames are discarded on arrival rather
        // than folded onto the restored state and then re-applied by that peer's replay.
        db.set_shuffle_recovery_gen(gen_id);
        db.coordinated_restores.fetch_add(1, Ordering::SeqCst);
        if let Some(m) = db.engine_metrics.lock().clone() {
            m.coordinated_recoveries_total.inc();
        }
        self.applied_gen = gen_id;
        self.stopped_for = None;
        controller.announce_recovered(gen_id).await;
        controller.clear_fault_report().await;
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

/// Run a stop/start lifecycle future on a dedicated thread since `start()` is `!Send`.
async fn run_lifecycle<F, Fut>(db: &Arc<LaminarDB>, f: F) -> bool
where
    F: FnOnce(Arc<LaminarDB>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), crate::DbError>>,
{
    let db = Arc::clone(db);
    let handle = Handle::current();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let spawned = std::thread::Builder::new()
        .name("laminar-coord-recover".into())
        .spawn(move || {
            let res = handle.block_on(f(db));
            let _ = tx.send(res);
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "coordinated recovery: failed to spawn lifecycle thread");
        return false;
    }
    match rx.await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::error!(error = %e, "coordinated recovery lifecycle step failed");
            false
        }
        Err(_) => {
            tracing::error!("coordinated recovery lifecycle thread dropped");
            false
        }
    }
}

/// Whether the vnode assignment reflects current live membership — every vnode owned, every
/// owner still assignable, and (when there are at least as many vnodes as nodes) every live node
/// owning a share. A stale assignment means a rebalance rotation is still pending.
fn assignment_reflects_membership(db: &Arc<LaminarDB>, controller: &ClusterController) -> bool {
    let guard = db.vnode_registry.lock();
    let Some(reg) = guard.as_ref() else {
        return true;
    };
    let mut owners: FxHashSet<u64> = FxHashSet::default();
    let assignment = reg.snapshot();
    for owner in assignment.iter().copied() {
        if owner.0 == 0 {
            return false; // unassigned vnode: rotation still in flight
        }
        owners.insert(owner.0);
    }
    let live: FxHashSet<u64> = controller
        .assignable_instances()
        .into_iter()
        .map(|n| n.0)
        .collect();
    let all_owners_live = owners.iter().all(|o| live.contains(o));
    let covers_live = (reg.vnode_count() as usize) < live.len() || owners.len() == live.len();
    all_owners_live && covers_live
}

/// Hold the round's quiet period open until a pending rebalance rotation has landed and been
/// adopted. Ownership rotates on a separate, un-gated membership path, so releasing first lets it
/// move vnodes mid-stream: the gainer double-folds a rehydrated chain over records it already
/// counted, and records shuffled to the old owner are dropped. `false` if it never converged.
async fn await_assignment_settled(db: &Arc<LaminarDB>, controller: &ClusterController) -> bool {
    if assignment_reflects_membership(db, controller) {
        return true;
    }
    tracing::warn!("assignment does not reflect live membership; holding sources for the rotation");
    let deadline = tokio::time::Instant::now() + ASSIGNMENT_SETTLE_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(POLL_INTERVAL).await;
        if assignment_reflects_membership(db, controller) {
            tracing::warn!("assignment converged inside the round; releasing sources");
            return true;
        }
    }
    false
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

/// Highest epoch committed cluster-wide per the 2PC decision store — the only sound rewind
/// target (the durable seal is node-local durability, not a cluster commit). `Ok(None)`
/// means nothing ever committed; `Err` means the store is unreadable right now.
async fn read_committed_cut(db: &LaminarDB) -> Result<Option<u64>, String> {
    // Bind the clone before awaiting — an if-let scrutinee would hold the lock guard across it.
    let ds = db.decision_store.lock().clone();
    match ds {
        Some(ds) => {
            // A durable intent is emitted only after the exact seal. Completing its idempotent
            // commit create is therefore the only safe way to make progress; merely returning
            // InDoubt here wedges automatic recovery before startup reconciliation can run.
            ds.resolve_in_doubt().await.map_err(|e| e.to_string())?;
            ds.highest_committed()
                .await
                .map(|decision| decision.map(|d| d.epoch))
                .map_err(|e| e.to_string())
        }
        None => Err("checkpoint decision store is not configured".into()),
    }
}

async fn read_recovery_gen(controller: &ClusterController) -> u64 {
    controller
        .kv()
        .scan(RECOVERY_GEN_KEY)
        .await
        .into_iter()
        .filter_map(|(_, v)| v.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
}

async fn write_recovery_gen(controller: &ClusterController, gen_id: u64) {
    controller
        .kv()
        .write(RECOVERY_GEN_KEY, gen_id.to_string())
        .await;
}

/// The live view UNION everyone that acked `stopped` for this generation. The union matters: a
/// just-restarted node driving its own round can see `live == {self}`, and awaiting only that
/// makes both quorums vacuous. A node that acked `stopped` is stopped and must be restored,
/// whatever the local view says.
async fn round_participants(controller: &ClusterController, gen_id: u64) -> FxHashSet<NodeId> {
    let mut set: FxHashSet<NodeId> = controller.live_instances().into_iter().collect();
    for (node, gen) in controller.read_stopped().await {
        if gen >= gen_id {
            set.insert(node);
        }
    }
    set
}

/// Wait until every participant reports `gen >= gen_id` via `read`, or the deadline. Participants
/// are re-derived each iteration so a late `stopped` acker joins the set instead of being stranded.
async fn wait_gen_quorum<R, Fut>(
    read: R,
    controller: &ClusterController,
    gen_id: u64,
    timeout: Duration,
) -> bool
where
    R: Fn() -> Fut,
    Fut: std::future::Future<Output = Vec<(NodeId, u64)>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let participants = round_participants(controller, gen_id).await;
        let acked: FxHashSet<NodeId> = read()
            .await
            .into_iter()
            .filter(|(_, g)| *g >= gen_id)
            .map(|(n, _)| n)
            .collect();
        let pending: Vec<NodeId> = participants
            .iter()
            .copied()
            .filter(|n| !acked.contains(n))
            .collect();
        if pending.is_empty() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(gen = gen_id, missing = ?pending, "recovery quorum timed out");
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
