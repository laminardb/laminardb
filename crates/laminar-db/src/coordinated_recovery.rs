//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode).
//!
//! Two-phase, stop-the-world: the leader announces `Prepare` (every node stops and acks),
//! computes the rewind target from the quiesced decision store, truncates every durable
//! artifact above it (the resumed pipeline reuses epoch numbers, so the abandoned timeline
//! must not survive), then announces `Start`. No committed epoch → target 0, a fresh start
//! from initial source offsets. Off by default.

use std::sync::atomic::{AtomicU64, Ordering};
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
/// A node stopped by `Prepare` whose `Start` never arrives (leader died mid-round) restarts
/// itself plainly after this long rather than staying wedged.
const ORPHAN_STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery generation, max-wins across leader change so a round keeps a stable id.
const RECOVERY_GEN_KEY: &str = "control:recovery-gen";

/// Rewind target meaning "no committed cut exists": truncate everything and start fresh.
const GENESIS: u64 = 0;

/// Per-process fault counter; resets to 0 on restart, so the leader triggers on any change
/// (not just an increase) to catch a re-fault.
static FAULT_SEQ: AtomicU64 = AtomicU64::new(0);

/// Publish a fault so the leader drives a global restart; this node's monitor then
/// restores it on observing the round.
pub(crate) async fn report_local_fault(controller: &ClusterController) {
    let seq = FAULT_SEQ.fetch_add(1, Ordering::SeqCst) + 1;
    controller.report_fault(seq).await;
    tracing::warn!(seq, "reported local fault for coordinated cluster recovery");
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

    /// Act on the leader's announcement: stop on `Prepare`, restore on `Start`. A stop whose
    /// `Start` never arrives falls back to a plain restart after [`ORPHAN_STOP_TIMEOUT`].
    async fn observe(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
        match controller.observe_recover().await {
            Some((RecoverPhase::Prepare, gen))
                if gen > self.applied_gen && self.stopped_for.map(|(g, _)| g) != Some(gen) =>
            {
                controller.set_recovering(true);
                stop_and_purge(db).await;
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
                    stop_and_purge(db).await;
                }
                if self.restore_and_ack(db, controller, epoch, gen).await {
                    controller.set_recovering(false);
                    tracing::warn!(target_epoch = epoch, gen, "node restored to recovery epoch");
                }
                // Failure: state untouched, `Start` still visible → retried next tick.
            }
            _ => {
                if let Some((gen, at)) = self.stopped_for {
                    if at.elapsed() > ORPHAN_STOP_TIMEOUT {
                        tracing::error!(
                            gen,
                            "recovery round orphaned (no Start); restarting plain"
                        );
                        start_pipeline(db, None).await;
                        self.stopped_for = None;
                        controller.set_recovering(false);
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

    /// Leader: stop the world, fix the target against the quiesced store, truncate the
    /// abandoned timeline, then restart the world. Always releases the fence and retires the
    /// announcement on exit (so an incomplete round can't leave a stale target for a later
    /// restart to replay); an incomplete round bumps `coordinated_recovery_failures_total`
    /// and relies on a still-faulted node re-triggering.
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
        let gen_id = read_recovery_gen(controller).await + 1;
        write_recovery_gen(controller, gen_id).await;
        let participants: FxHashSet<NodeId> = controller.live_instances().into_iter().collect();

        controller.set_recovering(true);
        controller.announce_recover_prepare(gen_id).await;
        tracing::warn!(gen = gen_id, "leader announced recovery prepare");
        stop_and_purge(db).await;
        controller.announce_stopped(gen_id).await;
        if !wait_gen_quorum(
            || controller.read_stopped(),
            controller,
            gen_id,
            &participants,
            STOP_QUORUM_TIMEOUT,
        )
        .await
        {
            // Bounded wait: a straggler catches up at `Start` or via its orphan fallback.
            tracing::warn!(gen = gen_id, "stop quorum timed out; proceeding");
        }

        // The world is stopped: the decision store is quiescent, so this read IS the cut —
        // no seal fallback, no probe. No committed epoch means a fresh start.
        let target = match read_committed_cut(db).await {
            Ok(cut) => cut.unwrap_or(GENESIS),
            Err(e) => {
                tracing::error!(error = %e, gen = gen_id, "target read failed; abandoning round");
                self.abandon_round(db, controller, gen_id).await;
                return;
            }
        };
        // Truncation must succeed before anyone restarts: the resumed pipeline reuses epoch
        // numbers above the target, and the adopt path's offset cut reads the durable seal.
        let backend = db.state_backend.lock().clone();
        if let Some(b) = backend {
            if let Err(e) = b.truncate_after(target).await {
                tracing::error!(error = %e, gen = gen_id, target, "truncate failed; abandoning round");
                self.abandon_round(db, controller, gen_id).await;
                return;
            }
        }
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
                &participants,
                RESTORE_QUORUM_TIMEOUT,
            )
            .await
        } else {
            tracing::error!(gen = gen_id, "leader self-restore failed; abandoning round");
            false
        };
        controller.set_recovering(false);
        // Always retire the announcement — a lingering target would be replayed by a later
        // fresh restart (applied_gen resets to 0); a still-faulted straggler re-triggers.
        controller.clear_recover().await;
        if restored && quorum_met {
            tracing::warn!(
                gen = gen_id,
                "coordinated recovery complete; fence released"
            );
        } else {
            if let Some(m) = db.engine_metrics.lock().clone() {
                m.coordinated_recovery_failures_total.inc();
            }
            tracing::error!(
                gen = gen_id,
                "coordinated recovery round incomplete; announcement retired"
            );
        }
    }

    /// Undo a round that failed before `Start`: retire the announcement, restart the leader
    /// plainly. Faults were not marked handled, so the next poll retries with a fresh gen.
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
        start_pipeline(db, None).await;
        controller.set_recovering(false);
        tracing::error!(
            gen = gen_id,
            "coordinated recovery round abandoned before start"
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
        if !start_pipeline(db, Some(target)).await {
            return false;
        }
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
/// futures are `!Send`). A faulted node is already stopped, so stop errors are ignored.
async fn stop_and_purge(db: &Arc<LaminarDB>) {
    run_lifecycle(db, |db| async move {
        let _ = db.stop_pipeline().await;
        // Pre-rewind shuffle slices are stale: their senders rewind and replay them, so
        // folding a buffered copy after the rewind double-counts.
        db.purge_shuffle_receiver_buffers();
        Ok(())
    })
    .await;
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

/// Highest epoch committed cluster-wide per the 2PC decision store — the only sound rewind
/// target (the durable seal is node-local durability, not a cluster commit). `Ok(None)`
/// means nothing ever committed; `Err` means the store is unreadable right now.
async fn read_committed_cut(db: &LaminarDB) -> Result<Option<u64>, String> {
    // Bind the clone before awaiting — an if-let scrutinee would hold the lock guard across it.
    let ds = db.decision_store.lock().clone();
    match ds {
        Some(ds) => ds.highest_committed().await.map_err(|e| e.to_string()),
        None => Ok(None),
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

/// Wait until every still-live `participant` reports `gen >= gen_id` via `read` (stopped or
/// restored acks), or the deadline. Only the round's original participants are awaited: one
/// that left membership can't ack (don't burn the timeout on it), and one that joined later
/// recovers on its own observe. `true` if every pending participant acked.
async fn wait_gen_quorum<R, Fut>(
    read: R,
    controller: &ClusterController,
    gen_id: u64,
    participants: &FxHashSet<NodeId>,
    timeout: Duration,
) -> bool
where
    R: Fn() -> Fut,
    Fut: std::future::Future<Output = Vec<(NodeId, u64)>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let live: FxHashSet<NodeId> = controller.live_instances().into_iter().collect();
        let acked: FxHashSet<NodeId> = read()
            .await
            .into_iter()
            .filter(|(_, g)| *g >= gen_id)
            .map(|(n, _)| n)
            .collect();
        let pending: Vec<NodeId> = participants
            .iter()
            .copied()
            .filter(|n| live.contains(n) && !acked.contains(n))
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
