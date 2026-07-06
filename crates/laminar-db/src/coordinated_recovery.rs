//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode).
//!
//! Every node rewinds to the highest cluster-wide committed epoch so a fault on one node
//! can't leave the cross-node shuffle cut inconsistent. Off by default.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use rustc_hash::{FxHashMap, FxHashSet};
use tokio::runtime::Handle;

use laminar_core::cluster::control::ClusterController;
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery generation, max-wins across leader change so a round keeps a stable id.
const RECOVERY_GEN_KEY: &str = "control:recovery-gen";

/// Per-process fault counter; resets to 0 on restart, so the leader triggers on any change
/// (not just an increase) to catch a re-fault.
static FAULT_SEQ: AtomicU64 = AtomicU64::new(0);

/// Publish a fault so the leader drives a global restart; this node's monitor then
/// restores it on observing `Recover`.
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

            // Any node restores an unapplied generation; the leader also drives a new round.
            if let Some((epoch, gen)) = controller.observe_recover().await {
                if gen > self.applied_gen {
                    self.restore(&db, &controller, epoch, gen).await;
                }
            }
            if controller.is_leader() {
                let pending = self.pending_faults(&controller).await;
                if !pending.is_empty() {
                    self.drive_round(&db, &controller, pending).await;
                }
            }
        }
    }

    /// Fault reports not yet handled. A `0` report is "no fault" and forgets the node, so a
    /// re-fault reusing a sequence still triggers. Pending faults are recorded as handled only
    /// once a round actually runs (`drive_round`), so a skipped round retries next poll.
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

    /// Leader: fix `N`, announce, restore self (retrying), then wait for every live node to report
    /// restored. Always releases the fence and retires the announcement on exit (so an incomplete
    /// round can't leave a stale target for a later restart to replay); an incomplete round bumps
    /// `coordinated_recovery_failures_total` and relies on a still-faulted node re-triggering.
    async fn drive_round(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        pending: Vec<(NodeId, u64)>,
    ) {
        let Some(target) = compute_target_epoch(db).await else {
            // Faults stay pending (not recorded as handled) so the next poll retries.
            tracing::warn!("coordinated recovery: no committed epoch readable; deferring round");
            return;
        };
        for (node, seq) in pending {
            self.handled_faults.insert(node, seq);
        }
        let gen_id = read_recovery_gen(controller).await + 1;
        write_recovery_gen(controller, gen_id).await;
        let participants: FxHashSet<NodeId> = controller.live_instances().into_iter().collect();

        controller.set_recovering(true);
        controller.announce_recover(target, gen_id).await;
        tracing::warn!(
            target_epoch = target,
            gen = gen_id,
            "leader announced recovery"
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
            wait_restore_quorum(controller, gen_id, &participants, RESTORE_QUORUM_TIMEOUT).await
        } else {
            tracing::error!(gen = gen_id, "leader self-restore failed; abandoning round");
            false
        };
        controller.set_recovering(false);
        // Always retire the announcement — a lingering target would be replayed by a later fresh
        // restart (applied_gen resets to 0); a still-faulted straggler re-triggers a fresh round.
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

    /// Observe-path restore (a follower acting on the leader's announcement): fence the
    /// restart, then release it. `false` if it failed (retried next tick).
    async fn restore(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        target: u64,
        gen_id: u64,
    ) -> bool {
        controller.set_recovering(true);
        let ok = self.restore_and_ack(db, controller, target, gen_id).await;
        controller.set_recovering(false);
        if ok {
            tracing::warn!(
                target_epoch = target,
                gen = gen_id,
                "node restored to recovery epoch"
            );
        }
        ok
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
        if !restore_pipeline(db, target).await {
            return false;
        }
        if let Some(m) = db.engine_metrics.lock().clone() {
            m.coordinated_recoveries_total.inc();
        }
        self.applied_gen = gen_id;
        controller.announce_recovered(gen_id).await;
        controller.clear_fault_report().await;
        true
    }
}

/// Stop, arm the target, and restart from `N`, on a dedicated thread since `start()` is
/// `!Send`. `true` on a clean restart. The fence makes the stop skip its final checkpoint
/// (which would commit past `N`).
async fn restore_pipeline(db: &Arc<LaminarDB>, target: u64) -> bool {
    let db = Arc::clone(db);
    let handle = Handle::current();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let spawned = std::thread::Builder::new()
        .name("laminar-coord-recover".into())
        .spawn(move || {
            let res = handle.block_on(async move {
                // A faulted node is already stopped, so ignore the stop error.
                let _ = db.stop_pipeline().await;
                // Pre-rewind shuffle slices are stale: their senders rewind and replay them,
                // so folding a buffered copy after the rewind double-counts.
                db.purge_shuffle_receiver_buffers();
                db.set_recover_target_epoch(target);
                db.start().await
            });
            let _ = tx.send(res);
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "coordinated recovery: failed to spawn restore thread");
        return false;
    }
    match rx.await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::error!(error = %e, "coordinated recovery restart failed");
            false
        }
        Err(_) => {
            tracing::error!("coordinated recovery restore thread dropped");
            false
        }
    }
}

/// Highest epoch committed cluster-wide, with intact recovery artifacts. Takes the max of
/// the decision cut and the state backend's durable seal (either read can lag on a
/// freshly-restarted leader), then probes the target's source-offset handoff: rewinding to
/// an epoch whose blobs are pruned makes the offset restore fall back to the startup
/// default — resumed ahead of the rewound state, the window is lost instead of replayed.
async fn compute_target_epoch(db: &LaminarDB) -> Option<u64> {
    // Bind clones before awaiting — an if-let scrutinee would hold the lock guard across it.
    let ds = db.decision_store.lock().clone();
    let decided = match ds {
        Some(ds) => ds.highest_committed().await.ok().flatten(),
        None => None,
    };
    let backend = db.state_backend.lock().clone();
    let sealed = match backend.as_ref() {
        Some(b) => b.latest_committed_epoch().await.ok().flatten(),
        None => None,
    };
    let target = decided.max(sealed)?;
    let blobs = match backend.as_ref() {
        Some(b) => b.read_source_offsets(target).await.map(|v| v.len()).ok(),
        None => None,
    };
    tracing::info!(
        ?decided,
        ?sealed,
        target,
        ?blobs,
        "coordinated recovery target"
    );
    if blobs.unwrap_or(0) == 0 {
        tracing::warn!(
            target,
            "recovery target has no source-offset handoff blobs; deferring round"
        );
        return None;
    }
    Some(target)
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

/// Wait until every still-live `participant` reports restored-to-`gen_id`, or the deadline.
/// Only the round's original participants are awaited: one that left membership can't ack
/// (don't burn the timeout on it), and one that joined later recovers on its own observe.
/// Returns `true` if every pending participant acked, `false` on timeout.
async fn wait_restore_quorum(
    controller: &ClusterController,
    gen_id: u64,
    participants: &FxHashSet<NodeId>,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let live: FxHashSet<NodeId> = controller.live_instances().into_iter().collect();
        let acked: FxHashSet<NodeId> = controller
            .read_recovered()
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
            tracing::error!(gen = gen_id, missing = ?pending, "restore quorum timed out");
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
