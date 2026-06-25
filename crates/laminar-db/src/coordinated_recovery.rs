//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode).
//!
//! Every node rewinds to the highest cluster-wide committed epoch so a fault on one node
//! can't leave the cross-node shuffle cut inconsistent. Off by default.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use rustc_hash::{FxHashMap, FxHashSet};
use tokio::runtime::Handle;

use laminar_core::cluster::control::{BarrierAnnouncement, ClusterController, Phase};
use laminar_core::cluster::discovery::NodeId;

use crate::LaminarDB;

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const RESTORE_QUORUM_TIMEOUT: Duration = Duration::from_secs(90);
/// How many times the leader retries restoring itself before abandoning the round.
const SELF_RESTORE_ATTEMPTS: u32 = 3;

/// Recovery generation, max-wins across leader change so a round keeps a stable id.
const RECOVERY_GEN_KEY: &str = "control:recovery-gen";

/// Per-process fault counter. Resets to 0 on restart, so the leader triggers on any
/// change (not only an increase) to catch a re-fault after a restart.
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

            // Any node restores on a Recover it hasn't applied; the leader also drives a
            // new round when a fault report changes.
            if let Some(ann) = controller.observe_recover().await {
                if ann.flags > self.applied_gen {
                    self.restore(&db, &controller, ann.epoch, ann.flags).await;
                }
            }
            if controller.is_leader() && self.take_new_fault(&controller).await {
                self.drive_round(&db, &controller).await;
            }
        }
    }

    /// `true` when any node's fault sequence changed since we last handled it. `0` is "no
    /// fault" (cleared after recovery), so a restarted leader doesn't re-trigger a handled one.
    async fn take_new_fault(&mut self, controller: &ClusterController) -> bool {
        let mut triggered = false;
        for (node, seq) in controller.read_fault_reports().await {
            if seq != 0 && self.handled_faults.get(&node) != Some(&seq) {
                self.handled_faults.insert(node, seq);
                triggered = true;
            }
        }
        triggered
    }

    /// Leader: fix `N`, announce, restore self (retrying), then wait for every live node to
    /// report restored. Always releases the fence and clears the announcement on exit — so a
    /// failed round can't leave a stale `Recover` for a later peer-restart to replay.
    async fn drive_round(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
        let Some(target) = compute_target_epoch(db).await else {
            tracing::warn!("coordinated recovery: no committed epoch yet; skipping round");
            return;
        };
        let gen_id = read_recovery_gen(controller).await + 1;
        write_recovery_gen(controller, gen_id).await;

        controller.set_recovering(true);
        let ann = BarrierAnnouncement {
            epoch: target,
            checkpoint_id: 0,
            phase: Phase::Recover,
            flags: gen_id,
            min_watermark_ms: None,
        };
        if let Err(e) = controller.announce_barrier(&ann).await {
            tracing::error!(error = %e, "coordinated recovery: announce failed");
            controller.set_recovering(false);
            return;
        }
        tracing::warn!(
            target_epoch = target,
            gen = gen_id,
            "leader announced recovery"
        );

        // Retry self-restore inline so the round (and the cleanup below) stays in the
        // leader's control; the lingering announcement gives peers time to observe it.
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
        if restored {
            wait_restore_quorum(controller, gen_id, RESTORE_QUORUM_TIMEOUT).await;
            tracing::warn!(
                gen = gen_id,
                "coordinated recovery complete; fence released"
            );
        } else {
            tracing::error!(gen = gen_id, "leader self-restore failed; abandoning round");
        }
        controller.set_recovering(false);
        controller.clear_recover_announcement(target).await;
    }

    /// Observe-path restore (a follower seeing the leader's `Recover`): fence the restart,
    /// then release it. `false` if it failed (retried next tick).
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

/// Highest epoch committed cluster-wide. Reads the DB-level decision store, so it works
/// even when a faulted leader's own coordinator is stopped.
async fn compute_target_epoch(db: &LaminarDB) -> Option<u64> {
    let ds = db.decision_store.lock().clone()?;
    ds.highest_committed().await.ok().flatten()
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

/// Wait until every live node reports restored-to-`gen_id`, or the deadline (a dead node
/// that never acks recovers on rejoin).
async fn wait_restore_quorum(controller: &ClusterController, gen_id: u64, timeout: Duration) {
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
        if live.iter().all(|n| acked.contains(n)) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let missing: Vec<NodeId> = live.into_iter().filter(|n| !acked.contains(n)).collect();
            tracing::error!(
                gen = gen_id,
                ?missing,
                "coordinated recovery: restore quorum timed out"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
