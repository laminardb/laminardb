//! Leader-coordinated global restart-to-epoch on a fatal fault (cluster mode).
//!
//! Every node rewinds to the highest cluster-wide committed epoch so a fault on one node
//! can't leave the cross-node shuffle cut inconsistent. Off by default; soak-gated (see
//! `docs/plans/1a-cluster-recovery-driver.md`).

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

    /// `true` when any node's fault sequence changed since we last handled it.
    async fn take_new_fault(&mut self, controller: &ClusterController) -> bool {
        let mut triggered = false;
        for (node, seq) in controller.read_fault_reports().await {
            if self.handled_faults.get(&node) != Some(&seq) {
                self.handled_faults.insert(node, seq);
                triggered = true;
            }
        }
        triggered
    }

    /// Leader: fix `N`, bump the generation, announce, restore self, then wait for every
    /// live node to report restored before releasing the fence.
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

        // Restore self into the quorum, marking applied so the observe path doesn't repeat it.
        self.restore_pipeline(db, target).await;
        self.applied_gen = gen_id;
        controller.announce_recovered(gen_id).await;

        wait_restore_quorum(controller, gen_id, RESTORE_QUORUM_TIMEOUT).await;
        controller.set_recovering(false);
        tracing::warn!(
            gen = gen_id,
            "coordinated recovery complete; fence released"
        );
    }

    async fn restore(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        target: u64,
        gen_id: u64,
    ) {
        controller.set_recovering(true);
        self.restore_pipeline(db, target).await;
        self.applied_gen = gen_id;
        controller.announce_recovered(gen_id).await;
        // Only the leader injects, so a follower clearing its fence before the quorum is fine.
        controller.set_recovering(false);
        tracing::warn!(
            target_epoch = target,
            gen = gen_id,
            "node restored to recovery epoch"
        );
    }

    /// Stop, arm the target, and restart from `N`, on a dedicated thread since `start()`
    /// is `!Send`. The fence makes the stop skip its final checkpoint (which would commit
    /// past `N`).
    async fn restore_pipeline(&self, db: &Arc<LaminarDB>, target: u64) {
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
            return;
        }
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "coordinated recovery restart failed"),
            Err(_) => tracing::error!("coordinated recovery restore thread dropped"),
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
