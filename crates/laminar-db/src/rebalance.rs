//! Dynamic vnode rebalance control plane. See
//! `docs/plans/cluster-production-readiness.md`.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, RotateOutcome,
};
use laminar_core::state::{round_robin_assignment, NodeId, VnodeRegistry};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::db::LaminarDB;

/// Tunables for the rebalance control plane.
#[derive(Debug, Clone, Copy)]
pub struct RebalanceConfig {
    /// Interval between snapshot-store polls.
    pub watcher_poll: Duration,
    /// Quiet period before a membership change triggers rotation.
    pub rebalance_debounce: Duration,
    /// Upper bound on the pre-rotation forced checkpoint.
    pub checkpoint_timeout: Duration,
    /// Delay before retrying a failed rotation.
    pub retry_delay: Duration,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            watcher_poll: Duration::from_secs(2),
            rebalance_debounce: Duration::from_secs(5),
            checkpoint_timeout: Duration::from_secs(60),
            retry_delay: Duration::from_secs(10),
        }
    }
}

impl RebalanceConfig {
    /// Fast timings for tests — 500ms debounce thrashes in production.
    #[doc(hidden)]
    #[must_use]
    pub fn test_defaults() -> Self {
        Self {
            watcher_poll: Duration::from_millis(200),
            rebalance_debounce: Duration::from_millis(500),
            checkpoint_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_millis(500),
        }
    }
}

/// Spawn the per-node snapshot watcher. Exits on `shutdown`.
pub fn spawn_snapshot_watcher(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<Notify>,
    config: RebalanceConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.watcher_poll);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await; // burn the immediate first tick
        loop {
            tokio::select! {
                biased;
                () = shutdown.notified() => return,
                _ = ticker.tick() => {}
            }
            let local = registry.assignment_version();
            match store.load().await {
                Ok(Some(snap)) if snap.version > local => {
                    debug!(local, remote = snap.version, "adopting newer assignment");
                    db.adopt_assignment_snapshot(snap).await;
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "snapshot watcher: load failed"),
            }
        }
    })
}

/// Spawn the leader-gated rebalance controller. Runs on every node;
/// leadership is re-checked after the debounce.
pub fn spawn_rebalance_controller(
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<Notify>,
    config: RebalanceConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut members = controller.members_watch();
        loop {
            tokio::select! {
                biased;
                () = shutdown.notified() => return,
                res = members.changed() => {
                    if res.is_err() { return; }
                }
            }

            // Debounce: absorb further churn before acting.
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.notified() => return,
                    res = tokio::time::timeout(
                        config.rebalance_debounce, members.changed()
                    ) => {
                        match res {
                            Ok(Ok(())) => {}       // another change; keep waiting
                            Ok(Err(_)) => return,  // sender dropped
                            Err(_) => break,        // quiet period elapsed
                        }
                    }
                }
            }

            // Retry transient failures so a single hiccup doesn't
            // leave the cluster on a stale assignment.
            loop {
                if !controller.is_leader() {
                    break;
                }
                let live = controller.live_instances();
                match try_rebalance(&db, &store, &registry, &live, config).await {
                    Ok(Some(v)) => {
                        info!(version = v, "rotated assignment");
                        break;
                    }
                    Ok(None) => {
                        debug!("live set matches current snapshot; no rotation");
                        break;
                    }
                    Err(e) => {
                        warn!(error = %e, "rebalance failed; retrying after backoff");
                        tokio::select! {
                            biased;
                            () = shutdown.notified() => return,
                            () = tokio::time::sleep(config.retry_delay) => {}
                        }
                    }
                }
            }
        }
    })
}

/// `Ok(Some(version))` on rotation (ours or a peer's), `Ok(None)` if
/// no change is needed.
async fn try_rebalance(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    registry: &Arc<VnodeRegistry>,
    live: &[NodeId],
    config: RebalanceConfig,
) -> Result<Option<u64>, String> {
    let current = store
        .load()
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no snapshot on store — boot seed missing".to_string())?;

    let new_assignment = round_robin_assignment(registry.vnode_count(), live);
    let new_vnodes = AssignmentSnapshot::vnodes_from_vec(&new_assignment);
    if new_vnodes == current.vnodes {
        return Ok(None);
    }

    // Drain in-flight shuffle rows into durable state at the old
    // fence version before rotating.
    let ckpt = tokio::time::timeout(config.checkpoint_timeout, db.checkpoint())
        .await
        .map_err(|_| {
            format!(
                "pre-rotation checkpoint did not complete within {}s",
                config.checkpoint_timeout.as_secs()
            )
        })?
        .map_err(|e| e.to_string())?;
    if !ckpt.success {
        return Err(ckpt
            .error
            .unwrap_or_else(|| "checkpoint returned success=false".into()));
    }

    let proposal = current.next(new_vnodes);
    match store
        .save_if_version(&proposal, current.version)
        .await
        .map_err(|e| e.to_string())?
    {
        RotateOutcome::Rotated => {
            let v = proposal.version;
            db.adopt_assignment_snapshot(proposal).await;
            // Keep the current plus one prior as slack for in-flight
            // readers — `prune_before(v - 1)` retains `[v-1, v]`.
            let prune = v.saturating_sub(1);
            if let Err(e) = store.prune_before(prune).await {
                warn!(error = %e, "snapshot prune failed");
            }
            Ok(Some(v))
        }
        RotateOutcome::Conflict(winner) => {
            let v = winner.version;
            db.adopt_assignment_snapshot(winner).await;
            Ok(Some(v))
        }
    }
}
