//! Dynamic vnode rebalance control plane.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, RotateOutcome,
};
use laminar_core::state::{
    owners_per_domain, rendezvous_assignment, Locality, NodeId, VnodeRegistry,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::db::{LaminarDB, SnapshotAdoption};
use crate::engine_metrics::EngineMetrics;

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
    /// Locality tier the placement metrics group by (0 = coarsest).
    pub placement_isolation_tier: usize,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            watcher_poll: Duration::from_secs(2),
            rebalance_debounce: Duration::from_secs(5),
            // A healthy pre-rotation drain commits in well under a
            // second; a long budget only delays recovery when a node
            // dies mid-drain (the drain then cannot succeed and the
            // rotation is what restores commit availability).
            checkpoint_timeout: Duration::from_secs(15),
            retry_delay: Duration::from_secs(2),
            placement_isolation_tier: 0,
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
            placement_isolation_tier: 0,
        }
    }
}

/// Surface the rehydration outcome of an adopted snapshot. A node that
/// gained vnodes pulled their last committed state off the shared backend
/// in [`LaminarDB::adopt_assignment_snapshot`]; log what moved so operators
/// have an audit trail of every rebalance-driven state transfer.
fn log_adoption(source: &str, adoption: &SnapshotAdoption) {
    if adoption.newly_acquired.is_empty() {
        return;
    }
    info!(
        source,
        version = adoption.version,
        newly_acquired = adoption.newly_acquired.len(),
        rehydrated = adoption.rehydrated,
        rehydration_epoch = ?adoption.rehydration_epoch,
        "rehydrated newly-acquired vnodes after rebalance",
    );
}

/// Spawn the per-node snapshot watcher. Exits on `shutdown`.
pub fn spawn_snapshot_watcher(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<Notify>,
    config: RebalanceConfig,
    controller: Option<Arc<ClusterController>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.watcher_poll);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await; // burn the immediate first tick
        let mut published_version = 0;
        loop {
            tokio::select! {
                biased;
                () = shutdown.notified() => return,
                _ = ticker.tick() => {}
            }
            let local = registry.assignment_version();

            let mut remote_newer = true;
            if let Some(ref c) = controller {
                if let Some(gossiped_version) = c.read_snapshot_version().await {
                    if gossiped_version <= local {
                        remote_newer = false;
                    }
                }
            }

            if remote_newer {
                match store.load().await {
                    Ok(Some(snap)) if snap.version > local => {
                        debug!(local, remote = snap.version, "adopting newer assignment");
                        let adoption = db.adopt_assignment_snapshot(snap).await;
                        log_adoption("watcher", &adoption);
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "snapshot watcher: load failed"),
                }
            }

            // Refresh placement metrics only when the assignment changed.
            let version = registry.assignment_version();
            if version != published_version {
                published_version = version;
                if let (Some(c), Some(metrics)) = (controller.as_ref(), db.engine_metrics()) {
                    let nodes = c.assignable_with_locality();
                    publish_placement_metrics(
                        &metrics,
                        &registry,
                        &nodes,
                        config.placement_isolation_tier,
                    );
                }
            }
        }
    })
}

/// Publish per-domain owner counts and the blast-radius ratio. The gauge vector
/// is reset so domains that disappear don't leave stale series.
#[allow(clippy::cast_precision_loss)]
fn publish_placement_metrics(
    metrics: &EngineMetrics,
    registry: &VnodeRegistry,
    nodes: &[(NodeId, Locality)],
    isolation_tier: usize,
) {
    let owners = registry.snapshot();
    let total = owners.len().max(1);
    let counts = owners_per_domain(&owners, nodes, isolation_tier);

    metrics.placement_vnodes_per_domain.reset();
    let mut max = 0u32;
    for (domain, &count) in &counts {
        let label = if domain.is_empty() {
            "unknown"
        } else {
            domain.as_str()
        };
        metrics
            .placement_vnodes_per_domain
            .with_label_values(&[label])
            .set(i64::from(count));
        max = max.max(count);
    }
    metrics
        .placement_blast_radius_ratio
        .set(f64::from(max) / total as f64);
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
                    if res.is_err() {
                        warn!("membership watch sender dropped; rebalance controller exiting");
                        return;
                    }
                }
            }
            debug!("membership change observed; debouncing");

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
                    debug!("membership changed; not the leader — skipping rotation check");
                    break;
                }
                // Use assignable (Active, non-draining) instances so
                // Draining/Suspected nodes are never handed vnodes. The
                // weak leader gate above still uses live membership;
                // `LeaderLeaseManager` is the fencing authority for split-
                // brain hardening (kept standalone, see leader_lease.rs).
                let live = controller.assignable_instances();
                match try_rebalance(&db, &controller, &store, &registry, &live, config).await {
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

/// Poll the durable assignment snapshot until `me` owns no vnodes (its
/// state has been reassigned elsewhere) or `deadline` elapses. Returns
/// true if fully drained. Used by a draining node to know when it is
/// safe to exit.
pub async fn wait_until_drained(
    store: &AssignmentSnapshotStore,
    me: NodeId,
    poll: Duration,
    deadline: Duration,
) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        match store.load().await {
            // No snapshot at all → nothing owns us → drained.
            Ok(None) => return true,
            Ok(Some(snap)) => {
                if !snap.vnodes.values().any(|owner| *owner == me) {
                    return true;
                }
            }
            Err(e) => warn!(error = %e, "wait_until_drained: snapshot load failed"),
        }
        if start.elapsed() >= deadline {
            return false;
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        tokio::time::sleep(poll.min(remaining)).await;
        if start.elapsed() >= deadline {
            return false;
        }
    }
}

/// `Ok(Some(version))` on rotation (ours or a peer's), `Ok(None)` if
/// no change is needed.
async fn try_rebalance(
    db: &Arc<LaminarDB>,
    controller: &Arc<ClusterController>,
    store: &Arc<AssignmentSnapshotStore>,
    registry: &Arc<VnodeRegistry>,
    live: &[NodeId],
    config: RebalanceConfig,
) -> Result<Option<u64>, String> {
    // No assignable instances (whole cluster draining/joining) — nothing to
    // rotate to. Hold the current assignment rather than panicking the
    // placement call on an empty node set.
    if live.is_empty() {
        return Ok(None);
    }

    let current = store
        .load()
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no snapshot on store — boot seed missing".to_string())?;

    let new_assignment = rendezvous_assignment(registry.vnode_count(), live);
    let new_vnodes = AssignmentSnapshot::vnodes_from_vec(&new_assignment);
    if new_vnodes == current.vnodes {
        return Ok(None);
    }

    // Drain in-flight shuffle rows into durable state at the old
    // fence version before rotating. When the rotation sheds a DEAD
    // node, skip the drain entirely: its durability gate needs captures
    // from a node that will never provide them, so it can only burn its
    // full timeout and abort — deadlocking rotation against the gate
    // when rotation is the only thing that restores commit
    // availability. The dead node's in-flight rows are unrecoverable
    // regardless; survivors rehydrate its vnodes from the last
    // committed epoch — the same at-least-once duplication window every
    // ungraceful failover already has.
    let shedding_dead = {
        let owners = current.to_vnode_vec(registry.vnode_count());
        owners.iter().any(|o| !live.contains(o))
    };
    if shedding_dead {
        warn!(
            "rotation sheds a dead node — skipping the pre-rotation drain \
             checkpoint (it cannot seal without the dead node's captures)"
        );
    } else {
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
    }

    let proposal = current.next(new_vnodes);
    match store
        .save_if_version(&proposal, current.version)
        .await
        .map_err(|e| e.to_string())?
    {
        RotateOutcome::Rotated => {
            let v = proposal.version;
            let adoption = db.adopt_assignment_snapshot(proposal).await;
            log_adoption("rebalance", &adoption);
            controller.announce_snapshot_version(v).await;
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
            let adoption = db.adopt_assignment_snapshot(winner).await;
            log_adoption("rebalance-conflict", &adoption);
            controller.announce_snapshot_version(v).await;
            Ok(Some(v))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    fn store() -> AssignmentSnapshotStore {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        AssignmentSnapshotStore::new(mem)
    }

    #[test]
    fn publish_placement_metrics_labels_by_domain() {
        let prom = prometheus::Registry::new();
        let metrics = EngineMetrics::new(&prom);

        // 4 vnodes: node 1 owns two, node 2 owns one, one is unassigned.
        let vreg = VnodeRegistry::new(4);
        vreg.set_assignment(vec![NodeId(1), NodeId(1), NodeId(2), NodeId::UNASSIGNED].into());
        let nodes = vec![
            (NodeId(1), Locality::parse("region=r;zone=z1")),
            (NodeId(2), Locality::parse("region=r;zone=z2")),
        ];

        publish_placement_metrics(&metrics, &vreg, &nodes, 1); // isolation_tier 1 = zone

        let g = &metrics.placement_vnodes_per_domain;
        assert_eq!(g.with_label_values(&["r;z1"]).get(), 2);
        assert_eq!(g.with_label_values(&["r;z2"]).get(), 1);
        assert_eq!(g.with_label_values(&["unknown"]).get(), 1); // the unassigned vnode
                                                                // Blast radius = largest domain (2) / total vnodes (4).
        assert!((metrics.placement_blast_radius_ratio.get() - 0.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn wait_until_drained_false_while_owning_vnodes() {
        let s = store();
        let me = NodeId(1);
        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, me);
        vnodes.insert(1, NodeId(2));
        let snap = AssignmentSnapshot::empty().next(vnodes);
        s.save_if_absent(&snap).await.unwrap();

        let drained = wait_until_drained(
            &s,
            me,
            Duration::from_millis(20),
            Duration::from_millis(120),
        )
        .await;
        assert!(!drained, "still owns vnode 0 → not drained");
    }

    #[tokio::test]
    async fn wait_until_drained_true_when_owning_none() {
        let s = store();
        let me = NodeId(1);
        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, NodeId(2));
        vnodes.insert(1, NodeId(3));
        let snap = AssignmentSnapshot::empty().next(vnodes);
        s.save_if_absent(&snap).await.unwrap();

        let drained =
            wait_until_drained(&s, me, Duration::from_millis(20), Duration::from_secs(5)).await;
        assert!(drained, "owns no vnode → drained quickly");
    }

    #[tokio::test]
    async fn wait_until_drained_true_when_no_snapshot() {
        let s = store();
        let drained = wait_until_drained(
            &s,
            NodeId(1),
            Duration::from_millis(20),
            Duration::from_secs(5),
        )
        .await;
        assert!(drained, "no snapshot → treated as drained");
    }
}
