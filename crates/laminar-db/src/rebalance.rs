//! Dynamic vnode rebalance control plane.
//!
//! Two background tasks are spawned per [`LaminarDB`] on cluster
//! startup when both an [`AssignmentSnapshotStore`] and a
//! [`ClusterController`] are installed:
//!
//! - [`spawn_snapshot_watcher`] runs on **every** node. It polls the
//!   snapshot store and, on a higher version, calls
//!   [`LaminarDB::adopt_assignment_snapshot`]. Adoption takes the
//!   `CheckpointCoordinator` mutex so it lands strictly between
//!   epochs.
//!
//! - [`spawn_rebalance_controller`] runs on **every** node but only
//!   acts when this node is leader. It watches
//!   [`ClusterController::members_watch`] with a debounce and, when
//!   the set of live instances differs from the current snapshot's,
//!   drives the rotation:
//!   1. force a cluster checkpoint via [`LaminarDB::checkpoint`] so
//!      every in-flight shuffle row lands in durable state at the
//!      **old** assignment version,
//!   2. compute `round_robin_assignment(live)` against the new
//!      membership,
//!   3. `save_if_version(new, current.version)` on the snapshot store,
//!   4. adopt locally — the watcher would catch it too, but adopting
//!      synchronously here avoids a two-poll-latency window where the
//!      leader stamps writes at the old version.
//!
//! A CAS conflict means another leader (e.g. a split-brain survivor)
//! raced us to rotate. We adopt that snapshot and return; the fence
//! ([`VnodeRegistry::assignment_version`]) has already been preventing
//! us from corrupting state since the conflicting version landed.
//!
//! Protocol guarantees delivered by this module:
//!
//! - **Monotonic**: CAS via `save_if_version` guarantees no gap or
//!   regression in the durable version sequence.
//! - **No mid-epoch row loss**: the forced checkpoint drains every
//!   shuffle buffer before rotation; after adoption, writes stamp at
//!   the new version and the fence rejects any stale writer.
//! - **No state migration required**: state backends today are
//!   shared-storage (`ObjectStoreBackend`); the new owner of a vnode
//!   reads partials from the same bucket under the shared
//!   `epoch=N/vnode=V/partial.bin` path.
//!
//! Deliberate scope cuts:
//!
//! - No consistent hashing; `round_robin_assignment` fully reshuffles
//!   on any membership change. Fine for small clusters; optimize in a
//!   follow-up.
//! - No graceful drain. A leaving node is treated identically to a
//!   crashed one — phi-accrual detects absence, membership updates,
//!   rebalance rotates.
//! - Rebalance against non-shared-storage state backends is undefined;
//!   the watcher still runs but adoption can't migrate state it can't
//!   see. Single-instance (`InProcessBackend`) deployments never have
//!   a `ClusterController` and so never start these tasks.
//!
//! [`LaminarDB`]: crate::LaminarDB
//! [`LaminarDB::adopt_assignment_snapshot`]: crate::LaminarDB::adopt_assignment_snapshot
//! [`LaminarDB::checkpoint`]: crate::LaminarDB::checkpoint
//! [`AssignmentSnapshotStore`]: laminar_core::cluster::control::AssignmentSnapshotStore
//! [`ClusterController`]: laminar_core::cluster::control::ClusterController
//! [`ClusterController::members_watch`]: laminar_core::cluster::control::ClusterController::members_watch
//! [`VnodeRegistry::assignment_version`]: laminar_core::state::VnodeRegistry::assignment_version

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, RotateOutcome,
};
use laminar_core::state::{round_robin_assignment, NodeId, VnodeRegistry};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::db::LaminarDB;

/// Tunables for the rebalance control plane. Defaults are
/// production values; the test harness overrides via
/// [`RebalanceConfig::test_defaults`] to keep integration-test wall
/// time under a few seconds.
#[derive(Debug, Clone, Copy)]
pub struct RebalanceConfig {
    /// Interval between snapshot-store polls on each node. Shorter
    /// means faster follower adoption but more list-calls on the
    /// object store.
    pub watcher_poll: Duration,
    /// Time of no membership change required before the leader
    /// triggers a rebalance. Gives shuffle-address gossip time to
    /// converge so a just-joined node isn't assigned vnodes before
    /// its receiver is reachable.
    pub rebalance_debounce: Duration,
    /// Maximum time to wait for the forced pre-rotation checkpoint
    /// before giving up and retrying on the next membership event.
    pub checkpoint_timeout: Duration,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            watcher_poll: Duration::from_secs(2),
            rebalance_debounce: Duration::from_secs(5),
            checkpoint_timeout: Duration::from_secs(60),
        }
    }
}

impl RebalanceConfig {
    /// Fast defaults for integration tests: 200ms polls, 500ms
    /// debounce. Production must not use these — a 500ms debounce
    /// thrashes under normal gossip churn.
    #[must_use]
    pub fn test_defaults() -> Self {
        Self {
            watcher_poll: Duration::from_millis(200),
            rebalance_debounce: Duration::from_millis(500),
            checkpoint_timeout: Duration::from_secs(30),
        }
    }
}

/// Spawn the per-node snapshot watcher with default tunables. Use
/// [`spawn_snapshot_watcher_with`] to inject a [`RebalanceConfig`].
pub fn spawn_snapshot_watcher(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
) -> JoinHandle<()> {
    spawn_snapshot_watcher_with(db, store, registry, shutdown, RebalanceConfig::default())
}

/// Spawn the per-node snapshot watcher with an explicit config.
pub fn spawn_snapshot_watcher_with(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
    config: RebalanceConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        snapshot_watcher_loop(db, store, registry, shutdown, config).await;
    })
}

async fn snapshot_watcher_loop(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
    config: RebalanceConfig,
) {
    let mut ticker = tokio::time::interval(config.watcher_poll);
    ticker.tick().await; // burn the immediate first tick
    loop {
        tokio::select! {
            biased;
            () = shutdown.notified() => return,
            _ = ticker.tick() => {}
        }
        let local_version = registry.assignment_version();
        match store.load().await {
            Ok(Some(snap)) if snap.version > local_version => {
                debug!(
                    local = local_version,
                    remote = snap.version,
                    "snapshot watcher: adopting newer version",
                );
                db.adopt_assignment_snapshot(snap).await;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "snapshot watcher: load failed");
            }
        }
    }
}

/// Spawn the leader's rebalance controller with default tunables.
/// Runs on every node but only acts when this node is leader
/// (re-checked after debounce). Exits cleanly when `shutdown` is
/// notified.
pub fn spawn_rebalance_controller(
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
) -> JoinHandle<()> {
    spawn_rebalance_controller_with(
        db,
        controller,
        store,
        registry,
        shutdown,
        RebalanceConfig::default(),
    )
}

/// Spawn the leader's rebalance controller with an explicit config.
pub fn spawn_rebalance_controller_with(
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
    config: RebalanceConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        rebalance_controller_loop(db, controller, store, registry, shutdown, config).await;
    })
}

async fn rebalance_controller_loop(
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
    config: RebalanceConfig,
) {
    let mut members = controller.members_watch();
    loop {
        // Wait for a membership change OR shutdown. `changed()`
        // returns after every update; we re-check leadership +
        // set-equality inside the debounce so a quick
        // joiner-then-leave round-trip only triggers one rebalance.
        tokio::select! {
            biased;
            () = shutdown.notified() => return,
            res = members.changed() => {
                if res.is_err() {
                    // Sender dropped — engine shutting down.
                    return;
                }
            }
        }

        // Debounce: absorb further churn.
        loop {
            tokio::select! {
                biased;
                () = shutdown.notified() => return,
                res = tokio::time::timeout(config.rebalance_debounce, members.changed()) => {
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => return,
                        Err(_) => break,
                    }
                }
            }
        }

        if !controller.is_leader() {
            continue;
        }

        let live: Vec<NodeId> = controller.live_instances();
        match try_rebalance(&db, &store, &registry, &live, config).await {
            RebalanceResult::Rotated(v) => {
                info!(version = v, "leader rotated assignment snapshot");
            }
            RebalanceResult::AlreadyAgrees => {
                debug!("leader: live set matches current snapshot; no rotation");
            }
            RebalanceResult::Conflict(adopted) => {
                info!(
                    version = adopted,
                    "rebalance CAS conflict — adopted peer's snapshot",
                );
            }
            RebalanceResult::CheckpointFailed(e) => {
                warn!(error = %e, "rebalance skipped: pre-rotation checkpoint failed");
            }
            RebalanceResult::StoreFailed(e) => {
                warn!(error = %e, "rebalance skipped: snapshot store I/O failed");
            }
            RebalanceResult::NoCurrentSnapshot => {
                warn!(
                    "rebalance skipped: no current snapshot on store — \
                     boot-time CAS-create should have seeded one",
                );
            }
        }
    }
}

#[derive(Debug)]
enum RebalanceResult {
    Rotated(u64),
    AlreadyAgrees,
    Conflict(u64),
    CheckpointFailed(String),
    StoreFailed(String),
    NoCurrentSnapshot,
}

async fn try_rebalance(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    registry: &Arc<VnodeRegistry>,
    live: &[NodeId],
    config: RebalanceConfig,
) -> RebalanceResult {
    // Load the current canonical snapshot. If the leader's local
    // registry lags (e.g. this node just became leader), the fetched
    // snapshot is the authoritative starting point.
    let current = match store.load().await {
        Ok(Some(s)) => s,
        Ok(None) => return RebalanceResult::NoCurrentSnapshot,
        Err(e) => return RebalanceResult::StoreFailed(e.to_string()),
    };

    let vnode_count = registry.vnode_count();
    let new_assignment = round_robin_assignment(vnode_count, live);
    let new_vnodes = AssignmentSnapshot::vnodes_from_vec(&new_assignment);

    if new_vnodes == current.vnodes {
        return RebalanceResult::AlreadyAgrees;
    }

    // Force a checkpoint before rotating so every in-flight shuffle
    // row flushes into durable state under the OLD assignment. The
    // rotation then moves ownership cleanly: any writer not yet
    // adopted is fenced out by the authoritative version bump, and
    // the NEW owner of a vnode inherits its durable partials via
    // shared storage.
    match tokio::time::timeout(config.checkpoint_timeout, db.checkpoint()).await {
        Ok(Ok(result)) if result.success => {}
        Ok(Ok(result)) => {
            return RebalanceResult::CheckpointFailed(
                result
                    .error
                    .unwrap_or_else(|| "checkpoint returned success=false with no error".into()),
            );
        }
        Ok(Err(e)) => return RebalanceResult::CheckpointFailed(e.to_string()),
        Err(_) => {
            return RebalanceResult::CheckpointFailed(format!(
                "checkpoint did not complete within {}s",
                config.checkpoint_timeout.as_secs(),
            ));
        }
    }

    let proposal = current.next(new_vnodes);
    match store.save_if_version(&proposal, current.version).await {
        Ok(RotateOutcome::Rotated) => {
            let v = proposal.version;
            db.adopt_assignment_snapshot(proposal).await;
            RebalanceResult::Rotated(v)
        }
        Ok(RotateOutcome::Conflict(winner)) => {
            let v = winner.version;
            db.adopt_assignment_snapshot(winner).await;
            RebalanceResult::Conflict(v)
        }
        Err(e) => RebalanceResult::StoreFailed(e.to_string()),
    }
}
