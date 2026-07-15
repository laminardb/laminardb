//! Dynamic vnode rebalance control plane.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use laminar_core::checkpoint::AssignmentDrainTransition;
use laminar_core::cluster::control::{
    AssignmentDrainDecision, AssignmentDrainVerdict, AssignmentSnapshot, AssignmentSnapshotStore,
    CheckpointAssignmentAdoption, CheckpointAssignmentFence, CheckpointParticipant,
    ClusterController, LeaderLeaseStore, RecordAssignmentDrainDecisionResult, RotateOutcome,
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
    /// Bound on waiting for all live nodes to ack the draining snapshot before the
    /// pre-rotation checkpoint; on timeout the rotation aborts. Must exceed `watcher_poll`.
    pub drain_ack_timeout: Duration,
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
            drain_ack_timeout: Duration::from_secs(10),
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
            drain_ack_timeout: Duration::from_secs(5),
            placement_isolation_tier: 0,
        }
    }
}

/// Log what moved so there's an audit trail of every rebalance-driven state transfer.
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

async fn close_local_assignment_authority(
    db: &Arc<LaminarDB>,
    controller: Option<&ClusterController>,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    db.set_source_gate(true);
    if let Some(controller) = controller {
        controller.publish_checkpoint_assignment_fence(None);
        controller.publish_checkpoint_drain_transition(None);
    }
    // Cancel first: a compute-cycle read guard may itself be blocked in shuffle admission.
    db.invalidate_shuffle_assignment_fence();
    let _adoption = tokio::time::timeout_at(deadline, db.assignment_adoption_lock.lock())
        .await
        .map_err(|_| "timed out serializing assignment authority closure".to_string())?;
    // A watcher that already owned the adoption lock could have republished and reopened after
    // the first cancellation. Reassert the full closure while serialized, before draining it.
    db.set_source_gate(true);
    if let Some(controller) = controller {
        controller.publish_checkpoint_assignment_fence(None);
        controller.publish_checkpoint_drain_transition(None);
    }
    db.invalidate_shuffle_assignment_fence();
    let _transition = tokio::time::timeout_at(
        deadline,
        Arc::clone(&db.rotation_execution_fence).write_owned(),
    )
    .await
    .map_err(|_| "timed out draining assignment execution after closure".to_string())?;
    Ok(())
}

/// Fail closed for a transient durable snapshot read without forcing a new assignment version.
/// The exact retained certificate can resume after the same durable head is audited again.
async fn suspend_local_assignment_authority(
    db: &Arc<LaminarDB>,
    controller: Option<&ClusterController>,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    db.set_source_gate(true);
    if let Some(controller) = controller {
        controller.publish_checkpoint_assignment_fence(None);
        controller.publish_checkpoint_drain_transition(None);
    }
    // Preserve the certificate and its sequence domain, but cancel its active lifetime before
    // waiting for compute cycles that may be blocked on that lifetime.
    db.suspend_shuffle_assignment_fence();
    let _adoption = tokio::time::timeout_at(deadline, db.assignment_adoption_lock.lock())
        .await
        .map_err(|_| "timed out serializing assignment authority suspension".to_string())?;
    // A watcher that already owned the adoption lock could have republished and resumed after
    // the first cancellation. Reassert the full suspension while serialized, before draining it.
    db.set_source_gate(true);
    if let Some(controller) = controller {
        controller.publish_checkpoint_assignment_fence(None);
        controller.publish_checkpoint_drain_transition(None);
    }
    db.suspend_shuffle_assignment_fence();
    let _transition = tokio::time::timeout_at(
        deadline,
        Arc::clone(&db.rotation_execution_fence).write_owned(),
    )
    .await
    .map_err(|_| "timed out draining assignment execution after suspension".to_string())?;
    Ok(())
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
        let mut published_adoption: Option<CheckpointAssignmentAdoption> = None;
        let mut metrics_version = 0;
        // Highest draining snapshot already adopted; a draining snapshot doesn't
        // bump the registry version, so this prevents re-adopting it every tick.
        let mut last_drained = 0u64;
        let mut durable_snapshot: Option<AssignmentSnapshot>;
        let mut durable_drain_transition: Option<AssignmentDrainTransition>;
        let mut active_local_drain: Option<AssignmentDrainTransition> = None;
        let mut installed_fence: Option<(u64, [u8; 32])> = None;
        let mut installed_authority_revision = 0u64;
        let mut assignment_invalidated = false;
        loop {
            tokio::select! {
                biased;
                () = shutdown.notified() => return,
                _ = ticker.tick() => {}
            }
            let local = registry.assignment_version();
            let head_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
            let mut authority_revision = db
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire);

            // Gossip only shortens discovery latency. The durable namespace remains authoritative,
            // so every audit reads it even when the last advertised version equals the local one.
            // This closes the crash window after a successful CAS but before adoption/announcement.
            let audit = tokio::select! {
                biased;
                () = shutdown.notified() => return,
                result = tokio::time::timeout_at(head_deadline, store.load()) => result,
            };
            if let Ok(Ok(Some(snapshot))) = &audit {
                if !snapshot.draining {
                    let materialization_audit = tokio::time::timeout_at(
                        head_deadline,
                        audit_assignment_snapshot_authority(
                            &store,
                            controller.as_deref(),
                            snapshot,
                        ),
                    )
                    .await;
                    if !matches!(&materialization_audit, Ok(Ok(()))) {
                        assignment_invalidated = true;
                        let _ = suspend_local_assignment_authority(
                            &db,
                            controller.as_deref(),
                            head_deadline,
                        )
                        .await;
                        match materialization_audit {
                            Ok(Err(error)) => {
                                warn!(%error, version = snapshot.version, "snapshot watcher: drain finalization audit failed; assignment authority suspended")
                            }
                            Err(_) => {
                                warn!(version = snapshot.version, timeout = ?config.checkpoint_timeout, "snapshot watcher: drain finalization audit timed out; assignment authority suspended")
                            }
                            Ok(Ok(())) => unreachable!(),
                        }
                        continue;
                    }
                }
            }
            match audit {
                // Drain phase: pause the partitions of vnodes we're about to
                // lose; ownership is unchanged so the registry version stays put.
                Ok(Ok(Some(snap))) if snap.draining && snap.version > local => {
                    // A draining head describes the successor map; checkpoint and shuffle still
                    // belong to the exact committed predecessor until the pre-rotation cut lands.
                    // Re-audit that predecessor on every tick, including after this process starts
                    // while a drain is already in progress. Retaining a previously cached fence
                    // without this read would trust an object that may no longer be the durable
                    // predecessor of the observed head.
                    let predecessor = tokio::time::timeout_at(
                        head_deadline,
                        audit_drain_predecessor(&store, &registry, &snap, controller.as_deref()),
                    )
                    .await;
                    match predecessor {
                        Ok(Ok(predecessor)) => {
                            let transition = snap
                                .drain_transition
                                .as_ref()
                                .expect("validated draining snapshot has a transition")
                                .clone();
                            durable_drain_transition = Some(transition.clone());
                            durable_snapshot = Some(predecessor);
                            if let Some(ref c) = controller {
                                c.publish_checkpoint_drain_transition(Some(transition.clone()));
                            }
                            if snap.version != last_drained {
                                match db.adopt_draining_snapshot(&snap) {
                                    Ok(revoking_vnodes) => {
                                        let acknowledgement = async {
                                            let c = controller.as_ref().ok_or_else(|| {
                                                "assignment drain has no cluster controller"
                                                    .to_string()
                                            })?;
                                            let Some(participant) =
                                                local_drain_participant(c, &transition)
                                            else {
                                                // A target-only joining process has no predecessor
                                                // input authority and is intentionally absent from
                                                // the receipt quorum.
                                                return Ok(());
                                            };
                                            active_local_drain = Some(transition.clone());
                                            prepare_and_announce_local_drain(
                                                &db,
                                                &store,
                                                &registry,
                                                c,
                                                &snap,
                                                &transition,
                                                participant,
                                                &revoking_vnodes,
                                                head_deadline,
                                                config.drain_ack_timeout,
                                            )
                                            .await
                                        }
                                        .await;
                                        match acknowledgement {
                                            Ok(()) => {
                                                last_drained = snap.version;
                                                // Receipt production may consume the entire head
                                                // budget. Re-read and recertify from a fresh
                                                // durable head on the next tick.
                                                continue;
                                            }
                                            Err(error) => {
                                                warn!(%error, version = snap.version, "snapshot watcher: durable drain acknowledgement failed; retrying");
                                                continue;
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        warn!(%error, "snapshot watcher: draining adoption failed");
                                    }
                                }
                            }
                        }
                        Ok(Err(error)) => {
                            durable_snapshot = None;
                            durable_drain_transition = None;
                            assignment_invalidated = true;
                            let _ = suspend_local_assignment_authority(
                                &db,
                                controller.as_deref(),
                                head_deadline,
                            )
                            .await;
                            warn!(%error, version = snap.version, "snapshot watcher: draining predecessor audit failed; assignment authority suspended");
                        }
                        Err(_) => {
                            durable_snapshot = None;
                            durable_drain_transition = None;
                            assignment_invalidated = true;
                            let _ = suspend_local_assignment_authority(
                                &db,
                                controller.as_deref(),
                                head_deadline,
                            )
                            .await;
                            warn!(version = snap.version, timeout = ?config.checkpoint_timeout, "snapshot watcher: draining predecessor audit timed out; assignment authority suspended");
                        }
                    }
                }
                Ok(Ok(Some(snap))) if !snap.draining && snap.version > local => {
                    if let Some(transition) = active_local_drain.clone() {
                        match settle_observed_local_drain(
                            &db,
                            &store,
                            &registry,
                            controller.as_deref(),
                            &transition,
                            &snap,
                            head_deadline,
                            config.drain_ack_timeout,
                        )
                        .await
                        {
                            Ok(true) => {
                                active_local_drain = None;
                                authority_revision = db
                                    .assignment_authority_revision
                                    .load(std::sync::atomic::Ordering::Acquire);
                            }
                            Ok(false) => {}
                            Err(error) => {
                                assignment_invalidated = true;
                                let _ = suspend_local_assignment_authority(
                                    &db,
                                    controller.as_deref(),
                                    head_deadline,
                                )
                                .await;
                                warn!(%error, version = snap.version, "snapshot watcher: local source drain resolution failed; assignment authority suspended");
                                continue;
                            }
                        }
                    }
                    durable_drain_transition = None;
                    durable_snapshot = Some(snap.clone());
                    let resolved_local = registry.assignment_version();
                    if snap.version > resolved_local {
                        debug!(
                            local = resolved_local,
                            remote = snap.version,
                            "adopting newer assignment"
                        );
                        match db.adopt_assignment_snapshot(snap, head_deadline).await {
                            Ok(adoption) => {
                                authority_revision = db
                                    .assignment_authority_revision
                                    .load(std::sync::atomic::Ordering::Acquire);
                                log_adoption("watcher", &adoption);
                            }
                            Err(e) => warn!(error = %e, "snapshot watcher: adoption failed"),
                        }
                    }
                }
                Ok(Ok(Some(snap))) => {
                    if let Some(transition) = active_local_drain.clone() {
                        match settle_observed_local_drain(
                            &db,
                            &store,
                            &registry,
                            controller.as_deref(),
                            &transition,
                            &snap,
                            head_deadline,
                            config.drain_ack_timeout,
                        )
                        .await
                        {
                            Ok(true) => {
                                active_local_drain = None;
                                authority_revision = db
                                    .assignment_authority_revision
                                    .load(std::sync::atomic::Ordering::Acquire);
                            }
                            Ok(false) => {}
                            Err(error) => {
                                assignment_invalidated = true;
                                let _ = suspend_local_assignment_authority(
                                    &db,
                                    controller.as_deref(),
                                    head_deadline,
                                )
                                .await;
                                warn!(%error, version = snap.version, "snapshot watcher: local source drain resolution failed; assignment authority suspended");
                                continue;
                            }
                        }
                    }
                    durable_drain_transition = snap.drain_transition.clone();
                    durable_snapshot = Some(snap);
                }
                Ok(Ok(None)) => {
                    durable_drain_transition = None;
                    durable_snapshot = None;
                }
                Ok(Err(error)) => {
                    durable_snapshot = None;
                    durable_drain_transition = None;
                    assignment_invalidated = true;
                    let _ = suspend_local_assignment_authority(
                        &db,
                        controller.as_deref(),
                        head_deadline,
                    )
                    .await;
                    warn!(%error, "snapshot watcher: durable audit failed; assignment authority suspended");
                }
                Err(_) => {
                    durable_snapshot = None;
                    durable_drain_transition = None;
                    assignment_invalidated = true;
                    let _ = suspend_local_assignment_authority(
                        &db,
                        controller.as_deref(),
                        head_deadline,
                    )
                    .await;
                    warn!(
                        timeout = ?config.checkpoint_timeout,
                        "snapshot watcher: durable audit timed out; assignment authority suspended"
                    );
                }
            }

            let current_authority_revision = db
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire);
            if authority_revision != current_authority_revision {
                // The durable head used above predates an authority closure by another adoption.
                // Keep that closure in force and re-read the head on the next tick.
                db.set_source_gate(true);
                if let Some(ref c) = controller {
                    c.publish_checkpoint_drain_transition(None);
                    c.publish_checkpoint_assignment_fence(None);
                }
                assignment_invalidated = true;
                continue;
            }
            if installed_fence.is_some()
                && installed_authority_revision != current_authority_revision
            {
                assignment_invalidated = true;
            }

            let assignment = registry.versioned_snapshot();
            let version = assignment.version();
            if let Some(ref c) = controller {
                let owner_ids: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
                let adoption = CheckpointAssignmentAdoption {
                    participant: CheckpointParticipant {
                        node_id: c.instance_id().0,
                        boot_incarnation: c.recovery_incarnation(),
                    },
                    assignment_version: version,
                    vnode_count: registry.vnode_count(),
                    assignment_digest: CheckpointAssignmentFence::owner_map_digest(
                        registry.vnode_count(),
                        &owner_ids,
                    ),
                };
                if published_adoption.as_ref() != Some(&adoption) {
                    match tokio::time::timeout_at(
                        head_deadline,
                        c.announce_adopted_assignment(&adoption),
                    )
                    .await
                    {
                        Ok(Ok(())) => published_adoption = Some(adoption),
                        Ok(Err(error)) => {
                            warn!(%error, version, "adopted assignment publication failed; retrying");
                        }
                        Err(_) => {
                            warn!(
                                version,
                                "adopted assignment publication exceeded the head deadline"
                            );
                        }
                    }
                }
            }
            if version != metrics_version {
                metrics_version = version;
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

            // Publish a version-bound, owner-complete fence off the hot path on every node.
            // The adoption lock and authority revision reject a proof computed from a head that
            // another task fenced while this watcher was awaiting durable I/O.
            if let Some(ref c) = controller {
                let fence = match durable_snapshot.as_ref() {
                    Some(snapshot)
                        if !snapshot.draining
                            && snapshot.version == version
                            && snapshot.has_canonical_participants()
                            && snapshot
                                .to_vnode_vec(registry.vnode_count())
                                .is_ok_and(|owners| owners.as_slice() == assignment.owners()) =>
                    {
                        match tokio::time::timeout_at(
                            head_deadline,
                            compute_checkpoint_assignment_fence(
                                c,
                                &registry,
                                &snapshot.participants,
                            ),
                        )
                        .await
                        {
                            Ok(fence) => {
                                fence.filter(|fence| fence.participants == snapshot.participants)
                            }
                            Err(_) => {
                                warn!(
                                    version,
                                    "assignment fence computation exceeded the head deadline"
                                );
                                None
                            }
                        }
                    }
                    _ => None,
                };
                if authority_revision
                    != db
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    db.set_source_gate(true);
                    c.publish_checkpoint_drain_transition(None);
                    c.publish_checkpoint_assignment_fence(None);
                    assignment_invalidated = true;
                    continue;
                }
                let drain_transition = durable_drain_transition
                    .as_ref()
                    .filter(|transition| fence.as_ref() == Some(&transition.predecessor))
                    .cloned();
                match fence {
                    Some(fence) => {
                        let identity = (fence.assignment_version, fence.digest());
                        let published_fence =
                            c.checkpoint_assignment_fence(fence.assignment_version);
                        let needs_activation = assignment_invalidated
                            || installed_fence != Some(identity)
                            || published_fence.as_ref() != Some(&fence)
                            || c.checkpoint_drain_transition() != drain_transition
                            || (!c.is_recovering() && db.cluster_intake_fenced());
                        if needs_activation {
                            db.set_source_gate(true);
                            if !assignment_invalidated && installed_fence != Some(identity) {
                                // A different certificate supersedes the active lifetime. Cancel it
                                // before waiting for compute cycles that may be blocked in send.
                                c.publish_checkpoint_drain_transition(None);
                                c.publish_checkpoint_assignment_fence(None);
                                db.invalidate_shuffle_assignment_fence();
                                authority_revision = db
                                    .assignment_authority_revision
                                    .load(std::sync::atomic::Ordering::Acquire);
                                installed_fence = None;
                            }
                            match db
                                .activate_assignment_authority(
                                    &fence,
                                    drain_transition,
                                    authority_revision,
                                    head_deadline,
                                )
                                .await
                            {
                                Ok(activation) if activation.installed => {
                                    installed_fence = Some(identity);
                                    installed_authority_revision = activation.revision;
                                    assignment_invalidated = false;
                                }
                                Ok(_) => assignment_invalidated = true,
                                Err(error) => {
                                    c.publish_checkpoint_drain_transition(None);
                                    c.publish_checkpoint_assignment_fence(None);
                                    installed_fence = None;
                                    assignment_invalidated = true;
                                    warn!(%error, version, "shuffle assignment certificate install failed");
                                }
                            }
                        }
                    }
                    None if !assignment_invalidated => {
                        // A process-roster change can invalidate a certificate without changing
                        // vnode owners. Cancel it before waiting for the old compute scope.
                        installed_fence = None;
                        assignment_invalidated = true;
                        if close_local_assignment_authority(&db, Some(c.as_ref()), head_deadline)
                            .await
                            .is_err()
                        {
                            warn!(
                                version,
                                "assignment invalidation could not drain the prior execution scope"
                            );
                        }
                    }
                    None => {}
                }
            }
        }
    })
}

/// Per-node checkpoint fence. A reported version is insufficient: the exact current
/// assignment must be owner-complete over the same canonical participant set.
async fn compute_checkpoint_assignment_fence(
    c: &ClusterController,
    registry: &VnodeRegistry,
    expected_participants: &[CheckpointParticipant],
) -> Option<CheckpointAssignmentFence> {
    let assignment = registry.versioned_snapshot();
    let participant_ids: Vec<u64> = expected_participants
        .iter()
        .map(|participant| participant.node_id)
        .collect();
    let participants = c
        .recovery_participant_incarnations(&participant_ids)
        .await
        .ok()?;
    if participants != expected_participants {
        return None;
    }
    let reported: rustc_hash::FxHashMap<u64, CheckpointAssignmentAdoption> = c
        .read_adopted_assignments()
        .await
        .ok()?
        .into_iter()
        .map(|(node, adoption)| (node.0, adoption))
        .collect();
    checkpoint_assignment_fence(
        assignment.version(),
        assignment.owners(),
        participants,
        &reported,
    )
}

fn checkpoint_assignment_fence(
    assignment_version: u64,
    owners: &[NodeId],
    participants: Vec<CheckpointParticipant>,
    reported: &rustc_hash::FxHashMap<u64, CheckpointAssignmentAdoption>,
) -> Option<CheckpointAssignmentFence> {
    let owner_ids: Vec<u64> = owners.iter().map(|owner| owner.0).collect();
    let vnode_count = u32::try_from(owners.len()).ok()?;
    let assignment_digest = CheckpointAssignmentFence::owner_map_digest(vnode_count, &owner_ids);
    if assignment_version == 0
        || owners.is_empty()
        || participants.is_empty()
        || owners.iter().any(|owner| {
            owner.is_unassigned()
                || participants
                    .binary_search_by_key(&owner.0, |participant| participant.node_id)
                    .is_err()
        })
        || participants.iter().any(|participant| {
            reported.get(&participant.node_id).is_none_or(|adoption| {
                adoption.participant != *participant
                    || adoption.assignment_version != assignment_version
                    || adoption.vnode_count != vnode_count
                    || adoption.assignment_digest != assignment_digest
            })
        })
    {
        return None;
    }

    CheckpointAssignmentFence::from_owner_map(assignment_version, &owner_ids, participants).ok()
}

fn local_drain_participant(
    controller: &ClusterController,
    transition: &AssignmentDrainTransition,
) -> Option<CheckpointParticipant> {
    let participant = CheckpointParticipant {
        node_id: controller.instance_id().0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    (transition
        .predecessor
        .participant_incarnation(participant.node_id)
        == Some(participant.boot_incarnation))
    .then_some(participant)
}

fn finalized_drain_outcome(
    transition: &AssignmentDrainTransition,
    finalized: &AssignmentSnapshot,
) -> Result<laminar_connectors::connector::SourceDrainOutcome, String> {
    if finalized.draining || finalized.version != transition.target.assignment_version {
        return Err(format!(
            "assignment {} is not a terminal snapshot for drain target {}",
            finalized.version, transition.target.assignment_version
        ));
    }
    let fence = finalized
        .assignment_fence()
        .map_err(|error| error.to_string())?;
    if fence == transition.target {
        return Ok(laminar_connectors::connector::SourceDrainOutcome::Commit);
    }
    if fence.assignment_version == transition.target.assignment_version
        && fence.vnode_count == transition.predecessor.vnode_count
        && fence.assignment_digest == transition.predecessor.assignment_digest
        && fence.participants == transition.predecessor.participants
    {
        return Ok(laminar_connectors::connector::SourceDrainOutcome::Abort);
    }
    Err(format!(
        "assignment {} is neither the committed target nor predecessor rollback for drain {:?}",
        finalized.version,
        transition.id()
    ))
}

/// Audit a materialized assignment-drain outcome against the shared authority sequence.
/// Ordinary assignments and still-draining transitions require no terminal decision.
///
/// # Errors
/// Returns an error for missing/malformed authority history or a materialization that disagrees
/// with the immutable terminal decision.
pub async fn audit_assignment_snapshot_authority(
    store: &AssignmentSnapshotStore,
    controller: Option<&ClusterController>,
    snapshot: &AssignmentSnapshot,
) -> Result<(), String> {
    if snapshot.draining
        || store
            .load_drain_transition(snapshot.version)
            .await
            .map_err(|error| error.to_string())?
            .is_none()
    {
        return Ok(());
    }
    let authority = match controller {
        Some(controller) => Some(
            controller
                .checkpoint_authority()
                .map_err(|error| error.to_string())?,
        ),
        None => None,
    };
    audit_materialized_drain_with_authority(store, authority.as_deref(), snapshot).await
}

async fn audit_materialized_drain_with_authority(
    store: &AssignmentSnapshotStore,
    authority: Option<&LeaderLeaseStore>,
    snapshot: &AssignmentSnapshot,
) -> Result<(), String> {
    if snapshot.draining {
        return Ok(());
    }
    let Some(transition) = store
        .load_drain_transition(snapshot.version)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(());
    };
    let authority = authority
        .ok_or_else(|| "materialized assignment drain has no cluster authority".to_string())?;
    let decision = authority
        .assignment_drain_decision(snapshot.version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "assignment {} has a materialized drain outcome without an authority decision",
                snapshot.version
            )
        })?;
    if decision.transition != transition {
        return Err(format!(
            "assignment {} materialization binds a different authority transition",
            snapshot.version
        ));
    }
    let observed = finalized_drain_outcome(&transition, snapshot)?;
    let expected = match decision.verdict {
        AssignmentDrainVerdict::Commit => laminar_connectors::connector::SourceDrainOutcome::Commit,
        AssignmentDrainVerdict::Abort => laminar_connectors::connector::SourceDrainOutcome::Abort,
    };
    if observed != expected {
        return Err(format!(
            "assignment {} materialization conflicts with its authority decision",
            snapshot.version
        ));
    }
    Ok(())
}

/// Apply a durable drain outcome before adopting any later assignment. Kafka commit resolution
/// deliberately requires the exact target generation to be installed while it unassigns revoked
/// partitions; skipping directly to a later generation would make the source cut unverifiable.
async fn settle_observed_local_drain(
    db: &Arc<LaminarDB>,
    store: &AssignmentSnapshotStore,
    registry: &VnodeRegistry,
    controller: Option<&ClusterController>,
    transition: &AssignmentDrainTransition,
    observed: &AssignmentSnapshot,
    adoption_deadline: tokio::time::Instant,
    drain_timeout: Duration,
) -> Result<bool, String> {
    if observed.draining {
        if observed.drain_transition.as_ref() == Some(transition) {
            return Ok(false);
        }
        if observed.version >= transition.target.assignment_version {
            return Err(format!(
                "drain {:?} was superseded by a different draining assignment {}",
                transition.id(),
                observed.version
            ));
        }
        return Ok(false);
    }
    if observed.version < transition.target.assignment_version {
        return Ok(false);
    }

    let finalized = if observed.version == transition.target.assignment_version {
        observed.clone()
    } else {
        tokio::time::timeout_at(
            adoption_deadline,
            store.load_version(transition.target.assignment_version),
        )
        .await
        .map_err(|_| {
            format!(
                "timed out loading terminal assignment {} for drain resolution",
                transition.target.assignment_version
            )
        })?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "terminal assignment {} was pruned before local drain resolution",
                transition.target.assignment_version
            )
        })?
    };
    tokio::time::timeout_at(
        adoption_deadline,
        audit_assignment_snapshot_authority(store, controller, &finalized),
    )
    .await
    .map_err(|_| {
        format!(
            "timed out auditing terminal assignment {} for drain resolution",
            finalized.version
        )
    })??;
    let outcome = finalized_drain_outcome(transition, &finalized)?;
    let target_version = transition.target.assignment_version;
    let local_version = registry.assignment_version();
    if local_version < target_version {
        let adoption = db
            .adopt_assignment_snapshot(finalized.clone(), adoption_deadline)
            .await
            .map_err(|error| error.to_string())?;
        log_adoption("watcher-drain-resolution", &adoption);
    }
    if registry.assignment_version() != target_version {
        return Err(format!(
            "cannot resolve drain {:?} at local assignment {}",
            transition.id(),
            registry.assignment_version()
        ));
    }
    let expected = finalized
        .to_vnode_vec(registry.vnode_count())
        .map_err(|error| error.to_string())?;
    if registry.snapshot().as_ref() != expected.as_slice() {
        return Err(format!(
            "local assignment {} does not match the durable drain outcome",
            target_version
        ));
    }
    db.resolve_local_source_drain(
        transition.id(),
        outcome,
        tokio::time::Instant::now() + drain_timeout,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(true)
}

async fn audit_drain_predecessor(
    store: &AssignmentSnapshotStore,
    registry: &VnodeRegistry,
    draining: &AssignmentSnapshot,
    controller: Option<&ClusterController>,
) -> Result<AssignmentSnapshot, String> {
    if !draining.draining {
        return Err("drain predecessor audit requires a draining head".into());
    }
    let local_version = registry.assignment_version();
    let expected_target = local_version
        .checked_add(1)
        .ok_or_else(|| "assignment version overflow during drain audit".to_string())?;
    if draining.version != expected_target {
        return Err(format!(
            "draining assignment {} is not the exact successor of local assignment {local_version}",
            draining.version
        ));
    }
    draining
        .to_vnode_vec(registry.vnode_count())
        .map_err(|error| error.to_string())?;

    let predecessor = store
        .load_version(local_version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "draining assignment {} lost committed predecessor {local_version}",
                draining.version
            )
        })?;
    if predecessor.draining || predecessor.version != local_version {
        return Err(format!(
            "assignment {local_version} is not the committed predecessor of draining assignment {}",
            draining.version
        ));
    }
    let transition = draining
        .drain_transition
        .as_ref()
        .ok_or_else(|| "draining assignment has no exact transition".to_string())?;
    if predecessor
        .assignment_fence()
        .map_err(|error| error.to_string())?
        != transition.predecessor
    {
        return Err(format!(
            "draining assignment {} does not bind committed predecessor {local_version}",
            draining.version
        ));
    }
    let controller = controller
        .ok_or_else(|| "draining assignment requires a cluster controller".to_string())?;
    if !controller.process_lease_is_live() {
        return Err("local process lease expired while auditing assignment drain".into());
    }
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    if authority
        .assignment_drain_decision(transition.target.assignment_version)
        .await
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("assignment drain already has a terminal authority decision".into());
    }
    let current_leader = authority
        .load()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "assignment drain leader authority is missing".to_string())?;
    if !current_leader.matches_proof(&transition.leader) {
        return Err("assignment drain leader term was superseded".into());
    }
    let owners = predecessor
        .to_vnode_vec(registry.vnode_count())
        .map_err(|error| error.to_string())?;
    let local = registry.snapshot();
    if owners.as_slice() != local.as_ref() {
        return Err(format!(
            "committed predecessor {local_version} does not match the local owner map"
        ));
    }
    Ok(predecessor)
}

async fn audit_exact_drain_head(
    store: &AssignmentSnapshotStore,
    registry: &VnodeRegistry,
    draining: &AssignmentSnapshot,
    controller: &ClusterController,
    deadline: tokio::time::Instant,
) -> Result<AssignmentSnapshot, String> {
    let refreshed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "durable drain re-audit timed out".to_string())?
        .map_err(|error| error.to_string())?;
    if refreshed.as_ref() != Some(draining) {
        return Err("durable assignment drain is no longer the unfinalized head".into());
    }
    tokio::time::timeout_at(
        deadline,
        audit_drain_predecessor(store, registry, draining, Some(controller)),
    )
    .await
    .map_err(|_| "durable drain predecessor audit timed out".to_string())?
}

async fn prepare_and_announce_local_drain(
    db: &LaminarDB,
    store: &AssignmentSnapshotStore,
    registry: &VnodeRegistry,
    controller: &ClusterController,
    draining: &AssignmentSnapshot,
    transition: &AssignmentDrainTransition,
    participant: CheckpointParticipant,
    revoking_vnodes: &[u32],
    deadline: tokio::time::Instant,
    drain_ack_timeout: Duration,
) -> Result<(), String> {
    let source_deadline = std::cmp::min(deadline, tokio::time::Instant::now() + drain_ack_timeout);
    let aggregate = db
        .prepare_local_source_drain(transition, participant, revoking_vnodes, source_deadline)
        .await
        .map_err(|error| error.to_string())?;

    // Receipt production can block on a connector FIFO. Re-read both the transition and its
    // leader lease before making that receipt visible to the frozen predecessor quorum.
    audit_exact_drain_head(store, registry, draining, controller, deadline).await?;
    controller.publish_checkpoint_drain_transition(Some(transition.clone()));
    tokio::time::timeout_at(
        deadline,
        controller.announce_drain_ack(transition, aggregate),
    )
    .await
    .map_err(|_| "durable drain acknowledgement publication timed out".to_string())?
}

/// Publish per-domain owner counts. Resets the gauge so disappeared domains don't leave stale series.
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
        let mut audit = tokio::time::interval(config.watcher_poll);
        audit.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let membership_changed = tokio::select! {
                biased;
                () = shutdown.notified() => return,
                res = members.changed() => {
                    if res.is_err() {
                        warn!("membership watch sender dropped; rebalance controller exiting");
                        return;
                    }
                    true
                }
                _ = audit.tick() => false,
            };

            if membership_changed {
                debug!("membership change observed; debouncing");
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
            }

            loop {
                if !controller.is_leader() {
                    debug!("membership changed; not the leader — skipping rotation check");
                    break;
                }
                // Assignable = Active, non-draining — Draining/Suspected nodes never receive vnodes.
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
/// true if fully drained. A version materialized from a drain is accepted only with its matching
/// shared-authority decision; `None` therefore supports ordinary snapshots but fails closed for a
/// drain finalization. Used by a draining node to know when it is safe to exit.
pub async fn wait_until_drained(
    store: &AssignmentSnapshotStore,
    authority: Option<&LeaderLeaseStore>,
    me: NodeId,
    vnode_count: u32,
    poll: Duration,
    deadline: Duration,
) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        if start.elapsed() >= deadline {
            return false;
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        match tokio::time::timeout(remaining, store.load()).await {
            Ok(Ok(None)) => {
                warn!(
                    "wait_until_drained: durable assignment head is missing; ownership is unknown"
                );
            }
            Ok(Ok(Some(snap))) => {
                match audit_materialized_drain_with_authority(store, authority, &snap).await {
                    Ok(()) => match snap.to_vnode_vec(vnode_count) {
                        Ok(owners) if !snap.draining && !owners.contains(&me) => return true,
                        Ok(_) => {}
                        Err(error) => {
                            warn!(%error, "wait_until_drained: snapshot cardinality mismatch")
                        }
                    },
                    Err(error) => {
                        warn!(%error, version = snap.version, "wait_until_drained: assignment authority audit failed");
                    }
                }
            }
            Ok(Err(e)) => warn!(error = %e, "wait_until_drained: snapshot load failed"),
            Err(_) => {
                warn!("wait_until_drained: snapshot load exceeded the shutdown deadline");
                return false;
            }
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        tokio::time::sleep(poll.min(remaining)).await;
        if start.elapsed() >= deadline {
            return false;
        }
    }
}

async fn try_rebalance(
    db: &Arc<LaminarDB>,
    controller: &Arc<ClusterController>,
    store: &Arc<AssignmentSnapshotStore>,
    registry: &Arc<VnodeRegistry>,
    live: &[NodeId],
    config: RebalanceConfig,
) -> Result<Option<u64>, String> {
    let head_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
    let current = tokio::time::timeout_at(head_deadline, store.load())
        .await
        .map_err(|_| "durable assignment head audit timed out".to_string())?
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no snapshot on store — boot seed missing".to_string())?;
    tokio::time::timeout_at(
        head_deadline,
        audit_assignment_snapshot_authority(store, Some(controller), &current),
    )
    .await
    .map_err(|_| "durable assignment authority audit timed out".to_string())??;
    let current_owners = current
        .to_vnode_vec(registry.vnode_count())
        .map_err(|error| error.to_string())?;

    let local_assignment = registry.versioned_snapshot();
    if current.version < local_assignment.version() {
        return Err(format!(
            "durable assignment head {} regressed behind local assignment {}",
            current.version,
            local_assignment.version()
        ));
    }
    if !current.draining && current.version > local_assignment.version() {
        // A writer can fail after its durable CAS succeeds but before it adopts or gossips the
        // result. Reconcile that durable fact before comparing it with desired placement; otherwise
        // an already-correct owner map would be mistaken for a no-op forever.
        let adoption = db
            .adopt_assignment_snapshot(current.clone(), head_deadline)
            .await
            .map_err(|error| error.to_string())?;
        log_adoption("rebalance-reconcile", &adoption);
        let reconciled_version = registry.assignment_version();
        if reconciled_version < current.version {
            return Err(format!(
                "durable assignment {} was not adopted; local assignment remains {}",
                current.version, reconciled_version
            ));
        }
        tokio::time::timeout_at(
            head_deadline,
            controller.announce_snapshot_version(reconciled_version),
        )
        .await
        .map_err(|_| {
            format!(
                "durable assignment {reconciled_version} announcement exceeded the head deadline"
            )
        })?;
        return Ok(Some(reconciled_version));
    }
    if current.version == local_assignment.version()
        && current_owners.as_slice() != local_assignment.owners()
    {
        return Err(format!(
            "durable and local assignment {} have different owner maps",
            current.version
        ));
    }

    // Whole cluster draining — hold the current assignment rather than panicking on an empty node
    // set. This check deliberately follows durable-head reconciliation: an empty placement input
    // must not hide a CAS winner that this process has not adopted or announced yet.
    if live.is_empty() {
        return Ok(None);
    }

    if current.draining {
        let prior_version = current
            .version
            .checked_sub(1)
            .ok_or_else(|| "draining assignment has no prior committed generation".to_string())?;
        let prior = tokio::time::timeout_at(head_deadline, store.load_version(prior_version))
            .await
            .map_err(|_| {
                format!("committed predecessor {prior_version} load exceeded the head deadline")
            })?
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                format!(
                    "draining assignment {} lost committed predecessor {prior_version}",
                    current.version
                )
            })?;
        if prior.draining {
            return Err(format!(
                "draining assignment {} has a non-committed predecessor",
                current.version
            ));
        }
        tokio::time::timeout_at(
            head_deadline,
            audit_assignment_snapshot_authority(store, Some(controller), &prior),
        )
        .await
        .map_err(|_| {
            format!(
                "committed predecessor {prior_version} authority audit exceeded the head deadline"
            )
        })??;
        prior
            .to_vnode_vec(registry.vnode_count())
            .map_err(|error| error.to_string())?;
        // The retained predecessor may name an owner process that died during the drain. Close
        // intake before publishing the rollback; if its old roster is no longer live, it remains
        // uncertified while the next audit rotates that dead stable owner from the committed cut.
        close_local_assignment_authority(db, Some(controller.as_ref()), head_deadline).await?;
        return finalize_drain_snapshot(
            db,
            store,
            controller,
            &current,
            &prior,
            AssignmentDrainVerdict::Abort,
            config,
        )
        .await;
    }
    let new_assignment = rendezvous_assignment(registry.vnode_count(), live);
    let new_vnodes = AssignmentSnapshot::vnodes_from_vec(&new_assignment);
    // The successor checkpoint quorum is the exact successor owner set. Deriving it from the
    // current membership/checkpoint roster retains a gracefully departing process after all of
    // its vnodes move away; the first checkpoint after that process exits can then never reach
    // quorum. The predecessor roster remains authoritative for the drain cut and is carried by
    // `current`.
    let participant_ids = successor_participant_ids(&new_assignment);
    let participants = controller
        .recovery_participant_incarnations(&participant_ids)
        .await?;
    let roster_changed = current.participants != participants;

    if new_vnodes == current.vnodes && !roster_changed {
        return Ok(None);
    }

    // A restarted stable node is a new state owner even when rendezvous placement is unchanged.
    // The old certificate is already unusable, so no checkpoint can be admitted under it. Commit
    // a fenced generation-only rotation; replacement startup remains intake-gated while it
    // restores its owned vnodes and all peers certify this higher version.
    if new_vnodes == current.vnodes {
        let next = current
            .next_for_participants(new_vnodes, participants)
            .map_err(|error| error.to_string())?;
        return commit_snapshot(
            db,
            store,
            controller,
            next,
            current.version,
            config.checkpoint_timeout,
        )
        .await;
    }

    // When shedding a dead node, skip the pre-rotation drain: the dead node can't provide captures,
    // so the durability gate would time out and deadlock against rotation — which is the only thing
    // that restores commit availability. Survivors rehydrate from the last committed epoch.
    // "Dead" is from MEMBERSHIP, not the assignable set: a Draining node is alive and must drain
    // before rotation takes its vnodes.
    let shedding_dead = {
        use laminar_core::cluster::discovery::NodeState;
        let members = controller.members_watch().borrow().clone();
        current_owners
            .iter()
            .filter(|o| !live.contains(o))
            .any(|&o| {
                let dead_in_membership = match members.iter().find(|m| m.id.0 == o.0) {
                    Some(node) => matches!(node.state, NodeState::Suspected | NodeState::Left),
                    None => true,
                };
                dead_in_membership || controller.is_recently_unresponsive(o)
            })
    };
    if shedding_dead {
        warn!(
            "rotation sheds a dead node — skipping the pre-rotation drain \
             checkpoint (it cannot seal without the dead node's captures)"
        );
    }

    // Exactly-once handoff: publish a draining snapshot so every predecessor process produces
    // connector-owned FIFO cut receipts, then checkpoint that exact cut. Skipped when shedding a
    // dead node (it cannot produce a receipt) or when not exactly-once.
    if !shedding_dead && db.requires_rotation_drain() {
        let leader = controller.capture_leader_proof().ok_or_else(|| {
            "assignment drain requires the current durable leader proof".to_string()
        })?;
        let drain = current
            .next_draining(new_vnodes.clone(), participants.clone(), leader)
            .map_err(|error| error.to_string())?;
        let transition = drain
            .drain_transition
            .as_ref()
            .expect("validated draining snapshot has a transition")
            .clone();
        match store
            .save_if_version(&drain, current.version)
            .await
            .map_err(|e| e.to_string())?
        {
            RotateOutcome::Rotated => {
                let drain_head_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
                let revoking_vnodes = db
                    .adopt_draining_snapshot(&drain)
                    .map_err(|error| error.to_string())?;
                controller.announce_snapshot_version(drain.version).await;
                controller.publish_checkpoint_drain_transition(Some(transition.clone()));
                // Wait for every process in the snapshot's frozen boot roster to durably ack the
                // exact source cut. Target-only joiners intentionally do not acknowledge inputs
                // they never owned under the predecessor.
                let local_receipt = match local_drain_participant(controller, &transition) {
                    Some(participant) => {
                        prepare_and_announce_local_drain(
                            db,
                            store,
                            registry,
                            controller,
                            &drain,
                            &transition,
                            participant,
                            &revoking_vnodes,
                            drain_head_deadline,
                            config.drain_ack_timeout,
                        )
                        .await
                    }
                    None => Ok(()),
                };
                let acked = match local_receipt {
                    Ok(()) => {
                        await_drain_quorum(controller, &transition, config.drain_ack_timeout).await
                    }
                    Err(error) => {
                        warn!(%error, version = drain.version, "leader durable drain acknowledgement failed");
                        false
                    }
                };
                if !acked {
                    let failure = "drain ack quorum not reached before timeout";
                    if let Err(error) = audit_exact_drain_head(
                        store,
                        registry,
                        &drain,
                        controller,
                        tokio::time::Instant::now() + config.checkpoint_timeout,
                    )
                    .await
                    {
                        return Err(format!(
                            "{failure}; drain is no longer authoritative: {error}"
                        ));
                    }
                    if let Err(abort_error) = finalize_drain_snapshot(
                        db,
                        store,
                        controller,
                        &drain,
                        &current,
                        AssignmentDrainVerdict::Abort,
                        config,
                    )
                    .await
                    {
                        return Err(format!(
                            "{failure}; assignment drain abort failed: {abort_error}"
                        ));
                    }
                    return Err(failure.into());
                }
                audit_exact_drain_head(
                    store,
                    registry,
                    &drain,
                    controller,
                    tokio::time::Instant::now() + config.checkpoint_timeout,
                )
                .await?;
                // Abort the drain on failure OR timeout (not just Ok(false)) — a bare
                // `?` here would leave nodes stuck draining.
                let checkpointed = pre_rotation_checkpoint(db, config).await;
                if !matches!(checkpointed, Ok(true)) {
                    let failure = checkpointed
                        .err()
                        .unwrap_or_else(|| "pre-rotation checkpoint failed during drain".into());
                    if let Err(error) = audit_exact_drain_head(
                        store,
                        registry,
                        &drain,
                        controller,
                        tokio::time::Instant::now() + config.checkpoint_timeout,
                    )
                    .await
                    {
                        return Err(format!(
                            "{failure}; drain is no longer authoritative: {error}"
                        ));
                    }
                    if let Err(abort_error) = finalize_drain_snapshot(
                        db,
                        store,
                        controller,
                        &drain,
                        &current,
                        AssignmentDrainVerdict::Abort,
                        config,
                    )
                    .await
                    {
                        return Err(format!(
                            "{failure}; assignment drain abort failed: {abort_error}"
                        ));
                    }
                    return Err(failure);
                }
                audit_exact_drain_head(
                    store,
                    registry,
                    &drain,
                    controller,
                    tokio::time::Instant::now() + config.checkpoint_timeout,
                )
                .await?;
                return finalize_drain_snapshot(
                    db,
                    store,
                    controller,
                    &drain,
                    &current,
                    AssignmentDrainVerdict::Commit,
                    config,
                )
                .await;
            }
            RotateOutcome::Conflict(winner) => {
                let v = winner.version;
                adopt_any(
                    db,
                    store,
                    controller,
                    winner,
                    tokio::time::Instant::now() + config.checkpoint_timeout,
                )
                .await?;
                controller.announce_snapshot_version(v).await;
                return Ok(Some(v));
            }
        }
    }

    // Single-phase path: dead-node shedding, or a non-exactly-once pipeline that
    // tolerates the bounded rotation duplicate.
    if !shedding_dead && !pre_rotation_checkpoint(db, config).await? {
        return Err("pre-rotation checkpoint returned success=false".into());
    }
    commit_snapshot(
        db,
        store,
        controller,
        current
            .next_for_participants(new_vnodes, participants)
            .map_err(|error| error.to_string())?,
        current.version,
        config.checkpoint_timeout,
    )
    .await
}

fn successor_participant_ids(owners: &[NodeId]) -> Vec<u64> {
    owners
        .iter()
        .map(|owner| owner.0)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Run the pre-rotation checkpoint with the configured timeout. `Ok(true)` on a
/// successful seal, `Ok(false)` if the checkpoint ran but did not succeed.
async fn pre_rotation_checkpoint(
    db: &Arc<LaminarDB>,
    config: RebalanceConfig,
) -> Result<bool, String> {
    let ckpt = tokio::time::timeout(config.checkpoint_timeout, db.checkpoint())
        .await
        .map_err(|_| {
            format!(
                "pre-rotation checkpoint did not complete within {}s",
                config.checkpoint_timeout.as_secs()
            )
        })?
        .map_err(|e| e.to_string())?;
    Ok(ckpt.success)
}

/// Wait until every exact process in the draining assignment certificate has durably proved it
/// paused the revoking partitions. Durable read errors are retried within the existing deadline;
/// they never become quorum.
async fn await_drain_quorum(
    controller: &Arc<ClusterController>,
    transition: &AssignmentDrainTransition,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            match controller.drain_ack_quorum_reached(transition).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    debug!(%error, version = transition.target.assignment_version, "durable drain quorum observation failed; retrying");
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok()
}

/// Settle a draining generation without changing the target version certified by source receipts.
/// The terminal verdict first enters the shared leader/checkpoint authority sequence; the snapshot
/// store then publishes that immutable verdict as the assignment materialization.
async fn finalize_drain_snapshot(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    controller: &Arc<ClusterController>,
    draining: &AssignmentSnapshot,
    predecessor: &AssignmentSnapshot,
    requested_verdict: AssignmentDrainVerdict,
    config: RebalanceConfig,
) -> Result<Option<u64>, String> {
    let transition = draining
        .drain_transition
        .as_ref()
        .ok_or_else(|| "draining assignment has no exact transition".to_string())?;
    if predecessor
        .assignment_fence()
        .map_err(|error| error.to_string())?
        != transition.predecessor
    {
        return Err("drain finalization predecessor does not match the transition".into());
    }
    let deciding_proof = controller
        .capture_leader_proof()
        .ok_or_else(|| "drain finalization requires a current leader proof".to_string())?;
    let requested =
        AssignmentDrainDecision::new(transition, deciding_proof.clone(), requested_verdict)
            .map_err(|error| error.to_string())?;
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    let decision = match authority
        .record_assignment_drain_decision(&deciding_proof, requested)
        .await
        .map_err(|error| error.to_string())?
    {
        RecordAssignmentDrainDecisionResult::Created(decision)
        | RecordAssignmentDrainDecisionResult::Unchanged(decision) => decision,
        RecordAssignmentDrainDecisionResult::Conflict { winner } => {
            warn!(
                requested = ?requested_verdict,
                winner = ?winner.verdict,
                version = draining.version,
                "another authority-sequenced drain decision already won"
            );
            winner
        }
    };
    if decision.transition != *transition {
        return Err(format!(
            "authority decision for assignment {} binds a different drain transition",
            draining.version
        ));
    }
    let proposal = match decision.verdict {
        AssignmentDrainVerdict::Commit => draining
            .committed_target()
            .map_err(|error| error.to_string())?,
        AssignmentDrainVerdict::Abort => draining
            .aborted_target(predecessor)
            .map_err(|error| error.to_string())?,
    };

    let durable = match store
        .finalize_drain(draining, &proposal)
        .await
        .map_err(|error| error.to_string())?
    {
        RotateOutcome::Rotated => proposal,
        RotateOutcome::Conflict(winner) => winner,
    };
    let source_outcome = finalized_drain_outcome(transition, &durable)?;
    let authority_outcome = match decision.verdict {
        AssignmentDrainVerdict::Commit => laminar_connectors::connector::SourceDrainOutcome::Commit,
        AssignmentDrainVerdict::Abort => laminar_connectors::connector::SourceDrainOutcome::Abort,
    };
    if source_outcome != authority_outcome {
        return Err(format!(
            "materialized assignment {} conflicts with the authority drain decision",
            durable.version
        ));
    }

    let version = durable.version;
    let adoption_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
    let adoption = db
        .adopt_assignment_snapshot(durable, adoption_deadline)
        .await
        .map_err(|error| error.to_string())?;
    log_adoption("rebalance-drain-finalize", &adoption);
    if local_drain_participant(controller, transition).is_some() {
        db.resolve_local_source_drain(
            transition.id(),
            source_outcome,
            tokio::time::Instant::now() + config.drain_ack_timeout,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    controller.publish_checkpoint_drain_transition(None);
    controller.announce_snapshot_version(version).await;
    let oldest_retained = version.saturating_sub(1);
    match store.prune_before(oldest_retained).await {
        Ok(()) => {
            if let Err(error) = authority
                .prune_assignment_drain_decisions_before(&deciding_proof, oldest_retained)
                .await
            {
                warn!(%error, "assignment drain authority prune failed after snapshot prune");
            }
        }
        Err(error) => {
            warn!(%error, "snapshot prune failed after drain finalization");
        }
    }
    Ok(Some(version))
}

/// CAS a committed (non-draining) snapshot, adopt it, announce, and prune. On a
/// CAS conflict adopt the winner instead and let the next cycle re-evaluate.
async fn commit_snapshot(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    controller: &Arc<ClusterController>,
    proposal: AssignmentSnapshot,
    prev_version: u64,
    adoption_timeout: Duration,
) -> Result<Option<u64>, String> {
    match store
        .save_if_version(&proposal, prev_version)
        .await
        .map_err(|e| e.to_string())?
    {
        RotateOutcome::Rotated => {
            let v = proposal.version;
            let adoption = db
                .adopt_assignment_snapshot(proposal, tokio::time::Instant::now() + adoption_timeout)
                .await
                .map_err(|e| e.to_string())?;
            log_adoption("rebalance", &adoption);
            controller.announce_snapshot_version(v).await;
            // Retain [v-1, v] as slack for in-flight readers.
            let oldest_retained = v.saturating_sub(1);
            match store.prune_before(oldest_retained).await {
                Ok(()) => {
                    let Some(proof) = controller.capture_leader_proof() else {
                        warn!(
                            "assignment drain authority retention could not capture current leader proof"
                        );
                        return Ok(Some(v));
                    };
                    match controller.checkpoint_authority() {
                        Ok(authority) => {
                            if let Err(error) = authority
                                .prune_assignment_drain_decisions_before(&proof, oldest_retained)
                                .await
                            {
                                warn!(%error, "assignment drain authority prune failed after snapshot prune");
                            }
                        }
                        Err(error) => {
                            warn!(%error, "assignment drain authority retention is unavailable");
                        }
                    }
                }
                Err(error) => {
                    warn!(%error, "snapshot prune failed");
                }
            }
            Ok(Some(v))
        }
        RotateOutcome::Conflict(winner) => {
            let v = winner.version;
            adopt_any(
                db,
                store,
                controller,
                winner,
                tokio::time::Instant::now() + adoption_timeout,
            )
            .await?;
            controller.announce_snapshot_version(v).await;
            Ok(Some(v))
        }
    }
}

/// Adopt a snapshot whether it is a draining or a committed one.
async fn adopt_any(
    db: &Arc<LaminarDB>,
    store: &AssignmentSnapshotStore,
    controller: &ClusterController,
    snap: AssignmentSnapshot,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    tokio::time::timeout_at(
        deadline,
        audit_assignment_snapshot_authority(store, Some(controller), &snap),
    )
    .await
    .map_err(|_| format!("assignment {} authority audit timed out", snap.version))??;
    if snap.draining {
        db.adopt_draining_snapshot(&snap)
            .map_err(|error| error.to_string())?;
    } else {
        let adoption = db
            .adopt_assignment_snapshot(snap, deadline)
            .await
            .map_err(|e| e.to_string())?;
        log_adoption("rebalance-conflict", &adoption);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use laminar_core::state::InProcessBackend;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};

    struct PendingListStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Debug for PendingListStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PendingListStore").finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for PendingListStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("PendingListStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for PendingListStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            _prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            Box::pin(futures::stream::pending())
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn store() -> AssignmentSnapshotStore {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        AssignmentSnapshotStore::new(mem)
    }

    fn test_cluster_checkpoint_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn test_cluster_controller(
        node: NodeId,
        boot: uuid::Uuid,
        assignment_store: Option<Arc<AssignmentSnapshotStore>>,
    ) -> Arc<ClusterController> {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;

        let kv = Arc::new(InMemoryKv::new(node));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            control,
            recovery,
            assignment_store,
            members_rx,
            boot,
        ))
    }

    async fn grant_test_leadership(
        controller: &Arc<ClusterController>,
    ) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
        use laminar_core::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

        let authority = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 10_000));
        let owner = LeaderLeaseOwner {
            node: controller.instance_id(),
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            panic!("empty test authority must grant leadership");
        };
        install_test_leadership(controller, authority, owner, lease)
    }

    fn install_test_leadership(
        controller: &Arc<ClusterController>,
        authority: Arc<LeaderLeaseStore>,
        owner: laminar_core::cluster::control::LeaderLeaseOwner,
        lease: laminar_core::cluster::control::LeaderLease,
    ) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
        use laminar_core::cluster::control::LeaseDeadline;

        let (lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
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
        assert!(controller.capture_leader_proof().is_some());
        lease_tx
    }

    fn snapshot(vnodes: BTreeMap<u32, NodeId>) -> AssignmentSnapshot {
        let mut node_ids: Vec<u64> = vnodes.values().map(|node| node.0).collect();
        node_ids.sort_unstable();
        node_ids.dedup();
        let participants = node_ids
            .into_iter()
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
            })
            .collect();
        AssignmentSnapshot::empty()
            .next_for_participants(vnodes, participants)
            .unwrap()
    }

    fn draining_snapshot(
        committed: &AssignmentSnapshot,
        vnodes: BTreeMap<u32, NodeId>,
        participants: Vec<CheckpointParticipant>,
    ) -> AssignmentSnapshot {
        let leader = committed.participants[0];
        committed
            .next_draining(
                vnodes,
                participants,
                laminar_core::checkpoint::LeaderProof {
                    owner: laminar_core::checkpoint::LeaderProofOwner {
                        node_id: leader.node_id,
                        boot_id: leader.boot_incarnation,
                        process_term: 1,
                    },
                    fencing_token: 1,
                },
            )
            .unwrap()
    }

    #[test]
    fn successor_checkpoint_roster_contains_only_successor_owners() {
        let owners = [NodeId(3), NodeId(1), NodeId(3), NodeId(1)];
        assert_eq!(successor_participant_ids(&owners), [1, 3]);
        assert!(!successor_participant_ids(&owners).contains(&2));
    }

    #[test]
    fn drain_abort_restores_committed_process_roster() {
        let committed = snapshot(BTreeMap::from([(0, NodeId(1))]));
        let replacement = CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(2),
        };
        let draining = draining_snapshot(
            &committed,
            BTreeMap::from([(0, NodeId(2))]),
            vec![committed.participants[0], replacement],
        );

        let aborted = draining.aborted_target(&committed).unwrap();
        assert!(!aborted.draining);
        assert_eq!(aborted.vnodes, committed.vnodes);
        assert_eq!(aborted.participants, committed.participants);
    }

    #[test]
    fn drain_certificate_binds_the_durable_target_map_and_boot_roster() {
        let committed = snapshot(BTreeMap::from([(0, NodeId(1)), (1, NodeId(1))]));
        assert!(committed.drain_transition.is_none());
        let replacement = CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(22),
        };
        let draining = draining_snapshot(
            &committed,
            BTreeMap::from([(0, NodeId(1)), (1, NodeId(2))]),
            vec![committed.participants[0], replacement],
        );
        draining.to_vnode_vec(2).unwrap();
        let fence = draining.drain_transition.as_ref().unwrap().target.clone();
        assert_eq!(fence.assignment_version, draining.version);
        assert_eq!(fence.participants, draining.participants);
        assert!(fence.matches_owner_map(&[1, 2]));
        assert!(draining.to_vnode_vec(1).is_err());
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

    #[test]
    fn checkpoint_fence_requires_exact_reports_and_complete_live_owners() {
        fn participant(node_id: u64, boot: u64) -> CheckpointParticipant {
            CheckpointParticipant {
                node_id,
                boot_incarnation: format!("00000000-0000-0000-0000-{boot:012x}")
                    .parse()
                    .unwrap(),
            }
        }

        fn adoption(
            participant: CheckpointParticipant,
            version: u64,
            owners: &[u64],
        ) -> CheckpointAssignmentAdoption {
            let vnode_count = u32::try_from(owners.len()).unwrap();
            CheckpointAssignmentAdoption {
                participant,
                assignment_version: version,
                vnode_count,
                assignment_digest: CheckpointAssignmentFence::owner_map_digest(vnode_count, owners),
            }
        }

        let p1 = participant(1, 11);
        let p2 = participant(2, 22);
        let owners = [1, 2, 1];
        let reported = rustc_hash::FxHashMap::from_iter([
            (1, adoption(p1, 7, &owners)),
            (2, adoption(p2, 7, &owners)),
        ]);
        let fence = checkpoint_assignment_fence(
            7,
            &[NodeId(1), NodeId(2), NodeId(1)],
            vec![p1, p2],
            &reported,
        )
        .expect("exact assignment should be checkpoint-ready");
        assert_eq!(fence.assignment_version, 7);
        assert_eq!(fence.participant_ids(), [1, 2]);
        assert!(fence.matches_owner_map(&owners));

        assert!(
            checkpoint_assignment_fence(7, &[NodeId(1), NodeId(9)], vec![p1, p2], &reported,)
                .is_none(),
            "an owner outside current checkpoint membership must close the fence"
        );
        assert!(
            checkpoint_assignment_fence(
                7,
                &[NodeId(1), NodeId::UNASSIGNED],
                vec![p1, p2],
                &reported,
            )
            .is_none(),
            "unassigned vnodes are never restorable"
        );

        let missing_report = rustc_hash::FxHashMap::from_iter([(1, adoption(p1, 7, &[1, 2]))]);
        assert!(checkpoint_assignment_fence(
            7,
            &[NodeId(1), NodeId(2)],
            vec![p1, p2],
            &missing_report,
        )
        .is_none());
        let stale_report = rustc_hash::FxHashMap::from_iter([
            (1, adoption(p1, 7, &[1, 2])),
            (2, adoption(p2, 6, &[1, 2])),
        ]);
        assert!(checkpoint_assignment_fence(
            7,
            &[NodeId(1), NodeId(2)],
            vec![p1, p2],
            &stale_report,
        )
        .is_none());

        let divergent_same_version = rustc_hash::FxHashMap::from_iter([
            (1, adoption(p1, 7, &[1, 2])),
            (2, adoption(p2, 7, &[2, 1])),
        ]);
        assert!(checkpoint_assignment_fence(
            7,
            &[NodeId(1), NodeId(2)],
            vec![p1, p2],
            &divergent_same_version,
        )
        .is_none());

        let restarted_p2 = participant(2, 222);
        assert!(checkpoint_assignment_fence(
            7,
            &[NodeId(1), NodeId(2)],
            vec![p1, restarted_p2],
            &rustc_hash::FxHashMap::from_iter([
                (1, adoption(p1, 7, &[1, 2])),
                (2, adoption(p2, 7, &[1, 2])),
            ]),
        )
        .is_none());
    }

    #[tokio::test]
    async fn wait_until_drained_false_while_owning_vnodes() {
        let s = store();
        let me = NodeId(1);
        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, me);
        vnodes.insert(1, NodeId(2));
        let snap = snapshot(vnodes);
        s.save_if_absent(&snap).await.unwrap();

        let drained = wait_until_drained(
            &s,
            None,
            me,
            2,
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
        let snap = snapshot(vnodes);
        s.save_if_absent(&snap).await.unwrap();

        let drained = wait_until_drained(
            &s,
            None,
            me,
            2,
            Duration::from_millis(20),
            Duration::from_secs(5),
        )
        .await;
        assert!(drained, "owns no vnode → drained quickly");
    }

    #[tokio::test]
    async fn wait_until_drained_fails_closed_when_no_snapshot() {
        let s = store();
        let drained = wait_until_drained(
            &s,
            None,
            NodeId(1),
            1,
            Duration::from_millis(10),
            Duration::from_millis(60),
        )
        .await;
        assert!(
            !drained,
            "missing ownership authority cannot certify a safe exit"
        );
    }

    #[tokio::test]
    async fn wait_until_drained_bounds_a_stalled_snapshot_read() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let blocked: Arc<dyn ObjectStore> = Arc::new(PendingListStore { inner });
        let store = AssignmentSnapshotStore::new(blocked);

        let drained = tokio::time::timeout(
            Duration::from_millis(250),
            wait_until_drained(
                &store,
                None,
                NodeId(1),
                1,
                Duration::from_millis(5),
                Duration::from_millis(40),
            ),
        )
        .await
        .expect("the shutdown deadline must cancel a stalled object-store read");
        assert!(!drained, "an unreadable durable head cannot certify drain");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_closure_cancels_shuffle_before_waiting_for_execution_drain() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};
        use laminar_core::shuffle::ShuffleSender;
        use uuid::Uuid;

        let local_boot = Uuid::from_u128(11);
        let participants = vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: local_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            },
        ];
        let assignment =
            CheckpointAssignmentFence::from_owner_map(1, &[1, 2], participants).unwrap();
        let sender = Arc::new(ShuffleSender::new(1, local_boot));
        sender
            .install_assignment_fence(&assignment, &[1, 2])
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        sender
            .register_peer(2, listener.local_addr().unwrap())
            .await;
        let accepted = Arc::new(Notify::new());
        let peer = {
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                let (_socket, _) = listener.accept().await.unwrap();
                accepted.notify_one();
                std::future::pending::<()>().await;
            })
        };

        let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
        let db = LaminarDB::builder()
            .cluster_controller(test_cluster_controller(NodeId(1), local_boot, None))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .shuffle_sender(Arc::clone(&sender))
            .build()
            .await
            .unwrap();
        let blocked_cycle = {
            let execution_fence = Arc::clone(&db.rotation_execution_fence);
            let sender = Arc::clone(&sender);
            let assignment = assignment.clone();
            tokio::spawn(async move {
                let _cycle = execution_fence.read_owned().await;
                sender
                    .fan_out_barrier(&[2], CheckpointBarrier::new(7, 7), &assignment)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), accepted.notified())
            .await
            .expect("shuffle send did not reach the peer handshake");

        tokio::time::timeout(
            Duration::from_secs(1),
            close_local_assignment_authority(
                &db,
                None,
                tokio::time::Instant::now() + Duration::from_millis(500),
            ),
        )
        .await
        .expect("authority closure deadlocked behind a shuffle-held read fence")
        .expect("authority closure exceeded its deadline");
        let error = tokio::time::timeout(Duration::from_secs(1), blocked_cycle)
            .await
            .expect("cancelled shuffle cycle did not exit")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
        assert!(db.cluster_intake_fenced());
        peer.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_suspension_reasserts_closure_after_serialization_race() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::shuffle::ShuffleSender;
        use uuid::Uuid;

        let local_boot = Uuid::from_u128(11);
        let participants = vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: local_boot,
        }];
        let assignment = CheckpointAssignmentFence::from_owner_map(1, &[1], participants).unwrap();
        let sender = Arc::new(ShuffleSender::new(1, local_boot));
        sender.install_assignment_fence(&assignment, &[1]).unwrap();

        let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
        let db = LaminarDB::builder()
            .cluster_controller(test_cluster_controller(NodeId(1), local_boot, None))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .shuffle_sender(Arc::clone(&sender))
            .build()
            .await
            .unwrap();
        let adoption = db.assignment_adoption_lock.lock().await;
        let suspension = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                suspend_local_assignment_authority(
                    &db,
                    None,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while sender.assignment_version() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("suspension did not close shuffle before waiting for serialization");

        assert!(sender
            .install_assignment_fence(&assignment, &[1])
            .expect("the retained same-version certificate must be resumable"));
        db.set_source_gate(false);
        drop(adoption);

        suspension
            .await
            .unwrap()
            .expect("serialized suspension exceeded its deadline");
        assert_eq!(sender.assignment_version(), 0);
        assert_eq!(sender.active_assignment_digest(), None);
        assert!(db.cluster_intake_fenced());
        assert!(sender
            .install_assignment_fence(&assignment, &[1])
            .expect("suspension must preserve the same-version certificate"));
    }

    #[tokio::test]
    async fn assignment_adoption_timeout_remains_fenced_and_is_retryable() {
        use uuid::Uuid;

        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: Uuid::from_u128(11),
        };
        let base = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant])
            .unwrap();
        let target = base
            .next_for_participants(base.vnodes.clone(), vec![participant])
            .unwrap();
        let snapshots = Arc::new(store());
        snapshots.save_if_absent(&base).await.unwrap();
        assert!(matches!(
            snapshots
                .save_if_version(&target, base.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));
        let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
        let db = LaminarDB::builder()
            .cluster_controller(test_cluster_controller(
                NodeId(1),
                participant.boot_incarnation,
                Some(Arc::clone(&snapshots)),
            ))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(snapshots)
            .build()
            .await
            .unwrap();

        let old_cycle = Arc::clone(&db.rotation_execution_fence).read_owned().await;
        let error = db
            .adopt_assignment_snapshot(
                target.clone(),
                tokio::time::Instant::now() + Duration::from_millis(30),
            )
            .await
            .expect_err("a held predecessor cycle must exhaust the adoption deadline");
        assert!(error.to_string().contains("deadline"), "{error}");
        assert_eq!(registry.assignment_version(), base.version);
        assert!(db.cluster_intake_fenced());

        drop(old_cycle);
        let adoption = db
            .adopt_assignment_snapshot(
                target.clone(),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("the same durable target must remain retryable");
        assert!(adoption.adopted);
        assert_eq!(registry.assignment_version(), target.version);
        assert!(db.cluster_intake_fenced());
    }

    #[tokio::test]
    async fn wait_until_drained_fails_closed_on_wrong_vnode_cardinality() {
        let store = store();
        let snapshot = snapshot(BTreeMap::from([(0, NodeId(2))]));
        store.save_if_absent(&snapshot).await.unwrap();

        let drained = wait_until_drained(
            &store,
            None,
            NodeId(1),
            2,
            Duration::from_millis(10),
            Duration::from_millis(60),
        )
        .await;
        assert!(
            !drained,
            "wrong-cardinality history cannot certify a safe exit"
        );
    }

    #[tokio::test]
    async fn wait_until_drained_does_not_treat_draining_target_as_committed() {
        let store = store();
        let me = NodeId(1);
        let replacement = NodeId(2);
        let committed = snapshot(BTreeMap::from([(0, me)]));
        store.save_if_absent(&committed).await.unwrap();

        let replacement_process = CheckpointParticipant {
            node_id: replacement.0,
            boot_incarnation: uuid::Uuid::from_u128(2),
        };
        let draining = draining_snapshot(
            &committed,
            BTreeMap::from([(0, replacement)]),
            vec![replacement_process],
        );
        store
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();

        assert!(
            !wait_until_drained(
                &store,
                None,
                me,
                1,
                Duration::from_millis(10),
                Duration::from_millis(60),
            )
            .await,
            "a drain target has not transferred durable ownership"
        );

        let replacement_committed = draining.committed_target().unwrap();
        store
            .finalize_drain(&draining, &replacement_committed)
            .await
            .unwrap();
        assert!(
            !wait_until_drained(
                &store,
                None,
                me,
                1,
                Duration::from_millis(10),
                Duration::from_millis(40),
            )
            .await,
            "a standalone materialization cannot certify shutdown"
        );

        let authority = LeaderLeaseStore::new(Arc::new(InMemory::new()), 1_000);
        let owner = laminar_core::cluster::control::LeaderLeaseOwner {
            node: me,
            boot: committed.participants[0].boot_incarnation,
            process_term: 1,
        };
        let laminar_core::cluster::control::LeaseOutcome::Acquired(lease) =
            authority.try_acquire(&owner, 0).await.unwrap()
        else {
            panic!("test authority acquisition must succeed");
        };
        let transition = draining.drain_transition.as_ref().unwrap();
        assert_eq!(lease.proof(), transition.leader);
        let decision =
            AssignmentDrainDecision::new(transition, lease.proof(), AssignmentDrainVerdict::Commit)
                .unwrap();
        authority
            .record_assignment_drain_decision(&lease.proof(), decision)
            .await
            .unwrap();
        assert!(
            wait_until_drained(
                &store,
                Some(&authority),
                me,
                1,
                Duration::from_millis(10),
                Duration::from_millis(60),
            )
            .await,
            "only the committed successor can certify shutdown"
        );
    }

    #[tokio::test]
    async fn durable_commit_survives_crash_before_local_adoption_or_announcement() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let boot = Uuid::from_u128(11);
        let participant = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: boot,
        };
        let vnodes = BTreeMap::from([(0, self_id)]);
        let durable = Arc::new(store());
        let first = AssignmentSnapshot::empty()
            .next_for_participants(vnodes.clone(), vec![participant])
            .unwrap();
        durable.save_if_absent(&first).await.unwrap();
        let committed = first
            .next_for_participants(vnodes, vec![participant])
            .unwrap();
        assert!(matches!(
            durable
                .save_if_version(&committed, first.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();

        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();

        let reconciled = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[],
            RebalanceConfig::test_defaults(),
        )
        .await
        .unwrap();

        assert_eq!(reconciled, Some(committed.version));
        assert_eq!(registry.assignment_version(), committed.version);
        assert_eq!(
            controller.read_snapshot_version().await,
            Some(committed.version)
        );
    }

    #[tokio::test]
    async fn adoption_failure_after_cas_keeps_durable_head_retryable() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let other_id = NodeId(2);
        let boot = Uuid::from_u128(11);
        let first = AssignmentSnapshot::empty()
            .next_for_participants(
                BTreeMap::from([(0, other_id)]),
                vec![CheckpointParticipant {
                    node_id: other_id.0,
                    boot_incarnation: Uuid::from_u128(22),
                }],
            )
            .unwrap();
        let committed = first
            .next_for_participants(
                BTreeMap::from([(0, self_id)]),
                vec![CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: boot,
                }],
            )
            .unwrap();
        let durable = Arc::new(store());
        durable.save_if_absent(&first).await.unwrap();

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);

        let registry = Arc::new(VnodeRegistry::single_owner(1, other_id));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();

        let first_attempt = commit_snapshot(
            &db,
            &durable,
            &controller,
            committed.clone(),
            first.version,
            RebalanceConfig::test_defaults().checkpoint_timeout,
        )
        .await
        .expect_err("acquiring state without a checkpoint coordinator must fail closed");
        assert!(first_attempt.contains("live checkpoint coordinator"));
        assert_eq!(
            durable.load().await.unwrap().unwrap().version,
            committed.version
        );
        assert_eq!(registry.assignment_version(), first.version);
        assert_eq!(controller.read_snapshot_version().await, None);

        let retry = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect_err("the durable winner must be retried before placement can report no change");
        assert!(retry.contains("live checkpoint coordinator"));
        assert_eq!(registry.assignment_version(), first.version);
    }

    #[tokio::test]
    async fn watcher_suspends_and_resumes_exact_authority_after_transient_corrupt_head() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use uuid::Uuid;

        let self_id = NodeId(1);
        let boot = Uuid::from_u128(11);
        let process = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: boot,
        };
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&object_store)));
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, self_id)]), vec![process])
            .unwrap();
        durable.save_if_absent(&committed).await.unwrap();

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);

        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        let receiver = Arc::new(
            ShuffleReceiver::bind(self_id.0, "127.0.0.1:0".parse().unwrap(), boot)
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(self_id.0, boot));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::clone(&object_store))
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .shuffle_sender(Arc::clone(&sender))
            .shuffle_receiver(Arc::clone(&receiver))
            .build()
            .await
            .unwrap();
        db.set_source_gate(true);
        let shutdown = Arc::new(Notify::new());
        let mut config = RebalanceConfig::test_defaults();
        config.watcher_poll = Duration::from_millis(10);
        config.checkpoint_timeout = Duration::from_millis(100);
        let watcher = spawn_snapshot_watcher(
            Arc::clone(&db),
            Arc::clone(&durable),
            Arc::clone(&registry),
            Arc::clone(&shutdown),
            config,
            Some(Arc::clone(&controller)),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while db.cluster_intake_fenced() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("valid durable authority should open intake");
        assert!(controller
            .checkpoint_assignment_fence(committed.version)
            .is_some());
        assert_eq!(sender.assignment_version(), committed.version);
        assert_eq!(receiver.assignment_version(), committed.version);

        let corrupt_path = object_store::path::Path::from(
            "control/assignment-snapshots/v00000000000000000002.json",
        );
        object_store
            .put(
                &corrupt_path,
                object_store::PutPayload::from(bytes::Bytes::from_static(b"{not-json")),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !db.cluster_intake_fenced() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a corrupt durable head must close intake");
        assert_eq!(
            controller.checkpoint_assignment_fence(committed.version),
            None
        );
        assert_eq!(sender.assignment_version(), 0);
        assert_eq!(receiver.assignment_version(), 0);

        object_store.delete(&corrupt_path).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while db.cluster_intake_fenced() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the exact durable head should resume after the transient read fault clears");
        assert_eq!(sender.assignment_version(), committed.version);
        assert_eq!(receiver.assignment_version(), committed.version);

        object_store
            .put(
                &corrupt_path,
                object_store::PutPayload::from(bytes::Bytes::from_static(b"{not-json")),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !db.cluster_intake_fenced() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the second corrupt head must suspend authority");
        let execution = Arc::clone(&db.rotation_execution_fence).read_owned().await;
        object_store.delete(&corrupt_path).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if db.assignment_adoption_lock.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("watcher must reach the serialized activation boundary");
        db.set_source_gate(true);
        controller.publish_checkpoint_drain_transition(None);
        controller.publish_checkpoint_assignment_fence(None);
        db.suspend_shuffle_assignment_fence();
        shutdown.notify_one();
        drop(execution);
        tokio::time::timeout(Duration::from_secs(1), watcher)
            .await
            .expect("watcher should observe shutdown")
            .unwrap();
        assert!(db.cluster_intake_fenced());
        assert_eq!(
            controller.checkpoint_assignment_fence(committed.version),
            None
        );
        assert_eq!(sender.assignment_version(), 0);
        assert_eq!(receiver.assignment_version(), 0);
    }

    #[tokio::test]
    async fn restarted_process_retains_durable_generation_until_recovery() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let old_process = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: Uuid::from_u128(11),
        };
        let vnodes = BTreeMap::from([(0, self_id), (1, self_id)]);
        let durable = Arc::new(store());
        let first = AssignmentSnapshot::empty()
            .next_for_participants(vnodes.clone(), vec![old_process])
            .unwrap();
        durable.save_if_absent(&first).await.unwrap();

        let control = Arc::new(InMemoryKv::new(self_id));
        let control_kv: Arc<dyn ClusterKv> = control.clone();
        let recovery_kv: Arc<dyn ClusterKv> = control;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let new_boot = Uuid::from_u128(111);
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control_kv,
            recovery_kv,
            Some(Arc::clone(&durable)),
            members_rx,
            new_boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();

        let registry = Arc::new(VnodeRegistry::single_owner(2, self_id));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(2)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();
        let error = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect_err("a new process incarnation must restore before adopting its old vnodes");

        assert!(error.contains("cannot acquire 2 vnodes"), "{error}");
        let advanced = durable.load().await.unwrap().unwrap();
        assert_eq!(advanced.version, first.version + 1);
        assert_eq!(advanced.vnodes, vnodes);
        assert_eq!(
            advanced.participants,
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: new_boot,
            }]
        );
        assert!(db.cluster_intake_fenced());
        assert_eq!(registry.assignment_version(), first.version);
        assert_eq!(controller.read_snapshot_version().await, None);
    }

    #[tokio::test]
    async fn restart_after_durable_drain_retains_abort_until_recovery() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let boot = Uuid::from_u128(11);
        let restart_boot = Uuid::from_u128(111);
        let participant = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: boot,
        };
        let committed_vnodes = BTreeMap::from([(0, self_id)]);
        let durable = Arc::new(store());
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(committed_vnodes.clone(), vec![participant])
            .unwrap();
        durable.save_if_absent(&committed).await.unwrap();
        let drain = draining_snapshot(
            &committed,
            BTreeMap::from([(0, NodeId(2))]),
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            }],
        );
        assert!(matches!(
            durable
                .save_if_version(&drain, committed.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            restart_boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        let _leader_lease = grant_test_leadership(&controller).await;
        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();
        assert_eq!(db.adopt_draining_snapshot(&drain).unwrap(), vec![0]);
        assert!(registry.is_draining(0));

        let error = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect_err("a skipped assignment generation must restore before local adoption");
        assert!(error.contains("cannot acquire 1 vnodes"), "{error}");
        let aborted = durable.load().await.unwrap().unwrap();
        assert!(!aborted.draining);
        assert_eq!(aborted.version, drain.version);
        assert_eq!(aborted.vnodes, committed_vnodes);
        assert_eq!(aborted.participants, vec![participant]);
        assert_eq!(registry.assignment_version(), committed.version);
        assert!(registry.is_draining(0));
        assert!(db.cluster_intake_fenced());
        assert_eq!(controller.read_snapshot_version().await, None);
    }

    #[tokio::test]
    async fn draining_head_with_dead_predecessor_owner_uses_retained_roster() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let dead_owner = NodeId(2);
        let self_process = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: Uuid::from_u128(11),
        };
        let dead_process = CheckpointParticipant {
            node_id: dead_owner.0,
            boot_incarnation: Uuid::from_u128(22),
        };
        let durable = Arc::new(store());
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, dead_owner)]), vec![dead_process])
            .unwrap();
        durable.save_if_absent(&committed).await.unwrap();
        let draining = draining_snapshot(
            &committed,
            BTreeMap::from([(0, self_id)]),
            vec![self_process],
        );
        durable
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            self_process.boot_incarnation,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let _leader_lease = grant_test_leadership(&controller).await;

        let registry = Arc::new(VnodeRegistry::single_owner(1, dead_owner));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();

        let version = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .unwrap();
        assert_eq!(version, Some(draining.version));

        let rollback = durable.load().await.unwrap().unwrap();
        assert!(!rollback.draining);
        assert_eq!(rollback.vnodes, committed.vnodes);
        assert_eq!(rollback.participants, committed.participants);
        assert_eq!(registry.assignment_version(), rollback.version);
        assert!(
            db.cluster_intake_fenced(),
            "the dead predecessor process cannot certify the rollback generation"
        );
        assert_eq!(
            controller.checkpoint_assignment_fence(rollback.version),
            None
        );
    }

    #[tokio::test]
    async fn replacement_process_aborts_drain_through_the_same_authority_sequence() {
        use laminar_core::cluster::control::{
            ClusterCheckpointAuthorityError, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaseOutcome,
        };
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let old_boot = Uuid::from_u128(11);
        let new_boot = Uuid::from_u128(111);
        let authority = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 10));
        let old_owner = LeaderLeaseOwner {
            node: self_id,
            boot: old_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(old_lease) = authority.try_acquire(&old_owner, 0).await.unwrap()
        else {
            panic!("empty authority must grant the predecessor term");
        };
        let old_proof = old_lease.proof();

        let durable = Arc::new(store());
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(
                BTreeMap::from([(0, self_id)]),
                vec![CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: old_boot,
                }],
            )
            .unwrap();
        durable.save_if_absent(&committed).await.unwrap();
        let draining = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: Uuid::from_u128(22),
                }],
                old_proof.clone(),
            )
            .unwrap();
        durable
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();

        let new_owner = LeaderLeaseOwner {
            node: self_id,
            boot: new_boot,
            process_term: 2,
        };
        let observation = authority.observe_rival(&new_owner, &old_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(takeover) = authority
            .try_takeover(&new_owner, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("replacement process must take over the observed durable term");
        };
        assert!(takeover.token > old_lease.token);

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            new_boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        let _lease_watch = install_test_leadership(
            &controller,
            Arc::clone(&authority),
            new_owner,
            takeover.clone(),
        );
        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();

        let error = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect_err("replacement must restore before adopting the predecessor rollback");
        assert!(error.contains("cannot acquire 1 vnodes"), "{error}");
        let materialized = durable.load().await.unwrap().unwrap();
        assert!(!materialized.draining);
        assert_eq!(materialized.version, draining.version);
        assert_eq!(materialized.vnodes, committed.vnodes);
        let winner = authority
            .assignment_drain_decision(draining.version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(winner.verdict, AssignmentDrainVerdict::Abort);
        assert_eq!(winner.leader_proof, takeover.proof());

        let stale = AssignmentDrainDecision::new(
            draining.drain_transition.as_ref().unwrap(),
            old_proof.clone(),
            AssignmentDrainVerdict::Abort,
        )
        .unwrap();
        assert!(matches!(
            authority
                .record_assignment_drain_decision(&old_proof, stale)
                .await,
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
    }

    #[tokio::test]
    async fn takeover_materializes_decision_written_before_snapshot_cas() {
        use laminar_core::cluster::control::{
            ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaseOutcome,
        };
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let old_boot = Uuid::from_u128(11);
        let new_boot = Uuid::from_u128(111);
        let authority = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 10));
        let old_owner = LeaderLeaseOwner {
            node: self_id,
            boot: old_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(old_lease) = authority.try_acquire(&old_owner, 0).await.unwrap()
        else {
            panic!("empty authority must grant the predecessor term");
        };
        let old_proof = old_lease.proof();
        let durable = Arc::new(store());
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(
                BTreeMap::from([(0, self_id)]),
                vec![CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: old_boot,
                }],
            )
            .unwrap();
        durable.save_if_absent(&committed).await.unwrap();
        let draining = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: Uuid::from_u128(22),
                }],
                old_proof.clone(),
            )
            .unwrap();
        durable
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();
        let committed_decision = AssignmentDrainDecision::new(
            draining.drain_transition.as_ref().unwrap(),
            old_proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        assert!(matches!(
            authority
                .record_assignment_drain_decision(&old_proof, committed_decision)
                .await
                .unwrap(),
            RecordAssignmentDrainDecisionResult::Created(_)
        ));

        let current_old_lease = authority.load().await.unwrap().unwrap();
        let new_owner = LeaderLeaseOwner {
            node: self_id,
            boot: new_boot,
            process_term: 2,
        };
        let observation = authority
            .observe_rival(&new_owner, &current_old_lease)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(takeover) = authority
            .try_takeover(&new_owner, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("replacement process must take over the decision-bearing term");
        };

        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            new_boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        let _lease_watch =
            install_test_leadership(&controller, Arc::clone(&authority), new_owner, takeover);
        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();

        assert_eq!(
            try_rebalance(
                &db,
                &controller,
                &durable,
                &registry,
                &[self_id],
                RebalanceConfig::test_defaults(),
            )
            .await
            .unwrap(),
            Some(draining.version)
        );
        let materialized = durable.load().await.unwrap().unwrap();
        assert!(!materialized.draining);
        assert_eq!(materialized.vnodes, draining.vnodes);
        assert_eq!(registry.assignment_version(), draining.version);
        assert_eq!(registry.owner(0), NodeId(2));
        let winner = authority
            .assignment_drain_decision(draining.version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(winner.verdict, AssignmentDrainVerdict::Commit);
        assert_eq!(winner.leader_proof, old_proof);
    }
}
