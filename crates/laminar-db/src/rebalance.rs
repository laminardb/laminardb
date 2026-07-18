//! Dynamic vnode rebalance control plane.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use laminar_connectors::connector::{SourceDrainOutcome, SourceDrainResolution};
use laminar_core::checkpoint::{AssignmentDrainId, AssignmentDrainTransition};
use laminar_core::cluster::control::{
    AssignmentDrainDecision, AssignmentDrainVerdict, AssignmentRecoveryDecision,
    AssignmentSnapshot, AssignmentSnapshotStore, CheckpointAssignmentAdoption,
    CheckpointAssignmentFence, CheckpointParticipant, ClusterController, LeaderLeaseStore,
    RecordAssignmentDrainDecisionResult, RecordAssignmentRecoveryDecisionResult, RotateOutcome,
};
use laminar_core::cluster::discovery::NodeState;
use laminar_core::state::{
    owners_per_domain, rendezvous_assignment, Locality, NodeId, VnodeRegistry,
};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
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
    /// Bound on waiting for the frozen predecessor roster to ack the draining snapshot before
    /// the pre-rotation checkpoint; on timeout the rotation aborts. Must exceed `watcher_poll`.
    pub drain_ack_timeout: Duration,
    /// Locality tier the placement metrics group by (0 = coarsest).
    pub placement_isolation_tier: usize,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            watcher_poll: Duration::from_secs(2),
            rebalance_debounce: Duration::from_secs(5),
            // A healthy pre-rotation drain commits in well under a second; the longer budget
            // absorbs slow external source cuts without weakening the frozen-roster quorum.
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

async fn ensure_local_recovery_fault(
    db: &LaminarDB,
    controller: &ClusterController,
) -> Result<(), String> {
    controller.set_recovering(true);
    crate::coordinated_recovery::request_local_fault(controller, &db.pending_recovery_fault)
        .await
        .map(|_| ())
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

async fn hold_terminal_source_resolution(
    db: &Arc<LaminarDB>,
    controller: Option<&ClusterController>,
    round: AssignmentDrainId,
    deadline: tokio::time::Instant,
    held: &mut Option<(AssignmentDrainId, u64)>,
) -> Result<u64, String> {
    let revision = db
        .assignment_authority_revision
        .load(std::sync::atomic::Ordering::Acquire);
    let controller_closed = controller.is_none_or(|controller| {
        controller
            .checkpoint_assignment_fence(round.predecessor_version)
            .is_none()
            && controller
                .checkpoint_assignment_fence(round.target_version)
                .is_none()
    });
    if *held == Some((round, revision)) && db.cluster_intake_fenced() && controller_closed {
        return Ok(revision);
    }

    suspend_local_assignment_authority(db, controller, deadline).await?;
    let revision = db
        .assignment_authority_revision
        .load(std::sync::atomic::Ordering::Acquire);
    *held = Some((round, revision));
    Ok(revision)
}

/// Spawn the per-node snapshot watcher. Exits on `shutdown`.
pub fn spawn_snapshot_watcher(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: CancellationToken,
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
        let mut terminal_resolution_hold: Option<(AssignmentDrainId, u64)> = None;
        let mut installed_fence: Option<(u64, [u8; 32])> = None;
        let mut installed_authority_revision = 0u64;
        let mut assignment_invalidated = false;
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let local = registry.assignment_version();
            let head_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
            let mut authority_revision = db
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire);

            // The durable namespace is authoritative, so every tick audits it even when the local
            // version has not changed. This closes the crash window after a successful CAS but
            // before local adoption.
            let audit = tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                result = tokio::time::timeout_at(head_deadline, store.load()) => result,
            };
            let mut audited_terminal = None;
            let mut audited_recovery = false;
            if let Ok(Ok(Some(snapshot))) = &audit {
                if !snapshot.draining {
                    let materialization_audit = tokio::time::timeout_at(
                        head_deadline,
                        audit_assignment_snapshot_authority_outcome(
                            &store,
                            controller.as_deref(),
                            snapshot,
                        ),
                    )
                    .await;
                    match materialization_audit {
                        Ok(Ok(outcome)) => {
                            audited_recovery = snapshot.version > 1 && outcome.is_none();
                            audited_terminal = outcome;
                        }
                        failed => {
                            assignment_invalidated = true;
                            let _ = suspend_local_assignment_authority(
                                &db,
                                controller.as_deref(),
                                head_deadline,
                            )
                            .await;
                            match failed {
                                Ok(Err(error)) => {
                                    warn!(%error, version = snapshot.version, "snapshot watcher: drain finalization audit failed; assignment authority suspended")
                                }
                                Err(_) => {
                                    warn!(version = snapshot.version, timeout = ?config.checkpoint_timeout, "snapshot watcher: drain finalization audit timed out; assignment authority suspended")
                                }
                                Ok(Ok(_)) => unreachable!(),
                            }
                            continue;
                        }
                    }
                }
            }
            if let Ok(Ok(Some(snapshot))) = &audit {
                if !snapshot.draining {
                    let transition = active_local_drain.clone().or_else(|| {
                        audited_terminal.as_ref().and_then(|audited| {
                            controller
                                .as_deref()
                                .and_then(|controller| {
                                    local_drain_participant(controller, &audited.transition)
                                })
                                .map(|_| audited.transition.clone())
                        })
                    });
                    if let Some(transition) = transition {
                        let audited_for_transition = audited_terminal
                            .as_ref()
                            .filter(|audited| audited.transition == transition);
                        let already_resolved = match audited_for_transition {
                            Some(audited) => {
                                let resolution = SourceDrainResolution {
                                    round: transition.id(),
                                    outcome: audited.outcome,
                                };
                                match crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
                                    &db.owned_source_tasks,
                                    resolution,
                                ) {
                                    Ok(resolved) => resolved,
                                    Err(error) => {
                                        assignment_invalidated = true;
                                        let _ = hold_terminal_source_resolution(
                                            &db,
                                            controller.as_deref(),
                                            transition.id(),
                                            head_deadline,
                                            &mut terminal_resolution_hold,
                                        )
                                        .await;
                                        warn!(%error, version = snapshot.version, "snapshot watcher: source drain status audit failed; assignment authority suspended");
                                        continue;
                                    }
                                }
                            }
                            None => false,
                        };
                        if already_resolved {
                            active_local_drain = None;
                            terminal_resolution_hold = None;
                        } else {
                            assignment_invalidated = true;
                            match hold_terminal_source_resolution(
                                &db,
                                controller.as_deref(),
                                transition.id(),
                                head_deadline,
                                &mut terminal_resolution_hold,
                            )
                            .await
                            {
                                Ok(revision) => authority_revision = revision,
                                Err(error) => {
                                    warn!(%error, version = snapshot.version, "snapshot watcher: could not suspend authority for source drain resolution");
                                    continue;
                                }
                            }
                            match settle_observed_local_drain(
                                &db,
                                &store,
                                &registry,
                                controller.as_deref(),
                                &transition,
                                snapshot,
                                audited_for_transition,
                                head_deadline,
                                SourceDrainResolutionDeadline::Fresh(config.drain_ack_timeout),
                            )
                            .await
                            {
                                Ok(true) => {
                                    active_local_drain = None;
                                    terminal_resolution_hold = None;
                                    authority_revision = db
                                        .assignment_authority_revision
                                        .load(std::sync::atomic::Ordering::Acquire);
                                }
                                Ok(false) => continue,
                                Err(error) => {
                                    warn!(%error, version = snapshot.version, "snapshot watcher: local source drain resolution failed; assignment authority suspended");
                                    continue;
                                }
                            }
                        }
                    } else {
                        terminal_resolution_hold = None;
                    }
                }
            }
            match audit {
                // Drain phase: hold every predecessor source at one global input frontier;
                // ownership is unchanged so the registry version stays put.
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
                                match db.validate_source_drain_snapshot(&snap) {
                                    Ok(()) => {
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
                                                std::cmp::min(
                                                    head_deadline,
                                                    tokio::time::Instant::now()
                                                        + config.drain_ack_timeout,
                                                ),
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
                    if audited_recovery {
                        let Some(controller) = controller.as_deref() else {
                            assignment_invalidated = true;
                            let _ =
                                suspend_local_assignment_authority(&db, None, head_deadline).await;
                            warn!(
                                version = snap.version,
                                "snapshot watcher: recovery assignment has no cluster controller"
                            );
                            continue;
                        };
                        if let Err(error) =
                            close_local_assignment_authority(&db, Some(controller), head_deadline)
                                .await
                        {
                            assignment_invalidated = true;
                            warn!(%error, version = snap.version, "snapshot watcher: could not fence recovery assignment");
                            continue;
                        }
                        if let Err(error) = ensure_local_recovery_fault(&db, controller).await {
                            assignment_invalidated = true;
                            warn!(%error, version = snap.version, "snapshot watcher: could not publish recovery fault");
                            continue;
                        }
                        authority_revision = db
                            .assignment_authority_revision
                            .load(std::sync::atomic::Ordering::Acquire);
                        assignment_invalidated = true;
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
                let participant = CheckpointParticipant {
                    node_id: c.instance_id().0,
                    boot_incarnation: c.recovery_incarnation(),
                };
                let local_is_participant = durable_snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot.version == version
                        && snapshot
                            .participants
                            .binary_search_by_key(&participant.node_id, |entry| entry.node_id)
                            .ok()
                            .and_then(|index| snapshot.participants.get(index))
                            == Some(&participant)
                });
                if local_is_participant {
                    let owner_ids: Vec<u64> =
                        assignment.owners().iter().map(|owner| owner.0).collect();
                    let adoption = CheckpointAssignmentAdoption {
                        participant,
                        assignment_version: version,
                        vnode_count: registry.vnode_count(),
                        partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
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
                } else {
                    published_adoption = None;
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
                        let local_has_authority = fence.participant_incarnation(c.instance_id().0)
                            == Some(c.recovery_incarnation());
                        let published_fence =
                            c.checkpoint_assignment_fence(fence.assignment_version);
                        let needs_activation = assignment_invalidated
                            || installed_fence != Some(identity)
                            || published_fence.as_ref() != Some(&fence)
                            || c.checkpoint_drain_transition() != drain_transition
                            || (local_has_authority
                                && !c.is_recovering()
                                && drain_transition.is_none()
                                && db.cluster_intake_fenced());
                        if needs_activation {
                            if !assignment_invalidated && installed_fence != Some(identity) {
                                // A different certificate supersedes the active lifetime. Cancel it
                                // before waiting for compute cycles that may be blocked in send.
                                db.set_source_gate(true);
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
) -> Result<SourceDrainOutcome, String> {
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
        return Ok(SourceDrainOutcome::Commit);
    }
    if fence.assignment_version == transition.target.assignment_version
        && fence.vnode_count == transition.predecessor.vnode_count
        && fence.assignment_digest == transition.predecessor.assignment_digest
        && fence.participants == transition.predecessor.participants
    {
        return Ok(SourceDrainOutcome::Abort);
    }
    Err(format!(
        "assignment {} is neither the committed target nor predecessor rollback for drain {:?}",
        finalized.version,
        transition.id()
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditedDrainOutcome {
    transition: AssignmentDrainTransition,
    outcome: SourceDrainOutcome,
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
    audit_assignment_snapshot_authority_outcome(store, controller, snapshot)
        .await
        .map(|_| ())
}

/// Select the last committed assignment that a cluster process may adopt during startup.
/// A draining head has not transferred ownership, so startup retains its audited predecessor.
///
/// # Errors
///
/// Returns an error when the head or retained predecessor lacks valid durable authority.
pub async fn startup_committed_assignment(
    store: &AssignmentSnapshotStore,
    controller: Option<&ClusterController>,
    head: AssignmentSnapshot,
) -> Result<AssignmentSnapshot, String> {
    audit_assignment_snapshot_authority(store, controller, &head)
        .await
        .map_err(|error| {
            format!(
                "audit startup assignment {} authority: {error}",
                head.version
            )
        })?;
    if !head.draining {
        return Ok(head);
    }

    let transition = store
        .load_drain_transition(head.version)
        .await
        .map_err(|error| {
            format!(
                "load startup assignment {} drain transition: {error}",
                head.version
            )
        })?
        .ok_or_else(|| {
            format!(
                "draining startup assignment {} has no exact transition",
                head.version
            )
        })?;
    if head.drain_transition.as_ref() != Some(&transition) {
        return Err(format!(
            "draining startup assignment {} does not match its durable transition",
            head.version
        ));
    }
    let prior_version = head
        .version
        .checked_sub(1)
        .ok_or_else(|| "draining assignment has no retained committed predecessor".to_string())?;
    let prior = store
        .load_version(prior_version)
        .await
        .map_err(|error| {
            format!(
                "load retained assignment {prior_version} before draining head {}: {error}",
                head.version
            )
        })?
        .ok_or_else(|| {
            format!(
                "draining assignment {} has no retained committed predecessor {prior_version}",
                head.version
            )
        })?;
    if prior.draining {
        return Err(format!(
            "draining assignment {} has a draining predecessor {prior_version}",
            head.version
        ));
    }
    if prior
        .assignment_fence()
        .map_err(|error| error.to_string())?
        != transition.predecessor
    {
        return Err(format!(
            "draining assignment {} does not bind retained predecessor {prior_version}",
            head.version
        ));
    }
    audit_assignment_snapshot_authority(store, controller, &prior)
        .await
        .map_err(|error| format!("audit retained assignment {prior_version} authority: {error}"))?;
    Ok(prior)
}

async fn audit_assignment_snapshot_authority_outcome(
    store: &AssignmentSnapshotStore,
    controller: Option<&ClusterController>,
    snapshot: &AssignmentSnapshot,
) -> Result<Option<AuditedDrainOutcome>, String> {
    if snapshot.draining {
        return Ok(None);
    }
    if snapshot.version == 1 {
        return Ok(None);
    }
    let authority = match controller {
        Some(controller) => Some(
            controller
                .checkpoint_authority()
                .map_err(|error| error.to_string())?,
        ),
        None => None,
    };
    let Some(transition) = store
        .load_drain_transition(snapshot.version)
        .await
        .map_err(|error| error.to_string())?
    else {
        audit_materialized_recovery_with_authority(store, authority.as_deref(), snapshot).await?;
        return Ok(None);
    };
    audit_materialized_drain_transition(authority.as_deref(), snapshot, transition)
        .await
        .map(Some)
}

async fn audit_materialized_drain_with_authority(
    store: &AssignmentSnapshotStore,
    authority: Option<&LeaderLeaseStore>,
    snapshot: &AssignmentSnapshot,
) -> Result<Option<AuditedDrainOutcome>, String> {
    if snapshot.draining {
        return Ok(None);
    }
    let Some(transition) = store
        .load_drain_transition(snapshot.version)
        .await
        .map_err(|error| error.to_string())?
    else {
        audit_materialized_recovery_with_authority(store, authority, snapshot).await?;
        return Ok(None);
    };
    audit_materialized_drain_transition(authority, snapshot, transition)
        .await
        .map(Some)
}

async fn audit_materialized_recovery_with_authority(
    store: &AssignmentSnapshotStore,
    authority: Option<&LeaderLeaseStore>,
    snapshot: &AssignmentSnapshot,
) -> Result<(), String> {
    if snapshot.version == 1 {
        return Ok(());
    }
    let authority = authority.ok_or_else(|| {
        format!(
            "materialized assignment recovery {} has no cluster authority",
            snapshot.version
        )
    })?;
    let decision = authority
        .assignment_recovery_decision(snapshot.version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "assignment {} has no drain transition or recovery authority decision",
                snapshot.version
            )
        })?;
    let proposal = store
        .load_recovery_proposal(&decision.proposal)
        .await
        .map_err(|error| error.to_string())?;
    if proposal != *snapshot
        || proposal
            .assignment_fence()
            .map_err(|error| error.to_string())?
            != decision.target
    {
        return Err(format!(
            "assignment {} does not match its authorized recovery proposal",
            snapshot.version
        ));
    }
    let predecessor_version = snapshot
        .version
        .checked_sub(1)
        .ok_or_else(|| "recovery assignment has no predecessor generation".to_string())?;
    let predecessor = store
        .load_version(predecessor_version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "recovery assignment {} lost predecessor {predecessor_version}",
                snapshot.version
            )
        })?;
    if predecessor.draining
        || predecessor
            .assignment_fence()
            .map_err(|error| error.to_string())?
            != decision.predecessor
    {
        return Err(format!(
            "assignment {} recovery decision does not bind its exact committed predecessor",
            snapshot.version
        ));
    }
    Ok(())
}

async fn audit_materialized_drain_transition(
    authority: Option<&LeaderLeaseStore>,
    snapshot: &AssignmentSnapshot,
    transition: AssignmentDrainTransition,
) -> Result<AuditedDrainOutcome, String> {
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
        AssignmentDrainVerdict::Commit => SourceDrainOutcome::Commit,
        AssignmentDrainVerdict::Abort => SourceDrainOutcome::Abort,
    };
    if observed != expected {
        return Err(format!(
            "assignment {} materialization conflicts with its authority decision",
            snapshot.version
        ));
    }
    Ok(AuditedDrainOutcome {
        transition,
        outcome: observed,
    })
}

/// Apply a durable drain outcome before adopting any later assignment. The exact terminal
/// generation must be installed before connector resolution; skipping it would make the source
/// cut unverifiable.
#[derive(Clone, Copy)]
enum SourceDrainResolutionDeadline {
    Fresh(Duration),
    Absolute(tokio::time::Instant),
}

impl SourceDrainResolutionDeadline {
    fn resolve(self) -> tokio::time::Instant {
        match self {
            Self::Fresh(timeout) => tokio::time::Instant::now() + timeout,
            Self::Absolute(deadline) => deadline,
        }
    }
}

async fn settle_observed_local_drain(
    db: &Arc<LaminarDB>,
    store: &AssignmentSnapshotStore,
    registry: &VnodeRegistry,
    controller: Option<&ClusterController>,
    transition: &AssignmentDrainTransition,
    observed: &AssignmentSnapshot,
    audited_observed: Option<&AuditedDrainOutcome>,
    adoption_deadline: tokio::time::Instant,
    drain_deadline: SourceDrainResolutionDeadline,
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
    let audited = match audited_observed
        .filter(|audited| {
            finalized.version == observed.version && audited.transition == *transition
        })
        .cloned()
    {
        Some(audited) => audited,
        None => tokio::time::timeout_at(
            adoption_deadline,
            audit_assignment_snapshot_authority_outcome(store, controller, &finalized),
        )
        .await
        .map_err(|_| {
            format!(
                "timed out auditing terminal assignment {} for drain resolution",
                finalized.version
            )
        })??
        .ok_or_else(|| {
            format!(
                "terminal assignment {} lost drain transition history",
                finalized.version
            )
        })?,
    };
    if audited.transition != *transition {
        return Err(format!(
            "terminal assignment {} binds a different drain transition",
            finalized.version
        ));
    }
    let outcome = audited.outcome;
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
    db.resolve_local_source_drain(transition.id(), outcome, drain_deadline.resolve())
        .await
        .map_err(|error| error.to_string())?;
    Ok(true)
}

fn clear_settled_source_drain(
    controller: &ClusterController,
    transition: &AssignmentDrainTransition,
) -> Result<(), String> {
    match controller.checkpoint_drain_transition() {
        Some(active) if active == *transition => controller
            .clear_checkpoint_drain_transition_if_matches(transition)
            .then_some(())
            .ok_or_else(|| "process-local source drain changed during settlement".into()),
        Some(_) => Err("process-local source drain changed during settlement".into()),
        None => Ok(()),
    }
}

/// Reapply a materialized drain terminal to a source generation created by coordinated recovery.
/// The caller keeps recovery and intake fenced until this returns.
pub(crate) async fn settle_source_drain_before_recovery_release(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    expected_fence: &CheckpointAssignmentFence,
    deadline: tokio::time::Instant,
) -> Result<Option<u64>, String> {
    if !controller.is_recovering() || !db.cluster_intake_fenced() {
        return Err("recovery source-drain settlement requires closed intake".into());
    }
    let published_transition = controller.checkpoint_drain_transition();
    let store =
        db.assignment_snapshot_store.lock().clone().ok_or_else(|| {
            "recovery source-drain settlement has no assignment store".to_string()
        })?;
    let snapshot = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery source-drain head read timed out".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "recovery source-drain settlement has no assignment head".to_string())?;
    if snapshot.draining {
        return Err(format!(
            "recovery Release reached unresolved draining assignment {}",
            snapshot.version
        ));
    }
    let fence = snapshot
        .assignment_fence()
        .map_err(|error| error.to_string())?;
    if &fence != expected_fence {
        return Err(format!(
            "recovery assignment {} changed before source-drain settlement",
            snapshot.version
        ));
    }
    let audited = tokio::time::timeout_at(
        deadline,
        audit_assignment_snapshot_authority_outcome(&store, Some(controller), &snapshot),
    )
    .await
    .map_err(|_| "recovery source-drain terminal audit timed out".to_string())??;
    let Some(audited) = audited else {
        if published_transition.is_some() {
            return Err("process-local source drain has no durable terminal".into());
        }
        return Ok(None);
    };
    if published_transition.is_some_and(|active| active != audited.transition) {
        return Err("process-local source drain conflicts with the durable terminal".into());
    }
    if local_drain_participant(controller, &audited.transition).is_none() {
        clear_settled_source_drain(controller, &audited.transition)?;
        return Ok(None);
    }
    if tokio::time::Instant::now() >= deadline {
        return Err("recovery source-drain settlement deadline expired".into());
    }
    let registry = db
        .vnode_registry
        .lock()
        .clone()
        .ok_or_else(|| "recovery source-drain settlement has no vnode registry".to_string())?;
    if !settle_observed_local_drain(
        db,
        &store,
        &registry,
        Some(controller),
        &audited.transition,
        &snapshot,
        Some(&audited),
        deadline,
        SourceDrainResolutionDeadline::Absolute(deadline),
    )
    .await?
    {
        return Err("materialized source drain was not ready for recovery Release".into());
    }
    let confirmed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "recovery source-drain head recheck timed out".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "recovery source-drain assignment disappeared".to_string())?;
    if confirmed != snapshot {
        return Err("recovery source-drain assignment changed during settlement".into());
    }
    let confirmed_terminal = tokio::time::timeout_at(
        deadline,
        audit_assignment_snapshot_authority_outcome(&store, Some(controller), &confirmed),
    )
    .await
    .map_err(|_| "recovery source-drain terminal recheck timed out".to_string())??;
    if confirmed_terminal.as_ref() != Some(&audited) {
        return Err("recovery source-drain terminal changed during settlement".into());
    }
    clear_settled_source_drain(controller, &audited.transition)?;
    Ok(Some(snapshot.version))
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
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    db.prepare_local_source_drain(transition, participant, deadline)
        .await
        .map_err(|error| error.to_string())?;

    // Receipt production can block on a connector FIFO. Re-read both the transition and its
    // leader lease before making that receipt visible to the frozen predecessor quorum.
    audit_exact_drain_head(store, registry, draining, controller, deadline).await?;
    controller.publish_checkpoint_drain_transition(Some(transition.clone()));
    tokio::time::timeout_at(deadline, controller.announce_drain_ack(transition))
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
    shutdown: CancellationToken,
    config: RebalanceConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut members = controller.members_watch();
        let mut audit = tokio::time::interval(config.watcher_poll);
        audit.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let membership_changed = tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
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
                        () = shutdown.cancelled() => return,
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
                match try_rebalance_owned(
                    Arc::clone(&db),
                    Arc::clone(&controller),
                    Arc::clone(&store),
                    Arc::clone(&registry),
                    live,
                    config,
                )
                .await
                {
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
                            () = shutdown.cancelled() => return,
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
                    Ok(_) => match snap.to_vnode_vec(vnode_count) {
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

async fn materialize_recovery_decision(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    controller: &ClusterController,
    decision: AssignmentRecoveryDecision,
    operation_timeout: Duration,
) -> Result<Option<u64>, String> {
    let deadline = tokio::time::Instant::now() + operation_timeout;
    close_local_assignment_authority(db, Some(controller), deadline).await?;
    ensure_local_recovery_fault(db, controller).await?;
    let proposal =
        tokio::time::timeout_at(deadline, store.load_recovery_proposal(&decision.proposal))
            .await
            .map_err(|_| {
                "recovery proposal load exceeded the materialization deadline".to_string()
            })?
            .map_err(|error| error.to_string())?;
    if proposal
        .assignment_fence()
        .map_err(|error| error.to_string())?
        != decision.target
    {
        return Err("recovery authority winner does not match its staged proposal".into());
    }
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    let durable = match tokio::time::timeout_at(
        deadline,
        authority.materialize_assignment_recovery(decision.target_version()),
    )
    .await
    .map_err(|_| "recovery assignment materialization exceeded its deadline".to_string())?
    .map_err(|error| error.to_string())?
    {
        RotateOutcome::Rotated => proposal.clone(),
        RotateOutcome::Conflict(winner) => winner,
    };
    if durable != proposal {
        return Err(format!(
            "assignment {} materialization conflicts with the authority recovery winner",
            decision.target_version()
        ));
    }
    tokio::time::timeout_at(
        deadline,
        audit_assignment_snapshot_authority(store, Some(controller), &durable),
    )
    .await
    .map_err(|_| "recovery assignment audit exceeded the materialization deadline".to_string())??;
    let version = durable.version;
    let adoption = db
        .adopt_assignment_snapshot(durable, deadline)
        .await
        .map_err(|error| error.to_string())?;
    log_adoption("rebalance-recovery", &adoption);
    let oldest_retained = version.saturating_sub(1);
    let maintenance_store = Arc::clone(store);
    let maintenance_authority = Arc::clone(&authority);
    let maintenance_proof = controller.capture_leader_proof();
    tokio::spawn(async move {
        match maintenance_store.prune_before(oldest_retained).await {
            Ok(()) => {
                if let Some(proof) = maintenance_proof {
                    if let Err(error) = maintenance_authority
                        .prune_assignment_drain_decisions_before(&proof, oldest_retained)
                        .await
                    {
                        warn!(%error, "assignment recovery authority prune failed after snapshot prune");
                    }
                }
            }
            Err(error) => warn!(%error, "snapshot prune failed after assignment recovery"),
        }
    });
    Ok(Some(version))
}

async fn reconcile_pending_recovery_decision(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    controller: &ClusterController,
    current: &AssignmentSnapshot,
    operation_timeout: Duration,
) -> Result<Option<u64>, String> {
    let target_version = current
        .version
        .checked_add(1)
        .ok_or_else(|| "assignment version exhausted".to_string())?;
    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    let Some(decision) = tokio::time::timeout(
        operation_timeout,
        authority.assignment_recovery_decision(target_version),
    )
    .await
    .map_err(|_| "recovery authority lookup timed out".to_string())?
    .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    if decision.predecessor
        != current
            .assignment_fence()
            .map_err(|error| error.to_string())?
    {
        return Err(format!(
            "pending recovery decision for assignment {target_version} has the wrong predecessor"
        ));
    }
    materialize_recovery_decision(db, store, controller, decision, operation_timeout).await
}

fn replaced_predecessor_processes(
    predecessor: &CheckpointAssignmentFence,
    target: &CheckpointAssignmentFence,
) -> Vec<CheckpointParticipant> {
    predecessor
        .participants
        .iter()
        .copied()
        .filter(|participant| {
            target.participant_incarnation(participant.node_id)
                != Some(participant.boot_incarnation)
        })
        .collect()
}

async fn authorize_recovery_successor(
    db: &Arc<LaminarDB>,
    store: &Arc<AssignmentSnapshotStore>,
    controller: &ClusterController,
    current: &AssignmentSnapshot,
    proposal: AssignmentSnapshot,
    operation_timeout: Duration,
    reason: &str,
) -> Result<Option<u64>, String> {
    let predecessor = current
        .assignment_fence()
        .map_err(|error| error.to_string())?;
    let target = proposal
        .assignment_fence()
        .map_err(|error| error.to_string())?;
    let deadline = controller.process_fencing_deadline(operation_timeout)?;
    close_local_assignment_authority(db, Some(controller), deadline).await?;
    let proposal_ref = tokio::time::timeout_at(deadline, store.stage_recovery_proposal(&proposal))
        .await
        .map_err(|_| "recovery proposal staging exceeded the fencing deadline".to_string())?
        .map_err(|error| error.to_string())?;

    let removed = replaced_predecessor_processes(&predecessor, &target);
    let fence_results = futures::future::join_all(
        removed
            .iter()
            .copied()
            .map(|participant| controller.fence_process_incarnation(participant, deadline)),
    )
    .await;
    let mut process_fences = Vec::with_capacity(fence_results.len());
    for result in fence_results {
        process_fences.push(result?);
    }

    let observed = tokio::time::timeout_at(deadline, store.load())
        .await
        .map_err(|_| "assignment head revalidation exceeded the fencing deadline".to_string())?
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "assignment head disappeared during recovery fencing".to_string())?;
    if observed != *current {
        return Err(format!(
            "assignment head advanced from {} while process fencing was in progress",
            current.version
        ));
    }
    tokio::time::timeout_at(
        deadline,
        audit_assignment_snapshot_authority(store, Some(controller), &observed),
    )
    .await
    .map_err(|_| "predecessor authority audit exceeded the fencing deadline".to_string())??;

    let target_checks =
        futures::future::join_all(target.participants.iter().copied().map(|participant| {
            controller.verify_current_process_incarnation(participant, deadline)
        }));
    for (participant, check) in target.participants.iter().zip(target_checks.await) {
        if !check? {
            return Err(format!(
                "successor process {} is not the current durable lease owner",
                participant.node_id
            ));
        }
    }

    let leader_proof = controller
        .capture_leader_proof()
        .ok_or_else(|| "assignment recovery lost the current durable leader proof".to_string())?;
    let decision = AssignmentRecoveryDecision::new(
        predecessor,
        target,
        proposal_ref,
        process_fences,
        leader_proof.clone(),
    )?;
    let decision = match tokio::time::timeout_at(
        deadline,
        controller.record_assignment_recovery_decision(&leader_proof, decision, deadline),
    )
    .await
    .map_err(|_| "recovery authority admission exceeded the fencing deadline".to_string())?
    .map_err(|error| error.to_string())?
    {
        RecordAssignmentRecoveryDecisionResult::Created(decision)
        | RecordAssignmentRecoveryDecisionResult::Unchanged(decision) => decision,
        RecordAssignmentRecoveryDecisionResult::Conflict { winner } => winner,
    };
    warn!(
        predecessor_version = current.version,
        target_version = decision.target_version(),
        %reason,
        "authorized successor assignment from the last committed cluster cut"
    );
    materialize_recovery_decision(db, store, controller, decision, operation_timeout).await
}

fn try_rebalance_owned(
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    live: Vec<NodeId>,
    config: RebalanceConfig,
) -> futures::future::BoxFuture<'static, Result<Option<u64>, String>> {
    Box::pin(async move {
        let head_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
        let current = tokio::time::timeout_at(head_deadline, store.load())
            .await
            .map_err(|_| "durable assignment head audit timed out".to_string())?
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "no snapshot on store — boot seed missing".to_string())?;
        tokio::time::timeout_at(
            head_deadline,
            audit_assignment_snapshot_authority_owned(
                Arc::clone(&store),
                Arc::clone(&controller),
                current.clone(),
            ),
        )
        .await
        .map_err(|_| "durable assignment authority audit timed out".to_string())??;
        if let Some(version) = reconcile_pending_recovery_decision_owned(
            Arc::clone(&db),
            Arc::clone(&store),
            Arc::clone(&controller),
            current.clone(),
            config.checkpoint_timeout,
        )
        .await?
        {
            return Ok(Some(version));
        }
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
            // A writer can fail after its durable CAS succeeds but before local adoption. Reconcile
            // that durable fact before comparing it with desired placement; otherwise an
            // already-correct owner map would be mistaken for a no-op forever.
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

        if current.draining {
            let prior_version = current.version.checked_sub(1).ok_or_else(|| {
                "draining assignment has no prior committed generation".to_string()
            })?;
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
                audit_assignment_snapshot_authority_owned(
                    Arc::clone(&store),
                    Arc::clone(&controller),
                    prior.clone(),
                ),
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
            close_local_assignment_authority(&db, Some(controller.as_ref()), head_deadline).await?;
            return finalize_drain_snapshot(
                &db,
                &store,
                &controller,
                &current,
                &prior,
                AssignmentDrainVerdict::Abort,
                config,
            )
            .await;
        }

        // Settle an existing drain before considering the current placement input. An empty live set
        // must not strand a durable transition without its authority-sequenced outcome.
        if live.is_empty() {
            return Ok(None);
        }
        let live_ids = successor_participant_ids(&live);
        let observed_processes = controller
            .available_recovery_participant_incarnations(&live_ids)
            .await?;
        let process_read_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
        let process_checks =
            futures::future::join_all(observed_processes.iter().copied().map(|participant| {
                controller.verify_current_process_incarnation(participant, process_read_deadline)
            }))
            .await;
        let mut available_processes = Vec::with_capacity(observed_processes.len());
        for (participant, check) in observed_processes.into_iter().zip(process_checks) {
            if check? {
                available_processes.push(participant);
            }
        }
        let successor_processes = available_processes
            .iter()
            .copied()
            .filter(|participant| controller.admit_successor_process(*participant))
            .collect::<Vec<_>>();
        let successors: Vec<NodeId> = successor_processes
            .iter()
            .map(|participant| NodeId(participant.node_id))
            .collect();
        if successors.is_empty() {
            return Ok(None);
        }
        let mut new_assignment =
            rendezvous_assignment(registry.vnode_count(), &successors).to_vec();
        let mut new_vnodes = AssignmentSnapshot::vnodes_from_vec(&new_assignment);
        // The successor checkpoint quorum is the exact successor owner set. Deriving it from the
        // current membership/checkpoint roster retains a gracefully departing process after all of
        // its vnodes move away; the first checkpoint after that process exits can then never reach
        // quorum. The predecessor roster remains authoritative for the drain cut and is carried by
        // `current`.
        let mut participants = successor_participants(&new_assignment, &successor_processes)?;
        let roster_changed = current.participants != participants;

        if new_vnodes == current.vnodes && !roster_changed {
            return Ok(None);
        }

        let recovery_reason =
            predecessor_cut_unavailability(&controller, &current, &current_owners, &successors)
                .await;
        if let Some(reason) = recovery_reason {
            retain_recovery_predecessors(
                &mut new_assignment,
                &current.participants,
                &successor_processes,
            )?;
            new_vnodes = AssignmentSnapshot::vnodes_from_vec(&new_assignment);
            participants = successor_participants(&new_assignment, &successor_processes)?;
            let proposal = current
                .next_for_participants(new_vnodes, participants)
                .map_err(|error| error.to_string())?;
            return authorize_recovery_successor_owned(
                Arc::clone(&db),
                Arc::clone(&store),
                Arc::clone(&controller),
                current.clone(),
                proposal,
                config.checkpoint_timeout,
                reason,
            )
            .await;
        }

        // Every graceful rotation uses one global predecessor source frontier. This is required for
        // at-least-once as well as exactly-once: replay can recover a failed cut, but it cannot repair
        // a successful handoff assembled from unrelated source and state frontiers.
        {
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
                    let drain_deadline = tokio::time::Instant::now() + config.drain_ack_timeout;
                    db.validate_source_drain_snapshot(&drain)
                        .map_err(|error| error.to_string())?;
                    controller.publish_checkpoint_drain_transition(Some(transition.clone()));
                    // Wait for every process in the snapshot's frozen boot roster to durably ack the
                    // exact source cut. Target-only joiners intentionally do not acknowledge inputs
                    // they never owned under the predecessor.
                    let local_receipt = match local_drain_participant(&controller, &transition) {
                        Some(participant) => {
                            prepare_and_announce_local_drain(
                                &db,
                                &store,
                                &registry,
                                &controller,
                                &drain,
                                &transition,
                                participant,
                                drain_deadline,
                            )
                            .await
                        }
                        None => Ok(()),
                    };
                    let acked = match local_receipt {
                        Ok(()) => {
                            await_drain_quorum(&controller, &transition, drain_deadline).await
                        }
                        Err(error) => {
                            warn!(%error, version = drain.version, "leader durable drain acknowledgement failed");
                            false
                        }
                    };
                    if !acked {
                        let failure = "drain ack quorum not reached before timeout";
                        if let Err(error) = audit_exact_drain_head(
                            &store,
                            &registry,
                            &drain,
                            &controller,
                            tokio::time::Instant::now() + config.checkpoint_timeout,
                        )
                        .await
                        {
                            return Err(format!(
                                "{failure}; drain is no longer authoritative: {error}"
                            ));
                        }
                        if let Err(abort_error) = finalize_drain_snapshot(
                            &db,
                            &store,
                            &controller,
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
                        &store,
                        &registry,
                        &drain,
                        &controller,
                        tokio::time::Instant::now() + config.checkpoint_timeout,
                    )
                    .await?;
                    // Abort the drain on failure OR timeout (not just Ok(false)) — a bare
                    // `?` here would leave nodes stuck draining.
                    let checkpointed = pre_rotation_checkpoint(&db, config).await;
                    if !matches!(checkpointed, Ok(true)) {
                        let failure = checkpointed.err().unwrap_or_else(|| {
                            "pre-rotation checkpoint failed during drain".into()
                        });
                        if let Err(error) = audit_exact_drain_head(
                            &store,
                            &registry,
                            &drain,
                            &controller,
                            tokio::time::Instant::now() + config.checkpoint_timeout,
                        )
                        .await
                        {
                            return Err(format!(
                                "{failure}; drain is no longer authoritative: {error}"
                            ));
                        }
                        if let Err(abort_error) = finalize_drain_snapshot(
                            &db,
                            &store,
                            &controller,
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
                        &store,
                        &registry,
                        &drain,
                        &controller,
                        tokio::time::Instant::now() + config.checkpoint_timeout,
                    )
                    .await?;
                    return finalize_drain_snapshot(
                        &db,
                        &store,
                        &controller,
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
                        &db,
                        &store,
                        &controller,
                        winner,
                        tokio::time::Instant::now() + config.checkpoint_timeout,
                    )
                    .await?;
                    return Ok(Some(v));
                }
            }
        }
    })
}

#[cfg(test)]
async fn try_rebalance(
    db: &Arc<LaminarDB>,
    controller: &Arc<ClusterController>,
    store: &Arc<AssignmentSnapshotStore>,
    registry: &Arc<VnodeRegistry>,
    live: &[NodeId],
    config: RebalanceConfig,
) -> Result<Option<u64>, String> {
    try_rebalance_owned(
        Arc::clone(db),
        Arc::clone(controller),
        Arc::clone(store),
        Arc::clone(registry),
        live.to_vec(),
        config,
    )
    .await
}

fn audit_assignment_snapshot_authority_owned(
    store: Arc<AssignmentSnapshotStore>,
    controller: Arc<ClusterController>,
    snapshot: AssignmentSnapshot,
) -> futures::future::BoxFuture<'static, Result<(), String>> {
    Box::pin(async move {
        audit_assignment_snapshot_authority(&store, Some(&controller), &snapshot).await
    })
}

fn reconcile_pending_recovery_decision_owned(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    controller: Arc<ClusterController>,
    current: AssignmentSnapshot,
    operation_timeout: Duration,
) -> futures::future::BoxFuture<'static, Result<Option<u64>, String>> {
    Box::pin(async move {
        reconcile_pending_recovery_decision(&db, &store, &controller, &current, operation_timeout)
            .await
    })
}

fn authorize_recovery_successor_owned(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    controller: Arc<ClusterController>,
    current: AssignmentSnapshot,
    proposal: AssignmentSnapshot,
    operation_timeout: Duration,
    reason: String,
) -> futures::future::BoxFuture<'static, Result<Option<u64>, String>> {
    Box::pin(async move {
        authorize_recovery_successor(
            &db,
            &store,
            &controller,
            &current,
            proposal,
            operation_timeout,
            &reason,
        )
        .await
    })
}

async fn predecessor_cut_unavailability(
    controller: &ClusterController,
    current: &AssignmentSnapshot,
    current_owners: &[NodeId],
    live: &[NodeId],
) -> Option<String> {
    let participant_ids: Vec<u64> = current
        .participants
        .iter()
        .map(|participant| participant.node_id)
        .collect();
    match controller
        .recovery_participant_incarnations(&participant_ids)
        .await
    {
        Ok(observed) if observed == current.participants => {}
        Ok(_) => {
            return Some("the predecessor process incarnation roster changed".into());
        }
        Err(error) => {
            return Some(format!(
                "the predecessor process incarnation roster is unavailable: {error}"
            ));
        }
    }

    let members = controller.members_watch().borrow().clone();
    let owners: std::collections::BTreeSet<NodeId> = current_owners.iter().copied().collect();
    for owner in owners {
        if controller.is_unresponsive(owner) {
            return Some(format!(
                "predecessor owner {owner} cannot certify the source cut"
            ));
        }
        if live.contains(&owner) {
            continue;
        }
        let can_drain = members.iter().any(|member| {
            member.id == owner && matches!(member.state, NodeState::Active | NodeState::Draining)
        });
        if !can_drain {
            return Some(format!(
                "predecessor owner {owner} cannot certify the source cut"
            ));
        }
    }
    None
}

fn successor_participant_ids(owners: &[NodeId]) -> Vec<u64> {
    owners
        .iter()
        .map(|owner| owner.0)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn successor_participants(
    owners: &[NodeId],
    available: &[CheckpointParticipant],
) -> Result<Vec<CheckpointParticipant>, String> {
    let participant_ids = successor_participant_ids(owners);
    let participant_set: std::collections::BTreeSet<u64> =
        participant_ids.iter().copied().collect();
    let participants = available
        .iter()
        .copied()
        .filter(|participant| participant_set.contains(&participant.node_id))
        .collect::<Vec<_>>();
    if participants.len() != participant_ids.len() {
        return Err("successor owner roster lost a lease-validated process identity".into());
    }
    Ok(participants)
}

fn retain_recovery_predecessors(
    assignment: &mut [NodeId],
    predecessor: &[CheckpointParticipant],
    successors: &[CheckpointParticipant],
) -> Result<(), String> {
    let successor_processes = successors
        .iter()
        .map(|participant| (participant.node_id, participant.boot_incarnation))
        .collect::<std::collections::BTreeMap<_, _>>();
    let retained = predecessor
        .iter()
        .filter(|participant| {
            successor_processes.get(&participant.node_id) == Some(&participant.boot_incarnation)
        })
        .map(|participant| NodeId(participant.node_id))
        .collect::<std::collections::BTreeSet<_>>();
    let mut counts = assignment.iter().copied().fold(
        std::collections::BTreeMap::<NodeId, usize>::new(),
        |mut counts, owner| {
            *counts.entry(owner).or_default() += 1;
            counts
        },
    );

    for participant in retained.iter().copied() {
        if counts.get(&participant).copied().unwrap_or_default() != 0 {
            continue;
        }
        let mut donor = None;
        for (index, owner) in assignment.iter().copied().enumerate() {
            let owner_count = counts.get(&owner).copied().unwrap_or_default();
            if retained.contains(&owner) && owner_count <= 1 {
                continue;
            }
            if donor.is_none_or(|(_, best_count)| owner_count > best_count) {
                donor = Some((index, owner_count));
            }
        }
        let Some((index, _)) = donor else {
            return Err(format!(
                "recovery placement cannot retain healthy predecessor {participant}"
            ));
        };
        let displaced = assignment[index];
        assignment[index] = participant;
        *counts
            .get_mut(&displaced)
            .expect("assignment owner has a placement count") -= 1;
        *counts.entry(participant).or_default() += 1;
    }
    Ok(())
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
/// reached the global input cut. Durable read errors are retried within the existing deadline;
/// they never become quorum.
async fn await_drain_quorum(
    controller: &Arc<ClusterController>,
    transition: &AssignmentDrainTransition,
    deadline: tokio::time::Instant,
) -> bool {
    tokio::time::timeout_at(deadline, async {
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
    controller: &ClusterController,
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
            .map_err(|error| error.clone())?;
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

/// Settle a durable drain before coordinated recovery freezes a new process-local source cut.
/// Recovery restarts source tasks, so a pre-restart receipt can never authorize a later Release.
pub(crate) async fn settle_source_drain_before_recovery(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    config: RebalanceConfig,
) -> Result<Option<u64>, String> {
    let published_transition = controller.checkpoint_drain_transition();
    let Some(store) = db.assignment_snapshot_store.lock().clone() else {
        return if published_transition.is_some() {
            Err("recovery source-drain settlement has no assignment store".into())
        } else {
            Ok(None)
        };
    };
    let Some(draining) = store.load().await.map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    if !draining.draining {
        return if published_transition.is_some() {
            Err("local source-drain authority outlived its durable draining assignment".into())
        } else {
            Ok(None)
        };
    }
    let transition = draining
        .drain_transition
        .as_ref()
        .ok_or_else(|| "draining assignment has no exact source transition".to_string())?;
    if published_transition.as_ref() != Some(transition) {
        return Err("durable and locally published source-drain transitions differ".into());
    }
    let predecessor = store
        .load_version(transition.predecessor.assignment_version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "source-drain predecessor assignment {} is unavailable during recovery",
                transition.predecessor.assignment_version
            )
        })?;
    finalize_drain_snapshot(
        db,
        &store,
        controller,
        &draining,
        &predecessor,
        AssignmentDrainVerdict::Abort,
        config,
    )
    .await
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
        db.validate_source_drain_snapshot(&snap)
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
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            control,
            recovery,
            assignment_store,
            members_rx,
            boot,
        ));
        controller
            .set_process_lease_deadline(Arc::new(
                laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
            ))
            .unwrap();
        controller
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
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty test authority must grant leadership");
        };
        install_test_leadership(controller, authority, owner, lease)
    }

    async fn install_test_process_authority(
        controller: &Arc<ClusterController>,
        participants: &[CheckpointParticipant],
    ) -> Arc<laminar_core::cluster::control::ProcessLeaseAuthority> {
        let authority = Arc::new(
            laminar_core::cluster::control::ProcessLeaseAuthority::new(
                Arc::new(InMemory::new()),
                Duration::from_millis(5),
            )
            .unwrap(),
        );
        let mut local_lease = None;
        for participant in participants {
            let outcome = authority
                .store_for(NodeId(participant.node_id))
                .try_acquire(participant.boot_incarnation, 0)
                .await
                .unwrap();
            if participant.node_id == controller.instance_id().0 {
                let laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(lease) = outcome
                else {
                    panic!("local test process lease must be acquired");
                };
                local_lease = Some(lease);
            }
        }
        if controller.process_lease_deadline().is_none() {
            controller
                .set_process_lease_deadline(Arc::new(
                    laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(
                        60,
                    )),
                ))
                .unwrap();
        }
        controller
            .set_process_lease_authority(Arc::clone(&authority))
            .unwrap();
        if let Some(lease) = local_lease {
            controller
                .publish_leased_recovery_incarnation(&lease)
                .await
                .unwrap();
        }
        authority
    }

    fn install_test_leadership(
        controller: &Arc<ClusterController>,
        authority: Arc<LeaderLeaseStore>,
        owner: laminar_core::cluster::control::LeaderLeaseOwner,
        lease: laminar_core::cluster::control::LeaderLease,
    ) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
        use laminar_core::cluster::control::LeaseDeadline;

        let (lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
        if controller.process_lease_deadline().is_none() {
            controller
                .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(
                    60,
                ))))
                .unwrap();
        }
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

    fn member(
        id: NodeId,
        state: laminar_core::cluster::discovery::NodeState,
    ) -> laminar_core::cluster::discovery::NodeInfo {
        laminar_core::cluster::discovery::NodeInfo {
            id,
            name: format!("node-{}", id.0),
            rpc_address: String::new(),
            raft_address: String::new(),
            state,
            metadata: laminar_core::cluster::discovery::NodeMetadata::default(),
            last_heartbeat_ms: 0,
        }
    }

    async fn predecessor_failure_fixture(
        self_process: CheckpointParticipant,
        failed_process: CheckpointParticipant,
        owners: Vec<NodeId>,
        additional_successors: Vec<CheckpointParticipant>,
    ) -> (
        Arc<LaminarDB>,
        Arc<ClusterController>,
        Arc<AssignmentSnapshotStore>,
        Arc<VnodeRegistry>,
        AssignmentSnapshot,
        Arc<laminar_core::cluster::control::ProcessLeaseAuthority>,
    ) {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeState;

        let self_id = NodeId(self_process.node_id);
        let failed_id = NodeId(failed_process.node_id);
        let mut current_processes = vec![self_process, failed_process];
        current_processes.sort_unstable_by_key(|participant| participant.node_id);
        let current = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&owners),
                current_processes,
            )
            .unwrap();
        let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared_store)));
        durable.save_if_absent(&current).await.unwrap();

        let kv = Arc::new(InMemoryKv::new(self_id));
        let mut successor_processes = current.participants.clone();
        successor_processes.extend(additional_successors);
        successor_processes.sort_unstable_by_key(|participant| participant.node_id);
        successor_processes.dedup_by_key(|participant| participant.node_id);
        for participant in &successor_processes {
            if participant.node_id != self_id.0 {
                kv.seed(
                    NodeId(participant.node_id),
                    "control:recovery-incarnation",
                    participant.boot_incarnation.to_string(),
                );
            }
        }
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let members = successor_processes
            .iter()
            .filter(|participant| participant.node_id != self_id.0)
            .map(|participant| {
                let id = NodeId(participant.node_id);
                member(
                    id,
                    if id == failed_id {
                        NodeState::Left
                    } else {
                        NodeState::Active
                    },
                )
            })
            .collect();
        let (_members_tx, members_rx) = tokio::sync::watch::channel(members);
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            self_process.boot_incarnation,
        ));
        let process_authority = Arc::new(
            laminar_core::cluster::control::ProcessLeaseAuthority::new(
                Arc::clone(&shared_store),
                Duration::from_millis(50),
            )
            .unwrap(),
        );
        for participant in &successor_processes {
            assert!(matches!(
                process_authority
                    .store_for(NodeId(participant.node_id))
                    .try_acquire(participant.boot_incarnation, 0)
                    .await
                    .unwrap(),
                laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(_)
            ));
        }
        let local_process_lease = process_authority
            .store_for(self_id)
            .load()
            .await
            .unwrap()
            .expect("local test process lease must be durable");
        controller
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared_store), 10_000));
        let leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
            node: self_id,
            boot: self_process.boot_incarnation,
            process_term: 1,
        };
        let laminar_core::cluster::control::LeaseOutcome::Acquired(leader_lease) = leader_authority
            .begin_new_term(&leader_owner, 0)
            .await
            .unwrap()
        else {
            panic!("test leader must acquire the empty authority");
        };
        let _leader_lease =
            install_test_leadership(&controller, leader_authority, leader_owner, leader_lease);
        controller
            .publish_leased_recovery_incarnation(&local_process_lease)
            .await
            .unwrap();

        let vnode_count = u32::try_from(owners.len()).unwrap();
        let registry = Arc::new(VnodeRegistry::new_unassigned(vnode_count));
        registry.set_assignment_and_version(owners.into(), current.version);
        let shuffle_receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                self_id.0,
                "127.0.0.1:0".parse().unwrap(),
                self_process.boot_incarnation,
            )
            .await
            .unwrap(),
        );
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .shuffle_sender(Arc::new(laminar_core::shuffle::ShuffleSender::new(
                self_id.0,
                self_process.boot_incarnation,
            )))
            .shuffle_receiver(shuffle_receiver)
            .state_backend(Arc::new(InProcessBackend::new(vnode_count)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();
        (
            db,
            controller,
            durable,
            registry,
            current,
            process_authority,
        )
    }

    async fn dead_predecessor_fixture() -> (
        Arc<LaminarDB>,
        Arc<ClusterController>,
        Arc<AssignmentSnapshotStore>,
        Arc<VnodeRegistry>,
        AssignmentSnapshot,
        Arc<laminar_core::cluster::control::ProcessLeaseAuthority>,
    ) {
        predecessor_failure_fixture(
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(11),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
            },
            vec![NodeId(1), NodeId(2)],
            Vec::new(),
        )
        .await
    }

    #[test]
    fn successor_checkpoint_roster_contains_only_successor_owners() {
        let owners = [NodeId(3), NodeId(1), NodeId(3), NodeId(1)];
        assert_eq!(successor_participant_ids(&owners), [1, 3]);
        assert!(!successor_participant_ids(&owners).contains(&2));
    }

    #[tokio::test]
    async fn failure_recovery_retains_a_healthy_predecessor_with_no_rendezvous_share() {
        let healthy = CheckpointParticipant {
            node_id: 3,
            boot_incarnation: uuid::Uuid::from_u128(33),
        };
        let failed = CheckpointParticipant {
            node_id: 9,
            boot_incarnation: uuid::Uuid::from_u128(99),
        };
        let successor_five = CheckpointParticipant {
            node_id: 5,
            boot_incarnation: uuid::Uuid::from_u128(55),
        };
        let successor_seven = CheckpointParticipant {
            node_id: 7,
            boot_incarnation: uuid::Uuid::from_u128(77),
        };
        assert_eq!(
            rendezvous_assignment(2, &[NodeId(3), NodeId(5), NodeId(7)]).as_ref(),
            &[NodeId(5), NodeId(7)]
        );
        let (db, controller, durable, registry, current, _process_authority) =
            predecessor_failure_fixture(
                healthy,
                failed,
                vec![NodeId(3), NodeId(9)],
                vec![successor_five, successor_seven],
            )
            .await;
        controller.note_unresponsive(&[NodeId(9)]);

        let version = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[NodeId(3), NodeId(5), NodeId(7), NodeId(9)],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect("recovery must retain the healthy predecessor without restoring local state");
        assert_eq!(version, Some(current.version + 1));
        let recovery = durable.load().await.unwrap().unwrap();
        assert_eq!(recovery.to_vnode_vec(2).unwrap(), [NodeId(3), NodeId(7)]);
        assert_eq!(
            recovery
                .assignment_fence()
                .unwrap()
                .participant_incarnation(healthy.node_id),
            Some(healthy.boot_incarnation)
        );
        let decision = controller
            .checkpoint_authority()
            .unwrap()
            .assignment_recovery_decision(recovery.version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.removed_process_fences.len(), 1);
        assert_eq!(
            decision.removed_process_fences[0].predecessor.node,
            NodeId(failed.node_id)
        );
        assert!(controller
            .verify_current_process_incarnation(
                healthy,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap());
        assert_eq!(registry.assignment_version(), recovery.version);
    }

    #[tokio::test]
    async fn recent_quorum_miss_bypasses_stale_active_membership() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeState;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let peer_id = NodeId(2);
        let self_boot = Uuid::from_u128(11);
        let peer_boot = Uuid::from_u128(22);
        let owners = [self_id, peer_id];
        let current = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&owners),
                vec![
                    CheckpointParticipant {
                        node_id: self_id.0,
                        boot_incarnation: self_boot,
                    },
                    CheckpointParticipant {
                        node_id: peer_id.0,
                        boot_incarnation: peer_boot,
                    },
                ],
            )
            .unwrap();
        let kv = Arc::new(InMemoryKv::new(self_id));
        kv.seed(
            peer_id,
            "control:recovery-incarnation",
            peer_boot.to_string(),
        );
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) =
            tokio::sync::watch::channel(vec![member(peer_id, NodeState::Active)]);
        let controller = ClusterController::new_with_recovery_incarnation(
            self_id, control, recovery, None, members_rx, self_boot,
        );
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        controller.note_unresponsive(&[peer_id]);

        let reason = predecessor_cut_unavailability(&controller, &current, &owners, &owners).await;

        assert_eq!(
            reason.as_deref(),
            Some("predecessor owner node-2 cannot certify the source cut")
        );
    }

    #[tokio::test]
    async fn at_least_once_live_rotation_uses_the_global_drain_protocol() {
        use laminar_connectors::connector::DeliveryGuarantee;
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeState;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let peer_id = NodeId(2);
        let self_boot = Uuid::from_u128(11);
        let peer_boot = Uuid::from_u128(22);
        let vnode_count = 32;
        let durable = Arc::new(store());
        let current = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&vec![self_id; vnode_count as usize]),
                vec![CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: self_boot,
                }],
            )
            .unwrap();
        durable.save_if_absent(&current).await.unwrap();

        let kv = Arc::new(InMemoryKv::new(self_id));
        kv.seed(
            peer_id,
            "control:recovery-incarnation",
            peer_boot.to_string(),
        );
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv;
        let (_members_tx, members_rx) =
            tokio::sync::watch::channel(vec![member(peer_id, NodeState::Active)]);
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control,
            recovery,
            Some(Arc::clone(&durable)),
            members_rx,
            self_boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let _leader_lease = grant_test_leadership(&controller).await;
        install_test_process_authority(
            &controller,
            &[
                current.participants[0],
                CheckpointParticipant {
                    node_id: peer_id.0,
                    boot_incarnation: peer_boot,
                },
            ],
        )
        .await;

        let desired = rendezvous_assignment(vnode_count, &[self_id, peer_id]);
        assert!(desired.contains(&peer_id));
        let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
        let db = LaminarDB::builder()
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(vnode_count)))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();
        let mut config = RebalanceConfig::test_defaults();
        config.checkpoint_timeout = Duration::from_secs(2);
        config.drain_ack_timeout = Duration::from_secs(1);

        let error = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id, peer_id],
            config,
        )
        .await
        .expect_err("the unstarted test pipeline cannot seal the forced checkpoint");
        assert!(error.contains("checkpoint"), "{error}");

        let transition = durable
            .load_drain_transition(current.version + 1)
            .await
            .unwrap()
            .expect("live at-least-once rotation must publish a drain transition");
        assert!(transition
            .target
            .matches_owner_map(&desired.iter().map(|owner| owner.0).collect::<Vec<_>>()));
        let materialized = durable.load().await.unwrap().unwrap();
        assert_eq!(materialized.version, current.version + 1);
        assert_eq!(materialized.vnodes, current.vnodes);
        assert!(!materialized.draining);
        let decision = controller
            .checkpoint_authority()
            .unwrap()
            .assignment_drain_decision(materialized.version)
            .await
            .unwrap()
            .expect("failed checkpoint must durably abort the source drain");
        assert_eq!(decision.verdict, AssignmentDrainVerdict::Abort);
    }

    #[tokio::test]
    async fn dead_predecessor_publishes_an_authorized_recovery_generation() {
        let self_id = NodeId(1);
        let (db, controller, durable, registry, current, _process_authority) =
            dead_predecessor_fixture().await;
        controller.note_unresponsive(&[NodeId(2)]);

        let error = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id, NodeId(2)],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect_err("the unstarted successor cannot restore the acquired state");
        assert!(error.contains("cannot acquire 1 vnodes"), "{error}");
        let successor = durable.load().await.unwrap().unwrap();
        assert_eq!(successor.version, current.version + 1);
        assert!(!successor.draining);
        assert_eq!(successor.participants.len(), 1);
        assert_eq!(successor.participants[0].node_id, self_id.0);
        assert!(successor
            .to_vnode_vec(2)
            .unwrap()
            .iter()
            .all(|owner| *owner == self_id));
        let decision = controller
            .checkpoint_authority()
            .unwrap()
            .assignment_recovery_decision(successor.version)
            .await
            .unwrap()
            .expect("successor must have one immutable recovery decision");
        assert_eq!(decision.predecessor, current.assignment_fence().unwrap());
        assert_eq!(decision.target, successor.assignment_fence().unwrap());
        assert_eq!(decision.removed_process_fences.len(), 1);
        assert_eq!(
            decision.removed_process_fences[0].predecessor.node,
            NodeId(2)
        );
        assert_eq!(
            durable
                .load_recovery_proposal(&decision.proposal)
                .await
                .unwrap(),
            successor
        );
        assert!(controller
            .verify_process_lease_fence(
                &decision.removed_process_fences[0],
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap());
        assert_eq!(registry.assignment_version(), current.version);
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
        assert!(controller
            .read_fault_reports()
            .await
            .unwrap()
            .iter()
            .any(|(node, sequence)| *node == controller.instance_id() && *sequence != 0));
    }

    #[tokio::test]
    async fn renewing_predecessor_cannot_be_removed_by_failure_recovery() {
        let self_id = NodeId(1);
        let (db, controller, durable, registry, current, process_authority) =
            dead_predecessor_fixture().await;
        let predecessor = current.participants[1];
        let predecessor_store = process_authority.store_for(NodeId(predecessor.node_id));
        let keep_renewing = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let renewal_flag = Arc::clone(&keep_renewing);
        let first_renewal = Arc::new(Notify::new());
        let renewal_started = Arc::clone(&first_renewal);
        let renewals = tokio::spawn(async move {
            let mut timestamp = 1;
            while renewal_flag.load(std::sync::atomic::Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
                predecessor_store
                    .try_acquire(predecessor.boot_incarnation, timestamp)
                    .await
                    .unwrap();
                renewal_started.notify_one();
                timestamp += 1;
            }
        });
        tokio::time::timeout(Duration::from_secs(1), first_renewal.notified())
            .await
            .expect("predecessor renewal task did not start");

        let error = try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .expect_err("a renewing process term must win against recovery fencing");
        keep_renewing.store(false, std::sync::atomic::Ordering::Release);
        renewals.await.unwrap();
        assert!(error.contains("renewed"), "{error}");
        assert_eq!(durable.load().await.unwrap().unwrap(), current);
        assert!(controller
            .checkpoint_authority()
            .unwrap()
            .assignment_recovery_decision(current.version + 1)
            .await
            .unwrap()
            .is_none());
        assert!(controller
            .read_local_fault_report()
            .await
            .unwrap()
            .is_none());
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
            vec![replacement],
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
                partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
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
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
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
        let controller = test_cluster_controller(NodeId(1), local_boot, None);
        let process_deadline = controller
            .process_lease_deadline()
            .expect("test controller process lease deadline");
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), local_boot)
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, local_boot));
        receiver
            .install_process_lease_deadline(Arc::clone(&process_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(process_deadline)
            .unwrap();
        receiver
            .install_assignment_fence(&assignment, &[1, 2])
            .unwrap();
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
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .shuffle_sender(Arc::clone(&sender))
            .shuffle_receiver(receiver)
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

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let closing = {
            let db = Arc::clone(&db);
            tokio::spawn(async move { close_local_assignment_authority(&db, None, deadline).await })
        };
        tokio::time::timeout_at(deadline, async {
            while sender.assignment_version() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("authority closure did not cancel shuffle admission");
        let error = tokio::time::timeout_at(deadline, blocked_cycle)
            .await
            .expect("cancelled shuffle cycle did not exit")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
        tokio::time::timeout_at(deadline, closing)
            .await
            .expect("authority closure deadlocked behind a shuffle-held read fence")
            .unwrap()
            .expect("authority closure exceeded its deadline");
        assert!(db.cluster_intake_fenced());
        peer.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_suspension_reasserts_closure_after_serialization_race() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use uuid::Uuid;

        let local_boot = Uuid::from_u128(11);
        let participants = vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: local_boot,
        }];
        let assignment = CheckpointAssignmentFence::from_owner_map(1, &[1], participants).unwrap();
        let controller = test_cluster_controller(NodeId(1), local_boot, None);
        let process_deadline = controller
            .process_lease_deadline()
            .expect("test controller process lease deadline");
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), local_boot)
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, local_boot));
        receiver
            .install_process_lease_deadline(Arc::clone(&process_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(process_deadline)
            .unwrap();
        receiver
            .install_assignment_fence(&assignment, &[1])
            .unwrap();
        sender.install_assignment_fence(&assignment, &[1]).unwrap();

        let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
        let db = LaminarDB::builder()
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .shuffle_sender(Arc::clone(&sender))
            .shuffle_receiver(receiver)
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
            authority.begin_new_term(&owner, 0).await.unwrap()
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
    async fn bare_recovery_successor_without_an_authority_decision_is_rejected() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&object_store)));
        let first = AssignmentSnapshot::empty()
            .next_for_participants(
                BTreeMap::from([(0, NodeId(1))]),
                vec![CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(11),
                }],
            )
            .unwrap();
        let unauthorized = first
            .next_for_participants(
                BTreeMap::from([(0, NodeId(2))]),
                vec![CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                }],
            )
            .unwrap();
        durable.save_if_absent(&first).await.unwrap();
        assert!(matches!(
            durable
                .save_if_version(&unauthorized, first.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));
        let controller = test_cluster_controller(
            NodeId(1),
            uuid::Uuid::from_u128(11),
            Some(Arc::clone(&durable)),
        );
        controller.set_leader_lease_store(Arc::new(LeaderLeaseStore::new(object_store, 10_000)));
        let error = audit_assignment_snapshot_authority(&durable, Some(&controller), &unauthorized)
            .await
            .expect_err("a bare stable successor must never pass authority audit");
        assert!(
            error.contains("no drain transition or recovery authority decision"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn startup_rejects_drain_that_does_not_bind_retained_predecessor() {
        let durable = Arc::new(store());
        let retained = snapshot(BTreeMap::from([(0, NodeId(1))]));
        durable.save_if_absent(&retained).await.unwrap();

        let different_predecessor = snapshot(BTreeMap::from([(0, NodeId(2))]));
        let forged_head = draining_snapshot(
            &different_predecessor,
            BTreeMap::from([(0, NodeId(3))]),
            vec![CheckpointParticipant {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(3),
            }],
        );
        assert!(matches!(
            durable
                .save_if_version(&forged_head, retained.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));

        let head = durable.load().await.unwrap().unwrap();
        let error = startup_committed_assignment(&durable, None, head)
            .await
            .expect_err("startup must reject a transition over another predecessor");
        assert!(
            error.contains("does not bind retained predecessor"),
            "{error}"
        );
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
        controller
            .set_process_lease_deadline(Arc::new(
                laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
            ))
            .unwrap();
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let _process_authority = install_test_process_authority(&controller, &[process]).await;
        let _leader_lease = grant_test_leadership(&controller).await;

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
        let shutdown = CancellationToken::new();
        let mut config = RebalanceConfig::test_defaults();
        config.watcher_poll = Duration::from_millis(10);
        config.checkpoint_timeout = Duration::from_millis(100);
        let watcher = spawn_snapshot_watcher(
            Arc::clone(&db),
            Arc::clone(&durable),
            Arc::clone(&registry),
            shutdown.clone(),
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
        shutdown.cancel();
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
    async fn restarted_process_publishes_an_authorized_recovery_generation() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let old_process = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: Uuid::from_u128(11),
        };
        let new_boot = Uuid::from_u128(111);
        let vnodes = BTreeMap::from([(0, self_id), (1, self_id)]);
        let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared_store)));
        let first = AssignmentSnapshot::empty()
            .next_for_participants(vnodes.clone(), vec![old_process])
            .unwrap();
        durable.save_if_absent(&first).await.unwrap();

        let control = Arc::new(InMemoryKv::new(self_id));
        let control_kv: Arc<dyn ClusterKv> = control.clone();
        let recovery_kv: Arc<dyn ClusterKv> = control;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            self_id,
            control_kv,
            recovery_kv,
            Some(Arc::clone(&durable)),
            members_rx,
            new_boot,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let process_authority = Arc::new(
            laminar_core::cluster::control::ProcessLeaseAuthority::new(
                Arc::clone(&shared_store),
                Duration::from_millis(1),
            )
            .unwrap(),
        );
        let process_store = process_authority.store_for(self_id);
        let laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(old_lease) =
            process_store
                .try_acquire(old_process.boot_incarnation, 0)
                .await
                .unwrap()
        else {
            panic!("old process must seed its lease");
        };
        let observation = process_store.observe_rival(&old_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(new_lease) =
            process_store
                .try_takeover(new_boot, &observation, 1)
                .await
                .unwrap()
        else {
            panic!("replacement process must take over");
        };
        controller
            .set_process_lease_authority(process_authority)
            .unwrap();
        controller
            .set_process_lease_deadline(Arc::new(
                laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
            ))
            .unwrap();
        controller
            .publish_leased_recovery_incarnation(&new_lease)
            .await
            .unwrap();
        controller.set_active(true);
        let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared_store), 10_000));
        let leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
            node: self_id,
            boot: new_boot,
            process_term: new_lease.term,
        };
        let laminar_core::cluster::control::LeaseOutcome::Acquired(leader_lease) = leader_authority
            .begin_new_term(&leader_owner, 0)
            .await
            .unwrap()
        else {
            panic!("replacement process must acquire leadership");
        };
        let _leader_lease =
            install_test_leadership(&controller, leader_authority, leader_owner, leader_lease);

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
        db.validate_source_drain_snapshot(&drain).unwrap();

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
        assert!(db.cluster_intake_fenced());
    }

    #[tokio::test]
    async fn recovery_settles_drain_before_reusing_process_local_source_cuts() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let boot = Uuid::from_u128(11);
        let participant = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: boot,
        };
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, self_id)]), vec![participant])
            .unwrap();
        let durable = Arc::new(store());
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
        let _leader_lease = grant_test_leadership(&controller).await;
        let drain = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: Uuid::from_u128(22),
                }],
                controller.capture_leader_proof().unwrap(),
            )
            .unwrap();
        assert!(matches!(
            durable
                .save_if_version(&drain, committed.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));
        controller.publish_checkpoint_drain_transition(drain.drain_transition.clone());

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
            settle_source_drain_before_recovery(
                &db,
                &controller,
                RebalanceConfig::test_defaults(),
            )
            .await
            .unwrap(),
            Some(drain.version)
        );
        let settled = durable.load().await.unwrap().unwrap();
        assert!(!settled.draining);
        assert_eq!(settled.version, drain.version);
        assert_eq!(settled.vnodes, committed.vnodes);
        assert_eq!(registry.assignment_version(), drain.version);
        assert!(controller.checkpoint_drain_transition().is_none());
        let decision = controller
            .checkpoint_authority()
            .unwrap()
            .assignment_drain_decision(drain.version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.verdict, AssignmentDrainVerdict::Abort);
    }

    #[tokio::test]
    async fn recovery_release_reapplies_a_committed_drain_to_replacement_sources() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeInfo;
        use uuid::Uuid;

        let self_id = NodeId(1);
        let boot = Uuid::from_u128(11);
        let participant = CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: boot,
        };
        let owners = BTreeMap::from([(0, self_id)]);
        let durable = Arc::new(store());
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(owners.clone(), vec![participant])
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
        let _leader_lease = grant_test_leadership(&controller).await;
        let draining = committed
            .next_draining(
                owners,
                vec![participant],
                controller.capture_leader_proof().unwrap(),
            )
            .unwrap();
        assert!(matches!(
            durable
                .save_if_version(&draining, committed.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));

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
            finalize_drain_snapshot(
                &db,
                &durable,
                &controller,
                &draining,
                &committed,
                AssignmentDrainVerdict::Commit,
                RebalanceConfig::test_defaults(),
            )
            .await
            .unwrap(),
            Some(draining.version)
        );
        let terminal = durable.load().await.unwrap().unwrap();
        let terminal_fence = terminal.assignment_fence().unwrap();
        let transition = draining.drain_transition.clone().unwrap();
        let task =
            crate::pipeline::streaming_coordinator::install_replacement_source_drain_task_for_test(
                &db.owned_source_tasks,
                "replacement-source",
            );

        controller.publish_checkpoint_drain_transition(Some(transition.clone()));
        controller.set_recovering(true);
        db.set_source_gate(true);
        let resolution = SourceDrainResolution {
            round: transition.id(),
            outcome: SourceDrainOutcome::Commit,
        };
        let error = settle_source_drain_before_recovery_release(
            &db,
            &controller,
            &committed.assignment_fence().unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .expect_err("a predecessor assignment must not authorize terminal source resolution");
        assert!(error.contains("assignment 2 changed"), "{error}");
        assert_eq!(
            controller.checkpoint_drain_transition(),
            Some(transition.clone())
        );
        assert!(
            !crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
                &db.owned_source_tasks,
                resolution,
            )
            .unwrap()
        );
        assert_eq!(
            settle_source_drain_before_recovery_release(
                &db,
                &controller,
                &terminal_fence,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap(),
            Some(terminal.version)
        );
        assert!(
            crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
                &db.owned_source_tasks,
                resolution,
            )
            .unwrap()
        );
        assert!(controller.checkpoint_drain_transition().is_none());

        task.request_shutdown();
        assert!(
            task.wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
        );
        db.owned_source_tasks
            .lock()
            .retain(|source| !source.is_finished());
        let replacement =
            crate::pipeline::streaming_coordinator::install_replacement_source_drain_task_for_test(
                &db.owned_source_tasks,
                "next-replacement-source",
            );
        assert!(
            !crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
                &db.owned_source_tasks,
                resolution,
            )
            .unwrap()
        );
        assert!(controller.checkpoint_drain_transition().is_none());
        assert_eq!(
            settle_source_drain_before_recovery_release(
                &db,
                &controller,
                &terminal_fence,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap(),
            Some(terminal.version),
            "a replacement generation must reconcile retained terminal authority even after the process-local marker was cleared"
        );
        assert!(
            crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
                &db.owned_source_tasks,
                resolution,
            )
            .unwrap()
        );

        replacement.request_shutdown();
        assert!(
            replacement
                .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
        );
        controller.publish_checkpoint_drain_transition(Some(transition.clone()));
        let error = settle_source_drain_before_recovery_release(
            &db,
            &controller,
            &terminal_fence,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .expect_err("a finished replacement source must block recovery Release");
        assert!(error.contains("exited before committing drain"), "{error}");
        assert_eq!(controller.checkpoint_drain_transition(), Some(transition));
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
        let LeaseOutcome::Acquired(old_lease) =
            authority.begin_new_term(&old_owner, 0).await.unwrap()
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
        let LeaseOutcome::Acquired(old_lease) =
            authority.begin_new_term(&old_owner, 0).await.unwrap()
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
