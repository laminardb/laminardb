//! Dynamic vnode rebalance control plane.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)] // cold path

use std::sync::Arc;
use std::time::Duration;

use laminar_connectors::connector::{SourceDrainOutcome, SourceDrainResolution};
use laminar_core::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CommittedCheckpointRef,
};
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

use crate::db::LaminarDB;
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
    suspend_local_assignment_authority_locked(db, controller, deadline).await
}

/// Suspend authority after the caller has serialized assignment adoption.
async fn suspend_local_assignment_authority_locked(
    db: &Arc<LaminarDB>,
    controller: Option<&ClusterController>,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    // A watcher that owned the adoption lock before the first cancellation could have republished
    // and resumed. Reassert the full suspension while serialized, before draining execution.
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

/// Recovery heads must not suspend predecessor assignment authority while the graph still owns a
/// pending vnode transition. Holding the adoption lock makes the preflight stable against both
/// assignment adoption and startup staging; the graph may only finish (not create) that work.
async fn try_suspend_recovery_assignment_authority(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    deadline: tokio::time::Instant,
) -> Result<bool, String> {
    let _adoption = tokio::time::timeout_at(deadline, db.assignment_adoption_lock.lock())
        .await
        .map_err(|_| "timed out serializing recovery assignment suspension".to_string())?;
    if db.has_unapplied_vnode_transition() {
        return Ok(false);
    }
    suspend_local_assignment_authority_locked(db, Some(controller), deadline).await?;
    Ok(true)
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
///
/// # Panics
///
/// The spawned watcher panics if an already-validated draining snapshot lacks its transition.
struct SnapshotWatcher {
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: CancellationToken,
    config: RebalanceConfig,
    controller: Option<Arc<ClusterController>>,
    ticker: tokio::time::Interval,
    metrics_version: u64,
    last_drained: u64,
    durable_snapshot: Option<AssignmentSnapshot>,
    durable_drain_transition: Option<AssignmentDrainTransition>,
    active_local_drain: Option<AssignmentDrainTransition>,
    terminal_resolution_hold: Option<(AssignmentDrainId, u64)>,
    installed_fence: Option<(u64, [u8; 32])>,
    installed_authority_revision: u64,
    assignment_authority_dirty: bool,
}

impl SnapshotWatcher {
    fn new(
        db: Arc<LaminarDB>,
        store: Arc<AssignmentSnapshotStore>,
        registry: Arc<VnodeRegistry>,
        shutdown: CancellationToken,
        config: RebalanceConfig,
        controller: Option<Arc<ClusterController>>,
    ) -> Self {
        let mut ticker = tokio::time::interval(config.watcher_poll);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            db,
            store,
            registry,
            shutdown,
            config,
            controller,
            ticker,
            metrics_version: 0,
            last_drained: 0,
            durable_snapshot: None,
            durable_drain_transition: None,
            active_local_drain: None,
            terminal_resolution_hold: None,
            installed_fence: None,
            installed_authority_revision: 0,
            assignment_authority_dirty: false,
        }
    }

    async fn ensure_current_assignment_authority_cached(
        &mut self,
        audited_successor: &AuditedAssignmentAuthority,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let assignment = self.registry.versioned_snapshot();
        let version = assignment.version();
        let expected_predecessor = audited_successor.retained_predecessor(version)?;
        let cache_is_exact = self.durable_snapshot.as_ref().is_some_and(|snapshot| {
            !snapshot.draining
                && snapshot.version == version
                && snapshot.has_canonical_participants()
                && snapshot
                    .assignment_fence()
                    .is_ok_and(|fence| &fence == expected_predecessor)
                && snapshot
                    .to_vnode_vec(self.registry.vnode_count())
                    .is_ok_and(|owners| owners.as_slice() == assignment.owners())
        });
        if cache_is_exact {
            return Ok(());
        }

        let snapshot = tokio::time::timeout_at(deadline, self.store.load_version(version))
            .await
            .map_err(|_| format!("assignment {version} history read timed out"))?
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("assignment {version} is absent from durable history"))?;
        if snapshot.draining
            || !snapshot.has_canonical_participants()
            || snapshot
                .to_vnode_vec(self.registry.vnode_count())
                .map_err(|error| error.to_string())?
                .as_slice()
                != assignment.owners()
        {
            return Err(format!(
                "assignment {version} durable history does not match the current registry"
            ));
        }
        if snapshot
            .assignment_fence()
            .map_err(|error| error.to_string())?
            != *expected_predecessor
        {
            return Err(format!(
                "assignment {version} durable history is not the audited successor's exact predecessor"
            ));
        }
        self.durable_drain_transition = None;
        self.durable_snapshot = Some(snapshot);
        Ok(())
    }

    async fn publish_authority(
        &mut self,
        mut authority_revision: u64,
        head_deadline: tokio::time::Instant,
    ) {
        let current_authority_revision = self
            .db
            .assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire);
        if authority_revision != current_authority_revision {
            // The durable head used above predates an authority closure by another adoption.
            // Keep that closure in force and re-read the head on the next tick.
            self.db.set_source_gate(true);
            if let Some(ref c) = self.controller {
                c.publish_checkpoint_drain_transition(None);
                c.publish_checkpoint_assignment_fence(None);
            }
            self.assignment_authority_dirty = true;
            return;
        }
        if self.installed_fence.is_some()
            && self.installed_authority_revision != current_authority_revision
        {
            self.assignment_authority_dirty = true;
        }

        // Serialize the report with assignment adoption and boot staging. A transition publishes
        // its false report under this same lock before exposing pending state, so a
        // delayed watcher can never overwrite that withdrawal with stale readiness.
        let Ok(adoption_guard) =
            tokio::time::timeout_at(head_deadline, self.db.assignment_adoption_lock.lock()).await
        else {
            self.assignment_authority_dirty = true;
            warn!(
                "vnode-state readiness publication timed out waiting for assignment serialization"
            );
            return;
        };
        let assignment = self.registry.versioned_snapshot();
        let version = assignment.version();
        if let Some(ref c) = self.controller {
            let participant = CheckpointParticipant {
                node_id: c.instance_id().0,
                boot_incarnation: c.recovery_incarnation(),
            };
            let exact_assignment_fence = self
                .durable_snapshot
                .as_ref()
                .filter(|snapshot| {
                    !snapshot.draining
                        && snapshot.version == version
                        && snapshot.has_canonical_participants()
                        && snapshot
                            .to_vnode_vec(self.registry.vnode_count())
                            .is_ok_and(|owners| owners.as_slice() == assignment.owners())
                })
                .and_then(|snapshot| snapshot.assignment_fence().ok());
            let local_is_participant = exact_assignment_fence.as_ref().is_some_and(|fence| {
                fence.participant_incarnation(participant.node_id)
                    == Some(participant.boot_incarnation)
            });
            if let Some(exact_assignment_fence) =
                exact_assignment_fence.filter(|_| local_is_participant)
            {
                let vnode_state_ready = match tokio::time::timeout_at(
                    head_deadline,
                    self.db
                        .local_vnode_state_is_ready(&self.registry, &exact_assignment_fence),
                )
                .await
                {
                    Ok(Ok(ready)) => ready,
                    Ok(Err(error)) => {
                        warn!(%error, version, "could not determine local vnode-state readiness");
                        drop(adoption_guard);
                        return;
                    }
                    Err(_) => {
                        warn!(
                            version,
                            "local vnode-state readiness exceeded the head deadline"
                        );
                        drop(adoption_guard);
                        return;
                    }
                };
                // Do not suppress same-value writes here. Startup, assignment adoption, and
                // recovery staging also publish this durable slot directly; a watcher-local cache
                // cannot observe those writes and could otherwise strand a newer false report.
                let publication =
                    self.db
                        .publish_local_vnode_state_report(c, &assignment, vnode_state_ready);
                match tokio::time::timeout_at(head_deadline, publication).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        warn!(%error, version, "assignment vnode-state report publication failed; retrying");
                        drop(adoption_guard);
                        return;
                    }
                    Err(_) => {
                        warn!(
                            version,
                            "assignment vnode-state report publication exceeded the head deadline"
                        );
                        drop(adoption_guard);
                        return;
                    }
                }
            }
        }
        drop(adoption_guard);
        if version != self.metrics_version {
            self.metrics_version = version;
            if let (Some(c), Some(metrics)) = (self.controller.as_ref(), self.db.engine_metrics()) {
                let nodes = c.assignable_with_locality();
                publish_placement_metrics(
                    &metrics,
                    &self.registry,
                    &nodes,
                    self.config.placement_isolation_tier,
                );
            }
        }

        // Publish a version-bound, owner-complete fence off the hot path on every node.
        // The adoption lock and authority revision reject a proof computed from a head that
        // another task fenced while this watcher was awaiting durable I/O.
        if let Some(ref c) = self.controller {
            let fence = match self.durable_snapshot.as_ref() {
                Some(snapshot)
                    if !snapshot.draining
                        && snapshot.version == version
                        && snapshot.has_canonical_participants()
                        && snapshot
                            .to_vnode_vec(self.registry.vnode_count())
                            .is_ok_and(|owners| owners.as_slice() == assignment.owners()) =>
                {
                    if let Ok(fence) = tokio::time::timeout_at(
                        head_deadline,
                        compute_checkpoint_assignment_fence(
                            c,
                            &self.registry,
                            &snapshot.participants,
                        ),
                    )
                    .await
                    {
                        fence.filter(|fence| fence.participants == snapshot.participants)
                    } else {
                        warn!(
                            version,
                            "assignment fence computation exceeded the head deadline"
                        );
                        None
                    }
                }
                _ => None,
            };
            if authority_revision
                != self
                    .db
                    .assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                self.db.set_source_gate(true);
                c.publish_checkpoint_drain_transition(None);
                c.publish_checkpoint_assignment_fence(None);
                self.assignment_authority_dirty = true;
                return;
            }
            let drain_transition = self
                .durable_drain_transition
                .as_ref()
                .filter(|transition| fence.as_ref() == Some(&transition.predecessor))
                .cloned();
            match fence {
                Some(fence) => {
                    let identity = (fence.assignment_version, fence.digest());
                    let local_has_authority = fence.participant_incarnation(c.instance_id().0)
                        == Some(c.recovery_incarnation());
                    let published_fence = c.checkpoint_assignment_fence(fence.assignment_version);
                    let needs_activation = self.assignment_authority_dirty
                        || self.installed_fence != Some(identity)
                        || published_fence.as_ref() != Some(&fence)
                        || c.checkpoint_drain_transition() != drain_transition
                        || (local_has_authority
                            && !c.is_recovering()
                            && drain_transition.is_none()
                            && self.db.cluster_intake_fenced());
                    if needs_activation {
                        if self.installed_fence.is_some() && self.installed_fence != Some(identity)
                        {
                            // Cancel the cached lifetime before waiting for compute cycles that
                            // may be blocked in send. Suspension is safe if another authority
                            // path already installed this exact successor; a genuinely higher
                            // certificate resets the delivery domain during installation.
                            self.db.set_source_gate(true);
                            c.publish_checkpoint_drain_transition(None);
                            c.publish_checkpoint_assignment_fence(None);
                            self.db.suspend_shuffle_assignment_fence();
                            authority_revision = self
                                .db
                                .assignment_authority_revision
                                .load(std::sync::atomic::Ordering::Acquire);
                            self.installed_fence = None;
                        }
                        match self
                            .db
                            .activate_assignment_authority(
                                &fence,
                                drain_transition,
                                authority_revision,
                                head_deadline,
                            )
                            .await
                        {
                            Ok(activation) if activation.installed => {
                                self.installed_fence = Some(identity);
                                self.installed_authority_revision = activation.revision;
                                self.assignment_authority_dirty = false;
                            }
                            Ok(_) => self.assignment_authority_dirty = true,
                            Err(error) => {
                                c.publish_checkpoint_drain_transition(None);
                                c.publish_checkpoint_assignment_fence(None);
                                self.installed_fence = None;
                                self.assignment_authority_dirty = true;
                                warn!(%error, version, "shuffle assignment certificate install failed");
                            }
                        }
                    }
                }
                None if !self.assignment_authority_dirty => {
                    // An incomplete adoption or process-roster read is not proof that the
                    // installed certificate was superseded. Close admission while retaining
                    // its delivery sequence; installing a later concrete successor resets the
                    // delivery domain from that certified boundary.
                    self.assignment_authority_dirty = true;
                    match suspend_local_assignment_authority(
                        &self.db,
                        Some(c.as_ref()),
                        head_deadline,
                    )
                    .await
                    {
                        Ok(()) => warn!(
                            version,
                            "owner-complete assignment certificate unavailable; authority suspended"
                        ),
                        Err(error) => warn!(
                            %error,
                            version,
                            "assignment suspension could not drain the prior execution scope"
                        ),
                    }
                }
                None => {}
            }
        }
    }
    async fn settle_terminal_drain(
        &mut self,
        snapshot: &AssignmentSnapshot,
        audited_terminal: Option<&AuditedDrainOutcome>,
        head_deadline: tokio::time::Instant,
        authority_revision: &mut u64,
    ) -> bool {
        let transition = self.active_local_drain.clone().or_else(|| {
            audited_terminal.and_then(|audited| {
                self.controller
                    .as_deref()
                    .and_then(|controller| local_drain_participant(controller, &audited.transition))
                    .map(|_| audited.transition.clone())
            })
        });
        let Some(transition) = transition else {
            self.terminal_resolution_hold = None;
            return true;
        };

        let audited = audited_terminal.filter(|audited| audited.transition == transition);
        let already_resolved = match audited {
            Some(audited) => {
                let resolution = SourceDrainResolution {
                    round: transition.id(),
                    outcome: audited.outcome,
                };
                match crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
                    &self.db.owned_source_tasks,
                    resolution,
                ) {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        self.assignment_authority_dirty = true;
                        let _ = hold_terminal_source_resolution(
                            &self.db,
                            self.controller.as_deref(),
                            transition.id(),
                            head_deadline,
                            &mut self.terminal_resolution_hold,
                        )
                        .await;
                        warn!(%error, version = snapshot.version, "snapshot watcher: source drain status audit failed; assignment authority suspended");
                        return false;
                    }
                }
            }
            None => false,
        };
        if already_resolved {
            self.active_local_drain = None;
            self.terminal_resolution_hold = None;
            return true;
        }

        self.assignment_authority_dirty = true;
        match hold_terminal_source_resolution(
            &self.db,
            self.controller.as_deref(),
            transition.id(),
            head_deadline,
            &mut self.terminal_resolution_hold,
        )
        .await
        {
            Ok(revision) => *authority_revision = revision,
            Err(error) => {
                warn!(%error, version = snapshot.version, "snapshot watcher: could not suspend authority for source drain resolution");
                return false;
            }
        }
        match settle_observed_local_drain(
            &self.db,
            &self.store,
            &self.registry,
            self.controller.as_deref(),
            &transition,
            snapshot,
            audited,
            head_deadline,
            SourceDrainResolutionDeadline::Fresh(self.config.drain_ack_timeout),
        )
        .await
        {
            Ok(true) => {
                self.active_local_drain = None;
                self.terminal_resolution_hold = None;
                *authority_revision = self
                    .db
                    .assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire);
                true
            }
            Ok(false) => false,
            Err(error) => {
                warn!(%error, version = snapshot.version, "snapshot watcher: local source drain resolution failed; assignment authority suspended");
                false
            }
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return,
                _ = self.ticker.tick() => {}
            }
            let local = self.registry.assignment_version();
            let head_deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
            let mut authority_revision = self
                .db
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire);

            // The durable namespace is authoritative, so every tick audits it even when the local
            // version has not changed. This closes the crash window after a successful CAS but
            // before local adoption.
            let audit = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return,
                result = tokio::time::timeout_at(head_deadline, self.store.load()) => result,
            };
            let mut audited_target = None;
            let mut audited_terminal = None;
            let mut audited_recovery = false;
            if let Ok(Ok(Some(snapshot))) = &audit {
                if !snapshot.draining {
                    let materialization_audit = tokio::time::timeout_at(
                        head_deadline,
                        audit_assignment_snapshot_authority_outcome(
                            &self.store,
                            self.controller.as_deref(),
                            snapshot,
                        ),
                    )
                    .await;
                    match materialization_audit {
                        Ok(Ok(outcome)) => {
                            audited_recovery = outcome.is_recovery();
                            audited_terminal = outcome.terminal().cloned();
                            audited_target = Some(outcome);
                        }
                        failed => {
                            self.assignment_authority_dirty = true;
                            let _ = suspend_local_assignment_authority(
                                &self.db,
                                self.controller.as_deref(),
                                head_deadline,
                            )
                            .await;
                            match failed {
                                Ok(Err(error)) => {
                                    warn!(%error, version = snapshot.version, "snapshot watcher: drain finalization audit failed; assignment authority suspended");
                                }
                                Err(_) => {
                                    warn!(version = snapshot.version, timeout = ?self.config.checkpoint_timeout, "snapshot watcher: drain finalization audit timed out; assignment authority suspended");
                                }
                                Ok(Ok(_)) => unreachable!(),
                            }
                            continue;
                        }
                    }
                }
            }
            if let Ok(Ok(Some(snapshot))) = &audit {
                if !snapshot.draining
                    && !self
                        .settle_terminal_drain(
                            snapshot,
                            audited_terminal.as_ref(),
                            head_deadline,
                            &mut authority_revision,
                        )
                        .await
                {
                    continue;
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
                        audit_drain_predecessor(
                            &self.store,
                            &self.registry,
                            &snap,
                            self.controller.as_deref(),
                        ),
                    )
                    .await;
                    match predecessor {
                        Ok(Ok(predecessor)) => {
                            let transition = snap
                                .drain_transition
                                .as_ref()
                                .expect("validated draining snapshot has a transition")
                                .clone();
                            self.durable_drain_transition = Some(transition.clone());
                            self.durable_snapshot = Some(predecessor);
                            if let Some(ref c) = self.controller {
                                c.publish_checkpoint_drain_transition(Some(transition.clone()));
                            }
                            if snap.version != self.last_drained {
                                match self.db.validate_source_drain_snapshot(&snap) {
                                    Ok(()) => {
                                        let acknowledgement = async {
                                            let c = self.controller.as_ref().ok_or_else(|| {
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
                                            self.active_local_drain = Some(transition.clone());
                                            prepare_and_announce_local_drain(
                                                &self.db,
                                                &self.store,
                                                &self.registry,
                                                c,
                                                &snap,
                                                &transition,
                                                participant,
                                                std::cmp::min(
                                                    head_deadline,
                                                    tokio::time::Instant::now()
                                                        + self.config.drain_ack_timeout,
                                                ),
                                            )
                                            .await
                                        }
                                        .await;
                                        match acknowledgement {
                                            Ok(()) => {
                                                self.last_drained = snap.version;
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
                            self.durable_snapshot = None;
                            self.durable_drain_transition = None;
                            self.assignment_authority_dirty = true;
                            let _ = suspend_local_assignment_authority(
                                &self.db,
                                self.controller.as_deref(),
                                head_deadline,
                            )
                            .await;
                            warn!(%error, version = snap.version, "snapshot watcher: draining predecessor audit failed; assignment authority suspended");
                        }
                        Err(_) => {
                            self.durable_snapshot = None;
                            self.durable_drain_transition = None;
                            self.assignment_authority_dirty = true;
                            let _ = suspend_local_assignment_authority(
                                &self.db,
                                self.controller.as_deref(),
                                head_deadline,
                            )
                            .await;
                            warn!(version = snap.version, timeout = ?self.config.checkpoint_timeout, "snapshot watcher: draining predecessor audit timed out; assignment authority suspended");
                        }
                    }
                }
                Ok(Ok(Some(snap))) if !snap.draining && snap.version > local => {
                    if audited_recovery {
                        let Some(controller) = self.controller.as_deref() else {
                            self.assignment_authority_dirty = true;
                            let _ =
                                suspend_local_assignment_authority(&self.db, None, head_deadline)
                                    .await;
                            warn!(
                                version = snap.version,
                                "snapshot watcher: recovery assignment has no cluster controller"
                            );
                            continue;
                        };
                        match try_suspend_recovery_assignment_authority(
                            &self.db,
                            controller,
                            head_deadline,
                        )
                        .await
                        {
                            Ok(true) => {}
                            Ok(false) => {
                                debug!(
                                    version = snap.version,
                                    "snapshot watcher: recovery assignment waits for local vnode transition"
                                );
                                if let Err(error) = self
                                    .ensure_current_assignment_authority_cached(
                                        audited_target.as_ref().expect(
                                            "stable successor was audited before suspension",
                                        ),
                                        head_deadline,
                                    )
                                    .await
                                {
                                    warn!(%error, version = snap.version, "snapshot watcher: could not audit predecessor authority for pending vnode transition");
                                    continue;
                                }
                                // The transition may have been staged before its predecessor
                                // transport certificate became active. Repair that exact audited
                                // authority before waiting for the newer durable head.
                                self.publish_authority(authority_revision, head_deadline)
                                    .await;
                                continue;
                            }
                            Err(error) => {
                                self.assignment_authority_dirty = true;
                                warn!(%error, version = snap.version, "snapshot watcher: could not suspend recovery assignment");
                                continue;
                            }
                        }
                        if let Err(error) = ensure_local_recovery_fault(&self.db, controller).await
                        {
                            self.assignment_authority_dirty = true;
                            warn!(%error, version = snap.version, "snapshot watcher: could not publish recovery fault");
                            continue;
                        }
                        authority_revision = self
                            .db
                            .assignment_authority_revision
                            .load(std::sync::atomic::Ordering::Acquire);
                        self.assignment_authority_dirty = true;
                    }
                    let resolved_local = self.registry.assignment_version();
                    if snap.version > resolved_local {
                        debug!(
                            local = resolved_local,
                            remote = snap.version,
                            "adopting newer assignment"
                        );
                        match self
                            .db
                            .adopt_assignment_snapshot(snap.clone(), head_deadline)
                            .await
                        {
                            Ok(_) => {
                                self.durable_drain_transition = None;
                                self.durable_snapshot = Some(snap.clone());
                                authority_revision = self
                                    .db
                                    .assignment_authority_revision
                                    .load(std::sync::atomic::Ordering::Acquire);
                            }
                            Err(error) if error.is_shuffle_not_ready() => {
                                // The local graph still needs the installed predecessor authority
                                // to consume its staged vnode transition. Keep the prior audited
                                // snapshot/certificate cached and retry this successor next tick.
                                debug!(
                                    error = %error,
                                    version = snap.version,
                                    "snapshot watcher: successor waits for local vnode transition"
                                );
                                if let Err(cache_error) = self
                                    .ensure_current_assignment_authority_cached(
                                        audited_target
                                            .as_ref()
                                            .expect("stable successor was audited before adoption"),
                                        head_deadline,
                                    )
                                    .await
                                {
                                    warn!(%cache_error, version = snap.version, "snapshot watcher: could not audit predecessor authority for pending vnode transition");
                                    continue;
                                }
                                // Fall through to predecessor authority publication below. A
                                // staged transition cannot complete if its retained certificate
                                // was suspended or its first activation previously timed out.
                            }
                            Err(e) => warn!(error = %e, "snapshot watcher: adoption failed"),
                        }
                    } else {
                        self.durable_drain_transition = None;
                        self.durable_snapshot = Some(snap);
                    }
                }
                Ok(Ok(Some(snap))) => {
                    self.durable_drain_transition
                        .clone_from(&snap.drain_transition);
                    self.durable_snapshot = Some(snap);
                }
                Ok(Ok(None)) => {
                    self.durable_drain_transition = None;
                    self.durable_snapshot = None;
                }
                Ok(Err(error)) => {
                    self.durable_snapshot = None;
                    self.durable_drain_transition = None;
                    self.assignment_authority_dirty = true;
                    let _ = suspend_local_assignment_authority(
                        &self.db,
                        self.controller.as_deref(),
                        head_deadline,
                    )
                    .await;
                    warn!(%error, "snapshot watcher: durable audit failed; assignment authority suspended");
                }
                Err(_) => {
                    self.durable_snapshot = None;
                    self.durable_drain_transition = None;
                    self.assignment_authority_dirty = true;
                    let _ = suspend_local_assignment_authority(
                        &self.db,
                        self.controller.as_deref(),
                        head_deadline,
                    )
                    .await;
                    warn!(
                        timeout = ?self.config.checkpoint_timeout,
                        "snapshot watcher: durable audit timed out; assignment authority suspended"
                    );
                }
            }

            self.publish_authority(authority_revision, head_deadline)
                .await;
        }
    }
}

/// Spawn the per-node snapshot watcher. Exits on shutdown.
///
/// # Panics
///
/// The spawned watcher panics if an already-validated draining snapshot lacks its transition.
pub fn spawn_snapshot_watcher(
    db: Arc<LaminarDB>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    shutdown: CancellationToken,
    config: RebalanceConfig,
    controller: Option<Arc<ClusterController>>,
) -> JoinHandle<()> {
    tokio::spawn(SnapshotWatcher::new(db, store, registry, shutdown, config, controller).run())
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
                // Transport activation requires exact assignment adoption, including while vnode
                // state is still installing. Graceful rotation applies the stronger semantic
                // readiness gate separately immediately before its durable CAS.
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

fn assignment_vnode_state_is_ready(
    fence: &CheckpointAssignmentFence,
    reported: &rustc_hash::FxHashMap<u64, CheckpointAssignmentAdoption>,
) -> bool {
    fence.participants.iter().all(|participant| {
        reported.get(&participant.node_id).is_some_and(|adoption| {
            adoption.vnode_state_ready
                && adoption.participant == *participant
                && adoption.matches_fence(fence)
        })
    })
}

async fn read_assignment_vnode_state_readiness(
    controller: &ClusterController,
    fence: &CheckpointAssignmentFence,
    deadline: tokio::time::Instant,
) -> Result<bool, String> {
    let reported = tokio::time::timeout_at(deadline, controller.read_adopted_assignments())
        .await
        .map_err(|_| {
            format!(
                "vnode-state readiness read for assignment {} timed out",
                fence.assignment_version
            )
        })??
        .into_iter()
        .map(|(node, adoption)| (node.0, adoption))
        .collect();
    Ok(assignment_vnode_state_is_ready(fence, &reported))
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
pub(crate) struct AuditedDrainOutcome {
    transition: AssignmentDrainTransition,
    outcome: SourceDrainOutcome,
    handoff_checkpoint: Option<CommittedCheckpointRef>,
}

/// Durable authority proof for one materialized assignment target.
///
/// This capability binds only the target's immediate predecessor. Older authority history may
/// already have been pruned and must not be reopened while installing or recertifying the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuditedAssignmentAuthority {
    target_version: u64,
    predecessor: Option<CheckpointAssignmentFence>,
    handoff_checkpoint: Option<CommittedCheckpointRef>,
    terminal: Option<AuditedDrainOutcome>,
}

impl AuditedAssignmentAuthority {
    fn without_successor_edge(target_version: u64) -> Self {
        Self {
            target_version,
            predecessor: None,
            handoff_checkpoint: None,
            terminal: None,
        }
    }

    fn recovery(
        target_version: u64,
        predecessor: CheckpointAssignmentFence,
        handoff_checkpoint: CommittedCheckpointRef,
    ) -> Self {
        Self {
            target_version,
            predecessor: Some(predecessor),
            handoff_checkpoint: Some(handoff_checkpoint),
            terminal: None,
        }
    }

    fn drain(target_version: u64, terminal: AuditedDrainOutcome) -> Self {
        let handoff_checkpoint = terminal.committed_handoff_checkpoint().cloned();
        Self {
            target_version,
            predecessor: Some(terminal.transition.predecessor.clone()),
            handoff_checkpoint,
            terminal: Some(terminal),
        }
    }

    fn is_recovery(&self) -> bool {
        self.predecessor.is_some() && self.terminal.is_none()
    }

    fn terminal(&self) -> Option<&AuditedDrainOutcome> {
        self.terminal.as_ref()
    }

    pub(crate) fn predecessor(&self) -> Option<&CheckpointAssignmentFence> {
        self.predecessor.as_ref()
    }

    pub(crate) fn handoff_checkpoint(&self) -> Option<&CommittedCheckpointRef> {
        self.handoff_checkpoint.as_ref()
    }

    pub(crate) fn into_terminal(self) -> Option<AuditedDrainOutcome> {
        self.terminal
    }

    fn retained_predecessor(
        &self,
        local_version: u64,
    ) -> Result<&CheckpointAssignmentFence, String> {
        if local_version.checked_add(1) != Some(self.target_version) {
            return Err(format!(
                "audited assignment {} is not the exact successor of local assignment {local_version}",
                self.target_version
            ));
        }
        self.predecessor.as_ref().ok_or_else(|| {
            format!(
                "audited assignment {} has no predecessor certificate",
                self.target_version
            )
        })
    }
}

/// A committed drain transition returned only after durable authority audit.
///
/// The private field makes this a capability: sibling modules may consume it, but cannot mint one
/// from an arbitrary canonical transition.
pub(crate) struct AuditedCommittedDrainTransition(AssignmentDrainTransition);

impl AuditedCommittedDrainTransition {
    pub(crate) fn into_transition(self) -> AssignmentDrainTransition {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn from_canonical_for_test(
        transition: AssignmentDrainTransition,
    ) -> Result<Self, String> {
        transition
            .is_canonical()
            .then_some(Self(transition))
            .ok_or_else(|| "test drain transition is not canonical".into())
    }
}

impl AuditedDrainOutcome {
    #[must_use]
    pub(crate) fn committed_handoff_checkpoint(&self) -> Option<&CommittedCheckpointRef> {
        if self.outcome == SourceDrainOutcome::Commit {
            self.handoff_checkpoint.as_ref()
        } else {
            None
        }
    }

    /// The exact transition whose target durably committed, if this was not an abort.
    #[must_use]
    pub(crate) fn into_committed_transition(self) -> Option<AuditedCommittedDrainTransition> {
        (self.outcome == SourceDrainOutcome::Commit)
            .then_some(AuditedCommittedDrainTransition(self.transition))
    }
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

pub(crate) async fn audit_assignment_snapshot_authority_outcome(
    store: &AssignmentSnapshotStore,
    controller: Option<&ClusterController>,
    snapshot: &AssignmentSnapshot,
) -> Result<AuditedAssignmentAuthority, String> {
    if snapshot.draining {
        return Ok(AuditedAssignmentAuthority::without_successor_edge(
            snapshot.version,
        ));
    }
    if snapshot.version == 1 {
        return Ok(AuditedAssignmentAuthority::without_successor_edge(
            snapshot.version,
        ));
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
        let (predecessor, handoff_checkpoint) =
            audit_materialized_recovery_with_authority(store, authority.as_deref(), snapshot)
                .await?;
        return Ok(AuditedAssignmentAuthority::recovery(
            snapshot.version,
            predecessor,
            handoff_checkpoint,
        ));
    };
    audit_materialized_drain_transition(store, authority.as_deref(), snapshot, transition)
        .await
        .map(|terminal| AuditedAssignmentAuthority::drain(snapshot.version, terminal))
}

async fn audit_materialized_drain_with_authority(
    store: &AssignmentSnapshotStore,
    authority: Option<&LeaderLeaseStore>,
    snapshot: &AssignmentSnapshot,
) -> Result<Option<AuditedDrainOutcome>, String> {
    if snapshot.draining {
        return Ok(None);
    }
    if snapshot.version == 1 {
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
    audit_materialized_drain_transition(store, authority, snapshot, transition)
        .await
        .map(Some)
}

async fn audit_materialized_recovery_with_authority(
    store: &AssignmentSnapshotStore,
    authority: Option<&LeaderLeaseStore>,
    snapshot: &AssignmentSnapshot,
) -> Result<(CheckpointAssignmentFence, CommittedCheckpointRef), String> {
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
    Ok((decision.predecessor, decision.recovery_checkpoint))
}

async fn audit_materialized_drain_transition(
    store: &AssignmentSnapshotStore,
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
    let predecessor_version = snapshot
        .version
        .checked_sub(1)
        .ok_or_else(|| "materialized drain has no predecessor generation".to_string())?;
    let predecessor = store
        .load_version(predecessor_version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "materialized drain assignment {} lost predecessor {predecessor_version}",
                snapshot.version
            )
        })?;
    if predecessor.draining
        || predecessor
            .assignment_fence()
            .map_err(|error| error.to_string())?
            != transition.predecessor
    {
        return Err(format!(
            "assignment {} drain transition does not bind its exact committed predecessor",
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
        handoff_checkpoint: decision.handoff_checkpoint,
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
        .into_terminal()
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
        db.adopt_assignment_snapshot(finalized.clone(), adoption_deadline)
            .await
            .map_err(|error| error.to_string())?;
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
            "local assignment {target_version} does not match the durable drain outcome"
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
    let Some(audited) = audited.into_terminal() else {
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
    if confirmed_terminal.terminal() != Some(&audited) {
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
fn publish_placement_metrics(
    metrics: &EngineMetrics,
    registry: &VnodeRegistry,
    nodes: &[(NodeId, Locality)],
    isolation_tier: usize,
) {
    let owners = registry.snapshot();
    let total = u32::try_from(owners.len().max(1)).unwrap_or(u32::MAX);
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
        .set(f64::from(max) / f64::from(total));
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
                            warn!(%error, "wait_until_drained: snapshot cardinality mismatch");
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
        RotateOutcome::Conflict(winner) => *winner,
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
    db.adopt_assignment_snapshot(durable, deadline)
        .await
        .map_err(|error| error.to_string())?;
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

    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    let pinned_checkpoint = tokio::time::timeout_at(
        deadline,
        authority.assignment_handoff_checkpoint(&predecessor),
    )
    .await
    .map_err(|_| "recovery handoff lookup exceeded the fencing deadline".to_string())?
    .map_err(|error| error.to_string())?;
    let recovery_checkpoint = if let Some(reference) = pinned_checkpoint {
        reference
    } else {
        let outcome =
            tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                .await
                .map_err(|_| {
                    "recovery checkpoint lookup exceeded the fencing deadline".to_string()
                })?
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    "assignment recovery requires a committed predecessor checkpoint".to_string()
                })?;
        if outcome.assignment_fence.as_ref() != Some(&predecessor) {
            return Err(
                "latest committed cluster checkpoint does not bind the recovery predecessor".into(),
            );
        }
        outcome.committed_checkpoint.ok_or_else(|| {
            "latest committed cluster checkpoint has no committed index reference".to_string()
        })?
    };
    let leader_proof = controller
        .capture_leader_proof()
        .ok_or_else(|| "assignment recovery lost the current durable leader proof".to_string())?;
    let decision = AssignmentRecoveryDecision::new(
        predecessor,
        target,
        proposal_ref,
        process_fences,
        recovery_checkpoint,
        leader_proof.clone(),
    )?;
    let decision = match tokio::time::timeout_at(
        deadline,
        controller.record_assignment_recovery_decision(&leader_proof, decision, deadline),
    )
    .await
    .map_err(|_| "recovery authority admission exceeded the fencing deadline".to_string())?
    .map_err(|error| error.clone())?
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

fn execute_graceful_rotation_owned(
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    store: Arc<AssignmentSnapshotStore>,
    registry: Arc<VnodeRegistry>,
    current: AssignmentSnapshot,
    new_vnodes: std::collections::BTreeMap<u32, NodeId>,
    participants: Vec<CheckpointParticipant>,
    config: RebalanceConfig,
) -> futures::future::BoxFuture<'static, Result<Option<u64>, String>> {
    Box::pin(async move {
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
        let current_owners = current
            .to_vnode_vec(registry.vnode_count())
            .map_err(|error| error.to_string())?;
        let publication_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
        let rotate_outcome = {
            // Assignment adoption and startup recovery create pending vnode work under this lock.
            // Keep it through the durable CAS so it cannot appear after preflight but
            // before V+1 is published. The scope must end before conflict adoption or drain
            // finalization, both of which acquire the same lock.
            let _adoption = tokio::time::timeout_at(
                publication_deadline,
                db.assignment_adoption_lock.lock(),
            )
            .await
            .map_err(|_| {
                format!(
                    "graceful rotation from assignment {} timed out waiting for assignment serialization",
                    current.version
                )
            })?;
            let local = registry.versioned_snapshot();
            if local.version() != current.version || local.owners() != current_owners.as_slice() {
                return Err(format!(
                    "graceful rotation predecessor assignment {} does not match local assignment {}",
                    current.version,
                    local.version()
                ));
            }
            let local_vnode_state_ready = tokio::time::timeout_at(
                publication_deadline,
                db.local_vnode_state_is_ready(&registry, &transition.predecessor),
            )
            .await
            .map_err(|_| {
                format!(
                    "graceful rotation from assignment {} timed out checking local vnode-state readiness",
                    current.version
                )
            })?
            .map_err(|error| error.to_string())?;
            if !local_vnode_state_ready {
                debug!(
                    version = current.version,
                    "graceful rotation deferred until local vnode state matches the predecessor"
                );
                return Ok(None);
            }
            let Some(published_fence) = controller.checkpoint_assignment_fence(current.version)
            else {
                debug!(
                    version = current.version,
                    "graceful rotation deferred until the predecessor assignment is adopted"
                );
                return Ok(None);
            };
            if published_fence != transition.predecessor {
                return Err(format!(
                    "graceful rotation predecessor assignment {} does not match the exact assignment-adoption certificate",
                    current.version
                ));
            }
            // This mutable report scan is a best-current preflight, not a cross-node lease. It
            // prevents assignment chaining before every participant's initial install; a later
            // withdrawal is contained by source drain and forced-checkpoint abort/recovery.
            if !read_assignment_vnode_state_readiness(
                &controller,
                &transition.predecessor,
                publication_deadline,
            )
            .await?
            {
                debug!(
                    version = current.version,
                    "graceful rotation deferred until every participant installs vnode state"
                );
                return Ok(None);
            }
            match tokio::time::timeout_at(
                publication_deadline,
                store.save_if_version(&drain, current.version),
            )
            .await
            {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => {
                    return Err(format!(
                        "assignment drain {} publication failed with an ambiguous durable outcome: {error}; the durable head must be reconciled on retry",
                        drain.version
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "assignment drain {} publication timed out with an ambiguous durable outcome; the durable head must be reconciled on retry",
                        drain.version
                    ));
                }
            }
        };
        match rotate_outcome {
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
                    Ok(()) => await_drain_quorum(&controller, &transition, drain_deadline).await,
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
                        None,
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
                let handoff_checkpoint =
                    match pre_rotation_checkpoint(&db, &controller, &transition, config).await {
                        Ok(reference) => reference,
                        Err(failure) => {
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
                                None,
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
                    };
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
                    Some(handoff_checkpoint),
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
                    *winner,
                    tokio::time::Instant::now() + config.checkpoint_timeout,
                )
                .await?;
                Ok(Some(v))
            }
        }
    })
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
                None,
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
        let current_roster_is_live = successor_participants(&current_owners, &successor_processes)
            .is_ok_and(|participants| participants == current.participants);
        if current.version > local_assignment.version() && current_roster_is_live {
            // A writer can fail after its durable CAS succeeds but before local adoption. Adopt
            // that exact live-process generation before planning another rotation. A replacement
            // process must not adopt a retained certificate naming its predecessor incarnation;
            // it proceeds below through the authority-sequenced recovery-successor path.
            db.adopt_assignment_snapshot(current.clone(), head_deadline)
                .await
                .map_err(|error| error.to_string())?;
            let reconciled_version = registry.assignment_version();
            if reconciled_version < current.version {
                return Err(format!(
                    "durable assignment {} was not adopted; local assignment remains {}",
                    current.version, reconciled_version
                ));
            }
            return Ok(Some(reconciled_version));
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

        execute_graceful_rotation_owned(
            db,
            controller,
            store,
            registry,
            current,
            new_vnodes,
            participants,
            config,
        )
        .await
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

#[cfg(test)]
pub(crate) async fn record_assignment_checkpoint_for_test(
    authority: &LeaderLeaseStore,
    authority_store: &Arc<dyn object_store::ObjectStore>,
    fence: &CheckpointAssignmentFence,
    proof: &laminar_core::checkpoint::LeaderProof,
) -> CommittedCheckpointRef {
    use laminar_core::checkpoint::{
        CheckpointScope, CommittedCheckpointIndex, CommittedParticipantRef, PipelineIdentity,
        COMMITTED_CHECKPOINT_INDEX_VERSION,
    };
    use laminar_core::checkpoint_decision::{CheckpointDecisionStore, CheckpointVerdict};

    let epoch = authority
        .highest_cluster_terminal_outcome()
        .await
        .unwrap()
        .map_or(1, |outcome| outcome.epoch.checked_add(1).unwrap());
    let predecessor = authority
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .and_then(|outcome| outcome.committed_checkpoint);
    let deployment_id = CheckpointDecisionStore::new(Arc::clone(authority_store))
        .load_or_create_deployment_id()
        .await
        .unwrap();
    let index = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: PipelineIdentity::empty(),
        epoch,
        checkpoint_id: epoch,
        scope: CheckpointScope::Cluster,
        vnode_count: u16::try_from(fence.vnode_count).unwrap(),
        assignment_fence: Some(fence.clone()),
        predecessor,
        participants: fence
            .participants
            .iter()
            .map(|participant| CommittedParticipantRef {
                participant_id: participant.node_id,
                manifest_len: 1,
                manifest_sha256: "0".repeat(64),
                node_data_len: 0,
                node_data_sha256: "1".repeat(64),
            })
            .collect(),
        source_offsets: Default::default(),
        channel_progress: Vec::new(),
        checkpoint_watermark: None,
    };
    let reference = authority.create_committed_checkpoint(&index).await.unwrap();
    authority
        .record_cluster_outcome(
            proof,
            epoch,
            epoch,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(reference.clone()),
        )
        .await
        .unwrap();
    reference
}

/// Seal and resolve the exact predecessor-fenced checkpoint used for state handoff.
async fn pre_rotation_checkpoint(
    db: &Arc<LaminarDB>,
    controller: &ClusterController,
    transition: &AssignmentDrainTransition,
    config: RebalanceConfig,
) -> Result<CommittedCheckpointRef, String> {
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
            .unwrap_or_else(|| "pre-rotation checkpoint did not commit".into()));
    }
    if let Some(error) = ckpt.continuation_error() {
        return Err(format!(
            "pre-rotation checkpoint committed but cannot continue safely: {error}"
        ));
    }

    let authority = controller
        .checkpoint_authority()
        .map_err(|error| error.to_string())?;
    let outcome = tokio::time::timeout(
        config.checkpoint_timeout,
        authority.cluster_outcome(ckpt.epoch),
    )
    .await
    .map_err(|_| "pre-rotation checkpoint authority read timed out".to_string())?
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "pre-rotation checkpoint disappeared from cluster authority".to_string())?;
    if !outcome.is_commit()
        || outcome.epoch != ckpt.epoch
        || outcome.checkpoint_id != ckpt.checkpoint_id
        || outcome.assignment_fence.as_ref() != Some(&transition.predecessor)
    {
        return Err("pre-rotation checkpoint authority does not bind the predecessor cut".into());
    }
    outcome
        .committed_checkpoint
        .ok_or_else(|| "pre-rotation Commit has no checkpoint reference".to_string())
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

fn assignment_binds_local_process(
    snapshot: &AssignmentSnapshot,
    controller: &ClusterController,
) -> Result<bool, String> {
    let fence = snapshot
        .assignment_fence()
        .map_err(|error| error.to_string())?;
    let owners = snapshot
        .to_vnode_vec(fence.vnode_count)
        .map_err(|error| error.to_string())?;
    let local = controller.instance_id().0;
    let owns = owners.iter().any(|owner| owner.0 == local);
    let bound = fence.participant_incarnation(local);
    Ok(if owns {
        bound == Some(controller.recovery_incarnation())
    } else {
        bound.is_none()
    })
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
    handoff_checkpoint: Option<CommittedCheckpointRef>,
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
    let requested = match (requested_verdict, handoff_checkpoint) {
        (AssignmentDrainVerdict::Commit, Some(reference)) => {
            AssignmentDrainDecision::commit(transition, deciding_proof.clone(), reference)
        }
        (AssignmentDrainVerdict::Abort, None) => {
            AssignmentDrainDecision::abort(transition, deciding_proof.clone())
        }
        _ => Err("drain finalization has an invalid handoff checkpoint".into()),
    }?;
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
        RotateOutcome::Conflict(winner) => *winner,
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
    if assignment_binds_local_process(&durable, controller)? {
        let adoption_deadline = tokio::time::Instant::now() + config.checkpoint_timeout;
        db.adopt_assignment_snapshot(durable, adoption_deadline)
            .await
            .map_err(|error| error.to_string())?;
        if local_drain_participant(controller, transition).is_some() {
            db.resolve_local_source_drain(
                transition.id(),
                source_outcome,
                tokio::time::Instant::now() + config.drain_ack_timeout,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
    } else {
        warn!(
            version,
            "materialized drain terminal names a predecessor process; local adoption waits for a recovery successor"
        );
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
        None,
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
        db.adopt_assignment_snapshot(snap, deadline)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
