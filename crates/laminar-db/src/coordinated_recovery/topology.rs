//! One topology phase per existing recovery-monitor poll; no independent scheduler or task queue.

use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use laminar_core::cluster::control::{
    ClusterController, TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError,
    TopologyOperationId,
};

use crate::{db::DbState, DbError, LaminarDB, PreparedTopologyRestore};

// Phase methods have their own 15/30/45-second bounds. An incomplete roster or repeated uncertain
// phase must also eventually request coordinated recovery, rather than hold the old cut forever.
const STALLED_PHASE_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Default)]
pub(crate) struct TopologyDriver {
    /// Exactly one private image owns the existing compiler permit. No graph/state copy is made.
    image: Option<Box<PreparedTopologyRestore>>,
    progress: Option<(TopologyOperationId, u64, tokio::time::Instant)>,
    completed: Option<(TopologyOperationId, u64)>,
    last_error: Option<String>,
}

impl TopologyDriver {
    fn available(db: &LaminarDB, controller: &ClusterController) -> bool {
        !db.is_closed()
            && !db.shutdown.load(Ordering::Acquire)
            && !db.cluster_authority_revoked.load(Ordering::Acquire)
            && !db.durable_terminal_recovery_fence.load(Ordering::Acquire)
            && !db.terminal_pipeline_halt.load(Ordering::Acquire)
            && db.pending_recovery_fault.load(Ordering::Acquire) == 0
            && db.last_fault.lock().is_none()
            && !db.coordinated_recovery_in_progress()
            && !controller.is_recovering()
            && !controller.is_draining()
            && controller.process_lease_is_live()
    }

    /// Drop private preparation before the recovery owner tries to acquire the compiler slot.
    /// Dropping an image cannot change a catalog, clear a cut hold or authorize intake/output.
    pub(super) fn pause_if_fenced(&mut self, db: &LaminarDB, controller: &ClusterController) {
        if !Self::available(db, controller) {
            self.image = None;
            self.completed = None;
        }
    }

    pub(super) async fn drive(&mut self, db: &Arc<LaminarDB>, controller: &ClusterController) {
        match self.poll(db, controller).await {
            Ok(()) => self.last_error = None,
            Err(error) => {
                let message = error.to_string();
                if self.last_error.as_ref() != Some(&message) {
                    tracing::warn!(%error, "topology phase did not complete; durable state and intake fences retained");
                    self.last_error = Some(message);
                }
                if self
                    .progress
                    .is_some_and(|(_, _, since)| since.elapsed() >= STALLED_PHASE_TIMEOUT)
                {
                    self.request_recovery(db, controller);
                }
            }
        }
    }

    /// The monitor owns this future and its image; an API status observer never owns either.
    pub(crate) async fn poll(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
    ) -> Result<(), DbError> {
        self.pause_if_fenced(db, controller);
        if !Self::available(db, controller) {
            return Ok(());
        }
        let authority = controller
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        // Finish this process's prior held boundary even if the leader has admitted the next
        // request after Release/abort. The latest journal entry alone is insufficient here.
        let installed = db
            .installed_topology_runtime
            .lock()
            .as_ref()
            .filter(|binding| {
                binding.released_sequence.is_none()
                    && binding
                        .recovery
                        .as_ref()
                        .is_none_or(|recovery| !recovery.released)
            })
            .map(|binding| binding.input.operation().operation_id);
        let retained = installed
            .or_else(|| {
                self.image
                    .as_ref()
                    .map(|image| image.input.operation().operation_id)
            })
            .or_else(|| {
                db.topology_cut_hold
                    .load(Ordering::Acquire)
                    .then(|| self.progress.map(|(operation, _, _)| operation))
                    .flatten()
            });
        let hint = if let Some(operation) = retained {
            Some((operation, 0))
        } else {
            authority.latest_topology_operation_hint().await?
        };
        let Some(hint) = hint else {
            return Ok(());
        };
        if self.completed == Some(hint)
            && DbState::load(&db.state) == DbState::Running
            && !db.topology_cut_hold.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let status = authority
            .topology_operation_status(hint.0)
            .await?
            .ok_or_else(|| {
                TopologyError::Conflict("retained topology operation disappeared".into())
            })?;
        if self.progress.is_none_or(|(operation, sequence, _)| {
            operation != status.operation_id || sequence != status.status_sequence
        }) {
            self.progress = Some((
                status.operation_id,
                status.status_sequence,
                tokio::time::Instant::now(),
            ));
            tracing::info!(operation = %status.operation_id.get(), phase = ?status.phase,
                "observed durable topology progress");
        }
        // Only an actual fault/recovery path can resume an aborted held parent. Never clear its
        // boundary merely because the journal no longer reserves admission.
        if matches!(status.phase, TopologyAdmissionPhase::Aborted { .. }) {
            self.image = None;
            if db.topology_cut_hold.load(Ordering::Acquire) {
                self.request_recovery(db, controller);
            } else {
                self.completed = Some((status.operation_id, status.status_sequence));
            }
            return Ok(());
        }
        if self
            .progress
            .is_some_and(|(_, _, since)| since.elapsed() >= STALLED_PHASE_TIMEOUT)
        {
            self.request_recovery(db, controller);
            return Err(TopologyError::Conflict(
                "topology phase made no durable progress within 180 seconds; coordinated recovery requested"
                    .into(),
            )
            .into());
        }
        let faults = tokio::time::timeout(
            super::DECISION_IO_TIMEOUT,
            controller.read_recovery_fault_inventory(),
        )
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
        .map_err(TopologyError::Protocol)?;
        if !faults.faults().is_empty() {
            self.image = None;
            return Err(TopologyError::Fenced.into());
        }
        // Only active phase work allocates this control future. Private graph reconstruction is
        // large in debug builds; keep it off the monitor/test stack without increasing stack limits.
        Box::pin(self.advance(db, controller, &status)).await?;
        Ok(())
    }

    fn request_recovery(&mut self, db: &LaminarDB, controller: &ClusterController) {
        self.image = None;
        self.completed = None;
        if db.pending_recovery_fault.load(Ordering::Acquire) == 0 {
            if let Err(error) = super::queue_local_fault(controller, &db.pending_recovery_fault) {
                tracing::error!(%error, "could not queue recovery for a stalled or aborted topology");
            }
        }
    }

    async fn advance(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        status: &TopologyAdmissionStatus,
    ) -> Result<(), DbError> {
        let operation = status.operation_id;
        match status.phase {
            TopologyAdmissionPhase::Planned | TopologyAdmissionPhase::Preparing => {
                let process = controller
                    .try_live_local_process_authority_identity()
                    .map_err(|_| TopologyError::Fenced)?;
                let prepared = status.preparation.as_ref().is_some_and(|preparation| {
                    preparation.certificates.iter().any(|certificate| {
                        certificate.participant == process.participant
                            && certificate.process_term == process.process_term
                    })
                });
                if !prepared {
                    db.prepare_cluster_topology_operation(operation).await?;
                } else if controller.is_leader()
                    && status
                        .preparation
                        .as_ref()
                        .is_some_and(|preparation| preparation.complete_sequence.is_some())
                {
                    // This route owns the exact attempt and waits through its terminal cleanup.
                    // Do not wrap it in a deadline that could abandon reserved checkpoint work.
                    db.checkpoint().await?;
                }
            }
            // Runtime-owned checkpoint tails publish each exact application receipt. The worker
            // must not manufacture one or stop actors while external settlement is unresolved.
            TopologyAdmissionPhase::Quiescing => {}
            TopologyAdmissionPhase::CutPrepared => {
                if status.migration_root.is_none() {
                    if controller.is_leader() {
                        db.stage_cluster_topology_migration_root(operation).await?;
                    }
                } else if self.image.is_none() {
                    self.image = Some(Box::new(
                        Box::pin(db.prepare_cluster_topology_restore(operation)).await?,
                    ));
                } else if !self
                    .image
                    .as_deref()
                    .is_some_and(PreparedTopologyRestore::parent_retirement_observed)
                    || !status.target_preparations.iter().any(|receipt| {
                        self.image.as_ref().is_some_and(|image| {
                            receipt.participant == image.input.process().participant
                                && receipt.process_term == image.input.process().process_term
                        })
                    })
                {
                    db.certify_cluster_topology_target_preparation(
                        self.image.as_deref_mut().ok_or(TopologyError::Fenced)?,
                    )
                    .await?;
                } else if controller.is_leader() && status.target_preparation_complete() {
                    db.commit_cluster_topology_target(
                        self.image.as_deref_mut().ok_or(TopologyError::Fenced)?,
                    )
                    .await?;
                }
            }
            TopologyAdmissionPhase::Committed | TopologyAdmissionPhase::Activating => {
                let installed = db
                    .installed_topology_runtime
                    .lock()
                    .as_ref()
                    .is_some_and(|binding| binding.input.operation().operation_id == operation);
                if !installed {
                    if self.image.is_none() {
                        self.image = Some(Box::new(
                            Box::pin(db.recover_committed_cluster_topology(operation)).await?,
                        ));
                    } else if !self
                        .image
                        .as_deref()
                        .is_some_and(PreparedTopologyRestore::is_committed)
                    {
                        // A follower observes the existing Commit; it never publishes another one.
                        db.commit_cluster_topology_target(
                            self.image.as_deref_mut().ok_or(TopologyError::Fenced)?,
                        )
                        .await?;
                    } else {
                        db.install_committed_cluster_topology(
                            *self.image.take().ok_or(TopologyError::Fenced)?,
                        )
                        .await?;
                    }
                } else if controller.is_leader()
                    && status.activation.as_ref().is_some_and(
                        laminar_core::cluster::control::TopologyActivation::installation_complete,
                    )
                {
                    db.release_installed_cluster_topology(operation).await?;
                } else {
                    // Idempotent certification re-observes live actors and a replacement leader's
                    // installation round. A historical receipt is insufficient for readiness.
                    db.certify_installed_cluster_topology(operation).await?;
                }
            }
            TopologyAdmissionPhase::Active => {
                let recovered = db
                    .installed_topology_runtime
                    .lock()
                    .clone()
                    .filter(|binding| {
                        binding.input.operation().operation_id == operation
                            && binding.recovery.is_some()
                    });
                if let Some(binding) = recovered {
                    if !db.recovered_topology_runtime_is_active(&binding).await? {
                        self.request_recovery(db, controller);
                        return Err(TopologyError::Fenced.into());
                    }
                    self.image = None;
                    self.completed = Some((operation, status.status_sequence));
                    return Ok(());
                }
                // Original Release only authorizes its exact runtime UUID. Never reconstruct a
                // post-Release failure from the parent root or reuse another runtime's receipt.
                db.apply_cluster_topology_release(operation).await?;
                self.image = None;
                self.completed = Some((operation, status.status_sequence));
            }
            TopologyAdmissionPhase::Aborted { .. } => {
                return Err(TopologyError::Conflict(
                    "aborted topology cannot advance a target phase".into(),
                )
                .into());
            }
        }
        Ok(())
    }
}

impl super::RecoveryMonitor {
    pub(super) async fn publish_recovery_prepare(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        round: super::RecoveryRound,
        required_prepare_fence: Option<&super::CheckpointAssignmentFence>,
    ) -> Option<super::RecoveryRound> {
        use super::{
            announce_recover_prepare_bounded, current_recovery_assignment_fence,
            replicate_recovery_gen, DECISION_IO_TIMEOUT,
        };
        let gen_id = round.id.generation;
        let round = match Box::pin(controller.bind_recovery_round(round)).await {
            Ok(round) => round,
            Err(error) => {
                tracing::error!(gen = gen_id, %error, "could not bind the exact recovery topology and process roster");
                return None;
            }
        };
        if let Err(error) = replicate_recovery_gen(controller, gen_id).await {
            tracing::error!(gen = gen_id, %error, "could not publish recovery generation");
            return None;
        }

        let prepare_fence = current_recovery_assignment_fence(
            db,
            controller,
            tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
        )
        .await;
        let prepare_fence_matches = matches!(
            prepare_fence.as_ref(),
            Ok(Some(fence))
                if fence == &round.assignment_fence
                    && required_prepare_fence.is_none_or(|required| required == fence)
        );
        if !prepare_fence_matches {
            if let Err(error) = prepare_fence {
                tracing::warn!(
                    gen = gen_id,
                    %error,
                    "recovery assignment audit failed before Prepare; deferring round"
                );
            } else {
                tracing::warn!(
                    gen = gen_id,
                    "recovery assignment changed before Prepare; deferring round"
                );
            }
            return None;
        }

        controller.set_recovering(true);
        db.set_source_gate(true);
        if let Err(error) = announce_recover_prepare_bounded(controller, &round).await {
            tracing::warn!(gen = gen_id, %error, "could not publish recovery Prepare");
            return None;
        }
        tracing::warn!(gen = gen_id, "leader announced recovery prepare");
        Some(round)
    }

    /// Consume the guarded Release while holding intake until the successor sink is ready.
    /// Every failed authority recheck restores the existing recovery and topology gates.
    pub(super) async fn release_recovered_data_plane(
        &mut self,
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &super::RecoveryAnnouncement,
        committed: &super::RecoveryAnnouncement,
        release_deadline: tokio::time::Instant,
        authority_revision: u64,
    ) -> DataPlaneRelease {
        controller.set_recovering(false);
        if committed.round.topology_binding().is_some() {
            // The exact generic terminal is already committed and guarded. Admit the successor
            // sink epoch while source intake remains held, using the ordinary coordinator path.
            let sink_ready = async {
                let mut coordinator = db.coordinator.lock().await;
                let coordinator = coordinator
                    .as_mut()
                    .ok_or(laminar_core::cluster::control::TopologyError::Fenced)?;
                coordinator
                    .reconcile_sink_open_witness_until(release_deadline)
                    .await?;
                coordinator
                    .ensure_assignment_sink_epoch_until(release_deadline)
                    .await
            }
            .await;
            if let Err(error) = sink_ready {
                controller.set_recovering(true);
                tracing::error!(gen = release.round.id.generation, %error, "target recovery successor sink epoch did not become ready");
                self.defer_release_retry(db, controller, release.round.id.generation, false);
                return DataPlaneRelease::Held;
            }
            if let Err(error) = db.record_recovered_topology_release(committed) {
                controller.set_recovering(true);
                tracing::error!(gen = release.round.id.generation, %error, "target recovery lost local runtime ownership before intake release");
                return DataPlaneRelease::Held;
            }
        }
        // This Release certifies retirement, restore/readiness and the exact current authority.
        // Assignment refresh alone cannot clear a held topology cut. Reuse the source-release
        // transition lock so checkpoint capture cannot race a gate reopen.
        let topology_cut_was_held = {
            let _transition = db.cluster_authority_transition.lock();
            db.topology_cut_hold.swap(false, Ordering::AcqRel)
        };
        db.set_source_gate(false);
        if db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
            || db.terminal_pipeline_halt.load(Ordering::Acquire)
            || db.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || controller.is_recovering()
            || db.cluster_intake_fenced()
            || !controller.process_lease_is_live()
            || controller
                .checkpoint_assignment_fence(release.round.assignment_fence.assignment_version)
                .as_ref()
                != Some(&release.round.assignment_fence)
            || controller.checkpoint_drain_transition().is_some()
        {
            controller.set_recovering(true);
            db.set_source_gate(true);
            if topology_cut_was_held {
                let _transition = db.cluster_authority_transition.lock();
                db.topology_cut_hold.store(true, Ordering::Release);
            }
            return DataPlaneRelease::RecheckFailed;
        }
        DataPlaneRelease::Released
    }
}

pub(super) enum DataPlaneRelease {
    Released,
    Held,
    RecheckFailed,
}

impl super::RecoveryMonitor {
    pub(super) async fn release_installation_is_current(
        db: &Arc<LaminarDB>,
        controller: &ClusterController,
        release: &super::RecoveryAnnouncement,
        release_deadline: tokio::time::Instant,
        authority_revision: u64,
    ) -> bool {
        use super::recovery_round_assignment_is_restorable;
        if let Err(error) = db
            .validate_recovered_cluster_topology_release(release)
            .await
        {
            tracing::error!(gen = release.round.id.generation, %error, "recovery Release does not match the live target runtime");
            return false;
        }
        let assignment_restorable = match recovery_round_assignment_is_restorable(
            db,
            controller,
            &release.round,
            release_deadline,
        )
        .await
        {
            Ok(restorable) => restorable,
            Err(error) => {
                tracing::error!(
                    gen = release.round.id.generation,
                    %error,
                    "could not audit recovery Release assignment"
                );
                false
            }
        };
        if !assignment_restorable
            || db.assignment_authority_revision.load(Ordering::Acquire) != authority_revision
        {
            return false;
        }
        true
    }
}
