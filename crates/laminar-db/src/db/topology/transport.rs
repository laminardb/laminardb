//! Exact committed generation on the process-owned shuffle fabric; runtime activation stays held.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use laminar_core::cluster::control::{TopologyError, TopologyRestoreInput, TopologyVersion};
use laminar_core::shuffle::ShuffleTopologyFence;

use super::{DbError, DbState, LaminarDB, PreparedTopologyRestore};

impl LaminarDB {
    /// Fence this process's transport and private graph to the exact committed target.
    /// Reobserves retirement for a held parent; a Created DB must have no runtime/connector owners.
    /// Current full-roster, process/adoption, assignment, source availability and Commit are audited
    /// around publication. Sources, sinks, local catalog/coordinator and intake remain unstarted/held.
    ///
    /// The cooperative budget is 45 seconds. Cancellation after local publication retains the
    /// target fence and cut hold; retry this image or reconstruct from the immutable Commit.
    /// This is transport preparation, not installed receivers/state/sinks readiness or Release.
    ///
    /// # Errors
    /// Rejects foreign/uncommitted/stale images, unresolved actors, process/assignment mismatches,
    /// local faults, damaged cursors and expired deadlines. Runtime DDL remains guarded.
    pub async fn prepare_cluster_topology_transport(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        if !image.belongs_to(self) || !image.input.is_committed() {
            return Err(TopologyError::Conflict(
                "transport preparation requires this DB's committed private image".into(),
            )
            .into());
        }
        self.prepare_topology_transport_image(image).await
    }

    pub(crate) async fn prepare_coordinated_topology_transport(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        let start = image.recovery_start.as_ref().ok_or(TopologyError::Fenced)?;
        if !image.belongs_to_recovery(self, start) || !image.is_committed() {
            return Err(TopologyError::Fenced.into());
        }
        self.prepare_topology_transport_image(image).await
    }

    fn ensure_topology_transport_available(
        &self,
        image: &PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        if let Some(start) = &image.recovery_start {
            self.ensure_topology_recovery_stopped(start)
        } else {
            self.ensure_topology_restore_available(true)
        }
    }

    async fn topology_transport_input(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<TopologyRestoreInput, DbError> {
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        if let Some(start) = &image.recovery_start {
            self.ensure_topology_recovery_stopped(start)?;
            if controller
                .observe_recover_control()
                .await
                .map_err(|error| TopologyError::Conflict(error.to_string()))?
                .as_ref()
                != Some(start)
            {
                return Err(TopologyError::Fenced.into());
            }
            let fresh = controller
                .topology_recovery_input(&start.round, super::recovery_runtime::start_epoch(start)?)
                .await?;
            if image
                .recovery_input()
                .is_none_or(|selected| !fresh.same_restore_requirements(selected))
            {
                return Err(TopologyError::Fenced.into());
            }
            Ok(fresh.migration().clone())
        } else {
            Ok(controller
                .committed_topology_restore_input(image.input.operation().operation_id)
                .await?)
        }
    }

    async fn prepare_topology_transport_image(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        tokio::time::timeout(std::time::Duration::from_secs(45), async {
            self.ensure_topology_transport_available(image)?;
            let controller = self
                .cluster_controller
                .lock()
                .clone()
                .ok_or(TopologyError::Fenced)?;
            let before = self.topology_transport_input(image).await?;
            if !before.same_restore_requirements(&image.input) {
                return Err(TopologyError::Fenced.into());
            }
            if DbState::load(&self.state) == DbState::ShuttingDown {
                self.stop_pipeline_for_topology_retirement().await?;
            }
            drop(
                super::restore::prepare_source_positions_at_cut(
                    &image.candidate,
                    &before,
                    image.recovery_input(),
                )
                .await?,
            );
            let _assignment = self.assignment_adoption_lock.lock().await;
            let _execution = Arc::clone(&self.rotation_execution_fence)
                .write_owned()
                .await;
            let fresh = self.topology_transport_input(image).await?;
            if !fresh.same_restore_requirements(&before) {
                return Err(TopologyError::Fenced.into());
            }
            let target = ShuffleTopologyFence::from_manifest(
                image.target_version(),
                &fresh.plan().target_manifest,
            )
            .map_err(|error| TopologyError::Invalid(error.to_string()))?;
            let sender = self
                .shuffle_sender
                .lock()
                .clone()
                .ok_or(TopologyError::Fenced)?;
            let receiver = self
                .shuffle_receiver
                .lock()
                .clone()
                .ok_or(TopologyError::Fenced)?;
            let mut parent = if fresh.plan().expected_parent == TopologyVersion::LEGACY_BASELINE {
                None
            } else {
                Some(
                    ShuffleTopologyFence::from_manifest(
                        fresh.plan().expected_parent,
                        &fresh.plan().parent_manifest,
                    )
                    .map_err(|error| TopologyError::Invalid(error.to_string()))?,
                )
            };
            if DbState::load(&self.state) == DbState::Created && sender.topology_fence().is_none() {
                parent = None;
            }
            if sender.local_id() != fresh.process().participant.node_id
                || sender.incarnation() != fresh.process().participant.boot_incarnation
                || sender.active_assignment_digest() != Some(fresh.assignment().digest())
                || receiver.active_assignment_digest() != Some(fresh.assignment().digest())
            {
                return Err(TopologyError::Fenced.into());
            }
            // Synchronous local publication shares the process/fault/lifecycle transition fence.
            {
                let _transition = self.cluster_authority_transition.lock();
                self.ensure_topology_transport_available(image)?;
                if controller.try_live_local_process_authority_identity().ok()
                    != Some(fresh.process())
                    || controller.is_draining()
                    || self
                        .startup_attempt
                        .lock()
                        .as_ref()
                        .is_some_and(|attempt| !attempt.is_complete())
                    || self
                        .runtime_handle
                        .try_lock()
                        .map_err(|_| TopologyError::Fenced)?
                        .is_some()
                    || !self.owned_source_tasks.lock().is_empty()
                    || !self.owned_sink_handles.lock().is_empty()
                    || !self.owned_connector_task_fences.lock().is_empty()
                {
                    return Err(TopologyError::Fenced.into());
                }
                image.graph.bind_cluster_topology_fence(target)?;
                self.source_gate.store(true, Ordering::Release);
                self.topology_cut_hold.store(true, Ordering::Release);
                if let Some(start) = &image.recovery_start {
                    sender.install_topology_fence_pair_for_recovery(
                        &receiver,
                        parent,
                        target,
                        start.round.id.generation,
                    )
                } else {
                    sender.install_topology_fence_pair(&receiver, parent, target)
                }
                .map_err(|error| TopologyError::Conflict(error.to_string()))?;
            }
            let after = self.topology_transport_input(image).await?;
            if !after.same_restore_requirements(&fresh) {
                return Err(TopologyError::Fenced.into());
            }
            self.ensure_topology_transport_available(image)?;
            Ok(())
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }
}
