//! Cold committed-target startup initializes control state and delegates actors to recovery.

use std::sync::atomic::Ordering;

use laminar_core::cluster::control::{TopologyCatalogState, TopologyError};

use super::{DbError, DbState, LaminarDB, RuntimeMode};

impl LaminarDB {
    /// Prepare a cold committed target for the existing coordinated recovery supervisor.
    /// Returns true when that supervisor must restore and release the actual target actors;
    /// false means there is no topology Commit and ordinary startup remains applicable.
    /// No actors, sink epochs or intake are started here. The DB remains Created and fenced.
    ///
    /// # Errors
    /// Rejects live/closed/terminal instances, missing deployment identity, divergent target
    /// inventory/environment, absent checkpoint storage or stale process/catalog authority.
    pub async fn prepare_committed_cluster_topology_startup(&self) -> Result<bool, DbError> {
        if !self.is_cluster_runtime() {
            return Ok(false);
        }
        let catalog = self
            .catalog_manifest_store
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let state = catalog.topology_state().await?;
        let TopologyCatalogState::Versioned {
            committed: Some(commit),
            ..
        } = state.clone()
        else {
            return Ok(false);
        };
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let authority = controller
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        let (operation, _, target, descriptor) = authority
            .topology_preparation_input(commit.operation_id)
            .await?;
        if operation.commit.as_ref() != Some(&commit)
            || !controller.process_lease_is_live()
            || controller.is_draining()
        {
            return Err(TopologyError::Fenced.into());
        }
        self.fence_coordinated_recovery_lifecycle();
        controller.set_recovering(true);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
        tokio::time::timeout_at(deadline, async {
            let _topology = self.topology_ddl_lock.write().await;
            let _lifecycle = self.lifecycle_lock.lock().await;
            self.ensure_catalog_cleanup_unfenced("cold committed topology startup")?;
            if self.is_closed()
                || DbState::load(&self.state) != DbState::Created
                || self.cluster_authority_revoked.load(Ordering::Acquire)
                || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
                || self.terminal_pipeline_halt.load(Ordering::Acquire)
                || self
                    .startup_attempt
                    .lock()
                    .as_ref()
                    .is_some_and(|attempt| !attempt.is_complete())
                || self.runtime_handle.lock().await.is_some()
                || !self.owned_source_tasks.lock().is_empty()
                || !self.owned_sink_handles.lock().is_empty()
                || !self.owned_connector_task_fences.lock().is_empty()
            {
                return Err(TopologyError::Fenced.into());
            }
            // Cold catalog bootstrap may replay the committed inventory, but runtime migration
            // never uses bootstrap mutation authority. The following round still owns all actors.
            self.restore_catalog_from_manifest().await?;
            let identities = self.topology_definition_identities()?;
            if self.catalog_manifest_inventory()? != target.entries
                || identities.pipeline != descriptor.target_pipeline
                || identities.environment_sha256 != descriptor.environment_sha256
            {
                return Err(TopologyError::Fenced.into());
            }
            let (sources, sinks, streams, tables) = {
                let manager = self.connector_manager.lock();
                (
                    manager.sources().clone(),
                    manager.sinks().clone(),
                    manager.streams().clone(),
                    manager.tables().clone(),
                )
            };
            let backing = self.validate_startup_durability(RuntimeMode::Cluster)?;
            let _assignment = self.assignment_adoption_lock.lock().await;
            let pipeline = self
                .initialize_checkpointing(
                    crate::pipeline_identity::PipelineRegistrations::new(
                        sources.values(),
                        sinks.values(),
                        streams.values(),
                        tables.values(),
                    ),
                    RuntimeMode::Cluster,
                    backing,
                    Some(&descriptor.deployment_id),
                )
                .await?;
            if pipeline.as_ref() != Some(&descriptor.target_pipeline)
                || self.coordinator.lock().await.is_none()
                || catalog.topology_state().await? != state
            {
                return Err(TopologyError::Fenced.into());
            }
            if catalog.topology_state().await?.committed_version() != Some(commit.topology_version)
                || authority
                    .topology_operation_status(commit.operation_id)
                    .await?
                    .as_ref()
                    .and_then(|status| status.commit.as_ref())
                    != Some(&commit)
                || !controller.process_lease_is_live()
                || controller.is_draining()
            {
                return Err(TopologyError::Fenced.into());
            }
            {
                let _transition = self.cluster_authority_transition.lock();
                self.topology_cut_hold.store(true, Ordering::Release);
                self.source_gate.store(true, Ordering::Release);
            }
            crate::coordinated_recovery::queue_local_fault(
                &controller,
                &self.pending_recovery_fault,
            )
            .map_err(DbError::Checkpoint)?;
            Ok(true)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }
}
