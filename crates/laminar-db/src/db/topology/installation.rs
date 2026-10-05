//! Transfer an exact committed image into the existing, still-held startup lifecycle.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use laminar_core::cluster::control::{TopologyError, TopologyRestoreInput};
use laminar_core::shuffle::ShuffleTopologyFence;

use super::{DbError, DbState, LaminarDB, PreparedTopologyRestore};

impl LaminarDB {
    /// Install the exact committed catalog, restored graph and connector actors with intake held.
    /// The existing startup owner outlives caller cancellation. No target checkpoint, source
    /// acknowledgement, readiness receipt, external epoch admission or Release is invented.
    /// Success means the local control loop is installed, never that the topology is active.
    ///
    /// # Errors
    /// Rejects foreign/uncommitted/executed images, stale authority, unresolved predecessor work,
    /// unsupported atomic source starts, local faults and the 45-second installation budget.
    /// Failed installation retains Commit, the catalog and namespace ownership for root recovery.
    pub async fn install_committed_cluster_topology(
        self: &Arc<Self>,
        mut image: PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
        tokio::time::timeout_at(
            deadline,
            self.prepare_cluster_topology_transport(&mut image),
        )
        .await
        .map_err(|_| TopologyError::Contended)??;
        self.start_with_topology_image(image, deadline).await
    }

    pub(crate) async fn validate_topology_installation(
        &self,
        input: &TopologyRestoreInput,
    ) -> Result<(), DbError> {
        self.ensure_topology_runtime_held()?;
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let fresh = controller
            .committed_topology_restore_input(input.operation().operation_id)
            .await?;
        if if DbState::load(&self.state) == DbState::Starting {
            !fresh.same_restore_requirements(input)
        } else {
            !fresh.same_installed_generation(input)
        } {
            return Err(TopologyError::Fenced.into());
        }
        if controller.is_recovering() {
            return Err(TopologyError::Fenced.into());
        }
        self.validate_topology_transport_identity(input)?;
        self.ensure_topology_runtime_held()
    }

    pub(crate) fn validate_topology_transport_identity(
        &self,
        input: &TopologyRestoreInput,
    ) -> Result<(), DbError> {
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let target = ShuffleTopologyFence::from_manifest(
            input.descriptor().target_version,
            &input.plan().target_manifest,
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
        let registry = self
            .vnode_registry
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let assignment = registry.versioned_snapshot();
        let owners = assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        if assignment.version() != input.assignment().assignment_version
            || !input.assignment().matches_owner_map(&owners)
            || sender.topology_fence() != Some(target)
            || receiver.topology_fence() != Some(target)
            || sender.active_assignment_digest() != Some(input.assignment().digest())
            || receiver.active_assignment_digest() != Some(input.assignment().digest())
            || controller.try_live_local_process_authority_identity().ok() != Some(input.process())
            || controller.is_draining()
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }

    pub(crate) fn ensure_topology_installation_held(&self) -> Result<(), DbError> {
        if DbState::load(&self.state) != DbState::Starting {
            return Err(TopologyError::Fenced.into());
        }
        self.ensure_topology_runtime_held()
    }

    pub(crate) fn ensure_topology_runtime_held(&self) -> Result<(), DbError> {
        if self.is_closed() {
            return Err(DbError::Shutdown);
        }
        if !self.is_cluster_runtime()
            || !matches!(
                DbState::load(&self.state),
                DbState::Starting | DbState::Running
            )
            || !self.source_gate.load(Ordering::Acquire)
            || !self.topology_cut_hold.load(Ordering::Acquire)
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || self.coordinated_recovery_in_progress()
            || self.pending_recovery_fault.load(Ordering::Acquire) != 0
            || self.last_fault.lock().is_some()
        {
            return Err(TopologyError::Fenced.into());
        }
        self.ensure_catalog_cleanup_unfenced("committed topology installation")
    }
}
