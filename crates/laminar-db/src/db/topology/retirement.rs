//! Observe the held parent generation through existing lifecycle and connector owners.

use std::sync::atomic::Ordering;
use std::time::Duration;

use laminar_core::cluster::control::{TopologyError, TopologyRestoreInput};

use super::{DbError, DbState, LaminarDB, PreparedTopologyRestore};

impl LaminarDB {
    /// Retire the old actors for this database's privately restored exact-root target.
    ///
    /// Revalidates the full current preparation roster, leader, process and assignment before
    /// signalling stop and after observing completion. Existing lifecycle code joins compute,
    /// settles decision/sink work and observes source, sink and connector-child termination.
    /// No request to cancel or close is accepted as terminal proof.
    ///
    /// The parent catalog/coordinator identity, cut hold and namespace lock remain owned, with
    /// the runtime in `ShuttingDown`. The target stays unstarted. No receipt or authority append,
    /// topology Commit, target install or Release occurs. Public start/stop remain fenced.
    ///
    /// The total budget is 45 seconds. Cancellation or error retains unresolved DB-owned handles
    /// and the held boundary; retry with the same image, or use coordinated recovery after abort.
    /// Dropping the image never reopens intake. Its observation must be revalidated at installation.
    ///
    /// # Errors
    /// Rejects another database's image, stale authority, loss of the held parent, concurrent
    /// recovery/shutdown/fault, and unobserved runtime or connector termination.
    pub async fn retire_cluster_topology_parent(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        if !image.belongs_to(self) {
            return Err(TopologyError::Conflict(
                "target image belongs to a different database runtime".into(),
            )
            .into());
        }
        image.parent_retirement_observed = false;
        tokio::time::timeout(Duration::from_secs(45), async {
            self.validate_topology_retirement_input(&image.input)
                .await?;
            self.stop_pipeline_for_topology_retirement().await?;
            self.validate_topology_retirement_input(&image.input)
                .await?;
            if DbState::load(&self.state) != DbState::ShuttingDown
                || !self.runtime_shutdown.read().is_cancelled()
            {
                return Err(TopologyError::Fenced.into());
            }
            image.parent_retirement_observed = true;
            Ok(())
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    pub(super) async fn validate_topology_retirement_input(
        &self,
        input: &TopologyRestoreInput,
    ) -> Result<(), DbError> {
        self.ensure_topology_retirement_available()?;
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            TopologyError::Protocol("retirement requires the configured controller".into())
        })?;
        if !controller
            .topology_restore_input(input.operation().operation_id)
            .await?
            .same_restore_requirements(input)
        {
            return Err(TopologyError::Fenced.into());
        }
        self.validate_bound_parent_pipeline(&input.descriptor().parent_pipeline)
            .await?;
        if self.catalog_manifest_inventory()? != input.parent().entries
            || self.topology_definition_identities()?.pipeline != input.descriptor().parent_pipeline
        {
            return Err(TopologyError::Fenced.into());
        }
        self.ensure_topology_retirement_available()?;
        // The coordinator lock above can suspend after the durable input read. Recheck the
        // exact local process at the final synchronous boundary, before recording observation.
        if controller.is_recovering()
            || controller.is_draining()
            || controller.try_live_local_process_authority_identity().ok() != Some(input.process())
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }

    pub(super) fn ensure_topology_retirement_available(&self) -> Result<(), DbError> {
        if self.is_closed() {
            return Err(DbError::Shutdown);
        }
        if !self.is_cluster_runtime()
            || !matches!(
                DbState::load(&self.state),
                DbState::Running | DbState::ShuttingDown
            )
            || !self.topology_cut_hold.load(Ordering::Acquire)
            || !self.source_gate.load(Ordering::Acquire)
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || self.coordinated_recovery_in_progress()
            || self.pending_recovery_fault.load(Ordering::Acquire) != 0
            || self.last_fault.lock().is_some()
        {
            return Err(TopologyError::Conflict(
                "parent retirement requires the exact held runtime without a recovery or fault"
                    .into(),
            )
            .into());
        }
        self.ensure_catalog_cleanup_unfenced("topology retirement")
    }
}
