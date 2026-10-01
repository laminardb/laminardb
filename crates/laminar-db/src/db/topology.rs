//! Read-only durable topology status. Runtime DDL remains guarded until cutover is implemented.

use laminar_core::cluster::control::{
    TopologyAdmissionStatus, TopologyCatalogState, TopologyOperationId, TopologyVersion,
};
use std::sync::atomic::Ordering;

use super::{DbError, DbState, LaminarDB};

mod migration_root;
mod planning;
mod preparation;
pub use planning::{
    ClusterTopologyObjectPlan, ClusterTopologyObjectTransition, ClusterTopologyValidation,
    TopologyActivationRequirement, TopologyInitialization, TopologyValidationScope,
};

/// Durable catalog version and this process's independently observed runtime activation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterTopologyStatus {
    /// Authority state, including an explicit unversioned legacy state.
    pub catalog: TopologyCatalogState,
    /// Committed logical version, absent for an unadopted or uninitialized catalog.
    pub committed_version: Option<TopologyVersion>,
    /// Version replayed by this process and running with live authority and intake released.
    /// A persisted inventory alone never sets this field.
    pub locally_active_version: Option<TopologyVersion>,
}

impl LaminarDB {
    /// Read an admitted request's definitive status; no request ownership depends on this call.
    ///
    /// # Errors
    /// Fails outside cluster mode or for unavailable/corrupt durable evidence.
    pub async fn cluster_topology_operation_status(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<Option<TopologyAdmissionStatus>, DbError> {
        if !self.is_cluster_runtime() {
            return Err(DbError::InvalidOperation(
                "cluster topology status requires cluster mode".into(),
            ));
        }
        let store = self.catalog_manifest_store.lock().clone().ok_or_else(|| {
            DbError::InvalidOperation("cluster topology status requires a catalog authority".into())
        })?;
        Ok(store.operation_status(operation_id).await?)
    }

    /// Read the cluster's durable topology and this process's local activation evidence.
    ///
    /// This is a control-path read, with no catalog mutation, checkpoint allocation or source
    /// effects. It does not declare that other participants have activated.
    ///
    /// # Errors
    /// Fails outside cluster mode or for unavailable, corrupt or inconsistent authority.
    pub async fn cluster_topology_status(&self) -> Result<ClusterTopologyStatus, DbError> {
        if !self.is_cluster_runtime() {
            return Err(DbError::InvalidOperation(
                "cluster topology status requires cluster mode".into(),
            ));
        }
        let store = self.catalog_manifest_store.lock().clone().ok_or_else(|| {
            DbError::InvalidOperation("cluster topology status requires a catalog authority".into())
        })?;
        let catalog = store.topology_state().await?;
        let committed_version = match &catalog {
            TopologyCatalogState::Versioned { baseline } => Some(baseline.topology_version),
            TopologyCatalogState::Uninitialized | TopologyCatalogState::LegacySealed { .. } => None,
        };
        let replayed_version = *self.replayed_topology_version.lock();
        let controller = self.cluster_controller.lock().clone();
        let locally_active_version = if DbState::load(&self.state) == DbState::Running
            && !self.source_gate.load(Ordering::Acquire)
            && !self.topology_cut_hold.load(Ordering::Acquire)
            && !self.cluster_authority_revoked.load(Ordering::Acquire)
            && !self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            && !self.terminal_pipeline_halt.load(Ordering::Acquire)
            && !self.coordinated_recovery_in_progress()
            && controller.as_ref().is_some_and(|controller| {
                controller.process_lease_is_live() && !controller.is_recovering()
            })
            && replayed_version == committed_version
        {
            replayed_version
        } else {
            None
        };
        Ok(ClusterTopologyStatus {
            catalog,
            committed_version,
            locally_active_version,
        })
    }
}
