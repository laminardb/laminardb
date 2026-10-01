//! DB-owned exact-cut metadata staging for the future migration driver.

use super::{DbError, LaminarDB, TopologyAdmissionStatus, TopologyOperationId};
use laminar_core::checkpoint::ObjectStoreCheckpointStore;
use laminar_core::cluster::control::TopologyError;
use std::sync::atomic::Ordering;

impl LaminarDB {
    /// Durably stage preserved state/progress requirements for a prepared downstream addition.
    /// Requires the current leader's running parent and held cut. The configured checkpoint
    /// reader supplies metadata only; no state is copied or restored and no target actor starts.
    /// New sources remain rejected until concrete connector positions can be persisted once.
    ///
    /// # Errors
    /// Rejects a missing held cut, shutdown/recovery/leader/process fences, damaged metadata,
    /// unsupported initialization or a 30 second total deadline (authority publication is bounded
    /// to 15 seconds/16 CAS attempts). A cancelled caller may have durably staged the root; query
    /// operation status or retry the same operation identity.
    pub async fn stage_cluster_topology_migration_root(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            self.ensure_topology_root_available()?;
            let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                TopologyError::Protocol("root staging requires the configured controller".into())
            })?;
            let authority = controller
                .checkpoint_authority()
                .map_err(|e| TopologyError::Protocol(e.to_string()))?;
            let operation = authority
                .topology_operation_status(operation_id)
                .await?
                .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
            let backing = self.checkpoint_object_store()?.ok_or_else(|| {
                TopologyError::Protocol("root staging requires checkpoint storage".into())
            })?;
            let max_bytes = self
                .config
                .checkpoint
                .as_ref()
                .and_then(|c| c.max_node_data_bytes)
                .unwrap_or(laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES);
            let store = ObjectStoreCheckpointStore::new(backing, "")
                .with_key_group_count(self.checkpoint_key_groups())
                .with_max_node_data_bytes(max_bytes)?;
            let status = controller
                .stage_topology_migration_root(&store, operation_id, &operation.plan)
                .await?;
            self.ensure_topology_root_available()?;
            Ok(status)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    fn ensure_topology_root_available(&self) -> Result<(), DbError> {
        self.ensure_topology_preparation_available()?;
        if !self.topology_cut_hold.load(Ordering::Acquire)
            || !self.source_gate.load(Ordering::Acquire)
        {
            return Err(TopologyError::Conflict(
                "root staging requires the prepared old-topology intake hold".into(),
            )
            .into());
        }
        Ok(())
    }
}
