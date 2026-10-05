//! DB-owned exact-cut metadata staging for the future migration driver.

use super::{DbError, LaminarDB, TopologyAdmissionStatus, TopologyOperationId};
use laminar_core::checkpoint::ObjectStoreCheckpointStore;
use laminar_core::cluster::control::topology::ClusterTopologyObjectTransition;
use laminar_core::cluster::control::{
    CatalogManifest, CatalogObjectKind, ClusterTopologyValidation, TopologyError,
    TopologySourceInitialization,
};
use std::sync::atomic::Ordering;

impl LaminarDB {
    /// Durably stage preserved state/progress and source initialization for a prepared addition.
    /// Requires the current leader's running parent and held cut. The configured checkpoint
    /// reader supplies metadata only; no state is copied or restored and no target actor starts.
    /// New sources use their configured connector's read-only initialization contract. The first
    /// sealed global cursor survives a disconnect before publication and is never resolved again.
    /// Built-in Kafka supports explicit topics with earliest/latest; other connectors fail closed.
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
            let _compiler = self.topology_validation_lock.try_lock()
                .map_err(|_| TopologyError::PlanningBusy)?;
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
                .stage_topology_migration_root_with_initialization(
                    &store, operation_id, &operation.plan,
                    |target, descriptor| self.resolve_topology_source_initializations(target, descriptor),
                )
                .await?;
            self.ensure_topology_root_available()?;
            Ok(status)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    async fn resolve_topology_source_initializations(
        &self,
        target: CatalogManifest,
        descriptor: ClusterTopologyValidation,
    ) -> Result<Vec<TopologySourceInitialization>, TopologyError> {
        let map_error = |error| match error {
            DbError::Topology(error) => error,
            error => TopologyError::Invalid(error.to_string()),
        };
        self.ensure_topology_root_available().map_err(map_error)?;
        let candidate = self.isolated_topology_catalog().map_err(map_error)?;
        for entry in &target.entries {
            super::planning::replay_entry(&candidate, entry)
                .await
                .map_err(map_error)?;
        }
        if candidate
            .reconcile_catalog_manifest_inventory(&target)
            .map_err(map_error)?
            != target.entries
        {
            return Err(TopologyError::Invalid(
                "isolated target replay changed its exact inventory or incarnations".into(),
            ));
        }
        // Resolve the immutable target with the exact configured factories/environment. No graph,
        // source actor or sink is installed, and no historical state/data payload is copied.
        let identities = candidate
            .topology_definition_identities()
            .map_err(map_error)?;
        if identities.pipeline != descriptor.target_pipeline
            || identities.environment_sha256 != descriptor.environment_sha256
        {
            return Err(TopologyError::Conflict(
                "resolved source configuration differs from the certified target; prepare a new operation".into(),
            ));
        }
        let sources = candidate.connector_manager.lock().sources().clone();
        let mut initialized = Vec::new();
        for object in descriptor.objects.iter().filter(|object| {
            object.kind == CatalogObjectKind::Source
                && object.transition == ClusterTopologyObjectTransition::AddFutureOnly
        }) {
            self.ensure_topology_root_available().map_err(map_error)?;
            let registration = sources.get(&object.name).ok_or_else(|| {
                TopologyError::Invalid(format!(
                    "new source '{}' has no configured connector",
                    object.name
                ))
            })?;
            let config = candidate
                .build_registered_source_config(&object.name, registration)
                .map_err(map_error)?;
            let mut connector = candidate
                .connector_registry
                .create_source(&config, None)
                .map_err(|e| {
                    TopologyError::Unsupported(format!("source '{}': {e}", object.name))
                })?;
            let checkpoint = connector
                .resolve_initial_position(&config)
                .await
                .map_err(|e| {
                    TopologyError::Unsupported(format!(
                        "source '{}' initialization: {e}",
                        object.name
                    ))
                })?;
            initialized.push(TopologySourceInitialization {
                name: object.name.clone(),
                catalog_generation: object.catalog_generation,
                compatibility_sha256: object.compatibility_sha256.clone(),
                checkpoint: crate::checkpoint_coordinator::source_to_connector_checkpoint(
                    &checkpoint,
                ),
            });
            // Connector drop retains its existing terminal-task ownership. Metadata clients are
            // created/destroyed inside tracked blocking work; no lifecycle cleanup future is added.
        }
        self.ensure_topology_root_available().map_err(map_error)?;
        Ok(initialized)
    }

    pub(super) fn ensure_topology_root_available(&self) -> Result<(), DbError> {
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
