//! Durable topology status, admission and exact committed runtime lifecycle.

use laminar_core::cluster::control::{
    TopologyAdmissionStatus, TopologyCatalogState, TopologyOperationId, TopologyVersion,
};
use std::sync::atomic::Ordering;

use super::{DbError, DbState, LaminarDB};

mod activation;
pub(super) mod catalog_changes;
mod commit;
pub(crate) use activation::InstalledTopologyRuntime;
mod forwarding;
mod installation;
mod migration_root;
mod planning;
mod preparation;
mod recovery;
mod recovery_runtime;
mod restore;
mod retirement;
mod submission;
pub use submission::{ClusterTopologyAdoptionRequest, ClusterTopologyRequest};
mod target_preparation;
mod transport;
pub use planning::{
    ClusterTopologyObjectPlan, ClusterTopologyObjectTransition, ClusterTopologyValidation,
    TopologyActivationRequirement, TopologyInitialization, TopologyValidationScope,
};
pub(crate) use restore::TopologyRuntimeMetadata;
pub use restore::{PreparedTopologyRestore, PreparedTopologySourcePosition};

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
        let committed_version = catalog.committed_version();
        let replayed_version = *self.replayed_topology_version.lock();
        let controller = self.cluster_controller.lock().clone();
        let release_observed = match &catalog {
            TopologyCatalogState::Versioned {
                committed: Some(commit),
                ..
            } => {
                let binding = self.installed_topology_runtime.lock().clone();
                match binding.filter(|binding| {
                    binding.input.operation().commit.as_ref() == Some(commit)
                        && !binding.shutdown.is_cancelled()
                        && self.owned_source_tasks.lock().iter().all(
                            crate::pipeline::streaming_coordinator::SourceTaskLease::is_running,
                        )
                        && self
                            .owned_sink_handles
                            .lock()
                            .iter()
                            .all(crate::sink_task::SinkTaskHandle::is_ready)
                }) {
                    Some(binding) if binding.recovery.is_some() => {
                        self.recovered_topology_runtime_is_active(&binding).await?
                    }
                    Some(binding) => store
                        .operation_status(commit.operation_id)
                        .await?
                        .is_some_and(|status| {
                            status.phase
                                == laminar_core::cluster::control::TopologyAdmissionPhase::Active
                                && status.activation.as_ref().is_some_and(|round| {
                                    round.release.as_ref().is_some_and(|release| {
                                        Some(release.authority_sequence)
                                            == binding.released_sequence
                                    }) && round.installations.iter().any(|receipt| {
                                        receipt.runtime_id == binding.runtime_id
                                            && receipt.process == binding.input.process()
                                    })
                                })
                        }),
                    None => false,
                }
            }
            _ => true,
        };
        let locally_active_version = if DbState::load(&self.state) == DbState::Running
            && release_observed
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

impl LaminarDB {
    pub(super) async fn validate_sealed_topology_bootstrap(
        &self,
        manifest: &laminar_core::cluster::control::CatalogManifest,
        parsed: &[(
            String,
            laminar_sql::parser::StreamingStatement,
            String,
            laminar_core::cluster::control::CatalogObjectKind,
        )],
    ) -> Result<Vec<crate::handle::ExecuteResult>, DbError> {
        let matches = |catalog: &laminar_core::cluster::control::CatalogManifest| {
            parsed.len() == catalog.entries.len()
                && parsed
                    .iter()
                    .zip(&catalog.entries)
                    .all(|((ddl, _, name, kind), entry)| {
                        ddl == &entry.ddl && name == &entry.canonical_name && kind == &entry.kind
                    })
        };
        if !matches(manifest) {
            let store = self.catalog_manifest_store.lock().clone().ok_or_else(|| {
                DbError::Pipeline("cluster catalog manifest store is not configured".into())
            })?;
            let original = store
                .original_topology_catalog(
                    &manifest
                        .reference()
                        .map_err(laminar_core::cluster::control::TopologyError::from)?,
                )
                .await?;
            if !original.as_ref().is_some_and(matches) {
                return Err(DbError::Pipeline(format!(
                    "configured cluster catalog must exactly match the complete ordered sealed inventory or its original adopted bootstrap (configured entries: {}, sealed entries: {})",
                    parsed.len(), manifest.entries.len()
                )));
            }
        }
        parsed
            .iter()
            .map(|(_, statement, _, _)| {
                let (name, _, statement_type) = super::catalog_create_identity(statement)?
                    .ok_or_else(|| {
                        DbError::InvalidOperation(
                            "bootstrap assertion requires typed CREATE statements".into(),
                        )
                    })?;
                Ok(crate::handle::ExecuteResult::Ddl(crate::handle::DdlInfo {
                    statement_type: statement_type.to_string(),
                    object_name: name,
                    topology_operation: None,
                    applied: false,
                }))
            })
            .collect()
    }
}
