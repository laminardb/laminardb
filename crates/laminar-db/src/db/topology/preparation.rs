//! Local compilation owns certificate publication; caller-supplied reports are never trusted.

use super::{DbError, DbState, LaminarDB, TopologyAdmissionStatus, TopologyOperationId};
use laminar_core::cluster::control::{TopologyAdmissionPhase, TopologyError};
use std::sync::atomic::Ordering;

impl LaminarDB {
    /// Independently compile and durably certify an already admitted operation on this process.
    /// This neither submits a mutation nor closes intake, restores state or authorizes target output.
    /// The exact target and descriptor come from immutable shared authority, never the caller.
    /// A disconnect may leave a successful certificate; query status or retry the same operation.
    ///
    /// # Errors
    /// Requires Running state, live process/assignment authority, an adopted exact parent and a
    /// matching compiled candidate. One existing compiler slot and a 45 second end-to-end deadline
    /// bound read/compile/publication; publication retains the authority's 15 second/16-CAS bounds.
    pub async fn prepare_cluster_topology_operation(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        tokio::time::timeout(std::time::Duration::from_secs(45), async {
            self.ensure_topology_preparation_available()?;
            let controller = self.cluster_controller.lock().clone().ok_or_else(|| TopologyError::Protocol("preparation requires the configured cluster controller".into()))?;
            let before = controller.try_live_local_process_authority_identity().map_err(|_| TopologyError::Fenced)?;
            let authority = controller.checkpoint_authority().map_err(|error| TopologyError::Protocol(error.to_string()))?;
            let (operation, plan, _target, descriptor) = authority.topology_preparation_input(operation_id).await?;
            if !matches!(operation.phase, TopologyAdmissionPhase::Planned | TopologyAdmissionPhase::Preparing | TopologyAdmissionPhase::Quiescing | TopologyAdmissionPhase::CutPrepared) {
                return Err(TopologyError::Conflict("aborted topology operation cannot prepare participants".into()).into());
            }
            if plan.assignment.participant_incarnation(before.participant.node_id) != Some(before.participant.boot_incarnation) {
                return Err(TopologyError::Fenced.into());
            }
            let compiled = self.validate_cluster_topology_change(plan.expected_parent, &descriptor.statements).await?;
            if compiled != descriptor {
                return Err(TopologyError::Conflict("this process compiled a divergent candidate; check binary, config, catalog and connector versions".into()).into());
            }
            self.ensure_topology_preparation_available()?;
            let status = controller.certify_topology_candidate(operation_id, &operation.plan, before, &compiled).await?;
            self.ensure_topology_preparation_available()?;
            Ok(status)
        }).await.map_err(|_| TopologyError::Contended)?
    }

    pub(super) fn ensure_topology_preparation_available(&self) -> Result<(), DbError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(DbError::Shutdown);
        }
        if !self.is_cluster_runtime()
            || DbState::load(&self.state) != DbState::Running
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || self.coordinated_recovery_in_progress()
        {
            return Err(TopologyError::Conflict("participant preparation requires a running parent with live process and recovery authority".into()).into());
        }
        self.ensure_catalog_cleanup_unfenced("topology preparation")
    }
}
