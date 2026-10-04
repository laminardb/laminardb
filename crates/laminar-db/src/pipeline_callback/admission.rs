//! Cold checkpoint admission and authority audits.

use super::{Arc, ConnectorPipelineCallback};

impl ConnectorPipelineCallback {
    #[cfg(feature = "cluster")]
    // None defers incomplete participant preparation or an already held cut without a fault.
    pub(super) async fn checkpoint_flags_for_assignment(
        controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<u64>, String> {
        let Some(controller) = controller else {
            return if assignment_fence.is_none() {
                Ok(Some(laminar_core::checkpoint::flags::NONE))
            } else {
                Err("local checkpoint received a cluster assignment fence".into())
            };
        };
        let Some(transition) = controller.checkpoint_drain_transition() else {
            if controller.has_leader_lease_fencing() {
                let proof = controller
                    .capture_leader_proof()
                    .ok_or_else(|| "checkpoint cut has no live leader proof".to_string())?;
                let fence = assignment_fence
                    .as_ref()
                    .ok_or_else(|| "checkpoint cut has no assignment fence".to_string())?;
                let authority = controller
                    .checkpoint_authority()
                    .map_err(|error| error.to_string())?;
                if let Some(operation) = tokio::time::timeout_at(
                    deadline,
                    authority.topology_checkpoint_operation(&proof, fence),
                )
                .await
                .map_err(|_| "topology checkpoint admission timed out".to_string())?
                .map_err(|error| error.to_string())?
                {
                    if operation.phase == laminar_core::cluster::control::topology::TopologyAdmissionPhase::CutPrepared {
                        return Ok(None);
                    }
                    if matches!(operation.phase, laminar_core::cluster::control::topology::TopologyAdmissionPhase::Planned | laminar_core::cluster::control::topology::TopologyAdmissionPhase::Preparing)
                        && operation.preparation.as_ref().is_none_or(|preparation| preparation.complete_sequence.is_none()) {
                        return Ok(None);
                    }
                    return Ok(Some(laminar_core::checkpoint::flags::TOPOLOGY_CUT));
                }
            }
            return Ok(Some(laminar_core::checkpoint::flags::NONE));
        };
        let fence = assignment_fence.as_ref().ok_or_else(|| {
            "active assignment drain has no admitted predecessor fence".to_string()
        })?;
        let leader = controller
            .capture_leader_proof()
            .ok_or_else(|| "active assignment drain has no live leader proof".to_string())?;
        if transition.predecessor != *fence || transition.leader != leader {
            return Err("checkpoint admission does not match the active assignment drain".into());
        }
        let quorum_ready =
            tokio::time::timeout_at(deadline, controller.drain_ack_quorum_reached(&transition))
                .await
                .map_err(|_| "HANDOFF readiness audit timed out".to_string())?
                .map_err(|error| format!("HANDOFF readiness audit failed: {error}"))?;
        if !quorum_ready {
            return Err("active assignment drain is not HANDOFF-ready".into());
        }
        // The readiness audit performs durable I/O. Re-read the process-local transition and
        // lease afterward so a concurrent watcher clear or leadership change cannot authorize a
        // checkpoint from the stale observation.
        if controller.checkpoint_drain_transition().as_ref() != Some(&transition)
            || controller.capture_leader_proof().as_ref() != Some(&transition.leader)
            || !controller.proof_is_live(&transition.leader)
        {
            return Err("assignment drain authority changed during HANDOFF readiness audit".into());
        }
        Ok(Some(laminar_core::checkpoint::flags::HANDOFF))
    }

    #[cfg(feature = "cluster")]
    pub(super) async fn checkpoint_assignment_for_admission_inner(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> crate::pipeline::CheckpointAssignmentAdmission {
        use crate::pipeline::CheckpointAssignmentAdmission;

        let Some(controller) = self.cluster_controller.clone() else {
            return CheckpointAssignmentAdmission::Ready {
                assignment_fence: None,
                flags: laminar_core::checkpoint::flags::NONE,
                assignment_guard: None,
            };
        };
        let Ok(assignment_guard) = tokio::time::timeout_at(
            deadline,
            Arc::clone(&self.assignment_adoption_lock).lock_owned(),
        )
        .await
        else {
            return CheckpointAssignmentAdmission::Deferred(
                "checkpoint admission timed out waiting for assignment serialization".into(),
            );
        };
        let Some(registry) = self.vnode_registry.clone() else {
            tracing::error!(
                "cluster checkpoint admission has no vnode registry; failing assignment fence"
            );
            return CheckpointAssignmentAdmission::Fault(
                "cluster checkpoint admission has no vnode registry".into(),
            );
        };
        let publication = registry.versioned_snapshot();
        // The snapshot watcher performs the gossip scan off the hot path. Retain the exact
        // certificate so later capture/quorum/durable phases cannot silently switch generations.
        let Some(fence) = controller.checkpoint_assignment_fence(publication.version()) else {
            return CheckpointAssignmentAdmission::Deferred(format!(
                "assignment {} is not checkpoint-ready",
                publication.version()
            ));
        };
        let verified = registry.versioned_snapshot();
        if verified.version() != publication.version() {
            return CheckpointAssignmentAdmission::Deferred(
                "assignment changed while checkpoint admission was being certified".into(),
            );
        }
        let drain_was_active = controller.checkpoint_drain_transition().is_some();
        let flags = match Self::checkpoint_flags_for_assignment(
            Some(Arc::clone(&controller)),
            Some(fence.clone()),
            deadline,
        )
        .await
        {
            Ok(Some(flags)) => flags,
            Ok(None) => {
                return CheckpointAssignmentAdmission::Deferred(
                    "topology participants are preparing or the cut remains held".into(),
                )
            }
            Err(error)
                if drain_was_active || controller.checkpoint_drain_transition().is_some() =>
            {
                return CheckpointAssignmentAdmission::Deferred(error);
            }
            Err(error) => return CheckpointAssignmentAdmission::Fault(error),
        };
        if registry.assignment_version() != publication.version()
            || controller
                .checkpoint_assignment_fence(publication.version())
                .as_ref()
                != Some(&fence)
        {
            return CheckpointAssignmentAdmission::Deferred(
                "assignment changed during HANDOFF readiness audit".into(),
            );
        }
        CheckpointAssignmentAdmission::Ready {
            assignment_fence: Some(fence),
            flags,
            assignment_guard: Some(assignment_guard),
        }
    }
}
