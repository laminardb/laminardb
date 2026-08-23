use std::sync::Arc;

use arrow::array::RecordBatch;
use rustc_hash::FxHashMap;

use super::{set_checkpoint_fault, ConnectorPipelineCallback};
use crate::pipeline::{CycleError, PipelineCallback};

impl ConnectorPipelineCallback {
    pub(super) async fn publish_checkpoint_drain_results(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        deadline: tokio::time::Instant,
    ) -> Result<(), CycleError> {
        #[cfg(feature = "cluster")]
        {
            let subscription_outputs = self.graph.take_prepared_subscription_outputs();
            if let Err(error) = self.stage_subscription_outputs(subscription_outputs) {
                self.abort_prepared_subscription_output_cycle();
                return Err(error);
            }
        }
        let (any_failed, _) = self.graph.take_cycle_failures();
        if any_failed {
            #[cfg(feature = "cluster")]
            self.abort_prepared_subscription_output_cycle();
            let reason =
                "checkpoint graph drain encountered a partial operator-domain failure".to_string();
            set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
            return Err(CycleError::Recovery(reason));
        }
        // Checkpoint quiescence permits barrier-aligned shuffle replay; consume only the normal
        // cycle report here because the retained channel state is captured by the operator.
        let _ = self.graph.take_cycle_deferrals();

        #[cfg(feature = "cluster")]
        self.require_checkpoint_output_authority("materialized-view")?;
        if let Err(error) = <Self as PipelineCallback>::update_mv_stores(self, results) {
            #[cfg(feature = "cluster")]
            self.abort_prepared_subscription_output_cycle();
            let reason = format!(
                "checkpoint graph drain could not publish materialized-view output: {error}"
            );
            set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
            return Err(CycleError::Recovery(reason));
        }

        #[cfg(feature = "cluster")]
        self.require_checkpoint_output_authority("stream")?;
        if let Err(error) = <Self as PipelineCallback>::push_to_streams(self, results) {
            #[cfg(feature = "cluster")]
            self.abort_prepared_subscription_output_cycle();
            let reason = format!("checkpoint graph drain could not publish stream output: {error}");
            set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
            return Err(CycleError::Recovery(reason));
        }

        #[cfg(feature = "cluster")]
        self.require_checkpoint_output_authority("sink")?;
        if let Err(error) =
            <Self as PipelineCallback>::write_to_sinks(self, results, Some(deadline)).await
        {
            #[cfg(feature = "cluster")]
            self.abort_prepared_subscription_output_cycle();
            if let CycleError::Recovery(reason) = &error {
                set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
            }
            return Err(error);
        }

        #[cfg(feature = "cluster")]
        {
            self.require_checkpoint_output_authority("continuation")?;
            self.commit_prepared_subscription_output_cycle();
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn require_checkpoint_output_authority(&mut self, output: &str) -> Result<(), CycleError> {
        if let Err(error) =
            self.require_process_authority(&format!("checkpoint {output} publication"))
        {
            self.abort_prepared_subscription_output_cycle();
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(super) fn stage_subscription_outputs(
        &mut self,
        outputs: Vec<crate::subscription::PreparedSubscriptionOutput>,
    ) -> Result<(), CycleError> {
        if outputs.is_empty() {
            return Ok(());
        }
        let authority = self.subscription_writer_authority(&outputs)?;
        self.cluster_subscription_output
            .stage_cycle(outputs, authority)
            .map_err(|error| {
                let reason = format!("stage cluster subscription output: {error}");
                set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                CycleError::Recovery(reason)
            })
    }

    #[cfg(feature = "cluster")]
    fn subscription_writer_authority(
        &mut self,
        outputs: &[crate::subscription::PreparedSubscriptionOutput],
    ) -> Result<crate::subscription::cluster::OutputWriterAuthority, CycleError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            CycleError::Recovery("cluster subscription output has no controller".into())
        })?;
        let registry = self.vnode_registry.as_ref().ok_or_else(|| {
            CycleError::Recovery("cluster subscription output has no vnode registry".into())
        })?;
        let process = controller
            .try_live_local_process_authority_identity()
            .map_err(|error| CycleError::Recovery(format!("stale subscription writer: {error}")))?;
        let assignment_version = registry.assignment_version();
        let assignment = controller
            .checkpoint_assignment_fence(assignment_version)
            .ok_or_else(|| {
                CycleError::Recovery(format!(
                    "stale subscription writer: assignment {assignment_version} is not certified"
                ))
            })?;
        if assignment.participant_incarnation(process.participant.node_id)
            != Some(process.participant.boot_incarnation)
        {
            return Err(CycleError::Recovery(
                "stale subscription writer: process incarnation is not assignment-certified".into(),
            ));
        }
        for frame in outputs.iter().flat_map(|output| &output.frames) {
            if registry.owner(u32::from(frame.id.partition.get())).0 != process.participant.node_id
            {
                return Err(CycleError::Recovery(format!(
                    "stale subscription writer does not own output partition {}",
                    frame.id.partition.get()
                )));
            }
        }
        Ok(crate::subscription::cluster::OutputWriterAuthority {
            participant: process.participant,
            process_term: process.process_term,
            assignment_version,
            assignment_digest: assignment.digest(),
        })
    }

    #[cfg(feature = "cluster")]
    pub(super) fn commit_prepared_subscription_output_cycle(&mut self) {
        self.cluster_subscription_output.commit_cycle();
        self.graph.commit_prepared_subscription_outputs();
    }

    #[cfg(feature = "cluster")]
    pub(super) fn abort_prepared_subscription_output_cycle(&mut self) {
        self.cluster_subscription_output.abort_cycle();
        self.graph.abort_prepared_subscription_outputs();
    }
}
