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
                self.graph.abort_prepared_subscription_outputs();
                return Err(error);
            }
        }
        let (any_failed, _) = self.graph.take_cycle_failures();
        if any_failed {
            #[cfg(feature = "cluster")]
            self.graph.abort_prepared_subscription_outputs();
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
            self.graph.abort_prepared_subscription_outputs();
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
            self.graph.abort_prepared_subscription_outputs();
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
            self.graph.abort_prepared_subscription_outputs();
            if let CycleError::Recovery(reason) = &error {
                set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
            }
            return Err(error);
        }

        #[cfg(feature = "cluster")]
        {
            self.require_checkpoint_output_authority("continuation")?;
            self.graph.commit_prepared_subscription_outputs();
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn require_checkpoint_output_authority(&mut self, output: &str) -> Result<(), CycleError> {
        if let Err(error) =
            self.require_process_authority(&format!("checkpoint {output} publication"))
        {
            self.graph.abort_prepared_subscription_outputs();
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(super) fn stage_subscription_outputs(
        &mut self,
        outputs: Vec<crate::subscription::PreparedSubscriptionOutput>,
    ) -> Result<(), CycleError> {
        for output in outputs {
            let generation = output.certificate.stream_generation;
            let mut previous: Option<laminar_core::checkpoint::OutputFrameId> = None;
            for frame in output.frames {
                if frame.id.stream_generation != generation || frame.batch.num_rows() == 0 {
                    let reason = format!(
                        "certified subscription output for '{}' has inconsistent frame metadata",
                        output.certificate.stream_id
                    );
                    set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                    return Err(CycleError::Recovery(reason));
                }
                if let Some(previous_id) = previous {
                    let ordered = frame.id.partition > previous_id.partition
                        || (frame.id.partition == previous_id.partition
                            && previous_id.sequence.checked_next().ok() == Some(frame.id.sequence));
                    if !ordered {
                        let reason = format!(
                            "certified subscription output for '{}' is not partition-canonical",
                            output.certificate.stream_id
                        );
                        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                        return Err(CycleError::Recovery(reason));
                    }
                }
                previous = Some(frame.id);
            }
        }
        // Phase 1 keeps external cluster subscriptions disabled. Phase 2 replaces this validated
        // handoff with the bounded checkpoint-committable writer owned by this callback.
        Ok(())
    }
}
