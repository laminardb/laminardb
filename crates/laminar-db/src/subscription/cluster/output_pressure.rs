use super::output_admission::{at_checkpoint_high_water, at_commit_high_water, CyclePlan};
use super::output_state::ClusterSubscriptionOutputState;
use crate::pipeline::callback::ExternalOutputPressure;

impl ClusterSubscriptionOutputState {
    pub(super) fn update_output_pressure_after_cycle(&mut self, plan: &CyclePlan) {
        let (target, _) = self.pressure_target();
        if self.output_pressure != target && plan.reaches_high_water {
            self.output_pressure = target;
        }
    }

    pub(super) fn recompute_output_pressure(&mut self) {
        let (target, threshold) = self.pressure_target();
        self.output_pressure = if self.all_output_reaches_high_water(threshold) {
            target
        } else {
            ExternalOutputPressure::Normal
        };
    }

    pub(super) fn pressure_target(&self) -> (ExternalOutputPressure, fn(usize, usize) -> bool) {
        if self.prepared.is_some() {
            (
                ExternalOutputPressure::CommitBackpressured,
                at_commit_high_water,
            )
        } else {
            (
                ExternalOutputPressure::CheckpointDue,
                at_checkpoint_high_water,
            )
        }
    }
}
