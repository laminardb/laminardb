use super::output_admission::{
    at_checkpoint_high_water, at_commit_high_water, resource_error, CyclePlan,
    MAX_PENDING_OUTPUT_BYTES,
};
use super::output_state::ClusterSubscriptionOutputState;
use crate::error::DbError;
use crate::pipeline::callback::ExternalOutputPressure;

/// How many of the largest open streams a pressure warning names.
const REPORTED_STREAMS: usize = 5;

impl ClusterSubscriptionOutputState {
    pub(super) fn update_output_pressure_after_cycle(&mut self, plan: &CyclePlan) {
        let (target, _) = self.pressure_target();
        if self.output_pressure != target && plan.reaches_high_water {
            self.set_output_pressure(target);
        }
    }

    pub(super) fn recompute_output_pressure(&mut self) {
        let pressure = if self.all_output_reaches_high_water(at_commit_high_water) {
            if self.prepared.is_some() {
                ExternalOutputPressure::CommitBackpressured
            } else {
                ExternalOutputPressure::CheckpointBackpressured
            }
        } else if self.prepared.is_none()
            && self.all_output_reaches_high_water(at_checkpoint_high_water)
        {
            ExternalOutputPressure::CheckpointDue
        } else {
            ExternalOutputPressure::Normal
        };
        self.set_output_pressure(pressure);
    }

    fn set_output_pressure(&mut self, pressure: ExternalOutputPressure) {
        // Unlike commit backpressure, this pause ends only when a new checkpoint starts, which
        // a topology hold or a manual checkpoint schedule can postpone indefinitely.
        if pressure == ExternalOutputPressure::CheckpointBackpressured
            && self.output_pressure != pressure
        {
            tracing::warn!(
                retained_bytes = self.retained_bytes(),
                streams = %self.largest_open_streams(),
                "cluster subscription output reached its high water with no prepared checkpoint; \
                 source intake and full-snapshot output wait for the next checkpoint"
            );
        }
        self.output_pressure = pressure;
    }

    /// The open streams holding the most pending output, largest first. For a full-snapshot
    /// stream, frames per partition is how often it emitted since the last cut.
    pub(super) fn largest_open_streams(&self) -> String {
        let mut streams = self.open.streams.values().collect::<Vec<_>>();
        streams.sort_unstable_by_key(|stream| std::cmp::Reverse(stream.retained_bytes));
        streams
            .iter()
            .take(REPORTED_STREAMS)
            .map(|stream| {
                format!(
                    "{} {} bytes in {} frames over {} partitions",
                    stream.certificate.stream_id,
                    stream.retained_bytes,
                    stream.frame_count,
                    stream.partitions.len()
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub(super) fn pending_output_bound_error(&self, plan: &CyclePlan) -> DbError {
        tracing::warn!(
            retained_bytes = self.retained_bytes(),
            cycle_bytes = plan.retained_bytes,
            cycle_frames = plan.frame_count,
            streams = %self.largest_open_streams(),
            "cluster subscription output would exceed its pending bound"
        );
        resource_error("total pending output", MAX_PENDING_OUTPUT_BYTES)
    }

    /// The pressure a cycle escalates to next, with the high water that triggers it. Without a
    /// prepared cut, a due checkpoint escalates to checkpoint backpressure at the commit high water.
    pub(super) fn pressure_target(&self) -> (ExternalOutputPressure, fn(usize, usize) -> bool) {
        if self.prepared.is_some() {
            (
                ExternalOutputPressure::CommitBackpressured,
                at_commit_high_water,
            )
        } else if self.output_pressure == ExternalOutputPressure::Normal {
            (
                ExternalOutputPressure::CheckpointDue,
                at_checkpoint_high_water,
            )
        } else {
            (
                ExternalOutputPressure::CheckpointBackpressured,
                at_commit_high_water,
            )
        }
    }
}
