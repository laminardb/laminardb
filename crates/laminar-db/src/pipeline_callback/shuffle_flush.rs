//! The checkpoint's distributed shuffle flush: waves of local drain and aligned activity markers
//! until no pre-cut row remains in transit, with sources held at the admitted cut.

use laminar_core::checkpoint::CheckpointAttempt;

use super::ConnectorPipelineCallback;
use crate::error::DbError;
use crate::operator_graph::ShuffleAlignmentOutcome;

/// Waves a healthy flush may run beyond its remote exchange depth: the first wave, the idle
/// confirmation, and emission a frontier releases mid-flush. Corpus checkpoints settle within
/// depth + 2.
const FLUSH_WAVE_SLACK: u64 = 4;

impl ConnectorPipelineCallback {
    /// Reach a distributed fixed point while every source remains held at the admitted cut.
    ///
    /// Each wave first drains local retained work to handoff quiescence, then publishes an ordered
    /// marker carrying whether that drain did any work. Wave zero is never terminal: it closes
    /// transport backlog that was not represented by a preceding activity marker. For later
    /// waves, every participant observes the same sender activity vector, so a completely idle
    /// vector plus local post-alignment quiescence proves no pre-cut row remains in transit or in
    /// operator channel state.
    pub(super) async fn flush_cluster_shuffle_until(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        let flush_started_at = std::time::Instant::now();
        let mut wave = 0_u64;
        let mut saw_local_activity = false;
        let mut saw_peer_activity = false;
        let mut saw_graph_staging = false;
        tracing::info!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            "checkpoint distributed shuffle fixed-point flush started"
        );
        loop {
            let local_activity = self
                .drain_handoff_edges_until_inner(deadline)
                .await
                .map_err(|error| match error {
                    crate::pipeline::CycleError::Halt(reason) => DbError::PipelineTerminal(
                        format!("shuffle flush wave {wave} graph drain halted: {reason}"),
                    ),
                    crate::pipeline::CycleError::Fatal(reason)
                    | crate::pipeline::CycleError::Recovery(reason) => DbError::Checkpoint(
                        format!("shuffle flush wave {wave} graph drain failed: {reason}"),
                    ),
                })?;
            let watermark = self.effective_pipeline_watermark();
            let aligned = self
                .graph
                .align_shuffle_flush_wave(
                    attempt,
                    wave,
                    local_activity,
                    watermark,
                    assignment_fence,
                    deadline,
                    Some(controller),
                )
                .await?;
            saw_local_activity |= local_activity;
            saw_peer_activity |= aligned.peer_activity;
            saw_graph_staging |= aligned.graph_state_staged;
            tracing::debug!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                wave,
                local_activity,
                peer_activity = aligned.peer_activity,
                graph_state_staged = aligned.graph_state_staged,
                elapsed = ?flush_started_at.elapsed(),
                "checkpoint distributed shuffle fixed-point wave settled"
            );
            if aligned.outcome != ShuffleAlignmentOutcome::Aligned {
                if Self::shuffle_flush_attempt_advanced(wave, local_activity, aligned) {
                    return Err(DbError::Checkpoint(format!(
                        "shuffle flush observed {:?} after distributed replay had already advanced; coordinated recovery is required",
                        aligned.outcome
                    )));
                }
                return Ok(aligned.outcome);
            }
            if wave != 0 && !local_activity && !aligned.peer_activity {
                if aligned.graph_state_staged || !self.graph.handoff_is_quiescent() {
                    return Err(DbError::Checkpoint(format!(
                        "shuffle flush wave {wave} received an all-idle activity vector but staged new replay; coordinated recovery is required"
                    )));
                }
                #[allow(clippy::cast_precision_loss)] // bounded by MAX_SHUFFLE_FLUSH_WAVE
                self.prom
                    .checkpoint_shuffle_flush_waves
                    .observe((wave + 1) as f64);
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    waves = wave + 1,
                    remote_exchange_depth = self.graph.remote_exchange_depth(),
                    saw_local_activity,
                    saw_peer_activity,
                    saw_graph_staging,
                    elapsed = ?flush_started_at.elapsed(),
                    "checkpoint distributed shuffle fixed-point flush completed"
                );
                return Ok(ShuffleAlignmentOutcome::Aligned);
            }
            wave = wave.checked_add(1).ok_or_else(|| {
                DbError::Checkpoint("shuffle flush wave counter overflowed u64".into())
            })?;
            let depth = self.graph.remote_exchange_depth();
            if wave >= depth.saturating_add(FLUSH_WAVE_SLACK) {
                return Err(DbError::Checkpoint(format!(
                    "shuffle flush did not settle within {} waves for remote exchange depth {depth}; coordinated recovery is required",
                    depth.saturating_add(FLUSH_WAVE_SLACK)
                )));
            }
            if wave > laminar_core::checkpoint::barrier::MAX_SHUFFLE_FLUSH_WAVE {
                return Err(DbError::Checkpoint(
                    "shuffle flush exhausted its reserved barrier wave field".into(),
                ));
            }
        }
    }

    /// Whether a non-aligned wave outcome arrived after replay had already moved state.
    pub(super) fn shuffle_flush_attempt_advanced(
        wave: u64,
        local_activity: bool,
        aligned: crate::operator_graph::ShuffleFlushWaveOutcome,
    ) -> bool {
        wave != 0 || local_activity || aligned.graph_state_staged
    }
}
