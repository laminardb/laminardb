//! Runtime batching and source replay-cut preparation.

use std::time::Duration;

use laminar_connectors::connector::SourceReplayOrder;

use super::{DbError, LaminarDB, PipelineWatermarks, TrackedSourceRegistration};
use crate::pipeline::{CheckpointSchedule, PipelineConfig};

impl LaminarDB {
    pub(super) fn prepare_pipeline_configuration(
        &self,
        has_external: bool,
        checkpoint_schedule: CheckpointSchedule,
        checkpoint_timeout: Duration,
        sources: &[TrackedSourceRegistration],
        watermarks: &mut PipelineWatermarks,
    ) -> Result<PipelineConfig, DbError> {
        let drain_budget_ns = self.config.pipeline_drain_budget_ns.unwrap_or(1_000_000);
        let query_budget_ns = self.config.pipeline_query_budget_ns.unwrap_or(8_000_000);
        let mut config = PipelineConfig {
            max_poll_records: self.config.default_buffer_size.min(1024),
            channel_capacity: self.config.pipeline_channel_capacity.unwrap_or(64),
            source_queue_max_bytes: self.config.source_queue_max_bytes,
            fallback_poll_interval: if has_external {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(1)
            },
            checkpoint_schedule,
            batch_window: self
                .config
                .pipeline_batch_window
                .unwrap_or(if has_external {
                    Duration::from_millis(5)
                } else {
                    Duration::ZERO
                }),
            checkpoint_timeout,
            delivery_guarantee: self.config.delivery_guarantee,
            cycle_budget_ns: 10_000_000_u64.max(drain_budget_ns + query_budget_ns),
            drain_budget_ns,
            query_budget_ns,
            max_input_buf_batches: self.config.pipeline_max_input_buf_batches.unwrap_or(256),
            max_input_buf_bytes: self.config.pipeline_max_input_buf_bytes,
            backpressure_policy: self.config.pipeline_backpressure_policy,
            shared_source_isolation: self.config.shared_source_isolation,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        };
        configure_replay_batch_cuts(sources, watermarks, &mut config)?;
        Ok(config)
    }
}

fn configure_replay_batch_cuts(
    sources: &[TrackedSourceRegistration],
    watermarks: &mut PipelineWatermarks,
    config: &mut PipelineConfig,
) -> Result<(), DbError> {
    let Some(source) = sources.iter().find(|source| {
        source.contract().replay_order == SourceReplayOrder::SingleChannelFixedBatches
    }) else {
        return Ok(());
    };
    if !source.contract().supports_fixed_batch_replay() {
        return Err(DbError::Config(format!(
            "source '{}' has an invalid fixed-batch replay contract",
            source.name
        )));
    }
    if sources.len() != 1 || watermarks.source_names.len() != 1 {
        return Err(DbError::Config(
            "fixed replay batches require one logical source; independent-source watermark cuts are unsupported".into(),
        ));
    }
    let entry = watermarks.source_entries.get(&source.name).ok_or_else(|| {
        DbError::Config(format!(
            "replay source '{}' has no catalog entry",
            source.name
        ))
    })?;
    let state = watermarks
        .watermark_states
        .get_mut(&source.name)
        .ok_or_else(|| {
            DbError::Config(format!(
                "replay source '{}' has no event-time watermark",
                source.name
            ))
        })?;
    if state.generator.is_processing_time() {
        return Err(DbError::Config(
            "fixed replay batches require event time".into(),
        ));
    }
    state
        .install_replay_batch_cuts(entry.max_out_of_orderness.unwrap_or(Duration::ZERO))
        .map_err(|error| {
            DbError::Checkpoint(format!("source '{}' replay cut: {error}", source.name))
        })?;
    if let (Some(tracker), Some(id)) = (
        watermarks.tracker.as_mut(),
        watermarks.source_ids.get(&source.name),
    ) {
        tracker.set_idle_timeout(*id, None);
    }
    // INVARIANT: the existing FIFO executes one complete replay batch before accepting its successor.
    config.batch_window = Duration::ZERO;
    config.drain_budget_ns = 0;
    Ok(())
}
