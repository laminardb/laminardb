use super::{
    Arc, DbError, FxHashMap, RecoveredInputChannelProgress, RuntimeMode, SourceWatermarkState,
    TrackedSourceRegistration, SINGLETON_WATERMARK_CHANNEL,
};

pub(super) fn recovered_source_watermark(
    progress: Option<&FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>>,
    owns_empty_inventory: bool,
    committed_source_watermark: Option<i64>,
) -> (Option<i64>, bool) {
    let Some(progress) = progress.filter(|progress| !progress.is_empty()) else {
        return if owns_empty_inventory {
            (committed_source_watermark, true)
        } else {
            (None, false)
        };
    };
    let mut active = false;
    let mut active_min = i64::MAX;
    let mut idle_max = None;
    for channel in progress.values() {
        if let Some(watermark) = channel.watermark {
            idle_max = Some(idle_max.map_or(watermark, |current: i64| current.max(watermark)));
        }
        if !channel.idle {
            active = true;
            let Some(watermark) = channel.watermark else {
                return (None, false);
            };
            active_min = active_min.min(watermark);
        }
    }
    if active {
        (Some(active_min), false)
    } else {
        (committed_source_watermark.or(idle_max), true)
    }
}

pub(super) fn physical_recovered_input_channel_progress(
    progress: Option<&FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>>,
) -> FxHashMap<Box<[u8]>, RecoveredInputChannelProgress> {
    let mut physical = progress.cloned().unwrap_or_default();
    physical.remove(SINGLETON_WATERMARK_CHANNEL);
    physical
}

pub(super) fn restore_source_watermark_state(
    state: &mut SourceWatermarkState,
    recovered: Option<i64>,
    idle: bool,
    committed_source_watermark: Option<i64>,
) {
    let Some(watermark) = recovered else {
        return;
    };
    if idle && committed_source_watermark == Some(watermark) {
        // A committed source decision is trusted recovery evidence. Installing it as the
        // partitioned external floor ensures a subsequently installed all-idle inventory writes
        // the same exact per-channel cut rather than regressing to its older physical values.
        let _ = state.install_committed_watermark_floor(watermark);
    } else {
        state.generator.restore_watermark_for_recovery(watermark);
    }
}

pub(super) fn validate_recovered_source_watermark(
    source_name: &str,
    recovered: Option<i64>,
    idle: bool,
    committed_source_watermark: Option<i64>,
    recovered_checkpoint_index_version: Option<u32>,
) -> Result<(), DbError> {
    if let Some(version) = recovered_checkpoint_index_version {
        if version < laminar_core::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION
            && idle
            && recovered.is_none()
            && committed_source_watermark.is_none()
        {
            return Err(DbError::Checkpoint(format!(
                "legacy committed checkpoint index version {version} cannot reconstruct the \
                 retained watermark for idle source '{source_name}' from empty or uninitialized \
                 channel progress"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "recovered_source_watermark_tests.rs"]
mod recovered_source_watermark_tests;

pub(super) fn validate_recovered_input_channels(
    source_name: &str,
    progress: &FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>,
    inventory: Option<&Arc<[Vec<u8>]>>,
) -> Result<(), DbError> {
    let inventory = inventory.ok_or_else(|| {
        DbError::Checkpoint(format!(
            "recovered ordered source '{source_name}' has no input-channel inventory"
        ))
    })?;
    if inventory.len() != progress.len()
        || inventory
            .iter()
            .any(|channel| !progress.contains_key(channel.as_slice()))
    {
        return Err(DbError::Checkpoint(format!(
            "recovered ordered source '{source_name}' input-channel inventory does not match its watermark progress"
        )));
    }
    Ok(())
}

pub(super) struct PipelineRuntimeSetup {
    pub(super) sources: Vec<TrackedSourceRegistration>,
    pub(super) config: crate::pipeline::PipelineConfig,
    pub(super) callback: crate::pipeline_callback::ConnectorPipelineCallback,
    pub(super) force_checkpoint_rx: crate::db::ForceCheckpointRx,
    pub(super) checkpoint_complete_rx:
        crossfire::AsyncRx<crossfire::mpsc::Array<crate::pipeline::CheckpointCompletion>>,
    pub(super) checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(feature = "cluster")]
    pub(super) source_process_authority:
        Option<Arc<laminar_core::cluster::control::ClusterController>>,
    pub(super) runtime_mode: RuntimeMode,
}

pub(super) struct PreparedPipelineRuntime {
    pub(super) runtime: PipelineRuntimeSetup,
}

pub(super) type ReferenceTableRuntimeSource = (
    String,
    Box<dyn laminar_connectors::reference::ReferenceTableSource>,
);
