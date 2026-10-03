//! Install the previously decoded root through the ordinary source/sink/runtime handoff.

use laminar_core::cluster::control::{TopologyError, TopologyRestoreInput};
use laminar_core::shuffle::ShuffleTopologyFence;

use super::{
    channel_progress_frontier, Arc, DbError, FxHashMap, LaminarDB, PipelineRecoveryState,
    RecoveredInputChannelProgress, TrackedSourceRegistration, SINGLETON_WATERMARK_CHANNEL,
};
use crate::db::TopologyRuntimeMetadata;

pub(super) struct TopologyStartup {
    pub(super) image: crate::db::PreparedTopologyRestore,
    pub(super) deadline: tokio::time::Instant,
}

impl LaminarDB {
    pub(super) fn prepare_topology_runtime_sources(
        &self,
        sources: &mut [TrackedSourceRegistration],
        metadata: &TopologyRuntimeMetadata,
    ) -> Result<(), DbError> {
        if sources.len() != metadata.sources.len() {
            return Err(TopologyError::Fenced.into());
        }
        for source in sources {
            source.position = metadata
                .sources
                .get(&source.name)
                .ok_or(TopologyError::Fenced)?
                .startup_position();
            if matches!(
                source.position,
                laminar_connectors::connector::SourcePosition::Initialized { .. }
            ) && !source.connector.supports_initialized_start()
            {
                return Err(TopologyError::Unsupported(format!(
                    "source '{}' has no atomic sealed-position startup contract",
                    source.name,
                ))
                .into());
            }
            // Validate requests for every source before any target sink is opened.
            drop(laminar_connectors::connector::SourceStart::new(
                source.config.clone(),
                source.position.clone(),
                self.config.delivery_guarantee,
            )?);
        }
        Ok(())
    }

    pub(super) async fn install_topology_runtime_state(
        &self,
        mut graph: crate::operator_graph::OperatorGraph,
        metadata: &TopologyRuntimeMetadata,
    ) -> Result<PipelineRecoveryState, DbError> {
        self.validate_topology_installation(&metadata.input).await?;
        if !self.connector_manager.lock().tables().is_empty() {
            return Err(TopologyError::Unsupported(
                "migration runtime has no reference-table initialization mapping".into(),
            )
            .into());
        }
        let target = ShuffleTopologyFence::from_manifest(
            metadata.input.descriptor().target_version,
            &metadata.input.plan().target_manifest,
        )
        .map_err(|error| TopologyError::Invalid(error.to_string()))?;
        graph.bind_cluster_topology_fence(target)?;
        graph.set_pending_vnode_transition_handle(Arc::clone(&self.pending_vnode_transition));
        graph.set_installed_vnode_state_handle(Arc::clone(&self.installed_vnode_state));
        graph.set_rotation_execution_fence(Arc::clone(&self.rotation_execution_fence));
        if let Some(metrics) = self.engine_metrics.lock().clone() {
            graph.set_metrics(metrics);
        }
        {
            let mut coordinator = self.coordinator.lock().await;
            coordinator
                .as_mut()
                .ok_or(TopologyError::Fenced)?
                .install_topology_root_metadata(&metadata.input, &metadata.recovered)?;
        }
        *self.last_recovery_epoch.lock() = Some(metadata.recovered.epoch());
        let mut progress: FxHashMap<String, FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>> =
            FxHashMap::default();
        for channel in metadata.recovered.channel_progress() {
            if channel.input_channel == SINGLETON_WATERMARK_CHANNEL
                && channel.participant_id != metadata.input.process().participant.node_id
            {
                continue;
            }
            progress
                .entry(channel.source_name.clone())
                .or_default()
                .insert(
                    channel.input_channel.clone().into_boxed_slice(),
                    RecoveredInputChannelProgress {
                        watermark: channel.watermark,
                        idle: channel.idle,
                    },
                );
        }
        let inventories = metadata
            .recovered
            .source_offsets()
            .iter()
            .filter_map(|(name, checkpoint)| {
                checkpoint
                    .input_channels
                    .as_ref()
                    .map(|channels| (name.clone(), Arc::from(channels.clone())))
            })
            .collect();
        Ok(PipelineRecoveryState {
            graph,
            recovered_mv_store: self.mv_store.read().fresh_image()?,
            recovered_channel_progress: progress,
            recovered_input_channels: inventories,
            recovered_source_watermarks: metadata
                .recovered
                .committed
                .effective_source_watermarks()
                .map_err(DbError::Checkpoint)?
                .into_iter()
                .collect(),
            recovered_checkpoint_index_version: Some(metadata.recovered.committed.version),
            recovered_watermark_frontier: channel_progress_frontier(
                metadata.recovered.channel_progress(),
            )
            .map_err(DbError::Checkpoint)?,
            restored_reference_tables: false,
        })
    }

    pub(crate) async fn ensure_topology_runtime_ready(
        &self,
        input: &TopologyRestoreInput,
    ) -> Result<(), DbError> {
        self.ensure_topology_runtime_live(input).await?;
        self.ensure_topology_runtime_held()
    }

    pub(crate) async fn ensure_topology_runtime_live(
        &self,
        input: &TopologyRestoreInput,
    ) -> Result<(), DbError> {
        let coordinator_matches = {
            let coordinator = self.coordinator.lock().await;
            let coordinator = coordinator.as_ref().ok_or(TopologyError::Fenced)?;
            coordinator.bound_pipeline_identity()? == input.descriptor().target_pipeline
                && (coordinator.last_committed_ref()
                    == input.outcome().committed_checkpoint.as_ref()
                    || (input.operation().phase
                        == laminar_core::cluster::control::TopologyAdmissionPhase::Active
                        && coordinator
                            .last_committed_manifest()
                            .is_some_and(|manifest| {
                                manifest.pipeline_identity == input.descriptor().target_pipeline
                            })))
        };
        let graph_matches = self
            .installed_vnode_state
            .lock()
            .as_ref()
            .is_some_and(|binding| {
                binding.matches(input.assignment(), &input.descriptor().target_pipeline)
            });
        let watcher_running = self
            .runtime_handle
            .lock()
            .await
            .as_ref()
            .is_some_and(|watcher| !watcher.is_finished());
        let expected_sources = input
            .target()
            .entries
            .iter()
            .filter(|entry| entry.kind == laminar_core::catalog::CatalogObjectKind::Source)
            .count();
        let expected_sinks = input
            .target()
            .entries
            .iter()
            .filter(|entry| entry.kind == laminar_core::catalog::CatalogObjectKind::Sink)
            .count();
        let sources_ready = {
            let sources = self.owned_source_tasks.lock();
            sources.len() == expected_sources
                && sources
                    .iter()
                    .all(crate::pipeline::streaming_coordinator::SourceTaskLease::is_running)
        };
        let sinks_ready = {
            let sinks = self.owned_sink_handles.lock();
            sinks.len() == expected_sinks
                && sinks.iter().all(crate::sink_task::SinkTaskHandle::is_ready)
        };
        if !coordinator_matches
            || !graph_matches
            || !watcher_running
            || !sources_ready
            || !sinks_ready
            || self.runtime_shutdown.read().is_cancelled()
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }
}
