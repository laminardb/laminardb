use super::{
    Arc, DbError, LaminarDB, PipelineRuntimeSetup, PipelineSinkSetup, PipelineWatermarks,
    PreparedPipelineRuntime, RuntimeMode, TrackedSourceRegistration,
};

struct CallbackCollections {
    source_name_arcs: rustc_hash::FxHashMap<usize, Arc<str>>,
    source_frontiers_buf: rustc_hash::FxHashMap<Arc<str>, crate::operator_graph::InputFrontier>,
    named_stream_names: rustc_hash::FxHashSet<Arc<str>>,
}

fn prepare_callback_collections(
    source_ids: &rustc_hash::FxHashMap<String, usize>,
    stream_entries: &[Arc<crate::catalog::StreamEntry>],
) -> CallbackCollections {
    let source_name_arcs: rustc_hash::FxHashMap<usize, Arc<str>> = source_ids
        .iter()
        .map(|(name, &source_id)| (source_id, Arc::<str>::from(name.as_str())))
        .collect();
    let source_frontiers_buf = rustc_hash::FxHashMap::with_capacity_and_hasher(
        source_name_arcs.len(),
        rustc_hash::FxBuildHasher,
    );
    let named_stream_names = stream_entries
        .iter()
        .map(|entry| Arc::from(entry.name.as_str()))
        .collect();
    CallbackCollections {
        source_name_arcs,
        source_frontiers_buf,
        named_stream_names,
    }
}

impl LaminarDB {
    pub(super) async fn prepare_pipeline_runtime(
        &self,
        sources: Vec<TrackedSourceRegistration>,
        mut graph: crate::operator_graph::OperatorGraph,
        sink_setup: PipelineSinkSetup,
        watermarks: PipelineWatermarks,
        config: crate::pipeline::PipelineConfig,
        runtime_mode: RuntimeMode,
    ) -> Result<PreparedPipelineRuntime, DbError> {
        let PipelineSinkSetup {
            sinks,
            sink_event_rx,
            #[cfg(feature = "cluster")]
            callback_controller,
        } = sink_setup;
        let PipelineWatermarks {
            stream_entries,
            watermark_states,
            source_entries,
            source_ids,
            source_names: checkpoint_source_names,
            tracker,
        } = watermarks;

        graph.set_query_budget_ns(config.query_budget_ns);
        graph.set_max_input_buf_batches(config.max_input_buf_batches);
        graph.set_max_input_buf_bytes(config.max_input_buf_bytes);
        graph.set_backpressure_policy(config.backpressure_policy);
        graph.set_shared_source_isolation(
            config.shared_source_isolation,
            config.max_replay_buffer_bytes,
        );

        let pending_sink_filter_compiles = sinks
            .iter()
            .filter(|(_, _, filter_sql, _, _, _)| filter_sql.is_some())
            .count();
        let CallbackCollections {
            source_name_arcs,
            source_frontiers_buf,
            named_stream_names,
        } = prepare_callback_collections(&source_ids, &stream_entries);
        let prom = self
            .engine_metrics
            .lock()
            .clone()
            .expect("EngineMetrics must be set before start()");

        let (force_checkpoint_tx, force_checkpoint_rx) =
            crossfire::mpsc::bounded_async::<crate::db::ForceCheckpointRequest>(
                crate::db::FORCE_CHECKPOINT_CHANNEL_CAPACITY,
            );
        *self.force_ckpt_tx.lock() = Some(force_checkpoint_tx);
        let (checkpoint_complete_tx, checkpoint_complete_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(16);

        let checkpoint_committable_sinks = sinks
            .iter()
            .any(|(_, handle, _, _, _, _)| handle.checkpoint_committable());
        let checkpoint_in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (
            epoch_allocator,
            quorum_timeout,
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            max_node_data_bytes,
            full_vnode_capture_needed,
        ) = {
            let coordinator = self.coordinator.lock().await;
            match coordinator.as_ref() {
                Some(coordinator) => {
                    let checkpoint = coordinator.config();
                    let full_vnode_capture_needed = coordinator
                        .last_committed_manifest()
                        .is_some_and(|manifest| {
                            coordinator.committed_manifest_needs_vnode_rebase(
                                laminar_core::checkpoint::CheckpointAttempt::new(
                                    manifest.epoch,
                                    manifest.checkpoint_id,
                                ),
                            )
                        });
                    (
                        Some(coordinator.epoch_allocator()),
                        checkpoint.quorum_timeout,
                        checkpoint.checkpoint_timeout,
                        checkpoint.cleanup_timeout,
                        checkpoint.max_node_data_bytes,
                        full_vnode_capture_needed,
                    )
                }
                None => (
                    None,
                    std::time::Duration::from_secs(3),
                    std::time::Duration::from_secs(120),
                    crate::checkpoint_coordinator::CheckpointConfig::default().cleanup_timeout,
                    u64::MAX,
                    false,
                ),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let _ = quorum_timeout;

        #[cfg(feature = "cluster")]
        let source_process_authority = (runtime_mode == RuntimeMode::Cluster)
            .then(|| callback_controller.clone())
            .flatten();
        #[cfg(feature = "cluster")]
        let vnode_registry = self.vnode_registry.lock().clone();
        #[cfg(feature = "cluster")]
        let (shuffle_delivery_loss_incidents, shuffle_recovered_delivery_loss_incidents) = self
            .shuffle_receiver
            .lock()
            .as_ref()
            .map_or((None, None), |receiver| {
                (
                    Some(receiver.delivery_loss_incidents()),
                    Some(receiver.recovered_delivery_loss_incidents()),
                )
            });

        #[cfg(feature = "cluster")]
        let cluster_subscription_output =
            crate::subscription::cluster::ClusterSubscriptionOutputState::new(
                graph.subscription_certificates(),
            )?;
        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            graph,
            stream_entries,
            sinks,
            owned_sink_handles: Arc::clone(&self.owned_sink_handles),
            watermark_states,
            source_entries_for_wm: source_entries,
            source_ids,
            source_name_arcs,
            checkpoint_source_names,
            source_frontiers_buf,
            #[cfg(feature = "cluster")]
            committed_source_watermarks_snapshot: Arc::new(rustc_hash::FxHashMap::default()),
            tracker,
            prom,
            #[cfg(feature = "cluster")]
            checkpoint_barrier_timings: Arc::clone(&self.checkpoint_barrier_timings),
            pipeline_watermark: Arc::clone(&self.pipeline_watermark),
            coordinator: Arc::clone(&self.coordinator),
            table_store: self.table_store.clone(),
            mv_store_has_any: self.mv_store.read().has_any_handle(),
            mv_store: self.mv_store.clone(),
            filter_ctx: laminar_sql::create_session_context(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles,
            delivery_guarantee: config.delivery_guarantee,
            serialization_timeout: checkpoint_timeout,
            checkpoint_state_cap_bytes: max_node_data_bytes,
            checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            sink_event_rx,
            sink_timed_out: false,
            sink_fault: None,
            checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
            pipeline_halt: None,
            last_checkpoint_admission_failure: None,
            checkpoint_admission_recovering: false,
            shutdown_signal: Arc::clone(&self.shutdown_signal),
            #[cfg(feature = "cluster")]
            vnode_registry,
            #[cfg(feature = "cluster")]
            cluster_controller: callback_controller,
            #[cfg(feature = "cluster")]
            assignment_adoption_lock: Arc::clone(&self.assignment_adoption_lock),
            #[cfg(feature = "cluster")]
            follower_tail: Arc::default(),
            #[cfg(feature = "cluster")]
            barrier_injectors: Vec::new(),
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents,
            #[cfg(feature = "cluster")]
            shuffle_recovered_delivery_loss_incidents,
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents_seen: 0,
            #[cfg(feature = "cluster")]
            pending_follower_checkpoint: None,
            #[cfg(feature = "cluster")]
            checkpoint_leader_proofs: rustc_hash::FxHashMap::default(),
            subscription_registry: Arc::clone(&self.subscription_registry),
            #[cfg(feature = "cluster")]
            cluster_subscription_output,
            named_stream_names,
            checkpoint_complete_tx,
            checkpoint_tail_runtime: self.control_runtime.handle()?,
            checkpoint_tail_tasks: tokio::task::JoinSet::new(),
            checkpoint_in_flight: Arc::clone(&checkpoint_in_flight),
            full_vnode_capture_needed: Arc::new(std::sync::atomic::AtomicBool::new(
                full_vnode_capture_needed,
            )),
            epoch_allocator,
            #[cfg(feature = "cluster")]
            quorum_timeout,
            checkpoint_committable_sinks,
            #[cfg(feature = "cluster")]
            intake_gate: Arc::clone(&self.source_gate),
            #[cfg(not(feature = "cluster"))]
            intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        Ok(PreparedPipelineRuntime {
            runtime: PipelineRuntimeSetup {
                sources,
                config,
                callback,
                force_checkpoint_rx,
                checkpoint_complete_rx,
                checkpoint_in_flight,
                #[cfg(feature = "cluster")]
                source_process_authority,
                runtime_mode,
            },
        })
    }
}
