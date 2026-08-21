use super::{
    channel_progress_frontier, validate_source_recovery_assignment, Arc, DbError, FxHashMap,
    HashMap, LaminarDB, PipelineRecoveryState, RecoveredInputChannelProgress, RuntimeMode,
    TrackedSourceRegistration, SINGLETON_WATERMARK_CHANNEL,
};

impl LaminarDB {
    pub(super) fn restore_reference_table_checkpoint(
        &self,
        checkpoint_id: u64,
        checkpoint: Option<&bytes::Bytes>,
    ) -> Result<bool, DbError> {
        let has_reference_tables = !self.table_store.read().table_names().is_empty();

        match (has_reference_tables, checkpoint) {
            (true, Some(state)) => {
                let restored = self.table_store.write().restore_checkpoint(state)?;
                if !restored {
                    return Err(DbError::Checkpoint(
                        "reference-table checkpoint did not cover the complete catalog".into(),
                    ));
                }
                Ok(true)
            }
            (true, None) => Err(DbError::Checkpoint(format!(
                "recovered checkpoint {checkpoint_id} has no atomic reference-table state"
            ))),
            (false, Some(_)) => Err(DbError::Checkpoint(
                "recovered checkpoint contains reference-table state but the catalog has no tables"
                    .into(),
            )),
            (false, None) => Ok(false),
        }
    }

    pub(super) fn restore_recovered_state_frames(
        &self,
        graph: crate::operator_graph::OperatorGraph,
        recovered: &crate::recovery_manager::RecoveredState,
        participant_id: u64,
    ) -> Result<
        (
            crate::operator_graph::OperatorGraph,
            crate::mv_store::MvStore,
            bool,
        ),
        DbError,
    > {
        use laminar_core::checkpoint::StateFrameKey;

        let mut graph_whole = Vec::new();
        let mut graph_vnodes = Vec::new();
        #[cfg(feature = "cluster")]
        let mut reassigned_graph = Vec::new();
        let mut mv_states = HashMap::new();
        let mut reference_tables = None;

        for frame in &recovered.state_frames {
            match &frame.key {
                StateFrameKey::OperatorWhole { operator_id } => {
                    if let Some(name) = operator_id.strip_prefix("graph:") {
                        #[cfg(feature = "cluster")]
                        if recovered.reassigned {
                            reassigned_graph.push(frame.clone());
                            continue;
                        }
                        if frame.participant_id != participant_id {
                            return Err(DbError::Checkpoint(
                                "checkpoint selected remote graph state without reassignment"
                                    .into(),
                            ));
                        }
                        graph_whole.push((name.to_owned(), frame.payload.clone()));
                    } else if operator_id == crate::table_store::REFERENCE_TABLE_CHECKPOINT_KEY {
                        if frame.participant_id != participant_id {
                            return Err(DbError::Checkpoint(
                                "checkpoint selected a remote reference-table image".into(),
                            ));
                        }
                        if reference_tables.replace(frame.payload.clone()).is_some() {
                            return Err(DbError::Checkpoint(
                                "checkpoint repeats reference-table state".into(),
                            ));
                        }
                    } else if let Some(name) =
                        operator_id.strip_prefix(crate::mv_store::CHECKPOINT_KEY_PREFIX)
                    {
                        if frame.participant_id != participant_id {
                            return Err(DbError::Checkpoint(
                                "checkpoint selected a remote materialized-view image".into(),
                            ));
                        }
                        if mv_states
                            .insert(name.to_owned(), frame.payload.to_vec())
                            .is_some()
                        {
                            return Err(DbError::Checkpoint(format!(
                                "checkpoint repeats materialized-view state '{name}'"
                            )));
                        }
                    } else {
                        return Err(DbError::Checkpoint(format!(
                            "checkpoint contains unknown state frame '{operator_id}'"
                        )));
                    }
                }
                StateFrameKey::Vnode { operator_id, vnode } => {
                    let name = operator_id.strip_prefix("graph:").ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "checkpoint contains unknown vnode state frame '{operator_id}'"
                        ))
                    })?;
                    #[cfg(feature = "cluster")]
                    if recovered.reassigned {
                        reassigned_graph.push(frame.clone());
                        continue;
                    }
                    if frame.participant_id != participant_id {
                        return Err(DbError::Checkpoint(
                            "checkpoint selected remote vnode state without reassignment".into(),
                        ));
                    }
                    graph_vnodes.push((name.to_owned(), u32::from(*vnode), frame.payload.clone()));
                }
            }
        }

        graph_whole.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        graph_vnodes.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        #[cfg(feature = "cluster")]
        if recovered.reassigned
            && recovered.target_vnodes.is_empty()
            && !reassigned_graph.is_empty()
        {
            return Err(DbError::Checkpoint(
                "zero-owner recovery selected graph state".into(),
            ));
        }
        #[cfg(feature = "cluster")]
        let (graph, restored_graph_frames) = if recovered.reassigned
            && !recovered.target_vnodes.is_empty()
        {
            let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "reassigned checkpoint restore has no active vnode registry".into(),
                )
            })?;
            let assignment = registry.versioned_snapshot();
            let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "reassigned checkpoint restore has no cluster controller".into(),
                )
            })?;
            let target = controller
                .checkpoint_assignment_fence(assignment.version())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "reassigned checkpoint restore has no certified assignment {}",
                        assignment.version()
                    ))
                })?;
            let current_owned = assignment
                .owners()
                .iter()
                .enumerate()
                .filter_map(|(vnode, owner)| {
                    (*owner == laminar_core::state::NodeId(participant_id))
                        .then_some(u32::try_from(vnode).expect("vnode count fits u32"))
                })
                .collect::<Vec<_>>();
            if current_owned != recovered.target_vnodes {
                return Err(DbError::Checkpoint(
                    "reassigned checkpoint target vnode roster changed before graph restore".into(),
                ));
            }
            let predecessor = recovered
                .committed
                .assignment_fence
                .as_ref()
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "reassigned checkpoint restore has no predecessor assignment".into(),
                    )
                })?;
            graph.restore_reassigned_vnode_state(
                predecessor,
                &recovered.predecessor_owners,
                &target,
                &reassigned_graph,
            )?
        } else {
            graph.restore_state_frames(
                &graph_whole,
                &graph_vnodes,
                u32::from(recovered.committed.vnode_count),
            )?
        };
        #[cfg(not(feature = "cluster"))]
        let (graph, restored_graph_frames) = graph.restore_state_frames(
            &graph_whole,
            &graph_vnodes,
            u32::from(recovered.committed.vnode_count),
        )?;
        let recovered_mv_store = self.mv_store.read().recovery_image(&mv_states)?;
        let restored_reference_tables =
            self.restore_reference_table_checkpoint(recovered.epoch(), reference_tables.as_ref())?;
        tracing::info!(
            checkpoint_id = recovered.committed.checkpoint_id,
            graph_frames = restored_graph_frames,
            materialized_views = mv_states.len(),
            "restored checkpoint state frames"
        );
        Ok((graph, recovered_mv_store, restored_reference_tables))
    }

    #[cfg(feature = "cluster")]
    async fn recover_cluster_checkpoint(
        coord: &mut crate::checkpoint_coordinator::CheckpointCoordinator,
        runtime_mode: RuntimeMode,
        recover_target: Option<u64>,
    ) -> (
        Result<Option<crate::recovery_manager::RecoveredState>, DbError>,
        bool,
    ) {
        if runtime_mode == RuntimeMode::Cluster && recover_target.is_none() {
            match coord.certified_idle_process() {
                Ok(true) => return (Ok(None), true),
                Ok(false) => {}
                Err(error) => return (Err(error), false),
            }
        }
        let recovery = match recover_target {
            Some(target) => coord.recover_to_epoch(target).await,
            None => coord.recover().await,
        };
        (recovery, false)
    }

    #[cfg(feature = "cluster")]
    fn report_empty_cluster_recovery(
        &self,
        runtime_mode: RuntimeMode,
        skipped_idle_recovery: bool,
    ) -> Result<(), DbError> {
        if runtime_mode == RuntimeMode::Cluster {
            self.validate_fresh_cluster_vnode_start()?;
        }
        if skipped_idle_recovery {
            tracing::info!("certified ownerless cluster process skipped checkpoint recovery");
        } else {
            tracing::info!("No checkpoint found, starting fresh");
        }
        Ok(())
    }

    pub(super) async fn recover_pipeline_state(
        &self,
        mut graph: crate::operator_graph::OperatorGraph,
        sources: &mut [TrackedSourceRegistration],
        runtime_mode: RuntimeMode,
        vnode_state_report_timeout: std::time::Duration,
    ) -> Result<PipelineRecoveryState, DbError> {
        #[cfg(not(feature = "cluster"))]
        let _ = vnode_state_report_timeout;
        #[cfg(feature = "cluster")]
        if runtime_mode == RuntimeMode::Cluster {
            // A new graph generation has no installed vnode state until fresh initialization or
            // exact-cut callbacks complete. A stopped/faulted graph must never lend its marker to
            // the replacement generation. Startup holds assignment_adoption_lock, so publish the
            // durable withdrawal before clearing the marker; otherwise a remote rotation could
            // consume a stale true report during recovery preparation.
            let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "cluster pipeline recovery has no vnode registry for readiness withdrawal"
                        .into(),
                )
            })?;
            let assignment = registry.versioned_snapshot();
            if assignment.version() != 0 {
                let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                    DbError::Checkpoint(
                        "cluster pipeline recovery has no controller for readiness withdrawal"
                            .into(),
                    )
                })?;
                let deadline = tokio::time::Instant::now() + vnode_state_report_timeout;
                tokio::time::timeout_at(
                    deadline,
                    self.publish_local_vnode_state_report(&controller, &assignment, false),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "assignment {} vnode-state readiness withdrawal timed out before pipeline recovery",
                        assignment.version()
                    ))
                })??;
            }
            self.installed_vnode_state.lock().take();
        }
        // Must run BEFORE begin_initial_epoch so the epoch reflects the recovered state.
        // Hoist watermarks now so generators are seeded before watermark-state construction;
        // without this, generators restart at i64::MIN while offsets resume mid-stream.
        let mut recovered_mv_store = self.mv_store.read().fresh_image()?;
        let mut recovered_channel_progress: FxHashMap<
            String,
            FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>,
        > = FxHashMap::default();
        let mut recovered_input_channels: FxHashMap<String, Arc<[Vec<u8>]>> = FxHashMap::default();
        let mut recovered_source_watermarks: FxHashMap<String, i64> = FxHashMap::default();
        let mut recovered_checkpoint_index_version = None;
        let mut recovered_watermark_frontier = None;
        let mut restored_reference_tables = false;
        {
            let mut guard = self.coordinator.lock().await;
            #[cfg(feature = "cluster")]
            if runtime_mode == RuntimeMode::Cluster && guard.is_none() {
                // A checkpoint-free cluster start cannot acquire state owned by another node.
                self.validate_fresh_cluster_vnode_start()?;
            }
            if let Some(ref mut coord) = *guard {
                #[cfg(feature = "cluster")]
                coord.set_recovery_graph_payload_limit(
                    self.config
                        .pipeline_max_managed_state_bytes
                        .expect("managed-state budget is resolved at database construction"),
                );
                // Restore to the cluster-agreed epoch if one was armed, else the local
                // latest. Take it owned first so the guard isn't held across the await.
                #[cfg(feature = "cluster")]
                let recover_target = self.recover_target_epoch.lock().take();
                #[cfg(feature = "cluster")]
                let (recovery, skipped_idle_recovery) =
                    Self::recover_cluster_checkpoint(coord, runtime_mode, recover_target).await;
                #[cfg(not(feature = "cluster"))]
                let recovery = coord.recover().await;
                // Resolve any interrupted sink epoch before opening its successor.
                if runtime_mode == RuntimeMode::Local && recovery.is_ok() {
                    coord.reconcile_sink_open_witness().await?;
                }
                #[cfg(feature = "cluster")]
                {
                    *self.last_recovery_epoch.lock() = match &recovery {
                        Ok(Some(recovered)) => Some(recovered.epoch()),
                        _ => None,
                    };
                    // A genesis rewind has no durable engine cursor. Keep every source's atomic
                    // startup request at Initial; the connector applies its configured initial
                    // policy as part of start rather than becoming active and then rewinding.
                    if recover_target == Some(0) && matches!(&recovery, Ok(None)) {
                        for src in sources.iter_mut() {
                            src.position = laminar_connectors::connector::SourcePosition::Initial;
                        }
                        tracing::info!("genesis rewind: sources will start at initial position");
                    }
                }
                match recovery {
                    Ok(Some(recovered)) => {
                        recovered_checkpoint_index_version = Some(recovered.committed.version);
                        recovered_source_watermarks = recovered
                            .committed
                            .effective_source_watermarks()
                            .map_err(DbError::Checkpoint)?
                            .into_iter()
                            .collect();
                        #[cfg(feature = "cluster")]
                        let recovered_assignment = if runtime_mode == RuntimeMode::Cluster {
                            Some(
                                std::num::NonZeroU64::new(
                                    recovered
                                        .committed
                                        .assignment_fence
                                        .as_ref()
                                        .ok_or_else(|| {
                                            DbError::Checkpoint(
                                                "cluster checkpoint has no assignment fence".into(),
                                            )
                                        })?
                                        .assignment_version,
                                )
                                .ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "recovered cluster assignment fence is zero".into(),
                                    )
                                })?,
                            )
                        } else {
                            None
                        };
                        #[cfg(not(feature = "cluster"))]
                        let recovered_assignment = None;

                        for source in sources.iter() {
                            validate_source_recovery_assignment(
                                &source.name,
                                source.assignment_scoped,
                                recovered.source_offsets().get(&source.name),
                                recovered_assignment,
                            )?;
                        }

                        recovered_watermark_frontier =
                            channel_progress_frontier(recovered.channel_progress())
                                .map_err(DbError::Checkpoint)?;
                        let participant_id = coord.store().participant_id();
                        for channel in recovered.channel_progress() {
                            // Physical channels may move on rescale; logical singleton state does not.
                            if channel.input_channel == SINGLETON_WATERMARK_CHANNEL
                                && channel.participant_id != participant_id
                            {
                                continue;
                            }
                            recovered_channel_progress
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
                        for (source_name, checkpoint) in recovered.source_offsets() {
                            if let Some(input_channels) = checkpoint.input_channels.as_ref() {
                                recovered_input_channels
                                    .insert(source_name.clone(), Arc::from(input_channels.clone()));
                            }
                        }

                        let recovered_attempt =
                            laminar_core::checkpoint::CheckpointAttempt::canonical(
                                recovered.epoch(),
                            );
                        for src in sources.iter_mut() {
                            if !src.contract().supports_replay() {
                                continue;
                            }
                            let checkpoint = recovered.source_offsets().get(&src.name).ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "recovered checkpoint {} has no offset for replayable source '{}'",
                                    recovered.epoch(), src.name
                                ))
                            })?;
                            src.position = laminar_connectors::connector::SourcePosition::Resume {
                                attempt: recovered_attempt,
                                checkpoint:
                                    crate::checkpoint_coordinator::connector_to_source_checkpoint(
                                        checkpoint,
                                    ),
                            };
                        }

                        let (restored_graph, restored_mvs, restored_tables) =
                            self.restore_recovered_state_frames(graph, &recovered, participant_id)?;
                        graph = restored_graph;
                        recovered_mv_store = restored_mvs;
                        restored_reference_tables = restored_tables;

                        tracing::info!(
                            checkpoint_id = recovered.committed.checkpoint_id,
                            epoch = recovered.epoch(),
                            "recovered committed checkpoint"
                        );
                    }
                    Ok(None) => {
                        #[cfg(feature = "cluster")]
                        self.report_empty_cluster_recovery(runtime_mode, skipped_idle_recovery)?;
                        #[cfg(not(feature = "cluster"))]
                        tracing::info!("No checkpoint found, starting fresh");
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        let graph_metrics = self.engine_metrics.lock().clone();
        if let Some(prom) = graph_metrics {
            graph.set_metrics(prom);
        }

        Ok(PipelineRecoveryState {
            graph,
            recovered_mv_store,
            recovered_channel_progress,
            recovered_input_channels,
            recovered_source_watermarks,
            recovered_checkpoint_index_version,
            recovered_watermark_frontier,
            restored_reference_tables,
        })
    }
}
