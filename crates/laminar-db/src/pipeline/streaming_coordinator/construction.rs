//! Mechanically extracted coordinator responsibility.

use super::source_actor::SourceActorSpawner;
#[cfg(all(test, feature = "cluster"))]
use super::OwnedConnectorTaskFences;
use super::{
    admit_append_only_source, mpsc, run_source_operation, schema_has_reserved_mutation_columns,
    start_source_once, Arc, AtomicU64, CheckpointAttempt, CheckpointBarrier,
    ConnectorCancellationPolicy, ControlMsgRx, DbError, DeliveryGuarantee, Duration, FxHashMap,
    FxHashSet, Instant, OwnedSourceTasks, PendingBarrier, PipelineConfig, PreparedSourceGeneration,
    SourceCheckpoint, SourceConsistency, SourceInputMode, SourceMsg, SourceOperationOutcome,
    SourcePosition, SourceRegistration, SourceRowPositionCapability, SourceStart,
    SourceStartFailure, SourceStartOutcome, SourceTaskLease, StreamingCoordinator,
    StreamingCoordinatorRuntime, TrackedSourceRegistration,
};
#[cfg(feature = "cluster")]
use super::{
    source_process_authority_is_live, ClusterController, CycleError, SourceProcessAuthority,
};

struct PreparedSourceSet {
    prepared_sources: Vec<PreparedSourceGeneration>,
    committed_offsets: Vec<Option<SourceCheckpoint>>,
}
impl StreamingCoordinator {
    pub(super) fn admit_public_source_shapes(
        sources: &[TrackedSourceRegistration],
    ) -> Result<(), DbError> {
        for source in sources {
            if source.expected_schema.fields().is_empty() {
                return Err(DbError::Config(format!(
                    "source '{}' must expose a non-empty schema before public coordinator startup; late-bound schemas require database-owned catalog admission",
                    source.name
                )));
            }
            if let Some(mode) = source.admitted_non_append_mode {
                if source.contract().input_mode != mode
                    || source.contract().row_positions
                        != SourceRowPositionCapability::OrderedDeterministic
                {
                    return Err(DbError::Config(format!(
                        "source '{}' lost its admitted stateful mutation contract",
                        source.name
                    )));
                }
                match mode {
                    SourceInputMode::AppendOnly => {
                        return Err(DbError::Config(format!(
                            "source '{}' has an invalid append-only mutation admission marker",
                            source.name
                        )));
                    }
                    SourceInputMode::KeyedUpsert => {
                        if source.has_reserved_mutation_columns() {
                            return Err(DbError::Config(format!(
                                "source '{}' keyed-upsert schema declares reserved mutation metadata",
                                source.name
                            )));
                        }
                    }
                    SourceInputMode::FullChangelog => {
                        let weight = laminar_core::changelog::WEIGHT_COLUMN;
                        let fields = source.expected_schema.fields();
                        let valid_weight = fields.last().is_some_and(|field| {
                            field.name() == weight
                                && field.data_type() == &arrow_schema::DataType::Int64
                                && !field.is_nullable()
                        });
                        let reserved_count = fields
                            .iter()
                            .filter(|field| {
                                ["_op", "__op", weight]
                                    .iter()
                                    .any(|name| field.name().eq_ignore_ascii_case(name))
                            })
                            .count();
                        if !valid_weight || reserved_count != 1 {
                            return Err(DbError::Config(format!(
                                "source '{}' full-changelog schema requires exact trailing non-null Int64 '{weight}'",
                                source.name
                            )));
                        }
                    }
                }
            } else {
                admit_append_only_source(
                    source.contract(),
                    source.has_reserved_mutation_columns(),
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "source '{}' is not admissible through the public coordinator: {reason} (contract: {:?})",
                        source.name,
                        source.contract()
                    ))
                })?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    #[inline]
    pub(super) fn require_process_authority(&self, boundary: &str) -> Result<(), CycleError> {
        if self
            .process_authority
            .as_deref()
            .is_none_or(SourceProcessAuthority::is_live)
        {
            Ok(())
        } else {
            Err(CycleError::Recovery(format!(
                "cluster process lease expired before {boundary}"
            )))
        }
    }

    pub(super) async fn close_startup_source(
        source: &mut SourceRegistration,
        cleanup_deadline: tokio::time::Instant,
        #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    ) {
        match run_source_operation(
            cleanup_deadline,
            #[cfg(feature = "cluster")]
            process_authority,
            || source.connector.close(),
        )
        .await
        {
            SourceOperationOutcome::Completed(Ok(())) => {}
            SourceOperationOutcome::Completed(Err(error)) => {
                tracing::warn!(
                    source = %source.name,
                    %error,
                    "source close failed while rolling back pipeline startup"
                );
            }
            SourceOperationOutcome::Deadline => {
                tracing::warn!(
                    source = %source.name,
                    "source close exceeded its pipeline-startup cleanup deadline"
                );
            }
            #[cfg(feature = "cluster")]
            SourceOperationOutcome::ProcessAuthorityLost => {}
        }
    }

    pub(super) async fn close_prepared_sources(
        sources: &mut Vec<PreparedSourceGeneration>,
        cleanup_timeout: Duration,
        #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    ) {
        let cleanup_deadline = tokio::time::Instant::now() + cleanup_timeout;
        futures::future::join_all(sources.iter_mut().map(|source| {
            Self::close_startup_source(
                &mut source.registration,
                cleanup_deadline,
                #[cfg(feature = "cluster")]
                process_authority,
            )
        }))
        .await;
        sources.clear();
    }

    /// Reap a source task while preserving its generation fence until actual termination.
    pub(super) fn reap_source_task(task: SourceTaskLease) {
        if !task.is_finished() {
            tracing::warn!(
                source = %task.name(),
                "source task did not exit within shutdown budget; retiring its connector generation"
            );
            task.abort();
            // The DB-owned lease remains registered until the actor wrapper and connector tracker
            // prove actual termination. Coordinator shutdown stays bounded without admitting an
            // overlapping replacement generation.
            drop(task);
            return;
        }
        task.log_terminal_outcome();
        drop(task);
    }

    /// Notify every source of a committed epoch so it can release retained upstream data.
    pub(super) fn broadcast_epoch_committed(
        &self,
        epoch: u64,
        per_source: &FxHashMap<String, SourceCheckpoint>,
    ) {
        for handle in &self.source_handles {
            let cp = per_source
                .get(handle.task.name())
                .cloned()
                .unwrap_or_else(SourceCheckpoint::new);
            let _ = handle.epoch_committed_tx.send(Some((epoch, cp)));
        }
    }

    pub(super) fn release_source_barrier_attempt(&self, attempt: CheckpointAttempt) {
        for handle in &self.source_handles {
            handle.barrier_control().release_exact(attempt);
        }
    }

    pub(super) fn release_source_barrier_for(&self, source_idx: usize, attempt: CheckpointAttempt) {
        if let Some(handle) = self.source_handles.get(source_idx) {
            handle.barrier_control().release_exact(attempt);
        }
    }

    pub(super) fn stop_source_barrier_holds(&self) {
        for handle in &self.source_handles {
            handle.barrier_control().stop_hold();
        }
    }

    pub(super) fn cancel_local_source_barriers(&self, barrier: CheckpointBarrier) {
        for handle in &self.source_handles {
            handle.barrier_control().cancel_exact(barrier);
        }
    }

    /// Build the coordinator, atomically start each source connector, and spawn source tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery guarantee constraints are violated or a source fails to start
    /// at its requested initial/recovered position.
    pub async fn new(
        runtime: &StreamingCoordinatorRuntime,
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, DbError> {
        let _construction = runtime.construction.lock().await;
        runtime.prune_and_require_idle()?;
        let generation = runtime.claim_generation()?;
        let sources = sources
            .into_iter()
            .map(|source| {
                TrackedSourceRegistration::capture(source, &runtime.owned_connector_task_fences)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::admit_public_source_shapes(&sources)?;
        if let Some(source) = sources.iter().find(|source| source.assignment_scoped) {
            return Err(DbError::Config(format!(
                "assignment-scoped source '{}' requires the database-owned cluster runtime",
                source.name
            )));
        }
        let mut coordinator = Self::new_with_tracked_source_registry(
            sources,
            config,
            shutdown,
            control_rx,
            source_gate,
            #[cfg(feature = "cluster")]
            None,
            Arc::clone(&runtime.owned_source_tasks),
            crate::db::RuntimeMode::Local,
        )
        .await?;
        coordinator.public_generation = Some(generation);
        Ok(coordinator)
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) async fn new_with_source_registry(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
        #[cfg(feature = "cluster")] source_process_authority: Option<Arc<ClusterController>>,
        owned_source_tasks: OwnedSourceTasks,
        owned_connector_task_fences: OwnedConnectorTaskFences,
        runtime_mode: crate::db::RuntimeMode,
    ) -> Result<Self, DbError> {
        let sources = sources
            .into_iter()
            .map(|source| TrackedSourceRegistration::capture(source, &owned_connector_task_fences))
            .collect::<Result<Vec<_>, _>>()?;
        Self::admit_public_source_shapes(&sources)?;
        Self::new_with_tracked_source_registry(
            sources,
            config,
            shutdown,
            control_rx,
            source_gate,
            #[cfg(feature = "cluster")]
            source_process_authority,
            owned_source_tasks,
            runtime_mode,
        )
        .await
    }

    fn validate_source_runtime_config(
        sources: &[TrackedSourceRegistration],
        config: &PipelineConfig,
        #[cfg(feature = "cluster")] source_process_authority: Option<&ClusterController>,
        #[cfg(feature = "cluster")] runtime_mode: crate::db::RuntimeMode,
    ) -> Result<(), DbError> {
        if config
            .checkpoint_schedule
            .periodic_interval()
            .is_some_and(|interval| interval.is_zero())
        {
            return Err(DbError::Config(
                "checkpoint interval must be greater than zero; use manual checkpointing instead"
                    .into(),
            ));
        }
        if config.delivery_guarantee == DeliveryGuarantee::BestEffort {
            for src in sources {
                if src.contract().consistency == SourceConsistency::CommitCoupled {
                    return Err(DbError::Config(format!(
                        "source '{}' is commit-coupled; commit-coupled sources currently support only at-least-once delivery",
                        src.name
                    )));
                }
            }
        }
        if config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            for src in sources {
                if !src.contract().is_exact_delivery_certified() {
                    return Err(DbError::Config(format!(
                        "[{}] exactly-once source '{}' is not production-certified",
                        laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED,
                        src.name
                    )));
                }
            }
        }
        if matches!(
            config.delivery_guarantee,
            DeliveryGuarantee::AtLeastOnce | DeliveryGuarantee::ExactlyOnce
        ) {
            for src in sources {
                if !src.contract().supports_replay() {
                    return Err(DbError::Config(format!(
                        "[LDB-5031] {} requires source '{}' to support replay",
                        config.delivery_guarantee, src.name
                    )));
                }
            }
            if !config.checkpoint_schedule.is_enabled() {
                return Err(DbError::Config(format!(
                    "[LDB-5032] {} requires checkpointing to be enabled",
                    config.delivery_guarantee
                )));
            }
        }

        // A source that releases externally retained data only on durable commit needs
        // checkpointing; otherwise that data can grow without bound. Reject the combination up
        // front.
        if !config.checkpoint_schedule.is_enabled() {
            for src in sources {
                if src.contract().requires_checkpointing() {
                    return Err(DbError::Config(format!(
                        "[LDB-5034] source '{}' requires checkpointing to be enabled: externally \
                         retained data is only released at a durable checkpoint",
                        src.name
                    )));
                }
            }
        }

        if config.channel_capacity == 0 {
            return Err(DbError::Config(
                "[LDB-0010] channel_capacity must be > 0".into(),
            ));
        }

        #[cfg(feature = "cluster")]
        if runtime_mode.is_cluster() {
            let controller = source_process_authority.as_ref().ok_or_else(|| {
                DbError::Config(
                    "cluster source runtime requires a cluster controller with process lease authority"
                        .into(),
                )
            })?;
            if controller.process_lease_deadline().is_none() {
                return Err(DbError::Config(
                    "cluster source runtime requires one shared process lease deadline before construction"
                        .into(),
                ));
            }
        } else if source_process_authority.is_some() {
            return Err(DbError::Config(
                "local source runtime cannot install cluster process lease authority".into(),
            ));
        }

        Ok(())
    }

    fn prepare_source_starts(
        sources: Vec<TrackedSourceRegistration>,
        delivery_guarantee: DeliveryGuarantee,
    ) -> Result<Vec<(TrackedSourceRegistration, SourceStart)>, DbError> {
        let mut source_starts = Vec::with_capacity(sources.len());
        for src in sources {
            let start = SourceStart::new(
                src.config.clone(),
                src.position.clone(),
                delivery_guarantee,
            )
            .map_err(|error| match &src.position {
                SourcePosition::Initial => DbError::Config(format!(
                    "source '{}' has an invalid initial startup request: {error}",
                    src.name
                )),
                SourcePosition::Resume { attempt, .. } => DbError::Checkpoint(format!(
                    "[LDB-6003] source '{}' has an invalid resume request for checkpoint epoch={} id={}: {error}",
                    src.name, attempt.epoch, attempt.checkpoint_id
                )),
            })?;
            source_starts.push((src, start));
        }
        Ok(source_starts)
    }

    async fn prepare_source_generations(
        source_starts: Vec<(TrackedSourceRegistration, SourceStart)>,
        source_start_timeout: Duration,
        #[cfg(feature = "cluster")] source_process_authority: Option<&SourceProcessAuthority>,
    ) -> Result<PreparedSourceSet, DbError> {
        let source_count = source_starts.len();
        let mut prepared_sources = Vec::with_capacity(source_count);
        let mut committed_offsets = Vec::with_capacity(source_count);
        let source_start_deadline = tokio::time::Instant::now() + source_start_timeout;

        // Do not spawn a polling task until every source has atomically installed its startup
        // position. Otherwise a later startup failure detaches the earlier tasks and they keep
        // polling without an owner capable of shutting them down.
        for (mut src, start) in source_starts {
            let src_name = src.name.clone();
            let start_position = src.position.clone();
            if tokio::time::Instant::now() >= source_start_deadline {
                #[cfg(feature = "cluster")]
                if !source_process_authority_is_live(source_process_authority) {
                    return Err(DbError::Connector(format!(
                        "source '{src_name}' start was not attempted: cluster process lease expired"
                    )));
                }
                // `timeout_at` polls its inner future once even when already expired. Starting a
                // source can acquire a lease/slot, so never construct or poll it after the shared
                // stage budget has been consumed by earlier sources.
                Self::close_prepared_sources(
                    &mut prepared_sources,
                    PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                    #[cfg(feature = "cluster")]
                    source_process_authority,
                )
                .await;
                let error = format!(
                    "shared {source_start_timeout:?} source-start stage deadline exhausted before start began"
                );
                return match start_position {
                    SourcePosition::Initial => Err(DbError::Config(format!(
                        "source '{src_name}' start was not attempted: {error}"
                    ))),
                    SourcePosition::Resume { attempt, .. } => Err(DbError::Checkpoint(format!(
                        "[LDB-6003] source '{src_name}' start was not attempted while resuming exact checkpoint epoch={} id={}: {error}",
                        attempt.epoch, attempt.checkpoint_id
                    ))),
                };
            }
            // Seed with the durable resume position so a pre-data shutdown still checkpoints it.
            // Capture it before moving the complete request into `start`; no connector lifecycle
            // operation is allowed between configuration and cursor installation.
            let committed_offset = match &src.position {
                SourcePosition::Initial => None,
                SourcePosition::Resume { checkpoint, .. } => Some(checkpoint.clone()),
            };
            let cancellation_policy = src.connector.cancellation_policy();
            let source_start_authorized = {
                #[cfg(feature = "cluster")]
                {
                    source_process_authority_is_live(source_process_authority)
                }
                #[cfg(not(feature = "cluster"))]
                {
                    true
                }
            };
            let mut start_error = if source_start_authorized {
                match start_source_once(
                    src.connector.as_mut(),
                    start,
                    source_start_deadline,
                    #[cfg(feature = "cluster")]
                    source_process_authority,
                )
                .await
                {
                    SourceStartOutcome::Completed(Ok(())) => {
                        #[cfg(feature = "cluster")]
                        if source_process_authority_is_live(source_process_authority) {
                            None
                        } else {
                            Some(SourceStartFailure::ProcessAuthorityLost(
                                "process lease expired as source start completed".to_owned(),
                            ))
                        }
                        #[cfg(not(feature = "cluster"))]
                        None
                    }
                    SourceStartOutcome::Completed(Err(error)) => {
                        Some(if error.is_outcome_unknown() {
                            SourceStartFailure::Retired(error.to_string())
                        } else {
                            SourceStartFailure::Connector(error.to_string())
                        })
                    }
                    SourceStartOutcome::TimedOut => Some(
                        if cancellation_policy == ConnectorCancellationPolicy::RetireConnector {
                            SourceStartFailure::Retired(format!(
                                "exceeded the shared {source_start_timeout:?} source-start stage deadline"
                            ))
                        } else {
                            SourceStartFailure::Connector(format!(
                                "exceeded the shared {source_start_timeout:?} source-start stage deadline"
                            ))
                        },
                    ),
                    #[cfg(feature = "cluster")]
                    SourceStartOutcome::ProcessAuthorityLost => {
                        Some(SourceStartFailure::ProcessAuthorityLost(
                            "process lease expired while source start was in flight".to_owned(),
                        ))
                    }
                }
            } else {
                #[cfg(feature = "cluster")]
                {
                    Some(SourceStartFailure::ProcessAuthorityLost(
                        "process lease expired before source start began".to_owned(),
                    ))
                }
                #[cfg(not(feature = "cluster"))]
                {
                    unreachable!("local source startup is always authorized")
                }
            };
            if start_error.is_none() {
                let started_schema = src.connector.schema();
                if src.schema_admitted {
                    if started_schema.as_ref() != src.expected_schema.as_ref() {
                        start_error = Some(SourceStartFailure::Connector(format!(
                            "schema after start does not match the admitted schema for source '{src_name}'"
                        )));
                    }
                } else {
                    src.expected_schema = started_schema;
                    if let Err(reason) = admit_append_only_source(
                        src.contract,
                        schema_has_reserved_mutation_columns(src.expected_schema.as_ref()),
                    ) {
                        start_error = Some(SourceStartFailure::Connector(format!(
                            "source '{src_name}' schema after start is not admissible: {reason}"
                        )));
                    }
                }
                if start_error.is_none() {
                    match TrackedSourceRegistration::metadata_schemas(
                        &src_name,
                        src.contract,
                        &src.expected_schema,
                    ) {
                        Ok((positioned, mutations)) => {
                            src.positioned_schema = positioned;
                            src.mutation_schema = mutations;
                        }
                        Err(error) => {
                            start_error = Some(SourceStartFailure::Connector(error.to_string()));
                        }
                    }
                }
            }
            if let Some(failure) = start_error {
                let error = match failure {
                    SourceStartFailure::Connector(error) => {
                        // A completed failure may have acquired resources, but the connector is
                        // still valid for its bounded terminal cleanup.
                        prepared_sources.push(PreparedSourceGeneration { registration: src });
                        Self::close_prepared_sources(
                            &mut prepared_sources,
                            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                            #[cfg(feature = "cluster")]
                            source_process_authority,
                        )
                        .await;
                        error
                    }
                    SourceStartFailure::Retired(error) => {
                        // The cancelled current generation is terminal. Drop it without invoking
                        // close and clean up only sources whose starts completed.
                        Self::close_prepared_sources(
                            &mut prepared_sources,
                            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                            #[cfg(feature = "cluster")]
                            source_process_authority,
                        )
                        .await;
                        error
                    }
                    #[cfg(feature = "cluster")]
                    SourceStartFailure::ProcessAuthorityLost(error) => {
                        // Generic close may publish externally. Drop every generation without
                        // another connector call after the process authority fence.
                        error
                    }
                };
                return match start_position {
                    SourcePosition::Initial => Err(DbError::Config(format!(
                        "source '{src_name}' start failed at initial position: {error}"
                    ))),
                    SourcePosition::Resume { attempt, .. } => Err(DbError::Checkpoint(format!(
                        "[LDB-6003] source '{src_name}' start failed while resuming exact \
                             checkpoint epoch={} id={}: {error}",
                        attempt.epoch, attempt.checkpoint_id
                    ))),
                };
            }

            committed_offsets.push(committed_offset);
            prepared_sources.push(PreparedSourceGeneration { registration: src });
        }

        Ok(PreparedSourceSet {
            prepared_sources,
            committed_offsets,
        })
    }
    pub(crate) async fn new_with_tracked_source_registry(
        sources: Vec<TrackedSourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
        #[cfg(feature = "cluster")] source_process_authority: Option<Arc<ClusterController>>,
        owned_source_tasks: OwnedSourceTasks,
        #[cfg(feature = "cluster")] runtime_mode: crate::db::RuntimeMode,
        #[cfg(not(feature = "cluster"))] _runtime_mode: crate::db::RuntimeMode,
    ) -> Result<Self, DbError> {
        Self::validate_source_runtime_config(
            &sources,
            &config,
            #[cfg(feature = "cluster")]
            source_process_authority.as_deref(),
            #[cfg(feature = "cluster")]
            runtime_mode,
        )?;

        #[cfg(feature = "cluster")]
        let source_process_authority = source_process_authority.map(SourceProcessAuthority::new);
        let source_starts = Self::prepare_source_starts(sources, config.delivery_guarantee)?;
        let PreparedSourceSet {
            prepared_sources,
            committed_offsets,
        } = Self::prepare_source_generations(
            source_starts,
            config.checkpoint_timeout,
            #[cfg(feature = "cluster")]
            source_process_authority.as_deref(),
        )
        .await?;
        let source_count = prepared_sources.len();
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(config.channel_capacity);
        let (source_fault_tx, source_fault_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut source_handles = Vec::with_capacity(source_count);
        let mut source_names = Vec::with_capacity(source_count);
        let mut source_input_modes = Vec::with_capacity(source_count);
        let source_runtime = tokio::runtime::Handle::current();

        let spawner = SourceActorSpawner {
            tx: &tx,
            source_fault_tx: &source_fault_tx,
            source_gate: &source_gate,
            config: &config,
            runtime: &source_runtime,
            owned_source_tasks: &owned_source_tasks,
            #[cfg(feature = "cluster")]
            source_process_authority: source_process_authority.as_ref(),
            #[cfg(feature = "cluster")]
            runtime_mode,
        };
        for (idx, prepared) in prepared_sources.into_iter().enumerate() {
            let source_actor = spawner.spawn(idx, prepared);
            source_handles.push(source_actor.handle);
            source_names.push(source_actor.name);
            source_input_modes.push(source_actor.input_mode);
        }
        Ok(Self {
            config,
            rx,
            source_fault_rx,
            source_handles,
            source_names,
            source_input_modes,
            shutdown,
            terminal_shutdown: tokio_util::sync::CancellationToken::new(),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_retry_not_before: None,
            checkpoint_retry_backoff: Duration::ZERO,
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            pending_offsets: vec![None; committed_offsets.len()],
            replay_pending: false,
            committed_offsets,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_handoff_required: false,
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            public_generation: None,
            #[cfg(feature = "cluster")]
            process_authority: source_process_authority,
        })
    }
}
