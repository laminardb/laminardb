//! Source contract, drain lifecycle, polling, checkpointing, and shutdown.

use super::{
    async_trait, info, join_background_task, kafka_output_schema, reap_last_arc_off_runtime, warn,
    Arc, CommitMode, ConnectorConfig, ConnectorError, ConnectorState, ConnectorTaskTracker,
    Consumer, Format, KafkaReaderDrainCommand, KafkaSource, KafkaSourceConfig, KafkaSourceDrain,
    Notify, OffsetTracker, Ordering, SchemaRef, SourceBatch, SourceCheckpoint, SourceConnector,
    SourceConsistency, SourceContract, SourceDrainRequest, SourceDrainResolution, SourceInputMode,
    SourceRowPositionCapability, SourceStart, SourceTopology, KAFKA_BACKGROUND_CLOSE_BUDGET,
};

#[async_trait]
impl SourceConnector for KafkaSource {
    fn supports_initialized_start(&self) -> bool {
        true
    }

    async fn resolve_initial_position(
        &mut self,
        config: &ConnectorConfig,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        self.resolve_initial_position_inner(config).await
    }

    async fn validate_initial_position(
        &mut self,
        config: &ConnectorConfig,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.inspect_initial_position_inner(config, Some(checkpoint))
            .await
            .map(|_| ())
    }

    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let format = if config.properties().is_empty() {
            self.config.validate()?;
            self.config.format
        } else {
            KafkaSourceConfig::from_config(config)?.format
        };
        let input_mode = if format == Format::Debezium {
            SourceInputMode::KeyedUpsert
        } else {
            SourceInputMode::AppendOnly
        };
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            input_mode,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        .with_exact_delivery_certification())
    }

    fn set_vnode_assignment(
        &mut self,
        source_identity: &str,
        registry: Arc<laminar_core::state::VnodeRegistry>,
        self_id: laminar_core::state::NodeId,
    ) -> Result<(), ConnectorError> {
        if source_identity.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "Kafka vnode assignment requires a non-empty canonical source identity".into(),
            ));
        }
        if self_id.is_unassigned() {
            return Err(ConnectorError::ConfigurationError(
                "Kafka vnode assignment requires a nonzero node identity".into(),
            ));
        }
        info!(
            source = source_identity,
            self_id = self_id.0,
            vnode_count = registry.vnode_count(),
            "Kafka source: engine-controlled partition-to-vnode assignment enabled"
        );
        self.source_name = Arc::from(source_identity);
        self.vnode_assignment = Some((registry, self_id));
        Ok(())
    }

    fn begin_drain(
        &mut self,
        request: &SourceDrainRequest,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.check_reader_health("starting a global source drain")?;
        if let Some(active) = self.source_drain.as_ref() {
            if active.request != *request {
                return Err(ConnectorError::InvalidState {
                    expected: format!("active Kafka drain {:?}", active.request.round),
                    actual: format!("conflicting Kafka drain {:?}", request.round),
                });
            }
            // Retain the first preparation deadline. A caller retry carries a wait budget,
            // not authority to extend or shorten work already in progress.
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ConnectorError::Internal(
                "Kafka source drain began after its engine deadline".into(),
            ));
        }
        let Some((registry, _)) = self.vnode_assignment.as_ref() else {
            return Err(ConnectorError::InvalidState {
                expected: "Kafka cluster assignment installed before source drain".into(),
                actual: "embedded/single Kafka source".into(),
            });
        };
        if registry.assignment_version() != request.round.predecessor_version {
            return Err(ConnectorError::InvalidState {
                expected: format!(
                    "Kafka predecessor assignment {}",
                    request.round.predecessor_version
                ),
                actual: registry.assignment_version().to_string(),
            });
        }
        let reconciled = self.reconciled_assignment_version.load(Ordering::Acquire);
        if reconciled != request.round.predecessor_version {
            return Err(ConnectorError::InvalidState {
                expected: format!(
                    "reconciled Kafka predecessor assignment {}",
                    request.round.predecessor_version
                ),
                actual: reconciled.to_string(),
            });
        }
        self.ensure_reader_started();
        let tx = self.reader_drain_tx.as_ref().ok_or_else(|| {
            ConnectorError::Internal("Kafka cluster reader has no drain control channel".into())
        })?;
        tx.send(KafkaReaderDrainCommand::Begin {
            request: request.clone(),
            deadline,
        })
        .map_err(|_| ConnectorError::Internal("Kafka reader drain channel closed".into()))?;
        self.source_drain = Some(KafkaSourceDrain {
            request: request.clone(),
            prepare_deadline: deadline,
            boundary: None,
            cut: None,
            pending_resolution: None,
        });
        self.data_ready.notify_one();
        Ok(())
    }

    fn poll_drain_ready(
        &mut self,
        round: laminar_core::checkpoint::AssignmentDrainId,
    ) -> Result<bool, ConnectorError> {
        self.check_reader_health("capturing a global source drain cut")?;
        let Some(active) = self.source_drain.as_ref() else {
            return Err(ConnectorError::InvalidState {
                expected: format!("active Kafka drain {round:?}"),
                actual: "no Kafka drain".into(),
            });
        };
        if active.request.round != round {
            return Err(ConnectorError::InvalidState {
                expected: format!("active Kafka drain {:?}", active.request.round),
                actual: format!("cut requested for {round:?}"),
            });
        }
        if active.cut.is_some() {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= active.prepare_deadline {
            return Err(ConnectorError::Internal(
                "Kafka drain deadline expired before cursor capture".into(),
            ));
        }
        let Some(boundary) = active.boundary.clone() else {
            return Ok(false);
        };
        let cut = self.capture_drain_positions(&boundary.inputs, Some(active.prepare_deadline))?;
        let active = self.source_drain.as_mut().expect("checked above");
        active.cut = Some(cut);
        Ok(true)
    }

    async fn finish_drain(
        &mut self,
        resolution: SourceDrainResolution,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.finish_drain_inner(resolution, deadline).await
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let result = self.start_with_contract(request).await;
        if result.is_err() && self.state == ConnectorState::Initializing {
            self.fail_startup();
        }
        result
    }
    async fn resolve_schema(
        &mut self,
        config: &crate::config::ConnectorConfig,
        explicit: Option<arrow_schema::SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        super::super::schema_resolution::resolve_source(config, explicit).await
    }

    async fn discover_schema(
        &mut self,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let config = crate::config::ConnectorConfig::with_properties("kafka", properties.clone());
        let binding = super::super::schema_resolution::resolve_source(&config, None).await?;
        let parsed = KafkaSourceConfig::from_config(&config)?;
        self.schema = super::super::schema_resolution::payload_schema(
            &binding,
            parsed.include_metadata,
            parsed.include_headers,
        )?;
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.poll_batch_inner(max_records).await
    }

    fn schema(&self) -> SchemaRef {
        kafka_output_schema(
            &self.schema,
            self.config.include_metadata,
            self.config.include_headers,
        )
    }

    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        self.check_reader_health("reconciling source ownership")?;
        Ok(self.vnode_assignment.as_ref().is_none_or(|(registry, _)| {
            let version = registry.assignment_version();
            version != 0 && self.reconciled_assignment_version.load(Ordering::Acquire) == version
        }))
    }

    fn drive_control_plane(&mut self) {
        self.ensure_reader_started();
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.try_capture_checkpoint()
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        self.validate_active_drain_cursor()?;
        self.try_capture_checkpoint()
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if !self.config.broker_commit_on_checkpoint || checkpoint.is_empty() {
            return Ok(());
        }
        let tpl = OffsetTracker::try_from_checkpoint(checkpoint)?.to_topic_partition_list();
        if tpl.count() == 0 {
            return Ok(());
        }
        let Some(consumer) = self.consumer.as_ref() else {
            // Engine recovery never uses broker-stored offsets. A missing consumer cannot
            // invalidate the already-durable checkpoint, so report the observability failure
            // without turning a committed epoch into a pipeline restart loop.
            self.metrics.commit_failures.inc();
            warn!(
                epoch,
                "Kafka progress commit skipped because the consumer is absent"
            );
            return Ok(());
        };

        // The engine checkpoint is the recovery authority; Kafka's group offset is an
        // observability cursor only. Enqueue it without blocking the checkpoint/recovery path.
        // The consumer context records the eventual broker acknowledgement or rejection.
        if let Err(error) = consumer.commit(&tpl, CommitMode::Async) {
            self.metrics.commit_failures.inc();
            warn!(epoch, %error, "Kafka progress commit was not accepted for enqueue");
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Kafka source connector");

        // Stop intake and give the reader plus final consumer reaper one shared cleanup budget
        // below the coordinator's outer source-shutdown deadline.
        if let Some(tx) = self.reader_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(progress) = &mut self.progress {
            progress.stop();
        }
        // Wake assignment and poll work before joining. Any advisory async commit cleanup remains
        // librdkafka-owned and is not allowed to extend the engine's source-shutdown deadline.
        if let Some(ref consumer) = self.consumer {
            consumer.unsubscribe();
        }
        let deadline = tokio::time::Instant::now() + KAFKA_BACKGROUND_CLOSE_BUDGET;
        join_background_task(&mut self.reader_handle, deadline, "reader").await;
        if let Some(mut progress) = self.progress.take() {
            progress.close(deadline).await;
        }
        self.msg_rx = None;
        self.reader_drain_tx = None;
        self.source_drain = None;
        self.channel_len.store(0, Ordering::Release);
        if let Some(consumer) = self.consumer.take() {
            reap_last_arc_off_runtime(&self.blocking_tasks, consumer, deadline, "consumer").await;
        }
        if !self.blocking_tasks.join_until(deadline).await {
            self.blocking_tasks.ensure_reaper();
        }
        self.state = ConnectorState::Closed;
        info!("Kafka source connector closed");
        Ok(())
    }
}
