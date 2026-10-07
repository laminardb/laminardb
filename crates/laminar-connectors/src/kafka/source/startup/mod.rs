//! Validated source startup, assignment, and schema-registry prefetch.

use super::{
    assignment_seek_tpl, build_vnode_assignment_tpl, consumer_creation_error,
    decode_partition_baselines, deterministic_initial_offset, fetch_explicit_topic_metadata,
    fetch_partition_low_watermarks, fetch_partition_watermarks, info,
    kafka_bootstrap_is_unassigned, kafka_input_channels, kafka_partition_routes,
    kafka_partition_set, lock_or_recover, resolve_timestamp_offsets, select_deserializer,
    startup_default_offset, validate_kafka_assignment, validate_kafka_output_schema,
    validate_kafka_partition_results, validate_partition_baselines, validate_positions_not_expired,
    validate_resume_input_channels, Arc, AvroDeserializer, ClientConfig, ConnectorError,
    ConnectorState, Consumer, DeliveryGuarantee, Format, KafkaAssignmentPublication,
    KafkaPartitionBaselines, KafkaPartitionRoutes, KafkaPartitionSet, KafkaRotationBaselines,
    KafkaSource, KafkaSourceConfig, KafkaStartPlan, LaminarConsumerContext, OffsetTracker,
    Ordering, SourcePosition, SourceStart, StartupMode, StreamConsumer, TopicPartitionList,
    TopicSubscription,
};

mod modes;
mod validation;
mod vnode;

use validation::VnodeStartInventory;

impl KafkaSource {
    pub(super) async fn start_with_contract(
        &mut self,
        request: SourceStart,
    ) -> Result<(), ConnectorError> {
        let (mut config, position, delivery) = request.into_parts();
        let metadata = crate::kafka::schema_configuration::source(&config, &self.config);
        if config.schema_binding().is_none() {
            if metadata
                .get("format")
                .is_some_and(|format| format.eq_ignore_ascii_case("avro"))
                && !matches!(position, SourcePosition::Initial)
            {
                return Err(ConnectorError::SchemaMismatch("Kafka recovery requires its committed native reader contract; migrate legacy catalog records without rediscovering latest".into()));
            }
            let explicit = config
                .arrow_schema()
                .or_else(|| (!self.schema.fields().is_empty()).then(|| Arc::clone(&self.schema)));
            let binding = match crate::kafka::schema_resolution::resolve_source_with_registry(
                &metadata,
                explicit,
                self.schema_registry.as_deref(),
            )
            .await
            {
                Ok(binding) => binding,
                Err(error) => {
                    if self.config.format == Format::Avro {
                        self.fail_startup();
                    }
                    return Err(error);
                }
            };
            config.set_schema_binding(binding)?;
        }
        self.start_inner(SourceStart::new(config, position, delivery)?)
            .await
    }

    pub(super) async fn start_inner(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        if let Some((config, checkpoint)) = request.initialized_checkpoint() {
            // Read-only full inventory/retention validation precedes active consumer creation.
            // Reuse the sealed numeric vector; never resolve latest, subscribe, poll or commit.
            self.inspect_initial_position_inner(config, Some(checkpoint))
                .await?;
        }
        let KafkaStartPlan {
            config: kafka_config,
            delivery,
            has_saved_position,
            resume_input_channels,
            resume_baselines,
        } = self.prepare_start(request)?;
        self.progress = self
            .metrics_registry
            .as_ref()
            .filter(|_| !self.source_name.is_empty())
            .map(|registry| super::KafkaProgress::register(registry, &self.source_name))
            .transpose()?;
        let mut rdkafka_config: ClientConfig = kafka_config.to_rdkafka_config();
        if delivery != DeliveryGuarantee::BestEffort
            || matches!(
                &kafka_config.startup_mode,
                StartupMode::SpecificOffsets(_) | StartupMode::Timestamp(_)
            )
        {
            // Once the engine owns the cursor, retention must surface as a fault. Allowing
            // librdkafka to auto-reset would silently cross the sealed checkpoint cut after the
            // preflight watermark validation (including a retention race while paused).
            rdkafka_config.set("auto.offset.reset", "error");
        }
        if has_saved_position {
            rdkafka_config.set("allow.auto.create.topics", "false");
        }
        let context = LaminarConsumerContext::new(
            Arc::clone(&self.rebalance_state),
            Arc::clone(&self.rebalance_counter),
            Arc::clone(&self.revoke_generation),
            Arc::clone(&self.assign_generation),
            // IntCounter::clone is an Arc bump; these are shared with the
            // metrics struct and bumped from librdkafka's background thread
            // inside `commit_callback`.
            self.metrics.commits.clone(),
            self.metrics.commit_failures.clone(),
        );
        let consumer: StreamConsumer<LaminarConsumerContext> = rdkafka_config
            .create_with_context(context)
            .map_err(|error| consumer_creation_error(&error))?;
        // Install ownership before any fallible activation work. If metadata, assignment, or
        // subscription fails, the source task's cleanup path can move the final consumer drop to
        // the bounded blocking reaper instead of running librdkafka Drop on a Tokio worker.
        let consumer = Arc::new(consumer);
        self.consumer = Some(Arc::clone(&consumer));

        let vnode_assigned = self
            .assign_vnode_partitions(
                &consumer,
                &kafka_config,
                delivery,
                has_saved_position,
                &resume_baselines,
            )
            .await?;
        let local_guaranteed_assignment = self
            .assign_local_guaranteed_partitions(
                &consumer,
                &kafka_config,
                delivery,
                vnode_assigned,
                has_saved_position,
                resume_input_channels.as_deref(),
                &resume_baselines,
            )
            .await?;
        self.activate_remaining_assignment(
            &consumer,
            &kafka_config,
            vnode_assigned,
            local_guaranteed_assignment,
            has_saved_position,
            resume_input_channels.as_deref(),
            &resume_baselines,
        )
        .await?;

        // Reader startup stays deferred until the first poll. Group
        // assignments are paused by the callback and explicitly seeked from
        // the position installed above before any record can enter the channel.

        self.state = ConnectorState::Running;
        self.start_progress();
        info!("Kafka source connector started successfully");
        Ok(())
    }
}
