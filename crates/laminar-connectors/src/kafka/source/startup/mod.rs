//! Validated source startup, assignment, and schema-registry prefetch.

use super::{
    assignment_seek_tpl, build_vnode_assignment_tpl, consumer_creation_error,
    decode_partition_baselines, deterministic_initial_offset, fetch_explicit_topic_metadata,
    fetch_partition_low_watermarks, fetch_partition_watermarks, info,
    kafka_bootstrap_is_unassigned, kafka_input_channels, kafka_partition_routes,
    kafka_partition_set, lock_or_recover, log_schema_drift, resolve_timestamp_offsets,
    resolve_value_subject, select_deserializer, startup_default_offset, validate_kafka_assignment,
    validate_kafka_output_schema, validate_kafka_partition_results, validate_partition_baselines,
    validate_positions_not_expired, validate_resume_input_channels, warn, Arc, AvroDeserializer,
    ClientConfig, ConnectorError, ConnectorState, Consumer, DeliveryGuarantee, Format,
    KafkaAssignmentPublication, KafkaPartitionBaselines, KafkaPartitionRoutes, KafkaPartitionSet,
    KafkaRotationBaselines, KafkaSource, KafkaSourceConfig, KafkaStartPlan, LaminarConsumerContext,
    OffsetTracker, Ordering, SourcePosition, SourceStart, StartupMode, StreamConsumer,
    TopicPartitionList, TopicSubscription,
};

mod modes;
mod validation;
mod vnode;

use validation::VnodeStartInventory;

impl KafkaSource {
    pub(super) async fn prefetch_schema_registry(
        &mut self,
        config: &KafkaSourceConfig,
    ) -> Result<(), ConnectorError> {
        // Eagerly fetch the SR schema so the Arrow schema is available at
        // plan time (before the first poll_batch).
        if let Some(ref sr) = self.schema_registry {
            if let TopicSubscription::Topics(topics) = &config.subscription {
                if topics.len() > 1 {
                    warn!("multiple topics with schema registry — using first topic's schema");
                }
                if let Some(topic) = topics.first() {
                    let subject = resolve_value_subject(
                        config.schema_registry_subject_strategy,
                        config.schema_registry_record_name.as_deref(),
                        topic,
                    );
                    match tokio::time::timeout(
                        config.schema_registry_discovery_timeout,
                        sr.get_latest_schema(&subject),
                    )
                    .await
                    {
                        Ok(Ok(cached)) => {
                            if let Some(avro_deser) = self
                                .deserializer
                                .as_any_mut()
                                .and_then(|any| any.downcast_mut::<AvroDeserializer>())
                            {
                                if let Err(error) =
                                    avro_deser.register_schema(cached.id, &cached.schema_str)
                                {
                                    let error = ConnectorError::Serde(error);
                                    self.fail_startup();
                                    return Err(error);
                                }
                                // Keep the catalog schema pinned — planner
                                // plans are already built against it.
                                log_schema_drift(&self.schema, &cached.arrow_schema, &subject);
                                info!(%subject, schema_id = cached.id,
                                    "SR schema fetched at start()");
                                self.last_avro_schema = Some(cached.arrow_schema);
                            }
                        }
                        Ok(Err(e)) if e.is_transient() => {
                            warn!(%subject, error = %e, "SR unavailable at start(), will resolve lazily");
                        }
                        Ok(Err(e)) => {
                            self.fail_startup();
                            return Err(e);
                        }
                        Err(_elapsed) => {
                            warn!(%subject, "SR prefetch timed out at start(), will resolve lazily");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl KafkaSource {
    pub(super) async fn start_inner(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let KafkaStartPlan {
            config: kafka_config,
            delivery,
            is_resume,
            resume_input_channels,
            resume_baselines,
        } = self.prepare_start(request)?;
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
                is_resume,
                &resume_baselines,
            )
            .await?;
        let local_guaranteed_assignment = self
            .assign_local_guaranteed_partitions(
                &consumer,
                &kafka_config,
                delivery,
                vnode_assigned,
                is_resume,
                resume_input_channels.as_deref(),
                &resume_baselines,
            )
            .await?;
        self.activate_remaining_assignment(
            &consumer,
            &kafka_config,
            vnode_assigned,
            local_guaranteed_assignment,
            is_resume,
            resume_input_channels.as_deref(),
            &resume_baselines,
        )
        .await?;

        // Reader startup stays deferred until the first poll. Group
        // assignments are paused by the callback and explicitly seeked from
        // the position installed above before any record can enter the channel.

        self.prefetch_schema_registry(&kafka_config).await?;

        self.state = ConnectorState::Running;
        info!("Kafka source connector started successfully");
        Ok(())
    }
}
