//! Fail-closed startup policy and durable-position installation.

use super::{
    decode_partition_baselines, info, lock_or_recover, select_deserializer,
    validate_kafka_output_schema, Arc, AvroDeserializer, ConnectorError, ConnectorState,
    DeliveryGuarantee, Format, KafkaAssignmentPublication, KafkaPartitionBaselines,
    KafkaPartitionRoutes, KafkaSource, KafkaSourceConfig, KafkaStartPlan, OffsetTracker, Ordering,
    SourcePosition, SourceStart, StartupMode, TopicSubscription,
};

struct PreparedStartPosition {
    offsets: OffsetTracker,
    resume_attempt: Option<laminar_core::checkpoint::CheckpointAttempt>,
    is_resume: bool,
    input_channels: Option<Vec<Vec<u8>>>,
    baselines: KafkaPartitionBaselines,
}

pub(super) struct VnodeStartInventory {
    pub(super) topics: Vec<(Arc<str>, i32)>,
    pub(super) routes: KafkaPartitionRoutes,
    pub(super) default_offset: rdkafka::Offset,
}

fn prepare_start_position(
    position: SourcePosition,
) -> Result<PreparedStartPosition, ConnectorError> {
    match position {
        SourcePosition::Initial => Ok(PreparedStartPosition {
            offsets: OffsetTracker::new(),
            resume_attempt: None,
            is_resume: false,
            input_channels: None,
            baselines: KafkaPartitionBaselines::new(),
        }),
        SourcePosition::Resume {
            attempt,
            checkpoint,
        } => {
            let input_channels = checkpoint.input_channels().map(<[Vec<u8>]>::to_vec);
            let baselines = decode_partition_baselines(&checkpoint)?;
            let offsets = OffsetTracker::try_from_checkpoint(&checkpoint)?;
            Ok(PreparedStartPosition {
                offsets,
                resume_attempt: Some(attempt),
                is_resume: true,
                input_channels,
                baselines,
            })
        }
    }
}

impl KafkaSource {
    pub(super) fn prepare_start(
        &mut self,
        request: SourceStart,
    ) -> Result<KafkaStartPlan, ConnectorError> {
        let (config, position, delivery) = request.into_parts();

        // Resolve and validate the complete cursor policy before creating a
        // consumer. `StreamConsumer` construction starts librdkafka background
        // activity, so no malformed durable position may reach that boundary.
        let kafka_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            KafkaSourceConfig::from_config(&config)?
        };
        let prepared_position = prepare_start_position(position)?;
        let PreparedStartPosition {
            offsets: installed_offsets,
            resume_attempt,
            is_resume,
            input_channels: resume_input_channels,
            baselines: resume_baselines,
        } = prepared_position;
        self.validate_start_policy(&kafka_config, delivery)?;

        let deterministic_unrecorded = is_resume || delivery != DeliveryGuarantee::BestEffort;
        let configured_source_name = config.get("laminar.source.name");
        if self.vnode_assignment.is_some() {
            if configured_source_name
                .is_some_and(|configured| configured != self.source_name.as_ref())
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "Kafka vnode assignment identity '{}' does not match canonical source name \
                     '{}'",
                    self.source_name,
                    configured_source_name.unwrap_or_default()
                )));
            }
        } else {
            self.source_name = Arc::from(configured_source_name.unwrap_or_default());
        }
        self.install_prepared_start(
            &kafka_config,
            delivery,
            installed_offsets,
            deterministic_unrecorded,
            resume_attempt,
        );

        self.select_start_deserializer(&kafka_config)?;

        if let Some(schema) = config.arrow_schema() {
            info!(
                fields = schema.fields().len(),
                "using SQL-defined schema for deserialization"
            );
            self.schema = schema;
        }
        validate_kafka_output_schema(
            &self.schema,
            kafka_config.include_metadata,
            kafka_config.include_headers,
        )?;

        info!(
            brokers = %kafka_config.bootstrap_servers,
            subscription = ?kafka_config.subscription,
            group_id = %kafka_config.group_id,
            format = %kafka_config.format,
            schema_fields = self.schema.fields().len(),
            "starting Kafka source connector"
        );

        Ok(KafkaStartPlan {
            config: kafka_config,
            delivery,
            is_resume,
            resume_input_channels,
            resume_baselines,
        })
    }

    fn validate_start_policy(
        &self,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
    ) -> Result<(), ConnectorError> {
        if (self.vnode_assignment.is_some() || delivery != DeliveryGuarantee::BestEffort)
            && matches!(&config.subscription, TopicSubscription::Pattern(_))
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka topic patterns are unsupported with engine-owned assignment; declare the \
                 exact topic inventory so ownership and checkpoint cuts stay stable"
                    .into(),
            ));
        }
        if matches!(
            &config.startup_mode,
            StartupMode::SpecificOffsets(_) | StartupMode::Timestamp(_)
        ) {
            if matches!(&config.subscription, TopicSubscription::Pattern(_)) {
                return Err(ConnectorError::ConfigurationError(
                    "Kafka specific-offset/timestamp startup requires an explicit topic list"
                        .into(),
                ));
            }
            if self.vnode_assignment.is_some() {
                return Err(ConnectorError::ConfigurationError(
                    "Kafka specific-offset/timestamp startup is unsupported with vnode assignment"
                        .into(),
                ));
            }
        }
        if let StartupMode::SpecificOffsets(offsets) = &config.startup_mode {
            if let Some((&partition, &offset)) = offsets
                .iter()
                .find(|(partition, offset)| **partition < 0 || **offset < 0)
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid Kafka specific position {partition}:{offset}: partition and offset must be non-negative"
                )));
            }
        }
        if delivery != DeliveryGuarantee::BestEffort
            && matches!(&config.startup_mode, StartupMode::GroupOffsets)
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka guaranteed delivery cannot use broker group offsets as its initial recovery \
                 authority; another group member can advance that cursor before LaminarDB seals \
                 it. Use earliest or explicit specific offsets"
                    .into(),
            ));
        }
        if delivery != DeliveryGuarantee::BestEffort
            && matches!(&config.startup_mode, StartupMode::Latest)
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka guaranteed delivery requires a stable unrecorded-partition start; latest \
                 can move forward across recovery. Use earliest, timestamp, or explicit specific \
                 offsets"
                    .into(),
            ));
        }

        Ok(())
    }

    fn install_prepared_start(
        &mut self,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        installed_offsets: OffsetTracker,
        deterministic_unrecorded: bool,
        resume_attempt: Option<laminar_core::checkpoint::CheckpointAttempt>,
    ) {
        self.state = ConnectorState::Initializing;
        self.config = config.clone();
        self.delivery = delivery;
        self.offsets = installed_offsets;
        self.manual_topic_partitions.clear();
        self.manual_input_channels = Arc::from([]);
        self.manual_partition_baselines.clear();
        *lock_or_recover(&self.assignment_publication) =
            Arc::new(KafkaAssignmentPublication::default());
        self.rotation_partition_baseline_count
            .store(0, Ordering::Release);
        self.applied_rotation_baseline_version = None;
        self.batch_cursor_assignment_version = None;
        self.reconciled_assignment_version
            .store(0, Ordering::Release);
        lock_or_recover(&self.offset_snapshot).clone_from(&self.offsets);
        self.deterministic_unrecorded_position
            .store(deterministic_unrecorded, Ordering::Release);
        if let Some(attempt) = resume_attempt {
            // Manual assignment has no rebalance callback, so explicitly arm
            // the first reader iteration to apply the installed exact cursor.
            self.assign_generation.fetch_add(1, Ordering::Release);
            info!(
                epoch = attempt.epoch,
                checkpoint_id = attempt.checkpoint_id,
                partition_count = self.offsets.partition_count(),
                "installed exact Kafka resume position before consumer activation"
            );
        }
    }

    fn select_start_deserializer(
        &mut self,
        config: &KafkaSourceConfig,
    ) -> Result<(), ConnectorError> {
        if let Some(sr_client) = Self::build_sr_client(config)? {
            let sr = Arc::new(sr_client);
            self.schema_registry = Some(Arc::clone(&sr));
            self.deserializer = if config.format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(sr))
            } else {
                select_deserializer(config.format)
            };
        } else if let Some(ref sr) = self.schema_registry {
            // Preserve SR client injected via with_schema_registry().
            self.deserializer = if config.format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(Arc::clone(sr)))
            } else {
                select_deserializer(config.format)
            };
        } else {
            self.deserializer = select_deserializer(config.format);
        }
        self.last_avro_schema = None;
        Ok(())
    }
}
