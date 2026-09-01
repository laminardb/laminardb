//! Local guaranteed, group, specific-offset, and timestamp startup modes.

use super::{
    assignment_seek_tpl, fetch_explicit_topic_metadata, fetch_partition_low_watermarks,
    fetch_partition_watermarks, info, kafka_input_channels, resolve_timestamp_offsets,
    validate_partition_baselines, validate_positions_not_expired, validate_resume_input_channels,
    Arc, ConnectorError, Consumer, DeliveryGuarantee, KafkaPartitionBaselines, KafkaPartitionSet,
    KafkaSource, KafkaSourceConfig, LaminarConsumerContext, StartupMode, StreamConsumer,
    TopicPartitionList, TopicSubscription,
};

impl KafkaSource {
    pub(super) async fn assign_local_guaranteed_partitions(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        vnode_assigned: bool,
        is_resume: bool,
        resume_input_channels: Option<&[Vec<u8>]>,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<bool, ConnectorError> {
        let local_guaranteed_assignment = delivery != DeliveryGuarantee::BestEffort
            && !vnode_assigned
            && matches!(&config.startup_mode, StartupMode::Earliest);
        if local_guaranteed_assignment {
            let TopicSubscription::Topics(topics) = &config.subscription else {
                return Err(ConnectorError::ConfigurationError(
                    "Kafka guaranteed delivery requires an explicit topic inventory".into(),
                ));
            };
            let topic_meta = fetch_explicit_topic_metadata(
                self.blocking_tasks.clone(),
                Arc::clone(consumer),
                topics.clone(),
            )
            .await?;
            let assigned: Vec<(String, i32)> = topic_meta
                .iter()
                .flat_map(|(topic, count)| {
                    (0..*count).map(move |partition| (topic.to_string(), partition))
                })
                .collect();
            let assigned_set: KafkaPartitionSet = assigned.iter().cloned().collect();
            if let Some(unexpected) = self
                .offsets
                .to_topic_partition_list()
                .elements()
                .iter()
                .find(|entry| {
                    !assigned_set.contains(&(entry.topic().to_string(), entry.partition()))
                })
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "Kafka resume checkpoint references partition '{}-{}' absent from the explicit topic inventory",
                    unexpected.topic(),
                    unexpected.partition()
                )));
            }
            let low_watermarks = fetch_partition_low_watermarks(
                self.blocking_tasks.clone(),
                Arc::clone(consumer),
                &assigned_set,
            )
            .await?;
            let baselines = if is_resume {
                validate_resume_input_channels(
                    self.source_name.as_ref(),
                    resume_input_channels,
                    &assigned_set,
                )?;
                validate_partition_baselines(resume_baselines, &assigned_set)?;
                resume_baselines.clone()
            } else {
                low_watermarks.clone()
            };
            validate_positions_not_expired(
                &self.offsets,
                &baselines,
                &low_watermarks,
                &assigned_set,
            )?;
            let assignment =
                assignment_seek_tpl(&self.offsets, &assigned, Some(&baselines), None, true)?;
            consumer.assign(&assignment).map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "failed to install local guaranteed Kafka assignment: {error}"
                ))
            })?;
            self.manual_input_channels =
                kafka_input_channels(self.source_name.as_ref(), &assigned_set)?;
            self.manual_topic_partitions = assigned_set;
            self.manual_partition_baselines = baselines;
            info!(
                partition_count = assignment.count(),
                "Kafka source assigned full explicit inventory (local guaranteed delivery)"
            );
        }

        Ok(local_guaranteed_assignment)
    }
}

impl KafkaSource {
    pub(super) async fn activate_remaining_assignment(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        kafka_config: &KafkaSourceConfig,
        vnode_assigned: bool,
        local_guaranteed_assignment: bool,
        is_resume: bool,
        resume_input_channels: Option<&[Vec<u8>]>,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<(), ConnectorError> {
        if vnode_assigned || local_guaranteed_assignment {
            return Ok(());
        }
        match &kafka_config.startup_mode {
            StartupMode::GroupOffsets | StartupMode::Earliest | StartupMode::Latest => {
                Self::subscribe_group_startup(consumer, &kafka_config.subscription)?;
            }
            StartupMode::SpecificOffsets(offsets) => {
                self.assign_specific_startup(
                    consumer,
                    kafka_config,
                    offsets,
                    is_resume,
                    resume_input_channels,
                    resume_baselines,
                )
                .await?;
            }
            StartupMode::Timestamp(timestamp_ms) => {
                self.assign_timestamp_startup(
                    consumer,
                    kafka_config,
                    *timestamp_ms,
                    is_resume,
                    resume_input_channels,
                    resume_baselines,
                )
                .await?;
            }
        }
        Ok(())
    }
}

impl KafkaSource {
    fn subscribe_group_startup(
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        subscription: &TopicSubscription,
    ) -> Result<(), ConnectorError> {
        match subscription {
            TopicSubscription::Topics(topics) => {
                let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
                consumer.subscribe(&topic_refs).map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to subscribe: {e}"))
                })?;
            }
            TopicSubscription::Pattern(pattern) => {
                let regex_pattern = if pattern.starts_with('^') {
                    pattern.clone()
                } else {
                    format!("^{pattern}")
                };
                consumer.subscribe(&[&regex_pattern]).map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to subscribe to pattern: {e}"))
                })?;
            }
        }
        Ok(())
    }

    async fn assign_specific_startup(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        kafka_config: &KafkaSourceConfig,
        offsets: &std::collections::HashMap<i32, i64>,
        is_resume: bool,
        resume_input_channels: Option<&[Vec<u8>]>,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<(), ConnectorError> {
        let TopicSubscription::Topics(topics) = &kafka_config.subscription else {
            return Err(ConnectorError::ConfigurationError(
                "Kafka specific-offset startup requires an explicit topic inventory".into(),
            ));
        };
        let mut assigned: Vec<(String, i32)> = topics
            .iter()
            .flat_map(|topic| {
                offsets
                    .keys()
                    .copied()
                    .map(move |partition| (topic.clone(), partition))
            })
            .collect();
        assigned.sort_unstable();
        if assigned.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "Kafka specific-offset startup resolved no partitions".into(),
            ));
        }
        let assigned_set: KafkaPartitionSet = assigned.iter().cloned().collect();
        let configured_baselines: KafkaPartitionBaselines = topics
            .iter()
            .flat_map(|topic| {
                offsets
                    .iter()
                    .map(move |(&partition, &next)| ((topic.clone(), partition), next))
            })
            .collect();
        let baselines = if is_resume {
            validate_resume_input_channels(
                self.source_name.as_ref(),
                resume_input_channels,
                &assigned_set,
            )?;
            validate_partition_baselines(resume_baselines, &assigned_set)?;
            if resume_baselines != &configured_baselines {
                return Err(ConnectorError::ConfigurationError(
                    "Kafka specific-offset configuration changed across recovery".into(),
                ));
            }
            resume_baselines.clone()
        } else {
            configured_baselines
        };
        let low_watermarks = fetch_partition_low_watermarks(
            self.blocking_tasks.clone(),
            Arc::clone(consumer),
            &assigned_set,
        )
        .await?;
        validate_positions_not_expired(&self.offsets, &baselines, &low_watermarks, &assigned_set)?;
        let assignment =
            assignment_seek_tpl(&self.offsets, &assigned, Some(&baselines), None, true)?;
        consumer.assign(&assignment).map_err(|e| {
            ConnectorError::ConnectionFailed(format!("failed to assign specific offsets: {e}"))
        })?;
        self.manual_input_channels =
            kafka_input_channels(self.source_name.as_ref(), &assigned_set)?;
        self.manual_topic_partitions = assigned_set;
        self.manual_partition_baselines = baselines;
        info!(
            partition_count = assignment.count(),
            "assigned consumer to exact checkpoint/specific offsets"
        );
        Ok(())
    }

    async fn assign_timestamp_startup(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        kafka_config: &KafkaSourceConfig,
        ts_ms: i64,
        is_resume: bool,
        resume_input_channels: Option<&[Vec<u8>]>,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<(), ConnectorError> {
        let TopicSubscription::Topics(topics) = &kafka_config.subscription else {
            return Err(ConnectorError::ConfigurationError(
                "Kafka timestamp startup requires an explicit topic inventory".into(),
            ));
        };
        let topic_meta = fetch_explicit_topic_metadata(
            self.blocking_tasks.clone(),
            Arc::clone(consumer),
            topics.clone(),
        )
        .await?;
        let assigned: Vec<(String, i32)> = topic_meta
            .iter()
            .flat_map(|(topic, partition_count)| {
                (0..*partition_count).map(move |partition| (topic.to_string(), partition))
            })
            .collect();
        if assigned.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "Kafka timestamp startup discovered no partitions".into(),
            ));
        }
        let assigned_set: KafkaPartitionSet = assigned.iter().cloned().collect();
        let baselines = self
            .timestamp_startup_baselines(
                consumer,
                &assigned,
                &assigned_set,
                ts_ms,
                is_resume,
                resume_input_channels,
                resume_baselines,
            )
            .await?;
        let positioned =
            assignment_seek_tpl(&self.offsets, &assigned, Some(&baselines), None, true)?;
        consumer.assign(&positioned).map_err(|e| {
            ConnectorError::ConnectionFailed(format!(
                "failed to assign timestamp/checkpoint offsets: {e}"
            ))
        })?;
        self.manual_input_channels =
            kafka_input_channels(self.source_name.as_ref(), &assigned_set)?;
        self.manual_topic_partitions = assigned_set;
        self.manual_partition_baselines = baselines;
        info!(
            timestamp_ms = ts_ms,
            partition_count = positioned.count(),
            "assigned consumer to exact checkpoint/timestamp offsets"
        );
        Ok(())
    }

    async fn timestamp_startup_baselines(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        assigned: &[(String, i32)],
        assigned_set: &KafkaPartitionSet,
        ts_ms: i64,
        is_resume: bool,
        resume_input_channels: Option<&[Vec<u8>]>,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<KafkaPartitionBaselines, ConnectorError> {
        if is_resume {
            validate_resume_input_channels(
                self.source_name.as_ref(),
                resume_input_channels,
                assigned_set,
            )?;
            validate_partition_baselines(resume_baselines, assigned_set)?;
            let low_watermarks = fetch_partition_low_watermarks(
                self.blocking_tasks.clone(),
                Arc::clone(consumer),
                assigned_set,
            )
            .await?;
            validate_positions_not_expired(
                &self.offsets,
                resume_baselines,
                &low_watermarks,
                assigned_set,
            )?;
            return Ok(resume_baselines.clone());
        }

        // Capture broker ends before timestamp resolution. An absent matching record resolves to
        // this numeric activation cut, not symbolic End, so later appends remain readable.
        let (low_watermarks, high_watermarks) = fetch_partition_watermarks(
            self.blocking_tasks.clone(),
            Arc::clone(consumer),
            assigned_set,
        )
        .await?;
        let requested = timestamp_lookup_request(assigned, ts_ms)?;
        let resolved =
            resolve_timestamp_offsets(self.blocking_tasks.clone(), Arc::clone(consumer), requested)
                .await?;
        let baselines = resolved_timestamp_baselines(&resolved, &high_watermarks)?;
        validate_partition_baselines(&baselines, assigned_set)?;
        validate_positions_not_expired(&self.offsets, &baselines, &low_watermarks, assigned_set)?;
        Ok(baselines)
    }
}

fn timestamp_lookup_request(
    assigned: &[(String, i32)],
    timestamp_ms: i64,
) -> Result<TopicPartitionList, ConnectorError> {
    let mut requested = TopicPartitionList::new();
    for (topic, partition) in assigned {
        requested
            .add_partition_offset(topic, *partition, rdkafka::Offset::Offset(timestamp_ms))
            .map_err(|error| {
                ConnectorError::Internal(format!(
                    "failed to build timestamp lookup for '{topic}-{partition}': {error}"
                ))
            })?;
    }
    Ok(requested)
}

fn resolved_timestamp_baselines(
    resolved: &TopicPartitionList,
    high_watermarks: &KafkaPartitionBaselines,
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    let mut baselines = KafkaPartitionBaselines::with_capacity(resolved.count());
    for element in resolved.elements() {
        if let Err(error) = element.error() {
            return Err(ConnectorError::ConnectionFailed(format!(
                "timestamp lookup failed for '{}-{}': {error}",
                element.topic(),
                element.partition()
            )));
        }
        let partition = (element.topic().to_string(), element.partition());
        let next = match element.offset() {
            rdkafka::Offset::Offset(next) if (0..i64::MAX).contains(&next) => next,
            rdkafka::Offset::Invalid | rdkafka::Offset::End => {
                *high_watermarks.get(&partition).ok_or_else(|| {
                    ConnectorError::ConnectionFailed(format!(
                        "Kafka watermark response omitted partition '{}-{}'",
                        partition.0, partition.1
                    ))
                })?
            }
            offset => {
                return Err(ConnectorError::ConnectionFailed(format!(
                    "Kafka timestamp lookup returned non-numeric offset {offset:?} for '{}-{}'",
                    partition.0, partition.1
                )));
            }
        };
        baselines.insert(partition, next);
    }
    Ok(baselines)
}
