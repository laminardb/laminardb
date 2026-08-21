//! Vnode inventory preparation and initial ownership activation.

use super::{
    build_vnode_assignment_tpl, deterministic_initial_offset, fetch_explicit_topic_metadata,
    fetch_partition_low_watermarks, info, kafka_bootstrap_is_unassigned, kafka_input_channels,
    kafka_partition_routes, kafka_partition_set, lock_or_recover, startup_default_offset,
    validate_kafka_assignment, validate_kafka_partition_results, validate_partition_baselines,
    validate_positions_not_expired, Arc, ConnectorError, Consumer, DeliveryGuarantee,
    KafkaAssignmentPublication, KafkaPartitionBaselines, KafkaPartitionSet, KafkaRotationBaselines,
    KafkaSource, KafkaSourceConfig, LaminarConsumerContext, Ordering, StreamConsumer,
    TopicPartitionList, TopicSubscription, VnodeStartInventory,
};

impl KafkaSource {
    pub(super) async fn assign_vnode_partitions(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        is_resume: bool,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<bool, ConnectorError> {
        self.vnode_partition_routes.clear();
        let Some((registry, self_id)) = self
            .vnode_assignment
            .as_ref()
            .map(|(registry, self_id)| (Arc::clone(registry), *self_id))
        else {
            return Ok(false);
        };
        let TopicSubscription::Topics(topics) = &config.subscription else {
            return Err(ConnectorError::ConfigurationError(
                "Kafka vnode assignment requires an explicit topic inventory".into(),
            ));
        };
        self.activate_vnode_assignment(
            consumer,
            config,
            delivery,
            is_resume,
            resume_baselines,
            &registry,
            self_id,
            topics,
        )
        .await?;
        Ok(true)
    }
}

impl KafkaSource {
    async fn activate_vnode_assignment(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        is_resume: bool,
        resume_baselines: &KafkaPartitionBaselines,
        registry: &Arc<laminar_core::state::VnodeRegistry>,
        self_id: laminar_core::state::NodeId,
        topics: &[String],
    ) -> Result<(), ConnectorError> {
        let inventory = self
            .prepare_vnode_inventory(
                consumer,
                config,
                delivery,
                is_resume,
                resume_baselines,
                registry.vnode_count(),
                topics,
            )
            .await?;
        // Pin the final ownership publication only across synchronous librdkafka calls.
        let published = registry.read_assignment();
        let assignment_version = published.version();
        let boot_unassigned = kafka_bootstrap_is_unassigned(&published, self_id)?;
        let tpl = if boot_unassigned {
            TopicPartitionList::new()
        } else {
            build_vnode_assignment_tpl(
                self.source_name.as_ref(),
                published.owners(),
                self_id,
                &inventory.topics,
                &self.offsets,
                &self.manual_partition_baselines,
                inventory.default_offset,
            )?
        };
        let owned_partitions =
            Arc::new(kafka_partition_set(&tpl).map_err(ConnectorError::ConfigurationError)?);
        // Incremental from empty so every later rebind can remain incremental.
        if tpl.count() > 0 {
            consumer.incremental_assign(&tpl).map_err(|e| {
                ConnectorError::ConnectionFailed(format!("vnode partition assign failed: {e}"))
            })?;
        }
        validate_kafka_partition_results("initial incremental assign", &tpl)
            .map_err(ConnectorError::ConnectionFailed)?;
        let active = consumer.assignment().map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "failed to inspect initial vnode assignment: {error}"
            ))
        })?;
        let active = kafka_partition_set(&active).map_err(ConnectorError::ConnectionFailed)?;
        validate_kafka_assignment(&owned_partitions, &active)
            .map_err(ConnectorError::ConnectionFailed)?;
        self.vnode_partition_routes = inventory.routes;
        let input_channels = kafka_input_channels(self.source_name.as_ref(), &owned_partitions)?;
        *lock_or_recover(&self.assignment_publication) = Arc::new(KafkaAssignmentPublication::new(
            assignment_version,
            Arc::clone(&owned_partitions),
            input_channels,
            KafkaRotationBaselines::new(),
        ));
        self.reconciled_assignment_version
            .store(assignment_version, Ordering::Release);
        drop(published);
        if boot_unassigned {
            info!("Kafka source started fenced with no partitions until durable vnode adoption");
        } else {
            info!(
                owned_partitions = owned_partitions.len(),
                "Kafka source assigned vnode-owned partitions (engine-controlled)"
            );
        }
        Ok(())
    }

    async fn prepare_vnode_inventory(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        is_resume: bool,
        resume_baselines: &KafkaPartitionBaselines,
        vnode_count: u32,
        topics: &[String],
    ) -> Result<VnodeStartInventory, ConnectorError> {
        let topic_meta = fetch_explicit_topic_metadata(
            self.blocking_tasks.clone(),
            Arc::clone(consumer),
            topics.to_vec(),
        )
        .await?;
        let partition_routes =
            kafka_partition_routes(self.source_name.as_ref(), vnode_count, &topic_meta)?;
        let all_partitions: KafkaPartitionSet = topic_meta
            .iter()
            .flat_map(|(topic, count)| {
                (0..*count).map(move |partition| (topic.to_string(), partition))
            })
            .collect();
        if let Some(unexpected) = self
            .offsets
            .to_topic_partition_list()
            .elements()
            .iter()
            .find(|entry| !all_partitions.contains(&(entry.topic().to_string(), entry.partition())))
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "Kafka resume checkpoint references partition '{}-{}' absent from the explicit topic inventory",
                unexpected.topic(),
                unexpected.partition()
            )));
        }
        let requires_numeric_cut = delivery != DeliveryGuarantee::BestEffort;
        if requires_numeric_cut {
            let low_watermarks = fetch_partition_low_watermarks(
                self.blocking_tasks.clone(),
                Arc::clone(consumer),
                &all_partitions,
            )
            .await?;
            let baselines = if is_resume {
                validate_partition_baselines(resume_baselines, &all_partitions)?;
                resume_baselines.clone()
            } else {
                low_watermarks.clone()
            };
            validate_positions_not_expired(
                &self.offsets,
                &baselines,
                &low_watermarks,
                &all_partitions,
            )?;
            self.manual_partition_baselines = baselines;
        }
        self.manual_topic_partitions = all_partitions;
        let default_offset = if self
            .deterministic_unrecorded_position
            .load(Ordering::Acquire)
        {
            deterministic_initial_offset(&config.startup_mode, config.auto_offset_reset)
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "Kafka startup mode has no deterministic vnode fallback".into(),
                    )
                })?
        } else {
            startup_default_offset(&config.startup_mode)
        };
        Ok(VnodeStartInventory {
            topics: topic_meta,
            routes: partition_routes,
            default_offset,
        })
    }
}
