//! Vnode assignment delta preparation, validation, and publication.

use super::{
    acquired_numeric_position, fetch_partition_low_watermarks, info, kafka_input_channels,
    kafka_owned_partition_sets, kafka_partition_set, lock_or_recover, publish_reader_fault,
    rotation_baselines_len, update_rotation_baselines, validate_kafka_assignment,
    validate_kafka_partition_results, validate_positions_not_expired, warn, Arc, Consumer,
    KafkaAssignmentPublication, KafkaPartitionBaselines, KafkaPartitionSet, KafkaRotationContext,
    LaminarConsumerContext, OffsetTracker, Ordering, ReaderLoopAction, StreamConsumer,
    TopicPartitionList,
};

struct PreparedRotation {
    version: u64,
    owned_set: Arc<KafkaPartitionSet>,
    to_remove: TopicPartitionList,
    to_add: TopicPartitionList,
    acquired_positions: KafkaPartitionBaselines,
    input_channels: Option<Arc<[Vec<u8>]>>,
}

pub(super) async fn reconcile_vnode_assignment(
    mut context: KafkaRotationContext<'_>,
    last_assignment_version: &mut u64,
) -> ReaderLoopAction {
    let Some((registry, self_id)) = context
        .vnode_reassign
        .as_ref()
        .map(|(registry, self_id)| (Arc::clone(registry), *self_id))
    else {
        return ReaderLoopAction::Proceed;
    };
    let published = registry.versioned_snapshot();
    let version = published.version();
    if version == *last_assignment_version {
        return ReaderLoopAction::Proceed;
    }

    let current = match context.consumer.assignment() {
        Ok(current) => current,
        Err(error) => {
            warn!(
                version,
                %error,
                "Kafka source could not inspect its current assignment; rotation will retry"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            return ReaderLoopAction::Retry;
        }
    };
    let Some(mut rotation) = prepare_rotation(
        &context,
        &published,
        self_id,
        *last_assignment_version,
        &current,
    ) else {
        return ReaderLoopAction::Stop;
    };

    // INVARIANT: publish the ownership cut before the first await. A checkpoint must never
    // resurrect a position from this node's earlier ownership stint.
    publish_rotation_cut(&context, &mut rotation);
    let validation = validate_acquired_positions(&context, &rotation).await;
    if validation != ReaderLoopAction::Proceed {
        return validation;
    }

    if !apply_rotation_delta(context.consumer, &rotation) {
        match tokio::time::timeout(
            std::time::Duration::from_millis(10),
            context.reader_shutdown.changed(),
        )
        .await
        {
            Ok(Ok(())) if *context.reader_shutdown.borrow() => return ReaderLoopAction::Stop,
            Ok(Err(_)) => return ReaderLoopAction::Stop,
            _ => return ReaderLoopAction::Retry,
        }
    }

    let current_assignment = registry.read_assignment();
    if current_assignment.version() != rotation.version {
        return ReaderLoopAction::Retry;
    }
    *last_assignment_version = rotation.version;
    context
        .reconciled_assignment_version
        .store(rotation.version, Ordering::Release);
    drop(current_assignment);
    finish_rotation(&mut context, &rotation);
    ReaderLoopAction::Proceed
}

fn prepare_rotation(
    context: &KafkaRotationContext<'_>,
    published: &laminar_core::state::VnodeAssignmentSnapshot,
    self_id: laminar_core::state::NodeId,
    last_assignment_version: u64,
    current: &TopicPartitionList,
) -> Option<PreparedRotation> {
    let current_set = match kafka_partition_set(current) {
        Ok(current) => current,
        Err(error) => {
            publish_reader_fault(context.reader_fault, context.data_ready, error);
            return None;
        }
    };
    let (owned_set, reacquired) = match kafka_owned_partition_sets(
        context.vnode_partition_routes,
        published,
        self_id,
        last_assignment_version,
    ) {
        Ok(partitions) => partitions,
        Err(error) => {
            warn!(
                source = context.source_name.as_ref(),
                %error,
                "Kafka source rejected its cached partition routes"
            );
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                format!("invalid cached partition route: {error}"),
            );
            return None;
        }
    };

    let mut to_remove = TopicPartitionList::new();
    for (topic, partition) in current_set
        .difference(&owned_set)
        .chain(reacquired.intersection(&current_set))
    {
        to_remove.add_partition(topic, *partition);
    }

    let offsets = lock_or_recover(context.reassign_snapshot).clone();
    let mut to_add = TopicPartitionList::new();
    let mut acquired_positions = KafkaPartitionBaselines::new();
    for (topic, partition) in owned_set
        .difference(&current_set)
        .chain(reacquired.intersection(&current_set))
    {
        let offset = rotation_partition_offset(
            context,
            &offsets,
            topic,
            *partition,
            &mut acquired_positions,
        )?;
        if let Err(error) = to_add.add_partition_offset(topic, *partition, offset) {
            warn!(
                topic = topic.as_str(),
                partition,
                %error,
                "failed to build Kafka rotation assignment"
            );
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                format!("invalid rotation assignment: {error}"),
            );
            return None;
        }
    }

    let owned_set = Arc::new(owned_set);
    let input_channels = match kafka_input_channels(context.source_name.as_ref(), &owned_set) {
        Ok(input_channels) => input_channels,
        Err(error) => {
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                format!("invalid rotated Kafka input channels: {error}"),
            );
            return None;
        }
    };
    Some(PreparedRotation {
        version: published.version(),
        owned_set,
        to_remove,
        to_add,
        acquired_positions,
        input_channels: Some(input_channels),
    })
}

fn rotation_partition_offset(
    context: &KafkaRotationContext<'_>,
    offsets: &OffsetTracker,
    topic: &str,
    partition: i32,
    acquired_positions: &mut KafkaPartitionBaselines,
) -> Option<rdkafka::Offset> {
    let durable =
        match acquired_numeric_position(offsets, context.reassign_baselines, topic, partition) {
            Ok(position) => position,
            Err(error) => {
                warn!(
                    topic,
                    partition,
                    %error,
                    "Kafka source rejected an invalid checkpoint position"
                );
                publish_reader_fault(
                    context.reader_fault,
                    context.data_ready,
                    format!("invalid checkpoint position: {error}"),
                );
                return None;
            }
        };
    if let Some(next) = durable {
        info!(
            topic,
            partition,
            resume = next,
            "acquired partition uses durable numeric position"
        );
        acquired_positions.insert((topic.to_string(), partition), next);
        return Some(rdkafka::Offset::Offset(next));
    }
    if context.require_durable_baselines {
        warn!(
            topic,
            partition, "acquired Kafka partition has no durable next-to-read baseline"
        );
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "acquired partition has no durable baseline",
        );
        return None;
    }
    if context.deterministic_unrecorded.load(Ordering::Acquire) {
        let Some(initial_offset) = context.deterministic_default else {
            warn!(
                topic,
                partition, "cannot deterministically position acquired Kafka partition"
            );
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                "acquired partition has no deterministic position",
            );
            return None;
        };
        return Some(initial_offset);
    }

    warn!(
        topic,
        partition,
        "acquired partition has no checkpoint or local offset; falling back to the startup default"
    );
    Some(context.reassign_default_offset)
}

fn publish_rotation_cut(context: &KafkaRotationContext<'_>, rotation: &mut PreparedRotation) {
    let input_channels = rotation
        .input_channels
        .take()
        .expect("prepared rotation owns its input channels");
    let mut current = lock_or_recover(context.assignment_publication);
    let updated = update_rotation_baselines(
        &current.baselines,
        &rotation.owned_set,
        &rotation.acquired_positions,
    );
    let count = rotation_baselines_len(&updated);
    *current = Arc::new(KafkaAssignmentPublication::new(
        rotation.version,
        Arc::clone(&rotation.owned_set),
        input_channels,
        updated,
    ));
    context
        .rotation_baseline_count
        .store(count, Ordering::Release);
}

async fn validate_acquired_positions(
    context: &KafkaRotationContext<'_>,
    rotation: &PreparedRotation,
) -> ReaderLoopAction {
    if rotation.acquired_positions.is_empty() {
        return ReaderLoopAction::Proceed;
    }
    let acquired: KafkaPartitionSet = rotation.acquired_positions.keys().cloned().collect();
    let low_watermarks = match fetch_partition_low_watermarks(
        context.blocking_tasks.clone(),
        Arc::clone(context.consumer),
        &acquired,
    )
    .await
    {
        Ok(low_watermarks) => low_watermarks,
        Err(error) => {
            warn!(
                version = rotation.version,
                %error,
                "Kafka source could not validate acquired positions; rotation will retry"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return ReaderLoopAction::Retry;
        }
    };
    if let Err(error) = validate_positions_not_expired(
        &OffsetTracker::new(),
        &rotation.acquired_positions,
        &low_watermarks,
        &acquired,
    ) {
        warn!(
            version = rotation.version,
            %error,
            "Kafka source rejected an expired checkpoint position"
        );
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            format!("expired checkpoint position: {error}"),
        );
        return ReaderLoopAction::Stop;
    }
    ReaderLoopAction::Proceed
}

fn apply_rotation_delta(
    consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
    rotation: &PreparedRotation,
) -> bool {
    let mut complete = true;
    if rotation.to_remove.count() > 0 {
        match consumer.incremental_unassign(&rotation.to_remove) {
            Ok(()) => {
                if let Err(error) =
                    validate_kafka_partition_results("incremental unassign", &rotation.to_remove)
                {
                    warn!(
                        version = rotation.version,
                        %error,
                        "Kafka source unassign incomplete"
                    );
                    complete = false;
                }
            }
            Err(error) => {
                warn!(
                    version = rotation.version,
                    %error,
                    "Kafka source unassign failed"
                );
                complete = false;
            }
        }
    }
    if rotation.to_add.count() > 0 {
        match consumer.incremental_assign(&rotation.to_add) {
            Ok(()) => {
                if let Err(error) =
                    validate_kafka_partition_results("incremental assign", &rotation.to_add)
                {
                    warn!(
                        version = rotation.version,
                        %error,
                        "Kafka source assign incomplete"
                    );
                    complete = false;
                }
            }
            Err(error) => {
                warn!(
                    version = rotation.version,
                    %error,
                    "Kafka source assign failed"
                );
                complete = false;
            }
        }
    }
    if !complete {
        return false;
    }

    match consumer.assignment() {
        Ok(active) => kafka_partition_set(&active)
            .and_then(|active| validate_kafka_assignment(&rotation.owned_set, &active))
            .map_or_else(
                |error| {
                    warn!(
                        version = rotation.version,
                        %error,
                        "Kafka source assignment verification failed"
                    );
                    false
                },
                |()| true,
            ),
        Err(error) => {
            warn!(
                version = rotation.version,
                %error,
                "Kafka source could not verify its rebound assignment"
            );
            false
        }
    }
}

fn finish_rotation(context: &mut KafkaRotationContext<'_>, rotation: &PreparedRotation) {
    if rotation.to_remove.count() > 0 || rotation.to_add.count() > 0 {
        info!(
            version = rotation.version,
            acquired = rotation.to_add.count(),
            revoked = rotation.to_remove.count(),
            "Kafka source rebound partitions after vnode rotation"
        );
    }
    for element in rotation.to_add.elements() {
        context
            .drain_paused
            .remove(&(Arc::from(element.topic()), element.partition()));
    }
    context.drain_paused.retain(|(topic, partition)| {
        rotation
            .owned_set
            .contains(&(topic.to_string(), *partition))
    });
    if let Some(active) = context.active_drain.as_mut() {
        active.held_assignment_version = None;
        active.hold_complete = false;
    }
    lock_or_recover(context.reassign_snapshot).retain_assigned(&rotation.owned_set);
    context.data_ready.notify_one();
}
