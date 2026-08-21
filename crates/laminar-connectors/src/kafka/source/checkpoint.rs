//! Partition positions, recovery baselines, and checkpoint capture.

use super::{
    lock_or_recover, try_capture_at_assignment_fence, Arc, BinaryBuilder, ConnectorError, DataType,
    Field, KafkaAssignmentPublication, KafkaError, KafkaPartitionBaselines, KafkaPartitionSet,
    KafkaRotationBaselines, KafkaSource, NonZeroU64, OffsetReset, OffsetTracker, RecordBatch,
    Schema, SchemaRef, SourceCheckpoint, SourceCheckpointDelta, SourceMutation, SourceRowPositions,
    StartupMode, TimeUnit, TopicPartitionList, UInt32Array, KAFKA_PARTITION_BASELINE_PREFIX,
};

impl KafkaSource {
    pub(super) fn capture_vnode_checkpoint(
        &self,
        publication: &KafkaAssignmentPublication,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        let mut checkpoint = self.offsets.to_checkpoint_for_partitions(
            publication
                .owned_partitions
                .iter()
                .filter(|(topic, partition)| {
                    rotation_partition_baseline(&publication.baselines, topic.as_str(), *partition)
                        .is_none()
                })
                .map(|(topic, partition)| (topic.as_str(), *partition)),
        );
        attach_partition_baselines(
            &mut checkpoint,
            &self.manual_partition_baselines,
            &publication.owned_partitions,
        );
        attach_rotation_baselines(
            &mut checkpoint,
            &publication.baselines,
            &publication.owned_partitions,
        );
        checkpoint.set_input_channels(Arc::clone(&publication.input_channels))?;
        let assignment_version =
            NonZeroU64::new(publication.assignment_version).ok_or_else(|| {
                ConnectorError::InvalidState {
                    expected: "a positive vnode assignment version".into(),
                    actual: publication.assignment_version.to_string(),
                }
            })?;
        checkpoint.bind_assignment_version(assignment_version);
        Ok(checkpoint)
    }

    pub(super) fn capture_vnode_checkpoint_delta(
        &self,
        publication: &KafkaAssignmentPublication,
    ) -> Result<SourceCheckpointDelta, ConnectorError> {
        let assignment_version =
            NonZeroU64::new(publication.assignment_version).ok_or_else(|| {
                ConnectorError::InvalidState {
                    expected: "a positive vnode assignment version".into(),
                    actual: publication.assignment_version.to_string(),
                }
            })?;
        let mut touched = rustc_hash::FxHashMap::<(&str, i32), i64>::default();
        touched.reserve(self.poll_staged_offsets.len());
        for (topic, partition, offset) in &self.poll_staged_offsets {
            touched
                .entry((topic.as_ref(), *partition))
                .and_modify(|current| *current = (*current).max(*offset))
                .or_insert(*offset);
        }
        let mut changes = std::collections::HashMap::with_capacity(touched.len());
        for ((topic, partition), accepted_offset) in touched {
            let offset = self.offsets.get(topic, partition).ok_or_else(|| {
                ConnectorError::Internal(format!(
                    "accepted Kafka partition '{topic}-{partition}' has no tracked offset"
                ))
            })?;
            changes.insert(format!("{topic}:{partition}"), Some(offset.to_string()));

            let previous_rotation =
                rotation_partition_baseline(&publication.baselines, topic, partition);
            let current_rotation = previous_rotation.filter(|next| accepted_offset < *next);
            if previous_rotation != current_rotation {
                let manual = self
                    .manual_partition_baselines
                    .get(&(topic.to_owned(), partition))
                    .copied();
                let previous_baseline = previous_rotation.or(manual);
                let current_baseline = current_rotation.or(manual);
                if previous_baseline != current_baseline {
                    changes.insert(
                        partition_baseline_key(topic, partition),
                        current_baseline.map(|next| next.to_string()),
                    );
                }
            }
        }
        SourceCheckpointDelta::new(
            assignment_version,
            Arc::clone(&publication.input_channels),
            changes,
        )
    }

    pub(super) fn capture_non_vnode_checkpoint(&self) -> Result<SourceCheckpoint, ConnectorError> {
        if !self.manual_topic_partitions.is_empty() {
            let mut checkpoint = self.offsets.to_checkpoint_for_partitions(
                self.manual_topic_partitions
                    .iter()
                    .map(|(topic, partition)| (topic.as_str(), *partition)),
            );
            attach_partition_baselines(
                &mut checkpoint,
                &self.manual_partition_baselines,
                &self.manual_topic_partitions,
            );
            checkpoint.set_input_channels(Arc::clone(&self.manual_input_channels))?;
            return Ok(checkpoint);
        }
        let assigned = lock_or_recover(&self.rebalance_state).assignment_snapshot();
        let mut checkpoint = self.offsets.to_checkpoint_for_partitions(
            assigned
                .iter()
                .map(|(topic, partition)| (topic.as_str(), *partition)),
        );
        checkpoint.set_input_channels(kafka_input_channels(
            self.source_name.as_ref(),
            assigned.as_ref(),
        )?)?;
        Ok(checkpoint)
    }

    pub(super) fn try_capture_checkpoint(
        &self,
    ) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        self.check_reader_health("capturing a checkpoint cursor")?;
        let Some((registry, _)) = &self.vnode_assignment else {
            return self.capture_non_vnode_checkpoint().map(Some);
        };
        // Cursor serialization runs outside the registry and publication locks. A final fence
        // check discards the candidate if ownership rotates while offsets are being encoded.
        try_capture_at_assignment_fence(
            registry,
            &self.reconciled_assignment_version,
            &self.assignment_publication,
            |publication| self.capture_vnode_checkpoint(publication),
        )
    }
}

/// Build an offset-less `TopicPartitionList` from `(topic, partition)` refs, for
/// `pause`/`resume` calls.
pub(super) fn tpl_of<'a>(parts: impl Iterator<Item = &'a (Arc<str>, i32)>) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in parts {
        let _ = tpl.add_partition(topic.as_ref(), *partition);
    }
    tpl
}

/// Partition list for the initial `start()` assignment of a vnode-assigned source.
/// Owned partitions start at their checkpointed offset + 1, otherwise at
/// `default_offset`. Rotations rebind incrementally in the reader loop.
pub(super) fn build_vnode_assignment_tpl(
    source_identity: &str,
    assignment: &[laminar_core::state::NodeId],
    self_id: laminar_core::state::NodeId,
    topic_meta: &[(Arc<str>, i32)],
    offsets: &OffsetTracker,
    baselines: &KafkaPartitionBaselines,
    default_offset: rdkafka::Offset,
) -> Result<TopicPartitionList, ConnectorError> {
    let mut tpl = TopicPartitionList::new();
    for (topic, count) in topic_meta {
        for partition in super::super::vnode_routing::owned_partitions_in_assignment(
            source_identity,
            topic.as_ref(),
            *count,
            assignment,
            self_id,
        )? {
            let offset = match offsets.get(topic.as_ref(), partition) {
                Some(offset) => {
                    rdkafka::Offset::Offset(offset.checked_add(1).ok_or_else(|| {
                        ConnectorError::ConfigurationError(format!(
                            "Kafka checkpoint offset overflow for '{topic}-{partition}'"
                        ))
                    })?)
                }
                None => baselines
                    .get(&(topic.to_string(), partition))
                    .map_or(default_offset, |next| rdkafka::Offset::Offset(*next)),
            };
            tpl.add_partition_offset(topic.as_ref(), partition, offset)
                .map_err(|error| {
                    ConnectorError::Internal(format!(
                        "failed to add vnode-owned Kafka partition '{topic}-{partition}' to assignment: {error}"
                    ))
                })?;
        }
    }
    Ok(tpl)
}

/// Resolves the numeric next-to-read position for an acquired vnode partition from this
/// process's durable checkpoint state.
pub(super) fn acquired_numeric_position(
    local: &OffsetTracker,
    local_baselines: &KafkaPartitionBaselines,
    topic: &str,
    partition: i32,
) -> Result<Option<i64>, ConnectorError> {
    if let Some(offset) = local.get(topic, partition) {
        return offset.checked_add(1).map(Some).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "Kafka local offset overflow for '{topic}-{partition}'"
            ))
        });
    }
    if let Some(next) = local_baselines.get(&(topic.to_string(), partition)) {
        return Ok(Some(*next));
    }
    Ok(None)
}

/// Builds the seek applied after a group assignment. With a deterministic
/// fallback this includes *every* assigned partition; otherwise it contains
/// only checkpointed partitions and leaves Kafka's configured group behavior
/// intact (best-effort mode).
pub(super) fn assignment_seek_tpl(
    offsets: &OffsetTracker,
    assigned: &[(String, i32)],
    baselines: Option<&KafkaPartitionBaselines>,
    deterministic_fallback: Option<rdkafka::Offset>,
    require_all: bool,
) -> Result<TopicPartitionList, ConnectorError> {
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in assigned {
        let position = match offsets.get(topic, *partition) {
            Some(offset) => rdkafka::Offset::Offset(offset.checked_add(1).ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "Kafka checkpoint offset overflow for '{topic}-{partition}'"
                ))
            })?),
            None => {
                if let Some(next) =
                    baselines.and_then(|positions| positions.get(&(topic.clone(), *partition)))
                {
                    rdkafka::Offset::Offset(*next)
                } else if let Some(offset) = deterministic_fallback {
                    offset
                } else if require_all {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "Kafka partition '{topic}-{partition}' has no durable next-to-read baseline"
                    )));
                } else {
                    continue;
                }
            }
        };
        tpl.add_partition_offset(topic, *partition, position)
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "failed to build Kafka assignment seek for '{topic}-{partition}': {e}"
                ))
            })?;
    }
    Ok(tpl)
}

/// rdkafka start position for a partition that has no checkpointed offset under
/// engine-controlled assignment, derived from the configured startup mode.
pub(super) fn startup_default_offset(mode: &StartupMode) -> rdkafka::Offset {
    match mode {
        StartupMode::Earliest => rdkafka::Offset::Beginning,
        StartupMode::Latest => rdkafka::Offset::End,
        // GroupOffsets resumes from committed offsets (falling back to
        // `auto.offset.reset`); Specific/Timestamp aren't combined with vnode
        // assignment, so they also defer to the stored position.
        _ => rdkafka::Offset::Stored,
    }
}

/// Deterministic position for a partition absent from engine state. Group
/// commits are deliberately excluded because they can belong to an abandoned
/// engine timeline. Specific/timestamp starts are assigned explicitly by
/// `start()` and therefore have no single partition-independent fallback.
pub(super) fn deterministic_initial_offset(
    mode: &StartupMode,
    reset: OffsetReset,
) -> Option<rdkafka::Offset> {
    match (mode, reset) {
        (StartupMode::Latest, _) | (StartupMode::GroupOffsets, OffsetReset::Latest) => {
            Some(rdkafka::Offset::End)
        }
        (StartupMode::GroupOffsets, OffsetReset::None)
        | (StartupMode::SpecificOffsets(_) | StartupMode::Timestamp(_), _) => None,
        (StartupMode::GroupOffsets, OffsetReset::Earliest) | (StartupMode::Earliest, _) => {
            Some(rdkafka::Offset::Beginning)
        }
    }
}

pub(super) fn validate_resume_input_channels(
    source_name: &str,
    checkpoint: Option<&[Vec<u8>]>,
    current: &KafkaPartitionSet,
) -> Result<(), ConnectorError> {
    let checkpoint = checkpoint.ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "Kafka engine-owned resume checkpoint has no input-channel inventory".into(),
        )
    })?;
    let current = kafka_input_channels(source_name, current)?;
    if checkpoint != current.as_ref() {
        let first_difference = checkpoint
            .iter()
            .zip(current.iter())
            .position(|(saved, discovered)| saved != discovered)
            .unwrap_or_else(|| checkpoint.len().min(current.len()));
        return Err(ConnectorError::ConfigurationError(format!(
            "Kafka input-channel inventory changed across recovery: checkpoint has {} channels, current assignment has {}; first difference at index {first_difference}",
            checkpoint.len(),
            current.len()
        )));
    }
    Ok(())
}

pub(super) fn partition_baseline_key(topic: &str, partition: i32) -> String {
    format!("{KAFKA_PARTITION_BASELINE_PREFIX}{topic}:{partition}")
}

pub(super) fn decode_partition_baselines_from_offsets(
    offsets: &std::collections::HashMap<String, String>,
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    let mut baselines = KafkaPartitionBaselines::new();
    for (key, value) in offsets {
        let Some(encoded) = key.strip_prefix(KAFKA_PARTITION_BASELINE_PREFIX) else {
            continue;
        };
        let (topic, partition_text) = encoded.rsplit_once(':').ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "invalid Kafka partition baseline key '{key}'"
            ))
        })?;
        let partition = partition_text.parse::<i32>().map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "invalid Kafka partition baseline key '{key}'"
            ))
        })?;
        let next = value.parse::<i64>().map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "invalid Kafka next-to-read baseline for '{topic}-{partition_text}': '{value}'"
            ))
        })?;
        if topic.is_empty()
            || topic.contains(':')
            || partition < 0
            || partition.to_string() != partition_text
            || next < 0
            || next == i64::MAX
            || next.to_string() != value.as_str()
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "invalid Kafka partition baseline '{key}' = '{value}'"
            )));
        }
        if baselines
            .insert((topic.to_string(), partition), next)
            .is_some()
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "duplicate Kafka partition baseline for '{topic}-{partition}'"
            )));
        }
    }
    Ok(baselines)
}

pub(super) fn decode_partition_baselines(
    checkpoint: &SourceCheckpoint,
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    decode_partition_baselines_from_offsets(checkpoint.offsets())
}

pub(super) fn attach_partition_baselines(
    checkpoint: &mut SourceCheckpoint,
    baselines: &KafkaPartitionBaselines,
    included: &KafkaPartitionSet,
) {
    for ((topic, partition), next) in baselines {
        if included.contains(&(topic.clone(), *partition)) {
            checkpoint.set_offset(partition_baseline_key(topic, *partition), next.to_string());
        }
    }
}

pub(super) fn rotation_partition_baseline(
    baselines: &KafkaRotationBaselines,
    topic: &str,
    partition: i32,
) -> Option<i64> {
    baselines
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .copied()
}

pub(super) fn rotation_baselines_len(baselines: &KafkaRotationBaselines) -> usize {
    baselines.values().map(std::collections::HashMap::len).sum()
}

pub(super) fn update_rotation_baselines(
    current: &KafkaRotationBaselines,
    owned: &KafkaPartitionSet,
    acquired: &KafkaPartitionBaselines,
) -> KafkaRotationBaselines {
    let mut updated = KafkaRotationBaselines::new();
    for (topic, partition) in owned {
        if let Some(next) = rotation_partition_baseline(current, topic, *partition) {
            updated
                .entry(Arc::from(topic.as_str()))
                .or_default()
                .insert(*partition, next);
        }
    }
    for ((topic, partition), next) in acquired {
        updated
            .entry(Arc::from(topic.as_str()))
            .or_default()
            .insert(*partition, *next);
    }
    updated
}

pub(super) fn attach_rotation_baselines(
    checkpoint: &mut SourceCheckpoint,
    baselines: &KafkaRotationBaselines,
    included: &KafkaPartitionSet,
) {
    for (topic, partition) in included {
        if let Some(next) = rotation_partition_baseline(baselines, topic, *partition) {
            checkpoint.set_offset(partition_baseline_key(topic, *partition), next.to_string());
        }
    }
}

pub(super) fn vnode_payload_is_current(
    ownership: Option<(&[laminar_core::state::NodeId], laminar_core::state::NodeId)>,
    partition_vnode: Option<u32>,
    required_next: Option<i64>,
    offset: i64,
) -> Result<bool, ConnectorError> {
    let owned = if let Some((assignment, self_id)) = ownership {
        let vnode = partition_vnode.ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "Kafka payload has no canonical source/topic/partition vnode route".into(),
            )
        })?;
        let vnode_index = usize::try_from(vnode).map_err(|_| {
            ConnectorError::ConfigurationError(
                "Kafka vnode id cannot be represented on this platform".into(),
            )
        })?;
        let owner = assignment.get(vnode_index).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "Kafka cached vnode {vnode} is outside owner map cardinality {}",
                assignment.len()
            ))
        })?;
        *owner == self_id
    } else {
        true
    };
    Ok(owned && required_next.is_none_or(|next| offset >= next))
}

pub(super) fn retire_accepted_rotation_baselines(
    baselines: &mut KafkaRotationBaselines,
    accepted_offsets: &[(Arc<str>, i32, i64)],
) {
    for (topic, partition, offset) in accepted_offsets {
        let remove_topic = baselines.get_mut(topic.as_ref()).is_some_and(|partitions| {
            if partitions.get(partition).is_some_and(|next| offset >= next) {
                partitions.remove(partition);
            }
            partitions.is_empty()
        });
        if remove_topic {
            baselines.remove(topic.as_ref());
        }
    }
}

pub(super) fn validate_partition_baselines(
    baselines: &KafkaPartitionBaselines,
    inventory: &KafkaPartitionSet,
) -> Result<(), ConnectorError> {
    let baseline_inventory: KafkaPartitionSet = baselines.keys().cloned().collect();
    if baseline_inventory != *inventory {
        return Err(ConnectorError::ConfigurationError(format!(
            "Kafka guaranteed recovery baseline inventory does not match its partition cut: baselines={baseline_inventory:?}, partitions={inventory:?}"
        )));
    }
    Ok(())
}

pub(super) fn validate_positions_not_expired(
    offsets: &OffsetTracker,
    baselines: &KafkaPartitionBaselines,
    low_watermarks: &KafkaPartitionBaselines,
    inventory: &KafkaPartitionSet,
) -> Result<(), ConnectorError> {
    for (topic, partition) in inventory {
        let desired = match offsets.get(topic, *partition) {
            Some(last) => last.checked_add(1).ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "Kafka checkpoint offset overflow for '{topic}-{partition}'"
                ))
            })?,
            None => *baselines.get(&(topic.clone(), *partition)).ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "Kafka partition '{topic}-{partition}' has no durable next-to-read baseline"
                ))
            })?,
        };
        let low = *low_watermarks
            .get(&(topic.clone(), *partition))
            .ok_or_else(|| {
                ConnectorError::ConnectionFailed(format!(
                    "Kafka watermark response omitted partition '{topic}-{partition}'"
                ))
            })?;
        if desired < low {
            return Err(ConnectorError::ConfigurationError(format!(
                "Kafka retention advanced partition '{topic}-{partition}' to {low} past the durable next-to-read position {desired}"
            )));
        }
    }
    Ok(())
}

pub(super) fn kafka_reader_error_is_transient(error: &KafkaError) -> bool {
    use rdkafka::types::RDKafkaErrorCode;

    let code = match error {
        KafkaError::PartitionEOF(_) | KafkaError::NoMessageReceived => return true,
        KafkaError::MessageConsumption(code) | KafkaError::Global(code) => *code,
        _ => return false,
    };

    matches!(
        code,
        RDKafkaErrorCode::BrokerDestroy
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::Resolve
            | RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::QueueFull
            | RDKafkaErrorCode::NodeUpdate
            | RDKafkaErrorCode::WaitingForCoordinator
            | RDKafkaErrorCode::UnknownGroup
            | RDKafkaErrorCode::InProgress
            | RDKafkaErrorCode::PreviousInProgress
            | RDKafkaErrorCode::TimedOutQueue
            | RDKafkaErrorCode::WaitCache
            | RDKafkaErrorCode::Interrupted
            | RDKafkaErrorCode::Partial
            | RDKafkaErrorCode::Retry
            | RDKafkaErrorCode::PollExceeded
            | RDKafkaErrorCode::UnknownBroker
            | RDKafkaErrorCode::AssignmentLost
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::UnknownTopicOrPartition
            | RDKafkaErrorCode::LeaderNotAvailable
            | RDKafkaErrorCode::NotLeaderForPartition
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::BrokerNotAvailable
            | RDKafkaErrorCode::ReplicaNotAvailable
            | RDKafkaErrorCode::NetworkException
            | RDKafkaErrorCode::CoordinatorLoadInProgress
            | RDKafkaErrorCode::CoordinatorNotAvailable
            | RDKafkaErrorCode::NotCoordinator
            | RDKafkaErrorCode::IllegalGeneration
            | RDKafkaErrorCode::UnknownMemberId
            | RDKafkaErrorCode::RebalanceInProgress
            | RDKafkaErrorCode::NotController
            | RDKafkaErrorCode::KafkaStorageError
            | RDKafkaErrorCode::ReassignmentInProgress
            | RDKafkaErrorCode::FetchSessionIdNotFound
            | RDKafkaErrorCode::InvalidFetchSessionEpoch
            | RDKafkaErrorCode::FencedLeaderEpoch
            | RDKafkaErrorCode::UnknownLeaderEpoch
            | RDKafkaErrorCode::StaleBrokerEpoch
            | RDKafkaErrorCode::OffsetNotAvailable
            | RDKafkaErrorCode::MemberIdRequired
            | RDKafkaErrorCode::PreferredLeaderNotAvailable
            | RDKafkaErrorCode::EligibleLeadersNotAvailable
            | RDKafkaErrorCode::UnstableOffsetCommit
            | RDKafkaErrorCode::ThrottlingQuotaExceeded
            | RDKafkaErrorCode::UnknownTopicId
    )
}

pub(super) fn consumer_creation_error(error: &KafkaError) -> ConnectorError {
    ConnectorError::ConfigurationError(format!(
        "failed to create Kafka consumer from local configuration: {error}"
    ))
}

pub(super) fn encode_kafka_input_channel(
    output: &mut Vec<u8>,
    source_name: &str,
    topic: &str,
    partition: i32,
) -> Result<(), ConnectorError> {
    if source_name.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "Kafka input channels require a canonical Laminar source name".into(),
        ));
    }
    let source_len = u32::try_from(source_name.len()).map_err(|_| {
        ConnectorError::Internal(
            "Kafka source name exceeds the input-channel encoding limit".into(),
        )
    })?;
    let topic_len = u32::try_from(topic.len()).map_err(|_| {
        ConnectorError::Internal("Kafka topic exceeds the input-channel encoding limit".into())
    })?;
    if partition < 0 {
        return Err(ConnectorError::Internal(format!(
            "Kafka input channel has invalid partition '{topic}-{partition}'"
        )));
    }
    output.clear();
    output.extend_from_slice(&source_len.to_be_bytes());
    output.extend_from_slice(source_name.as_bytes());
    output.extend_from_slice(&topic_len.to_be_bytes());
    output.extend_from_slice(topic.as_bytes());
    output.extend_from_slice(&partition.to_be_bytes());
    Ok(())
}

pub(super) fn kafka_input_channels(
    source_name: &str,
    inventory: &KafkaPartitionSet,
) -> Result<Arc<[Vec<u8>]>, ConnectorError> {
    let mut channels = Vec::with_capacity(inventory.len());
    let mut encoded = Vec::new();
    for (topic, partition) in inventory {
        encode_kafka_input_channel(&mut encoded, source_name, topic, *partition)?;
        channels.push(encoded.clone());
    }
    channels.sort_unstable();
    Ok(channels.into())
}

pub(super) fn kafka_row_positions(
    source_name: &str,
    positions: &[(Arc<str>, i32, i64)],
    good_indices: Option<&[usize]>,
) -> Result<SourceRowPositions, ConnectorError> {
    let row_count = good_indices.map_or(positions.len(), <[usize]>::len);
    let partition_bytes = match good_indices {
        Some(indices) => indices.iter().try_fold(0_usize, |total, &index| {
            let (topic, _, _) = positions.get(index).ok_or_else(|| {
                ConnectorError::Internal(
                    "Kafka decoded-row index is outside the staged position batch".into(),
                )
            })?;
            Ok::<_, ConnectorError>(total.saturating_add(source_name.len() + topic.len() + 12))
        })?,
        None => positions.iter().fold(0_usize, |total, (topic, _, _)| {
            total.saturating_add(source_name.len() + topic.len() + 12)
        }),
    };
    let mut partitions = BinaryBuilder::with_capacity(row_count, partition_bytes);
    let mut order_keys = BinaryBuilder::with_capacity(row_count, row_count.saturating_mul(8));
    let mut encoded_partition = Vec::new();
    let mut append = |(topic, partition, offset): &(Arc<str>, i32, i64)| {
        if *offset < 0 {
            return Err(ConnectorError::Internal(format!(
                "Kafka emitted invalid row position '{}-{partition}@{offset}'",
                topic.as_ref()
            )));
        }
        encode_kafka_input_channel(
            &mut encoded_partition,
            source_name,
            topic.as_ref(),
            *partition,
        )?;
        partitions.append_value(&encoded_partition);

        let mut ordered_offset = offset.to_be_bytes();
        ordered_offset[0] ^= 0x80;
        order_keys.append_value(ordered_offset);
        Ok::<_, ConnectorError>(())
    };

    match good_indices {
        Some(indices) => {
            for &index in indices {
                append(positions.get(index).ok_or_else(|| {
                    ConnectorError::Internal(
                        "Kafka decoded-row index is outside the staged position batch".into(),
                    )
                })?)?;
            }
        }
        None => {
            for position in positions {
                append(position)?;
            }
        }
    }

    SourceRowPositions::try_new(
        partitions.finish(),
        order_keys.finish(),
        UInt32Array::from(vec![0; row_count]),
    )
}

const KAFKA_METADATA_COLUMNS: [&str; 3] = ["_partition", "_offset", "_timestamp"];
const KAFKA_HEADERS_COLUMN: &str = "_headers";

pub(super) fn validate_kafka_output_schema(
    payload_schema: &SchemaRef,
    include_metadata: bool,
    include_headers: bool,
) -> Result<(), ConnectorError> {
    let collision = payload_schema.fields().iter().find(|field| {
        (include_metadata
            && KAFKA_METADATA_COLUMNS
                .iter()
                .any(|name| field.name().eq_ignore_ascii_case(name)))
            || (include_headers && field.name().eq_ignore_ascii_case(KAFKA_HEADERS_COLUMN))
    });
    if let Some(field) = collision {
        return Err(ConnectorError::SchemaMismatch(format!(
            "Kafka payload schema contains configured connector metadata column '{}'",
            field.name()
        )));
    }
    Ok(())
}

pub(super) fn kafka_output_schema(
    payload_schema: &SchemaRef,
    include_metadata: bool,
    include_headers: bool,
) -> SchemaRef {
    if !include_metadata && !include_headers {
        return Arc::clone(payload_schema);
    }
    let mut fields = payload_schema.fields().to_vec();
    if include_metadata {
        fields.extend([
            Arc::new(Field::new("_partition", DataType::Int32, false)),
            Arc::new(Field::new("_offset", DataType::Int64, false)),
            Arc::new(Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        ]);
    }
    if include_headers {
        fields.push(Arc::new(Field::new(
            KAFKA_HEADERS_COLUMN,
            DataType::Utf8,
            true,
        )));
    }
    Arc::new(Schema::new_with_metadata(
        fields,
        payload_schema.metadata().clone(),
    ))
}

pub(super) type NormalizedDebeziumBatch = (RecordBatch, Option<Box<[SourceMutation]>>);
