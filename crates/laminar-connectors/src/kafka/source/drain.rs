//! Drain fencing, blocking-task ownership, and bounded cleanup.

use super::{
    warn, Arc, AtomicU64, AtomicU8, ConnectorError, ConnectorState, Consumer, DeliveryGuarantee,
    KafkaBlockingTasks, KafkaSource, KafkaSourceConfig, KafkaSourceMetrics, LaminarConsumerContext,
    Mutex, Notify, Ordering, SourceDrainOutcome, SourceDrainRequest, SourceDrainResolution,
    StreamConsumer, TopicPartitionList,
};

/// Locks a mutex, recovering from poison if a prior holder panicked.
///
/// Used for state shared with rdkafka's rebalance callback thread.
/// Poison indicates a panic in the callback — the data may be stale
/// but is structurally sound, so we recover rather than propagate.
pub(super) fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        tracing::warn!("mutex poisoned, recovering");
        poisoned.into_inner()
    })
}

pub(super) fn publish_reader_fault(
    fault: &Mutex<Option<Arc<str>>>,
    data_ready: &Notify,
    reason: impl Into<Arc<str>>,
) {
    let mut published = lock_or_recover(fault);
    if published.is_none() {
        *published = Some(reason.into());
    }
    drop(published);
    data_ready.notify_one();
}

/// A guaranteed-delivery poll cannot retry in place after removing input from the reader queue.
/// The Kafka consumer has already moved past that input, so only a fresh generation seeking the
/// durable checkpoint can retry it safely. Preserve terminal errors, but turn transient errors into
/// terminal-for-this-generation failures so the source actor cannot consume later records.
pub(super) fn terminalize_guaranteed_poll_error(
    delivery: DeliveryGuarantee,
    state: &mut ConnectorState,
    metrics: &KafkaSourceMetrics,
    reader_shutdown: Option<&tokio::sync::watch::Sender<bool>>,
    error: ConnectorError,
) -> ConnectorError {
    if delivery == DeliveryGuarantee::BestEffort {
        return error;
    }

    metrics.record_error();
    *state = ConnectorState::Failed;
    if let Some(shutdown) = reader_shutdown {
        let _ = shutdown.send(true);
    }
    warn!(delivery = %delivery, %error, "Kafka source generation stopped after a post-drain failure");

    if error.is_transient() {
        ConnectorError::Internal(format!(
            "Kafka guaranteed-delivery poll failed after draining input; recovery from the durable cursor is required: {error}"
        ))
    } else {
        error
    }
}

/// Payload sent from the background Kafka reader task to [`KafkaSource::poll_batch`].
pub(super) struct KafkaPayload {
    pub(super) data: Vec<u8>,
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
    /// Precomputed route for cluster-owned inputs; absent for local readers.
    pub(super) partition_vnode: Option<u32>,
    pub(super) offset: i64,
    pub(super) timestamp_ms: Option<i64>,
    /// Message headers as a JSON string; populated only when `include_headers` is set.
    pub(super) headers_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KafkaDrainPartition {
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
}

#[derive(Debug, Clone)]
pub(super) struct KafkaDrainBoundary {
    pub(super) round: laminar_core::checkpoint::AssignmentDrainId,
    pub(super) inputs: Arc<[KafkaDrainPartition]>,
}

pub(super) enum KafkaReaderItem {
    Payload(KafkaPayload),
    DrainBoundary(KafkaDrainBoundary),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KafkaDrainPosition {
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
    pub(super) next_offset: i64,
}

pub(super) enum KafkaReaderDrainCommand {
    Begin {
        request: SourceDrainRequest,
        deadline: tokio::time::Instant,
    },
    Resolve {
        resolution: SourceDrainResolution,
        cut: Arc<[KafkaDrainPosition]>,
        deadline: tokio::time::Instant,
        execution: Arc<AtomicU8>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

pub(super) struct KafkaReaderDrain {
    pub(super) request: SourceDrainRequest,
    pub(super) prepare_deadline: tokio::time::Instant,
    pub(super) inputs: Arc<[KafkaDrainPartition]>,
    pub(super) held_inputs: Arc<[KafkaDrainPartition]>,
    pub(super) held_assignment_version: Option<u64>,
    pub(super) hold_complete: bool,
    pub(super) boundary_queued: bool,
}

pub(super) struct KafkaPendingDrainResolution {
    pub(super) resolution: SourceDrainResolution,
    pub(super) deadline: tokio::time::Instant,
    pub(super) execution: Arc<AtomicU8>,
    pub(super) reply: tokio::sync::oneshot::Receiver<Result<(), String>>,
    pub(super) terminal_error: Option<Arc<str>>,
}

pub(super) struct KafkaSourceDrain {
    pub(super) request: SourceDrainRequest,
    pub(super) prepare_deadline: tokio::time::Instant,
    pub(super) boundary: Option<KafkaDrainBoundary>,
    pub(super) cut: Option<Arc<[KafkaDrainPosition]>>,
    pub(super) pending_resolution: Option<KafkaPendingDrainResolution>,
}

pub(super) const KAFKA_BACKGROUND_CLOSE_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(500);
pub(super) const KAFKA_DRAIN_EXECUTION_PENDING: u8 = 0;
pub(super) const KAFKA_DRAIN_EXECUTION_STARTED: u8 = 1;
pub(super) const KAFKA_DRAIN_EXECUTION_CANCELLED: u8 = 2;
pub(super) const KAFKA_POSITION_LOOKUP_BUDGET: std::time::Duration =
    std::time::Duration::from_secs(10);
pub(super) const KAFKA_POSITION_LOOKUP_CONCURRENCY: usize = 32;

pub(super) struct KafkaDrainWaitGuard {
    execution: Arc<AtomicU8>,
    armed: bool,
}

impl KafkaDrainWaitGuard {
    pub(super) fn new(execution: Arc<AtomicU8>) -> Self {
        Self {
            execution,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for KafkaDrainWaitGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.execution.compare_exchange(
                KAFKA_DRAIN_EXECUTION_PENDING,
                KAFKA_DRAIN_EXECUTION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

pub(super) fn claim_kafka_drain_execution(
    execution: &AtomicU8,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    if tokio::time::Instant::now() >= deadline {
        return Err("Kafka drain deadline expired before provider execution".into());
    }
    execution
        .compare_exchange(
            KAFKA_DRAIN_EXECUTION_PENDING,
            KAFKA_DRAIN_EXECUTION_STARTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map(|_| ())
        .map_err(|state| match state {
            KAFKA_DRAIN_EXECUTION_CANCELLED => {
                "Kafka drain resolution was cancelled before execution".into()
            }
            _ => "Kafka drain resolution execution was already claimed".into(),
        })
}

pub(super) type KafkaPartitionSet = std::collections::HashSet<(String, i32)>;
pub(super) type KafkaPartitionBaselines = std::collections::HashMap<(String, i32), i64>;

pub(super) struct KafkaStartPlan {
    pub(super) config: KafkaSourceConfig,
    pub(super) delivery: DeliveryGuarantee,
    pub(super) is_resume: bool,
    pub(super) resume_input_channels: Option<Vec<Vec<u8>>>,
    pub(super) resume_baselines: KafkaPartitionBaselines,
}
pub(super) type KafkaPartitionRoutes = std::collections::HashMap<Arc<str>, Arc<[u32]>>;
pub(super) type KafkaRotationBaselines =
    std::collections::HashMap<Arc<str>, std::collections::HashMap<i32, i64>>;

pub(super) fn kafka_drain_partitions(
    assignment: &TopicPartitionList,
) -> Result<Arc<[KafkaDrainPartition]>, ConnectorError> {
    let mut inputs = Vec::with_capacity(assignment.count());
    for element in assignment.elements() {
        if element.topic().is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "Kafka assignment contains an empty topic".into(),
            ));
        }
        if element.partition() < 0 {
            return Err(ConnectorError::ConfigurationError(format!(
                "Kafka assignment contains invalid partition '{}-{}'",
                element.topic(),
                element.partition()
            )));
        }
        inputs.push(KafkaDrainPartition {
            topic: Arc::from(element.topic()),
            partition: element.partition(),
        });
    }
    inputs.sort_unstable_by(|left, right| {
        left.topic
            .as_ref()
            .cmp(right.topic.as_ref())
            .then(left.partition.cmp(&right.partition))
    });
    if let Some(duplicate) = inputs.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(ConnectorError::ConfigurationError(format!(
            "Kafka assignment contains duplicate input '{}-{}'",
            duplicate[0].topic, duplicate[0].partition
        )));
    }
    Ok(inputs.into())
}

pub(super) fn kafka_partition_routes(
    source_identity: &str,
    vnode_count: u32,
    topic_meta: &[(Arc<str>, i32)],
) -> Result<KafkaPartitionRoutes, ConnectorError> {
    let mut routes = KafkaPartitionRoutes::with_capacity(topic_meta.len());
    for (topic, count) in topic_meta {
        if *count < 0 {
            return Err(ConnectorError::ConfigurationError(format!(
                "Kafka topic '{topic}' reported a negative partition count {count}"
            )));
        }
        let topic_routes = super::super::vnode_routing::partition_vnodes(
            source_identity,
            topic,
            *count,
            vnode_count,
        )?;
        if routes
            .insert(Arc::clone(topic), topic_routes.into())
            .is_some()
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "Kafka vnode assignment contains duplicate topic '{topic}'"
            )));
        }
    }
    Ok(routes)
}

pub(super) fn kafka_partition_set(tpl: &TopicPartitionList) -> Result<KafkaPartitionSet, String> {
    let mut partitions = KafkaPartitionSet::with_capacity(tpl.count());
    for element in tpl.elements() {
        if element.partition() < 0 {
            return Err(format!(
                "Kafka assignment contains negative partition '{}-{}'",
                element.topic(),
                element.partition()
            ));
        }
        let partition = (element.topic().to_string(), element.partition());
        if !partitions.insert(partition) {
            return Err(format!(
                "Kafka assignment contains duplicate partition '{}-{}'",
                element.topic(),
                element.partition()
            ));
        }
    }
    Ok(partitions)
}

pub(super) fn validate_kafka_assignment(
    expected: &KafkaPartitionSet,
    actual: &KafkaPartitionSet,
) -> Result<(), String> {
    if expected == actual {
        return Ok(());
    }
    let missing = expected
        .difference(actual)
        .min()
        .map(|(topic, partition)| format!("{topic}-{partition}"));
    let unexpected = actual
        .difference(expected)
        .min()
        .map(|(topic, partition)| format!("{topic}-{partition}"));
    Err(format!(
        "Kafka consumer assignment mismatch: expected {} partitions, found {}; first missing: {}, first unexpected: {}",
        expected.len(),
        actual.len(),
        missing.as_deref().unwrap_or("none"),
        unexpected.as_deref().unwrap_or("none")
    ))
}

pub(super) fn kafka_bootstrap_is_unassigned(
    published: &laminar_core::state::VnodeAssignmentSnapshot,
    self_id: laminar_core::state::NodeId,
) -> Result<bool, ConnectorError> {
    if self_id.is_unassigned() {
        return Err(ConnectorError::ConfigurationError(
            "Kafka vnode ownership requires a nonzero node identity".into(),
        ));
    }
    if published.owners().is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "Kafka vnode assignment cannot use an empty owner map".into(),
        ));
    }
    if published.version() != 0 {
        super::super::vnode_routing::validate_owner_map(published.owners(), self_id)?;
        return Ok(false);
    }
    if !published
        .owners()
        .iter()
        .all(laminar_core::state::NodeId::is_unassigned)
    {
        return Err(ConnectorError::ConfigurationError(
            "Kafka vnode assignment version 0 must be the fully unassigned bootstrap publication"
                .into(),
        ));
    }
    Ok(true)
}

pub(super) fn kafka_owned_partition_sets(
    routes: &KafkaPartitionRoutes,
    published: &laminar_core::state::VnodeAssignmentSnapshot,
    self_id: laminar_core::state::NodeId,
    reconciled_version: u64,
) -> Result<(KafkaPartitionSet, KafkaPartitionSet), ConnectorError> {
    super::super::vnode_routing::validate_owner_map(published.owners(), self_id)?;
    let mut owned = KafkaPartitionSet::new();
    let mut reacquired = KafkaPartitionSet::new();
    for (topic, topic_routes) in routes {
        for (partition_index, vnode) in topic_routes.iter().copied().enumerate() {
            let vnode_index = usize::try_from(vnode).map_err(|_| {
                ConnectorError::ConfigurationError(
                    "Kafka vnode id cannot be represented on this platform".into(),
                )
            })?;
            let assigned_owner = published.owners().get(vnode_index).ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "Kafka cached vnode {vnode} is outside owner map cardinality {}",
                    published.owners().len()
                ))
            })?;
            if *assigned_owner != self_id {
                continue;
            }
            let partition = i32::try_from(partition_index).map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "Kafka topic '{topic}' partition index cannot be represented as i32"
                ))
            })?;
            let input = (topic.to_string(), partition);
            if reconciled_version != 0
                && published
                    .owner_changed_version(vnode)
                    .is_some_and(|changed| changed > reconciled_version)
            {
                reacquired.insert(input.clone());
            }
            owned.insert(input);
        }
    }
    Ok((owned, reacquired))
}

pub(super) fn kafka_assignment_fence_matches(
    registry: &laminar_core::state::VnodeRegistry,
    reconciled: &AtomicU64,
    assignment_version: u64,
) -> bool {
    assignment_version != 0
        && registry.assignment_version() == assignment_version
        && reconciled.load(Ordering::Acquire) == assignment_version
}

pub(super) fn try_capture_at_assignment_fence<T>(
    registry: &laminar_core::state::VnodeRegistry,
    reconciled: &AtomicU64,
    assignment_publication: &Mutex<Arc<KafkaAssignmentPublication>>,
    capture: impl FnOnce(&KafkaAssignmentPublication) -> Result<T, ConnectorError>,
) -> Result<Option<T>, ConnectorError> {
    // Keep this lock order: registry publication, then Kafka publication.
    let publication = {
        let published = registry.read_assignment();
        let version = published.version();
        if version == 0 || reconciled.load(Ordering::Acquire) != version {
            return Ok(None);
        }
        let publication = Arc::clone(&lock_or_recover(assignment_publication));
        if publication.assignment_version != version {
            return Ok(None);
        }
        publication
    };
    let captured = capture(&publication)?;
    Ok(
        kafka_assignment_fence_matches(registry, reconciled, publication.assignment_version)
            .then_some(captured),
    )
}

pub(super) fn cached_partition_vnode(
    topic_routes: Option<&[u32]>,
    partition: i32,
) -> Result<Option<u32>, ConnectorError> {
    let Some(topic_routes) = topic_routes else {
        return Ok(None);
    };
    let partition_index = usize::try_from(partition).map_err(|_| {
        ConnectorError::ConfigurationError(format!(
            "Kafka payload has negative partition id {partition}"
        ))
    })?;
    topic_routes
        .get(partition_index)
        .copied()
        .map(Some)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "Kafka payload partition {partition} is outside the activated topic inventory {}",
                topic_routes.len()
            ))
        })
}

pub(super) fn kafka_partition_result_errors(tpl: &TopicPartitionList) -> Vec<String> {
    tpl.elements()
        .iter()
        .filter_map(|element| {
            element
                .error()
                .err()
                .map(|error| format!("{}-{}: {error}", element.topic(), element.partition()))
        })
        .collect()
}

pub(super) fn validate_kafka_partition_results(
    operation: &str,
    tpl: &TopicPartitionList,
) -> Result<(), String> {
    let errors = kafka_partition_result_errors(tpl);
    validate_kafka_partition_error_list(operation, &errors)
}

pub(super) fn validate_kafka_partition_error_list(
    operation: &str,
    errors: &[String],
) -> Result<(), String> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Kafka {operation} failed for partitions: {}",
            errors.join(", ")
        ))
    }
}

pub(super) fn kafka_drain_target_ready(
    target_version: u64,
    registry_version: u64,
    reconciled_version: u64,
) -> Result<bool, String> {
    if registry_version > target_version || reconciled_version > target_version {
        return Err(format!(
            "Kafka drain target {target_version} was superseded (registry {registry_version}, reconciled {reconciled_version})"
        ));
    }
    if reconciled_version > registry_version {
        return Err(format!(
            "Kafka drain reconciliation {reconciled_version} is ahead of registry {registry_version}"
        ));
    }
    Ok(registry_version == target_version && reconciled_version == target_version)
}

pub(super) async fn resolve_kafka_reader_drain(
    consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
    blocking_tasks: &KafkaBlockingTasks,
    vnode_reassign: Option<&(
        Arc<laminar_core::state::VnodeRegistry>,
        laminar_core::state::NodeId,
    )>,
    active: &KafkaReaderDrain,
    resolution: SourceDrainResolution,
    cut: &[KafkaDrainPosition],
    globally_paused: bool,
    deadline: tokio::time::Instant,
    execution: &Arc<AtomicU8>,
) -> Result<(), String> {
    if resolution.round != active.request.round {
        return Err("Kafka drain resolution does not match the active round".into());
    }
    let Some((registry, _)) = vnode_reassign else {
        return Err("Kafka drain resolution has no cluster assignment".into());
    };
    if registry.assignment_version() != resolution.round.target_version {
        return Err(format!(
            "Kafka drain target {} is not installed (current {})",
            resolution.round.target_version,
            registry.assignment_version()
        ));
    }
    let assignment = consumer
        .assignment()
        .map_err(|error| format!("Kafka drain could not inspect target assignment: {error}"))?;
    if resolution.outcome == SourceDrainOutcome::Abort {
        let assigned = kafka_partition_set(&assignment)
            .map_err(|error| format!("Kafka drain target assignment is invalid: {error}"))?;
        let mut seek = TopicPartitionList::new();
        for position in cut
            .iter()
            .filter(|position| assigned.contains(&(position.topic.to_string(), position.partition)))
        {
            seek.add_partition_offset(
                position.topic.as_ref(),
                position.partition,
                rdkafka::Offset::Offset(position.next_offset),
            )
            .map_err(|error| {
                format!(
                    "Kafka drain could not build abort seek for '{}-{}': {error}",
                    position.topic, position.partition
                )
            })?;
        }
        if seek.count() > 0 {
            let seek_consumer = Arc::clone(consumer);
            let seek_execution = Arc::clone(execution);
            let positioned = tokio::time::timeout_at(
                deadline,
                blocking_tasks.run(move || -> Result<_, String> {
                    claim_kafka_drain_execution(&seek_execution, deadline)?;
                    let timeout = deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .min(std::time::Duration::from_secs(5));
                    if timeout.is_zero() {
                        return Err("Kafka drain deadline expired before abort seek".into());
                    }
                    seek_consumer
                        .seek_partitions(seek, timeout)
                        .map_err(|error| format!("Kafka drain abort seek failed: {error}"))
                }),
            )
            .await
            .map_err(|_| "Kafka drain deadline expired during abort seek".to_string())?
            .map_err(|error| format!("Kafka drain abort seek task failed: {error}"))??;
            validate_kafka_partition_results("drain abort seek", &positioned)?;
        } else {
            claim_kafka_drain_execution(execution, deadline)?;
        }
    } else {
        claim_kafka_drain_execution(execution, deadline)?;
    }
    if tokio::time::Instant::now() >= deadline {
        return Err("Kafka drain deadline expired before target resume".into());
    }
    if !globally_paused && assignment.count() > 0 {
        consumer
            .resume(&assignment)
            .map_err(|error| format!("Kafka drain target resume failed: {error}"))?;
        validate_kafka_partition_results("drain target resume", &assignment)?;
    }
    Ok(())
}

#[derive(Clone, Default)]
pub(super) struct KafkaAssignmentPublication {
    pub(super) assignment_version: u64,
    pub(super) owned_partitions: Arc<KafkaPartitionSet>,
    pub(super) input_channels: Arc<[Vec<u8>]>,
    pub(super) baselines: KafkaRotationBaselines,
}

impl KafkaAssignmentPublication {
    pub(super) fn new(
        assignment_version: u64,
        owned_partitions: Arc<KafkaPartitionSet>,
        input_channels: Arc<[Vec<u8>]>,
        baselines: KafkaRotationBaselines,
    ) -> Self {
        Self {
            assignment_version,
            owned_partitions,
            input_channels,
            baselines,
        }
    }
}

/// Single-consumer async receiver for the reader to `poll_batch` queue.
pub(super) type KafkaReaderRx = crossfire::AsyncRx<crossfire::mpsc::Array<KafkaReaderItem>>;

impl KafkaSource {
    pub(super) async fn finish_drain_inner(
        &mut self,
        resolution: SourceDrainResolution,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.ensure_pending_drain_resolution(resolution, deadline)?;
        let pending = self
            .source_drain
            .as_mut()
            .and_then(|active| active.pending_resolution.as_mut())
            .expect("pending Kafka resolution was installed above");
        let execution = Arc::clone(&pending.execution);
        let mut wait_guard = KafkaDrainWaitGuard::new(Arc::clone(&execution));
        let reply = match tokio::time::timeout_at(pending.deadline, &mut pending.reply).await {
            Ok(reply) => reply,
            Err(_) => match execution.compare_exchange(
                KAFKA_DRAIN_EXECUTION_PENDING,
                KAFKA_DRAIN_EXECUTION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) | Err(KAFKA_DRAIN_EXECUTION_CANCELLED) => {
                    let error: Arc<str> = Arc::from(
                        "Kafka drain engine deadline expired before resolution completed",
                    );
                    pending.terminal_error = Some(Arc::clone(&error));
                    wait_guard.disarm();
                    return Err(ConnectorError::Internal(error.to_string()));
                }
                Err(KAFKA_DRAIN_EXECUTION_STARTED) => (&mut pending.reply).await,
                Err(state) => {
                    wait_guard.disarm();
                    return Err(ConnectorError::Internal(format!(
                        "Kafka drain resolution has invalid execution state {state}"
                    )));
                }
            },
        };
        wait_guard.disarm();
        match reply {
            Ok(Ok(())) => {
                self.source_drain = None;
                Ok(())
            }
            Ok(Err(error)) => {
                pending.terminal_error = Some(Arc::from(error.as_str()));
                Err(ConnectorError::Internal(error))
            }
            Err(_) => {
                let error: Arc<str> = Arc::from("Kafka reader dropped drain resolution");
                pending.terminal_error = Some(Arc::clone(&error));
                Err(ConnectorError::Internal(error.to_string()))
            }
        }
    }

    fn ensure_pending_drain_resolution(
        &mut self,
        resolution: SourceDrainResolution,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.check_reader_health("resolving a global source drain")?;
        let active = self
            .source_drain
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: format!("active Kafka drain {:?}", resolution.round),
                actual: "no Kafka drain".into(),
            })?;
        if active.request.round != resolution.round {
            return Err(ConnectorError::InvalidState {
                expected: format!("active Kafka drain {:?}", active.request.round),
                actual: format!("resolution for {:?}", resolution.round),
            });
        }
        let cut =
            active
                .cut
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "Kafka FIFO drain cut before resolution".into(),
                    actual: "drain boundary not consumed".into(),
                })?;
        if let Some(pending) = active.pending_resolution.as_ref() {
            if pending.resolution != resolution {
                return Err(ConnectorError::InvalidState {
                    expected: format!("pending Kafka resolution {:?}", pending.resolution),
                    actual: format!("conflicting Kafka resolution {resolution:?}"),
                });
            }
            // The first terminal command owns the resolution deadline. A retry waits on that
            // same command and cannot extend or shorten its provider execution window.
            if let Some(error) = pending.terminal_error.as_ref() {
                return Err(ConnectorError::Internal(error.to_string()));
            }
            if pending.execution.load(Ordering::Acquire) == KAFKA_DRAIN_EXECUTION_CANCELLED {
                return Err(ConnectorError::Internal(
                    "Kafka drain resolution was cancelled before execution".into(),
                ));
            }
        } else {
            if tokio::time::Instant::now() >= deadline {
                return Err(ConnectorError::Internal(
                    "Kafka drain engine deadline expired before resolution".into(),
                ));
            }
            let tx = self.reader_drain_tx.as_ref().ok_or_else(|| {
                ConnectorError::Internal("Kafka reader drain channel is absent".into())
            })?;
            let execution = Arc::new(AtomicU8::new(KAFKA_DRAIN_EXECUTION_PENDING));
            let (reply, result) = tokio::sync::oneshot::channel();
            tx.send(KafkaReaderDrainCommand::Resolve {
                resolution,
                cut,
                deadline,
                execution: Arc::clone(&execution),
                reply,
            })
            .map_err(|_| ConnectorError::Internal("Kafka reader drain channel closed".into()))?;
            self.source_drain
                .as_mut()
                .expect("active drain was validated above")
                .pending_resolution = Some(KafkaPendingDrainResolution {
                resolution,
                deadline,
                execution,
                reply: result,
                terminal_error: None,
            });
            self.data_ready.notify_one();
        }
        Ok(())
    }
}
