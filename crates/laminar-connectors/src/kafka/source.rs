//! Kafka source connector: consumes topics via rdkafka's `StreamConsumer`,
//! deserializes with pluggable formats, and yields Arrow `RecordBatch`es.

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use rdkafka::TopicPartitionList;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tracing::{debug, info, warn};

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskGuard, ConnectorTaskOwner, ConnectorTaskTracker, DeliveryGuarantee, SourceBatch,
    SourceConnector, SourceConsistency, SourceContract, SourceDrainOutcome, SourceDrainRequest,
    SourceDrainResolution, SourceInputMode, SourceMutation, SourcePosition,
    SourceRowPositionCapability, SourceRowPositions, SourceStart, SourceTopology,
};
use crate::error::{ConnectorError, SerdeError};
use crate::serde::{self, Format, RecordDeserializer};

use super::avro::AvroDeserializer;
use super::config::{
    resolve_value_subject, KafkaSourceConfig, OffsetReset, SchemaEvolutionStrategy, StartupMode,
    TopicSubscription,
};
use super::metadata_error::{fetch_error, invalid_response, topic_error};
use super::metrics::KafkaSourceMetrics;
use super::offsets::{OffsetTracker, KAFKA_PARTITION_BASELINE_PREFIX};
use super::rebalance::RebalanceState;
use super::schema_registry::SchemaRegistryClient;

use super::rebalance::LaminarConsumerContext;
use crate::schema::evolution::SchemaEvolution;
use crate::schema::traits::{CompatibilityMode, EvolutionVerdict};

/// Locks a mutex, recovering from poison if a prior holder panicked.
///
/// Used for state shared with rdkafka's rebalance callback thread.
/// Poison indicates a panic in the callback — the data may be stale
/// but is structurally sound, so we recover rather than propagate.
fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        tracing::warn!("mutex poisoned, recovering");
        poisoned.into_inner()
    })
}

fn publish_reader_fault(
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
fn terminalize_guaranteed_poll_error(
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
struct KafkaPayload {
    data: Vec<u8>,
    topic: Arc<str>,
    partition: i32,
    /// Precomputed route for cluster-owned inputs; absent for local readers.
    partition_vnode: Option<u32>,
    offset: i64,
    timestamp_ms: Option<i64>,
    /// Message headers as a JSON string; populated only when `include_headers` is set.
    headers_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaDrainPartition {
    topic: Arc<str>,
    partition: i32,
}

#[derive(Debug, Clone)]
struct KafkaDrainBoundary {
    round: laminar_core::checkpoint::AssignmentDrainId,
    inputs: Arc<[KafkaDrainPartition]>,
}

enum KafkaReaderItem {
    Payload(KafkaPayload),
    DrainBoundary(KafkaDrainBoundary),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaDrainPosition {
    topic: Arc<str>,
    partition: i32,
    next_offset: i64,
}

enum KafkaReaderDrainCommand {
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

struct KafkaReaderDrain {
    request: SourceDrainRequest,
    prepare_deadline: tokio::time::Instant,
    inputs: Arc<[KafkaDrainPartition]>,
    held_inputs: Arc<[KafkaDrainPartition]>,
    held_assignment_version: Option<u64>,
    hold_complete: bool,
    boundary_queued: bool,
}

struct KafkaPendingDrainResolution {
    resolution: SourceDrainResolution,
    deadline: tokio::time::Instant,
    execution: Arc<AtomicU8>,
    reply: tokio::sync::oneshot::Receiver<Result<(), String>>,
    terminal_error: Option<Arc<str>>,
}

struct KafkaSourceDrain {
    request: SourceDrainRequest,
    prepare_deadline: tokio::time::Instant,
    boundary: Option<KafkaDrainBoundary>,
    cut: Option<Arc<[KafkaDrainPosition]>>,
    pending_resolution: Option<KafkaPendingDrainResolution>,
}

const KAFKA_BACKGROUND_CLOSE_BUDGET: std::time::Duration = std::time::Duration::from_millis(500);
const KAFKA_DRAIN_EXECUTION_PENDING: u8 = 0;
const KAFKA_DRAIN_EXECUTION_STARTED: u8 = 1;
const KAFKA_DRAIN_EXECUTION_CANCELLED: u8 = 2;
const KAFKA_PARTITION_INVENTORY_METADATA: &str = "kafka.partition.inventory.v1";
const KAFKA_POSITION_LOOKUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
const KAFKA_POSITION_LOOKUP_CONCURRENCY: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KafkaBlockingTaskError {
    Retired,
    WorkerDropped,
}

impl std::fmt::Display for KafkaBlockingTaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retired => f.write_str("Kafka connector generation retired"),
            Self::WorkerDropped => f.write_str("Kafka blocking worker ended without a result"),
        }
    }
}

/// Retains every synchronous librdkafka call started by one source generation.
///
/// Tokio cannot cancel a `spawn_blocking` closure after it starts. Keeping its
/// handle here lets close abort queued work and reap completed joins. The
/// connector's generic terminal tracker, retained by `terminal_guard`, is the
/// replacement fence when a native call outlives its calling future.
#[derive(Clone)]
struct KafkaBlockingTasks {
    retired: Arc<AtomicBool>,
    handles: Arc<AsyncMutex<Vec<tokio::task::JoinHandle<()>>>>,
    reaper_started: Arc<AtomicBool>,
    terminal_guard: Arc<ConnectorTaskGuard>,
}

impl KafkaBlockingTasks {
    fn new(terminal_guard: ConnectorTaskGuard) -> Self {
        Self {
            retired: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(AsyncMutex::new(Vec::new())),
            reaper_started: Arc::new(AtomicBool::new(false)),
            terminal_guard: Arc::new(terminal_guard),
        }
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, KafkaBlockingTaskError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        if self.retired.load(Ordering::Acquire) {
            return Err(KafkaBlockingTaskError::Retired);
        }

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let mut handles = self.handles.lock().await;
        if self.retired.load(Ordering::Acquire) {
            return Err(KafkaBlockingTaskError::Retired);
        }
        // This under-lock check is the admission seal: after retirement no
        // code path may clone the grouped terminal guard for provider work.
        let retired = Arc::clone(&self.retired);
        let terminal_guard = Arc::clone(&self.terminal_guard);
        handles.push(tokio::task::spawn_blocking(move || {
            let _terminal_guard = terminal_guard;
            if retired.load(Ordering::Acquire) {
                let _ = result_tx.send(Err(KafkaBlockingTaskError::Retired));
                return;
            }
            let result = operation();
            if retired.load(Ordering::Acquire) {
                let _ = result_tx.send(Err(KafkaBlockingTaskError::Retired));
            } else {
                let _ = result_tx.send(Ok(result));
            }
        }));
        drop(handles);

        let result = result_rx
            .await
            .map_err(|_| KafkaBlockingTaskError::WorkerDropped)?;
        self.reap_finished().await;
        result
    }

    async fn reap_finished(&self) {
        let completed = {
            let mut handles = self.handles.lock().await;
            let mut completed = Vec::new();
            let mut index = 0;
            while index < handles.len() {
                if handles[index].is_finished() {
                    completed.push(handles.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            completed
        };
        for handle in completed {
            if let Err(error) = handle.await {
                warn!(%error, "Kafka blocking worker failed");
            }
        }
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    async fn join_until(&self, deadline: tokio::time::Instant) -> bool {
        self.retire();
        let mut handles = self.handles.lock().await;
        for handle in handles.iter() {
            handle.abort();
        }
        while let Some(handle) = handles.first_mut() {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(result) => {
                    if let Err(error) = result {
                        debug!(%error, "Kafka blocking worker cancelled during retirement");
                    }
                    handles.swap_remove(0);
                }
                Err(_) => return false,
            }
        }
        self.reaper_started.store(true, Ordering::Release);
        true
    }

    fn ensure_reaper(&self) {
        self.retire();
        if self.reaper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        // Retirement is published before this lock. If the list is empty while
        // holding it, no racing `run` can pass its under-lock retirement check
        // and install a later handle.
        if self
            .handles
            .try_lock()
            .is_ok_and(|handles| handles.is_empty())
        {
            return;
        }
        let generation = self.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.reaper_started.store(false, Ordering::Release);
            warn!("Kafka blocking generation retired outside a Tokio runtime");
            return;
        };
        drop(runtime.spawn(async move {
            let mut handles = generation.handles.lock().await;
            for handle in handles.iter() {
                handle.abort();
            }
            while let Some(handle) = handles.first_mut() {
                if let Err(error) = handle.await {
                    debug!(%error, "Kafka blocking worker cancelled during reaping");
                }
                handles.swap_remove(0);
            }
        }));
    }

    /// Admit only terminal destruction after retirement. This does not run a
    /// provider operation or expose a result to the retired connector.
    fn spawn_final_drop<T: Send + Sync + 'static>(&self, owner: Arc<T>, resource: &'static str) {
        let terminal_guard = Arc::clone(&self.terminal_guard);
        let reap = move || {
            let _terminal_guard = terminal_guard;
            while Arc::strong_count(&owner) > 1 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            drop(owner);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn_blocking(reap));
        } else {
            warn!(resource, "Kafka resource retired outside a Tokio runtime");
            if let Err(error) = std::thread::Builder::new()
                .name("laminardb-kafka-resource-drop".into())
                .spawn(reap)
            {
                tracing::error!(resource, %error, "failed to start Kafka resource teardown thread");
            }
        }
    }

    #[cfg(test)]
    async fn tracked_count(&self) -> usize {
        self.handles.lock().await.len()
    }
}

struct KafkaDrainWaitGuard {
    execution: Arc<AtomicU8>,
    armed: bool,
}

impl KafkaDrainWaitGuard {
    fn new(execution: Arc<AtomicU8>) -> Self {
        Self {
            execution,
            armed: true,
        }
    }

    fn disarm(&mut self) {
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

fn claim_kafka_drain_execution(
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

type KafkaPartitionSet = std::collections::HashSet<(String, i32)>;
type KafkaPartitionBaselines = std::collections::HashMap<(String, i32), i64>;

struct KafkaStartPlan {
    config: KafkaSourceConfig,
    delivery: DeliveryGuarantee,
    is_resume: bool,
    resume_inventory: Option<KafkaPartitionSet>,
    resume_baselines: KafkaPartitionBaselines,
}
type KafkaPartitionRoutes = std::collections::HashMap<Arc<str>, Arc<[u32]>>;
type KafkaRotationBaselines =
    std::collections::HashMap<Arc<str>, std::collections::HashMap<i32, i64>>;

fn kafka_drain_partitions(
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

fn kafka_partition_routes(
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
        let topic_routes =
            super::vnode_routing::partition_vnodes(source_identity, topic, *count, vnode_count)?;
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

fn kafka_partition_set(tpl: &TopicPartitionList) -> Result<KafkaPartitionSet, String> {
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

fn validate_kafka_assignment(
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

fn kafka_bootstrap_is_unassigned(
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
        super::vnode_routing::validate_owner_map(published.owners(), self_id)?;
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

fn kafka_owned_partition_sets(
    routes: &KafkaPartitionRoutes,
    published: &laminar_core::state::VnodeAssignmentSnapshot,
    self_id: laminar_core::state::NodeId,
    reconciled_version: u64,
) -> Result<(KafkaPartitionSet, KafkaPartitionSet), ConnectorError> {
    super::vnode_routing::validate_owner_map(published.owners(), self_id)?;
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

fn kafka_assignment_fence_matches(
    registry: &laminar_core::state::VnodeRegistry,
    reconciled: &AtomicU64,
    assignment_version: u64,
) -> bool {
    assignment_version != 0
        && registry.assignment_version() == assignment_version
        && reconciled.load(Ordering::Acquire) == assignment_version
}

fn try_capture_at_assignment_fence<T>(
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

fn cached_partition_vnode(
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

fn kafka_partition_result_errors(tpl: &TopicPartitionList) -> Vec<String> {
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

fn validate_kafka_partition_results(
    operation: &str,
    tpl: &TopicPartitionList,
) -> Result<(), String> {
    let errors = kafka_partition_result_errors(tpl);
    validate_kafka_partition_error_list(operation, &errors)
}

fn validate_kafka_partition_error_list(operation: &str, errors: &[String]) -> Result<(), String> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Kafka {operation} failed for partitions: {}",
            errors.join(", ")
        ))
    }
}

fn kafka_drain_target_ready(
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

async fn resolve_kafka_reader_drain(
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
struct KafkaAssignmentPublication {
    assignment_version: u64,
    owned_partitions: Arc<KafkaPartitionSet>,
    baselines: KafkaRotationBaselines,
}

impl KafkaAssignmentPublication {
    fn new(
        assignment_version: u64,
        owned_partitions: Arc<KafkaPartitionSet>,
        baselines: KafkaRotationBaselines,
    ) -> Self {
        Self {
            assignment_version,
            owned_partitions,
            baselines,
        }
    }
}

async fn join_background_task(
    handle: &mut Option<tokio::task::JoinHandle<()>>,
    deadline: tokio::time::Instant,
    task: &'static str,
) {
    let Some(owned) = handle.as_mut() else {
        return;
    };
    let completed = match tokio::time::timeout_at(deadline, &mut *owned).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(task, %error, "Kafka background task failed during shutdown");
            true
        }
        Err(_) => {
            warn!(
                task,
                "Kafka background task shutdown timed out; aborting it"
            );
            owned.abort();
            false
        }
    };
    if completed {
        *handle = None;
    }
}

fn ensure_background_task_reaper(
    handle: tokio::task::JoinHandle<()>,
    task_owner: &ConnectorTaskOwner,
    task: &'static str,
) {
    handle.abort();
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!(
            task,
            "Kafka background task retired outside a Tokio runtime"
        );
        return;
    };
    let Some(terminal_guard) = task_owner.track() else {
        warn!(
            task,
            "Kafka task generation was sealed before reader reaping"
        );
        return;
    };
    drop(runtime.spawn(async move {
        let _terminal_guard = terminal_guard;
        if let Err(error) = handle.await {
            debug!(task, %error, "Kafka retired background task reaped");
        }
    }));
}

/// Keep one owner on Tokio's blocking pool until all async task owners drain, then perform the
/// potentially blocking final drop there. The generation tracker retains the blocking handle if
/// the caller's close deadline expires.
async fn reap_last_arc_off_runtime<T: Send + Sync + 'static>(
    blocking_tasks: &KafkaBlockingTasks,
    owner: Arc<T>,
    deadline: tokio::time::Instant,
    resource: &'static str,
) {
    let reaper = blocking_tasks.run(move || {
        while Arc::strong_count(&owner) > 1 {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(owner);
    });
    match tokio::time::timeout_at(deadline, reaper).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(resource, %error, "Kafka blocking reaper failed"),
        Err(_) => warn!(
            resource,
            "Kafka resource cleanup exceeded close deadline; generation reaper retained it"
        ),
    }
}

/// Single-consumer async receiver for the reader → `poll_batch` queue.
type KafkaReaderRx = crossfire::AsyncRx<crossfire::mpsc::Array<KafkaReaderItem>>;

/// Kafka source connector that consumes messages and produces Arrow batches.
///
/// Operates in Ring 1 (background) and pushes deserialized `RecordBatch`
/// data to Ring 0 via the streaming `Source<T>` API.
///
/// # Lifecycle
///
/// 1. Create with [`KafkaSource::new`] or [`KafkaSource::with_schema_registry`]
/// 2. Call `start()` with the initial or exact recovered position
/// 3. Call `poll_batch()` in a loop to consume messages
/// 4. Call `checkpoint()` for fault tolerance
/// 5. Call `close()` for clean shutdown
///
/// Guaranteed delivery uses engine-owned manual assignment: the complete explicit topic inventory
/// in embedded/single-node mode or the owned vnode subset in cluster mode. Broker consumer-group
/// ownership can revoke a partition before an asynchronous engine checkpoint captures its final
/// offset, so dynamic group ownership is intentionally limited to `BestEffort`.
pub struct KafkaSource {
    consumer: Option<Arc<StreamConsumer<LaminarConsumerContext>>>,
    config: KafkaSourceConfig,
    /// Delivery contract selected by the engine for this connector generation.
    /// Only `BestEffort` may discard records that fail deserialization.
    delivery: DeliveryGuarantee,
    deserializer: Box<dyn RecordDeserializer>,
    offsets: OffsetTracker,
    state: ConnectorState,
    metrics: KafkaSourceMetrics,
    schema: SchemaRef,
    channel_len: Arc<AtomicUsize>,
    rebalance_state: Arc<Mutex<RebalanceState>>,
    /// Shared rebalance counter bridging `LaminarConsumerContext` → `KafkaSourceMetrics`.
    rebalance_counter: Arc<AtomicU64>,
    /// Bumped on each partition revoke; `poll_batch` compares it lock-free to
    /// detect a revoke and purge the lost partitions' offsets.
    revoke_generation: Arc<AtomicU64>,
    /// Bumped on each partition assign; the reader loop seeks the newly-assigned
    /// partitions on change (see the seek block in `ensure_reader_started`).
    assign_generation: Arc<AtomicU64>,
    last_seen_revoke_gen: u64,
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    data_ready: Arc<Notify>,
    msg_rx: Option<KafkaReaderRx>,
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
    /// Sole admission authority for detached work in this connector generation.
    task_owner: ConnectorTaskOwner,
    /// Cloneable terminal observer returned to the connector runtime.
    task_tracker: ConnectorTaskTracker,
    /// Blocking native calls owned and fenced by this connector generation.
    blocking_tasks: KafkaBlockingTasks,
    /// First terminal reader reason, published before an assignment wait can hide its exit.
    reader_fault: Arc<Mutex<Option<Arc<str>>>>,
    /// Allocated only for a cluster-assigned instance; local readers have no rotation path.
    reader_drain_tx: Option<tokio::sync::mpsc::UnboundedSender<KafkaReaderDrainCommand>>,
    source_drain: Option<KafkaSourceDrain>,
    /// Offset snapshot for the rebalance callback's seek-on-assign, refreshed
    /// once per `poll_batch()` cycle.
    offset_snapshot: Arc<Mutex<OffsetTracker>>,
    /// When set, every partition absent from the installed engine checkpoint is
    /// explicitly positioned at the configured deterministic start. This must
    /// remain armed for the connector lifetime: a later rebalance can introduce
    /// a partition that was not present in the startup checkpoint, and using the
    /// broker's stored group offset there would cross engine timelines.
    deterministic_unrecorded_position: Arc<AtomicBool>,

    /// Stable catalog source identity for namespaced cluster handoff offsets.
    source_name: Arc<str>,

    /// Cluster vnode assignment: when set, `start()` manually `assign()`s this
    /// source's vnode-owned partitions instead of `subscribe()`, and the reader
    /// re-binds on version rotation.
    vnode_assignment: Option<(
        Arc<laminar_core::state::VnodeRegistry>,
        laminar_core::state::NodeId,
    )>,
    /// Precomputed source/topic/partition routes used by the per-record stale-owner fence.
    vnode_partition_routes: KafkaPartitionRoutes,
    /// Canonical non-vnode manual assignment used by local guaranteed, specific, and timestamp
    /// starts. Checkpointing trusts this engine-owned inventory, never a fallible broker query.
    manual_topic_partitions: KafkaPartitionSet,
    /// Concrete next-to-read position captured before intake for every engine-owned partition.
    /// It remains authoritative until that partition has an accepted-record offset.
    manual_partition_baselines: KafkaPartitionBaselines,
    /// Exact Kafka ownership and durable handoff positions for one assignment version.
    assignment_publication: Arc<Mutex<Arc<KafkaAssignmentPublication>>>,
    /// Zero-cost steady-state fast path: avoids locking the rotation snapshot
    /// when no acquired partition is waiting for its first accepted record.
    rotation_partition_baseline_count: Arc<AtomicUsize>,
    /// Rotation publication whose stale local offsets have already been removed.
    /// Baseline publication is immutable within one assignment version, so this
    /// keeps that cleanup off the steady-state poll path.
    applied_rotation_baseline_version: Option<u64>,
    /// Assignment version whose Kafka consumer rebind and durable cursor
    /// validation have completed. Barriers stay fenced while this trails the
    /// registry's atomically published version.
    reconciled_assignment_version: Arc<AtomicU64>,

    /// Previous Avro writer schema, diffed against the next for evolution detection.
    last_avro_schema: Option<SchemaRef>,

    // Reusable poll_batch buffers — cleared each cycle, capacity retained.
    poll_payloads: Vec<KafkaPayload>,
    poll_payload_buf: Vec<u8>,
    poll_payload_offsets: Vec<(usize, usize)>,
    poll_meta_partitions: Vec<i32>,
    poll_meta_offsets: Vec<i64>,
    poll_meta_timestamps: Vec<Option<i64>>,
    poll_meta_headers: Vec<Option<String>>,
    /// This poll's (topic, partition, offset) triples, folded into `offsets` only after the complete
    /// output batch is constructed — so decode or finalization failure cannot advance beyond data
    /// that was never emitted. Reused across polls.
    poll_staged_offsets: Vec<(Arc<str>, i32, i64)>,
}

impl KafkaSource {
    /// Creates a new Kafka source connector with explicit schema.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: KafkaSourceConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Self {
        Self::build_base(schema, config, select_deserializer, None, registry)
    }

    /// Creates a new Kafka source connector with Schema Registry.
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        config: KafkaSourceConfig,
        sr_client: SchemaRegistryClient,
    ) -> Self {
        let sr = Arc::new(sr_client);
        let sr_clone = Arc::clone(&sr);
        let deser_factory = move |format: Format| -> Box<dyn RecordDeserializer> {
            if format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(sr_clone))
            } else {
                select_deserializer(format)
            }
        };
        Self::build_base(schema, config, deser_factory, Some(sr), None)
    }

    /// Build a Schema Registry client from the parsed config, or
    /// `Ok(None)` when `schema.registry.url` is not set.
    fn build_sr_client(
        config: &KafkaSourceConfig,
    ) -> Result<Option<SchemaRegistryClient>, ConnectorError> {
        let Some(sr_url) = config.schema_registry_url.as_ref() else {
            return Ok(None);
        };
        let client = if let Some(ca) = config.schema_registry_ssl_ca_location.as_deref() {
            SchemaRegistryClient::with_tls_mtls(
                sr_url.clone(),
                config.schema_registry_auth.clone(),
                ca,
                config.schema_registry_ssl_certificate_location.as_deref(),
                config.schema_registry_ssl_key_location.as_deref(),
            )?
        } else {
            SchemaRegistryClient::new(sr_url.clone(), config.schema_registry_auth.clone())?
        };
        Ok(Some(client))
    }

    fn build_base(
        schema: SchemaRef,
        config: KafkaSourceConfig,
        deser_factory: impl FnOnce(Format) -> Box<dyn RecordDeserializer>,
        schema_registry: Option<Arc<SchemaRegistryClient>>,
        registry: Option<&prometheus::Registry>,
    ) -> Self {
        let deserializer = deser_factory(config.format);
        let channel_len = Arc::new(AtomicUsize::new(0));
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        let blocking_guard = task_owner
            .track()
            .expect("new Kafka source task generation must be live");

        Self {
            consumer: None,
            config,
            delivery: DeliveryGuarantee::BestEffort,
            deserializer,
            offsets: OffsetTracker::new(),
            state: ConnectorState::Created,
            metrics: KafkaSourceMetrics::new(registry),
            schema,
            channel_len,
            rebalance_state: Arc::new(Mutex::new(RebalanceState::new())),
            rebalance_counter: Arc::new(AtomicU64::new(0)),
            revoke_generation: Arc::new(AtomicU64::new(0)),
            assign_generation: Arc::new(AtomicU64::new(0)),
            last_seen_revoke_gen: 0,
            schema_registry,
            data_ready: Arc::new(Notify::new()),
            msg_rx: None,
            reader_handle: None,
            reader_shutdown: None,
            task_owner,
            task_tracker,
            blocking_tasks: KafkaBlockingTasks::new(blocking_guard),
            reader_fault: Arc::new(Mutex::new(None)),
            reader_drain_tx: None,
            source_drain: None,
            offset_snapshot: Arc::new(Mutex::new(OffsetTracker::new())),
            deterministic_unrecorded_position: Arc::new(AtomicBool::new(false)),
            source_name: Arc::from(""),
            vnode_assignment: None,
            vnode_partition_routes: KafkaPartitionRoutes::new(),
            manual_topic_partitions: std::collections::HashSet::new(),
            manual_partition_baselines: std::collections::HashMap::new(),
            assignment_publication: Arc::new(Mutex::new(Arc::new(
                KafkaAssignmentPublication::default(),
            ))),
            rotation_partition_baseline_count: Arc::new(AtomicUsize::new(0)),
            applied_rotation_baseline_version: None,
            reconciled_assignment_version: Arc::new(AtomicU64::new(0)),
            last_avro_schema: None,
            poll_payloads: Vec::new(),
            poll_payload_buf: Vec::new(),
            poll_payload_offsets: Vec::new(),
            poll_meta_partitions: Vec::new(),
            poll_meta_offsets: Vec::new(),
            poll_meta_timestamps: Vec::new(),
            poll_meta_headers: Vec::new(),
            poll_staged_offsets: Vec::new(),
        }
    }

    /// Lifecycle state (Created → Initializing → Running → Closed).
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Shared backpressure fill counter for downstream wiring.
    #[must_use]
    pub fn channel_len(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.channel_len)
    }

    /// Shared partition assignment state (updated by rebalance callbacks).
    #[must_use]
    pub fn rebalance_state(&self) -> Arc<Mutex<RebalanceState>> {
        Arc::clone(&self.rebalance_state)
    }

    /// Whether a Schema Registry client is configured.
    #[must_use]
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    fn fail_startup(&mut self) {
        self.state = ConnectorState::Failed;
        self.blocking_tasks.retire();
        self.blocking_tasks.ensure_reaper();
        if let Some(consumer) = self.consumer.take() {
            consumer.unsubscribe();
            self.blocking_tasks
                .spawn_final_drop(consumer, "consumer after failed startup");
        }
    }

    fn capture_drain_positions(
        &self,
        inputs: &[KafkaDrainPartition],
        prepare_deadline: Option<tokio::time::Instant>,
    ) -> Result<Arc<[KafkaDrainPosition]>, ConnectorError> {
        if prepare_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Err(ConnectorError::Internal(
                "Kafka drain deadline expired before cursor capture".into(),
            ));
        }
        let publication = Arc::clone(&lock_or_recover(&self.assignment_publication));
        let mut cut = Vec::with_capacity(inputs.len());
        for input in inputs {
            if prepare_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                return Err(ConnectorError::Internal(
                    "Kafka drain deadline expired during cursor capture".into(),
                ));
            }
            let next_offset =
                if let Some(offset) = self.offsets.get(input.topic.as_ref(), input.partition) {
                    offset.checked_add(1).ok_or_else(|| {
                        ConnectorError::Internal(format!(
                            "Kafka drain offset overflow for '{}-{}'",
                            input.topic, input.partition
                        ))
                    })?
                } else if let Some(next) = rotation_partition_baseline(
                    &publication.baselines,
                    input.topic.as_ref(),
                    input.partition,
                ) {
                    next
                } else if let Some(next) = self
                    .manual_partition_baselines
                    .get(&(input.topic.to_string(), input.partition))
                {
                    *next
                } else {
                    return Err(ConnectorError::InvalidState {
                        expected: format!(
                            "numeric next-to-read position for drained Kafka input '{}-{}'",
                            input.topic, input.partition
                        ),
                        actual: "no accepted offset or deterministic baseline".into(),
                    });
                };
            cut.push(KafkaDrainPosition {
                topic: Arc::clone(&input.topic),
                partition: input.partition,
                next_offset,
            });
        }
        if prepare_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Err(ConnectorError::Internal(
                "Kafka drain deadline expired during cursor capture".into(),
            ));
        }
        Ok(cut.into())
    }

    fn check_reader_health(&self, operation: &str) -> Result<(), ConnectorError> {
        if let Some(reason) = lock_or_recover(&self.reader_fault).clone() {
            return Err(ConnectorError::Internal(format!(
                "Kafka reader terminated while {operation}: {reason}"
            )));
        }
        if self
            .reader_handle
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return Err(ConnectorError::Internal(format!(
                "Kafka reader task exited while {operation}"
            )));
        }
        Ok(())
    }

    fn validate_active_drain_cursor(&self) -> Result<(), ConnectorError> {
        self.check_reader_health("validating a drain cursor")?;
        let Some(active) = self.source_drain.as_ref() else {
            return Ok(());
        };
        let Some(expected) = active.cut.as_deref() else {
            return Ok(());
        };
        let Some(boundary) = active.boundary.as_ref() else {
            return Err(ConnectorError::Internal(
                "Kafka drain cut exists without its FIFO boundary".into(),
            ));
        };
        // The certified cut remains valid while terminal materialization is pending; the
        // preparation deadline no longer applies after the cut has been captured.
        let current = self.capture_drain_positions(&boundary.inputs, None)?;
        if current.as_ref() != expected {
            return Err(ConnectorError::Internal(
                "Kafka input cursor advanced after its drain receipt".into(),
            ));
        }
        Ok(())
    }

    /// Spawns the background reader task on the first `poll_batch()`.
    /// The startup cursor has already been installed by `start()` before these
    /// tasks can observe the consumer.
    fn ensure_reader_started(&mut self) {
        if self.reader_handle.is_some() || self.consumer.is_none() {
            return;
        }

        let consumer = Arc::clone(self.consumer.as_ref().unwrap());
        // Drain control exists only on a cluster-assigned source. Embedded and single-node
        // readers retain their existing allocation-free control path.
        let vnode_reassign = self
            .vnode_assignment
            .as_ref()
            .map(|(r, s)| (Arc::clone(r), *s));
        let (msg_tx, msg_rx) =
            crossfire::mpsc::bounded_async::<KafkaReaderItem>(self.config.reader_channel_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (reader_drain_tx, mut reader_drain_rx) = if vnode_reassign.is_some() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let data_ready = Arc::clone(&self.data_ready);
        let reader_fault = Arc::clone(&self.reader_fault);
        let channel_len = Arc::clone(&self.channel_len);
        let capture_headers = self.config.include_headers;
        let reader_channel_capacity = self.config.reader_channel_capacity;
        let reader_channel_capacity_f64 =
            u32::try_from(reader_channel_capacity).map_or(f64::from(u32::MAX), f64::from);
        let assign_generation = Arc::clone(&self.assign_generation);
        let rebalance_state = Arc::clone(&self.rebalance_state);
        let pause_threshold = self.config.backpressure_high_watermark;
        let resume_threshold = self.config.backpressure_low_watermark;

        // -- Reader task: message consumption, backpressure, revoke pruning --
        // Engine-controlled re-assignment inputs (cluster mode; `None` otherwise).
        let vnode_partition_routes = std::mem::take(&mut self.vnode_partition_routes);
        let reassign_snapshot = Arc::clone(&self.offset_snapshot);
        let reassign_baselines = self.manual_partition_baselines.clone();
        let assignment_publication = Arc::clone(&self.assignment_publication);
        let rotation_baseline_count = Arc::clone(&self.rotation_partition_baseline_count);
        let reconciled_assignment_version = Arc::clone(&self.reconciled_assignment_version);
        let require_durable_baselines = !reassign_baselines.is_empty();
        let reassign_default_offset = startup_default_offset(&self.config.startup_mode);
        let deterministic_unrecorded = Arc::clone(&self.deterministic_unrecorded_position);
        let source_name = Arc::clone(&self.source_name);
        let blocking_tasks = self.blocking_tasks.clone();
        let deterministic_default =
            deterministic_initial_offset(&self.config.startup_mode, self.config.auto_offset_reset);
        let mut reader_shutdown = shutdown_rx;
        let reader_guard = self
            .task_owner
            .track()
            .expect("live Kafka source must admit its reader task");
        let reader_handle = tokio::spawn(async move {
            let _reader_guard = reader_guard;
            let mut cached_topic: Arc<str> = Arc::from("");
            let mut cached_topic_routes: Option<Arc<[u32]>> = None;
            let mut is_paused = false;
            let mut last_assign_gen: u64 = 0;
            // start() records the exact publication used for its initial Kafka assignment.
            // Starting from that fence detects even a self→other→self sequence that lands before
            // this lazy reader gets its first turn; boot-unassigned sources legitimately start 0.
            let mut last_assignment_version = reconciled_assignment_version.load(Ordering::Acquire);
            let mut drain_paused: std::collections::HashSet<(Arc<str>, i32)> =
                std::collections::HashSet::new();
            let mut active_drain: Option<KafkaReaderDrain> = None;
            let mut deferred_drain_command = None;

            loop {
                if active_drain.as_ref().is_some_and(|active| {
                    !active.boundary_queued
                        && tokio::time::Instant::now() >= active.prepare_deadline
                }) {
                    publish_reader_fault(
                        &reader_fault,
                        &data_ready,
                        "Kafka source drain preparation exceeded its engine deadline",
                    );
                    return;
                }
                // On rotation, apply only the DELTA (incremental assign/unassign): a
                // full re-assign would re-seek kept partitions and re-fetch records
                // already buffered ahead, re-emitting committed rows. Newly-acquired
                // partitions seek to their handoff offset; kept ones are untouched.
                if let Some((registry, self_id)) = &vnode_reassign {
                    let published = registry.versioned_snapshot();
                    let version = published.version();
                    if version != last_assignment_version {
                        let current = match consumer.assignment() {
                            Ok(current) => current,
                            Err(error) => {
                                warn!(
                                    version,
                                    %error,
                                    "Kafka source could not inspect its current assignment; rotation will retry"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                continue;
                            }
                        };
                        // A source can miss an intermediate self→other→self publication while
                        // fenced (for example consecutive dead-node shedding). The latest owner
                        // set then matches the live Kafka assignment even though the partition
                        // must be unassigned and re-seeked to its durable checkpoint position.
                        let current_set = match kafka_partition_set(&current) {
                            Ok(current) => current,
                            Err(error) => {
                                publish_reader_fault(&reader_fault, &data_ready, error);
                                return;
                            }
                        };
                        let (owned_set, reacquired) = match kafka_owned_partition_sets(
                            &vnode_partition_routes,
                            &published,
                            *self_id,
                            last_assignment_version,
                        ) {
                            Ok(partitions) => partitions,
                            Err(error) => {
                                warn!(
                                    source = source_name.as_ref(),
                                    %error,
                                    "Kafka source rejected its cached partition routes"
                                );
                                publish_reader_fault(
                                    &reader_fault,
                                    &data_ready,
                                    format!("invalid cached partition route: {error}"),
                                );
                                return;
                            }
                        };

                        let mut to_remove = TopicPartitionList::new();
                        for (topic, partition) in current_set
                            .difference(&owned_set)
                            .chain(reacquired.intersection(&current_set))
                        {
                            to_remove.add_partition(topic, *partition);
                        }

                        let offsets = lock_or_recover(&reassign_snapshot).clone();
                        let mut to_add = TopicPartitionList::new();
                        let mut acquired_positions = KafkaPartitionBaselines::new();
                        for (topic, partition) in owned_set
                            .difference(&current_set)
                            .chain(reacquired.intersection(&current_set))
                        {
                            let p = *partition;
                            let offset = if let Some(next) = match acquired_numeric_position(
                                &offsets,
                                &reassign_baselines,
                                topic.as_str(),
                                p,
                            ) {
                                Ok(position) => position,
                                Err(error) => {
                                    warn!(
                                        topic = topic.as_str(),
                                        partition = p,
                                        %error,
                                    "Kafka source rejected an invalid checkpoint position"
                                    );
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        format!("invalid checkpoint position: {error}"),
                                    );
                                    return;
                                }
                            } {
                                info!(
                                    topic = topic.as_str(),
                                    partition = p,
                                    resume = next,
                                    "acquired partition uses durable numeric position"
                                );
                                acquired_positions.insert((topic.clone(), p), next);
                                rdkafka::Offset::Offset(next)
                            } else if require_durable_baselines {
                                warn!(
                                    topic = topic.as_str(),
                                    partition = p,
                                    "acquired Kafka partition has no durable next-to-read baseline"
                                );
                                publish_reader_fault(
                                    &reader_fault,
                                    &data_ready,
                                    "acquired partition has no durable baseline",
                                );
                                return;
                            } else if deterministic_unrecorded.load(Ordering::Acquire) {
                                let Some(initial_offset) = deterministic_default else {
                                    warn!(
                                            topic = topic.as_str(),
                                            partition = p,
                                            "cannot deterministically position acquired Kafka partition"
                                        );
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "acquired partition has no deterministic position",
                                    );
                                    return;
                                };
                                initial_offset
                            } else {
                                warn!(
                                    topic = topic.as_str(),
                                    partition = p,
                                    "acquired partition has no checkpoint or local offset; \
                                         falling back to the startup default"
                                );
                                reassign_default_offset
                            };
                            if let Err(error) =
                                to_add.add_partition_offset(topic.as_str(), p, offset)
                            {
                                warn!(
                                    topic = topic.as_str(),
                                    partition = p,
                                    %error,
                                    "failed to build Kafka rotation assignment"
                                );
                                publish_reader_fault(
                                    &reader_fault,
                                    &data_ready,
                                    format!("invalid rotation assignment: {error}"),
                                );
                                return;
                            }
                        }

                        let owned_set = Arc::new(owned_set);

                        // Registry ownership is already committed. Publish the assignment cut before
                        // the first await so a concurrent checkpoint cannot resurrect a stale
                        // position from this node's earlier ownership stint.
                        {
                            let mut current = lock_or_recover(&assignment_publication);
                            let updated = update_rotation_baselines(
                                &current.baselines,
                                &owned_set,
                                &acquired_positions,
                            );
                            let count = rotation_baselines_len(&updated);
                            *current = Arc::new(KafkaAssignmentPublication::new(
                                version,
                                Arc::clone(&owned_set),
                                updated,
                            ));
                            rotation_baseline_count.store(count, Ordering::Release);
                        }

                        if !acquired_positions.is_empty() {
                            let acquired: KafkaPartitionSet =
                                acquired_positions.keys().cloned().collect();
                            let low_watermarks = match fetch_partition_low_watermarks(
                                blocking_tasks.clone(),
                                Arc::clone(&consumer),
                                &acquired,
                            )
                            .await
                            {
                                Ok(low_watermarks) => low_watermarks,
                                Err(error) => {
                                    warn!(
                                        version,
                                        %error,
                                        "Kafka source could not validate acquired positions; rotation will retry"
                                    );
                                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                    continue;
                                }
                            };
                            if let Err(error) = validate_positions_not_expired(
                                &OffsetTracker::new(),
                                &acquired_positions,
                                &low_watermarks,
                                &acquired,
                            ) {
                                warn!(
                                    version,
                                    %error,
                                    "Kafka source rejected an expired checkpoint position"
                                );
                                publish_reader_fault(
                                    &reader_fault,
                                    &data_ready,
                                    format!("expired checkpoint position: {error}"),
                                );
                                return;
                            }
                        }

                        let mut ok = true;
                        if to_remove.count() > 0 {
                            match consumer.incremental_unassign(&to_remove) {
                                Ok(()) => {
                                    if let Err(error) = validate_kafka_partition_results(
                                        "incremental unassign",
                                        &to_remove,
                                    ) {
                                        warn!(version, %error, "Kafka source unassign incomplete");
                                        ok = false;
                                    }
                                }
                                Err(e) => {
                                    warn!(version, error = %e, "Kafka source unassign failed");
                                    ok = false;
                                }
                            }
                        }
                        if to_add.count() > 0 {
                            match consumer.incremental_assign(&to_add) {
                                Ok(()) => {
                                    if let Err(error) = validate_kafka_partition_results(
                                        "incremental assign",
                                        &to_add,
                                    ) {
                                        warn!(version, %error, "Kafka source assign incomplete");
                                        ok = false;
                                    }
                                }
                                Err(e) => {
                                    warn!(version, error = %e, "Kafka source assign failed");
                                    ok = false;
                                }
                            }
                        }
                        if ok {
                            match consumer.assignment() {
                                Ok(active) => {
                                    match kafka_partition_set(&active).and_then(|active| {
                                        validate_kafka_assignment(&owned_set, &active)
                                    }) {
                                        Ok(()) => {}
                                        Err(error) => {
                                            warn!(
                                                version,
                                                %error,
                                                "Kafka source assignment verification failed"
                                            );
                                            ok = false;
                                        }
                                    }
                                }
                                Err(error) => {
                                    warn!(
                                        version,
                                        %error,
                                        "Kafka source could not verify its rebound assignment"
                                    );
                                    ok = false;
                                }
                            }
                        }
                        if !ok {
                            match tokio::time::timeout(
                                std::time::Duration::from_millis(10),
                                reader_shutdown.changed(),
                            )
                            .await
                            {
                                Ok(Ok(())) if *reader_shutdown.borrow() => break,
                                Ok(Err(_)) => break,
                                _ => {}
                            }
                            continue;
                        }

                        // Do not expose a successfully rebound but already obsolete target.
                        let current_assignment = registry.read_assignment();
                        if current_assignment.version() != version {
                            continue;
                        }
                        last_assignment_version = version;
                        reconciled_assignment_version.store(version, Ordering::Release);
                        drop(current_assignment);
                        if to_remove.count() > 0 || to_add.count() > 0 {
                            info!(
                                version,
                                acquired = to_add.count(),
                                revoked = to_remove.count(),
                                "Kafka source rebound partitions after vnode rotation"
                            );
                        }
                        // Reassignment can clear librdkafka's pause bit even when the same input
                        // is re-acquired. Require every added input to prove its target pause.
                        for element in to_add.elements() {
                            drain_paused.remove(&(Arc::from(element.topic()), element.partition()));
                        }
                        drain_paused.retain(|(t, p)| owned_set.contains(&(t.to_string(), *p)));
                        if let Some(active) = active_drain.as_mut() {
                            active.held_assignment_version = None;
                            active.hold_complete = false;
                        }
                        // A stale stint position must not shadow the handoff on re-acquire.
                        lock_or_recover(&reassign_snapshot).retain_assigned(&owned_set);
                        data_ready.notify_one();
                    }
                }

                // Exact source drain control is deliberately independent of assignment gossip.
                // A retained source-task command names the predecessor/target/leader round.
                let command = deferred_drain_command.take().or_else(|| {
                    reader_drain_rx
                        .as_mut()
                        .and_then(|receiver| receiver.try_recv().ok())
                });
                if let Some(command) = command {
                    match command {
                        KafkaReaderDrainCommand::Begin { request, deadline } => {
                            if let Some(current) = active_drain.as_ref() {
                                if current.request != request {
                                    warn!(
                                        current = ?current.request.round,
                                        requested = ?request.round,
                                        "Kafka reader received a conflicting drain round"
                                    );
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "conflicting Kafka drain round",
                                    );
                                    return;
                                }
                            } else {
                                if tokio::time::Instant::now() >= deadline {
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "Kafka source drain began after its engine deadline",
                                    );
                                    return;
                                }
                                let Some((registry, _)) = vnode_reassign.as_ref() else {
                                    warn!("Kafka reader received drain control without cluster ownership");
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "drain control has no cluster ownership",
                                    );
                                    return;
                                };
                                if registry.assignment_version()
                                    != request.round.predecessor_version
                                {
                                    warn!(
                                        current = registry.assignment_version(),
                                        predecessor = request.round.predecessor_version,
                                        "Kafka reader rejected drain for a stale predecessor"
                                    );
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "drain predecessor does not match current assignment",
                                    );
                                    return;
                                }
                                if last_assignment_version != request.round.predecessor_version {
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "Kafka drain predecessor is not reconciled",
                                    );
                                    return;
                                }
                                let assignment = match consumer.assignment() {
                                    Ok(assignment) => assignment,
                                    Err(error) => {
                                        publish_reader_fault(
                                            &reader_fault,
                                            &data_ready,
                                            format!(
                                                "Kafka drain could not inspect its assignment: {error}"
                                            ),
                                        );
                                        return;
                                    }
                                };
                                let inputs = match kafka_drain_partitions(&assignment) {
                                    Ok(inputs) => inputs,
                                    Err(error) => {
                                        warn!(
                                            source = source_name.as_ref(),
                                            %error,
                                            "Kafka reader rejected its drain assignment"
                                        );
                                        publish_reader_fault(
                                            &reader_fault,
                                            &data_ready,
                                            format!("invalid Kafka drain assignment: {error}"),
                                        );
                                        return;
                                    }
                                };
                                active_drain = Some(KafkaReaderDrain {
                                    request,
                                    prepare_deadline: deadline,
                                    held_inputs: Arc::clone(&inputs),
                                    inputs,
                                    held_assignment_version: Some(last_assignment_version),
                                    hold_complete: false,
                                    boundary_queued: false,
                                });
                            }
                        }
                        KafkaReaderDrainCommand::Resolve {
                            resolution,
                            cut,
                            deadline,
                            execution,
                            reply,
                        } => {
                            if active_drain.is_none() {
                                let _ = reply.send(Err(
                                    "Kafka reader has no active drain to resolve".into(),
                                ));
                                continue;
                            }
                            if tokio::time::Instant::now() >= deadline {
                                let _ = reply.send(Err(
                                    "Kafka drain deadline expired before target reconciliation"
                                        .into(),
                                ));
                                continue;
                            }
                            let Some((registry, _)) = vnode_reassign.as_ref() else {
                                let _ = reply.send(Err(
                                    "Kafka drain resolution has no cluster assignment".into(),
                                ));
                                continue;
                            };
                            let target_ready = match kafka_drain_target_ready(
                                resolution.round.target_version,
                                registry.assignment_version(),
                                last_assignment_version,
                            ) {
                                Ok(ready) => ready,
                                Err(error) => {
                                    let _ = reply.send(Err(error));
                                    continue;
                                }
                            };
                            let target_paused = target_ready
                                && active_drain.as_ref().is_some_and(|active| {
                                    active.held_assignment_version
                                        == Some(resolution.round.target_version)
                                        && active.hold_complete
                                });
                            if !target_ready || !target_paused {
                                deferred_drain_command = Some(KafkaReaderDrainCommand::Resolve {
                                    resolution,
                                    cut,
                                    deadline,
                                    execution,
                                    reply,
                                });
                            } else {
                                let result = resolve_kafka_reader_drain(
                                    &consumer,
                                    &blocking_tasks,
                                    vnode_reassign.as_ref(),
                                    active_drain.as_ref().expect("validated above"),
                                    resolution,
                                    &cut,
                                    is_paused,
                                    deadline,
                                    &execution,
                                )
                                .await;
                                if result.is_ok() {
                                    active_drain = None;
                                    drain_paused.clear();
                                }
                                let _ = reply.send(result);
                                continue;
                            }
                        }
                    }
                }

                // Hold the complete live Kafka assignment for the full source cut. Assignment
                // reconciliation can replace inputs while the cut is held, so new inputs are
                // paused before the reader is allowed to receive from them.
                if let Some(active) = active_drain.as_mut() {
                    if active.held_assignment_version != Some(last_assignment_version) {
                        let assignment = match consumer.assignment() {
                            Ok(assignment) => assignment,
                            Err(error) => {
                                warn!(%error, "Kafka drain could not inspect the live assignment");
                                continue;
                            }
                        };
                        active.held_inputs = match kafka_drain_partitions(&assignment) {
                            Ok(inputs) => inputs,
                            Err(error) => {
                                publish_reader_fault(
                                    &reader_fault,
                                    &data_ready,
                                    format!("invalid Kafka assignment while draining: {error}"),
                                );
                                return;
                            }
                        };
                        let current_set: std::collections::HashSet<(Arc<str>, i32)> = active
                            .held_inputs
                            .iter()
                            .map(|input| (Arc::clone(&input.topic), input.partition))
                            .collect();
                        drain_paused.retain(|input| current_set.contains(input));
                        active.held_assignment_version = Some(last_assignment_version);
                        active.hold_complete = false;
                    }
                    if !active.hold_complete {
                        let remaining: Vec<(Arc<str>, i32)> = active
                            .held_inputs
                            .iter()
                            .filter(|input| {
                                !drain_paused.contains(&(Arc::clone(&input.topic), input.partition))
                            })
                            .map(|input| (Arc::clone(&input.topic), input.partition))
                            .collect();
                        let to_pause = tpl_of(remaining.iter());
                        if to_pause.count() > 0 {
                            match consumer.pause(&to_pause) {
                                Ok(()) => {
                                    for element in to_pause.elements() {
                                        if element.error().is_ok() {
                                            drain_paused.insert((
                                                Arc::from(element.topic()),
                                                element.partition(),
                                            ));
                                        }
                                    }
                                    if let Err(error) =
                                        validate_kafka_partition_results("drain pause", &to_pause)
                                    {
                                        warn!(%error, "Kafka drain pause incomplete; will retry");
                                        continue;
                                    }
                                }
                                Err(error) => {
                                    warn!(%error, "Kafka drain pause failed; will retry");
                                    continue;
                                }
                            }
                        }
                        active.hold_complete = active.held_inputs.iter().all(|input| {
                            drain_paused.contains(&(Arc::clone(&input.topic), input.partition))
                        });
                    }
                    if !active.boundary_queued {
                        let cut_is_paused = active.inputs.iter().all(|input| {
                            drain_paused.contains(&(Arc::clone(&input.topic), input.partition))
                        });
                        if cut_is_paused {
                            let boundary = KafkaDrainBoundary {
                                round: active.request.round,
                                inputs: Arc::clone(&active.inputs),
                            };
                            channel_len.fetch_add(1, Ordering::Relaxed);
                            let sent = tokio::select! {
                                biased;
                                _ = reader_shutdown.changed() => false,
                                () = tokio::time::sleep_until(active.prepare_deadline) => false,
                                result = msg_tx.send(KafkaReaderItem::DrainBoundary(boundary)) => result.is_ok(),
                            };
                            if !sent {
                                channel_len.fetch_sub(1, Ordering::Relaxed);
                                if tokio::time::Instant::now() >= active.prepare_deadline {
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "Kafka drain boundary delivery exceeded its engine deadline",
                                    );
                                } else if !*reader_shutdown.borrow() {
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        "reader drain-boundary channel closed unexpectedly",
                                    );
                                }
                                break;
                            }
                            active.boundary_queued = true;
                            data_ready.notify_one();
                        }
                    }
                }

                // Newly-assigned partitions were paused by the callback. Seek to the
                // checkpointed offsets HERE, in the poll loop where the assignment is
                // fetch-ready (the in-callback seek fails `Local: Erroneous state`),
                // then resume — otherwise recovery resumes from auto.offset.reset.
                let cur_assign_gen = assign_generation.load(Ordering::Acquire);
                if cur_assign_gen != last_assign_gen {
                    let mut assigned: Vec<(String, i32)> = lock_or_recover(&rebalance_state)
                        .assigned_partitions()
                        .iter()
                        .cloned()
                        .collect();
                    if assigned.is_empty() {
                        // Vnode mode fires no rebalance callback, so use the live
                        // assignment to apply the cursor staged by start().
                        if let Ok(a) = consumer.assignment() {
                            assigned = a
                                .elements()
                                .iter()
                                .map(|e| (e.topic().to_string(), e.partition()))
                                .collect();
                        }
                    }
                    let deterministic_fallback = if deterministic_unrecorded.load(Ordering::Acquire)
                    {
                        deterministic_default
                    } else {
                        None
                    };
                    let seek_tpl = match assignment_seek_tpl(
                        &lock_or_recover(&reassign_snapshot),
                        &assigned,
                        if reassign_baselines.is_empty() {
                            None
                        } else {
                            Some(&reassign_baselines)
                        },
                        deterministic_fallback,
                        deterministic_unrecorded.load(Ordering::Acquire),
                    ) {
                        Ok(tpl) => tpl,
                        Err(e) => {
                            warn!(error = %e, "failed to build Kafka recovery assignment");
                            publish_reader_fault(
                                &reader_fault,
                                &data_ready,
                                format!("invalid recovery assignment: {e}"),
                            );
                            return;
                        }
                    };
                    let seek_ok = if seek_tpl.count() == 0 {
                        true // fresh start: nothing checkpointed to seek to
                    } else {
                        let seek_consumer = Arc::clone(&consumer);
                        match blocking_tasks
                            .run(move || {
                                seek_consumer
                                    .seek_partitions(seek_tpl, std::time::Duration::from_secs(5))
                            })
                            .await
                        {
                            Ok(Ok(result))
                                if assign_generation.load(Ordering::Acquire) == cur_assign_gen =>
                            {
                                let failed = result
                                    .elements()
                                    .iter()
                                    .filter(|e| e.error().is_err())
                                    .count();
                                if failed == 0 {
                                    info!(
                                        partition_count = result.count(),
                                        "seeked assigned partitions to checkpointed offsets"
                                    );
                                    true
                                } else {
                                    // Not all fetch-ready yet — retry next loop.
                                    debug!(failed, "assign-seek incomplete; will retry");
                                    false
                                }
                            }
                            Ok(Ok(_)) => false,
                            Ok(Err(e)) => {
                                debug!(error = %e, "assign-seek failed; will retry");
                                false
                            }
                            Err(error) => {
                                warn!(%error, "Kafka assign-seek worker failed");
                                if error == KafkaBlockingTaskError::Retired {
                                    return;
                                }
                                false
                            }
                        }
                    };
                    if seek_ok {
                        // Positioned — undo the callback's pause only when neither
                        // backpressure nor a global source cut is holding intake.
                        let mut resumed_ok = true;
                        if !is_paused && active_drain.is_none() {
                            if let Ok(assignment) = consumer.assignment() {
                                if let Err(e) = consumer.resume(&assignment) {
                                    warn!(error = %e, "post-seek resume failed; will retry");
                                    resumed_ok = false;
                                } else if let Err(error) = validate_kafka_partition_results(
                                    "post-seek resume",
                                    &assignment,
                                ) {
                                    warn!(%error, "post-seek resume incomplete; will retry");
                                    resumed_ok = false;
                                }
                            }
                        }
                        if resumed_ok {
                            last_assign_gen = cur_assign_gen;
                        } else {
                            continue;
                        }
                    }
                }

                // Backpressure: pause/resume Kafka partitions based on channel fill.
                let fill = if reader_channel_capacity > 0 {
                    let channel_len = u32::try_from(channel_len.load(Ordering::Acquire))
                        .map_or(f64::from(u32::MAX), f64::from);
                    channel_len / reader_channel_capacity_f64
                } else {
                    0.0
                };
                if active_drain.is_none() && fill >= pause_threshold && !is_paused {
                    if let Ok(assignment) = consumer.assignment() {
                        match consumer.pause(&assignment) {
                            Ok(()) => match validate_kafka_partition_results(
                                "backpressure pause",
                                &assignment,
                            ) {
                                Ok(()) => {
                                    is_paused = true;
                                    debug!("reader: paused Kafka partitions (fill={fill:.2})");
                                }
                                Err(error) => {
                                    warn!(%error, "reader backpressure pause incomplete");
                                }
                            },
                            Err(error) => {
                                warn!(%error, "reader backpressure pause failed");
                            }
                        }
                    }
                } else if active_drain.is_none() && fill <= resume_threshold && is_paused {
                    if let Ok(assignment) = consumer.assignment() {
                        let resumed = consumer
                            .resume(&assignment)
                            .map_err(|error| format!("Kafka backpressure resume failed: {error}"))
                            .and_then(|()| {
                                validate_kafka_partition_results("backpressure resume", &assignment)
                            });
                        if let Err(error) = resumed {
                            warn!(%error, "reader backpressure resume incomplete");
                            continue;
                        }
                        is_paused = false;
                        debug!("reader: resumed Kafka partitions (fill={fill:.2})");
                    }
                }

                // While paused, recv() yields nothing, so a long timeout would
                // gate the resume re-check at the top of the loop behind it.
                // Poll briefly when paused so resume fires promptly; block
                // longer when running so an idle topic doesn't spin.
                let recv_timeout = if is_paused {
                    std::time::Duration::from_millis(10)
                } else {
                    std::time::Duration::from_millis(200)
                };
                let drain_held = active_drain
                    .as_ref()
                    .is_some_and(|drain| drain.boundary_queued);
                let prepare_deadline = active_drain
                    .as_ref()
                    .filter(|drain| !drain.boundary_queued)
                    .map(|drain| drain.prepare_deadline);
                let resolution_deferred = deferred_drain_command.is_some();
                let msg_result = tokio::select! {
                    biased;
                    _ = reader_shutdown.changed() => break,
                    () = async {
                        match prepare_deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    } => {
                        publish_reader_fault(
                            &reader_fault,
                            &data_ready,
                            "Kafka source drain preparation exceeded its engine deadline",
                        );
                        return;
                    },
                    command = async {
                        match reader_drain_rx.as_mut() {
                            Some(receiver) => receiver.recv().await,
                            None => std::future::pending().await,
                        }
                    }, if !resolution_deferred => {
                        deferred_drain_command = command;
                        if deferred_drain_command.is_none() {
                            reader_drain_rx = None;
                        }
                        continue;
                    },
                    () = tokio::time::sleep(std::time::Duration::from_millis(10)), if drain_held => continue,
                    msg = tokio::time::timeout(recv_timeout, consumer.recv()), if !drain_held => match msg {
                        Ok(result) => result,
                        Err(_timeout) => continue,
                    },
                };
                match msg_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload() {
                            let topic = msg.topic();
                            if &*cached_topic != topic {
                                if vnode_reassign.is_some() {
                                    let Some((canonical_topic, routes)) =
                                        vnode_partition_routes.get_key_value(topic)
                                    else {
                                        warn!(
                                            topic,
                                            "Kafka reader received a topic outside its activated vnode inventory"
                                        );
                                        publish_reader_fault(
                                            &reader_fault,
                                            &data_ready,
                                            "payload topic is outside the activated inventory",
                                        );
                                        return;
                                    };
                                    cached_topic = Arc::clone(canonical_topic);
                                    cached_topic_routes = Some(Arc::clone(routes));
                                } else {
                                    cached_topic = Arc::from(topic);
                                    cached_topic_routes = None;
                                }
                            }
                            let partition_vnode = match cached_partition_vnode(
                                cached_topic_routes.as_deref(),
                                msg.partition(),
                            ) {
                                Ok(vnode) => vnode,
                                Err(error) => {
                                    warn!(
                                        topic,
                                        partition = msg.partition(),
                                        %error,
                                        "Kafka reader rejected a payload outside its activated vnode inventory"
                                    );
                                    publish_reader_fault(
                                        &reader_fault,
                                        &data_ready,
                                        format!("payload route is outside the activated inventory: {error}"),
                                    );
                                    return;
                                }
                            };
                            let timestamp_ms = match msg.timestamp() {
                                rdkafka::Timestamp::CreateTime(ts)
                                | rdkafka::Timestamp::LogAppendTime(ts) => Some(ts),
                                rdkafka::Timestamp::NotAvailable => None,
                            };
                            let headers_json = if capture_headers {
                                use rdkafka::message::Headers;
                                msg.headers().and_then(|hdrs| {
                                    let pairs: Vec<(String, serde_json::Value)> = (0..hdrs.count())
                                        .map(|i| {
                                            let h = hdrs.get(i);
                                            let val = match h.value {
                                                Some(v) => serde_json::Value::String(
                                                    String::from_utf8_lossy(v).into_owned(),
                                                ),
                                                None => serde_json::Value::Null,
                                            };
                                            (h.key.to_string(), val)
                                        })
                                        .collect();
                                    serde_json::to_string(&pairs).ok()
                                })
                            } else {
                                None
                            };
                            let kp = KafkaPayload {
                                data: payload.to_vec(),
                                topic: Arc::clone(&cached_topic),
                                partition: msg.partition(),
                                partition_vnode,
                                offset: msg.offset(),
                                timestamp_ms,
                                headers_json,
                            };
                            let item = KafkaReaderItem::Payload(kp);
                            match msg_tx.try_send(item) {
                                Ok(()) => {
                                    channel_len.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(crossfire::TrySendError::Full(item)) => {
                                    if !is_paused {
                                        if let Ok(assignment) = consumer.assignment() {
                                            let paused = consumer
                                                .pause(&assignment)
                                                .map_err(|error| {
                                                    format!(
                                                        "Kafka full-channel pause failed: {error}"
                                                    )
                                                })
                                                .and_then(|()| {
                                                    validate_kafka_partition_results(
                                                        "full-channel pause",
                                                        &assignment,
                                                    )
                                                });
                                            match paused {
                                                Ok(()) => {
                                                    is_paused = true;
                                                    debug!(
                                                        "reader: paused partitions (channel full)"
                                                    );
                                                }
                                                Err(error) => {
                                                    warn!(%error, "reader full-channel pause incomplete");
                                                }
                                            }
                                        }
                                    }
                                    channel_len.fetch_add(1, Ordering::Relaxed);
                                    let send_ok = tokio::select! {
                                        biased;
                                        _ = reader_shutdown.changed() => false,
                                        result = msg_tx.send(item) => result.is_ok(),
                                    };
                                    if !send_ok {
                                        channel_len.fetch_sub(1, Ordering::Relaxed);
                                        if !*reader_shutdown.borrow() {
                                            publish_reader_fault(
                                                &reader_fault,
                                                &data_ready,
                                                "reader output channel closed unexpectedly",
                                            );
                                        }
                                        break;
                                    }
                                }
                                Err(crossfire::TrySendError::Disconnected(_)) => {
                                    if !*reader_shutdown.borrow() {
                                        publish_reader_fault(
                                            &reader_fault,
                                            &data_ready,
                                            "reader output channel disconnected unexpectedly",
                                        );
                                    }
                                    break;
                                }
                            }
                            data_ready.notify_one();
                        }
                    }
                    Err(e) if kafka_reader_error_is_transient(&e) => {
                        debug!(error = %e, "Kafka consumer poll event");
                    }
                    Err(e) => {
                        warn!(error = %e, "Kafka consumer error");
                        publish_reader_fault(
                            &reader_fault,
                            &data_ready,
                            format!("terminal Kafka consumer error: {e}"),
                        );
                        break;
                    }
                }
            }

            // Reader does not commit or unsubscribe on shutdown. `close()`
            // owns the connector's single final unsubscribe.
            // Wake a parked runtime so an unexpected exit is observed as a
            // disconnected channel on the next poll instead of a silent stall.
            data_ready.notify_one();
        });

        self.msg_rx = Some(msg_rx);
        self.reader_handle = Some(reader_handle);
        self.reader_shutdown = Some(shutdown_tx);
        self.reader_drain_tx = reader_drain_tx;
    }

    fn capture_vnode_checkpoint(
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

    fn capture_non_vnode_checkpoint(&self) -> SourceCheckpoint {
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
            checkpoint.set_metadata(
                KAFKA_PARTITION_INVENTORY_METADATA,
                encode_partition_inventory(&self.manual_topic_partitions),
            );
            return checkpoint;
        }
        let assigned = lock_or_recover(&self.rebalance_state).assignment_snapshot();
        self.offsets.to_checkpoint_for_partitions(
            assigned
                .iter()
                .map(|(topic, partition)| (topic.as_str(), *partition)),
        )
    }

    fn try_capture_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        self.check_reader_health("capturing a checkpoint cursor")?;
        let Some((registry, _)) = &self.vnode_assignment else {
            return Ok(Some(self.capture_non_vnode_checkpoint()));
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
fn tpl_of<'a>(parts: impl Iterator<Item = &'a (Arc<str>, i32)>) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in parts {
        let _ = tpl.add_partition(topic.as_ref(), *partition);
    }
    tpl
}

/// Partition list for the initial `start()` assignment of a vnode-assigned source.
/// Owned partitions start at their checkpointed offset + 1, otherwise at
/// `default_offset`. Rotations rebind incrementally in the reader loop.
fn build_vnode_assignment_tpl(
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
        for partition in super::vnode_routing::owned_partitions_in_assignment(
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
fn acquired_numeric_position(
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
fn assignment_seek_tpl(
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
fn startup_default_offset(mode: &StartupMode) -> rdkafka::Offset {
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
fn deterministic_initial_offset(mode: &StartupMode, reset: OffsetReset) -> Option<rdkafka::Offset> {
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

fn encode_partition_inventory(inventory: &KafkaPartitionSet) -> String {
    let mut canonical: Vec<_> = inventory.iter().cloned().collect();
    canonical.sort_unstable();
    serde_json::to_string(&canonical).expect("Kafka partition inventory is serializable")
}

fn decode_partition_inventory(
    checkpoint: &SourceCheckpoint,
) -> Result<Option<KafkaPartitionSet>, ConnectorError> {
    let Some(encoded) = checkpoint.get_metadata(KAFKA_PARTITION_INVENTORY_METADATA) else {
        return Ok(None);
    };
    let canonical: Vec<(String, i32)> = serde_json::from_str(encoded).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid Kafka checkpoint partition inventory: {error}"
        ))
    })?;
    if canonical
        .iter()
        .any(|(topic, partition)| topic.is_empty() || *partition < 0)
        || canonical.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(ConnectorError::ConfigurationError(
            "invalid Kafka checkpoint partition inventory: entries must be canonical, unique, and non-negative"
                .into(),
        ));
    }
    Ok(Some(canonical.into_iter().collect()))
}

fn validate_resume_inventory(
    checkpoint: Option<&KafkaPartitionSet>,
    current: &KafkaPartitionSet,
) -> Result<(), ConnectorError> {
    let checkpoint = checkpoint.ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "Kafka engine-owned resume checkpoint has no partition inventory".into(),
        )
    })?;
    if checkpoint != current {
        return Err(ConnectorError::ConfigurationError(format!(
            "Kafka partition inventory changed across recovery: checkpoint={checkpoint:?}, current={current:?}"
        )));
    }
    Ok(())
}

fn partition_baseline_key(topic: &str, partition: i32) -> String {
    format!("{KAFKA_PARTITION_BASELINE_PREFIX}{topic}:{partition}")
}

fn decode_partition_baselines_from_offsets(
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

fn decode_partition_baselines(
    checkpoint: &SourceCheckpoint,
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    decode_partition_baselines_from_offsets(checkpoint.offsets())
}

fn attach_partition_baselines(
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

fn rotation_partition_baseline(
    baselines: &KafkaRotationBaselines,
    topic: &str,
    partition: i32,
) -> Option<i64> {
    baselines
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .copied()
}

fn rotation_baselines_len(baselines: &KafkaRotationBaselines) -> usize {
    baselines.values().map(std::collections::HashMap::len).sum()
}

fn update_rotation_baselines(
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

fn attach_rotation_baselines(
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

fn vnode_payload_is_current(
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

fn retire_accepted_rotation_baselines(
    baselines: &mut KafkaRotationBaselines,
    accepted_offsets: &[(Arc<str>, i32, i64)],
) {
    let mut accepted = std::collections::HashMap::<(&str, i32), i64>::new();
    for (topic, partition, offset) in accepted_offsets {
        accepted
            .entry((topic.as_ref(), *partition))
            .and_modify(|current| *current = (*current).max(*offset))
            .or_insert(*offset);
    }
    baselines.retain(|topic, partitions| {
        partitions.retain(|partition, next| {
            accepted
                .get(&(topic.as_ref(), *partition))
                .is_none_or(|offset| offset < next)
        });
        !partitions.is_empty()
    });
}

fn validate_partition_baselines(
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

fn validate_positions_not_expired(
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

fn kafka_reader_error_is_transient(error: &KafkaError) -> bool {
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

fn consumer_creation_error(error: &KafkaError) -> ConnectorError {
    ConnectorError::ConfigurationError(format!(
        "failed to create Kafka consumer from local configuration: {error}"
    ))
}

fn kafka_row_positions(
    source_name: &str,
    positions: &[(Arc<str>, i32, i64)],
    good_indices: Option<&[usize]>,
) -> Result<SourceRowPositions, ConnectorError> {
    if source_name.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "Kafka row positions require a canonical Laminar source name".into(),
        ));
    }
    let source_len = u32::try_from(source_name.len()).map_err(|_| {
        ConnectorError::Internal("Kafka source name exceeds the row-position encoding limit".into())
    })?;
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
        if *partition < 0 || *offset < 0 {
            return Err(ConnectorError::Internal(format!(
                "Kafka emitted invalid row position '{}-{partition}@{offset}'",
                topic.as_ref()
            )));
        }
        let topic_len = u32::try_from(topic.len()).map_err(|_| {
            ConnectorError::Internal("Kafka topic exceeds the row-position encoding limit".into())
        })?;
        encoded_partition.clear();
        encoded_partition.extend_from_slice(&source_len.to_be_bytes());
        encoded_partition.extend_from_slice(source_name.as_bytes());
        encoded_partition.extend_from_slice(&topic_len.to_be_bytes());
        encoded_partition.extend_from_slice(topic.as_bytes());
        encoded_partition.extend_from_slice(&partition.to_be_bytes());
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

fn kafka_debezium_mutations(
    records: &RecordBatch,
) -> Result<Option<Box<[SourceMutation]>>, ConnectorError> {
    let operation_index = records.schema().index_of("__op").map_err(|_| {
        ConnectorError::SchemaMismatch("Kafka Debezium batch omitted the __op column".into())
    })?;
    let operations = records
        .column(operation_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch("Kafka Debezium __op column must be Utf8".into())
        })?;
    decode_debezium_mutations(operations, records.num_rows())
}

fn decode_debezium_mutations(
    operations: &StringArray,
    row_count: usize,
) -> Result<Option<Box<[SourceMutation]>>, ConnectorError> {
    if operations.len() != row_count {
        return Err(ConnectorError::SchemaMismatch(format!(
            "Kafka Debezium operation count {} does not match decoded row count {}",
            operations.len(),
            row_count
        )));
    }

    let mut mutations: Option<Vec<SourceMutation>> = None;
    for (row, operation) in operations.iter().enumerate() {
        let operation = operation.ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!(
                "Kafka Debezium __op is null at decoded row {row}"
            ))
        })?;
        match operation {
            "c" | "u" | "r" => {
                if let Some(mutations) = mutations.as_mut() {
                    mutations.push(SourceMutation::Put);
                }
            }
            "d" => {
                let mutations = mutations.get_or_insert_with(|| {
                    let mut mutations = Vec::with_capacity(row_count);
                    mutations.resize(row, SourceMutation::Put);
                    mutations
                });
                mutations.push(SourceMutation::Tombstone);
            }
            unknown => {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "Kafka Debezium __op has unknown value '{unknown}' at decoded row {row}"
                )));
            }
        }
    }
    Ok(mutations.map(Vec::into_boxed_slice))
}

async fn fetch_explicit_topic_metadata(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    topics: Vec<String>,
) -> Result<Vec<(Arc<str>, i32)>, ConnectorError> {
    const METADATA_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
    let deadline = tokio::time::Instant::now() + METADATA_BUDGET;
    let task = blocking_tasks.run(move || {
        let blocking_deadline = std::time::Instant::now() + METADATA_BUDGET;
        let mut topic_meta = Vec::with_capacity(topics.len());
        for topic in topics {
            let remaining = blocking_deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(ConnectorError::Timeout(
                    u64::try_from(METADATA_BUDGET.as_millis()).unwrap_or(u64::MAX),
                ));
            }
            let metadata = consumer
                .fetch_metadata(Some(topic.as_str()), remaining)
                .map_err(|error| fetch_error(&topic, &error))?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|candidate| candidate.name() == topic.as_str())
                .ok_or_else(|| invalid_response(&topic, "metadata response omitted the topic"))?;
            if let Some(error) = topic_metadata.error() {
                return Err(topic_error(&topic, error.into()));
            }
            let partition_count = topic_metadata.partitions().len();
            if partition_count == 0 {
                return Err(invalid_response(&topic, "broker returned no partitions"));
            }
            topic_meta.push((
                Arc::from(topic.as_str()),
                i32::try_from(partition_count).unwrap_or(i32::MAX),
            ));
        }
        Ok(topic_meta)
    });
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(ConnectorError::Internal(format!(
            "Kafka metadata worker failed: {error}"
        ))),
        Err(_) => Err(ConnectorError::Timeout(
            u64::try_from(METADATA_BUDGET.as_millis()).unwrap_or(u64::MAX),
        )),
    }
}

async fn fetch_partition_low_watermarks(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    partitions: &KafkaPartitionSet,
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    let deadline = tokio::time::Instant::now() + KAFKA_POSITION_LOOKUP_BUDGET;
    let mut remaining = partitions.iter().cloned();
    let mut jobs = tokio::task::JoinSet::new();
    let mut baselines = KafkaPartitionBaselines::with_capacity(partitions.len());

    loop {
        while jobs.len() < KAFKA_POSITION_LOOKUP_CONCURRENCY {
            let Some((topic, partition)) = remaining.next() else {
                break;
            };
            jobs.spawn(fetch_partition_low_watermark(
                blocking_tasks.clone(),
                Arc::clone(&consumer),
                topic,
                partition,
                deadline,
            ));
        }
        let Some(result) = jobs.join_next().await else {
            break;
        };
        let ((topic, partition), low) = result.map_err(|error| {
            ConnectorError::Internal(format!("Kafka watermark worker failed: {error}"))
        })??;
        baselines.insert((topic, partition), low);
    }

    Ok(baselines)
}

async fn fetch_partition_low_watermark(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    topic: String,
    partition: i32,
    deadline: tokio::time::Instant,
) -> Result<((String, i32), i64), ConnectorError> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(ConnectorError::Timeout(
            u64::try_from(KAFKA_POSITION_LOOKUP_BUDGET.as_millis()).unwrap_or(u64::MAX),
        ));
    }
    let task_topic = topic.clone();
    let task = blocking_tasks.run(move || {
        consumer
            .fetch_watermarks(&task_topic, partition, remaining)
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "failed to fetch Kafka watermarks for '{task_topic}-{partition}': {error}"
                ))
            })
    });
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(Ok((low, _high)))) if (0..i64::MAX).contains(&low) => Ok(((topic, partition), low)),
        Ok(Ok(Ok((low, _)))) => Err(ConnectorError::ConnectionFailed(format!(
            "Kafka returned invalid low watermark {low} for '{topic}-{partition}'"
        ))),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(error)) => Err(ConnectorError::Internal(format!(
            "Kafka watermark lookup task failed: {error}"
        ))),
        Err(_) => Err(ConnectorError::Timeout(
            u64::try_from(KAFKA_POSITION_LOOKUP_BUDGET.as_millis()).unwrap_or(u64::MAX),
        )),
    }
}

async fn resolve_timestamp_offsets(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    requested: TopicPartitionList,
) -> Result<TopicPartitionList, ConnectorError> {
    const LOOKUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
    let deadline = tokio::time::Instant::now() + LOOKUP_BUDGET;
    let task = blocking_tasks.run(move || {
        consumer
            .offsets_for_times(requested, LOOKUP_BUDGET)
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "failed to resolve Kafka timestamp offsets: {error}"
                ))
            })
    });
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(ConnectorError::Internal(format!(
            "Kafka timestamp worker failed: {error}"
        ))),
        Err(_) => Err(ConnectorError::Timeout(
            u64::try_from(LOOKUP_BUDGET.as_millis()).unwrap_or(u64::MAX),
        )),
    }
}

impl KafkaSource {
    async fn decode_polled_payloads(
        &mut self,
    ) -> Result<(arrow_array::RecordBatch, Option<Vec<usize>>), ConnectorError> {
        // Resolve Avro schemas from Schema Registry before deserialization.
        // Also detect schema evolution when new schema IDs appear.
        if let Some(avro_deser) = self
            .deserializer
            .as_any_mut()
            .and_then(|any| any.downcast_mut::<AvroDeserializer>())
        {
            let mut new_schema_ids = Vec::new();
            for &(start, len) in &self.poll_payload_offsets {
                if let Some(schema_id) = AvroDeserializer::extract_confluent_id(
                    &self.poll_payload_buf[start..start + len],
                ) {
                    let is_new = avro_deser
                        .ensure_schema_registered(schema_id)
                        .await
                        .map_err(|error| {
                            terminalize_guaranteed_poll_error(
                                self.delivery,
                                &mut self.state,
                                &self.metrics,
                                self.reader_shutdown.as_ref(),
                                error,
                            )
                        })?;
                    if is_new {
                        new_schema_ids.push(schema_id);
                    }
                }
            }

            // Detect schema evolution by diffing successive writer schemas.
            if !new_schema_ids.is_empty()
                && self.config.schema_evolution_strategy != SchemaEvolutionStrategy::Ignore
            {
                if let Some(ref sr) = self.schema_registry {
                    let compat = self
                        .config
                        .schema_compatibility
                        .map_or(CompatibilityMode::Backward, CompatibilityMode::from);
                    let evolver = SchemaEvolution::new(compat);

                    for id in new_schema_ids {
                        let cached = sr.resolve_confluent_id(id).await.map_err(|error| {
                            terminalize_guaranteed_poll_error(
                                self.delivery,
                                &mut self.state,
                                &self.metrics,
                                self.reader_shutdown.as_ref(),
                                error,
                            )
                        })?;

                        let Some(ref prev) = self.last_avro_schema else {
                            // First schema — establish baseline, nothing to diff.
                            info!(schema_id = id, "initial Avro schema registered");
                            self.last_avro_schema = Some(Arc::clone(&cached.arrow_schema));
                            continue;
                        };

                        let changes = evolver.diff_schemas(prev, &cached.arrow_schema);
                        self.last_avro_schema = Some(Arc::clone(&cached.arrow_schema));

                        if changes.is_empty() {
                            info!(
                                schema_id = id,
                                "new Avro schema ID registered, no field changes"
                            );
                            continue;
                        }
                        let verdict = evolver.evaluate_evolution(&changes);
                        match &verdict {
                            EvolutionVerdict::Compatible => {
                                info!(schema_id = id, ?changes, "schema evolved (compatible)");
                            }
                            EvolutionVerdict::RequiresMigration => {
                                warn!(
                                    schema_id = id,
                                    ?changes,
                                    "schema evolved (requires migration)"
                                );
                            }
                            EvolutionVerdict::Incompatible(reason) => {
                                if self.config.schema_evolution_strategy
                                    == SchemaEvolutionStrategy::Reject
                                {
                                    let error = ConnectorError::SchemaMismatch(format!(
                                        "incompatible schema evolution for ID {id}: {reason}"
                                    ));
                                    return Err(terminalize_guaranteed_poll_error(
                                        self.delivery,
                                        &mut self.state,
                                        &self.metrics,
                                        self.reader_shutdown.as_ref(),
                                        error,
                                    ));
                                }
                                warn!(
                                    schema_id = id, %reason, ?changes,
                                    "incompatible schema evolution detected"
                                );
                            }
                        }
                    }
                }
            }
        }

        let refs: Vec<&[u8]> = self
            .poll_payload_offsets
            .iter()
            .map(|&(start, len)| &self.poll_payload_buf[start..start + len])
            .collect();

        // Try batch deserialization first (fast path). If it fails, fall back
        // to per-record deserialization to isolate poison pills.
        let (batch, good_indices) = match self.deserializer.deserialize_batch(&refs, &self.schema) {
            Ok(batch) => (batch, None),
            Err(batch_err) if self.delivery != DeliveryGuarantee::BestEffort => {
                // Without a checkpoint-coupled dead-letter path, skipping even one input would
                // let a later checkpoint seal a cursor beyond data that was never emitted. Stop
                // this connector generation so recovery must restart from its durable cursor.
                return Err(terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    ConnectorError::Serde(batch_err),
                ));
            }
            Err(batch_err) => {
                // Best-effort-only fallback: deserialize one at a time, collect successful
                // batches directly (avoids double-deserialization).
                // Track indices of successful records so metadata vectors can
                // be filtered to match the reduced row count.
                let mut good_batches = Vec::with_capacity(refs.len());
                let mut good_idx = Vec::with_capacity(refs.len());
                let mut error_count = 0usize;
                for (i, r) in refs.iter().enumerate() {
                    match self
                        .deserializer
                        .deserialize_batch(std::slice::from_ref(r), &self.schema)
                    {
                        Ok(batch) => {
                            good_batches.push(batch);
                            good_idx.push(i);
                        }
                        Err(e) => {
                            error_count += 1;
                            self.metrics.record_error();
                            warn!(error = %e, "skipping poison pill record");
                        }
                    }
                }
                if good_batches.is_empty() {
                    return Err(ConnectorError::Serde(batch_err));
                }
                // Escalate if the error rate exceeds the configured threshold.
                if error_count > 0 {
                    let error_count = u32::try_from(error_count).map_err(|_| {
                        ConnectorError::Internal(
                            "Kafka deserialization error count exceeds u32".into(),
                        )
                    })?;
                    let record_count = u32::try_from(refs.len()).map_err(|_| {
                        ConnectorError::Internal("Kafka batch record count exceeds u32".into())
                    })?;
                    let error_rate = f64::from(error_count) / f64::from(record_count);
                    if error_rate > self.config.max_deser_error_rate {
                        return Err(ConnectorError::Serde(batch_err));
                    }
                    warn!(
                        skipped = error_count,
                        total = refs.len(),
                        error_rate = %format_args!("{error_rate:.1}"),
                        "deserialized batch with poison pill isolation"
                    );
                }
                let concat_schema = good_batches[0].schema();
                let batch = arrow_select::concat::concat_batches(&concat_schema, &good_batches)
                    .map_err(|e| {
                        ConnectorError::Internal(format!("failed to concat batches: {e}"))
                    })?;
                (batch, Some(good_idx))
            }
        };

        // Kafka source formats map one broker message to one row. A short successful decode is a
        // silent drop unless it is rejected before the message offsets become checkpointable.
        let expected_rows = good_indices.as_ref().map_or(refs.len(), Vec::len);
        if batch.num_rows() != expected_rows {
            let error = ConnectorError::Serde(SerdeError::RecordCountMismatch {
                expected: expected_rows,
                got: batch.num_rows(),
            });
            return Err(terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            ));
        }

        Ok((batch, good_indices))
    }
}

impl KafkaSource {
    fn append_metadata_columns(
        &mut self,
        batch: arrow_array::RecordBatch,
        good_indices: Option<&[usize]>,
        include_metadata: bool,
        include_headers: bool,
    ) -> Result<arrow_array::RecordBatch, ConnectorError> {
        // If poison pill fallback filtered records, also filter metadata
        // vectors so their lengths match the deserialized batch row count.
        if let Some(idx) = good_indices {
            if include_metadata {
                self.poll_meta_partitions =
                    idx.iter().map(|&i| self.poll_meta_partitions[i]).collect();
                self.poll_meta_offsets = idx.iter().map(|&i| self.poll_meta_offsets[i]).collect();
                self.poll_meta_timestamps =
                    idx.iter().map(|&i| self.poll_meta_timestamps[i]).collect();
            }
            if include_headers {
                self.poll_meta_headers = idx
                    .iter()
                    .map(|&i| std::mem::take(&mut self.poll_meta_headers[i]))
                    .collect();
            }
        }

        // Append metadata columns if configured.
        let needs_meta = include_metadata && !self.poll_meta_partitions.is_empty();
        let needs_headers = include_headers && !self.poll_meta_headers.is_empty();
        let batch = if needs_meta || needs_headers {
            use arrow_schema::{DataType, Field};

            let mut fields = batch.schema().fields().to_vec();
            let mut columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();

            if needs_meta {
                use arrow_array::{Int32Array, Int64Array, TimestampMillisecondArray};
                use arrow_schema::TimeUnit;
                fields.push(Arc::new(Field::new("_partition", DataType::Int32, false)));
                columns.push(Arc::new(Int32Array::from(std::mem::take(
                    &mut self.poll_meta_partitions,
                ))));
                fields.push(Arc::new(Field::new("_offset", DataType::Int64, false)));
                columns.push(Arc::new(Int64Array::from(std::mem::take(
                    &mut self.poll_meta_offsets,
                ))));
                fields.push(Arc::new(Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )));
                columns.push(Arc::new(TimestampMillisecondArray::from(std::mem::take(
                    &mut self.poll_meta_timestamps,
                ))));
            }
            if needs_headers {
                fields.push(Arc::new(Field::new("_headers", DataType::Utf8, true)));
                columns.push(Arc::new(arrow_array::StringArray::from(std::mem::take(
                    &mut self.poll_meta_headers,
                ))));
            }

            let meta_schema = Arc::new(arrow_schema::Schema::new(fields));
            arrow_array::RecordBatch::try_new(meta_schema, columns).map_err(|e| {
                terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    ConnectorError::Internal(format!("failed to append metadata columns: {e}")),
                )
            })?
        } else {
            batch
        };

        Ok(batch)
    }
}
impl KafkaSource {
    fn prepare_start(&mut self, request: SourceStart) -> Result<KafkaStartPlan, ConnectorError> {
        let (config, position, delivery) = request.into_parts();

        // Resolve and validate the complete cursor policy before creating a
        // consumer. `StreamConsumer` construction starts librdkafka background
        // activity, so no malformed durable position may reach that boundary.
        let kafka_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            KafkaSourceConfig::from_config(&config)?
        };
        let (installed_offsets, resume_attempt, is_resume, resume_inventory, resume_baselines) =
            match position {
                SourcePosition::Initial => (
                    OffsetTracker::new(),
                    None,
                    false,
                    None,
                    KafkaPartitionBaselines::new(),
                ),
                SourcePosition::Resume {
                    attempt,
                    checkpoint,
                } => {
                    let inventory = decode_partition_inventory(&checkpoint)?;
                    let baselines = decode_partition_baselines(&checkpoint)?;
                    (
                        OffsetTracker::try_from_checkpoint(&checkpoint)?,
                        Some(attempt),
                        true,
                        inventory,
                        baselines,
                    )
                }
            };
        if (self.vnode_assignment.is_some() || delivery != DeliveryGuarantee::BestEffort)
            && matches!(&kafka_config.subscription, TopicSubscription::Pattern(_))
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka topic patterns are unsupported with engine-owned assignment; declare the \
                 exact topic inventory so ownership and checkpoint cuts stay stable"
                    .into(),
            ));
        }
        if matches!(
            &kafka_config.startup_mode,
            StartupMode::SpecificOffsets(_) | StartupMode::Timestamp(_)
        ) {
            if matches!(&kafka_config.subscription, TopicSubscription::Pattern(_)) {
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
        if let StartupMode::SpecificOffsets(offsets) = &kafka_config.startup_mode {
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
            && matches!(&kafka_config.startup_mode, StartupMode::GroupOffsets)
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka guaranteed delivery cannot use broker group offsets as its initial recovery \
                 authority; another group member can advance that cursor before LaminarDB seals \
                 it. Use earliest or explicit specific offsets"
                    .into(),
            ));
        }
        if delivery != DeliveryGuarantee::BestEffort
            && matches!(
                &kafka_config.startup_mode,
                StartupMode::Latest | StartupMode::Timestamp(_)
            )
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka guaranteed delivery requires a stable unrecorded-partition start; latest \
                 and timestamp-with-no-match can move forward across recovery. Use earliest or \
                 explicit specific offsets until checkpointed partition baselines are available"
                    .into(),
            ));
        }

        // Guaranteed delivery never joins Kafka's rebalance protocol. Cluster mode assigns the
        // vnode subset; embedded/single-node mode manually assigns the full explicit inventory. A
        // topology change is picked up only by a fresh, checkpoint-positioned source instance.

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
        self.state = ConnectorState::Initializing;
        self.config = kafka_config.clone();
        self.delivery = delivery;
        self.offsets = installed_offsets;
        self.manual_topic_partitions.clear();
        self.manual_partition_baselines.clear();
        *lock_or_recover(&self.assignment_publication) =
            Arc::new(KafkaAssignmentPublication::default());
        self.rotation_partition_baseline_count
            .store(0, Ordering::Release);
        self.applied_rotation_baseline_version = None;
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

        // Re-select deserializer (factory defaults to JSON).
        if let Some(sr_client) = Self::build_sr_client(&kafka_config)? {
            let sr = Arc::new(sr_client);
            self.schema_registry = Some(Arc::clone(&sr));
            self.deserializer = if kafka_config.format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(sr))
            } else {
                select_deserializer(kafka_config.format)
            };
        } else if let Some(ref sr) = self.schema_registry {
            // Preserve SR client injected via with_schema_registry().
            self.deserializer = if kafka_config.format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(Arc::clone(sr)))
            } else {
                select_deserializer(kafka_config.format)
            };
        } else {
            self.deserializer = select_deserializer(kafka_config.format);
        }

        // New deserializer has empty known_ids; reset evolution baseline to match.
        self.last_avro_schema = None;

        // Override schema from SQL DDL if provided.
        if let Some(schema) = config.arrow_schema() {
            info!(
                fields = schema.fields().len(),
                "using SQL-defined schema for deserialization"
            );
            self.schema = schema;
        }

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
            resume_inventory,
            resume_baselines,
        })
    }
}
impl KafkaSource {
    async fn assign_vnode_partitions(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        is_resume: bool,
        resume_baselines: &KafkaPartitionBaselines,
    ) -> Result<bool, ConnectorError> {
        // Engine-controlled partition assignment (cluster mode): map the
        // canonical source/topic/partition identity to a vnode and manually
        // `assign()` only those this node owns. Manual assign bypasses the broker callbacks,
        // so partitions are positioned here directly (checkpointed offset, else
        // the startup default). The reader loop re-binds on assignment rotation.
        //
        // Reset stale metadata so a re-`start()` that falls back to subscribe
        // doesn't leave `checkpoint()` filtering by a prior run's vnode ownership.
        self.vnode_partition_routes.clear();
        let vnode = self
            .vnode_assignment
            .as_ref()
            .map(|(r, s)| (Arc::clone(r), *s));
        let vnode_assigned = if let Some((registry, self_id)) = vnode {
            if let TopicSubscription::Topics(topics) = &config.subscription {
                let topic_meta = fetch_explicit_topic_metadata(
                    self.blocking_tasks.clone(),
                    Arc::clone(consumer),
                    topics.clone(),
                )
                .await?;
                let partition_routes = kafka_partition_routes(
                    self.source_name.as_ref(),
                    registry.vnode_count(),
                    &topic_meta,
                )?;
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
                    .find(|entry| {
                        !all_partitions.contains(&(entry.topic().to_string(), entry.partition()))
                    })
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
                // Pin the final ownership publication only across synchronous librdkafka calls.
                // Metadata and watermark I/O above must not delay assignment writers.
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
                        &topic_meta,
                        &self.offsets,
                        &self.manual_partition_baselines,
                        default_offset,
                    )?
                };
                let owned_partitions = Arc::new(
                    kafka_partition_set(&tpl).map_err(ConnectorError::ConfigurationError)?,
                );
                // Incremental from empty so rebinds can stay incremental — librdkafka
                // rejects mixing a full assign() with incremental_assign/unassign.
                if tpl.count() > 0 {
                    consumer.incremental_assign(&tpl).map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "vnode partition assign failed: {e}"
                        ))
                    })?;
                }
                validate_kafka_partition_results("initial incremental assign", &tpl)
                    .map_err(ConnectorError::ConnectionFailed)?;
                let active = consumer.assignment().map_err(|error| {
                    ConnectorError::ConnectionFailed(format!(
                        "failed to inspect initial vnode assignment: {error}"
                    ))
                })?;
                let active =
                    kafka_partition_set(&active).map_err(ConnectorError::ConnectionFailed)?;
                validate_kafka_assignment(&owned_partitions, &active)
                    .map_err(ConnectorError::ConnectionFailed)?;
                self.vnode_partition_routes = partition_routes;
                *lock_or_recover(&self.assignment_publication) =
                    Arc::new(KafkaAssignmentPublication::new(
                        assignment_version,
                        Arc::clone(&owned_partitions),
                        KafkaRotationBaselines::new(),
                    ));
                self.reconciled_assignment_version
                    .store(assignment_version, Ordering::Release);
                drop(published);
                if boot_unassigned {
                    info!(
                        "Kafka source started fenced with no partitions until durable vnode adoption"
                    );
                } else {
                    info!(
                        owned_partitions = owned_partitions.len(),
                        "Kafka source assigned vnode-owned partitions (engine-controlled)"
                    );
                }
                true
            } else {
                return Err(ConnectorError::ConfigurationError(
                    "Kafka vnode assignment requires an explicit topic inventory".into(),
                ));
            }
        } else {
            false
        };

        Ok(vnode_assigned)
    }
}
impl KafkaSource {
    async fn assign_local_guaranteed_partitions(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
        config: &KafkaSourceConfig,
        delivery: DeliveryGuarantee,
        vnode_assigned: bool,
        is_resume: bool,
        resume_inventory: Option<&KafkaPartitionSet>,
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
                validate_resume_inventory(resume_inventory, &assigned_set)?;
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
    async fn prefetch_schema_registry(
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
#[async_trait]
impl SourceConnector for KafkaSource {
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
        .with_row_positions(SourceRowPositionCapability::Deterministic)
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
            "Kafka source: engine-controlled partition→vnode assignment enabled"
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

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
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
            resume_inventory,
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
                resume_inventory.as_ref(),
                &resume_baselines,
            )
            .await?;
        // Group modes subscribe only after the engine position has been staged.
        // Specific/timestamp modes use a fully positioned manual assignment, so
        // they never briefly join the group at an unrelated broker cursor.
        if !vnode_assigned && !local_guaranteed_assignment {
            match &kafka_config.startup_mode {
                StartupMode::GroupOffsets | StartupMode::Earliest | StartupMode::Latest => {
                    match &kafka_config.subscription {
                        TopicSubscription::Topics(topics) => {
                            let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
                            consumer.subscribe(&topic_refs).map_err(|e| {
                                ConnectorError::ConnectionFailed(format!(
                                    "failed to subscribe: {e}"
                                ))
                            })?;
                        }
                        TopicSubscription::Pattern(pattern) => {
                            let regex_pattern = if pattern.starts_with('^') {
                                pattern.clone()
                            } else {
                                format!("^{pattern}")
                            };
                            consumer.subscribe(&[&regex_pattern]).map_err(|e| {
                                ConnectorError::ConnectionFailed(format!(
                                    "failed to subscribe to pattern: {e}"
                                ))
                            })?;
                        }
                    }
                }
                StartupMode::SpecificOffsets(offsets) => {
                    let TopicSubscription::Topics(topics) = &kafka_config.subscription else {
                        return Err(ConnectorError::ConfigurationError(
                            "Kafka specific-offset startup requires an explicit topic inventory"
                                .into(),
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
                        validate_resume_inventory(resume_inventory.as_ref(), &assigned_set)?;
                        validate_partition_baselines(&resume_baselines, &assigned_set)?;
                        if resume_baselines != configured_baselines {
                            return Err(ConnectorError::ConfigurationError(
                                "Kafka specific-offset configuration changed across recovery"
                                    .into(),
                            ));
                        }
                        resume_baselines.clone()
                    } else {
                        configured_baselines
                    };
                    let low_watermarks = fetch_partition_low_watermarks(
                        self.blocking_tasks.clone(),
                        Arc::clone(&consumer),
                        &assigned_set,
                    )
                    .await?;
                    validate_positions_not_expired(
                        &self.offsets,
                        &baselines,
                        &low_watermarks,
                        &assigned_set,
                    )?;
                    let assignment = assignment_seek_tpl(
                        &self.offsets,
                        &assigned,
                        Some(&baselines),
                        None,
                        true,
                    )?;
                    consumer.assign(&assignment).map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "failed to assign specific offsets: {e}"
                        ))
                    })?;
                    self.manual_topic_partitions = assigned_set;
                    self.manual_partition_baselines = baselines;
                    info!(
                        partition_count = assignment.count(),
                        "assigned consumer to exact checkpoint/specific offsets"
                    );
                }
                StartupMode::Timestamp(ts_ms) => {
                    let TopicSubscription::Topics(topics) = &kafka_config.subscription else {
                        return Err(ConnectorError::ConfigurationError(
                            "Kafka timestamp startup requires an explicit topic inventory".into(),
                        ));
                    };
                    let topic_meta = fetch_explicit_topic_metadata(
                        self.blocking_tasks.clone(),
                        Arc::clone(&consumer),
                        topics.clone(),
                    )
                    .await?;
                    let mut tpl = rdkafka::TopicPartitionList::new();
                    for (topic, partition_count) in &topic_meta {
                        for partition in 0..*partition_count {
                            tpl.add_partition_offset(
                                topic,
                                partition,
                                rdkafka::Offset::Offset(*ts_ms),
                            )
                            .map_err(|error| {
                                ConnectorError::Internal(format!(
                                    "failed to build timestamp lookup for '{topic}-{partition}': {error}"
                                ))
                            })?;
                        }
                    }
                    if tpl.count() == 0 {
                        return Err(ConnectorError::ConfigurationError(
                            "Kafka timestamp startup discovered no partitions".into(),
                        ));
                    }
                    let resolved = resolve_timestamp_offsets(
                        self.blocking_tasks.clone(),
                        Arc::clone(&consumer),
                        tpl,
                    )
                    .await?;
                    let mut positioned = TopicPartitionList::new();
                    let mut restored = 0usize;
                    for elem in resolved.elements() {
                        if let Err(e) = elem.error() {
                            return Err(ConnectorError::ConnectionFailed(format!(
                                "timestamp lookup failed for '{}-{}': {e}",
                                elem.topic(),
                                elem.partition()
                            )));
                        }
                        let offset = if let Some(checkpointed) =
                            self.offsets.get(elem.topic(), elem.partition())
                        {
                            restored += 1;
                            rdkafka::Offset::Offset(checkpointed.checked_add(1).ok_or_else(
                                || {
                                    ConnectorError::ConfigurationError(format!(
                                        "Kafka checkpoint offset overflow for '{}-{}'",
                                        elem.topic(),
                                        elem.partition()
                                    ))
                                },
                            )?)
                        } else if elem.offset() == rdkafka::Offset::Invalid {
                            // No record at/after the timestamp is a deterministic
                            // start at the current end, not a broker group cursor.
                            rdkafka::Offset::End
                        } else {
                            elem.offset()
                        };
                        positioned
                            .add_partition_offset(elem.topic(), elem.partition(), offset)
                            .map_err(|e| {
                                ConnectorError::Internal(format!(
                                    "failed to build timestamp assignment for '{}-{}': {e}",
                                    elem.topic(),
                                    elem.partition()
                                ))
                            })?;
                    }
                    if restored != self.offsets.partition_count() {
                        return Err(ConnectorError::ConfigurationError(
                            "Kafka resume checkpoint references a partition absent from timestamp metadata"
                                .into(),
                        ));
                    }
                    consumer.assign(&positioned).map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "failed to assign timestamp/checkpoint offsets: {e}"
                        ))
                    })?;
                    self.manual_topic_partitions = positioned
                        .elements()
                        .iter()
                        .map(|entry| (entry.topic().to_string(), entry.partition()))
                        .collect();
                    info!(
                        timestamp_ms = ts_ms,
                        partition_count = positioned.count(),
                        restored_partitions = restored,
                        "assigned consumer to exact checkpoint/timestamp offsets"
                    );
                }
            }
        } // end `if !vnode_assigned && !local_guaranteed_assignment`

        // Reader startup stays deferred until the first poll. Group
        // assignments are paused by the callback and explicitly seeked from
        // the position installed above before any record can enter the channel.

        self.prefetch_schema_registry(&kafka_config).await?;

        self.state = ConnectorState::Running;
        info!("Kafka source connector started successfully");
        Ok(())
    }

    async fn discover_schema(
        &mut self,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let cfg = crate::config::ConnectorConfig::with_properties("kafka", properties.clone());
        let kafka_config = KafkaSourceConfig::from_config(&cfg)?;
        if kafka_config.format != Format::Avro {
            return Ok(());
        }

        let topic = match &kafka_config.subscription {
            TopicSubscription::Topics(topics) => match topics.first() {
                Some(t) => {
                    if topics.len() > 1 {
                        warn!(topics = ?topics, chosen = %t,
                            "multi-topic source: using first topic's SR schema");
                    }
                    t.clone()
                }
                None => return Ok(()),
            },
            TopicSubscription::Pattern(pattern) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "topic.pattern '{pattern}' cannot auto-discover a schema; \
                     declare columns explicitly"
                )));
            }
        };

        let Some(sr_client) = Self::build_sr_client(&kafka_config)? else {
            return Ok(());
        };

        let subject = resolve_value_subject(
            kafka_config.schema_registry_subject_strategy,
            kafka_config.schema_registry_record_name.as_deref(),
            &topic,
        );
        let timeout = kafka_config.schema_registry_discovery_timeout;

        match tokio::time::timeout(timeout, sr_client.get_latest_schema(&subject)).await {
            Ok(Ok(cached)) => {
                self.metrics.record_sr_discovery_success();
                info!(%subject, schema_id = cached.id,
                    fields = cached.arrow_schema.fields().len(),
                    "discovered Avro schema from Schema Registry");
                self.schema = cached.arrow_schema;
                Ok(())
            }
            Ok(Err(e)) => {
                self.metrics.record_sr_discovery_failure();
                Err(e)
            }
            Err(_) => {
                self.metrics.record_sr_discovery_timeout();
                Err(ConnectorError::Timeout(
                    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                ))
            }
        }
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        // Lazily spawn the background reader task on first poll.
        self.ensure_reader_started();
        self.check_reader_health("polling source data")?;

        let limit = max_records.min(self.config.max_poll_records);

        // Reuse struct-level buffers — clear without freeing capacity. Clearing staged offsets
        // here also discards any left by a prior poll whose output failed to finalize.
        self.poll_payloads.clear();
        self.poll_payload_buf.clear();
        self.poll_payload_offsets.clear();
        self.poll_staged_offsets.clear();
        self.poll_meta_partitions.clear();
        self.poll_meta_offsets.clear();
        self.poll_meta_timestamps.clear();
        self.poll_meta_headers.clear();

        let mut total_bytes: u64 = 0;
        let include_metadata = self.config.include_metadata;
        let include_headers = self.config.include_headers;

        // Pin one ownership publication only while draining the non-awaiting
        // payload queue. An assignment writer waits for this short read-side
        // critical section, so one cut cannot mix ownership publications.
        let (drained_assignment_version, retires_rotation_baseline) = {
            let vnode_registry = self
                .vnode_assignment
                .as_ref()
                .map(|(registry, self_id)| (Arc::clone(registry), *self_id));
            let vnode_publication = vnode_registry
                .as_ref()
                .map(|(registry, self_id)| (registry.read_assignment(), *self_id));
            if let Some((published, _)) = vnode_publication.as_ref() {
                if self.reconciled_assignment_version.load(Ordering::Acquire) != published.version()
                {
                    return Ok(None);
                }
            }

            // A vnode acquired after rehydration may also have a stale local cursor from an earlier
            // ownership stint. Keep the durable handoff baseline authoritative until this instance
            // has actually accepted a record from the acquired assignment.
            let rotation_baselines = if self
                .rotation_partition_baseline_count
                .load(Ordering::Acquire)
                == 0
            {
                None
            } else {
                Some(Arc::clone(&lock_or_recover(&self.assignment_publication)))
            };
            if let Some(publication) = rotation_baselines.as_deref() {
                if self.applied_rotation_baseline_version != Some(publication.assignment_version) {
                    let mut snapshot = lock_or_recover(&self.offset_snapshot);
                    for (topic, partitions) in &publication.baselines {
                        for partition in partitions.keys() {
                            self.offsets.remove(topic, *partition);
                            snapshot.remove(topic, *partition);
                        }
                    }
                    self.applied_rotation_baseline_version = Some(publication.assignment_version);
                }
            }
            let vnode_ownership = vnode_publication
                .as_ref()
                .map(|(published, self_id)| (published.owners(), *self_id));

            let rx = self
                .msg_rx
                .as_mut()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "reader initialized".into(),
                    actual: "reader is None".into(),
                })?;

            while self.poll_payloads.len() < limit {
                match rx.try_recv() {
                    Ok(item) => {
                        self.channel_len.fetch_sub(1, Ordering::Release);
                        let kp = match item {
                            KafkaReaderItem::Payload(payload) => payload,
                            KafkaReaderItem::DrainBoundary(boundary) => {
                                let Some(active) = self.source_drain.as_mut() else {
                                    self.state = ConnectorState::Failed;
                                    return Err(ConnectorError::Internal(
                                        "Kafka reader emitted a drain boundary without an active round"
                                            .into(),
                                    ));
                                };
                                if boundary.round != active.request.round
                                    || active.boundary.is_some()
                                {
                                    self.state = ConnectorState::Failed;
                                    return Err(ConnectorError::Internal(
                                        "Kafka reader emitted a stale or duplicate drain boundary"
                                            .into(),
                                    ));
                                }
                                active.boundary = Some(boundary);
                                // Never consume a post-boundary payload in this poll. If payloads
                                // were collected first, they are returned and sent before the
                                // source task is allowed to publish Ready.
                                break;
                            }
                        };
                        let payload_is_current = vnode_payload_is_current(
                            vnode_ownership
                                .as_ref()
                                .map(|(assignment, self_id)| (*assignment, *self_id)),
                            kp.partition_vnode,
                            rotation_baselines.as_deref().and_then(|publication| {
                                rotation_partition_baseline(
                                    &publication.baselines,
                                    kp.topic.as_ref(),
                                    kp.partition,
                                )
                            }),
                            kp.offset,
                        )
                        .map_err(|error| {
                            terminalize_guaranteed_poll_error(
                                self.delivery,
                                &mut self.state,
                                &self.metrics,
                                self.reader_shutdown.as_ref(),
                                error,
                            )
                        })?;
                        if !payload_is_current {
                            debug!(
                                topic = kp.topic.as_ref(),
                                partition = kp.partition,
                                offset = kp.offset,
                                "discarded Kafka payload outside the current vnode handoff cut"
                            );
                            continue;
                        }
                        self.poll_payloads.push(kp);
                    }
                    Err(crossfire::TryRecvError::Empty) => break,
                    Err(crossfire::TryRecvError::Disconnected) => {
                        self.state = ConnectorState::Failed;
                        return Err(ConnectorError::Internal(
                            "Kafka reader task exited unexpectedly".into(),
                        ));
                    }
                }
            }
            let assignment_version = vnode_publication
                .as_ref()
                .map(|(published, _)| published.version());
            let retires_baseline = rotation_baselines.as_deref().is_some_and(|publication| {
                self.poll_payloads.iter().any(|payload| {
                    rotation_partition_baseline(
                        &publication.baselines,
                        payload.topic.as_ref(),
                        payload.partition,
                    )
                    .is_some_and(|next| payload.offset >= next)
                })
            });
            (assignment_version, retires_baseline)
        };
        // Schema Registry resolution and decode can await or consume substantial CPU. They do not
        // inspect ownership, so release the publication before either operation; otherwise a slow
        // registry request would stall assignment activation for the full network timeout.
        for kp in self.poll_payloads.drain(..) {
            total_bytes += kp.data.len() as u64;
            let start = self.poll_payload_buf.len();
            self.poll_payload_buf.extend_from_slice(&kp.data);
            self.poll_payload_offsets.push((start, kp.data.len()));

            // Stage the offset; it is folded into `self.offsets` only after the complete output
            // finalizes, so decode or metadata construction failure cannot advance past it.
            self.poll_staged_offsets
                .push((Arc::clone(&kp.topic), kp.partition, kp.offset));

            if include_metadata {
                self.poll_meta_partitions.push(kp.partition);
                self.poll_meta_offsets.push(kp.offset);
                self.poll_meta_timestamps.push(kp.timestamp_ms);
            }
            if include_headers {
                self.poll_meta_headers.push(kp.headers_json);
            }
        }

        // Sync rebalance counter → metrics (bridge from rdkafka background thread).
        let rebalance_events = self.rebalance_counter.swap(0, Ordering::Relaxed);
        for _ in 0..rebalance_events {
            self.metrics.record_rebalance();
        }

        // Lock-free revoke detection: check if a rebalance revoke happened
        // since the last poll cycle. If so, purge offsets for revoked partitions.
        let current_revoke_gen = self.revoke_generation.load(Ordering::Acquire);
        let had_revoke = current_revoke_gen != self.last_seen_revoke_gen;
        if had_revoke {
            self.last_seen_revoke_gen = current_revoke_gen;
            let assigned = lock_or_recover(&self.rebalance_state).assignment_snapshot();
            let before = self.offsets.partition_count();
            self.offsets.retain_assigned(&assigned);
            lock_or_recover(&self.offset_snapshot).retain_assigned(&assigned);
            let after = self.offsets.partition_count();
            if before != after {
                debug!(
                    before,
                    after, "purged revoked partition offsets after rebalance"
                );
            }
        }

        if self.poll_payload_offsets.is_empty() {
            return Ok(None);
        }

        let (batch, good_indices) = self.decode_polled_payloads().await?;
        let row_positions = kafka_row_positions(
            self.source_name.as_ref(),
            &self.poll_staged_offsets,
            good_indices.as_deref(),
        )
        .map_err(|error| {
            terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            )
        })?;

        let batch = self.append_metadata_columns(
            batch,
            good_indices.as_deref(),
            include_metadata,
            include_headers,
        )?;
        let mutations = if self.config.format == Format::Debezium {
            kafka_debezium_mutations(&batch).map_err(|error| {
                terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    error,
                )
            })?
        } else {
            None
        };
        // Construct the complete output before publishing its cursor. In particular,
        // metadata/header column validation above is fallible and must not retire a rotation
        // baseline or advance the recovery position for a batch that cannot be returned.
        let output = SourceBatch::positioned(batch, row_positions).map_err(|error| {
            terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            )
        })?;
        let output = if let Some(mutations) = mutations {
            output.with_mutations(mutations).map_err(|error| {
                terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    error,
                )
            })?
        } else {
            output
        };
        let num_rows = output.num_rows();

        if !self.poll_staged_offsets.is_empty() {
            {
                let mut snapshot = lock_or_recover(&self.offset_snapshot);
                for (topic, partition, offset) in &self.poll_staged_offsets {
                    self.offsets.update_arc(topic, *partition, *offset);
                    snapshot.update_arc(topic, *partition, *offset);
                }
            }
            if retires_rotation_baseline {
                if let Some(version) = drained_assignment_version {
                    let mut published = lock_or_recover(&self.assignment_publication);
                    if published.assignment_version == version {
                        let publication = Arc::make_mut(&mut published);
                        retire_accepted_rotation_baselines(
                            &mut publication.baselines,
                            &self.poll_staged_offsets,
                        );
                        let count = rotation_baselines_len(&publication.baselines);
                        self.rotation_partition_baseline_count
                            .store(count, Ordering::Release);
                    }
                }
            }
            self.poll_staged_offsets.clear();
        }

        self.metrics.record_poll(num_rows as u64, total_bytes);

        debug!(
            records = num_rows,
            bytes = total_bytes,
            "polled batch from Kafka"
        );

        Ok(Some(output))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        // Wake assignment and poll work before joining. Any advisory async commit cleanup remains
        // librdkafka-owned and is not allowed to extend the engine's source-shutdown deadline.
        if let Some(ref consumer) = self.consumer {
            consumer.unsubscribe();
        }
        let deadline = tokio::time::Instant::now() + KAFKA_BACKGROUND_CLOSE_BUDGET;
        join_background_task(&mut self.reader_handle, deadline, "reader").await;
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

impl Drop for KafkaSource {
    fn drop(&mut self) {
        if let Some(shutdown) = self.reader_shutdown.take() {
            let _ = shutdown.send(true);
        }
        if let Some(consumer) = self.consumer.as_ref() {
            consumer.unsubscribe();
        }
        self.blocking_tasks.retire();
        if let Some(handle) = self.reader_handle.take() {
            ensure_background_task_reaper(handle, &self.task_owner, "reader");
        }
        self.blocking_tasks.ensure_reaper();
        if let Some(consumer) = self.consumer.take() {
            self.blocking_tasks.spawn_final_drop(consumer, "consumer");
        }
    }
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("state", &self.state)
            .field("subscription", &self.config.subscription)
            .field("group_id", &self.config.group_id)
            .field("format", &self.config.format)
            .field("delivery", &self.delivery)
            .field("partitions", &self.offsets.partition_count())
            .finish_non_exhaustive()
    }
}

/// Warn if the CREATE-SOURCE catalog schema has drifted from the live
/// Schema Registry schema. Empty `declared` means nothing was declared.
fn log_schema_drift(declared: &arrow_schema::Schema, live: &arrow_schema::Schema, subject: &str) {
    if declared.fields().is_empty() || declared.fields() == live.fields() {
        return;
    }
    let decl: BTreeSet<&str> = declared
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let lv: BTreeSet<&str> = live.fields().iter().map(|f| f.name().as_str()).collect();
    warn!(
        %subject,
        missing_in_sr = ?decl.difference(&lv).collect::<Vec<_>>(),
        added_in_sr = ?lv.difference(&decl).collect::<Vec<_>>(),
        "schema drift: re-apply CREATE SOURCE DDL to pick up the current SR schema"
    );
}

fn select_deserializer(format: Format) -> Box<dyn RecordDeserializer> {
    match format {
        Format::Avro => Box::new(AvroDeserializer::new()),
        other => serde::create_deserializer(other).unwrap_or_else(|_| {
            warn!(format = %other, "unsupported format, falling back to JSON");
            Box::new(serde::json::JsonDeserializer::new())
        }),
    }
}

#[cfg(test)]
mod tests;
