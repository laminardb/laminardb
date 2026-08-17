//! Kafka source connector: consumes topics via rdkafka's `StreamConsumer`,
//! deserializes with pluggable formats, and yields Arrow `RecordBatch`es.

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, RecordBatch, RecordBatchOptions, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
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

use crate::checkpoint::{SourceCheckpoint, SourceCheckpointDelta};
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

mod background;
mod checkpoint;
mod debezium;
mod decoding;
mod drain;
mod lifecycle;
mod metadata;
mod polling;
mod reader;
mod startup;

use background::{
    ensure_background_task_reaper, join_background_task, reap_last_arc_off_runtime,
    KafkaBlockingTaskError, KafkaBlockingTasks,
};
use checkpoint::{
    acquired_numeric_position, assignment_seek_tpl, build_vnode_assignment_tpl,
    consumer_creation_error, decode_partition_baselines, deterministic_initial_offset,
    kafka_input_channels, kafka_output_schema, kafka_reader_error_is_transient,
    kafka_row_positions, retire_accepted_rotation_baselines, rotation_baselines_len,
    rotation_partition_baseline, startup_default_offset, tpl_of, update_rotation_baselines,
    validate_kafka_output_schema, validate_partition_baselines, validate_positions_not_expired,
    validate_resume_input_channels, vnode_payload_is_current, NormalizedDebeziumBatch,
};
use debezium::normalize_kafka_debezium_batch;
use drain::{
    cached_partition_vnode, kafka_bootstrap_is_unassigned, kafka_drain_partitions,
    kafka_drain_target_ready, kafka_owned_partition_sets, kafka_partition_routes,
    kafka_partition_set, lock_or_recover, publish_reader_fault, resolve_kafka_reader_drain,
    terminalize_guaranteed_poll_error, try_capture_at_assignment_fence, validate_kafka_assignment,
    validate_kafka_partition_results, KafkaAssignmentPublication, KafkaDrainBoundary,
    KafkaDrainPartition, KafkaDrainPosition, KafkaPartitionBaselines, KafkaPartitionRoutes,
    KafkaPartitionSet, KafkaPayload, KafkaReaderDrain, KafkaReaderDrainCommand, KafkaReaderItem,
    KafkaReaderRx, KafkaRotationBaselines, KafkaSourceDrain, KafkaStartPlan,
    KAFKA_BACKGROUND_CLOSE_BUDGET, KAFKA_POSITION_LOOKUP_BUDGET, KAFKA_POSITION_LOOKUP_CONCURRENCY,
};
use metadata::{
    fetch_explicit_topic_metadata, fetch_partition_low_watermarks, fetch_partition_watermarks,
    resolve_timestamp_offsets,
};

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
    /// Cached physical-channel inventory for stable engine-owned local assignments.
    manual_input_channels: Arc<[Vec<u8>]>,
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
    /// Assignment whose complete batch cursor has already been produced.
    batch_cursor_assignment_version: Option<u64>,
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
            manual_input_channels: Arc::from([]),
            manual_partition_baselines: std::collections::HashMap::new(),
            assignment_publication: Arc::new(Mutex::new(Arc::new(
                KafkaAssignmentPublication::default(),
            ))),
            rotation_partition_baseline_count: Arc::new(AtomicUsize::new(0)),
            applied_rotation_baseline_version: None,
            batch_cursor_assignment_version: None,
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
