//! Background Kafka consumption, assignment rotation, and queue publication.

use super::{
    acquired_numeric_position, assignment_seek_tpl, cached_partition_vnode, debug,
    deterministic_initial_offset, fetch_partition_low_watermarks, info, kafka_drain_partitions,
    kafka_drain_target_ready, kafka_input_channels, kafka_owned_partition_sets,
    kafka_partition_set, kafka_reader_error_is_transient, lock_or_recover, publish_reader_fault,
    resolve_kafka_reader_drain, rotation_baselines_len, startup_default_offset, tpl_of,
    update_rotation_baselines, validate_kafka_assignment, validate_kafka_partition_results,
    validate_positions_not_expired, warn, Arc, AtomicBool, AtomicU64, AtomicU8, AtomicUsize,
    Consumer, KafkaAssignmentPublication, KafkaBlockingTaskError, KafkaBlockingTasks,
    KafkaDrainBoundary, KafkaDrainPosition, KafkaPartitionBaselines, KafkaPartitionRoutes,
    KafkaPartitionSet, KafkaPayload, KafkaReaderDrain, KafkaReaderDrainCommand, KafkaReaderItem,
    KafkaSource, LaminarConsumerContext, Message, Mutex, Notify, OffsetTracker, Ordering,
    RebalanceState, SourceDrainRequest, SourceDrainResolution, StreamConsumer, TopicPartitionList,
};

mod drain_control;
mod payload;
mod positioning;
mod rotation;

use drain_control::{
    hold_reader_drain, process_reader_drain_command, KafkaDrainCommandContext,
    KafkaDrainHoldContext,
};
use payload::{
    build_reader_payload, send_full_reader_item, update_reader_backpressure,
    KafkaBackpressureContext, KafkaFullQueueContext,
};
use positioning::{position_reader_assignment, KafkaPositioningContext};
use rotation::reconcile_vnode_assignment;

type KafkaReaderTx = crossfire::MAsyncTx<crossfire::mpsc::Array<KafkaReaderItem>>;
type KafkaReaderDrainRx = tokio::sync::mpsc::UnboundedReceiver<KafkaReaderDrainCommand>;

/// Owns every dependency and mutable control state for one background reader generation.
struct KafkaReaderTask {
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    vnode_reassign: Option<(
        Arc<laminar_core::state::VnodeRegistry>,
        laminar_core::state::NodeId,
    )>,
    msg_tx: KafkaReaderTx,
    reader_drain_rx: Option<KafkaReaderDrainRx>,
    data_ready: Arc<Notify>,
    reader_fault: Arc<Mutex<Option<Arc<str>>>>,
    channel_len: Arc<AtomicUsize>,
    capture_headers: bool,
    reader_channel_capacity: usize,
    reader_channel_capacity_f64: f64,
    assign_generation: Arc<AtomicU64>,
    rebalance_state: Arc<Mutex<RebalanceState>>,
    pause_threshold: f64,
    resume_threshold: f64,
    vnode_partition_routes: KafkaPartitionRoutes,
    reassign_snapshot: Arc<Mutex<OffsetTracker>>,
    reassign_baselines: KafkaPartitionBaselines,
    assignment_publication: Arc<Mutex<Arc<KafkaAssignmentPublication>>>,
    rotation_baseline_count: Arc<AtomicUsize>,
    reconciled_assignment_version: Arc<AtomicU64>,
    require_durable_baselines: bool,
    reassign_default_offset: rdkafka::Offset,
    deterministic_unrecorded: Arc<AtomicBool>,
    source_name: Arc<str>,
    blocking_tasks: KafkaBlockingTasks,
    deterministic_default: Option<rdkafka::Offset>,
    reader_shutdown: tokio::sync::watch::Receiver<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReaderLoopAction {
    Proceed,
    Retry,
    Stop,
}

struct KafkaRotationContext<'a> {
    consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    vnode_reassign: &'a Option<(
        Arc<laminar_core::state::VnodeRegistry>,
        laminar_core::state::NodeId,
    )>,
    vnode_partition_routes: &'a KafkaPartitionRoutes,
    reassign_snapshot: &'a Arc<Mutex<OffsetTracker>>,
    reassign_baselines: &'a KafkaPartitionBaselines,
    assignment_publication: &'a Arc<Mutex<Arc<KafkaAssignmentPublication>>>,
    rotation_baseline_count: &'a Arc<AtomicUsize>,
    reconciled_assignment_version: &'a Arc<AtomicU64>,
    require_durable_baselines: bool,
    reassign_default_offset: rdkafka::Offset,
    deterministic_unrecorded: &'a Arc<AtomicBool>,
    deterministic_default: Option<rdkafka::Offset>,
    source_name: &'a Arc<str>,
    blocking_tasks: &'a KafkaBlockingTasks,
    reader_fault: &'a Arc<Mutex<Option<Arc<str>>>>,
    data_ready: &'a Arc<Notify>,
    reader_shutdown: &'a mut tokio::sync::watch::Receiver<bool>,
    active_drain: &'a mut Option<KafkaReaderDrain>,
    drain_paused: &'a mut std::collections::HashSet<(Arc<str>, i32)>,
}

impl KafkaSource {
    /// Spawns the background reader task on the first `poll_batch()`.
    /// The startup cursor has already been installed by `start()` before these
    /// tasks can observe the consumer.
    pub(super) fn ensure_reader_started(&mut self) {
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
        let (reader_drain_tx, reader_drain_rx) = if vnode_reassign.is_some() {
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
        let reader_shutdown = shutdown_rx;
        let reader_guard = self
            .task_owner
            .track()
            .expect("live Kafka source must admit its reader task");
        let task = KafkaReaderTask {
            consumer,
            vnode_reassign,
            msg_tx,
            reader_drain_rx,
            data_ready,
            reader_fault,
            channel_len,
            capture_headers,
            reader_channel_capacity,
            reader_channel_capacity_f64,
            assign_generation,
            rebalance_state,
            pause_threshold,
            resume_threshold,
            vnode_partition_routes,
            reassign_snapshot,
            reassign_baselines,
            assignment_publication,
            rotation_baseline_count,
            reconciled_assignment_version,
            require_durable_baselines,
            reassign_default_offset,
            deterministic_unrecorded,
            source_name,
            blocking_tasks,
            deterministic_default,
            reader_shutdown,
        };
        let reader_handle = tokio::spawn(async move {
            let _reader_guard = reader_guard;
            task.run().await;
        });

        self.msg_rx = Some(msg_rx);
        self.reader_handle = Some(reader_handle);
        self.reader_shutdown = Some(shutdown_tx);
        self.reader_drain_tx = reader_drain_tx;
    }
}

impl KafkaReaderTask {
    // PERF: Keep select priority, cancellation, broker receive, and the allocation-free
    // `try_send` fast path in one future. Cold rotation, drain, recovery, and full-queue
    // work is extracted; another split would add a state transition to every message and
    // obscure the ordering between shutdown, drain deadlines, commands, and receive.
    async fn run(self) {
        let Self {
            consumer,
            vnode_reassign,
            msg_tx,
            mut reader_drain_rx,
            data_ready,
            reader_fault,
            channel_len,
            capture_headers,
            reader_channel_capacity,
            reader_channel_capacity_f64,
            assign_generation,
            rebalance_state,
            pause_threshold,
            resume_threshold,
            vnode_partition_routes,
            reassign_snapshot,
            reassign_baselines,
            assignment_publication,
            rotation_baseline_count,
            reconciled_assignment_version,
            require_durable_baselines,
            reassign_default_offset,
            deterministic_unrecorded,
            source_name,
            blocking_tasks,
            deterministic_default,
            mut reader_shutdown,
        } = self;
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
                !active.boundary_queued && tokio::time::Instant::now() >= active.prepare_deadline
            }) {
                publish_reader_fault(
                    &reader_fault,
                    &data_ready,
                    "Kafka source drain preparation exceeded its engine deadline",
                );
                return;
            }
            let assignment_changed = vnode_reassign.as_ref().is_some_and(|(registry, _)| {
                registry.assignment_version() != last_assignment_version
            });
            if assignment_changed {
                let action = reconcile_vnode_assignment(
                    KafkaRotationContext {
                        consumer: &consumer,
                        vnode_reassign: &vnode_reassign,
                        vnode_partition_routes: &vnode_partition_routes,
                        reassign_snapshot: &reassign_snapshot,
                        reassign_baselines: &reassign_baselines,
                        assignment_publication: &assignment_publication,
                        rotation_baseline_count: &rotation_baseline_count,
                        reconciled_assignment_version: &reconciled_assignment_version,
                        require_durable_baselines,
                        reassign_default_offset,
                        deterministic_unrecorded: &deterministic_unrecorded,
                        deterministic_default,
                        source_name: &source_name,
                        blocking_tasks: &blocking_tasks,
                        reader_fault: &reader_fault,
                        data_ready: &data_ready,
                        reader_shutdown: &mut reader_shutdown,
                        active_drain: &mut active_drain,
                        drain_paused: &mut drain_paused,
                    },
                    &mut last_assignment_version,
                )
                .await;
                match action {
                    ReaderLoopAction::Proceed => {}
                    ReaderLoopAction::Retry => continue,
                    ReaderLoopAction::Stop => break,
                }
            }
            let command = deferred_drain_command.take().or_else(|| {
                reader_drain_rx
                    .as_mut()
                    .and_then(|receiver| receiver.try_recv().ok())
            });
            if let Some(command) = command {
                let action = process_reader_drain_command(
                    KafkaDrainCommandContext {
                        consumer: &consumer,
                        blocking_tasks: &blocking_tasks,
                        vnode_reassign: &vnode_reassign,
                        reader_fault: &reader_fault,
                        data_ready: &data_ready,
                        source_name: &source_name,
                        last_assignment_version,
                        is_paused,
                        active_drain: &mut active_drain,
                        deferred_command: &mut deferred_drain_command,
                        drain_paused: &mut drain_paused,
                    },
                    command,
                )
                .await;
                match action {
                    ReaderLoopAction::Proceed => {}
                    ReaderLoopAction::Retry => continue,
                    ReaderLoopAction::Stop => return,
                }
            }
            if let Some(active) = active_drain.as_mut() {
                let action = hold_reader_drain(KafkaDrainHoldContext {
                    consumer: &consumer,
                    msg_tx: &msg_tx,
                    channel_len: &channel_len,
                    reader_shutdown: &mut reader_shutdown,
                    reader_fault: &reader_fault,
                    data_ready: &data_ready,
                    last_assignment_version,
                    drain_paused: &mut drain_paused,
                    active,
                })
                .await;
                match action {
                    ReaderLoopAction::Proceed => {}
                    ReaderLoopAction::Retry => continue,
                    ReaderLoopAction::Stop => break,
                }
            }
            let current_assign_generation = assign_generation.load(Ordering::Acquire);
            if current_assign_generation != last_assign_gen {
                let action = position_reader_assignment(
                    KafkaPositioningContext {
                        consumer: &consumer,
                        assign_generation: &assign_generation,
                        rebalance_state: &rebalance_state,
                        deterministic_unrecorded: &deterministic_unrecorded,
                        deterministic_default,
                        reassign_snapshot: &reassign_snapshot,
                        reassign_baselines: &reassign_baselines,
                        blocking_tasks: &blocking_tasks,
                        active_drain: &active_drain,
                        is_paused,
                        reader_fault: &reader_fault,
                        data_ready: &data_ready,
                    },
                    current_assign_generation,
                    &mut last_assign_gen,
                )
                .await;
                match action {
                    ReaderLoopAction::Proceed => {}
                    ReaderLoopAction::Retry => continue,
                    ReaderLoopAction::Stop => return,
                }
            }

            let backpressure = update_reader_backpressure(
                &KafkaBackpressureContext {
                    consumer: &consumer,
                    channel_len: &channel_len,
                    channel_capacity: reader_channel_capacity,
                    channel_capacity_f64: reader_channel_capacity_f64,
                    pause_threshold,
                    resume_threshold,
                    drain_active: active_drain.is_some(),
                },
                &mut is_paused,
            );
            if backpressure == ReaderLoopAction::Retry {
                continue;
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
                    let Ok(payload) = build_reader_payload(
                        &msg,
                        vnode_reassign.is_some(),
                        &vnode_partition_routes,
                        capture_headers,
                        &mut cached_topic,
                        &mut cached_topic_routes,
                        &reader_fault,
                        &data_ready,
                    ) else {
                        return;
                    };
                    let Some(payload) = payload else {
                        continue;
                    };
                    let item = KafkaReaderItem::Payload(payload);
                    match msg_tx.try_send(item) {
                        Ok(()) => {
                            channel_len.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(crossfire::TrySendError::Full(item)) => {
                            let sent = send_full_reader_item(
                                KafkaFullQueueContext {
                                    consumer: &consumer,
                                    msg_tx: &msg_tx,
                                    channel_len: &channel_len,
                                    reader_shutdown: &mut reader_shutdown,
                                    reader_fault: &reader_fault,
                                    data_ready: &data_ready,
                                },
                                item,
                                &mut is_paused,
                            )
                            .await;
                            if !sent {
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
    }
}
