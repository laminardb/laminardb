//! Recovery cursor installation after broker assignment changes.

use super::{
    assignment_seek_tpl, debug, info, lock_or_recover, publish_reader_fault,
    validate_kafka_partition_results, warn, Arc, AtomicBool, AtomicU64, Consumer,
    KafkaBlockingTaskError, KafkaBlockingTasks, KafkaPartitionBaselines, KafkaReaderDrain,
    LaminarConsumerContext, Mutex, Notify, OffsetTracker, Ordering, ReaderLoopAction,
    RebalanceState, StreamConsumer,
};

pub(super) struct KafkaPositioningContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) assign_generation: &'a Arc<AtomicU64>,
    pub(super) rebalance_state: &'a Arc<Mutex<RebalanceState>>,
    pub(super) deterministic_unrecorded: &'a Arc<AtomicBool>,
    pub(super) deterministic_default: Option<rdkafka::Offset>,
    pub(super) reassign_snapshot: &'a Arc<Mutex<OffsetTracker>>,
    pub(super) reassign_baselines: &'a KafkaPartitionBaselines,
    pub(super) blocking_tasks: &'a KafkaBlockingTasks,
    pub(super) active_drain: &'a Option<KafkaReaderDrain>,
    pub(super) is_paused: bool,
    pub(super) reader_fault: &'a Arc<Mutex<Option<Arc<str>>>>,
    pub(super) data_ready: &'a Arc<Notify>,
}

pub(super) async fn position_reader_assignment(
    context: KafkaPositioningContext<'_>,
    current_generation: u64,
    last_generation: &mut u64,
) -> ReaderLoopAction {
    let mut assigned: Vec<(String, i32)> = lock_or_recover(context.rebalance_state)
        .assigned_partitions()
        .iter()
        .cloned()
        .collect();
    if assigned.is_empty() {
        if let Ok(assignment) = context.consumer.assignment() {
            assigned = assignment
                .elements()
                .iter()
                .map(|element| (element.topic().to_string(), element.partition()))
                .collect();
        }
    }
    let deterministic_fallback = context
        .deterministic_unrecorded
        .load(Ordering::Acquire)
        .then_some(context.deterministic_default)
        .flatten();
    let seek = match assignment_seek_tpl(
        &lock_or_recover(context.reassign_snapshot),
        &assigned,
        (!context.reassign_baselines.is_empty()).then_some(context.reassign_baselines),
        deterministic_fallback,
        context.deterministic_unrecorded.load(Ordering::Acquire),
    ) {
        Ok(seek) => seek,
        Err(error) => {
            warn!(%error, "failed to build Kafka recovery assignment");
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                format!("invalid recovery assignment: {error}"),
            );
            return ReaderLoopAction::Stop;
        }
    };

    let positioned = if seek.count() == 0 {
        true
    } else {
        let consumer = Arc::clone(context.consumer);
        match context
            .blocking_tasks
            .run(move || consumer.seek_partitions(seek, std::time::Duration::from_secs(5)))
            .await
        {
            Ok(Ok(result))
                if context.assign_generation.load(Ordering::Acquire) == current_generation =>
            {
                let failed = result
                    .elements()
                    .iter()
                    .filter(|element| element.error().is_err())
                    .count();
                if failed == 0 {
                    info!(
                        partition_count = result.count(),
                        "seeked assigned partitions to checkpointed offsets"
                    );
                    true
                } else {
                    debug!(failed, "assign-seek incomplete; will retry");
                    false
                }
            }
            Ok(Ok(_)) => false,
            Ok(Err(error)) => {
                debug!(%error, "assign-seek failed; will retry");
                false
            }
            Err(error) => {
                warn!(%error, "Kafka assign-seek worker failed");
                if error == KafkaBlockingTaskError::Retired {
                    return ReaderLoopAction::Stop;
                }
                false
            }
        }
    };
    if !positioned {
        return ReaderLoopAction::Proceed;
    }

    if !context.is_paused && context.active_drain.is_none() {
        if let Ok(assignment) = context.consumer.assignment() {
            if let Err(error) = context.consumer.resume(&assignment) {
                warn!(%error, "post-seek resume failed; will retry");
                return ReaderLoopAction::Retry;
            }
            if let Err(error) = validate_kafka_partition_results("post-seek resume", &assignment) {
                warn!(%error, "post-seek resume incomplete; will retry");
                return ReaderLoopAction::Retry;
            }
        }
    }
    *last_generation = current_generation;
    ReaderLoopAction::Proceed
}
