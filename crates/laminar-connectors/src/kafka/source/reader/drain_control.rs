//! Exact drain commands, assignment holding, and FIFO boundary publication.

use super::{
    kafka_drain_partitions, kafka_drain_target_ready, publish_reader_fault,
    resolve_kafka_reader_drain, tpl_of, validate_kafka_partition_results, warn, Arc, AtomicU8,
    AtomicUsize, Consumer, KafkaBlockingTasks, KafkaDrainBoundary, KafkaDrainPosition,
    KafkaReaderDrain, KafkaReaderDrainCommand, KafkaReaderItem, KafkaReaderTx,
    LaminarConsumerContext, Mutex, Notify, Ordering, ReaderLoopAction, SourceDrainRequest,
    SourceDrainResolution, StreamConsumer,
};

pub(super) struct KafkaDrainHoldContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) msg_tx: &'a KafkaReaderTx,
    pub(super) channel_len: &'a Arc<AtomicUsize>,
    pub(super) reader_shutdown: &'a mut tokio::sync::watch::Receiver<bool>,
    pub(super) reader_fault: &'a Arc<Mutex<Option<Arc<str>>>>,
    pub(super) data_ready: &'a Arc<Notify>,
    pub(super) last_assignment_version: u64,
    pub(super) drain_paused: &'a mut std::collections::HashSet<(Arc<str>, i32)>,
    pub(super) active: &'a mut KafkaReaderDrain,
}

pub(super) async fn hold_reader_drain(context: KafkaDrainHoldContext<'_>) -> ReaderLoopAction {
    if context.active.held_assignment_version != Some(context.last_assignment_version) {
        let assignment = match context.consumer.assignment() {
            Ok(assignment) => assignment,
            Err(error) => {
                warn!(%error, "Kafka drain could not inspect the live assignment");
                return ReaderLoopAction::Retry;
            }
        };
        context.active.held_inputs = match kafka_drain_partitions(&assignment) {
            Ok(inputs) => inputs,
            Err(error) => {
                publish_reader_fault(
                    context.reader_fault,
                    context.data_ready,
                    format!("invalid Kafka assignment while draining: {error}"),
                );
                return ReaderLoopAction::Stop;
            }
        };
        let current_set: std::collections::HashSet<(Arc<str>, i32)> = context
            .active
            .held_inputs
            .iter()
            .map(|input| (Arc::clone(&input.topic), input.partition))
            .collect();
        context
            .drain_paused
            .retain(|input| current_set.contains(input));
        context.active.held_assignment_version = Some(context.last_assignment_version);
        context.active.hold_complete = false;
    }

    if !context.active.hold_complete {
        let remaining: Vec<(Arc<str>, i32)> = context
            .active
            .held_inputs
            .iter()
            .filter(|input| {
                !context
                    .drain_paused
                    .contains(&(Arc::clone(&input.topic), input.partition))
            })
            .map(|input| (Arc::clone(&input.topic), input.partition))
            .collect();
        let to_pause = tpl_of(remaining.iter());
        if to_pause.count() > 0 {
            match context.consumer.pause(&to_pause) {
                Ok(()) => {
                    for element in to_pause.elements() {
                        if element.error().is_ok() {
                            context
                                .drain_paused
                                .insert((Arc::from(element.topic()), element.partition()));
                        }
                    }
                    if let Err(error) = validate_kafka_partition_results("drain pause", &to_pause) {
                        warn!(%error, "Kafka drain pause incomplete; will retry");
                        return ReaderLoopAction::Retry;
                    }
                }
                Err(error) => {
                    warn!(%error, "Kafka drain pause failed; will retry");
                    return ReaderLoopAction::Retry;
                }
            }
        }
        context.active.hold_complete = context.active.held_inputs.iter().all(|input| {
            context
                .drain_paused
                .contains(&(Arc::clone(&input.topic), input.partition))
        });
    }

    if context.active.boundary_queued {
        return ReaderLoopAction::Proceed;
    }
    let cut_is_paused = context.active.inputs.iter().all(|input| {
        context
            .drain_paused
            .contains(&(Arc::clone(&input.topic), input.partition))
    });
    if !cut_is_paused {
        return ReaderLoopAction::Proceed;
    }

    let boundary = KafkaDrainBoundary {
        round: context.active.request.round,
        inputs: Arc::clone(&context.active.inputs),
    };
    context.channel_len.fetch_add(1, Ordering::Relaxed);
    let sent = tokio::select! {
        biased;
        _ = context.reader_shutdown.changed() => false,
        () = tokio::time::sleep_until(context.active.prepare_deadline) => false,
        result = context.msg_tx.send(KafkaReaderItem::DrainBoundary(boundary)) => result.is_ok(),
    };
    if !sent {
        context.channel_len.fetch_sub(1, Ordering::Relaxed);
        if tokio::time::Instant::now() >= context.active.prepare_deadline {
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                "Kafka drain boundary delivery exceeded its engine deadline",
            );
        } else if !*context.reader_shutdown.borrow() {
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                "reader drain-boundary channel closed unexpectedly",
            );
        }
        return ReaderLoopAction::Stop;
    }
    context.active.boundary_queued = true;
    context.data_ready.notify_one();
    ReaderLoopAction::Proceed
}

pub(super) struct KafkaDrainCommandContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) blocking_tasks: &'a KafkaBlockingTasks,
    pub(super) vnode_reassign: &'a Option<(
        Arc<laminar_core::state::VnodeRegistry>,
        laminar_core::state::NodeId,
    )>,
    pub(super) reader_fault: &'a Arc<Mutex<Option<Arc<str>>>>,
    pub(super) data_ready: &'a Arc<Notify>,
    pub(super) source_name: &'a Arc<str>,
    pub(super) last_assignment_version: u64,
    pub(super) is_paused: bool,
    pub(super) active_drain: &'a mut Option<KafkaReaderDrain>,
    pub(super) deferred_command: &'a mut Option<KafkaReaderDrainCommand>,
    pub(super) drain_paused: &'a mut std::collections::HashSet<(Arc<str>, i32)>,
}

pub(super) async fn process_reader_drain_command(
    mut context: KafkaDrainCommandContext<'_>,
    command: KafkaReaderDrainCommand,
) -> ReaderLoopAction {
    match command {
        KafkaReaderDrainCommand::Begin { request, deadline } => {
            begin_reader_drain(&mut context, request, deadline)
        }
        KafkaReaderDrainCommand::Resolve {
            resolution,
            cut,
            deadline,
            execution,
            reply,
        } => {
            resolve_reader_drain_command(&mut context, resolution, cut, deadline, execution, reply)
                .await
        }
    }
}

fn begin_reader_drain(
    context: &mut KafkaDrainCommandContext<'_>,
    request: SourceDrainRequest,
    deadline: tokio::time::Instant,
) -> ReaderLoopAction {
    if let Some(current) = context.active_drain.as_ref() {
        if current.request == request {
            return ReaderLoopAction::Proceed;
        }
        warn!(
            current = ?current.request.round,
            requested = ?request.round,
            "Kafka reader received a conflicting drain round"
        );
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "conflicting Kafka drain round",
        );
        return ReaderLoopAction::Stop;
    }
    if tokio::time::Instant::now() >= deadline {
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "Kafka source drain began after its engine deadline",
        );
        return ReaderLoopAction::Stop;
    }
    let Some((registry, _)) = context.vnode_reassign.as_ref() else {
        warn!("Kafka reader received drain control without cluster ownership");
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "drain control has no cluster ownership",
        );
        return ReaderLoopAction::Stop;
    };
    if registry.assignment_version() != request.round.predecessor_version {
        warn!(
            current = registry.assignment_version(),
            predecessor = request.round.predecessor_version,
            "Kafka reader rejected drain for a stale predecessor"
        );
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "drain predecessor does not match current assignment",
        );
        return ReaderLoopAction::Stop;
    }
    if context.last_assignment_version != request.round.predecessor_version {
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "Kafka drain predecessor is not reconciled",
        );
        return ReaderLoopAction::Stop;
    }
    let assignment = match context.consumer.assignment() {
        Ok(assignment) => assignment,
        Err(error) => {
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                format!("Kafka drain could not inspect its assignment: {error}"),
            );
            return ReaderLoopAction::Stop;
        }
    };
    let inputs = match kafka_drain_partitions(&assignment) {
        Ok(inputs) => inputs,
        Err(error) => {
            warn!(
                source = context.source_name.as_ref(),
                %error,
                "Kafka reader rejected its drain assignment"
            );
            publish_reader_fault(
                context.reader_fault,
                context.data_ready,
                format!("invalid Kafka drain assignment: {error}"),
            );
            return ReaderLoopAction::Stop;
        }
    };
    *context.active_drain = Some(KafkaReaderDrain {
        request,
        prepare_deadline: deadline,
        held_inputs: Arc::clone(&inputs),
        inputs,
        held_assignment_version: Some(context.last_assignment_version),
        hold_complete: false,
        boundary_queued: false,
    });
    ReaderLoopAction::Proceed
}

async fn resolve_reader_drain_command(
    context: &mut KafkaDrainCommandContext<'_>,
    resolution: SourceDrainResolution,
    cut: Arc<[KafkaDrainPosition]>,
    deadline: tokio::time::Instant,
    execution: Arc<AtomicU8>,
    reply: tokio::sync::oneshot::Sender<Result<(), String>>,
) -> ReaderLoopAction {
    if context.active_drain.is_none() {
        let _ = reply.send(Err("Kafka reader has no active drain to resolve".into()));
        return ReaderLoopAction::Retry;
    }
    if tokio::time::Instant::now() >= deadline {
        let _ = reply.send(Err(
            "Kafka drain deadline expired before target reconciliation".into(),
        ));
        return ReaderLoopAction::Retry;
    }
    let Some((registry, _)) = context.vnode_reassign.as_ref() else {
        let _ = reply.send(Err(
            "Kafka drain resolution has no cluster assignment".into()
        ));
        return ReaderLoopAction::Retry;
    };
    let target_ready = match kafka_drain_target_ready(
        resolution.round.target_version,
        registry.assignment_version(),
        context.last_assignment_version,
    ) {
        Ok(ready) => ready,
        Err(error) => {
            let _ = reply.send(Err(error));
            return ReaderLoopAction::Retry;
        }
    };
    let target_paused = target_ready
        && context.active_drain.as_ref().is_some_and(|active| {
            active.held_assignment_version == Some(resolution.round.target_version)
                && active.hold_complete
        });
    if !target_ready || !target_paused {
        *context.deferred_command = Some(KafkaReaderDrainCommand::Resolve {
            resolution,
            cut,
            deadline,
            execution,
            reply,
        });
        return ReaderLoopAction::Proceed;
    }

    let result = resolve_kafka_reader_drain(
        context.consumer,
        context.blocking_tasks,
        context.vnode_reassign.as_ref(),
        context.active_drain.as_ref().expect("validated above"),
        resolution,
        &cut,
        context.is_paused,
        deadline,
        &execution,
    )
    .await;
    if result.is_ok() {
        *context.active_drain = None;
        context.drain_paused.clear();
    }
    let _ = reply.send(result);
    ReaderLoopAction::Retry
}
