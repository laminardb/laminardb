//! Backpressure, payload construction, and reader-queue publication.

use super::{
    cached_partition_vnode, debug, publish_reader_fault, validate_kafka_partition_results, warn,
    Arc, AtomicUsize, Consumer, KafkaPartitionRoutes, KafkaPayload, KafkaReaderItem, KafkaReaderTx,
    LaminarConsumerContext, Message, Mutex, Notify, Ordering, ReaderLoopAction, StreamConsumer,
};

pub(super) struct KafkaBackpressureContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) channel_len: &'a Arc<AtomicUsize>,
    pub(super) channel_capacity: usize,
    pub(super) channel_capacity_f64: f64,
    pub(super) pause_threshold: f64,
    pub(super) resume_threshold: f64,
    pub(super) drain_active: bool,
}

pub(super) fn update_reader_backpressure(
    context: &KafkaBackpressureContext<'_>,
    is_paused: &mut bool,
) -> ReaderLoopAction {
    let fill = if context.channel_capacity > 0 {
        let queued = u32::try_from(context.channel_len.load(Ordering::Acquire))
            .map_or(f64::from(u32::MAX), f64::from);
        queued / context.channel_capacity_f64
    } else {
        0.0
    };
    if context.drain_active {
        return ReaderLoopAction::Proceed;
    }
    if fill >= context.pause_threshold && !*is_paused {
        if let Ok(assignment) = context.consumer.assignment() {
            match context.consumer.pause(&assignment) {
                Ok(()) => match validate_kafka_partition_results("backpressure pause", &assignment)
                {
                    Ok(()) => {
                        *is_paused = true;
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
    } else if fill <= context.resume_threshold && *is_paused {
        if let Ok(assignment) = context.consumer.assignment() {
            let resumed = context
                .consumer
                .resume(&assignment)
                .map_err(|error| format!("Kafka backpressure resume failed: {error}"))
                .and_then(|()| {
                    validate_kafka_partition_results("backpressure resume", &assignment)
                });
            if let Err(error) = resumed {
                warn!(%error, "reader backpressure resume incomplete");
                return ReaderLoopAction::Retry;
            }
            *is_paused = false;
            debug!("reader: resumed Kafka partitions (fill={fill:.2})");
        }
    }
    ReaderLoopAction::Proceed
}

#[inline]
pub(super) fn build_reader_payload(
    message: &rdkafka::message::BorrowedMessage<'_>,
    vnode_routing: bool,
    routes: &KafkaPartitionRoutes,
    capture_headers: bool,
    cached_topic: &mut Arc<str>,
    cached_topic_routes: &mut Option<Arc<[u32]>>,
    reader_fault: &Arc<Mutex<Option<Arc<str>>>>,
    data_ready: &Arc<Notify>,
) -> Result<Option<KafkaPayload>, ()> {
    let Some(payload) = message.payload() else {
        return Ok(None);
    };
    let topic = message.topic();
    if cached_topic.as_ref() != topic {
        if vnode_routing {
            let Some((canonical_topic, topic_routes)) = routes.get_key_value(topic) else {
                warn!(
                    topic,
                    "Kafka reader received a topic outside its activated vnode inventory"
                );
                publish_reader_fault(
                    reader_fault,
                    data_ready,
                    "payload topic is outside the activated inventory",
                );
                return Err(());
            };
            *cached_topic = Arc::clone(canonical_topic);
            *cached_topic_routes = Some(Arc::clone(topic_routes));
        } else {
            *cached_topic = Arc::from(topic);
            *cached_topic_routes = None;
        }
    }
    let partition_vnode =
        match cached_partition_vnode(cached_topic_routes.as_deref(), message.partition()) {
            Ok(vnode) => vnode,
            Err(error) => {
                warn!(
                    topic,
                    partition = message.partition(),
                    %error,
                    "Kafka reader rejected a payload outside its activated vnode inventory"
                );
                publish_reader_fault(
                    reader_fault,
                    data_ready,
                    format!("payload route is outside the activated inventory: {error}"),
                );
                return Err(());
            }
        };
    let timestamp_ms = match message.timestamp() {
        rdkafka::Timestamp::CreateTime(timestamp)
        | rdkafka::Timestamp::LogAppendTime(timestamp) => Some(timestamp),
        rdkafka::Timestamp::NotAvailable => None,
    };
    let headers_json = if capture_headers {
        use rdkafka::message::Headers;
        message.headers().and_then(|headers| {
            let pairs: Vec<(String, serde_json::Value)> = (0..headers.count())
                .map(|index| {
                    let header = headers.get(index);
                    let value = match header.value {
                        Some(value) => {
                            serde_json::Value::String(String::from_utf8_lossy(value).into_owned())
                        }
                        None => serde_json::Value::Null,
                    };
                    (header.key.to_string(), value)
                })
                .collect();
            serde_json::to_string(&pairs).ok()
        })
    } else {
        None
    };
    Ok(Some(KafkaPayload {
        data: payload.to_vec(),
        topic: Arc::clone(cached_topic),
        partition: message.partition(),
        partition_vnode,
        offset: message.offset(),
        timestamp_ms,
        headers_json,
    }))
}

pub(super) struct KafkaFullQueueContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) msg_tx: &'a KafkaReaderTx,
    pub(super) channel_len: &'a Arc<AtomicUsize>,
    pub(super) reader_shutdown: &'a mut tokio::sync::watch::Receiver<bool>,
    pub(super) reader_fault: &'a Arc<Mutex<Option<Arc<str>>>>,
    pub(super) data_ready: &'a Arc<Notify>,
}

pub(super) async fn send_full_reader_item(
    context: KafkaFullQueueContext<'_>,
    item: KafkaReaderItem,
    is_paused: &mut bool,
) -> bool {
    if !*is_paused {
        if let Ok(assignment) = context.consumer.assignment() {
            let paused = context
                .consumer
                .pause(&assignment)
                .map_err(|error| format!("Kafka full-channel pause failed: {error}"))
                .and_then(|()| validate_kafka_partition_results("full-channel pause", &assignment));
            match paused {
                Ok(()) => {
                    *is_paused = true;
                    debug!("reader: paused partitions (channel full)");
                }
                Err(error) => {
                    warn!(%error, "reader full-channel pause incomplete");
                }
            }
        }
    }
    context.channel_len.fetch_add(1, Ordering::Relaxed);
    let sent = tokio::select! {
        biased;
        _ = context.reader_shutdown.changed() => false,
        result = context.msg_tx.send(item) => result.is_ok(),
    };
    if sent {
        return true;
    }
    context.channel_len.fetch_sub(1, Ordering::Relaxed);
    if !*context.reader_shutdown.borrow() {
        publish_reader_fault(
            context.reader_fault,
            context.data_ready,
            "reader output channel closed unexpectedly",
        );
    }
    false
}
