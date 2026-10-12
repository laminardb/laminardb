//! Backpressure, payload construction, and reader-queue publication.

use super::{
    cached_partition_vnode, debug, publish_reader_fault, validate_kafka_partition_results, warn,
    Arc, AtomicUsize, Consumer, KafkaHeader, KafkaPartitionRoutes, KafkaPayload, KafkaReaderItem,
    KafkaReaderTx, KafkaTimestampType, LaminarConsumerContext, Message, Mutex, Notify, Ordering,
    ReaderLoopAction, StreamConsumer,
};

/// The reader queue's budget of connector-owned bytes not yet taken by the poll path.
pub(super) struct KafkaByteBudget {
    permits: Arc<tokio::sync::Semaphore>,
    capacity: usize,
}

impl KafkaByteBudget {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity,
        }
    }

    fn fill(&self) -> f64 {
        let held = self
            .capacity
            .saturating_sub(self.permits.available_permits());
        u32::try_from(held).map_or(1.0, f64::from)
            / u32::try_from(self.capacity).map_or(f64::from(u32::MAX), f64::from)
    }
}

pub(super) struct KafkaBackpressureContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) channel_len: &'a Arc<AtomicUsize>,
    pub(super) channel_capacity: usize,
    pub(super) channel_capacity_f64: f64,
    pub(super) byte_budget: &'a KafkaByteBudget,
    pub(super) pause_threshold: f64,
    pub(super) resume_threshold: f64,
    pub(super) drain_active: bool,
}

pub(super) fn update_reader_backpressure(
    context: &KafkaBackpressureContext<'_>,
    is_paused: &mut bool,
) -> ReaderLoopAction {
    let record_fill = if context.channel_capacity > 0 {
        let queued = u32::try_from(context.channel_len.load(Ordering::Acquire))
            .map_or(f64::from(u32::MAX), f64::from);
        queued / context.channel_capacity_f64
    } else {
        0.0
    };
    let fill = record_fill.max(context.byte_budget.fill());
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

/// Which optional parts of a record the reader copies, and the largest record it accepts.
#[derive(Clone, Copy)]
pub(super) struct KafkaRecordCapture {
    pub(super) key: bool,
    pub(super) headers: bool,
    pub(super) record_max_bytes: usize,
}

#[inline]
#[allow(clippy::too_many_arguments)]
pub(super) fn build_reader_payload(
    message: &rdkafka::message::BorrowedMessage<'_>,
    vnode_routing: bool,
    routes: &KafkaPartitionRoutes,
    capture: KafkaRecordCapture,
    cached_topic: &mut Arc<str>,
    cached_topic_routes: &mut Option<Arc<[u32]>>,
    reader_fault: &Arc<Mutex<Option<Arc<str>>>>,
    data_ready: &Arc<Notify>,
) -> Result<KafkaPayload, ()> {
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
    let (timestamp_ms, timestamp_type) = match message.timestamp() {
        rdkafka::Timestamp::CreateTime(timestamp) => {
            (Some(timestamp), KafkaTimestampType::CreateTime)
        }
        rdkafka::Timestamp::LogAppendTime(timestamp) => {
            (Some(timestamp), KafkaTimestampType::LogAppendTime)
        }
        rdkafka::Timestamp::NotAvailable => (None, KafkaTimestampType::NotAvailable),
    };
    let mut payload = KafkaPayload {
        data: None,
        topic: Arc::clone(cached_topic),
        partition: message.partition(),
        partition_vnode,
        offset: message.offset(),
        timestamp_ms,
        timestamp_type,
        key: None,
        headers: Vec::new(),
        rejected: None,
        budget: None,
    };
    let value = message.payload();
    let key = capture.key.then(|| message.key()).flatten();
    // Check the size before copying anything into connector-owned memory.
    let bytes = value.map_or(0, <[u8]>::len) + key.map_or(0, <[u8]>::len);
    if bytes > capture.record_max_bytes {
        payload.rejected = Some(
            format!(
                "record at offset {} of {topic}-{} has {bytes} key and value bytes, above \
                 record.max.bytes = {}",
                payload.offset, payload.partition, capture.record_max_bytes
            )
            .into(),
        );
        return Ok(payload);
    }
    if capture.headers {
        match read_headers(message) {
            Ok(headers) => payload.headers = headers,
            Err(reason) => {
                payload.rejected = Some(
                    format!(
                        "record at offset {} of {topic}-{}: {reason}",
                        payload.offset, payload.partition
                    )
                    .into(),
                );
                return Ok(payload);
            }
        }
    }
    payload.data = value.map(<[u8]>::to_vec);
    payload.key = key.map(<[u8]>::to_vec);
    Ok(payload)
}

/// Every header of `message` in order, duplicates included. rdkafka 0.39's `Headers::try_get`
/// unwraps the UTF-8 conversion of the header name, so a producer-supplied non-UTF-8 name would
/// panic the reader; this reads the same native accessors and reports it instead.
fn read_headers(
    message: &rdkafka::message::BorrowedMessage<'_>,
) -> Result<Vec<KafkaHeader>, &'static str> {
    use rdkafka::bindings::{
        rd_kafka_header_cnt, rd_kafka_header_get_all, rd_kafka_headers_t, rd_kafka_message_headers,
    };
    use rdkafka::types::RDKafkaRespErr;

    let mut native: *mut rd_kafka_headers_t = std::ptr::null_mut();
    // SAFETY: `message.ptr()` is the librdkafka message this borrow keeps alive. librdkafka either
    // stores a headers handle owned by that message in `native` or reports NOENT.
    let status = unsafe { rd_kafka_message_headers(message.ptr(), &raw mut native) };
    match status {
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR if !native.is_null() => {}
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR | RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOENT => {
            return Ok(Vec::new());
        }
        _ => return Err("librdkafka could not parse the record headers"),
    }
    // SAFETY: `native` is the message-owned headers handle obtained above.
    let count = unsafe { rd_kafka_header_cnt(native) };
    let mut headers = Vec::with_capacity(count);
    for index in 0..count {
        let mut name: *const std::ffi::c_char = std::ptr::null();
        let mut value: *const std::ffi::c_void = std::ptr::null();
        let mut size = 0_usize;
        // SAFETY: `index < count` for the live handle, and the out-pointers are valid locals.
        // The returned name and value point into memory owned by the borrowed message.
        let status = unsafe {
            rd_kafka_header_get_all(native, index, &raw mut name, &raw mut value, &raw mut size)
        };
        if status != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR || name.is_null() {
            return Err("librdkafka could not read a record header");
        }
        // SAFETY: librdkafka returns a NUL-terminated header name owned by the message.
        let key = unsafe { std::ffi::CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| "a header name is not valid UTF-8")?
            .to_owned();
        let value = (!value.is_null()).then(|| {
            // SAFETY: a non-null value points to `size` bytes owned by the message.
            unsafe { std::slice::from_raw_parts(value.cast::<u8>(), size) }.to_vec()
        });
        headers.push(KafkaHeader { key, value });
    }
    Ok(headers)
}

pub(super) struct KafkaByteBudgetContext<'a> {
    pub(super) consumer: &'a Arc<StreamConsumer<LaminarConsumerContext>>,
    pub(super) byte_budget: &'a KafkaByteBudget,
    pub(super) reader_shutdown: &'a mut tokio::sync::watch::Receiver<bool>,
}

/// Reserve `payload`'s connector-owned bytes from the reader-channel byte budget, pausing
/// consumption while the poll path drains. A record larger than the whole budget (its headers are
/// not bounded by `record.max.bytes`) reserves the whole budget, so the queue holds at most the
/// budget plus one record. Returns `false` when the reader is shutting down.
pub(super) async fn acquire_reader_bytes(
    context: KafkaByteBudgetContext<'_>,
    payload: &mut KafkaPayload,
    is_paused: &mut bool,
) -> bool {
    let budget = context.byte_budget;
    // Configuration bounds the budget by u32::MAX.
    let permits = u32::try_from(payload.retained_bytes().min(budget.capacity)).unwrap_or(u32::MAX);
    if permits == 0 {
        return true;
    }
    match Arc::clone(&budget.permits).try_acquire_many_owned(permits) {
        Ok(permit) => {
            payload.budget = Some(permit);
            return true;
        }
        Err(tokio::sync::TryAcquireError::Closed) => return false,
        Err(tokio::sync::TryAcquireError::NoPermits) => {}
    }
    if !*is_paused {
        if let Ok(assignment) = context.consumer.assignment() {
            let paused = context
                .consumer
                .pause(&assignment)
                .map_err(|error| format!("Kafka byte-budget pause failed: {error}"))
                .and_then(|()| validate_kafka_partition_results("byte-budget pause", &assignment));
            match paused {
                Ok(()) => {
                    *is_paused = true;
                    debug!("reader: paused partitions (byte budget full)");
                }
                Err(error) => warn!(%error, "reader byte-budget pause incomplete"),
            }
        }
    }
    payload.budget = tokio::select! {
        biased;
        _ = context.reader_shutdown.changed() => None,
        permit = Arc::clone(&budget.permits).acquire_many_owned(permits) => permit.ok(),
    };
    payload.budget.is_some()
}

/// Report a reader output channel that closed while the source was still running.
pub(super) fn report_closed_output(
    reader_shutdown: &tokio::sync::watch::Receiver<bool>,
    reader_fault: &Arc<Mutex<Option<Arc<str>>>>,
    data_ready: &Arc<Notify>,
) {
    if !*reader_shutdown.borrow() {
        publish_reader_fault(
            reader_fault,
            data_ready,
            "reader output channel disconnected unexpectedly",
        );
    }
}

/// Report a consumer error the reader cannot continue past.
pub(super) fn report_consumer_error(
    error: &rdkafka::error::KafkaError,
    reader_fault: &Arc<Mutex<Option<Arc<str>>>>,
    data_ready: &Arc<Notify>,
) {
    warn!(%error, "Kafka consumer error");
    publish_reader_fault(
        reader_fault,
        data_ready,
        format!("terminal Kafka consumer error: {error}"),
    );
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
