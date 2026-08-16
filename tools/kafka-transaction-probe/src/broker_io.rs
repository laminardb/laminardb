//! Kafka administration, producer operations, and delivery/fencing checks.
//!
//! All waits are bounded by the probe's fixed deadlines. Scenario modules use these operations to
//! keep protocol steps visible without duplicating low-level client handling.

use super::*;

pub(super) fn create_admin(brokers: &str) -> ProbeResult<AdminClient<DefaultClientContext>> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("client.id", "ldb-kafka-transaction-probe-admin")
        .set("socket.timeout.ms", "10000")
        .set("request.timeout.ms", "10000")
        .create()
        .map_err(|error| format!("admin client creation failed: {error}"))
}

pub(super) fn create_topic(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> ProbeResult<()> {
    let new_topic = NewTopic::new(topic, 3, TopicReplication::Fixed(1))
        .set("cleanup.policy", "delete")
        .set("compression.type", "uncompressed");
    let options = AdminOptions::new()
        .request_timeout(Some(IO_TIMEOUT))
        .operation_timeout(Some(IO_TIMEOUT));
    let results = block_on(admin.create_topics([&new_topic], &options))
        .map_err(|error| format!("create_topics request failed: {error}"))?;
    if results.len() != 1 {
        return Err(format!(
            "create_topics returned {} results, expected 1",
            results.len()
        ));
    }
    match results.into_iter().next() {
        Some(Ok(created)) if created == topic => Ok(()),
        Some(Ok(created)) => Err(format!(
            "broker reported unexpected created topic {created}"
        )),
        Some(Err((name, error))) => Err(format!("topic creation failed name={name} error={error}")),
        None => Err("create_topics returned no result".to_owned()),
    }
}

pub(super) fn require_topic_inventory(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> ProbeResult<()> {
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        match admin
            .inner()
            .fetch_metadata(Some(topic), Duration::from_secs(2))
        {
            Ok(metadata) => {
                let topics = metadata.topics();
                if topics.len() == 1 && topics[0].name() == topic && topics[0].error().is_none() {
                    let mut ids = topics[0]
                        .partitions()
                        .iter()
                        .map(|partition| partition.id())
                        .collect::<Vec<_>>();
                    ids.sort_unstable();
                    if ids == PARTITIONS
                        && topics[0].partitions().iter().all(|partition| {
                            partition.error().is_none()
                                && partition.leader() >= 0
                                && partition.replicas().len() == 1
                                && partition.isr() == partition.replicas()
                        })
                    {
                        return Ok(());
                    }
                }
            }
            Err(error) if Instant::now() >= deadline => {
                return Err(format!("topic metadata remained unavailable: {error}"));
            }
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return Err("topic metadata never exposed exact ready inventory [0, 1, 2]".to_owned());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub(super) fn require_proxy_route(
    admin: &AdminClient<DefaultClientContext>,
    expected: SocketAddr,
) -> ProbeResult<()> {
    let metadata = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(2))
        .map_err(|error| format!("proxy-route metadata failed: {error}"))?;
    let brokers = metadata.brokers();
    if brokers.len() != 1
        || brokers[0].host() != expected.ip().to_string()
        || brokers[0].port() != i32::from(expected.port())
    {
        let observed = brokers
            .iter()
            .map(|broker| format!("{}:{}", broker.host(), broker.port()))
            .collect::<Vec<_>>();
        return Err(format!(
            "broker metadata could bypass proxy: expected [{expected}], observed {observed:?}"
        ));
    }
    Ok(())
}

pub(super) fn freeze_high_watermarks(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> ProbeResult<[i64; 3]> {
    let mut high = [0_i64; 3];
    for (index, partition) in PARTITIONS.into_iter().enumerate() {
        let (low, partition_high) = admin
            .inner()
            .fetch_watermarks(topic, partition, IO_TIMEOUT)
            .map_err(|error| {
                format!("watermark fetch failed for partition {partition}: {error}")
            })?;
        if low != 0 || partition_high < 0 {
            return Err(format!(
                "unexpected frozen cut for partition {partition}: low={low} high={partition_high}"
            ));
        }
        high[index] = partition_high;
    }
    Ok(high)
}

pub(super) fn create_producer(
    brokers: &str,
    transactional_id: &str,
    client_id: &str,
) -> ProbeResult<BaseProducer<ProbeProducerContext>> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("client.id", client_id)
        .set("transactional.id", transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("compression.type", "none")
        .set("transaction.timeout.ms", "20000")
        .set("message.timeout.ms", "15000")
        .set("request.timeout.ms", "10000")
        .set("socket.timeout.ms", "10000")
        .set("max.in.flight.requests.per.connection", "1")
        .set("allow.auto.create.topics", "false")
        .set("queue.buffering.max.messages", "1000")
        .set("queue.buffering.max.kbytes", "1024")
        .set("queue.buffering.max.ms", "0")
        .set("batch.num.messages", "100")
        .create_with_context(ProbeProducerContext::default())
        .map_err(|error| format!("transactional producer creation failed: {error}"))
}

pub(super) fn commit_marker(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    marker: &[u8],
) -> ProbeResult<()> {
    stage_marker(producer, topic, marker)?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit marker transaction failed: {error}"))
}

pub(super) fn stage_marker(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    marker: &[u8],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin marker transaction failed: {error}"))?;
    for partition in PARTITIONS {
        send_record(producer, topic, partition, None, &[], marker, false)?;
    }
    require_deliveries(producer, "marker", &PARTITIONS)
}

pub(super) fn commit_replay_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    interval: &[u8; 16],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin data transaction failed: {error}"))?;
    let header = encode_data_header(&[0xa1; 32], interval, 0)?;
    send_record(
        producer,
        topic,
        PARTITIONS[0],
        Some(STABLE_KEY),
        STABLE_PAYLOAD,
        &header,
        true,
    )?;
    require_deliveries(producer, "data commit", &[PARTITIONS[0]])?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit data transaction failed: {error}"))
}

pub(super) fn stage_replay_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    interval: &[u8; 16],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin staged data transaction failed: {error}"))?;
    let header = encode_data_header(&[0xa1; 32], interval, 0)?;
    send_record(
        producer,
        topic,
        PARTITIONS[0],
        Some(STABLE_KEY),
        STABLE_PAYLOAD,
        &header,
        true,
    )?;
    require_deliveries(producer, "staged data", &[PARTITIONS[0]])
}

pub(super) fn commit_selection_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    selected_interval: &[u8; 16],
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin selection-data transaction failed: {error}"))?;
    let header = encode_data_header(&[0xb1; 32], selected_interval, 0)?;
    send_record(
        producer,
        topic,
        PARTITIONS[0],
        Some(SELECTION_KEY),
        SELECTION_PAYLOAD,
        &header,
        true,
    )?;
    require_deliveries(producer, "selection data", &[PARTITIONS[0]])?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit selection-data transaction failed: {error}"))
}

pub(super) fn commit_data_fanout(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin retry transaction failed: {error}"))?;
    send_data_fanout(producer, topic, fanout)?;
    require_deliveries(producer, "data retry", &PARTITIONS)?;
    producer
        .commit_transaction(IO_TIMEOUT)
        .map_err(|error| format!("commit retry transaction failed: {error}"))
}

pub(super) fn abort_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin abort transaction failed: {error}"))?;
    send_data_fanout(producer, topic, fanout)?;
    require_deliveries(producer, "data abort", &PARTITIONS)?;
    producer
        .abort_transaction(IO_TIMEOUT)
        .map_err(|error| format!("confirmed abort failed: {error}"))
}

pub(super) fn stage_data(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    producer
        .begin_transaction()
        .map_err(|error| format!("begin staged transaction failed: {error}"))?;
    send_data_fanout(producer, topic, fanout)?;
    require_deliveries(producer, "staged predecessor", &PARTITIONS)
}

fn send_data_fanout(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    fanout: DataFanout<'_>,
) -> ProbeResult<()> {
    for (index, partition) in PARTITIONS.into_iter().enumerate() {
        let operation_id = operation_id(fanout.operation_tag, index);
        let sequence = fanout
            .sequence_base
            .checked_add(index as u64)
            .ok_or_else(|| "admission sequence overflow".to_owned())?;
        let header = encode_data_header(&operation_id, fanout.interval, sequence)?;
        send_record(
            producer,
            topic,
            partition,
            Some(fanout.keys[index]),
            fanout.payloads[index],
            &header,
            true,
        )?;
    }
    Ok(())
}

fn send_record(
    producer: &BaseProducer<ProbeProducerContext>,
    topic: &str,
    partition: i32,
    key: Option<&[u8]>,
    payload: &[u8],
    header: &[u8],
    include_trace_header: bool,
) -> ProbeResult<()> {
    if !PARTITIONS.contains(&partition) {
        return Err(format!("refusing unexpected target partition {partition}"));
    }
    let mut headers = OwnedHeaders::new_with_capacity(usize::from(include_trace_header) + 1)
        .insert(Header {
            key: HEADER_NAME,
            value: Some(header),
        });
    if include_trace_header {
        headers = headers.insert(Header {
            key: TRACE_HEADER_NAME,
            value: Some(TRACE_HEADER_VALUE),
        });
    }
    let mut record = BaseRecord::<[u8], [u8]>::to(topic)
        .partition(partition)
        .payload(payload)
        .headers(headers);
    if let Some(key) = key {
        record = record.key(key);
    }
    producer
        .send(record)
        .map_err(|(error, _)| format!("enqueue failed for partition {partition}: {error}"))
}

fn require_deliveries(
    producer: &BaseProducer<ProbeProducerContext>,
    label: &str,
    expected_partitions: &[i32],
) -> ProbeResult<()> {
    producer
        .flush(IO_TIMEOUT)
        .map_err(|error| format!("{label} flush failed: {error}"))?;
    let context = producer.context();
    let delivered = context.delivered.swap(0, Ordering::SeqCst);
    let failed = context.failed.swap(0, Ordering::SeqCst);
    let partition_mask = context.partition_mask.swap(0, Ordering::SeqCst);
    let invalid_partition = context.invalid_partition.swap(0, Ordering::SeqCst);
    let invalid_offset = context.invalid_offset.swap(0, Ordering::SeqCst);
    if delivered + failed != expected_partitions.len() {
        return Err(format!(
            "{label} produced {} delivery reports, expected {}",
            delivered + failed,
            expected_partitions.len()
        ));
    }
    if failed != 0 || invalid_partition != 0 || invalid_offset != 0 {
        return Err(format!(
            "{label} delivery validation failed: delivered={delivered} failed={failed} invalid_partition={invalid_partition} invalid_offset={invalid_offset}"
        ));
    }
    let expected_mask = expected_partitions
        .iter()
        .try_fold(0_usize, |mask, partition| {
            partition_index(*partition).map(|index| mask | (1 << index))
        })?;
    if partition_mask != expected_mask {
        return Err(format!(
            "{label} delivered partition mask {partition_mask:#x}, expected {expected_mask:#x}"
        ));
    }
    Ok(())
}

pub(super) fn require_fatal_fence(
    result: Result<(), KafkaError>,
    client_fatal: Option<(RDKafkaErrorCode, String)>,
) -> ProbeResult<(RDKafkaErrorCode, RDKafkaErrorCode)> {
    let commit_code = match result {
        Err(KafkaError::Transaction(error)) if error.is_fatal() && is_fence_code(error.code()) => {
            error.code()
        }
        Err(KafkaError::Transaction(error)) => return Err(format!(
            "old producer rejection was not a fatal fence: code={:?} fatal={} retriable={} abortable={} error={error}",
            error.code(),
            error.is_fatal(),
            error.is_retriable(),
            error.txn_requires_abort()
        )),
        Err(error) => return Err(format!(
            "old producer rejection was not a transaction fence: {error}"
        )),
        Ok(()) => return Err("old producer unexpectedly committed after successor initialization".to_owned()),
    };
    match client_fatal {
        Some((code, _reason)) if is_fence_code(code) => Ok((commit_code, code)),
        Some((code, reason)) => Err(format!(
            "old producer client fatal error was not fencing: code={code:?} reason={reason}"
        )),
        None => Err("old producer did not record a client-level fatal fence".to_owned()),
    }
}

fn is_fence_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::ProducerFenced
            | RDKafkaErrorCode::InvalidProducerEpoch
    )
}

pub(super) fn require_ambiguity_timeout(result: Result<(), KafkaError>) -> ProbeResult<()> {
    match result {
        Err(KafkaError::Transaction(error))
            if error.code() == RDKafkaErrorCode::OperationTimedOut
                && error.is_retriable()
                && !error.is_fatal()
                && !error.txn_requires_abort() =>
        {
            Ok(())
        }
        Err(KafkaError::Transaction(error)) => Err(format!(
            "ambiguous commit did not return the exact retriable local timeout: code={:?} fatal={} retriable={} abortable={} error={error}",
            error.code(),
            error.is_fatal(),
            error.is_retriable(),
            error.txn_requires_abort()
        )),
        Err(error) => Err(format!(
            "ambiguous commit did not return a transaction timeout: {error}"
        )),
        Ok(()) => Err("ambiguous commit unexpectedly returned success to producer A".to_owned()),
    }
}
