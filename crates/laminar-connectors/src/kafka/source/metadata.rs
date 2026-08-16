//! Bounded broker metadata, watermark, and timestamp-offset lookup.

use super::{
    fetch_error, invalid_response, topic_error, Arc, ConnectorError, Consumer, KafkaBlockingTasks,
    KafkaPartitionBaselines, KafkaPartitionSet, LaminarConsumerContext, StreamConsumer,
    TopicPartitionList, KAFKA_POSITION_LOOKUP_BUDGET, KAFKA_POSITION_LOOKUP_CONCURRENCY,
};

pub(super) async fn fetch_explicit_topic_metadata(
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

pub(super) async fn fetch_partition_low_watermarks(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    partitions: &KafkaPartitionSet,
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    fetch_partition_watermarks(blocking_tasks, consumer, partitions)
        .await
        .map(|(low, _)| low)
}

pub(super) async fn fetch_partition_watermarks(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    partitions: &KafkaPartitionSet,
) -> Result<(KafkaPartitionBaselines, KafkaPartitionBaselines), ConnectorError> {
    let deadline = tokio::time::Instant::now() + KAFKA_POSITION_LOOKUP_BUDGET;
    let mut remaining = partitions.iter().cloned();
    let mut jobs = tokio::task::JoinSet::new();
    let mut low_watermarks = KafkaPartitionBaselines::with_capacity(partitions.len());
    let mut high_watermarks = KafkaPartitionBaselines::with_capacity(partitions.len());

    loop {
        while jobs.len() < KAFKA_POSITION_LOOKUP_CONCURRENCY {
            let Some((topic, partition)) = remaining.next() else {
                break;
            };
            jobs.spawn(fetch_partition_watermark(
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
        let ((topic, partition), low, high) = result.map_err(|error| {
            ConnectorError::Internal(format!("Kafka watermark worker failed: {error}"))
        })??;
        low_watermarks.insert((topic.clone(), partition), low);
        high_watermarks.insert((topic, partition), high);
    }

    Ok((low_watermarks, high_watermarks))
}

pub(super) async fn fetch_partition_watermark(
    blocking_tasks: KafkaBlockingTasks,
    consumer: Arc<StreamConsumer<LaminarConsumerContext>>,
    topic: String,
    partition: i32,
    deadline: tokio::time::Instant,
) -> Result<((String, i32), i64, i64), ConnectorError> {
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
        Ok(Ok(Ok((low, high))))
            if (0..i64::MAX).contains(&low) && (0..i64::MAX).contains(&high) && low <= high =>
        {
            Ok(((topic, partition), low, high))
        }
        Ok(Ok(Ok((low, high)))) => Err(ConnectorError::ConnectionFailed(format!(
            "Kafka returned invalid watermark range {low}..{high} for '{topic}-{partition}'"
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

pub(super) async fn resolve_timestamp_offsets(
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
