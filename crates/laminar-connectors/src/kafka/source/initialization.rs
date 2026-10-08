//! Read-only, bounded discovery of a new source's global numeric initial cursor.

use rdkafka::consumer::BaseConsumer;

use super::checkpoint::attach_partition_baselines;
use super::{
    consumer_creation_error, fetch_error, invalid_response, kafka_input_channels, topic_error,
    ConnectorConfig, ConnectorError, ConnectorState, Consumer, KafkaPartitionBaselines,
    KafkaPartitionSet, KafkaSource, KafkaSourceConfig, OffsetTracker, SourceCheckpoint,
    StartupMode, TopicSubscription,
};

const INITIAL_POSITION_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_INITIAL_TOPICS: usize = 64;
const MAX_INITIAL_PARTITIONS: usize = 4096;
// A cancelled metadata future may leave native work until its bounded calls and Drop finish.
// Retain this permit in that work so retries cannot accumulate detached metadata clients.
static INITIALIZATION_SLOT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

impl KafkaSource {
    pub(super) async fn resolve_initial_position_inner(
        &mut self,
        config: &ConnectorConfig,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        self.inspect_initial_position_inner(config, None).await
    }

    pub(super) async fn inspect_initial_position_inner(
        &mut self,
        config: &ConnectorConfig,
        sealed: Option<&SourceCheckpoint>,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: "Created source without an active reader".into(),
                actual: self.state.to_string(),
            });
        }
        let kafka_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            KafkaSourceConfig::from_config(config)?
        };
        let topics = validate_initialization_config(&kafka_config)?;
        let source_name = config
            .get("laminar.source.name")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "Kafka initialization requires the canonical catalog source name".into(),
                )
            })?
            .to_owned();
        let sealed_baselines = sealed
            .map(|checkpoint| validate_sealed_position(checkpoint, &source_name, &topics))
            .transpose()?;
        let deadline = tokio::time::Instant::now() + INITIAL_POSITION_BUDGET;
        let permit = tokio::time::timeout_at(deadline, INITIALIZATION_SLOT.acquire())
            .await
            .map_err(|_| ConnectorError::Timeout(10_000))?
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
        let lookup_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
        // Creation and final native-client drop both occur inside the existing tracked blocking
        // task. Cancellation cannot leak a consumer or run its blocking Drop on a Tokio worker.
        // No subscription, assignment, record poll, group join, commit or schema discovery occurs.
        let task = self.blocking_tasks.run(move || {
            let _permit = permit;
            let deadline = std::time::Instant::now() + lookup_budget;
            let mut client_config = kafka_config.to_rdkafka_config();
            client_config.set("enable.auto.commit", "false");
            client_config.set("enable.auto.offset.store", "false");
            client_config.set("allow.auto.create.topics", "false");
            let consumer: BaseConsumer = client_config
                .create()
                .map_err(|error| consumer_creation_error(&error))?;
            let inventory = fetch_initial_inventory(&consumer, topics, deadline)?;
            let mut baselines = KafkaPartitionBaselines::with_capacity(inventory.len());
            if sealed_baselines.as_ref().is_some_and(|sealed| {
                sealed.len() != inventory.len() || inventory.iter().any(|partition| !sealed.contains_key(partition))
            }) {
                return Err(ConnectorError::ConfigurationError("sealed Kafka initialization inventory changed; the sealed topology cursor cannot be replaced".into()));
            }
            // The boundary is an explicit vector of broker low/high watermarks. It is not a
            // cross-partition transaction timestamp or a claim that pre-cut input was processed.
            for (topic, partition) in &inventory {
                let (low, high) = consumer
                    .fetch_watermarks(topic, *partition, remaining(deadline)?)
                    .map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "Kafka initial watermark lookup failed for '{topic}-{partition}': {e}"
                        ))
                    })?;
                let next = if let Some(sealed) = &sealed_baselines {
                    let next = sealed[&(topic.clone(), *partition)];
                    validate_sealed_next_offset(next, low, high)?;
                    next
                } else { initial_next_offset(&kafka_config.startup_mode, low, high)? };
                baselines.insert((topic.clone(), *partition), next);
            }
            let mut checkpoint = OffsetTracker::new().to_checkpoint_for_partitions(
                inventory
                    .iter()
                    .map(|(topic, partition)| (topic.as_str(), *partition)),
            );
            attach_partition_baselines(&mut checkpoint, &baselines, &inventory);
            checkpoint.set_input_channels(kafka_input_channels(&source_name, &inventory)?)?;
            Ok(checkpoint)
        });
        tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| ConnectorError::Timeout(10_000))?
            .map_err(|e| {
                ConnectorError::Internal(format!("Kafka initialization worker failed: {e}"))
            })?
    }
}

fn fetch_initial_inventory(
    consumer: &BaseConsumer,
    topics: Vec<String>,
    deadline: std::time::Instant,
) -> Result<KafkaPartitionSet, ConnectorError> {
    let mut inventory = KafkaPartitionSet::new();
    for topic in topics {
        let metadata = consumer
            .fetch_metadata(Some(&topic), remaining(deadline)?)
            .map_err(|e| fetch_error(&topic, &e))?;
        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|m| m.name() == topic)
            .ok_or_else(|| invalid_response(&topic, "metadata omitted the topic"))?;
        if let Some(error) = topic_metadata.error() {
            return Err(topic_error(&topic, error.into()));
        }
        if topic_metadata.partitions().is_empty()
            || inventory
                .len()
                .saturating_add(topic_metadata.partitions().len())
                > MAX_INITIAL_PARTITIONS
        {
            return Err(invalid_response(
                &topic,
                "initial inventory must contain 1..=4096 partitions in total",
            ));
        }
        for partition in topic_metadata.partitions() {
            if let Some(error) = partition.error() {
                return Err(topic_error(&topic, error.into()));
            }
            if partition.id() < 0
                || usize::try_from(partition.id())
                    .map_or(true, |id| id >= topic_metadata.partitions().len())
                || !inventory.insert((topic.clone(), partition.id()))
            {
                return Err(invalid_response(
                    &topic,
                    "invalid or duplicate partition identity",
                ));
            }
        }
    }
    Ok(inventory)
}

fn remaining(deadline: std::time::Instant) -> Result<std::time::Duration, ConnectorError> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(ConnectorError::Timeout(10_000));
    }
    Ok(remaining)
}

fn validate_sealed_position(
    checkpoint: &SourceCheckpoint,
    source_name: &str,
    topics: &[String],
) -> Result<KafkaPartitionBaselines, ConnectorError> {
    if checkpoint.offsets().is_empty() || checkpoint.offsets().len() > MAX_INITIAL_PARTITIONS {
        return Err(ConnectorError::ConfigurationError(
            "sealed Kafka cursor exceeds its 1..=4096 partition bound".into(),
        ));
    }
    let baselines = super::decode_partition_baselines(checkpoint)?;
    if checkpoint.assignment_version().is_some()
        || checkpoint.metadata().get("connector").map(String::as_str) != Some("kafka")
        || checkpoint
            .metadata()
            .get("checkpoint.version")
            .map(String::as_str)
            != Some("2")
        || baselines.is_empty()
        || baselines.len() > MAX_INITIAL_PARTITIONS
        || baselines.len() != checkpoint.offsets().len()
        || baselines
            .keys()
            .any(|(topic, _)| topics.binary_search(topic).is_err())
    {
        return Err(ConnectorError::ConfigurationError("invalid sealed Kafka initialization cursor/ABI; processed offsets and assignment ownership are not new-source positions".into()));
    }
    let inventory = baselines.keys().cloned().collect::<KafkaPartitionSet>();
    if checkpoint.input_channels() != Some(kafka_input_channels(source_name, &inventory)?.as_ref())
    {
        return Err(ConnectorError::ConfigurationError(
            "sealed Kafka initialization channels differ from the exact source inventory".into(),
        ));
    }
    Ok(baselines)
}

fn validate_sealed_next_offset(next: i64, low: i64, high: i64) -> Result<(), ConnectorError> {
    initial_next_offset(&StartupMode::Earliest, low, high)?;
    if next < low || next > high {
        return Err(ConnectorError::ConfigurationError(format!("sealed Kafka next position {next} is outside retained range {low}..{high}; never reset the sealed boundary")));
    }
    Ok(())
}

fn validate_initialization_config(
    config: &KafkaSourceConfig,
) -> Result<Vec<String>, ConnectorError> {
    config.validate()?;
    if !matches!(
        config.startup_mode,
        StartupMode::Earliest | StartupMode::Latest
    ) {
        return Err(ConnectorError::ConfigurationError(
            "sealed Kafka topology initialization supports explicit topics with earliest or latest; group offsets, timestamp and specific-offset modes require separate contracts".into(),
        ));
    }
    let TopicSubscription::Topics(topics) = &config.subscription else {
        return Err(ConnectorError::ConfigurationError(
            "sealed Kafka initialization requires an explicit topic inventory".into(),
        ));
    };
    let mut topics = topics.clone();
    topics.sort_unstable();
    if topics.is_empty()
        || topics.len() > MAX_INITIAL_TOPICS
        || !topics.windows(2).all(|p| p[0] < p[1])
        || topics
            .iter()
            .any(|t| t.is_empty() || t.len() > 249 || t.contains(':'))
    {
        return Err(ConnectorError::ConfigurationError(
            "Kafka initialization requires 1..=64 unique explicit topic names".into(),
        ));
    }
    Ok(topics)
}

fn initial_next_offset(mode: &StartupMode, low: i64, high: i64) -> Result<i64, ConnectorError> {
    if !(0..i64::MAX).contains(&low) || !(0..i64::MAX).contains(&high) || low > high {
        return Err(ConnectorError::ConnectionFailed(format!(
            "invalid Kafka initial watermark range {low}..{high}"
        )));
    }
    match mode {
        StartupMode::Earliest => Ok(low),
        StartupMode::Latest => Ok(high),
        _ => Err(ConnectorError::ConfigurationError(
            "unsupported Kafka initialization mode".into(),
        )),
    }
}

#[cfg(test)]
mod startup_tests;
#[cfg(test)]
mod tests;
