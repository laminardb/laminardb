//! Real-time recency for Kafka sources.
//!
//! The upstream's end is each partition's end offset under the source's own isolation level: the
//! high watermark, or the last stable offset under `read_committed`. A checkpoint holds a
//! partition's records before that end when its next-to-read reaches it. A checkpoint records the
//! last offset it delivered, not the consumer position, so transaction markers or aborted records
//! can lie between the two. Then a read of that range, under the same isolation level, decides:
//! reaching the end of the partition without a record means nothing more could be delivered.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use super::checkpoint::decode_partition_baselines;
use super::{fetch_error, invalid_response, topic_error, KafkaSourceConfig, TopicSubscription};
use crate::checkpoint::SourceCheckpoint;
use crate::connector::{RecencyTarget, SourceRecency};
use crate::error::ConnectorError;
use crate::kafka::offsets::OffsetTracker;

/// Native clients that recency probes may hold at once. A cancelled probe keeps its permit until
/// its bounded calls return, so abandoned probes cannot accumulate clients.
static PROBE_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);

type Partition = (String, i32);

/// The recency probe of a source subscribed to an explicit topic list; a pattern has no stable
/// partition inventory to compare checkpoints with.
pub(super) fn kafka_recency(config: &KafkaSourceConfig) -> Option<Arc<dyn SourceRecency>> {
    let TopicSubscription::Topics(topics) = &config.subscription else {
        return None;
    };
    Some(Arc::new(KafkaRecency {
        client: config.to_rdkafka_config(),
        topics: topics.clone(),
    }))
}

struct KafkaRecency {
    client: ClientConfig,
    topics: Vec<String>,
}

// The client configuration may hold credentials.
impl std::fmt::Debug for KafkaRecency {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaRecency")
            .field("topics", &self.topics)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SourceRecency for KafkaRecency {
    async fn upstream_end(
        &self,
        deadline: Instant,
    ) -> Result<Box<dyn RecencyTarget>, ConnectorError> {
        let client = self.client.clone();
        let topics = self.topics.clone();
        let ends = blocking(deadline, move || end_offsets(&client, &topics, deadline)).await?;
        Ok(Box::new(KafkaRecencyTarget {
            client: self.client.clone(),
            ends,
            tails: parking_lot::Mutex::new(BTreeMap::new()),
        }))
    }
}

/// Every partition's end offsets when a read arrived.
struct KafkaRecencyTarget {
    client: ClientConfig,
    /// Low watermark and end offset of each partition.
    ends: BTreeMap<Partition, (i64, i64)>,
    /// What earlier reads past a checkpoint found; both stay true for later checkpoints.
    tails: parking_lot::Mutex<BTreeMap<Partition, Tail>>,
}

impl std::fmt::Debug for KafkaRecencyTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaRecencyTarget")
            .field("ends", &self.ends)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Tail {
    /// A next-to-read from which no record remains before the end.
    clear_from: Option<i64>,
    /// A record before the end; a next-to-read at or below it is not covered.
    record_at: Option<i64>,
}

/// One partition's coverage by a checkpoint, before any read.
#[derive(Debug, PartialEq, Eq)]
enum Coverage {
    Covered,
    NotCovered,
    /// Next-to-read is below the end; a read from it decides.
    ReadFrom(i64),
}

#[async_trait]
impl RecencyTarget for KafkaRecencyTarget {
    async fn covered_by(
        &self,
        checkpoint: &SourceCheckpoint,
        deadline: Instant,
    ) -> Result<bool, ConnectorError> {
        let next = next_to_read(checkpoint, self.ends.keys())?;
        let mut reads = Vec::new();
        {
            let tails = self.tails.lock();
            for (partition, &(low, end)) in &self.ends {
                let tail = tails.get(partition).copied().unwrap_or_default();
                match coverage(next.get(partition).copied(), low, end, tail) {
                    Coverage::Covered => {}
                    Coverage::NotCovered => return Ok(false),
                    Coverage::ReadFrom(from) => reads.push((partition.clone(), from, end)),
                }
            }
        }
        if reads.is_empty() {
            return Ok(true);
        }
        let client = self.client.clone();
        let found = blocking(deadline, move || read_tails(&client, reads, deadline)).await?;
        let mut tails = self.tails.lock();
        let mut covered = true;
        for (partition, from, record) in found {
            let tail = tails.entry(partition).or_default();
            match record {
                Some(offset) => {
                    tail.record_at = Some(tail.record_at.map_or(offset, |at| at.max(offset)));
                    covered = false;
                }
                None => tail.clear_from = Some(tail.clear_from.map_or(from, |at| at.min(from))),
            }
        }
        Ok(covered)
    }
}

fn coverage(next: Option<i64>, low: i64, end: i64, tail: Tail) -> Coverage {
    match next {
        Some(next) if next >= end => Coverage::Covered,
        Some(next) if tail.record_at.is_some_and(|at| next <= at) => Coverage::NotCovered,
        Some(next) if tail.clear_from.is_some_and(|from| next >= from) => Coverage::Covered,
        Some(next) => Coverage::ReadFrom(next),
        // A partition the checkpoint does not know is covered only while it is empty.
        None if low >= end => Coverage::Covered,
        None => Coverage::NotCovered,
    }
}

/// The next-to-read of each of `partitions` in `checkpoint`: one past the last record it
/// delivered, or its recorded baseline when that is further.
fn next_to_read<'a>(
    checkpoint: &SourceCheckpoint,
    partitions: impl Iterator<Item = &'a Partition>,
) -> Result<BTreeMap<Partition, i64>, ConnectorError> {
    let delivered = OffsetTracker::try_from_checkpoint(checkpoint)?;
    let baselines = decode_partition_baselines(checkpoint)?;
    let mut next = BTreeMap::new();
    for partition in partitions {
        let after = delivered
            .get(&partition.0, partition.1)
            .map(|offset| offset.saturating_add(1));
        if let Some(position) = after.max(baselines.get(partition).copied()) {
            next.insert(partition.clone(), position);
        }
    }
    Ok(next)
}

fn end_offsets(
    client: &ClientConfig,
    topics: &[String],
    deadline: Instant,
) -> Result<BTreeMap<Partition, (i64, i64)>, ConnectorError> {
    let consumer = reader(client)?;
    let mut ends = BTreeMap::new();
    for topic in topics {
        let metadata = consumer
            .fetch_metadata(Some(topic), remaining(deadline)?)
            .map_err(|error| fetch_error(topic, &error))?;
        let described = metadata
            .topics()
            .iter()
            .find(|described| described.name() == topic)
            .ok_or_else(|| invalid_response(topic, "metadata omitted the topic"))?;
        if let Some(error) = described.error() {
            return Err(topic_error(topic, error.into()));
        }
        for partition in described.partitions() {
            let watermarks = consumer
                .fetch_watermarks(topic, partition.id(), remaining(deadline)?)
                .map_err(|error| fetch_error(topic, &error))?;
            ends.insert((topic.clone(), partition.id()), watermarks);
        }
    }
    Ok(ends)
}

/// For each `(partition, from, end)`, the first record in `from..end`, or `None` when the
/// partition ends first. Stops at the first record found: one is enough to refuse coverage.
fn read_tails(
    client: &ClientConfig,
    reads: Vec<(Partition, i64, i64)>,
    deadline: Instant,
) -> Result<Vec<(Partition, i64, Option<i64>)>, ConnectorError> {
    let consumer = reader(client)?;
    let mut found = Vec::with_capacity(reads.len());
    for ((topic, partition), from, end) in reads {
        let record = first_record(&consumer, &topic, partition, from, end, deadline)?;
        let stop = record.is_some();
        found.push(((topic, partition), from, record));
        if stop {
            break;
        }
    }
    Ok(found)
}

fn first_record(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    from: i64,
    end: i64,
    deadline: Instant,
) -> Result<Option<i64>, ConnectorError> {
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, partition, Offset::Offset(from))
        .map_err(|error| fetch_error(topic, &error))?;
    consumer
        .assign(&assignment)
        .map_err(|error| fetch_error(topic, &error))?;
    loop {
        let Some(polled) = consumer.poll(remaining(deadline)?) else {
            continue;
        };
        match polled {
            // Messages of a partition read earlier through this consumer may still be queued.
            Ok(message) if message.topic() != topic || message.partition() != partition => {}
            Err(KafkaError::PartitionEOF(eof)) if eof != partition => {}
            Ok(message) if message.offset() < end => return Ok(Some(message.offset())),
            Ok(_) | Err(KafkaError::PartitionEOF(_)) => return Ok(None),
            Err(error) => return Err(fetch_error(topic, &error)),
        }
    }
}

/// A consumer that reads assigned partitions only: it never joins a group or commits.
fn reader(client: &ClientConfig) -> Result<BaseConsumer, ConnectorError> {
    let mut config = client.clone();
    config
        .set(
            "group.id",
            format!("laminardb-recency-{}", uuid::Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("allow.auto.create.topics", "false");
    config.create().map_err(|error| {
        ConnectorError::ConnectionFailed(format!("create Kafka recency reader: {error}"))
    })
}

fn remaining(deadline: Instant) -> Result<Duration, ConnectorError> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(ConnectorError::ConnectionFailed(
            "Kafka recency probe reached its deadline".into(),
        ));
    }
    Ok(left)
}

/// Run blocking client work under a probe slot, bounded by `deadline`.
async fn blocking<T: Send + 'static>(
    deadline: Instant,
    work: impl FnOnce() -> Result<T, ConnectorError> + Send + 'static,
) -> Result<T, ConnectorError> {
    let permit = tokio::time::timeout_at(deadline.into(), PROBE_SLOTS.acquire())
        .await
        .map_err(|_| ConnectorError::ConnectionFailed("Kafka recency probes are all busy".into()))?
        .map_err(|error| ConnectorError::Internal(error.to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|error| ConnectorError::Internal(format!("Kafka recency probe: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_follows_next_to_read_and_earlier_reads() {
        let none = Tail::default();
        assert_eq!(coverage(Some(10), 0, 10, none), Coverage::Covered);
        assert_eq!(coverage(Some(8), 0, 10, none), Coverage::ReadFrom(8));
        let record = Tail {
            record_at: Some(8),
            ..Tail::default()
        };
        assert_eq!(coverage(Some(8), 0, 10, record), Coverage::NotCovered);
        assert_eq!(coverage(Some(9), 0, 10, record), Coverage::ReadFrom(9));
        let clear = Tail {
            clear_from: Some(9),
            ..Tail::default()
        };
        assert_eq!(coverage(Some(9), 0, 10, clear), Coverage::Covered);
        assert_eq!(coverage(Some(8), 0, 10, clear), Coverage::ReadFrom(8));
        assert_eq!(coverage(None, 4, 4, none), Coverage::Covered);
        assert_eq!(coverage(None, 0, 4, none), Coverage::NotCovered);
    }

    #[test]
    fn next_to_read_takes_delivered_offsets_past_baselines() {
        let mut checkpoint = OffsetTracker::new().to_checkpoint_for_partitions([]);
        checkpoint.set_offset("events:0", "41");
        checkpoint.set_offset(
            super::super::checkpoint::partition_baseline_key("events", 0),
            "7",
        );
        checkpoint.set_offset(
            super::super::checkpoint::partition_baseline_key("events", 1),
            "12",
        );
        let partitions = [
            ("events".to_owned(), 0),
            ("events".to_owned(), 1),
            ("events".to_owned(), 2),
        ];
        let next = next_to_read(&checkpoint, partitions.iter()).unwrap();
        assert_eq!(next[&partitions[0]], 42);
        assert_eq!(next[&partitions[1]], 12);
        assert!(!next.contains_key(&partitions[2]));
    }
}
