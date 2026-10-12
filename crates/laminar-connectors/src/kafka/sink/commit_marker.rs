//! The exactly-once cursor: one marker record per committed checkpoint, written in the same
//! transaction as that checkpoint's output to a compacted commit topic, keyed by the sink's
//! coordinated-commit namespace.

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::connector::CoordinatedCommitCursor;
use crate::error::ConnectorError;

const MARKER_VERSION: u32 = 1;
/// Initial number of trailing records scanned for the newest marker; doubled until found.
const INITIAL_SCAN_WINDOW: i64 = 64;

/// The value of one marker record.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct CommitMarker {
    pub(super) version: u32,
    pub(super) checkpoint_id: u64,
    pub(super) fencing_token: u64,
    /// Hex SHA-256 of the exact coordinated batch the transaction published.
    pub(super) batch_fingerprint: String,
    pub(super) topic: String,
    pub(super) partition_count: i32,
}

impl CommitMarker {
    pub(super) fn new(
        cursor: CoordinatedCommitCursor,
        fingerprint: [u8; 32],
        topic: &str,
        partition_count: i32,
    ) -> Self {
        Self {
            version: MARKER_VERSION,
            checkpoint_id: cursor.checkpoint_id,
            fencing_token: cursor.fencing_token,
            batch_fingerprint: fingerprint.iter().fold(
                String::with_capacity(fingerprint.len() * 2),
                |mut hex, byte| {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{byte:02x}");
                    hex
                },
            ),
            topic: topic.to_owned(),
            partition_count,
        }
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, ConnectorError> {
        serde_json::to_vec(self)
            .map_err(|error| ConnectorError::Internal(format!("encode commit marker: {error}")))
    }

    fn decode(bytes: &[u8]) -> Result<Self, ConnectorError> {
        let marker: Self = serde_json::from_slice(bytes).map_err(|error| {
            ConnectorError::TransactionError(format!("unreadable Kafka commit marker: {error}"))
        })?;
        if marker.version != MARKER_VERSION || marker.fencing_token == 0 {
            return Err(ConnectorError::TransactionError(format!(
                "unsupported Kafka commit marker version {} or zero fencing token",
                marker.version
            )));
        }
        Ok(marker)
    }

    pub(super) fn cursor(&self) -> CoordinatedCommitCursor {
        CoordinatedCommitCursor {
            checkpoint_id: self.checkpoint_id,
            fencing_token: self.fencing_token,
        }
    }
}

/// Where markers for one namespace live.
#[derive(Debug, Clone)]
pub(super) struct MarkerLocation {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) key: String,
}

impl MarkerLocation {
    /// The commit-topic partition for `key`, chosen like a keyed Kafka record.
    pub(super) fn resolve(
        base: &ClientConfig,
        topic: &str,
        key: String,
        deadline: std::time::Instant,
    ) -> Result<Self, ConnectorError> {
        let consumer = marker_consumer(base, "read_committed")?;
        let metadata = consumer
            .fetch_metadata(Some(topic), remaining(deadline)?)
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "Kafka commit topic '{topic}' metadata: {error}"
                ))
            })?;
        let partitions = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .filter(|candidate| candidate.error().is_none())
            .map(|candidate| candidate.partitions().len())
            .filter(|count| *count > 0)
            .and_then(|count| i32::try_from(count).ok())
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "Kafka commit topic '{topic}' does not exist"
                ))
            })?;
        let partition = crate::kafka::partitioner::KafkaPartitioner::partition(
            &mut crate::kafka::partitioner::KeyHashPartitioner::new(),
            Some(key.as_bytes()),
            partitions,
        )
        .ok_or_else(|| ConnectorError::Internal("commit marker partition".into()))?;
        Ok(Self {
            topic: topic.to_owned(),
            partition,
            key,
        })
    }
}

/// How far a marker read must reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MarkerRead {
    /// After this sink's `init_transactions`: wait until every transaction open when the read
    /// started has resolved, so the answer is the committed cursor.
    Authoritative,
    /// Before fencing: read what is committed now. It can only lag the true cursor.
    Advisory,
}

/// Read the newest committed marker for `location.key`.
///
/// Must run after the caller's transactional producer has completed `init_transactions`, so no
/// transaction of this sink is still open. The read waits until the `read_committed` stable offset
/// covers everything written before it started, so another writer's open transaction can delay
/// the answer but never hide a committed marker. Any failure is an error, never "no marker".
pub(super) fn read_newest_marker(
    base: &ClientConfig,
    location: &MarkerLocation,
    read: MarkerRead,
    deadline: std::time::Instant,
) -> Result<Option<CommitMarker>, ConnectorError> {
    let committed = marker_consumer(base, "read_committed")?;
    let (low, high) = if read == MarkerRead::Advisory {
        watermarks(&committed, location, "stable offset lookup", deadline)?
    } else {
        let uncommitted = marker_consumer(base, "read_uncommitted")?;
        watermarks(&uncommitted, location, "high watermark lookup", deadline)?
    };
    if read == MarkerRead::Authoritative {
        wait_for_stable_offset(&committed, location, high, deadline)?;
    }
    let mut window = INITIAL_SCAN_WINDOW;
    loop {
        let start = high.saturating_sub(window).max(low);
        if let Some(marker) = scan_marker(&committed, location, start, high, deadline)? {
            return Ok(Some(marker));
        }
        if start == low {
            return Ok(None);
        }
        window = window.saturating_mul(2);
    }
}

/// Wait until no open transaction remains below `high`, so the committed view is complete.
fn wait_for_stable_offset(
    committed: &BaseConsumer,
    location: &MarkerLocation,
    high: i64,
    deadline: std::time::Instant,
) -> Result<(), ConnectorError> {
    loop {
        let (_, stable) = watermarks(committed, location, "stable offset lookup", deadline)?;
        if stable >= high {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(ConnectorError::outcome_unknown(
                format!(
                    "Kafka commit topic '{}' partition {} has an open transaction below offset {high}; \
                     the committed cursor cannot be read before the deadline",
                    location.topic, location.partition
                ),
                true,
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn scan_marker(
    consumer: &BaseConsumer,
    location: &MarkerLocation,
    start: i64,
    end: i64,
    deadline: std::time::Instant,
) -> Result<Option<CommitMarker>, ConnectorError> {
    if start >= end {
        return Ok(None);
    }
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&location.topic, location.partition, Offset::Offset(start))
        .map_err(|error| marker_error(location, "assign", &error))?;
    consumer
        .assign(&assignment)
        .map_err(|error| marker_error(location, "assign", &error))?;
    let mut newest = None;
    loop {
        let Some(message) = consumer.poll(remaining(deadline)?) else {
            continue;
        };
        match message {
            Ok(message) => {
                if message.key() == Some(location.key.as_bytes()) {
                    let value = message.payload().ok_or_else(|| {
                        ConnectorError::TransactionError(format!(
                            "Kafka commit marker at offset {} has no value",
                            message.offset()
                        ))
                    })?;
                    newest = Some(CommitMarker::decode(value)?);
                }
                if message.offset() + 1 >= end {
                    return Ok(newest);
                }
            }
            Err(KafkaError::PartitionEOF(_)) => return Ok(newest),
            Err(error) => return Err(marker_error(location, "read", &error)),
        }
    }
}

/// Partition watermarks, retrying the transient leadership errors of a new topic or a broker
/// failover until `deadline`.
fn watermarks(
    consumer: &BaseConsumer,
    location: &MarkerLocation,
    step: &str,
    deadline: std::time::Instant,
) -> Result<(i64, i64), ConnectorError> {
    loop {
        match consumer.fetch_watermarks(&location.topic, location.partition, remaining(deadline)?) {
            Ok(watermarks) => return Ok(watermarks),
            Err(error)
                if matches!(
                    error.rdkafka_error_code(),
                    Some(
                        RDKafkaErrorCode::NotLeaderForPartition
                            | RDKafkaErrorCode::LeaderNotAvailable
                            | RDKafkaErrorCode::RequestTimedOut
                            | RDKafkaErrorCode::OperationTimedOut
                    )
                ) && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(marker_error(location, step, &error)),
        }
    }
}

fn marker_consumer(base: &ClientConfig, isolation: &str) -> Result<BaseConsumer, ConnectorError> {
    let mut config = base.clone();
    config
        .set(
            "group.id",
            format!("laminardb-commit-reader-{}", uuid::Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "error")
        .set("isolation.level", isolation);
    config.create().map_err(|error| {
        ConnectorError::ConnectionFailed(format!("create Kafka commit-marker reader: {error}"))
    })
}

fn remaining(deadline: std::time::Instant) -> Result<Duration, ConnectorError> {
    let left = deadline.saturating_duration_since(std::time::Instant::now());
    if left.is_zero() {
        return Err(ConnectorError::TransactionError(
            "Kafka commit-marker read exceeded its deadline".into(),
        ));
    }
    Ok(left)
}

fn marker_error(location: &MarkerLocation, step: &str, error: &KafkaError) -> ConnectorError {
    let detail = format!(
        "Kafka commit topic '{}' partition {} {step} failed: {error}",
        location.topic, location.partition
    );
    match error.rdkafka_error_code() {
        Some(
            RDKafkaErrorCode::UnknownTopicOrPartition
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed,
        ) => ConnectorError::ConfigurationError(detail),
        _ => ConnectorError::TransactionError(detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_round_trip_and_reject_unknown_versions() {
        let cursor = CoordinatedCommitCursor {
            checkpoint_id: 7,
            fencing_token: 3,
        };
        let marker = CommitMarker::new(cursor, [0xab; 32], "out", 6);
        let decoded = CommitMarker::decode(&marker.encode().unwrap()).unwrap();
        assert_eq!(decoded, marker);
        assert_eq!(decoded.cursor(), cursor);
        assert_eq!(decoded.batch_fingerprint.len(), 64);

        let mut future = marker.clone();
        future.version = 2;
        assert!(CommitMarker::decode(&future.encode().unwrap()).is_err());
        assert!(CommitMarker::decode(b"{}").is_err());
    }
}
