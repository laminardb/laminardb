//! Persistence certainty, error scope, retry policy, and batch failure accounting.

use std::time::{Duration, Instant};

use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::error::{ConnectorError, SerdeError};

/// One queue-full retry deadline is shared by every record sent through a
/// producer phase. A non-record failure stops new enqueue work.
pub(super) const QUEUE_RETRY_TIMEOUT: Duration = Duration::from_millis(500);
const QUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const WRITE_TIMEOUT_HEADROOM: Duration = Duration::from_secs(5);

pub(super) fn queue_retry_delay(deadline: Instant, now: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(now);
    (!remaining.is_zero()).then_some(remaining.min(QUEUE_RETRY_INTERVAL))
}

fn delivery_outcome_unknown(
    operation: &str,
    detail: impl std::fmt::Display,
    retryable: bool,
) -> ConnectorError {
    ConnectorError::outcome_unknown(
        format!(
            "Kafka {operation} was dispatched but its external outcome is not fully known: {detail}"
        ),
        retryable,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KafkaFailureCertainty {
    DefinitelyNotPersisted,
    OutcomeUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KafkaFailureScope {
    Record,
    Infrastructure,
    Connector,
}

#[derive(Debug)]
pub(super) struct KafkaFailure {
    pub(super) certainty: KafkaFailureCertainty,
    pub(super) scope: KafkaFailureScope,
    pub(super) retryable: bool,
    pub(super) detail: String,
}

impl KafkaFailure {
    /// `FutureProducer::send_result` returned the record, proving that this
    /// attempt never entered librdkafka's queue.
    pub(super) fn enqueue(error: &KafkaError, operation: &str) -> Self {
        let (scope, retryable) = kafka_error_policy(error);
        Self {
            certainty: KafkaFailureCertainty::DefinitelyNotPersisted,
            scope,
            retryable,
            detail: format!("{operation} enqueue failed before dispatch: {error}"),
        }
    }

    /// rdkafka 0.39's `FutureProducer` discards native
    /// `rd_kafka_message_status` when it creates an owned delivery result.
    /// Error codes alone cannot prove non-persistence after driver retries.
    pub(super) fn delivery(error: &KafkaError, operation: &str) -> Self {
        let (scope, retryable) = kafka_error_policy(error);
        Self {
            certainty: KafkaFailureCertainty::OutcomeUnknown,
            scope,
            retryable,
            detail: format!("{operation} delivery failed: {error}"),
        }
    }

    pub(super) fn canceled(operation: &str) -> Self {
        Self {
            certainty: KafkaFailureCertainty::OutcomeUnknown,
            scope: KafkaFailureScope::Infrastructure,
            retryable: true,
            detail: format!("{operation} delivery canceled because the producer was dropped"),
        }
    }

    pub(super) fn dlq_eligible(&self) -> bool {
        self.certainty == KafkaFailureCertainty::DefinitelyNotPersisted
            && self.scope == KafkaFailureScope::Record
            && !self.retryable
    }
}

/// Error scope and retryability are independent of persistence certainty. This
/// is deliberately a positive transient list: fatal, unknown, and future codes
/// fail closed instead of creating an unbounded restart loop.
fn kafka_error_policy(error: &KafkaError) -> (KafkaFailureScope, bool) {
    match error.rdkafka_error_code() {
        Some(
            RDKafkaErrorCode::KeySerialization
            | RDKafkaErrorCode::ValueSerialization
            | RDKafkaErrorCode::MessageSizeTooLarge
            | RDKafkaErrorCode::InvalidTimestamp
            | RDKafkaErrorCode::InvalidRecord,
        ) => (KafkaFailureScope::Record, false),
        Some(
            RDKafkaErrorCode::BrokerDestroy
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::Resolve
            | RDKafkaErrorCode::MessageTimedOut
            | RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::QueueFull
            | RDKafkaErrorCode::ISRInsufficient
            | RDKafkaErrorCode::TimedOutQueue
            | RDKafkaErrorCode::WaitCache
            | RDKafkaErrorCode::Interrupted
            | RDKafkaErrorCode::Retry
            | RDKafkaErrorCode::PurgeQueue
            | RDKafkaErrorCode::PurgeInflight
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::UnknownTopicOrPartition
            | RDKafkaErrorCode::LeaderNotAvailable
            | RDKafkaErrorCode::NotLeaderForPartition
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::BrokerNotAvailable
            | RDKafkaErrorCode::ReplicaNotAvailable
            | RDKafkaErrorCode::NetworkException
            | RDKafkaErrorCode::NotEnoughReplicas
            | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
            | RDKafkaErrorCode::NotController
            | RDKafkaErrorCode::KafkaStorageError
            | RDKafkaErrorCode::ReassignmentInProgress
            | RDKafkaErrorCode::FencedLeaderEpoch
            | RDKafkaErrorCode::UnknownLeaderEpoch
            | RDKafkaErrorCode::StaleBrokerEpoch
            | RDKafkaErrorCode::EligibleLeadersNotAvailable
            | RDKafkaErrorCode::ThrottlingQuotaExceeded
            | RDKafkaErrorCode::UnknownTopicId,
        ) => (KafkaFailureScope::Infrastructure, true),
        _ => (KafkaFailureScope::Connector, false),
    }
}

pub(super) fn unresolved_delivery_error(
    operation: &str,
    total: usize,
    applied: usize,
    definitely_not_persisted: usize,
    ambiguous: usize,
    first_error: Option<String>,
    retryable: bool,
) -> ConnectorError {
    let detail = format!(
        "{definitely_not_persisted} definitely not persisted, {ambiguous} outcome unknown, \
         {applied} already applied out of {total}; first error: {}",
        first_error.unwrap_or_else(|| "unknown".into())
    );
    if ambiguous > 0 || applied > 0 {
        delivery_outcome_unknown(operation, detail, retryable)
    } else if retryable {
        ConnectorError::WriteError(format!("Kafka {operation} failed: {detail}"))
    } else {
        ConnectorError::ConfigurationError(format!("Kafka {operation} was rejected: {detail}"))
    }
}

pub(super) fn record_failure(
    failure: &KafkaFailure,
    count: usize,
    definitely_not_persisted: &mut usize,
    ambiguous: &mut usize,
    first_error: &mut Option<String>,
    retryable: &mut bool,
) {
    match failure.certainty {
        KafkaFailureCertainty::DefinitelyNotPersisted => *definitely_not_persisted += count,
        KafkaFailureCertainty::OutcomeUnknown => *ambiguous += count,
    }
    *retryable &= failure.retryable;
    first_error.get_or_insert_with(|| failure.detail.clone());
}

pub(super) fn kafka_write_timeout(delivery_timeout: Duration) -> Duration {
    delivery_timeout
        .saturating_add(QUEUE_RETRY_TIMEOUT)
        .saturating_add(QUEUE_RETRY_TIMEOUT)
        .saturating_add(WRITE_TIMEOUT_HEADROOM)
}

pub(super) fn producer_creation_error(role: &str, error: &KafkaError) -> ConnectorError {
    ConnectorError::ConfigurationError(format!("failed to create Kafka {role} producer: {error}"))
}

pub(super) fn validate_payload_cardinality(
    expected: usize,
    actual: usize,
) -> Result<(), ConnectorError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ConnectorError::Serde(SerdeError::RecordCountMismatch {
            expected,
            got: actual,
        }))
    }
}
