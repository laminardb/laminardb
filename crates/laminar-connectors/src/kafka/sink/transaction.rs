//! Blocking librdkafka transaction steps shared by the exactly-once committer and the cluster
//! keyed writer.

use std::time::Instant;

use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::ClientConfig;

use crate::connector::ConnectorTaskAdmission;
use crate::error::ConnectorError;

/// How an attempt to end a transaction finished.
pub(super) enum TransactionEnd {
    Committed,
    /// Aborted for this reason; nothing from the transaction is visible.
    Aborted(String),
    /// The transaction may or may not have committed; the producer must be replaced.
    Unknown(String),
}

/// Run a blocking librdkafka call of a live sink off the async workers within `deadline`, as a
/// task the sink's terminal tracker waits for.
pub(super) async fn run_blocking<T: Send + 'static>(
    admission: &ConnectorTaskAdmission,
    deadline: tokio::time::Instant,
    work: impl FnOnce(Instant) -> Result<T, ConnectorError> + Send + 'static,
) -> Result<T, ConnectorError> {
    let guard = admission
        .track()
        .ok_or_else(|| ConnectorError::InvalidState {
            expected: "live Kafka sink".into(),
            actual: "sink generation is closed".into(),
        })?;
    blocking_until(deadline, move |deadline| {
        let _guard = guard;
        work(deadline)
    })
    .await
}

/// Run a blocking librdkafka call off the async workers within `deadline`. The call gets the
/// same deadline for its own timeout, so an abandoned call still finishes.
pub(super) async fn blocking_until<T: Send + 'static>(
    deadline: tokio::time::Instant,
    work: impl FnOnce(Instant) -> Result<T, ConnectorError> + Send + 'static,
) -> Result<T, ConnectorError> {
    let blocking_deadline = deadline.into_std();
    let task = tokio::task::spawn_blocking(move || work(blocking_deadline));
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(ConnectorError::outcome_unknown(
            format!("Kafka transaction worker failed: {error}"),
            true,
        )),
        Err(_) => Err(ConnectorError::outcome_unknown(
            "Kafka transaction step exceeded its deadline",
            true,
        )),
    }
}

/// Create a producer from `config`, which names a `transactional.id`, and initialize its
/// transactions: the broker fences every earlier producer under that id and aborts the
/// transaction it left open.
pub(super) fn transactional_producer(
    config: &ClientConfig,
    deadline: Instant,
) -> Result<FutureProducer, ConnectorError> {
    let producer: FutureProducer = config.create().map_err(|error| {
        ConnectorError::ConfigurationError(format!("create Kafka transactional producer: {error}"))
    })?;
    init_transactions(&producer, deadline)?;
    Ok(producer)
}

fn init_transactions(producer: &FutureProducer, deadline: Instant) -> Result<(), ConnectorError> {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(ConnectorError::TransactionError(
                "Kafka init_transactions did not finish before the deadline".into(),
            ));
        }
        match producer.init_transactions(left) {
            Ok(()) => return Ok(()),
            Err(KafkaError::Transaction(error)) if error.is_retriable() && !error.is_fatal() => {}
            Err(error) => {
                return Err(ConnectorError::TransactionError(format!(
                    "Kafka init_transactions: {error}"
                )));
            }
        }
    }
}

/// Commit the open transaction, aborting it when the broker requires that.
pub(super) fn commit(producer: &FutureProducer, deadline: Instant) -> TransactionEnd {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return TransactionEnd::Unknown("commit_transaction did not finish".into());
        }
        match producer.commit_transaction(left) {
            Ok(()) => return TransactionEnd::Committed,
            Err(KafkaError::Transaction(error)) if error.txn_requires_abort() => {
                return abort(producer, deadline, &format!("commit_transaction: {error}"));
            }
            Err(KafkaError::Transaction(error)) if error.is_retriable() && !error.is_fatal() => {}
            Err(error) => return TransactionEnd::Unknown(format!("commit_transaction: {error}")),
        }
    }
}

pub(super) fn abort(producer: &FutureProducer, deadline: Instant, reason: &str) -> TransactionEnd {
    let left = deadline.saturating_duration_since(Instant::now());
    match producer.abort_transaction(left) {
        Ok(()) => TransactionEnd::Aborted(reason.to_string()),
        Err(error) => TransactionEnd::Unknown(format!("{reason}; abort_transaction: {error}")),
    }
}
