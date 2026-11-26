//! The cluster writer of a Kafka upsert topic that several processes write by key.
//!
//! librdkafka keeps retrying a queued record until its delivery timeout, long after the writer's
//! lease deadline, so a departed process's record could otherwise land after its successor wrote
//! the same key. Each process therefore writes through its own transactional producer, one
//! transaction per batch, under a `transactional.id` naming the sink, node and boot incarnation.
//! Before a successor takes over its keys, the leader initializes transactions under that id: the
//! broker aborts the departed process's open transaction and rejects whatever it still sends. A
//! restart is another incarnation, so a fence never reaches a live process.

use laminar_core::checkpoint::CheckpointParticipant;
use rdkafka::producer::FutureProducer;

use super::transaction::{
    self, blocking_until, run_blocking, transactional_producer, TransactionEnd,
};
use crate::config::ConnectorConfig;
use crate::connector::{ConnectorTaskAdmission, DestinationWriter, WriteResult};
use crate::error::ConnectorError;
use crate::kafka::sink_config::KafkaSinkConfig;

const TRANSACTIONAL_ID_PREFIX: &str = "laminardb-writer-";

pub(super) fn transactional_id(writer: &DestinationWriter) -> String {
    format!(
        "{TRANSACTIONAL_ID_PREFIX}{}-{:016x}-{}",
        writer.sink_id, writer.process.node_id, writer.process.boot_incarnation
    )
}

pub(super) async fn open(
    config: &KafkaSinkConfig,
    writer: &DestinationWriter,
    admission: &ConnectorTaskAdmission,
) -> Result<FutureProducer, ConnectorError> {
    let producer_config = config.transactional_rdkafka_config(&transactional_id(writer));
    let deadline = tokio::time::Instant::now() + config.transaction_timeout;
    run_blocking(admission, deadline, move |deadline| {
        transactional_producer(&producer_config, deadline)
    })
    .await
}

/// The Kafka sink's [`SinkWriterFence`](crate::registry::SinkWriterFence).
pub(crate) async fn fence_departed_writers(
    config: ConnectorConfig,
    sink_id: String,
    departed: Vec<CheckpointParticipant>,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let config = KafkaSinkConfig::from_config(&config)?;
    if !config.is_key_partitioned() {
        return Ok(());
    }
    let producer_configs = departed
        .into_iter()
        .map(|process| {
            let writer = DestinationWriter {
                sink_id: sink_id.clone(),
                process,
            };
            config.transactional_rdkafka_config(&transactional_id(&writer))
        })
        .collect::<Vec<_>>();
    // A cold start fences every process of the previous run, all within one deadline.
    let mut fences = tokio::task::JoinSet::new();
    for producer_config in producer_configs {
        fences.spawn(blocking_until(deadline, move |deadline| {
            transactional_producer(&producer_config, deadline).map(drop)
        }));
    }
    while let Some(fenced) = fences.join_next().await {
        fenced.map_err(|error| {
            ConnectorError::Internal(format!("Kafka writer fence task failed: {error}"))
        })??;
    }
    Ok(())
}

/// Commit the batch's transaction when every record was delivered, and abort it otherwise.
pub(super) async fn finish(
    producer: &FutureProducer,
    delivered: Result<WriteResult, ConnectorError>,
    config: &KafkaSinkConfig,
    admission: &ConnectorTaskAdmission,
) -> Result<WriteResult, ConnectorError> {
    let producer = producer.clone();
    let deadline = tokio::time::Instant::now() + config.transaction_timeout;
    let reason = delivered.as_ref().err().map(ToString::to_string);
    let end = run_blocking(admission, deadline, move |deadline| {
        Ok(match reason {
            None => transaction::commit(&producer, deadline),
            Some(reason) => transaction::abort(&producer, deadline, &reason),
        })
    })
    .await?;
    match (end, delivered) {
        (TransactionEnd::Committed, delivered)
        | (TransactionEnd::Aborted(_), delivered @ Err(_)) => delivered,
        (TransactionEnd::Aborted(reason), Ok(_)) => Err(ConnectorError::WriteError(format!(
            "Kafka transaction aborted ({reason}); the batch was not committed"
        ))),
        (TransactionEnd::Unknown(detail), _) => Err(ConnectorError::outcome_unknown(
            format!("Kafka keyed writer transaction: {detail}"),
            true,
        )),
    }
}
