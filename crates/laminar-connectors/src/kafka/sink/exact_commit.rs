//! The designated committer for exactly-once Kafka output.
//!
//! One transactional producer per coordinated-commit namespace publishes every participant's
//! staged epoch output together with a commit marker, after the engine's durable Commit
//! decision. Its `transactional.id` derives from the namespace (pipeline identity, deployment
//! UUID, sink id), so a successor committer's `init_transactions` fences the predecessor and
//! resolves any transaction it left open. The marker topic is the external cursor.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use super::commit_marker::{read_newest_marker, CommitMarker, MarkerLocation, MarkerRead};
use super::staged_epoch::{OutputRecord, StagedOutput};
use crate::connector::{
    ConnectorTaskAdmission, CoordinatedCommitBatch, CoordinatedCommitContext,
    CoordinatedCommitCursor, CoordinatedCommitNamespace, CoordinatedCommitter,
};
use crate::error::ConnectorError;
use crate::kafka::sink_config::KafkaSinkConfig;

const TRANSACTIONAL_ID_PREFIX: &str = "laminardb-";
const QUEUE_RETRY: Duration = Duration::from_millis(5);

/// The live fenced writer for one namespace.
struct FencedWriter {
    namespace_key: String,
    producer: FutureProducer,
    marker: MarkerLocation,
    /// Newest cursor this writer read after fencing or committed itself. Only this writer can
    /// advance the cursor until a successor fences it, and a fenced writer cannot commit.
    cursor: Option<CoordinatedCommitCursor>,
}

/// The exact cut whose publication outcome is unknown. Until a cursor read reconciles it, only
/// the same cut may be retried.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedPublication {
    namespace_key: String,
    target: CoordinatedCommitCursor,
    fingerprint: [u8; 32],
}

#[derive(Default)]
struct CommitterState {
    writer: Option<FencedWriter>,
    unresolved: Option<UnresolvedPublication>,
}

/// How one publication attempt ended.
enum PublishOutcome {
    Committed,
    /// The transaction was aborted; nothing from it is visible.
    Aborted(ConnectorError),
    /// The transaction may or may not have committed; the writer must be replaced.
    Unknown(String),
}

pub(super) struct KafkaExactCommitter {
    config: KafkaSinkConfig,
    commit_topic: String,
    admission: ConnectorTaskAdmission,
    state: tokio::sync::Mutex<CommitterState>,
}

impl KafkaExactCommitter {
    pub(super) fn new(
        config: KafkaSinkConfig,
        commit_topic: String,
        admission: ConnectorTaskAdmission,
    ) -> Self {
        Self {
            config,
            commit_topic,
            admission,
            state: tokio::sync::Mutex::new(CommitterState::default()),
        }
    }

    /// The writer for `namespace`, fencing every predecessor and reading the cursor on creation.
    async fn writer<'a>(
        &self,
        state: &'a mut CommitterState,
        namespace: &CoordinatedCommitNamespace,
        deadline: tokio::time::Instant,
    ) -> Result<&'a mut FencedWriter, ConnectorError> {
        namespace.validate()?;
        let namespace_key = namespace.external_key();
        if state
            .writer
            .as_ref()
            .is_some_and(|writer| writer.namespace_key != namespace_key)
        {
            self.retire(state.writer.take());
        }
        if state.writer.is_none() {
            state.writer = Some(self.fence(namespace_key, deadline).await?);
        }
        state
            .writer
            .as_mut()
            .ok_or_else(|| ConnectorError::Internal("Kafka fenced writer vanished".into()))
    }

    async fn fence(
        &self,
        namespace_key: String,
        deadline: tokio::time::Instant,
    ) -> Result<FencedWriter, ConnectorError> {
        let connection = self.config.connection_rdkafka_config();
        let topic_config = crate::kafka::topic_config::describe_topic(
            &connection,
            &self.commit_topic,
            remaining_until(deadline)?,
        )
        .await?;
        if !crate::kafka::topic_config::is_compact_only(&topic_config) {
            return Err(ConnectorError::ConfigurationError(format!(
                "Kafka commit topic '{}' must have cleanup.policy=compact so each sink's newest \
                 marker is never removed",
                self.commit_topic
            )));
        }
        let transactional_id = format!("{TRANSACTIONAL_ID_PREFIX}{namespace_key}");
        let producer_config = self.config.transactional_rdkafka_config(&transactional_id);
        let commit_topic = self.commit_topic.clone();
        let output_topic = self.config.topic.clone();
        let key = namespace_key.clone();
        let blocking_deadline = to_std(deadline);
        let fenced = self
            .run_blocking(deadline, move || {
                let marker =
                    MarkerLocation::resolve(&connection, &commit_topic, key, blocking_deadline)?;
                let producer: FutureProducer = producer_config.create().map_err(|error| {
                    ConnectorError::ConfigurationError(format!(
                        "create Kafka transactional producer: {error}"
                    ))
                })?;
                init_transactions(&producer, blocking_deadline)?;
                let output_partitions =
                    topic_partitions(&producer, &output_topic, blocking_deadline)?;
                let newest = read_newest_marker(
                    &connection,
                    &marker,
                    MarkerRead::Authoritative,
                    blocking_deadline,
                )?;
                Ok((producer, marker, newest, output_partitions))
            })
            .await?;
        let (producer, marker, newest, output_partitions) = fenced;
        if let Some(newest) = newest.as_ref() {
            if newest.topic != self.config.topic {
                return Err(ConnectorError::ConfigurationError(format!(
                    "Kafka commit marker records output topic '{}', but the sink writes '{}'",
                    newest.topic, self.config.topic
                )));
            }
            if output_partitions < newest.partition_count {
                return Err(ConnectorError::ConfigurationError(format!(
                    "Kafka topic '{}' has {output_partitions} partitions, fewer than the {} \
                     its committed output was routed over",
                    self.config.topic, newest.partition_count
                )));
            }
        }
        Ok(FencedWriter {
            namespace_key,
            producer,
            marker,
            cursor: newest.map(|marker| marker.cursor()),
        })
    }

    /// Run a blocking librdkafka call off the async workers within `deadline`. The call carries
    /// its own timeout bounded by the same deadline, so an abandoned call still finishes.
    async fn run_blocking<T: Send + 'static>(
        &self,
        deadline: tokio::time::Instant,
        work: impl FnOnce() -> Result<T, ConnectorError> + Send + 'static,
    ) -> Result<T, ConnectorError> {
        let guard = self
            .admission
            .track()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "live Kafka sink".into(),
                actual: "sink generation is closed".into(),
            })?;
        let task = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            work()
        });
        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(ConnectorError::outcome_unknown(
                format!("Kafka transaction worker failed: {error}"),
                true,
            )),
            Err(_) => Err(ConnectorError::outcome_unknown(
                "Kafka transaction step exceeded the commit deadline",
                true,
            )),
        }
    }

    /// Destroy a producer off the async workers: rdkafka's `Drop` purges and joins threads.
    fn retire(&self, writer: Option<FencedWriter>) {
        let Some(writer) = writer else {
            return;
        };
        let guard = self.admission.track();
        let teardown = move || {
            let _guard = guard;
            drop(writer);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn_blocking(teardown));
        } else {
            teardown();
        }
    }

    /// The newest marker committed so far, read without fencing. It can only lag the cursor.
    async fn advisory_marker(
        &self,
        key: String,
        deadline: tokio::time::Instant,
    ) -> Result<Option<CommitMarker>, ConnectorError> {
        let connection = self.config.connection_rdkafka_config();
        let commit_topic = self.commit_topic.clone();
        let blocking_deadline = to_std(deadline);
        self.run_blocking(deadline, move || {
            let marker =
                MarkerLocation::resolve(&connection, &commit_topic, key, blocking_deadline)?;
            read_newest_marker(
                &connection,
                &marker,
                MarkerRead::Advisory,
                blocking_deadline,
            )
        })
        .await
    }

    /// Refuse to fence when the committed marker already carries a newer authority: an obsolete
    /// committer must not abort its successor's transaction. Reading before fencing can only lag,
    /// so this never rejects a current committer.
    async fn reject_superseded_authority(
        &self,
        batch: &CoordinatedCommitBatch,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let newest = self
            .advisory_marker(batch.namespace.external_key(), deadline)
            .await?;
        match newest {
            Some(marker) if marker.fencing_token > batch.fencing_token => {
                Err(ConnectorError::TransactionError(format!(
                    "Kafka commit marker fencing token {} is newer than this committer's token {}; \
                     a successor owns the sink",
                    marker.fencing_token, batch.fencing_token
                )))
            }
            _ => Ok(()),
        }
    }

    fn decode_outputs(
        &self,
        batch: &CoordinatedCommitBatch,
        after: u64,
    ) -> Result<(Vec<OutputRecord>, i32), ConnectorError> {
        let mut records = Vec::new();
        let mut partition_count = None;
        for entry in &batch.entries {
            if entry.attempt.checkpoint_id <= after {
                continue;
            }
            let Some(payload) = entry.payload.as_deref() else {
                continue;
            };
            let output = StagedOutput::decode(payload)?;
            if output.topic != self.config.topic {
                return Err(ConnectorError::TransactionError(format!(
                    "staged Kafka output targets '{}', but the sink writes '{}'",
                    output.topic, self.config.topic
                )));
            }
            match partition_count {
                Some(count) if count != output.partition_count => {
                    return Err(ConnectorError::TransactionError(
                        "staged Kafka outputs in one commit were routed over different partition \
                         counts"
                            .into(),
                    ));
                }
                _ => partition_count = Some(output.partition_count),
            }
            records.extend(output.records);
        }
        Ok((records, partition_count.unwrap_or(0)))
    }
}

#[async_trait]
impl CoordinatedCommitter for KafkaExactCommitter {
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        let deadline = context.deadline();
        batch.validate_shape().map_err(|error| {
            ConnectorError::TransactionError(format!(
                "Kafka coordinated batch validation failed: {error}"
            ))
        })?;
        let target = CoordinatedCommitCursor {
            checkpoint_id: batch.target.checkpoint_id,
            fencing_token: batch.fencing_token,
        };
        let pending = UnresolvedPublication {
            namespace_key: batch.namespace.external_key(),
            target,
            fingerprint: batch.exact_fingerprint(),
        };
        let mut state = self.state.lock().await;
        if state
            .unresolved
            .as_ref()
            .is_some_and(|unresolved| unresolved != &pending)
        {
            return Err(ConnectorError::TransactionError(
                "Kafka has a different unresolved exactly-once publication; only that exact cut \
                 may be retried until its cursor is reconciled"
                    .into(),
            ));
        }
        let live = state
            .writer
            .as_ref()
            .is_some_and(|writer| writer.namespace_key == pending.namespace_key);
        if !live {
            self.reject_superseded_authority(&batch, deadline).await?;
        }
        let observed = self
            .writer(&mut state, &batch.namespace, deadline)
            .await?
            .cursor;
        batch.validate_observed_cursor(observed).map_err(|error| {
            ConnectorError::TransactionError(format!("Kafka exactly-once cursor: {error}"))
        })?;
        // The cursor already covers this cut, for example when the engine re-drives an older cut
        // during recovery; publishing again would duplicate output and move the marker backwards.
        if observed.is_some_and(|cursor| cursor.checkpoint_id >= target.checkpoint_id) {
            if state.unresolved.as_ref() == Some(&pending) {
                state.unresolved = None;
            }
            return Ok(());
        }
        state.unresolved = Some(pending.clone());
        let after = observed.map_or(0, |cursor| cursor.checkpoint_id);
        let (records, partition_count) = self.decode_outputs(&batch, after)?;
        let writer = state
            .writer
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("Kafka fenced writer vanished".into()))?;
        let marker = CommitMarker::new(
            target,
            pending.fingerprint,
            &self.config.topic,
            partition_count,
        )
        .encode()?;
        let producer = writer.producer.clone();
        let location = writer.marker.clone();
        let topic = self.config.topic.clone();
        let blocking_deadline = to_std(deadline);
        let outcome = self
            .run_blocking(deadline, move || {
                Ok(publish(
                    &producer,
                    &topic,
                    &records,
                    &location,
                    &marker,
                    blocking_deadline,
                ))
            })
            .await;
        match outcome {
            Ok(PublishOutcome::Committed) => {
                if let Some(writer) = state.writer.as_mut() {
                    writer.cursor = Some(target);
                }
                state.unresolved = None;
                Ok(())
            }
            Ok(PublishOutcome::Aborted(error)) => {
                state.unresolved = None;
                Err(error)
            }
            Ok(PublishOutcome::Unknown(detail)) => {
                let writer = state.writer.take();
                self.retire(writer);
                Err(ConnectorError::outcome_unknown(
                    format!(
                        "Kafka exactly-once commit of checkpoint {} has an unknown outcome: \
                         {detail}; a fenced cursor read decides it",
                        target.checkpoint_id
                    ),
                    true,
                ))
            }
            Err(error) => {
                let writer = state.writer.take();
                self.retire(writer);
                Err(error)
            }
        }
    }

    async fn committed_cursor(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        namespace.validate()?;
        let key = namespace.external_key();
        let deadline = tokio::time::Instant::now() + self.config.transaction_timeout;
        let mut state = self.state.lock().await;
        // A live writer has fenced every predecessor and its cursor is current. Without one,
        // read what is committed instead of fencing: only a commit may take the writer role, and
        // only after its authority check, so a stale reader never aborts a successor.
        let cursor = match state
            .writer
            .as_ref()
            .filter(|writer| writer.namespace_key == key)
        {
            Some(writer) => writer.cursor,
            None => self
                .advisory_marker(key, deadline)
                .await?
                .map(|marker| marker.cursor()),
        };
        if state.unresolved.as_ref().is_some_and(|pending| {
            pending.namespace_key == namespace.external_key() && cursor == Some(pending.target)
        }) {
            state.unresolved = None;
        }
        Ok(cursor)
    }
}

impl Drop for KafkaExactCommitter {
    fn drop(&mut self) {
        let writer = self.state.get_mut().writer.take();
        self.retire(writer);
    }
}

/// Produce `records` and the marker in one transaction and commit it.
fn publish(
    producer: &FutureProducer,
    topic: &str,
    records: &[OutputRecord],
    marker_location: &MarkerLocation,
    marker: &[u8],
    deadline: Instant,
) -> PublishOutcome {
    if let Err(error) = producer.begin_transaction() {
        return PublishOutcome::Unknown(format!("begin_transaction: {error}"));
    }
    for record in records {
        let mut output: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic).partition(record.partition);
        if let Some(key) = record.key.as_deref() {
            output = output.key(key);
        }
        if let Some(value) = record.value.as_deref() {
            output = output.payload(value);
        }
        if let Err(error) = enqueue(producer, output, deadline) {
            let failure = super::failure::KafkaFailure::enqueue(&error, "Kafka exactly-once");
            if failure.dlq_eligible() {
                // A record the client rejects can never be published; retrying the committed
                // cut would fail the same way forever.
                return match abort(producer, deadline, &failure.detail) {
                    PublishOutcome::Aborted(_) => {
                        PublishOutcome::Aborted(ConnectorError::ConfigurationError(format!(
                            "a committed Kafka record for partition {} cannot be published \
                             ({}); the checkpoint needs operator action",
                            record.partition, failure.detail
                        )))
                    }
                    other => other,
                };
            }
            return abort(producer, deadline, &format!("produce: {error}"));
        }
    }
    let marker_record = FutureRecord::to(&marker_location.topic)
        .partition(marker_location.partition)
        .key(marker_location.key.as_bytes())
        .payload(marker);
    if let Err(error) = enqueue(producer, marker_record, deadline) {
        return abort(
            producer,
            deadline,
            &format!("produce commit marker: {error}"),
        );
    }
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return PublishOutcome::Unknown("commit_transaction did not finish".into());
        }
        match producer.commit_transaction(left) {
            Ok(()) => return PublishOutcome::Committed,
            Err(KafkaError::Transaction(error)) if error.txn_requires_abort() => {
                return abort(producer, deadline, &format!("commit_transaction: {error}"));
            }
            Err(KafkaError::Transaction(error)) if error.is_retriable() && !error.is_fatal() => {}
            Err(error) => return PublishOutcome::Unknown(format!("commit_transaction: {error}")),
        }
    }
}

/// Enqueue one record, retrying while librdkafka's queue is full. Delivery failures surface as
/// an abortable `commit_transaction` error.
fn enqueue(
    producer: &FutureProducer,
    mut record: FutureRecord<'_, [u8], [u8]>,
    deadline: Instant,
) -> Result<(), KafkaError> {
    loop {
        match producer.send_result(record) {
            Ok(_delivery) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned))
                if Instant::now() < deadline =>
            {
                record = returned;
                std::thread::sleep(QUEUE_RETRY);
            }
            Err((error, _)) => return Err(error),
        }
    }
}

fn abort(producer: &FutureProducer, deadline: Instant, reason: &str) -> PublishOutcome {
    let left = deadline.saturating_duration_since(Instant::now());
    match producer.abort_transaction(left) {
        Ok(()) => PublishOutcome::Aborted(ConnectorError::TransactionError(format!(
            "Kafka exactly-once transaction aborted ({reason}); nothing was published"
        ))),
        Err(error) => PublishOutcome::Unknown(format!("{reason}; abort_transaction: {error}")),
    }
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

fn topic_partitions(
    producer: &FutureProducer,
    topic: &str,
    deadline: Instant,
) -> Result<i32, ConnectorError> {
    let left = deadline.saturating_duration_since(Instant::now());
    let metadata = producer
        .client()
        .fetch_metadata(Some(topic), left)
        .map_err(|error| {
            ConnectorError::ConnectionFailed(format!("Kafka metadata for '{topic}': {error}"))
        })?;
    let partitions = metadata
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .filter(|candidate| candidate.error().is_none())
        .map(|candidate| candidate.partitions().len())
        .filter(|count| *count > 0)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("Kafka topic '{topic}' does not exist"))
        })?;
    i32::try_from(partitions).map_err(|_| {
        ConnectorError::ConfigurationError(format!("Kafka topic '{topic}' has too many partitions"))
    })
}

fn remaining_until(deadline: tokio::time::Instant) -> Result<Duration, ConnectorError> {
    let left = deadline.saturating_duration_since(tokio::time::Instant::now());
    if left.is_zero() {
        return Err(ConnectorError::TransactionError(
            "Kafka exactly-once commit deadline elapsed before I/O".into(),
        ));
    }
    Ok(left)
}

fn to_std(deadline: tokio::time::Instant) -> Instant {
    deadline.into_std()
}
