//! Upsert-envelope writes: changelog collapse, merge-key validation, and keyed records with
//! tombstones for deletes.

use std::fmt::Write as _;
use std::time::Instant;

use arrow_array::StringArray;
use rdkafka::producer::{DeliveryFuture, FutureRecord};

use super::failure::{
    record_failure, unresolved_delivery_error, KafkaFailure, QUEUE_RETRY_TIMEOUT,
};
use super::{required_producer, stage_records, KafkaSink, KeyBuffer};
use crate::changelog::collapse_changelog;
use crate::connector::WriteResult;
use crate::error::ConnectorError;

fn project_upsert_values(
    collapsed: &arrow_array::RecordBatch,
) -> Result<(arrow_array::RecordBatch, StringArray), ConnectorError> {
    let operation_index = collapsed
        .schema()
        .index_of("_op")
        .map_err(|_| ConnectorError::Internal("collapsed changelog missing _op".into()))?;
    let operations = collapsed
        .column(operation_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ConnectorError::Internal("_op column is not Utf8".into()))?
        .clone();
    let value_columns = (0..collapsed.num_columns())
        .filter(|index| *index != operation_index)
        .collect::<Vec<_>>();
    let values = collapsed
        .project(&value_columns)
        .map_err(|error| ConnectorError::Internal(format!("project value columns: {error}")))?;
    Ok((values, operations))
}

fn validate_upsert_keys(keys: &KeyBuffer, rows: usize) -> Result<(), ConnectorError> {
    if keys.len() != rows {
        return Err(ConnectorError::Internal(format!(
            "upsert envelope: {} keys for {rows} rows",
            keys.len()
        )));
    }
    if let Some(row) = (0..rows).find(|row| keys.key(*row).is_none()) {
        return Err(ConnectorError::WriteError(format!(
            "upsert envelope: row {row} has a NULL merge key; a compacted topic needs a key \
             (use key.format = 'json' to encode nullable key columns)"
        )));
    }
    Ok(())
}

impl KafkaSink {
    pub(super) fn collapse_upsert_changelog(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<arrow_array::RecordBatch, ConnectorError> {
        let key_columns = self.config.key_columns();
        if key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "envelope = 'upsert' requires 'key.column'".into(),
            ));
        }
        collapse_changelog(batch, &key_columns)
    }

    /// Upsert-envelope produce: collapse the Z-set changelog to one record per merge key, then
    /// emit a keyed value for a live group (`_op = U`) or a null-value tombstone for a removed group
    /// (`_op = D`). The topic must be log-compacted and keyed on the merge key for the tombstones to
    /// GC and for the latest-per-key state to be recoverable from offset 0.
    pub(super) async fn write_upsert_batch(
        &mut self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        self.ensure_schema_ready(&batch.schema())?;
        let collapsed = self.collapse_upsert_changelog(batch)?;
        let rows = collapsed.num_rows();
        if rows == 0 {
            return Ok(WriteResult::new(0, 0));
        }
        let (value_batch, ops) = project_upsert_values(&collapsed)?;

        let payloads = self.serialize_payloads(&value_batch)?;
        let keys = self.extract_keys(&collapsed)?.ok_or_else(|| {
            ConnectorError::ConfigurationError("envelope = 'upsert' requires 'key.column'".into())
        })?;
        // Reject NULL merge keys before producing any record: a compacted topic cannot represent
        // an unkeyed row, and a mid-loop bail would leave earlier rows already enqueued.
        validate_upsert_keys(&keys, payloads.len())?;

        let partition_count = self.required_partition_count()?;
        let partitions = (0..rows)
            .map(|row| self.route(keys.key(row), partition_count))
            .collect::<Vec<_>>();
        if let Some(stage) = self.staged.as_mut() {
            let values = payloads
                .iter()
                .enumerate()
                .map(|(row, payload)| (ops.value(row) != "D").then_some(payload.as_slice()));
            return stage_records(stage, &partitions, Some(&keys), values);
        }
        let producer = required_producer(self.producer.as_ref())?;

        let mut delivery_futures = Vec::with_capacity(rows);
        let mut enqueue_failure: Option<(KafkaFailure, usize)> = None;
        let queue_deadline = Instant::now() + QUEUE_RETRY_TIMEOUT;
        for (i, payload) in payloads.iter().enumerate() {
            let is_delete = ops.value(i) == "D";
            // No `.payload()` for a delete → null value = Kafka tombstone.
            let mut record: FutureRecord<'_, [u8], [u8]> =
                FutureRecord::to(&self.config.topic).partition(partitions[i]);
            if let Some(key) = keys.key(i) {
                record = record.key(key);
            }
            if !is_delete {
                record = record.payload(payload.as_slice());
            }
            match Self::enqueue_with_queue_retry(producer, record, queue_deadline).await {
                Ok(future) => delivery_futures.push((Instant::now(), future, is_delete, i)),
                Err(error) => {
                    let mut failure = KafkaFailure::enqueue(&error, "Kafka upsert");
                    let affected = rows - i;
                    if affected > 1 {
                        let _ = write!(
                            failure.detail,
                            "; {} later record(s) were not attempted",
                            affected - 1
                        );
                    }
                    enqueue_failure = Some((failure, affected));
                    break;
                }
            }
        }

        self.settle_upsert_deliveries(delivery_futures, enqueue_failure, &payloads)
            .await
    }

    /// Await every accepted upsert delivery; a record that was not persisted, or whose outcome is
    /// unknown, fails the batch after all accepted deliveries have settled.
    #[allow(clippy::cast_possible_truncation)]
    async fn settle_upsert_deliveries(
        &self,
        delivery_futures: Vec<(Instant, DeliveryFuture, bool, usize)>,
        enqueue_failure: Option<(KafkaFailure, usize)>,
        payloads: &[Vec<u8>],
    ) -> Result<WriteResult, ConnectorError> {
        let rows = payloads.len();
        let mut records_written: usize = 0;
        let mut bytes_written: u64 = 0;
        let mut definitely_not_persisted: usize = 0;
        let mut ambiguous: usize = 0;
        let mut first_error: Option<String> = None;
        let mut retryable = true;
        for (send_time, future, is_delete, i) in delivery_futures {
            match future.await {
                Ok(Ok(_)) => {
                    self.metrics
                        .record_produce_latency(send_time.elapsed().as_micros() as u64);
                    records_written += 1;
                    if !is_delete {
                        bytes_written += payloads[i].len() as u64;
                    }
                }
                Ok(Err((err, _))) => {
                    let failure = KafkaFailure::delivery(&err, "Kafka upsert");
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
                Err(_canceled) => {
                    let failure = KafkaFailure::canceled("Kafka upsert");
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
            }
        }
        if let Some((failure, affected)) = enqueue_failure {
            record_failure(
                &failure,
                affected,
                &mut definitely_not_persisted,
                &mut ambiguous,
                &mut first_error,
                &mut retryable,
            );
        }
        self.metrics
            .record_write(records_written as u64, bytes_written);
        if definitely_not_persisted > 0 || ambiguous > 0 {
            self.metrics.record_error();
            return Err(unresolved_delivery_error(
                "upsert produce",
                rows,
                records_written,
                definitely_not_persisted,
                ambiguous,
                first_error,
                retryable,
            ));
        }
        Ok(WriteResult::new(records_written, bytes_written))
    }
}
